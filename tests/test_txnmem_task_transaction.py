from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

from txnmem_task_transaction import (
    InMemoryTransactionBackend,
    SQLiteStagingTransactionBackend,
    TaskTransactionError,
    TaskTransactionGateway,
)
from txnmem_transaction_journal import TransactionJournal


def _policy(
    version: int = 1,
    denied_actions: Sequence[str] = (),
    scope_overrides: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "version": version,
        "denied_actions": list(denied_actions),
        "scope_overrides": dict(scope_overrides or {}),
    }


class _VerificationBackend(InMemoryTransactionBackend):
    def __init__(self, status: str):
        super().__init__()
        self.status = status

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        return {"status": self.status, "txn_id": txn_id}


class _DecisionFaultJournal(TransactionJournal):
    def __init__(self, path: Path, mode: str):
        super().__init__(path)
        self.mode = mode
        self.injected = False

    def decide(self, txn_id: str, decision: str):
        if decision.upper() != "COMMITTED" or self.injected:
            return super().decide(txn_id, decision)
        self.injected = True
        if self.mode == "before_write":
            raise RuntimeError("decision write failed")
        record = super().decide(txn_id, decision)
        raise RuntimeError("decision response lost")


class TaskTransactionGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.journal = TransactionJournal(Path(self.temporary_directory.name) / "journal.sqlite3")
        self.addCleanup(self.journal.close)

    def gateway(
        self,
        *,
        txn_id: str = "txn_task_1",
        backend: InMemoryTransactionBackend | None = None,
        policy_snapshot_provider: Any = None,
        phase_hook: Any = None,
    ) -> TaskTransactionGateway:
        return TaskTransactionGateway(
            journal=self.journal,
            backend=backend or InMemoryTransactionBackend(),
            task_id=f"task_{txn_id}",
            agent_id="agent_model",
            txn_id=txn_id,
            policy_snapshot_provider=policy_snapshot_provider or (lambda: _policy()),
            phase_hook=phase_hook,
        )

    def test_begin_is_the_first_canonical_event(self) -> None:
        gateway = self.gateway()

        self.assertEqual(
            gateway.validated_events(),
            [
                {
                    "event_id": "txn_task_1:event:0001",
                    "kind": "begin_txn",
                    "step": 1,
                    "agent_id": "agent_model",
                    "txn_id": "txn_task_1",
                }
            ],
        )
        self.assertEqual(self.journal.load("txn_task_1").state, "ACTIVE")

    def test_invalid_policy_provider_keeps_coded_validation_error(self) -> None:
        with self.assertRaises(TaskTransactionError) as raised:
            self.gateway(policy_snapshot_provider=lambda: None)

        self.assertEqual(raised.exception.code, "invalid_policy_snapshot")

    def test_mutations_are_pending_and_hidden_from_other_transactions(self) -> None:
        backend = InMemoryTransactionBackend()
        owner = self.gateway(backend=backend)
        observer = self.gateway(txn_id="txn_task_2", backend=backend)

        for name, arguments in (
            ("memory_write", {"memory_id": "memory_a", "value": "a"}),
            (
                "memory_derive",
                {"memory_id": "memory_b", "source_ids": ["memory_a"], "value": "b"},
            ),
            (
                "memory_propagate",
                {"memory_id": "memory_c", "source_id": "memory_b", "value": "c"},
            ),
        ):
            self.assertEqual(
                owner.call(name, arguments),
                {"pending": True, "txn_id": "txn_task_1"},
            )

        self.assertIsNone(backend.read_committed("memory_a"))
        self.assertIsNone(observer.call("memory_read", {"memory_id": "memory_a"}))
        self.assertEqual(observer.call("memory_search", {}), [])
        self.assertEqual(
            [event["kind"] for event in owner.validated_events()],
            ["begin_txn", "memory_write", "memory_derive", "memory_propagate"],
        )

    def test_read_your_writes_records_only_committed_dependencies(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "committed_source": {
                    "memory_id": "committed_source",
                    "value": "source",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "version": 7,
                    "derived_from": [],
                }
            }
        )
        gateway = self.gateway(backend=backend)

        gateway.call("memory_write", {"memory_id": "pending_source", "value": "pending"})
        self.assertEqual(
            gateway.call("memory_read", {"memory_id": "pending_source"})["value"],
            "pending",
        )
        gateway.call(
            "memory_derive",
            {
                "memory_id": "derived_pending",
                "source_ids": ["pending_source"],
                "value": "derived",
            },
        )
        self.assertEqual(
            gateway.call("memory_read", {"memory_id": "committed_source"})["version"],
            7,
        )
        gateway.call(
            "memory_derive",
            {
                "memory_id": "derived_committed",
                "source_ids": ["committed_source"],
                "value": "derived",
            },
        )

        self.assertEqual(
            self.journal.read_set("txn_task_1"),
            [
                {
                    "txn_id": "txn_task_1",
                    "memory_id": "committed_source",
                    "observed_version": 7,
                    "scope": "tenant:user_001",
                }
            ],
        )

    def test_status_overlays_do_not_mutate_committed_state_before_decision(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "memory_old": {
                    "memory_id": "memory_old",
                    "value": "old",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "version": 2,
                    "derived_from": [],
                },
                "memory_child": {
                    "memory_id": "memory_child",
                    "value": "child",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "version": 4,
                    "derived_from": ["memory_old"],
                },
            }
        )
        gateway = self.gateway(backend=backend)

        gateway.call(
            "memory_supersede",
            {"old_memory_id": "memory_old", "new_memory_id": "memory_new", "value": "new"},
        )
        gateway.call("memory_invalidate", {"memory_id": "memory_old"})

        self.assertEqual(backend.read_committed("memory_old")["status"], "active")
        self.assertEqual(backend.read_committed("memory_old")["version"], 2)
        self.assertEqual(backend.read_committed("memory_child")["status"], "active")
        overlay_intents = [
            item for item in self.journal.intents("txn_task_1") if item["tool_name"] == "status_overlay"
        ]
        self.assertEqual(
            [(item["arguments"]["memory_id"], item["arguments"]["target_status"]) for item in overlay_intents],
            [
                ("memory_old", "superseded"),
                ("memory_old", "invalid"),
                ("memory_child", "invalid"),
            ],
        )

    def test_owner_reads_and_sources_observe_local_invalidate_and_supersede(self) -> None:
        for index, (tool_name, arguments) in enumerate(
            (
                ("memory_invalidate", {"memory_id": "old"}),
                (
                    "memory_supersede",
                    {"old_memory_id": "old", "new_memory_id": "new", "value": "new"},
                ),
            ),
            start=1,
        ):
            with self.subTest(tool_name=tool_name):
                backend = InMemoryTransactionBackend(
                    {
                        "old": {
                            "memory_id": "old",
                            "value": "old",
                            "status": "active",
                            "scope": "tenant:user_001",
                            "version": 3,
                            "derived_from": [],
                        }
                    }
                )
                gateway = self.gateway(txn_id=f"txn_overlay_{index}", backend=backend)
                gateway.call(tool_name, arguments)

                self.assertIsNone(gateway.call("memory_read", {"memory_id": "old"}))
                self.assertNotIn(
                    "old",
                    [
                        item["memory_id"]
                        for item in gateway.call("memory_search", {})
                    ],
                )
                with self.assertRaises(TaskTransactionError) as raised:
                    gateway.call(
                        "memory_derive",
                        {
                            "memory_id": "derived",
                            "source_ids": ["old"],
                            "value": "derived",
                        },
                    )
                self.assertEqual(raised.exception.code, "source_invalidated")

    def test_unchanged_policy_denial_rejects_intent_and_commit(self) -> None:
        denied_at_begin = self.gateway(
            txn_id="txn_denied_at_begin",
            policy_snapshot_provider=lambda: _policy(denied_actions=["write"]),
        )
        with self.assertRaises(TaskTransactionError) as raised:
            denied_at_begin.call(
                "memory_write", {"memory_id": "blocked", "value": "blocked"}
            )
        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.intents("txn_denied_at_begin"), [])

        policies = [_policy()]
        denied_before_call = self.gateway(
            txn_id="txn_denied_before_call",
            policy_snapshot_provider=lambda: policies[-1],
        )
        policies.append(_policy(denied_actions=["write"]))
        with self.assertRaises(TaskTransactionError) as raised:
            denied_before_call.call(
                "memory_write", {"memory_id": "blocked", "value": "blocked"}
            )
        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.intents("txn_denied_before_call"), [])

        commit_policies = [_policy()]
        denied_before_commit = self.gateway(
            txn_id="txn_denied_before_commit",
            policy_snapshot_provider=lambda: commit_policies[-1],
        )
        denied_before_commit.call(
            "memory_write", {"memory_id": "blocked", "value": "blocked"}
        )
        commit_policies.append(_policy(denied_actions=["write"]))
        with self.assertRaises(TaskTransactionError) as raised:
            denied_before_commit.commit()
        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.load("txn_denied_before_commit").state, "ABORTED")

    def test_scope_override_controls_accepted_scope_and_is_revalidated(self) -> None:
        forced = self.gateway(
            txn_id="txn_forced_scope",
            policy_snapshot_provider=lambda: _policy(
                scope_overrides={"memory_a": "tenant:forced"}
            ),
        )
        forced.call(
            "memory_write",
            {
                "memory_id": "memory_a",
                "value": "a",
                "scope": "tenant:model_requested",
            },
        )
        forced.commit()
        self.assertEqual(
            forced.coordinator.backend.read_committed("memory_a")["scope"],
            "tenant:forced",
        )

        policies = [_policy(scope_overrides={"memory_b": "tenant:first"})]
        changed = self.gateway(
            txn_id="txn_changed_scope",
            policy_snapshot_provider=lambda: policies[-1],
        )
        changed.call("memory_write", {"memory_id": "memory_b", "value": "b"})
        policies.append(_policy(scope_overrides={"memory_b": "tenant:second"}))

        with self.assertRaises(TaskTransactionError) as raised:
            changed.commit()

        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.load("txn_changed_scope").state, "ABORTED")

    def test_new_source_scope_override_is_enforced_at_commit(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "source": {
                    "memory_id": "source",
                    "value": "source",
                    "status": "active",
                    "scope": "tenant:observed",
                    "version": 2,
                    "derived_from": [],
                }
            }
        )
        policies = [_policy()]
        gateway = self.gateway(
            txn_id="txn_source_scope",
            backend=backend,
            policy_snapshot_provider=lambda: policies[-1],
        )
        gateway.call(
            "memory_derive",
            {"memory_id": "derived", "source_ids": ["source"], "value": "derived"},
        )
        policies.append(
            _policy(scope_overrides={"source": "tenant:policy_required"})
        )

        with self.assertRaises(TaskTransactionError) as raised:
            gateway.commit()

        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.load("txn_source_scope").state, "ABORTED")

    def test_changed_invalidate_scope_override_is_enforced_at_commit(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "source": {
                    "memory_id": "source",
                    "value": "source",
                    "status": "active",
                    "scope": "tenant:observed",
                    "version": 2,
                    "derived_from": [],
                }
            }
        )
        policies = [
            _policy(scope_overrides={"source": "tenant:observed"})
        ]
        gateway = self.gateway(
            txn_id="txn_invalidate_scope",
            backend=backend,
            policy_snapshot_provider=lambda: policies[-1],
        )
        gateway.call("memory_invalidate", {"memory_id": "source"})
        policies.append(
            _policy(scope_overrides={"source": "tenant:policy_required"})
        )

        with self.assertRaises(TaskTransactionError) as raised:
            gateway.commit()

        self.assertEqual(raised.exception.code, "policy_revalidation_failed")
        self.assertEqual(self.journal.load("txn_invalidate_scope").state, "ABORTED")

    def test_commit_revalidation_failures_abort_and_hide_new_objects(self) -> None:
        cases = (
            ("policy", "policy_revalidation_failed"),
            ("read_version", "read_version_changed"),
            ("source_invalid", "source_invalidated"),
            ("source_scope", "source_scope_changed"),
            ("cycle", "provenance_cycle"),
            ("partial", "backend_stage_incomplete"),
            ("unknown", "backend_state_unknown"),
        )
        for index, (scenario, expected_code) in enumerate(cases, start=1):
            with self.subTest(scenario=scenario):
                journal = TransactionJournal(
                    Path(self.temporary_directory.name) / f"failure_{index}.sqlite3"
                )
                self.addCleanup(journal.close)
                policies = [_policy()]
                initial = {
                    "source": {
                        "memory_id": "source",
                        "value": "source",
                        "status": "active",
                        "scope": "tenant:user_001",
                        "version": 3,
                        "derived_from": ["new"] if scenario == "cycle" else [],
                    },
                    "read_only": {
                        "memory_id": "read_only",
                        "value": "read",
                        "status": "active",
                        "scope": "tenant:user_001",
                        "version": 5,
                        "derived_from": [],
                    },
                }
                if scenario in {"partial", "unknown"}:
                    backend: InMemoryTransactionBackend = _VerificationBackend(scenario)
                    backend.committed.update(initial)
                else:
                    backend = InMemoryTransactionBackend(initial)
                gateway = TaskTransactionGateway(
                    journal=journal,
                    backend=backend,
                    task_id=f"task_failure_{index}",
                    agent_id="agent_model",
                    txn_id=f"txn_failure_{index}",
                    policy_snapshot_provider=lambda policies=policies: policies[-1],
                )
                gateway.call("memory_read", {"memory_id": "read_only"})
                gateway.call(
                    "memory_derive",
                    {"memory_id": "new", "source_ids": ["source"], "value": "new"},
                )

                if scenario == "policy":
                    policies.append(_policy(version=2, denied_actions=["write"]))
                elif scenario == "read_version":
                    backend.committed["read_only"]["version"] = 6
                elif scenario == "source_invalid":
                    backend.committed["source"]["status"] = "invalid"
                    backend.committed["source"]["version"] = 4
                elif scenario == "source_scope":
                    backend.committed["source"]["scope"] = "tenant:user_002"

                with self.assertRaises(TaskTransactionError) as raised:
                    gateway.commit()

                self.assertEqual(raised.exception.code, expected_code)
                self.assertEqual(journal.load(f"txn_failure_{index}").state, "ABORTED")
                self.assertNotIn("new", backend.search_committed())
                self.assertEqual(backend.raw_transaction_state(f"txn_failure_{index}", journal.intents(f"txn_failure_{index}"))["gateway_visible"], [])

    def test_valid_commit_records_ordered_phases_and_only_logical_events(self) -> None:
        backend = InMemoryTransactionBackend()
        hooks: list[str] = []
        gateway = self.gateway(
            backend=backend,
            phase_hook=lambda phase, evidence: hooks.append(phase),
        )
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "a"})

        result = gateway.commit()

        self.assertEqual(
            result["phases"],
            [
                "prepare_recorded",
                "qdrant_staged",
                "neo4j_staged",
                "stage_verified",
                "commit_decided",
                "finalize_complete",
            ],
        )
        self.assertEqual(
            hooks,
            [
                "after_prepare",
                "after_qdrant_stage",
                "after_neo4j_stage",
                "after_stage_verify",
                "after_commit_decision",
                "after_finalize",
            ],
        )
        self.assertEqual(
            [event["kind"] for event in gateway.validated_events()],
            ["begin_txn", "memory_write", "commit"],
        )
        self.assertEqual(backend.read_committed("memory_a")["status"], "active")
        frozen = self.journal.frozen_snapshot("txn_task_1")
        self.assertEqual(
            [intent["tool_name"] for intent in frozen["intents"]],
            ["memory_write"],
        )
        self.assertEqual(frozen["read_set"], [])

    def test_repeated_write_to_one_memory_stages_latest_value_once(self) -> None:
        backend = InMemoryTransactionBackend()
        gateway = self.gateway(backend=backend)
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "first"})
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "second"})

        result = gateway.commit()

        self.assertEqual(result["decision"], "COMMITTED")
        self.assertEqual(backend.read_committed("memory_a")["value"], "second")
        self.assertEqual(
            backend.raw_transaction_state(
                "txn_task_1", self.journal.intents("txn_task_1")
            )["qdrant"]["objects"],
            [{"memory_id": "memory_a"}],
        )

    def test_latest_plain_write_removes_an_earlier_derived_cycle(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "source": {
                    "memory_id": "source",
                    "value": "source",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "version": 1,
                    "derived_from": ["target"],
                }
            }
        )
        gateway = self.gateway(backend=backend)
        gateway.call(
            "memory_derive",
            {"memory_id": "target", "source_ids": ["source"], "value": "derived"},
        )
        gateway.call("memory_write", {"memory_id": "target", "value": "plain"})

        result = gateway.commit()

        self.assertEqual(result["decision"], "COMMITTED")
        self.assertEqual(backend.read_committed("target")["derived_from"], [])
        self.assertEqual(
            backend.raw_transaction_state(
                "txn_task_1", self.journal.intents("txn_task_1")
            )["neo4j"]["edges"],
            [],
        )

    def test_latest_derive_still_rejects_a_real_cycle(self) -> None:
        backend = InMemoryTransactionBackend(
            {
                "source": {
                    "memory_id": "source",
                    "value": "source",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "version": 1,
                    "derived_from": ["target"],
                }
            }
        )
        gateway = self.gateway(backend=backend)
        gateway.call("memory_write", {"memory_id": "target", "value": "plain"})
        gateway.call(
            "memory_derive",
            {"memory_id": "target", "source_ids": ["source"], "value": "derived"},
        )

        with self.assertRaises(TaskTransactionError) as raised:
            gateway.commit()

        self.assertEqual(raised.exception.code, "provenance_cycle")
        self.assertEqual(self.journal.load("txn_task_1").state, "ABORTED")

    def test_response_fault_after_commit_decision_never_reverses_decision(self) -> None:
        backend = InMemoryTransactionBackend()
        fail_after_decision = {"enabled": True}

        def phase_hook(phase: str, evidence: Mapping[str, Any]) -> None:
            if phase == "after_commit_decision" and fail_after_decision["enabled"]:
                raise RuntimeError("response lost")

        gateway = self.gateway(backend=backend, phase_hook=phase_hook)
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "a"})

        with self.assertRaises(TaskTransactionError) as raised:
            gateway.commit()

        self.assertEqual(raised.exception.code, "commit_decided_response_lost")
        self.assertEqual(self.journal.load("txn_task_1").state, "COMMITTED")
        fail_after_decision["enabled"] = False
        recovered = gateway.commit()
        self.assertEqual(recovered["decision"], "COMMITTED")
        self.assertEqual(backend.read_committed("memory_a")["status"], "active")
        self.assertEqual(self.journal.load("txn_task_1").state, "COMMITTED")

    def test_decision_write_failure_aborts_and_cleans_staged_state(self) -> None:
        journal = _DecisionFaultJournal(
            Path(self.temporary_directory.name) / "decision_before.sqlite3",
            "before_write",
        )
        self.addCleanup(journal.close)
        backend = InMemoryTransactionBackend()
        gateway = TaskTransactionGateway(
            journal=journal,
            backend=backend,
            task_id="task_decision_before",
            agent_id="agent_model",
            txn_id="txn_decision_before",
            policy_snapshot_provider=lambda: _policy(),
        )
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "a"})

        with self.assertRaises(TaskTransactionError) as raised:
            gateway.commit()

        self.assertEqual(raised.exception.code, "commit_decision_failed")
        self.assertEqual(journal.load("txn_decision_before").state, "ABORTED")
        self.assertIsNone(backend.read_committed("memory_a"))
        self.assertEqual(
            backend.raw_transaction_state(
                "txn_decision_before", journal.frozen_snapshot("txn_decision_before")["intents"]
            )["gateway_visible"],
            [],
        )

    def test_decision_response_loss_reloads_commit_and_finishes(self) -> None:
        journal = _DecisionFaultJournal(
            Path(self.temporary_directory.name) / "decision_after.sqlite3",
            "after_write",
        )
        self.addCleanup(journal.close)
        backend = InMemoryTransactionBackend()
        gateway = TaskTransactionGateway(
            journal=journal,
            backend=backend,
            task_id="task_decision_after",
            agent_id="agent_model",
            txn_id="txn_decision_after",
            policy_snapshot_provider=lambda: _policy(),
        )
        gateway.call("memory_write", {"memory_id": "memory_a", "value": "a"})

        result = gateway.commit()

        self.assertEqual(result["decision"], "COMMITTED")
        self.assertEqual(journal.load("txn_decision_after").state, "COMMITTED")
        self.assertEqual(backend.read_committed("memory_a")["value"], "a")
        self.assertIn(
            "finalize_complete",
            [phase["phase"] for phase in journal.phases("txn_decision_after")],
        )


class SQLiteStagingTransactionBackendTests(unittest.TestCase):
    def test_staging_survives_reopen_with_uniform_raw_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "staging.sqlite3"
            intents = [
                {
                    "txn_id": "txn_1",
                    "sequence": 1,
                    "tool_name": "memory_write",
                    "arguments": {"memory_id": "memory_a", "value": "a"},
                }
            ]
            hooks: list[str] = []
            backend = SQLiteStagingTransactionBackend(path)
            backend.stage_transaction(
                "txn_1", intents, lambda phase, evidence: hooks.append(phase)
            )
            backend.close()

            reopened = SQLiteStagingTransactionBackend(path)
            self.addCleanup(reopened.close)
            self.assertEqual(hooks, ["after_qdrant_stage", "after_neo4j_stage"])
            self.assertEqual(
                reopened.raw_transaction_state("txn_1", intents),
                {
                    "qdrant": {"read_ok": True, "objects": [{"memory_id": "memory_a"}]},
                    "neo4j": {
                        "read_ok": True,
                        "nodes": [{"memory_id": "memory_a"}],
                        "edges": [],
                    },
                    "gateway_visible": [],
                },
            )


class DeterministicTransactionBackendRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

    def _backend(self, adapter: str, name: str, memories: Mapping[str, Mapping[str, Any]] | None = None):
        if adapter == "memory":
            return InMemoryTransactionBackend(memories)
        backend = SQLiteStagingTransactionBackend(
            Path(self.temporary_directory.name) / f"{name}.sqlite3", memories
        )
        self.addCleanup(backend.close)
        return backend

    def test_external_invalidation_updates_committed_status_and_version(self) -> None:
        initial = {
            "source": {
                "memory_id": "source",
                "value": "source",
                "status": "active",
                "version": 3,
            }
        }
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                backend = self._backend(adapter, f"external_invalidate_{adapter}", initial)

                result = backend.invalidate_committed("source")

                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["version"], 4)
                self.assertIsNone(backend.read_committed("source"))
                self.assertEqual(backend.current_version("source"), 4)

    def test_finalized_stage_never_shadows_a_newer_committed_rewrite(self) -> None:
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                journal = TransactionJournal(
                    Path(self.temporary_directory.name) / f"shadow_{adapter}_journal.sqlite3"
                )
                self.addCleanup(journal.close)
                backend = self._backend(adapter, f"shadow_{adapter}")
                first = TaskTransactionGateway(
                    journal=journal,
                    backend=backend,
                    task_id="task_first",
                    agent_id="agent_model",
                    txn_id="txn_1",
                    policy_snapshot_provider=lambda: _policy(),
                )
                first.call("memory_write", {"memory_id": "shared", "value": "first"})
                first.commit()
                second = TaskTransactionGateway(
                    journal=journal,
                    backend=backend,
                    task_id="task_second",
                    agent_id="agent_model",
                    txn_id="txn_2",
                    policy_snapshot_provider=lambda: _policy(),
                )
                second.call("memory_write", {"memory_id": "shared", "value": "second"})
                second.commit()

                record = backend.read_committed("shared")
                self.assertEqual(record["value"], "second")
                self.assertEqual(record["version"], 2)

    def test_verify_requires_exact_supersede_and_invalidate_overlays(self) -> None:
        intents = [
            {
                "txn_id": "txn_overlay",
                "sequence": 1,
                "tool_name": "memory_supersede",
                "arguments": {
                    "old_memory_id": "old",
                    "new_memory_id": "new",
                    "value": "new",
                },
            },
            {
                "txn_id": "txn_overlay",
                "sequence": 2,
                "tool_name": "status_overlay",
                "arguments": {"memory_id": "old", "target_status": "superseded"},
            },
            {
                "txn_id": "txn_overlay",
                "sequence": 3,
                "tool_name": "status_overlay",
                "arguments": {"memory_id": "new", "target_status": "invalid"},
            },
        ]
        initial = {
            "old": {
                "memory_id": "old",
                "value": "old",
                "status": "active",
                "version": 4,
            }
        }
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                backend = self._backend(adapter, f"overlay_{adapter}", initial)
                backend.stage_transaction("txn_overlay", intents)
                self.assertEqual(
                    backend.verify_transaction("txn_overlay", intents)["status"],
                    "complete",
                )
                if adapter == "memory":
                    backend.pending["txn_overlay"]["overlays"].pop()
                else:
                    connection = sqlite3.connect(backend.path)
                    connection.execute(
                        "DELETE FROM status_overlays WHERE txn_id = ? AND sequence = ?",
                        ("txn_overlay", 3),
                    )
                    connection.commit()
                    connection.close()

                self.assertEqual(
                    backend.verify_transaction("txn_overlay", intents)["status"],
                    "partial",
                )

    def test_repeated_target_stages_only_latest_provenance_edge(self) -> None:
        intents = [
            {
                "txn_id": "txn_edges",
                "sequence": 1,
                "tool_name": "memory_derive",
                "arguments": {
                    "memory_id": "target",
                    "source_ids": ["source_a"],
                    "value": "first",
                },
            },
            {
                "txn_id": "txn_edges",
                "sequence": 2,
                "tool_name": "memory_propagate",
                "arguments": {
                    "memory_id": "target",
                    "source_ids": ["source_b"],
                    "value": "second",
                },
            },
        ]
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                backend = self._backend(adapter, f"edges_{adapter}")
                backend.stage_transaction("txn_edges", intents)

                self.assertEqual(
                    backend.raw_transaction_state("txn_edges", intents)["neo4j"]["edges"],
                    [
                        {
                            "kind": "DERIVED_FROM",
                            "source_id": "source_b",
                            "target_id": "target",
                        }
                    ],
                )

    def test_state_changing_intents_increment_staged_version_in_sequence(self) -> None:
        intents = [
            {
                "txn_id": "txn_version",
                "sequence": 1,
                "tool_name": "memory_write",
                "arguments": {"memory_id": "memory_a", "value": "updated"},
            },
            {
                "txn_id": "txn_version",
                "sequence": 2,
                "tool_name": "status_overlay",
                "arguments": {"memory_id": "memory_a", "target_status": "invalid"},
            },
        ]
        initial = {
            "memory_a": {
                "memory_id": "memory_a",
                "value": "initial",
                "status": "active",
                "version": 5,
            }
        }
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                backend = self._backend(adapter, f"version_{adapter}", initial)
                backend.stage_transaction("txn_version", intents)
                backend.finalize_transaction("txn_version", intents)

                self.assertEqual(backend.current_version("memory_a"), 7)
                self.assertIsNone(backend.read_committed("memory_a"))

    def test_later_write_reactivates_an_earlier_overlay_at_decision_point(self) -> None:
        intents = [
            {
                "txn_id": "txn_reactivate",
                "sequence": 1,
                "tool_name": "status_overlay",
                "arguments": {"memory_id": "memory_a", "target_status": "invalid"},
            },
            {
                "txn_id": "txn_reactivate",
                "sequence": 2,
                "tool_name": "memory_write",
                "arguments": {"memory_id": "memory_a", "value": "reactivated"},
            },
        ]
        initial = {
            "memory_a": {
                "memory_id": "memory_a",
                "value": "initial",
                "status": "active",
                "version": 5,
            }
        }
        for adapter in ("memory", "sqlite"):
            with self.subTest(adapter=adapter):
                backend = self._backend(adapter, f"reactivate_{adapter}", initial)
                backend.bind_decision_resolver(
                    lambda txn_id: "COMMITTED" if txn_id == "txn_reactivate" else None
                )
                backend.stage_transaction("txn_reactivate", intents)

                staged_visible = backend.read_committed("memory_a")
                self.assertEqual(staged_visible["value"], "reactivated")
                self.assertEqual(staged_visible["version"], 7)
                self.assertEqual(staged_visible["status"], "active")


if __name__ == "__main__":
    unittest.main()
