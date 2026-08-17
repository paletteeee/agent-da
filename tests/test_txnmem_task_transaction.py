from __future__ import annotations

import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
