import copy
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_reference import reference_outcome  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemReferenceTests(unittest.TestCase):
    def _instance(self, operations, *, initial_memories=None, edges=None, failure_schedule=None):
        return {
            "instance_id": "task_1_reference",
            "workload": "trace_grounded_replay",
            "seed": 0,
            "config": {},
            "initial_memories": initial_memories or [],
            "policies": [],
            "failure_schedule": failure_schedule or [],
            "provenance_edges": edges or [],
            "operations": operations,
        }

    def test_reference_import_does_not_load_transaction_implementation_modules(self):
        process = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json, sys; import txnmem_reference; print(json.dumps(sorted(sys.modules)))",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        )

        loaded_modules = set(json.loads(process.stdout))
        self.assertFalse(
            loaded_modules
            & {
                "txnmem_transaction_gateway",
                "txnmem_transaction_journal",
                "txnmem_transaction_recovery",
                "txnmem_vector_graph_backend",
            }
        )

    def test_explicit_abort_discards_staged_writes_and_records_reason(self):
        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "write", "step": 2, "type": "write", "txn_id": "txn_1", "memory_id": "m1", "agent_id": "agent_1"},
                    {"op_id": "abort", "step": 3, "type": "abort", "txn_id": "txn_1", "abort_reason": "MODEL_CANCELLED", "agent_id": "agent_1"},
                ]
            )
        )

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_1"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])
        abort_event = next(event for event in oracle["event_trace"] if event["operation_id"] == "abort")
        self.assertEqual(abort_event["decision"], "aborted")
        self.assertEqual(abort_event["reason_codes"], ["MODEL_CANCELLED"])

        default_oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_2", "agent_id": "agent_1"},
                    {"op_id": "abort", "step": 2, "type": "abort", "txn_id": "txn_2", "agent_id": "agent_1"},
                ]
            )
        )
        default_abort = next(event for event in default_oracle["event_trace"] if event["operation_id"] == "abort")
        self.assertEqual(default_abort["reason_codes"], ["EXPLICIT_ABORT"])

    def test_terminal_abort_cannot_be_revived_by_write_or_commit(self):
        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "write", "step": 2, "type": "write", "txn_id": "txn_1", "memory_id": "m1", "agent_id": "agent_1"},
                    {"op_id": "abort", "step": 3, "type": "abort", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "late_write", "step": 4, "type": "write", "txn_id": "txn_1", "memory_id": "m2", "agent_id": "agent_1"},
                    {"op_id": "late_commit", "step": 5, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
                ]
            )
        )

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_1"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])
        terminal_events = [
            event
            for event in oracle["event_trace"]
            if event["operation_id"] in {"late_write", "late_commit"}
        ]
        self.assertEqual([event["reason_codes"] for event in terminal_events], [["TERMINAL_TRANSACTION"], ["TERMINAL_TRANSACTION"]])

    def test_post_commit_process_crash_keeps_committer_and_aborts_active_siblings(self):
        """This normative crash boundary is not an ambiguous pre-linearization outcome."""

        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin_a", "step": 1, "type": "begin_txn", "txn_id": "txn_a", "agent_id": "agent_1"},
                    {"op_id": "begin_b", "step": 2, "type": "begin_txn", "txn_id": "txn_b", "agent_id": "agent_1"},
                    {"op_id": "write_b", "step": 3, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "agent_id": "agent_1"},
                    {"op_id": "commit_b", "step": 4, "type": "commit", "txn_id": "txn_b", "agent_id": "agent_1"},
                ],
                failure_schedule=[
                    {"trigger": {"after_operation": "commit_b"}, "type": "crash", "target": "txn_b", "phase": "after_operation"}
                ],
            )
        )

        self.assertEqual(oracle["allowed_outcomes"], [{
            "txn_states": {"txn_a": "aborted", "txn_b": "committed"},
            "committed_memory_ids": ["m_b"],
            "visible_memory_ids": ["m_b"],
            "invalid_memory_ids": [],
            "superseded_memory_ids": [],
            "provenance_edges": [],
            "policy_version": 1,
            "invariants": {
                "atomicity": True,
                "commit_authorization": True,
                "no_invalid_visibility": True,
                "supersession_consistency": True,
                "provenance_closure": True,
                "graph_validity": True,
            },
        }])
        self.assertEqual(oracle["oracle_version"], "0.3")

    def test_unfinished_supersession_and_invalidation_transactions_abort_at_end_of_run(self):
        """Every staged mutation category is terminally aborted without a commit."""

        supersession = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_super", "agent_id": "agent_1"},
                    {"op_id": "supersede", "step": 2, "type": "supersede", "txn_id": "txn_super", "old_memory_id": "m_old", "new_memory_id": "m_new", "agent_id": "agent_1"},
                ],
                initial_memories=[
                    {"memory_id": "m_old", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
                    {"memory_id": "m_new", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
                ],
            )
        )["allowed_outcomes"][0]
        invalidation = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_invalidate", "agent_id": "agent_1"},
                    {"op_id": "invalidate", "step": 2, "type": "invalidate", "txn_id": "txn_invalidate", "memory_id": "m_root", "agent_id": "agent_1"},
                ],
                initial_memories=[{"memory_id": "m_root", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"}],
            )
        )["allowed_outcomes"][0]

        self.assertEqual(supersession["txn_states"], {"txn_super": "aborted"})
        self.assertEqual(supersession["superseded_memory_ids"], [])
        self.assertEqual(invalidation["txn_states"], {"txn_invalidate": "aborted"})
        self.assertEqual(invalidation["invalid_memory_ids"], [])
        self.assertEqual(reference_outcome(self._instance([]))["oracle_version"], "0.3")

    def test_transactional_invalidation_applies_only_when_transaction_commits(self):
        initial_memories = [
            {"memory_id": "root", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
            {"memory_id": "child", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
        ]
        edges = [{"source_id": "root", "derived_id": "child", "relation": "read_derive"}]
        def outcome_for(terminal_type):
            return reference_outcome(
                self._instance(
                    [
                        {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                        {"op_id": "invalidate", "step": 2, "type": "invalidate", "txn_id": "txn_1", "memory_id": "root", "agent_id": "agent_1"},
                        {"op_id": terminal_type, "step": 3, "type": terminal_type, "txn_id": "txn_1", "agent_id": "agent_1"},
                    ],
                    initial_memories=initial_memories,
                    edges=edges,
                )
            )["allowed_outcomes"][0]

        aborted = outcome_for("abort")
        committed = outcome_for("commit")

        self.assertEqual(aborted["txn_states"]["txn_1"], "aborted")
        self.assertEqual(aborted["invalid_memory_ids"], [])
        self.assertEqual(committed["txn_states"]["txn_1"], "committed")
        self.assertEqual(committed["invalid_memory_ids"], ["child", "root"])

    def test_source_invalidation_after_derive_aborts_commit_before_publish(self):
        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "derive", "step": 2, "type": "derive", "txn_id": "txn_1", "memory_id": "derived", "source_ids": ["root"], "scope": "tenant:user_001", "agent_id": "agent_1"},
                    {"op_id": "commit", "step": 3, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
                ],
                initial_memories=[{"memory_id": "root", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"}],
                failure_schedule=[{"trigger": {"after_operation": "derive"}, "type": "invalidate", "target": "root"}],
            )
        )

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_1"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])
        self.assertEqual(outcome["invalid_memory_ids"], ["root"])
        self.assertNotIn("derived", outcome["visible_memory_ids"])

    def test_policy_revoke_after_write_aborts_commit_during_revalidation(self):
        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "write", "step": 2, "type": "write", "txn_id": "txn_1", "memory_id": "m1", "agent_id": "agent_1"},
                    {"op_id": "commit", "step": 3, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
                ],
                failure_schedule=[{"trigger": {"after_operation": "write"}, "type": "revoke", "target": "write"}],
            )
        )

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_1"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])
        commit_event = next(event for event in oracle["event_trace"] if event["operation_id"] == "commit")
        self.assertEqual(commit_event["reason_codes"], ["POLICY_REVOKED"])

    def test_policy_revoke_after_derive_revalidates_the_authorized_action(self):
        oracle = reference_outcome(
            self._instance(
                [
                    {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                    {"op_id": "derive", "step": 2, "type": "derive", "txn_id": "txn_1", "memory_id": "derived", "source_ids": ["root"], "scope": "tenant:user_001", "agent_id": "agent_1"},
                    {"op_id": "commit", "step": 3, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
                ],
                initial_memories=[{"memory_id": "root", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"}],
                failure_schedule=[{"trigger": {"after_operation": "derive"}, "type": "revoke", "target": "derive"}],
            )
        )

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_1"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])

    def test_scheduled_supersede_revoke_denies_before_or_aborts_after_operation(self):
        schedules = (
            (
                "before",
                {"trigger": {"before_operation": "supersede"}, "type": "revoke", "target": "supersede"},
                "denied",
                "committed",
            ),
            (
                "after",
                {"trigger": {"after_operation": "supersede"}, "type": "revoke", "target": "supersede"},
                "allowed",
                "aborted",
            ),
        )
        for phase, schedule, operation_decision, transaction_state in schedules:
            with self.subTest(phase=phase):
                oracle = reference_outcome(
                    self._instance(
                        [
                            {"op_id": "begin", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                            {"op_id": "supersede", "step": 2, "type": "supersede", "txn_id": "txn_1", "old_memory_id": "old", "new_memory_id": "new", "agent_id": "agent_1"},
                            {"op_id": "commit", "step": 3, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
                        ],
                        initial_memories=[
                            {"memory_id": "old", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
                            {"memory_id": "new", "agent_id": "agent_1", "scope": "tenant:user_001", "status": "active"},
                        ],
                        failure_schedule=[schedule],
                    )
                )

                outcome = oracle["allowed_outcomes"][0]
                supersede_event = next(
                    event
                    for event in oracle["event_trace"]
                    if event["operation_id"] == "supersede" and event["event_type"] == "supersede"
                )
                self.assertEqual(supersede_event["decision"], operation_decision)
                self.assertEqual(outcome["txn_states"]["txn_1"], transaction_state)
                self.assertEqual(outcome["superseded_memory_ids"], [])
    def test_reference_ignores_generator_expected_outcome(self):
        instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})
        instance = copy.deepcopy(instance)
        instance["expected_outcome"] = {
            "transaction_state": "committed",
            "committed_memory_ids": ["m_write_1", "m_write_2"],
        }

        oracle = reference_outcome(instance)

        self.assertEqual(len(oracle["allowed_outcomes"]), 1)
        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(outcome["txn_states"]["txn_001"], "aborted")
        self.assertEqual(outcome["committed_memory_ids"], [])

    def test_reference_exposes_both_atomic_commit_boundary_outcomes(self):
        instance = generate_instance("crash_during_commit", seed=1)

        oracle = reference_outcome(instance)

        states = {outcome["txn_states"]["txn_001"] for outcome in oracle["allowed_outcomes"]}
        self.assertEqual(states, {"aborted", "committed"})
        committed = next(
            outcome
            for outcome in oracle["allowed_outcomes"]
            if outcome["txn_states"]["txn_001"] == "committed"
        )
        self.assertEqual(committed["committed_memory_ids"], ["m_commit"])

    def test_reference_repairs_transitive_legacy_chain(self):
        instance = generate_instance(
            "provenance_chain_repair", seed=2, config={"provenance_depth": 3}
        )

        oracle = reference_outcome(instance)

        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(
            set(outcome["invalid_memory_ids"]),
            {"m_root", "m_derived_1", "m_derived_2", "m_derived_3"},
        )
        self.assertEqual(outcome["visible_memory_ids"], [])

    def test_reference_derives_provenance_from_operation(self):
        instance = {
            "instance_id": "derive_seed_0",
            "workload": "provenance_chain_repair",
            "seed": 0,
            "config": {},
            "initial_memories": [
                {
                    "memory_id": "m_root",
                    "agent_id": "agent_1",
                    "scope": "tenant:user_001",
                    "status": "active",
                    "value": "root",
                }
            ],
            "policies": [],
            "failure_schedule": [],
            "provenance_edges": [],
            "operations": [
                {"op_id": "op_001", "step": 1, "type": "begin_txn", "txn_id": "txn_1", "agent_id": "agent_1"},
                {"op_id": "op_002", "step": 2, "type": "read", "txn_id": "txn_1", "agent_id": "agent_1", "memory_id": "m_root", "scope": "tenant:user_001"},
                {"op_id": "op_003", "step": 3, "type": "derive", "txn_id": "txn_1", "agent_id": "agent_1", "source_ids": ["m_root"], "memory_id": "m_derived", "value": "derived", "scope": "tenant:user_001"},
                {"op_id": "op_004", "step": 4, "type": "commit", "txn_id": "txn_1", "agent_id": "agent_1"},
            ],
        }

        oracle = reference_outcome(instance)

        edges = oracle["allowed_outcomes"][0]["provenance_edges"]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["source_id"], "m_root")
        self.assertEqual(edges[0]["derived_id"], "m_derived")
        self.assertEqual(edges[0]["relation"], "read_derive")


if __name__ == "__main__":
    unittest.main()
