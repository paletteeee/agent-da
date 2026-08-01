import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_reference import reference_outcome  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemReferenceTests(unittest.TestCase):
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
