import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_differential import compare_result_to_oracle  # noqa: E402
from txnmem_metrics import result_row  # noqa: E402
from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import WORKLOADS, generate_instance, generate_suite  # noqa: E402


class TxnMemDifferentialTests(unittest.TestCase):
    def test_every_generated_workload_is_accepted_by_full_txnmem_and_reference(self):
        for instance in generate_suite(WORKLOADS, range(3)):
            result = run_instance(instance, "TxnMem")
            row = result_row(instance, result)
            assert row["any_violation"] == 0
            assert row["oracle_match"] == 1

    def test_txnmem_matches_independent_w1_oracle(self):
        instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})

        comparison = compare_result_to_oracle(instance, run_instance(instance, "TxnMem"))

        self.assertTrue(comparison["matches"])
        self.assertEqual(comparison["oracle_version"], "0.1")

    def test_naive_partial_write_is_rejected_by_w1_oracle(self):
        instance = generate_instance("atomic_multi_write", seed=1, config={"txn_size": 2})

        comparison = compare_result_to_oracle(instance, run_instance(instance, "Naive"))

        self.assertFalse(comparison["matches"])
        self.assertIn("committed_memory_ids", comparison["mismatches"])

    def test_crash_boundary_result_is_allowed_by_oracle_set(self):
        instance = generate_instance("crash_during_commit", seed=2)

        comparison = compare_result_to_oracle(instance, run_instance(instance, "TxnMem"))

        self.assertTrue(comparison["matches"])
        self.assertEqual(comparison["allowed_outcome_count"], 2)

    def test_interleaved_abort_preserves_the_other_transaction_buffer(self):
        """Aborting transaction A must neither commit nor clear transaction B's write."""

        instance = generate_instance("atomic_multi_write", seed=3, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_a", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_b"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")
        comparison = compare_result_to_oracle(instance, result)

        self.assertEqual(result["committed_memory_ids"], ["m_b"])
        self.assertEqual(
            result["transaction_states"], {"txn_a": "aborted", "txn_b": "committed"}
        )
        self.assertTrue(comparison["matches"])


if __name__ == "__main__":
    unittest.main()
