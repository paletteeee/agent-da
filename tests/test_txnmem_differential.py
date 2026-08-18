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


if __name__ == "__main__":
    unittest.main()
