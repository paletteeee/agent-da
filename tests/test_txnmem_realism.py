import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_realism import bootstrap_mean_interval, compare_distributions, extract_trace_features  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemRealismTests(unittest.TestCase):
    def test_trace_features_cover_transaction_policy_and_provenance_shape(self):
        instance = generate_instance("provenance_chain_repair", 0, {"provenance_depth": 2})

        features = extract_trace_features(instance["operations"], instance["failure_schedule"])

        self.assertEqual(features["operation_count"], len(instance["operations"]))
        self.assertEqual(features["provenance_depth"], 2)
        self.assertGreaterEqual(features["transaction_size"], 2)

    def test_distribution_comparison_is_deterministic(self):
        left = [extract_trace_features(generate_instance("atomic_multi_write", 0)["operations"], [])]
        right = [
            {
                "operation_count": 4,
                "transaction_size": 2,
                "policy_change_rate": 0.0,
                "provenance_depth": 0,
                "branch_factor": 0,
                "agent_count": 1,
            }
        ]

        comparison = compare_distributions(left, right)

        self.assertEqual(comparison["features"]["operation_count"]["mean_abs_diff"], 0.0)
        self.assertIn("transaction_size", comparison["features"])

    def test_bootstrap_mean_interval_is_deterministic_and_bounded(self):
        interval = bootstrap_mean_interval([1.0, 2.0, 3.0], repetitions=200, seed=17)
        self.assertEqual(interval["sample_count"], 3)
        self.assertAlmostEqual(interval["estimate"], 2.0)
        self.assertLessEqual(interval["lower"], interval["estimate"])
        self.assertGreaterEqual(interval["upper"], interval["estimate"])
        self.assertEqual(interval, bootstrap_mean_interval([1.0, 2.0, 3.0], repetitions=200, seed=17))

    def test_distribution_comparison_reports_bootstrap_intervals(self):
        comparison = compare_distributions(
            [{"operation_count": 1, "transaction_size": 1}],
            [{"operation_count": 3, "transaction_size": 3}],
            bootstrap_repetitions=100,
            seed=17,
        )
        operation = comparison["features"]["operation_count"]
        self.assertIn("synthetic_mean_interval", operation)
        self.assertIn("trace_mean_interval", operation)
        self.assertIn("mean_abs_diff_interval", operation)
        self.assertEqual(comparison["bootstrap_repetitions"], 100)


if __name__ == "__main__":
    unittest.main()
