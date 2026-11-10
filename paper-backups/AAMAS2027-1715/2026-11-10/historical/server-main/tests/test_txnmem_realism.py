import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_realism import compare_distributions, extract_trace_features  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
