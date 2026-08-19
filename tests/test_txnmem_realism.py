import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_realism import (  # noqa: E402
    bootstrap_mean_interval,
    compare_distributions,
    cross_fitted_realism,
    extract_trace_features,
    multivariate_rff_mmd_test,
)
from txnmem_trace_pipeline import leave_one_group_out  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemRealismTests(unittest.TestCase):
    def test_formal_realism_config_freezes_group_units_and_seed(self):
        config = json.loads((ROOT / "configs" / "realism_scale.json").read_text())

        self.assertEqual(config["seed"], 17)
        self.assertEqual(config["appworld"]["group_key"], "family_id")
        self.assertEqual(config["appworld"]["evaluation_family_count"], 50)
        self.assertIsNone(config["appworld"]["calibration_family_count"])
        self.assertEqual(config["locomo"]["group_key"], "conversation_id")
        self.assertEqual(len(config["synthetic"]["workloads"]), 8)
        self.assertEqual(len(config["synthetic"]["seeds"]), 10)

    def test_leave_one_group_out_never_leaks_a_group(self):
        records = [
            {"family_id": f"family-{group}", "row": row}
            for group in range(10)
            for row in range(2)
        ]

        folds = leave_one_group_out(records, "family_id")

        self.assertEqual(len(folds), 10)
        held_out = []
        for fold in folds:
            train_groups = {row["family_id"] for row in fold["train_records"]}
            holdout_groups = {row["family_id"] for row in fold["holdout_records"]}
            self.assertEqual(len(holdout_groups), 1)
            self.assertTrue(train_groups.isdisjoint(holdout_groups))
            held_out.extend(holdout_groups)
        self.assertEqual(sorted(held_out), [f"family-{group}" for group in range(10)])

    def test_cross_fitted_realism_calibrates_independently_per_group(self):
        records = [
            {
                "family_id": f"family-{group}",
                "operation_count": group + 2,
                "transaction_size": group % 3 + 1,
                "policy_change_rate": 0.0,
                "provenance_depth": 0,
                "branch_factor": 0,
                "agent_count": 1,
            }
            for group in range(10)
        ]
        calibration_groups = []

        def recording_calibrator(rows, base_config=None):
            calibration_groups.append({row["family_id"] for row in rows})
            return {"txn_size": 2, "agent_count": 1}

        report = cross_fitted_realism(
            records,
            "family_id",
            parameter_ranges={"txn_size": [1, 3]},
            seeds=[0, 1],
            workloads=["atomic_multi_write"],
            calibrator=recording_calibrator,
            bootstrap_repetitions=20,
            joint_test_permutations=9,
            joint_test_dimensions=8,
            cluster_bootstrap_repetitions=50,
            seed=17,
        )

        self.assertEqual(len(report["folds"]), 10)
        self.assertEqual(len(calibration_groups), 10)
        self.assertTrue(all(len(groups) == 9 for groups in calibration_groups))
        for group in range(10):
            self.assertEqual(
                sum(f"family-{group}" not in groups for groups in calibration_groups),
                1,
            )
        self.assertEqual(report["cluster_aggregate"]["group_count"], 10)
        self.assertEqual(report["calibration_invocation_count"], 10)
        self.assertIsNotNone(report["low_sample_warning"])
        self.assertIn("not evidence", report["claim_boundary"])

    def test_disjoint_family_mode_never_calibrates_on_evaluation_families(self):
        records = [
            {
                "family_id": f"family-{group}",
                "operation_count": group + 2,
                "transaction_size": 1,
                "policy_change_rate": 0.0,
                "provenance_depth": 0,
                "branch_factor": 0,
                "agent_count": 1,
            }
            for group in range(12)
        ]
        calls = []

        def recording_calibrator(rows, base_config=None):
            calls.append({row["family_id"] for row in rows})
            return {"txn_size": 1, "agent_count": 1}

        report = cross_fitted_realism(
            records,
            "family_id",
            parameter_ranges={"txn_size": [1, 2]},
            seeds=[0],
            workloads=["atomic_multi_write"],
            calibrator=recording_calibrator,
            evaluation_groups=[f"family-{group}" for group in range(10)],
            calibration_groups=["family-10", "family-11"],
            bootstrap_repetitions=10,
            joint_test_permutations=9,
            joint_test_dimensions=8,
            cluster_bootstrap_repetitions=20,
            seed=17,
        )

        self.assertEqual(report["fold_count"], 10)
        self.assertEqual(report["split_method"], "disjoint_calibration_and_evaluation_groups")
        self.assertEqual(calls, [{"family-10", "family-11"}] * 10)
        self.assertTrue(all(fold["train_group_count"] == 2 for fold in report["folds"]))

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

    def test_joint_rff_mmd_test_is_deterministic_for_identical_samples(self):
        sample = [
            {name: float(index + offset) for offset, name in enumerate((
                "operation_count",
                "transaction_size",
                "policy_change_rate",
                "provenance_depth",
                "branch_factor",
                "agent_count",
            ))}
            for index in range(6)
        ]

        result = multivariate_rff_mmd_test(
            sample,
            list(sample),
            permutations=99,
            rff_dimensions=32,
            seed=17,
        )

        self.assertEqual(result["status"], "available")
        self.assertAlmostEqual(result["statistic"], 0.0)
        self.assertEqual(result["p_value"], 1.0)
        self.assertEqual(
            result,
            multivariate_rff_mmd_test(
                sample,
                list(sample),
                permutations=99,
                rff_dimensions=32,
                seed=17,
            ),
        )

    def test_joint_rff_mmd_detects_a_clear_multivariate_shift(self):
        left = [
            {name: float(index % 3) for name in (
                "operation_count",
                "transaction_size",
                "policy_change_rate",
                "provenance_depth",
                "branch_factor",
                "agent_count",
            )}
            for index in range(30)
        ]
        right = [
            {name: float(20 + index % 3) for name in (
                "operation_count",
                "transaction_size",
                "policy_change_rate",
                "provenance_depth",
                "branch_factor",
                "agent_count",
            )}
            for index in range(30)
        ]

        result = multivariate_rff_mmd_test(
            left,
            right,
            permutations=199,
            rff_dimensions=64,
            seed=23,
        )

        self.assertEqual(result["status"], "available")
        self.assertLessEqual(result["p_value"], 0.02)
        self.assertGreater(result["statistic"], 0.0)

    def test_distribution_comparison_includes_a_joint_multivariate_test(self):
        left = [{"operation_count": index, "transaction_size": index % 2} for index in range(8)]
        right = [{"operation_count": index + 1, "transaction_size": (index + 1) % 2} for index in range(8)]

        comparison = compare_distributions(
            left,
            right,
            bootstrap_repetitions=20,
            joint_test_permutations=19,
            joint_test_dimensions=16,
            seed=17,
        )

        self.assertEqual(comparison["multivariate_test"]["status"], "available")
        self.assertEqual(comparison["multivariate_test"]["permutations"], 19)
        self.assertIn("joint", comparison["comparison_method"])


if __name__ == "__main__":
    unittest.main()
