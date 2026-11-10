import unittest

from txnmem_statistics import (
    aggregate_native_repetitions,
    binomial_interval,
    controlled_diversity,
    controlled_violation_saturation,
)
from txnmem_simulator import VARIANTS
from txnmem_workloads import WORKLOADS, WORKLOAD_SEMANTIC_PARAMETERS, generate_suite


class TxnMemStatisticsTests(unittest.TestCase):
    @staticmethod
    def _controlled_rows():
        rows = []
        for family in ("family_a", "family_b"):
            for seed in range(4):
                for variant in ("Naive", "TxnMem"):
                    rows.append(
                        {
                            "instance_id": f"{family}_seed_{seed}",
                            "workload": family,
                            "seed": seed,
                            "variant": variant,
                            "any_violation": int(variant == "Naive" and seed == 0),
                            "oracle_match": int(variant == "TxnMem" or seed > 0),
                        }
                    )
        return rows

    def test_zero_trials_returns_zero_width_safe_interval(self):
        interval = binomial_interval(0, 0)
        self.assertEqual(interval["estimate"], 0.0)
        self.assertEqual(interval["lower"], 0.0)
        self.assertEqual(interval["upper"], 0.0)

    def test_perfect_success_has_unit_estimate_and_bounded_interval(self):
        interval = binomial_interval(10, 10)
        self.assertEqual(interval["estimate"], 1.0)
        self.assertGreaterEqual(interval["lower"], 0.0)
        self.assertEqual(interval["upper"], 1.0)

    def test_aggregate_separates_expected_failures_from_contract_success(self):
        reports = [
            {
                "task_count": 2,
                "native_event_count": 3,
                "evaluation_error_count": 0,
                "task_summaries": [
                    {"task_evaluator": {"success": True}},
                    {"task_evaluator": {"success": True}, "failure_code": "injected_crash"},
                ],
                "variants": {"TxnMem": {"count": 2, "oracle_matched": 2}},
            }
        ]
        aggregate = aggregate_native_repetitions(reports)
        self.assertEqual(aggregate["total_tasks"], 2)
        self.assertEqual(aggregate["contract_success_count"], 2)
        self.assertEqual(aggregate["expected_failure_counts"], {"injected_crash": 1})
        self.assertEqual(aggregate["txnmem_oracle_match_count"], 2)

    def test_aggregation_is_deterministic_and_reports_confidence_intervals(self):
        report = {
            "task_count": 1,
            "native_event_count": 1,
            "evaluation_error_count": 0,
            "task_summaries": [{"task_evaluator": {"success": True}}],
            "variants": {"TxnMem": {"count": 1, "oracle_matched": 1}},
        }
        first = aggregate_native_repetitions([report, report])
        second = aggregate_native_repetitions([report, report])
        self.assertEqual(first, second)
        self.assertIn("contract_success_interval", first)
        self.assertIn("txnmem_oracle_match_interval", first)
        self.assertEqual(first["repetitions"], 2)

    def test_controlled_saturation_uses_balanced_nested_family_seed_prefixes(self):
        rows = self._controlled_rows()

        domains = {
            "approved_families": ("family_a", "family_b"),
            "approved_variants": ("Naive", "TxnMem"),
        }
        first = controlled_violation_saturation(rows, [2, 4], **domains)
        second = controlled_violation_saturation(reversed(rows), [2, 4], **domains)

        self.assertEqual(first, second)
        self.assertEqual(first["checkpoint_seed_counts"], [2, 4])
        checkpoint = first["checkpoints"][0]
        self.assertEqual(checkpoint["checkpoint_seed_count"], 2)
        self.assertEqual(checkpoint["family_count"], 2)
        self.assertEqual(checkpoint["instance_count"], 4)
        self.assertEqual(checkpoint["instance_unit"], "family_seed")
        naive = next(row for row in checkpoint["variants"] if row["variant"] == "Naive")
        self.assertEqual(naive["variant_result_count"], 4)
        self.assertEqual(naive["variant_result_unit"], "family_seed_variant")
        self.assertEqual(naive["violations"], 2)
        self.assertEqual(naive["violation_rate"], 0.5)
        self.assertEqual(naive["violation_interval"], binomial_interval(2, 4))
        self.assertEqual(naive["oracle_matches"], 2)
        self.assertEqual(naive["oracle_match_rate"], 0.5)
        self.assertEqual(naive["oracle_match_interval"], binomial_interval(2, 4))

    def test_controlled_saturation_rejects_incomplete_or_ambiguous_cells(self):
        rows = self._controlled_rows()
        domains = {
            "approved_families": ("family_a", "family_b"),
            "approved_variants": ("Naive", "TxnMem"),
        }
        malformed = {
            "duplicate": [*rows, dict(rows[0])],
            "missing": rows[:-1],
            "unknown extra": [
                *rows,
                {
                    **rows[0],
                    "variant": "Lookalike",
                },
            ],
            "imbalanced domains": [
                row
                for row in rows
                if not (row["workload"] == "family_b" and row["seed"] == 3)
            ],
        }
        for label, candidate in malformed.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                controlled_violation_saturation(candidate, [2, 4], **domains)
        for checkpoints in ([], [0], [2, 2], [4, 2], [5]):
            with self.subTest(checkpoints=checkpoints), self.assertRaises(ValueError):
                controlled_violation_saturation(rows, checkpoints, **domains)

    def test_controlled_saturation_rejects_whole_missing_or_extra_domains(self):
        rows = self._controlled_rows()
        domains = {
            "approved_families": ("family_a", "family_b"),
            "approved_variants": ("Naive", "TxnMem"),
        }
        extra_variant = [
            *rows,
            *[
                {**row, "variant": "Unknown"}
                for row in rows
                if row["variant"] == "TxnMem"
            ],
        ]
        extra_family = [
            *rows,
            *[
                {
                    **row,
                    "instance_id": row["instance_id"].replace("family_b", "family_c"),
                    "workload": "family_c",
                }
                for row in rows
                if row["workload"] == "family_b"
            ],
        ]
        malformed = {
            "missing whole variant": [row for row in rows if row["variant"] != "Naive"],
            "missing whole family": [row for row in rows if row["workload"] != "family_b"],
            "extra complete variant": extra_variant,
            "extra complete family": extra_family,
        }
        for label, candidate in malformed.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                controlled_violation_saturation(candidate, [4], **domains)

        with self.assertRaises(ValueError):
            controlled_violation_saturation(rows, [4])

    def test_controlled_saturation_rejects_coordinate_and_binary_metric_tampering(self):
        rows = self._controlled_rows()
        domains = {
            "approved_families": ("family_a", "family_b"),
            "approved_variants": ("Naive", "TxnMem"),
        }
        wrong_coordinate = [dict(row) for row in rows]
        wrong_coordinate[1]["instance_id"] = "different-id"
        wrong_but_consistent = [
            {**row, "instance_id": f"custom_{row['workload']}_{row['seed']}"}
            for row in rows
        ]
        reused_id = [dict(row) for row in rows]
        reused_id[4]["instance_id"] = reused_id[0]["instance_id"]
        invalid_binary = [dict(row) for row in rows]
        invalid_binary[0]["any_violation"] = 2
        for candidate in (wrong_coordinate, wrong_but_consistent, reused_id, invalid_binary):
            with self.assertRaises(ValueError):
                controlled_violation_saturation(candidate, [4], **domains)

    def test_controlled_diversity_recomputes_executable_fingerprints_and_coverage(self):
        ranges = {
            "txn_size": [1, 4],
            "provenance_depth": [1, 4],
            "branch_factor": [1, 3],
            "policy_churn": [0, 2],
            "concurrency": [1, 3],
        }
        instances = generate_suite(WORKLOADS, range(200), parameter_ranges=ranges)

        report = controlled_diversity(instances)

        self.assertEqual(report["family_count"], len(WORKLOADS))
        atomic = report["families"]["atomic_multi_write"]
        self.assertEqual(atomic["instance_count"], 200)
        self.assertGreater(atomic["unique_semantic_fingerprint_count"], 1)
        self.assertEqual(
            atomic["parameter_value_counts"]["txn_size"],
            {"1": 56, "2": 45, "3": 55, "4": 44},
        )
        self.assertEqual(atomic["parameter_coverage"]["txn_size"]["approved_interval"], [1, 4])
        self.assertEqual(atomic["parameter_coverage"]["txn_size"]["coverage_ratio"], 1.0)
        self.assertEqual(atomic["parameter_combination_coverage"]["coverage_ratio"], 1.0)

        tampered = [dict(instance) for instance in instances]
        tampered[0] = {**tampered[0], "semantic_fingerprint": "0" * 64}
        with self.assertRaisesRegex(ValueError, "semantic_fingerprint"):
            controlled_diversity(tampered)

    def test_controlled_diversity_enforces_registered_family_parameters_and_config(self):
        ranges = {
            "txn_size": [1, 4],
            "provenance_depth": [1, 4],
            "branch_factor": [1, 3],
            "policy_churn": [0, 2],
            "concurrency": [1, 3],
        }
        instances = generate_suite(WORKLOADS, range(4), parameter_ranges=ranges)
        branch_index = next(
            index
            for index, row in enumerate(instances)
            if row["workload"] == "provenance_branch_repair"
        )
        family_contract = set(WORKLOAD_SEMANTIC_PARAMETERS["provenance_branch_repair"])
        self.assertEqual(
            set(instances[branch_index]["semantic_parameters"]), family_contract
        )

        missing_dimension = [dict(row) for row in instances]
        missing_parameters = dict(missing_dimension[branch_index]["semantic_parameters"])
        missing_parameters.pop("branch_factor")
        missing_dimension[branch_index] = {
            **missing_dimension[branch_index],
            "semantic_parameters": missing_parameters,
        }
        unknown_dimension = [dict(row) for row in instances]
        unknown_dimension[branch_index] = {
            **unknown_dimension[branch_index],
            "semantic_parameters": {
                **unknown_dimension[branch_index]["semantic_parameters"],
                "txn_size": 1,
            },
        }
        config_mismatch = [dict(row) for row in instances]
        config_mismatch[branch_index] = {
            **config_mismatch[branch_index],
            "config": {
                **config_mismatch[branch_index]["config"],
                "branch_factor": 99,
            },
        }
        missing_seed = instances[:-1]
        duplicate_seed = [*instances, dict(instances[0])]
        for label, candidate in {
            "missing dimension": missing_dimension,
            "unknown dimension": unknown_dimension,
            "config mismatch": config_mismatch,
            "missing seed": missing_seed,
            "duplicate seed": duplicate_seed,
        }.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                controlled_diversity(candidate)

    def test_default_saturation_domains_are_exact_registered_domains(self):
        instances = generate_suite(WORKLOADS, range(1), parameter_ranges={
            "txn_size": [1, 4], "provenance_depth": [1, 4],
            "branch_factor": [1, 3], "policy_churn": [0, 2], "concurrency": [1, 3],
        })
        rows = [
            {
                "instance_id": instance["instance_id"],
                "workload": instance["workload"],
                "seed": instance["seed"],
                "variant": variant,
                "any_violation": 0,
                "oracle_match": 1,
            }
            for instance in instances
            for variant in VARIANTS
        ]
        report = controlled_violation_saturation(rows, [1])
        self.assertEqual(report["families"], sorted(WORKLOADS))
        self.assertEqual(report["variant_domain"], sorted(VARIANTS))
