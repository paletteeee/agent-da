import unittest

from txnmem_statistics import aggregate_native_repetitions, binomial_interval


class TxnMemStatisticsTests(unittest.TestCase):
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
