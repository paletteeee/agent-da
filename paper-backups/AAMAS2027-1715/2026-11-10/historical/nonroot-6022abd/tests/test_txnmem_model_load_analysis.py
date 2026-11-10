import unittest

from txnmem_model_load_analysis import analyze_model_load_summary


class TxnMemModelLoadAnalysisTests(unittest.TestCase):
    def test_analysis_separates_stable_cycles_from_cross_host_outage(self):
        report = analyze_model_load_summary(
            {
                "completed_cycles": 3,
                "attempt_count": 6,
                "task_summaries": [
                    {"cycle": 1, "failure_code": None, "task_evaluator": {"success": True}},
                    {"cycle": 1, "failure_code": "injected_crash", "task_evaluator": {"success": True}},
                    {"cycle": 2, "failure_code": None, "task_evaluator": {"success": True}},
                    {"cycle": 2, "failure_code": "model_http_error", "task_evaluator": {"success": False}},
                    {"cycle": 3, "failure_code": "model_http_error", "task_evaluator": {"success": False}},
                    {"cycle": 3, "failure_code": "model_http_error", "task_evaluator": {"success": False}},
                ],
                "model_usage": {
                    "request_count": 10,
                    "responses_with_usage": 7,
                    "total_tokens": 500,
                },
            }
        )

        self.assertEqual(report["first_endpoint_or_transport_failure_cycle"], 2)
        self.assertEqual(report["stable_prefix_cycle_count"], 1)
        self.assertEqual(report["stable_prefix_attempt_count"], 2)
        self.assertEqual(report["stable_prefix_contract_success_count"], 2)
        self.assertEqual(report["endpoint_or_transport_failure_attempt_count"], 3)
        self.assertEqual(report["full_failure_cycles"], [3])
        self.assertFalse(report["token_usage_complete"])
        self.assertTrue(report["reported_token_total_is_lower_bound_for_all_requests"])

    def test_missing_usage_marks_lower_bound_without_assuming_network_loss(self):
        report = analyze_model_load_summary(
            {
                "completed_cycles": 1,
                "task_summaries": [
                    {"cycle": 1, "failure_code": None, "task_evaluator": {"success": True}}
                ],
                "model_usage": {
                    "request_count": 2,
                    "responses_with_usage": 1,
                    "total_tokens": 100,
                },
            }
        )

        self.assertEqual(report["endpoint_or_transport_failure_attempt_count"], 0)
        self.assertTrue(report["reported_token_total_is_lower_bound_for_all_requests"])


if __name__ == "__main__":
    unittest.main()
