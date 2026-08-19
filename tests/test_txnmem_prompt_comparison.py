import unittest

from txnmem_prompt_comparison import (
    compare_appworld_prompt_profiles,
    compare_locomo_prompt_profiles,
)


class TxnMemPromptComparisonTests(unittest.TestCase):
    def test_locomo_comparison_pairs_identical_repetition_seeds(self):
        common = {
            "model": "qwen2.5-7b-instruct",
            "condition_fingerprint": "same-condition",
            "task_manifest_sha256": "same-task-manifest",
            "model_identity": {"model": "qwen2.5-7b-instruct", "revision": "same"},
            "repetition_count": 5,
            "repetition_seeds": [17, 1017, 2017, 3017, 4017],
            "question_count_per_repetition": [100, 100, 100, 100, 100],
            "sample_count_per_repetition": [10, 10, 10, 10, 10],
            "category_f1_mean": {"1": 0.1, "2": 0.2},
            "model_usage": {"total_tokens": 1000},
            "token_usage_complete": True,
        }
        report = compare_locomo_prompt_profiles(
            {
                **common,
                "prompt_profile": "baseline",
                "mean_f1_by_repetition": [0.1, 0.2, 0.3, 0.4, 0.5],
                "conversation_score_summaries": [
                    {
                        "conversation_id_sha256": "a" * 64,
                        "question_evaluation_count": 10,
                        "score_sum": 2.0,
                        "mean_f1": 0.2,
                    },
                    {
                        "conversation_id_sha256": "b" * 64,
                        "question_evaluation_count": 20,
                        "score_sum": 8.0,
                        "mean_f1": 0.4,
                    },
                ],
            },
            {
                **common,
                "prompt_profile": "tuned",
                "mean_f1_by_repetition": [0.2, 0.3, 0.5, 0.5, 0.7],
                "category_f1_mean": {"1": 0.15, "2": 0.25},
                "model_usage": {"total_tokens": 1200},
                "conversation_score_summaries": [
                    {
                        "conversation_id_sha256": "a" * 64,
                        "question_evaluation_count": 10,
                        "score_sum": 3.0,
                        "mean_f1": 0.3,
                    },
                    {
                        "conversation_id_sha256": "b" * 64,
                        "question_evaluation_count": 20,
                        "score_sum": 12.0,
                        "mean_f1": 0.6,
                    },
                ],
            },
        )

        self.assertEqual(report["paired_repetition_count"], 5)
        self.assertEqual(report["paired_mean_f1_deltas"], [0.1, 0.1, 0.2, 0.1, 0.2])
        self.assertAlmostEqual(report["mean_f1_delta"], 0.14)
        self.assertAlmostEqual(report["category_f1_delta"]["1"], 0.05)
        self.assertEqual(report["token_delta"], 200)
        self.assertEqual(report["token_delta_status"], "exact")
        self.assertEqual(report["paired_conversation_count"], 2)
        self.assertEqual(
            report["paired_conversation_cluster_interval"]["sampling_unit"],
            "whole_group",
        )
        self.assertAlmostEqual(
            report["paired_conversation_cluster_interval"]["estimate"],
            1 / 6,
        )

    def test_locomo_comparison_rejects_seed_mismatch(self):
        baseline = {
            "model": "qwen",
            "condition_fingerprint": "same-condition",
            "prompt_profile": "baseline",
            "repetition_count": 1,
            "repetition_seeds": [17],
            "mean_f1_by_repetition": [0.1],
            "question_count_per_repetition": [1],
            "sample_count_per_repetition": [1],
        }
        with self.assertRaises(ValueError):
            compare_locomo_prompt_profiles(
                baseline,
                {**baseline, "prompt_profile": "tuned", "repetition_seeds": [18]},
            )

    def test_locomo_comparison_rejects_non_formal_schedule_and_task_mismatch(self):
        common = {
            "model": "qwen",
            "model_identity": {"model": "qwen", "revision": "same"},
            "condition_fingerprint": "same-condition",
            "task_manifest_sha256": "same-task-manifest",
            "repetition_seeds": [17, 1017, 2017],
            "mean_f1_by_repetition": [0.1, 0.1, 0.1],
            "question_count_per_repetition": [1, 1, 1],
            "sample_count_per_repetition": [1, 1, 1],
        }
        with self.assertRaisesRegex(ValueError, "five-seed"):
            compare_locomo_prompt_profiles(
                {**common, "prompt_profile": "baseline"},
                {**common, "prompt_profile": "tuned"},
            )
        formal = {
            **common,
            "repetition_seeds": [17, 1017, 2017, 3017, 4017],
            "mean_f1_by_repetition": [0.1] * 5,
            "question_count_per_repetition": [1] * 5,
            "sample_count_per_repetition": [1] * 5,
        }
        with self.assertRaisesRegex(ValueError, "task manifests differ"):
            compare_locomo_prompt_profiles(
                {**formal, "prompt_profile": "baseline"},
                {
                    **formal,
                    "prompt_profile": "tuned",
                    "task_manifest_sha256": "different",
                },
            )

    def test_locomo_comparison_rejects_condition_fingerprint_mismatch(self):
        common = {
            "model": "qwen",
            "repetition_seeds": [17],
            "mean_f1_by_repetition": [0.1],
            "question_count_per_repetition": [1],
            "sample_count_per_repetition": [1],
            "model_usage": {"total_tokens": 10},
            "token_usage_complete": True,
        }
        with self.assertRaises(ValueError):
            compare_locomo_prompt_profiles(
                {
                    **common,
                    "prompt_profile": "baseline",
                    "condition_fingerprint": "condition-a",
                },
                {
                    **common,
                    "prompt_profile": "tuned",
                    "condition_fingerprint": "condition-b",
                },
            )

    def test_appworld_comparison_pairs_task_ids_and_official_assertions(self):
        def summary(profile, successes, passes, tokens):
            return {
                "benchmark": "appworld",
                "manifest_sha256": "same-manifest",
                "condition_fingerprint": "same-condition",
                "prompt_profile": profile,
                "unique_task_count": 2,
                "repetitions": 1,
                "official": {
                    "successes": sum(successes),
                    "trials": 2,
                    "pass_count": sum(passes),
                    "total_count": 14,
                },
                "model_usage": {"total_tokens": tokens},
                "token_usage_complete": True,
                "task_summaries": [
                    {
                        "task_id": f"task-{index}",
                        "status": "completed",
                        "failure_code": "no_events" if index == 1 else None,
                        "model_visible_benchmark_tool_count": 10 + index,
                        "model_visible_benchmark_tool_names_sha256": f"digest-{index}",
                        "official": {
                            "success": success,
                            "pass_count": pass_count,
                            "total_count": 7,
                        },
                    }
                    for index, (success, pass_count) in enumerate(zip(successes, passes), 1)
                ],
            }

        report = compare_appworld_prompt_profiles(
            summary("baseline", [False, False], [1, 2], 1000),
            summary("tuned", [True, False], [7, 4], 1400),
        )

        self.assertEqual(report["paired_task_count"], 2)
        self.assertEqual(report["official_success_delta"], 1)
        self.assertAlmostEqual(report["official_assertion_rate_delta"], 8 / 14)
        self.assertEqual(report["improved_task_count"], 2)
        self.assertEqual(report["baseline_status_counts"], {"completed": 2})
        self.assertEqual(report["tuned_status_counts"], {"completed": 2})
        self.assertEqual(report["baseline_failure_code_counts"], {})
        self.assertEqual(report["tuned_failure_code_counts"], {})
        self.assertEqual(report["token_delta"], 400)
        self.assertEqual(report["token_delta_status"], "exact")

    def test_appworld_comparison_counts_blocked_task_as_failure_and_excludes_its_assertions(self):
        baseline = {
            "manifest_sha256": "same",
            "condition_fingerprint": "same-condition",
            "prompt_profile": "baseline",
            "model_usage": {"total_tokens": 100},
            "token_usage_complete": True,
            "task_summaries": [
                {"task_id": "task-1", "model_visible_benchmark_tool_count": 10, "model_visible_benchmark_tool_names_sha256": "digest-1", "official": {"status": "available", "success": False, "pass_count": 1, "total_count": 7}},
                {"task_id": "task-2", "model_visible_benchmark_tool_count": 11, "model_visible_benchmark_tool_names_sha256": "digest-2", "official": {"status": "available", "success": False, "pass_count": 2, "total_count": 5}},
            ],
            "official": {"successes": 0, "trials": 2, "pass_count": 3, "total_count": 12},
        }
        tuned = {
            "manifest_sha256": "same",
            "condition_fingerprint": "same-condition",
            "prompt_profile": "tuned",
            "model_usage": {"total_tokens": 200},
            "token_usage_complete": False,
            "task_summaries": [
                {"task_id": "task-1", "model_visible_benchmark_tool_count": 10, "model_visible_benchmark_tool_names_sha256": "digest-1", "official": {"status": "available", "success": True, "pass_count": 7, "total_count": 7}},
                {"task_id": "task-2", "model_visible_benchmark_tool_count": 11, "model_visible_benchmark_tool_names_sha256": "digest-2", "status": "failed", "failure_code": "model_http_error", "official": None},
            ],
            "official": {"successes": 1, "trials": 1, "pass_count": 7, "total_count": 7},
        }

        report = compare_appworld_prompt_profiles(baseline, tuned)

        self.assertEqual(report["paired_task_count"], 2)
        self.assertEqual(report["paired_available_assertion_task_count"], 1)
        self.assertEqual(report["official_success_tuned_all_tasks"], 1)
        self.assertEqual(report["official_success_tuned_denominator"], 2)
        self.assertEqual(report["tuned_unavailable_task_count"], 1)
        self.assertEqual(report["official_assertion_total_common"], 7)
        self.assertAlmostEqual(report["official_assertion_rate_delta_common"], 6 / 7)
        self.assertIsNone(report["token_delta"])
        self.assertEqual(report["observed_token_delta"], 100)
        self.assertEqual(report["token_delta_status"], "observed_lower_bound_only")

    def test_appworld_comparison_rejects_model_visible_tool_mismatch(self):
        common = {
            "manifest_sha256": "same",
            "condition_fingerprint": "same-condition",
            "model_usage": {"total_tokens": 100},
            "token_usage_complete": True,
        }
        baseline = {
            **common,
            "prompt_profile": "baseline",
            "task_summaries": [
                {
                    "task_id": "task-1",
                    "model_visible_benchmark_tool_count": 10,
                    "model_visible_benchmark_tool_names_sha256": "baseline-digest",
                    "official": {"success": False, "pass_count": 1, "total_count": 7},
                }
            ],
        }
        tuned = {
            **common,
            "prompt_profile": "tuned",
            "task_summaries": [
                {
                    "task_id": "task-1",
                    "model_visible_benchmark_tool_count": 10,
                    "model_visible_benchmark_tool_names_sha256": "tuned-digest",
                    "official": {"success": False, "pass_count": 2, "total_count": 7},
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "model-visible tool attestation differs"):
            compare_appworld_prompt_profiles(baseline, tuned)


if __name__ == "__main__":
    unittest.main()
