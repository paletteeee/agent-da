import sys
from tempfile import TemporaryDirectory
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from locomo_official_eval import (  # noqa: E402
    aggregate_scores,
    normalize_qa_for_evaluator,
    parse_batch_answers,
)
from locomo_paired_eval import (  # noqa: E402
    aggregate_repetition_summaries,
    aggregate_paired_scores,
    build_ingestion_system_prompt,
    build_paired_question_prompt,
    format_memory_context,
    resolve_category5_answer,
    run_paired_repetitions,
)


class LoCoMoOfficialEvalTests(unittest.TestCase):
    def test_parse_batch_answers_accepts_json_and_strips_code_fence(self):
        raw = "```json\n{\"0\": \" May 7, 2023 \", \"1\": \"Alice\"}\n```"
        self.assertEqual(
            parse_batch_answers(raw, 2),
            {0: "May 7, 2023", 1: "Alice"},
        )

    def test_parse_batch_answers_falls_back_to_numbered_lines(self):
        self.assertEqual(
            parse_batch_answers("1. first answer\n2) second answer", 2),
            {0: "first answer", 1: "second answer"},
        )

    def test_parse_batch_answers_accepts_zero_based_numbered_lines(self):
        self.assertEqual(
            parse_batch_answers("0: first answer\n1: second answer", 2),
            {0: "first answer", 1: "second answer"},
        )

    def test_aggregate_scores_reports_question_and_category_counts(self):
        result = aggregate_scores(
            [
                {"category": 1, "score": 0.5},
                {"category": 1, "score": 1.0},
                {"category": 5, "score": 0.0},
            ]
        )
        self.assertEqual(result["question_count"], 3)
        self.assertAlmostEqual(result["mean_f1"], 0.5)
        self.assertEqual(result["category_counts"], {"1": 2, "5": 1})

    def test_normalize_qa_uses_locomo_adversarial_answer_when_answer_is_absent(self):
        self.assertEqual(
            normalize_qa_for_evaluator(
                {"question": "q", "category": 5, "adversarial_answer": "known"}
            )["answer"],
            "known",
        )

    def test_paired_question_prompt_requires_memory_and_has_stable_question_ids(self):
        prompt, _choice_maps = build_paired_question_prompt(
            [
                {"question": "Where did Alice move?", "category": 1},
                {"question": "When did that happen?", "category": 2},
            ]
        )
        self.assertIn("Use the memory tools", prompt)
        self.assertIn("0: Where did Alice move?", prompt)
        self.assertIn("1: When did that happen?", prompt)
        self.assertIn("JSON object", prompt)

    def test_aggregate_paired_scores_keeps_native_and_qa_evidence_separate(self):
        report = aggregate_paired_scores(
            [{"sample_id": "conv-1", "category": 1, "score": 0.5}],
            native_event_count=7,
            sample_count=1,
            ingestion_completed=1,
        )
        self.assertEqual(report["question_count"], 1)
        self.assertAlmostEqual(report["mean_f1"], 0.5)
        self.assertEqual(report["native_event_count"], 7)
        self.assertEqual(report["ingestion_completed"], 1)
        self.assertNotIn("oracle_match", report)

    def test_format_memory_context_is_backend_output_not_original_conversation(self):
        context = format_memory_context(
            [{"memory_id": "m1", "value": "Alice moved to Boston", "status": "active"}]
        )
        self.assertIn("m1", context)
        self.assertIn("Alice moved to Boston", context)
        self.assertNotIn("conversation", context.lower())

    def test_tuned_profile_strengthens_ingestion_and_memory_qa_instructions(self):
        ingestion = build_ingestion_system_prompt("tuned")
        prompt, _choice_maps = build_paired_question_prompt(
            [{"question": "Where did Alice move?", "category": 1}],
            prompt_profile="tuned",
        )

        self.assertIn("atomic facts", ingestion)
        self.assertIn("stable memory_id", ingestion)
        self.assertIn("cross-check", prompt)
        self.assertIn("backend-retrieved", prompt)

    def test_category5_choice_parser_requires_isolated_label(self):
        choices = {"a": "known answer", "b": "adversarial answer"}

        self.assertEqual(resolve_category5_answer("Option B", choices), "adversarial answer")
        self.assertEqual(resolve_category5_answer("(A)", choices), "known answer")
        self.assertEqual(
            resolve_category5_answer("No information available", choices),
            "No information available",
        )
        self.assertEqual(resolve_category5_answer("banana", choices), "banana")

    def test_repetition_aggregate_keeps_profile_denominators_and_token_usage(self):
        aggregate = aggregate_repetition_summaries(
            [
                {
                    "status": "available",
                    "sample_count": 10,
                    "question_count": 1986,
                    "mean_f1": 0.2,
                    "category_mean_f1": {"1": 0.1, "2": 0.3},
                    "native_event_count": 300,
                    "model_usage": {
                        "request_count": 20,
                        "responses_with_usage": 20,
                        "prompt_tokens": 1000,
                        "completion_tokens": 200,
                        "total_tokens": 1200,
                    },
                },
                {
                    "status": "available",
                    "sample_count": 10,
                    "question_count": 1986,
                    "mean_f1": 0.4,
                    "category_mean_f1": {"1": 0.3, "2": 0.5},
                    "native_event_count": 320,
                    "model_usage": {
                        "request_count": 22,
                        "responses_with_usage": 22,
                        "prompt_tokens": 1100,
                        "completion_tokens": 220,
                        "total_tokens": 1320,
                    },
                },
            ],
            prompt_profile="tuned",
            model="qwen2.5-7b-instruct",
        )

        self.assertEqual(aggregate["repetition_count"], 2)
        self.assertEqual(aggregate["total_question_evaluations"], 3972)
        self.assertAlmostEqual(aggregate["mean_f1_mean"], 0.3)
        self.assertEqual(aggregate["prompt_profile"], "tuned")
        self.assertEqual(aggregate["model_usage"]["total_tokens"], 2520)
        self.assertEqual(aggregate["category_f1_mean"], {"1": 0.2, "2": 0.4})

    def test_repetition_runner_records_exact_seed_schedule(self):
        fixture_summary = {
            "status": "available",
            "sample_count": 1,
            "question_count": 2,
            "mean_f1": 0.5,
            "native_event_count": 1,
            "model_usage": {
                "request_count": 1,
                "responses_with_usage": 1,
                "prompt_tokens": 10,
                "completion_tokens": 2,
                "total_tokens": 12,
            },
        }
        with TemporaryDirectory() as tmp:
            with patch("locomo_paired_eval.run_paired_eval", return_value=fixture_summary) as run:
                report = run_paired_repetitions(
                    "locomo.json",
                    tmp,
                    "http://model.test/v1",
                    "qwen",
                    repetitions=3,
                    seed=17,
                    prompt_profile="tuned",
                )

        self.assertEqual(report["repetition_seeds"], [17, 1017, 2017])
        self.assertEqual([call.kwargs["seed"] for call in run.call_args_list], [17, 1017, 2017])

    def test_partial_repetition_is_not_counted_as_successful_score(self):
        aggregate = aggregate_repetition_summaries(
            [
                {
                    "status": "available",
                    "sample_count": 1,
                    "question_count": 2,
                    "mean_f1": 0.5,
                    "category_mean_f1": {"1": 0.5},
                    "native_event_count": 1,
                    "qa_batch_attempt_count": 1,
                    "qa_batch_success_count": 1,
                    "qa_batch_failure_count": 0,
                    "qa_question_attempt_count": 2,
                    "qa_question_successful_response_count": 2,
                    "model_usage": {
                        "request_count": 2,
                        "responses_with_usage": 2,
                        "prompt_tokens": 10,
                        "completion_tokens": 2,
                        "total_tokens": 12,
                    },
                },
                {
                    "status": "partial",
                    "sample_count": 1,
                    "question_count": 2,
                    "mean_f1": 0.0,
                    "category_mean_f1": {"1": 0.0},
                    "native_event_count": 1,
                    "qa_batch_attempt_count": 1,
                    "qa_batch_success_count": 0,
                    "qa_batch_failure_count": 1,
                    "qa_question_attempt_count": 2,
                    "qa_question_successful_response_count": 0,
                    "model_usage": {
                        "request_count": 2,
                        "responses_with_usage": 1,
                        "prompt_tokens": 5,
                        "completion_tokens": 1,
                        "total_tokens": 6,
                    },
                },
            ],
            prompt_profile="baseline",
            model="qwen",
        )

        self.assertEqual(aggregate["status"], "partial")
        self.assertEqual(aggregate["successful_repetition_count"], 1)
        self.assertEqual(aggregate["mean_f1_by_repetition"], [0.5, None])
        self.assertEqual(aggregate["qa_batch_failure_count_total"], 1)
        self.assertEqual(aggregate["qa_question_successful_response_count_total"], 2)
        self.assertFalse(aggregate["token_usage_complete"])


if __name__ == "__main__":
    unittest.main()
