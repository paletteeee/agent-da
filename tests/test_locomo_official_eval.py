import json
import sys
from tempfile import TemporaryDirectory
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from locomo_official_eval import (  # noqa: E402
    aggregate_scores,
    iter_conversation_sessions,
    normalize_qa_for_evaluator,
    parse_batch_answers,
)
from locomo_paired_eval import (  # noqa: E402
    FORMAL_PAIRED_SEEDS,
    aggregate_repetition_summaries,
    aggregate_paired_scores,
    build_ingestion_system_prompt,
    build_paired_question_prompt,
    conversation_namespace,
    format_memory_context,
    ingest_session_stream,
    resolve_category5_answer,
    run_paired_eval,
    run_paired_repetitions,
)
from txnmem_model_protocol import ModelResponse, TokenUsage  # noqa: E402


class LoCoMoOfficialEvalTests(unittest.TestCase):
    @staticmethod
    def _stream_sample():
        return {
            "sample_id": "conversation-1",
            "conversation": {
                "session_3": [{"speaker": "B", "text": "third " * 12}],
                "session_3_date_time": "2024-01-03",
                "session_1": [{"speaker": "A", "text": "first " * 12}],
                "session_1_date_time": "2024-01-01",
                "session_2": [{"speaker": "A", "text": "second " * 12}],
                "session_2_date_time": "2024-01-02",
            },
        }

    def test_conversation_sessions_are_complete_and_chronological(self):
        sessions = list(iter_conversation_sessions(self._stream_sample()))

        self.assertEqual(
            [session["session_id"] for session in sessions],
            ["session_1", "session_2", "session_3"],
        )
        self.assertEqual(
            [session["date_time"] for session in sessions],
            ["2024-01-01", "2024-01-02", "2024-01-03"],
        )
        self.assertTrue(all(session["char_count"] == len(session["content"]) for session in sessions))

    def test_session_stream_bounds_each_request_without_truncating_history(self):
        calls = []

        def ingest(payload):
            calls.append(dict(payload))
            return {"status": "completed"}

        report = ingest_session_stream(
            self._stream_sample(),
            ingest,
            max_session_chars=64,
        )

        self.assertGreater(report["source_char_count"], 64)
        self.assertEqual(report["source_session_count"], 3)
        self.assertEqual(report["attempted_session_count"], 3)
        self.assertEqual(report["attempted_char_count"], report["source_char_count"])
        self.assertEqual(report["completed_char_count"], report["source_char_count"])
        self.assertEqual(report["character_attempt_coverage"], 1.0)
        self.assertEqual(report["character_completion_coverage"], 1.0)
        self.assertEqual(report["source_stream_sha256"], report["attempted_stream_sha256"])
        self.assertEqual(
            list(dict.fromkeys(call["session_id"] for call in calls)),
            ["session_1", "session_2", "session_3"],
        )
        self.assertTrue(all(len(call["content"]) <= 64 for call in calls))

    def test_namespace_isolated_by_conversation_profile_and_repetition_seed(self):
        namespaces = {
            conversation_namespace("conversation-1", "baseline", 17),
            conversation_namespace("conversation-2", "baseline", 17),
            conversation_namespace("conversation-1", "tuned", 17),
            conversation_namespace("conversation-1", "baseline", 1017),
        }

        self.assertEqual(len(namespaces), 4)
        self.assertTrue(all(namespace.startswith("locomo:") for namespace in namespaces))

    def test_paired_runner_streams_all_sessions_into_one_isolated_namespace(self):
        sample = {
            **self._stream_sample(),
            "qa": [{"question": "What happened?", "answer": "answer", "category": 1}],
        }
        agent_calls = []
        search_namespaces = []

        class FakeClient:
            def __init__(self, *_args, **_kwargs):
                pass

            def complete(self, *_args, **_kwargs):
                return ModelResponse(
                    text='{"0":"answer"}',
                    tool_calls=[],
                    usage=TokenUsage(10, 2, 12),
                )

        class FakeBackend:
            def __init__(self, _path):
                self._events = []

            def search(self, *, query, agent_id, projection):
                search_namespaces.append(agent_id)
                return [{"memory_id": "m1", "value": "answer"}]

            def validated_events(self):
                return list(self._events)

            def close(self):
                pass

        def fake_agent(task, *_args, **_kwargs):
            agent_calls.append(dict(task))
            return {
                "status": "completed",
                "failure_code": None,
                "model_usage": {
                    "request_count": 1,
                    "responses_with_usage": 1,
                    "prompt_tokens": 10,
                    "completion_tokens": 2,
                    "total_tokens": 12,
                },
            }

        evaluator = types.ModuleType("task_eval.evaluation")
        evaluator.__file__ = __file__
        evaluator.eval_question_answering = lambda qas, _field: (
            [1.0 for _ in qas],
            None,
            None,
        )
        task_eval = types.ModuleType("task_eval")
        task_eval.evaluation = evaluator
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "locomo.json"
            source.write_text(json.dumps([sample]), encoding="utf-8")
            with patch.dict(
                sys.modules,
                {"task_eval": task_eval, "task_eval.evaluation": evaluator},
            ), patch("locomo_paired_eval.OpenAICompatibleClient", FakeClient), patch(
                "locomo_paired_eval.SQLiteInstrumentedMemoryBackend", FakeBackend
            ), patch("locomo_paired_eval.run_real_agent", side_effect=fake_agent):
                summary = run_paired_eval(
                    source,
                    root / "out",
                    "http://model.test/v1",
                    "qwen",
                    batch_size=1,
                    max_session_chars=64,
                    seed=17,
                    prompt_profile="baseline",
                    model_revision="a" * 64,
                )

        expected_namespace = conversation_namespace("conversation-1", "baseline", 17)
        self.assertGreater(len(agent_calls), 3)
        self.assertEqual({call["agent_id"] for call in agent_calls}, {expected_namespace})
        self.assertEqual(search_namespaces, [expected_namespace])
        coverage = summary["ingestion_coverage"]
        self.assertEqual(coverage["source_session_count"], 3)
        self.assertEqual(coverage["source_char_count"], coverage["attempted_char_count"])
        self.assertEqual(
            coverage["source_stream_set_sha256"],
            coverage["attempted_stream_set_sha256"],
        )
        self.assertNotIn("sample_id", summary["phase_rows"][0])
        self.assertEqual(summary["conversation_score_summaries"][0]["question_count"], 1)

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
                    "model": "qwen2.5-7b-instruct",
                    "sample_count": 1,
                    "question_count": 1986,
                    "mean_f1": 0.2,
                    "condition_fingerprint": "same-condition",
                    "task_manifest_sha256": "same-task-manifest",
                    "model_identity": {"model": "qwen2.5-7b-instruct", "revision": "same"},
                    "conversation_score_summaries": [
                        {
                            "conversation_id_sha256": "a" * 64,
                            "question_count": 1986,
                            "score_sum": 397.2,
                            "mean_f1": 0.2,
                        }
                    ],
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
                    "model": "qwen2.5-7b-instruct",
                    "sample_count": 1,
                    "question_count": 1986,
                    "mean_f1": 0.4,
                    "condition_fingerprint": "same-condition",
                    "task_manifest_sha256": "same-task-manifest",
                    "model_identity": {"model": "qwen2.5-7b-instruct", "revision": "same"},
                    "conversation_score_summaries": [
                        {
                            "conversation_id_sha256": "a" * 64,
                            "question_count": 1986,
                            "score_sum": 794.4,
                            "mean_f1": 0.4,
                        }
                    ],
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

    def test_repetition_runner_records_exact_five_seed_schedule(self):
        fixture_summary = {
            "status": "available",
            "model": "qwen",
            "sample_count": 1,
            "question_count": 2,
            "mean_f1": 0.5,
            "task_manifest_sha256": "same-task-manifest",
            "model_identity": {"model": "qwen", "revision": "revision"},
            "condition_fingerprint": "same-condition",
            "conversation_score_summaries": [
                {
                    "conversation_id_sha256": "a" * 64,
                    "question_count": 2,
                    "score_sum": 1.0,
                    "mean_f1": 0.5,
                }
            ],
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
                    repetitions=5,
                    seed=17,
                    prompt_profile="tuned",
                )

        self.assertEqual(tuple(report["repetition_seeds"]), FORMAL_PAIRED_SEEDS)
        self.assertEqual(
            [call.kwargs["seed"] for call in run.call_args_list],
            list(FORMAL_PAIRED_SEEDS),
        )
        self.assertEqual(report["cluster_bootstrap_interval"]["group_count"], 1)

    def test_formal_repetition_runner_rejects_non_five_schedule(self):
        with TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "exactly five"):
                run_paired_repetitions(
                    "locomo.json",
                    tmp,
                    "http://model.test/v1",
                    "qwen",
                    repetitions=3,
                    seed=17,
                    prompt_profile="baseline",
                )

    def test_repetition_aggregate_rejects_task_or_model_identity_mismatch(self):
        base = {
            "status": "available",
            "model": "qwen",
            "sample_count": 1,
            "question_count": 1,
            "mean_f1": 1.0,
            "native_event_count": 1,
            "condition_fingerprint": "same-condition",
            "task_manifest_sha256": "same-task-manifest",
            "model_identity": {"model": "qwen", "revision": "same"},
            "conversation_score_summaries": [
                {
                    "conversation_id_sha256": "a" * 64,
                    "question_count": 1,
                    "score_sum": 1.0,
                    "mean_f1": 1.0,
                }
            ],
            "model_usage": {
                "request_count": 1,
                "responses_with_usage": 1,
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }
        with self.assertRaisesRegex(ValueError, "task manifests differ"):
            aggregate_repetition_summaries(
                [base, {**base, "task_manifest_sha256": "different"}],
                prompt_profile="baseline",
                model="qwen",
            )
        with self.assertRaisesRegex(ValueError, "model identities differ"):
            aggregate_repetition_summaries(
                [base, {**base, "model_identity": {"model": "other"}}],
                prompt_profile="baseline",
                model="qwen",
            )

    def test_partial_repetition_is_not_counted_as_successful_score(self):
        aggregate = aggregate_repetition_summaries(
            [
                {
                    "status": "available",
                    "model": "qwen",
                    "sample_count": 1,
                    "question_count": 2,
                    "mean_f1": 0.5,
                    "condition_fingerprint": "same-condition",
                    "task_manifest_sha256": "same-task-manifest",
                    "model_identity": {"model": "qwen", "revision": "same"},
                    "conversation_score_summaries": [
                        {
                            "conversation_id_sha256": "a" * 64,
                            "question_count": 2,
                            "score_sum": 1.0,
                            "mean_f1": 0.5,
                        }
                    ],
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
                    "model": "qwen",
                    "sample_count": 1,
                    "question_count": 2,
                    "mean_f1": 0.0,
                    "condition_fingerprint": "same-condition",
                    "task_manifest_sha256": "same-task-manifest",
                    "model_identity": {"model": "qwen", "revision": "same"},
                    "conversation_score_summaries": [
                        {
                            "conversation_id_sha256": "a" * 64,
                            "question_count": 2,
                            "score_sum": 0.0,
                            "mean_f1": 0.0,
                        }
                    ],
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
        self.assertEqual(
            aggregate["cluster_bootstrap_interval"]["estimate"], 0.5
        )
        self.assertEqual(
            aggregate["conversation_score_summaries"][0][
                "question_evaluation_count"
            ],
            2,
        )
        self.assertEqual(aggregate["qa_batch_failure_count_total"], 1)
        self.assertEqual(aggregate["qa_question_successful_response_count_total"], 2)
        self.assertFalse(aggregate["token_usage_complete"])


if __name__ == "__main__":
    unittest.main()
