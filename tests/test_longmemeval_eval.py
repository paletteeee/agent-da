import json
import hashlib
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from longmemeval_eval import (  # noqa: E402
    LONGMEMEVAL_OFFICIAL_EVALUATOR_SHA256,
    LONGMEMEVAL_OFFICIAL_REPO_COMMIT,
    LONGMEMEVAL_ORACLE_SHA256,
    aggregate_longmemeval,
    load_longmemeval_s,
    preflight_longmemeval_oracle,
    run_longmemeval_item,
    write_official_hypotheses,
)
from txnmem_model_protocol import ModelResponse, TokenUsage  # noqa: E402


def _item(
    question_id="question-1",
    *,
    content="Alice studies biology.",
    answer_session_ids=None,
):
    session_id = f"session-{question_id}"
    if answer_session_ids is None:
        answer_session_ids = [session_id]
    return {
        "question_id": question_id,
        "question_type": "single-session-user",
        "question": "What does Alice study?",
        "answer": "biology",
        "question_date": "2024/01/02 (Tue) 10:00",
        "answer_session_ids": answer_session_ids,
        "haystack_session_ids": [session_id],
        "haystack_dates": ["2024/01/01 (Mon) 10:00"],
        "haystack_sessions": [
            [
                {"role": "user", "content": content, "has_answer": True},
                {"role": "assistant", "content": "Noted."},
            ]
        ],
    }


class _LeakyBackend:
    """A deliberately filter-blind backend used to test runner isolation."""

    def __init__(self):
        self.records = []

    def write(self, memory_id, value=None, **fields):
        record = {
            "memory_id": str(memory_id),
            "value": value,
            "status": "active",
            "agent_id": fields.get("agent_id"),
        }
        self.records.append(record)
        return dict(record)

    def search(self, query=None, **_fields):
        return [dict(record) for record in self.records]


class _RecordingModel:
    def __init__(self, answer="biology"):
        self.answer = answer
        self.requests = []

    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        self.requests.append(
            {
                "messages": [dict(message) for message in messages],
                "tools": list(tools),
                "seed": seed,
                "temperature": temperature,
            }
        )
        return ModelResponse(
            text=self.answer,
            tool_calls=[],
            usage=TokenUsage(20, 2, 22),
        )


class LongMemEvalSchemaTests(unittest.TestCase):
    def _load(self, rows, *, formal=False):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "longmemeval.json"
            path.write_text(json.dumps(rows), encoding="utf-8")
            return load_longmemeval_s(path, formal=formal)

    def test_loads_valid_two_item_offline_fixture(self):
        loaded = self._load([_item(), _item("question-2")])

        self.assertEqual([item.question_id for item in loaded], ["question-1", "question-2"])
        self.assertEqual(loaded[0].answer_session_ids, ("session-question-1",))

    def test_rejects_malformed_turn_role(self):
        row = _item()
        row["haystack_sessions"][0][0]["role"] = "system"

        with self.assertRaisesRegex(ValueError, "role"):
            self._load([row])

    def test_rejects_invalid_or_mislabeled_date(self):
        for invalid in ("2024-01-01", "2024/01/01 (Tue) 10:00"):
            with self.subTest(invalid=invalid):
                row = _item()
                row["haystack_dates"] = [invalid]
                with self.assertRaisesRegex(ValueError, "date"):
                    self._load([row])

    def test_preserves_release_duplicate_session_ids_as_positional_events(self):
        row = _item()
        row["haystack_session_ids"].append(row["haystack_session_ids"][0])
        row["haystack_dates"].append("2024/01/01 (Mon) 11:00")
        row["haystack_sessions"].append(
            [{"role": "user", "content": "The same released session was replayed."}]
        )

        loaded = self._load([row])[0]

        self.assertEqual(len(loaded.haystack_session_ids), 2)
        self.assertEqual(loaded.haystack_session_ids[0], loaded.haystack_session_ids[1])

    def test_normalizes_released_numeric_answers_without_using_them_for_retrieval(self):
        row = _item()
        row["answer"] = 3

        loaded = self._load([row])[0]

        self.assertEqual(loaded.answer, "3")

    def test_preserves_released_empty_turn_content(self):
        row = _item()
        row["haystack_sessions"][0][0]["content"] = ""

        loaded = self._load([row])[0]

        self.assertEqual(loaded.haystack_sessions[0][0].content, "")

    def test_preserves_release_order_for_same_day_time_inversions(self):
        row = _item()
        row["haystack_session_ids"].append("session-later-in-array")
        row["haystack_dates"] = [
            "2024/01/01 (Mon) 11:00",
            "2024/01/01 (Mon) 10:00",
        ]
        row["haystack_sessions"].append(
            [{"role": "assistant", "content": "Second source-position event."}]
        )

        loaded = self._load([row])[0]

        self.assertEqual(
            loaded.haystack_session_ids,
            ("session-question-1", "session-later-in-array"),
        )

    def test_rejects_calendar_day_inversion(self):
        row = _item()
        row["haystack_session_ids"].append("session-prior-day")
        row["haystack_dates"] = [
            "2024/01/02 (Tue) 11:00",
            "2024/01/01 (Mon) 10:00",
        ]
        row["haystack_sessions"].append(
            [{"role": "assistant", "content": "Prior day in the wrong position."}]
        )

        with self.assertRaisesRegex(ValueError, "chronological"):
            self._load([row])

    def test_formal_mode_requires_exact_500_unique_ids_and_30_abstentions(self):
        with self.assertRaisesRegex(ValueError, "500"):
            self._load([_item()], formal=True)

        duplicate_rows = [_item(f"question-{index}") for index in range(499)]
        duplicate_rows.append(_item("question-1"))
        with self.assertRaisesRegex(ValueError, "unique"):
            self._load(duplicate_rows, formal=True)

        no_abstentions = [_item(f"question-{index}") for index in range(500)]
        with self.assertRaisesRegex(ValueError, "30 abstention"):
            self._load(no_abstentions, formal=True)

    def test_oracle_preflight_requires_exact_matching_question_set(self):
        rows = [
            {
                "question_id": "q1",
                "question_type": "single-session-user",
                "question": "Question one?",
                "answer": "answer one",
            },
            {
                "question_id": "q2",
                "question_type": "multi-session",
                "question": "Question two?",
                "answer": 2,
            },
        ]
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "oracle.json"
            path.write_text(json.dumps(rows), encoding="utf-8")
            report = preflight_longmemeval_oracle(
                path,
                expected_question_ids=["q1", "q2"],
                formal=False,
                require_pinned_source=False,
            )
            with self.assertRaisesRegex(ValueError, "question IDs"):
                preflight_longmemeval_oracle(
                    path,
                    expected_question_ids=["q1", "q3"],
                    formal=False,
                    require_pinned_source=False,
                )

        self.assertEqual(report["question_count"], 2)
        self.assertEqual(report["question_type_counts"]["multi-session"], 1)


class LongMemEvalRunnerTests(unittest.TestCase):
    def _loaded_item(self, row):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "item.json"
            path.write_text(json.dumps([row]), encoding="utf-8")
            return load_longmemeval_s(path, formal=False)[0]

    def test_question_namespaces_prevent_cross_item_memory_leakage(self):
        first = self._loaded_item(
            _item("question-a", content="PRIVATE-FIRST-MEMORY: Alice studies chemistry.")
        )
        second = self._loaded_item(
            _item("question-b", content="SECOND-MEMORY: Alice studies biology.")
        )
        backend = _LeakyBackend()
        model = _RecordingModel()

        first_row = run_longmemeval_item(first, backend, model, top_k=5, seed=17)
        second_row = run_longmemeval_item(second, backend, model, top_k=5, seed=17)

        self.assertNotEqual(first_row["namespace_sha256"], second_row["namespace_sha256"])
        second_prompt = "\n".join(
            message["content"] for message in model.requests[-1]["messages"]
        )
        self.assertIn("SECOND-MEMORY", second_prompt)
        self.assertNotIn("PRIVATE-FIRST-MEMORY", second_prompt)
        self.assertEqual(second_row["retrieved_session_ids"], ["session-question-b"])

    def test_run_reports_evidence_session_recall_and_endpoint_usage(self):
        row = _item()
        row["haystack_session_ids"].append("distractor")
        row["haystack_dates"].append("2024/01/01 (Mon) 11:00")
        row["haystack_sessions"].append(
            [{"role": "user", "content": "Alice likes hiking."}]
        )
        item = self._loaded_item(row)

        result = run_longmemeval_item(
            item,
            _LeakyBackend(),
            _RecordingModel(),
            top_k=1,
            seed=17,
        )

        self.assertEqual(result["hypothesis"], "biology")
        self.assertEqual(result["evidence_session_count"], 1)
        self.assertEqual(result["retrieved_evidence_session_count"], 1)
        self.assertEqual(result["evidence_session_recall"], 1.0)
        self.assertEqual(result["model_usage"]["total_tokens"], 22)

    def test_model_context_comes_from_backend_round_trip_not_source_side_channel(self):
        item = self._loaded_item(_item(content="SOURCE-ONLY-CONTENT"))

        class RoundTripBackend(_LeakyBackend):
            def search(self, query=None, **fields):
                rows = super().search(query=query, **fields)
                rows[0]["value"] = "BACKEND-ROUNDTRIP-CONTENT"
                return rows

        model = _RecordingModel()
        run_longmemeval_item(item, RoundTripBackend(), model, top_k=1, seed=17)

        prompt = "\n".join(
            message["content"] for message in model.requests[0]["messages"]
        )
        self.assertIn("BACKEND-ROUNDTRIP-CONTENT", prompt)
        self.assertNotIn("SOURCE-ONLY-CONTENT", prompt)

    def test_aggregate_excludes_abstentions_from_retrieval_denominator(self):
        rows = [
            {
                "question_id": "a",
                "is_abstention": False,
                "evidence_session_count": 2,
                "retrieved_evidence_session_count": 1,
                "evidence_session_recall": 0.5,
                "model_usage": {"request_count": 1, "total_tokens": 10},
            },
            {
                "question_id": "b",
                "is_abstention": False,
                "evidence_session_count": 1,
                "retrieved_evidence_session_count": 1,
                "evidence_session_recall": 1.0,
                "model_usage": {"request_count": 1, "total_tokens": 11},
            },
            {
                "question_id": "c_abs",
                "is_abstention": True,
                "evidence_session_count": 0,
                "retrieved_evidence_session_count": 0,
                "evidence_session_recall": None,
                "model_usage": {"request_count": 1, "total_tokens": 12},
            },
        ]

        aggregate = aggregate_longmemeval(rows)

        self.assertEqual(aggregate["question_count"], 3)
        self.assertEqual(aggregate["abstention_question_count"], 1)
        self.assertEqual(aggregate["retrieval_question_count"], 2)
        self.assertEqual(aggregate["evidence_session_denominator"], 3)
        self.assertAlmostEqual(aggregate["evidence_session_recall_micro"], 2 / 3)
        self.assertEqual(aggregate["official_qa_status"], "blocked")
        self.assertNotIn("official_qa_score", aggregate)

    def test_unproven_local_score_cannot_activate_official_qa(self):
        rows = [
            {
                "question_id": "a",
                "is_abstention": False,
                "evidence_session_count": 1,
                "retrieved_evidence_session_count": 1,
                "evidence_session_recall": 1.0,
                "model_usage": {},
            }
        ]

        aggregate = aggregate_longmemeval(
            rows,
            official_evaluator_report={"status": "available", "score": 1.0},
        )

        self.assertEqual(aggregate["official_qa_status"], "blocked")
        self.assertNotIn("official_qa_metrics", aggregate)

    def test_caller_supplied_hashes_and_metrics_cannot_activate_official_qa(self):
        rows = [
            {
                "question_id": "a",
                "is_abstention": False,
                "evidence_session_count": 1,
                "retrieved_evidence_session_count": 1,
                "evidence_session_recall": 1.0,
                "model_usage": {},
            }
        ]
        aggregate = aggregate_longmemeval(
            rows,
            official_evaluator_report={
                "status": "available",
                "command_succeeded": True,
                "returncode": 0,
                "question_count": 1,
                "evaluator_commit": LONGMEMEVAL_OFFICIAL_REPO_COMMIT,
                "evaluator_sha256": LONGMEMEVAL_OFFICIAL_EVALUATOR_SHA256,
                "reference_sha256": LONGMEMEVAL_ORACLE_SHA256,
                "hypotheses_sha256": "a" * 64,
                "evaluation_log_sha256": "b" * 64,
                "metric_model": "gpt-4o-2024-08-06",
                "metrics": {
                    "overall_accuracy": 0.5,
                    "task_averaged_accuracy": 0.5,
                    "abstention_accuracy": 0.0,
                },
            },
        )

        self.assertEqual(aggregate["official_qa_status"], "blocked")
        self.assertNotIn("official_qa_metrics", aggregate)

    def test_artifact_backed_official_evaluator_recomputes_metrics(self):
        question_ids = [f"question-{index:03d}" for index in range(470)] + [
            f"question-{index:03d}_abs" for index in range(30)
        ]
        question_types = [
            "single-session-user",
            "single-session-preference",
            "single-session-assistant",
            "multi-session",
            "temporal-reasoning",
            "knowledge-update",
        ]
        rows = []
        hypotheses = []
        evaluation_log = []
        oracle = []
        labels = []
        for index, question_id in enumerate(question_ids):
            is_abstention = question_id.endswith("_abs")
            label = index % 3 != 0
            labels.append(label)
            rows.append(
                {
                    "question_id": question_id,
                    "is_abstention": is_abstention,
                    "evidence_session_count": 0 if is_abstention else 1,
                    "retrieved_evidence_session_count": 0 if is_abstention else 1,
                    "evidence_session_recall": None if is_abstention else 1.0,
                    "model_usage": {},
                }
            )
            hypothesis = {"question_id": question_id, "hypothesis": f"answer-{index}"}
            hypotheses.append(hypothesis)
            evaluation_log.append(
                {
                    **hypothesis,
                    "autoeval_label": {
                        "model": "gpt-4o-2024-08-06",
                        "label": label,
                    },
                }
            )
            oracle.append(
                {
                    "question_id": question_id,
                    "question_type": question_types[index % len(question_types)],
                    "question": f"question text {index}",
                    "answer": f"answer-{index}",
                }
            )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            hypotheses_path = root / "hypotheses.jsonl"
            log_path = root / "hypotheses.jsonl.eval-results-gpt-4o"
            oracle_path = root / "oracle.json"
            evaluator_path = root / "evaluate_qa.py"
            metrics_path = root / "print_qa_metrics.py"
            hypotheses_path.write_text(
                "".join(json.dumps(item) + "\n" for item in hypotheses),
                encoding="utf-8",
            )
            log_path.write_text(
                "".join(json.dumps(item) + "\n" for item in evaluation_log),
                encoding="utf-8",
            )
            oracle_path.write_text(json.dumps(oracle), encoding="utf-8")
            evaluator_path.write_text("# pinned evaluator fixture\n", encoding="utf-8")
            metrics_path.write_text("# pinned metrics fixture\n", encoding="utf-8")

            def digest(path):
                return hashlib.sha256(path.read_bytes()).hexdigest()

            with (
                patch("longmemeval_eval.LONGMEMEVAL_ORACLE_SHA256", digest(oracle_path)),
                patch("longmemeval_eval.LONGMEMEVAL_ORACLE_SIZE_BYTES", oracle_path.stat().st_size),
                patch("longmemeval_eval.LONGMEMEVAL_OFFICIAL_EVALUATOR_SHA256", digest(evaluator_path)),
                patch("longmemeval_eval.LONGMEMEVAL_OFFICIAL_METRICS_SHA256", digest(metrics_path)),
            ):
                aggregate = aggregate_longmemeval(
                    rows,
                    official_evaluator_report={
                        "status": "available",
                        "command_succeeded": True,
                        "returncode": 0,
                        "evaluator_commit": LONGMEMEVAL_OFFICIAL_REPO_COMMIT,
                        "metric_model": "gpt-4o-2024-08-06",
                        "hypotheses_path": str(hypotheses_path),
                        "evaluation_log_path": str(log_path),
                        "reference_path": str(oracle_path),
                        "evaluator_path": str(evaluator_path),
                        "metrics_script_path": str(metrics_path),
                    },
                )

        self.assertEqual(aggregate["official_qa_status"], "available")
        self.assertAlmostEqual(
            aggregate["official_qa_metrics"]["overall_accuracy"],
            sum(labels) / len(labels),
        )
        abstention_labels = labels[-30:]
        self.assertAlmostEqual(
            aggregate["official_qa_metrics"]["abstention_accuracy"],
            sum(abstention_labels) / len(abstention_labels),
        )
        self.assertEqual(aggregate["official_qa_evaluated_question_count"], 500)
        self.assertEqual(aggregate["official_qa_abstention_count"], 30)

    def test_official_output_has_only_required_keys_and_is_immutable(self):
        question_ids = [f"q-{index:03d}" for index in range(470)] + [
            f"q-{index:03d}_abs" for index in range(30)
        ]
        rows = [
            {
                "question_id": question_id,
                "hypothesis": f"answer {index}",
                "retrieved_session_ids": ["private-session"],
            }
            for index, question_id in enumerate(question_ids)
        ]
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "hypotheses.jsonl"
            write_official_hypotheses(
                rows,
                path,
                expected_question_ids=question_ids,
            )
            payloads = [json.loads(line) for line in path.read_text().splitlines()]
            with self.assertRaises(FileExistsError):
                write_official_hypotheses(
                    rows,
                    path,
                    expected_question_ids=question_ids,
                )

        self.assertEqual(len(payloads), 500)
        self.assertEqual(set(payloads[0]), {"question_id", "hypothesis"})
        self.assertNotIn("private-session", json.dumps(payloads))

    def test_official_output_rejects_partial_or_wrong_question_set(self):
        formal_ids = [f"q-{index:03d}" for index in range(470)] + [
            f"q-{index:03d}_abs" for index in range(30)
        ]
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "hypotheses.jsonl"
            with self.assertRaisesRegex(ValueError, "exact formal question ID set"):
                write_official_hypotheses(
                    [{"question_id": formal_ids[0], "hypothesis": "partial"}],
                    path,
                    expected_question_ids=formal_ids,
                )
            self.assertFalse(path.exists())


class LongMemEvalSetupTests(unittest.TestCase):
    def test_setup_pins_dataset_oracle_and_official_evaluator_source(self):
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "setup_longmemeval.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("98d7416c24c778c2fee6e6f3006e7a073259d48f", script)
        self.assertIn(
            "821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c",
            script,
        )
        self.assertIn("9e0b455f4ef0e2ab8f2e582289761153549043fc", script)
        self.assertIn(
            "ecce9c4c79dc89d99534ac17b383a5cbb5b9f0c69ee98adaf0684742e3d95251",
            script,
        )
        self.assertNotIn("resolve/main", script)


if __name__ == "__main__":
    unittest.main()
