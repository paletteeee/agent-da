from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from txnmem_model_protocol import ModelResponse, ToolCall
from txnmem_real_experiment import (
    load_task_manifest,
    run_experiment_manifest,
    sanitize_run_report,
)
from txnmem_task_transaction import InMemoryTransactionBackend


class _ScriptedModel:
    def __init__(self, responses):
        self.responses = list(responses)

    def complete(self, _messages, _tools, *, seed=None, temperature=0.0):
        return self.responses.pop(0)


class RealExperimentTransactionTests(unittest.TestCase):
    def test_manifest_normalization_preserves_transaction_opt_in(self):
        normalized, _digest = load_task_manifest(
            {
                "manifest_version": 1,
                "transaction_mode": "task",
                "tasks": [{"task_id": "case_normalized", "prompt": "write"}],
            }
        )

        self.assertEqual(normalized["transaction_mode"], "task")

    def test_task_manifest_uses_unique_per_case_journals_and_keeps_sanitized_summary(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("a1", "memory_write", {"memory_id": "a", "value": "secret-a"})]),
                ModelResponse("done a", []),
                ModelResponse("", [ToolCall("b1", "memory_write", {"memory_id": "b", "value": "secret-b"})]),
                ModelResponse("done b", []),
            ]
        )
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            result = run_experiment_manifest(
                {
                    "transaction_mode": "task",
                    "backend_factory": InMemoryTransactionBackend,
                    "tasks": [
                        {"task_id": "case_a", "prompt": "write a"},
                        {"task_id": "case_b", "prompt": "write b"},
                    ],
                },
                model,
                out_dir,
            )
            journals = sorted(path.name for path in (out_dir / "journals").glob("*.sqlite3"))
            summary_text = (out_dir / "results" / "native_model_summary.json").read_text(
                encoding="utf-8"
            )

        self.assertEqual(journals, ["case_a.sqlite3", "case_b.sqlite3"])
        self.assertEqual(
            [item["transaction"]["decision"] for item in result["task_summaries"]],
            ["committed", "committed"],
        )
        self.assertIn("state_digest", summary_text)
        self.assertNotIn("secret-a", summary_text)
        self.assertNotIn("secret-b", summary_text)

    def test_old_manifest_keeps_direct_output_shape_and_creates_no_journals(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            result = run_experiment_manifest(
                {"tasks": [{"task_id": "legacy", "prompt": "finish"}]},
                _ScriptedModel([ModelResponse("done", [])]),
                out_dir,
            )

            self.assertFalse((out_dir / "journals").exists())
        self.assertNotIn("transaction", result["task_summaries"][0])
        self.assertEqual(
            sorted(result["task_summaries"][0]),
            ["failure_code", "model_usage", "status", "steps", "task_evaluator", "task_id"],
        )

    def test_sanitizer_whitelists_transaction_summary_and_removes_endpoints(self):
        report = sanitize_run_report(
            {
                "transaction": {
                    "txn_id": "txn_safe",
                    "state": "committed",
                    "decision": "committed",
                    "phases": ["prepare_recorded", "commit_decided"],
                    "intent_count": 1,
                    "read_set_count": 0,
                    "state_digest": "a" * 64,
                    "raw_values": ["SECRET_RAW_VALUE"],
                    "prompt_secret": "SECRET_PROMPT",
                    "backend_endpoint": "http://private-backend.internal",
                },
                "endpoint": "http://private-model.internal",
            }
        )

        self.assertEqual(
            sorted(report["transaction"]),
            [
                "decision",
                "intent_count",
                "phases",
                "read_set_count",
                "state",
                "state_digest",
                "txn_id",
            ],
        )
        serialized = json.dumps(report)
        self.assertNotIn("SECRET", serialized)
        self.assertNotIn("internal", serialized)


if __name__ == "__main__":
    unittest.main()
