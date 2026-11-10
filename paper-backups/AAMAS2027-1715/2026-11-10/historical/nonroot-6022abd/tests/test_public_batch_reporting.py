"""Task-level official-evaluator aggregation for public native runs."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_benchmark_bridge import BenchmarkEnvAdapter
from txnmem_model_protocol import ModelResponse, ToolCall
from txnmem_real_experiment import run_benchmark_batch
from txnmem_statistics import aggregate_official_results


class _BatchModel:
    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        if not any(message.get("role") == "tool" for message in messages):
            return ModelResponse(
                "",
                [ToolCall("write", "memory_write", {"memory_id": "m1", "value": "fixture"})],
            )
        return ModelResponse("done", [])


class _BatchAdapter(BenchmarkEnvAdapter):
    dataset = "fixture"

    def tool_schemas(self):
        return []

    def reset(self, task):
        return str(task["prompt"])

    def execute(self, name, arguments):
        raise AssertionError("fixture does not expose benchmark tools")

    def evaluate(self, run_report):
        return {"success": run_report["task_id"] == "task-a", "score": 1.0}


class PublicBatchReportingTests(unittest.TestCase):
    def test_official_aggregate_uses_tasks_not_event_rows(self):
        rows = [
            {"task_id": "a", "official": {"success": True}, "native_event_count": 100},
            {"task_id": "b", "official": {"success": False}, "native_event_count": 1},
        ]
        aggregate = aggregate_official_results(rows, "appworld")
        self.assertEqual(aggregate["successes"], 1)
        self.assertEqual(aggregate["trials"], 2)
        self.assertEqual(aggregate["event_count"], 101)
        self.assertEqual(aggregate["official_evaluator_status"], "available")

    def test_official_aggregate_preserves_blocked_status_without_fake_score(self):
        aggregate = aggregate_official_results(
            [{"task_id": "a", "official": {"status": "blocked", "error": "missing"}}],
            "locomo",
        )
        self.assertEqual(aggregate["official_evaluator_status"], "blocked")
        self.assertEqual(aggregate["trials"], 0)
        self.assertNotIn("score", aggregate)

    def test_batch_report_separates_official_result_and_oracle(self):
        manifest = {
            "manifest_version": 1,
            "dataset_name": "fixture",
            "tasks": [
                {"task_id": "task-a", "prompt": "a", "acceptance": {"expected_status": "completed"}},
                {"task_id": "task-b", "prompt": "b", "acceptance": {"expected_status": "completed"}},
            ],
        }
        with TemporaryDirectory() as tmp:
            report = run_benchmark_batch(
                manifest,
                _BatchModel(),
                Path(tmp),
                backend_factory=None,
                adapter_factory=_BatchAdapter,
            )
            self.assertEqual(report["task_count"], 2)
            self.assertEqual(report["official"]["trials"], 2)
            self.assertEqual(report["official"]["successes"], 1)
            self.assertEqual(report["official"]["official_evaluator_status"], "available")
            self.assertIn("variants", report)
            self.assertIn("TxnMem", report["variants"])
            self.assertTrue((Path(tmp) / "results" / "native_batch_summary.json").exists())
            raw = json.loads((Path(tmp) / "results" / "native_batch_summary.json").read_text())
            self.assertNotIn("events", raw)
            self.assertTrue(
                raw["raw_report_payloads_included_in_summary"] is False
            )

    def test_task_aware_adapter_factory_receives_manifest_metadata(self):
        manifest = {
            "manifest_version": 1,
            "dataset_name": "fixture",
            "tasks": [{"task_id": "task-a", "prompt": "a", "app_names": ["mail"]}],
        }
        seen = []

        def factory(task):
            seen.append(task["app_names"])
            return _BatchAdapter()

        with TemporaryDirectory() as tmp:
            run_benchmark_batch(manifest, _BatchModel(), Path(tmp), adapter_factory=factory)
        self.assertEqual(seen, [["mail"]])


if __name__ == "__main__":
    unittest.main()
