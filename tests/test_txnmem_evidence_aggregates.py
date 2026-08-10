from __future__ import annotations

import json
import math
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_evidence_aggregates import (  # noqa: E402
    aggregate_e2e_submission_evidence,
    aggregate_tau_submission_evidence,
)


MODEL_REVISION = "7" * 64
SOURCE_COMMIT = "a" * 40


class SubmissionEvidenceAggregateTests(unittest.TestCase):
    def _write(self, root: Path, name: str, payload: dict) -> Path:
        path = root / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _tau_source(self) -> dict:
        rows = [
            {
                "task_id": "tau-0000",
                "status": "completed",
                "native_event_count": 2,
                "official": {"status": "available", "reward": 1.0},
            },
            {
                "task_id": "tau-0001",
                "status": "failed",
                "failure_code": "max_steps_exceeded",
                "native_event_count": 3,
                "official": {"status": "available", "reward": 0.0},
            },
        ]
        return {
            "status": "available",
            "dataset": "tau-bench-airline-test",
            "task_count": 2,
            "unique_task_count": 2,
            "native_event_count": 5,
            "model_id": "qwen2.5-7b-instruct",
            "model_execution_mode": "remote_endpoint",
            "memory_backend": "sqlite",
            "primary_manifest_sha256": "b" * 64,
            "replaced_network_error_tasks": [],
            "retry_manifests": [],
            "official": {
                "official_evaluator_status": "available",
                "evaluator_available_task_count": 2,
                "task_count": 2,
                "trials": 2,
                "reward_sum": 1.0,
                "reward_mean": 0.5,
                "event_count": 5,
            },
            "task_summaries": rows,
            "production_latency_claim": False,
        }

    def _e2e_source(self) -> dict:
        return {
            "status": "available",
            "benchmark": "tau-bench-airline",
            "task_count": 2,
            "model": "qwen2.5-7b-instruct",
            "model_revision": MODEL_REVISION,
            "model_server_build": "vllm:0.8.5.post1",
            "backend_health": {
                "qdrant": {"available": True, "version": "1.11.5"},
                "neo4j": {"available": True, "version": "5.22.0"},
            },
            "manifest_sha256": "b" * 64,
            "rows": [
                {
                    "task_id": "tau-0000",
                    "elapsed_ms": 10.0,
                    "status": "completed",
                    "native_event_count": 2,
                    "official": {"status": "available", "reward": 1.0},
                },
                {
                    "task_id": "tau-0001",
                    "elapsed_ms": 30.0,
                    "status": "completed",
                    "native_event_count": 3,
                    "official": {"status": "available", "reward": 0.0},
                },
            ],
            "mean_ms": 20.0,
            "p50_ms": 20.0,
            "production_latency_claim": False,
        }

    def test_tau_aggregate_recomputes_all_totals_and_records_attestation(self):
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "tau.json", self._tau_source())
            result = aggregate_tau_submission_evidence(
                path,
                expected_task_count=2,
                model_revision=MODEL_REVISION,
                model_server_build="vllm:0.8.5.post1",
                source_commit=SOURCE_COMMIT,
                run_command="python txnmem_experiment.py benchmark-native-batch ...",
                runtime_attestation={"server_pid": 123, "server_continuity": True},
            )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["task_count"], 2)
        self.assertEqual(result["native_event_count"], 5)
        self.assertEqual(result["evaluator_available_task_count"], 2)
        self.assertEqual(result["reward_sum"], 1.0)
        self.assertEqual(result["reward_mean"], 0.5)
        self.assertEqual(result["max_steps_exceeded_count"], 1)
        self.assertEqual(result["model_revision"], MODEL_REVISION)
        self.assertEqual(result["source_commit"], SOURCE_COMMIT)
        self.assertEqual(len(result["source_artifact"]["sha256"]), 64)
        self.assertFalse(result["production_latency_claim"])

    def test_tau_aggregate_rejects_duplicate_or_incomplete_evaluator_tasks(self):
        source = self._tau_source()
        source["task_summaries"][1]["task_id"] = "tau-0000"
        source["task_summaries"][1].pop("official")
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "tau.json", source)
            with self.assertRaisesRegex(ValueError, "unique task IDs"):
                aggregate_tau_submission_evidence(
                    path,
                    expected_task_count=2,
                    model_revision=MODEL_REVISION,
                    model_server_build="vllm:0.8.5.post1",
                    source_commit=SOURCE_COMMIT,
                    run_command="command",
                    runtime_attestation={"server_continuity": True},
                )

    def test_e2e_aggregate_recomputes_latency_completion_and_service_health(self):
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "e2e.json", self._e2e_source())
            result = aggregate_e2e_submission_evidence(
                path,
                expected_task_count=2,
                source_commit=SOURCE_COMMIT,
                run_command="python scripts/run_e2e_real_backend.py",
            )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(result["native_event_count"], 5)
        self.assertEqual(result["mean_ms"], 20.0)
        self.assertEqual(result["p50_ms"], 20.0)
        self.assertEqual(result["backend_health"]["qdrant"]["version"], "1.11.5")
        self.assertEqual(result["model_revision"], MODEL_REVISION)
        self.assertFalse(result["production_latency_claim"])

    def test_e2e_aggregate_rejects_missing_health_or_nonfinite_latency(self):
        source = self._e2e_source()
        source["backend_health"]["neo4j"]["available"] = False
        source["rows"][0]["elapsed_ms"] = math.inf
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "e2e.json", source)
            with self.assertRaisesRegex(ValueError, "finite positive latency"):
                aggregate_e2e_submission_evidence(
                    path,
                    expected_task_count=2,
                    source_commit=SOURCE_COMMIT,
                    run_command="command",
                )


if __name__ == "__main__":
    unittest.main()
