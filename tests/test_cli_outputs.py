import subprocess
import sys
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]


class TxnMemCliOutputTests(unittest.TestCase):
    def test_experiment_command_writes_all_artifacts(self):
        with TemporaryDirectory() as tmp:
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "experiment",
                    "--out-dir",
                    tmp,
                    "--seeds",
                    "1",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("wrote", completed.stdout)
            self.assertTrue((Path(tmp) / "data/generated_instances.jsonl").exists())
            self.assertTrue((Path(tmp) / "data/reference_oracles.jsonl").exists())
            self.assertTrue((Path(tmp) / "results/experiment_results.csv").exists())
            self.assertTrue((Path(tmp) / "results/summary.json").exists())
            self.assertTrue((Path(tmp) / "results/coverage.json").exists())
            self.assertTrue((Path(tmp) / "results/mutation_report.json").exists())
            self.assertTrue((Path(tmp) / "results/schedule_baseline.json").exists())
            self.assertTrue((Path(tmp) / "results/realism.json").exists())
            self.assertTrue(list((Path(tmp) / "results/figures").glob("*.svg")))

    def test_trace_replay_command_writes_trace_grounded_artifacts(self):
        with TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            events.write_text(
                '{"episode_id":"ep1","event_id":"e1","kind":"memory_write","memory_id":"m1"}\n'
                '{"episode_id":"ep1","event_id":"e2","kind":"memory_read","memory_id":"m1"}\n',
                encoding="utf-8",
            )
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "trace-replay",
                    "--events",
                    str(events),
                    "--adapter",
                    "normalized",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("adapted 2 records into 1 trace instances", completed.stdout)
            self.assertTrue((out_dir / "data/trace_grounded_instances.jsonl").exists())
            self.assertTrue((out_dir / "results/trace_replay.csv").exists())
            self.assertTrue((out_dir / "results/trace_realism.json").exists())
            realism = json.loads((out_dir / "results/trace_realism.json").read_text(encoding="utf-8"))
            self.assertIn("evidence", realism)
            self.assertFalse(realism["evidence"]["trace_ground_truth_native"])

    def test_process_smoke_command_writes_aggregate_only_report(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "process-smoke",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("process concurrency", completed.stdout)
            report = json.loads(
                (out_dir / "results/process_concurrency.json").read_text(encoding="utf-8")
            )
            self.assertTrue(report["completed"])
            self.assertEqual(report["event_count"], 3)
            serialized = json.dumps(report)
            for sensitive_key in ("value", "arguments", "data", "password", "token"):
                self.assertNotIn(sensitive_key, serialized.lower())

    def test_real_model_smoke_fixture_writes_native_trace_and_sanitized_summary(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "real-model-smoke",
                    "--manifest",
                    "configs/real_model_smoke.json",
                    "--offline-fixture",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("native model", completed.stdout)
            self.assertTrue((out_dir / "data/native_model_traces.jsonl").exists())
            summary_path = out_dir / "results/native_model_summary.json"
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertTrue(summary["trace_ground_truth_native"])
            self.assertTrue(summary["task_summaries"][0]["task_evaluator"]["success"])
            serialized = json.dumps(summary, ensure_ascii=False).lower()
            for sensitive_key in ("value", "content", "arguments", "messages", "events"):
                self.assertNotIn(sensitive_key, serialized)


if __name__ == "__main__":
    unittest.main()
