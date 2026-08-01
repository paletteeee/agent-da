import subprocess
import sys
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


if __name__ == "__main__":
    unittest.main()
