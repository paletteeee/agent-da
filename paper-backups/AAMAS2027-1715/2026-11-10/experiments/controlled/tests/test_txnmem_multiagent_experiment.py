import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from txnmem_multiagent_experiment import run_multi_agent_controlled


class MultiAgentExperimentTests(unittest.TestCase):
    def test_small_run_writes_separate_seeded_and_exhaustive_denominators(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = run_multi_agent_controlled(root, seeds=2, replications=2)
            with (root / "results" / "experiment_results.csv").open(
                encoding="utf-8"
            ) as handle:
                seeded = list(csv.DictReader(handle))
            with (root / "results" / "exhaustive_results.csv").open(
                encoding="utf-8"
            ) as handle:
                exhaustive = list(csv.DictReader(handle))

            self.assertEqual(len(seeded), 6 * 2 * 5)
            self.assertGreater(len(exhaustive), 0)
            self.assertEqual(report["seeded_observation_count"], 60)
            self.assertEqual(report["replication_count"], 2)
            self.assertTrue((root / "results" / "claim_status.json").exists())
            self.assertTrue((root / "results" / "minimal_witnesses.json").exists())
            self.assertFalse(
                {row["instance_id"] for row in seeded}
                & {row["instance_id"] for row in exhaustive}
            )


if __name__ == "__main__":
    unittest.main()
