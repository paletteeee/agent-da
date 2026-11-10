import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_mutation import MUTANTS, run_mutation_campaign  # noqa: E402
from txnmem_workloads import generate_suite  # noqa: E402


class TxnMemMutationTests(unittest.TestCase):
    def test_campaign_kills_targeted_mutants(self):
        instances = generate_suite(
            [
                "atomic_multi_write",
                "revoke_before_commit",
                "scope_bypass",
                "provenance_chain_repair",
            ],
            [0],
        )

        report = run_mutation_campaign(instances)

        self.assertEqual(set(report["mutants"]), set(MUTANTS))
        self.assertTrue(all(item["killed"] > 0 for item in report["mutants"].values()))
        self.assertGreater(report["kill_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
