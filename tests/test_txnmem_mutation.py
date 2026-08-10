import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_invariants import check_invariants  # noqa: E402
from txnmem_mutation import (  # noqa: E402
    MUTANTS,
    build_minimal_mutant_witnesses,
    run_mutation_campaign,
    validate_minimal_mutant_witnesses,
)
from txnmem_simulator import run_instance  # noqa: E402
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

    def test_every_major_mutant_has_a_replayable_prefix_minimal_witness(self):
        instances = generate_suite(
            [
                "atomic_multi_write",
                "revoke_before_commit",
                "provenance_chain_repair",
                "scope_bypass",
            ],
            [0],
        )

        report = build_minimal_mutant_witnesses(instances)

        self.assertEqual(set(report["witnesses"]), set(MUTANTS))
        self.assertEqual(report["witness_count"], 4)
        self.assertTrue(report["all_prefix_minimal"])
        self.assertTrue(validate_minimal_mutant_witnesses(report))
        for mutant, witness in report["witnesses"].items():
            spec = MUTANTS[mutant]
            minimal = witness["minimal_instance"]
            result = run_instance(minimal, spec["variant"])
            self.assertIn(spec["target_violation"], check_invariants(minimal, result))
            self.assertEqual(len(witness["source_instance_sha256"]), 64)
            self.assertLessEqual(
                witness["minimal_operation_count"], witness["source_operation_count"]
            )
            self.assertFalse(
                witness["minimality"]["predecessor_reproduces_target_violation"]
            )


if __name__ == "__main__":
    unittest.main()
