import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_coverage import (  # noqa: E402
    coverage_report,
    schedule_effectiveness,
    find_minimal_counterexample,
    randomize_schedule,
)
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemCoverageTests(unittest.TestCase):
    def test_random_schedule_is_seeded_and_trigger_based(self):
        instance = generate_instance("atomic_multi_write", seed=0)

        first = randomize_schedule(instance, seed=7)
        second = randomize_schedule(instance, seed=7)

        self.assertEqual(first["failure_schedule"], second["failure_schedule"])
        self.assertTrue(all("trigger" in event for event in first["failure_schedule"]))

    def test_minimal_counterexample_shrinks_naive_w1(self):
        instance = generate_instance("atomic_multi_write", seed=1)

        counterexample = find_minimal_counterexample(instance, "Naive")

        self.assertIsNotNone(counterexample)
        self.assertLess(counterexample["operation_count"], len(instance["operations"]))
        self.assertIn("atomicity_violation", counterexample["violations"])
        self.assertIn("minimal_instance", counterexample)
        self.assertFalse(
            counterexample["minimality"]["predecessor_reproduces_failure"]
        )

    def test_coverage_report_contains_schedule_and_invariant_coverage(self):
        instances = [
            generate_instance("atomic_multi_write", 0),
            generate_instance("scope_bypass", 0),
        ]

        report = coverage_report(instances, "TxnMem")

        self.assertIn("schedule_coverage", report)
        self.assertIn("invariant_coverage", report)
        self.assertIn("minimal_counterexamples", report)
        self.assertGreater(report["schedule_coverage"]["event_count"], 0)

    def test_schedule_effectiveness_compares_causal_and_random_faults(self):
        instances = [generate_instance("atomic_multi_write", 0)]

        report = schedule_effectiveness(instances, "Naive", random_seeds=[1, 2])

        self.assertIn("causal_detection_rate", report)
        self.assertIn("random_detection_rate", report)
        self.assertEqual(report["random_runs"], 2)


if __name__ == "__main__":
    unittest.main()
