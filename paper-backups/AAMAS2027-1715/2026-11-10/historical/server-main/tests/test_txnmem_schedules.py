import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_schedules import events_for_operation, schedule_coverage  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemScheduleTests(unittest.TestCase):
    def test_atomic_write_crash_is_triggered_after_first_write(self):
        instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})

        event = instance["failure_schedule"][0]

        self.assertEqual(event["trigger"], {"after_operation": "op_002"})
        self.assertEqual(event["phase"], "after_operation")
        self.assertEqual(events_for_operation(instance, instance["operations"][1], "after"), [event])

    def test_revoke_is_triggered_before_commit(self):
        instance = generate_instance("revoke_before_commit", seed=1)
        event = instance["failure_schedule"][0]

        self.assertEqual(event["trigger"], {"before_operation": "op_003"})
        self.assertEqual(event["phase"], "before_validate")

    def test_schedule_coverage_reports_trigger_and_action(self):
        coverage = schedule_coverage(generate_instance("mixed_stress", seed=2))

        self.assertGreaterEqual(coverage["event_count"], 2)
        self.assertIn("crash", coverage["actions"])
        self.assertIn("revoke", coverage["actions"])
        self.assertTrue(coverage["trigger_kinds"])


if __name__ == "__main__":
    unittest.main()
