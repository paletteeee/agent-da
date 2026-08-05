"""Fault schedule validation tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_service_faults import ToxiproxyFaultController, deterministic_fault_matrix


class ServiceFaultTests(unittest.TestCase):
    def test_fault_matrix_has_triggered_actions_and_stable_seed(self):
        first = deterministic_fault_matrix(seed=17)
        second = deterministic_fault_matrix(seed=17)
        self.assertEqual(first, second)
        self.assertEqual([item["name"] for item in first], ["normal", "delay", "timeout", "connection_drop", "retry_success"])
        self.assertTrue(all("trigger_operation" in item for item in first))

    def test_controller_fires_on_operation_ordinal_not_wall_clock(self):
        controller = ToxiproxyFaultController(
            {"name": "drop-on-second-write", "service": "qdrant", "trigger_operation": "write", "trigger_ordinal": 2, "action": "connection_drop", "seed": 17}
        )
        self.assertIsNone(controller.observe("qdrant", "write"))
        fired = controller.observe("qdrant", "write")
        self.assertEqual(fired["request_ordinal"], 2)
        self.assertEqual(fired["action"], "connection_drop")
        self.assertIsNone(controller.observe("qdrant", "write"))


if __name__ == "__main__":
    unittest.main()
