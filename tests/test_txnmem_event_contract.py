import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_event_contract import EventContractError, validate_event, validate_events  # noqa: E402


class TxnMemEventContractTests(unittest.TestCase):
    def test_transaction_lifecycle_events_require_txn_id(self):
        base = {
            "event_id": "e1",
            "kind": "begin_txn",
            "agent_id": "agent_model",
            "step": 1,
        }
        with self.assertRaisesRegex(EventContractError, "txn_id"):
            validate_event(base)
        self.assertEqual(
            validate_event({**base, "txn_id": "txn_task_1"})["txn_id"],
            "txn_task_1",
        )
        for kind, step in (("commit", 2), ("abort", 3)):
            self.assertEqual(
                validate_event({**base, "event_id": f"e_{kind}", "kind": kind, "step": step, "txn_id": "txn_task_1"})["kind"],
                kind,
            )

    def test_direct_memory_events_remain_valid_while_transactional_ids_are_non_empty(self):
        direct = {
            "event_id": "e1",
            "kind": "memory_write",
            "agent_id": "agent_model",
            "step": 1,
            "memory_id": "m1",
        }
        self.assertNotIn("txn_id", validate_event(direct))
        with self.assertRaisesRegex(EventContractError, "txn_id"):
            validate_event({**direct, "txn_id": " "})
        self.assertEqual(validate_event({**direct, "txn_id": "txn_1"})["txn_id"], "txn_1")

    def test_memory_invalidate_is_a_memory_operation_not_external_invalidate(self):
        event = validate_event(
            {
                "event_id": "e1",
                "kind": "memory_invalidate",
                "agent_id": "agent_model",
                "step": 1,
                "memory_id": "m1",
                "txn_id": "txn_1",
            }
        )
        self.assertEqual(event["memory_id"], "m1")
        with self.assertRaisesRegex(EventContractError, "memory_id"):
            validate_event({key: value for key, value in event.items() if key != "memory_id"})

    def test_lifecycle_events_preserve_unique_ids_and_strict_steps(self):
        events = [
            {"event_id": "e1", "kind": "begin_txn", "agent_id": "agent_model", "step": 1, "txn_id": "txn_1"},
            {"event_id": "e2", "kind": "memory_write", "agent_id": "agent_model", "step": 2, "memory_id": "m1", "txn_id": "txn_1"},
            {"event_id": "e3", "kind": "commit", "agent_id": "agent_model", "step": 3, "txn_id": "txn_1"},
        ]
        self.assertEqual([event["kind"] for event in validate_events(events)], ["begin_txn", "memory_write", "commit"])
        with self.assertRaisesRegex(EventContractError, "duplicate"):
            validate_events([*events, {**events[-1], "step": 4}])
        with self.assertRaisesRegex(EventContractError, "increase"):
            validate_events([{**events[0]}, {**events[1], "step": 1, "event_id": "e4"}])


if __name__ == "__main__":
    unittest.main()
