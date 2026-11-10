import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_trace import normalize_trace, trace_to_instance  # noqa: E402
from txnmem_schema import validate_instance  # noqa: E402


class TxnMemTraceTests(unittest.TestCase):
    def test_normalize_trace_records_real_memory_operations(self):
        raw = [
            {"event_id": "e1", "kind": "memory_read", "memory_id": "m0", "agent_id": "agent_1"},
            {"event_id": "e2", "kind": "memory_derive", "source_ids": ["m0"], "memory_id": "m1", "agent_id": "agent_1"},
            {"event_id": "e3", "kind": "memory_write", "memory_id": "m1", "agent_id": "agent_1"},
        ]

        operations = normalize_trace(raw)

        self.assertEqual([operation["type"] for operation in operations], ["read", "derive", "write"])
        self.assertEqual(operations[1]["source_ids"], ["m0"])

    def test_trace_to_instance_has_no_handwritten_provenance_edges(self):
        raw = [
            {"event_id": "e1", "kind": "memory_read", "memory_id": "m0", "agent_id": "agent_1"},
            {"event_id": "e2", "kind": "memory_derive", "source_ids": ["m0"], "memory_id": "m1", "agent_id": "agent_1"},
        ]

        instance = trace_to_instance(raw, "trace_0", seed=0)

        self.assertEqual(instance["provenance_edges"], [])
        self.assertNotIn("expected_outcome", instance)
        self.assertEqual(instance["operations"][1]["type"], "derive")
        validate_instance(instance)

    def test_trace_to_instance_preserves_policy_and_failure_events(self):
        raw = [
            {"event_id": "e1", "kind": "memory_write", "memory_id": "m1", "agent_id": "agent_1"},
            {"event_id": "e2", "kind": "policy_revoke", "target": "write", "agent_id": "agent_1"},
            {"event_id": "e3", "kind": "crash", "agent_id": "agent_1"},
        ]

        instance = trace_to_instance(raw, "trace_policy_0", seed=0)

        self.assertEqual([event["type"] for event in instance["failure_schedule"]], ["revoke", "crash"])
        self.assertEqual(instance["failure_schedule"][0]["trigger"], {"after_operation": "e1"})


if __name__ == "__main__":
    unittest.main()
