import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_schema import load_workload_config, validate_instance  # noqa: E402


def minimal_instance():
    return {
        "instance_id": "i1",
        "workload": "atomic_multi_write",
        "seed": 0,
        "config": {"txn_size": 1},
        "initial_memories": [{"memory_id": "m1", "status": "active"}],
        "operations": [{"op_id": "op1", "step": 1, "type": "begin_txn"}],
        "policies": [],
        "failure_schedule": [],
        "provenance_edges": [],
        "expected_outcome": {},
    }


class TxnMemSchemaTests(unittest.TestCase):
    def test_validate_instance_rejects_missing_top_level_key(self):
        with self.assertRaises(ValueError):
            validate_instance({"workload": "atomic_multi_write"})

    def test_validate_instance_rejects_unknown_provenance_reference(self):
        instance = minimal_instance()
        instance["provenance_edges"] = [
            {"source_id": "missing", "derived_id": "m1", "relation": "derived_from"}
        ]
        with self.assertRaises(ValueError):
            validate_instance(instance)

    def test_load_workload_config_returns_w1_to_w8_definitions(self):
        config = load_workload_config(ROOT / "configs" / "workload_families.yaml")
        self.assertEqual(
            set(config["workloads"]),
            {
                "atomic_multi_write",
                "crash_during_commit",
                "revoke_before_commit",
                "scope_bypass",
                "supersession_consistency",
                "provenance_chain_repair",
                "provenance_branch_repair",
                "mixed_stress",
            },
        )

    def test_validate_instance_accepts_multi_agent_metadata(self):
        instance = minimal_instance()
        instance["workload"] = "multi_agent_visibility"
        instance["evidence_tier"] = "seeded"
        instance["structure_signature"] = "a" * 64

        validate_instance(instance)

    def test_validate_instance_rejects_invalid_multi_agent_signature(self):
        instance = minimal_instance()
        instance["workload"] = "multi_agent_visibility"
        instance["evidence_tier"] = "seeded"
        instance["structure_signature"] = "not-a-sha256"

        with self.assertRaisesRegex(ValueError, "structure_signature"):
            validate_instance(instance)


if __name__ == "__main__":
    unittest.main()
