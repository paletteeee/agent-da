import sys
import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

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

    def test_load_workload_config_rejects_malformed_parameter_range(self):
        """A reversed range must not reach generation as a valid interval."""

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "invalid.json"
            path.write_text(
                json.dumps(
                    {
                        "workloads": {
                            workload: {} for workload in (
                                "atomic_multi_write",
                                "crash_during_commit",
                                "revoke_before_commit",
                                "scope_bypass",
                                "supersession_consistency",
                                "provenance_chain_repair",
                                "provenance_branch_repair",
                                "mixed_stress",
                            )
                        },
                        "parameter_ranges": {"txn_size": [4, 1]},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "txn_size"):
                load_workload_config(path)

    def test_controlled_scale_config_exposes_the_approved_intervals(self):
        config = load_workload_config(ROOT / "configs" / "controlled_scale_200.json")

        self.assertEqual(
            config["parameter_ranges"],
            {
                "txn_size": (1, 4),
                "provenance_depth": (1, 4),
                "branch_factor": (1, 3),
                "policy_churn": (0, 2),
                "concurrency": (1, 3),
            },
        )


if __name__ == "__main__":
    unittest.main()
