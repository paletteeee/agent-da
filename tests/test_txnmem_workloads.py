import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_schema import validate_instance  # noqa: E402
from txnmem_workloads import WORKLOADS, generate_instance, generate_suite  # noqa: E402


class TxnMemWorkloadTests(unittest.TestCase):
    def test_all_workloads_are_deterministic_and_schema_valid(self):
        self.assertEqual(len(WORKLOADS), 8)
        for workload in WORKLOADS:
            first = generate_instance(workload, seed=13)
            second = generate_instance(workload, seed=13)
            self.assertEqual(first, second)
            validate_instance(first)

    def test_scope_bypass_contains_search_and_direct_id_read(self):
        instance = generate_instance("scope_bypass", seed=10)
        types = [operation["type"] for operation in instance["operations"]]
        self.assertIn("search", types)
        self.assertIn("get_by_id", types)

    def test_branch_repair_has_all_branch_derive_operations(self):
        instance = generate_instance(
            "provenance_branch_repair",
            seed=11,
            config={"branch_factor": 3, "provenance_depth": 2},
        )
        derive_operations = [
            operation for operation in instance["operations"] if operation["type"] == "derive"
        ]
        self.assertEqual(len(derive_operations), 6)
        self.assertEqual(instance["provenance_edges"], [])

    def test_generate_suite_returns_workload_seed_cartesian_product(self):
        suite = generate_suite(["atomic_multi_write", "scope_bypass"], [1, 2])
        self.assertEqual([item["seed"] for item in suite], [1, 2, 1, 2])

    def test_provenance_chain_records_real_derive_operations(self):
        instance = generate_instance("provenance_chain_repair", seed=14, config={"provenance_depth": 2})
        types = [operation["type"] for operation in instance["operations"]]

        self.assertIn("read", types)
        self.assertIn("derive", types)
        self.assertIn("commit", types)
        self.assertEqual(instance["provenance_edges"], [])

    def test_generator_does_not_emit_expected_outcome_ground_truth(self):
        instance = generate_instance("atomic_multi_write", seed=15)

        self.assertNotIn("expected_outcome", instance)


if __name__ == "__main__":
    unittest.main()
