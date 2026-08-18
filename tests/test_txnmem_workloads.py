import sys
import unittest
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_schema import validate_instance  # noqa: E402
from txnmem_workloads import (  # noqa: E402
    WORKLOADS,
    generate_instance,
    generate_suite,
    sample_semantic_config,
    semantic_fingerprint,
)


PARAMETER_RANGES = {
    "txn_size": [1, 4],
    "provenance_depth": [1, 4],
    "branch_factor": [1, 3],
    "policy_churn": [0, 2],
    "concurrency": [1, 3],
}


class TxnMemWorkloadTests(unittest.TestCase):
    def test_supersession_workload_declares_supersede_policy(self):
        instance = generate_instance("supersession_consistency", 0)
        assert any(p["action"] == "supersede" and p["effect"] == "allow" for p in instance["policies"])

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

    def test_parameter_ranges_are_consumed_deterministically(self):
        """Dropping range sampling would collapse the 8x200 semantic population."""

        first = generate_suite(WORKLOADS, range(200), parameter_ranges=PARAMETER_RANGES)
        second = generate_suite(WORKLOADS, range(200), parameter_ranges=PARAMETER_RANGES)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 8 * 200)
        self.assertEqual(len({row["instance_id"] for row in first}), 8 * 200)
        self.assertGreater(len({row["semantic_fingerprint"] for row in first}), len(WORKLOADS))
        for name, (low, high) in PARAMETER_RANGES.items():
            sampled = {
                row["semantic_parameters"][name]
                for row in first
                if name in row["semantic_parameters"]
            }
            self.assertTrue(sampled <= set(range(low, high + 1)))
            self.assertEqual({low, high}, {low, high} & sampled)
        for workload in WORKLOADS:
            rows = [row for row in first if row["workload"] == workload]
            self.assertEqual({row["seed"] for row in rows}, set(range(200)))
            self.assertGreater(len({row["semantic_fingerprint"] for row in rows}), 1)

    def test_semantic_sampling_uses_inclusive_ranges_and_fingerprint_normalizes_labels(self):
        """A seed/agent/identifier relabel must not create a new semantic shape."""

        sampled = sample_semantic_config("atomic_multi_write", 7, {"txn_size": [3, 3]})
        self.assertEqual(sampled, {"txn_size": 3})
        instance = generate_suite(
            ["atomic_multi_write"], [7], parameter_ranges={"txn_size": [3, 3]}
        )[0]
        relabeled = json.loads(json.dumps(instance))
        relabeled["instance_id"] = "different_identifier"
        relabeled["seed"] = 999
        for policy in relabeled["policies"]:
            policy["agent_id"] = "another_agent"
        for operation in relabeled["operations"]:
            operation["agent_id"] = "another_agent"

        self.assertEqual(semantic_fingerprint(instance), semantic_fingerprint(relabeled))

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
