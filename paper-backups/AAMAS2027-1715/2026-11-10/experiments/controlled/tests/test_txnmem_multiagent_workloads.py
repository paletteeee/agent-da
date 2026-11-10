import unittest
from collections import Counter, defaultdict

from txnmem_multiagent_workloads import (
    MULTI_AGENT_WORKLOADS,
    exhaustive_multi_agent_instances,
    generate_multi_agent_instance,
    generate_multi_agent_suite,
    structural_signature,
)
from txnmem_schema import validate_instance


class MultiAgentWorkloadTests(unittest.TestCase):
    def test_suite_has_fifty_unique_two_agent_instances_per_family(self):
        suite = generate_multi_agent_suite(range(50))
        self.assertEqual(len(MULTI_AGENT_WORKLOADS), 6)
        self.assertEqual(len(suite), 300)
        counts = Counter(item["workload"] for item in suite)
        self.assertEqual(set(counts.values()), {50})
        signatures = defaultdict(set)
        for item in suite:
            validate_instance(item)
            agents = {op["agent_id"] for op in item["operations"]}
            self.assertEqual(agents, {"agent_1", "agent_2"})
            transaction_ids = {
                op.get("txn_id") for op in item["operations"] if op.get("txn_id")
            }
            self.assertGreaterEqual(len(transaction_ids), 2)
            self.assertEqual(item["structure_signature"], structural_signature(item))
            signatures[item["workload"]].add(item["structure_signature"])
        self.assertTrue(all(len(values) == 50 for values in signatures.values()))

    def test_same_seed_is_byte_deterministic(self):
        for workload in MULTI_AGENT_WORKLOADS:
            self.assertEqual(
                generate_multi_agent_instance(workload, 17),
                generate_multi_agent_instance(workload, 17),
            )

    def test_exhaustive_instances_preserve_each_agent_program_order(self):
        instances = exhaustive_multi_agent_instances()
        self.assertGreater(len(instances), len(MULTI_AGENT_WORKLOADS))
        for item in instances:
            self.assertEqual(item["evidence_tier"], "exhaustive")
            validate_instance(item)
            for agent_id in ("agent_1", "agent_2"):
                ranks = [
                    op["program_index"]
                    for op in item["operations"]
                    if op["agent_id"] == agent_id
                ]
                self.assertEqual(ranks, sorted(ranks))


if __name__ == "__main__":
    unittest.main()
