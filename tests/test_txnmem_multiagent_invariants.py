import copy
import unittest

from txnmem_multiagent_invariants import (
    MULTI_AGENT_VIOLATION_ORDER,
    check_multi_agent_invariants,
)
from txnmem_multiagent_workloads import (
    MULTI_AGENT_WORKLOADS,
    generate_multi_agent_instance,
)
from txnmem_simulator import run_instance


EXPECTED = (
    "dirty_read_violation",
    "atomicity_violation",
    "cross_txn_flush_violation",
    "actor_authorization_violation",
    "actor_scope_violation",
    "conflict_consistency_violation",
    "cross_agent_provenance_violation",
    "recovery_consistency_violation",
)


class MultiAgentInvariantTests(unittest.TestCase):
    def assert_only(self, expected, instance, result):
        self.assertEqual(check_multi_agent_invariants(instance, result), [expected])

    def test_violation_order_is_stable(self):
        self.assertEqual(MULTI_AGENT_VIOLATION_ORDER, EXPECTED)

    def test_dirty_read_violation(self):
        instance = generate_multi_agent_instance("multi_agent_visibility", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        before = next(
            row
            for row in mutated["read_observations"]
            if row["probe_role"] == "before_writer_commit"
        )
        before["visible_memory_ids"] = ["m_visibility_1"]
        self.assert_only("dirty_read_violation", instance, mutated)

    def test_atomicity_violation(self):
        instance = generate_multi_agent_instance("interleaved_atomicity", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        mutated["transaction_write_sets"]["txn_probe"] = ["m_probe_1", "m_probe_2"]
        mutated["transaction_states"]["txn_probe"] = "committed"
        mutated["committed_memory_ids"].append("m_probe_1")
        self.assert_only("atomicity_violation", instance, mutated)

    def test_cross_transaction_flush_violation(self):
        instance = generate_multi_agent_instance("interleaved_atomicity", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        mutated["transaction_write_sets"]["txn_probe"] = ["m_probe"]
        mutated["transaction_states"]["txn_probe"] = "aborted"
        mutated["committed_memory_ids"].append("m_probe")
        self.assert_only("cross_txn_flush_violation", instance, mutated)

    def test_actor_authorization_violation(self):
        instance = generate_multi_agent_instance("actor_policy_scope", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        revoked = instance["config"]["sampled_parameters"]["revoked_agent"]
        txn_id = f"txn_{revoked}"
        mutated["transaction_states"][txn_id] = "committed"
        mutated["committed_memory_ids"].extend(
            memory_id
            for memory_id in mutated["transaction_write_sets"][txn_id]
            if memory_id not in mutated["committed_memory_ids"]
        )
        self.assert_only("actor_authorization_violation", instance, mutated)

    def test_actor_scope_violation(self):
        instance = generate_multi_agent_instance("actor_policy_scope", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        probe = next(
            row
            for row in mutated["read_observations"]
            if row["probe_role"] == "cross_scope_probe"
        )
        operation = next(
            operation
            for operation in instance["operations"]
            if operation["op_id"] == probe["operation_id"]
        )
        probe["visible_memory_ids"] = [
            "m_private_2" if operation["agent_id"] == "agent_1" else "m_private_1"
        ]
        self.assert_only("actor_scope_violation", instance, mutated)

    def test_conflict_consistency_violation(self):
        instance = generate_multi_agent_instance("concurrent_supersession", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        active = next(
            memory
            for memory in mutated["final_memories"].values()
            if memory.get("supersedes_id") == "m_shared_old"
        )
        duplicate = copy.deepcopy(active)
        duplicate["memory_id"] = "m_duplicate_winner"
        mutated["final_memories"][duplicate["memory_id"]] = duplicate
        self.assert_only("conflict_consistency_violation", instance, mutated)

    def test_cross_agent_provenance_violation(self):
        instance = next(
            generate_multi_agent_instance("cross_agent_provenance", seed)
            for seed in range(50)
            if generate_multi_agent_instance("cross_agent_provenance", seed)["config"]
            ["sampled_parameters"]["invalidation_phase"]
            == "after_derived_commit"
        )
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        descendant = next(
            memory
            for memory in mutated["final_memories"].values()
            if memory.get("derived_from")
        )
        descendant["status"] = "active"
        self.assert_only("cross_agent_provenance_violation", instance, mutated)

    def test_recovery_consistency_violation(self):
        instance = generate_multi_agent_instance("interleaved_atomicity", 0)
        result = run_instance(instance, "TxnMem")
        mutated = copy.deepcopy(result)
        crash = next(
            event
            for event in instance["failure_schedule"]
            if event["type"] == "crash"
        )
        txn_id = crash["target"]
        mutated["transaction_states"][txn_id] = "committed"
        mutated["committed_memory_ids"].extend(
            memory_id
            for memory_id in mutated["transaction_write_sets"][txn_id]
            if memory_id not in mutated["committed_memory_ids"]
        )
        self.assert_only("recovery_consistency_violation", instance, mutated)

    def test_full_txnmem_has_no_violation_for_all_seed_zero_families(self):
        for workload in MULTI_AGENT_WORKLOADS:
            with self.subTest(workload=workload):
                instance = generate_multi_agent_instance(workload, 0)
                self.assertEqual(
                    check_multi_agent_invariants(
                        instance,
                        run_instance(instance, "TxnMem"),
                    ),
                    [],
                )


if __name__ == "__main__":
    unittest.main()
