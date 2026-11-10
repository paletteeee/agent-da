import unittest

from txnmem_multiagent_workloads import generate_multi_agent_instance
from txnmem_simulator import run_instance


class MultiAgentSimulatorTests(unittest.TestCase):
    def test_other_transaction_pending_write_is_not_visible(self):
        instance = generate_multi_agent_instance("multi_agent_visibility", 0)
        result = run_instance(instance, "TxnMem")
        probes = [
            row
            for row in result["read_observations"]
            if row["probe_role"] == "before_writer_commit"
        ]
        self.assertTrue(probes)
        self.assertTrue(all(not row["visible_memory_ids"] for row in probes))

    def test_commit_flushes_only_its_own_write_set(self):
        instance = generate_multi_agent_instance("interleaved_atomicity", 0)
        result = run_instance(instance, "TxnMem")
        for txn_id, write_ids in result["transaction_write_sets"].items():
            committed = set(write_ids) & set(result["committed_memory_ids"])
            state = result["transaction_states"][txn_id]
            self.assertEqual(
                committed,
                set(write_ids) if state == "committed" else set(),
            )

    def test_operation_actor_must_match_transaction_actor(self):
        instance = generate_multi_agent_instance("multi_agent_visibility", 1)
        transaction_operation = next(
            operation
            for operation in instance["operations"]
            if operation["type"] == "write"
        )
        transaction_operation["agent_id"] = (
            "agent_2"
            if transaction_operation["agent_id"] == "agent_1"
            else "agent_1"
        )
        with self.assertRaisesRegex(ValueError, "transaction actor mismatch"):
            run_instance(instance, "TxnMem")

    def test_revocation_targets_only_one_agent(self):
        instance = generate_multi_agent_instance("actor_policy_scope", 0)
        result = run_instance(instance, "TxnMem")
        revoked = instance["config"]["sampled_parameters"]["revoked_agent"]
        survivor = "agent_2" if revoked == "agent_1" else "agent_1"
        self.assertEqual(result["transaction_states"][f"txn_{revoked}"], "aborted")
        self.assertEqual(
            result["transaction_states"][f"txn_{survivor}"],
            "committed",
        )

    def test_second_conflicting_supersession_aborts(self):
        result = run_instance(
            generate_multi_agent_instance("concurrent_supersession", 0),
            "TxnMem",
        )
        states = list(result["transaction_states"].values())
        self.assertEqual(states.count("committed"), 1)
        self.assertEqual(states.count("aborted"), 1)
        self.assertEqual(result["metrics"]["conflict_abort_count"], 1)

    def test_cross_agent_descendants_are_invalidated(self):
        result = run_instance(
            generate_multi_agent_instance("cross_agent_provenance", 0),
            "TxnMem",
        )
        self.assertTrue(
            all(
                memory["status"] == "invalid"
                for memory in result["final_memories"].values()
                if memory.get("derived_from")
            )
        )


if __name__ == "__main__":
    unittest.main()
