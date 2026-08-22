import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_adapter_contract import MemoryAdapter, normalize_result  # noqa: E402
from txnmem_controlled_baselines import (  # noqa: E402
    AppendOnlyAdapter,
    LastWriteWinsAdapter,
    MetadataFilteredAdapter,
)
from txnmem_invariants import check_invariants  # noqa: E402
from txnmem_metrics import result_row  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


ADAPTERS = (AppendOnlyAdapter, LastWriteWinsAdapter, MetadataFilteredAdapter)


class ControlledBaselineTests(unittest.TestCase):
    def test_adapters_implement_the_shared_memory_contract(self):
        for adapter_type in ADAPTERS:
            self.assertIsInstance(adapter_type(), MemoryAdapter)

    def test_writes_are_visible_before_commit_for_every_adapter(self):
        instance = generate_instance("atomic_multi_write", 101, {"txn_size": 2})
        instance["failure_schedule"] = []
        instance["operations"].insert(
            2,
            {
                "op_id": "op_visible",
                "step": 3,
                "agent_id": instance["operations"][0]["agent_id"],
                "txn_id": "txn_001",
                "type": "get_by_id",
                "memory_id": "m_write_1",
                "scope": "tenant:user_001",
            },
        )
        instance["operations"][3]["step"] = 4
        instance["operations"][4]["step"] = 5

        for adapter_type in ADAPTERS:
            observation = adapter_type().run(instance)

            self.assertEqual(observation.transaction_state, "committed")
            self.assertEqual(observation.metrics["exposed_memory_ids"], ["m_write_1"])
            self.assertEqual(observation.committed_memory_ids, ["m_write_1", "m_write_2"])

    def test_crash_after_first_write_leaves_a_partial_immediate_update(self):
        instance = generate_instance("atomic_multi_write", 102, {"txn_size": 2})

        for adapter_type in ADAPTERS:
            observation = adapter_type().run(instance)

            self.assertEqual(observation.transaction_state, "partial_commit")
            self.assertEqual(observation.committed_memory_ids, ["m_write_1"])
            self.assertIn("m_write_1", observation.final_memories)
            self.assertEqual(observation.trace[-1], {"step": 2, "event": "crash"})

    def test_only_metadata_filtered_blocks_search_and_direct_id_scope_bypass(self):
        instance = generate_instance("scope_bypass", 103)

        for adapter_type in (AppendOnlyAdapter, LastWriteWinsAdapter):
            observation = adapter_type().run(instance)
            self.assertEqual(observation.metrics["exposed_memory_ids"], ["m_private"])
            self.assertEqual(observation.metrics["denied_reads"], 0)

        observation = MetadataFilteredAdapter().run(instance)
        self.assertEqual(observation.metrics["exposed_memory_ids"], [])
        self.assertEqual(observation.metrics["denied_reads"], 2)
        self.assertEqual(
            [event["event"] for event in observation.trace if event.get("event") == "denied_read"],
            ["denied_read", "denied_read"],
        )

    def test_metadata_filtered_blocks_a_different_agent_in_the_same_scope(self):
        instance = generate_instance("scope_bypass", 107)
        private_scope = instance["initial_memories"][0]["scope"]
        for operation in instance["operations"]:
            operation["agent_id"] = "agent_other"
            operation["scope"] = private_scope

        observation = MetadataFilteredAdapter().run(instance)

        self.assertEqual(observation.metrics["exposed_memory_ids"], [])
        self.assertEqual(observation.metrics["denied_reads"], 2)
        self.assertEqual(
            [event["memory_id"] for event in observation.trace if event.get("event") == "denied_read"],
            ["m_private", "m_private"],
        )

    def test_last_write_wins_searches_the_latest_logical_record(self):
        instance = generate_instance("atomic_multi_write", 104, {"txn_size": 2})
        instance["failure_schedule"] = []
        instance["operations"].append(
            {
                "op_id": "op_search_latest",
                "step": 5,
                "agent_id": instance["operations"][0]["agent_id"],
                "type": "search",
                "scope": "tenant:user_001",
            }
        )

        observation = LastWriteWinsAdapter().run(instance)

        self.assertEqual(observation.metrics["exposed_memory_ids"], ["m_write_2"])
        self.assertEqual(set(observation.final_memories), {"m_write_1", "m_write_2"})

    def test_only_last_write_wins_repairs_the_w5_supersession_pointer(self):
        instance = generate_instance("supersession_consistency", 108)

        observations = {
            adapter_type: adapter_type().run(instance)
            for adapter_type in ADAPTERS
        }

        self.assertEqual(
            check_invariants(
                instance,
                normalize_result(observations[LastWriteWinsAdapter], "LastWriteWinsAdapter"),
            ),
            [],
        )
        for adapter_type in (AppendOnlyAdapter, MetadataFilteredAdapter):
            observation = observations[adapter_type]
            violations = check_invariants(
                instance,
                normalize_result(observation, adapter_type.__name__),
            )
            self.assertIn("supersession_consistency_violation", violations)
            self.assertEqual(observation.final_memories["m_old"]["status"], "active")
            self.assertEqual(observation.metrics["supersession_updates"], 0)

    def test_append_only_preserves_the_first_object_on_a_memory_id_collision(self):
        instance = generate_instance("atomic_multi_write", 109, {"txn_size": 2})
        instance["failure_schedule"] = []
        instance["operations"][1]["value"] = "first_value"
        instance["operations"][2]["memory_id"] = "m_write_1"
        instance["operations"][2]["value"] = "replacement_value"

        observation = AppendOnlyAdapter().run(instance)

        self.assertEqual(observation.final_memories["m_write_1"]["value"], "first_value")
        self.assertEqual(observation.committed_memory_ids, ["m_write_1"])
        self.assertIn(
            {
                "step": 3,
                "event": "capability_absent",
                "capability": "duplicate_memory_id",
                "memory_id": "m_write_1",
            },
            observation.trace,
        )

    def test_adapters_do_not_revalidate_revocation_at_commit(self):
        instance = generate_instance("revoke_before_commit", 105)

        for adapter_type in ADAPTERS:
            observation = adapter_type().run(instance)
            row = result_row(instance, normalize_result(observation, adapter_type.__name__))

            self.assertEqual(observation.transaction_state, "committed")
            self.assertEqual(observation.final_memories["m_protected_write"]["status"], "active")
            self.assertEqual(row["invalid_commit_rate"], 1.0)
            self.assertEqual(row["stale_write_rate"], 1.0)

    def test_adapters_invalidate_only_the_named_source_without_provenance_repair(self):
        instance = generate_instance(
            "provenance_branch_repair", 106, {"branch_factor": 2, "provenance_depth": 2}
        )

        for adapter_type in ADAPTERS:
            observation = adapter_type().run(instance)
            row = result_row(instance, normalize_result(observation, adapter_type.__name__))

            self.assertEqual(observation.final_memories["m_root"]["status"], "invalid")
            self.assertTrue(
                all(
                    memory["status"] == "active"
                    for memory_id, memory in observation.final_memories.items()
                    if memory_id != "m_root"
                )
            )
            self.assertEqual(observation.metrics["repair_count"], 0)
            self.assertEqual(row["repair_recall"], 0.0)


if __name__ == "__main__":
    unittest.main()
