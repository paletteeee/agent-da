"""Native Mem0 adapter behavior, using a dependency-free SDK double."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_adapter_contract import (  # noqa: E402
    MemoryAdapter,
    RuntimeAdapterError,
)
from txnmem_mem0_adapter import (  # noqa: E402
    Mem0Adapter,
    close_mem0_memory,
    deterministic_mem0_factory,
    mem0_capabilities,
)
from txnmem_workloads import generate_instance  # noqa: E402


HAS_MEM0 = importlib.util.find_spec("mem0") is not None


class _FakeMem0:
    """Small pinned-Mem0-shaped double; it never calls an LLM or network."""

    def __init__(self) -> None:
        self.records: dict[str, dict] = {}
        self.add_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.get_all_calls: list[dict] = []
        self.update_calls: list[dict] = []
        self.delete_calls: list[str] = []
        self.llm_calls = 0
        self._counter = 0

    def add(self, messages, *, user_id, agent_id, metadata, infer=True):
        if infer:
            self.llm_calls += 1
        self._counter += 1
        sdk_id = f"sdk-{self._counter}"
        stored_metadata = {key: value for key, value in metadata.items() if key != "agent_id"}
        record = {
            "id": sdk_id,
            "memory": messages,
            "user_id": user_id,
            "agent_id": agent_id,
            "metadata": stored_metadata,
        }
        self.records[sdk_id] = record
        self.add_calls.append({"messages": messages, "user_id": user_id, "agent_id": agent_id, "metadata": dict(metadata), "infer": infer})
        return {"results": [{"id": sdk_id, "memory": messages, "event": "ADD", "actor_id": user_id, "role": "user"}]}

    def get(self, memory_id):
        record = self.records.get(memory_id)
        return None if record is None else dict(record)

    def get_all(self, *, filters=None, **kwargs):
        self.get_all_calls.append({"filters": filters, **kwargs})
        user_id = (filters or {}).get("user_id")
        return {"results": [dict(record) for record in self.records.values() if record["user_id"] == user_id]}

    def search(self, query, *, filters=None, **kwargs):
        self.search_calls.append({"query": query, "filters": filters, **kwargs})
        user_id = (filters or {}).get("user_id")
        return {
            "results": [
                dict(record)
                for record in self.records.values()
                if record["user_id"] == user_id and (query is None or query in record["memory"])
            ]
        }

    def update(self, memory_id, text=None, metadata=None, **kwargs):
        self.update_calls.append({"memory_id": memory_id, "text": text, "metadata": dict(metadata or {}), **kwargs})
        record = self.records[memory_id]
        if text is not None:
            record["memory"] = text
        if metadata is not None:
            record["metadata"] = dict(metadata)
        return {"message": "Memory updated successfully"}

    def delete(self, memory_id):
        self.delete_calls.append(memory_id)
        self.records.pop(memory_id, None)
        return {"message": "Memory deleted successfully"}


class _BadAddMem0(_FakeMem0):
    def add(self, *args, **kwargs):
        super().add(*args, **kwargs)
        return {"unexpected": []}


class _BadGetMem0(_FakeMem0):
    def get(self, memory_id):
        return {"id": memory_id, "memory": "secret", "metadata": {"benchmark_memory_id": "wrong"}}


class _BadSearchMem0(_FakeMem0):
    def search(self, *args, **kwargs):
        super().search(*args, **kwargs)
        return []


class _BadGetAllMem0(_FakeMem0):
    def get_all(self, *args, **kwargs):
        super().get_all(*args, **kwargs)
        return {"items": []}


class _BadUpdateMem0(_FakeMem0):
    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        return {"updated": True}


class _ExplodingMem0(_FakeMem0):
    def add(self, *args, **kwargs):
        raise ConnectionError("backend unavailable")


class _PersistentFakeFactory:
    persistent = True

    def __init__(self) -> None:
        self.first = _FakeMem0()
        self.reopened = _FakeMem0()
        self.reopen_calls: list[str] = []

    def __call__(self, instance_id):
        return self.first

    def reopen(self, instance_id):
        self.reopen_calls.append(instance_id)
        self.reopened.records = self.first.records
        return self.reopened


class _ClosableFakeMem0(_FakeMem0):
    def __init__(self) -> None:
        super().__init__()
        self.memory_close_calls = 0
        self.vector_close_calls = 0

        class _Client:
            def __init__(client_self) -> None:
                client_self.owner = self

            def close(client_self) -> None:
                client_self.owner.vector_close_calls += 1

        class _VectorStore:
            def __init__(store_self) -> None:
                store_self.client = _Client()

        self.vector_store = _VectorStore()

    def close(self) -> None:
        self.memory_close_calls += 1


class Mem0AdapterFakeSdkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.memory = _FakeMem0()
        self.adapter = Mem0Adapter(lambda instance_id: self.memory)

    def test_adapter_uses_shared_contract_and_raw_add_inference_is_disabled(self):
        instance = generate_instance("atomic_multi_write", 601, {"txn_size": 1})
        instance["failure_schedule"] = []

        observation = self.adapter.run(instance)

        self.assertIsInstance(self.adapter, MemoryAdapter)
        self.assertEqual(len(self.memory.add_calls), 1)
        self.assertFalse(self.memory.add_calls[0]["infer"])
        self.assertEqual(self.memory.add_calls[0]["agent_id"], instance["operations"][0]["agent_id"])
        self.assertEqual(self.memory.llm_calls, 0)
        self.assertEqual(observation.final_memories["m_write_1"]["memory_id"], "m_write_1")
        self.assertEqual(observation.final_memories["m_write_1"]["value"], "m_write_1")

    def test_add_metadata_contains_benchmark_identity_and_sdk_uuid_mapping(self):
        instance = generate_instance("atomic_multi_write", 602, {"txn_size": 1})
        instance["failure_schedule"] = []

        self.adapter.run(instance)

        added = self.memory.add_calls[0]
        metadata = added["metadata"]
        self.assertEqual(metadata["benchmark_memory_id"], "m_write_1")
        self.assertEqual(metadata["instance_id"], instance["instance_id"])
        self.assertNotIn("agent_id", metadata)
        self.assertEqual(metadata["scope"], "tenant:user_001")
        self.assertEqual(metadata["status"], "active")
        self.assertEqual(set(metadata), {"benchmark_memory_id", "instance_id", "scope", "entity_id", "attribute", "status", "policy_version", "supersedes_id", "derived_from"})
        self.assertEqual(self.memory.update_calls, [])
        self.assertEqual(set(self.memory.records), {"sdk-1"})

    def test_repeated_benchmark_id_uses_native_update_and_retains_one_normalized_record(self):
        instance = generate_instance("atomic_multi_write", 603, {"txn_size": 2})
        instance["failure_schedule"] = []
        instance["operations"][2]["memory_id"] = "m_write_1"
        instance["operations"][2]["value"] = "replacement"

        observation = self.adapter.run(instance)

        self.assertEqual(len(self.memory.add_calls), 1)
        self.assertEqual(self.memory.update_calls[0]["memory_id"], "sdk-1")
        self.assertEqual(observation.final_memories["m_write_1"]["value"], "replacement")

    def test_scope_and_agent_checks_deny_search_and_direct_id_after_native_retrieval(self):
        instance = generate_instance("scope_bypass", 604)
        private = instance["initial_memories"][0]
        instance["operations"].append(
            {"op_id": "op_agent", "step": 3, "agent_id": "agent_other", "type": "get_by_id", "memory_id": private["memory_id"], "scope": private["scope"]}
        )
        instance["operations"].append(
            {"op_id": "op_agent_search", "step": 4, "agent_id": "agent_other", "type": "search", "query": "private_fact", "scope": private["scope"]}
        )

        observation = self.adapter.run(instance)

        self.assertEqual(observation.metrics["exposed_memory_ids"], [])
        self.assertEqual(observation.metrics["denied_reads"], 4)
        self.assertEqual([event["memory_id"] for event in observation.trace if event.get("event") == "denied_read"], ["m_private", "m_private", "m_private", "m_private"])
        self.assertTrue(all(call["filters"] == {"user_id": f"txnmembench:{instance['instance_id']}"} for call in self.memory.search_calls))
        self.assertTrue(all(call["filters"] == {"user_id": f"txnmembench:{instance['instance_id']}"} for call in self.memory.get_all_calls))
        self.assertTrue(all(call["top_k"] >= 1 for call in self.memory.get_all_calls))

    def test_instances_are_isolated_when_factory_returns_the_same_sdk_object(self):
        first = generate_instance("atomic_multi_write", 605, {"txn_size": 1})
        second = generate_instance("atomic_multi_write", 606, {"txn_size": 1})
        first["failure_schedule"] = []
        second["failure_schedule"] = []

        self.adapter.run(first)
        self.adapter.run(second)

        self.assertEqual({record["user_id"] for record in self.memory.records.values()}, {f"txnmembench:{first['instance_id']}", f"txnmembench:{second['instance_id']}"})

    def test_initial_write_supersede_and_invalidate_are_ordered_single_record_updates(self):
        instance = generate_instance("supersession_consistency", 607)

        observation = self.adapter.run(instance)

        self.assertEqual(observation.final_memories["m_old"]["status"], "superseded")
        self.assertEqual(observation.final_memories["m_new"]["status"], "active")
        self.assertEqual(observation.final_memories["m_new"]["supersedes_id"], "m_old")
        self.assertEqual(observation.metrics["supersession_updates"], 1)
        self.assertIn({"step": 3, "event": "ordered_supersession_updates"}, observation.trace)

        invalidation = generate_instance("provenance_chain_repair", 608, {"provenance_depth": 2})
        invalidated = Mem0Adapter(lambda instance_id: _FakeMem0()).run(invalidation)
        self.assertEqual(invalidated.final_memories["m_root"]["status"], "invalid")
        self.assertEqual(invalidated.final_memories["m_derived_1"]["status"], "active")

    def test_crash_stops_immediately_and_records_capability_absent_without_reopen_claim(self):
        instance = generate_instance("atomic_multi_write", 609, {"txn_size": 2})

        observation = self.adapter.run(instance)

        self.assertEqual(observation.transaction_state, "partial_commit")
        self.assertEqual(observation.committed_memory_ids, ["m_write_1"])
        self.assertIn({"step": 2, "event": "capability_absent", "capability": "crash_recovery"}, observation.trace)

    def test_unknown_sdk_envelopes_and_conflicting_metadata_are_runtime_errors_without_content(self):
        instance = generate_instance("atomic_multi_write", 610, {"txn_size": 1})
        instance["failure_schedule"] = []
        with self.assertRaisesRegex(RuntimeAdapterError, f"{instance['instance_id']}.*op_002") as caught:
            Mem0Adapter(lambda instance_id: _BadAddMem0()).run(instance)
        self.assertNotIn("m_write_1", str(caught.exception))

        scoped = generate_instance("scope_bypass", 611)
        with self.assertRaisesRegex(RuntimeAdapterError, f"{scoped['instance_id']}.*op_002"):
            Mem0Adapter(lambda instance_id: _BadGetMem0()).run(scoped)

        with self.assertRaisesRegex(RuntimeAdapterError, f"{scoped['instance_id']}.*op_001"):
            Mem0Adapter(lambda instance_id: _BadSearchMem0()).run(scoped)

        final_shape = generate_instance("atomic_multi_write", 614, {"txn_size": 1})
        final_shape["failure_schedule"] = []
        with self.assertRaisesRegex(RuntimeAdapterError, f"{final_shape['instance_id']}.*final_state"):
            Mem0Adapter(lambda instance_id: _BadGetAllMem0()).run(final_shape)

        collision = generate_instance("atomic_multi_write", 615, {"txn_size": 2})
        collision["failure_schedule"] = []
        collision["operations"][2]["memory_id"] = "m_write_1"
        with self.assertRaisesRegex(RuntimeAdapterError, f"{collision['instance_id']}.*op_003"):
            Mem0Adapter(lambda instance_id: _BadUpdateMem0()).run(collision)

    def test_fake_sdk_delete_has_its_pinned_success_envelope_and_removes_one_uuid(self):
        response = self.memory.add("delete me", user_id="user", agent_id="agent", metadata={}, infer=False)
        sdk_id = response["results"][0]["id"]

        deleted = self.memory.delete(sdk_id)

        self.assertEqual(deleted, {"message": "Memory deleted successfully"})
        self.assertIsNone(self.memory.get(sdk_id))

    def test_native_call_failure_is_runtime_error_and_unconfigured_recovery_is_capability_absent(self):
        instance = generate_instance("atomic_multi_write", 612, {"txn_size": 1})
        with self.assertRaisesRegex(RuntimeAdapterError, f"{instance['instance_id']}.*op_002"):
            Mem0Adapter(lambda instance_id: _ExplodingMem0()).run(instance)

        crash = generate_instance("crash_during_commit", 613)
        recovered = Mem0Adapter(lambda instance_id: _FakeMem0()).run(crash)
        self.assertIn(
            {"step": 3, "event": "capability_absent", "capability": "crash_recovery"},
            recovered.trace,
        )

    def test_explicit_persistent_reopen_replaces_the_backend_for_final_snapshot(self):
        instance = generate_instance("crash_during_commit", 616)
        factory = _PersistentFakeFactory()

        observation = Mem0Adapter(factory).run(instance)

        self.assertEqual(factory.reopen_calls, [instance["instance_id"]])
        self.assertEqual(observation.final_memories["m_commit"]["memory_id"], "m_commit")
        self.assertEqual(len(factory.reopened.get_all_calls), 1)
        self.assertNotIn("capability_absent", [event.get("event") for event in observation.trace])

    def test_close_calls_memory_and_embedded_qdrant_client(self):
        memory = _ClosableFakeMem0()

        close_mem0_memory(memory)

        self.assertEqual(memory.memory_close_calls, 1)
        self.assertEqual(memory.vector_close_calls, 1)


@unittest.skipUnless(HAS_MEM0, "mem0ai is an optional external-baseline dependency")
class Mem0AdapterNativeTests(unittest.TestCase):
    def test_deterministic_embedded_qdrant_factory_exercises_raw_crud_without_network(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            factory = deterministic_mem0_factory(Path(temporary))
            memory = factory("native-crud")
            try:
                response = memory.add("native value", user_id="native-user", agent_id="native-agent", metadata={"benchmark_memory_id": "native-memory"}, infer=False)
                sdk_id = response["results"][0]["id"]
                self.assertIsNotNone(memory.get(sdk_id))
                self.assertIsInstance(memory.search("native value", filters={"user_id": "native-user"}), dict)
                self.assertIsInstance(memory.update(sdk_id, metadata={"benchmark_memory_id": "native-memory", "status": "active"}), dict)
                self.assertIsInstance(memory.delete(sdk_id), dict)
                self.assertEqual(os.environ["MEM0_TELEMETRY"], "false")
            finally:
                close_mem0_memory(memory)

    def test_adapter_replay_reaches_final_state_and_controls_identity(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            factory = deterministic_mem0_factory(Path(temporary))
            adapter = Mem0Adapter(factory)
            normal = generate_instance("atomic_multi_write", 701, {"txn_size": 1})
            normal["failure_schedule"] = []
            observation = adapter.run(normal)
            self.assertEqual(observation.transaction_state, "committed")
            self.assertEqual(observation.final_memories["m_write_1"]["agent_id"], normal["operations"][0]["agent_id"])

            scoped = generate_instance("scope_bypass", 702)
            private = scoped["initial_memories"][0]
            scoped["operations"].append(
                {
                    "op_id": "op_cross_agent",
                    "step": 3,
                    "agent_id": "agent_other",
                    "type": "get_by_id",
                    "memory_id": private["memory_id"],
                    "scope": private["scope"],
                }
            )
            denied = adapter.run(scoped)
            self.assertEqual(denied.metrics["exposed_memory_ids"], [])
            self.assertEqual(denied.metrics["denied_reads"], 3)

            repeated = generate_instance("atomic_multi_write", 703, {"txn_size": 2})
            repeated["failure_schedule"] = []
            repeated["operations"][2]["memory_id"] = "m_write_1"
            repeated["operations"][2]["value"] = "replacement"
            replaced = adapter.run(repeated)
            self.assertEqual(set(replaced.final_memories), {"m_write_1"})
            self.assertEqual(replaced.final_memories["m_write_1"]["value"], "replacement")

    def test_deterministic_factory_reopens_persistent_qdrant_before_final_snapshot(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            adapter = Mem0Adapter(deterministic_mem0_factory(Path(temporary)))
            instance = generate_instance("crash_during_commit", 704)
            observation = adapter.run(instance)

            self.assertEqual(observation.transaction_state, "partial_commit")
            self.assertIn("m_commit", observation.final_memories)


class Mem0CapabilityTests(unittest.TestCase):
    def test_capability_rows_state_native_record_operations_and_absent_semantics(self):
        by_name = {capability.capability: capability for capability in mem0_capabilities()}
        self.assertTrue(by_name["single_record_read_write"].supported)
        self.assertTrue(by_name["shared_scope_isolation"].supported)
        self.assertTrue(by_name["version_supersession"].supported)
        self.assertFalse(by_name["atomic_multi_record_commit"].supported)
        self.assertFalse(by_name["recursive_provenance_invalidation"].supported)
        self.assertFalse(by_name["crash_recovery"].supported)


if __name__ == "__main__":
    unittest.main()
