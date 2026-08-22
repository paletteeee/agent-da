"""Native LangGraph Store adapter behavior."""

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
    UnsupportedMappingError,
)
from txnmem_langgraph_adapter import (  # noqa: E402
    LangGraphStoreAdapter,
    langgraph_capabilities,
)
from txnmem_workloads import generate_instance  # noqa: E402


HAS_LANGGRAPH = importlib.util.find_spec("langgraph") is not None


class _Item:
    def __init__(self, value):
        self.value = value


class _MemoryStore:
    def __init__(self):
        self.records = {}
        self.search_calls = []

    def put(self, namespace, key, value, index=None):
        self.records[(namespace, key)] = dict(value)

    def get(self, namespace, key):
        value = self.records.get((namespace, key))
        return None if value is None else _Item(dict(value))

    def search(self, namespace_prefix, *, query=None, filter=None):
        self.search_calls.append((namespace_prefix, query, filter))
        return [
            _Item(dict(value))
            for (namespace, _), value in self.records.items()
            if namespace[: len(namespace_prefix)] == namespace_prefix
            and (
                filter is None
                or all(
                    value.get(key) != expected["$ne"]
                    if isinstance(expected, dict) and "$ne" in expected
                    else value.get(key) == expected
                    for key, expected in filter.items()
                )
            )
        ]

    def delete(self, namespace, key):
        self.records.pop((namespace, key), None)


class _StoreContext:
    def __init__(self, store, enter_error=None):
        self.store = store
        self.enter_error = enter_error
        self.entered = False
        self.closed = False

    def __enter__(self):
        self.entered = True
        if self.enter_error is not None:
            raise self.enter_error
        return self.store

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True


class _ExplodingStore(_MemoryStore):
    def put(self, namespace, key, value, index=None):
        raise ConnectionError("backend unavailable")


@unittest.skipUnless(
    HAS_LANGGRAPH,
    "langgraph is an optional external-baseline dependency; "
    "install requirements-baselines.txt before running native Store tests",
)
class LangGraphStoreAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        from langgraph.store.memory import InMemoryStore

        self.store = InMemoryStore()
        self.adapter = LangGraphStoreAdapter(
            lambda: self.store,
            experiment_run_id="run_native_test",
        )

    def test_adapter_uses_the_shared_memory_contract(self):
        self.assertIsInstance(self.adapter, MemoryAdapter)

    def test_write_uses_native_put_and_direct_id_read_uses_native_get(self):
        instance = generate_instance("atomic_multi_write", 401, {"txn_size": 1})
        instance["failure_schedule"] = []
        instance["operations"].insert(
            2,
            {
                "op_id": "op_read_written",
                "step": 3,
                "agent_id": instance["operations"][0]["agent_id"],
                "type": "get_by_id",
                "memory_id": "m_write_1",
                "scope": "tenant:user_001",
            },
        )
        instance["operations"][3]["step"] = 4

        observation = self.adapter.run(instance)

        namespace = (
            "run_native_test",
            instance["instance_id"],
            instance["operations"][1]["agent_id"],
            "tenant:user_001",
        )
        native_item = self.store.get(namespace, "m_write_1")
        self.assertIsNotNone(native_item)
        self.assertEqual(native_item.value["memory_id"], "m_write_1")
        self.assertEqual(observation.metrics["exposed_memory_ids"], ["m_write_1"])
        self.assertEqual(observation.final_memories["m_write_1"], native_item.value)

    def test_search_uses_native_namespace_and_metadata_filter(self):
        instance = generate_instance("scope_bypass", 402)
        private = instance["initial_memories"][0]
        instance["operations"] = [
            {
                "op_id": "op_visible_search",
                "step": 1,
                "agent_id": private["agent_id"],
                "type": "search",
                "query": "private_fact",
                "scope": private["scope"],
            }
        ]

        observation = self.adapter.run(instance)

        namespace = (
            "run_native_test",
            instance["instance_id"],
            private["agent_id"],
            private["scope"],
        )
        active = self.store.search(namespace, query=None, filter={"status": "active"})
        self.assertEqual([item.value["memory_id"] for item in active], ["m_private"])
        self.assertEqual(observation.metrics["exposed_memory_ids"], ["m_private"])

    def test_same_key_is_a_native_last_write_and_normalizes_item_values(self):
        instance = generate_instance("atomic_multi_write", 403, {"txn_size": 2})
        instance["failure_schedule"] = []
        instance["operations"][1]["value"] = "first"
        instance["operations"][2]["memory_id"] = "m_write_1"
        instance["operations"][2]["value"] = "replacement"

        observation = self.adapter.run(instance)

        self.assertEqual(observation.final_memories["m_write_1"]["value"], "replacement")
        self.assertEqual(observation.committed_memory_ids, ["m_write_1", "m_write_1"])

    def test_instance_namespaces_are_isolated_in_one_native_store(self):
        first = generate_instance("atomic_multi_write", 404, {"txn_size": 1})
        second = generate_instance("atomic_multi_write", 405, {"txn_size": 1})
        first["failure_schedule"] = []
        second["failure_schedule"] = []

        self.adapter.run(first)
        self.adapter.run(second)

        first_namespace = (
            "run_native_test",
            first["instance_id"],
            first["operations"][0]["agent_id"],
            "tenant:user_001",
        )
        second_namespace = (
            "run_native_test",
            second["instance_id"],
            second["operations"][0]["agent_id"],
            "tenant:user_001",
        )
        self.assertNotEqual(first_namespace, second_namespace)
        self.assertEqual(self.store.get(first_namespace, "m_write_1").value["memory_id"], "m_write_1")
        self.assertEqual(self.store.get(second_namespace, "m_write_1").value["memory_id"], "m_write_1")

    def test_native_delete_removes_one_store_record(self):
        namespace = ("run_native_test", "delete_instance", "agent_1", "shared")
        self.store.put(
            namespace,
            "m_delete",
            {"memory_id": "m_delete", "status": "active"},
            index=False,
        )

        self.store.delete(namespace, "m_delete")

        self.assertIsNone(self.store.get(namespace, "m_delete"))

    def test_wrong_shared_scope_cannot_expose_a_direct_id_record(self):
        instance = generate_instance("scope_bypass", 406)

        observation = self.adapter.run(instance)

        self.assertEqual(observation.metrics["exposed_memory_ids"], [])
        self.assertEqual(observation.metrics["denied_reads"], 2)

    def test_invalidation_updates_only_the_named_native_record(self):
        instance = generate_instance("provenance_chain_repair", 407, {"provenance_depth": 2})

        observation = self.adapter.run(instance)

        self.assertEqual(observation.final_memories["m_root"]["status"], "invalid")
        self.assertEqual(observation.final_memories["m_derived_1"]["status"], "active")
        self.assertEqual(observation.metrics["repair_count"], 0)

    def test_in_memory_crash_recovery_is_recorded_as_capability_absent(self):
        instance = generate_instance("crash_during_commit", 408)

        observation = self.adapter.run(instance)

        self.assertEqual(observation.transaction_state, "partial_commit")
        self.assertIn(
            {"step": 3, "event": "capability_absent", "capability": "crash_recovery"},
            observation.trace,
        )


class LangGraphCapabilityTests(unittest.TestCase):
    def test_capability_rows_are_deterministic_and_describe_missing_semantics(self):
        capabilities = langgraph_capabilities()

        self.assertIsInstance(capabilities, tuple)
        by_name = {capability.capability: capability for capability in capabilities}
        self.assertTrue(by_name["single_record_read_write"].supported)
        self.assertTrue(by_name["shared_scope_isolation"].supported)
        self.assertTrue(by_name["version_supersession"].supported)
        self.assertFalse(by_name["atomic_multi_record_commit"].supported)
        self.assertFalse(by_name["recursive_provenance_invalidation"].supported)
        self.assertFalse(by_name["crash_recovery"].supported)


class LangGraphStoreLifecycleTests(unittest.TestCase):
    def test_persistent_crash_claim_raises_unsupported_mapping_without_observation(self):
        instance = generate_instance("crash_during_commit", 501)
        adapter = LangGraphStoreAdapter(
            _MemoryStore,
            experiment_run_id="run_lifecycle_test",
            persistent_store=True,
        )

        with self.assertRaisesRegex(
            UnsupportedMappingError,
            f"{instance['instance_id']}.*op_003",
        ):
            adapter.run(instance)

    def test_context_manager_factory_is_entered_and_closed_once_per_run(self):
        instance = generate_instance("atomic_multi_write", 502, {"txn_size": 1})
        instance["failure_schedule"] = []
        context = _StoreContext(_MemoryStore())

        observation = LangGraphStoreAdapter(
            lambda: context,
            experiment_run_id="run_lifecycle_test",
        ).run(instance)

        self.assertEqual(observation.transaction_state, "committed")
        self.assertTrue(context.entered)
        self.assertTrue(context.closed)

    def test_context_manager_enter_failure_is_a_runtime_adapter_error(self):
        instance = generate_instance("atomic_multi_write", 503, {"txn_size": 1})
        context = _StoreContext(_MemoryStore(), enter_error=ConnectionError("postgres unavailable"))

        with self.assertRaisesRegex(RuntimeAdapterError, instance["instance_id"]):
            LangGraphStoreAdapter(lambda: context).run(instance)

        self.assertTrue(context.entered)
        self.assertFalse(context.closed)

    def test_context_manager_is_closed_when_a_native_call_fails(self):
        instance = generate_instance("atomic_multi_write", 504, {"txn_size": 1})
        context = _StoreContext(_ExplodingStore())

        with self.assertRaises(RuntimeAdapterError):
            LangGraphStoreAdapter(lambda: context).run(instance)

        self.assertTrue(context.closed)

    def test_search_uses_a_native_non_invalid_status_filter(self):
        instance = generate_instance("scope_bypass", 505)
        store = _MemoryStore()

        LangGraphStoreAdapter(lambda: store).run(instance)

        self.assertEqual(
            [filter for _, _, filter in store.search_calls],
            [{"status": {"$ne": "invalid"}}],
        )

    def test_invalid_store_factory_result_is_a_clear_type_error_not_runtime_failure(self):
        instance = generate_instance("atomic_multi_write", 506, {"txn_size": 1})

        with self.assertRaisesRegex(TypeError, "put, get, and search") as caught:
            LangGraphStoreAdapter(lambda: object()).run(instance)

        self.assertNotIsInstance(caught.exception, RuntimeAdapterError)


if __name__ == "__main__":
    unittest.main()
