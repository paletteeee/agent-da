"""Tests for the benchmark environment bridge and native benchmark runs."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_benchmark_bridge import (
    AppWorldAdapter,
    BenchmarkEnvAdapter,
    BenchmarkToolGateway,
    LoCoMoAdapter,
    TauBenchAdapter,
    _ScriptedUser,
    _official_tau_user_strategy,
    _normalize_appworld_root,
    build_merged_schemas,
    run_benchmark_agent,
)
from txnmem_benchmark_manifests import (
    generate_appworld_manifest,
    generate_locomo_manifest,
    generate_tau_bench_manifest,
)
from txnmem_model_protocol import ModelResponse, ToolCall
from txnmem_real_agent import NativeMemoryToolGateway


class _StubModel:
    def __init__(self, calls):
        self._calls = list(calls)

    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        return self._calls.pop(0)


class _StubAdapter(BenchmarkEnvAdapter):
    dataset = "stub"

    def __init__(self):
        self.executed = []

    def tool_schemas(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": "book_item",
                    "description": "Book an item.",
                    "parameters": {
                        "type": "object",
                        "properties": {"item_id": {"type": "string"}},
                        "required": ["item_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_item",
                    "description": "Get an item.",
                    "parameters": {
                        "type": "object",
                        "properties": {"item_id": {"type": "string"}},
                    },
                },
            },
        ]

    def reset(self, task):
        return f"start task {task.get('task_id')}"

    def execute(self, name, arguments):
        self.executed.append((name, dict(arguments)))
        return f"done {name}", {"ok": True}

    def evaluate(self, run_report):
        return {"reward": 1.0}

    def tool_event_kind(self, name):
        return {
            "book_item": "memory_write",
            "get_item": "memory_read",
        }.get(name)


class BenchmarkBridgeTest(unittest.TestCase):
    def test_merged_schemas_contain_memory_and_benchmark_tools(self):
        memory = NativeMemoryToolGateway.schemas()
        benchmark = [
            {"type": "function", "function": {"name": "book_item", "parameters": {"type": "object"}}}
        ]
        merged = build_merged_schemas(memory, benchmark)
        names = {schema["function"]["name"] for schema in merged}
        self.assertIn("memory_write", names)
        self.assertIn("book_item", names)
        self.assertEqual(len(merged), len(memory) + 1)

    def test_gateway_dispatches_benchmark_and_memory_calls(self):
        backend = InstrumentedMemoryBackend()
        adapter = _StubAdapter()
        gateway = BenchmarkToolGateway(backend, adapter)
        result = gateway.call_benchmark("book_item", {"item_id": "a1"}, step=2)
        self.assertEqual(result[0], "done book_item")
        kinds = [event["kind"] for event in backend.events]
        self.assertEqual(kinds, ["memory_write"])
        write_event = backend.events[0]
        self.assertEqual(write_event["projection"], "benchmark_tool_call")
        self.assertEqual(write_event["step"], 2)
        self.assertEqual(adapter.executed, [("book_item", {"item_id": "a1"})])

    def test_gateway_records_read_tool_as_memory_read(self):
        backend = InstrumentedMemoryBackend()
        gateway = BenchmarkToolGateway(backend, _StubAdapter())
        gateway.call_benchmark("get_item", {"item_id": "a1"}, step=1)
        self.assertEqual(backend.events[0]["kind"], "memory_read")

    def test_run_benchmark_agent_full_loop(self):
        backend = InstrumentedMemoryBackend()
        model = _StubModel(
            [
                ModelResponse(
                    "",
                    [ToolCall("c1", "book_item", {"item_id": "a1"})],
                ),
                ModelResponse(
                    "",
                    [ToolCall("c2", "memory_write", {"memory_id": "m1", "value": "booked"})],
                ),
                ModelResponse("all done", []),
            ]
        )
        report = run_benchmark_agent(
            {"task_id": "stub-task-1", "instruction": "book a1"},
            model,
            backend,
            _StubAdapter(),
            max_steps=5,
        )
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["official"], {"reward": 1.0})
        kinds = [event["kind"] for event in report["events"]]
        self.assertIn("memory_write", kinds)
        self.assertEqual(len(report["events"]), 2)

    def test_run_benchmark_agent_records_benchmark_projection(self):
        backend = InstrumentedMemoryBackend()
        model = _StubModel(
            [
                ModelResponse("", [ToolCall("c1", "book_item", {"item_id": "a1"})]),
                ModelResponse("ok", []),
            ]
        )
        report = run_benchmark_agent(
            {"task_id": "stub-task-2", "instruction": "book"},
            model,
            backend,
            _StubAdapter(),
            max_steps=3,
        )
        projections = {event.get("projection") for event in report["events"]}
        self.assertIn("benchmark_tool_call", projections)


class ScriptedUserTest(unittest.TestCase):
    def test_scripted_user_is_non_interactive_and_terminates(self):
        user = _ScriptedUser()
        self.assertIn("Book the itinerary", user.reset("Book the itinerary"))
        self.assertEqual(user.step("Thanks"), "###STOP###")
        self.assertEqual(user.get_total_cost(), 0.0)

    def test_scripted_boundary_uses_official_human_constructor_then_replaces_input(self):
        self.assertEqual(_official_tau_user_strategy("scripted"), "human")
        self.assertEqual(_official_tau_user_strategy("human"), "human")


class TauBenchAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from tau_bench.envs.airline.env import MockAirlineDomainEnv
        except ImportError:
            raise unittest.SkipTest("tau-bench not installed")
        cls.env_cls = MockAirlineDomainEnv

    def test_tool_schemas_from_env(self):
        env = self.env_cls(user_strategy="human", task_index=0)
        adapter = TauBenchAdapter(lambda: env)
        schemas = adapter.tool_schemas()
        names = {schema["function"]["name"] for schema in schemas}
        self.assertIn("book_reservation", names)
        self.assertIn("search_direct_flight", names)

    def test_classify_read_write_tools(self):
        env = self.env_cls(user_strategy="human", task_index=0)
        adapter = TauBenchAdapter(lambda: env)
        self.assertEqual(adapter.tool_event_kind("search_direct_flight"), "memory_search")
        self.assertEqual(adapter.tool_event_kind("get_user_details"), "memory_read")
        self.assertEqual(adapter.tool_event_kind("book_reservation"), "memory_write")
        self.assertEqual(adapter.tool_event_kind("respond"), None)

    def test_execute_steps_env(self):
        env = self.env_cls(user_strategy="human", task_index=0)
        adapter = TauBenchAdapter(lambda: env)
        observation, metadata = adapter.execute("list_all_airports", {})
        self.assertIsInstance(observation, str)
        self.assertTrue(len(observation) > 0)


class AppWorldAdapterTest(unittest.TestCase):
    def test_appworld_root_accepts_data_directory_or_root_directory(self):
        self.assertEqual(_normalize_appworld_root(Path("/tmp/appworld/data")), Path("/tmp/appworld"))
        self.assertEqual(_normalize_appworld_root(Path("/tmp/appworld")), Path("/tmp/appworld"))

    def test_appworld_adapter_accepts_task_specific_app_allowlist(self):
        adapter = AppWorldAdapter(app_names=["venmo", "supervisor"])
        self.assertEqual(adapter.app_names, ("venmo", "supervisor"))

    def test_classify_api_tools(self):
        adapter = AppWorldAdapter(requester_factory=lambda: None)
        adapter.tool_schemas()
        self.assertEqual(adapter.tool_event_kind("venmo__create_payment_request"), "memory_write")
        self.assertEqual(adapter.tool_event_kind("gmail__show_email"), "memory_read")

    def test_tool_schemas_from_api_docs(self):
        try:
            import appworld  # noqa: F401
        except ImportError:
            self.skipTest("appworld not installed")
        adapter = AppWorldAdapter(requester_factory=lambda: None)
        schemas = adapter.tool_schemas()
        names = {schema["function"]["name"] for schema in schemas}
        self.assertTrue(any("__" in name for name in names))
        self.assertTrue(adapter._tool_kinds)

    def test_memory_id_namespace(self):
        adapter = AppWorldAdapter(requester_factory=lambda: None)
        self.assertTrue(adapter.memory_id_for("venmo__create", {}, 3).startswith("appworld:"))


class ManifestGeneratorTest(unittest.TestCase):
    def test_locomo_manifest_from_sample(self):
        manifest = generate_locomo_manifest(
            source=Path("external_data/raw/locomo10.json"), max_tasks=2
        )
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(len(manifest["tasks"]), 2)
        task = manifest["tasks"][0]
        self.assertIn("instruction", task)
        self.assertIn("sample_id", task)

    def test_tau_manifest_requires_installed_package(self):
        try:
            import tau_bench  # noqa: F401
        except ImportError:
            self.skipTest("tau-bench not installed")
        manifest = generate_tau_bench_manifest(domain="airline", task_split="test", max_tasks=2)
        self.assertEqual(len(manifest["tasks"]), 2)
        self.assertTrue(manifest["tasks"][0]["task_id"].startswith("tau-airline-"))

    def test_appworld_manifest_from_data_root(self):
        root = Path("external_data/deps/appworld-data/data")
        if not (root / "tasks").is_dir():
            self.skipTest("appworld data not present")
        manifest = generate_appworld_manifest(data_root=root, max_tasks=2)
        self.assertEqual(len(manifest["tasks"]), 2)
        task = manifest["tasks"][0]
        self.assertTrue(task["task_id"].startswith("appworld-"))
        self.assertIn("instruction", task)


if __name__ == "__main__":
    unittest.main()
