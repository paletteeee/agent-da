"""Tests for the benchmark environment bridge and native benchmark runs."""

from __future__ import annotations

import json
import sys
import tempfile
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
from txnmem_model_protocol import ModelProtocolError, ModelResponse, ToolCall
from txnmem_real_agent import AgentToolError, NativeMemoryToolGateway
from txnmem_task_transaction import InMemoryTransactionBackend
from txnmem_transaction_journal import TransactionJournal


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
        self.assertEqual(write_event["step"], 1)
        self.assertEqual(write_event["model_step"], 2)
        self.assertEqual(adapter.executed, [("book_item", {"item_id": "a1"})])

    def test_gateway_records_read_tool_as_memory_read(self):
        backend = InstrumentedMemoryBackend()
        gateway = BenchmarkToolGateway(backend, _StubAdapter())
        gateway.call_benchmark("get_item", {"item_id": "a1"}, step=1)
        self.assertEqual(backend.events[0]["kind"], "memory_read")

    def test_gateway_keeps_event_steps_strict_for_multiple_calls_in_one_model_step(self):
        backend = InstrumentedMemoryBackend()
        gateway = BenchmarkToolGateway(backend, _StubAdapter())
        gateway.call_benchmark("book_item", {"item_id": "a1"}, step=2)
        gateway.call_benchmark("book_item", {"item_id": "a2"}, step=2)
        self.assertEqual([event["step"] for event in backend.events], [1, 2])
        self.assertEqual([event["model_step"] for event in backend.events], [2, 2])

    def test_gateway_does_not_project_failed_benchmark_call_as_memory_event(self):
        class _FailedAdapter(_StubAdapter):
            def execute(self, name, arguments):
                return "Error: rejected", {"ok": False}

        backend = InstrumentedMemoryBackend()
        gateway = BenchmarkToolGateway(backend, _FailedAdapter())

        observation, metadata = gateway.call_benchmark(
            "book_item", {"item_id": "a1"}, step=1
        )

        self.assertEqual(observation, "Error: rejected")
        self.assertFalse(metadata["ok"])
        self.assertEqual(backend.events, [])

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

    def test_task_mode_executes_model_and_benchmark_memory_through_one_journaled_transaction(self):
        class _TransactionOnlyBackend(InMemoryTransactionBackend):
            def __init__(self):
                super().__init__()
                self.direct_calls = []
                self.events = []

            def write(self, memory_id, value=None, **fields):
                self.direct_calls.append(("write", memory_id))
                return {"memory_id": memory_id, "value": value, "status": "active"}

            def read(self, memory_id=None, **fields):
                self.direct_calls.append(("read", memory_id))
                return None

            def search(self, query=None, **fields):
                self.direct_calls.append(("search", query))
                return []

            def validated_events(self):
                return list(self.events)

        backend = _TransactionOnlyBackend()
        model = _StubModel(
            [
                ModelResponse(
                    "", [ToolCall("c1", "book_item", {"item_id": "a1"})]
                ),
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "c2",
                            "memory_write",
                            {"memory_id": "model-memory", "value": "booked"},
                        )
                    ],
                ),
                ModelResponse("done", []),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "benchmark-task.sqlite3"
            report = run_benchmark_agent(
                {
                    "task_id": "benchmark-task",
                    "instruction": "book a1",
                    "transaction_mode": "task",
                    "transaction_journal_path": journal_path,
                    "transaction_id": "txn-benchmark-task",
                },
                model,
                backend,
                _StubAdapter(),
                max_steps=5,
            )

            self.assertTrue(journal_path.exists())
            journal = TransactionJournal(journal_path)
            self.addCleanup(journal.close)
            self.assertEqual(journal.load("txn-benchmark-task").decision, "COMMITTED")
            self.assertEqual(len(journal.intents("txn-benchmark-task")), 2)

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["transaction"]["decision"], "committed")
        self.assertEqual(backend.direct_calls, [])
        self.assertEqual(backend.committed["model-memory"]["value"], "booked")
        projected_ids = [
            memory_id
            for memory_id in backend.committed
            if memory_id.startswith("stub:book_item:")
        ]
        self.assertEqual(projected_ids, ["stub:book_item:0001"])

    def test_model_failure_still_runs_benchmark_evaluator(self):
        evaluated = []

        class _FailingModel:
            def complete(self, messages, tools, *, seed=None, temperature=0.0):
                raise ModelProtocolError("http_error", "offline")

        class _LifecycleAdapter(_StubAdapter):
            def evaluate(self, run_report):
                evaluated.append(run_report["status"])
                return {"status": "available", "success": False}

        report = run_benchmark_agent(
            {"task_id": "model-failure", "instruction": "book"},
            _FailingModel(),
            InstrumentedMemoryBackend(),
            _LifecycleAdapter(),
        )

        self.assertEqual(report["status"], "failed")
        self.assertEqual(evaluated, ["failed"])
        self.assertEqual(report["official"]["status"], "available")
        self.assertIn("model_visible_benchmark_tool_names_sha256", report)
        self.assertEqual(report["model_visible_benchmark_tool_count"], 2)
        self.assertFalse(report["trusted_preflight_enabled"])

    def test_tool_failure_still_runs_benchmark_evaluator(self):
        evaluated = []

        class _ToolFailureAdapter(_StubAdapter):
            def execute(self, name, arguments):
                raise AgentToolError("unauthorized_tool", "not exposed")

            def evaluate(self, run_report):
                evaluated.append(run_report["status"])
                return {"status": "available", "success": False}

        report = run_benchmark_agent(
            {"task_id": "tool-failure", "instruction": "book"},
            _StubModel(
                [ModelResponse("", [ToolCall("c1", "book_item", {"item_id": "a1"})])]
            ),
            InstrumentedMemoryBackend(),
            _ToolFailureAdapter(),
        )

        self.assertEqual(report["status"], "failed")
        self.assertEqual(evaluated, ["failed"])
        self.assertEqual(report["official"]["status"], "available")


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

    def test_appworld_adapter_accepts_task_specific_api_allowlist(self):
        adapter = AppWorldAdapter(api_name_allowlist=["amazon__show_cart"])
        self.assertEqual(adapter.api_name_allowlist, ("amazon__show_cart",))

    def test_execute_rejects_tool_not_exposed_by_schema(self):
        calls = []

        class _Requester:
            def request(self, app_name, method_name, **arguments):
                calls.append((app_name, method_name, arguments))
                return {"ok": True}

        adapter = AppWorldAdapter(requester_factory=lambda: _Requester())
        adapter.requester = _Requester()
        adapter._authorized_tool_names = {"amazon__show_cart"}

        with self.assertRaises(AgentToolError) as raised:
            adapter.execute("amazon__show_account_passwords", {})

        self.assertEqual(raised.exception.code, "unauthorized_tool")
        self.assertEqual(calls, [])
        self.assertEqual(adapter.unauthorized_tool_attempt_count, 1)

    def test_execute_marks_structured_appworld_failure_as_error(self):
        class _Requester:
            def request(self, app_name, method_name, **arguments):
                return {"success": False, "message": "invalid request"}

        adapter = AppWorldAdapter(requester_factory=lambda: _Requester())
        adapter.requester = _Requester()
        adapter._authorized_tool_names = {"amazon__show_cart"}

        observation, metadata = adapter.execute("amazon__show_cart", {})

        self.assertTrue(observation.startswith("Error:"))
        self.assertFalse(metadata["ok"])

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

    def test_official_result_requires_tracker_success_and_task_completion(self):
        class _Tracker:
            success = False
            pass_count = 2
            num_tests = 3

        class _Environment:
            task = type("Task", (), {"instruction": "fixture"})()

            def task_completed(self):
                return True

            def evaluate(self, suppress_errors=True):
                return _Tracker()

            def close(self):
                return None

        adapter = AppWorldAdapter(requester_factory=lambda: None)
        adapter.environment = _Environment()
        result = adapter.evaluate({})
        self.assertFalse(result["success"])
        self.assertFalse(result["official_evaluator_success"])
        self.assertTrue(result["task_completed"])
        self.assertEqual(result["pass_count"], 2)

    def test_official_result_does_not_accept_tracker_success_without_completion(self):
        class _Tracker:
            success = True
            pass_count = 3
            num_tests = 3

        class _Environment:
            def task_completed(self):
                return False

            def evaluate(self, suppress_errors=True):
                return _Tracker()

            def close(self):
                return None

        adapter = AppWorldAdapter(requester_factory=lambda: None)
        adapter.environment = _Environment()
        result = adapter.evaluate({})
        self.assertFalse(result["success"])
        self.assertTrue(result["official_evaluator_success"])
        self.assertFalse(result["task_completed"])

    def test_official_evaluator_saves_appworld_state_before_reading_output_dbs(self):
        order = []

        class _Tracker:
            success = True
            pass_count = 1
            num_tests = 1

        class _Environment:
            def task_completed(self):
                order.append("task_completed")
                return True

            def save(self):
                order.append("save")

            def evaluate(self, suppress_errors=True):
                order.append("evaluate")
                return _Tracker()

            def close(self):
                order.append("close")

        adapter = AppWorldAdapter(requester_factory=lambda: None)
        adapter.environment = _Environment()
        result = adapter.evaluate({})

        self.assertTrue(result["success"])
        self.assertLess(order.index("save"), order.index("evaluate"))
        self.assertEqual(order[-1], "close")


class ManifestGeneratorTest(unittest.TestCase):
    def test_locomo_manifest_from_sample(self):
        manifest = generate_locomo_manifest(
            source=Path("tests/fixtures/locomo_redacted_minimal.json"), max_tasks=2
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
