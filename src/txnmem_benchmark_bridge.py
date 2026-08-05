"""Benchmark environment bridge for native TxnMem memory trace collection.

A BenchmarkEnvAdapter exposes the real tool schemas and execution of an
official Agent benchmark (tau-bench, AppWorld, LoCoMo) so that a model agent
can call the benchmark's real tools AND the TxnMem memory tools in one loop.
Every benchmark tool call that changes or reads state is recorded into the
instrumented memory backend as a native event (projection=benchmark_tool_call),
while memory_* calls made by the model are recorded by the memory gateway as
real_model_native events.  The two sources together form the native trace.
"""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Callable

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_failure_controller import FailureController, FailureInjectionError
from txnmem_model_protocol import ModelProtocolError
from txnmem_real_agent import NativeMemoryToolGateway, AgentToolError, _assistant_message, _failed_report


class BenchmarkEnvAdapter:
    """Uniform interface over an official benchmark environment."""

    dataset = "benchmark"
    official_evaluator_status = "available"

    def tool_schemas(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def reset(self, task: Mapping[str, Any]) -> str:
        """Return the initial observation for a task."""
        raise NotImplementedError

    def execute(self, name: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        """Execute one benchmark tool call; return (observation, metadata)."""
        raise NotImplementedError

    def evaluate(self, run_report: Mapping[str, Any]) -> dict[str, Any]:
        """Run the official evaluator over a completed run."""
        raise NotImplementedError

    def tool_event_kind(self, name: str) -> str | None:
        """Map a benchmark tool name to a canonical memory event kind, or None."""
        return None

    def memory_id_for(self, name: str, arguments: Mapping[str, Any], step: int) -> str:
        return f"{self.dataset}:{name}:{step:04d}"


def _classify_tool_name(name: str, read_prefixes: tuple[str, ...], write_prefixes: tuple[str, ...]) -> str | None:
    normalized = name.lower()
    if normalized.startswith(read_prefixes):
        return "memory_search" if normalized.startswith(("search_", "list_", "find_")) else "memory_read"
    if normalized.startswith(write_prefixes):
        return "memory_write"
    return None


def _official_tau_user_strategy(strategy: str) -> str:
    """Map TxnMem's scripted boundary to an official tau-bench constructor."""

    return "human" if strategy == "scripted" else strategy


def _normalize_appworld_root(path: Path) -> Path:
    """Accept either AppWorld's root directory or its nested data directory."""

    path = Path(path)
    return path.parent if path.name == "data" else path


class TauBenchAdapter(BenchmarkEnvAdapter):
    dataset = "tau-bench"

    _READ_PREFIXES = ("get_", "list_", "search_", "find_", "calculate_", "think_")
    _WRITE_PREFIXES = ("book_", "cancel_", "modify_", "update_", "edit_", "change_", "add_", "remove_", "send_")

    def __init__(
        self,
        env_factory: Callable[[], Any],
        task_split: str = "test",
        user_strategy: str = "human",
    ):
        self.env_factory = env_factory
        self.task_split = task_split
        self.user_strategy = user_strategy
        self.env = None

    def tool_schemas(self) -> list[dict[str, Any]]:
        if self.env is None:
            self.env = self.env_factory()
        return [copy.deepcopy(info) for info in self.env.tools_info]

    def reset(self, task: Mapping[str, Any]) -> str:
        if self.env is None:
            self.env = self.env_factory()
        if self.user_strategy == "scripted":
            self.env.user = _ScriptedUser()
        task_index = task.get("task_index")
        response = self.env.reset(task_index=task_index)
        self.task_index = task_index if task_index is not None else self.env.task_index
        return str(response.observation)

    def execute(self, name: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        from tau_bench.types import Action, RESPOND_ACTION_NAME

        if self.env is None:
            self.env = self.env_factory()
        action_name = RESPOND_ACTION_NAME if name == "respond" else name
        action = Action(name=action_name, kwargs=dict(arguments))
        response = self.env.step(action)
        metadata = {
            "reward": response.reward,
            "done": response.done,
            "source": response.info.source,
            "task_index": getattr(self.env, "task_index", None),
        }
        return str(response.observation), metadata

    def evaluate(self, run_report: Mapping[str, Any]) -> dict[str, Any]:
        reward = 0.0
        if self.env is not None:
            try:
                result = self.env.calculate_reward()
                reward = result.reward
            except Exception as exc:
                self.official_evaluator_status = "error"
                return {
                    "status": "error",
                    "official_evaluator_error": f"{type(exc).__name__}: {exc}",
                }
        self.official_evaluator_status = "available"
        return {"status": "available", "reward": float(reward)}

    def tool_event_kind(self, name: str) -> str | None:
        if name == "respond":
            return None
        return _classify_tool_name(name, self._READ_PREFIXES, self._WRITE_PREFIXES)


class _ScriptedUser:
    """Non-interactive user boundary for tool-runtime smoke tests.

    The official tau-bench environment still supplies the task, tools, state,
    and reward evaluator.  Only the human stdin boundary is replaced so a
    reproducible model smoke can run unattended.
    """

    def reset(self, instruction: str | None = None) -> str:
        return str(instruction or "")

    def step(self, content: str) -> str:
        return "###STOP###"

    def get_total_cost(self) -> float:
        return 0.0


class AppWorldAdapter(BenchmarkEnvAdapter):
    dataset = "appworld"

    _READ_PREFIXES = ("get_", "list_", "read_", "search_", "find_", "show_", "fetch_", "lookup_", "describe_")
    _WRITE_PREFIXES = ("create_", "add_", "update_", "delete_", "remove_", "send_", "pay_", "save_", "complete_", "book_", "set_", "modify_")

    def __init__(
        self,
        requester_factory: Callable[[], Any] | None = None,
        appworld_root: Path | None = None,
        app_names: Sequence[str] | None = None,
        experiment_name: str = "txnmem_native",
    ):
        self.requester_factory = requester_factory
        self.appworld_root = _normalize_appworld_root(appworld_root) if appworld_root else None
        self.app_names = tuple(str(name) for name in app_names) if app_names is not None else None
        self.experiment_name = experiment_name
        self.requester = None
        self.environment = None
        self.api_docs: list[dict[str, Any]] = []
        self._tool_kinds: dict[str, str] = {}

    def tool_schemas(self) -> list[dict[str, Any]]:
        if self.requester is None and self.requester_factory is not None:
            self.requester = self.requester_factory()
        try:
            from appworld.api_docs import prepare_api_docs
            from appworld.apps import get_all_apps

            apps = list(self.app_names) if self.app_names is not None else [app for app in get_all_apps() if app != "admin"]
            self.api_docs = []
            for app_name in apps:
                docs = prepare_api_docs(
                    app_name, include_private_apis=False, format="function_calling"
                )
                if isinstance(docs, list):
                    self.api_docs.extend(docs)
        except Exception:
            self.api_docs = []
        schemas = []
        for doc in self.api_docs:
            function = doc.get("function", {}) if isinstance(doc, Mapping) else {}
            tool_name = function.get("name", "")
            if not isinstance(tool_name, str) or not tool_name:
                continue
            method_name = tool_name.split("__")[-1] if "__" in tool_name else tool_name
            self._tool_kinds[tool_name] = _classify_tool_name(
                method_name, self._READ_PREFIXES, self._WRITE_PREFIXES
            ) or "memory_read"
            schemas.append(dict(doc))
        return schemas

    def reset(self, task: Mapping[str, Any]) -> str:
        if self.requester is None:
            if self.requester_factory is not None:
                self.requester = self.requester_factory()
            elif self.appworld_root is not None:
                import os

                from appworld.environment import AppWorld

                os.environ["APPWORLD_ROOT"] = str(self.appworld_root)
                task_id = str(task.get("task_dir") or task.get("task_id") or "")
                task_id = task_id.removeprefix("appworld-")
                self.environment = AppWorld(
                    task_id=task_id,
                    experiment_name=self.experiment_name,
                    load_ground_truth=True,
                    ground_truth_mode="minimal",
                    random_seed=int(task.get("seed", 0)),
                    show_api_response_schemas=True,
                    max_interactions=int(task.get("max_steps", 30)),
                    max_api_calls_per_interaction=20,
                    raise_on_failure=False,
                )
                self.requester = self.environment.requester
        if self.environment is not None:
            return str(self.environment.task.instruction)
        return str(task.get("instruction", ""))

    def execute(self, name: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        if self.requester is None:
            raise AgentToolError("missing_runtime", "AppWorld requester is not initialized")
        if "__" not in name:
            raise AgentToolError("unknown_tool", f"unsupported appworld tool: {name}")
        app_name, method_name = name.split("__", 1)
        try:
            response = self.requester.request(app_name, method_name, **dict(arguments))
        except Exception as exc:
            return f"Error: {type(exc).__name__}: {exc}", {"ok": False}
        return str(response), {"ok": True}

    def evaluate(self, run_report: Mapping[str, Any]) -> dict[str, Any]:
        if self.environment is not None:
            try:
                task_completed = bool(self.environment.task_completed())
                tracker = self.environment.evaluate(suppress_errors=True)
                result = {
                    "status": "available",
                    "official_evaluator": "appworld.task_completed",
                    "success": task_completed,
                    "task_completed": task_completed,
                    "pass_count": int(tracker.pass_count),
                    "total_count": int(tracker.num_tests),
                }
            finally:
                self.environment.close()
                self.environment = None
            self.official_evaluator_status = "available"
            return result
        self.official_evaluator_status = "blocked"
        return {
            "status": "blocked",
            "official_evaluator": "appworld_task_completed_not_available_offline",
            "error": "official AppWorld environment is not initialized",
        }

    def tool_event_kind(self, name: str) -> str | None:
        if name in self._tool_kinds:
            return self._tool_kinds[name]
        method_name = name.split("__")[-1] if "__" in name else name
        return _classify_tool_name(method_name, self._READ_PREFIXES, self._WRITE_PREFIXES)

    def memory_id_for(self, name: str, arguments: Mapping[str, Any], step: int) -> str:
        return f"appworld:tool:{step:04d}"

class LoCoMoAdapter(BenchmarkEnvAdapter):
    """LoCoMo has no tools; sessions are the transaction boundaries."""

    dataset = "locomo"

    def __init__(self, evaluator_command: Sequence[str] | None = None, evaluator_timeout: float = 60.0):
        self.evaluator_command = tuple(str(part) for part in (evaluator_command or ()))
        self.evaluator_timeout = float(evaluator_timeout)
        self.current_task: Mapping[str, Any] = {}

    def tool_schemas(self) -> list[dict[str, Any]]:
        return []

    def reset(self, task: Mapping[str, Any]) -> str:
        self.current_task = task
        return str(task.get("instruction", task.get("prompt", "")))

    def execute(self, name: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        raise AgentToolError("no_tools", "locomo adapter exposes no benchmark tools")

    def evaluate(self, run_report: Mapping[str, Any]) -> dict[str, Any]:
        if not self.evaluator_command:
            self.official_evaluator_status = "blocked"
            return {
                "status": "blocked",
                "official_evaluator": "locomo_qa_not_available_offline",
                "error": "official LoCoMo QA evaluator is not configured",
            }
        executable = shutil.which(self.evaluator_command[0])
        if executable is None and not Path(self.evaluator_command[0]).is_file():
            self.official_evaluator_status = "blocked"
            return {
                "status": "blocked",
                "official_evaluator": "locomo_qa_command_missing",
                "error": f"evaluator executable not found: {self.evaluator_command[0]}",
            }
        payload = {
            "task_id": self.current_task.get("task_id"),
            "prediction": run_report.get("final_text", ""),
            "annotation": self.current_task.get("qa_annotation"),
        }
        try:
            completed = subprocess.run(
                list(self.evaluator_command),
                input=json.dumps(payload, ensure_ascii=False),
                text=True,
                capture_output=True,
                timeout=self.evaluator_timeout,
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(f"evaluator exited {completed.returncode}")
            result = json.loads(completed.stdout)
            if not isinstance(result, Mapping):
                raise ValueError("evaluator output must be a JSON object")
            required = ("question_count", "correct_count", "score")
            if any(key not in result for key in required):
                raise ValueError("evaluator output must contain question_count, correct_count, score")
        except Exception as exc:
            self.official_evaluator_status = "error"
            return {
                "status": "error",
                "official_evaluator": "locomo_qa_command",
                "error": f"{type(exc).__name__}: {exc}",
            }
        self.official_evaluator_status = "available"
        return {
            "status": "available",
            "official_evaluator": "locomo_qa_command",
            "question_count": int(result["question_count"]),
            "correct_count": int(result["correct_count"]),
            "score": float(result["score"]),
        }

    def tool_event_kind(self, name: str) -> str | None:
        return None


class BenchmarkToolGateway(NativeMemoryToolGateway):
    """A tool gateway that dispatches memory_* calls to the memory backend and
    benchmark tools to the environment adapter."""

    def __init__(
        self,
        backend: InstrumentedMemoryBackend,
        adapter: BenchmarkEnvAdapter,
        agent_id: str = "agent_model",
        failure_controller: FailureController | None = None,
    ):
        super().__init__(backend, agent_id=agent_id, failure_controller=failure_controller)
        self.adapter = adapter
        self.benchmark_calls: list[dict[str, Any]] = []

    def call_benchmark(self, name: str, arguments: Mapping[str, Any], step: int) -> tuple[str, dict[str, Any]]:
        observation, metadata = self.adapter.execute(name, arguments)
        self.benchmark_calls.append(
            {"name": name, "arguments": dict(arguments), "observation": observation, "step": step}
        )
        kind = self.adapter.tool_event_kind(name)
        if kind is not None:
            memory_id = self.adapter.memory_id_for(name, arguments, step)
            event_fields = {
                "agent_id": self.agent_id,
                "projection": "benchmark_tool_call",
                "tool_name": name,
                "model_step": step,
            }
            if kind == "memory_write":
                self.backend.write(memory_id, value={"tool_name": name, "arguments": dict(arguments)}, **event_fields)
            elif kind in {"memory_read", "memory_search"}:
                self.backend.read(memory_id, **event_fields) if kind == "memory_read" else self.backend.search(
                    query=str(arguments.get("query", "")), **event_fields
                )
        return observation, metadata


def build_merged_schemas(memory_schemas: Sequence[Mapping[str, Any]], benchmark_schemas: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    merged = []
    for schema in benchmark_schemas:
        merged.append(dict(schema))
    for schema in memory_schemas:
        merged.append(dict(schema))
    return merged


def run_benchmark_agent(
    task: Mapping[str, Any],
    model: Any,
    backend: InstrumentedMemoryBackend,
    adapter: BenchmarkEnvAdapter,
    *,
    max_steps: int = 30,
    seed: int = 0,
    temperature: float = 0.0,
) -> dict[str, Any]:
    """Run a model agent with merged benchmark + memory tools and return the
    native event recording plus the official evaluator result."""

    if not isinstance(task, Mapping):
        raise ValueError("task must be a mapping")
    task_id = task.get("task_id")
    agent_id = task.get("agent_id", "agent_model")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id must be a non-empty string")

    failure_controller = task.get("failure_controller", task.get("failure_schedule"))
    if failure_controller is not None and not isinstance(failure_controller, FailureController):
        failure_controller = FailureController(failure_controller)
    gateway = BenchmarkToolGateway(backend, adapter, agent_id=agent_id, failure_controller=failure_controller)

    benchmark_schemas = adapter.tool_schemas()
    memory_schemas = NativeMemoryToolGateway.schemas()
    schemas = build_merged_schemas(memory_schemas, benchmark_schemas)

    messages: list[dict[str, Any]] = []
    system_prompt = task.get("system_prompt")
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": adapter.reset(task)})

    for step in range(1, max_steps + 1):
        try:
            response = model.complete(messages, schemas, seed=seed, temperature=temperature)
        except ModelProtocolError as exc:
            return _failed_report(task_id, step, f"model_{exc.code}", backend.validated_events(), messages)
        assistant_message = _assistant_message(response)
        messages.append(assistant_message)
        tool_calls = list(getattr(response, "tool_calls", []))
        if not tool_calls:
            report = {
                "task_id": task_id,
                "status": "completed",
                "steps": step,
                "final_text": getattr(response, "text", ""),
                "events": backend.validated_events(),
                "messages": messages,
            }
            report["official"] = adapter.evaluate(report)
            return report
        for call in tool_calls:
            try:
                if call.name.startswith("memory_"):
                    result = gateway.call(call.name, call.arguments)
                else:
                    result, _metadata = gateway.call_benchmark(call.name, call.arguments, step)
            except AgentToolError as exc:
                return _failed_report(task_id, step, exc.code, backend.validated_events(), messages)
            except FailureInjectionError as exc:
                return _failed_report(task_id, step, exc.code, backend.validated_events(), messages)
            try:
                content = __import__("json").dumps(result, ensure_ascii=False)
            except (TypeError, ValueError) as exc:
                return _failed_report(task_id, step, "non_json_tool_result", backend.validated_events(), messages)
            messages.append({"role": "tool", "tool_call_id": call.call_id, "content": content})
        if step == max_steps:
            report = _failed_report(task_id, step, "max_steps_exceeded", backend.validated_events(), messages)
            report["official"] = adapter.evaluate(report)
            return report
    report = _failed_report(task_id, max_steps, "max_steps_exceeded", backend.validated_events(), messages)
    report["official"] = adapter.evaluate(report)
    return report
