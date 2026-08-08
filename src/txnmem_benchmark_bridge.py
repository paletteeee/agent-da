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
import ast
import json
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Callable

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_failure_controller import FailureController, FailureInjectionError
from txnmem_model_protocol import ModelProtocolError, add_response_usage, empty_usage_summary
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


PROMPT_PROFILES = ("baseline", "tuned")
APPWORLD_TOOL_STRATEGIES = ("instruction_inferred", "manifest_scoped", "all_public")
_TRUSTED_APPWORLD_PREFLIGHT_TOOLS = frozenset(
    {
        "supervisor__show_profile",
        "supervisor__show_account_passwords",
    }
)

_APPWORLD_KEYWORDS = {
    "amazon": ("amazon", "shopping cart", "wish list", "wishlist", "product order"),
    "file_system": ("file system", "folder", "directory", "rename file", "move file"),
    "gmail": ("gmail", "email", "e-mail", "inbox"),
    "phone": (
        "phone",
        "contact",
        "sms",
        "text message",
        "call log",
        "friend",
        "roommate",
        "coworker",
        "colleague",
        "mother",
        "father",
        "sister",
        "brother",
    ),
    "simple_note": ("simple note", "notes app", "note app"),
    "spotify": ("spotify", "playlist", "song", "album", "artist", "music"),
    "splitwise": ("splitwise", "shared expense", "split expense", "owe "),
    "todoist": ("todoist", "to-do", "todo list", "task list", "reminder"),
    "venmo": ("venmo", "request money", "send money", "payment request"),
}


def infer_appworld_app_names(
    instruction: str,
    supplied_app_names: Sequence[str] | None = None,
) -> list[str]:
    """Select task apps from public instruction text for AppWorld tool resolution."""

    lowered = str(instruction).lower()
    selected = {
        app_name
        for app_name, keywords in _APPWORLD_KEYWORDS.items()
        if any(keyword in lowered for keyword in keywords)
    }
    selected.update(
        str(app_name)
        for app_name in (supplied_app_names or ())
        if str(app_name) not in {"admin", "supervisor", "api_docs"}
    )
    if not selected:
        selected.update(_APPWORLD_KEYWORDS)
    return [*sorted(selected), "supervisor"]


def resolve_appworld_app_names(
    strategy: str,
    instruction: str,
    supplied_app_names: Sequence[str] | None,
) -> list[str] | None:
    """Resolve one shared AppWorld tool-exposure strategy."""

    if strategy == "instruction_inferred":
        return infer_appworld_app_names(instruction, supplied_app_names)
    if strategy == "manifest_scoped":
        return list(supplied_app_names) if supplied_app_names is not None else None
    if strategy == "all_public":
        return None
    raise ValueError(f"unsupported AppWorld tool strategy: {strategy}")


def appworld_tool_allowed(
    tool_name: str,
    api_name_allowlist: Sequence[str] | None,
    *,
    always_allow_supervisor: bool = False,
) -> bool:
    """Apply a task allowlist without accidentally hiding supervisor tools."""

    if api_name_allowlist is None:
        return True
    if tool_name in api_name_allowlist:
        return True
    return always_allow_supervisor and tool_name.startswith("supervisor__")


def adapt_appworld_arguments(
    arguments: Mapping[str, Any],
    allowed_properties: Sequence[str] | set[str],
) -> dict[str, Any]:
    """Drop hallucinated AppWorld arguments that are absent from a tool schema."""

    allowed = {str(name) for name in allowed_properties}
    return {str(name): value for name, value in arguments.items() if str(name) in allowed}


def _parse_appworld_observation(observation: str) -> Any:
    """Parse AppWorld's JSON or Python-repr response without executing content."""

    try:
        return json.loads(observation)
    except (TypeError, ValueError):
        try:
            return ast.literal_eval(observation)
        except (SyntaxError, ValueError):
            return None


def _appworld_response_ok(response: Any) -> bool:
    """Normalize AppWorld success/error envelopes without trusting display text."""

    parsed = _parse_appworld_observation(response) if isinstance(response, str) else response
    if isinstance(parsed, Mapping):
        for field in ("ok", "success"):
            if field in parsed and parsed[field] is False:
                return False
        status = str(parsed.get("status", "")).strip().lower()
        if status in {"error", "failed", "failure"}:
            return False
        if any(parsed.get(field) for field in ("error", "error_message")):
            return False
    if isinstance(response, str) and response.lstrip().lower().startswith("error:"):
        return False
    return True


def _appworld_schema_index(
    schemas: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    for schema in schemas:
        function = schema.get("function") if isinstance(schema, Mapping) else None
        if not isinstance(function, Mapping):
            continue
        name = function.get("name")
        if isinstance(name, str) and name:
            index[name] = function
    return index


def _appworld_prelogin_arguments(
    profile: Any,
    account_passwords: Any,
    schema_index: Mapping[str, Mapping[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    if not isinstance(profile, Mapping) or not isinstance(account_passwords, list):
        return []
    passwords = {
        str(item.get("account_name", "")).strip().lower().replace("-", "_").replace(" ", "_"): item.get("password")
        for item in account_passwords
        if isinstance(item, Mapping) and item.get("password") is not None
    }
    plan: list[tuple[str, dict[str, Any]]] = []
    for tool_name in sorted(schema_index):
        if not tool_name.endswith("__login") or tool_name.startswith("supervisor__"):
            continue
        app_name = tool_name.split("__", 1)[0]
        password = passwords.get(app_name.lower())
        function = schema_index[tool_name]
        parameters = function.get("parameters")
        properties = parameters.get("properties", {}) if isinstance(parameters, Mapping) else {}
        username_schema = properties.get("username", {}) if isinstance(properties, Mapping) else {}
        description = str(username_schema.get("description", "")).lower() if isinstance(username_schema, Mapping) else ""
        profile_key = "phone_number" if "phone" in description else "email"
        username = profile.get(profile_key)
        if password is None or username is None:
            continue
        plan.append(
            (
                tool_name,
                {"username": str(username), "password": str(password)},
            )
        )
    return plan


_APPWORLD_RELATION_PATTERNS = (
    (r"friends?", "friend"),
    (r"roommates?", "roommate"),
    (r"coworkers?", "coworker"),
    (r"colleagues?", "colleague"),
    (r"siblings?", "sibling"),
    (r"brothers?", "brother"),
    (r"sisters?", "sister"),
    (r"mothers?", "mother"),
    (r"fathers?", "father"),
)


def _appworld_contact_lookup_arguments(
    instruction: str,
    phone_login: Any,
    schema_index: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    tool_name = "phone__search_contacts"
    function = schema_index.get(tool_name)
    if not isinstance(function, Mapping) or not isinstance(phone_login, Mapping):
        return None
    access_token = phone_login.get("access_token")
    if not access_token:
        return None
    relation = None
    query = None
    for pattern, normalized in _APPWORLD_RELATION_PATTERNS:
        if not re.search(rf"\b{pattern}\b", instruction, flags=re.IGNORECASE):
            continue
        relation = normalized
        named = re.search(
            rf"\b{pattern}\b\s*,?\s*(?:named\s+)?([A-Z][A-Za-z'\-]+)",
            instruction,
        )
        if named:
            query = named.group(1)
        break
    if relation is None and not re.search(r"\bcontacts?\b", instruction, flags=re.IGNORECASE):
        return None
    arguments: dict[str, Any] = {
        "access_token": str(access_token),
        "page_index": 0,
        "page_limit": 20,
    }
    if query:
        arguments["query"] = query
    if relation:
        arguments["relationship"] = relation
    parameters = function.get("parameters")
    properties = parameters.get("properties") if isinstance(parameters, Mapping) else None
    return adapt_appworld_arguments(arguments, properties.keys()) if isinstance(properties, Mapping) else arguments


def build_benchmark_system_prompt(
    task: Mapping[str, Any],
    *,
    dataset: str,
    prompt_profile: str,
) -> str:
    """Build an explicit, auditable benchmark-agent prompting condition."""

    if prompt_profile not in PROMPT_PROFILES:
        raise ValueError(f"unsupported prompt profile: {prompt_profile}")
    original = str(task.get("system_prompt", "")).strip()
    if prompt_profile == "baseline":
        return original
    if dataset == "appworld":
        tuned = (
            "You are an autonomous AppWorld tool agent. Never guess credentials, access tokens, "
            "IDs, email addresses, or other arguments. Use the internally prepared read-only "
            "Supervisor profile, target-app login, and contact observations, and reuse each returned "
            "access token. Never request or expose account passwords. Resolve every reference to a friend, relative, or "
            "other person through Phone contacts and use the exact returned identity. Translate "
            "privacy language literally: publicly means private=false and privately means "
            "private=true. Plan only the requested state transitions, follow the exact function "
            "schema, inspect every observation, and never repeat a successful state-changing call. "
            "Verify the final state with read/search functions and then call "
            "supervisor__complete_task with status='success'; for action tasks that ask no question, "
            "use answer=null. Memory tools may retain intermediate facts but do not change AppWorld "
            "state."
        )
    else:
        tuned = (
            "Use the listed benchmark tools deliberately: plan first, follow each exact function "
            "schema, inspect errors, and verify the requested final state before finishing. Memory "
            "tools retain intermediate facts but do not replace benchmark actions."
        )
    return "\n\n".join(part for part in (original, tuned) if part)


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
        api_name_allowlist: Sequence[str] | None = None,
        experiment_name: str = "txnmem_native",
        always_allow_supervisor: bool = False,
    ):
        self.requester_factory = requester_factory
        self.appworld_root = _normalize_appworld_root(appworld_root) if appworld_root else None
        self.app_names = tuple(str(name) for name in app_names) if app_names is not None else None
        self.api_name_allowlist = (
            tuple(str(name) for name in api_name_allowlist)
            if api_name_allowlist is not None
            else None
        )
        self.experiment_name = experiment_name
        self.always_allow_supervisor = bool(always_allow_supervisor)
        self.requester = None
        self.environment = None
        self.api_docs: list[dict[str, Any]] = []
        self._tool_kinds: dict[str, str] = {}
        self._authorized_tool_names: set[str] = set()
        self.unauthorized_tool_attempt_count = 0

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
        self._tool_kinds.clear()
        self._authorized_tool_names.clear()
        for doc in self.api_docs:
            function = doc.get("function", {}) if isinstance(doc, Mapping) else {}
            tool_name = function.get("name", "")
            if not isinstance(tool_name, str) or not tool_name:
                continue
            method_name = tool_name.split("__")[-1] if "__" in tool_name else tool_name
            if not appworld_tool_allowed(
                tool_name,
                self.api_name_allowlist,
                always_allow_supervisor=self.always_allow_supervisor,
            ):
                continue
            self._tool_kinds[tool_name] = _classify_tool_name(
                method_name, self._READ_PREFIXES, self._WRITE_PREFIXES
            ) or "memory_read"
            self._authorized_tool_names.add(tool_name)
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
                    raise_on_failure=True,
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
        if name not in self._authorized_tool_names:
            self.unauthorized_tool_attempt_count += 1
            raise AgentToolError(
                "unauthorized_tool",
                f"AppWorld tool was not exposed by the public task schema: {name}",
            )
        return self._request(name, arguments)

    def set_model_authorized_tools(self, names: Sequence[str]) -> None:
        """Restrict runtime execution to the schemas actually sent to the model."""

        self._authorized_tool_names.intersection_update(str(name) for name in names)

    def execute_trusted_preflight(
        self, name: str, arguments: Mapping[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        if name not in _TRUSTED_APPWORLD_PREFLIGHT_TOOLS:
            raise AgentToolError(
                "unauthorized_preflight_tool",
                f"unsupported trusted AppWorld preflight tool: {name}",
            )
        return self._request(name, arguments)

    def _request(self, name: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        if self.requester is None:
            raise AgentToolError("missing_runtime", "AppWorld requester is not initialized")
        app_name, method_name = name.split("__", 1)
        try:
            response = self.requester.request(app_name, method_name, **dict(arguments))
        except Exception as exc:
            return f"Error: {type(exc).__name__}: {exc}", {"ok": False}
        if not _appworld_response_ok(response):
            return f"Error: AppWorld API failure: {response}", {"ok": False}
        return str(response), {"ok": True}

    def evaluate(self, run_report: Mapping[str, Any]) -> dict[str, Any]:
        if self.environment is not None:
            try:
                task_completed = bool(self.environment.task_completed())
                save = getattr(self.environment, "save", None)
                if callable(save):
                    save()
                tracker = self.environment.evaluate(suppress_errors=True)
                evaluator_success = bool(tracker.success)
                result = {
                    "status": "available",
                    "official_evaluator": "appworld.TestTracker.success_and_task_completed",
                    "success": bool(evaluator_success and task_completed),
                    "official_evaluator_success": evaluator_success,
                    "task_completed": task_completed,
                    "pass_count": int(tracker.pass_count),
                    "total_count": int(tracker.num_tests),
                }
            finally:
                self.close()
            self.official_evaluator_status = "available"
            return result
        self.official_evaluator_status = "blocked"
        return {
            "status": "blocked",
            "official_evaluator": "appworld_task_completed_not_available_offline",
            "error": "official AppWorld environment is not initialized",
        }

    def close(self) -> None:
        if self.environment is not None:
            self.environment.close()
            self.environment = None

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
            {
                "name": name,
                "arguments": dict(arguments),
                "observation": observation,
                "metadata": dict(metadata),
                "step": step,
            }
        )
        kind = self.adapter.tool_event_kind(name)
        if kind is not None and metadata.get("ok", True):
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

    def call_trusted_preflight(
        self, name: str, arguments: Mapping[str, Any], step: int = 0
    ) -> tuple[str, dict[str, Any]]:
        execute = getattr(self.adapter, "execute_trusted_preflight", None)
        if callable(execute):
            observation, metadata = execute(name, arguments)
        else:
            observation, metadata = self.adapter.execute(name, arguments)
        self.benchmark_calls.append(
            {
                "name": name,
                "arguments": dict(arguments),
                "observation": observation,
                "metadata": dict(metadata),
                "origin": "trusted_preflight",
                "step": step,
            }
        )
        return observation, metadata


def build_merged_schemas(memory_schemas: Sequence[Mapping[str, Any]], benchmark_schemas: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    merged = []
    for schema in benchmark_schemas:
        merged.append(dict(schema))
    for schema in memory_schemas:
        merged.append(dict(schema))
    return merged


def _safe_benchmark_tool_trace(gateway: BenchmarkToolGateway) -> list[dict[str, Any]]:
    """Expose call order and schema-key use without retaining argument values."""

    trace: list[dict[str, Any]] = []
    for call in gateway.benchmark_calls:
        arguments = call.get("arguments")
        metadata = call.get("metadata")
        ok = metadata.get("ok", True) if isinstance(metadata, Mapping) else True
        trace.append(
            {
                "name": str(call.get("name", "")),
                "origin": str(call.get("origin", "model_or_agent_tool")),
                "step": int(call.get("step", 0)),
                "argument_keys": sorted(str(key) for key in arguments) if isinstance(arguments, Mapping) else [],
                "observation_status": "ok" if ok else "error",
            }
        )
    return trace


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

    prompt_profile = str(task.get("prompt_profile", "baseline"))
    if prompt_profile not in PROMPT_PROFILES:
        raise ValueError(f"unsupported prompt profile: {prompt_profile}")
    if prompt_profile == "tuned":
        instruction = adapter.reset(task)
        all_benchmark_schemas = adapter.tool_schemas()
    else:
        all_benchmark_schemas = adapter.tool_schemas()
        instruction = adapter.reset(task)
    trusted_preflight_schemas: list[Mapping[str, Any]] = []
    benchmark_schemas: list[Mapping[str, Any]] = []
    for schema in all_benchmark_schemas:
        function = schema.get("function") if isinstance(schema, Mapping) else None
        name = function.get("name") if isinstance(function, Mapping) else None
        if adapter.dataset == "appworld" and name in _TRUSTED_APPWORLD_PREFLIGHT_TOOLS:
            trusted_preflight_schemas.append(schema)
        else:
            benchmark_schemas.append(schema)
    restrict_tools = getattr(adapter, "set_model_authorized_tools", None)
    if callable(restrict_tools):
        restrict_tools(
            [
                str(schema.get("function", {}).get("name", ""))
                for schema in benchmark_schemas
                if isinstance(schema, Mapping)
            ]
        )
    memory_schemas = NativeMemoryToolGateway.schemas()
    schemas = build_merged_schemas(memory_schemas, benchmark_schemas)

    messages: list[dict[str, Any]] = []
    system_prompt = build_benchmark_system_prompt(
        task,
        dataset=adapter.dataset,
        prompt_profile=prompt_profile,
    )
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": instruction})
    model_usage = empty_usage_summary()
    preflight: list[dict[str, str]] = []
    preflight_call_count = 0
    preflight_login_count = 0
    preflight_contact_lookup_count = 0
    appworld_schema_index = _appworld_schema_index(benchmark_schemas)
    trusted_preflight_schema_index = _appworld_schema_index(trusted_preflight_schemas)
    if prompt_profile == "tuned" and adapter.dataset == "appworld":
        schema_names = set(trusted_preflight_schema_index)
        preflight_values: dict[str, Any] = {}
        for tool_name in (
            "supervisor__show_profile",
            "supervisor__show_account_passwords",
        ):
            if tool_name not in schema_names:
                continue
            try:
                observation, _metadata = gateway.call_trusted_preflight(tool_name, {}, 0)
            except AgentToolError:
                continue
            preflight_call_count += 1
            if tool_name != "supervisor__show_account_passwords":
                preflight.append({"tool_name": tool_name, "observation": observation})
            preflight_values[tool_name] = _parse_appworld_observation(observation)
        login_values: dict[str, Any] = {}
        for tool_name, arguments in _appworld_prelogin_arguments(
            preflight_values.get("supervisor__show_profile"),
            preflight_values.get("supervisor__show_account_passwords"),
            appworld_schema_index,
        ):
            try:
                observation, _metadata = gateway.call_benchmark(tool_name, arguments, 0)
            except AgentToolError:
                continue
            preflight_call_count += 1
            preflight.append({"tool_name": tool_name, "observation": observation})
            parsed_login = _parse_appworld_observation(observation)
            if isinstance(parsed_login, Mapping) and parsed_login.get("access_token"):
                preflight_login_count += 1
                login_values[tool_name.split("__", 1)[0]] = parsed_login
        contact_arguments = _appworld_contact_lookup_arguments(
            instruction,
            login_values.get("phone"),
            appworld_schema_index,
        )
        if contact_arguments is not None:
            try:
                observation, _metadata = gateway.call_benchmark(
                    "phone__search_contacts", contact_arguments, 0
                )
            except AgentToolError:
                pass
            else:
                preflight_call_count += 1
                preflight.append(
                    {"tool_name": "phone__search_contacts", "observation": observation}
                )
                if not str(observation).startswith("Error:"):
                    preflight_contact_lookup_count += 1
        if preflight:
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Read-only preflight observations from official AppWorld Supervisor APIs. "
                        "Credentials were used internally and are intentionally omitted. Use these exact "
                        "profile/login/contact values; do not request account passwords or repeat the same "
                        "preflight calls.\n"
                        + json.dumps(preflight, ensure_ascii=False)
                    ),
                }
            )

    def failed(step: int, code: str) -> dict[str, Any]:
        report = _failed_report(
            task_id,
            step,
            code,
            backend.validated_events(),
            messages,
            model_usage,
        )
        report["prompt_profile"] = prompt_profile
        report["preflight_tool_count"] = preflight_call_count
        report["preflight_login_count"] = preflight_login_count
        report["preflight_contact_lookup_count"] = preflight_contact_lookup_count
        report["authorized_benchmark_tool_count"] = len(benchmark_schemas)
        report["unauthorized_tool_attempt_count"] = int(
            getattr(adapter, "unauthorized_tool_attempt_count", 0) or 0
        )
        report["benchmark_tool_trace"] = _safe_benchmark_tool_trace(gateway)
        return finalize(report)

    def finalize(report: dict[str, Any]) -> dict[str, Any]:
        try:
            report["official"] = adapter.evaluate(report)
        except Exception as exc:
            report["official"] = {
                "status": "error",
                "official_evaluator": f"{adapter.dataset}_evaluator",
                "error": f"{type(exc).__name__}: {exc}",
            }
        return report

    def completed(step: int, final_text: str) -> dict[str, Any]:
        report = {
            "task_id": task_id,
            "status": "completed",
            "steps": step,
            "final_text": final_text,
            "events": backend.validated_events(),
            "messages": messages,
            "model_usage": model_usage,
            "prompt_profile": prompt_profile,
            "preflight_tool_count": preflight_call_count,
            "preflight_login_count": preflight_login_count,
            "preflight_contact_lookup_count": preflight_contact_lookup_count,
            "authorized_benchmark_tool_count": len(benchmark_schemas),
            "unauthorized_tool_attempt_count": int(
                getattr(adapter, "unauthorized_tool_attempt_count", 0) or 0
            ),
            "benchmark_tool_trace": _safe_benchmark_tool_trace(gateway),
        }
        return finalize(report)

    for step in range(1, max_steps + 1):
        model_usage["request_count"] += 1
        try:
            response = model.complete(messages, schemas, seed=seed, temperature=temperature)
        except ModelProtocolError as exc:
            return failed(step, f"model_{exc.code}")
        add_response_usage(model_usage, response)
        assistant_message = _assistant_message(response)
        messages.append(assistant_message)
        tool_calls = list(getattr(response, "tool_calls", []))
        if not tool_calls:
            if (
                prompt_profile == "tuned"
                and adapter.dataset == "appworld"
                and "supervisor__complete_task" in appworld_schema_index
            ):
                if step == max_steps:
                    return failed(step, "max_steps_exceeded_without_complete_task")
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "No function calls available. Please call at least one function. "
                            "If the requested state change is finished, call "
                            "supervisor__complete_task with status='success' now."
                        ),
                    }
                )
                continue
            return completed(step, getattr(response, "text", ""))
        successful_completion_call = False
        for call in tool_calls:
            try:
                if call.name.startswith("memory_"):
                    result = gateway.call(call.name, call.arguments)
                else:
                    arguments = call.arguments
                    if prompt_profile == "tuned" and adapter.dataset == "appworld":
                        function = appworld_schema_index.get(call.name)
                        parameters = function.get("parameters") if isinstance(function, Mapping) else None
                        properties = parameters.get("properties") if isinstance(parameters, Mapping) else None
                        if isinstance(properties, Mapping):
                            arguments = adapt_appworld_arguments(arguments, properties.keys())
                    result, _metadata = gateway.call_benchmark(call.name, arguments, step)
                    if (
                        call.name == "supervisor__complete_task"
                        and _metadata.get("ok", True)
                        and not str(result).startswith("Error:")
                    ):
                        successful_completion_call = True
            except AgentToolError as exc:
                return failed(step, exc.code)
            except FailureInjectionError as exc:
                return failed(step, exc.code)
            try:
                content = __import__("json").dumps(result, ensure_ascii=False)
            except (TypeError, ValueError) as exc:
                return failed(step, "non_json_tool_result")
            messages.append({"role": "tool", "tool_call_id": call.call_id, "content": content})
        if prompt_profile == "tuned" and adapter.dataset == "appworld" and successful_completion_call:
            return completed(step, getattr(response, "text", ""))
        if step == max_steps:
            return failed(step, "max_steps_exceeded")
    return failed(max_steps, "max_steps_exceeded")
