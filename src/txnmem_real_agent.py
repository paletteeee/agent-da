"""Model-agnostic Agent tool loop for native TxnMem memory traces."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_failure_controller import FailureController, FailureInjectionError
from txnmem_model_protocol import ModelProtocolError, add_response_usage, empty_usage_summary
from txnmem_task_transaction import TaskTransactionError, TaskTransactionGateway
from txnmem_transaction_journal import TransactionJournal


class AgentToolError(RuntimeError):
    """A structured memory-tool dispatch error."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class NativeMemoryToolGateway:
    """Expose canonical memory operations as model-callable tools."""

    _METHODS = {
        "memory_read": "read",
        "memory_search": "search",
        "memory_write": "write",
        "memory_derive": "derive",
        "memory_propagate": "propagate",
        "memory_supersede": "supersede",
        "memory_invalidate": "invalidate",
    }

    @classmethod
    def schemas(cls) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "memory_read",
                    "description": "Read one active memory by id.",
                    "parameters": {
                        "type": "object",
                        "properties": {"memory_id": {"type": "string"}},
                        "required": ["memory_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_search",
                    "description": "Search active memories by a query.",
                    "parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_write",
                    "description": "Write one memory.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "memory_id": {"type": "string"},
                            "value": {},
                        },
                        "required": ["memory_id", "value"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_derive",
                    "description": "Derive a memory from explicitly named source memories.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "memory_id": {"type": "string"},
                            "source_ids": {"type": "array", "items": {"type": "string"}},
                            "value": {},
                        },
                        "required": ["memory_id", "source_ids", "value"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_propagate",
                    "description": "Propagate a source memory to a target memory.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "memory_id": {"type": "string"},
                            "source_id": {"type": "string"},
                            "value": {},
                        },
                        "required": ["memory_id", "source_id", "value"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_supersede",
                    "description": "Create a new memory that supersedes an old memory.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "old_memory_id": {"type": "string"},
                            "new_memory_id": {"type": "string"},
                            "value": {},
                        },
                        "required": ["old_memory_id", "new_memory_id", "value"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "memory_invalidate",
                    "description": "Invalidate one memory.",
                    "parameters": {
                        "type": "object",
                        "properties": {"memory_id": {"type": "string"}},
                        "required": ["memory_id"],
                    },
                },
            },
        ]

    def __init__(
        self,
        backend: InstrumentedMemoryBackend,
        agent_id: str = "agent_model",
        failure_controller: FailureController | None = None,
    ):
        self.backend = backend
        self.agent_id = agent_id
        self.failure_controller = failure_controller
        self.revoked_actions: set[str] = set()

    def revoke_policy(self, action: str, *, trigger_event: Mapping[str, Any]) -> None:
        self.revoked_actions.add(action)
        self.backend.record_control_event(
            "policy_revoke",
            action=action,
            policy_version=trigger_event.get("step", 1) + 1,
            trigger_event_id=trigger_event.get("event_id"),
            agent_id=self.agent_id,
            projection="failure_injection",
        )

    def call(self, name: str, arguments: Mapping[str, Any]) -> Any:
        if name not in self._METHODS:
            raise AgentToolError("unknown_tool", f"unsupported memory tool: {name}")
        if not isinstance(arguments, Mapping):
            raise AgentToolError("invalid_tool_arguments", "tool arguments must be an object")
        operation = dict(arguments)
        operation.setdefault("agent_id", self.agent_id)
        operation.setdefault("projection", "real_model_native")
        method_name = self._METHODS[name]
        required_policy = {
            "memory_read": "read",
            "memory_search": "read",
            "memory_write": "write",
            "memory_derive": "write",
            "memory_propagate": "write",
            "memory_supersede": "write",
            "memory_invalidate": "write",
        }[name]
        if required_policy in self.revoked_actions:
            raise AgentToolError("policy_denied", f"policy revoked for {required_policy}")
        try:
            result = getattr(self.backend, method_name)(**operation)
        except (KeyError, TypeError, ValueError) as exc:
            raise AgentToolError("invalid_tool_arguments", f"invalid arguments for {name}") from exc
        if self.failure_controller and self.backend.events:
            try:
                self.failure_controller.observe(
                    self.backend.events[-1], backend=self.backend, gateway=self
                )
            except FailureInjectionError as exc:
                raise AgentToolError(exc.code, str(exc)) from exc
        if name == "memory_invalidate":
            return {"ok": True, "memory_id": operation.get("memory_id")}
        return copy.deepcopy(result)


def _assistant_message(response: Any) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": getattr(response, "text", "")}
    calls = []
    for call in getattr(response, "tool_calls", []):
        calls.append(
            {
                "id": call.call_id,
                "type": "function",
                "function": {"name": call.name, "arguments": json.dumps(call.arguments, ensure_ascii=False)},
            }
        )
    if calls:
        message["tool_calls"] = calls
    return message


def _failed_report(
    task_id: str,
    steps: int,
    code: str,
    events: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    model_usage: Mapping[str, Any] | None = None,
    final_text: str | None = None,
) -> dict[str, Any]:
    report = {
        "task_id": task_id,
        "status": "failed",
        "failure_code": code,
        "steps": steps,
        "events": events,
        "messages": messages,
        "model_usage": dict(model_usage or empty_usage_summary()),
    }
    if final_text is not None:
        report["final_text"] = final_text
    return report


def _transaction_summary(journal: TransactionJournal, txn_id: str) -> dict[str, Any]:
    record = journal.load(txn_id)
    phase_order = (
        "prepare_recorded",
        "qdrant_staged",
        "neo4j_staged",
        "stage_verified",
        "commit_decided",
        "finalize_complete",
        "abort_decided",
        "cleanup_complete",
    )
    recorded_phases = {item["phase"] for item in journal.phases(txn_id)}
    return {
        "txn_id": txn_id,
        "state": record.state.lower(),
        "decision": record.decision.lower() if record.decision is not None else None,
        "phases": [phase for phase in phase_order if phase in recorded_phases],
        "intent_count": len(journal.intents(txn_id)),
        "read_set_count": len(journal.read_set(txn_id)),
        "state_digest": journal.state_digest(txn_id),
    }


def run_real_agent(
    task: Mapping[str, Any],
    model: Any,
    backend: InstrumentedMemoryBackend,
    *,
    max_steps: int = 12,
    seed: int = 0,
    temperature: float = 0.0,
    transaction_mode: str = "direct",
    transaction_journal_path: str | Path | None = None,
    transaction_id: str | None = None,
    policy_snapshot_provider: Callable[[], Mapping[str, Any]] | None = None,
    transaction_phase_hook: Callable[[str, Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run a structured model tool loop and return a native event recording."""

    if not isinstance(task, Mapping):
        raise ValueError("task must be a mapping")
    task_id = task.get("task_id")
    prompt = task.get("prompt")
    agent_id = task.get("agent_id", "agent_model")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id must be a non-empty string")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("prompt must be a non-empty string")
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise ValueError("agent_id must be a non-empty string")
    if max_steps < 1:
        raise ValueError("max_steps must be positive")
    if transaction_mode not in {"direct", "task"}:
        raise ValueError("transaction_mode must be 'direct' or 'task'")
    if transaction_mode == "task" and transaction_journal_path is None:
        raise ValueError("transaction_journal_path is required in task mode")

    failure_controller = task.get("failure_controller", task.get("failure_schedule"))
    if failure_controller is not None and not isinstance(failure_controller, FailureController):
        failure_controller = FailureController(failure_controller)
    journal: TransactionJournal | None = None
    txn_id: str | None = None
    gateway: NativeMemoryToolGateway | TaskTransactionGateway

    def phase_hook(phase: str, evidence: Mapping[str, Any]) -> None:
        if transaction_phase_hook is not None:
            transaction_phase_hook(phase, evidence)
        if failure_controller is not None:
            try:
                failure_controller.observe_phase(
                    phase,
                    evidence,
                    backend=backend,
                    gateway=gateway,
                )
            except FailureInjectionError as exc:
                if transaction_mode == "task":
                    raise TaskTransactionError(exc.code, str(exc)) from exc
                raise AgentToolError(exc.code, str(exc)) from exc

    if transaction_mode == "task":
        journal_path = Path(transaction_journal_path)
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        journal = TransactionJournal(journal_path)
        txn_id = transaction_id or f"txn_{task_id}"
        gateway = TaskTransactionGateway(
            journal=journal,
            backend=backend,
            task_id=task_id,
            agent_id=agent_id,
            txn_id=txn_id,
            policy_snapshot_provider=policy_snapshot_provider
            or (lambda: {"version": 1, "denied_actions": [], "scope_overrides": {}}),
            phase_hook=phase_hook,
        )
    else:
        gateway = NativeMemoryToolGateway(
            backend,
            agent_id=agent_id,
            failure_controller=failure_controller,
        )

    def current_events() -> list[dict[str, Any]]:
        if transaction_mode == "task":
            return gateway.validated_events()
        return backend.validated_events()

    def failed(step: int, code: str, final_text: str | None = None) -> dict[str, Any]:
        if transaction_mode == "task":
            gateway.abort(code)
        report = _failed_report(
            task_id,
            step,
            code,
            current_events(),
            messages,
            model_usage,
            final_text,
        )
        if journal is not None and txn_id is not None:
            report["transaction"] = _transaction_summary(journal, txn_id)
        return report

    messages: list[dict[str, Any]] = []
    system_prompt = task.get("system_prompt")
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    model_usage = empty_usage_summary()
    mutation_count = 0
    try:
        for step in range(1, max_steps + 1):
            model_usage["request_count"] += 1
            try:
                response = model.complete(
                    messages,
                    NativeMemoryToolGateway.schemas(),
                    seed=seed,
                    temperature=temperature,
                )
            except ModelProtocolError as exc:
                return failed(step, f"model_{exc.code}")
            add_response_usage(model_usage, response)
            assistant_message = _assistant_message(response)
            messages.append(assistant_message)
            tool_calls = list(getattr(response, "tool_calls", []))
            if not tool_calls:
                final_text = getattr(response, "text", "")
                if transaction_mode == "task":
                    try:
                        gateway.commit()
                    except TaskTransactionError as exc:
                        return failed(step, exc.code, final_text)
                report = {
                    "task_id": task_id,
                    "status": "completed",
                    "steps": step,
                    "final_text": final_text,
                    "events": current_events(),
                    "messages": messages,
                    "model_usage": model_usage,
                }
                if journal is not None and txn_id is not None:
                    report["transaction"] = _transaction_summary(journal, txn_id)
                return report
            for call in tool_calls:
                try:
                    result = gateway.call(call.name, call.arguments)
                    if call.name in {
                        "memory_write",
                        "memory_derive",
                        "memory_propagate",
                        "memory_supersede",
                        "memory_invalidate",
                    }:
                        mutation_count += 1
                        phase_hook(
                            "after_mutation",
                            {"mutation_count": mutation_count, "tool_name": call.name},
                        )
                except (AgentToolError, TaskTransactionError) as exc:
                    return failed(step, exc.code)
                try:
                    content = json.dumps(result, ensure_ascii=False)
                except (TypeError, ValueError):
                    return failed(step, "non_json_tool_result")
                messages.append(
                    {"role": "tool", "tool_call_id": call.call_id, "content": content}
                )
            if step == max_steps:
                return failed(step, "max_steps_exceeded")
        return failed(max_steps, "max_steps_exceeded")
    finally:
        if journal is not None:
            journal.close()
