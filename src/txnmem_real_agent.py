"""Model-agnostic Agent tool loop for native TxnMem memory traces."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_model_protocol import ModelProtocolError


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

    def __init__(self, backend: InstrumentedMemoryBackend, agent_id: str = "agent_model"):
        self.backend = backend
        self.agent_id = agent_id

    def call(self, name: str, arguments: Mapping[str, Any]) -> Any:
        if name not in self._METHODS:
            raise AgentToolError("unknown_tool", f"unsupported memory tool: {name}")
        if not isinstance(arguments, Mapping):
            raise AgentToolError("invalid_tool_arguments", "tool arguments must be an object")
        operation = dict(arguments)
        operation.setdefault("agent_id", self.agent_id)
        operation.setdefault("projection", "real_model_native")
        method_name = self._METHODS[name]
        try:
            result = getattr(self.backend, method_name)(**operation)
        except (KeyError, TypeError, ValueError) as exc:
            raise AgentToolError("invalid_tool_arguments", f"invalid arguments for {name}") from exc
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


def _failed_report(task_id: str, steps: int, code: str, events: list[dict[str, Any]], messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "status": "failed",
        "failure_code": code,
        "steps": steps,
        "events": events,
        "messages": messages,
    }


def run_real_agent(
    task: Mapping[str, Any],
    model: Any,
    backend: InstrumentedMemoryBackend,
    *,
    max_steps: int = 12,
    seed: int = 0,
    temperature: float = 0.0,
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

    gateway = NativeMemoryToolGateway(backend, agent_id=agent_id)
    messages: list[dict[str, Any]] = []
    system_prompt = task.get("system_prompt")
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    for step in range(1, max_steps + 1):
        try:
            response = model.complete(
                messages,
                NativeMemoryToolGateway.schemas(),
                seed=seed,
                temperature=temperature,
            )
        except ModelProtocolError as exc:
            return _failed_report(task_id, step, f"model_{exc.code}", backend.validated_events(), messages)
        assistant_message = _assistant_message(response)
        messages.append(assistant_message)
        tool_calls = list(getattr(response, "tool_calls", []))
        if not tool_calls:
            return {
                "task_id": task_id,
                "status": "completed",
                "steps": step,
                "final_text": getattr(response, "text", ""),
                "events": backend.validated_events(),
                "messages": messages,
            }
        for call in tool_calls:
            try:
                result = gateway.call(call.name, call.arguments)
            except AgentToolError as exc:
                return _failed_report(task_id, step, exc.code, backend.validated_events(), messages)
            try:
                content = json.dumps(result, ensure_ascii=False)
            except (TypeError, ValueError) as exc:
                return _failed_report(task_id, step, "non_json_tool_result", backend.validated_events(), messages)
            messages.append({"role": "tool", "tool_call_id": call.call_id, "content": content})
        if step == max_steps:
            return _failed_report(task_id, step, "max_steps_exceeded", backend.validated_events(), messages)
    return _failed_report(task_id, max_steps, "max_steps_exceeded", backend.validated_events(), messages)
