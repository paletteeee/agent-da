"""Injectable memory backend and deterministic Agent replay harness."""

from __future__ import annotations

import copy
from typing import Any, Callable, Iterable

from txnmem_event_contract import validate_events
from txnmem_trace import trace_to_instance


class InstrumentedMemoryBackend:
    """Small backend used to record actual memory events from an Agent.

    A production connector can implement the same methods and retain the
    event contract.  The benchmark then consumes recorded events rather than
    manufacturing provenance edges after the fact.
    """

    def __init__(self, memories: dict[str, dict[str, Any]] | None = None):
        self.memories = copy.deepcopy(memories or {})
        self.events: list[dict[str, Any]] = []

    def _event(self, kind: str, **fields: Any) -> dict[str, Any]:
        event = {
            "event_id": f"backend_event_{len(self.events) + 1:04d}",
            "kind": kind,
            "step": len(self.events) + 1,
            "agent_id": fields.get("agent_id", "agent_1"),
        }
        event.update({key: value for key, value in fields.items() if value is not None})
        self.events.append(event)
        return event

    def write(self, memory_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        memory = {
            "memory_id": memory_id,
            "value": value if value is not None else memory_id,
            "status": "active",
            "agent_id": fields.get("agent_id", "agent_1"),
            "scope": fields.get("scope", "tenant:user_001"),
            "derived_from": list(fields.get("source_ids", [])),
        }
        self.memories[memory_id] = memory
        self._event("memory_write", memory_id=memory_id, value=memory["value"], **fields)
        return copy.deepcopy(memory)

    def read(self, memory_id: str | None = None, **fields: Any) -> dict[str, Any] | None:
        memory = self.memories.get(memory_id) if memory_id is not None else None
        self._event("memory_read", memory_id=memory_id, **fields)
        return copy.deepcopy(memory) if memory and memory.get("status") == "active" else None

    def search(self, query: str | None = None, **fields: Any) -> list[dict[str, Any]]:
        matches = [
            copy.deepcopy(memory)
            for memory in self.memories.values()
            if memory.get("status") == "active"
            and (query is None or query in {memory.get("memory_id"), memory.get("value"), memory.get("attribute")})
        ]
        self._event("memory_search", query=query, **fields)
        return matches

    def derive(self, memory_id: str, source_ids: Iterable[str], value: Any = None, **fields: Any) -> dict[str, Any]:
        source_ids = list(source_ids)
        if any(source_id not in self.memories for source_id in source_ids):
            raise KeyError("derive source is missing")
        memory = self.write(memory_id, value=value, source_ids=source_ids, **fields)
        # Keep one canonical derive event for provenance; the write event is
        # still useful to a backend audit log, so do not remove it.
        self.events[-1]["kind"] = "memory_derive"
        self.events[-1]["source_ids"] = source_ids
        return memory

    def propagate(self, memory_id: str, source_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        memory = self.write(memory_id, value=value, source_ids=[source_id], **fields)
        self.events[-1]["kind"] = "memory_propagate"
        self.events[-1]["source_id"] = source_id
        return memory

    def supersede(self, old_memory_id: str, new_memory_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        if old_memory_id not in self.memories:
            raise KeyError(old_memory_id)
        memory = self.write(new_memory_id, value=value, supersedes_id=old_memory_id, **fields)
        self.memories[old_memory_id]["status"] = "superseded"
        self._event(
            "memory_supersede",
            old_memory_id=old_memory_id,
            new_memory_id=new_memory_id,
            **fields,
        )
        return memory

    def invalidate(self, memory_id: str, **fields: Any) -> None:
        if memory_id in self.memories:
            self.memories[memory_id]["status"] = "invalid"
        self._event("invalidate", memory_id=memory_id, **fields)

    def snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self.memories)

    def validated_events(self) -> list[dict[str, Any]]:
        """Validate and return a JSON-safe copy of the native event log."""

        return validate_events(self.events)


class AgentReplayRunner:
    """Run explicit Agent actions against an injectable backend."""

    def __init__(self, backend: InstrumentedMemoryBackend):
        self.backend = backend

    def run(self, actions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        for action in actions:
            if not isinstance(action, dict):
                raise ValueError("Agent actions must be mappings")
            operation = dict(action)
            operation_type = str(operation.pop("type", operation.pop("kind", "")))
            if operation_type in {"begin_txn", "commit"}:
                continue
            if not hasattr(self.backend, operation_type):
                raise ValueError(f"unsupported Agent action: {operation_type}")
            getattr(self.backend, operation_type)(**operation)
        return copy.deepcopy(self.backend.events)

    def run_agent(self, agent: Callable[[InstrumentedMemoryBackend], Iterable[dict[str, Any]]]) -> list[dict[str, Any]]:
        return self.run(agent(self.backend))

    def to_instance(self, instance_id: str, seed: int = 0) -> dict[str, Any]:
        return trace_to_instance(self.backend.validated_events(), instance_id, seed=seed)
