"""Native LangGraph Store replay adapter for TxnMemBench instances.

The adapter deliberately maps only individual Store operations.  It does not
add a transaction log, commit-time authorization check, or provenance walk.
"""

from __future__ import annotations

import copy
from collections import defaultdict
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any

from txnmem_adapter_contract import (
    CapabilitySupport,
    MemoryAdapter,
    ReplayObservation,
    RuntimeAdapterError,
    UnsupportedMappingError,
)
from txnmem_schema import validate_instance


def langgraph_capabilities() -> tuple[CapabilitySupport, ...]:
    """Return stable capability rows for the external-baseline runner.

    A tuple is returned instead of one row because the runner needs to report
    both available native CRUD and each unavailable benchmark semantic.
    """

    return (
        CapabilitySupport(
            "single_record_read_write",
            True,
            "Uses native Store.put, get, and search operations.",
        ),
        CapabilitySupport(
            "atomic_multi_record_commit",
            False,
            "Individual Store.put calls have no shared transaction boundary.",
        ),
        CapabilitySupport(
            "commit_policy_revalidation",
            False,
            "Commit is only a trace marker; the adapter does not revalidate policy.",
        ),
        CapabilitySupport(
            "shared_scope_isolation",
            True,
            "Run, instance, agent, and shared scope form the native namespace.",
        ),
        CapabilitySupport(
            "version_supersession",
            True,
            "Supersession uses two ordered Store.put updates; it is not atomic.",
        ),
        CapabilitySupport(
            "provenance_propagation",
            False,
            "LangGraph Store has no native provenance-propagation operation.",
        ),
        CapabilitySupport(
            "recursive_provenance_invalidation",
            False,
            "Invalidation changes only the named record; descendants are not traversed.",
        ),
        CapabilitySupport(
            "crash_recovery",
            False,
            "In-memory fallback excludes crash workloads; persistent mapping also does not claim recovery semantics.",
        ),
    )


def _memory_from_operation(operation: dict[str, Any]) -> dict[str, Any]:
    """Build the normalized record that is the native Store value."""

    return {
        "memory_id": operation["memory_id"],
        "agent_id": operation.get("agent_id", "agent_1"),
        "scope": operation.get("scope", "tenant:user_001"),
        "entity_id": operation.get("entity_id", "user_001"),
        "attribute": operation.get("attribute", "fact"),
        "value": operation.get("value", operation["memory_id"]),
        "status": "active",
        "policy_version": operation.get("policy_version", 1),
        "supersedes_id": operation.get("supersedes_id"),
        "derived_from": list(operation.get("source_ids", [])),
    }


def _matches(memory: dict[str, Any], operation: dict[str, Any]) -> bool:
    query = operation.get("query")
    return query is None or query in {
        memory.get("value"),
        memory.get("memory_id"),
        memory.get("attribute"),
    }


class LangGraphStoreAdapter(MemoryAdapter):
    """Replay one instance using the official synchronous LangGraph Store API."""

    capabilities = langgraph_capabilities()

    def __init__(
        self,
        store_factory: Callable[[], Any],
        *,
        experiment_run_id: str = "txnmembench",
        persistent_store: bool = False,
    ) -> None:
        self._store_factory = store_factory
        self.experiment_run_id = experiment_run_id
        self.persistent_store = persistent_store

    def _namespace(self, instance_id: str, agent_id: str, shared_scope: str) -> tuple[str, ...]:
        return (self.experiment_run_id, instance_id, agent_id, shared_scope)

    @staticmethod
    def _item_value(item: Any, *, instance_id: str, operation_id: str) -> dict[str, Any] | None:
        if item is None:
            return None
        value = getattr(item, "value", None)
        if not isinstance(value, dict):
            raise RuntimeAdapterError(
                "LangGraph Store returned a non-dictionary value "
                f"for instance {instance_id}, operation {operation_id}"
            )
        return copy.deepcopy(value)

    @staticmethod
    def _has_store_methods(store: Any) -> bool:
        return all(callable(getattr(store, method, None)) for method in ("put", "get", "search"))

    @classmethod
    def _validate_store(cls, store: Any) -> None:
        if not cls._has_store_methods(store):
            raise TypeError("LangGraph Store must provide callable put, get, and search methods")

    @staticmethod
    def _store_call(instance_id: str, operation_id: str, call: Callable[[], Any]) -> Any:
        try:
            return call()
        except (RuntimeAdapterError, TypeError, AttributeError, KeyError):
            raise
        except Exception as exc:
            raise RuntimeAdapterError(
                f"LangGraph Store call failed for instance {instance_id}, operation {operation_id}"
            ) from exc

    def run(self, instance: dict[str, Any]) -> ReplayObservation:
        """Replay only operations that have a native single-record mapping."""

        validate_instance(instance)
        instance_id = instance["instance_id"]
        try:
            resource = self._store_factory()
        except Exception as exc:
            raise RuntimeAdapterError(
                f"LangGraph Store initialization failed for instance {instance_id}"
            ) from exc

        if self._has_store_methods(resource):
            store_context = nullcontext(resource)
        elif callable(getattr(resource, "__enter__", None)) and callable(
            getattr(resource, "__exit__", None)
        ):
            store_context = resource
        else:
            self._validate_store(resource)
            raise AssertionError("unreachable")

        try:
            with store_context as store:
                self._validate_store(store)
                return self._run_with_store(store, instance)
        except (RuntimeAdapterError, UnsupportedMappingError, TypeError, AttributeError, KeyError):
            raise
        except Exception as exc:
            raise RuntimeAdapterError(
                f"LangGraph Store initialization or lifecycle failed for instance {instance_id}"
            ) from exc

    def _run_with_store(self, store: Any, instance: dict[str, Any]) -> ReplayObservation:
        """Run against an initialized Store whose lifecycle is owned by ``run``."""

        instance_id = instance["instance_id"]
        locations: dict[str, tuple[str, ...]] = {}
        known_memories: dict[str, dict[str, Any]] = {}
        committed_memory_ids: list[str] = []
        trace: list[dict[str, Any]] = []
        exposed_memory_ids: list[str] = []
        denied_reads = 0
        supersession_updates = 0
        current_policy_version = 1
        transaction_state = "active"

        def put(memory: dict[str, Any], operation_id: str) -> tuple[str, ...]:
            namespace = self._namespace(instance_id, memory["agent_id"], memory["scope"])
            value = copy.deepcopy(memory)
            self._store_call(
                instance_id,
                operation_id,
                lambda: store.put(namespace, memory["memory_id"], value, index=None),
            )
            locations[memory["memory_id"]] = namespace
            known_memories[memory["memory_id"]] = copy.deepcopy(value)
            return namespace

        def get(namespace: tuple[str, ...], memory_id: str, operation_id: str) -> dict[str, Any] | None:
            item = self._store_call(
                instance_id,
                operation_id,
                lambda: store.get(namespace, memory_id),
            )
            return self._item_value(item, instance_id=instance_id, operation_id=operation_id)

        for memory in instance["initial_memories"]:
            put(copy.deepcopy(memory), "initial_memory")

        scheduled_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for event in instance["failure_schedule"]:
            scheduled_by_step[int(event["step"])].append(event)

        for operation in instance["operations"]:
            step = int(operation["step"])
            operation_id = operation["op_id"]
            step_events = scheduled_by_step.get(step, [])
            for event in step_events:
                if event["type"] == "revoke":
                    current_policy_version += 1
                    trace.append(
                        {"step": step, "event": "revoke", "policy_version": current_policy_version}
                    )
                elif event["type"] == "delay":
                    trace.append({"step": step, "event": "delay"})

            op_type = operation["type"]
            trace.append({"step": step, "operation": op_type})

            if op_type == "write":
                put(_memory_from_operation(operation), operation_id)
                committed_memory_ids.append(operation["memory_id"])

            elif op_type in {"get_by_id", "read"} and operation.get("memory_id") is not None:
                namespace = self._namespace(
                    instance_id,
                    operation.get("agent_id", "agent_1"),
                    operation.get("scope", "tenant:user_001"),
                )
                memory_id = operation["memory_id"]
                memory = get(namespace, memory_id, operation_id)
                if memory is None and memory_id in locations and locations[memory_id] != namespace:
                    denied_reads += 1
                    trace.append({"step": step, "event": "denied_read", "memory_id": memory_id})
                elif memory is not None and memory.get("status") in {"active", "pending"}:
                    if _matches(memory, operation):
                        if memory_id not in exposed_memory_ids:
                            exposed_memory_ids.append(memory_id)
                        trace.append({"step": step, "event": "exposed_read", "memory_id": memory_id})

            elif op_type in {"search", "read"}:
                namespace = self._namespace(
                    instance_id,
                    operation.get("agent_id", "agent_1"),
                    operation.get("scope", "tenant:user_001"),
                )
                items = self._store_call(
                    instance_id,
                    operation_id,
                    lambda: store.search(
                        namespace,
                        query=operation.get("query"),
                        filter={"status": {"$ne": "invalid"}},
                    ),
                )
                for item in items:
                    memory = self._item_value(item, instance_id=instance_id, operation_id=operation_id)
                    if memory is None or memory.get("status") not in {"active", "pending"}:
                        continue
                    if not _matches(memory, operation):
                        continue
                    memory_id = memory["memory_id"]
                    if memory_id not in exposed_memory_ids:
                        exposed_memory_ids.append(memory_id)
                    trace.append({"step": step, "event": "exposed_read", "memory_id": memory_id})
                    break
                else:
                    denied_memory_ids = [
                        memory_id
                        for memory_id, candidate in known_memories.items()
                        if locations[memory_id] != namespace and _matches(candidate, operation)
                    ]
                    if denied_memory_ids:
                        denied_reads += 1
                        trace.append(
                            {
                                "step": step,
                                "event": "denied_read",
                                "memory_id": denied_memory_ids[0],
                            }
                        )

            elif op_type == "supersede":
                old_id = operation["old_memory_id"]
                new_id = operation["new_memory_id"]
                if old_id not in locations or new_id not in locations:
                    raise KeyError(f"unknown memory_id: {old_id if old_id not in locations else new_id}")
                old_memory = get(locations[old_id], old_id, operation_id)
                new_memory = get(locations[new_id], new_id, operation_id)
                if old_memory is None or new_memory is None:
                    raise KeyError(f"unknown memory_id: {old_id if old_memory is None else new_id}")
                old_memory["status"] = "superseded"
                new_memory["status"] = "active"
                new_memory["supersedes_id"] = old_id
                put(old_memory, operation_id)
                put(new_memory, operation_id)
                supersession_updates += 1
                trace.append({"step": step, "event": "ordered_supersession_updates"})

            elif op_type == "invalidate":
                memory_id = operation["memory_id"]
                if memory_id not in locations:
                    raise KeyError(f"unknown memory_id: {memory_id}")
                memory = get(locations[memory_id], memory_id, operation_id)
                if memory is None:
                    raise KeyError(f"unknown memory_id: {memory_id}")
                memory["status"] = "invalid"
                put(memory, operation_id)
                transaction_state = "invalidated"

            elif op_type == "propagate":
                trace.append(
                    {
                        "step": step,
                        "event": "capability_absent",
                        "capability": "provenance_propagation",
                    }
                )

            elif op_type == "commit":
                transaction_state = "committed"

            if any(event["type"] == "crash" for event in step_events):
                if self.persistent_store:
                    raise UnsupportedMappingError(
                        "LangGraph Store cannot map crash recovery claim "
                        f"for instance {instance_id}, operation {operation_id}"
                    )
                transaction_state = "partial_commit" if committed_memory_ids else "crashed"
                trace.append({"step": step, "event": "crash"})
                trace.append(
                    {
                        "step": step,
                        "event": "capability_absent",
                        "capability": "crash_recovery",
                    }
                )
                break

        final_memories: dict[str, dict[str, Any]] = {}
        for memory_id, namespace in locations.items():
            memory = get(namespace, memory_id, "final_state")
            if memory is not None:
                final_memories[memory_id] = memory

        if transaction_state == "active":
            transaction_state = "completed"

        return ReplayObservation(
            transaction_state=transaction_state,
            final_memories=final_memories,
            committed_memory_ids=committed_memory_ids,
            trace=trace,
            metrics={
                "operation_count": len(trace),
                "repair_count": 0,
                "policy_version_at_end": current_policy_version,
                "exposed_memory_ids": exposed_memory_ids,
                "denied_reads": denied_reads,
                "supersession_updates": supersession_updates,
            },
        )
