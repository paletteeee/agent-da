"""Deterministic replay engine for TxnMemBench instances."""

from __future__ import annotations

import copy
from collections import defaultdict, deque
from typing import Any

from txnmem_schema import validate_instance
from txnmem_schedules import events_for_operation


VARIANTS = (
    "Naive",
    "TxnMem-NoTxn",
    "TxnMem-NoPolicyCommit",
    "TxnMem-NoRepair",
    "TxnMem",
)
NO_TRANSACTION_VARIANTS = {"Naive", "TxnMem-NoTxn"}
POLICY_REVALIDATION_VARIANTS = {"TxnMem", "TxnMem-NoTxn", "TxnMem-NoRepair"}
REPAIR_VARIANTS = {"TxnMem", "TxnMem-NoTxn", "TxnMem-NoPolicyCommit"}
SCOPE_ENFORCEMENT_VARIANTS = {"TxnMem", "TxnMem-NoTxn", "TxnMem-NoPolicyCommit", "TxnMem-NoRepair"}
SUPERSESSION_VARIANTS = {"TxnMem", "TxnMem-NoPolicyCommit", "TxnMem-NoRepair"}


def _record_committed(committed_memory_ids: list[str], memory_id: str) -> None:
    """Record each final memory id once even when a tool call is retried."""

    if memory_id not in committed_memory_ids:
        committed_memory_ids.append(memory_id)


def _operation_edges(instance: dict[str, Any]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for operation in instance.get("operations", []):
        if operation.get("type") not in {"derive", "propagate"}:
            continue
        derived_id = operation.get("memory_id") or operation.get("output_id")
        source_ids = list(operation.get("source_ids", []))
        if operation.get("type") == "propagate" and not source_ids:
            source_id = operation.get("source_id")
            source_ids = [source_id] if source_id else []
        for source_id in source_ids:
            edges.append(
                {
                    "source_id": source_id,
                    "derived_id": derived_id,
                    "relation": "read_derive" if operation.get("type") == "derive" else "propagate",
                    "operation_id": operation.get("op_id"),
                    "txn_id": operation.get("txn_id"),
                }
            )
    return edges


def _descendants(
    instance: dict[str, Any], source_id: str, provenance_edges: list[dict[str, Any]] | None = None
) -> set[str]:
    children: dict[str, list[str]] = defaultdict(list)
    edges = provenance_edges if provenance_edges is not None else instance.get("provenance_edges", [])
    for edge in edges:
        children[edge["source_id"]].append(edge["derived_id"])
    found: set[str] = set()
    queue = deque(children.get(source_id, []))
    while queue:
        current = queue.popleft()
        if current in found:
            continue
        found.add(current)
        queue.extend(children.get(current, []))
    return found


def _apply_repair(
    instance: dict[str, Any],
    memories: dict[str, dict[str, Any]],
    provenance_edges: list[dict[str, Any]] | None = None,
) -> int:
    repaired = 0
    invalid_sources = [
        memory_id
        for memory_id, memory in memories.items()
        if memory.get("status") == "invalid"
    ]
    for source_id in invalid_sources:
        for descendant_id in _descendants(instance, source_id, provenance_edges):
            if memories[descendant_id].get("status") != "invalid":
                memories[descendant_id]["status"] = "invalid"
                repaired += 1
    return repaired


def _memory_from_operation(operation: dict[str, Any]) -> dict[str, Any]:
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
    if query is None:
        return True
    # Tool-generated memory values may be structured dictionaries/lists, so
    # membership in a set would fail before equality is even evaluated.
    return any(query == candidate for candidate in (memory.get("value"), memory.get("memory_id"), memory.get("attribute")))


def _scope_allowed(memory: dict[str, Any], operation: dict[str, Any]) -> bool:
    return memory.get("scope") == operation.get("scope", memory.get("scope"))


def run_instance(instance: dict[str, Any], variant: str) -> dict[str, Any]:
    """Replay one instance using a baseline, ablation, or full semantics."""

    validate_instance(instance)
    if variant not in VARIANTS:
        raise ValueError(f"unsupported variant: {variant}")

    uses_transaction = variant not in NO_TRANSACTION_VARIANTS
    memories = {
        memory["memory_id"]: copy.deepcopy(memory)
        for memory in instance["initial_memories"]
    }
    buffered_writes: list[dict[str, Any]] = []
    buffered_edges: list[dict[str, Any]] = []
    provenance_edges = copy.deepcopy(instance.get("provenance_edges", []))
    committed_memory_ids: list[str] = []
    trace: list[dict[str, Any]] = []
    current_policy_version = 1
    begin_policy_version = 1
    write_allowed = True
    transaction_state = "active"
    transaction_states: dict[str, str] = {}
    repair_count = 0
    exposed_memory_ids: list[str] = []
    denied_reads = 0
    supersession_updates = 0

    for operation in instance["operations"]:
        step = int(operation["step"])
        pre_events = events_for_operation(instance, operation, "before")
        for event in pre_events:
            if event["type"] == "revoke":
                current_policy_version += 1
                write_allowed = False
                trace.append({"step": step, "event": "revoke", "policy_version": current_policy_version})
            elif event["type"] == "delay":
                trace.append({"step": step, "event": "delay"})

        op_type = operation["type"]
        trace.append({"step": step, "operation": op_type})

        if op_type == "begin_txn":
            begin_policy_version = current_policy_version
            if operation.get("txn_id"):
                transaction_states[operation["txn_id"]] = "active"

        elif op_type == "write":
            memory = _memory_from_operation(operation)
            if not uses_transaction:
                memories[memory["memory_id"]] = memory
                _record_committed(committed_memory_ids, memory["memory_id"])
            else:
                buffered_writes.append(memory)

        elif op_type in {"search", "read", "get_by_id"}:
            requested_id = operation.get("memory_id")
            candidates = [memories.get(requested_id)] if requested_id else list(memories.values())
            found = None
            for memory in candidates:
                if memory is None or memory.get("status") not in {"active", "pending"}:
                    continue
                if not _matches(memory, operation):
                    continue
                if variant in SCOPE_ENFORCEMENT_VARIANTS and not _scope_allowed(memory, operation):
                    denied_reads += 1
                    trace.append({"step": step, "event": "denied_read", "memory_id": memory["memory_id"]})
                    continue
                found = memory
                break
            if found is not None:
                if found["memory_id"] not in exposed_memory_ids:
                    exposed_memory_ids.append(found["memory_id"])
                trace.append({"step": step, "event": "exposed_read", "memory_id": found["memory_id"]})

        elif op_type == "supersede":
            old_id = operation["old_memory_id"]
            new_id = operation["new_memory_id"]
            def visible_memory(memory_id: str) -> dict[str, Any] | None:
                for pending in reversed(buffered_writes):
                    if pending["memory_id"] == memory_id:
                        return pending
                return memories.get(memory_id)

            old_memory = visible_memory(old_id)
            if old_memory is None:
                raise KeyError(f"unknown memory_id: {old_id}")
            new_memory = visible_memory(new_id)
            if new_memory is None:
                raise KeyError(f"unknown memory_id: {new_id}")
            if variant in SUPERSESSION_VARIANTS:
                old_memory["status"] = "superseded"
                new_memory["status"] = "active"
                new_memory["supersedes_id"] = old_id
                # A native Agent may emit a write followed by a supersede and
                # may repeat the write while retrying the tool call.  Keep the
                # final visible write consistent with the supersession edge.
                for pending in buffered_writes:
                    if pending["memory_id"] == old_id:
                        pending["status"] = "superseded"
                    elif pending["memory_id"] == new_id:
                        pending["status"] = "active"
                        pending["supersedes_id"] = old_id
                supersession_updates += 1

        elif op_type == "commit":
            policy_changed = current_policy_version != begin_policy_version or not write_allowed
            crash_on_commit = any(event["type"] in {"crash", "crash_during_commit"} for event in pre_events)
            if variant in POLICY_REVALIDATION_VARIANTS and policy_changed:
                buffered_writes.clear()
                buffered_edges.clear()
                transaction_state = "aborted"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "aborted"
            elif crash_on_commit and uses_transaction:
                buffered_writes.clear()
                buffered_edges.clear()
                transaction_state = "aborted"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "aborted"
            elif not uses_transaction:
                transaction_state = "committed"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "committed"
            else:
                for memory in buffered_writes:
                    memories[memory["memory_id"]] = memory
                    _record_committed(committed_memory_ids, memory["memory_id"])
                buffered_writes.clear()
                provenance_edges.extend(buffered_edges)
                buffered_edges.clear()
                transaction_state = "committed"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "committed"

        elif op_type in {"derive", "propagate"}:
            memory = _memory_from_operation(operation)
            source_ids = list(operation.get("source_ids", []))
            if op_type == "propagate" and not source_ids and operation.get("source_id"):
                source_ids = [operation["source_id"]]
            operation_edges = [
                {
                    "source_id": source_id,
                    "derived_id": memory["memory_id"],
                    "relation": "read_derive" if op_type == "derive" else "propagate",
                    "operation_id": operation.get("op_id"),
                    "txn_id": operation.get("txn_id"),
                }
                for source_id in source_ids
            ]
            if uses_transaction:
                buffered_writes.append(memory)
                buffered_edges.extend(operation_edges)
            else:
                memories[memory["memory_id"]] = memory
                _record_committed(committed_memory_ids, memory["memory_id"])
                provenance_edges.extend(operation_edges)

        elif op_type == "invalidate":
            memory_id = operation["memory_id"]
            if memory_id not in memories:
                raise KeyError(f"unknown memory_id: {memory_id}")
            memories[memory_id]["status"] = "invalid"
            if variant in REPAIR_VARIANTS:
                repair_count += _apply_repair(instance, memories, provenance_edges)
                transaction_state = "repaired"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "repaired"
            else:
                transaction_state = "invalidated"
                if operation.get("txn_id"):
                    transaction_states[operation["txn_id"]] = "invalidated"

        post_events = events_for_operation(instance, operation, "after")
        if any(event["type"] in {"crash", "crash_during_commit"} for event in pre_events + post_events):
            if op_type != "commit":
                if uses_transaction:
                    buffered_writes.clear()
                    buffered_edges.clear()
                    transaction_state = "aborted"
                    if operation.get("txn_id"):
                        transaction_states[operation["txn_id"]] = "aborted"
                elif committed_memory_ids:
                    transaction_state = "partial_commit"
                else:
                    transaction_state = "crashed"
            trace.append({"step": step, "event": "crash"})
            break

    if transaction_state == "active":
        transaction_state = "completed"

    return {
        "variant": variant,
        "transaction_state": transaction_state,
        "transaction_states": transaction_states,
        "final_memories": memories,
        "committed_memory_ids": committed_memory_ids,
        "trace": trace,
        "provenance_edges": provenance_edges,
        "metrics": {
            "operation_count": len(trace),
            "repair_count": repair_count,
            "policy_version_at_end": current_policy_version,
            "exposed_memory_ids": exposed_memory_ids,
            "denied_reads": denied_reads,
            "supersession_updates": supersession_updates,
        },
    }
