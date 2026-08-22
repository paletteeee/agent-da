"""Deterministic replay engine for TxnMemBench instances."""

from __future__ import annotations

import copy
from collections import defaultdict, deque
from typing import Any

from txnmem_schema import validate_instance


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


def _descendants(instance: dict[str, Any], source_id: str) -> set[str]:
    children: dict[str, list[str]] = defaultdict(list)
    for edge in instance["provenance_edges"]:
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


def _apply_repair(instance: dict[str, Any], memories: dict[str, dict[str, Any]]) -> int:
    repaired = 0
    invalid_sources = [
        memory_id
        for memory_id, memory in memories.items()
        if memory.get("status") == "invalid"
    ]
    for source_id in invalid_sources:
        for descendant_id in _descendants(instance, source_id):
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
    return query in {memory.get("value"), memory.get("memory_id"), memory.get("attribute")}


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
    committed_memory_ids: list[str] = []
    trace: list[dict[str, Any]] = []
    current_policy_version = 1
    begin_policy_version = 1
    write_allowed = True
    transaction_state = "active"
    repair_count = 0
    exposed_memory_ids: list[str] = []
    denied_reads = 0
    supersession_updates = 0

    scheduled_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in instance["failure_schedule"]:
        scheduled_by_step[int(event["step"])].append(event)

    for operation in instance["operations"]:
        step = int(operation["step"])
        step_events = scheduled_by_step.get(step, [])
        for event in step_events:
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

        elif op_type == "write":
            memory = _memory_from_operation(operation)
            if not uses_transaction:
                memories[memory["memory_id"]] = memory
                committed_memory_ids.append(memory["memory_id"])
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
            if old_id not in memories:
                raise KeyError(f"unknown memory_id: {old_id}")
            if new_id in memories:
                new_memory = memories[new_id]
            else:
                pending = next((item for item in buffered_writes if item["memory_id"] == new_id), None)
                new_memory = pending
            if new_memory is None:
                raise KeyError(f"unknown memory_id: {new_id}")
            if variant in SUPERSESSION_VARIANTS:
                memories[old_id]["status"] = "superseded"
                new_memory["status"] = "active"
                new_memory["supersedes_id"] = old_id
                supersession_updates += 1

        elif op_type == "commit":
            policy_changed = current_policy_version != begin_policy_version or not write_allowed
            crash_on_commit = any(event["type"] == "crash" for event in step_events)
            if variant in POLICY_REVALIDATION_VARIANTS and policy_changed:
                buffered_writes.clear()
                transaction_state = "aborted"
            elif crash_on_commit and uses_transaction:
                buffered_writes.clear()
                transaction_state = "aborted"
            elif not uses_transaction:
                transaction_state = "committed"
            else:
                for memory in buffered_writes:
                    memories[memory["memory_id"]] = memory
                    committed_memory_ids.append(memory["memory_id"])
                buffered_writes.clear()
                transaction_state = "committed"

        elif op_type == "invalidate":
            memory_id = operation["memory_id"]
            if memory_id not in memories:
                raise KeyError(f"unknown memory_id: {memory_id}")
            memories[memory_id]["status"] = "invalid"
            if variant in REPAIR_VARIANTS:
                repair_count += _apply_repair(instance, memories)
                transaction_state = "repaired"
            else:
                transaction_state = "invalidated"

        if any(event["type"] == "crash" for event in step_events):
            if op_type != "commit":
                if uses_transaction:
                    buffered_writes.clear()
                    transaction_state = "aborted"
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
        "final_memories": memories,
        "committed_memory_ids": committed_memory_ids,
        "trace": trace,
        "metrics": {
            "operation_count": len(trace),
            "repair_count": repair_count,
            "policy_version_at_end": current_policy_version,
            "exposed_memory_ids": exposed_memory_ids,
            "denied_reads": denied_reads,
            "supersession_updates": supersession_updates,
        },
    }
