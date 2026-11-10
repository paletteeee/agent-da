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
    memories: dict[str, dict[str, Any]] = {}
    for memory in instance["initial_memories"]:
        item = copy.deepcopy(memory)
        item.setdefault("status", "active")
        item.setdefault("version", 1)
        memories[item["memory_id"]] = item
    buffered_writes: dict[str, list[dict[str, Any]]] = {}
    buffered_edges: dict[str, list[dict[str, Any]]] = {}
    buffered_supersessions: dict[str, list[tuple[str, str]]] = {}
    buffered_invalidations: dict[str, set[str]] = {}
    provenance_edges = copy.deepcopy(instance.get("provenance_edges", []))
    committed_memory_ids: list[str] = []
    trace: list[dict[str, Any]] = []
    current_policy_version = 1
    transaction_authorized_actions: dict[str, set[str]] = {}
    transaction_read_versions: dict[str, dict[str, tuple[int, Any, Any]]] = {}
    revoked_actions: set[str] = set()
    transaction_state = "active"
    transaction_states: dict[str, str] = {}
    repair_count = 0
    exposed_memory_ids: list[str] = []
    denied_reads = 0
    supersession_updates = 0

    def transaction_id(operation: dict[str, Any]) -> str:
        return str(operation.get("txn_id") or "implicit")

    def ensure_transaction(txn_id: str) -> None:
        buffered_writes.setdefault(txn_id, [])
        buffered_edges.setdefault(txn_id, [])
        buffered_supersessions.setdefault(txn_id, [])
        buffered_invalidations.setdefault(txn_id, set())
        transaction_authorized_actions.setdefault(txn_id, set())
        transaction_read_versions.setdefault(txn_id, {})
        transaction_states.setdefault(txn_id, "active")

    def has_pending_mutations(txn_id: str) -> bool:
        return bool(
            buffered_writes.get(txn_id)
            or buffered_edges.get(txn_id)
            or buffered_supersessions.get(txn_id)
            or buffered_invalidations.get(txn_id)
        )

    def clear_staged_mutations(txn_id: str) -> None:
        ensure_transaction(txn_id)
        buffered_writes[txn_id].clear()
        buffered_edges[txn_id].clear()
        buffered_supersessions[txn_id].clear()
        buffered_invalidations[txn_id].clear()

    def stage_write(txn_id: str, memory: dict[str, Any]) -> None:
        ensure_transaction(txn_id)
        buffered_writes[txn_id] = [
            pending
            for pending in buffered_writes[txn_id]
            if pending["memory_id"] != memory["memory_id"]
        ]
        buffered_writes[txn_id].append(memory)

    def abort_transaction(txn_id: str) -> None:
        nonlocal transaction_state
        clear_staged_mutations(txn_id)
        transaction_states[txn_id] = "aborted"
        transaction_state = "aborted"

    def abort_active_transactions() -> None:
        for txn_id in sorted(transaction_states):
            if transaction_states[txn_id] == "active":
                abort_transaction(txn_id)

    def crash_applies(event: dict[str, Any], operation: dict[str, Any]) -> bool:
        if (event.get("type") or event.get("action")) not in {"crash", "crash_during_commit"}:
            return False
        target = event.get("target")
        return target in {None, operation.get("txn_id"), operation.get("type"), "commit"}

    def apply_schedule_event(event: dict[str, Any], operation: dict[str, Any]) -> None:
        """Apply one non-crash schedule event at its causal boundary."""

        nonlocal current_policy_version, repair_count, transaction_state
        event_type = event.get("type") or event.get("action")
        step = int(operation["step"])
        if event_type in {"revoke", "policy_change"}:
            current_policy_version += 1
            target = event.get("target") or event.get("action")
            if target in {"read", "search", "write", "derive", "propagate", "supersede"}:
                revoked_actions.add(target)
            trace.append(
                {
                    "step": step,
                    "event": "revoke" if event_type == "revoke" else "policy_change",
                    "policy_version": current_policy_version,
                }
            )
        elif event_type == "delay":
            trace.append({"step": step, "event": "delay"})
        elif event_type == "invalidate":
            memory_id = event.get("target") or event.get("memory_id")
            memory = memories.get(memory_id)
            if memory is None:
                trace.append({"step": step, "event": "denied_invalidate", "memory_id": memory_id})
                return
            if memory.get("status") != "invalid":
                memory["status"] = "invalid"
                memory["version"] = int(memory.get("version", 1)) + 1
            if variant in REPAIR_VARIANTS:
                repair_count += _apply_repair(instance, memories, provenance_edges)
                transaction_state = "repaired"
            else:
                transaction_state = "invalidated"
            trace.append({"step": step, "event": "invalidate", "memory_id": memory_id})

    def read_dependency_changed(txn_id: str) -> bool:
        for memory_id, (version, scope, status) in transaction_read_versions[txn_id].items():
            memory = memories.get(memory_id)
            if memory is None:
                return True
            if (
                int(memory.get("version", 1)) != version
                or memory.get("scope") != scope
                or memory.get("status") != status
                or memory.get("status") != "active"
            ):
                return True
        return False

    for operation in instance["operations"]:
        step = int(operation["step"])
        pre_events = events_for_operation(instance, operation, "before")
        for event in pre_events:
            apply_schedule_event(event, operation)

        op_type = operation["type"]
        trace.append({"step": step, "operation": op_type})
        txn_id = transaction_id(operation)
        pre_crashes = [event for event in pre_events if crash_applies(event, operation)]
        ambiguous_commit_crash = bool(
            pre_crashes
            and op_type == "commit"
            and all(event.get("phase") is None for event in pre_crashes)
        )
        if pre_crashes and not ambiguous_commit_crash:
            if uses_transaction:
                abort_active_transactions()
                transaction_state = "aborted"
            elif committed_memory_ids:
                transaction_state = "partial_commit"
            else:
                transaction_state = "crashed"
            trace.append({"step": step, "event": "crash"})
            break

        terminal = uses_transaction and transaction_states.get(txn_id) in {"committed", "aborted"}
        if terminal:
            trace.append({"step": step, "event": "terminal_transaction", "txn_id": txn_id})

        elif op_type == "begin_txn":
            ensure_transaction(txn_id)

        elif op_type == "write":
            memory = _memory_from_operation(operation)
            if not uses_transaction:
                memories[memory["memory_id"]] = memory
                _record_committed(committed_memory_ids, memory["memory_id"])
            else:
                ensure_transaction(txn_id)
                if "write" in revoked_actions:
                    trace.append({"step": step, "event": "denied_write"})
                else:
                    stage_write(txn_id, memory)
                    transaction_authorized_actions[txn_id].add("write")

        elif op_type in {"search", "read", "get_by_id"}:
            if uses_transaction:
                ensure_transaction(txn_id)
            requested_id = operation.get("memory_id")
            candidates = [memories.get(requested_id)] if requested_id else list(memories.values())
            found = None
            action = "search" if op_type == "search" else "read"
            for memory in candidates:
                if memory is None or memory.get("status") not in {"active", "pending"}:
                    continue
                if action in revoked_actions:
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
                if uses_transaction and found["memory_id"] in memories:
                    transaction_read_versions[txn_id][found["memory_id"]] = (
                        int(found.get("version", 1)),
                        found.get("scope"),
                        found.get("status"),
                    )
                trace.append({"step": step, "event": "exposed_read", "memory_id": found["memory_id"]})

        elif op_type == "supersede":
            ensure_transaction(txn_id)
            if "supersede" in revoked_actions:
                trace.append({"step": step, "event": "denied_supersede"})
            else:
                old_id = operation["old_memory_id"]
                new_id = operation["new_memory_id"]

                def visible_memory(memory_id: str) -> dict[str, Any] | None:
                    for pending in reversed(buffered_writes[txn_id]):
                        if pending["memory_id"] == memory_id:
                            return pending
                    return memories.get(memory_id)

                if visible_memory(old_id) is None:
                    raise KeyError(f"unknown memory_id: {old_id}")
                if visible_memory(new_id) is None:
                    raise KeyError(f"unknown memory_id: {new_id}")
                if variant in SUPERSESSION_VARIANTS:
                    buffered_supersessions[txn_id].append((old_id, new_id))
                    supersession_updates += 1
                    transaction_authorized_actions[txn_id].add("supersede")

        elif op_type == "commit":
            ensure_transaction(txn_id)
            policy_revoked = bool(transaction_authorized_actions[txn_id] & revoked_actions)
            if variant in POLICY_REVALIDATION_VARIANTS and (
                policy_revoked or read_dependency_changed(txn_id)
            ):
                abort_transaction(txn_id)
            elif ambiguous_commit_crash and uses_transaction:
                abort_transaction(txn_id)
            elif not uses_transaction:
                transaction_state = "committed"
                transaction_states[txn_id] = "committed"
            else:
                for memory in buffered_writes[txn_id]:
                    previous = memories.get(memory["memory_id"])
                    memory["version"] = (
                        int(previous.get("version", 1)) + 1 if previous is not None else 1
                    )
                    memories[memory["memory_id"]] = memory
                    _record_committed(committed_memory_ids, memory["memory_id"])
                provenance_edges.extend(buffered_edges[txn_id])
                for old_id, new_id in buffered_supersessions[txn_id]:
                    old_memory = memories.get(old_id)
                    new_memory = memories.get(new_id)
                    if old_memory is None or new_memory is None:
                        raise KeyError(f"unknown supersession target: {old_id}->{new_id}")
                    old_memory["status"] = "superseded"
                    old_memory["version"] = int(old_memory.get("version", 1)) + 1
                    new_memory["status"] = "active"
                    new_memory["supersedes_id"] = old_id
                for memory_id in buffered_invalidations[txn_id]:
                    memory = memories.get(memory_id)
                    if memory is not None and memory.get("status") != "invalid":
                        memory["status"] = "invalid"
                        memory["version"] = int(memory.get("version", 1)) + 1
                if buffered_invalidations[txn_id]:
                    repair_count += _apply_repair(instance, memories, provenance_edges)
                clear_staged_mutations(txn_id)
                transaction_state = "committed"
                transaction_states[txn_id] = "committed"

        elif op_type == "abort":
            abort_transaction(txn_id)

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
                ensure_transaction(txn_id)
                if op_type in revoked_actions:
                    trace.append({"step": step, "event": f"denied_{op_type}"})
                else:
                    stage_write(txn_id, memory)
                    buffered_edges[txn_id].extend(operation_edges)
                    transaction_authorized_actions[txn_id].add(op_type)
                    for source_id in source_ids:
                        source = memories.get(source_id)
                        if source is not None:
                            transaction_read_versions[txn_id][source_id] = (
                                int(source.get("version", 1)),
                                source.get("scope"),
                                source.get("status"),
                            )
            else:
                memories[memory["memory_id"]] = memory
                _record_committed(committed_memory_ids, memory["memory_id"])
                provenance_edges.extend(operation_edges)

        elif op_type == "invalidate":
            memory_id = operation["memory_id"]
            if uses_transaction and transaction_states.get(txn_id) == "active":
                known_memory_ids = set(memories) | {
                    memory["memory_id"] for memory in buffered_writes[txn_id]
                }
                if memory_id not in known_memory_ids:
                    trace.append({"step": step, "event": "denied_invalidate", "memory_id": memory_id})
                else:
                    invalidated = {memory_id}
                    invalidated.update(
                        _descendants(instance, memory_id, provenance_edges + buffered_edges[txn_id])
                    )
                    buffered_invalidations[txn_id].update(invalidated)
                    trace.append({"step": step, "event": "staged_invalidate", "memory_id": memory_id})
            else:
                if memory_id not in memories:
                    raise KeyError(f"unknown memory_id: {memory_id}")
                memories[memory_id]["status"] = "invalid"
                memories[memory_id]["version"] = int(memories[memory_id].get("version", 1)) + 1
                if variant in REPAIR_VARIANTS:
                    repair_count += _apply_repair(instance, memories, provenance_edges)
                    transaction_state = "repaired"
                else:
                    transaction_state = "invalidated"

        post_events = events_for_operation(instance, operation, "after")
        for event in post_events:
            apply_schedule_event(event, operation)
        if any(crash_applies(event, operation) for event in post_events):
            if uses_transaction:
                abort_active_transactions()
                if transaction_states.get(txn_id) != "committed":
                    transaction_state = "aborted"
            elif committed_memory_ids:
                transaction_state = "partial_commit"
            else:
                transaction_state = "crashed"
            trace.append({"step": step, "event": "crash"})
            break

    for txn_id in sorted(transaction_states):
        if transaction_states[txn_id] != "active":
            continue
        if has_pending_mutations(txn_id):
            abort_transaction(txn_id)
        else:
            transaction_states[txn_id] = "completed"
    if transaction_state == "active":
        transaction_state = transaction_states.get("implicit", "completed")

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
