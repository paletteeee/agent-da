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
            descendant = memories.get(descendant_id)
            if descendant is not None and descendant.get("status") != "invalid":
                descendant["status"] = "invalid"
                descendant["version"] = int(descendant.get("version", 1)) + 1
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
        "version": int(operation.get("version", 1)),
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
    for initial_memory in instance["initial_memories"]:
        memory = copy.deepcopy(initial_memory)
        memory.setdefault("status", "active")
        memory.setdefault("version", 1)
        memories[str(memory["memory_id"])] = memory
    provenance_edges = copy.deepcopy(instance.get("provenance_edges", []))
    committed_memory_ids: list[str] = []
    trace: list[dict[str, Any]] = []
    policy_versions: dict[str, int] = defaultdict(lambda: 1)
    for policy in instance.get("policies", []):
        actor = str(policy.get("agent_id") or "*")
        policy_versions[actor] = max(
            policy_versions[actor], int(policy.get("version", 1))
        )
    revoked_permissions: set[tuple[str, str, str]] = set()
    transactions: dict[str, dict[str, Any]] = {}
    transaction_write_sets: dict[str, list[str]] = defaultdict(list)
    read_observations: list[dict[str, Any]] = []
    transaction_state = "active"
    transaction_states: dict[str, str] = {}
    repair_count = 0
    exposed_memory_ids: list[str] = []
    denied_reads = 0
    supersession_updates = 0
    conflict_abort_count = 0

    def policy_epoch(actor: str) -> tuple[int, int]:
        return policy_versions["*"], policy_versions[actor]

    def transaction(operation: dict[str, Any]) -> dict[str, Any] | None:
        raw_txn_id = operation.get("txn_id")
        if raw_txn_id is None:
            return None
        txn_id = str(raw_txn_id)
        actor = str(operation.get("agent_id") or "agent_1")
        if txn_id not in transactions:
            transactions[txn_id] = {
                "agent_id": actor,
                "status": "active",
                "begin_policy_version": policy_epoch(actor),
                "writes": {},
                "edges": [],
                "supersessions": [],
                "read_versions": {},
                "mutations": [],
            }
            transaction_states[txn_id] = "active"
        txn = transactions[txn_id]
        if txn["agent_id"] != actor:
            raise ValueError(f"transaction actor mismatch: {txn_id}")
        return txn

    def permission_revoked(actor: str, action: str, scope: str | None) -> bool:
        normalized_scope = str(scope or "*")
        return any(
            (candidate_actor, action, candidate_scope) in revoked_permissions
            for candidate_actor in (actor, "*")
            for candidate_scope in (normalized_scope, "*")
        )

    def abort_transaction(txn_id: str, state: str = "aborted") -> None:
        txn = transactions[txn_id]
        txn["status"] = state
        txn["writes"].clear()
        txn["edges"].clear()
        txn["supersessions"].clear()
        transaction_states[txn_id] = state

    def abort_active_transactions() -> None:
        nonlocal transaction_state
        any_partial = False
        for txn_id, txn in transactions.items():
            if txn["status"] != "active":
                continue
            if uses_transaction:
                abort_transaction(txn_id)
            else:
                has_published_write = any(
                    memory_id in committed_memory_ids
                    for memory_id in transaction_write_sets.get(txn_id, [])
                )
                state = "partial_commit" if has_published_write else "aborted"
                abort_transaction(txn_id, state)
                any_partial = any_partial or has_published_write
        transaction_state = "partial_commit" if any_partial else "aborted"

    def record_write(txn_id: str | None, memory_id: str) -> None:
        if txn_id is None:
            return
        if memory_id not in transaction_write_sets[txn_id]:
            transaction_write_sets[txn_id].append(memory_id)

    def publish_memory(memory: dict[str, Any]) -> None:
        memory_id = str(memory["memory_id"])
        existing = memories.get(memory_id)
        committed = copy.deepcopy(memory)
        committed["status"] = "active"
        committed["version"] = (
            int(existing.get("version", 1)) + 1 if existing is not None else 1
        )
        memories[memory_id] = committed
        _record_committed(committed_memory_ids, memory_id)

    def snapshot_committed_read(txn: dict[str, Any] | None, memory_id: str) -> None:
        if txn is None or memory_id in txn["writes"]:
            return
        memory = memories.get(memory_id)
        if memory is None:
            txn["read_versions"].setdefault(memory_id, None)
            return
        txn["read_versions"].setdefault(
            memory_id,
            (
                int(memory.get("version", 1)),
                str(memory.get("status", "active")),
                str(memory.get("scope", "")),
            ),
        )

    def current_read_version(memory_id: str) -> tuple[int, str, str] | None:
        memory = memories.get(memory_id)
        if memory is None:
            return None
        return (
            int(memory.get("version", 1)),
            str(memory.get("status", "active")),
            str(memory.get("scope", "")),
        )

    def invalidate_committed(memory_id: str) -> None:
        nonlocal repair_count, transaction_state
        memory = memories.get(memory_id)
        if memory is None:
            raise KeyError(f"unknown memory_id: {memory_id}")
        if memory.get("status") != "invalid":
            memory["status"] = "invalid"
            memory["version"] = int(memory.get("version", 1)) + 1
        if variant in REPAIR_VARIANTS:
            repair_count += _apply_repair(instance, memories, provenance_edges)
            transaction_state = "repaired"
        else:
            transaction_state = "invalidated"

    def apply_non_crash_event(event: dict[str, Any], step: int) -> None:
        event_type = str(event.get("type") or event.get("action"))
        if event_type in {"revoke", "policy_change"}:
            actor = str(event.get("agent_id") or "*")
            action = str(event.get("target") or event.get("action") or "*")
            if action in {"revoke", "policy_change"}:
                action = "*"
            scope = str(event.get("scope") or "*")
            policy_versions[actor] += 1
            revoked_permissions.add((actor, action, scope))
            trace.append(
                {
                    "step": step,
                    "event": "revoke",
                    "agent_id": actor,
                    "action": action,
                    "scope": scope,
                    "policy_version": policy_versions[actor],
                }
            )
        elif event_type == "invalidate":
            memory_id = str(event.get("memory_id") or event.get("target"))
            invalidate_committed(memory_id)
            trace.append(
                {"step": step, "event": "invalidate", "memory_id": memory_id}
            )
        elif event_type == "delay":
            trace.append({"step": step, "event": "delay"})

    def is_crash(event: dict[str, Any]) -> bool:
        return str(event.get("type") or event.get("action")) in {
            "crash",
            "crash_during_commit",
        }

    def stage_write(
        operation: dict[str, Any],
        txn: dict[str, Any] | None,
        *,
        action: str,
    ) -> dict[str, Any]:
        memory = _memory_from_operation(operation)
        memory_id = str(memory["memory_id"])
        txn_id = str(operation["txn_id"]) if operation.get("txn_id") else None
        record_write(txn_id, memory_id)
        if uses_transaction:
            if txn is None:
                raise ValueError("transactional write requires txn_id")
            existing = memories.get(memory_id)
            if existing is not None:
                merged = copy.deepcopy(existing)
                merged.update(memory)
                memory = merged
            txn["writes"][memory_id] = memory
            txn["mutations"].append(
                {
                    "action": action,
                    "scope": memory.get("scope"),
                    "operation_id": operation.get("op_id"),
                }
            )
        else:
            publish_memory(memory)
        return memory

    def visible_candidates(
        operation: dict[str, Any], txn: dict[str, Any] | None
    ) -> list[tuple[dict[str, Any], bool]]:
        requested_id = operation.get("memory_id")
        own_writes = txn["writes"] if uses_transaction and txn is not None else {}
        if requested_id:
            if requested_id in own_writes:
                return [(own_writes[requested_id], True)]
            committed = memories.get(str(requested_id))
            return [(committed, False)] if committed is not None else []
        candidates: list[tuple[dict[str, Any], bool]] = [
            (memory, True) for memory in own_writes.values()
        ]
        candidates.extend(
            (memory, False)
            for memory_id, memory in memories.items()
            if memory_id not in own_writes
        )
        return candidates

    for operation in instance["operations"]:
        step = int(operation["step"])
        pre_events = events_for_operation(instance, operation, "before")
        for event in pre_events:
            if not is_crash(event):
                apply_non_crash_event(event, step)
        if any(is_crash(event) for event in pre_events):
            abort_active_transactions()
            trace.append({"step": step, "event": "crash", "phase": "before"})
            break

        op_type = operation["type"]
        txn = transaction(operation)
        trace.append(
            {
                "step": step,
                "operation": op_type,
                "operation_id": operation.get("op_id"),
                "agent_id": operation.get("agent_id"),
                "txn_id": operation.get("txn_id"),
            }
        )

        if op_type == "begin_txn":
            if txn is not None:
                txn["begin_policy_version"] = policy_epoch(txn["agent_id"])

        elif op_type in {"write", "stage_write"}:
            stage_write(operation, txn, action="write")

        elif op_type in {"search", "read", "get_by_id"}:
            actor = str(operation.get("agent_id") or "agent_1")
            action = "search" if op_type == "search" else "read"
            visible_ids: list[str] = []
            denied_reason: str | None = None
            for memory, is_own_write in visible_candidates(operation, txn):
                if memory.get("status") not in {"active", "pending"}:
                    continue
                if not _matches(memory, operation):
                    continue
                if variant in SCOPE_ENFORCEMENT_VARIANTS and not _scope_allowed(memory, operation):
                    denied_reads += 1
                    denied_reason = "scope_mismatch"
                    trace.append(
                        {
                            "step": step,
                            "event": "denied_read",
                            "memory_id": memory["memory_id"],
                            "reason": denied_reason,
                        }
                    )
                    continue
                if (
                    variant in SCOPE_ENFORCEMENT_VARIANTS
                    and permission_revoked(actor, action, memory.get("scope"))
                ):
                    denied_reads += 1
                    denied_reason = "policy_revoked"
                    continue
                memory_id = str(memory["memory_id"])
                visible_ids.append(memory_id)
                if memory_id not in exposed_memory_ids:
                    exposed_memory_ids.append(memory_id)
                if not is_own_write:
                    snapshot_committed_read(txn, memory_id)
                trace.append(
                    {"step": step, "event": "exposed_read", "memory_id": memory_id}
                )
            read_observations.append(
                {
                    "operation_id": operation.get("op_id"),
                    "reader_agent_id": actor,
                    "txn_id": operation.get("txn_id"),
                    "probe_role": operation.get("probe_role"),
                    "visible_memory_ids": sorted(visible_ids),
                    "denied_reason": denied_reason if not visible_ids else None,
                }
            )

        elif op_type == "supersede":
            old_id = operation["old_memory_id"]
            new_id = operation["new_memory_id"]
            own_writes = txn["writes"] if txn is not None else {}
            old_memory = own_writes.get(old_id) or memories.get(old_id)
            if old_memory is None:
                raise KeyError(f"unknown memory_id: {old_id}")
            new_memory = own_writes.get(new_id) or memories.get(new_id)
            if new_memory is None:
                raise KeyError(f"unknown memory_id: {new_id}")
            if variant in SUPERSESSION_VARIANTS:
                if txn is None:
                    raise ValueError("transactional supersession requires txn_id")
                txn["supersessions"].append((old_id, new_id))
                txn["mutations"].append(
                    {
                        "action": "supersede",
                        "scope": old_memory.get("scope"),
                        "operation_id": operation.get("op_id"),
                    }
                )
                snapshot_committed_read(txn, old_id)

        elif op_type == "commit":
            if txn is None:
                transaction_state = "committed"
            else:
                actor = str(txn["agent_id"])
                policy_denied = variant in POLICY_REVALIDATION_VARIANTS and any(
                    permission_revoked(actor, item["action"], item.get("scope"))
                    for item in txn["mutations"]
                )
                version_changed = any(
                    current_read_version(memory_id) != recorded
                    for memory_id, recorded in txn["read_versions"].items()
                )
                if policy_denied:
                    abort_transaction(str(operation["txn_id"]))
                    transaction_state = "aborted"
                elif uses_transaction and version_changed:
                    abort_transaction(str(operation["txn_id"]))
                    transaction_state = "aborted"
                    conflict_abort_count += 1
                elif not uses_transaction:
                    txn["status"] = "committed"
                    transaction_states[str(operation["txn_id"])] = "committed"
                    transaction_state = "committed"
                else:
                    for memory in txn["writes"].values():
                        publish_memory(memory)
                    provenance_edges.extend(copy.deepcopy(txn["edges"]))
                    for old_id, new_id in txn["supersessions"]:
                        old_memory = memories.get(old_id)
                        new_memory = memories.get(new_id)
                        if old_memory is None or new_memory is None:
                            raise KeyError(
                                f"unknown supersession target: {old_id}->{new_id}"
                            )
                        old_memory["status"] = "superseded"
                        old_memory["version"] = int(old_memory.get("version", 1)) + 1
                        new_memory["status"] = "active"
                        new_memory["supersedes_id"] = old_id
                        new_memory["version"] = int(new_memory.get("version", 1)) + 1
                        supersession_updates += 1
                    txn["writes"].clear()
                    txn["edges"].clear()
                    txn["supersessions"].clear()
                    txn["status"] = "committed"
                    transaction_states[str(operation["txn_id"])] = "committed"
                    transaction_state = "committed"

        elif op_type in {"derive", "propagate"}:
            source_ids = list(operation.get("source_ids", []))
            if op_type == "propagate" and not source_ids and operation.get("source_id"):
                source_ids = [operation["source_id"]]
            normalized_operation = {**operation, "source_ids": source_ids}
            for source_id in source_ids:
                own_source = txn is not None and source_id in txn["writes"]
                source = txn["writes"].get(source_id) if own_source else memories.get(source_id)
                if source is None:
                    raise KeyError(f"unknown source memory_id: {source_id}")
                if not own_source:
                    snapshot_committed_read(txn, source_id)
            memory = stage_write(normalized_operation, txn, action=op_type)
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
                if txn is None:
                    raise ValueError("transactional derive requires txn_id")
                txn["edges"].extend(operation_edges)
            else:
                provenance_edges.extend(operation_edges)

        elif op_type == "invalidate":
            memory_id = operation["memory_id"]
            invalidate_committed(memory_id)
            if txn is not None:
                txn["status"] = transaction_state
                transaction_states[str(operation["txn_id"])] = transaction_state

        post_events = events_for_operation(instance, operation, "after")
        for event in post_events:
            if not is_crash(event):
                apply_non_crash_event(event, step)
        if any(is_crash(event) for event in post_events):
            abort_active_transactions()
            trace.append({"step": step, "event": "crash", "phase": "after"})
            break

    for txn_id, txn in transactions.items():
        if txn["status"] != "active":
            continue
        if txn["writes"] or txn["edges"] or txn["supersessions"]:
            abort_transaction(txn_id)
        else:
            txn["status"] = "completed"
            transaction_states[txn_id] = "completed"
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
        "read_observations": read_observations,
        "transaction_write_sets": {
            txn_id: list(memory_ids)
            for txn_id, memory_ids in transaction_write_sets.items()
        },
        "metrics": {
            "operation_count": len(trace),
            "repair_count": repair_count,
            "policy_version_at_end": max(policy_versions.values() or [1]),
            "exposed_memory_ids": exposed_memory_ids,
            "denied_reads": denied_reads,
            "supersession_updates": supersession_updates,
            "conflict_abort_count": conflict_abort_count,
        },
    }
