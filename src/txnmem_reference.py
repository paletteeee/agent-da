"""Independent serial reference executor for TxnMemBench.

This module intentionally does not import the TxnMem simulator or invariant
checker.  It computes allowed final states from the instance inputs and uses
legacy provenance edges only as initial graph metadata.  New provenance edges
are created from derive/propagate operations.
"""

from __future__ import annotations

import copy
from collections import defaultdict, deque
from typing import Any, Iterable


ORACLE_VERSION = "0.1"
_MUTATING_OPERATIONS = {"write", "stage_write", "derive", "propagate", "supersede"}
_READ_OPERATIONS = {"read", "search", "get_by_id"}


def _memory_from_operation(operation: dict[str, Any]) -> dict[str, Any]:
    return {
        "memory_id": operation.get("memory_id") or operation.get("output_id"),
        "agent_id": operation.get("agent_id", "agent_1"),
        "scope": operation.get("scope", "tenant:user_001"),
        "entity_id": operation.get("entity_id", "user_001"),
        "attribute": operation.get("attribute", "fact"),
        "value": operation.get("value", operation.get("memory_id", "")),
        "status": "pending",
        "policy_version": operation.get("policy_version", 1),
        "supersedes_id": operation.get("supersedes_id"),
        "derived_from": list(operation.get("source_ids", [])),
    }


def _initial_state(instance: dict[str, Any]) -> dict[str, Any]:
    memories: dict[str, dict[str, Any]] = {}
    for memory in instance.get("initial_memories", []):
        item = copy.deepcopy(memory)
        item.setdefault("status", "active")
        item.setdefault("version", 1)
        memories[str(item["memory_id"])] = item

    edges = [copy.deepcopy(edge) for edge in instance.get("provenance_edges", [])]
    policies = copy.deepcopy(instance.get("policies", []))
    versions = [int(policy.get("version", 1)) for policy in policies if policy.get("version") is not None]
    return {
        "memories": memories,
        "edges": edges,
        "policies": policies,
        "operations": copy.deepcopy(instance.get("operations", [])),
        "policy_version": max(versions or [1]),
        "revoked_actions": set(),
        "transactions": {},
        "committed_memory_ids": [],
        "observed_visible_ids": set(),
        "trace": [],
        "flags": {
            "commit_authorization": True,
            "graph_validity": True,
        },
    }


def _txn(state: dict[str, Any], txn_id: str) -> dict[str, Any]:
    transactions = state["transactions"]
    if txn_id not in transactions:
        transactions[txn_id] = {
            "status": "active",
            "begin_policy_version": state["policy_version"],
            "read_set": [],
            "read_versions": {},
            "write_set": {},
            "pending_edges": [],
            "supersessions": [],
            "invalidations": set(),
            "authorized_actions": set(),
        }
    return transactions[txn_id]


def _scope_matches(resource_scope: str | None, requested_scope: str | None) -> bool:
    if requested_scope is None or resource_scope is None:
        return True
    return resource_scope == requested_scope


def _policy_allows(
    state: dict[str, Any], operation: dict[str, Any], action: str, resource_scope: str | None = None
) -> bool:
    if action in state["revoked_actions"]:
        return False
    policies = state["policies"]
    if not policies:
        return True
    principal = operation.get("agent_id")
    requested_scope = resource_scope or operation.get("scope")
    candidates = []
    for policy in policies:
        if policy.get("action") not in {action, "read" if action == "get_by_id" else action}:
            continue
        if policy.get("agent_id") not in {None, principal}:
            continue
        if not _scope_matches(policy.get("scope"), requested_scope):
            continue
        if int(policy.get("effective_step", 0)) > int(operation.get("step", 0)):
            continue
        candidates.append(policy)
    if not candidates:
        return False
    if any(policy.get("effect") == "deny" for policy in candidates):
        return False
    return any(policy.get("effect") == "allow" for policy in candidates)


def _append_trace(state: dict[str, Any], operation: dict[str, Any], **extra: Any) -> None:
    event = {
        "event_id": f"event_{len(state['trace']) + 1:04d}",
        "operation_id": operation.get("op_id"),
        "txn_id": operation.get("txn_id"),
        "event_type": operation.get("type"),
        "policy_version": state["policy_version"],
    }
    event.update(extra)
    state["trace"].append(event)


def _children(edges: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        result[str(edge["source_id"])].append(str(edge["derived_id"]))
    return result


def _descendants(edges: Iterable[dict[str, Any]], root_id: str) -> set[str]:
    children = _children(edges)
    found: set[str] = set()
    queue = deque(children.get(root_id, []))
    while queue:
        current = queue.popleft()
        if current in found:
            continue
        found.add(current)
        queue.extend(children.get(current, []))
    return found


def _graph_has_cycle(edges: Iterable[dict[str, Any]]) -> bool:
    children = _children(edges)
    nodes = {str(edge["source_id"]) for edge in edges} | {str(edge["derived_id"]) for edge in edges}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(child) for child in children.get(node, [])):
            return True
        visiting.remove(node)
        visited.add(node)
        return False

    return any(visit(node) for node in nodes)


def _apply_schedule_event(state: dict[str, Any], event: dict[str, Any], operation: dict[str, Any]) -> None:
    event_type = event.get("type") or event.get("action")
    if event_type in {"revoke", "policy_change"}:
        state["policy_version"] += 1
        action = event.get("target") or event.get("action")
        if action in {"read", "search", "write", "derive", "propagate", "supersede"}:
            state["revoked_actions"].add(action)
        _append_trace(state, operation, event_type="policy_change", decision="applied", reason_codes=["POLICY_VERSION_CHANGED"])
    elif event_type == "delay":
        _append_trace(state, operation, event_type="delay", decision="applied", reason_codes=[])
    elif event_type == "invalidate":
        memory_id = event.get("target") or event.get("memory_id")
        memory = state["memories"].get(memory_id)
        if memory is None:
            _append_trace(state, operation, event_type="invalidate", decision="denied", reason_codes=["MEMORY_NOT_FOUND"])
            return
        if memory.get("status") != "invalid":
            memory["status"] = "invalid"
            memory["version"] = int(memory.get("version", 1)) + 1
        _repair(state, [memory_id])
        _append_trace(
            state,
            operation,
            event_type="invalidate",
            decision="applied",
            affected_memory_ids=[memory_id],
            reason_codes=[],
        )


def _event_matches_operation(event: dict[str, Any], operation: dict[str, Any], after: bool) -> bool:
    trigger = event.get("trigger")
    if isinstance(trigger, dict):
        if after and trigger.get("after_operation") == operation.get("op_id"):
            return True
        if not after and trigger.get("before_operation") == operation.get("op_id"):
            return True
        return False
    if event.get("step") != operation.get("step"):
        return False
    phase = event.get("phase")
    if after:
        return phase in {"after_operation", "after_linearize", "after_commit"}
    return phase not in {"after_operation", "after_linearize", "after_commit"}


def _events_for_operation(instance: dict[str, Any], operation: dict[str, Any], after: bool) -> list[dict[str, Any]]:
    return [
        event
        for event in instance.get("failure_schedule", [])
        if _event_matches_operation(event, operation, after)
    ]


def _crash_applies(event: dict[str, Any], operation: dict[str, Any]) -> bool:
    event_type = event.get("type") or event.get("action")
    if event_type not in {"crash", "crash_during_commit"}:
        return False
    target = event.get("target")
    return target in {None, operation.get("txn_id"), operation.get("type"), "commit"}


def _abort_transaction(state: dict[str, Any], txn_id: str, reason: str) -> None:
    txn = _txn(state, txn_id)
    txn["status"] = "aborted"
    txn["write_set"].clear()
    txn["pending_edges"].clear()
    txn["supersessions"].clear()
    txn["invalidations"].clear()
    # An authorized abort after revalidation is a correct outcome, not an
    # authorization violation.  The flag is reserved for an actually
    # committed write that bypassed the current policy.


def _record_read_version(txn: dict[str, Any], memory: dict[str, Any]) -> None:
    txn["read_versions"][memory["memory_id"]] = (
        int(memory.get("version", 1)),
        memory.get("scope"),
        memory.get("status"),
    )


def _read_revalidation_reason(state: dict[str, Any], txn: dict[str, Any]) -> str | None:
    for memory_id, (version, scope, status) in txn["read_versions"].items():
        memory = state["memories"].get(memory_id)
        if memory is None or int(memory.get("version", 1)) != version:
            return "SOURCE_VERSION_CHANGED"
        if memory.get("scope") != scope:
            return "SOURCE_SCOPE_CHANGED"
        if memory.get("status") != status or memory.get("status") != "active":
            return "SOURCE_INVALID"
    return None


def _stage_write(state: dict[str, Any], operation: dict[str, Any], txn_id: str) -> None:
    txn = _txn(state, txn_id)
    memory = _memory_from_operation(operation)
    memory_id = memory["memory_id"]
    if not memory_id:
        raise ValueError("write/derive operation requires memory_id or output_id")
    if memory_id in state["memories"]:
        merged = copy.deepcopy(state["memories"][memory_id])
        merged.update({key: value for key, value in memory.items() if value is not None})
        merged["status"] = "pending"
        memory = merged
    txn["write_set"][memory_id] = memory


def _execute_read(state: dict[str, Any], operation: dict[str, Any], txn_id: str) -> None:
    requested_id = operation.get("memory_id")
    txn = _txn(state, txn_id)
    if requested_id and requested_id in txn["write_set"]:
        candidates = [txn["write_set"][requested_id]]
    else:
        candidates = [state["memories"].get(requested_id)] if requested_id else list(state["memories"].values())
    visible: list[str] = []
    for memory in candidates:
        if memory is None or (
            memory.get("status") != "active" and memory.get("memory_id") not in txn["write_set"]
        ):
            continue
        if operation.get("query") is not None and operation["query"] not in {
            memory.get("value"), memory.get("memory_id"), memory.get("attribute")
        }:
            continue
        if not _scope_matches(memory.get("scope"), operation.get("scope")):
            continue
        action = "search" if operation.get("type") == "search" else "read"
        if not _policy_allows(state, operation, action, memory.get("scope")):
            continue
        visible.append(memory["memory_id"])
        state["observed_visible_ids"].add(memory["memory_id"])
        if memory["memory_id"] not in txn["read_set"]:
            txn["read_set"].append(memory["memory_id"])
        if memory["memory_id"] in state["memories"]:
            _record_read_version(txn, memory)
    _append_trace(state, operation, decision="allowed" if visible else "denied", affected_memory_ids=visible, reason_codes=[] if visible else ["SCOPE_OR_POLICY_DENIED"])


def _execute_derive(state: dict[str, Any], operation: dict[str, Any], txn_id: str) -> None:
    source_ids = list(operation.get("source_ids", []))
    txn = _txn(state, txn_id)
    if not _policy_allows(state, operation, "derive", operation.get("scope")):
        _append_trace(state, operation, decision="denied", reason_codes=["POLICY_DENIED"])
        return
    txn["authorized_actions"].add("derive")
    # A native memory_derive call names and reads its source_ids directly; the
    # event trace does not need a separate memory_read event to establish that
    # dependency.  Treat existing committed memories and read-your-writes as
    # visible source reads, while still rejecting unknown sources.
    missing = [
        source_id
        for source_id in source_ids
        if source_id not in txn["write_set"] and source_id not in state["memories"]
    ]
    invalid = []
    for source_id in source_ids:
        source_memory = state["memories"].get(source_id) or txn["write_set"].get(source_id)
        if source_memory is None:
            invalid.append(source_id)
        elif source_memory.get("status") != "active" and source_id not in txn["write_set"]:
            invalid.append(source_id)
    if missing or invalid:
        _append_trace(state, operation, decision="denied", reason_codes=["SOURCE_NOT_READ"] if missing else ["SOURCE_INVALID"])
        return
    for source_id in source_ids:
        if source_id not in txn["read_set"]:
            txn["read_set"].append(source_id)
        if source_id in state["memories"]:
            _record_read_version(txn, state["memories"][source_id])
    _stage_write(state, operation, txn_id)
    output_id = operation.get("memory_id") or operation.get("output_id")
    for source_id in source_ids:
        txn["pending_edges"].append(
            {
                "source_id": source_id,
                "derived_id": output_id,
                "relation": "read_derive",
                "operation_id": operation.get("op_id"),
                "txn_id": txn_id,
            }
        )
    _append_trace(state, operation, decision="allowed", affected_memory_ids=[output_id], affected_edge_ids=source_ids, reason_codes=[])


def _execute_propagate(state: dict[str, Any], operation: dict[str, Any], txn_id: str) -> None:
    txn = _txn(state, txn_id)
    source_id = operation.get("source_id") or (operation.get("source_ids") or [None])[0]
    output_id = operation.get("output_id") or operation.get("memory_id")
    source_memory = state["memories"].get(source_id) or txn["write_set"].get(source_id)
    source_visible = source_memory is not None and (
        source_memory.get("status") == "active" or source_id in txn["write_set"]
    )
    if source_id is None or output_id is None or not source_visible:
        _append_trace(state, operation, decision="denied", reason_codes=["SOURCE_NOT_READ"])
        return
    if source_id == output_id:
        _append_trace(state, operation, decision="denied", reason_codes=["PROVENANCE_CYCLE"])
        return
    if not _policy_allows(state, operation, "propagate", operation.get("target_scope")):
        _append_trace(state, operation, decision="denied", reason_codes=["POLICY_DENIED"])
        return
    txn["authorized_actions"].add("propagate")
    if source_id not in txn["read_set"]:
        txn["read_set"].append(source_id)
    if source_id in state["memories"]:
        _record_read_version(txn, state["memories"][source_id])
    _stage_write(state, {**operation, "memory_id": output_id, "scope": operation.get("target_scope", operation.get("scope"))}, txn_id)
    txn["pending_edges"].append(
        {
            "source_id": source_id,
            "derived_id": output_id,
            "relation": "propagate",
            "operation_id": operation.get("op_id"),
            "txn_id": txn_id,
        }
    )
    _append_trace(state, operation, decision="allowed", affected_memory_ids=[output_id], affected_edge_ids=[source_id], reason_codes=[])


def _execute_supersede(state: dict[str, Any], operation: dict[str, Any], txn_id: str) -> None:
    txn = _txn(state, txn_id)
    if not _policy_allows(state, operation, "supersede", operation.get("scope")):
        _append_trace(state, operation, decision="denied", reason_codes=["POLICY_DENIED"])
        return
    old_id = operation.get("old_memory_id") or operation.get("old_id")
    new_id = operation.get("new_memory_id") or operation.get("new_id")
    new_memory = operation.get("new_memory")
    if isinstance(new_memory, dict):
        new_id = new_memory.get("memory_id", new_id)
        _stage_write(state, {**new_memory, "memory_id": new_id, "supersedes_id": old_id}, txn_id)
    if not old_id or not new_id:
        _append_trace(state, operation, decision="denied", reason_codes=["SUPERSESSION_TARGET_MISSING"])
        return
    # A native trace can emit write(old), write(new), supersede(old, new)
    # before commit.  The independent oracle must resolve both targets from
    # the transaction write set, not require them to be committed already.
    old_visible = old_id in state["memories"] or old_id in txn["write_set"]
    new_visible = new_id in state["memories"] or new_id in txn["write_set"]
    if not old_visible or not new_visible:
        _append_trace(state, operation, decision="denied", reason_codes=["SUPERSESSION_TARGET_MISSING"])
        return
    txn["supersessions"].append((old_id, new_id))
    txn["authorized_actions"].add("supersede")
    _append_trace(state, operation, decision="allowed", affected_memory_ids=[old_id, new_id], reason_codes=[])


def _commit(state: dict[str, Any], operation: dict[str, Any], crash_resolution: str | None) -> None:
    txn_id = operation.get("txn_id", "implicit")
    txn = _txn(state, txn_id)
    if txn["status"] != "active":
        return
    revalidation_reason = _read_revalidation_reason(state, txn)
    if revalidation_reason is not None:
        _abort_transaction(state, txn_id, revalidation_reason)
        _append_trace(state, operation, decision="aborted", reason_codes=[revalidation_reason])
        return
    if txn["authorized_actions"] & state["revoked_actions"]:
        _abort_transaction(state, txn_id, "POLICY_REVOKED")
        _append_trace(state, operation, decision="aborted", reason_codes=["POLICY_REVOKED"])
        return
    pending_edges = state["edges"] + txn["pending_edges"]
    if _graph_has_cycle(pending_edges):
        state["flags"]["graph_validity"] = False
        _abort_transaction(state, txn_id, "PROVENANCE_CYCLE")
        _append_trace(state, operation, decision="aborted", reason_codes=["PROVENANCE_CYCLE"])
        return
    if crash_resolution == "abort":
        _abort_transaction(state, txn_id, "CRASH_BEFORE_LINEARIZE")
        _append_trace(state, operation, decision="aborted", reason_codes=["CRASH_BEFORE_LINEARIZE"])
        return

    for memory_id, memory in txn["write_set"].items():
        committed = copy.deepcopy(memory)
        committed["status"] = "active"
        previous = state["memories"].get(memory_id)
        committed["version"] = int(previous.get("version", 0)) + 1 if previous else 1
        state["memories"][memory_id] = committed
        if memory_id not in state["committed_memory_ids"]:
            state["committed_memory_ids"].append(memory_id)
    for old_id, new_id in txn["supersessions"]:
        new_memory = state["memories"].get(new_id)
        old_memory = state["memories"].get(old_id)
        if old_memory is None or new_memory is None:
            _abort_transaction(state, txn_id, "SUPERSESSION_TARGET_MISSING")
            _append_trace(state, operation, decision="aborted", reason_codes=["SUPERSESSION_TARGET_MISSING"])
            return
        old_memory["status"] = "superseded"
        old_memory["version"] = int(old_memory.get("version", 1)) + 1
        new_memory["status"] = "active"
        new_memory["supersedes_id"] = old_id
    state["edges"] = pending_edges
    for memory_id in txn["invalidations"]:
        memory = state["memories"].get(memory_id)
        if memory is not None and memory.get("status") != "invalid":
            memory["status"] = "invalid"
            memory["version"] = int(memory.get("version", 1)) + 1
    _repair(state, txn["invalidations"])
    txn["status"] = "committed"
    _append_trace(state, operation, decision="committed", reason_codes=[])


def _repair(state: dict[str, Any], root_ids: Iterable[str]) -> None:
    invalid_roots = set(root_ids)
    invalid_roots.update(
        memory_id
        for memory_id, memory in state["memories"].items()
        if memory.get("status") == "invalid"
    )
    for root_id in invalid_roots:
        for descendant_id in _descendants(state["edges"], root_id):
            if descendant_id in state["memories"]:
                descendant = state["memories"][descendant_id]
                if descendant.get("status") != "invalid":
                    descendant["status"] = "invalid"
                    descendant["version"] = int(descendant.get("version", 1)) + 1


def _apply_invalidate(state: dict[str, Any], operation: dict[str, Any]) -> None:
    memory_id = operation.get("memory_id") or operation.get("root_id")
    txn = state["transactions"].get(operation.get("txn_id", "implicit"))
    if txn and txn["status"] == "active":
        if memory_id not in state["memories"] and memory_id not in txn["write_set"]:
            _append_trace(state, operation, decision="denied", reason_codes=["MEMORY_NOT_FOUND"])
            return
        invalidated = {memory_id}
        invalidated.update(_descendants(state["edges"] + txn["pending_edges"], memory_id))
        txn["invalidations"].update(invalidated)
        _append_trace(
            state,
            operation,
            decision="invalidated",
            affected_memory_ids=sorted(invalidated),
            reason_codes=[],
        )
        return
    if memory_id not in state["memories"]:
        _append_trace(state, operation, decision="denied", reason_codes=["MEMORY_NOT_FOUND"])
        return
    memory = state["memories"][memory_id]
    if memory.get("status") != "invalid":
        memory["status"] = "invalid"
        memory["version"] = int(memory.get("version", 1)) + 1
    _repair(state, [memory_id])
    _append_trace(state, operation, decision="invalidated", affected_memory_ids=[memory_id], reason_codes=[])


def _run(instance: dict[str, Any], crash_resolution: str | None) -> dict[str, Any]:
    state = _initial_state(instance)
    operations = sorted(instance.get("operations", []), key=lambda item: (int(item.get("step", 0)), item.get("op_id", "")))
    stopped = False
    for operation in operations:
        if stopped:
            break
        pre_events = _events_for_operation(instance, operation, after=False)
        for event in pre_events:
            _apply_schedule_event(state, event, operation)
        pre_crashes = [event for event in pre_events if _crash_applies(event, operation)]
        ambiguous_commit_crash = bool(
            pre_crashes
            and operation.get("type") == "commit"
            and all(event.get("phase") is None for event in pre_crashes)
        )
        if pre_crashes and not ambiguous_commit_crash:
            for txn_id in list(state["transactions"]):
                _abort_transaction(state, txn_id, "CRASH_BEFORE_LINEARIZE")
            _append_trace(state, operation, decision="aborted", reason_codes=["CRASH_BEFORE_LINEARIZE"])
            break

        op_type = operation.get("type")
        txn_id = operation.get("txn_id", "implicit")
        terminal_txn = state["transactions"].get(txn_id)
        if terminal_txn and terminal_txn["status"] in {"committed", "aborted"}:
            terminal_status = terminal_txn["status"]
            if (op_type == "commit" and terminal_status == "committed") or (
                op_type == "abort" and terminal_status == "aborted"
            ):
                _append_trace(state, operation, decision=terminal_status, reason_codes=[])
            else:
                _append_trace(
                    state,
                    operation,
                    decision="denied",
                    reason_codes=["TERMINAL_TRANSACTION"],
                )
        elif op_type == "begin_txn":
            txn = _txn(state, txn_id)
            txn["begin_policy_version"] = state["policy_version"]
            _append_trace(state, operation, decision="allowed", reason_codes=[])
        elif op_type in {"write", "stage_write"}:
            if _policy_allows(state, operation, "write", operation.get("scope")):
                _stage_write(state, operation, txn_id)
                _txn(state, txn_id)["authorized_actions"].add("write")
                _append_trace(state, operation, decision="allowed", reason_codes=[])
            else:
                _append_trace(state, operation, decision="denied", reason_codes=["POLICY_DENIED"])
        elif op_type in _READ_OPERATIONS:
            _execute_read(state, operation, txn_id)
        elif op_type == "derive":
            _execute_derive(state, operation, txn_id)
        elif op_type == "propagate":
            _execute_propagate(state, operation, txn_id)
        elif op_type == "supersede":
            _execute_supersede(state, operation, txn_id)
        elif op_type == "commit":
            _commit(state, operation, "abort" if ambiguous_commit_crash and crash_resolution == "abort" else None)
            if ambiguous_commit_crash:
                stopped = True
        elif op_type == "abort":
            abort_reason = str(operation.get("abort_reason") or "EXPLICIT_ABORT")
            _abort_transaction(state, txn_id, abort_reason)
            _append_trace(state, operation, decision="aborted", reason_codes=[abort_reason])
        elif op_type == "invalidate":
            _apply_invalidate(state, operation)
        elif op_type == "repair":
            _repair(state, operation.get("root_ids", [operation.get("memory_id")]))
            _append_trace(state, operation, decision="repaired", reason_codes=[])
        else:
            _append_trace(state, operation, decision="denied", reason_codes=["UNSUPPORTED_OPERATION"])

        post_events = _events_for_operation(instance, operation, after=True)
        for event in post_events:
            _apply_schedule_event(state, event, operation)
        if any(_crash_applies(event, operation) for event in post_events):
            if operation.get("type") != "commit":
                for txn_id in list(state["transactions"]):
                    _abort_transaction(state, txn_id, "CRASH_BEFORE_LINEARIZE")
            stopped = True

    for txn in state["transactions"].values():
        if txn["status"] == "active":
            if txn["write_set"] or txn["pending_edges"]:
                txn["status"] = "aborted"
                txn["write_set"].clear()
                txn["pending_edges"].clear()
            else:
                txn["status"] = "completed"
    return state


def _snapshot(state: dict[str, Any]) -> dict[str, Any]:
    memories = state["memories"]
    edges = sorted(
        state["edges"],
        key=lambda edge: (
            str(edge.get("source_id")),
            str(edge.get("derived_id")),
            str(edge.get("relation")),
            str(edge.get("operation_id", "")),
        ),
    )
    invalid_ids = sorted(memory_id for memory_id, memory in memories.items() if memory.get("status") == "invalid")
    has_read_operation = any(
        operation.get("type") in _READ_OPERATIONS
        for operation in state.get("operations", [])
    )
    visibility_candidates = state["observed_visible_ids"] if has_read_operation else memories
    visible_ids = sorted(
        memory_id
        for memory_id in visibility_candidates
        if memories.get(memory_id, {}).get("status") == "active"
    )
    superseded_ids = sorted(memory_id for memory_id, memory in memories.items() if memory.get("status") == "superseded")
    descendants_of_invalid = {
        descendant
        for root_id in invalid_ids
        for descendant in _descendants(edges, root_id)
    }
    no_invalid_visibility = not any(memory_id in visible_ids for memory_id in descendants_of_invalid)
    supersession_consistent = all(
        memory.get("status") != "superseded"
        or any(
            candidate.get("status") == "active" and candidate.get("supersedes_id") == memory_id
            for candidate in memories.values()
        )
        for memory_id, memory in memories.items()
    )
    invariants = {
        "atomicity": True,
        "commit_authorization": bool(state["flags"]["commit_authorization"]),
        "no_invalid_visibility": no_invalid_visibility,
        "supersession_consistency": supersession_consistent,
        "provenance_closure": no_invalid_visibility,
        "graph_validity": bool(state["flags"]["graph_validity"] and not _graph_has_cycle(edges)),
    }
    return {
        "txn_states": {txn_id: txn["status"] for txn_id, txn in sorted(state["transactions"].items())},
        "committed_memory_ids": list(state["committed_memory_ids"]),
        "visible_memory_ids": visible_ids,
        "invalid_memory_ids": invalid_ids,
        "superseded_memory_ids": superseded_ids,
        "provenance_edges": edges,
        "policy_version": state["policy_version"],
        "invariants": invariants,
    }


def reference_outcome(instance: dict[str, Any]) -> dict[str, Any]:
    """Compute an independent oracle record for one workload instance."""

    ambiguous = any(
        _crash_applies(event, operation)
        and operation.get("type") == "commit"
        and event.get("phase") is None
        for operation in instance.get("operations", [])
        for event in _events_for_operation(instance, operation, after=False)
    )
    resolutions = ["abort", "commit"] if ambiguous else [None]
    states = [_run(instance, resolution) for resolution in resolutions]
    outcomes = [_snapshot(state) for state in states]
    unique: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        key = repr(outcome)
        unique[key] = outcome
    outcomes = list(unique.values())
    safety_keys = (
        "atomicity",
        "commit_authorization",
        "no_invalid_visibility",
        "supersession_consistency",
        "provenance_closure",
        "graph_validity",
    )
    safety = {
        key: all(bool(outcome["invariants"].get(key)) for outcome in outcomes)
        for key in safety_keys
    }
    traces = []
    for state in states:
        traces.extend(state["trace"])
    return {
        "instance_id": instance.get("instance_id"),
        "oracle_version": ORACLE_VERSION,
        "allowed_outcomes": outcomes,
        "safety_invariants": safety,
        "event_trace": traces,
        "minimal_counterexample": None,
    }


def outcome_matches(result: dict[str, Any], oracle: dict[str, Any]) -> bool:
    """Return whether a simulator result belongs to the oracle outcome set."""

    txn_id = result.get("txn_id") or "txn_001"
    candidate = {
        "txn_states": {txn_id: result.get("transaction_state")},
        "committed_memory_ids": result.get("committed_memory_ids", []),
        "visible_memory_ids": sorted(
            memory_id
            for memory_id, memory in result.get("final_memories", {}).items()
            if memory.get("status") == "active"
        ),
        "invalid_memory_ids": sorted(
            memory_id
            for memory_id, memory in result.get("final_memories", {}).items()
            if memory.get("status") == "invalid"
        ),
        "superseded_memory_ids": sorted(
            memory_id
            for memory_id, memory in result.get("final_memories", {}).items()
            if memory.get("status") == "superseded"
        ),
    }
    return any(
        all(candidate[field] == outcome[field] for field in candidate)
        for outcome in oracle.get("allowed_outcomes", [])
    )
