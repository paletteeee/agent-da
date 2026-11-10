"""Direct invariant checks for concurrent shared-memory executions."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any


MULTI_AGENT_VIOLATION_ORDER = (
    "dirty_read_violation",
    "atomicity_violation",
    "cross_txn_flush_violation",
    "actor_authorization_violation",
    "actor_scope_violation",
    "conflict_consistency_violation",
    "cross_agent_provenance_violation",
    "recovery_consistency_violation",
)

_WRITE_TYPES = {"write", "stage_write", "derive", "propagate"}


def _operation_positions(instance: dict[str, Any]) -> dict[str, int]:
    return {
        str(operation.get("op_id")): index
        for index, operation in enumerate(instance.get("operations", []))
    }


def _memory_metadata(instance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metadata = {
        str(memory["memory_id"]): {
            **memory,
            "producer_txn_id": None,
            "producer_action": None,
        }
        for memory in instance.get("initial_memories", [])
    }
    for operation in instance.get("operations", []):
        if operation.get("type") not in _WRITE_TYPES:
            continue
        memory_id = operation.get("memory_id") or operation.get("output_id")
        if not memory_id:
            continue
        metadata[str(memory_id)] = {
            **metadata.get(str(memory_id), {}),
            **operation,
            "memory_id": str(memory_id),
            "producer_txn_id": operation.get("txn_id"),
            "producer_action": operation.get("type"),
            "scope": operation.get("scope", "tenant:user_001"),
            "entity_id": operation.get("entity_id", "user_001"),
            "attribute": operation.get("attribute", "fact"),
        }
    return metadata


def _event_position(
    event: dict[str, Any], positions: dict[str, int]
) -> float | None:
    trigger = event.get("trigger")
    if isinstance(trigger, dict):
        if trigger.get("before_operation") in positions:
            return positions[str(trigger["before_operation"])] - 0.25
        if trigger.get("after_operation") in positions:
            return positions[str(trigger["after_operation"])] + 0.25
    if isinstance(event.get("step"), int):
        return float(event["step"])
    return None


def _dirty_read(
    instance: dict[str, Any], result: dict[str, Any], metadata: dict[str, dict[str, Any]]
) -> bool:
    positions = _operation_positions(instance)
    commit_positions = {
        str(operation.get("txn_id")): positions[str(operation["op_id"])]
        for operation in instance.get("operations", [])
        if operation.get("type") == "commit" and operation.get("txn_id")
    }
    for observation in result.get("read_observations", []):
        observation_position = positions.get(str(observation.get("operation_id")))
        reader_txn = observation.get("txn_id")
        if observation_position is None:
            continue
        for memory_id in observation.get("visible_memory_ids", []):
            producer_txn = metadata.get(str(memory_id), {}).get("producer_txn_id")
            if producer_txn is None or producer_txn == reader_txn:
                continue
            producer_commit = commit_positions.get(str(producer_txn))
            if producer_commit is None or producer_commit > observation_position:
                return True
    return False


def _actor_authorization(
    instance: dict[str, Any],
    result: dict[str, Any],
    metadata: dict[str, dict[str, Any]],
) -> bool:
    positions = _operation_positions(instance)
    commits = {
        str(operation.get("txn_id")): positions[str(operation["op_id"])]
        for operation in instance.get("operations", [])
        if operation.get("type") == "commit" and operation.get("txn_id")
    }
    operations_by_txn: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for operation in instance.get("operations", []):
        if operation.get("txn_id"):
            operations_by_txn[str(operation["txn_id"])].append(operation)

    for txn_id, state in result.get("transaction_states", {}).items():
        if state != "committed" or txn_id not in commits:
            continue
        transaction_operations = operations_by_txn.get(str(txn_id), [])
        actor = next(
            (
                str(operation.get("agent_id"))
                for operation in transaction_operations
                if operation.get("agent_id")
            ),
            "agent_1",
        )
        for event in instance.get("failure_schedule", []):
            if (event.get("type") or event.get("action")) not in {
                "revoke",
                "policy_change",
            }:
                continue
            event_at = _event_position(event, positions)
            if event_at is None or event_at > commits[str(txn_id)]:
                continue
            if event.get("agent_id") not in {None, actor}:
                continue
            revoked_action = str(event.get("target") or event.get("action"))
            revoked_scope = event.get("scope")
            for operation in transaction_operations:
                action = str(operation.get("type"))
                if action not in _WRITE_TYPES | {"supersede"}:
                    continue
                if revoked_action not in {action, "policy_change", "*"}:
                    continue
                if revoked_scope is not None and operation.get(
                    "scope", "tenant:user_001"
                ) != revoked_scope:
                    continue
                return True
    return False


def _actor_scope(
    instance: dict[str, Any], result: dict[str, Any], metadata: dict[str, dict[str, Any]]
) -> bool:
    operations = {
        str(operation.get("op_id")): operation
        for operation in instance.get("operations", [])
    }
    for observation in result.get("read_observations", []):
        operation = operations.get(str(observation.get("operation_id")))
        if operation is None:
            continue
        requested_scope = operation.get("scope")
        actor = operation.get("agent_id")
        action = "search" if operation.get("type") == "search" else "read"
        for memory_id in observation.get("visible_memory_ids", []):
            memory_scope = metadata.get(str(memory_id), {}).get("scope")
            if (
                requested_scope is not None
                and memory_scope is not None
                and requested_scope != memory_scope
            ):
                return True
            matching_policies = [
                policy
                for policy in instance.get("policies", [])
                if policy.get("agent_id") in {None, actor}
                and policy.get("action") == action
                and policy.get("scope") in {None, memory_scope}
                and int(policy.get("effective_step", 0))
                <= int(operation.get("step", 0))
            ]
            if matching_policies and not any(
                policy.get("effect") == "allow" for policy in matching_policies
            ):
                return True
    return False


def _conflict_consistency(
    instance: dict[str, Any], result: dict[str, Any]
) -> bool:
    active = {
        memory_id: memory
        for memory_id, memory in result.get("final_memories", {}).items()
        if memory.get("status") == "active"
    }
    supersession_groups: dict[str, list[str]] = defaultdict(list)
    for memory_id, memory in active.items():
        if memory.get("supersedes_id"):
            supersession_groups[str(memory["supersedes_id"])].append(memory_id)
    if any(len(memory_ids) > 1 for memory_ids in supersession_groups.values()):
        return True

    operations = {
        str(operation.get("memory_id") or operation.get("output_id")): operation
        for operation in instance.get("operations", [])
        if operation.get("memory_id") or operation.get("output_id")
    }
    base_groups: dict[tuple[str, str, int], list[str]] = defaultdict(list)
    for memory_id in active:
        operation = operations.get(str(memory_id), {})
        if operation.get("base_version") is None:
            continue
        key = (
            str(operation.get("entity_id", "user_001")),
            str(operation.get("attribute", "fact")),
            int(operation["base_version"]),
        )
        base_groups[key].append(memory_id)
    return any(len(memory_ids) > 1 for memory_ids in base_groups.values())


def _cross_agent_provenance(result: dict[str, Any]) -> bool:
    children: dict[str, list[str]] = defaultdict(list)
    for edge in result.get("provenance_edges", []):
        children[str(edge["source_id"])].append(str(edge["derived_id"]))
    memories = result.get("final_memories", {})
    for root_id, root in memories.items():
        if root.get("status") != "invalid":
            continue
        queue = deque(children.get(str(root_id), []))
        visited: set[str] = set()
        while queue:
            descendant_id = queue.popleft()
            if descendant_id in visited:
                continue
            visited.add(descendant_id)
            if memories.get(descendant_id, {}).get("status") == "active":
                return True
            queue.extend(children.get(descendant_id, []))
    return False


def _recovery_consistency(instance: dict[str, Any], result: dict[str, Any]) -> bool:
    write_sets = result.get("transaction_write_sets", {})
    committed = set(result.get("committed_memory_ids", []))
    states = result.get("transaction_states", {})
    for event in instance.get("failure_schedule", []):
        if (event.get("type") or event.get("action")) not in {
            "crash",
            "crash_during_commit",
        }:
            continue
        txn_id = str(event.get("target"))
        if txn_id not in write_sets:
            continue
        write_ids = set(write_sets[txn_id])
        visible_count = len(write_ids & committed)
        phase = str(event.get("phase") or "")
        if phase == "before_linearize" and visible_count:
            return True
        if 0 < visible_count < len(write_ids):
            return True
        if (
            phase == "after_linearize"
            and states.get(txn_id) == "committed"
            and visible_count != len(write_ids)
        ):
            return True
    return False


def check_multi_agent_invariants(
    instance: dict[str, Any], result: dict[str, Any]
) -> list[str]:
    """Return ordered violations without consulting the reference executor."""

    violations: list[str] = []
    metadata = _memory_metadata(instance)
    write_sets = result.get("transaction_write_sets", {})
    committed = set(result.get("committed_memory_ids", []))
    states = result.get("transaction_states", {})

    if _dirty_read(instance, result, metadata):
        violations.append("dirty_read_violation")
    if any(
        0 < len(set(write_ids) & committed) < len(set(write_ids))
        for write_ids in write_sets.values()
        if write_ids
    ):
        violations.append("atomicity_violation")
    if any(
        states.get(txn_id) != "committed" and bool(set(write_ids) & committed)
        for txn_id, write_ids in write_sets.items()
    ):
        violations.append("cross_txn_flush_violation")
    if _actor_authorization(instance, result, metadata):
        violations.append("actor_authorization_violation")
    if _actor_scope(instance, result, metadata):
        violations.append("actor_scope_violation")
    if _conflict_consistency(instance, result):
        violations.append("conflict_consistency_violation")
    if _cross_agent_provenance(result):
        violations.append("cross_agent_provenance_violation")
    if _recovery_consistency(instance, result):
        violations.append("recovery_consistency_violation")
    return [name for name in MULTI_AGENT_VIOLATION_ORDER if name in violations]
