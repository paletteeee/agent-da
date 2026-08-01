"""Trace-grounded adaptation helpers.

The adapter consumes a small normalized event contract rather than pretending
that τ-bench, AppWorld, or LoCoMo already contain TxnMem provenance ground
truth.  Dataset-specific connectors can map their native logs into this
contract without changing the reference executor.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Iterable


KIND_TO_TYPE = {
    "memory_read": "read",
    "read": "read",
    "memory_search": "search",
    "search": "search",
    "memory_write": "write",
    "write": "write",
    "memory_derive": "derive",
    "derive": "derive",
    "memory_propagate": "propagate",
    "propagate": "propagate",
    "memory_supersede": "supersede",
    "supersede": "supersede",
    "begin_txn": "begin_txn",
    "commit": "commit",
}


def normalize_trace(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for index, event in enumerate(events, start=1):
        kind = str(event.get("kind") or event.get("type") or "")
        operation_type = KIND_TO_TYPE.get(kind)
        if operation_type is None:
            continue
        operation: dict[str, Any] = {
            "op_id": str(event.get("event_id") or f"trace_op_{index:04d}"),
            "step": index,
            "agent_id": event.get("agent_id", "agent_1"),
            "txn_id": event.get("txn_id") or event.get("transaction_id", "txn_trace"),
            "type": operation_type,
        }
        for field in (
            "memory_id",
            "output_id",
            "source_id",
            "source_ids",
            "old_memory_id",
            "new_memory_id",
            "scope",
            "target_scope",
            "value",
            "query",
            "attribute",
            "content",
            "tool_name",
            "projection",
            "raw_event_id",
            "task_id",
            "sample_id",
            "session_id",
        ):
            if field in event:
                operation[field] = event[field]
        operations.append(operation)
    return operations


def _initial_memories(events: list[dict[str, Any]], operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    outputs = {
        operation.get("memory_id") or operation.get("output_id")
        for operation in operations
        if operation.get("type") in {"write", "derive", "propagate"}
    }
    referenced: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for operation in operations:
        ids = []
        if operation.get("type") in {"read", "search", "get_by_id"}:
            ids.append(operation.get("memory_id"))
        ids.extend(operation.get("source_ids", []))
        if operation.get("source_id"):
            ids.append(operation["source_id"])
        for memory_id in ids:
            if memory_id and memory_id not in outputs and memory_id not in referenced:
                referenced[memory_id] = {
                    "memory_id": memory_id,
                    "agent_id": operation.get("agent_id", "agent_1"),
                    "scope": operation.get("scope", "tenant:user_001"),
                    "status": "active",
                    "value": memory_id,
                    "derived_from": [],
                }
    return list(referenced.values())


def trace_to_instance(
    events: Iterable[dict[str, Any]], instance_id: str, seed: int = 0
) -> dict[str, Any]:
    materialized = list(events)
    operations = normalize_trace(materialized)
    operation_ids = [operation["op_id"] for operation in operations]
    failure_schedule: list[dict[str, Any]] = []
    operation_index = 0
    for event in materialized:
        kind = str(event.get("kind") or event.get("type") or "")
        action = {
            "policy_revoke": "revoke",
            "policy_change": "revoke",
            "revoke": "revoke",
            "crash": "crash",
            "delay": "delay",
            "invalidate": "invalidate",
        }.get(kind)
        if kind in KIND_TO_TYPE:
            operation_index += 1
        if action is None:
            continue
        event_record: dict[str, Any] = {
            "trigger": {"after_operation": operation_ids[max(0, operation_index - 1)]}
            if operation_ids
            else {"before_operation": "trace_op_0001"},
            "type": action,
        }
        if event.get("target") is not None:
            event_record["target"] = event["target"]
        if action == "revoke":
            event_record.setdefault("target", "write")
            event_record["phase"] = "before_validate"
        failure_schedule.append(event_record)
    agent_ids = sorted({operation.get("agent_id", "agent_1") for operation in operations}) or ["agent_1"]
    policies = []
    for action in ("read", "search", "write", "derive", "propagate", "supersede"):
        policies.append(
            {
                "policy_id": f"trace_{action}",
                "version": 1,
                "agent_id": agent_ids[0],
                "action": action,
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            }
        )
    return {
        "instance_id": instance_id,
        "workload": "trace_grounded_replay",
        "seed": int(seed),
        "config": {"agent_count": len(agent_ids), "txn_size": 1, "provenance_depth": 1, "branch_factor": 1, "concurrency": 1, "policy_churn": 0},
        "initial_memories": _initial_memories(materialized, operations),
        "operations": operations,
        "policies": policies,
        "failure_schedule": failure_schedule,
        "provenance_edges": [],
    }
