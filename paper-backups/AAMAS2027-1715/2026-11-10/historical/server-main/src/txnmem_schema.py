"""Schema validation and configuration loading for TxnMemBench instances."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


KNOWN_WORKLOADS = (
    "atomic_multi_write",
    "crash_during_commit",
    "revoke_before_commit",
    "scope_bypass",
    "supersession_consistency",
    "provenance_chain_repair",
    "provenance_branch_repair",
    "mixed_stress",
    "trace_grounded_replay",
)
CONFIG_WORKLOADS = KNOWN_WORKLOADS[:-1]

REQUIRED_INSTANCE_KEYS = (
    "instance_id",
    "workload",
    "seed",
    "config",
    "initial_memories",
    "operations",
    "policies",
    "failure_schedule",
    "provenance_edges",
)

DEFAULT_CONFIG: dict[str, Any] = {
    "agent_count": 2,
    "txn_size": 2,
    "provenance_depth": 2,
    "branch_factor": 1,
    "concurrency": 1,
    "policy_churn": 0,
}


def _load_yaml_fallback(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise ValueError(
            "configuration is not JSON-compatible YAML and PyYAML is unavailable"
        ) from exc
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("workload configuration must be a mapping")
    return loaded


def load_workload_config(path: Path) -> dict[str, Any]:
    """Load a JSON-compatible YAML config without requiring PyYAML."""

    text = path.read_text(encoding="utf-8")
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        loaded = _load_yaml_fallback(path)
    if not isinstance(loaded, dict) or not isinstance(loaded.get("workloads"), dict):
        raise ValueError("workload configuration must contain a workloads mapping")
    missing = set(CONFIG_WORKLOADS) - set(loaded["workloads"])
    if missing:
        raise ValueError(f"workload configuration is missing: {sorted(missing)}")
    return loaded


def _require_list(instance: dict[str, Any], name: str) -> list[Any]:
    value = instance.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    return value


def validate_instance(instance: dict[str, Any]) -> None:
    """Validate references and structural invariants of one instance."""

    missing = [key for key in REQUIRED_INSTANCE_KEYS if key not in instance]
    if missing:
        raise ValueError(f"missing instance keys: {missing}")
    workload = instance["workload"]
    if workload not in KNOWN_WORKLOADS:
        raise ValueError(f"unsupported workload: {workload}")
    if not isinstance(instance["seed"], int):
        raise ValueError("seed must be an integer")
    config = instance["config"]
    if not isinstance(config, dict):
        raise ValueError("config must be a mapping")
    for name in ("agent_count", "txn_size", "provenance_depth", "branch_factor", "concurrency"):
        if name in config and int(config[name]) < 1:
            raise ValueError(f"{name} must be >= 1")
    if "policy_churn" in config and int(config["policy_churn"]) < 0:
        raise ValueError("policy_churn must be >= 0")

    initial_memories = _require_list(instance, "initial_memories")
    operations = _require_list(instance, "operations")
    _require_list(instance, "policies")
    _require_list(instance, "failure_schedule")
    edges = _require_list(instance, "provenance_edges")
    if "expected_outcome" in instance and not isinstance(instance["expected_outcome"], dict):
        raise ValueError("expected_outcome must be a mapping")

    memory_ids: set[str] = set()
    for memory in initial_memories:
        if not isinstance(memory, dict) or not memory.get("memory_id"):
            raise ValueError("each initial memory must have a memory_id")
        memory_id = str(memory["memory_id"])
        if memory_id in memory_ids:
            raise ValueError(f"duplicate memory_id: {memory_id}")
        memory_ids.add(memory_id)

    operation_ids: set[str] = set()
    last_step = -1
    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError("each operation must be a mapping")
        op_id = operation.get("op_id")
        if not op_id or op_id in operation_ids:
            raise ValueError(f"duplicate or missing op_id: {op_id}")
        operation_ids.add(op_id)
        step = operation.get("step")
        if not isinstance(step, int) or step < last_step:
            raise ValueError("operation steps must be non-decreasing integers")
        last_step = step
        operation_type = operation.get("type")
        output_id = operation.get("memory_id") or operation.get("output_id")
        if operation_type in {"write", "stage_write", "derive", "propagate"} and output_id:
            memory_ids.add(str(output_id))
        source_ids = operation.get("source_ids", [])
        if source_ids is not None and not isinstance(source_ids, list):
            raise ValueError("operation source_ids must be a list")
        for source_id in source_ids or []:
            if source_id not in memory_ids:
                raise ValueError(f"unknown operation source reference: {source_id}")
        requested_id = operation.get("memory_id")
        if operation_type in {"read", "get_by_id", "invalidate"} and requested_id and requested_id not in memory_ids:
            raise ValueError(f"unknown operation memory reference: {requested_id}")

    for event in instance["failure_schedule"]:
        if not isinstance(event, dict):
            raise ValueError("each failure event must be a mapping")
        trigger = event.get("trigger")
        if trigger is not None:
            if not isinstance(trigger, dict) or set(trigger) - {"before_operation", "after_operation"}:
                raise ValueError("failure trigger must use before_operation or after_operation")
            operation_ids = set(operation_ids)
            if not any(value in operation_ids for value in trigger.values()):
                raise ValueError("failure trigger references an unknown operation")
        elif "step" not in event:
            raise ValueError("failure event requires trigger or step")

    for edge in edges:
        if not isinstance(edge, dict):
            raise ValueError("each provenance edge must be a mapping")
        source_id = edge.get("source_id")
        derived_id = edge.get("derived_id")
        if source_id not in memory_ids or derived_id not in memory_ids:
            raise ValueError(f"unknown provenance reference: {source_id}->{derived_id}")
