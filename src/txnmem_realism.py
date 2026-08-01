"""Feature extraction and distribution comparison for trace-grounded realism checks."""

from __future__ import annotations

from collections import defaultdict
from statistics import mean, pstdev
from typing import Any, Iterable


FEATURES = (
    "operation_count",
    "transaction_size",
    "policy_change_rate",
    "provenance_depth",
    "branch_factor",
    "agent_count",
)


def _graph_features(operations: list[dict[str, Any]]) -> tuple[int, int]:
    parents: dict[str, list[str]] = defaultdict(list)
    for operation in operations:
        if operation.get("type") not in {"derive", "propagate"}:
            continue
        output_id = operation.get("memory_id") or operation.get("output_id")
        source_ids = list(operation.get("source_ids", []))
        if operation.get("type") == "propagate" and not source_ids and operation.get("source_id"):
            source_ids = [operation["source_id"]]
        for source_id in source_ids:
            if output_id:
                parents[str(output_id)].append(str(source_id))
    children: dict[str, list[str]] = defaultdict(list)
    for child, sources in parents.items():
        for source in sources:
            children[source].append(child)

    def depth(node: str, visiting: set[str]) -> int:
        if node in visiting:
            return 0
        visiting.add(node)
        value = 1 + max((depth(child, visiting) for child in children.get(node, [])), default=0)
        visiting.remove(node)
        return value

    graph_depth = max((depth(root, set()) for root in children), default=0)
    max_branch = max((len(children[node]) for node in children), default=0)
    return max(0, graph_depth - 1), max_branch


def extract_trace_features(
    operations: Iterable[dict[str, Any]], failure_schedule: Iterable[dict[str, Any]]
) -> dict[str, float | int]:
    operations = list(operations)
    failures = list(failure_schedule)
    transaction_counts: dict[str, int] = defaultdict(int)
    for operation in operations:
        if operation.get("txn_id") and operation.get("type") in {
            "write", "stage_write", "derive", "propagate", "supersede"
        }:
            transaction_counts[str(operation["txn_id"])] += 1
    depth, branch_factor = _graph_features(operations)
    return {
        "operation_count": len(operations),
        "transaction_size": max(transaction_counts.values(), default=0),
        "policy_change_rate": sum(
            1 for event in failures if event.get("type") in {"revoke", "policy_change"}
        ) / max(1, len(operations)),
        "provenance_depth": depth,
        "branch_factor": branch_factor,
        "agent_count": len({operation.get("agent_id") for operation in operations if operation.get("agent_id")}),
    }


def compare_distributions(
    synthetic: Iterable[dict[str, Any]], trace_grounded: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    left = list(synthetic)
    right = list(trace_grounded)
    features: dict[str, Any] = {}
    for name in FEATURES:
        left_values = [float(item.get(name, 0.0)) for item in left]
        right_values = [float(item.get(name, 0.0)) for item in right]
        left_mean = mean(left_values) if left_values else 0.0
        right_mean = mean(right_values) if right_values else 0.0
        features[name] = {
            "synthetic_mean": left_mean,
            "synthetic_std": pstdev(left_values) if left_values else 0.0,
            "trace_mean": right_mean,
            "trace_std": pstdev(right_values) if right_values else 0.0,
            "mean_abs_diff": abs(left_mean - right_mean),
        }
    return {
        "synthetic_count": len(left),
        "trace_count": len(right),
        "features": features,
    }
