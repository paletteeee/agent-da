"""Feature extraction and distribution comparison for trace-grounded realism checks."""

from __future__ import annotations

from collections import defaultdict
import random
from statistics import mean, pstdev
from typing import Any, Iterable

from txnmem_schema import DEFAULT_CONFIG
from txnmem_workloads import WORKLOADS, generate_instance


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


def split_holdout(
    records: Iterable[dict[str, Any]], holdout_fraction: float = 0.2, seed: int = 0
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split trace records by episode when an episode key is available."""

    materialized = list(records)
    if not 0.0 <= holdout_fraction < 1.0:
        raise ValueError("holdout_fraction must be in [0, 1)")
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in materialized:
        if record.get("task_id") is not None and record.get("trial") is not None:
            key = f"{record['task_id']}:trial:{record['trial']}"
        else:
            key = str(
                record.get("episode_id")
                or record.get("task_id")
                or record.get("conversation_id")
                or record.get("trajectory_id")
                or record.get("sample_id")
                or "episode_0001"
            )
        groups[key].append(record)
    keys = sorted(groups)
    random.Random(seed).shuffle(keys)
    count = int(round(len(keys) * holdout_fraction)) if keys else 0
    if holdout_fraction > 0 and keys:
        count = max(1, count)
    holdout_keys = set(keys[:count])
    train = [record for key in sorted(groups) if key not in holdout_keys for record in groups[key]]
    holdout = [record for key in sorted(groups) if key in holdout_keys for record in groups[key]]
    return train, holdout


def calibrate_config(
    trace_features: Iterable[dict[str, Any]], base_config: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Map observed trace shape to bounded generator parameters.

    Calibration only controls synthetic workload shape.  It never changes an
    oracle or creates a trace event, which keeps the evaluation split honest.
    """

    config = dict(DEFAULT_CONFIG)
    if base_config:
        config.update(base_config)
    values = list(trace_features)
    mappings = {
        "txn_size": "transaction_size",
        "provenance_depth": "provenance_depth",
        "branch_factor": "branch_factor",
        "agent_count": "agent_count",
    }
    for config_key, feature_key in mappings.items():
        observed = [float(item[feature_key]) for item in values if item.get(feature_key) is not None]
        if observed:
            config[config_key] = max(1, int(round(mean(observed))))
    if any(item.get("policy_change_rate", 0) for item in values):
        config["policy_churn"] = max(1, int(round(mean(float(item.get("policy_change_rate", 0)) for item in values) * 10)))
    return config


def calibrated_suite(
    trace_features: Iterable[dict[str, Any]],
    seeds: Iterable[int],
    workloads: Iterable[str] = WORKLOADS,
    base_config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Generate a synthetic training suite calibrated from training traces."""

    config = calibrate_config(trace_features, base_config=base_config)
    return [
        generate_instance(workload, seed, config=config)
        for workload in workloads
        for seed in seeds
    ]
