"""Feature extraction and distribution comparison for trace-grounded realism checks."""

from __future__ import annotations

from collections import defaultdict
import random
import math
from statistics import mean, median, pstdev
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


def _standardized_feature_matrix(
    left: list[dict[str, Any]], right: list[dict[str, Any]]
) -> list[list[float]]:
    raw = [
        [float(record.get(feature, 0.0)) for feature in FEATURES]
        for record in [*left, *right]
    ]
    if not raw:
        return []
    centers = [mean(row[index] for row in raw) for index in range(len(FEATURES))]
    scales = [
        pstdev(row[index] for row in raw) or 1.0
        for index in range(len(FEATURES))
    ]
    return [
        [(value - centers[index]) / scales[index] for index, value in enumerate(row)]
        for row in raw
    ]


def _median_pairwise_distance(matrix: list[list[float]], seed: int) -> float:
    """Return a deterministic median distance, sampling large pair sets."""

    pair_count = len(matrix) * (len(matrix) - 1) // 2
    if pair_count <= 4096:
        pairs = [
            (left_index, right_index)
            for left_index in range(len(matrix))
            for right_index in range(left_index + 1, len(matrix))
        ]
    else:
        rng = random.Random(seed)
        pairs = []
        seen: set[tuple[int, int]] = set()
        while len(pairs) < 4096:
            left_index = rng.randrange(len(matrix))
            right_index = rng.randrange(len(matrix) - 1)
            if right_index >= left_index:
                right_index += 1
            pair = tuple(sorted((left_index, right_index)))
            if pair not in seen:
                seen.add(pair)
                pairs.append(pair)
    squared_distances = [
        sum(
            (matrix[left_index][feature] - matrix[right_index][feature]) ** 2
            for feature in range(len(FEATURES))
        )
        for left_index, right_index in pairs
    ]
    positive = [distance for distance in squared_distances if distance > 1e-12]
    return math.sqrt(median(positive)) if positive else 1.0


def multivariate_rff_mmd_test(
    synthetic: Iterable[dict[str, Any]],
    trace_grounded: Iterable[dict[str, Any]],
    *,
    permutations: int = 999,
    rff_dimensions: int = 64,
    seed: int = 17,
) -> dict[str, Any]:
    """Permutation test over a joint standardized RBF feature embedding.

    Random Fourier features make the MMD-style statistic practical without a
    heavy numerical dependency.  The permutation p-value tests distributional
    difference; a large p-value is not evidence that the distributions are
    equivalent.
    """

    if permutations < 1:
        raise ValueError("permutations must be positive")
    if rff_dimensions < 1:
        raise ValueError("rff_dimensions must be positive")
    left = list(synthetic)
    right = list(trace_grounded)
    base = {
        "method": "standardized_rbf_random_fourier_mmd_permutation",
        "feature_names": list(FEATURES),
        "synthetic_count": len(left),
        "trace_count": len(right),
        "permutations": int(permutations),
        "rff_dimensions": int(rff_dimensions),
        "seed": int(seed),
        "null_hypothesis": "synthetic and trace-grounded joint feature distributions are equal",
        "claim_boundary": "a non-significant result is not evidence of distributional equivalence",
        "small_sample_warning": (
            "joint-test inference is low-power and unstable when either sample has fewer than 20 instances"
            if min(len(left), len(right)) < 20
            else None
        ),
    }
    if len(left) < 2 or len(right) < 2:
        return {
            **base,
            "status": "insufficient_data",
            "statistic": None,
            "p_value": None,
        }

    matrix = _standardized_feature_matrix(left, right)
    bandwidth = _median_pairwise_distance(matrix, seed + 101)
    rng_features = random.Random(seed + 211)
    frequencies = [
        [rng_features.gauss(0.0, 1.0 / bandwidth) for _ in FEATURES]
        for _ in range(rff_dimensions)
    ]
    phases = [rng_features.random() * 2.0 * math.pi for _ in range(rff_dimensions)]
    scale = math.sqrt(2.0 / rff_dimensions)
    embedded = [
        [
            scale
            * math.cos(
                sum(weight * value for weight, value in zip(frequency, row)) + phase
            )
            for frequency, phase in zip(frequencies, phases)
        ]
        for row in matrix
    ]
    total = [sum(row[index] for row in embedded) for index in range(rff_dimensions)]
    left_count = len(left)
    right_count = len(right)
    if left_count <= right_count:
        selected_count = left_count
        observed_indices = range(left_count)
    else:
        selected_count = right_count
        observed_indices = range(left_count, left_count + right_count)
    complement_count = len(embedded) - selected_count

    def statistic(selected_indices: Iterable[int]) -> float:
        selected_sums = [0.0] * rff_dimensions
        for row_index in selected_indices:
            row = embedded[row_index]
            for feature_index, value in enumerate(row):
                selected_sums[feature_index] += value
        return sum(
            (
                selected_sums[index] / selected_count
                - (total[index] - selected_sums[index]) / complement_count
            )
            ** 2
            for index in range(rff_dimensions)
        )

    observed = statistic(observed_indices)
    permutation_rng = random.Random(seed + 307)
    indices = list(range(len(embedded)))
    greater_or_equal = 0
    for _ in range(permutations):
        permutation_rng.shuffle(indices)
        permuted = statistic(indices[:selected_count])
        if permuted >= observed - 1e-15:
            greater_or_equal += 1
    return {
        **base,
        "status": "available",
        "statistic": observed,
        "p_value": (greater_or_equal + 1) / (permutations + 1),
        "bandwidth": bandwidth,
    }


def compare_distributions(
    synthetic: Iterable[dict[str, Any]],
    trace_grounded: Iterable[dict[str, Any]],
    *,
    bootstrap_repetitions: int = 2000,
    joint_test_permutations: int = 999,
    joint_test_dimensions: int = 64,
    seed: int = 17,
) -> dict[str, Any]:
    if bootstrap_repetitions < 1:
        raise ValueError("bootstrap_repetitions must be positive")
    left = list(synthetic)
    right = list(trace_grounded)
    features: dict[str, Any] = {}
    for name in FEATURES:
        left_values = [float(item.get(name, 0.0)) for item in left]
        right_values = [float(item.get(name, 0.0)) for item in right]
        left_mean = mean(left_values) if left_values else 0.0
        right_mean = mean(right_values) if right_values else 0.0
        feature_seed = seed + FEATURES.index(name) * 1009
        features[name] = {
            "synthetic_mean": left_mean,
            "synthetic_std": pstdev(left_values) if left_values else 0.0,
            "trace_mean": right_mean,
            "trace_std": pstdev(right_values) if right_values else 0.0,
            "mean_abs_diff": abs(left_mean - right_mean),
            "relative_mean_abs_diff": abs(left_mean - right_mean)
            / max(1.0, abs(left_mean), abs(right_mean)),
            "synthetic_mean_interval": bootstrap_mean_interval(
                left_values, repetitions=bootstrap_repetitions, seed=feature_seed
            ),
            "trace_mean_interval": bootstrap_mean_interval(
                right_values, repetitions=bootstrap_repetitions, seed=feature_seed + 1
            ),
            "mean_abs_diff_interval": bootstrap_mean_abs_diff_interval(
                left_values,
                right_values,
                repetitions=bootstrap_repetitions,
                seed=feature_seed + 2,
            ),
        }
    relative_diffs = [float(features[name]["relative_mean_abs_diff"]) for name in FEATURES]
    multivariate_test = multivariate_rff_mmd_test(
        left,
        right,
        permutations=joint_test_permutations,
        rff_dimensions=joint_test_dimensions,
        seed=seed + 10007,
    )
    return {
        "synthetic_count": len(left),
        "trace_count": len(right),
        "bootstrap_repetitions": bootstrap_repetitions,
        "bootstrap_seed": seed,
        "comparison_method": "feature-wise bootstrap intervals plus a joint RBF-RFF MMD permutation test",
        "mean_feature_relative_abs_diff": mean(relative_diffs) if relative_diffs else 0.0,
        "features": features,
        "multivariate_test": multivariate_test,
    }


def _percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(probability * len(ordered)) - 1))
    return ordered[index]


def bootstrap_mean_interval(
    values: Iterable[float],
    *,
    repetitions: int = 2000,
    confidence: float = 0.95,
    seed: int = 17,
) -> dict[str, Any]:
    """Return a deterministic percentile bootstrap interval for a sample mean."""

    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be in (0, 1)")
    sample = [float(value) for value in values]
    estimate = mean(sample) if sample else 0.0
    if not sample:
        return {
            "confidence": float(confidence),
            "estimate": 0.0,
            "lower": 0.0,
            "upper": 0.0,
            "sample_count": 0,
            "bootstrap_repetitions": int(repetitions),
            "seed": int(seed),
        }
    rng = random.Random(seed)
    bootstrap_means = [
        mean(sample[rng.randrange(len(sample))] for _ in range(len(sample)))
        for _ in range(repetitions)
    ]
    alpha = (1.0 - confidence) / 2.0
    return {
        "confidence": float(confidence),
        "estimate": estimate,
        "lower": _percentile(bootstrap_means, alpha),
        "upper": _percentile(bootstrap_means, 1.0 - alpha),
        "sample_count": len(sample),
        "bootstrap_repetitions": int(repetitions),
        "seed": int(seed),
    }


def bootstrap_mean_abs_diff_interval(
    left: Iterable[float],
    right: Iterable[float],
    *,
    repetitions: int = 2000,
    confidence: float = 0.95,
    seed: int = 17,
) -> dict[str, Any]:
    """Bootstrap the absolute difference between two independent sample means."""

    left_sample = [float(value) for value in left]
    right_sample = [float(value) for value in right]
    estimate = abs((mean(left_sample) if left_sample else 0.0) - (mean(right_sample) if right_sample else 0.0))
    if not left_sample or not right_sample:
        return {
            "confidence": float(confidence),
            "estimate": estimate,
            "lower": estimate,
            "upper": estimate,
            "sample_count_left": len(left_sample),
            "sample_count_right": len(right_sample),
            "bootstrap_repetitions": int(repetitions),
            "seed": int(seed),
        }
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be in (0, 1)")
    rng = random.Random(seed)
    diffs = []
    for _ in range(repetitions):
        left_mean = mean(left_sample[rng.randrange(len(left_sample))] for _ in range(len(left_sample)))
        right_mean = mean(right_sample[rng.randrange(len(right_sample))] for _ in range(len(right_sample)))
        diffs.append(abs(left_mean - right_mean))
    alpha = (1.0 - confidence) / 2.0
    return {
        "confidence": float(confidence),
        "estimate": estimate,
        "lower": _percentile(diffs, alpha),
        "upper": _percentile(diffs, 1.0 - alpha),
        "sample_count_left": len(left_sample),
        "sample_count_right": len(right_sample),
        "bootstrap_repetitions": int(repetitions),
        "seed": int(seed),
    }


def trace_evidence_summary(
    instances: Iterable[dict[str, Any]], rows: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    """Summarize replay evidence without treating projections as ground truth."""

    materialized_instances = list(instances)
    materialized_rows = list(rows)
    source_operation_count = 0
    replay_operation_count = 0
    envelope_operation_count = 0
    for instance in materialized_instances:
        operations = list(instance.get("operations", []))
        replay_operation_count += len(operations)
        envelope_operation_count += sum(
            operation.get("type") in {"begin_txn", "commit"} for operation in operations
        )
        metadata = instance.get("trace_metadata", {})
        source_operation_count += int(
            metadata.get(
                "event_count",
                sum(
                    operation.get("type") not in {"begin_txn", "commit"}
                    for operation in operations
                ),
            )
        )

    oracle_match_by_variant: dict[str, dict[str, Any]] = {}
    for variant in sorted({str(row.get("variant")) for row in materialized_rows}):
        variant_rows = [row for row in materialized_rows if str(row.get("variant")) == variant]
        matched = sum(bool(int(row.get("oracle_match", 0))) for row in variant_rows)
        total = len(variant_rows)
        oracle_match_by_variant[variant] = {
            "matched": matched,
            "total": total,
            "rate": matched / total if total else 0.0,
        }
    return {
        "instance_count": len(materialized_instances),
        "source_operation_count": source_operation_count,
        "replay_operation_count": replay_operation_count,
        "replay_envelope_operation_count": envelope_operation_count,
        "holdout_grouping": "episode/task_id+trial/conversation_id/trajectory_id/sample_id",
        "oracle_match_by_variant": oracle_match_by_variant,
        "trace_ground_truth_native": False,
        "production_latency_claim": False,
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
