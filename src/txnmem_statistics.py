"""Aggregate native-agent repetitions without treating expected failures as errors."""

from __future__ import annotations

import copy
import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from txnmem_workloads import semantic_fingerprint


APPROVED_CONTROLLED_PARAMETER_INTERVALS = {
    "txn_size": (1, 4),
    "provenance_depth": (1, 4),
    "branch_factor": (1, 3),
    "policy_churn": (0, 2),
    "concurrency": (1, 3),
}


def binomial_interval(successes: int, trials: int, confidence: float = 0.95) -> dict[str, float]:
    """Return a Wilson score interval for a binomial proportion."""

    if isinstance(successes, bool) or isinstance(trials, bool) or successes < 0 or trials < 0:
        raise ValueError("successes and trials must be non-negative integers")
    if not isinstance(successes, int) or not isinstance(trials, int) or successes > trials:
        raise ValueError("successes must be an integer no greater than trials")
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be in (0, 1)")
    if trials == 0:
        return {
            "confidence": float(confidence),
            "estimate": 0.0,
            "lower": 0.0,
            "upper": 0.0,
            "successes": 0,
            "trials": 0,
        }
    # The normal approximation is deterministic and dependency-free.  The
    # 95% value is exact enough for the benchmark's aggregate report; other
    # confidence values use the inverse-normal approximation below.
    z = 1.959963984540054 if abs(confidence - 0.95) < 1e-12 else _normal_quantile((1.0 + confidence) / 2.0)
    n = float(trials)
    p = float(successes) / n
    z2 = z * z
    denominator = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denominator
    half = z * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n))) / denominator
    return {
        "confidence": float(confidence),
        "estimate": p,
        "lower": 0.0 if successes == 0 else max(0.0, center - half),
        "upper": 1.0 if successes == trials else min(1.0, center + half),
        "successes": successes,
        "trials": trials,
    }


def _normal_quantile(probability: float) -> float:
    """Acklam-style inverse normal approximation using only math."""

    if not 0.0 < probability < 1.0:
        raise ValueError("probability must be in (0, 1)")
    # Coefficients from the rational approximation by Peter J. Acklam.
    a = (-39.6968302866538, 220.946098424521, -275.928510446969, 138.357751867269, -30.6647980661472, 2.50662827745924)
    b = (-54.4760987982241, 161.585836858041, -155.698979859887, 66.8013118877197, -13.2806815528857)
    c = (-0.00778489400243029, -0.322396458041136, -2.40075827716184, -2.54973253934373, 4.37466414146497, 2.93816398269878)
    d = (0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742)
    lower = 0.02425
    upper = 1.0 - lower
    if probability < lower:
        q = math.sqrt(-2.0 * math.log(probability))
        numerator = ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
        denominator = (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        return numerator / denominator
    if probability > upper:
        q = math.sqrt(-2.0 * math.log(1.0 - probability))
        numerator = ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
        denominator = (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        return -(numerator / denominator)
    q = probability - 0.5
    r = q * q
    numerator = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    denominator = ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0
    return numerator / denominator


def _controlled_binary(value: Any, field: str) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in {0, 1}:
        return value
    if isinstance(value, str) and value in {"0", "1"}:
        return int(value)
    raise ValueError(f"{field} must be 0 or 1")


def _controlled_seed(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("seed must be a non-negative integer")
    try:
        seed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("seed must be a non-negative integer") from exc
    if seed < 0 or str(seed) != str(value):
        raise ValueError("seed must be a canonical non-negative integer")
    return seed


def controlled_violation_saturation(
    rows: Iterable[Mapping[str, Any]],
    checkpoints: Iterable[int],
    confidence: float = 0.95,
) -> dict[str, Any]:
    """Aggregate complete family×seed×variant prefixes with Wilson intervals."""

    materialized = [dict(row) for row in rows]
    if not materialized:
        raise ValueError("controlled saturation requires result rows")
    normalized_checkpoints = list(checkpoints)
    if (
        not normalized_checkpoints
        or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in normalized_checkpoints)
        or normalized_checkpoints != sorted(set(normalized_checkpoints))
    ):
        raise ValueError("checkpoints must be strictly increasing positive integers")

    cells: dict[tuple[str, int, str], dict[str, Any]] = {}
    instance_coordinates: dict[str, tuple[str, int]] = {}
    coordinate_instances: dict[tuple[str, int], str] = {}
    family_seeds: dict[str, set[int]] = defaultdict(set)
    variants: set[str] = set()
    for row in materialized:
        family = row.get("workload")
        variant = row.get("variant")
        instance_id = row.get("instance_id")
        if not isinstance(family, str) or not family:
            raise ValueError("every result row must name a workload family")
        if not isinstance(variant, str) or not variant:
            raise ValueError("every result row must name a variant")
        if not isinstance(instance_id, str) or not instance_id:
            raise ValueError("every result row must name an instance_id")
        seed = _controlled_seed(row.get("seed"))
        key = (family, seed, variant)
        if key in cells:
            raise ValueError(f"duplicate family/seed/variant row: {family}/{seed}/{variant}")
        coordinate = (family, seed)
        previous_coordinate = instance_coordinates.setdefault(instance_id, coordinate)
        if previous_coordinate != coordinate:
            raise ValueError(f"instance_id spans multiple family/seed cells: {instance_id}")
        previous_instance = coordinate_instances.setdefault(coordinate, instance_id)
        if previous_instance != instance_id:
            raise ValueError(f"family/seed cell has multiple instance IDs: {family}/{seed}")
        cells[key] = {
            **row,
            "seed": seed,
            "any_violation": _controlled_binary(row.get("any_violation"), "any_violation"),
            "oracle_match": _controlled_binary(row.get("oracle_match"), "oracle_match"),
        }
        family_seeds[family].add(seed)
        variants.add(variant)

    families = sorted(family_seeds)
    seed_domain = sorted(set().union(*family_seeds.values()))
    if seed_domain != list(range(len(seed_domain))):
        raise ValueError("controlled seed domain must be the contiguous prefix 0..N-1")
    for family in families:
        if family_seeds[family] != set(seed_domain):
            raise ValueError("controlled family seed domains are imbalanced")
    expected_cells = {
        (family, seed, variant)
        for family in families
        for seed in seed_domain
        for variant in variants
    }
    if set(cells) != expected_cells:
        missing = expected_cells - set(cells)
        extra = set(cells) - expected_cells
        raise ValueError(
            "controlled rows are not a complete family-by-seed-by-variant cube "
            f"(missing={len(missing)}, extra={len(extra)})"
        )
    if normalized_checkpoints[-1] > len(seed_domain):
        raise ValueError("checkpoint exceeds the complete seed domain")
    # Validate confidence even if future schema changes produce no variants.
    binomial_interval(0, 1, confidence)

    checkpoint_reports: list[dict[str, Any]] = []
    for checkpoint in normalized_checkpoints:
        prefix = set(seed_domain[:checkpoint])
        instance_count = len(families) * checkpoint
        variant_reports: list[dict[str, Any]] = []
        for variant in sorted(variants):
            selected = [
                cells[(family, seed, variant)]
                for family in families
                for seed in seed_domain
                if seed in prefix
            ]
            violations = sum(row["any_violation"] for row in selected)
            oracle_matches = sum(row["oracle_match"] for row in selected)
            variant_reports.append(
                {
                    "variant": variant,
                    "variant_result_count": len(selected),
                    "variant_result_unit": "family_seed_variant",
                    "violations": violations,
                    "violation_rate": violations / len(selected),
                    "violation_interval": binomial_interval(violations, len(selected), confidence),
                    "oracle_matches": oracle_matches,
                    "oracle_match_rate": oracle_matches / len(selected),
                    "oracle_match_interval": binomial_interval(oracle_matches, len(selected), confidence),
                }
            )
        checkpoint_reports.append(
            {
                "checkpoint_seed_count": checkpoint,
                "checkpoint_seed_unit": "seeds_per_family",
                "family_count": len(families),
                "instance_count": instance_count,
                "instance_unit": "family_seed",
                "variants": variant_reports,
            }
        )
    return {
        "schema_version": 1,
        "evidence_id": "controlled_violation_saturation",
        "confidence": float(confidence),
        "interval_method": "wilson_score",
        "interpretation_boundary": "intervals describe only the tested controlled parameter space",
        "families": families,
        "seed_domain": seed_domain,
        "variant_domain": sorted(variants),
        "checkpoint_seed_counts": normalized_checkpoints,
        "checkpoints": checkpoint_reports,
    }


def controlled_diversity(instances: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Report executable semantic and approved-parameter coverage per family."""

    materialized = [dict(instance) for instance in instances]
    if not materialized:
        raise ValueError("controlled diversity requires instances")
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_coordinates: set[tuple[str, int]] = set()
    family_seeds: dict[str, set[int]] = defaultdict(set)
    for instance in materialized:
        family = instance.get("workload")
        if not isinstance(family, str) or not family:
            raise ValueError("every controlled instance must name a workload family")
        seed = _controlled_seed(instance.get("seed"))
        coordinate = (family, seed)
        if coordinate in seen_coordinates:
            raise ValueError(f"duplicate controlled family/seed instance: {family}/{seed}")
        seen_coordinates.add(coordinate)
        recorded = instance.get("semantic_fingerprint")
        computed = semantic_fingerprint(instance)
        if recorded != computed:
            raise ValueError(f"semantic_fingerprint mismatch for {family}/{seed}")
        parameters = instance.get("semantic_parameters")
        if not isinstance(parameters, Mapping) or not parameters:
            raise ValueError(f"semantic_parameters missing for {family}/{seed}")
        normalized_parameters: dict[str, int] = {}
        for name, value in parameters.items():
            if name not in APPROVED_CONTROLLED_PARAMETER_INTERVALS:
                raise ValueError(f"unapproved semantic parameter: {name}")
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"semantic parameter {name} must be an integer")
            low, high = APPROVED_CONTROLLED_PARAMETER_INTERVALS[name]
            if not low <= value <= high:
                raise ValueError(f"semantic parameter {name} is outside its approved interval")
            normalized_parameters[str(name)] = value
        normalized = dict(instance)
        normalized["seed"] = seed
        normalized["semantic_parameters"] = normalized_parameters
        by_family[family].append(normalized)
        family_seeds[family].add(seed)

    seed_domain = sorted(set().union(*family_seeds.values()))
    if seed_domain != list(range(len(seed_domain))):
        raise ValueError("controlled diversity seed domain must be the contiguous prefix 0..N-1")
    if any(seeds != set(seed_domain) for seeds in family_seeds.values()):
        raise ValueError("controlled diversity family seed domains are imbalanced")

    family_reports: dict[str, Any] = {}
    for family in sorted(by_family):
        records = sorted(by_family[family], key=lambda row: row["seed"])
        parameter_names = sorted(records[0]["semantic_parameters"])
        if any(sorted(row["semantic_parameters"]) != parameter_names for row in records):
            raise ValueError(f"semantic parameter domains vary within family: {family}")
        value_counts: dict[str, dict[str, int]] = {}
        parameter_coverage: dict[str, dict[str, Any]] = {}
        approved_combination_count = 1
        combinations: set[tuple[int, ...]] = set()
        for name in parameter_names:
            low, high = APPROVED_CONTROLLED_PARAMETER_INTERVALS[name]
            counts = Counter(row["semantic_parameters"][name] for row in records)
            value_counts[name] = {str(value): counts[value] for value in sorted(counts)}
            approved_value_count = high - low + 1
            approved_combination_count *= approved_value_count
            parameter_coverage[name] = {
                "approved_interval": [low, high],
                "approved_value_count": approved_value_count,
                "observed_unique_value_count": len(counts),
                "coverage_ratio": len(counts) / approved_value_count,
            }
        combinations = {
            tuple(row["semantic_parameters"][name] for name in parameter_names)
            for row in records
        }
        family_reports[family] = {
            "family": family,
            "instance_count": len(records),
            "unique_semantic_fingerprint_count": len(
                {row["semantic_fingerprint"] for row in records}
            ),
            "semantic_fingerprint_source": "executable_state_operations_policies_schedules_provenance",
            "parameter_value_counts": value_counts,
            "parameter_coverage": parameter_coverage,
            "parameter_combination_coverage": {
                "parameter_order": parameter_names,
                "approved_combination_count": approved_combination_count,
                "observed_unique_combination_count": len(combinations),
                "coverage_ratio": len(combinations) / approved_combination_count,
            },
        }
    return {
        "schema_version": 1,
        "evidence_id": "controlled_diversity",
        "family_count": len(family_reports),
        "seed_count_per_family": len(seed_domain),
        "instance_count": len(materialized),
        "approved_parameter_intervals": {
            name: list(bounds)
            for name, bounds in sorted(APPROVED_CONTROLLED_PARAMETER_INTERVALS.items())
        },
        "families": family_reports,
    }


def aggregate_native_repetitions(reports: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = [copy.deepcopy(dict(report)) for report in reports]
    contract_success_count = 0
    total_tasks = 0
    native_event_count = 0
    evaluation_error_count = 0
    txnmem_oracle_match_count = 0
    txnmem_oracle_trials = 0
    expected_failures: Counter[str] = Counter()
    per_replication: list[dict[str, Any]] = []
    for index, report in enumerate(materialized, start=1):
        tasks = list(report.get("task_summaries", []))
        task_count = int(report.get("task_count", len(tasks)))
        successes = sum(
            bool(task.get("task_evaluator", {}).get("success"))
            for task in tasks
            if isinstance(task, Mapping)
        )
        variant = report.get("variants", {}).get("TxnMem", {})
        matches = int(variant.get("oracle_matched", 0))
        trials = int(variant.get("count", task_count))
        for task in tasks:
            if isinstance(task, Mapping) and task.get("failure_code"):
                expected_failures[str(task["failure_code"])] += 1
        contract_success_count += successes
        total_tasks += task_count
        native_event_count += int(report.get("native_event_count", 0))
        evaluation_error_count += int(report.get("evaluation_error_count", 0))
        txnmem_oracle_match_count += matches
        txnmem_oracle_trials += trials
        per_replication.append(
            {
                "replication": index,
                "task_count": task_count,
                "contract_success_count": successes,
                "txnmem_oracle_match_count": matches,
                "evaluation_error_count": int(report.get("evaluation_error_count", 0)),
            }
        )
    return {
        "repetitions": len(materialized),
        "total_tasks": total_tasks,
        "native_event_count": native_event_count,
        "evaluation_error_count": evaluation_error_count,
        "contract_success_count": contract_success_count,
        "contract_success_rate": contract_success_count / total_tasks if total_tasks else 0.0,
        "contract_success_interval": binomial_interval(contract_success_count, total_tasks),
        "txnmem_oracle_match_count": txnmem_oracle_match_count,
        "txnmem_oracle_match_rate": txnmem_oracle_match_count / txnmem_oracle_trials if txnmem_oracle_trials else 0.0,
        "txnmem_oracle_match_interval": binomial_interval(txnmem_oracle_match_count, txnmem_oracle_trials),
        "expected_failure_counts": dict(sorted(expected_failures.items())),
        "per_replication": per_replication,
        "production_claim": False,
    }


def _official_status(result: Mapping[str, Any] | None) -> str:
    """Normalize an adapter result without treating TxnMem oracle as official."""

    if not isinstance(result, Mapping):
        return "blocked"
    explicit = result.get("status", result.get("official_evaluator_status"))
    if explicit in {"available", "blocked", "error"}:
        return str(explicit)
    if result.get("official_evaluator_error") or result.get("error"):
        return "error"
    marker = str(result.get("official_evaluator", "")).lower()
    if any(token in marker for token in ("not_available", "unavailable", "offline", "blocked")):
        return "blocked"
    if any(key in result for key in ("success", "reward", "score", "pass_count", "total_count")):
        return "available"
    return "blocked"


def aggregate_official_results(
    task_summaries: Iterable[Mapping[str, Any]], dataset: str
) -> dict[str, Any]:
    """Aggregate official benchmark results using task-level denominators.

    ``TxnMem`` contract/oracle fields are deliberately ignored here.  A task
    contributes to ``trials`` only when its official evaluator reports a
    boolean success or a numeric reward/score; blocked evaluator rows stay in
    the failure classification but cannot become synthetic failures or
    successes.
    """

    rows = [row for row in task_summaries if isinstance(row, Mapping)]
    statuses = Counter(_official_status(row.get("official")) for row in rows)
    successes = 0
    trials = 0
    event_count = 0
    rewards: list[float] = []
    scores: list[float] = []
    pass_count = 0
    total_count = 0
    for row in rows:
        event_count += int(row.get("native_event_count", 0) or 0)
        official = row.get("official")
        if not isinstance(official, Mapping) or _official_status(official) != "available":
            continue
        success = official.get("success")
        reward = official.get("reward")
        score = official.get("score")
        if isinstance(success, bool):
            trials += 1
            successes += int(success)
        elif isinstance(reward, (int, float)) and not isinstance(reward, bool):
            trials += 1
            rewards.append(float(reward))
        elif isinstance(score, (int, float)) and not isinstance(score, bool):
            trials += 1
            scores.append(float(score))
        if isinstance(success, bool):
            if isinstance(reward, (int, float)) and not isinstance(reward, bool):
                rewards.append(float(reward))
            if isinstance(score, (int, float)) and not isinstance(score, bool):
                scores.append(float(score))
        pass_count += int(official.get("pass_count", 0) or 0)
        total_count += int(official.get("total_count", 0) or 0)

    available = statuses.get("available", 0)
    blocked = statuses.get("blocked", 0)
    errors = statuses.get("error", 0)
    if available and errors == 0 and blocked == 0:
        status = "available"
    elif available:
        status = "error" if errors else "blocked"
    elif errors:
        status = "error"
    else:
        status = "blocked"
    result: dict[str, Any] = {
        "dataset": str(dataset),
        "official_evaluator_status": status,
        "task_count": len(rows),
        "evaluator_available_task_count": available,
        "blocked_task_count": blocked,
        "error_task_count": errors,
        "successes": successes,
        "trials": trials,
        "event_count": event_count,
        "success_interval": binomial_interval(successes, trials),
    }
    if rewards:
        result["reward_sum"] = sum(rewards)
        result["reward_mean"] = sum(rewards) / len(rewards)
    if scores:
        result["score_sum"] = sum(scores)
        result["score_mean"] = sum(scores) / len(scores)
    if total_count:
        result["pass_count"] = pass_count
        result["total_count"] = total_count
    return result


def run_repetitions(
    manifest: Mapping[str, Any], model: Any, out_dir: Path, repetitions: int = 5
) -> dict[str, Any]:
    """Run a fixed task manifest repeatedly with deterministic seed offsets."""

    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    if model is None or not callable(getattr(model, "complete", None)):
        raise ValueError("a configured model client is required")
    from txnmem_real_experiment import run_experiment_manifest, sanitize_run_report

    reports: list[dict[str, Any]] = []
    for replication in range(repetitions):
        tasks = []
        for task in manifest.get("tasks", []):
            item = dict(task)
            item["seed"] = int(item.get("seed", 0)) + replication * 100
            tasks.append(item)
        report = run_experiment_manifest(
            {"manifest_version": 1, "dataset_name": manifest.get("dataset_name", "native-repetition"), "tasks": tasks},
            model,
            out_dir / f"rep_{replication + 1:02d}",
        )
        reports.append(sanitize_run_report(report))
    aggregate = aggregate_native_repetitions(reports)
    aggregate["model_id"] = getattr(model, "model", "unknown")
    aggregate["seed_offsets"] = [replication * 100 for replication in range(repetitions)]
    aggregate["raw_reports_location"] = "rep_*/results/native_model_summary.json"
    aggregate["raw_reports_committed"] = False
    aggregate["production_claim"] = False
    return aggregate
