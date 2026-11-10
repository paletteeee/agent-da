"""Aggregate native-agent repetitions without treating expected failures as errors."""

from __future__ import annotations

import copy
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


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
