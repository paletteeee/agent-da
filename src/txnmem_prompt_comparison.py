"""Paired, sanitized comparisons for LoCoMo and AppWorld prompt profiles."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence


def _profile(summary: Mapping[str, Any], expected: str) -> None:
    if summary.get("prompt_profile") != expected:
        raise ValueError(f"expected {expected} prompt profile")


def _condition_fingerprint_pair(
    baseline: Mapping[str, Any], tuned: Mapping[str, Any]
) -> str:
    left = baseline.get("condition_fingerprint")
    right = tuned.get("condition_fingerprint")
    if not isinstance(left, str) or not left or left != right:
        raise ValueError("paired condition fingerprints differ or are missing")
    return left


def _token_comparison(
    baseline: Mapping[str, Any], tuned: Mapping[str, Any]
) -> dict[str, Any]:
    baseline_tokens = int(baseline.get("model_usage", {}).get("total_tokens", 0) or 0)
    tuned_tokens = int(tuned.get("model_usage", {}).get("total_tokens", 0) or 0)
    baseline_complete = bool(baseline.get("token_usage_complete", False))
    tuned_complete = bool(tuned.get("token_usage_complete", False))
    exact = baseline_complete and tuned_complete
    observed_delta = tuned_tokens - baseline_tokens
    return {
        "baseline_total_tokens": baseline_tokens,
        "tuned_total_tokens": tuned_tokens,
        "baseline_token_usage_complete": baseline_complete,
        "tuned_token_usage_complete": tuned_complete,
        "observed_token_delta": observed_delta,
        "token_delta": observed_delta if exact else None,
        "token_delta_status": "exact" if exact else "observed_lower_bound_only",
    }


def _numbers(value: Any, field: str) -> list[float]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field} must be a sequence")
    numbers = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(f"{field} must contain only numbers")
        numbers.append(float(item))
    return numbers


def compare_locomo_prompt_profiles(
    baseline: Mapping[str, Any],
    tuned: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare LoCoMo repetitions paired by the recorded seed schedule."""

    _profile(baseline, "baseline")
    _profile(tuned, "tuned")
    condition_fingerprint = _condition_fingerprint_pair(baseline, tuned)
    if baseline.get("model") != tuned.get("model"):
        raise ValueError("LoCoMo model IDs differ")
    baseline_seeds = list(baseline.get("repetition_seeds", []))
    tuned_seeds = list(tuned.get("repetition_seeds", []))
    if not baseline_seeds or baseline_seeds != tuned_seeds:
        raise ValueError("LoCoMo repetition seed schedules differ")
    baseline_scores = _numbers(baseline.get("mean_f1_by_repetition"), "baseline scores")
    tuned_scores = _numbers(tuned.get("mean_f1_by_repetition"), "tuned scores")
    if len(baseline_scores) != len(tuned_scores) or len(baseline_scores) != len(baseline_seeds):
        raise ValueError("LoCoMo repetition denominators differ")
    for field in ("question_count_per_repetition", "sample_count_per_repetition"):
        if list(baseline.get(field, [])) != list(tuned.get(field, [])):
            raise ValueError(f"LoCoMo {field} differs")
    deltas = [round(right - left, 12) for left, right in zip(baseline_scores, tuned_scores)]
    categories = sorted(
        set(str(key) for key in baseline.get("category_f1_mean", {}))
        | set(str(key) for key in tuned.get("category_f1_mean", {}))
    )
    category_delta = {
        category: float(tuned.get("category_f1_mean", {}).get(category, 0.0))
        - float(baseline.get("category_f1_mean", {}).get(category, 0.0))
        for category in categories
    }
    token_comparison = _token_comparison(baseline, tuned)
    return {
        "comparison": "locomo_paired_prompt_profiles",
        "model": baseline.get("model"),
        "condition_fingerprint": condition_fingerprint,
        "paired_repetition_count": len(deltas),
        "repetition_seeds": baseline_seeds,
        "question_count_per_repetition": baseline.get("question_count_per_repetition", []),
        "sample_count_per_repetition": baseline.get("sample_count_per_repetition", []),
        "baseline_mean_f1_by_repetition": baseline_scores,
        "tuned_mean_f1_by_repetition": tuned_scores,
        "paired_mean_f1_deltas": deltas,
        "mean_f1_delta": mean(deltas),
        "mean_f1_delta_std": pstdev(deltas),
        "category_f1_delta": category_delta,
        **token_comparison,
        "claim_boundary": "descriptive paired repetitions; no population-level significance claim",
        "raw_predictions_committed": False,
    }


def _task_map(summary: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = summary.get("task_summaries")
    if not isinstance(rows, list):
        raise ValueError("AppWorld task_summaries must be a list")
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(row.get("task_id"), str):
            raise ValueError("invalid AppWorld task summary")
        task_id = str(row["task_id"])
        if task_id in result:
            raise ValueError("AppWorld comparison requires one repetition per task")
        result[task_id] = row
    return result


def _model_visible_tool_attestation(row: Mapping[str, Any], task_id: str) -> tuple[int, str]:
    count = row.get("model_visible_benchmark_tool_count")
    digest = row.get("model_visible_benchmark_tool_names_sha256")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"AppWorld model-visible tool count missing for {task_id}")
    if not isinstance(digest, str) or not digest:
        raise ValueError(f"AppWorld model-visible tool digest missing for {task_id}")
    return count, digest


def compare_appworld_prompt_profiles(
    baseline: Mapping[str, Any],
    tuned: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare official AppWorld outcomes for identical task manifests."""

    _profile(baseline, "baseline")
    _profile(tuned, "tuned")
    condition_fingerprint = _condition_fingerprint_pair(baseline, tuned)
    if baseline.get("manifest_sha256") != tuned.get("manifest_sha256"):
        raise ValueError("AppWorld manifest hashes differ")
    baseline_tasks = _task_map(baseline)
    tuned_tasks = _task_map(tuned)
    if set(baseline_tasks) != set(tuned_tasks):
        raise ValueError("AppWorld task IDs differ")
    per_task = []
    left_common_pass = 0
    right_common_pass = 0
    common_total = 0
    baseline_successes = 0
    tuned_successes = 0
    baseline_unavailable = 0
    tuned_unavailable = 0
    for task_id in sorted(baseline_tasks):
        left_tool_attestation = _model_visible_tool_attestation(
            baseline_tasks[task_id], task_id
        )
        right_tool_attestation = _model_visible_tool_attestation(
            tuned_tasks[task_id], task_id
        )
        if left_tool_attestation != right_tool_attestation:
            raise ValueError(
                f"AppWorld model-visible tool attestation differs for {task_id}"
            )
        left_value = baseline_tasks[task_id].get("official")
        right_value = tuned_tasks[task_id].get("official")
        left = left_value if isinstance(left_value, Mapping) else {}
        right = right_value if isinstance(right_value, Mapping) else {}
        left_available = bool(left) and left.get("status", "available") == "available"
        right_available = bool(right) and right.get("status", "available") == "available"
        baseline_unavailable += int(not left_available)
        tuned_unavailable += int(not right_available)
        left_success = left_available and bool(left.get("success", False))
        right_success = right_available and bool(right.get("success", False))
        baseline_successes += int(left_success)
        tuned_successes += int(right_success)
        row: dict[str, Any] = {
            "task_id": task_id,
            "baseline_available": left_available,
            "tuned_available": right_available,
            "baseline_success": left_success,
            "tuned_success": right_success,
            "assertion_rate_delta": None,
        }
        if left_available and right_available:
            left_total = int(left.get("total_count", 0) or 0)
            right_total = int(right.get("total_count", 0) or 0)
            if left_total != right_total:
                raise ValueError(f"AppWorld assertion denominator differs for {task_id}")
            left_pass = int(left.get("pass_count", 0) or 0)
            right_pass = int(right.get("pass_count", 0) or 0)
            left_common_pass += left_pass
            right_common_pass += right_pass
            common_total += left_total
            row.update(
                {
                    "baseline_pass_count": left_pass,
                    "tuned_pass_count": right_pass,
                    "total_count": left_total,
                    "assertion_rate_delta": (
                        (right_pass - left_pass) / left_total if left_total else 0.0
                    ),
                }
            )
        per_task.append(row)
    comparable_rows = [row for row in per_task if row["assertion_rate_delta"] is not None]
    token_comparison = _token_comparison(baseline, tuned)
    return {
        "comparison": "appworld_official_prompt_profiles",
        "manifest_sha256": baseline.get("manifest_sha256"),
        "condition_fingerprint": condition_fingerprint,
        "paired_task_count": len(per_task),
        "model_visible_tool_attestation_status": "matched_for_all_paired_tasks",
        "model_visible_tool_attestation_matched_task_count": len(per_task),
        "paired_available_assertion_task_count": len(comparable_rows),
        "baseline_unavailable_task_count": baseline_unavailable,
        "tuned_unavailable_task_count": tuned_unavailable,
        "official_success_baseline": baseline_successes,
        "official_success_tuned": tuned_successes,
        "official_success_delta": tuned_successes - baseline_successes,
        "official_success_baseline_all_tasks": baseline_successes,
        "official_success_tuned_all_tasks": tuned_successes,
        "official_success_baseline_denominator": len(per_task),
        "official_success_tuned_denominator": len(per_task),
        "official_assertion_pass_baseline": left_common_pass,
        "official_assertion_pass_tuned": right_common_pass,
        "official_assertion_total": common_total,
        "official_assertion_rate_delta": (
            (right_common_pass - left_common_pass) / common_total if common_total else 0.0
        ),
        "official_assertion_pass_baseline_common": left_common_pass,
        "official_assertion_pass_tuned_common": right_common_pass,
        "official_assertion_total_common": common_total,
        "official_assertion_rate_delta_common": (
            (right_common_pass - left_common_pass) / common_total if common_total else 0.0
        ),
        "improved_task_count": sum(row["assertion_rate_delta"] > 0 for row in comparable_rows),
        "unchanged_task_count": sum(row["assertion_rate_delta"] == 0 for row in comparable_rows),
        "regressed_task_count": sum(row["assertion_rate_delta"] < 0 for row in comparable_rows),
        **token_comparison,
        "per_task": per_task,
        "official_evaluator": "appworld.TestTracker.success plus task_completed protocol",
        "raw_reports_committed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("locomo", "appworld"), required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--tuned", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    tuned = json.loads(args.tuned.read_text(encoding="utf-8"))
    if args.kind == "locomo":
        report = compare_locomo_prompt_profiles(baseline, tuned)
    else:
        report = compare_appworld_prompt_profiles(baseline, tuned)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
