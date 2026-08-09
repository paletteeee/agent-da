"""Validate and aggregate independent cross-host model-load repetitions."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


_CONDITION_PATHS = (
    ("dataset",),
    ("manifest_sha256",),
    ("model_id",),
    ("execution_identity", "model_revision"),
    ("execution_identity", "model_server_build"),
    ("execution_identity", "runner_source_identity", "fingerprint"),
    ("configured_concurrency",),
    ("execution_scope",),
    ("host_count",),
    ("agent_worker_host_count",),
    ("model_server_host_count",),
    ("network_transport",),
    ("task_count_per_cycle",),
    ("minimum_duration_seconds",),
)


def _nested(row: Mapping[str, Any], path: Sequence[str]) -> Any:
    value: Any = row
    for key in path:
        if not isinstance(value, Mapping) or key not in value:
            raise ValueError(f"missing repetition condition: {'.'.join(path)}")
        value = value[key]
    return value


def _condition(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {".".join(path): _nested(summary, path) for path in _CONDITION_PATHS}


def _integer(summary: Mapping[str, Any], key: str) -> int:
    value = summary.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"invalid non-negative integer: {key}")
    return value


def _usage(summary: Mapping[str, Any]) -> dict[str, int]:
    usage = summary.get("model_usage")
    if not isinstance(usage, Mapping):
        raise ValueError("missing model usage")
    return {
        key: _integer(usage, key)
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "request_count",
            "responses_with_usage",
        )
    }


def aggregate_model_load_repetitions(
    summaries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Aggregate only condition-matched, fully attested repetitions."""

    if len(summaries) < 2:
        raise ValueError("at least two model-load repetitions are required")
    if not all(isinstance(summary, Mapping) for summary in summaries):
        raise ValueError("each model-load repetition must be a mapping")
    reference_condition = _condition(summaries[0])
    for index, summary in enumerate(summaries, start=1):
        if _condition(summary) != reference_condition:
            raise ValueError(f"repetition condition mismatch at repetition {index}")
        if not bool(summary.get("duration_target_met")):
            raise ValueError(f"duration target not met at repetition {index}")
        if not bool(summary.get("topology_attested")) or not bool(
            summary.get("cross_host_network_claim")
        ):
            raise ValueError(f"topology attestation missing at repetition {index}")
        topology = summary.get("topology_attestation")
        if not isinstance(topology, Mapping) or topology.get("status") != "process_observed":
            raise ValueError(f"topology attestation invalid at repetition {index}")
        if not bool(summary.get("token_usage_complete")):
            raise ValueError(f"token usage incomplete at repetition {index}")
        usage_row = _usage(summary)
        if (
            usage_row["request_count"] != usage_row["responses_with_usage"]
            or usage_row["total_tokens"]
            != usage_row["prompt_tokens"] + usage_row["completion_tokens"]
        ):
            raise ValueError(f"usage counts inconsistent at repetition {index}")
        attempt_count = _integer(summary, "attempt_count")
        if attempt_count != _integer(
            summary, "completed_attempt_count"
        ) + _integer(summary, "failed_attempt_count"):
            raise ValueError(f"attempt counts inconsistent at repetition {index}")

    elapsed = [float(summary.get("elapsed_seconds", 0.0) or 0.0) for summary in summaries]
    minimum_duration = float(reference_condition["minimum_duration_seconds"])
    if any(value < minimum_duration for value in elapsed):
        raise ValueError("recorded elapsed duration is below the configured target")

    usage_rows = [_usage(summary) for summary in summaries]
    usage = {
        key: sum(row[key] for row in usage_rows)
        for key in usage_rows[0]
    }
    failure_counts: Counter[str] = Counter()
    repetition_rows = []
    total_contract_success = 0
    for index, (summary, duration, usage_row) in enumerate(
        zip(summaries, elapsed, usage_rows), start=1
    ):
        failures = summary.get("failure_counts")
        if not isinstance(failures, Mapping):
            raise ValueError(f"failure counts missing at repetition {index}")
        for code, count in failures.items():
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError(f"invalid failure count at repetition {index}")
            failure_counts[str(code)] += count
        task_summaries = summary.get("task_summaries")
        if not isinstance(task_summaries, list):
            raise ValueError(f"task summaries missing at repetition {index}")
        contract_success = sum(
            bool(row.get("task_evaluator", {}).get("success"))
            for row in task_summaries
            if isinstance(row, Mapping)
        )
        total_contract_success += contract_success
        repetition_rows.append(
            {
                "repetition": index,
                "elapsed_seconds": duration,
                "completed_cycles": _integer(summary, "completed_cycles"),
                "attempt_count": _integer(summary, "attempt_count"),
                "completed_attempt_count": _integer(
                    summary, "completed_attempt_count"
                ),
                "failed_attempt_count": _integer(summary, "failed_attempt_count"),
                "contract_success_count": contract_success,
                "observed_peak_in_flight": _integer(
                    summary, "observed_peak_in_flight"
                ),
                "model_usage": usage_row,
                "latency_ms": dict(summary.get("latency_ms", {})),
                "topology_status": summary["topology_attestation"]["status"],
            }
        )

    total_elapsed = sum(elapsed)
    total_attempts = sum(row["attempt_count"] for row in repetition_rows)
    total_completed = sum(row["completed_attempt_count"] for row in repetition_rows)
    total_failed = sum(row["failed_attempt_count"] for row in repetition_rows)
    total_cycles = sum(row["completed_cycles"] for row in repetition_rows)
    duration_label = int(minimum_duration) if minimum_duration.is_integer() else minimum_duration
    return {
        "analysis": "attested_cross_host_model_load_repetitions",
        "repetition_count": len(summaries),
        "duration_design": (
            f"{len(summaries)}_independent_repetitions_x_{duration_label}_seconds"
        ),
        "single_continuous_tunnel_claim": False,
        "all_repetitions_duration_target_met": True,
        "all_repetitions_topology_attested": True,
        "cross_host_network_claim": True,
        "cross_host_multi_agent_workers_claim": False,
        "agent_worker_host_count": reference_condition["agent_worker_host_count"],
        "model_server_host_count": reference_condition["model_server_host_count"],
        "configured_concurrency": reference_condition["configured_concurrency"],
        "observed_peak_in_flight": max(
            row["observed_peak_in_flight"] for row in repetition_rows
        ),
        "total_elapsed_seconds": total_elapsed,
        "total_completed_cycles": total_cycles,
        "total_attempt_count": total_attempts,
        "total_completed_attempt_count": total_completed,
        "total_failed_attempt_count": total_failed,
        "total_contract_success_count": total_contract_success,
        "failure_counts": dict(sorted(failure_counts.items())),
        "model_usage": usage,
        "token_usage_complete": True,
        "throughput_attempts_per_second": (
            total_attempts / total_elapsed if total_elapsed else 0.0
        ),
        "tokens_per_second": usage["total_tokens"] / total_elapsed if total_elapsed else 0.0,
        "condition": reference_condition,
        "repetitions": repetition_rows,
        "raw_traces_committed": False,
        "monetary_cost_status": "not_computed_without_an_explicit_pricing_rate",
        "claim_boundary": (
            "three independently attested client-to-model-server repetitions; "
            "not one continuous SSH tunnel and not multi-host Agent workers"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summaries", type=Path, nargs="+", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    summaries = [
        json.loads(path.read_text(encoding="utf-8")) for path in args.summaries
    ]
    report = aggregate_model_load_repetitions(summaries)
    report["source_summary_sha256"] = {
        path.name + f":{index}": __import__("hashlib").sha256(path.read_bytes()).hexdigest()
        for index, path in enumerate(args.summaries, start=1)
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
