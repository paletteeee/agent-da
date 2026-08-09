"""Validate and aggregate independent cross-host model-load repetitions."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence


_USAGE_FIELDS = (
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "request_count",
    "responses_with_usage",
)

_CONDITION_PATHS = (
    ("dataset",),
    ("manifest_sha256",),
    ("model_id",),
    ("execution_identity", "model_revision"),
    ("execution_identity", "model_server_build"),
    ("execution_identity", "runner_source_identity", "fingerprint"),
    ("configured_concurrency",),
    ("generation_parameters", "max_steps"),
    ("generation_parameters", "max_tokens"),
    ("generation_parameters", "timeout_seconds"),
    ("execution_scope",),
    ("host_count",),
    ("agent_worker_host_count",),
    ("model_server_host_count",),
    ("network_transport",),
    ("task_count_per_cycle",),
    ("minimum_cycles",),
    ("minimum_duration_seconds",),
    ("topology_attestation", "agent_host_identity_sha256"),
    ("topology_attestation", "model_host_identity_sha256"),
    ("topology_attestation", "model_host_identity_source"),
    ("topology_attestation", "ssh_target_identity_sha256"),
    ("topology_attestation", "host_identities_distinct"),
    ("topology_attestation", "controlmaster_session_verified"),
    ("topology_attestation", "controlmaster_pid_matches_tunnel"),
    ("topology_attestation", "process_command_sha256"),
    ("topology_attestation", "local_forward_matches_model_endpoint"),
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


def _positive_integer(summary: Mapping[str, Any], key: str) -> int:
    value = _integer(summary, key)
    if value < 1:
        raise ValueError(f"invalid positive integer: {key}")
    return value


def _finite_number(summary: Mapping[str, Any], key: str) -> float:
    value = summary.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid finite number: {key}")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"invalid finite number: {key}")
    return number


def _positive_finite_number(summary: Mapping[str, Any], key: str) -> float:
    number = _finite_number(summary, key)
    if number <= 0:
        raise ValueError(f"invalid positive finite number: {key}")
    return number


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def _validate_cross_host_condition(summary: Mapping[str, Any], index: int) -> None:
    host_count = _integer(summary, "host_count")
    agent_hosts = _integer(summary, "agent_worker_host_count")
    model_hosts = _integer(summary, "model_server_host_count")
    if (
        summary.get("execution_scope") != "cross_host_client_server"
        or summary.get("network_transport") != "ssh_local_port_forward"
        or host_count != 2
        or agent_hosts != 1
        or model_hosts != 1
    ):
        raise ValueError(f"invalid cross-host condition at repetition {index}")
    _positive_integer(summary, "configured_concurrency")
    _positive_integer(summary, "task_count_per_cycle")
    _positive_integer(summary, "minimum_cycles")
    _positive_finite_number(summary, "minimum_duration_seconds")
    generation = summary.get("generation_parameters")
    if not isinstance(generation, Mapping):
        raise ValueError(f"invalid generation parameters at repetition {index}")
    try:
        _positive_integer(generation, "max_steps")
        _positive_integer(generation, "max_tokens")
        _positive_finite_number(generation, "timeout_seconds")
    except ValueError as exc:
        raise ValueError(f"invalid generation parameters at repetition {index}") from exc
    topology = summary.get("topology_attestation")
    if not isinstance(topology, Mapping):
        raise ValueError(f"invalid topology identity at repetition {index}")
    agent_hash = topology.get("agent_host_identity_sha256")
    model_hash = topology.get("model_host_identity_sha256")
    if (
        not _is_sha256(agent_hash)
        or not _is_sha256(model_hash)
        or not _is_sha256(topology.get("ssh_target_identity_sha256"))
        or not _is_sha256(topology.get("process_command_sha256"))
        or topology.get("model_host_identity_source")
        != "ssh_controlmaster_bound_remote_hostname_sha256"
        or topology.get("host_identities_distinct") is not True
        or topology.get("controlmaster_session_verified") is not True
        or topology.get("controlmaster_pid_matches_tunnel") is not True
        or str(agent_hash).lower() == str(model_hash).lower()
    ):
        raise ValueError(f"invalid topology identity at repetition {index}")


def _usage(summary: Mapping[str, Any]) -> dict[str, int]:
    usage = summary.get("model_usage")
    if not isinstance(usage, Mapping):
        raise ValueError("missing model usage")
    result = {key: _integer(usage, key) for key in _USAGE_FIELDS}
    if (
        result["request_count"] != result["responses_with_usage"]
        or result["total_tokens"]
        != result["prompt_tokens"] + result["completion_tokens"]
    ):
        raise ValueError("usage counts inconsistent")
    return result


def _canonical_digest(summary: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(
            summary,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("repetition summary is not canonical finite JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _utc_time(summary: Mapping[str, Any], key: str) -> datetime:
    value = summary.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing UTC timestamp: {key}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid UTC timestamp: {key}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp lacks timezone: {key}")
    return parsed


def _validate_task_summaries(
    summary: Mapping[str, Any],
    index: int,
    top_usage: Mapping[str, int],
) -> tuple[int, Counter[str]]:
    task_summaries = summary.get("task_summaries")
    if not isinstance(task_summaries, list) or not all(
        isinstance(row, Mapping) for row in task_summaries
    ):
        raise ValueError(f"task summaries missing at repetition {index}")
    attempt_count = _integer(summary, "attempt_count")
    completed_cycles = _integer(summary, "completed_cycles")
    task_count_per_cycle = _integer(summary, "task_count_per_cycle")
    if (
        len(task_summaries) != attempt_count
        or attempt_count != completed_cycles * task_count_per_cycle
    ):
        raise ValueError(f"task summary counts inconsistent at repetition {index}")

    statuses: Counter[str] = Counter()
    failures: Counter[str] = Counter()
    row_usage: Counter[str] = Counter()
    native_event_count = 0
    contract_success = 0
    attempt_ids: set[str] = set()
    source_ids_by_cycle: dict[int, set[str]] = {}
    for row in task_summaries:
        status = row.get("status")
        if status not in {"completed", "failed"}:
            raise ValueError(f"invalid task status at repetition {index}")
        statuses[str(status)] += 1
        failure_code = row.get("failure_code")
        if status == "failed":
            if not isinstance(failure_code, str) or not failure_code:
                raise ValueError(f"failed task lacks failure code at repetition {index}")
            failures[failure_code] += 1
        attempt_id = row.get("attempt_id")
        cycle = row.get("cycle")
        source_task_id = row.get("source_task_id")
        if (
            not isinstance(attempt_id, str)
            or not attempt_id
            or attempt_id in attempt_ids
            or isinstance(cycle, bool)
            or not isinstance(cycle, int)
            or cycle < 1
            or cycle > completed_cycles
            or not isinstance(source_task_id, str)
            or not source_task_id
            or attempt_id != f"cycle_{cycle:04d}:{source_task_id}"
        ):
            raise ValueError(f"attempt IDs inconsistent at repetition {index}")
        attempt_ids.add(attempt_id)
        source_ids_by_cycle.setdefault(cycle, set())
        if source_task_id in source_ids_by_cycle[cycle]:
            raise ValueError(f"attempt grid inconsistent at repetition {index}")
        source_ids_by_cycle[cycle].add(source_task_id)
        native_event_count += _integer(row, "native_event_count")
        usage = _usage(row)
        if usage["request_count"] < 1 or usage["total_tokens"] < 1:
            raise ValueError(f"attempt usage missing at repetition {index}")
        row_usage.update(usage)
        evaluator = row.get("task_evaluator")
        if not isinstance(evaluator, Mapping) or type(evaluator.get("success")) is not bool:
            raise ValueError(f"task evaluator missing at repetition {index}")
        contract_success += int(evaluator["success"] is True)

    expected_sources = source_ids_by_cycle.get(1, set())
    if (
        len(source_ids_by_cycle) != completed_cycles
        or len(expected_sources) != task_count_per_cycle
        or any(
            source_ids_by_cycle.get(cycle) != expected_sources
            for cycle in range(1, completed_cycles + 1)
        )
    ):
        raise ValueError(f"attempt grid inconsistent at repetition {index}")

    if statuses["completed"] != _integer(summary, "completed_attempt_count") or statuses[
        "failed"
    ] != _integer(summary, "failed_attempt_count"):
        raise ValueError(f"task status counts inconsistent at repetition {index}")
    top_failures = summary.get("failure_counts")
    if not isinstance(top_failures, Mapping):
        raise ValueError(f"failure counts missing at repetition {index}")
    normalized_top_failures: Counter[str] = Counter()
    for code, count in top_failures.items():
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError(f"invalid failure count at repetition {index}")
        normalized_top_failures[str(code)] = count
    if failures != normalized_top_failures:
        raise ValueError(f"failure counts inconsistent at repetition {index}")
    if native_event_count != _integer(summary, "native_event_count"):
        raise ValueError(f"native event counts inconsistent at repetition {index}")
    if any(row_usage[key] != top_usage[key] for key in _USAGE_FIELDS):
        raise ValueError(f"task-level usage counts inconsistent at repetition {index}")
    return contract_success, failures


def aggregate_model_load_repetitions(
    summaries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Aggregate only condition-matched, independent, fully attested runs."""

    if len(summaries) < 2:
        raise ValueError("at least two model-load repetitions are required")
    if not all(isinstance(summary, Mapping) for summary in summaries):
        raise ValueError("each model-load repetition must be a mapping")
    summary_digests = [_canonical_digest(summary) for summary in summaries]
    if len(set(summary_digests)) != len(summary_digests):
        raise ValueError("duplicate repetition summary detected")

    reference_condition = _condition(summaries[0])
    repetition_rows: list[dict[str, Any]] = []
    aggregate_failures: Counter[str] = Counter()
    total_contract_success = 0
    tunnel_process_ids: list[int] = []
    intervals: list[tuple[datetime, datetime, int]] = []
    usage_rows: list[dict[str, int]] = []
    elapsed_rows: list[float] = []
    minimum_duration = _finite_number(summaries[0], "minimum_duration_seconds")

    for index, summary in enumerate(summaries, start=1):
        _validate_cross_host_condition(summary, index)
        if _condition(summary) != reference_condition:
            raise ValueError(f"repetition condition mismatch at repetition {index}")
        if summary.get("duration_target_met") is not True:
            raise ValueError(f"duration target not met at repetition {index}")
        if summary.get("topology_attested") is not True or summary.get(
            "cross_host_network_claim"
        ) is not True:
            raise ValueError(f"topology attestation missing at repetition {index}")
        if summary.get("cross_host_multi_agent_workers_claim") is not False:
            raise ValueError(f"worker-host claim invalid at repetition {index}")
        topology = summary.get("topology_attestation")
        if not isinstance(topology, Mapping) or topology.get("status") != "process_observed":
            raise ValueError(f"topology attestation invalid at repetition {index}")
        if topology.get("local_forward_matches_model_endpoint") is not True:
            raise ValueError(f"topology endpoint mismatch at repetition {index}")
        try:
            process_id = _positive_integer(topology, "process_id")
        except ValueError as exc:
            raise ValueError(f"invalid topology identity at repetition {index}") from exc
        tunnel_process_ids.append(process_id)
        if summary.get("token_usage_complete") is not True:
            raise ValueError(f"token usage incomplete at repetition {index}")

        elapsed = _positive_finite_number(summary, "elapsed_seconds")
        if elapsed < minimum_duration:
            raise ValueError(f"elapsed duration below target at repetition {index}")
        started = _utc_time(summary, "started_at_utc")
        ended = _utc_time(summary, "ended_at_utc")
        if ended <= started:
            raise ValueError(f"invalid run time interval at repetition {index}")
        wall_seconds = (ended - started).total_seconds()
        tolerance = max(1.0, elapsed * 0.01)
        if abs(wall_seconds - elapsed) > tolerance:
            raise ValueError(f"elapsed duration disagrees with UTC interval at repetition {index}")
        intervals.append((started, ended, index))
        elapsed_rows.append(elapsed)

        completed_cycles = _integer(summary, "completed_cycles")
        if completed_cycles < _positive_integer(summary, "minimum_cycles"):
            raise ValueError(f"completed cycles below minimum at repetition {index}")
        attempt_count = _positive_integer(summary, "attempt_count")
        if attempt_count != _integer(
            summary, "completed_attempt_count"
        ) + _integer(summary, "failed_attempt_count"):
            raise ValueError(f"attempt counts inconsistent at repetition {index}")
        usage = _usage(summary)
        if usage["request_count"] < 1 or usage["total_tokens"] < 1:
            raise ValueError(f"nonzero token usage required at repetition {index}")
        observed_peak = _positive_integer(summary, "observed_peak_in_flight")
        if observed_peak > _positive_integer(summary, "configured_concurrency"):
            raise ValueError(f"observed concurrency invalid at repetition {index}")
        usage_rows.append(usage)
        contract_success, failures = _validate_task_summaries(
            summary, index, usage
        )
        total_contract_success += contract_success
        aggregate_failures.update(failures)
        repetition_rows.append(
            {
                "repetition": index,
                "summary_content_sha256": summary_digests[index - 1],
                "started_at_utc": str(summary["started_at_utc"]),
                "ended_at_utc": str(summary["ended_at_utc"]),
                "elapsed_seconds": elapsed,
                "completed_cycles": completed_cycles,
                "attempt_count": attempt_count,
                "completed_attempt_count": _integer(
                    summary, "completed_attempt_count"
                ),
                "failed_attempt_count": _integer(summary, "failed_attempt_count"),
                "contract_success_count": contract_success,
                "observed_peak_in_flight": observed_peak,
                "model_usage": usage,
                "latency_ms": dict(summary.get("latency_ms", {})),
                "tunnel_process_id": process_id,
                "topology_status": topology["status"],
            }
        )

    if len(set(tunnel_process_ids)) != len(tunnel_process_ids):
        raise ValueError("tunnel process identity reused across repetitions")
    ordered_intervals = sorted(intervals)
    for previous, current in zip(ordered_intervals, ordered_intervals[1:]):
        if current[0] < previous[1]:
            raise ValueError("repetition time intervals overlap")

    aggregate_usage = {
        key: sum(row[key] for row in usage_rows) for key in _USAGE_FIELDS
    }
    total_elapsed = sum(elapsed_rows)
    total_attempts = sum(row["attempt_count"] for row in repetition_rows)
    total_completed = sum(row["completed_attempt_count"] for row in repetition_rows)
    total_failed = sum(row["failed_attempt_count"] for row in repetition_rows)
    total_cycles = sum(row["completed_cycles"] for row in repetition_rows)
    duration_label: int | float = (
        int(minimum_duration) if minimum_duration.is_integer() else minimum_duration
    )
    repetition_count = len(summaries)
    return {
        "analysis": "attested_cross_host_model_load_repetitions",
        "repetition_count": repetition_count,
        "duration_design": (
            f"{repetition_count}_independent_repetitions_x_{duration_label}_seconds"
        ),
        "single_continuous_tunnel_claim": False,
        "all_repetitions_duration_target_met": True,
        "all_repetitions_topology_attested": True,
        "independence_attestation": {
            "distinct_summary_count": len(set(summary_digests)),
            "distinct_tunnel_process_count": len(set(tunnel_process_ids)),
            "non_overlapping_utc_intervals": True,
        },
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
        "failure_counts": dict(sorted(aggregate_failures.items())),
        "model_usage": aggregate_usage,
        "token_usage_complete": True,
        "throughput_attempts_per_second": (
            total_attempts / total_elapsed if total_elapsed else 0.0
        ),
        "tokens_per_second": (
            aggregate_usage["total_tokens"] / total_elapsed if total_elapsed else 0.0
        ),
        "condition": reference_condition,
        "repetitions": repetition_rows,
        "raw_traces_committed": False,
        "monetary_cost_status": "not_computed_without_an_explicit_pricing_rate",
        "claim_boundary": (
            f"{repetition_count} independently attested client-to-model-server "
            "repetitions; not one continuous SSH tunnel and not multi-host Agent workers"
        ),
    }


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _load_summary(path: Path) -> tuple[Mapping[str, Any], str]:
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(
            raw.decode("utf-8"), parse_constant=_reject_json_constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid repetition summary JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"repetition summary must contain an object: {path}")
    return payload, digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summaries", type=Path, nargs="+", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    loaded = [_load_summary(path) for path in args.summaries]
    summaries = [payload for payload, _digest in loaded]
    report = aggregate_model_load_repetitions(summaries)
    report["source_summary_sha256"] = [digest for _payload, digest in loaded]
    report["aggregator_source_sha256"] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
