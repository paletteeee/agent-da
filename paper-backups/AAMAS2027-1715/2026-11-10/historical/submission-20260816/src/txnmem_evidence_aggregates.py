"""Strict, sanitized aggregates for submission-facing remote evidence."""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any


_HEX = re.compile(r"^[0-9a-f]+$")
_SECRET_TEXT = re.compile(r"password|api[_-]?key|access[_-]?token|secret", re.I)
_ABSOLUTE_PATH = re.compile(r"(?:^|[\s=])/")
_IP_LITERAL = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_HOSTNAME_TOKEN = re.compile(r"(?:^|\s)[a-z0-9-]+(?:\.[a-z0-9-]+)+(?=\s|$)", re.I)
_SAFE_BASENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_STATE_CLASSIFICATIONS = ("complete", "absent", "partial", "unknown")
_TOXIPROXY_SCENARIOS = {
    "normal": ("none", "none", "none", "none"),
    "delay": ("qdrant", "write", "delay", "continue"),
    "timeout": ("qdrant", "write", "timeout", "abort"),
    "connection_drop": ("neo4j", "commit", "connection_drop", "abort"),
    "retry_success": ("qdrant", "write", "connection_drop", "retry_once"),
}
_TOXIPROXY_ROUTES = {
    "qdrant": ("txnmem-qdrant", "qdrant:6333"),
    "neo4j": ("txnmem-neo4j", "neo4j:7687"),
}
_TOXIPROXY_REQUEST_ORDINALS = {
    "normal": {"qdrant:write": 2, "neo4j:commit": 2},
    "delay": {"qdrant:write": 2, "neo4j:commit": 2},
    "retry_success": {"qdrant:write": 2, "neo4j:commit": 2},
    "timeout": {"qdrant:write": 1},
    "connection_drop": {
        "qdrant:write": 1,
        "neo4j:commit": 1,
        "qdrant:delete_compensation": 1,
        "neo4j:delete_compensation": 1,
    },
}


def _load(path: str | Path) -> tuple[Path, dict[str, Any]]:
    source = Path(path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("source aggregate must be a JSON object")
    return source, payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hex_digest(value: Any, field: str, lengths: set[int]) -> str:
    normalized = str(value or "").lower()
    if len(normalized) not in lengths or not _HEX.fullmatch(normalized):
        raise ValueError(f"{field} must be an auditable hexadecimal digest")
    return normalized


def _run_metadata(
    *,
    model_revision: str,
    model_server_build: str,
    source_commit: str,
    run_command: str,
) -> dict[str, str]:
    revision = _hex_digest(model_revision, "model_revision", {64})
    commit = _hex_digest(source_commit, "source_commit", {40, 64})
    build = str(model_server_build or "").strip()
    command = str(run_command or "").strip()
    if not build or build.lower() in {"unknown", "unspecified"}:
        raise ValueError("model_server_build must be specified")
    if not command:
        raise ValueError("run_command must be specified")
    if _SECRET_TEXT.search(command):
        raise ValueError("run_command contains a secret-bearing token")
    return {
        "model_revision": revision,
        "model_server_build": build,
        "source_commit": commit,
        "run_command": command,
    }


def _finite_number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or (positive and number <= 0.0):
        if positive:
            raise ValueError(f"{field} must be a finite positive latency")
        raise ValueError(f"{field} must be a finite number")
    return number


def aggregate_tau_submission_evidence(
    source_path: str | Path,
    *,
    expected_task_count: int = 50,
    model_revision: str,
    model_server_build: str,
    source_commit: str,
    run_command: str,
    runtime_attestation: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and compact a retry-merged τ-bench native aggregate."""

    source, payload = _load(source_path)
    metadata = _run_metadata(
        model_revision=model_revision,
        model_server_build=model_server_build,
        source_commit=source_commit,
        run_command=run_command,
    )
    if expected_task_count <= 0:
        raise ValueError("expected_task_count must be positive")
    rows = payload.get("task_summaries")
    if not isinstance(rows, list) or len(rows) != expected_task_count:
        raise ValueError(f"τ aggregate must contain exactly {expected_task_count} task rows")
    task_ids = [str(row.get("task_id", "")) for row in rows if isinstance(row, Mapping)]
    if len(task_ids) != expected_task_count or any(not task_id for task_id in task_ids):
        raise ValueError("every τ row must have a task ID")
    if len(set(task_ids)) != expected_task_count:
        raise ValueError("τ aggregate must contain unique task IDs")

    event_count = 0
    rewards: list[float] = []
    status_counts: Counter[str] = Counter()
    failure_counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("every τ task row must be an object")
        event_count += int(row.get("native_event_count", 0) or 0)
        status_counts[str(row.get("status", "unknown"))] += 1
        if row.get("failure_code"):
            failure_counts[str(row["failure_code"])] += 1
        official = row.get("official")
        if not isinstance(official, Mapping) or official.get("status") != "available":
            raise ValueError("every τ task must have an available official evaluator result")
        rewards.append(_finite_number(official.get("reward"), "official reward"))

    reward_sum = sum(rewards)
    reward_mean = reward_sum / expected_task_count
    official = payload.get("official")
    if not isinstance(official, Mapping):
        raise ValueError("τ aggregate is missing official evaluator totals")
    expected_official = {
        "official_evaluator_status": "available",
        "evaluator_available_task_count": expected_task_count,
        "task_count": expected_task_count,
        "trials": expected_task_count,
        "event_count": event_count,
    }
    for field, expected in expected_official.items():
        if official.get(field) != expected:
            raise ValueError(f"τ official aggregate field mismatch: {field}")
    if not math.isclose(
        _finite_number(official.get("reward_sum"), "reward_sum"),
        reward_sum,
        rel_tol=0.0,
        abs_tol=1e-12,
    ) or not math.isclose(
        _finite_number(official.get("reward_mean"), "reward_mean"),
        reward_mean,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError("τ reward totals do not match task rows")
    for field in ("task_count", "unique_task_count", "native_event_count"):
        expected = expected_task_count if field != "native_event_count" else event_count
        if payload.get(field) != expected:
            raise ValueError(f"τ aggregate field mismatch: {field}")

    manifest_sha256 = _hex_digest(
        payload.get("primary_manifest_sha256"), "primary_manifest_sha256", {64}
    )
    replacements = [str(item) for item in payload.get("replaced_network_error_tasks", [])]
    if any(task_id not in set(task_ids) for task_id in replacements):
        raise ValueError("τ retry replacement references an unknown task")
    retry_manifests = [str(item) for item in payload.get("retry_manifests", [])]
    if replacements and not retry_manifests:
        raise ValueError("τ replacement tasks require retry manifest evidence")
    attestation = dict(runtime_attestation or {})
    if attestation.get("server_continuity") is not True:
        raise ValueError("τ runtime attestation must prove model-server continuity")

    return {
        "schema_version": 1,
        "evidence_id": "tau_bench_native_50",
        "status": "complete",
        "dataset": str(payload.get("dataset")),
        "task_count": expected_task_count,
        "unique_task_count": expected_task_count,
        "task_ids": sorted(task_ids),
        "native_event_count": event_count,
        "evaluator_available_task_count": expected_task_count,
        "reward_sum": reward_sum,
        "reward_mean": reward_mean,
        "task_status_counts": dict(sorted(status_counts.items())),
        "failure_code_counts": dict(sorted(failure_counts.items())),
        "max_steps_exceeded_count": failure_counts.get("max_steps_exceeded", 0),
        "no_event_count": sum(int(row.get("native_event_count", 0) or 0) == 0 for row in rows),
        "replaced_network_error_tasks": replacements,
        "retry_manifests": retry_manifests,
        "manifest_sha256": manifest_sha256,
        "model_id": str(payload.get("model_id")),
        "model_execution_mode": str(payload.get("model_execution_mode")),
        "memory_backend": str(payload.get("memory_backend")),
        "runtime_attestation": attestation,
        **metadata,
        "source_artifact": {"path": str(source), "sha256": _sha256(source)},
        "claim_boundary": "official reward is workflow evaluator output, not memory accuracy",
        "production_latency_claim": False,
    }


def aggregate_e2e_submission_evidence(
    source_path: str | Path,
    *,
    expected_task_count: int = 5,
    source_commit: str,
    run_command: str,
) -> dict[str, Any]:
    """Validate a Qwen + Qdrant + Neo4j end-to-end task aggregate."""

    source, payload = _load(source_path)
    rows = payload.get("rows")
    if not isinstance(rows, list) or len(rows) != expected_task_count:
        raise ValueError(f"E2E aggregate must contain exactly {expected_task_count} task rows")
    task_ids: list[str] = []
    latencies: list[float] = []
    compact_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("every E2E row must be an object")
        task_id = str(row.get("task_id", ""))
        task_ids.append(task_id)
        latency = _finite_number(row.get("elapsed_ms"), "elapsed_ms", positive=True)
        latencies.append(latency)
        official = row.get("official")
        if not isinstance(official, Mapping) or official.get("status") != "available":
            raise ValueError("every E2E row must have an available official evaluator result")
        if row.get("status") != "completed":
            raise ValueError("every E2E task must complete")
        compact_rows.append(
            {
                "task_id": task_id,
                "elapsed_ms": latency,
                "native_event_count": int(row.get("native_event_count", 0) or 0),
                "official_reward": _finite_number(official.get("reward"), "official reward"),
            }
        )
    if any(not task_id for task_id in task_ids) or len(set(task_ids)) != expected_task_count:
        raise ValueError("E2E aggregate must contain unique task IDs")

    mean_ms = statistics.mean(latencies)
    p50_ms = statistics.median(latencies)
    if not math.isclose(
        _finite_number(payload.get("mean_ms"), "mean_ms"), mean_ms, rel_tol=0.0, abs_tol=1e-9
    ) or not math.isclose(
        _finite_number(payload.get("p50_ms"), "p50_ms"), p50_ms, rel_tol=0.0, abs_tol=1e-9
    ):
        raise ValueError("E2E latency totals do not match task rows")
    if payload.get("task_count") != expected_task_count:
        raise ValueError("E2E task_count does not match task rows")

    health = payload.get("backend_health")
    if not isinstance(health, Mapping):
        raise ValueError("E2E aggregate is missing backend health")
    normalized_health: dict[str, dict[str, Any]] = {}
    for service in ("qdrant", "neo4j"):
        item = health.get(service)
        if not isinstance(item, Mapping) or item.get("available") is not True or not item.get("version"):
            raise ValueError(f"E2E backend health is unavailable: {service}")
        normalized_health[service] = {
            "available": True,
            "version": str(item["version"]),
        }

    metadata = _run_metadata(
        model_revision=str(payload.get("model_revision", "")),
        model_server_build=str(payload.get("model_server_build", "")),
        source_commit=source_commit,
        run_command=run_command,
    )
    if str(payload.get("source_commit", "")) != metadata["source_commit"]:
        raise ValueError("E2E source_commit does not match the attested command")
    manifest_sha256 = _hex_digest(payload.get("manifest_sha256"), "manifest_sha256", {64})
    return {
        "schema_version": 1,
        "evidence_id": "qwen_vector_graph_e2e_5",
        "status": "complete",
        "benchmark": str(payload.get("benchmark")),
        "task_count": expected_task_count,
        "completed_count": expected_task_count,
        "task_ids": sorted(task_ids),
        "rows": compact_rows,
        "native_event_count": sum(row["native_event_count"] for row in compact_rows),
        "evaluator_available_task_count": expected_task_count,
        "mean_ms": mean_ms,
        "p50_ms": p50_ms,
        "model_id": str(payload.get("model")),
        "manifest_sha256": manifest_sha256,
        "backend_health": normalized_health,
        **metadata,
        "source_artifact": {"path": str(source), "sha256": _sha256(source)},
        "claim_boundary": "single-host end-to-end smoke; not production latency",
        "production_latency_claim": False,
    }


def aggregate_toxiproxy_submission_evidence(
    source_path: str | Path,
    *,
    expected_repetitions: int = 30,
    toxiproxy_version: str,
    source_commit: str,
    run_command: str,
    runtime_attestation: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate real-proxy fault evidence and recomputed backend readback state."""

    source, payload = _load(source_path)
    if expected_repetitions <= 0:
        raise ValueError("expected_repetitions must be positive")
    commit = _hex_digest(source_commit, "source_commit", {40, 64})
    command = _safe_run_command(run_command)
    version = str(toxiproxy_version or "").strip()
    if not version:
        raise ValueError("toxiproxy_version must be specified")
    attestation = _validate_toxiproxy_runtime_attestation(
        runtime_attestation,
        source=source,
        source_commit=commit,
        toxiproxy_version=version,
        run_command=command,
    )
    if payload.get("backend") != "vector-graph":
        raise ValueError("Toxiproxy evidence must use the vector-graph backend")

    health = payload.get("backend_health")
    if not isinstance(health, Mapping):
        raise ValueError("Toxiproxy evidence is missing backend health")
    normalized_health: dict[str, dict[str, Any]] = {}
    for service in ("qdrant", "neo4j"):
        item = health.get(service)
        if not isinstance(item, Mapping) or item.get("available") is not True or not item.get("version"):
            raise ValueError(f"Toxiproxy backend health is unavailable: {service}")
        normalized_health[service] = {
            "available": True,
            "version": str(item["version"]),
        }
    for service, service_version in normalized_health.items():
        if attestation["services"][service]["version"] != service_version["version"]:
            raise ValueError(f"Toxiproxy attestation service version mismatch: {service}")

    matrix = payload.get("fault_matrix")
    if not isinstance(matrix, Mapping):
        raise ValueError("Toxiproxy fault matrix is missing")
    if matrix.get("all_scenarios_evidence_valid") is not True:
        raise ValueError("Toxiproxy matrix contains invalid trigger evidence")
    if matrix.get("all_scenarios_state_verified") is not True:
        raise ValueError("Toxiproxy matrix does not attest state verification")
    if matrix.get("all_observed_states_consistent") is not True:
        raise ValueError("Toxiproxy matrix does not attest consistent observed state")
    raw_scenarios = matrix.get("scenarios")
    expected_names = set(_TOXIPROXY_SCENARIOS)
    if not isinstance(raw_scenarios, Mapping) or set(raw_scenarios) != expected_names:
        raise ValueError("Toxiproxy matrix must contain the five fixed scenarios")
    workload_events = _toxiproxy_workload_events(payload)

    scenarios: dict[str, dict[str, Any]] = {}
    state_totals: Counter[str] = Counter()
    for name in sorted(expected_names):
        row = raw_scenarios[name]
        if not isinstance(row, Mapping) or row.get("repetitions") != expected_repetitions:
            raise ValueError(f"Toxiproxy scenario repetition mismatch: {name}")
        repetitions = expected_repetitions
        observed_counts = _recompute_toxiproxy_fault_counts(name, row, repetitions)

        scenario_states = _recompute_toxiproxy_states(
            name, row, repetitions=repetitions, workload_events=workload_events
        )
        declared_state_counts = row.get("persistent_state_classification_counts")
        if not isinstance(declared_state_counts, Mapping) or set(declared_state_counts) != set(
            _STATE_CLASSIFICATIONS
        ):
            raise ValueError(f"Toxiproxy state counts are missing: {name}")
        for field, expected in scenario_states.items():
            if _nonnegative_int(declared_state_counts.get(field), f"{name}/{field}") != expected:
                raise ValueError(f"Toxiproxy state count mismatch: {name}/{field}")
        state_totals.update(scenario_states)

        scenarios[name] = {
            "repetitions": repetitions,
            **observed_counts,
            "state_counts": scenario_states,
        }

    return {
        "schema_version": 2,
        "evidence_id": "toxiproxy_state_verified_30",
        "status": "complete_state_verified_fault_observations",
        "scenario_count": len(scenarios),
        "repetitions_per_scenario": expected_repetitions,
        "total_repetitions": expected_repetitions * len(scenarios),
        "all_scenarios_evidence_valid": True,
        "all_scenarios_state_verified": True,
        "all_observed_states_consistent": True,
        "workload_events": workload_events,
        "state_totals": {state: state_totals[state] for state in ("complete", "absent", "partial", "unknown")},
        "backend_health": normalized_health,
        "toxiproxy_version": version,
        "scenarios": scenarios,
        "source_commit": commit,
        "run_command": command,
        "runtime_attestation": {
            "sha256": _mapping_sha256(attestation),
            "image_digests": {
                service: attestation["services"][service]["image_digest"]
                for service in ("qdrant", "neo4j", "toxiproxy")
            },
            "host_identity_sha256": attestation["host_identity_sha256"],
        },
        "source_artifact": {"basename": _safe_source_basename(source), "sha256": _sha256(source)},
        "claim_boundary": (
            "single-host real Qdrant/Neo4j with deterministic Toxiproxy fault injection and "
            "post-operation readback for the tested workload and five scenarios; not general "
            "distributed transactions, cross-host fault tolerance, availability, linearizability, "
            "or production latency"
        ),
        "production_latency_claim": False,
    }


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _strict_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _safe_run_command(value: Any) -> str:
    command = str(value or "").strip()
    privacy_view = command.replace("'", "").replace('"', "")
    if (
        not command
        or _SECRET_TEXT.search(privacy_view)
        or _ABSOLUTE_PATH.search(privacy_view)
        or _IP_LITERAL.search(privacy_view)
        or _HOSTNAME_TOKEN.search(privacy_view)
        or "://" in privacy_view
        or "@" in privacy_view
        or re.search(r"\b(?:ssh|scp|rsync)\b", privacy_view, re.I)
    ):
        raise ValueError("run_command is missing or contains private or secret-bearing text")
    return command


def _safe_source_basename(source: Path) -> str:
    basename = source.name
    if not _SAFE_BASENAME.fullmatch(basename):
        raise ValueError("source artifact basename is unsafe")
    return basename


def _recompute_toxiproxy_fault_counts(
    scenario: str, row: Mapping[str, Any], repetitions: int
) -> dict[str, int]:
    service, operation, action, recovery_action = _TOXIPROXY_SCENARIOS[scenario]
    expected_metadata = {
        "name": scenario,
        "service": service,
        "trigger_operation": operation,
        "action": action,
        "recovery_action": recovery_action,
    }
    for field, expected in expected_metadata.items():
        if row.get(field) != expected:
            raise ValueError(f"Toxiproxy scenario metadata mismatch: {scenario}/{field}")
    if _nonnegative_int(row.get("trigger_ordinal"), f"{scenario}/trigger_ordinal") != 1:
        raise ValueError(f"Toxiproxy scenario trigger ordinal mismatch: {scenario}")
    if _strict_bool(row.get("evidence_valid"), f"{scenario}/evidence_valid") is not True:
        raise ValueError(f"Toxiproxy scenario lacks valid evidence: {scenario}")
    evidence_rows = row.get("repetition_evidence")
    if not isinstance(evidence_rows, list) or len(evidence_rows) != repetitions:
        raise ValueError(f"Toxiproxy repetition evidence missing: {scenario}")

    fields = (
        "success_count",
        "error_count",
        "abort_count",
        "retry_count",
        "retry_success_count",
        "fault_evidence_count",
        "trigger_fired_count",
        "toxic_installed_count",
        "toxic_cleared_count",
        "proxy_path_verified_count",
        "fault_observed_count",
        "evidence_valid_count",
    )
    counts: Counter[str] = Counter()
    for evidence in evidence_rows:
        if not isinstance(evidence, Mapping) or evidence.get("scenario") != scenario:
            raise ValueError(f"Toxiproxy repetition evidence is malformed: {scenario}")
        if _strict_bool(evidence.get("evidence_valid"), f"{scenario}/evidence_valid") is not True:
            raise ValueError(f"Toxiproxy repetition evidence is invalid: {scenario}")
        routes = evidence.get("proxy_routes")
        if not isinstance(routes, Mapping) or not routes:
            raise ValueError(f"Toxiproxy proxy routes are missing: {scenario}")
        required_routes = ("qdrant", "neo4j") if scenario == "normal" else (service,)
        if any(route_name not in routes for route_name in required_routes):
            raise ValueError(f"Toxiproxy required proxy route is missing: {scenario}")
        for route_name, route in routes.items():
            expected_route = _TOXIPROXY_ROUTES.get(route_name)
            if (
                expected_route is None
                or not isinstance(route, Mapping)
                or route.get("service") != route_name
                or route.get("proxy_name") != expected_route[0]
                or route.get("upstream") != expected_route[1]
                or not str(route.get("client_endpoint", "")).strip()
                or not str(route.get("listen", "")).strip()
                or _strict_bool(route.get("verified"), f"{scenario}/{route_name}/verified") is not True
            ):
                raise ValueError(f"Toxiproxy proxy route is invalid: {scenario}/{route_name}")
        if _strict_bool(evidence.get("proxy_path_verified"), f"{scenario}/proxy_path_verified") is not True:
            raise ValueError(f"Toxiproxy proxy path is not verified: {scenario}")

        ordinals = evidence.get("request_ordinals")
        expected_ordinals = _TOXIPROXY_REQUEST_ORDINALS[scenario]
        if not isinstance(ordinals, Mapping) or set(ordinals) != set(expected_ordinals):
            raise ValueError(f"Toxiproxy request ordinals are missing: {scenario}")
        for key, expected_ordinal in expected_ordinals.items():
            if _nonnegative_int(ordinals.get(key), f"{scenario}/{key}") != expected_ordinal:
                raise ValueError(f"Toxiproxy request ordinal mismatch: {scenario}/{key}")

        events = evidence.get("events")
        if not isinstance(events, list):
            raise ValueError(f"Toxiproxy events are missing: {scenario}")
        non_normal = scenario != "normal"
        expected_flags = {
            "trigger_fired": non_normal,
            "toxic_installed": non_normal,
            "toxic_cleared": non_normal,
            "fault_observed": non_normal,
        }
        for field, expected in expected_flags.items():
            if _strict_bool(evidence.get(field), f"{scenario}/{field}") is not expected:
                raise ValueError(f"Toxiproxy repetition flag mismatch: {scenario}/{field}")

        counts["fault_evidence_count"] += 1
        counts["proxy_path_verified_count"] += 1
        counts["evidence_valid_count"] += 1
        for field, expected in expected_flags.items():
            counts[f"{field}_count"] += int(expected)
        if not non_normal:
            if events:
                raise ValueError("Toxiproxy normal scenario must not contain fault events")
            if any(_nonnegative_int(evidence.get(field), f"{scenario}/{field}") != 0 for field in ("retry_count", "retry_success_count")):
                raise ValueError("Toxiproxy normal scenario must not retry")
            counts["success_count"] += 1
            continue

        if len(events) != 1:
            raise ValueError(f"Toxiproxy scenario must contain one fault event: {scenario}")
        event = events[0]
        if not isinstance(event, Mapping):
            raise ValueError(f"Toxiproxy fault event is malformed: {scenario}")
        expected_event = {
            "scenario": scenario,
            "service": service,
            "operation": operation,
            "action": action,
            "recovery_action": recovery_action,
        }
        if any(event.get(field) != expected for field, expected in expected_event.items()):
            raise ValueError(f"Toxiproxy fault event metadata mismatch: {scenario}")
        if _nonnegative_int(event.get("request_ordinal"), f"{scenario}/event request_ordinal") != 1:
            raise ValueError(f"Toxiproxy fault event trigger ordinal mismatch: {scenario}")
        target_request = f"{service}:{operation}"
        if target_request not in ordinals or _nonnegative_int(ordinals[target_request], target_request) < 1:
            raise ValueError(f"Toxiproxy fault event request evidence is missing: {scenario}")
        for field in ("toxic_installed", "toxic_cleared", "proxy_path_verified", "fault_observed"):
            if _strict_bool(event.get(field), f"{scenario}/event {field}") is not True:
                raise ValueError(f"Toxiproxy fault event flag mismatch: {scenario}/{field}")
        if event.get("proxy_name") != _TOXIPROXY_ROUTES[service][0]:
            raise ValueError(f"Toxiproxy fault event proxy mismatch: {scenario}")

        if scenario == "delay":
            if event.get("observed_exception") is not None:
                raise ValueError("Toxiproxy delay event must complete without an exception")
            retry_count = retry_success_count = 0
            counts["success_count"] += 1
        elif scenario in {"timeout", "connection_drop"}:
            if not str(event.get("observed_exception", "")).strip():
                raise ValueError(f"Toxiproxy abort event is missing an exception: {scenario}")
            retry_count = retry_success_count = 0
            counts["error_count"] += 1
            counts["abort_count"] += 1
        else:
            if not str(event.get("observed_exception", "")).strip() or _strict_bool(
                event.get("retry_success"), f"{scenario}/event retry_success"
            ) is not True:
                raise ValueError("Toxiproxy retry-success event is invalid")
            retry_count = retry_success_count = 1
            counts["success_count"] += 1
        if _nonnegative_int(evidence.get("retry_count"), f"{scenario}/retry_count") != retry_count or _nonnegative_int(
            evidence.get("retry_success_count"), f"{scenario}/retry_success_count"
        ) != retry_success_count:
            raise ValueError(f"Toxiproxy repetition retry count mismatch: {scenario}")
        counts["retry_count"] += retry_count
        counts["retry_success_count"] += retry_success_count

    for field in fields:
        if _nonnegative_int(row.get(field), f"{scenario}/{field}") != counts[field]:
            raise ValueError(f"Toxiproxy scenario count mismatch: {scenario}/{field}")
    return {field: counts[field] for field in fields}


def _toxiproxy_workload_events(payload: Mapping[str, Any]) -> int:
    performance = payload.get("performance")
    rows = performance.get("rows") if isinstance(performance, Mapping) else None
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise ValueError("Toxiproxy evidence must contain one performance workload row")
    workload_events = _nonnegative_int(rows[0].get("workload_events"), "workload_events")
    if workload_events != 2:
        raise ValueError("Toxiproxy workload_events must equal 2")
    return workload_events


def _recompute_toxiproxy_states(
    scenario: str,
    row: Mapping[str, Any],
    *,
    repetitions: int,
    workload_events: int,
) -> Counter[str]:
    verifications = row.get("persistent_state_verifications")
    if not isinstance(verifications, list) or len(verifications) != repetitions:
        raise ValueError(f"Toxiproxy state-verification row count mismatch: {scenario}")
    required_state = "absent" if scenario in {"timeout", "connection_drop"} else "complete"
    counts: Counter[str] = Counter()
    for verification in verifications:
        if not isinstance(verification, Mapping):
            raise ValueError(f"Toxiproxy state verification must be an object: {scenario}")
        items = verification.get("items")
        if not isinstance(items, list) or len(items) != workload_events:
            raise ValueError(f"Toxiproxy state item count mismatch: {scenario}")
        memory_ids: list[str] = []
        item_states: list[str] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise ValueError(f"Toxiproxy state item must be an object: {scenario}")
            memory_id = str(item.get("memory_id", "")).strip()
            if not memory_id:
                raise ValueError(f"Toxiproxy state item is missing memory ID: {scenario}")
            memory_ids.append(memory_id)
            qdrant = item.get("qdrant")
            neo4j = item.get("neo4j")
            if not isinstance(qdrant, Mapping) or not isinstance(neo4j, Mapping):
                raise ValueError(f"Toxiproxy backend readback is missing: {scenario}")
            if qdrant.get("read_ok") is not True or neo4j.get("read_ok") is not True:
                raise ValueError(f"Toxiproxy backend readback failed: {scenario}")
            if qdrant.get("present") != neo4j.get("present") or qdrant.get("matches") != neo4j.get("matches"):
                raise ValueError(f"Toxiproxy backend readback disagrees: {scenario}")
            if qdrant.get("present") is True and qdrant.get("matches") is True:
                state = "complete"
            elif qdrant.get("present") is False and neo4j.get("present") is False:
                state = "absent"
            else:
                raise ValueError(f"Toxiproxy backend readback is partial or unknown: {scenario}")
            if item.get("classification") != state:
                raise ValueError(f"Toxiproxy item classification mismatch: {scenario}")
            item_states.append(state)
        if len(set(memory_ids)) != workload_events:
            raise ValueError(f"Toxiproxy state verification has duplicate memory IDs: {scenario}")
        if set(item_states) != {required_state} or verification.get("classification") != required_state:
            raise ValueError(f"Toxiproxy state classification mismatch: {scenario}")
        counts[required_state] += 1
    return Counter({state: counts[state] for state in ("complete", "absent", "partial", "unknown")})


def _validate_toxiproxy_runtime_attestation(
    raw_attestation: Mapping[str, Any],
    *,
    source: Path,
    source_commit: str,
    toxiproxy_version: str,
    run_command: str,
) -> dict[str, Any]:
    if not isinstance(raw_attestation, Mapping):
        raise ValueError("Toxiproxy runtime_attestation must be an object")
    attestation = dict(raw_attestation)
    if attestation.get("schema_version") != 1:
        raise ValueError("Toxiproxy runtime attestation schema_version must equal 1")
    captured_at = str(attestation.get("captured_at", "")).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", captured_at):
        raise ValueError("Toxiproxy runtime attestation captured_at must be UTC ISO-8601")
    if attestation.get("execution_scope") != "single_host_real_services":
        raise ValueError("Toxiproxy runtime attestation must describe single-host real services")
    if _hex_digest(attestation.get("source_commit"), "attestation source_commit", {40}) != source_commit:
        raise ValueError("Toxiproxy runtime attestation source_commit mismatch")
    if _hex_digest(attestation.get("source_artifact_sha256"), "source_artifact_sha256", {64}) != _sha256(source):
        raise ValueError("Toxiproxy runtime attestation source artifact hash mismatch")
    if attestation.get("exit_code") != 0:
        raise ValueError("Toxiproxy runtime attestation exit_code must equal 0")
    attested_command = _safe_run_command(attestation.get("run_command"))
    if attested_command != run_command:
        raise ValueError("Toxiproxy runtime attestation command mismatch")
    runtime = attestation.get("runtime")
    if not isinstance(runtime, Mapping):
        raise ValueError("Toxiproxy runtime attestation runtime is missing")
    for field in ("python", "docker", "compose", "kernel"):
        if not str(runtime.get(field, "")).strip():
            raise ValueError(f"Toxiproxy runtime attestation runtime version is missing: {field}")
    services = attestation.get("services")
    if not isinstance(services, Mapping) or set(services) != {"qdrant", "neo4j", "toxiproxy"}:
        raise ValueError("Toxiproxy runtime attestation must describe three services")
    for service in ("qdrant", "neo4j", "toxiproxy"):
        item = services[service]
        if not isinstance(item, Mapping):
            raise ValueError(f"Toxiproxy runtime attestation service is malformed: {service}")
        for field in ("version", "tag", "pull_source"):
            if not str(item.get(field, "")).strip():
                raise ValueError(f"Toxiproxy runtime attestation service field is missing: {service}/{field}")
        _hex_digest(item.get("image_digest"), f"{service} image_digest", {64})
    if str(services["toxiproxy"]["version"]) != toxiproxy_version:
        raise ValueError("Toxiproxy runtime attestation version mismatch")
    network_boundary = attestation.get("network_boundary")
    if not isinstance(network_boundary, Mapping) or network_boundary.get("data_services_directly_published") is not False or network_boundary.get("client_data_path") != "toxiproxy":
        raise ValueError("Toxiproxy runtime attestation network boundary is invalid")
    _hex_digest(attestation.get("host_identity_sha256"), "host_identity_sha256", {64})
    return attestation


def _mapping_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
