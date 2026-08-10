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
