"""Concurrent real-model load runner with endpoint-reported token accounting."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import shlex
import socket
import subprocess
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_conditions import source_identity
from txnmem_model_protocol import merge_usage_summaries
from txnmem_real_agent import run_real_agent
from txnmem_real_experiment import evaluate_task_contract, sanitize_run_report


def _percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(probability * len(ordered)) - 1))
    return ordered[index]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _observe_ssh_tunnel(
    model_endpoint: str,
    process_id: int | None,
    *,
    command_override: str | None = None,
) -> dict[str, Any]:
    base = {
        "status": "not_observed",
        "process_id": process_id,
        "agent_host_identity_sha256": hashlib.sha256(
            socket.gethostname().encode("utf-8")
        ).hexdigest(),
        "model_host_identity_sha256": None,
        "process_command_sha256": None,
        "local_forward_matches_model_endpoint": False,
        "raw_host_identities_committed": False,
        "raw_process_command_committed": False,
    }
    if process_id is None:
        return base
    if command_override is None:
        completed = subprocess.run(
            ["ps", "-p", str(process_id), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0 or not completed.stdout.strip():
            return {**base, "status": "process_not_found"}
        command = completed.stdout.strip()
    else:
        command = command_override.strip()
    try:
        tokens = shlex.split(command)
    except ValueError:
        return {**base, "status": "unparseable_process_command"}
    if not tokens or Path(tokens[0]).name != "ssh":
        return {**base, "status": "process_is_not_ssh"}
    forward_spec = None
    for index, token in enumerate(tokens):
        if token == "-L" and index + 1 < len(tokens):
            forward_spec = tokens[index + 1]
            break
        if token.startswith("-L") and len(token) > 2:
            forward_spec = token[2:]
            break
    remote_target = next((token for token in reversed(tokens) if "@" in token), None)
    endpoint = urlparse(str(model_endpoint))
    endpoint_port = endpoint.port
    local_port = None
    if forward_spec:
        parts = forward_spec.split(":")
        candidate = parts[-3] if len(parts) >= 3 else ""
        if candidate.isdigit():
            local_port = int(candidate)
    endpoint_is_loopback = endpoint.hostname in {"127.0.0.1", "localhost", "::1"}
    matches = bool(endpoint_is_loopback and endpoint_port and endpoint_port == local_port)
    observed = bool(forward_spec and remote_target and matches)
    return {
        **base,
        "status": "process_observed" if observed else "process_observed_but_mismatch",
        "model_host_identity_sha256": (
            hashlib.sha256(remote_target.encode("utf-8")).hexdigest()
            if remote_target
            else None
        ),
        "process_command_sha256": hashlib.sha256(command.encode("utf-8")).hexdigest(),
        "local_forward_matches_model_endpoint": matches,
    }


def run_model_load(
    manifest: Mapping[str, Any],
    model: Any,
    out_dir: Path,
    *,
    concurrency: int = 4,
    minimum_cycles: int = 1,
    minimum_duration_s: float = 0.0,
    max_steps: int = 12,
    execution_scope: str = "single_host_multi_agent",
    host_count: int = 1,
    network_transport: str = "loopback_or_unspecified",
    tunnel_process_id: int | None = None,
    tunnel_command_for_test: str | None = None,
    model_revision: str = "unspecified",
    model_server_build: str = "unknown",
) -> dict[str, Any]:
    """Run isolated Agent attempts concurrently for cycles and/or a duration.

    Each attempt gets its own in-memory backend.  Raw messages and events are
    retained only in ``data/model_load_traces.jsonl``.  A cross-host
    client/server run records one Agent-worker host and a distinct model-server
    host; it never claims that Agent workers themselves span multiple hosts.
    """

    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    if minimum_cycles < 1:
        raise ValueError("minimum_cycles must be positive")
    if minimum_duration_s < 0:
        raise ValueError("minimum_duration_s must be non-negative")
    if max_steps < 1:
        raise ValueError("max_steps must be positive")
    if execution_scope not in {"single_host_multi_agent", "cross_host_client_server"}:
        raise ValueError("unsupported execution_scope")
    if execution_scope == "single_host_multi_agent" and host_count != 1:
        raise ValueError("single_host_multi_agent requires host_count=1")
    if execution_scope == "cross_host_client_server" and host_count < 2:
        raise ValueError("cross_host_client_server requires host_count>=2")
    if not str(network_transport).strip():
        raise ValueError("network_transport must be non-empty")
    if model is None or not callable(getattr(model, "complete", None)):
        raise ValueError("a configured model client is required")
    tasks = manifest.get("tasks") if isinstance(manifest, Mapping) else None
    if not isinstance(tasks, list) or not tasks or not all(isinstance(task, Mapping) for task in tasks):
        raise ValueError("manifest.tasks must be a non-empty list of mappings")

    root = Path(out_dir)
    raw_path = root / "data" / "model_load_traces.jsonl"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    attempts: list[dict[str, Any]] = []
    started_at_utc = datetime.now(timezone.utc).isoformat()
    run_started = perf_counter()
    completed_cycles = 0
    in_flight_lock = Lock()
    in_flight = 0
    observed_peak_in_flight = 0

    def run_attempt(task: Mapping[str, Any], cycle: int, task_index: int) -> dict[str, Any]:
        nonlocal in_flight, observed_peak_in_flight
        with in_flight_lock:
            in_flight += 1
            observed_peak_in_flight = max(observed_peak_in_flight, in_flight)
        attempt = dict(task)
        task_id = str(attempt.get("task_id") or f"task_{task_index:04d}")
        attempt_id = f"cycle_{cycle:04d}:{task_id}"
        attempt["task_id"] = attempt_id
        attempt["agent_id"] = f"load_agent_{cycle:04d}_{task_index:04d}"
        started = perf_counter()
        try:
            run = run_real_agent(
                attempt,
                model,
                InstrumentedMemoryBackend(),
                max_steps=int(attempt.get("max_steps", max_steps)),
                seed=int(attempt.get("seed", task_index - 1)) + cycle * 100000,
                temperature=float(attempt.get("temperature", 0.0)),
            )
            return {
                "attempt_id": attempt_id,
                "source_task_id": task_id,
                "cycle": cycle,
                "agent_id": attempt["agent_id"],
                "latency_ms": (perf_counter() - started) * 1000.0,
                "run": run,
                "task_evaluator": evaluate_task_contract(task, run),
            }
        finally:
            with in_flight_lock:
                in_flight -= 1

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while completed_cycles < minimum_cycles or perf_counter() - run_started < minimum_duration_s:
            cycle = completed_cycles + 1
            futures = [
                executor.submit(run_attempt, task, cycle, index)
                for index, task in enumerate(tasks, start=1)
            ]
            attempts.extend(future.result() for future in futures)
            completed_cycles += 1

    elapsed_s = perf_counter() - run_started
    ended_at_utc = datetime.now(timezone.utc).isoformat()
    with raw_path.open("w", encoding="utf-8") as handle:
        for attempt in attempts:
            handle.write(json.dumps(attempt, ensure_ascii=False) + "\n")

    usage = merge_usage_summaries(
        [attempt["run"].get("model_usage", {}) for attempt in attempts]
    )
    latencies = [float(attempt["latency_ms"]) for attempt in attempts]
    failures = Counter(
        str(attempt["run"].get("failure_code", "none"))
        for attempt in attempts
        if attempt["run"].get("status") != "completed"
    )
    task_summaries = [
        sanitize_run_report(
            {
                "attempt_id": attempt["attempt_id"],
                "source_task_id": attempt["source_task_id"],
                "cycle": attempt["cycle"],
                "agent_id": attempt["agent_id"],
                "latency_ms": attempt["latency_ms"],
                "status": attempt["run"].get("status"),
                "failure_code": attempt["run"].get("failure_code"),
                "native_event_count": len(attempt["run"].get("events", [])),
                "model_usage": attempt["run"].get("model_usage", {}),
                "task_evaluator": attempt["task_evaluator"],
            }
        )
        for attempt in attempts
    ]
    topology_attestation = _observe_ssh_tunnel(
        str(getattr(model, "endpoint", "")),
        tunnel_process_id,
        command_override=tunnel_command_for_test,
    )
    topology_attested = bool(
        execution_scope == "cross_host_client_server"
        and topology_attestation["status"] == "process_observed"
    )
    import txnmem_model_protocol as model_protocol_module
    import txnmem_real_agent as real_agent_module
    import txnmem_real_experiment as real_experiment_module

    execution_identity = {
        "model_revision": str(model_revision),
        "model_revision_status": (
            "sha256"
            if len(str(model_revision)) == 64
            and all(character in "0123456789abcdefABCDEF" for character in str(model_revision))
            else "unspecified_or_non_hash"
        ),
        "model_server_build": str(model_server_build),
        "runner_source_identity": source_identity(
            {
                "txnmem_model_load": Path(__file__),
                "txnmem_model_protocol": Path(model_protocol_module.__file__),
                "txnmem_real_agent": Path(real_agent_module.__file__),
                "txnmem_real_experiment": Path(real_experiment_module.__file__),
            }
        ),
    }

    report = {
        "dataset": str(manifest.get("dataset_name", "model-load")),
        "model_id": str(getattr(model, "model", "unknown")),
        "execution_scope": execution_scope,
        "host_count": host_count,
        "agent_worker_host_count": 1,
        "model_server_host_count": 1,
        "network_transport": str(network_transport),
        "topology_attested": topology_attested,
        "topology_attestation": topology_attestation,
        "execution_identity": execution_identity,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "configured_concurrency": concurrency,
        "observed_peak_in_flight": observed_peak_in_flight,
        "task_count_per_cycle": len(tasks),
        "completed_cycles": completed_cycles,
        "minimum_cycles": minimum_cycles,
        "minimum_duration_seconds": float(minimum_duration_s),
        "elapsed_seconds": elapsed_s,
        "duration_target_met": elapsed_s >= minimum_duration_s,
        "attempt_count": len(attempts),
        "completed_attempt_count": sum(
            attempt["run"].get("status") == "completed" for attempt in attempts
        ),
        "failed_attempt_count": sum(
            attempt["run"].get("status") != "completed" for attempt in attempts
        ),
        "failure_counts": dict(sorted(failures.items())),
        "native_event_count": sum(len(attempt["run"].get("events", [])) for attempt in attempts),
        "throughput_attempts_per_second": len(attempts) / elapsed_s if elapsed_s else 0.0,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "max": max(latencies, default=0.0),
        },
        "model_usage": usage,
        "token_usage_complete": bool(usage["request_count"])
        and usage["request_count"] == usage["responses_with_usage"],
        "tokens_per_second": usage["total_tokens"] / elapsed_s if elapsed_s else 0.0,
        "token_cost_basis": "endpoint_reported_usage",
        "monetary_cost_status": "not_computed_without_an_explicit_pricing_rate",
        "cross_host_network_claim": topology_attested,
        "cross_host_multi_agent_workers_claim": False,
        "production_latency_claim": False,
        "raw_trace_path": str(raw_path),
        "raw_traces_committed": False,
        "task_summaries": task_summaries,
    }
    sanitized = sanitize_run_report(report)
    _write_json(root / "results" / "model_load_summary.json", sanitized)
    return sanitized
