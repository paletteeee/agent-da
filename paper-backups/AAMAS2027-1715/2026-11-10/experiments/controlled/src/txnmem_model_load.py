"""Concurrent real-model load runner with endpoint-reported token accounting."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
import re
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


_ATTESTED_SSH_OPTIONS = frozenset(
    {
        "ControlPersist=no",
        "ServerAliveInterval=30",
        "ServerAliveCountMax=3",
        "ExitOnForwardFailure=yes",
    }
)

_ATTESTATION_CONTINUITY_FIELDS = (
    "process_id",
    "agent_host_identity_sha256",
    "model_host_identity_sha256",
    "ssh_target_identity_sha256",
    "process_command_sha256",
    "forwarding_binding",
    "listener_binding",
)


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


def _parse_attested_ssh_command(tokens: list[str]) -> dict[str, Any] | None:
    """Accept only direct, config-free SSH forwarding command shapes."""

    if not tokens or Path(tokens[0]).name != "ssh":
        return None
    index = 1
    has_master = False
    has_no_remote_command = False
    has_no_config = False
    control_socket = None
    ssh_port = None
    forward_spec = None
    tty_option = None
    ssh_options: set[str] = set()
    while index < len(tokens):
        token = tokens[index]
        if token == "-M":
            if has_master:
                return None
            has_master = True
            index += 1
            continue
        if token == "-N":
            if has_no_remote_command:
                return None
            has_no_remote_command = True
            index += 1
            continue
        if token in {"-t", "-tt"}:
            if tty_option is not None:
                return None
            tty_option = token
            index += 1
            continue
        if token in {"-F", "-S", "-p", "-L", "-o"}:
            if index + 1 >= len(tokens):
                return None
            value = tokens[index + 1]
            if token == "-F":
                if has_no_config or value != "/dev/null":
                    return None
                has_no_config = True
            elif token == "-S":
                if control_socket is not None or not value:
                    return None
                control_socket = value
            elif token == "-p":
                if ssh_port is not None or not value.isdigit() or not 0 < int(value) < 65536:
                    return None
                ssh_port = value
            elif token == "-L":
                if forward_spec is not None:
                    return None
                forward_spec = value
            else:
                if value not in _ATTESTED_SSH_OPTIONS or value in ssh_options:
                    return None
                ssh_options.add(value)
            index += 2
            continue
        break
    destinations = tokens[index:]
    if (
        not has_no_config
        or not has_master
        or not has_no_remote_command
        or control_socket is None
        or forward_spec is None
        or ssh_options != _ATTESTED_SSH_OPTIONS
        or len(destinations) != 1
        or destinations[0].count("@") != 1
    ):
        return None
    remote_target = destinations[0]
    user, host = remote_target.rsplit("@", 1)
    if not user or not host:
        return None
    try:
        target_ip = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return None
    forward_match = re.fullmatch(
        r"127\.0\.0\.1:([1-9][0-9]{0,4}):127\.0\.0\.1:([1-9][0-9]{0,4})",
        forward_spec,
    )
    if forward_match is None:
        return None
    local_port = int(forward_match.group(1))
    remote_port = int(forward_match.group(2))
    if local_port >= 65536 or remote_port >= 65536:
        return None
    return {
        "remote_target": remote_target,
        "target_is_loopback": target_ip.is_loopback,
        "target_is_public": target_ip.is_global,
        "control_socket": control_socket,
        "has_master": has_master,
        "ssh_port": ssh_port,
        "local_port": local_port,
        "forwarding_binding": {
            "local_address_class": "ipv4_loopback",
            "local_port": local_port,
            "remote_address_class": "ipv4_loopback",
            "remote_port": remote_port,
        },
        "exit_on_forward_failure": True,
    }


def _controlmaster_pid(
    control_options: list[str], remote_target: str
) -> tuple[bool, int | None]:
    check = subprocess.run(
        [*control_options, "-O", "check", remote_target],
        check=False,
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        return False, None
    pid_matches = re.findall(r"\bpid=(\d+)\b", f"{check.stdout}\n{check.stderr}")
    if len(pid_matches) != 1:
        return True, None
    return True, int(pid_matches[0])


def _listener_is_owned_by_process(output: str, process_id: int, port: int) -> bool:
    observed_process_id: int | None = None
    in_file_record = False
    exact_name_observed = False
    listen_state_observed = False
    expected_name = f"127.0.0.1:{port}"

    def current_record_matches() -> bool:
        return bool(
            observed_process_id == process_id
            and in_file_record
            and exact_name_observed
            and listen_state_observed
        )

    for line in output.splitlines():
        if line.startswith("p"):
            if current_record_matches():
                return True
            value = line[1:]
            observed_process_id = int(value) if value.isdigit() else None
            in_file_record = False
            exact_name_observed = False
            listen_state_observed = False
        elif line.startswith("f"):
            if current_record_matches():
                return True
            in_file_record = bool(line[1:])
            exact_name_observed = False
            listen_state_observed = False
        elif line.startswith("n") and in_file_record:
            exact_name_observed = line[1:] == expected_name
        elif line.startswith("TST=") and in_file_record:
            listen_state_observed = line == "TST=LISTEN"
    return current_record_matches()


def _observe_ssh_tunnel(
    model_endpoint: str,
    process_id: int | None,
    *,
    command_override: str | None = None,
) -> dict[str, Any]:
    agent_host_identity_sha256 = hashlib.sha256(
        socket.gethostname().encode("utf-8")
    ).hexdigest()
    base = {
        "status": "not_observed",
        "process_id": process_id,
        "agent_host_identity_sha256": agent_host_identity_sha256,
        "model_host_identity_sha256": None,
        "model_host_identity_source": None,
        "ssh_target_identity_sha256": None,
        "host_identities_distinct": False,
        "controlmaster_precheck_verified": False,
        "controlmaster_precheck_pid_matches_tunnel": False,
        "controlmaster_postcheck_verified": False,
        "controlmaster_postcheck_pid_matches_tunnel": False,
        "controlmaster_session_verified": False,
        "controlmaster_pid_matches_tunnel": False,
        "process_command_sha256": None,
        "exit_on_forward_failure": False,
        "local_forward_matches_model_endpoint": False,
        "forwarding_binding": {
            "local_address_class": None,
            "local_port": None,
            "remote_address_class": None,
            "remote_port": None,
        },
        "listener_binding": {
            "address_class": "ipv4_loopback",
            "port": None,
            "owned_by_tunnel_process": False,
        },
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
    parsed = _parse_attested_ssh_command(tokens)
    command_sha256 = hashlib.sha256(command.encode("utf-8")).hexdigest()
    if parsed is None:
        return {
            **base,
            "status": "process_observed_but_unsafe_ssh_command",
            "process_command_sha256": command_sha256,
        }
    remote_target = str(parsed["remote_target"])
    try:
        endpoint = urlparse(str(model_endpoint))
        endpoint_port = endpoint.port
    except ValueError:
        endpoint_port = None
        endpoint = urlparse("")
    endpoint_is_loopback = endpoint.hostname == "127.0.0.1"
    matches = bool(
        endpoint_is_loopback
        and endpoint_port
        and endpoint_port == parsed["local_port"]
    )
    common = {
        "ssh_target_identity_sha256": hashlib.sha256(
            remote_target.encode("utf-8")
        ).hexdigest(),
        "process_command_sha256": command_sha256,
        "exit_on_forward_failure": bool(parsed["exit_on_forward_failure"]),
        "local_forward_matches_model_endpoint": matches,
        "forwarding_binding": dict(parsed["forwarding_binding"]),
        "listener_binding": {
            "address_class": "ipv4_loopback",
            "port": parsed["local_port"],
            "owned_by_tunnel_process": False,
        },
    }
    if not matches:
        return {
            **base,
            **common,
            "status": "process_observed_but_mismatch",
        }
    if parsed["target_is_loopback"]:
        return {
            **base,
            **common,
            "status": "process_observed_but_remote_target_loopback",
        }
    if not parsed["target_is_public"]:
        return {**base, **common, "status": "process_observed_but_unsafe_ssh_command"}
    control_socket = str(parsed["control_socket"])
    control_options = ["ssh", "-F", "/dev/null", "-S", control_socket]
    if parsed["ssh_port"]:
        control_options.extend(["-p", str(parsed["ssh_port"])])
    precheck_verified, precheck_pid = _controlmaster_pid(
        control_options, remote_target
    )
    if not precheck_verified:
        return {
            **base,
            **common,
            "status": "process_observed_but_controlmaster_check_failed",
        }
    precheck = {
        "controlmaster_precheck_verified": True,
        "controlmaster_precheck_pid_matches_tunnel": precheck_pid == process_id,
    }
    if precheck_pid is None:
        return {
            **base,
            **common,
            **precheck,
            "status": "process_observed_but_controlmaster_pid_unavailable",
        }
    if precheck_pid != process_id:
        return {
            **base,
            **common,
            **precheck,
            "status": "process_observed_but_controlmaster_pid_mismatch",
        }
    hostname = subprocess.run(
        [*control_options, remote_target, "hostname"],
        check=False,
        capture_output=True,
        text=True,
    )
    remote_hostname = hostname.stdout.strip() if hostname.returncode == 0 else ""
    if not remote_hostname:
        return {
            **base,
            **common,
            **precheck,
            "status": "process_observed_but_remote_hostname_unavailable",
        }
    postcheck_verified, postcheck_pid = _controlmaster_pid(
        control_options, remote_target
    )
    postcheck = {
        "controlmaster_postcheck_verified": postcheck_verified,
        "controlmaster_postcheck_pid_matches_tunnel": postcheck_pid == process_id,
    }
    if not postcheck_verified:
        return {
            **base,
            **common,
            **precheck,
            **postcheck,
            "status": "process_observed_but_controlmaster_postcheck_failed",
        }
    if postcheck_pid is None:
        return {
            **base,
            **common,
            **precheck,
            **postcheck,
            "status": "process_observed_but_controlmaster_postcheck_pid_unavailable",
        }
    if postcheck_pid != process_id:
        return {
            **base,
            **common,
            **precheck,
            **postcheck,
            "status": "process_observed_but_controlmaster_postcheck_pid_mismatch",
        }
    observed_host_identity = hashlib.sha256(remote_hostname.encode("utf-8")).hexdigest()
    identities_distinct = observed_host_identity != agent_host_identity_sha256.lower()
    listener = subprocess.run(
        [
            "lsof",
            "-nP",
            "-a",
            "-p",
            str(process_id),
            f"-iTCP@127.0.0.1:{parsed['local_port']}",
            "-sTCP:LISTEN",
            "-FpcnT",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    listener_owned = bool(
        listener.returncode == 0
        and _listener_is_owned_by_process(
            listener.stdout, process_id, int(parsed["local_port"])
        )
    )
    verified = {
        **precheck,
        **postcheck,
        "model_host_identity_sha256": observed_host_identity,
        "model_host_identity_source": (
            "ssh_controlmaster_bound_remote_hostname_sha256"
        ),
        "host_identities_distinct": identities_distinct,
        "controlmaster_session_verified": True,
        "controlmaster_pid_matches_tunnel": True,
        "listener_binding": {
            "address_class": "ipv4_loopback",
            "port": parsed["local_port"],
            "owned_by_tunnel_process": listener_owned,
        },
    }
    if not listener_owned:
        return {
            **base,
            **common,
            **verified,
            "status": "process_observed_but_listener_not_owned",
        }
    status = (
        "process_observed"
        if identities_distinct
        else "process_observed_but_model_host_not_distinct"
    )
    return {
        **base,
        **common,
        **verified,
        "status": status,
    }


def _attestations_are_continuous(
    preflight: Mapping[str, Any], final: Mapping[str, Any]
) -> bool:
    return bool(
        preflight.get("status") == "process_observed"
        and final.get("status") == "process_observed"
        and all(
            preflight.get(field) == final.get(field)
            for field in _ATTESTATION_CONTINUITY_FIELDS
        )
    )


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
    if execution_scope == "cross_host_client_server" and host_count != 2:
        raise ValueError("cross_host_client_server requires host_count=2")
    if (
        execution_scope == "cross_host_client_server"
        and network_transport != "ssh_local_port_forward"
    ):
        raise ValueError(
            "cross_host_client_server requires network_transport=ssh_local_port_forward"
        )
    if not str(network_transport).strip():
        raise ValueError("network_transport must be non-empty")
    if model is None or not callable(getattr(model, "complete", None)):
        raise ValueError("a configured model client is required")
    tasks = manifest.get("tasks") if isinstance(manifest, Mapping) else None
    if not isinstance(tasks, list) or not tasks or not all(isinstance(task, Mapping) for task in tasks):
        raise ValueError("manifest.tasks must be a non-empty list of mappings")

    model_endpoint = str(getattr(model, "endpoint", ""))
    topology_preflight_attestation = _observe_ssh_tunnel(
        model_endpoint,
        tunnel_process_id if execution_scope == "cross_host_client_server" else None,
    )
    if (
        execution_scope == "cross_host_client_server"
        and topology_preflight_attestation["status"] != "process_observed"
    ):
        raise ValueError(
            "cross-host topology preflight failed: "
            f"{topology_preflight_attestation['status']}"
        )

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
        model_endpoint,
        tunnel_process_id if execution_scope == "cross_host_client_server" else None,
    )
    topology_continuity_verified = bool(
        execution_scope == "cross_host_client_server"
        and _attestations_are_continuous(
            topology_preflight_attestation, topology_attestation
        )
    )
    topology_attested = bool(
        execution_scope == "cross_host_client_server"
        and topology_continuity_verified
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
        "topology_continuity_verified": topology_continuity_verified,
        "topology_preflight_attestation": topology_preflight_attestation,
        "topology_attestation": topology_attestation,
        "execution_identity": execution_identity,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "configured_concurrency": concurrency,
        "observed_peak_in_flight": observed_peak_in_flight,
        "generation_parameters": {
            "max_steps": max_steps,
            "max_tokens": getattr(model, "max_tokens", None),
            "timeout_seconds": getattr(model, "timeout_s", None),
        },
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
