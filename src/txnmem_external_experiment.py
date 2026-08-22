#!/usr/bin/env python3
"""Reproducible replay runner for controlled and external memory baselines.

The independent invariant oracle remains in :mod:`txnmem_metrics`; this
module only selects adapters, classifies replay failures, and publishes bound
artifacts.  Optional backends are imported only if explicitly selected.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Callable, Iterable, Sequence

from txnmem_adapter_contract import (
    CapabilitySupport,
    RuntimeAdapterError,
    UnsupportedMappingError,
)
from txnmem_metrics import result_row, summarize
from txnmem_schema import validate_instance


SCHEMA_VERSION = "txnmem-external-runner-v1"
REGISTRY_ORDER = ("AppendOnly", "LastWriteWins", "MetadataFiltered", "Mem0", "LangGraphStore")
CAPABILITY_DIMENSIONS = (
    "single_record_read_write", "atomic_multi_record_commit", "commit_policy_revalidation",
    "shared_scope_isolation", "version_supersession", "provenance_propagation",
    "recursive_provenance_invalidation", "crash_recovery",
)
RESULTS_FIELDS = (
    "instance_id", "workload", "seed", "adapter", "variant", "adapter_version", "backend_mode",
    "run_status", "error_category", "capability_absent_observed", "correctness_included",
    "transaction_state", "partial_update_rate", "invalid_commit_rate", "stale_write_rate",
    "repair_recall", "leak_rate", "supersession_consistency", "scope_bypass_rate", "latency",
    "latency_ms", "any_violation", "violations", "committed_count", "operation_count", "repair_count",
)
CAPABILITIES_FIELDS = ("adapter", "adapter_version", "backend_mode", "capability", "supported", "detail")


@dataclass
class RunContext:
    """Run-scoped inputs exposed to adapter factories without CLI secrets."""

    run_id: str = field(default_factory=lambda: f"run-{uuid.uuid4().hex}")
    mem0_state_root: Path | None = None
    backend_state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdapterRegistration:
    name: str
    adapter_version: str
    backend_mode: str
    factory: Callable[[RunContext], Any]
    capabilities: Callable[[], Iterable[CapabilitySupport]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_json_bytes(value))


def _sha256(path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    return {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}


def _git_state() -> dict[str, Any]:
    def command(*args: str) -> str | None:
        try:
            completed = subprocess.run(args, check=True, capture_output=True, text=True)
        except (OSError, subprocess.CalledProcessError):
            return None
        return completed.stdout.strip()

    head = command("git", "rev-parse", "HEAD")
    dirty = command("git", "status", "--porcelain")
    return {"head": head, "dirty": bool(dirty) if dirty is not None else None}


def _environment() -> dict[str, Any]:
    distributions = sorted(
        (
            {"name": distribution.metadata.get("Name", distribution.metadata["Name"]), "version": distribution.version}
            for distribution in importlib.metadata.distributions()
        ),
        key=lambda item: (str(item["name"]).lower(), str(item["version"])),
    )
    try:
        freeze = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"], check=True, capture_output=True, text=True
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError):
        freeze = []
    versions_by_name = {str(item["name"]).lower(): item["version"] for item in distributions}
    return {
        "captured_at_utc": _utc_now(),
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "executable": sys.executable,
        },
        "platform": platform.platform(),
        "installed_packages": distributions,
        "package_versions": {
            package: versions_by_name.get(package)
            for package in ("mem0ai", "langgraph", "langgraph-checkpoint-postgres", "psycopg-binary")
        },
        "pip_freeze": sorted(freeze),
        "git": _git_state(),
    }


def _controlled_capabilities(name: str) -> tuple[CapabilitySupport, ...]:
    """Describe the real, intentionally limited behavior of each controlled baseline."""

    return (
        CapabilitySupport("single_record_read_write", True, f"{name} performs deterministic immediate single-record replay."),
        CapabilitySupport("atomic_multi_record_commit", False, "Controlled baseline writes are immediate."),
        CapabilitySupport("commit_policy_revalidation", False, "Controlled baseline does not revalidate policy at commit."),
        CapabilitySupport("shared_scope_isolation", name == "MetadataFiltered", "Only MetadataFiltered checks agent and scope metadata on reads/search; this is controlled filtering."),
        CapabilitySupport("version_supersession", name == "LastWriteWins", "Only LastWriteWins applies ordered old/new replacement updates; it is not atomic."),
        CapabilitySupport("provenance_propagation", False, "Controlled baseline has no native provenance propagation."),
        CapabilitySupport("recursive_provenance_invalidation", False, "Controlled baseline does not traverse provenance."),
        CapabilitySupport("crash_recovery", False, "Controlled baseline has no durable crash recovery."),
    )


def _validate_run_id(run_id: str) -> None:
    """Permit one portable path component only; it is used below a caller-owned root."""

    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", run_id):
        raise ValueError("run_id must be a safe path component")


def _default_registry(context: RunContext, selected: set[str], out_dir: Path) -> tuple[AdapterRegistration, ...]:
    """Create only selected production registrations, preserving optional imports."""

    records: dict[str, AdapterRegistration] = {}
    if {"AppendOnly", "LastWriteWins", "MetadataFiltered"} & selected:
        from txnmem_controlled_baselines import AppendOnlyAdapter, LastWriteWinsAdapter, MetadataFilteredAdapter

        for name, adapter_type in (
            ("AppendOnly", AppendOnlyAdapter),
            ("LastWriteWins", LastWriteWinsAdapter),
            ("MetadataFiltered", MetadataFilteredAdapter),
        ):
            if name in selected:
                records[name] = AdapterRegistration(
                    name, "controlled-v1", "controlled", lambda run_context, cls=adapter_type: cls(),
                    lambda baseline=name: _controlled_capabilities(baseline),
                )

    if "Mem0" in selected:
        base_root = (context.mem0_state_root or (out_dir / "backend_state" / "mem0")).resolve()
        state_root = base_root / context.run_id
        try:
            state_root.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            raise ValueError(f"Mem0 run root already exists: {state_root}") from exc
        context.backend_state["mem0"] = {
            "mode": "embedded_qdrant", "base_root": str(base_root), "root": str(state_root.resolve()),
        }

        def mem0_factory(run_context: RunContext) -> Any:
            from txnmem_mem0_adapter import Mem0Adapter, deterministic_mem0_factory

            return Mem0Adapter(deterministic_mem0_factory(state_root))

        def mem0_capabilities() -> Iterable[CapabilitySupport]:
            from txnmem_mem0_adapter import mem0_capabilities as supplied

            return supplied(persistent_reopen=True)

        records["Mem0"] = AdapterRegistration("Mem0", "mem0-oss-2.0.18", "embedded_qdrant", mem0_factory, mem0_capabilities)

    if "LangGraphStore" in selected:
        dsn = os.environ.get("TXNMEM_LANGGRAPH_POSTGRES_DSN")
        fallback_reason: str | None = None
        if dsn:
            try:
                from langgraph.store.postgres import PostgresStore

                def persistent_factory() -> Any:
                    return PostgresStore.from_conn_string(dsn)

                with persistent_factory() as store:
                    store.setup()
                context.backend_state["langgraph_store"] = {"mode": "postgres_persistent", "configured": True}

                def langgraph_factory(run_context: RunContext) -> Any:
                    from txnmem_langgraph_adapter import LangGraphStoreAdapter

                    return LangGraphStoreAdapter(persistent_factory, experiment_run_id=run_context.run_id, persistent_store=True)

                mode = "postgres_persistent"
            except Exception as exc:  # Preflight failure is recorded without leaking the DSN.
                fallback_reason = f"postgres_preflight_failed:{type(exc).__name__}"
        else:
            fallback_reason = "postgres_dsn_not_configured"

        if fallback_reason is not None:
            try:
                from langgraph.store.memory import InMemoryStore
            except Exception as exc:
                context.backend_state["langgraph_store"] = {
                    "mode": "unavailable_optional_dependency",
                    "reason": f"{fallback_reason};in_memory_import_failed:{type(exc).__name__}",
                }

                def langgraph_factory(run_context: RunContext) -> Any:
                    raise RuntimeAdapterError("LangGraph Store optional dependency is unavailable")

                mode = "unavailable_optional_dependency"
            else:
                context.backend_state["langgraph_store"] = {
                    "mode": "in_memory_fallback", "reason": fallback_reason,
                }

                def langgraph_factory(run_context: RunContext) -> Any:
                    from txnmem_langgraph_adapter import LangGraphStoreAdapter

                    return LangGraphStoreAdapter(InMemoryStore, experiment_run_id=run_context.run_id)

                mode = "in_memory_fallback"

        def langgraph_capabilities() -> Iterable[CapabilitySupport]:
            from txnmem_langgraph_adapter import langgraph_capabilities as supplied

            return supplied()

        records["LangGraphStore"] = AdapterRegistration("LangGraphStore", "langgraph-store-v1", mode, langgraph_factory, langgraph_capabilities)

    return tuple(records[name] for name in REGISTRY_ORDER if name in records)


def _validate_instances(instances: Sequence[dict[str, Any]]) -> None:
    identifiers: set[str] = set()
    for instance in instances:
        if not isinstance(instance, dict):
            raise ValueError("each JSONL record must be an object")
        validate_instance(instance)
        instance_id = instance["instance_id"]
        if instance_id in identifiers:
            raise ValueError(f"duplicate instance_id: {instance_id}")
        identifiers.add(instance_id)


def _has_crash(instance: dict[str, Any]) -> bool:
    return any(event.get("type") == "crash" for event in instance.get("failure_schedule", []))


def _capability_absent(observation: Any) -> bool:
    return any(event.get("event") == "capability_absent" for event in observation.trace)


def _base_row(instance: dict[str, Any], registration: AdapterRegistration, latency_ms: float) -> dict[str, Any]:
    return {
        "instance_id": instance["instance_id"], "workload": instance["workload"], "seed": instance["seed"],
        "adapter": registration.name, "variant": registration.name,
        "adapter_version": registration.adapter_version, "backend_mode": registration.backend_mode,
        "latency_ms": latency_ms,
    }


def _excluded_row(instance: dict[str, Any], registration: AdapterRegistration, category: str, latency_ms: float) -> dict[str, Any]:
    row = _base_row(instance, registration, latency_ms)
    row.update({"run_status": "excluded", "error_category": category, "capability_absent_observed": False, "correctness_included": False})
    for field in RESULTS_FIELDS:
        row.setdefault(field, "")
    return row


def _error_record(
    instance: dict[str, Any], registration: AdapterRegistration, category: str, latency_ms: float, error: Exception
) -> dict[str, Any]:
    """Emit only stable, non-sensitive attempt metadata; never backend messages or traces."""

    return {
        "instance_id": instance["instance_id"], "workload": instance["workload"], "seed": instance["seed"],
        "adapter": registration.name, "adapter_version": registration.adapter_version,
        "backend_mode": registration.backend_mode, "run_status": "excluded", "error_category": category,
        "error_type": type(error).__name__, "latency_ms": latency_ms,
    }


def _success_row(instance: dict[str, Any], registration: AdapterRegistration, observation: Any, latency_ms: float) -> dict[str, Any]:
    # This is intentionally the single adapter-to-oracle conversion point.
    oracle_row = result_row(instance, observation.to_oracle_result(registration.name))
    row = _base_row(instance, registration, latency_ms)
    row.update(oracle_row)
    row.update({
        "run_status": "success", "error_category": "", "capability_absent_observed": _capability_absent(observation),
        "correctness_included": True,
    })
    return {field: row.get(field, "") for field in RESULTS_FIELDS}


def _capability_rows(registrations: Sequence[AdapterRegistration]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for registration in registrations:
        for capability in registration.capabilities():
            rows.append({
                "adapter": registration.name, "adapter_version": registration.adapter_version,
                "backend_mode": registration.backend_mode, "capability": capability.capability,
                "supported": capability.supported, "detail": capability.detail or "",
            })
    return rows


def _capabilities_json(rows: Sequence[dict[str, Any]], registrations: Sequence[AdapterRegistration]) -> dict[str, Any]:
    """Build JSON from the exact normalized rows used for capabilities.csv."""

    by_adapter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_adapter[row["adapter"]].append(dict(row))
    return {
        "schema_version": SCHEMA_VERSION,
        "adapters": [
            {
                "adapter": registration.name,
                "adapter_version": registration.adapter_version,
                "backend_mode": registration.backend_mode,
                "capabilities": by_adapter[registration.name],
            }
            for registration in registrations
        ],
    }


def _summary(rows: Sequence[dict[str, Any]], registrations: Sequence[AdapterRegistration]) -> dict[str, Any]:
    included = [row for row in rows if row["correctness_included"] is True]
    # Keep this direct oracle summary unchanged; runner bookkeeping lives beside it.
    oracle = summarize(included, ("workload", "variant"))
    grouped_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    adapter_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    workload_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    adapter_exclusions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    workload_exclusions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    exclusions: dict[str, int] = defaultdict(int)
    for row in rows:
        group = f"{row['workload']}/{row['variant']}"
        grouped_counts[group]["attempted"] += 1
        adapter_counts[row["adapter"]]["attempted"] += 1
        workload_counts[row["workload"]]["attempted"] += 1
        if row["correctness_included"] is True:
            grouped_counts[group]["correctness_included"] += 1
            adapter_counts[row["adapter"]]["correctness_included"] += 1
            workload_counts[row["workload"]]["correctness_included"] += 1
            adapter_counts[row["adapter"]]["successful"] += 1
            workload_counts[row["workload"]]["successful"] += 1
        else:
            grouped_counts[group]["excluded"] += 1
            adapter_counts[row["adapter"]]["excluded"] += 1
            workload_counts[row["workload"]]["excluded"] += 1
            exclusions[str(row["error_category"])] += 1
            adapter_exclusions[row["adapter"]][str(row["error_category"])] += 1
            workload_exclusions[row["workload"]][str(row["error_category"])] += 1
        if row["capability_absent_observed"] is True:
            grouped_counts[group]["capability_absent_observed"] += 1
            adapter_counts[row["adapter"]]["capability_absent_observed"] += 1
            workload_counts[row["workload"]]["capability_absent_observed"] += 1
    groups: dict[str, dict[str, Any]] = {}
    for group in sorted(grouped_counts):
        stats = dict(oracle["groups"].get(group, {}))
        workload, variant = group.split("/", 1)
        stats.setdefault("workload", workload)
        stats.setdefault("variant", variant)
        stats.update({
            key: grouped_counts[group].get(key, 0)
            for key in ("attempted", "correctness_included", "excluded", "capability_absent_observed")
        })
        groups[group] = stats
    counts = {
        "attempted": len(rows), "successful": len(included), "correctness_included": len(included), "excluded": len(rows) - len(included),
        "capability_absent_observed": sum(row["capability_absent_observed"] is True for row in rows),
    }
    assert counts["attempted"] == counts["correctness_included"] + counts["excluded"]
    return {
        "schema_version": SCHEMA_VERSION, "group_keys": oracle["group_keys"], "groups": groups, "oracle": oracle,
        "count_semantics": {"successful": "run_status=success; capability-absent observations remain successful and included"},
        "adapter_counts": {
            registration.name: {
                key: adapter_counts[registration.name].get(key, 0)
                for key in ("attempted", "successful", "correctness_included", "excluded", "capability_absent_observed")
            } | {"exclusions_by_category": dict(sorted(adapter_exclusions[registration.name].items()))}
            for registration in registrations
        },
        "workload_counts": {
            workload: {
                key: workload_counts[workload].get(key, 0)
                for key in ("attempted", "successful", "correctness_included", "excluded", "capability_absent_observed")
            } | {"exclusions_by_category": dict(sorted(workload_exclusions[workload].items()))}
            for workload in sorted(workload_counts)
        },
        "counts": counts, "exclusions_by_category": dict(sorted(exclusions.items())),
    }


def _write_csv(path: Path, fields: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                field: ("true" if value else "false") if isinstance(value := row.get(field, ""), bool) else value
                for field in fields
            })


def run_external_experiment(
    instances: Sequence[dict[str, Any]],
    out_dir: Path,
    *,
    requested_adapters: Sequence[str] | None = None,
    registry: Sequence[AdapterRegistration] | None = None,
    context: RunContext | None = None,
    input_bytes: bytes | None = None,
    input_path: Path | None = None,
    argv: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Replay each selected adapter/instance exactly once and publish artifacts."""

    canonical_instances = tuple(copy.deepcopy(instance) for instance in instances)
    _validate_instances(canonical_instances)  # Before creating any artifact directory.
    started_at_utc = _utc_now()
    run_started_ns = perf_counter_ns()
    context = context or RunContext()
    _validate_run_id(context.run_id)
    if registry is None:
        requested = tuple(requested_adapters or REGISTRY_ORDER)
        unknown = sorted(set(requested) - set(REGISTRY_ORDER))
        if unknown:
            raise ValueError(f"unknown adapters: {unknown}")
        registrations = _default_registry(context, set(requested), out_dir)
    else:
        by_name = {registration.name: registration for registration in registry}
        if len(by_name) != len(registry):
            raise ValueError("registry adapter names must be unique")
        requested = tuple(requested_adapters or tuple(by_name))
        unknown = sorted(set(requested) - set(by_name))
        if unknown:
            raise ValueError(f"unknown adapters: {unknown}")
        registrations = tuple(sorted((by_name[name] for name in requested), key=lambda item: (REGISTRY_ORDER.index(item.name) if item.name in REGISTRY_ORDER else len(REGISTRY_ORDER), item.name)))
    for registration in registrations:
        if not registration.adapter_version:
            raise ValueError(f"adapter_version must be nonempty: {registration.name}")

    out_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    pairs: set[tuple[str, str]] = set()
    for registration in registrations:
        for instance in canonical_instances:
            pair = (registration.name, instance["instance_id"])
            if pair in pairs:
                raise AssertionError(f"duplicate attempt pair: {pair}")
            pairs.add(pair)
            started = perf_counter_ns()
            attempt = copy.deepcopy(instance)
            try:
                replay_error: Exception | None = None
                observation: Any | None = None
                try:
                    if registration.backend_mode == "in_memory_fallback" and _has_crash(attempt):
                        raise UnsupportedMappingError("in-memory fallback cannot score crash recovery")
                    observation = registration.factory(context).run(attempt)
                except Exception as exc:
                    replay_error = exc
                if attempt != instance:
                    raise RuntimeAdapterError("adapter mutated its replay input")
                if replay_error is not None:
                    raise replay_error
                assert observation is not None
                latency_ms = (perf_counter_ns() - started) / 1_000_000
                rows.append(_success_row(instance, registration, observation, latency_ms))
            except UnsupportedMappingError as exc:
                latency_ms = (perf_counter_ns() - started) / 1_000_000
                rows.append(_excluded_row(instance, registration, "unsupported_mapping", latency_ms))
                errors.append(_error_record(instance, registration, "unsupported_mapping", latency_ms, exc))
            except Exception as exc:
                latency_ms = (perf_counter_ns() - started) / 1_000_000
                rows.append(_excluded_row(instance, registration, "runtime_error", latency_ms))
                errors.append(_error_record(instance, registration, "runtime_error", latency_ms, exc))
    assert len(pairs) == len(registrations) * len(canonical_instances) == len(rows)

    capability_rows = _capability_rows(registrations)
    summary = _summary(rows, registrations)
    environment = _environment()
    _write_csv(out_dir / "results.csv", RESULTS_FIELDS, rows)
    _write_json(out_dir / "summary.json", summary)
    _write_csv(out_dir / "capabilities.csv", CAPABILITIES_FIELDS, capability_rows)
    _write_json(out_dir / "capabilities.json", _capabilities_json(capability_rows, registrations))
    with (out_dir / "errors.jsonl").open("w", encoding="utf-8", newline="") as handle:
        for error in errors:
            handle.write(json.dumps(error, ensure_ascii=False, sort_keys=True) + "\n")
    _write_json(out_dir / "environment.json", environment)

    artifacts = {name: _sha256(out_dir / name) for name in ("results.csv", "summary.json", "capabilities.csv", "capabilities.json", "environment.json", "errors.jsonl")}
    source = input_bytes if input_bytes is not None else b"".join(
        json.dumps(instance, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        for instance in canonical_instances
    )
    ended = _utc_now()
    manifest = {
        "schema_version": SCHEMA_VERSION, "run_id": context.run_id, "argv": list(argv or []),
        "started_at_utc": started_at_utc, "ended_at_utc": ended,
        "duration_ms": (perf_counter_ns() - run_started_ns) / 1_000_000,
        "input": {
            "path": str(input_path.resolve()) if input_path is not None else None,
            "sha256": hashlib.sha256(source).hexdigest(), "bytes": len(source), "count": len(canonical_instances),
        },
        "selected_adapters": [{"name": record.name, "adapter_version": record.adapter_version, "backend_mode": record.backend_mode} for record in registrations],
        "backend_state": context.backend_state, "counts": summary["counts"], "git": environment["git"],
        "environment": {
            "artifact": "environment.json", "python": environment["python"], "platform": environment["platform"],
            "package_versions": environment["package_versions"],
        }, "artifacts": artifacts,
    }
    _write_json(out_dir / "run_manifest.json", manifest)
    return {"counts": summary["counts"], "summary": summary, "manifest": manifest}


def _load_jsonl(path: Path) -> tuple[list[dict[str, Any]], bytes]:
    payload = path.read_bytes()
    instances: list[dict[str, Any]] = []
    for line_number, line in enumerate(payload.splitlines(), 1):
        if not line.strip():
            raise ValueError(f"blank JSONL line: {line_number}")
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}") from exc
        instances.append(item)
    _validate_instances(instances)
    return instances, payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="replay external memory baselines")
    run.add_argument("--instances", type=Path, required=True)
    run.add_argument("--out-dir", type=Path, required=True)
    run.add_argument("--adapters", nargs="+", choices=REGISTRY_ORDER, default=list(REGISTRY_ORDER))
    run.add_argument("--run-id")
    run.add_argument("--mem0-state-root", type=Path, help="base directory containing one unique Mem0 <run-id> state root")
    args = parser.parse_args(argv)
    if args.command == "run":
        instances, payload = _load_jsonl(args.instances)
        result = run_external_experiment(
            instances, args.out_dir, requested_adapters=args.adapters,
            context=RunContext(
                run_id=args.run_id or f"run-{uuid.uuid4().hex}", mem0_state_root=args.mem0_state_root,
            ),
            input_bytes=payload, input_path=args.instances,
            argv=["run", "--instances", str(args.instances), "--out-dir", str(args.out_dir), "--adapters", *args.adapters]
            + (["--run-id", args.run_id] if args.run_id else [])
            + (["--mem0-state-root", str(args.mem0_state_root)] if args.mem0_state_root else []),
        )
        print(f"wrote {result['counts']['attempted']} attempts to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
