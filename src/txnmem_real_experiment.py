"""Native model-trace collection and independent differential evaluation."""

from __future__ import annotations

import copy
import json
import hashlib
import inspect
import random
from collections import Counter
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_event_contract import validate_events
from txnmem_failure_controller import FailureController, validate_failure_schedule
from txnmem_model_protocol import merge_usage_summaries
from txnmem_real_agent import run_real_agent
from txnmem_realism import trace_evidence_summary
from txnmem_statistics import aggregate_official_results
from txnmem_trace_pipeline import build_trace_instances, replay_trace_instances


class RealExperimentError(ValueError):
    """A real-model experiment configuration or output error."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def load_task_manifest(source: Mapping[str, Any] | Path) -> tuple[dict[str, Any], str]:
    """Validate a JSON task manifest and return its canonical SHA-256 digest."""

    if isinstance(source, Path):
        try:
            source = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RealExperimentError("invalid_manifest", "task manifest is not valid JSON") from exc
    if not isinstance(source, Mapping):
        raise RealExperimentError("invalid_manifest", "task manifest must be a mapping")
    version = source.get("manifest_version", 1)
    if version != 1:
        raise RealExperimentError("unsupported_manifest_version", "only manifest_version=1 is supported")
    tasks = source.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RealExperimentError("missing_tasks", "manifest.tasks must be a non-empty list")
    normalized_tasks: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        if not isinstance(task, Mapping):
            raise RealExperimentError("invalid_task", f"task {index} must be a mapping")
        task_id = task.get("task_id")
        prompt = task.get("prompt")
        if not isinstance(task_id, str) or not task_id.strip():
            raise RealExperimentError("missing_task_id", f"task {index} needs task_id")
        if task_id in seen_ids:
            raise RealExperimentError("duplicate_task_id", f"duplicate task_id: {task_id}")
        if not isinstance(prompt, str) or not prompt.strip():
            raise RealExperimentError("missing_prompt", f"task {task_id} needs prompt")
        schedule = task.get("failure_schedule", [])
        validate_failure_schedule(schedule)
        seen_ids.add(task_id)
        normalized_tasks.append(dict(task))
    normalized = {
        "manifest_version": 1,
        "dataset_name": str(source.get("dataset_name", "txnmem-real-model")),
        "tasks": normalized_tasks,
    }
    for field in (
        "benchmark",
        "domain",
        "seed",
        "split",
        "task_count",
        "source_task_count",
        "task_level_split",
        "public_split_identity",
        "source_sha256",
        "source_identity",
        "condition_fingerprint",
        "parent_manifest_hash",
        "shard_index",
        "shard_count",
    ):
        if field in source:
            normalized[field] = source[field]
    if "transaction_mode" in source:
        normalized["transaction_mode"] = source["transaction_mode"]
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    computed_digest = hashlib.sha256(encoded).hexdigest()
    provided_digest = source.get("manifest_hash")
    if provided_digest is not None:
        if provided_digest != computed_digest:
            raise RealExperimentError("manifest_hash_mismatch", "manifest_hash does not match canonical manifest")
        normalized["manifest_hash"] = provided_digest
    return normalized, computed_digest


def split_task_manifest(
    manifest: Mapping[str, Any], holdout_fraction: float = 0.2, seed: int = 0
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split complete task episodes deterministically by task_id."""

    if not 0.0 <= holdout_fraction < 1.0:
        raise ValueError("holdout_fraction must be in [0, 1)")
    tasks = list(manifest.get("tasks", []))
    groups = {str(task["task_id"]): task for task in tasks}
    keys = sorted(groups)
    random.Random(seed).shuffle(keys)
    count = int(round(len(keys) * holdout_fraction)) if keys else 0
    if holdout_fraction > 0 and keys:
        count = max(1, count)
    holdout_keys = set(keys[:count])
    train = [groups[key] for key in sorted(groups) if key not in holdout_keys]
    holdout = [groups[key] for key in sorted(groups) if key in holdout_keys]
    return train, holdout


def evaluate_task_contract(task: Mapping[str, Any], run_report: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic event-level acceptance criteria without an LLM judge."""

    acceptance = task.get("acceptance", {})
    if not isinstance(acceptance, Mapping):
        return {"success": False, "reasons": ["invalid_acceptance"]}
    events = [event for event in run_report.get("events", []) if isinstance(event, Mapping)]
    reasons: list[str] = []
    expected_status = acceptance.get("expected_status", "completed")
    if run_report.get("status") != expected_status:
        if expected_status == "completed":
            reasons.append("run_not_completed")
        else:
            reasons.append(f"unexpected_status:{run_report.get('status')}!=expected:{expected_status}")
    required_failure_code = acceptance.get("required_failure_code")
    if required_failure_code is not None and run_report.get("failure_code") != required_failure_code:
        reasons.append(
            f"unexpected_failure_code:{run_report.get('failure_code')}!=expected:{required_failure_code}"
        )
    kinds = {str(event.get("kind")) for event in events}
    for required_kind in acceptance.get("required_event_kinds", []):
        if required_kind not in kinds:
            reasons.append(f"missing_event_kind:{required_kind}")
    memory_ids: set[str] = set()
    for event in events:
        for field in ("memory_id", "old_memory_id", "new_memory_id", "output_id"):
            value = event.get(field)
            if isinstance(value, str):
                memory_ids.add(value)
    for required_id in acceptance.get("required_memory_ids", []):
        if required_id not in memory_ids:
            reasons.append(f"missing_memory_id:{required_id}")
    for required_edge in acceptance.get("required_provenance", []):
        if not isinstance(required_edge, Mapping):
            reasons.append("invalid_required_provenance")
            continue
        source_id = required_edge.get("source_id")
        derived_id = required_edge.get("derived_id")
        matched = False
        for event in events:
            output_id = event.get("memory_id") or event.get("output_id")
            source_ids = list(event.get("source_ids", []))
            if event.get("source_id"):
                source_ids.append(event.get("source_id"))
            if output_id == derived_id and source_id in source_ids:
                matched = True
                break
        if not matched:
            reasons.append(f"missing_provenance:{source_id}->{derived_id}")
    return {"success": not reasons, "reasons": reasons}


_RAW_KEYS = frozenset(
    {
        "events",
        "messages",
        "instance",
        "raw_trace",
        "transcript",
        "final_memories",
        "memory_snapshot",
        "prompt",
        "content",
        "value",
        "arguments",
        "data",
        "password",
        "api_key",
        "token",
        "secret",
        "endpoint",
        "backend_endpoint",
    }
)
_TRANSACTION_SUMMARY_KEYS = frozenset(
    {
        "txn_id",
        "state",
        "decision",
        "phases",
        "intent_count",
        "read_set_count",
        "state_digest",
    }
)


def _sanitize_value(value: Any, key: str | None = None) -> Any:
    if key in _RAW_KEYS:
        return None
    if isinstance(value, Mapping):
        cleaned = {}
        for child_key, child_value in value.items():
            if child_key in _RAW_KEYS:
                continue
            if key == "transaction" and child_key not in _TRANSACTION_SUMMARY_KEYS:
                continue
            sanitized = _sanitize_value(child_value, str(child_key))
            if sanitized is not None:
                cleaned[str(child_key)] = sanitized
        return cleaned
    if isinstance(value, list):
        return [item for item in (_sanitize_value(item) for item in value) if item is not None]
    if isinstance(value, tuple):
        return [_sanitize_value(item) for item in value]
    return value


def sanitize_run_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Remove raw content and payload-bearing fields from an aggregate report."""

    cleaned = _sanitize_value(report)
    if not isinstance(cleaned, dict):
        raise RealExperimentError("invalid_report", "run report must be a mapping")
    return cleaned


def _variant_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    variants = sorted({str(row.get("variant")) for row in rows})
    for variant in variants:
        records = [row for row in rows if str(row.get("variant")) == variant]
        summary[variant] = {
            "count": len(records),
            "oracle_matched": sum(int(row.get("oracle_match", 0)) for row in records),
            "violating": sum(int(row.get("any_violation", 0)) for row in records),
        }
    return summary


def evaluate_native_trace(
    events: list[Mapping[str, Any]], instance_id: str, seed: int = 0
) -> dict[str, Any]:
    """Convert and replay a native trace against the independent oracle."""

    validated = validate_events(events)
    instances = build_trace_instances(
        validated,
        "normalized",
        source="real-model-native",
        seed=seed,
    )
    if len(instances) != 1:
        raise RealExperimentError("invalid_trace_instance_count", "native trace must form exactly one instance")
    instance = instances[0]
    instance["instance_id"] = instance_id
    rows = replay_trace_instances([instance])
    evidence = trace_evidence_summary([instance], rows)
    evidence["trace_ground_truth_native"] = True
    return {
        "instance_id": instance_id,
        "events": validated,
        "instance": instance,
        "rows": rows,
        "variant_summary": _variant_summary(rows),
        "evidence": evidence,
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _make_task_adapter(adapter_factory: Any, task: Mapping[str, Any]) -> Any:
    """Create an adapter, optionally passing the current task metadata.

    Public benchmark adapters normally need no arguments.  AppWorld can
    reduce its official API schema to the apps present in a task's official
    DB snapshot, so the factory may opt into the task-aware form.
    """

    try:
        signature = inspect.signature(adapter_factory)
    except (TypeError, ValueError):
        return adapter_factory()
    parameters = list(signature.parameters.values())
    accepts_positional = any(
        parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        for parameter in parameters
    )
    accepts_varargs = any(parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in parameters)
    return adapter_factory(task) if accepts_positional or accepts_varargs else adapter_factory()


def _empty_manifest_report(raw_path: str, task_count: int = 0) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "completed_task_count": 0,
        "native_event_count": 0,
        "evaluation_error_count": 0,
        "task_summaries": [],
        "variants": {},
        "trace_ground_truth_native": True,
        "production_latency_claim": False,
        "raw_trace_path": raw_path,
    }


def run_benchmark_experiment_manifest(
    manifest: Mapping[str, Any],
    model: Any | None,
    adapter_factory: Any,
    out_dir: Path,
    backend_factory: Any | None = None,
) -> dict[str, Any]:
    """Run task manifest entries through a benchmark adapter and write
    raw-local plus aggregate outputs."""

    if model is None or not callable(getattr(model, "complete", None)):
        raise RealExperimentError("missing_model", "a configured model client is required")
    tasks = manifest.get("tasks") if isinstance(manifest, Mapping) else None
    if not isinstance(tasks, list) or not tasks:
        raise RealExperimentError("missing_tasks", "manifest.tasks must be a non-empty list")
    if not callable(adapter_factory):
        raise RealExperimentError("missing_adapter", "a benchmark adapter factory is required")

    from txnmem_benchmark_bridge import run_benchmark_agent

    raw_path = out_dir / "data" / "native_model_traces.jsonl"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    aggregate_tasks: list[dict[str, Any]] = []
    variant_totals: Counter[str] = Counter()
    variant_matches: Counter[str] = Counter()
    variant_violations: Counter[str] = Counter()
    native_event_count = 0
    evaluation_error_count = 0
    with raw_path.open("w", encoding="utf-8") as raw_handle:
        for index, task in enumerate(tasks, start=1):
            if not isinstance(task, Mapping):
                raise RealExperimentError("invalid_task", f"manifest task {index} must be a mapping")
            backend = (
                backend_factory(index, out_dir)
                if callable(backend_factory)
                else InstrumentedMemoryBackend()
            )
            task_record = dict(task)
            adapter = _make_task_adapter(adapter_factory, task_record)
            task_id = str(task_record.get("task_id") or f"native_task_{index:04d}")
            run_report = run_benchmark_agent(
                task_record,
                model,
                backend,
                adapter,
                max_steps=int(task_record.get("max_steps", 30)),
                seed=int(task_record.get("seed", index - 1)),
                temperature=float(task_record.get("temperature", 0.0)),
            )
            native_event_count += len(run_report.get("events", []))
            raw_handle.write(json.dumps({"task_id": task_id, "run": run_report}, ensure_ascii=False) + "\n")
            task_summary: dict[str, Any] = {
                "task_id": task_id,
                "status": run_report.get("status"),
                "steps": run_report.get("steps", 0),
                "official": run_report.get("official"),
                "native_event_count": len(run_report.get("events", [])),
                "model_usage": run_report.get("model_usage", {}),
                "prompt_profile": run_report.get(
                    "prompt_profile", task_record.get("prompt_profile", "baseline")
                ),
                "preflight_tool_count": int(run_report.get("preflight_tool_count", 0) or 0),
                "preflight_login_count": int(run_report.get("preflight_login_count", 0) or 0),
                "preflight_contact_lookup_count": int(
                    run_report.get("preflight_contact_lookup_count", 0) or 0
                ),
                "authorized_benchmark_tool_count": int(
                    run_report.get("authorized_benchmark_tool_count", 0) or 0
                ),
                "model_visible_benchmark_tool_names_sha256": str(
                    run_report.get("model_visible_benchmark_tool_names_sha256", "") or ""
                ),
                "model_visible_benchmark_tool_count": int(
                    run_report.get("model_visible_benchmark_tool_count", 0) or 0
                ),
                "trusted_preflight_enabled": bool(
                    run_report.get("trusted_preflight_enabled", False)
                ),
                "unauthorized_tool_attempt_count": int(
                    run_report.get("unauthorized_tool_attempt_count", 0) or 0
                ),
                "benchmark_tool_trace": run_report.get("benchmark_tool_trace", []),
            }
            if run_report.get("failure_code") is not None:
                task_summary["failure_code"] = run_report.get("failure_code")
            if isinstance(run_report.get("transaction"), Mapping):
                task_summary["transaction"] = dict(run_report["transaction"])
            task_summary["task_evaluator"] = evaluate_task_contract(task_record, run_report)
            events = run_report.get("events", [])
            if events:
                try:
                    evaluation = evaluate_native_trace(
                        events, task_id, seed=int(task_record.get("seed", index - 1))
                    )
                except (KeyError, RealExperimentError, ValueError) as exc:
                    evaluation_error_count += 1
                    task_summary["evaluation_status"] = "error"
                    task_summary["evaluation_error"] = {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                else:
                    task_summary["evidence"] = evaluation["evidence"]
                    task_summary["variant_summary"] = evaluation["variant_summary"]
                    for variant, values in evaluation["variant_summary"].items():
                        variant_totals[variant] += int(values["count"])
                        variant_matches[variant] += int(values["oracle_matched"])
                        variant_violations[variant] += int(values["violating"])
            else:
                task_summary["failure_code"] = run_report.get("failure_code", "no_events")
            aggregate_tasks.append(task_summary)
            close_adapter = getattr(adapter, "close", None)
            if callable(close_adapter):
                close_adapter()
            close = getattr(backend, "close", None)
            if callable(close):
                close()

    variants = {
        variant: {
            "count": variant_totals[variant],
            "oracle_matched": variant_matches[variant],
            "oracle_match_rate": variant_matches[variant] / variant_totals[variant]
            if variant_totals[variant]
            else 0.0,
            "violating": variant_violations[variant],
        }
        for variant in sorted(variant_totals)
    }
    model_usage = merge_usage_summaries(
        [task.get("model_usage", {}) for task in aggregate_tasks]
    )
    report = {
        "task_count": len(tasks),
        "completed_task_count": sum(task["status"] == "completed" for task in aggregate_tasks),
        "native_event_count": native_event_count,
        "evaluation_error_count": evaluation_error_count,
        "task_summaries": aggregate_tasks,
        "variants": variants,
        "prompt_profiles": sorted(
            {str(task.get("prompt_profile", "baseline")) for task in aggregate_tasks}
        ),
        "model_usage": model_usage,
        "token_usage_complete": bool(model_usage["request_count"])
        and model_usage["request_count"] == model_usage["responses_with_usage"],
        "trace_ground_truth_native": True,
        "production_latency_claim": False,
        "raw_trace_path": str(raw_path),
    }
    sanitized = sanitize_run_report(report)
    _write_json(out_dir / "results" / "native_model_summary.json", sanitized)
    return sanitized


def run_benchmark_batch(
    manifest: Mapping[str, Any],
    model: Any,
    out_dir: Path,
    backend_factory: Any | None = None,
    adapter_factory: Any | None = None,
    repetitions: int = 1,
) -> dict[str, Any]:
    """Run fixed benchmark tasks and aggregate official results by task.

    Each repetition gets an isolated output directory.  Raw model traces stay
    there; the returned report contains only sanitized task summaries and
    aggregate counters.  Official evaluator output is intentionally kept
    separate from TxnMem's independent oracle/contract summary.
    """

    if repetitions < 1:
        raise RealExperimentError("invalid_repetitions", "repetitions must be positive")
    if not isinstance(manifest, Mapping):
        raise RealExperimentError("invalid_manifest", "manifest must be a mapping")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RealExperimentError("missing_tasks", "manifest.tasks must be a non-empty list")
    if not callable(adapter_factory):
        raise RealExperimentError("missing_adapter", "adapter_factory is required")
    if model is None or not callable(getattr(model, "complete", None)):
        raise RealExperimentError("missing_model", "a configured model client is required")

    all_task_summaries: list[dict[str, Any]] = []
    reports: list[dict[str, Any]] = []
    for repetition in range(repetitions):
        repetition_dir = out_dir if repetitions == 1 else out_dir / f"rep_{repetition + 1:02d}"
        repetition_tasks: list[dict[str, Any]] = []
        for task_index, task in enumerate(tasks, start=1):
            item = dict(task)
            item["seed"] = int(item.get("seed", 0)) + repetition * 100
            failure_config = item.get(
                "failure_controller",
                item.get(
                    "failure_schedule",
                    manifest.get(
                        "failure_controller", manifest.get("failure_schedule")
                    ),
                ),
            )
            if failure_config is not None:
                schedule = (
                    failure_config.schedule
                    if isinstance(failure_config, FailureController)
                    else failure_config
                )
                item["failure_controller"] = FailureController(
                    copy.deepcopy(list(schedule))
                )
            transaction_mode = str(
                item.get(
                    "transaction_mode",
                    manifest.get("transaction_mode", "direct"),
                )
            )
            item["transaction_mode"] = transaction_mode
            if transaction_mode == "task":
                task_id = str(
                    item.get("task_id") or f"native_task_{task_index:04d}"
                )
                base_transaction_id = str(
                    item.get(
                        "transaction_id",
                        manifest.get("transaction_id", f"txn_{task_id}"),
                    )
                )
                identity_material = json.dumps(
                    [
                        base_transaction_id,
                        task_id,
                        int(task_index),
                        int(repetition + 1),
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                transaction_id = f"txn_{hashlib.sha256(identity_material).hexdigest()}"
                item["transaction_id"] = transaction_id
                item["transaction_journal_path"] = (
                    repetition_dir
                    / "journals"
                    / f"{transaction_id}.sqlite3"
                )
                for option in (
                    "policy_snapshot_provider",
                    "transaction_phase_hook",
                ):
                    if option not in item and option in manifest:
                        item[option] = manifest[option]
            repetition_tasks.append(item)
        repetition_manifest = {
            "manifest_version": int(manifest.get("manifest_version", 1)),
            "dataset_name": str(manifest.get("dataset_name", "benchmark")),
            "tasks": repetition_tasks,
        }
        if "transaction_mode" in manifest:
            repetition_manifest["transaction_mode"] = manifest[
                "transaction_mode"
            ]
        report = run_benchmark_experiment_manifest(
            repetition_manifest,
            model,
            adapter_factory,
            repetition_dir,
            backend_factory=backend_factory,
        )
        reports.append(report)
        for task_summary in report.get("task_summaries", []):
            if isinstance(task_summary, Mapping):
                all_task_summaries.append(dict(task_summary))

    official = aggregate_official_results(
        all_task_summaries, str(manifest.get("dataset_name", "benchmark"))
    )
    variant_totals: Counter[str] = Counter()
    variant_matches: Counter[str] = Counter()
    variant_violations: Counter[str] = Counter()
    for report in reports:
        for variant, values in report.get("variants", {}).items():
            if not isinstance(values, Mapping):
                continue
            variant_totals[str(variant)] += int(values.get("count", 0) or 0)
            variant_matches[str(variant)] += int(values.get("oracle_matched", 0) or 0)
            variant_violations[str(variant)] += int(values.get("violating", 0) or 0)
    variants = {
        variant: {
            "count": variant_totals[variant],
            "oracle_matched": variant_matches[variant],
            "oracle_match_rate": (
                variant_matches[variant] / variant_totals[variant]
                if variant_totals[variant]
                else 0.0
            ),
            "violating": variant_violations[variant],
        }
        for variant in sorted(variant_totals)
    }
    model_usage = merge_usage_summaries(
        [report.get("model_usage", {}) for report in reports]
    )
    result: dict[str, Any] = {
        "dataset": str(manifest.get("dataset_name", "benchmark")),
        "task_count": len(all_task_summaries),
        "unique_task_count": len(tasks),
        "repetitions": repetitions,
        "native_event_count": sum(int(report.get("native_event_count", 0) or 0) for report in reports),
        "evaluation_error_count": sum(
            int(report.get("evaluation_error_count", 0) or 0) for report in reports
        ),
        "task_summaries": all_task_summaries,
        "official": official,
        "variants": variants,
        "prompt_profiles": sorted(
            {
                str(task.get("prompt_profile", "baseline"))
                for task in all_task_summaries
            }
        ),
        "model_usage": model_usage,
        "token_usage_complete": bool(model_usage["request_count"])
        and model_usage["request_count"] == model_usage["responses_with_usage"],
        "trace_ground_truth_native": True,
        "raw_reports_location": "rep_*/results/native_model_summary.json" if repetitions > 1 else "results/native_model_summary.json",
        "raw_reports_committed": False,
        "production_latency_claim": False,
    }
    sanitized = sanitize_run_report(result)
    _write_json(out_dir / "results" / "native_batch_summary.json", sanitized)
    return sanitized


def run_experiment_manifest(
    manifest: Mapping[str, Any], model: Any | None, out_dir: Path
) -> dict[str, Any]:
    """Run task manifest entries and write raw-local plus aggregate outputs."""

    if model is None or not callable(getattr(model, "complete", None)):
        raise RealExperimentError("missing_model", "a configured model client is required")
    tasks = manifest.get("tasks") if isinstance(manifest, Mapping) else None
    if not isinstance(tasks, list) or not tasks:
        raise RealExperimentError("missing_tasks", "manifest.tasks must be a non-empty list")

    raw_path = out_dir / "data" / "native_model_traces.jsonl"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    aggregate_tasks: list[dict[str, Any]] = []
    variant_totals: Counter[str] = Counter()
    variant_matches: Counter[str] = Counter()
    variant_violations: Counter[str] = Counter()
    native_event_count = 0
    evaluation_error_count = 0
    with raw_path.open("w", encoding="utf-8") as raw_handle:
        for index, task in enumerate(tasks, start=1):
            if not isinstance(task, Mapping):
                raise RealExperimentError("invalid_task", f"manifest task {index} must be a mapping")
            task_record = dict(task)
            task_id = str(task_record.get("task_id") or f"native_task_{index:04d}")
            transaction_mode = str(
                task_record.get(
                    "transaction_mode",
                    manifest.get("transaction_mode", "direct"),
                )
            )
            backend = manifest.get("backend_factory", lambda: None)
            if callable(backend):
                backend = backend()
            if backend is None:
                if transaction_mode == "task":
                    from txnmem_task_transaction import InMemoryTransactionBackend

                    backend = InMemoryTransactionBackend()
                else:
                    from txnmem_backend import InstrumentedMemoryBackend

                    backend = InstrumentedMemoryBackend()
            transaction_options: dict[str, Any] = {
                "transaction_mode": transaction_mode,
            }
            if transaction_mode == "task":
                case_id = str(task_record.get("case_id") or task_id)
                transaction_options.update(
                    {
                        "transaction_journal_path": out_dir
                        / "journals"
                        / f"{case_id}.sqlite3",
                        "transaction_id": task_record.get("transaction_id")
                        or f"txn_{case_id}",
                    }
                )
                policy_provider = task_record.get(
                    "policy_snapshot_provider",
                    manifest.get("policy_snapshot_provider"),
                )
                if callable(policy_provider):
                    transaction_options["policy_snapshot_provider"] = policy_provider
                phase_hook = task_record.get(
                    "transaction_phase_hook",
                    manifest.get("transaction_phase_hook"),
                )
                if callable(phase_hook):
                    transaction_options["transaction_phase_hook"] = phase_hook
            run_report = run_real_agent(
                task_record,
                model,
                backend,
                max_steps=int(task_record.get("max_steps", 12)),
                seed=int(task_record.get("seed", index - 1)),
                temperature=float(task_record.get("temperature", 0.0)),
                **transaction_options,
            )
            raw_handle.write(json.dumps({"task_id": task_id, "run": run_report}, ensure_ascii=False) + "\n")
            native_event_count += len(run_report.get("events", []))
            task_summary: dict[str, Any] = {
                "task_id": task_id,
                "status": run_report.get("status"),
                "steps": run_report.get("steps", 0),
                "model_usage": run_report.get("model_usage", {}),
            }
            if run_report.get("failure_code") is not None:
                task_summary["failure_code"] = run_report.get("failure_code")
            if isinstance(run_report.get("transaction"), Mapping):
                task_summary["transaction"] = dict(run_report["transaction"])
            task_summary["task_evaluator"] = evaluate_task_contract(task_record, run_report)
            events = run_report.get("events", [])
            if events:
                try:
                    evaluation = evaluate_native_trace(
                        events, task_id, seed=int(task_record.get("seed", index - 1))
                    )
                except (KeyError, RealExperimentError, ValueError) as exc:
                    evaluation_error_count += 1
                    task_summary["evaluation_status"] = "error"
                    task_summary["evaluation_error"] = {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                else:
                    task_summary["evidence"] = evaluation["evidence"]
                    task_summary["variant_summary"] = evaluation["variant_summary"]
                    for variant, values in evaluation["variant_summary"].items():
                        variant_totals[variant] += int(values["count"])
                        variant_matches[variant] += int(values["oracle_matched"])
                        variant_violations[variant] += int(values["violating"])
            else:
                task_summary["failure_code"] = run_report.get("failure_code", "no_events")
            aggregate_tasks.append(task_summary)

    variants = {
        variant: {
            "count": variant_totals[variant],
            "oracle_matched": variant_matches[variant],
            "oracle_match_rate": variant_matches[variant] / variant_totals[variant]
            if variant_totals[variant]
            else 0.0,
            "violating": variant_violations[variant],
        }
        for variant in sorted(variant_totals)
    }
    model_usage = merge_usage_summaries(
        [task.get("model_usage", {}) for task in aggregate_tasks]
    )
    report = {
        "task_count": len(tasks),
        "completed_task_count": sum(task["status"] == "completed" for task in aggregate_tasks),
        "native_event_count": native_event_count,
        "evaluation_error_count": evaluation_error_count,
        "task_summaries": aggregate_tasks,
        "variants": variants,
        "model_usage": model_usage,
        "token_usage_complete": bool(model_usage["request_count"])
        and model_usage["request_count"] == model_usage["responses_with_usage"],
        "trace_ground_truth_native": True,
        "production_latency_claim": False,
        "raw_trace_path": str(raw_path),
    }
    sanitized = sanitize_run_report(report)
    _write_json(out_dir / "results" / "native_model_summary.json", sanitized)
    return sanitized
