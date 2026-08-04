"""Native model-trace collection and independent differential evaluation."""

from __future__ import annotations

import json
import hashlib
import random
from collections import Counter
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from txnmem_event_contract import validate_events
from txnmem_failure_controller import validate_failure_schedule
from txnmem_real_agent import run_real_agent
from txnmem_realism import trace_evidence_summary
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
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return normalized, hashlib.sha256(encoded).hexdigest()


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
    if run_report.get("status") != "completed":
        reasons.append("run_not_completed")
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
            backend = manifest.get("backend_factory", lambda: None)
            if callable(backend):
                backend = backend()
            if backend is None:
                from txnmem_backend import InstrumentedMemoryBackend

                backend = InstrumentedMemoryBackend()
            task_record = dict(task)
            task_id = str(task_record.get("task_id") or f"native_task_{index:04d}")
            run_report = run_real_agent(
                task_record,
                model,
                backend,
                max_steps=int(task_record.get("max_steps", 12)),
                seed=int(task_record.get("seed", index - 1)),
                temperature=float(task_record.get("temperature", 0.0)),
            )
            raw_handle.write(json.dumps({"task_id": task_id, "run": run_report}, ensure_ascii=False) + "\n")
            native_event_count += len(run_report.get("events", []))
            task_summary: dict[str, Any] = {
                "task_id": task_id,
                "status": run_report.get("status"),
                "steps": run_report.get("steps", 0),
            }
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
    report = {
        "task_count": len(tasks),
        "completed_task_count": sum(task["status"] == "completed" for task in aggregate_tasks),
        "native_event_count": native_event_count,
        "evaluation_error_count": evaluation_error_count,
        "task_summaries": aggregate_tasks,
        "variants": variants,
        "trace_ground_truth_native": True,
        "production_latency_claim": False,
        "raw_trace_path": str(raw_path),
    }
    sanitized = sanitize_run_report(report)
    _write_json(out_dir / "results" / "native_model_summary.json", sanitized)
    return sanitized
