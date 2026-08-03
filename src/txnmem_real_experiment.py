"""Native model-trace collection and independent differential evaluation."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from txnmem_event_contract import validate_events
from txnmem_real_agent import run_real_agent
from txnmem_realism import trace_evidence_summary
from txnmem_trace_pipeline import build_trace_instances, replay_trace_instances


class RealExperimentError(ValueError):
    """A real-model experiment configuration or output error."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


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
            events = run_report.get("events", [])
            if events:
                evaluation = evaluate_native_trace(events, task_id, seed=int(task_record.get("seed", index - 1)))
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
        "task_summaries": aggregate_tasks,
        "variants": variants,
        "trace_ground_truth_native": True,
        "production_latency_claim": False,
        "raw_trace_path": str(raw_path),
    }
    sanitized = sanitize_run_report(report)
    _write_json(out_dir / "results" / "native_model_summary.json", sanitized)
    return sanitized
