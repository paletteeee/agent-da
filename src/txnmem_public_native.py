"""Public workflow to native-agent boundary.

This module deliberately separates executable native-agent runs from the
existing projection adapters.  A missing benchmark runtime is a blocked
experiment, never an implicit fallback to a projected trace.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping
from typing import Any


_RAW_KEYS = frozenset(
    {"raw_context", "context", "prompt", "content", "value", "arguments", "data", "response", "body"}
)


def _load_records(source: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"public source is not readable JSON: {source}") from exc
    if not isinstance(payload, list):
        raise ValueError("public source must contain a JSON list")
    return [record for record in payload if isinstance(record, dict)]


def _sha256(source: Path) -> str:
    return hashlib.sha256(source.read_bytes()).hexdigest()


def _strip_raw(value: Any, key: str | None = None) -> Any:
    if key in _RAW_KEYS:
        return None
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for child_key, child_value in value.items():
            cleaned = _strip_raw(child_value, str(child_key))
            if cleaned is not None:
                result[str(child_key)] = cleaned
        return result
    if isinstance(value, list):
        return [_strip_raw(item) for item in value]
    return value


@dataclass(frozen=True)
class PublicWorkflowTask:
    dataset: str
    episode_id: str
    context: str
    prompt: str
    metadata: dict[str, Any]

    def to_manifest_task(self, index: int) -> dict[str, Any]:
        return {
            "task_id": self.episode_id,
            "agent_id": f"{self.dataset}_agent",
            "prompt": self.prompt,
            "system_prompt": (
                "You are running a public workflow episode. Use the memory tools when a fact "
                "must be retained or derived. Do not invent provenance edges; source_ids must "
                "come from actual memory reads or writes."
            ),
            "seed": index,
            "temperature": 0.0,
            "max_steps": 12,
            "metadata": dict(self.metadata),
            "acceptance": {"expected_status": "completed"},
        }


class PublicWorkflowAdapter:
    dataset = "public"
    execution_mode = "native_contextual_agent_run"
    required_module: str | None = None

    def check_environment(self, source: Path) -> dict[str, Any]:
        source_exists = source.is_file()
        module_available = (
            importlib.util.find_spec(self.required_module) is not None
            if self.required_module
            else False
        )
        checks = {
            "source_exists": source_exists,
            "required_module": self.required_module,
            "module_available": module_available,
            "execution_mode": self.execution_mode,
        }
        checks["available"] = source_exists and module_available
        if not source_exists:
            checks["reason"] = "missing_public_source"
        elif not module_available:
            checks["reason"] = "missing_executable_benchmark_runtime"
        return checks

    def load_tasks(self, source: Path, limit: int | None = None) -> list[PublicWorkflowTask]:
        records = _load_records(source)
        tasks = self._records_to_tasks(records)
        return tasks[:limit] if limit is not None else tasks

    def _records_to_tasks(self, records: list[dict[str, Any]]) -> list[PublicWorkflowTask]:
        raise NotImplementedError


class TauBenchPublicAdapter(PublicWorkflowAdapter):
    dataset = "tau-bench"
    required_module = "tau_bench"
    execution_mode = "native_workflow_agent_run"

    def _records_to_tasks(self, records: list[dict[str, Any]]) -> list[PublicWorkflowTask]:
        tasks: list[PublicWorkflowTask] = []
        for index, record in enumerate(records):
            task_id = record.get("task_id", index)
            info = record.get("info") if isinstance(record.get("info"), Mapping) else {}
            task = info.get("task") if isinstance(info.get("task"), Mapping) else {}
            instruction = task.get("instruction") or record.get("instruction")
            if not isinstance(instruction, str) or not instruction.strip():
                continue
            episode_id = f"{self.dataset}:{task_id}"
            tasks.append(
                PublicWorkflowTask(
                    dataset=self.dataset,
                    episode_id=episode_id,
                    context=instruction,
                    prompt=(
                        "Execute this τ-bench episode using the available workflow tools. "
                        "Record durable facts through memory tools when appropriate.\n\n"
                        f"Episode context:\n{instruction}"
                    ),
                    metadata={
                        "dataset": self.dataset,
                        "execution_mode": self.execution_mode,
                        "source_index": index,
                    },
                )
            )
        return tasks


class AppWorldPublicAdapter(PublicWorkflowAdapter):
    dataset = "appworld"
    required_module = "appworld"
    execution_mode = "native_workflow_agent_run"

    def _records_to_tasks(self, records: list[dict[str, Any]]) -> list[PublicWorkflowTask]:
        tasks: list[PublicWorkflowTask] = []
        for index, record in enumerate(records):
            task_id = record.get("task_id", index)
            instruction = record.get("instruction") or record.get("task")
            if isinstance(instruction, Mapping):
                instruction = instruction.get("instruction") or instruction.get("goal")
            if not isinstance(instruction, str) or not instruction.strip():
                continue
            episode_id = f"{self.dataset}:{task_id}"
            tasks.append(
                PublicWorkflowTask(
                    dataset=self.dataset,
                    episode_id=episode_id,
                    context=instruction,
                    prompt=(
                        "Execute this AppWorld workflow and use memory tools for durable facts.\n\n"
                        f"Workflow context:\n{instruction}"
                    ),
                    metadata={
                        "dataset": self.dataset,
                        "execution_mode": self.execution_mode,
                        "source_index": index,
                    },
                )
            )
        return tasks


class LoCoMoPublicAdapter(PublicWorkflowAdapter):
    dataset = "locomo"
    required_module = None
    execution_mode = "native_contextual_agent_run"

    def check_environment(self, source: Path) -> dict[str, Any]:
        checks = super().check_environment(source)
        checks["available"] = False
        checks["reason"] = "no_executable_agent_environment"
        checks["execution_mode"] = self.execution_mode
        return checks

    @staticmethod
    def _conversation_context(conversation: Mapping[str, Any]) -> str:
        lines: list[str] = []
        for key, turns in sorted(conversation.items()):
            if str(key).endswith("_date_time") or not str(key).startswith("session_"):
                continue
            if not isinstance(turns, list):
                continue
            for turn in turns:
                if not isinstance(turn, Mapping):
                    continue
                speaker = turn.get("speaker") or turn.get("role") or "speaker"
                text = turn.get("text") or turn.get("content")
                if text:
                    lines.append(f"{speaker}: {text}")
        return "\n".join(lines)

    def _records_to_tasks(self, records: list[dict[str, Any]]) -> list[PublicWorkflowTask]:
        tasks: list[PublicWorkflowTask] = []
        for index, record in enumerate(records):
            sample_id = record.get("sample_id", index)
            conversation = record.get("conversation")
            if not isinstance(conversation, Mapping):
                continue
            context = self._conversation_context(conversation)
            if not context:
                continue
            episode_id = f"{self.dataset}:{sample_id}"
            tasks.append(
                PublicWorkflowTask(
                    dataset=self.dataset,
                    episode_id=episode_id,
                    context=context,
                    prompt=(
                        "Review this conversation as a contextual memory episode. Use memory tools "
                        "for facts that should persist, and preserve provenance from actual reads.\n\n"
                        f"Conversation context:\n{context}"
                    ),
                    metadata={
                        "dataset": self.dataset,
                        "execution_mode": self.execution_mode,
                        "source_index": index,
                    },
                )
            )
        return tasks


_ADAPTERS = {
    "tau-bench": TauBenchPublicAdapter,
    "tau_bench": TauBenchPublicAdapter,
    "appworld": AppWorldPublicAdapter,
    "locomo": LoCoMoPublicAdapter,
}


def get_public_adapter(dataset: str) -> PublicWorkflowAdapter:
    try:
        return _ADAPTERS[dataset.lower()]()
    except KeyError as exc:
        raise ValueError(f"unsupported public workflow dataset: {dataset}") from exc


def load_public_tasks(dataset: str, source: Path, limit: int | None = None) -> list[PublicWorkflowTask]:
    return get_public_adapter(dataset).load_tasks(source, limit=limit)


def write_blocked_report(
    out_dir: Path,
    *,
    dataset: str,
    reason: str,
    checks: Mapping[str, Any],
) -> Path:
    report = {
        "status": "blocked",
        "dataset": dataset,
        "reason": reason,
        "checks": _strip_raw(dict(checks)),
        "native_ground_truth": False,
        "projection_fallback": False,
    }
    path = out_dir / "results" / "blocked_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def run_public_native_manifest(
    manifest: Mapping[str, Any], model: Any | None, out_dir: Path
) -> dict[str, Any]:
    dataset = str(manifest.get("dataset", ""))
    source = Path(str(manifest.get("source", "")))
    adapter = get_public_adapter(dataset)
    checks = adapter.check_environment(source)
    if not checks.get("available"):
        path = write_blocked_report(
            out_dir,
            dataset=dataset,
            reason="blocked_external_dependency",
            checks=checks,
        )
        return json.loads(path.read_text(encoding="utf-8"))
    if model is None or not callable(getattr(model, "complete", None)):
        path = write_blocked_report(
            out_dir,
            dataset=dataset,
            reason="missing_model",
            checks={"source_exists": True, "available": True, "execution_mode": adapter.execution_mode},
        )
        return json.loads(path.read_text(encoding="utf-8"))
    tasks = adapter.load_tasks(source, limit=manifest.get("limit"))
    if not tasks:
        path = write_blocked_report(
            out_dir,
            dataset=dataset,
            reason="no_executable_tasks",
            checks={"source_exists": True, "available": True},
        )
        return json.loads(path.read_text(encoding="utf-8"))
    from txnmem_real_experiment import run_experiment_manifest, sanitize_run_report

    task_manifest = {
        "manifest_version": 1,
        "dataset_name": f"{dataset}-native",
        "tasks": [task.to_manifest_task(index) for index, task in enumerate(tasks, start=1)],
    }
    report = run_experiment_manifest(task_manifest, model, out_dir)
    report.update(
        {
            "status": "completed",
            "dataset": dataset,
            "execution_mode": adapter.execution_mode,
            "source_sha256": _sha256(source),
            "native_ground_truth": True,
            "projection_fallback": False,
        }
    )
    report = sanitize_run_report(report)
    path = out_dir / "results" / "native_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
