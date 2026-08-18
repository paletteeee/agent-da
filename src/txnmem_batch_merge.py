"""Fail-closed merging for native public-benchmark shard reports."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from txnmem_benchmark_manifests import _canonical_hash, shard_manifest


def _require_equal(report: Mapping[str, Any], field: str, expected: Any) -> None:
    if report.get(field) != expected:
        raise ValueError(f"{field} mismatch")


def _official_status(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "blocked"
    status = value.get("status")
    if status == "available":
        return "available"
    if status in {"error", "evaluator_error"}:
        return "error"
    if status in {"blocked", "unavailable"}:
        return "blocked"
    if value.get("error") is not None:
        return "error"
    return "blocked"


def merge_native_shards(
    manifest: Mapping[str, Any], shard_reports: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """Validate and merge shard summaries in parent source order.

    Execution failures and unavailable evaluators remain statistical units;
    only an explicit official boolean success contributes a success.
    """

    if not isinstance(manifest, Mapping):
        raise ValueError("manifest must be a mapping")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("manifest.tasks must be a non-empty list")
    parent_hash = manifest.get("manifest_hash")
    if not isinstance(parent_hash, str) or not parent_hash:
        raise ValueError("manifest is missing parent manifest_hash")
    if parent_hash != _canonical_hash(manifest):
        raise ValueError("parent manifest_hash does not match manifest content")
    required_parent_fields = ("benchmark", "split", "source_identity", "condition_fingerprint")
    for field in required_parent_fields:
        if field not in manifest:
            raise ValueError(f"parent manifest is missing {field}")
    if not isinstance(manifest.get("benchmark"), str) or not manifest["benchmark"]:
        raise ValueError("parent manifest has malformed benchmark")
    if not isinstance(manifest.get("split"), str) or not manifest["split"]:
        raise ValueError("parent manifest has malformed split")
    if not isinstance(manifest.get("source_identity"), Mapping):
        raise ValueError("parent manifest has malformed source_identity")
    if (
        not isinstance(manifest.get("condition_fingerprint"), str)
        or not manifest["condition_fingerprint"]
    ):
        raise ValueError("parent manifest has malformed condition_fingerprint")
    if manifest.get("task_count", len(tasks)) != len(tasks):
        raise ValueError("parent manifest task_count mismatch")
    expected: dict[str, tuple[int, Mapping[str, Any]]] = {}
    for index, task in enumerate(tasks):
        if not isinstance(task, Mapping):
            raise ValueError(f"malformed manifest task at position {index}")
        task_id = task.get("task_id")
        position = task.get("source_position", index)
        if not isinstance(task_id, str) or not task_id:
            raise ValueError(f"malformed manifest task at position {index}")
        if task_id in expected:
            raise ValueError(f"duplicate manifest task ID: {task_id}")
        if position != index:
            raise ValueError(f"source position mismatch for task {task_id}")
        raw_task_id = task.get("raw_task_id")
        if (
            isinstance(raw_task_id, bool)
            or not isinstance(raw_task_id, (str, int))
            or (isinstance(raw_task_id, str) and not raw_task_id)
        ):
            raise ValueError(f"malformed raw task ID for task {task_id}")
        expected[task_id] = (index, task)

    reports = list(shard_reports)
    if not reports:
        raise ValueError("missing shard reports")
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    shard_indexes: set[int] = set()
    declared_shard_count: int | None = None
    declared_execution_condition: str | None = None
    for report_index, report in enumerate(reports):
        if not isinstance(report, Mapping):
            raise ValueError(f"malformed shard report {report_index}")
        _require_equal(report, "parent_manifest_hash", parent_hash)
        _require_equal(report, "benchmark", manifest.get("benchmark"))
        _require_equal(report, "split", manifest.get("split"))
        _require_equal(report, "source_identity", manifest.get("source_identity"))
        _require_equal(
            report,
            "condition_fingerprint",
            manifest.get("condition_fingerprint"),
        )
        execution_condition = report.get("execution_condition_fingerprint")
        if not isinstance(execution_condition, str) or not execution_condition:
            raise ValueError("malformed execution condition fingerprint")
        if declared_execution_condition is None:
            declared_execution_condition = execution_condition
        elif execution_condition != declared_execution_condition:
            raise ValueError("execution condition mismatch")
        if "domain" in manifest:
            _require_equal(report, "domain", manifest.get("domain"))
        shard_count = report.get("shard_count")
        shard_index = report.get("shard_index")
        if (
            isinstance(shard_count, bool)
            or not isinstance(shard_count, int)
            or shard_count < 1
            or shard_count > len(tasks)
            or isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or not 0 <= shard_index < shard_count
        ):
            raise ValueError(f"malformed shard_count/index metadata in report {report_index}")
        if declared_shard_count is None:
            declared_shard_count = shard_count
        elif shard_count != declared_shard_count:
            raise ValueError("shard_count mismatch")
        if shard_index in shard_indexes:
            raise ValueError(f"duplicate shard index: {shard_index}")
        shard_indexes.add(shard_index)
        expected_shard_hash = shard_manifest(manifest, shard_count)[shard_index][
            "manifest_hash"
        ]
        if report.get("execution_manifest_hash") != expected_shard_hash:
            raise ValueError(f"execution manifest mismatch for shard {shard_index}")
        repetitions = report.get("repetitions")
        if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions < 1:
            raise ValueError("malformed repetitions")
        if report_index == 0:
            declared_repetitions = repetitions
        elif repetitions != declared_repetitions:
            raise ValueError("conflicting repetitions")
        rows = report.get("task_summaries")
        if not isinstance(rows, list):
            raise ValueError(f"malformed task_summaries in shard {shard_index}")
        for row_index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise ValueError(f"malformed row {row_index} in shard {shard_index}")
            task_id = row.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                raise ValueError(f"malformed task ID in shard {shard_index}")
            if task_id not in expected:
                raise ValueError(f"extra task: {task_id}")
            position = expected[task_id][0]
            if row.get("source_position") != position:
                raise ValueError(f"source position mismatch for task {task_id}")
            status = row.get("status")
            if not isinstance(status, str) or not status:
                raise ValueError(f"malformed status for task {task_id}")
            official = row.get("official")
            if official is not None and not isinstance(official, Mapping):
                raise ValueError(f"malformed official result for task {task_id}")
            repetition = row.get("repetition", 1)
            if (
                isinstance(repetition, bool)
                or not isinstance(repetition, int)
                or not 1 <= repetition <= repetitions
                or (repetitions > 1 and "repetition" not in row)
            ):
                raise ValueError(f"malformed repetition for task {task_id}")
            key = (task_id, repetition)
            if key in rows_by_key:
                raise ValueError(f"duplicate task repetition: {task_id}/{repetition}")
            if position % shard_count != shard_index:
                raise ValueError(f"shard assignment mismatch for task {task_id}")
            item = dict(row)
            raw_task_id = expected[task_id][1]["raw_task_id"]
            if "raw_task_id" in item and item["raw_task_id"] != raw_task_id:
                raise ValueError(f"raw task ID mismatch for task {task_id}")
            item["raw_task_id"] = raw_task_id
            item["repetition"] = repetition
            rows_by_key[key] = item
    if declared_shard_count is None or shard_indexes != set(range(declared_shard_count)):
        raise ValueError("missing shard reports")
    expected_keys = {
        (task_id, repetition)
        for task_id in expected
        for repetition in range(1, declared_repetitions + 1)
    }
    missing = sorted(expected_keys - set(rows_by_key), key=lambda item: (expected[item[0]][0], item[1]))
    if missing:
        raise ValueError(f"missing task repetition: {missing[0][0]}/{missing[0][1]}")

    ordered_rows = [
        rows_by_key[(str(task["task_id"]), repetition)]
        for task in tasks
        for repetition in range(1, declared_repetitions + 1)
    ]
    per_task_status: list[str] = []
    per_task_evaluator_status: list[str] = []
    successful_tasks = 0
    for task in tasks:
        task_id = str(task["task_id"])
        task_rows = [
            rows_by_key[(task_id, repetition)]
            for repetition in range(1, declared_repetitions + 1)
        ]
        statuses = {str(row["status"]) for row in task_rows}
        per_task_status.append(next(iter(statuses)) if len(statuses) == 1 else "mixed")
        evaluator_statuses = {_official_status(row.get("official")) for row in task_rows}
        if "error" in evaluator_statuses:
            per_task_evaluator_status.append("error")
        elif "blocked" in evaluator_statuses:
            per_task_evaluator_status.append("blocked")
        else:
            per_task_evaluator_status.append("available")
        if all(
            row["status"] == "completed"
            and isinstance(row.get("official"), Mapping)
            and _official_status(row["official"]) == "available"
            and row["official"].get("success") is True
            for row in task_rows
        ):
            successful_tasks += 1
    status_counts = Counter(per_task_status)
    evaluator_status_counts = Counter(per_task_evaluator_status)
    denominator = len(tasks)
    return {
        "schema_version": 1,
        "benchmark": manifest.get("benchmark"),
        "domain": manifest.get("domain"),
        "split": manifest.get("split"),
        "parent_manifest_hash": parent_hash,
        "condition_fingerprint": manifest.get("condition_fingerprint"),
        "execution_condition_fingerprint": declared_execution_condition,
        "source_identity": manifest.get("source_identity"),
        "shard_count": declared_shard_count,
        "repetitions": declared_repetitions,
        "task_count": denominator,
        "row_count": len(ordered_rows),
        "task_summaries": ordered_rows,
        "task_aggregate": {
            "denominator": denominator,
            "successes": successful_tasks,
            "failures": denominator - successful_tasks,
            "success_rate": successful_tasks / denominator,
            "status_counts": dict(sorted(status_counts.items())),
        },
        "official": {
            "trials": denominator,
            "successes": successful_tasks,
            "failures": denominator - successful_tasks,
            "success_rate": successful_tasks / denominator,
            "evaluator_status_counts": dict(sorted(evaluator_status_counts.items())),
        },
    }
