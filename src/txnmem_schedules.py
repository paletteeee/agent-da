"""Causal failure schedules and schedule coverage helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _matches_trigger(event: dict[str, Any], operation: dict[str, Any], phase: str) -> bool:
    trigger = event.get("trigger")
    if isinstance(trigger, dict):
        if phase == "before":
            return trigger.get("before_operation") == operation.get("op_id")
        return trigger.get("after_operation") == operation.get("op_id")
    if event.get("step") != operation.get("step"):
        return False
    if phase == "after":
        return event.get("phase") in {"after_operation", "after_linearize", "after_commit"}
    return event.get("phase") not in {"after_operation", "after_linearize", "after_commit"}


def events_for_operation(
    instance: dict[str, Any], operation: dict[str, Any], phase: str = "before"
) -> list[dict[str, Any]]:
    """Return schedule events whose causal trigger fires at an operation boundary."""

    if phase not in {"before", "after"}:
        raise ValueError("phase must be before or after")
    return [
        event
        for event in instance.get("failure_schedule", [])
        if _matches_trigger(event, operation, phase)
    ]


def schedule_coverage(instance: dict[str, Any]) -> dict[str, Any]:
    """Summarize action, trigger, phase, and target coverage for one instance."""

    events = instance.get("failure_schedule", [])
    actions = Counter(str(event.get("type") or event.get("action")) for event in events)
    trigger_kinds = Counter()
    phases = Counter()
    targets = Counter()
    for event in events:
        trigger = event.get("trigger")
        if isinstance(trigger, dict):
            trigger_kinds.update(trigger.keys())
        elif "step" in event:
            trigger_kinds["legacy_step"] += 1
        phases[str(event.get("phase", "unspecified"))] += 1
        targets[str(event.get("target", "unspecified"))] += 1
    return {
        "event_count": len(events),
        "actions": dict(sorted(actions.items())),
        "trigger_kinds": dict(sorted(trigger_kinds.items())),
        "phases": dict(sorted(phases.items())),
        "targets": dict(sorted(targets.items())),
    }


def normalize_legacy_schedule(
    instance: dict[str, Any], *, default_phase: str = "before_validate"
) -> list[dict[str, Any]]:
    """Convert old step schedules to explicit before-operation triggers."""

    operations = {int(operation["step"]): operation for operation in instance.get("operations", [])}
    normalized: list[dict[str, Any]] = []
    for event in instance.get("failure_schedule", []):
        if "trigger" in event:
            normalized.append(dict(event))
            continue
        operation = operations.get(int(event.get("step", -1)))
        if operation is None:
            raise ValueError(f"schedule event has no matching operation step: {event}")
        item = dict(event)
        item.pop("step", None)
        item["trigger"] = {"before_operation": operation["op_id"]}
        item.setdefault("phase", default_phase)
        normalized.append(item)
    return normalized
