"""Validation boundary for native Agent memory events."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any


SUPPORTED_KINDS = frozenset(
    {
        "memory_read",
        "memory_search",
        "memory_write",
        "memory_derive",
        "memory_propagate",
        "memory_supersede",
        "invalidate",
        "policy_change",
        "policy_revoke",
    }
)


class EventContractError(ValueError):
    """A canonical event contract violation with a stable machine code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _require_non_empty_string(event: Mapping[str, Any], field: str, code: str) -> str:
    value = event.get(field)
    if not isinstance(value, str) or not value.strip():
        raise EventContractError(code, f"{field} must be a non-empty string")
    return value.strip()


def _require_memory_id(event: Mapping[str, Any], field: str = "memory_id") -> str:
    return _require_non_empty_string(event, field, f"missing_{field}")


def _validate_provenance_fields(event: Mapping[str, Any]) -> None:
    kind = event["kind"]
    if kind in {"memory_derive", "memory_propagate"}:
        source_ids = event.get("source_ids")
        if source_ids is None and kind == "memory_propagate":
            source_ids = [event.get("source_id")]
        if not isinstance(source_ids, list) or not source_ids:
            raise EventContractError("missing_source_ids", f"{kind} requires source_ids")
        if any(not isinstance(source_id, str) or not source_id.strip() for source_id in source_ids):
            raise EventContractError("invalid_source_ids", "source_ids must contain non-empty strings")


def validate_event(
    event: Mapping[str, Any], seen_ids: set[str] | None = None
) -> dict[str, Any]:
    """Return a JSON-safe normalized event or raise a coded contract error."""

    if not isinstance(event, Mapping):
        raise EventContractError("invalid_event", "event must be a mapping")
    try:
        normalized = json.loads(json.dumps(dict(event), ensure_ascii=False))
    except (TypeError, ValueError) as exc:
        raise EventContractError("non_json_payload", "event must be JSON serializable") from exc

    event_id = _require_non_empty_string(normalized, "event_id", "missing_event_id")
    if seen_ids is not None and event_id in seen_ids:
        raise EventContractError("duplicate_event_id", f"duplicate event_id: {event_id}")
    kind = _require_non_empty_string(normalized, "kind", "missing_kind")
    if kind not in SUPPORTED_KINDS:
        raise EventContractError("unsupported_kind", f"unsupported event kind: {kind}")
    agent_id = _require_non_empty_string(normalized, "agent_id", "missing_agent_id")
    step = normalized.get("step")
    if isinstance(step, bool) or not isinstance(step, int) or step <= 0:
        raise EventContractError("invalid_step", "step must be a positive integer")

    normalized["event_id"] = event_id
    normalized["kind"] = kind
    normalized["agent_id"] = agent_id
    if kind in {"memory_write", "memory_derive", "memory_propagate"}:
        _require_memory_id(normalized)
    elif kind == "memory_supersede":
        _require_memory_id(normalized, "old_memory_id")
        _require_memory_id(normalized, "new_memory_id")
    elif kind == "invalidate":
        _require_memory_id(normalized)
    _validate_provenance_fields(normalized)
    return normalized


def validate_events(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Validate a recording, enforcing unique IDs and increasing source steps."""

    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    previous_step = 0
    for event in events:
        item = validate_event(event, seen_ids=seen_ids)
        if item["step"] <= previous_step:
            raise EventContractError("non_monotonic_step", "event steps must increase strictly")
        previous_step = item["step"]
        seen_ids.add(item["event_id"])
        normalized.append(item)
    return normalized
