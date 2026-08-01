"""Dataset-specific trace adapters for trace-grounded TxnMem replay.

The adapters deliberately implement a narrow boundary.  They only translate
events that are explicitly present in a source log; a conversation turn or an
unrecognised tool call is skipped rather than converted into a fabricated
memory operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


CANONICAL_KINDS = {
    "memory_read",
    "memory_search",
    "memory_write",
    "memory_derive",
    "memory_propagate",
    "memory_supersede",
    "policy_revoke",
    "policy_change",
    "crash",
    "delay",
    "invalidate",
}


@dataclass(frozen=True)
class AdaptationResult:
    dataset: str
    events: list[dict[str, Any]]
    skipped_events: int
    warnings: list[str]
    episode_id: str | None = None


def _first(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = record.get(name)
        if value is not None:
            return value
    return None


def _arguments(record: dict[str, Any]) -> dict[str, Any]:
    value = _first(record, "arguments", "args", "parameters", "input", "payload")
    return value if isinstance(value, dict) else {}


def _copy_memory_fields(event: dict[str, Any], record: dict[str, Any]) -> None:
    args = _arguments(record)
    for name in (
        "memory_id",
        "output_id",
        "source_id",
        "source_ids",
        "old_memory_id",
        "new_memory_id",
        "scope",
        "target_scope",
        "value",
        "query",
        "attribute",
        "agent_id",
        "txn_id",
        "transaction_id",
    ):
        value = _first(record, name)
        if value is None:
            value = args.get(name)
        if value is not None:
            event[name] = value
    if event.get("source_id") is not None and event.get("source_ids") is None:
        event["source_ids"] = [event["source_id"]]


def _canonical_event(record: dict[str, Any], kind: str, index: int) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": str(_first(record, "event_id", "id", "turn_id", "step") or f"event_{index:04d}"),
        "kind": kind,
    }
    for name in ("episode_id", "task_id", "conversation_id", "timestamp", "agent_id"):
        value = record.get(name)
        if value is not None:
            event[name] = value
    _copy_memory_fields(event, record)
    return event


def _canonical_kind(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower().replace("-", "_").replace(".", "_")
    aliases = {
        "read": "memory_read",
        "retrieve": "memory_read",
        "get_memory": "memory_read",
        "read_memory": "memory_read",
        "memory_get": "memory_read",
        "search": "memory_search",
        "search_memory": "memory_search",
        "write": "memory_write",
        "write_memory": "memory_write",
        "add_memory": "memory_write",
        "create_memory": "memory_write",
        "derive": "memory_derive",
        "derive_memory": "memory_derive",
        "propagate": "memory_propagate",
        "propagate_memory": "memory_propagate",
        "supersede": "memory_supersede",
        "supersede_memory": "memory_supersede",
        "memory_update": "memory_supersede",
        "update_memory": "memory_supersede",
        "revoke": "policy_revoke",
        "policy_revoke": "policy_revoke",
        "policy_change": "policy_change",
        "crash": "crash",
        "delay": "delay",
        "invalidate": "invalidate",
        "invalidate_memory": "invalidate",
    }
    if normalized in CANONICAL_KINDS:
        return normalized
    return aliases.get(normalized)


class BenchmarkAdapter:
    """Base adapter with conservative common-field handling."""

    dataset = "normalized"

    def classify(self, record: dict[str, Any]) -> str | None:
        return _canonical_kind(
            _first(record, "kind", "type", "event_type", "operation", "action", "api_name", "tool_name", "tool")
        )

    def adapt(self, records: Iterable[dict[str, Any]], episode_id: str | None = None) -> AdaptationResult:
        events: list[dict[str, Any]] = []
        skipped = 0
        warnings: list[str] = []
        for index, record in enumerate(records, start=1):
            if not isinstance(record, dict):
                skipped += 1
                warnings.append(f"record {index} is not an object")
                continue
            kind = self.classify(record)
            if kind is None:
                skipped += 1
                continue
            event = _canonical_event(record, kind, index)
            if episode_id is not None:
                event["episode_id"] = episode_id
            if kind == "memory_supersede":
                old_id = event.get("old_memory_id")
                new_id = event.get("new_memory_id") or event.get("memory_id")
                if old_id is None or new_id is None:
                    skipped += 1
                    warnings.append(f"record {index} supersession has no old/new memory id")
                    continue
                event["old_memory_id"] = old_id
                event["new_memory_id"] = new_id
            events.append(event)
        return AdaptationResult(self.dataset, events, skipped, warnings, episode_id)


class TauBenchAdapter(BenchmarkAdapter):
    dataset = "tau-bench"

    def classify(self, record: dict[str, Any]) -> str | None:
        kind = super().classify(record)
        if kind:
            return kind
        # Policy guidelines and explicit policy events are retained as a
        # schedule signal; ordinary user/assistant messages are not memory ops.
        if any(key in record for key in ("policy_guideline", "policy_guidelines", "policy_event")):
            return "policy_revoke" if "deny" in str(record).lower() else "policy_change"
        return None


class AppWorldAdapter(BenchmarkAdapter):
    dataset = "appworld"

    def classify(self, record: dict[str, Any]) -> str | None:
        kind = super().classify(record)
        if kind:
            return kind
        api = _first(record, "api", "api_call", "function_name", "name")
        return _canonical_kind(api)


class LoCoMoAdapter(BenchmarkAdapter):
    dataset = "locomo"

    def classify(self, record: dict[str, Any]) -> str | None:
        kind = super().classify(record)
        if kind:
            return kind
        if "facts" in record or "memory_events" in record:
            return "memory_write"
        return None

    def adapt(self, records: Iterable[dict[str, Any]], episode_id: str | None = None) -> AdaptationResult:
        expanded: list[dict[str, Any]] = []
        for record in records:
            if isinstance(record, dict) and isinstance(record.get("memory_events"), list):
                expanded.extend(item for item in record["memory_events"] if isinstance(item, dict))
            elif isinstance(record, dict) and isinstance(record.get("facts"), list):
                for fact_index, fact in enumerate(record["facts"], start=1):
                    if isinstance(fact, dict):
                        expanded.append({**record, **fact, "id": f"{record.get('id', 'fact')}_{fact_index}"})
                    else:
                        expanded.append({**record, "id": f"{record.get('id', 'fact')}_{fact_index}", "value": fact})
            else:
                expanded.append(record)
        return super().adapt(expanded, episode_id=episode_id)


ADAPTERS = {
    "normalized": BenchmarkAdapter,
    "tau-bench": TauBenchAdapter,
    "tau_bench": TauBenchAdapter,
    "appworld": AppWorldAdapter,
    "locomo": LoCoMoAdapter,
}


def get_adapter(name: str) -> BenchmarkAdapter:
    try:
        return ADAPTERS[name.lower()]()
    except KeyError as exc:
        raise ValueError(f"unsupported trace adapter: {name}") from exc


def adapt_records(
    name: str, records: Iterable[dict[str, Any]], episode_id: str | None = None
) -> AdaptationResult:
    return get_adapter(name).adapt(records, episode_id=episode_id)
