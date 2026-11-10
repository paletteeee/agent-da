"""Dataset-specific trace adapters for trace-grounded TxnMem replay.

The adapters deliberately implement a narrow boundary.  They only translate
events that are explicitly present in a source log; a conversation turn or an
unrecognised tool call is skipped rather than converted into a fabricated
memory operation.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
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
    event["projection"] = record.get("projection", "explicit_memory_event")
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

    _READ_TOOLS = {
        "get_user_details",
        "get_reservation_details",
        "search_flights",
        "get_flight_details",
        "list_reservations",
        "get_trip_details",
    }

    def _tool_kind(self, name: str) -> str | None:
        normalized = name.lower()
        if normalized in self._READ_TOOLS or normalized.startswith(("get_", "list_", "search_", "find_")):
            return "memory_search" if normalized.startswith(("search_", "list_")) else "memory_read"
        if normalized.startswith(
            ("book_", "cancel_", "modify_", "update_", "edit_", "change_", "add_", "remove_")
        ):
            return "memory_write"
        return None

    def _expand_record(self, record: dict[str, Any], index: int) -> list[dict[str, Any]]:
        trajectory = record.get("traj")
        if not isinstance(trajectory, list):
            trajectory = [record]
        expanded: list[dict[str, Any]] = []
        task_id = record.get("task_id") or record.get("episode_id") or f"tau_episode_{index:04d}"
        call_index = 0
        for turn_index, turn in enumerate(trajectory, start=1):
            if not isinstance(turn, dict):
                continue
            role = turn.get("role")
            if role == "system" and turn.get("content"):
                expanded.append(
                    {
                        "id": f"{task_id}:policy:{turn_index}",
                        "kind": "policy_change",
                        "content": turn["content"],
                        "task_id": task_id,
                        "projection": "tau_policy_guideline",
                    }
                )
            for tool_call in turn.get("tool_calls", []) or []:
                function = tool_call.get("function", {}) if isinstance(tool_call, dict) else {}
                tool_name = function.get("name") or tool_call.get("name")
                if not tool_name:
                    continue
                arguments = function.get("arguments", tool_call.get("arguments", {}))
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except json.JSONDecodeError:
                        arguments = {"raw_arguments": arguments}
                if not isinstance(arguments, dict):
                    arguments = {"value": arguments}
                kind = self._tool_kind(str(tool_name))
                if kind is None:
                    continue
                call_index += 1
                expanded.append(
                    {
                        "id": f"{task_id}:tool:{call_index:04d}",
                        "kind": kind,
                        "tool_name": str(tool_name),
                        "arguments": arguments,
                        "task_id": task_id,
                        "agent_id": "agent_1",
                        "projection": "tau_api_tool_call",
                        "memory_id": f"tau:{task_id}:{call_index:04d}",
                        "value": {"tool_name": tool_name, "arguments": arguments},
                    }
                )
        return expanded

    def adapt(self, records: Iterable[dict[str, Any]], episode_id: str | None = None) -> AdaptationResult:
        expanded: list[dict[str, Any]] = []
        for index, record in enumerate(records, start=1):
            if isinstance(record, dict) and (
                isinstance(record.get("traj"), list) or isinstance(record.get("tool_calls"), list)
            ):
                expanded.extend(self._expand_record(record, index))
            else:
                expanded.append(record)
        return super().adapt(expanded, episode_id=episode_id)

    def classify(self, record: dict[str, Any]) -> str | None:
        kind = super().classify(record)
        if kind:
            return kind
        if record.get("content") and record.get("role") == "system":
            return "policy_change"
        # Policy guidelines and explicit policy events are retained as a
        # schedule signal; ordinary user/assistant messages are not memory ops.
        if any(key in record for key in ("policy_guideline", "policy_guidelines", "policy_event")):
            return "policy_revoke" if "deny" in str(record).lower() else "policy_change"
        return None


class AppWorldAdapter(BenchmarkAdapter):
    dataset = "appworld"

    def _api_kind(self, name: str) -> str | None:
        normalized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        if normalized.startswith(("get_", "list_", "read_", "search_", "find_", "show_", "fetch_", "lookup_")):
            return "memory_search" if normalized.startswith(("list_", "search_")) else "memory_read"
        if normalized.startswith(
            ("create_", "add_", "update_", "delete_", "remove_", "send_", "pay_", "save_", "complete_", "book_")
        ):
            return "memory_write"
        return None

    def adapt(self, records: Iterable[dict[str, Any]], episode_id: str | None = None) -> AdaptationResult:
        expanded: list[dict[str, Any]] = []
        for index, record in enumerate(records, start=1):
            if not isinstance(record, dict):
                expanded.append(record)
                continue
            if super().classify(record) is not None:
                expanded.append(record)
                continue
            calls = record.get("api_calls") or record.get("calls")
            if isinstance(calls, list):
                for call_index, call in enumerate(calls, start=1):
                    if not isinstance(call, dict):
                        continue
                    name = call.get("api_name") or call.get("name") or call.get("function_name")
                    kind = self._api_kind(str(name)) if name else None
                    if kind is None:
                        continue
                    expanded.append(
                        {
                            **call,
                            "id": f"{record.get('task_id', episode_id or index)}:api:{call_index:04d}",
                            "kind": kind,
                            "projection": "appworld_api_call",
                            "memory_id": f"appworld:{record.get('task_id', episode_id or index)}:{call_index:04d}",
                        }
                    )
            else:
                api_name = record.get("api_name") or record.get("name") or record.get("url")
                method = str(record.get("method", "")).upper()
                kind = (_canonical_kind(api_name) or self._api_kind(str(api_name))) if api_name else None
                if kind is None and method in {"GET", "HEAD"}:
                    kind = "memory_search" if "list" in str(api_name).lower() else "memory_read"
                elif kind is None and method in {"POST", "PUT", "PATCH", "DELETE"}:
                    kind = "memory_write"
                if kind is not None:
                    item = dict(record)
                    item["id"] = str(record.get("id") or f"{episode_id or index}:api:{index:04d}")
                    item["kind"] = kind
                    item["projection"] = "appworld_api_call"
                    if kind in {"memory_write", "memory_derive", "memory_propagate"}:
                        item["memory_id"] = f"appworld:{episode_id or 'episode'}:{index:04d}"
                        item["value"] = {"method": method, "url": record.get("url", api_name)}
                    expanded.append(item)
        return super().adapt(expanded, episode_id=episode_id)

    def classify(self, record: dict[str, Any]) -> str | None:
        kind = super().classify(record)
        if kind:
            return kind
        api = _first(record, "api", "api_call", "function_name", "name")
        if api:
            return _canonical_kind(api) or self._api_kind(str(api))
        url = record.get("url")
        method = str(record.get("method", "")).upper()
        if url and method in {"GET", "HEAD"}:
            return "memory_search" if "list" in str(url).lower() else "memory_read"
        if url and method in {"POST", "PUT", "PATCH", "DELETE"}:
            return "memory_write"
        return None


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
            if isinstance(record, dict) and isinstance(record.get("conversation"), dict):
                sample_id = record.get("sample_id") or episode_id or "locomo"
                summaries = record.get("session_summary", {})
                for key, summary in sorted(summaries.items()):
                    if not str(key).endswith("_summary") or not summary:
                        continue
                    session_id = str(key).removesuffix("_summary")
                    expanded.append(
                        {
                            "id": f"{sample_id}:{session_id}:summary",
                            "kind": "memory_write",
                            "memory_id": f"locomo:{sample_id}:{session_id}",
                            "value": summary,
                            "sample_id": sample_id,
                            "session_id": session_id,
                            "timestamp": record["conversation"].get(f"{session_id}_date_time"),
                            "agent_id": "agent_1",
                            "projection": "locomo_session_summary",
                        }
                    )
                if not summaries:
                    for key, turns in sorted(record["conversation"].items()):
                        if not str(key).startswith("session_") or str(key).endswith("_date_time"):
                            continue
                        value = " ".join(
                            str(turn.get("text", "")) for turn in turns if isinstance(turn, dict)
                        )
                        if value:
                            expanded.append(
                                {
                                    "id": f"{sample_id}:{key}",
                                    "kind": "memory_write",
                                    "memory_id": f"locomo:{sample_id}:{key}",
                                    "value": value,
                                    "sample_id": sample_id,
                                    "session_id": key,
                                    "agent_id": "agent_1",
                                    "projection": "locomo_session_transcript",
                                }
                            )
                continue
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
