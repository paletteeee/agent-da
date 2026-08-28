"""Canonical progress events for the formal provenance experiment."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


class ProgressProtocolError(RuntimeError):
    """Raised when a progress line or state transition is not protocol-valid."""


PROGRESS_EVENT_SCHEMA = "txnmem-provenance-progress-event-v1"
PROGRESS_SNAPSHOT_SCHEMA = "txnmem-provenance-progress-snapshot-v1"
MAX_PROGRESS_LINE_BYTES = 4096
FORMAL_MATRIX_CELLS: tuple[tuple[int, int], ...] = tuple(
    (graph_size, concurrency)
    for graph_size in (100, 1000, 10000)
    for concurrency in (1, 2, 4, 8, 16)
)
EVENT_FIELDS = frozenset(
    {
        "schema",
        "run_binding_sha256",
        "config_sha256",
        "phase",
        "cell_index",
        "cell_count",
        "graph_size",
        "concurrency",
        "repetition_index",
        "repetition_count",
        "completed_repetitions",
        "total_repetitions",
        "completed_samples",
        "total_samples",
        "update_sequence",
        "status",
    }
)

_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_INTEGER_FIELDS = frozenset(
    {
        "cell_index",
        "cell_count",
        "graph_size",
        "concurrency",
        "repetition_index",
        "repetition_count",
        "completed_repetitions",
        "total_repetitions",
        "completed_samples",
        "total_samples",
        "update_sequence",
    }
)


def _protocol_error(message: str) -> None:
    raise ProgressProtocolError(message)


def _validate_hash(name: str, value: Any) -> str:
    if type(value) is not str or _HASH_PATTERN.fullmatch(value) is None:
        _protocol_error(f"{name} must be a lowercase 64-character SHA-256 hex string")
    return value


def _validate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(event, Mapping):
        _protocol_error("progress event must be a mapping")
    try:
        normalized = dict(event)
        actual_fields = frozenset(normalized)
    except (TypeError, ValueError):
        _protocol_error("progress event must be a plain mapping")

    if actual_fields != EVENT_FIELDS:
        unknown = sorted(actual_fields - EVENT_FIELDS, key=repr)
        missing = sorted(EVENT_FIELDS - actual_fields)
        details = []
        if unknown:
            details.append(f"unknown fields: {unknown!r}")
        if missing:
            details.append(f"missing fields: {missing!r}")
        _protocol_error("invalid progress event fields (" + "; ".join(details) + ")")

    string_values = {
        "schema": PROGRESS_EVENT_SCHEMA,
        "phase": "measurement",
        "status": "running",
    }
    for name, expected in string_values.items():
        if type(normalized[name]) is not str or normalized[name] != expected:
            _protocol_error(f"{name} has an invalid value")

    _validate_hash("run_binding_sha256", normalized["run_binding_sha256"])
    _validate_hash("config_sha256", normalized["config_sha256"])

    for name in _INTEGER_FIELDS:
        if type(normalized[name]) is not int:
            _protocol_error(f"{name} must be an integer")

    if not 1 <= normalized["cell_index"] <= len(FORMAL_MATRIX_CELLS):
        _protocol_error("cell_index is outside the formal matrix")
    if normalized["cell_count"] != len(FORMAL_MATRIX_CELLS):
        _protocol_error("cell_count must be 15")
    expected_graph_size, expected_concurrency = FORMAL_MATRIX_CELLS[normalized["cell_index"] - 1]
    if normalized["graph_size"] != expected_graph_size:
        _protocol_error("graph_size does not match cell_index")
    if normalized["concurrency"] != expected_concurrency:
        _protocol_error("concurrency does not match cell_index")
    if not 1 <= normalized["repetition_index"] <= 30:
        _protocol_error("repetition_index is outside the formal repetition count")
    if normalized["repetition_count"] != 30:
        _protocol_error("repetition_count must be 30")
    if not 1 <= normalized["completed_repetitions"] <= 450:
        _protocol_error("completed_repetitions is outside the formal count")
    if normalized["total_repetitions"] != 450:
        _protocol_error("total_repetitions must be 450")
    if not 1 <= normalized["completed_samples"] <= 14400:
        _protocol_error("completed_samples is outside the formal count")
    if normalized["total_samples"] != 14400:
        _protocol_error("total_samples must be 14400")
    if not 1 <= normalized["update_sequence"] <= 450:
        _protocol_error("update_sequence is outside the formal count")
    if normalized["completed_samples"] != normalized["completed_repetitions"] * 32:
        _protocol_error("completed_samples must equal completed_repetitions times 32")

    return normalized


def _canonical_json(event: Mapping[str, Any]) -> bytes:
    normalized = _validate_event(event)
    try:
        encoded = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ProgressProtocolError("progress event cannot be canonically encoded") from exc
    return encoded


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _protocol_error(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    _protocol_error(f"non-finite JSON number: {value}")


def decode_progress_line(payload: bytes) -> dict[str, Any]:
    """Decode one strict, canonical progress record."""

    if type(payload) is not bytes:
        _protocol_error("progress line must be bytes")
    if len(payload) > MAX_PROGRESS_LINE_BYTES:
        _protocol_error("progress line exceeds 4096 bytes")
    if not payload.endswith(b"\n") or payload[:-1].find(b"\n") != -1:
        _protocol_error("progress line must contain exactly one final newline")

    body = payload[:-1]
    try:
        text = body.decode("utf-8", errors="strict")
        decoded = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except ProgressProtocolError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ProgressProtocolError("invalid progress JSON") from exc

    if not isinstance(decoded, dict):
        _protocol_error("progress JSON must be an object")
    normalized = _validate_event(decoded)
    try:
        canonical = _canonical_json(normalized) + b"\n"
    except ProgressProtocolError:
        raise
    if canonical != payload:
        _protocol_error("progress line is not canonical JSON")
    return copy.deepcopy(normalized)


def build_progress_event(
    *,
    run_binding_sha256: str,
    config_sha256: str,
    cell_index: int,
    graph_size: int,
    concurrency: int,
    repetition_index: int,
    completed_repetitions: int,
    completed_samples: int,
    update_sequence: int,
) -> dict[str, Any]:
    """Build one event using the fixed formal experiment dimensions."""

    event = {
        "schema": PROGRESS_EVENT_SCHEMA,
        "run_binding_sha256": run_binding_sha256,
        "config_sha256": config_sha256,
        "phase": "measurement",
        "cell_index": cell_index,
        "cell_count": 15,
        "graph_size": graph_size,
        "concurrency": concurrency,
        "repetition_index": repetition_index,
        "repetition_count": 30,
        "completed_repetitions": completed_repetitions,
        "total_repetitions": 450,
        "completed_samples": completed_samples,
        "total_samples": 14400,
        "update_sequence": update_sequence,
        "status": "running",
    }
    return copy.deepcopy(_validate_event(event))


def canonical_progress_line(event: Mapping[str, Any]) -> bytes:
    """Return the only accepted JSON representation of a progress event."""

    line = _canonical_json(event) + b"\n"
    if len(line) > MAX_PROGRESS_LINE_BYTES:
        _protocol_error("progress line exceeds 4096 bytes")
    return line


@dataclass
class FormalProgressState:
    run_binding_sha256: str
    config_sha256: str
    _last_sequence: int = field(default=0, init=False, repr=False)
    _last_event: dict[str, Any] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        _validate_hash("run_binding_sha256", self.run_binding_sha256)
        _validate_hash("config_sha256", self.config_sha256)

    def consume(self, event: Mapping[str, Any]) -> dict[str, Any]:
        normalized = _validate_event(event)
        expected_sequence = self._last_sequence + 1
        if expected_sequence > 450:
            _protocol_error("progress state is already complete")

        expected_cell_index = (expected_sequence - 1) // 30 + 1
        expected_repetition_index = (expected_sequence - 1) % 30 + 1
        expected_graph_size, expected_concurrency = FORMAL_MATRIX_CELLS[expected_cell_index - 1]
        expected_samples = expected_sequence * 32
        expected = {
            "run_binding_sha256": self.run_binding_sha256,
            "config_sha256": self.config_sha256,
            "cell_index": expected_cell_index,
            "graph_size": expected_graph_size,
            "concurrency": expected_concurrency,
            "repetition_index": expected_repetition_index,
            "completed_repetitions": expected_sequence,
            "completed_samples": expected_samples,
            "update_sequence": expected_sequence,
        }
        for name, expected_value in expected.items():
            if normalized[name] != expected_value:
                _protocol_error(f"{name} is not the legal successor value")

        self._last_sequence = expected_sequence
        self._last_event = copy.deepcopy(normalized)
        return copy.deepcopy(self._last_event)
