"""Canonical progress events for the formal provenance experiment."""

from __future__ import annotations

import copy
import json
import math
import os
import re
import secrets
import select
import stat
import threading
import time
import weakref
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
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
SNAPSHOT_FIELDS = frozenset((EVENT_FIELDS - {"schema"}) | {"schema", "last_update_age_seconds"})
TERMINAL_STATUSES = frozenset({"completed", "blocked", "interrupted"})
TERMINAL_REASON_CLASSES = frozenset(
    {
        "completed",
        "formal_eligibility_failed",
        "backend_timeout",
        "progress_protocol_failed",
        "collector_interrupted",
        "resource_cleanup_failed",
    }
)
_STORE_LOCKS_GUARD = threading.Lock()


class _StoreWriterLock:
    def __init__(self) -> None:
        self.lock = threading.RLock()


_STORE_LOCKS: weakref.WeakValueDictionary[tuple[int, int, str], _StoreWriterLock] = (
    weakref.WeakValueDictionary()
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


def _store_writer_lock(parent_identity: tuple[int, int], target_name: str) -> _StoreWriterLock:
    lock_key = (*parent_identity, target_name)
    with _STORE_LOCKS_GUARD:
        writer_lock = _STORE_LOCKS.get(lock_key)
        if writer_lock is None:
            writer_lock = _StoreWriterLock()
            _STORE_LOCKS[lock_key] = writer_lock
        return writer_lock


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


def _validate_snapshot(snapshot: Mapping[str, Any], *, persisted: bool = False) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        _protocol_error("progress snapshot must be a mapping")
    try:
        normalized = dict(snapshot)
        actual_fields = frozenset(normalized)
    except (TypeError, ValueError):
        _protocol_error("progress snapshot must be a plain mapping")

    has_terminal_reason = "terminal_reason_class" in actual_fields
    expected_fields = SNAPSHOT_FIELDS | ({"terminal_reason_class"} if has_terminal_reason else set())
    if actual_fields != expected_fields:
        _protocol_error("invalid progress snapshot fields")
    if normalized["schema"] != PROGRESS_SNAPSHOT_SCHEMA:
        _protocol_error("snapshot schema has an invalid value")
    if normalized["phase"] != "measurement":
        _protocol_error("snapshot phase has an invalid value")
    _validate_hash("run_binding_sha256", normalized["run_binding_sha256"])
    _validate_hash("config_sha256", normalized["config_sha256"])
    if type(normalized["last_update_age_seconds"]) is not int or normalized["last_update_age_seconds"] < 0:
        _protocol_error("last_update_age_seconds must be a non-negative integer")
    if persisted and normalized["last_update_age_seconds"] != 0:
        _protocol_error("persisted last_update_age_seconds must be zero")

    status = normalized["status"]
    if type(status) is not str or status not in {"starting", "running"} | TERMINAL_STATUSES:
        _protocol_error("snapshot status has an invalid value")
    if status in TERMINAL_STATUSES:
        if (
            not has_terminal_reason
            or type(normalized["terminal_reason_class"]) is not str
            or normalized["terminal_reason_class"] not in TERMINAL_REASON_CLASSES
        ):
            _protocol_error("snapshot terminal reason class has an invalid value")
    elif has_terminal_reason:
        _protocol_error("non-terminal snapshot cannot have a terminal reason class")

    event_like = {key: value for key, value in normalized.items() if key not in {"last_update_age_seconds", "terminal_reason_class"}}
    event_like["schema"] = PROGRESS_EVENT_SCHEMA
    event_like["status"] = "running"
    if normalized["update_sequence"] == 0:
        if any(type(normalized[name]) is not int for name in _INTEGER_FIELDS):
            _protocol_error("snapshot starting counts must be integers")
        zero_values = {
            "cell_index": 1,
            "cell_count": 15,
            "graph_size": 100,
            "concurrency": 1,
            "repetition_index": 0,
            "repetition_count": 30,
            "completed_repetitions": 0,
            "total_repetitions": 450,
            "completed_samples": 0,
            "total_samples": 14400,
            "update_sequence": 0,
        }
        if status == "running" or any(normalized.get(name) != value for name, value in zero_values.items()):
            _protocol_error("snapshot starting counts are invalid")
    else:
        if status == "starting":
            _protocol_error("positive progress snapshot cannot be starting")
        _validate_event(event_like)
    return copy.deepcopy(normalized)


def canonical_snapshot_line(snapshot: Mapping[str, Any]) -> bytes:
    """Return the sole canonical encoding for an already-valid snapshot closure."""

    normalized = _validate_snapshot(snapshot)
    try:
        encoded = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ProgressProtocolError("progress snapshot cannot be canonically encoded") from exc
    line = encoded + b"\n"
    if len(line) > MAX_PROGRESS_LINE_BYTES:
        _protocol_error("progress snapshot exceeds 4096 bytes")
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


class ProgressSnapshotStore:
    """Persist a sanitized formal-progress snapshot using one directory-owned replace."""

    def __init__(
        self,
        path: Path,
        *,
        expected_uid: int,
        expected_gid: int,
        expected_parent_identity: tuple[int, int] | None = None,
    ) -> None:
        if not isinstance(path, Path) or not path.name or path.name in {".", ".."}:
            _protocol_error("progress snapshot path must name a file")
        if type(expected_uid) is not int or expected_uid < 0:
            _protocol_error("expected snapshot UID must be a non-negative integer")
        if type(expected_gid) is not int or expected_gid < 0:
            _protocol_error("expected snapshot GID must be a non-negative integer")
        if expected_parent_identity is not None and (
            type(expected_parent_identity) is not tuple
            or len(expected_parent_identity) != 2
            or any(type(value) is not int or value < 0 for value in expected_parent_identity)
        ):
            _protocol_error("expected snapshot parent identity is invalid")
        self._parent = path.parent
        self._target_name = path.name
        self._expected_uid = expected_uid
        self._expected_gid = expected_gid
        self._transition_lock = threading.Lock()
        parent_fd = self._open_parent()
        try:
            parent_stat = os.fstat(parent_fd)
            self._parent_identity = (parent_stat.st_dev, parent_stat.st_ino)
            if (
                expected_parent_identity is not None
                and self._parent_identity != expected_parent_identity
            ):
                _protocol_error("progress snapshot parent identity does not match")
        finally:
            os.close(parent_fd)
        self._writer_lock = _store_writer_lock(self._parent_identity, self._target_name)

    def _open_parent(self) -> int:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            parent_fd = os.open(self._parent, flags)
        except OSError as exc:
            raise ProgressProtocolError("progress snapshot parent is unsafe") from exc
        try:
            parent_stat = os.fstat(parent_fd)
            if not stat.S_ISDIR(parent_stat.st_mode):
                _protocol_error("progress snapshot parent is not a directory")
        except BaseException:
            os.close(parent_fd)
            raise
        return parent_fd

    def _open_bound_parent(self) -> int:
        parent_fd = self._open_parent()
        try:
            parent_stat = os.fstat(parent_fd)
            if (parent_stat.st_dev, parent_stat.st_ino) != self._parent_identity:
                _protocol_error("progress snapshot parent changed")
        except BaseException:
            os.close(parent_fd)
            raise
        return parent_fd

    def _validate_file_stat(self, file_stat: os.stat_result) -> None:
        if not stat.S_ISREG(file_stat.st_mode):
            _protocol_error("progress snapshot is not a regular file")
        if file_stat.st_uid != self._expected_uid or file_stat.st_gid != self._expected_gid:
            _protocol_error("progress snapshot owner does not match")
        if stat.S_IMODE(file_stat.st_mode) != 0o600:
            _protocol_error("progress snapshot mode must be 0600")
        if file_stat.st_nlink != 1:
            _protocol_error("progress snapshot link count must be one")

    def _stat_target(self, parent_fd: int) -> os.stat_result | None:
        try:
            file_stat = os.stat(self._target_name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ProgressProtocolError("progress snapshot target cannot be inspected") from exc
        self._validate_file_stat(file_stat)
        return file_stat

    def _read_persisted(self, parent_fd: int) -> tuple[dict[str, Any], os.stat_result]:
        initial_stat = self._stat_target(parent_fd)
        if initial_stat is None:
            _protocol_error("progress snapshot does not exist")
        try:
            descriptor = os.open(
                self._target_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise ProgressProtocolError("progress snapshot cannot be opened safely") from exc
        try:
            opened_stat = os.fstat(descriptor)
            self._validate_file_stat(opened_stat)
            if (opened_stat.st_dev, opened_stat.st_ino) != (initial_stat.st_dev, initial_stat.st_ino):
                _protocol_error("progress snapshot changed while opening")
            chunks: list[bytes] = []
            remaining = MAX_PROGRESS_LINE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            if remaining == 0 and os.read(descriptor, 1):
                _protocol_error("progress snapshot exceeds 4096 bytes")
            payload = b"".join(chunks)
            final_stat = os.fstat(descriptor)
            self._validate_file_stat(final_stat)
            current_stat = self._stat_target(parent_fd)
            if current_stat is None or (final_stat.st_dev, final_stat.st_ino) != (
                current_stat.st_dev,
                current_stat.st_ino,
            ):
                _protocol_error("progress snapshot changed while reading")
        except OSError as exc:
            raise ProgressProtocolError("progress snapshot cannot be read") from exc
        finally:
            os.close(descriptor)

        if not payload.endswith(b"\n") or payload[:-1].find(b"\n") != -1:
            _protocol_error("progress snapshot must contain exactly one final newline")
        try:
            decoded = json.loads(
                payload[:-1].decode("utf-8", errors="strict"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except ProgressProtocolError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ProgressProtocolError("invalid progress snapshot JSON") from exc
        if not isinstance(decoded, dict):
            _protocol_error("progress snapshot JSON must be an object")
        snapshot = _validate_snapshot(decoded, persisted=True)
        if canonical_snapshot_line(snapshot) != payload:
            _protocol_error("progress snapshot is not canonical JSON")
        return snapshot, final_stat

    def _write_all(self, descriptor: int, payload: bytes) -> None:
        written = 0
        while written < len(payload):
            try:
                count = os.write(descriptor, payload[written:])
            except OSError as exc:
                raise ProgressProtocolError("progress snapshot write failed") from exc
            if count <= 0:
                _protocol_error("progress snapshot write was incomplete")
            written += count

    def _write_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        parent_fd: int,
        expected_identity: tuple[int, int] | None = None,
        reject_terminal_predecessor: bool = False,
    ) -> None:
        normalized = _validate_snapshot(snapshot, persisted=True)
        payload = canonical_snapshot_line(normalized)
        temporary_fd: int | None = None
        temporary_stat: os.stat_result | None = None
        temporary_name = ".txnmem-progress-snapshot-" + secrets.token_hex(16) + ".tmp"
        try:
            existing = self._stat_target(parent_fd)
            if existing is not None:
                existing_snapshot, existing_stat = self._read_persisted(parent_fd)
                existing_identity = (existing_stat.st_dev, existing_stat.st_ino)
                if expected_identity is not None and existing_identity != expected_identity:
                    _protocol_error("progress snapshot changed before replacement")
                if reject_terminal_predecessor and existing_snapshot["status"] in TERMINAL_STATUSES:
                    _protocol_error("terminal progress snapshot cannot resume running")
            elif expected_identity is not None:
                _protocol_error("progress snapshot changed before replacement")
            try:
                temporary_fd = os.open(
                    temporary_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                temporary_stat = os.fstat(temporary_fd)
            except OSError as exc:
                raise ProgressProtocolError("progress snapshot temporary file cannot be created") from exc
            try:
                os.fchmod(temporary_fd, 0o600)
                temporary_stat = os.fstat(temporary_fd)
                self._validate_file_stat(temporary_stat)
                self._write_all(temporary_fd, payload)
                os.fsync(temporary_fd)
            except OSError as exc:
                raise ProgressProtocolError("progress snapshot temporary file cannot be finalized") from exc
            finally:
                os.close(temporary_fd)
                temporary_fd = None
            try:
                os.replace(
                    temporary_name,
                    self._target_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
            except OSError as exc:
                raise ProgressProtocolError("progress snapshot atomic replace failed") from exc
            replaced_stat = self._stat_target(parent_fd)
            if replaced_stat is None or (replaced_stat.st_dev, replaced_stat.st_ino) != (
                temporary_stat.st_dev,
                temporary_stat.st_ino,
            ):
                _protocol_error("progress snapshot changed during replacement")
            try:
                os.fsync(parent_fd)
            except OSError as exc:
                raise ProgressProtocolError("progress snapshot directory cannot be finalized") from exc
        finally:
            if temporary_fd is not None:
                os.close(temporary_fd)

    def write_starting(self, run_binding_sha256: str, config_sha256: str) -> None:
        _validate_hash("run_binding_sha256", run_binding_sha256)
        _validate_hash("config_sha256", config_sha256)
        snapshot = {
            "schema": PROGRESS_SNAPSHOT_SCHEMA,
            "run_binding_sha256": run_binding_sha256,
            "config_sha256": config_sha256,
            "phase": "measurement",
            "cell_index": 1,
            "cell_count": 15,
            "graph_size": 100,
            "concurrency": 1,
            "repetition_index": 0,
            "repetition_count": 30,
            "completed_repetitions": 0,
            "total_repetitions": 450,
            "completed_samples": 0,
            "total_samples": 14400,
            "update_sequence": 0,
            "status": "starting",
            "last_update_age_seconds": 0,
        }
        with self._transition_lock:
            with self._writer_lock.lock:
                parent_fd = self._open_bound_parent()
                try:
                    self._write_snapshot(snapshot, parent_fd=parent_fd)
                finally:
                    os.close(parent_fd)

    def write_running(self, event: Mapping[str, Any]) -> None:
        normalized = _validate_event(event)
        snapshot = dict(normalized)
        snapshot["schema"] = PROGRESS_SNAPSHOT_SCHEMA
        snapshot["last_update_age_seconds"] = 0
        with self._transition_lock:
            with self._writer_lock.lock:
                parent_fd = self._open_bound_parent()
                try:
                    self._write_snapshot(
                        snapshot,
                        parent_fd=parent_fd,
                        reject_terminal_predecessor=True,
                    )
                finally:
                    os.close(parent_fd)

    def write_terminal(self, status: str, reason_class: str) -> None:
        if type(status) is not str or status not in TERMINAL_STATUSES:
            _protocol_error("terminal snapshot status has an invalid value")
        if type(reason_class) is not str or reason_class not in TERMINAL_REASON_CLASSES:
            _protocol_error("terminal snapshot reason class has an invalid value")
        with self._transition_lock:
            with self._writer_lock.lock:
                parent_fd = self._open_bound_parent()
                try:
                    snapshot, current_stat = self._read_persisted(parent_fd)
                    if snapshot["status"] in TERMINAL_STATUSES:
                        _protocol_error("terminal progress snapshot cannot transition again")
                    snapshot["status"] = status
                    snapshot["terminal_reason_class"] = reason_class
                    snapshot["last_update_age_seconds"] = 0
                    self._write_snapshot(
                        snapshot,
                        parent_fd=parent_fd,
                        expected_identity=(current_stat.st_dev, current_stat.st_ino),
                    )
                finally:
                    os.close(parent_fd)

    def read_view(self) -> dict[str, Any]:
        with self._transition_lock:
            with self._writer_lock.lock:
                parent_fd = self._open_bound_parent()
                try:
                    snapshot, file_stat = self._read_persisted(parent_fd)
                finally:
                    os.close(parent_fd)
        view = dict(snapshot)
        view["last_update_age_seconds"] = max(0, math.floor(time.time() - file_stat.st_mtime))
        return copy.deepcopy(view)


class ProgressPipeDrainer:
    """Drain canonical progress lines without retaining unbounded pipe input."""

    def __init__(
        self,
        descriptor: int,
        state: FormalProgressState,
        store: ProgressSnapshotStore,
        *,
        allow_empty: bool = False,
    ) -> None:
        if type(descriptor) is not int or descriptor < 0:
            _protocol_error("progress pipe descriptor must be a non-negative integer")
        if not isinstance(state, FormalProgressState) or not isinstance(store, ProgressSnapshotStore):
            _protocol_error("progress drainer dependencies are invalid")
        if type(allow_empty) is not bool:
            _protocol_error("progress empty-record policy must be a boolean")
        self._descriptor = descriptor
        self._state = state
        self._store = store
        self._lock = threading.Lock()
        self._descriptor_lock = threading.Lock()
        self._stopped = threading.Event()
        self._aborted = threading.Event()
        self._descriptor_closed = False
        self._thread: threading.Thread | None = None
        self._failure: ProgressProtocolError | None = None
        self._last_view: dict[str, Any] | None = None
        self._received_event = False
        self._allow_empty = allow_empty

    @property
    def thread(self) -> threading.Thread:
        if self._thread is None:
            _protocol_error("progress drainer has not started")
        return self._thread

    @property
    def failure(self) -> ProgressProtocolError | None:
        with self._lock:
            return self._failure

    def _close_descriptor(self) -> None:
        with self._descriptor_lock:
            with self._lock:
                if self._descriptor_closed:
                    return
                self._descriptor_closed = True
                descriptor = self._descriptor
        try:
            os.close(descriptor)
        except OSError:
            pass

    def start(self) -> None:
        with self._lock:
            if self._failure is not None:
                raise self._failure
            if self._thread is not None:
                return
            thread = threading.Thread(target=self._drain, name="formal-progress-drainer", daemon=True)
            self._thread = thread
        try:
            thread.start()
        except Exception:
            failure = ProgressProtocolError("progress drainer could not start")
            with self._lock:
                self._thread = None
                self._failure = failure
            self._close_descriptor()
            raise failure from None

    def _drain(self) -> None:
        record = bytearray()
        try:
            while not self._aborted.is_set():
                try:
                    with self._descriptor_lock:
                        if self._descriptor_closed:
                            return
                        readable, _, _ = select.select([self._descriptor], [], [], 0.1)
                        if not readable:
                            continue
                        chunk = os.read(self._descriptor, 1)
                except (OSError, ValueError) as exc:
                    if self._aborted.is_set():
                        return
                    raise ProgressProtocolError("progress pipe cannot be monitored") from exc
                if not chunk:
                    if record:
                        _protocol_error("progress pipe closed with a partial record")
                    if not self._received_event and not self._allow_empty:
                        _protocol_error("progress pipe closed without events")
                    return
                record.extend(chunk)
                if len(record) > MAX_PROGRESS_LINE_BYTES:
                    _protocol_error("progress pipe record exceeds 4096 bytes")
                if chunk != b"\n":
                    continue
                event = decode_progress_line(bytes(record))
                consumed = self._state.consume(event)
                self._store.write_running(consumed)
                self._last_view = self._store.read_view()
                self._received_event = True
                record.clear()
        except ProgressProtocolError as exc:
            with self._lock:
                self._failure = exc
        except Exception:
            with self._lock:
                self._failure = ProgressProtocolError("progress drainer worker failed")
        finally:
            self._close_descriptor()
            self._stopped.set()

    def finish(self, timeout_seconds: float) -> dict[str, Any] | None:
        if type(timeout_seconds) not in {int, float} or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            _protocol_error("progress drainer timeout must be finite and positive")
        thread = self.thread
        thread.join(timeout_seconds)
        if thread.is_alive():
            self.abort()
            _protocol_error("progress drainer did not stop")
        failure = self.failure
        if failure is not None:
            raise failure
        return copy.deepcopy(self._last_view)

    def abort(self) -> None:
        self._aborted.set()
        self._close_descriptor()
