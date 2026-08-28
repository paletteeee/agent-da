"""Independent two-phase collector for formal provenance measurements.

The collector writes an immutable launch record before starting the benchmark,
then writes a separate immutable completion record after the child exits.  Raw
host/process identifiers remain in these out-of-tree records; the topology
sanitizer hashes them before anything becomes eligible for source control.
"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import errno
from email import policy as email_policy
from email.parser import BytesParser
import functools
import hashlib
import ipaddress
import importlib
import io
import json
import math
import os
import platform
import re
import secrets
import select
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from txnmem_formal_io import FormalStore, canonical_json_bytes
from txnmem_toxiproxy_metrics import (
    ToxiproxyMetricsError,
    derive_proxy_counter_deltas,
    parse_toxiproxy_byte_counters,
    proxy_counter_payload_sha256,
    proxy_counter_values,
    validate_proxy_counter_snapshot,
)
from txnmem_provenance_contract import (
    FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
    FORMAL_RUNNER_GID,
    FORMAL_RUNNER_UID,
    is_registered_service_version,
)
from txnmem_provenance_performance import (
    candidate_attestation_material,
    formal_config_file_sha256,
    formal_matrix_config_sha256,
    formal_matrix_workload_sha256,
    load_strict_json_document,
    provenance_bundle_id,
    validate_environment_attestation,
    validate_matrix_config,
)
from txnmem_provenance_progress import (
    FormalProgressState,
    PROGRESS_SNAPSHOT_SCHEMA,
    SNAPSHOT_FIELDS,
    ProgressPipeDrainer,
    ProgressProtocolError,
    ProgressSnapshotStore,
    canonical_snapshot_line,
)
from txnmem_topology_attestation import (
    COLLECTOR_ID,
    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
    RAW_COMPLETION_SCHEMA,
    RAW_LAUNCH_SCHEMA,
    _read_private_authorization_nonce as _read_private_nonce_file,
    _validate_raw_backend_isolation,
    _validate_candidate_seal,
    _validate_child_process,
    _validate_command_manifest,
    _validate_external_tools,
    _validate_execution_monitor_attestation,
    _validate_network_guard_attestation,
    _validate_runtime_manifest,
    execution_authorization_proof,
)


SNAPSHOT_SCHEMA = "txnmem-provenance-topology-snapshot-v3"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_RFC1918_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_FORMAL_QDRANT_PROXY = "txnmem-qdrant"
_FORMAL_NEO4J_PROXY = "txnmem-neo4j"
_FORMAL_QDRANT_CONTAINER = "txnmem-qdrant"
_FORMAL_NEO4J_CONTAINER = "txnmem-neo4j"
_FORMAL_TOXIPROXY_CONTAINER = "txnmem-toxiproxy"
_FORMAL_BACKGROUND_CPU_BUSY_LIMIT_PERMILLE = 200
_FORMAL_MAX_LOAD1_PER_CPU_MILLI = 1000
_FORMAL_MAX_LOGICAL_CPU_COUNT = 1024
_FORMAL_RUNTIME_WHEEL_DIRECTORY = Path("/opt/txnmem-formal-runtime/wheels")
_FORMAL_RUNS_ROOT = Path("/var/lib/txnmem-formal/runs")
_FORMAL_CONTROLLER_UID = 0
_FORMAL_CONTROLLER_GID = 0
_FORMAL_GIT_EXECUTABLE = "/usr/bin/git"
_FORMAL_DOCKER_EXECUTABLE = "/usr/bin/docker"
_FORMAL_NFT_EXECUTABLE = "/usr/sbin/nft"
_FORMAL_CONTROLLER_CONTEXT_SCHEMA = "txnmem-formal-controller-context-v1"
_FORMAL_APPROVAL_SCHEMA = "txnmem-formal-approved-source-v1"
_REQUIRED_SOURCE_PATHS = (
    "configs/provenance_performance_matrix.json",
    "configs/provenance_runtime_lock.json",
    "infra/real_backend/docker-compose.yml",
    "scripts/install_formal_provenance_runtime.sh",
    "scripts/run_cross_host_provenance_performance.sh",
    "scripts/run_provenance_performance.sh",
    "src/txnmem_experiment.py",
    "src/txnmem_formal_controller.py",
    "src/txnmem_formal_io.py",
    "src/txnmem_provenance_contract.py",
    "src/txnmem_provenance_execution_collector.py",
    "src/txnmem_provenance_performance.py",
    "src/txnmem_provenance_progress.py",
    "src/txnmem_provenance_runner.py",
    "src/txnmem_topology_attestation.py",
    "src/txnmem_vector_graph_backend.py",
)
_SOURCE_PATHS_FOR_TESTS: tuple[str, ...] | None = None
_MATERIAL_FIELDS = frozenset(
    {
        "schema",
        "candidate_bundle_id",
        "run_id_sha256",
        "config_sha256",
        "config_file_sha256",
        "workload_sha256",
        "environment_attestation_sha256",
        "evidence_manifest_sha256",
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
        "observed_service_versions",
        "candidate_operation_samples_sha256",
        "candidate_repetitions_sha256",
    }
)


class CollectorError(RuntimeError):
    """The collector could not establish a trustworthy execution boundary."""


class _MonitorCandidateExited(RuntimeError):
    """Internal signal that continuous sampling reached the child exit boundary."""


class _CollectorInterruption(RuntimeError):
    """The collector was asked to stop through its self-pipe signal latch."""


def _pidfd_primitives() -> tuple[Callable[[int, int], int], Callable[..., Any]]:
    if (
        not sys.platform.startswith("linux")
        or not callable(getattr(os, "pidfd_open", None))
        or not callable(getattr(signal, "pidfd_send_signal", None))
    ):
        raise CollectorError("formal pidfd support is unavailable")
    return os.pidfd_open, signal.pidfd_send_signal


def _require_pidfd_support() -> None:
    """Prove the running kernel accepts the exact formal pidfd contract."""

    descriptor: int | None = None
    primary_failure: BaseException | None = None
    close_failure: BaseException | None = None
    try:
        pidfd_open, pidfd_send_signal = _pidfd_primitives()
        descriptor = pidfd_open(os.getpid(), 0)
        if type(descriptor) is not int or descriptor < 0:
            descriptor = None
            raise CollectorError("formal pidfd probe returned an invalid descriptor")
        result = pidfd_send_signal(descriptor, 0, None, 0)
        if result is not None:
            raise CollectorError("formal pidfd signal probe returned an invalid result")
    except BaseException as exc:
        primary_failure = exc
    finally:
        if descriptor is not None:
            closing_descriptor = descriptor
            descriptor = None
            try:
                os.close(closing_descriptor)
            except BaseException as exc:
                close_failure = exc
    if primary_failure is not None:
        if isinstance(primary_failure, CollectorError):
            raise primary_failure
        raise CollectorError("formal pidfd syscall probe failed") from primary_failure
    if close_failure is not None:
        raise CollectorError("formal pidfd probe cleanup failed") from close_failure


def _pidfd_open(pid: int) -> int:
    pidfd_open, _pidfd_send = _pidfd_primitives()
    if type(pid) is not int or pid <= 0:
        raise CollectorError("formal pidfd target is invalid")
    descriptor = pidfd_open(pid, 0)
    if type(descriptor) is not int or descriptor < 0:
        raise CollectorError("formal pidfd open returned an invalid descriptor")
    return descriptor


def _pidfd_send_signal(descriptor: int, signal_number: int) -> None:
    _pidfd_open_call, pidfd_send_signal = _pidfd_primitives()
    if type(descriptor) is not int or descriptor < 0:
        raise CollectorError("formal pidfd descriptor is invalid")
    result = pidfd_send_signal(descriptor, signal_number, None, 0)
    if result is not None:
        raise CollectorError("formal pidfd signal returned an invalid result")


def _pidfd_close(descriptor: int) -> None:
    if type(descriptor) is not int or descriptor < 0:
        raise CollectorError("formal pidfd descriptor is invalid")
    os.close(descriptor)


def _formal_startup_fail_stop() -> None:
    """Trigger the protected supervisor/PDEATHSIG boundary without PID fallback."""

    os._exit(os.EX_SOFTWARE)
    raise CollectorError("formal startup fail-stop returned unexpectedly")


class _SignalLatch:
    """Latch SIGINT/SIGTERM state and wake select through a nonblocking pipe."""

    def __init__(self) -> None:
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._read_fd, False)
        os.set_blocking(self._write_fd, False)
        self._interrupted = False
        self._closed = False
        self._previous_handlers: dict[int, Any] = {}

    @property
    def read_fd(self) -> int:
        if self._closed:
            raise CollectorError("collector signal latch is closed")
        return self._read_fd

    @property
    def interrupted(self) -> bool:
        return self._interrupted

    def raise_if_interrupted(self) -> None:
        if self._interrupted:
            raise _CollectorInterruption("collector interruption requested")

    def trigger(self, _signal_number: int | None = None, _frame: Any = None) -> None:
        self._interrupted = True
        if self._closed:
            return
        try:
            os.write(self._write_fd, b"I")
        except BlockingIOError:
            pass
        except OSError:
            pass

    def install(self) -> None:
        if self._closed or self._previous_handlers:
            raise CollectorError("collector signal latch cannot be installed")
        for signal_number in (signal.SIGTERM, signal.SIGINT):
            self._previous_handlers[signal_number] = signal.signal(
                signal_number, self.trigger
            )

    def close(self) -> list[BaseException]:
        if self._closed:
            return []
        self._closed = True
        previous_handlers = tuple(self._previous_handlers.items())
        self._previous_handlers.clear()
        failures: list[BaseException] = []
        for signal_number, handler in previous_handlers:
            try:
                signal.signal(signal_number, handler)
            except BaseException as exc:
                failures.append(exc)
        for descriptor in (self._read_fd, self._write_fd):
            try:
                os.close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        return failures


_FINAL_PROGRESS_FIELDS = frozenset(
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
        "last_update_age_seconds",
    }
)
_FINAL_TERMINAL_PROGRESS_FIELDS = _FINAL_PROGRESS_FIELDS | {
    "terminal_reason_class"
}


def _expected_final_running_progress(
    progress_state: FormalProgressState,
    last_update_age_seconds: int,
) -> dict[str, Any]:
    return {
        "schema": PROGRESS_SNAPSHOT_SCHEMA,
        "run_binding_sha256": progress_state.run_binding_sha256,
        "config_sha256": progress_state.config_sha256,
        "phase": "measurement",
        "cell_index": 15,
        "cell_count": 15,
        "graph_size": 10000,
        "concurrency": 16,
        "repetition_index": 30,
        "repetition_count": 30,
        "completed_repetitions": 450,
        "total_repetitions": 450,
        "completed_samples": 14400,
        "total_samples": 14400,
        "update_sequence": 450,
        "status": "running",
        "last_update_age_seconds": last_update_age_seconds,
    }


def _exact_progress_mapping_matches(
    value: Any,
    expected: Mapping[str, Any],
    expected_fields: frozenset[str],
) -> bool:
    if not isinstance(value, Mapping):
        return False
    actual = dict(value)
    if set(actual) != expected_fields or set(expected) != expected_fields:
        return False
    age = actual["last_update_age_seconds"]
    if type(age) is not int or age < 0:
        return False
    return all(
        key == "last_update_age_seconds"
        or (
            type(actual[key]) is type(expected_value)
            and actual[key] == expected_value
        )
        for key, expected_value in expected.items()
    )


def _validate_final_running_progress(
    value: Any,
    progress_state: FormalProgressState,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CollectorError("candidate progress completion state is invalid")
    current = dict(value)
    age = current.get("last_update_age_seconds")
    expected = _expected_final_running_progress(progress_state, age)
    if not _exact_progress_mapping_matches(
        current, expected, _FINAL_PROGRESS_FIELDS
    ):
        raise CollectorError("candidate progress completion state is invalid")
    return current


def _completed_progress_matches(
    value: Any,
    expected: Mapping[str, Any],
) -> bool:
    if not isinstance(value, Mapping):
        return False
    return _exact_progress_mapping_matches(
        value, expected, _FINAL_TERMINAL_PROGRESS_FIELDS
    )


def _persist_blocked_progress(store: ProgressSnapshotStore) -> dict[str, Any]:
    """Persist one safe blocked terminal without exposing storage failures."""

    try:
        current = store.read_view()
    except BaseException:
        raise CollectorError("candidate progress blocking failed") from None
    if current.get("status") in {"blocked", "completed", "interrupted"}:
        return current
    try:
        store.write_terminal("blocked", "progress_protocol_failed")
    except BaseException:
        try:
            persisted = store.read_view()
        except BaseException:
            persisted = None
        if isinstance(persisted, Mapping) and persisted.get("status") == "blocked":
            return dict(persisted)
        raise CollectorError("candidate progress blocking failed") from None
    try:
        persisted = store.read_view()
    except BaseException:
        raise CollectorError("candidate progress blocking failed") from None
    if persisted.get("status") != "blocked":
        raise CollectorError("candidate progress blocking failed")
    return dict(persisted)


def _attempt_progress_blocker(
    progress_blocker: Callable[[], None] | None,
) -> None:
    """Attempt secondary progress cleanup without replacing a primary failure."""

    if progress_blocker is None:
        return
    try:
        progress_blocker()
    except BaseException:
        pass


def _complete_progress_terminal(
    progress_completer: Callable[[], Mapping[str, Any]],
    progress_blocker: Callable[[], None] | None,
) -> dict[str, Any]:
    """Complete progress while retaining the exact active primary failure."""

    try:
        terminal = progress_completer()
        if (
            not isinstance(terminal, Mapping)
            or terminal.get("status") != "completed"
        ):
            raise CollectorError("candidate progress terminal is invalid")
    except Exception:
        _attempt_progress_blocker(progress_blocker)
        raise
    except BaseException:
        _attempt_progress_blocker(progress_blocker)
        raise
    return dict(terminal)


@dataclass(frozen=True)
class _FormalChildSpec:
    command: tuple[str, ...]
    cwd: Path
    environment: dict[str, str]
    command_manifest: dict[str, Any]


@dataclass(frozen=True)
class _FormalRunWorkspace:
    root: Path
    candidate: Path
    root_device: int
    root_inode: int


@dataclass
class _GatedCandidate:
    process: subprocess.Popen[Any]
    _release_fd: int | None
    _receipt_fd: int | None
    ready_observed: bool
    _progress_drainer: ProgressPipeDrainer | None = None
    _progress_store: ProgressSnapshotStore | None = None
    _progress_state: FormalProgressState | None = None
    _bound_start_identity: str | None = None
    _formal_uid: int | None = None
    _leader_pidfd: int | None = None
    gate_released_monotonic_ns: int | None = None
    exit_observed_monotonic_ns: int | None = None

    def release(self) -> None:
        if self._release_fd is None:
            raise CollectorError("candidate launch gate was already released")
        descriptor = self._release_fd
        self._release_fd = None
        release_started = time.monotonic_ns()
        primary_failure: BaseException | None = None
        primary_traceback = None
        cleanup_failures: list[BaseException] = []
        try:
            written = os.write(descriptor, b"G")
            if written != 1:
                raise CollectorError("candidate launch gate write was incomplete")
            self.gate_released_monotonic_ns = release_started
        except BaseException as exc:
            primary_failure = exc
            primary_traceback = exc.__traceback__
            try:
                self.block_progress()
            except BaseException as cleanup_exc:
                cleanup_failures.append(cleanup_exc)
        try:
            os.close(descriptor)
        except BaseException as exc:
            cleanup_failures.append(exc)
        if primary_failure is not None:
            raise primary_failure.with_traceback(primary_traceback)
        if cleanup_failures:
            raise CollectorError("candidate launch gate cleanup failed") from None

    def close(self) -> None:
        failures: list[BaseException] = []
        if self._release_fd is not None:
            descriptor = self._release_fd
            self._release_fd = None
            try:
                os.close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        if self._receipt_fd is not None:
            descriptor = self._receipt_fd
            self._receipt_fd = None
            try:
                os.close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        if self._progress_drainer is not None:
            progress_drainer = self._progress_drainer
            self._progress_drainer = None
            try:
                progress_drainer.abort()
            except BaseException as exc:
                failures.append(exc)
        if self._leader_pidfd is not None:
            descriptor = self._leader_pidfd
            self._leader_pidfd = None
            try:
                _pidfd_close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise CollectorError("candidate descriptor cleanup failed") from None

    def finish_progress(self, timeout: float) -> dict[str, Any]:
        if self._progress_drainer is None:
            raise CollectorError("candidate progress channel is unavailable")
        try:
            snapshot = self._progress_drainer.finish(timeout)
        except ProgressProtocolError as exc:
            raise CollectorError("candidate progress channel failed") from exc
        if not isinstance(snapshot, dict):
            raise CollectorError("candidate progress snapshot is unavailable")
        return snapshot

    def complete_progress(self) -> dict[str, Any]:
        if self._progress_store is None:
            raise CollectorError("candidate progress store is unavailable")
        if not isinstance(self._progress_state, FormalProgressState):
            raise CollectorError("candidate progress completion state is invalid")
        try:
            current = self._progress_store.read_view()
        except BaseException:
            raise CollectorError("candidate progress completion failed") from None
        if current.get("status") == "completed":
            age = current.get("last_update_age_seconds")
            if type(age) is not int or age < 0:
                raise CollectorError(
                    "candidate progress completion state is invalid"
                )
            expected = _expected_final_running_progress(
                self._progress_state, age
            )
            expected["status"] = "completed"
            expected["terminal_reason_class"] = "completed"
            if not _completed_progress_matches(current, expected):
                raise CollectorError(
                    "candidate progress completion state is invalid"
                )
            return dict(current)
        running = _validate_final_running_progress(
            current, self._progress_state
        )
        terminal = dict(running)
        terminal["status"] = "completed"
        terminal["terminal_reason_class"] = "completed"
        terminal["last_update_age_seconds"] = 0
        try:
            self._progress_store.write_terminal("completed", "completed")
        except BaseException:
            try:
                persisted = self._progress_store.read_view()
            except BaseException:
                persisted = None
            if _completed_progress_matches(persisted, terminal):
                return dict(persisted)
            raise CollectorError("candidate progress completion failed") from None
        return terminal

    def block_progress(self) -> None:
        if self._progress_store is None:
            return
        _persist_blocked_progress(self._progress_store)

    def interrupt_progress(self) -> None:
        if self._progress_store is None:
            return
        try:
            current = self._progress_store.read_view()
            if current.get("status") in {
                "blocked",
                "completed",
                "interrupted",
            }:
                return
            self._progress_store.write_terminal(
                "interrupted", "collector_interrupted"
            )
            persisted = self._progress_store.read_view()
        except BaseException:
            raise CollectorError("candidate progress interruption failed") from None
        if persisted.get("status") != "interrupted":
            raise CollectorError("candidate progress interruption failed")

    def bind_process_identity(self, start_identity: str) -> None:
        expected_prefix = f"candidate:{self.process.pid}:"
        if (
            not isinstance(start_identity, str)
            or not start_identity.startswith(expected_prefix)
            or not start_identity[len(expected_prefix) :].isdigit()
        ):
            raise CollectorError("candidate process identity binding is invalid")
        if (
            self._bound_start_identity is not None
            and self._bound_start_identity != start_identity
        ):
            raise CollectorError("candidate process identity binding changed")
        self._bound_start_identity = start_identity

    def _validate_bound_group(self) -> None:
        if self._bound_start_identity is None:
            raise CollectorError("candidate process identity is not bound")
        observed = _read_process_group_identity(
            self.process.pid, self.process.args
        )
        if (
            not isinstance(observed, Mapping)
            or observed.get("pid") != self.process.pid
            or observed.get("start_identity") != self._bound_start_identity
            or observed.get("pgid") != self.process.pid
            or observed.get("sid") != self.process.pid
        ):
            raise CollectorError("candidate process identity changed")

    def _formal_group_members(self) -> dict[int, str]:
        if self._formal_uid is None:
            return {}
        group_members = _process_group_members(
            self.process.pid, self.process.pid
        )
        uid_processes = _formal_uid_processes(self._formal_uid)
        if group_members != uid_processes:
            raise CollectorError(
                "candidate process-group and UID quiescence identity changed"
            )
        return group_members

    def _require_formal_quiescence(self) -> None:
        if self._formal_uid is None:
            return
        try:
            leader_running = self.process.poll() is None
        except BaseException:
            raise CollectorError(
                "candidate process exit state is unavailable"
            ) from None
        members = self._formal_group_members()
        if members or leader_running:
            raise CollectorError(
                "candidate process-group and UID quiescence was not proven"
            )
        try:
            self.process.wait(timeout=0)
        except BaseException:
            raise CollectorError(
                "candidate process-group and UID quiescence was not proven"
            ) from None

    def require_quiescence(self) -> None:
        self._require_formal_quiescence()

    def _wait_for_formal_quiescence(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                leader_running = self.process.poll() is None
            except BaseException:
                raise CollectorError(
                    "candidate process exit state is unavailable"
                ) from None
            members = self._formal_group_members()
            if not members and not leader_running:
                self._require_formal_quiescence()
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                return False
            time.sleep(min(0.01, remaining))

    def _validate_surviving_formal_group(self) -> dict[int, str]:
        members = self._formal_group_members()
        if not members or self._bound_start_identity is None:
            raise CollectorError(
                "candidate surviving process-group identity is unavailable"
            )
        leader_start = self._bound_start_identity.rsplit(":", 1)[-1]
        if not leader_start.isdigit() or any(
            not start.isdigit() or int(start) < int(leader_start)
            for start in members.values()
        ):
            raise CollectorError(
                "candidate surviving process-group identity changed"
            )
        return members

    def _require_stable_formal_group_inventory(self) -> dict[int, str]:
        first = self._formal_group_members()
        second = self._formal_group_members()
        if first != second:
            raise CollectorError(
                "candidate surviving process-group identity changed"
            )
        return second

    def _signal_formal_inventory(
        self,
        inventory: Mapping[int, str],
        signal_number: int,
    ) -> None:
        """Bind every exact member to a pidfd before sending any signal."""

        expected = dict(inventory)
        if not expected or any(
            type(pid) is not int
            or pid <= 0
            or not isinstance(start_identity, str)
            or not start_identity.isdigit()
            for pid, start_identity in expected.items()
        ):
            raise CollectorError("candidate pidfd inventory is invalid")
        opened: list[tuple[int, int]] = []
        primary_failure: BaseException | None = None
        primary_traceback = None
        close_failures: list[BaseException] = []
        try:
            for pid in sorted(expected):
                try:
                    descriptor = _pidfd_open(pid)
                except OSError:
                    raise CollectorError(
                        "candidate pidfd binding failed"
                    ) from None
                opened.append((pid, descriptor))
            observed_after_open = self._formal_group_members()
            if observed_after_open != expected:
                raise CollectorError(
                    "candidate surviving process-group identity changed"
                )
            for _pid, descriptor in opened:
                try:
                    _pidfd_send_signal(descriptor, signal_number)
                except OSError as exc:
                    if exc.errno == errno.ESRCH:
                        continue
                    raise CollectorError(
                        "candidate pidfd signal failed"
                    ) from None
        except BaseException as exc:
            primary_failure = exc
            primary_traceback = exc.__traceback__
        finally:
            for _pid, descriptor in opened:
                try:
                    _pidfd_close(descriptor)
                except BaseException as exc:
                    close_failures.append(exc)
        if primary_failure is not None:
            raise primary_failure.with_traceback(primary_traceback)
        if close_failures:
            raise CollectorError("candidate pidfd cleanup failed") from None

    def terminate_validated_group(
        self, *, term_seconds: float = 5.0, kill_seconds: float = 5.0
    ) -> None:
        for value in (term_seconds, kill_seconds):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0.0
            ):
                raise CollectorError("candidate cleanup timeout is invalid")
        if self._formal_uid is not None:
            try:
                running = self.process.poll() is None
            except BaseException:
                raise CollectorError(
                    "candidate process exit state is unavailable"
                ) from None
            if running:
                self._validate_bound_group()
            initial_inventory = self._formal_group_members()
            if not initial_inventory:
                self._require_formal_quiescence()
                self.exit_observed_monotonic_ns = time.monotonic_ns()
                return
            leader_start = (
                self._bound_start_identity.rsplit(":", 1)[-1]
                if self._bound_start_identity is not None
                else ""
            )
            if (
                not leader_start.isdigit()
                or (
                    running
                    and initial_inventory.get(self.process.pid) != leader_start
                )
                or any(
                    not start.isdigit() or int(start) < int(leader_start)
                    for start in initial_inventory.values()
                )
            ):
                raise CollectorError(
                    "candidate surviving process-group identity changed"
                )
            self._signal_formal_inventory(initial_inventory, signal.SIGTERM)
            if self._wait_for_formal_quiescence(float(term_seconds)):
                self.exit_observed_monotonic_ns = time.monotonic_ns()
                return
            kill_deadline = time.monotonic() + float(kill_seconds)
            while True:
                survivors = self._validate_surviving_formal_group()
                self._signal_formal_inventory(survivors, signal.SIGKILL)
                remaining = kill_deadline - time.monotonic()
                if remaining <= 0.0:
                    break
                if self._wait_for_formal_quiescence(min(0.05, remaining)):
                    self.exit_observed_monotonic_ns = time.monotonic_ns()
                    return
            raise CollectorError(
                "candidate process-group and UID quiescence was not proven"
            )
        try:
            running = self.process.poll() is None
        except BaseException:
            raise CollectorError("candidate process exit state is unavailable") from None
        if not running:
            self.process.wait(timeout=0)
            if self.exit_observed_monotonic_ns is None:
                self.exit_observed_monotonic_ns = time.monotonic_ns()
            return
        self._validate_bound_group()
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except OSError:
            raise CollectorError("candidate process-group termination failed") from None
        try:
            self.process.wait(timeout=float(term_seconds))
        except subprocess.TimeoutExpired:
            self._validate_bound_group()
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except OSError:
                raise CollectorError("candidate process-group kill failed") from None
            try:
                self.process.wait(timeout=float(kill_seconds))
            except BaseException:
                raise CollectorError("candidate process group did not stop") from None
        except BaseException:
            raise CollectorError("candidate process-group termination failed") from None
        self.exit_observed_monotonic_ns = time.monotonic_ns()

    def wait_with_receipt(
        self,
        *,
        timeout: float | None = None,
        interrupt_latch: _SignalLatch | None = None,
        interrupt_fd: int | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Wait for exit while draining one bounded canonical completion receipt."""

        if self._receipt_fd is None:
            raise CollectorError("candidate completion receipt is unavailable")
        if interrupt_latch is not None and interrupt_fd is not None:
            raise CollectorError("candidate interruption channel is ambiguous")
        if interrupt_latch is not None:
            interrupt_latch.raise_if_interrupted()
            interrupt_fd = interrupt_latch.read_fd
        descriptor = self._receipt_fd
        self._receipt_fd = None
        deadline = None if timeout is None else time.monotonic() + timeout
        payload = bytearray()
        try:
            while True:
                if interrupt_latch is not None:
                    interrupt_latch.raise_if_interrupted()
                remaining = None
                if deadline is not None:
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining == 0.0:
                        raise subprocess.TimeoutExpired(
                            self.process.args, timeout
                        )
                descriptors = [descriptor]
                if interrupt_fd is not None:
                    descriptors.append(interrupt_fd)
                select_timeout = remaining
                if interrupt_latch is not None:
                    select_timeout = (
                        0.05 if remaining is None else min(0.05, remaining)
                    )
                try:
                    readable, _, _ = select.select(
                        descriptors, [], [], select_timeout
                    )
                except InterruptedError:
                    if interrupt_latch is not None:
                        interrupt_latch.raise_if_interrupted()
                    continue
                if not readable:
                    if interrupt_latch is not None:
                        interrupt_latch.raise_if_interrupted()
                        if deadline is None or time.monotonic() < deadline:
                            continue
                    raise subprocess.TimeoutExpired(self.process.args, timeout)
                if interrupt_fd is not None and interrupt_fd in readable:
                    try:
                        wakeup = os.read(interrupt_fd, 4096)
                    except BlockingIOError:
                        wakeup = None
                    except InterruptedError:
                        wakeup = None
                    if wakeup == b"":
                        raise CollectorError(
                            "collector signal latch channel closed"
                        )
                    if interrupt_latch is not None:
                        interrupt_latch.raise_if_interrupted()
                    else:
                        raise _CollectorInterruption(
                            "collector interruption requested"
                        )
                if descriptor not in readable:
                    continue
                if interrupt_latch is not None:
                    interrupt_latch.raise_if_interrupted()
                try:
                    chunk = os.read(descriptor, 65536 - len(payload) + 1)
                except InterruptedError:
                    continue
                if interrupt_latch is not None:
                    interrupt_latch.raise_if_interrupted()
                if not chunk:
                    break
                payload.extend(chunk)
                if len(payload) > 65536:
                    raise CollectorError("candidate completion receipt is oversized")
            while True:
                if interrupt_latch is not None:
                    interrupt_latch.raise_if_interrupted()
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise subprocess.TimeoutExpired(self.process.args, timeout)
                wait_timeout = remaining
                if interrupt_latch is not None:
                    wait_timeout = (
                        0.05 if remaining is None else min(0.05, remaining)
                    )
                try:
                    exit_code = self.process.wait(timeout=wait_timeout)
                    break
                except subprocess.TimeoutExpired:
                    if interrupt_latch is None:
                        raise
                    interrupt_latch.raise_if_interrupted()
                    if deadline is not None and time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired(self.process.args, timeout)
            if interrupt_latch is not None:
                interrupt_latch.raise_if_interrupted()
            self.exit_observed_monotonic_ns = time.monotonic_ns()
        finally:
            primary_failure = sys.exc_info()[1]
            try:
                os.close(descriptor)
            except BaseException:
                if primary_failure is None:
                    raise CollectorError(
                        "candidate receipt descriptor cleanup failed"
                    ) from None
        self._require_formal_quiescence()
        if exit_code != 0 and not payload:
            return exit_code, {}
        return exit_code, _decode_completion_receipt(bytes(payload))


def _decode_completion_receipt(payload: bytes) -> dict[str, Any]:
    if not payload or len(payload) > 65536:
        raise CollectorError("candidate completion receipt is unavailable")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CollectorError("candidate completion receipt has duplicate keys")
            result[key] = value
        return result

    try:
        document = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                CollectorError(
                    f"candidate completion receipt contains {value}"
                )
            ),
        )
    except CollectorError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CollectorError("candidate completion receipt is malformed") from exc
    if not isinstance(document, dict):
        raise CollectorError("candidate completion receipt must be a mapping")
    if canonical_json_bytes(document) != payload:
        raise CollectorError("candidate completion receipt is not canonical")
    return document


_MONITORED_INVARIANTS = (
    "backend_isolation",
    "continuous_load_ceiling",
    "host_environment",
    "network_guard",
    "runner_uid_process_set",
    "terminal_process_exit",
    "toxiproxy_routes",
)
_MONITOR_PROBE_FIELDS = frozenset(
    {
        "network_guard",
        "toxiproxy_routes",
        "backend_isolation",
        "runner_uid_processes",
        "host_environment",
        "load1_milli",
    }
)


def _normalize_execution_monitor_probe(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _MONITOR_PROBE_FIELDS:
        raise CollectorError("execution integrity monitor probe is malformed")
    try:
        guard = _validate_network_guard_attestation(value.get("network_guard"))
        backend = _validate_raw_backend_isolation(value.get("backend_isolation"))
    except ValueError as exc:
        raise CollectorError("execution integrity monitor probe is invalid") from exc
    routes = value.get("toxiproxy_routes")
    if not isinstance(routes, list) or len(routes) != 2:
        raise CollectorError("execution integrity monitor routes are incomplete")
    process_rows = value.get("runner_uid_processes")
    if not isinstance(process_rows, list) or len(process_rows) > 1:
        raise CollectorError("execution integrity monitor process set is invalid")
    normalized_processes: list[dict[str, Any]] = []
    for row in process_rows:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"pid", "start_identity"}
            or type(row.get("pid")) is not int
            or row["pid"] <= 0
            or not isinstance(row.get("start_identity"), str)
            or not row["start_identity"].isdigit()
        ):
            raise CollectorError("execution integrity monitor process set is invalid")
        normalized_processes.append(dict(row))
    host = value.get("host_environment")
    if (
        not isinstance(host, Mapping)
        or set(host)
        != {
            "host_identity_sha256",
            "cpu_logical_count",
            "memory_total_bytes",
            "disk_medium",
        }
        or not _SHA256.fullmatch(str(host.get("host_identity_sha256")))
        or type(host.get("cpu_logical_count")) is not int
        or host["cpu_logical_count"] <= 0
        or type(host.get("memory_total_bytes")) is not int
        or host["memory_total_bytes"] <= 0
        or host.get("disk_medium") not in {"nvme", "ssd", "hdd", "network-block"}
        or host["cpu_logical_count"] > _FORMAL_MAX_LOGICAL_CPU_COUNT
    ):
        raise CollectorError("execution integrity monitor host state is invalid")
    load = value.get("load1_milli")
    if type(load) is not int or load < 0:
        raise CollectorError("execution integrity monitor load is invalid")
    load_limit = (
        int(host["cpu_logical_count"]) * _FORMAL_MAX_LOAD1_PER_CPU_MILLI
    )
    if load > load_limit:
        raise CollectorError(
            "execution integrity monitor detected excessive host load"
        )
    normalized = {
        "network_guard": guard,
        "toxiproxy_routes": [dict(row) for row in routes],
        "backend_isolation": backend,
        "runner_uid_processes": normalized_processes,
        "host_environment": dict(host),
        "load1_milli": load,
    }
    try:
        canonical_json_bytes(normalized)
    except (TypeError, ValueError) as exc:
        raise CollectorError("execution integrity monitor probe is not canonical") from exc
    return normalized


class _ExecutionIntegrityMonitor:
    """Continuously hash invariant observations while the candidate is running."""

    def __init__(
        self,
        *,
        probe: Callable[[], Mapping[str, Any]],
        terminal_probe: Callable[[], Mapping[str, Any]] | None = None,
        interval_seconds: float = 0.25,
        maximum_gap_seconds: float = 2.0,
    ) -> None:
        if not callable(probe):
            raise CollectorError("execution integrity monitor probe is unavailable")
        for value, label in (
            (interval_seconds, "sampling interval"),
            (maximum_gap_seconds, "maximum gap"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0.0
            ):
                raise CollectorError(f"execution integrity monitor {label} is invalid")
        if float(maximum_gap_seconds) < float(interval_seconds):
            raise CollectorError("execution integrity monitor maximum gap is too small")
        self._probe = probe
        self._terminal_probe = terminal_probe or probe
        self._interval_seconds = float(interval_seconds)
        self._maximum_gap_ns = int(float(maximum_gap_seconds) * 1_000_000_000)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._rows: list[dict[str, Any]] = []
        self._baseline_sha256: str | None = None
        self._failure: BaseException | None = None

    def _record(
        self, probe: Callable[[], Mapping[str, Any]] | None = None
    ) -> None:
        observed = _normalize_execution_monitor_probe((probe or self._probe)())
        static = dict(observed)
        load = int(static.pop("load1_milli"))
        static.pop("runner_uid_processes")
        baseline_hash = hashlib.sha256(canonical_json_bytes(static)).hexdigest()
        with self._lock:
            if self._baseline_sha256 is None:
                self._baseline_sha256 = baseline_hash
            elif baseline_hash != self._baseline_sha256:
                raise CollectorError("execution integrity monitor detected invariant drift")
            row = {
                "ordinal": len(self._rows),
                "monotonic_ns": time.monotonic_ns(),
                "network_guard_sha256": hashlib.sha256(
                    canonical_json_bytes(observed["network_guard"])
                ).hexdigest(),
                "toxiproxy_routes_sha256": hashlib.sha256(
                    canonical_json_bytes(observed["toxiproxy_routes"])
                ).hexdigest(),
                "backend_isolation_sha256": hashlib.sha256(
                    canonical_json_bytes(observed["backend_isolation"])
                ).hexdigest(),
                "runner_uid_process_set_sha256": hashlib.sha256(
                    canonical_json_bytes(observed["runner_uid_processes"])
                ).hexdigest(),
                "runner_uid_process_count": len(
                    observed["runner_uid_processes"]
                ),
                "host_environment_sha256": hashlib.sha256(
                    canonical_json_bytes(observed["host_environment"])
                ).hexdigest(),
                "cpu_logical_count": int(
                    observed["host_environment"]["cpu_logical_count"]
                ),
                "load1_milli": load,
            }
            self._rows.append(row)

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._interval_seconds):
                self._record()
        except _MonitorCandidateExited:
            self._stop.set()
        except BaseException as exc:
            with self._lock:
                self._failure = exc
            self._stop.set()

    def start(self) -> None:
        if self._thread is not None:
            raise CollectorError("execution integrity monitor was already started")
        self._record()
        self._thread = threading.Thread(
            target=self._run,
            name="txnmem-execution-integrity-monitor",
            daemon=True,
        )
        self._thread.start()

    def abort(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval_seconds * 4.0))

    def finalize(
        self,
        *,
        gate_release_monotonic_ns: int,
        child_exit_monotonic_ns: int,
    ) -> dict[str, Any]:
        self.abort()
        if self._thread is None or self._thread.is_alive():
            raise CollectorError("execution integrity monitor did not stop")
        try:
            self._record(self._terminal_probe)
        except BaseException as exc:
            with self._lock:
                if self._failure is None:
                    self._failure = exc
        with self._lock:
            failure = self._failure
            rows = [dict(row) for row in self._rows]
        if failure is not None:
            raise CollectorError("execution integrity monitor failed") from failure
        if len(rows) < 2:
            raise CollectorError("execution integrity monitor coverage is insufficient")
        if (
            type(gate_release_monotonic_ns) is not int
            or type(child_exit_monotonic_ns) is not int
            or gate_release_monotonic_ns <= 0
            or child_exit_monotonic_ns <= gate_release_monotonic_ns
            or rows[0]["monotonic_ns"] > gate_release_monotonic_ns
            or child_exit_monotonic_ns > rows[-1]["monotonic_ns"]
            or rows[-1]["runner_uid_process_count"] != 0
        ):
            raise CollectorError("execution integrity monitor boundary is invalid")
        gaps = [
            int(second["monotonic_ns"]) - int(first["monotonic_ns"])
            for first, second in zip(rows, rows[1:])
        ]
        if any(gap <= 0 for gap in gaps) or max(gaps) > self._maximum_gap_ns:
            raise CollectorError("execution integrity monitor sampling gap is invalid")
        if (
            gate_release_monotonic_ns - int(rows[0]["monotonic_ns"])
            > self._maximum_gap_ns
            or int(rows[-1]["monotonic_ns"]) - child_exit_monotonic_ns
            > self._maximum_gap_ns
        ):
            raise CollectorError(
                "execution integrity monitor boundary coverage is invalid"
            )
        cpu_count = int(rows[-1]["cpu_logical_count"])
        load_limit = cpu_count * _FORMAL_MAX_LOAD1_PER_CPU_MILLI
        return {
            "schema": "txnmem-provenance-execution-monitor-v2",
            "sampling_interval_ms": int(round(self._interval_seconds * 1000.0)),
            "sample_count": len(rows),
            "first_sample_monotonic_ns": int(rows[0]["monotonic_ns"]),
            "last_sample_monotonic_ns": int(rows[-1]["monotonic_ns"]),
            "gate_release_monotonic_ns": gate_release_monotonic_ns,
            "child_exit_monotonic_ns": child_exit_monotonic_ns,
            "max_observed_gap_ns": max(gaps),
            "violation_count": 0,
            "cpu_logical_count": cpu_count,
            "load1_limit_milli": load_limit,
            "max_load1_milli": max(int(row["load1_milli"]) for row in rows),
            "invariants": list(_MONITORED_INVARIANTS),
            "samples_sha256": hashlib.sha256(
                canonical_json_bytes(rows)
            ).hexdigest(),
            "first_sample_sha256": hashlib.sha256(
                canonical_json_bytes(rows[0])
            ).hexdigest(),
            "last_sample_sha256": hashlib.sha256(
                canonical_json_bytes(rows[-1])
            ).hexdigest(),
        }


def _file_sha256(path: Path, field: str) -> str:
    if path.is_symlink() or not path.is_file():
        raise CollectorError(f"{field} must be a real file")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise CollectorError(f"cannot hash {field}") from exc
    return digest.hexdigest()


def _attest_external_executable(
    path: Path,
    *,
    role: str,
    expected_uid: int = 0,
    require_protected_parents: bool = True,
) -> dict[str, Any]:
    """Bind an absolute executable to protected ownership and exact bytes."""

    if role not in {"python", "git", "docker", "nft"}:
        raise CollectorError("formal external executable role is unsupported")
    requested = path.expanduser().absolute()
    if not requested.exists() or not (requested.is_file() or requested.is_symlink()):
        raise CollectorError(f"formal {role} executable is unavailable")
    try:
        requested_metadata = requested.lstat()
        resolved = requested.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as exc:
        raise CollectorError(f"formal {role} executable is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or requested_metadata.st_uid != expected_uid
    ):
        raise CollectorError(f"formal {role} executable owner is invalid")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & 0o022:
        raise CollectorError(f"formal {role} executable is group/world writable")
    if not mode & 0o111:
        raise CollectorError(f"formal {role} executable is not executable")
    if require_protected_parents:
        parent = resolved.parent
        while True:
            parent_metadata = parent.stat()
            if (
                parent_metadata.st_uid != expected_uid
                or stat.S_IMODE(parent_metadata.st_mode) & 0o022
            ):
                raise CollectorError(
                    f"formal {role} executable parent is not protected"
                )
            if parent == parent.parent:
                break
            parent = parent.parent
    return {
        "role": role,
        "requested_path_sha256": hashlib.sha256(
            str(requested).encode("utf-8")
        ).hexdigest(),
        "resolved_path_sha256": hashlib.sha256(
            str(resolved).encode("utf-8")
        ).hexdigest(),
        "executable_sha256": _file_sha256(
            resolved, f"formal {role} executable"
        ),
        "owner_uid": int(metadata.st_uid),
        "mode": mode,
    }


def _attest_formal_external_tools(
    python_executable: Path,
) -> list[dict[str, Any]]:
    return [
        _attest_external_executable(
            executable, role=role, expected_uid=0, require_protected_parents=True
        )
        for role, executable in (
            ("docker", Path(_FORMAL_DOCKER_EXECUTABLE)),
            ("git", Path(_FORMAL_GIT_EXECUTABLE)),
            ("nft", Path(_FORMAL_NFT_EXECUTABLE)),
            ("python", python_executable),
        )
    ]


def _formal_endpoint_port(
    value: str,
    *,
    field: str,
    schemes: set[str],
    expected_port: int,
    expected_hosts: set[str] | None = None,
) -> int:
    try:
        parsed = urllib.parse.urlparse(value)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise CollectorError(f"{field} is malformed") from exc
    if (
        parsed.scheme not in schemes
        or not parsed.hostname
        or (
            expected_hosts is not None
            and parsed.hostname.lower() not in expected_hosts
        )
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or port != expected_port
    ):
        raise CollectorError(f"{field} must use the dedicated Toxiproxy port")
    return expected_port


def _runtime_python_identity(executable_hash: str) -> dict[str, str]:
    return {
        "implementation": platform.python_implementation(),
        "version": platform.python_version(),
        "executable_sha256": _validate_hash(
            executable_hash, "Python executable"
        ),
        "build_sha256": hashlib.sha256(
            "|".join(platform.python_build()).encode("utf-8")
        ).hexdigest(),
        "compiler_sha256": hashlib.sha256(
            platform.python_compiler().encode("utf-8")
        ).hexdigest(),
        "platform_sha256": hashlib.sha256(
            platform.platform().encode("utf-8")
        ).hexdigest(),
    }


def _read_regular_file_bytes(path: Path, field: str) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise CollectorError(f"{field} must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = None
            return stream.read()
    except CollectorError:
        raise
    except OSError as exc:
        raise CollectorError(f"{field} is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def verify_immutable_runtime_snapshot(
    snapshot_path: Path,
    runtime_manifest: Mapping[str, Any],
    *,
    directory_mode: int = 0o500,
    file_mode: int = 0o400,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> dict[str, Any]:
    """Re-attest a closed runtime snapshot before and after candidate execution."""

    try:
        manifest = _validate_runtime_manifest(runtime_manifest)
    except ValueError as exc:
        raise CollectorError("runtime snapshot manifest is invalid") from exc
    snapshot = snapshot_path.expanduser().absolute()
    if snapshot.is_symlink() or not snapshot.is_dir():
        raise CollectorError("runtime snapshot root is invalid")
    snapshot = snapshot.resolve(strict=True)
    root_metadata = snapshot.stat()
    if stat.S_IMODE(root_metadata.st_mode) != directory_mode:
        raise CollectorError("runtime snapshot root is not read-only")
    if expected_uid is not None and root_metadata.st_uid != expected_uid:
        raise CollectorError("runtime snapshot owner is invalid")
    if expected_gid is not None and root_metadata.st_gid != expected_gid:
        raise CollectorError("runtime snapshot group is invalid")
    expected: dict[str, str] = {}
    for distribution in manifest["distributions"]:
        for row in distribution["files"]:
            previous = expected.get(row["path"])
            if previous is not None and previous != row["sha256"]:
                raise CollectorError("runtime snapshot manifest path collision")
            expected[row["path"]] = row["sha256"]
    observed: set[str] = set()
    for path in snapshot.rglob("*"):
        if path.is_symlink():
            raise CollectorError("runtime snapshot contains a link")
        relative = path.relative_to(snapshot).as_posix()
        metadata = path.stat()
        mode = stat.S_IMODE(metadata.st_mode)
        if expected_uid is not None and metadata.st_uid != expected_uid:
            raise CollectorError("runtime snapshot owner changed")
        if expected_gid is not None and metadata.st_gid != expected_gid:
            raise CollectorError("runtime snapshot group changed")
        if path.is_dir():
            if mode != directory_mode:
                raise CollectorError("runtime snapshot directory is not read-only")
            continue
        if not path.is_file() or mode != file_mode:
            raise CollectorError("runtime snapshot file is not read-only")
        if relative not in expected:
            raise CollectorError("runtime snapshot contains an unregistered file")
        if _file_sha256(path, "runtime snapshot file") != expected[relative]:
            raise CollectorError("runtime snapshot file hash mismatch")
        observed.add(relative)
    if observed != set(expected):
        raise CollectorError("runtime snapshot is incomplete")
    return manifest


@contextlib.contextmanager
def _locked_neo4j_graph_database(runtime_snapshot: Path):
    """Load the root probe's Neo4j driver only from the attested wheel snapshot."""

    root = runtime_snapshot.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("locked Neo4j runtime is unavailable")
    root = root.resolve(strict=True)
    prefixes = ("neo4j", "pytz")
    if any(
        name == prefix or name.startswith(prefix + ".")
        for name in sys.modules
        for prefix in prefixes
    ):
        raise CollectorError("Neo4j runtime was already imported before attestation")
    original_modules = set(sys.modules)
    sys.path.insert(0, str(root))
    integrity_failure: CollectorError | None = None
    try:
        importlib.invalidate_caches()
        module = importlib.import_module("neo4j")
        module_file = Path(str(getattr(module, "__file__", ""))).resolve()
        if not _is_within(module_file, root):
            raise CollectorError("Neo4j driver escaped the locked runtime")
        graph_database = getattr(module, "GraphDatabase", None)
        if graph_database is None:
            raise CollectorError("locked Neo4j driver has no GraphDatabase")
        yield graph_database
    finally:
        for name, module in list(sys.modules.items()):
            if name in original_modules or not any(
                name == prefix or name.startswith(prefix + ".")
                for prefix in prefixes
            ):
                continue
            module_file = getattr(module, "__file__", None)
            if module_file is None or not _is_within(
                Path(str(module_file)).resolve(), root
            ):
                integrity_failure = CollectorError(
                    "Neo4j dependency escaped the locked runtime"
                )
            sys.modules.pop(name, None)
        if sys.path and sys.path[0] == str(root):
            sys.path.pop(0)
        else:
            try:
                sys.path.remove(str(root))
            except ValueError:
                integrity_failure = CollectorError(
                    "locked Neo4j runtime path changed during observation"
                )
        importlib.invalidate_caches()
        if integrity_failure is not None and sys.exc_info()[0] is None:
            raise integrity_failure


def _load_runtime_lock(lock_path: Path) -> tuple[dict[str, Any], str]:
    document, raw = load_strict_json_document(lock_path)
    if (
        not isinstance(document, Mapping)
        or set(document) != {"schema", "python_versions", "distributions"}
        or document.get("schema") != "txnmem-provenance-runtime-lock-v1"
    ):
        raise CollectorError("runtime lock schema is invalid")
    versions = document.get("python_versions")
    if (
        not isinstance(versions, list)
        or not versions
        or any(
            not isinstance(version, str)
            or not re.fullmatch(r"[0-9]+(?:\.[0-9]+){2}", version)
            for version in versions
        )
        or versions
        != sorted(
            set(versions), key=lambda value: tuple(int(part) for part in value.split("."))
        )
        or platform.python_version() not in versions
    ):
        raise CollectorError("runtime lock Python versions are invalid")
    rows = document.get("distributions")
    if not isinstance(rows, list) or not rows:
        raise CollectorError("runtime lock distributions are invalid")
    normalized: list[dict[str, Any]] = []
    names: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {
            "name",
            "version",
            "filename",
            "sha256",
            "dependency_names",
            "requires_dist",
        }:
            raise CollectorError("runtime lock distribution fields are invalid")
        name = row.get("name")
        version = row.get("version")
        filename = row.get("filename")
        dependencies = row.get("dependency_names")
        requirements = row.get("requires_dist")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,127}", name)
            or not isinstance(version, str)
            or not re.fullmatch(
                r"[0-9]+(?:\.[0-9]+)+(?:[A-Za-z0-9._+-]*)?", version
            )
            or not isinstance(filename, str)
            or Path(filename).name != filename
            or not filename.endswith(".whl")
            or not isinstance(dependencies, list)
            or dependencies != sorted(set(dependencies))
            or any(
                not isinstance(item, str)
                or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,127}", item)
                for item in dependencies
            )
            or not isinstance(requirements, list)
            or requirements != sorted(set(requirements))
            or any(
                not isinstance(item, str)
                or not item
                or len(item) > 512
                or any(ord(char) < 32 for char in item)
                for item in requirements
            )
        ):
            raise CollectorError("runtime lock distribution identity is invalid")
        normalized.append(
            {
                "name": name,
                "version": version,
                "filename": filename,
                "sha256": _validate_hash(row.get("sha256"), "runtime wheel"),
                "dependency_names": list(dependencies),
                "requires_dist": list(requirements),
            }
        )
        names.append(name)
    if names != sorted(set(names)) or names != ["neo4j", "pytz"]:
        raise CollectorError("runtime lock dependency set is not registered")
    name_set = set(names)
    if any(
        not set(row["dependency_names"]).issubset(name_set)
        for row in normalized
    ):
        raise CollectorError("runtime lock dependency closure is incomplete")
    if (
        normalized[0]["dependency_names"] != ["pytz"]
        or normalized[1]["dependency_names"] != []
    ):
        raise CollectorError("runtime lock dependency graph is not registered")
    return (
        {
            "schema": "txnmem-provenance-runtime-lock-v1",
            "python_versions": list(versions),
            "distributions": normalized,
        },
        hashlib.sha256(raw).hexdigest(),
    )


def _create_locked_runtime_snapshot(
    private_parent: Path,
    *,
    lock_path: Path,
    wheel_directory: Path,
    python_executable_hash: str,
    require_protected_wheels: bool,
) -> tuple[Path, dict[str, Any]]:
    """Build the runtime only from source-registered, byte-pinned wheels."""

    parent = _private_directory(private_parent, "runtime snapshot")
    lock, _lock_file_hash = _load_runtime_lock(lock_path)
    wheel_root = wheel_directory.expanduser().absolute()
    if wheel_root.is_symlink() or not wheel_root.is_dir():
        raise CollectorError("runtime wheel directory is unavailable")
    wheel_root = wheel_root.resolve(strict=True)
    wheel_root_stat = wheel_root.stat()
    if require_protected_wheels and (
        wheel_root_stat.st_uid != 0
        or stat.S_IMODE(wheel_root_stat.st_mode) & 0o022
    ):
        raise CollectorError("runtime wheel directory is not root protected")
    expected_filenames = {
        row["filename"] for row in lock["distributions"]
    }
    observed_entries = {path.name for path in wheel_root.iterdir()}
    if observed_entries != expected_filenames:
        raise CollectorError("runtime wheel directory contains an unregistered wheel")

    snapshot = parent / f"runtime-{secrets.token_hex(16)}"
    directories: set[Path] = {snapshot}
    written_hashes: dict[str, str] = {}
    manifests: list[dict[str, Any]] = []
    try:
        snapshot.mkdir(mode=0o700)
        for locked in lock["distributions"]:
            wheel_path = wheel_root / locked["filename"]
            if wheel_path.is_symlink() or not wheel_path.is_file():
                raise CollectorError("registered runtime wheel is unavailable")
            wheel_stat = wheel_path.stat()
            if require_protected_wheels and (
                wheel_stat.st_uid != 0
                or stat.S_IMODE(wheel_stat.st_mode) & 0o022
            ):
                raise CollectorError("registered runtime wheel is not root protected")
            wheel_payload = _read_regular_file_bytes(
                wheel_path, "registered runtime wheel"
            )
            if hashlib.sha256(wheel_payload).hexdigest() != locked["sha256"]:
                raise CollectorError("registered runtime wheel hash mismatch")
            distribution_files: list[dict[str, str]] = []
            metadata_payloads: list[bytes] = []
            try:
                archive = zipfile.ZipFile(io.BytesIO(wheel_payload), "r")
            except (OSError, zipfile.BadZipFile) as exc:
                raise CollectorError("registered runtime wheel is malformed") from exc
            with archive:
                for member in sorted(archive.infolist(), key=lambda item: item.filename):
                    raw_path = member.filename.replace("\\", "/")
                    parts = Path(raw_path).parts
                    unix_mode = (member.external_attr >> 16) & 0o170000
                    if (
                        not raw_path
                        or raw_path.startswith("/")
                        or ".." in parts
                        or unix_mode == stat.S_IFLNK
                    ):
                        raise CollectorError("registered runtime wheel path is unsafe")
                    if member.is_dir():
                        continue
                    try:
                        payload = archive.read(member)
                    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
                        raise CollectorError(
                            "registered runtime wheel member is unreadable"
                        ) from exc
                    digest = hashlib.sha256(payload).hexdigest()
                    previous = written_hashes.get(raw_path)
                    if previous is not None and previous != digest:
                        raise CollectorError("runtime snapshot path collision")
                    target = snapshot / raw_path
                    if previous is None:
                        target.parent.mkdir(
                            parents=True, exist_ok=True, mode=0o700
                        )
                        directories.update(
                            directory
                            for directory in target.parents
                            if directory == snapshot
                            or _is_within(directory, snapshot)
                        )
                        descriptor = os.open(
                            target,
                            os.O_WRONLY
                            | os.O_CREAT
                            | os.O_EXCL
                            | getattr(os, "O_NOFOLLOW", 0),
                            0o400,
                        )
                        with os.fdopen(descriptor, "wb") as stream:
                            stream.write(payload)
                            stream.flush()
                            os.fsync(stream.fileno())
                        written_hashes[raw_path] = digest
                    distribution_files.append(
                        {"path": raw_path, "sha256": digest}
                    )
                    if raw_path.endswith(".dist-info/METADATA"):
                        metadata_payloads.append(payload)
            if len(metadata_payloads) != 1:
                raise CollectorError("registered runtime wheel metadata is ambiguous")
            metadata = BytesParser(policy=email_policy.default).parsebytes(
                metadata_payloads[0]
            )
            metadata_name = re.sub(
                r"[-_.]+", "-", str(metadata.get("Name", ""))
            ).lower()
            metadata_version = str(metadata.get("Version", ""))
            metadata_requirements = sorted(metadata.get_all("Requires-Dist", []))
            if (
                metadata_name != locked["name"]
                or metadata_version != locked["version"]
                or metadata_requirements != locked["requires_dist"]
            ):
                raise CollectorError("registered runtime wheel metadata mismatch")
            distribution_files.sort(key=lambda row: row["path"])
            manifests.append(
                {
                    "name": locked["name"],
                    "version": locked["version"],
                    "files": distribution_files,
                    "files_sha256": hashlib.sha256(
                        canonical_json_bytes(distribution_files)
                    ).hexdigest(),
                    "declared_requirements_sha256": hashlib.sha256(
                        canonical_json_bytes(locked["requires_dist"])
                    ).hexdigest(),
                }
            )
        runtime = {
            "schema": "txnmem-provenance-runtime-manifest-v1",
            "python": _runtime_python_identity(python_executable_hash),
            "distributions": manifests,
        }
        try:
            validated = _validate_runtime_manifest(runtime)
        except ValueError as exc:
            raise CollectorError("runtime snapshot manifest is invalid") from exc
        for directory in sorted(
            directories, key=lambda path: len(path.parts), reverse=True
        ):
            directory.chmod(0o500)
        verify_immutable_runtime_snapshot(snapshot, validated)
        return snapshot, validated
    except BaseException:
        if snapshot.exists():
            for path in sorted(
                snapshot.rglob("*"), key=lambda item: len(item.parts), reverse=True
            ):
                if path.is_symlink():
                    continue
                path.chmod(0o700 if path.is_dir() else 0o600)
            snapshot.chmod(0o700)
            shutil.rmtree(snapshot)
        raise


def _build_formal_child_spec(
    *,
    source_export: Path,
    runtime_snapshot: Path,
    runtime_manifest: Mapping[str, Any],
    candidate_root: Path,
    environment_attestation_path: Path,
    run_id: str,
    transport: str,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    neo4j_user: str,
    neo4j_password: str,
    source_manifest_sha256: str,
    runner_sha256: str,
    config_file_sha256: str,
    environment_attestation_sha256: str,
    external_tools: Sequence[Mapping[str, Any]],
    runtime_directory_mode: int = 0o500,
    runtime_file_mode: int = 0o400,
    runtime_owner_uid: int | None = None,
    runtime_owner_gid: int | None = None,
) -> _FormalChildSpec:
    """Construct the one closed command allowed to produce a formal candidate."""

    export = source_export.expanduser().absolute().resolve(strict=True)
    runtime_site = runtime_snapshot.expanduser().absolute().resolve(strict=True)
    candidate = candidate_root.expanduser().absolute().resolve(strict=True)
    environment_path = (
        environment_attestation_path.expanduser().absolute().resolve(strict=True)
    )
    if not isinstance(run_id, str) or not run_id:
        raise CollectorError("run_id must be non-empty")
    if transport == "local_loopback":
        endpoint_hosts = {"127.0.0.1"}
    elif transport == "container_bridge":
        endpoint_hosts = {"txnmem-toxiproxy"}
    else:
        raise CollectorError("formal transport is unsupported")
    for value, label in (
        (qdrant_url, "Qdrant endpoint"),
        (neo4j_uri, "Neo4j endpoint"),
        (toxiproxy_url, "Toxiproxy endpoint"),
        (neo4j_user, "Neo4j user"),
        (neo4j_password, "Neo4j credential"),
    ):
        if not isinstance(value, str) or not value:
            raise CollectorError(f"{label} is unavailable")
    qdrant_port = _formal_endpoint_port(
        qdrant_url,
        field="Qdrant Toxiproxy endpoint",
        schemes={"http"},
        expected_port=19000,
        expected_hosts=endpoint_hosts,
    )
    neo4j_port = _formal_endpoint_port(
        neo4j_uri,
        field="Neo4j Toxiproxy endpoint",
        schemes={"bolt"},
        expected_port=19001,
        expected_hosts=endpoint_hosts,
    )
    _formal_endpoint_port(
        toxiproxy_url,
        field="Toxiproxy management endpoint",
        schemes={"http"},
        expected_port=8474,
        expected_hosts=endpoint_hosts,
    )
    source_hash = _validate_hash(source_manifest_sha256, "source manifest")
    expected_runner_hash = _validate_hash(runner_sha256, "runner source")
    expected_config_hash = _validate_hash(config_file_sha256, "config file")
    environment_hash = _validate_hash(
        environment_attestation_sha256, "environment attestation"
    )
    try:
        verified_external_tools = _validate_external_tools(list(external_tools))
    except ValueError as exc:
        raise CollectorError("formal external tool closure is invalid") from exc
    environment_document, environment_raw = load_strict_json_document(
        environment_path
    )
    if (
        not isinstance(environment_document, Mapping)
        or _environment_hash(environment_document) != environment_hash
    ):
        raise CollectorError("environment attestation hash mismatch")
    environment_file_hash = hashlib.sha256(environment_raw).hexdigest()
    runner = export / "src" / "txnmem_provenance_runner.py"
    config = export / "configs" / "provenance_performance_matrix.json"
    runtime_lock = export / "configs" / "provenance_runtime_lock.json"
    if _file_sha256(runner, "immutable runner") != expected_runner_hash:
        raise CollectorError("immutable runner hash mismatch")
    if _file_sha256(config, "immutable config") != expected_config_hash:
        raise CollectorError("immutable config hash mismatch")
    _runtime_lock_document, runtime_lock_file_hash = _load_runtime_lock(
        runtime_lock
    )
    python_executable = Path(sys.executable).expanduser().absolute()
    python_hash = _file_sha256(
        python_executable.resolve(strict=True), "Python executable"
    )
    verified_runtime = verify_immutable_runtime_snapshot(
        runtime_site,
        runtime_manifest,
        directory_mode=runtime_directory_mode,
        file_mode=runtime_file_mode,
        expected_uid=runtime_owner_uid,
        expected_gid=runtime_owner_gid,
    )
    if verified_runtime["python"]["executable_sha256"] != python_hash:
        raise CollectorError("runtime snapshot Python executable mismatch")
    command = (
        sys.executable,
        "-I",
        "-S",
        "-B",
        str(runner),
        "provenance-performance",
        "--backend",
        "vector-graph",
        "--config",
        str(config),
        "--run-id",
        run_id,
        "--out-dir",
        str(candidate),
        "--service-url",
        qdrant_url,
        "--environment-attestation",
        str(environment_path),
    )
    environment = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TXNMEM_NEO4J_URI": neo4j_uri,
        "TXNMEM_NEO4J_USER": neo4j_user,
        "TXNMEM_NEO4J_PASSWORD": neo4j_password,
        "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime_site),
    }
    argv_hash = hashlib.sha256("\0".join(command).encode("utf-8")).hexdigest()
    progress_binding_document = {
        "schema": "txnmem-provenance-progress-binding-v1",
        "source_manifest_sha256": source_hash,
        "argv_sha256": argv_hash,
        "config_file_sha256": expected_config_hash,
        "run_id_sha256": hashlib.sha256(run_id.encode("utf-8")).hexdigest(),
        "candidate_root_sha256": hashlib.sha256(
            str(candidate).encode("utf-8")
        ).hexdigest(),
    }
    progress_binding_hash = hashlib.sha256(
        canonical_json_bytes(progress_binding_document)
    ).hexdigest()
    config_document, _config_raw = load_strict_json_document(config)
    validated_config = validate_matrix_config(config_document, formal=True)
    request_timeout = float(validated_config["request_timeout_seconds"])
    command_manifest = {
        "schema": "txnmem-provenance-command-manifest-v3",
        "transport": transport,
        "argv_sha256": argv_hash,
        "argv_template": [
            "<python-executable>",
            "-I",
            "-S",
            "-B",
            "<immutable-source>/src/txnmem_provenance_runner.py",
            "provenance-performance",
            "--backend",
            "vector-graph",
            "--config",
            "<immutable-source>/configs/provenance_performance_matrix.json",
            "--run-id",
            "<run-id>",
            "--out-dir",
            "<candidate-root>",
            "--service-url",
            "<qdrant-endpoint>",
            "--environment-attestation",
            "<environment-attestation>",
        ],
        "python_executable_path_sha256": hashlib.sha256(
            str(python_executable).encode("utf-8")
        ).hexdigest(),
        "python_executable_sha256": python_hash,
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "runtime_manifest": verified_runtime,
        "runtime_manifest_sha256": hashlib.sha256(
            canonical_json_bytes(verified_runtime)
        ).hexdigest(),
        "runtime_lock_file_sha256": runtime_lock_file_hash,
        "runtime_snapshot_path_sha256": hashlib.sha256(
            str(runtime_site).encode("utf-8")
        ).hexdigest(),
        "external_tools": verified_external_tools,
        "working_directory_sha256": hashlib.sha256(
            str(export).encode("utf-8")
        ).hexdigest(),
        "source_manifest_sha256": source_hash,
        "runner_sha256": expected_runner_hash,
        "config_file_sha256": expected_config_hash,
        "run_id_sha256": hashlib.sha256(run_id.encode("utf-8")).hexdigest(),
        "candidate_root_sha256": hashlib.sha256(
            str(candidate).encode("utf-8")
        ).hexdigest(),
        "environment_attestation_sha256": environment_hash,
        "environment_attestation_file_sha256": environment_file_hash,
        "qdrant_endpoint_sha256": hashlib.sha256(
            qdrant_url.encode("utf-8")
        ).hexdigest(),
        "qdrant_endpoint_port": qdrant_port,
        "neo4j_endpoint_sha256": hashlib.sha256(
            neo4j_uri.encode("utf-8")
        ).hexdigest(),
        "neo4j_endpoint_port": neo4j_port,
        "toxiproxy_endpoint_sha256": hashlib.sha256(
            toxiproxy_url.encode("utf-8")
        ).hexdigest(),
        "literal_environment": {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        "hashed_environment": {
            "TXNMEM_NEO4J_URI": hashlib.sha256(
                neo4j_uri.encode("utf-8")
            ).hexdigest(),
            "TXNMEM_NEO4J_USER": hashlib.sha256(
                neo4j_user.encode("utf-8")
            ).hexdigest(),
            "TXNMEM_PROVENANCE_RUNTIME_SITE": hashlib.sha256(
                str(runtime_site).encode("utf-8")
            ).hexdigest(),
        },
        "secret_environment_variables": ["TXNMEM_NEO4J_PASSWORD"],
        "gate_environment_variable": "TXNMEM_PROVENANCE_START_GATE_FD",
        "ready_environment_variable": "TXNMEM_PROVENANCE_READY_FD",
        "completion_environment_variable": "TXNMEM_PROVENANCE_COMPLETION_FD",
        "completion_receipt_required": True,
        "progress_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_FD",
        "progress_binding_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256",
        "progress_binding_sha256": progress_binding_hash,
        "progress_channel_required": True,
        "backend_timeout_policy": {
            "qdrant_request_seconds": request_timeout,
            "neo4j_connection_seconds": request_timeout,
            "neo4j_connection_acquisition_seconds": request_timeout,
            "neo4j_transaction_query_seconds": request_timeout,
        },
        "runtime_environment_variable": "TXNMEM_PROVENANCE_RUNTIME_SITE",
        "inherited_environment": False,
    }
    return _FormalChildSpec(
        command=command,
        cwd=export,
        environment=environment,
        command_manifest=command_manifest,
    )


def _start_gated_candidate(
    *,
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str],
    formal_uid: int | None = None,
    formal_gid: int | None = None,
    require_completion_receipt: bool = False,
    require_progress: bool = False,
    progress_binding_sha256: str | None = None,
    progress_config_sha256: str | None = None,
    progress_snapshot_path: Path | None = None,
    progress_expected_uid: int | None = None,
    progress_expected_gid: int | None = None,
) -> _GatedCandidate:
    """Start a child that cannot import project code until the gate is released."""

    if not command or any(not isinstance(value, str) or not value for value in command):
        raise CollectorError("candidate command is invalid")
    working_directory = cwd.expanduser().absolute().resolve(strict=True)
    if not working_directory.is_dir():
        raise CollectorError("candidate working directory is invalid")
    child_environment = dict(environment)
    if any(
        not isinstance(key, str)
        or not key
        or not isinstance(value, str)
        for key, value in child_environment.items()
    ):
        raise CollectorError("candidate environment is invalid")
    if any(
        name in child_environment
        for name in (
            "TXNMEM_PROVENANCE_START_GATE_FD",
            "TXNMEM_PROVENANCE_READY_FD",
            "TXNMEM_PROVENANCE_COMPLETION_FD",
            "TXNMEM_PROVENANCE_PROGRESS_FD",
            "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256",
        )
    ):
        raise CollectorError("candidate gate environment is reserved")
    if (formal_uid is None) != (formal_gid is None):
        raise CollectorError("formal child identity is incomplete")
    progress_values = (
        progress_binding_sha256,
        progress_config_sha256,
        progress_snapshot_path,
        progress_expected_uid,
        progress_expected_gid,
    )
    if require_progress:
        if (
            not isinstance(progress_snapshot_path, Path)
            or type(progress_expected_uid) is not int
            or type(progress_expected_gid) is not int
        ):
            raise CollectorError("candidate progress ownership is incomplete")
        try:
            _validate_hash(progress_binding_sha256, "progress binding")
            _validate_hash(progress_config_sha256, "progress config")
        except CollectorError:
            raise
    elif any(value is not None for value in progress_values):
        raise CollectorError("candidate progress parameters require a channel")
    preexec_fn = None
    if formal_uid is not None and formal_gid is not None:
        if not hasattr(os, "geteuid") or os.geteuid() != 0:
            raise CollectorError("formal child launch requires root")
        _require_pidfd_support()
        preexec_fn = functools.partial(
            _prepare_formal_child_process,
            os.getpid(),
            formal_uid,
            formal_gid,
        )
    owned_descriptors: list[int] = []

    def allocate_pipe() -> tuple[int, int]:
        pair = os.pipe()
        owned_descriptors.extend(pair)
        return pair

    def close_owned(descriptor: int | None) -> None:
        if descriptor is None or descriptor not in owned_descriptors:
            return
        owned_descriptors.remove(descriptor)
        try:
            os.close(descriptor)
        except BaseException:
            raise CollectorError("candidate startup cleanup failed") from None

    def transfer_owned(descriptor: int | None) -> None:
        if descriptor is not None and descriptor in owned_descriptors:
            owned_descriptors.remove(descriptor)

    def close_startup_leader_pidfd() -> list[BaseException]:
        nonlocal startup_leader_pidfd
        failures: list[BaseException] = []
        if startup_leader_pidfd is not None:
            descriptor = startup_leader_pidfd
            startup_leader_pidfd = None
            try:
                _pidfd_close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        return failures

    def stop_process(process: subprocess.Popen[Any]) -> list[BaseException]:
        nonlocal startup_absence_proven
        failures: list[BaseException] = []
        if formal_uid is not None:
            if startup_start_identity is None:
                child_exit_proven = False
                try:
                    process.wait(timeout=5.0)
                    child_exit_proven = True
                except subprocess.TimeoutExpired:
                    if startup_leader_pidfd is None:
                        failures.append(
                            CollectorError(
                                "candidate startup leader pidfd is unavailable"
                            )
                        )
                    else:
                        try:
                            _pidfd_send_signal(
                                startup_leader_pidfd, signal.SIGKILL
                            )
                        except OSError as exc:
                            if exc.errno != errno.ESRCH:
                                failures.append(
                                    CollectorError(
                                        "candidate startup pidfd kill failed"
                                    )
                                )
                        except BaseException as exc:
                            failures.append(exc)
                    try:
                        process.wait(timeout=5.0)
                        child_exit_proven = True
                    except BaseException as exc:
                        failures.append(exc)
                except BaseException as exc:
                    failures.append(exc)
                uid_empty_proven = False
                try:
                    _require_formal_uid_processes(formal_uid, expected={})
                except BaseException as exc:
                    failures.append(exc)
                else:
                    uid_empty_proven = True
                if child_exit_proven and uid_empty_proven:
                    startup_absence_proven = True
                else:
                    _formal_startup_fail_stop()
                    raise CollectorError(
                        "formal startup fail-stop returned unexpectedly"
                    )
                failures.extend(close_startup_leader_pidfd())
                return failures
            startup_candidate = _GatedCandidate(
                process=process,
                _release_fd=None,
                _receipt_fd=None,
                ready_observed=False,
                _formal_uid=formal_uid,
            )
            try:
                startup_candidate.bind_process_identity(startup_start_identity)
                startup_candidate.terminate_validated_group(
                    term_seconds=5.0, kill_seconds=5.0
                )
            except BaseException as exc:
                failures.append(exc)
            failures.extend(close_startup_leader_pidfd())
            return failures
        try:
            running = process.poll() is None
        except BaseException as exc:
            failures.append(exc)
            running = True
        if running:
            try:
                process.terminate()
            except BaseException as exc:
                failures.append(exc)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except BaseException as exc:
                    failures.append(exc)
                try:
                    process.wait(timeout=5)
                except BaseException as exc:
                    failures.append(exc)
            except BaseException as exc:
                failures.append(exc)
                try:
                    process.kill()
                except BaseException as kill_exc:
                    failures.append(kill_exc)
                try:
                    process.wait(timeout=5)
                except BaseException as wait_exc:
                    failures.append(wait_exc)
        else:
            try:
                process.wait()
            except BaseException as exc:
                failures.append(exc)
        return failures

    def close_remaining_owned() -> list[BaseException]:
        failures: list[BaseException] = []
        for descriptor in tuple(reversed(owned_descriptors)):
            owned_descriptors.remove(descriptor)
            try:
                os.close(descriptor)
            except BaseException as exc:
                failures.append(exc)
        return failures

    read_fd: int | None = None
    write_fd: int | None = None
    ready_read_fd: int | None = None
    ready_write_fd: int | None = None
    receipt_read_fd: int | None = None
    receipt_write_fd: int | None = None
    progress_read_fd: int | None = None
    progress_write_fd: int | None = None
    progress_store: ProgressSnapshotStore | None = None
    progress_state: FormalProgressState | None = None
    progress_drainer: ProgressPipeDrainer | None = None
    progress_started = False
    drainer_owns_descriptor = False
    process: subprocess.Popen[Any] | None = None
    startup_start_identity: str | None = None
    startup_leader_pidfd: int | None = None
    startup_absence_proven: bool | None = None
    retain_startup_cleanup_owner = False
    try:
        read_fd, write_fd = allocate_pipe()
        ready_read_fd, ready_write_fd = allocate_pipe()
        if require_completion_receipt:
            receipt_read_fd, receipt_write_fd = allocate_pipe()
        if require_progress:
            assert progress_snapshot_path is not None
            assert progress_expected_uid is not None
            assert progress_expected_gid is not None
            assert progress_binding_sha256 is not None
            assert progress_config_sha256 is not None
            progress_read_fd, progress_write_fd = allocate_pipe()
            progress_store = ProgressSnapshotStore(
                progress_snapshot_path,
                expected_uid=progress_expected_uid,
                expected_gid=progress_expected_gid,
            )
            progress_store.write_starting(
                progress_binding_sha256, progress_config_sha256
            )
            progress_started = True
            progress_state = FormalProgressState(
                progress_binding_sha256, progress_config_sha256
            )
            progress_drainer = ProgressPipeDrainer(
                progress_read_fd, progress_state, progress_store
            )

        assert read_fd is not None
        assert write_fd is not None
        assert ready_read_fd is not None
        assert ready_write_fd is not None
        child_environment["TXNMEM_PROVENANCE_START_GATE_FD"] = str(read_fd)
        child_environment["TXNMEM_PROVENANCE_READY_FD"] = str(ready_write_fd)
        if receipt_write_fd is not None:
            child_environment["TXNMEM_PROVENANCE_COMPLETION_FD"] = str(
                receipt_write_fd
            )
        if progress_write_fd is not None:
            child_environment["TXNMEM_PROVENANCE_PROGRESS_FD"] = str(
                progress_write_fd
            )
            child_environment[
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256"
            ] = progress_binding_sha256
        inherited_descriptors = [read_fd, ready_write_fd]
        if receipt_write_fd is not None:
            inherited_descriptors.append(receipt_write_fd)
        if progress_write_fd is not None:
            inherited_descriptors.append(progress_write_fd)
        process = subprocess.Popen(
            tuple(command),
            cwd=working_directory,
            env=child_environment,
            close_fds=True,
            pass_fds=tuple(inherited_descriptors),
            preexec_fn=preexec_fn,
            start_new_session=True,
        )
        if formal_uid is not None:
            startup_leader_pidfd = _pidfd_open(process.pid)
            observed_group = _read_process_group_identity(
                process.pid, tuple(command)
            )
            if (
                observed_group.get("pid") != process.pid
                or observed_group.get("pgid") != process.pid
                or observed_group.get("sid") != process.pid
            ):
                raise CollectorError("candidate startup process identity changed")
            startup_start_identity = str(
                observed_group.get("start_identity", "")
            )

        close_owned(read_fd)
        close_owned(ready_write_fd)
        close_owned(receipt_write_fd)
        close_owned(progress_write_fd)
        if progress_drainer is not None:
            try:
                progress_drainer.start()
            except BaseException:
                progress_drainer.abort()
                transfer_owned(progress_read_fd)
                drainer_owns_descriptor = True
                raise
            transfer_owned(progress_read_fd)
            drainer_owns_descriptor = True

        readable, _, _ = select.select([ready_read_fd], [], [], 5.0)
        token = os.read(ready_read_fd, 1) if readable else b""
        if token != b"R" or process.poll() is not None:
            raise CollectorError(
                "candidate readiness handshake failed before launch attestation"
            )
        close_owned(ready_read_fd)
        candidate = _GatedCandidate(
            process=process,
            _release_fd=write_fd,
            _receipt_fd=receipt_read_fd,
            _progress_drainer=progress_drainer,
            _progress_store=progress_store,
            _progress_state=progress_state,
            ready_observed=True,
            _formal_uid=formal_uid,
            _leader_pidfd=startup_leader_pidfd,
        )
        if startup_start_identity is not None:
            candidate.bind_process_identity(startup_start_identity)
        transfer_owned(write_fd)
        transfer_owned(receipt_read_fd)
        startup_leader_pidfd = None
        return candidate
    except BaseException:
        cleanup_failures: list[BaseException] = []
        if process is not None:
            if formal_uid is not None and startup_start_identity is None:
                try:
                    close_owned(write_fd)
                except BaseException as exc:
                    cleanup_failures.append(exc)
            cleanup_failures.extend(stop_process(process))
            if (
                formal_uid is not None
                and startup_start_identity is None
                and startup_absence_proven is not True
            ):
                retain_startup_cleanup_owner = True
                raise CollectorError(
                    "candidate startup absence proof failed"
                ) from None
        if progress_drainer is not None and drainer_owns_descriptor:
            try:
                progress_drainer.abort()
            except BaseException as exc:
                cleanup_failures.append(exc)
        blocking_failure: BaseException | None = None
        if progress_started and progress_store is not None:
            try:
                _persist_blocked_progress(progress_store)
            except BaseException as exc:
                blocking_failure = exc
        descriptor_failures = close_remaining_owned()
        cleanup_failures.extend(close_startup_leader_pidfd())
        if blocking_failure is not None:
            raise blocking_failure from None
        if cleanup_failures:
            raise CollectorError("candidate startup cleanup failed") from None
        # Descriptor restoration is secondary to the startup failure that made
        # cleanup necessary. Every owned descriptor has still been attempted.
        if descriptor_failures:
            raise
        raise
    finally:
        primary_failure = sys.exc_info()[1]
        close_failures = (
            [] if retain_startup_cleanup_owner else close_remaining_owned()
        )
        if close_failures and primary_failure is None:
            raise CollectorError("candidate startup cleanup failed") from None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _private_directory(path: Path, field: str) -> Path:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_dir():
        raise CollectorError(f"{field} requires an existing private directory")
    resolved = candidate.resolve(strict=True)
    metadata = resolved.stat()
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        raise CollectorError(f"{field} directory must have mode 0700")
    if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
        raise CollectorError(f"{field} directory must be owned by the current user")
    return resolved


def _validate_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise CollectorError(f"{field} must be a SHA-256 digest")
    return value


def _formal_candidate_root(
    run_id_sha256: str,
    authorization_nonce_sha256: str,
    *,
    runs_root: Path = _FORMAL_RUNS_ROOT,
) -> Path:
    """Derive the sole candidate location from registered execution identity."""

    run_hash = _validate_hash(run_id_sha256, "formal run id")
    nonce_hash = _validate_hash(
        authorization_nonce_sha256, "formal authorization nonce"
    )
    root = runs_root.expanduser().absolute()
    return root / f"run-{run_hash}-{nonce_hash[:16]}" / "candidate"


def _formal_network_table_name(run_hash: str) -> str:
    return "txnmem_" + _validate_hash(run_hash, "formal network run")[:16]


def _nft_guard_batch(
    table_name: str,
    *,
    runner_uid: int,
    backend_ipv4_subnet: str,
    ingress_ipv4_subnet: str,
    backend_bridge_interface: str,
    ingress_bridge_interface: str,
    toxiproxy_ingress_ipv4: str,
) -> str:
    if (
        not isinstance(table_name, str)
        or not re.fullmatch(r"txnmem_[0-9a-f]{16}", table_name)
        or type(runner_uid) is not int
        or runner_uid <= 0
    ):
        raise CollectorError("formal nftables guard identity is invalid")
    normalized_subnets: list[str] = []
    for value in (backend_ipv4_subnet, ingress_ipv4_subnet):
        try:
            subnet = ipaddress.ip_network(value, strict=True)
        except (TypeError, ValueError) as exc:
            raise CollectorError("formal nftables bridge subnet is invalid") from exc
        if (
            not isinstance(subnet, ipaddress.IPv4Network)
            or str(subnet) != value
            or not any(
                subnet.subnet_of(parent) for parent in _RFC1918_IPV4_NETWORKS
            )
        ):
            raise CollectorError("formal nftables bridge subnet is invalid")
        normalized_subnets.append(str(subnet))
    if (
        normalized_subnets[0] == normalized_subnets[1]
        or ipaddress.ip_network(normalized_subnets[0]).overlaps(
            ipaddress.ip_network(normalized_subnets[1])
        )
    ):
        raise CollectorError("formal nftables bridge subnets overlap")
    backend_subnet = ipaddress.ip_network(normalized_subnets[0])
    ingress_subnet = ipaddress.ip_network(normalized_subnets[1])
    try:
        ingress_address = ipaddress.IPv4Address(toxiproxy_ingress_ipv4)
    except (TypeError, ValueError) as exc:
        raise CollectorError("formal nftables ingress address is invalid") from exc
    if (
        str(ingress_address) != toxiproxy_ingress_ipv4
        or ingress_address not in ingress_subnet
        or ingress_address in backend_subnet
        or ingress_address
        in {
            ingress_subnet.network_address,
            ingress_subnet.network_address + 1,
            ingress_subnet.broadcast_address,
        }
        or ingress_address.is_loopback
    ):
        raise CollectorError("formal nftables ingress address is invalid")
    bridge_interfaces = (
        backend_bridge_interface,
        ingress_bridge_interface,
    )
    if (
        any(
            not isinstance(value, str)
            or not re.fullmatch(r"br-[0-9a-f]{12}", value)
            for value in bridge_interfaces
        )
        or bridge_interfaces[0] == bridge_interfaces[1]
    ):
        raise CollectorError("formal nftables bridge interface is invalid")
    subnet_set = ", ".join(normalized_subnets)
    interface_set = ", ".join(f'"{value}"' for value in bridge_interfaces)
    # The runner UID gates each new proxy flow.  Kernel-emitted packets can
    # lose socket ownership after that flow is established, so preserve only
    # the established proxy tuple before applying the non-runner reset rule.
    return (
        f"table inet {table_name} {{\n"
        "  chain output {\n"
        "    type filter hook output priority -150; policy accept;\n"
        f"    meta skuid {runner_uid} ip daddr 127.0.0.1 "
        "tcp dport { 19000, 19001 } accept comment \"txnmem-proxy-allow\"\n"
        "    ct state established ip daddr 127.0.0.1 "
        "tcp dport { 19000, 19001 } accept "
        "comment \"txnmem-proxy-established-allow\"\n"
        "    meta skuid 0 ip daddr 127.0.0.1 tcp dport 8474 "
        "accept comment \"txnmem-management-allow\"\n"
        f"    meta skuid 0 ip daddr {ingress_address} "
        "tcp dport { 8474, 19000, 19001 } accept "
        "comment \"txnmem-docker-proxy-ingress-allow\"\n"
        f"    meta skuid {runner_uid} reject comment \"txnmem-runner-deny\"\n"
        "    ip daddr 127.0.0.1 tcp dport 8474 "
        "reject with tcp reset comment \"txnmem-management-deny\"\n"
        "    ip daddr 127.0.0.1 tcp dport { 19000, 19001 } "
        "reject with tcp reset comment \"txnmem-attribution-deny\"\n"
        f"    ip daddr {{ {subnet_set} }} tcp flags & rst == rst "
        "accept comment \"txnmem-host-bridge-reset-allow\"\n"
        "    tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 } "
        f"ip daddr {{ {subnet_set} }} "
        "reject with tcp reset comment \"txnmem-host-bridge-tcp-deny\"\n"
        f"    ip daddr {{ {subnet_set} }} "
        "reject comment \"txnmem-host-bridge-deny\"\n"
        "  }\n"
        "  chain forward {\n"
        "    type filter hook forward priority -150; policy accept;\n"
        f"    iifname != {{ {interface_set} }} "
        "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 } "
        f"ip daddr {{ {subnet_set} }} "
        "reject with tcp reset comment \"txnmem-forward-bridge-tcp-deny\"\n"
        f"    iifname != {{ {interface_set} }} "
        f"ip daddr {{ {subnet_set} }} "
        "reject comment \"txnmem-forward-bridge-deny\"\n"
        "  }\n"
        "}\n"
    )


def _strict_json_bytes(payload: bytes, field: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        document: dict[str, Any] = {}
        for key, value in pairs:
            if key in document:
                raise CollectorError(f"{field} contains duplicate keys")
            document[key] = value
        return document

    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                CollectorError(f"{field} contains {value}")
            ),
        )
    except CollectorError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CollectorError(f"{field} is malformed") from exc


def _normalize_nft_snapshot(document: Any, *, table_name: str) -> dict[str, Any]:
    if (
        not isinstance(document, Mapping)
        or set(document) != {"nftables"}
        or not isinstance(document.get("nftables"), list)
    ):
        raise CollectorError("formal nftables snapshot schema is invalid")
    normalized: list[Any] = []
    comments: list[str] = []
    table_seen = False
    chains_seen: set[str] = set()
    for item in document["nftables"]:
        if not isinstance(item, Mapping) or len(item) != 1:
            raise CollectorError("formal nftables snapshot item is invalid")
        kind, raw_value = next(iter(item.items()))
        if kind == "metainfo":
            continue
        if not isinstance(raw_value, Mapping):
            raise CollectorError("formal nftables snapshot value is invalid")
        value = {
            key: raw_value[key]
            for key in sorted(raw_value)
            if key not in {"handle", "index", "position"}
        }
        if value.get("family") != "inet" or value.get("table", table_name) != table_name:
            raise CollectorError("formal nftables snapshot escaped its table")
        if kind == "table":
            if value.get("name") != table_name:
                raise CollectorError("formal nftables table identity mismatch")
            table_seen = True
        elif kind == "chain":
            name = value.get("name")
            if name not in {"output", "forward"} or name in chains_seen:
                raise CollectorError("formal nftables chain identity mismatch")
            if (
                value.get("type") != "filter"
                or value.get("hook") != name
                or value.get("policy") != "accept"
            ):
                raise CollectorError("formal nftables chain identity mismatch")
            chains_seen.add(str(name))
        elif kind == "rule":
            comment = value.get("comment")
            if not isinstance(comment, str):
                raise CollectorError("formal nftables rule comment is missing")
            comments.append(comment)
        else:
            raise CollectorError("formal nftables snapshot contains an extra object")
        normalized.append({kind: value})
    if not table_seen or chains_seen != {"output", "forward"} or sorted(comments) != [
        "txnmem-attribution-deny",
        "txnmem-docker-proxy-ingress-allow",
        "txnmem-forward-bridge-deny",
        "txnmem-forward-bridge-tcp-deny",
        "txnmem-host-bridge-deny",
        "txnmem-host-bridge-reset-allow",
        "txnmem-host-bridge-tcp-deny",
        "txnmem-management-allow",
        "txnmem-management-deny",
        "txnmem-proxy-allow",
        "txnmem-proxy-established-allow",
        "txnmem-runner-deny",
    ]:
        raise CollectorError("formal nftables rule closure is incomplete")
    return {"nftables": normalized}


@dataclass
class _NftNetworkGuard:
    table_name: str
    backend_ipv4_subnet: str
    ingress_ipv4_subnet: str
    backend_bridge_interface: str
    ingress_bridge_interface: str
    toxiproxy_ingress_ipv4: str
    runner_uid: int = FORMAL_RUNNER_UID
    executable: Path = Path(_FORMAL_NFT_EXECUTABLE)
    active: bool = False
    _expected_snapshot: dict[str, Any] | None = None

    def _run(
        self,
        arguments: Sequence[str],
        *,
        stdin: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[Any]:
        try:
            return subprocess.run(
                [str(self.executable), *arguments],
                input=stdin,
                capture_output=True,
                text=True,
                check=check,
                env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise CollectorError("formal nftables operation failed") from exc

    def _table_names(self) -> set[str]:
        result = self._run(("-j", "list", "tables"))
        document = _strict_json_bytes(
            result.stdout.encode("utf-8"), "formal nftables table list"
        )
        if not isinstance(document, Mapping) or not isinstance(
            document.get("nftables"), list
        ):
            raise CollectorError("formal nftables table list is malformed")
        names: set[str] = set()
        for item in document["nftables"]:
            if not isinstance(item, Mapping):
                raise CollectorError("formal nftables table list item is invalid")
            table = item.get("table")
            if table is None:
                if "metainfo" in item:
                    continue
                raise CollectorError("formal nftables table list item is invalid")
            if (
                not isinstance(table, Mapping)
                or table.get("family") != "inet"
                or not isinstance(table.get("name"), str)
            ):
                continue
            names.add(str(table["name"]))
        return names

    def snapshot(self) -> dict[str, Any]:
        if not self.active:
            raise CollectorError("formal nftables guard is inactive")
        result = self._run(
            ("-j", "list", "table", "inet", self.table_name)
        )
        normalized = _normalize_nft_snapshot(
            _strict_json_bytes(
                result.stdout.encode("utf-8"), "formal nftables snapshot"
            ),
            table_name=self.table_name,
        )
        return {
            "schema": "txnmem-provenance-network-guard-v3",
            "table_name_sha256": hashlib.sha256(
                self.table_name.encode("utf-8")
            ).hexdigest(),
            "runner_uid": self.runner_uid,
            "controller_uid": 0,
            "allowed_ipv4_loopback_ports": [19000, 19001],
            "allowed_root_ingress_ports": [8474, 19000, 19001],
            "root_ingress_destination_exact": True,
            "management_port_root_only": True,
            "non_runner_proxy_traffic_blocked": True,
            "host_bridge_access_blocked": True,
            "forwarded_bridge_access_blocked": True,
            "backend_ipv4_subnet_sha256": hashlib.sha256(
                self.backend_ipv4_subnet.encode("utf-8")
            ).hexdigest(),
            "ingress_ipv4_subnet_sha256": hashlib.sha256(
                self.ingress_ipv4_subnet.encode("utf-8")
            ).hexdigest(),
            "backend_bridge_interface_sha256": hashlib.sha256(
                self.backend_bridge_interface.encode("utf-8")
            ).hexdigest(),
            "ingress_bridge_interface_sha256": hashlib.sha256(
                self.ingress_bridge_interface.encode("utf-8")
            ).hexdigest(),
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                self.toxiproxy_ingress_ipv4.encode("utf-8")
            ).hexdigest(),
            "policy_sha256": hashlib.sha256(
                _nft_guard_batch(
                    self.table_name,
                    runner_uid=self.runner_uid,
                    backend_ipv4_subnet=self.backend_ipv4_subnet,
                    ingress_ipv4_subnet=self.ingress_ipv4_subnet,
                    backend_bridge_interface=self.backend_bridge_interface,
                    ingress_bridge_interface=self.ingress_bridge_interface,
                    toxiproxy_ingress_ipv4=self.toxiproxy_ingress_ipv4,
                ).encode("utf-8")
            ).hexdigest(),
            "ruleset_sha256": hashlib.sha256(
                canonical_json_bytes(normalized)
            ).hexdigest(),
        }

    def activate(self) -> dict[str, Any]:
        if self.active or self.table_name in self._table_names():
            raise CollectorError("formal nftables guard already exists")
        batch = _nft_guard_batch(
            self.table_name,
            runner_uid=self.runner_uid,
            backend_ipv4_subnet=self.backend_ipv4_subnet,
            ingress_ipv4_subnet=self.ingress_ipv4_subnet,
            backend_bridge_interface=self.backend_bridge_interface,
            ingress_bridge_interface=self.ingress_bridge_interface,
            toxiproxy_ingress_ipv4=self.toxiproxy_ingress_ipv4,
        )
        self._run(("--check", "-f", "-"), stdin=batch)
        try:
            self._run(("-f", "-"), stdin=batch)
            self.active = True
            self._expected_snapshot = self.snapshot()
            return dict(self._expected_snapshot)
        except BaseException:
            rollback_failure: BaseException | None = None
            try:
                table_present = self.table_name in self._table_names()
            except BaseException as exc:
                self.active = True
                raise CollectorError(
                    "formal nftables guard rollback failed"
                ) from exc
            if table_present:
                try:
                    result = self._run(
                        ("delete", "table", "inet", self.table_name),
                        check=False,
                    )
                    if getattr(result, "returncode", 0) != 0:
                        rollback_failure = CollectorError(
                            "formal nftables guard rollback failed"
                        )
                except BaseException as exc:
                    rollback_failure = exc
                try:
                    if self.table_name in self._table_names():
                        rollback_failure = CollectorError(
                            "formal nftables guard rollback failed"
                        )
                except BaseException as exc:
                    rollback_failure = exc
            if rollback_failure is not None:
                self.active = True
                raise CollectorError(
                    "formal nftables guard rollback failed"
                ) from rollback_failure
            self.active = False
            raise

    def verify(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        if self._expected_snapshot is None or snapshot != self._expected_snapshot:
            raise CollectorError("formal nftables guard changed during execution")
        return snapshot

    def deactivate(self) -> None:
        if not self.active:
            return
        try:
            table_present = self.table_name in self._table_names()
        except BaseException as exc:
            self.active = True
            raise CollectorError("formal nftables guard cleanup failed") from exc
        if not table_present:
            self.active = False
            return
        try:
            self._run(("delete", "table", "inet", self.table_name))
        except BaseException as exc:
            self.active = True
            raise CollectorError("formal nftables guard cleanup failed") from exc
        try:
            table_present = self.table_name in self._table_names()
        except BaseException as exc:
            self.active = True
            raise CollectorError("formal nftables guard cleanup failed") from exc
        if table_present:
            self.active = True
            raise CollectorError("formal nftables guard cleanup failed")
        self.active = False


def _require_derived_candidate_root(
    candidate_root: Path,
    *,
    run_hash: str,
    nonce_hash: str,
    runs_root: Path = _FORMAL_RUNS_ROOT,
) -> Path:
    """Reject caller-selected candidate roots outside the derived formal run."""

    expected = _formal_candidate_root(
        run_hash, nonce_hash, runs_root=runs_root
    )
    candidate = candidate_root.expanduser().absolute()
    if candidate.is_symlink() or candidate != expected:
        raise CollectorError(
            "candidate root must be the source-derived formal run location"
        )
    return expected


def _prepare_formal_run_workspace(
    run_hash: str,
    nonce_hash: str,
    *,
    runs_root: Path = _FORMAL_RUNS_ROOT,
    controller_uid: int = 0,
    runner_uid: int = FORMAL_RUNNER_UID,
    runner_gid: int = FORMAL_RUNNER_GID,
    require_root: bool = True,
) -> _FormalRunWorkspace:
    """Exclusively create one protected, identity-derived execution workspace."""

    for value, label in (
        (controller_uid, "formal controller uid"),
        (runner_uid, "formal runner uid"),
        (runner_gid, "formal runner gid"),
    ):
        if type(value) is not int or value < 0:
            raise CollectorError(f"{label} is invalid")
    if require_root and (
        not hasattr(os, "geteuid")
        or os.geteuid() != 0
        or controller_uid != 0
        or runner_uid != FORMAL_RUNNER_UID
        or runner_gid != FORMAL_RUNNER_GID
    ):
        raise CollectorError("formal run workspace requires the root controller")
    expected_candidate = _formal_candidate_root(
        run_hash, nonce_hash, runs_root=runs_root
    )
    root = runs_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("formal runs root is unavailable")
    root = root.resolve(strict=True)
    root_metadata = root.stat()
    if (
        root_metadata.st_uid != controller_uid
        or root_metadata.st_gid != runner_gid
        or stat.S_IMODE(root_metadata.st_mode) != 0o750
    ):
        raise CollectorError("formal runs root is not controller protected")
    run_directory = expected_candidate.parent
    run_name = run_directory.name
    base_descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        try:
            os.mkdir(run_name, mode=0o750, dir_fd=base_descriptor)
        except FileExistsError as exc:
            raise CollectorError("formal run workspace already exists") from exc
        if controller_uid != os.getuid() or runner_gid != os.getgid():
            os.chown(
                run_name,
                controller_uid,
                runner_gid,
                dir_fd=base_descriptor,
                follow_symlinks=False,
            )
        os.chmod(run_name, 0o750, dir_fd=base_descriptor, follow_symlinks=False)
        run_descriptor = os.open(
            run_name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=base_descriptor,
        )
        try:
            run_metadata = os.fstat(run_descriptor)
            if (
                not stat.S_ISDIR(run_metadata.st_mode)
                or run_metadata.st_uid != controller_uid
                or run_metadata.st_gid != runner_gid
                or stat.S_IMODE(run_metadata.st_mode) != 0o750
            ):
                raise CollectorError("formal run workspace identity is invalid")
            os.mkdir("candidate", mode=0o700, dir_fd=run_descriptor)
            if runner_uid != os.getuid() or runner_gid != os.getgid():
                os.chown(
                    "candidate",
                    runner_uid,
                    runner_gid,
                    dir_fd=run_descriptor,
                    follow_symlinks=False,
                )
            os.chmod(
                "candidate",
                0o700,
                dir_fd=run_descriptor,
                follow_symlinks=False,
            )
            candidate_descriptor = os.open(
                "candidate",
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=run_descriptor,
            )
            try:
                candidate_metadata = os.fstat(candidate_descriptor)
                if (
                    not stat.S_ISDIR(candidate_metadata.st_mode)
                    or candidate_metadata.st_uid != runner_uid
                    or candidate_metadata.st_gid != runner_gid
                    or stat.S_IMODE(candidate_metadata.st_mode) != 0o700
                ):
                    raise CollectorError("formal candidate directory identity is invalid")
            finally:
                os.close(candidate_descriptor)
        finally:
            os.close(run_descriptor)
    finally:
        os.close(base_descriptor)
    return _FormalRunWorkspace(
        root=run_directory,
        candidate=expected_candidate,
        root_device=int(run_metadata.st_dev),
        root_inode=int(run_metadata.st_ino),
    )


def _create_formal_input_staging(
    workspace: _FormalRunWorkspace,
    *,
    controller_uid: int = 0,
    runner_gid: int = FORMAL_RUNNER_GID,
) -> Path:
    """Create the root-only staging subtree inside the attested run inode."""

    root = workspace.root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("formal run workspace disappeared")
    root = root.resolve(strict=True)
    root_metadata = root.stat()
    if (
        int(root_metadata.st_dev) != workspace.root_device
        or int(root_metadata.st_ino) != workspace.root_inode
        or root_metadata.st_uid != controller_uid
        or root_metadata.st_gid != runner_gid
        or stat.S_IMODE(root_metadata.st_mode) != 0o750
    ):
        raise CollectorError("formal run workspace identity changed")
    root_descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.mkdir("inputs", mode=0o700, dir_fd=root_descriptor)
        os.chmod(
            "inputs", 0o700, dir_fd=root_descriptor, follow_symlinks=False
        )
        input_descriptor = os.open(
            "inputs",
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_descriptor,
        )
        try:
            metadata = os.fstat(input_descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != controller_uid
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise CollectorError("formal input staging identity is invalid")
        finally:
            os.close(input_descriptor)
    finally:
        os.close(root_descriptor)
    return root / "inputs"


def _set_parent_death_signal(
    parent_pid: int,
    *,
    prctl: Callable[[int, int, int, int, int], int] | None = None,
    getppid: Callable[[], int] = os.getppid,
) -> None:
    """Require Linux to kill the child if the registered collector dies."""

    if type(parent_pid) is not int or parent_pid <= 0:
        raise CollectorError("formal child parent identity is invalid")
    operation = prctl
    if operation is None:
        if platform.system() != "Linux":
            raise CollectorError("formal parent-death signal requires Linux")
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            raw_prctl = libc.prctl
            raw_prctl.argtypes = [
                ctypes.c_int,
                ctypes.c_ulong,
                ctypes.c_ulong,
                ctypes.c_ulong,
                ctypes.c_ulong,
            ]
            raw_prctl.restype = ctypes.c_int
            operation = raw_prctl
        except (AttributeError, OSError):
            raise CollectorError("formal parent-death signal is unavailable") from None
    try:
        result = operation(1, signal.SIGKILL, 0, 0, 0)
    except BaseException:
        raise CollectorError("formal parent-death signal setup failed") from None
    if result != 0:
        raise CollectorError("formal parent-death signal setup failed")
    try:
        observed_parent = getppid()
    except BaseException:
        raise CollectorError("formal child parent identity is unavailable") from None
    if observed_parent != parent_pid:
        raise CollectorError("formal child parent identity changed")
    observed_signal = ctypes.c_int(0)
    try:
        result = operation(2, ctypes.addressof(observed_signal), 0, 0, 0)
    except BaseException:
        raise CollectorError("formal parent-death signal query failed") from None
    if result != 0 or observed_signal.value != signal.SIGKILL:
        raise CollectorError("formal parent-death signal verification failed")


def _prepare_formal_child_process(parent_pid: int, uid: int, gid: int) -> None:
    """Apply the complete formal child setup through one ordered pre-exec hook."""

    try:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
    except BaseException:
        raise CollectorError(
            "formal child signal disposition reset failed"
        ) from None
    if not hasattr(signal, "pthread_sigmask"):
        raise CollectorError("formal child signal mask is unavailable")
    try:
        signal.pthread_sigmask(signal.SIG_SETMASK, {signal.SIGTERM})
    except BaseException:
        raise CollectorError("formal child signal mask setup failed") from None
    _set_parent_death_signal(parent_pid)
    _drop_formal_child_privileges(uid, gid)
    _set_parent_death_signal(parent_pid)


def _drop_formal_child_privileges(uid: int, gid: int) -> None:
    """Enter the closed, non-privileged identity used by a formal child."""

    if (
        type(uid) is not int
        or uid <= 0
        or type(gid) is not int
        or gid <= 0
        or not hasattr(os, "geteuid")
        or os.geteuid() != 0
    ):
        raise CollectorError("formal child privilege drop requires root")
    _set_no_new_privileges()
    try:
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    except OSError as exc:
        raise CollectorError("formal child privilege drop failed") from exc
    if (
        os.getuid() != uid
        or os.geteuid() != uid
        or os.getgid() != gid
        or os.getegid() != gid
    ):
        raise CollectorError("formal child privilege drop was incomplete")
    os.umask(0o077)


def _set_no_new_privileges(
    *, prctl: Callable[[int, int, int, int, int], int] | None = None
) -> None:
    """Irreversibly block privilege gains across the formal child exec."""

    operation = prctl
    if operation is None:
        if platform.system() != "Linux":
            raise CollectorError("formal no-new-privileges requires Linux")
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            raw_prctl = libc.prctl
            raw_prctl.argtypes = [
                ctypes.c_int,
                ctypes.c_ulong,
                ctypes.c_ulong,
                ctypes.c_ulong,
                ctypes.c_ulong,
            ]
            raw_prctl.restype = ctypes.c_int
            operation = raw_prctl
        except (AttributeError, OSError) as exc:
            raise CollectorError("formal no-new-privileges is unavailable") from exc
    if operation(38, 1, 0, 0, 0) != 0 or operation(39, 0, 0, 0, 0) != 1:
        raise CollectorError("formal no-new-privileges verification failed")


def _verify_formal_input_tree(
    input_root: Path,
    *,
    controller_uid: int = 0,
    runner_gid: int = FORMAL_RUNNER_GID,
) -> dict[str, Any]:
    """Re-attest every protected execution input by inode, mode, and bytes."""

    root = input_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("formal input tree root is invalid")
    root = root.resolve(strict=True)
    rows: list[dict[str, Any]] = []
    directory_count = 0
    file_count = 0
    for path in [root, *sorted(root.rglob("*"), key=lambda item: item.as_posix())]:
        metadata = path.lstat()
        relative = "." if path == root else path.relative_to(root).as_posix()
        if (
            metadata.st_uid != controller_uid
            or metadata.st_gid != runner_gid
            or stat.S_ISLNK(metadata.st_mode)
        ):
            raise CollectorError("formal input tree ownership is invalid")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o550:
                raise CollectorError("formal input directory is not read-only")
            directory_count += 1
            rows.append(
                {
                    "path": relative,
                    "kind": "directory",
                    "device": int(metadata.st_dev),
                    "inode": int(metadata.st_ino),
                    "size": 0,
                    "sha256": "0" * 64,
                }
            )
        elif stat.S_ISREG(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o440:
                raise CollectorError("formal input file is not read-only")
            payload = _read_regular_file_bytes(path, "formal input file")
            file_count += 1
            rows.append(
                {
                    "path": relative,
                    "kind": "file",
                    "device": int(metadata.st_dev),
                    "inode": int(metadata.st_ino),
                    "size": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        else:
            raise CollectorError("formal input tree contains a special file")
    root_metadata = root.stat()
    return {
        "schema": "txnmem-provenance-input-tree-v1",
        "root_device": int(root_metadata.st_dev),
        "root_inode": int(root_metadata.st_ino),
        "directory_count": directory_count,
        "file_count": file_count,
        "tree_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
    }


def _publish_formal_input_tree(
    input_root: Path,
    *,
    controller_uid: int = 0,
    runner_gid: int = FORMAL_RUNNER_GID,
) -> dict[str, Any]:
    """Transfer staged inputs to root-owned, runner-readable immutable modes."""

    root = input_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("formal input staging tree is invalid")
    root = root.resolve(strict=True)
    observed = [root, *sorted(root.rglob("*"), key=lambda item: item.as_posix())]
    for path in observed:
        metadata = path.lstat()
        if (
            metadata.st_uid != controller_uid
            or stat.S_ISLNK(metadata.st_mode)
            or not (
                stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISREG(metadata.st_mode)
            )
        ):
            raise CollectorError("formal input staging tree is unsafe")
    for path in sorted(observed, key=lambda item: len(item.parts), reverse=True):
        metadata = path.lstat()
        if metadata.st_gid != runner_gid:
            os.chown(
                path,
                controller_uid,
                runner_gid,
                follow_symlinks=False,
            )
        path.chmod(0o550 if stat.S_ISDIR(metadata.st_mode) else 0o440)
    return _verify_formal_input_tree(
        root, controller_uid=controller_uid, runner_gid=runner_gid
    )


def _seal_candidate_tree(
    candidate_root: Path,
    *,
    expected_owner_uid: int,
    sealed_owner_uid: int,
    sealed_owner_gid: int,
    completion_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze a completed candidate and bind every inode and byte to its receipt."""

    for value, label in (
        (expected_owner_uid, "candidate owner"),
        (sealed_owner_uid, "sealed owner"),
        (sealed_owner_gid, "sealed group"),
    ):
        if type(value) is not int or value < 0:
            raise CollectorError(f"{label} is invalid")
    if not isinstance(completion_receipt, Mapping):
        raise CollectorError("candidate completion receipt is invalid")
    try:
        receipt_raw = canonical_json_bytes(dict(completion_receipt))
    except (TypeError, ValueError) as exc:
        raise CollectorError("candidate completion receipt is not canonical") from exc

    root = candidate_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("candidate seal root must be a real directory")
    root = root.resolve(strict=True)
    observed = [root, *sorted(root.rglob("*"), key=lambda item: item.as_posix())]
    for path in observed:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise CollectorError("candidate seal tree contains a link")
        if not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
            raise CollectorError("candidate seal tree contains a special file")
        if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1:
            raise CollectorError("candidate seal tree contains a hard-linked file")
        if metadata.st_uid != expected_owner_uid:
            raise CollectorError("candidate seal tree owner changed")

    # The benchmark child has exited before this function is called.  Ownership
    # is transferred before hashing so the unprivileged producer can no longer
    # replace or rewrite any candidate member while it is being attested.
    for path in sorted(observed, key=lambda item: len(item.parts), reverse=True):
        metadata = path.lstat()
        if metadata.st_uid != sealed_owner_uid or metadata.st_gid != sealed_owner_gid:
            os.chown(
                path,
                sealed_owner_uid,
                sealed_owner_gid,
                follow_symlinks=False,
            )
        path.chmod(0o500 if stat.S_ISDIR(metadata.st_mode) else 0o400)

    rows: list[dict[str, Any]] = []
    directory_count = 0
    file_count = 0
    for path in [root, *sorted(root.rglob("*"), key=lambda item: item.as_posix())]:
        metadata = path.lstat()
        relative = "." if path == root else path.relative_to(root).as_posix()
        if stat.S_ISDIR(metadata.st_mode):
            if (
                stat.S_IMODE(metadata.st_mode) != 0o500
                or metadata.st_uid != sealed_owner_uid
                or metadata.st_gid != sealed_owner_gid
            ):
                raise CollectorError("candidate directory was not sealed")
            directory_count += 1
            rows.append(
                {
                    "path": relative,
                    "kind": "directory",
                    "device": int(metadata.st_dev),
                    "inode": int(metadata.st_ino),
                    "size": 0,
                    "sha256": "0" * 64,
                }
            )
        elif stat.S_ISREG(metadata.st_mode):
            if (
                stat.S_IMODE(metadata.st_mode) != 0o400
                or metadata.st_uid != sealed_owner_uid
                or metadata.st_gid != sealed_owner_gid
            ):
                raise CollectorError("candidate file was not sealed")
            file_count += 1
            payload = _read_regular_file_bytes(path, "sealed candidate file")
            rows.append(
                {
                    "path": relative,
                    "kind": "file",
                    "device": int(metadata.st_dev),
                    "inode": int(metadata.st_ino),
                    "size": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        else:
            raise CollectorError("candidate seal tree changed while sealing")
    return {
        "schema": "txnmem-provenance-candidate-seal-v1",
        "root_device": int(root.stat().st_dev),
        "root_inode": int(root.stat().st_ino),
        "directory_count": directory_count,
        "file_count": file_count,
        "tree_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
        "completion_receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
    }


def _source_identity_fields(value: Any) -> dict[str, Any]:
    fields = {
        "source_commit",
        "source_manifest",
        "source_manifest_sha256",
        "collector_sha256",
        "runner_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise CollectorError("source identity fields do not match schema")
    commit = value.get("source_commit")
    if not isinstance(commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}(?:[0-9a-f]{24})?", commit
    ):
        raise CollectorError("source commit is invalid")
    manifest = value.get("source_manifest")
    if (
        not isinstance(manifest, Mapping)
        or set(manifest) != {"schema", "source_commit", "files"}
        or manifest.get("schema") != "txnmem-provenance-source-manifest-v1"
        or manifest.get("source_commit") != commit
        or not isinstance(manifest.get("files"), list)
        or not manifest["files"]
    ):
        raise CollectorError("source manifest is malformed")
    manifest_hash = _validate_hash(
        value.get("source_manifest_sha256"), "source manifest"
    )
    if hashlib.sha256(canonical_json_bytes(dict(manifest))).hexdigest() != manifest_hash:
        raise CollectorError("source manifest hash mismatch")
    return {
        "source_commit": commit,
        "source_manifest": dict(manifest),
        "source_manifest_sha256": manifest_hash,
        "collector_sha256": _validate_hash(
            value.get("collector_sha256"), "collector source"
        ),
        "runner_sha256": _validate_hash(value.get("runner_sha256"), "runner source"),
    }


def _validate_formal_controller_context(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {
            "schema",
            "source_commit",
            "source_manifest",
            "approval_manifest_sha256",
        }
        or value.get("schema") != _FORMAL_CONTROLLER_CONTEXT_SCHEMA
    ):
        raise CollectorError("formal controller context is unavailable")
    commit = value.get("source_commit")
    manifest = value.get("source_manifest")
    if (
        not isinstance(commit, str)
        or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", commit)
        or not isinstance(manifest, Mapping)
        or set(manifest) != {"schema", "source_commit", "files"}
        or manifest.get("schema") != "txnmem-provenance-source-manifest-v1"
        or manifest.get("source_commit") != commit
        or not isinstance(manifest.get("files"), list)
        or not manifest["files"]
    ):
        raise CollectorError("formal controller source manifest is invalid")
    rows: list[dict[str, str]] = []
    paths: list[str] = []
    for row in manifest["files"]:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"path", "blob_sha256"}
            or not isinstance(row.get("path"), str)
            or not isinstance(row.get("blob_sha256"), str)
            or not _SHA256.fullmatch(str(row["blob_sha256"]))
        ):
            raise CollectorError("formal controller source row is invalid")
        relative = str(row["path"])
        path = Path(relative)
        if (
            path.is_absolute()
            or path.as_posix() != relative
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise CollectorError("formal controller source path is unsafe")
        paths.append(relative)
        rows.append(
            {"path": relative, "blob_sha256": str(row["blob_sha256"])}
        )
    if paths != sorted(set(paths)) or not set(_REQUIRED_SOURCE_PATHS).issubset(
        paths
    ):
        raise CollectorError("formal controller source closure is incomplete")
    normalized_manifest = {
        "schema": "txnmem-provenance-source-manifest-v1",
        "source_commit": commit,
        "files": rows,
    }
    approval = {
        "schema": _FORMAL_APPROVAL_SCHEMA,
        "source_commit": commit,
        "files": rows,
    }
    approval_hash = _validate_hash(
        value.get("approval_manifest_sha256"), "formal approval manifest"
    )
    if hashlib.sha256(canonical_json_bytes(approval)).hexdigest() != approval_hash:
        raise CollectorError("formal approval manifest hash mismatch")
    return {
        "schema": _FORMAL_CONTROLLER_CONTEXT_SCHEMA,
        "source_commit": commit,
        "source_manifest": normalized_manifest,
        "approval_manifest_sha256": approval_hash,
    }


def _snapshot_components(
    snapshot: Any,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    if (
        not isinstance(snapshot, Mapping)
        or set(snapshot)
        != {
            "schema",
            "roles",
            "proxy_routes",
            "proxy_counters",
            "backend_isolation",
        }
        or snapshot.get("schema") != SNAPSHOT_SCHEMA
        or not isinstance(snapshot.get("roles"), list)
        or not isinstance(snapshot.get("proxy_routes"), list)
        or not isinstance(snapshot.get("proxy_counters"), Mapping)
        or not isinstance(snapshot.get("backend_isolation"), Mapping)
    ):
        raise CollectorError("topology probe returned an invalid snapshot")
    try:
        proxy_counters = validate_proxy_counter_snapshot(
            snapshot["proxy_counters"]
        )
    except ToxiproxyMetricsError as exc:
        raise CollectorError(
            "topology probe returned invalid proxy counters"
        ) from exc
    return (
        [dict(row) for row in snapshot["roles"]],
        [dict(row) for row in snapshot["proxy_routes"]],
        proxy_counters,
        dict(snapshot["backend_isolation"]),
    )


def _preflight_external_outputs(
    project_root: Path,
    candidate_root: Path,
    launch_path: Path,
    completion_path: Path,
) -> tuple[FormalStore, str, FormalStore, str]:
    project = project_root.expanduser().absolute()
    if project.is_symlink() or not project.is_dir():
        raise CollectorError("project root must be a real existing directory")
    project = project.resolve(strict=True)
    candidate = candidate_root.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_dir():
        raise CollectorError("candidate root must be a real existing directory")
    candidate = candidate.resolve(strict=True)
    launch = launch_path.expanduser().absolute()
    completion = completion_path.expanduser().absolute()
    if launch == completion:
        raise CollectorError("launch and completion outputs must be distinct")
    for path, label in ((launch, "launch"), (completion, "completion")):
        resolved = path.resolve(strict=False)
        if path.is_symlink() or _is_within(resolved, candidate) or _is_within(
            resolved, project
        ):
            raise CollectorError(
                f"{label} output must be outside the candidate and project roots"
            )
        if not path.name or path.name in {".", ".."}:
            raise CollectorError(f"{label} output must name a file")
        _private_directory(path.parent, f"{label} output")
    try:
        launch_store = FormalStore(launch.parent)
        completion_store = FormalStore(completion.parent)
        if launch_store.entry_kind(launch.name) != "missing":
            raise CollectorError("refusing to overwrite launch evidence")
        if completion_store.entry_kind(completion.name) != "missing":
            raise CollectorError("refusing to overwrite completion evidence")
    except ValueError as exc:
        if isinstance(exc, CollectorError):
            raise
        raise CollectorError("collector output path is unsafe") from exc
    return launch_store, launch.name, completion_store, completion.name


def _validate_candidate_material(
    material: Any,
    *,
    expected_candidate_id: str,
    expected_run_hash: str,
    expected_config_hash: str,
    expected_config_file_hash: str,
    expected_workload_hash: str,
    expected_environment_hash: str,
) -> dict[str, Any]:
    if not isinstance(material, Mapping) or set(material) != _MATERIAL_FIELDS:
        raise CollectorError("candidate material fields do not match schema")
    expected = {
        "schema": "txnmem-provenance-candidate-attestation-material-v1",
        "candidate_bundle_id": expected_candidate_id,
        "run_id_sha256": expected_run_hash,
        "config_sha256": expected_config_hash,
        "config_file_sha256": expected_config_file_hash,
        "workload_sha256": expected_workload_hash,
        "environment_attestation_sha256": expected_environment_hash,
        "matrix_cell_count": 15,
        "repetition_count": 450,
        "operation_sample_count": 14_400,
    }
    for field, value in expected.items():
        if material.get(field) != value:
            raise CollectorError(f"candidate material {field} mismatch")
    for field in (
        "evidence_manifest_sha256",
        "candidate_operation_samples_sha256",
        "candidate_repetitions_sha256",
    ):
        _validate_hash(material.get(field), field)
    versions = material.get("observed_service_versions")
    if not isinstance(versions, Mapping) or set(versions) != {
        "qdrant",
        "neo4j",
        "toxiproxy",
    }:
        raise CollectorError("candidate service versions are incomplete")
    for role, version in versions.items():
        if not is_registered_service_version(str(role), version):
            raise CollectorError("candidate service version is not registered")
    return dict(material)


def _validate_candidate_receipt_for_sealing(
    receipt: Any,
    *,
    expected_candidate_id: str,
    expected_run_hash: str,
    expected_config_hash: str,
    expected_config_file_hash: str,
    expected_workload_hash: str,
    expected_environment_hash: str,
    progress_blocker: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Validate the decoded receipt before sealing and block on any mismatch."""

    try:
        return _validate_candidate_material(
            receipt,
            expected_candidate_id=expected_candidate_id,
            expected_run_hash=expected_run_hash,
            expected_config_hash=expected_config_hash,
            expected_config_file_hash=expected_config_file_hash,
            expected_workload_hash=expected_workload_hash,
            expected_environment_hash=expected_environment_hash,
        )
    except BaseException:
        _attempt_progress_blocker(progress_blocker)
        raise


def _collect_execution_evidence(
    *,
    project_root: Path,
    candidate_root: Path,
    launch_path: Path,
    completion_path: Path,
    run_id: str,
    transport: str,
    config_sha256: str,
    config_file_sha256: str,
    workload_sha256: str,
    environment_attestation_sha256: str,
    command_manifest: Mapping[str, Any],
    child_process: Mapping[str, Any],
    authorization_nonce: bytes,
    network_guard_activate: Callable[[], Mapping[str, Any]],
    network_guard_finalize: Callable[[], Mapping[str, Any]],
    network_guard_deactivate: Callable[[], None],
    execution_monitor_start: Callable[[], None],
    execution_monitor_finalize: Callable[[], Mapping[str, Any]],
    run_candidate: Callable[[], tuple[int, Mapping[str, Any]]],
    candidate_sealer: Callable[[Path, Mapping[str, Any]], Mapping[str, Any]],
    topology_probe: Callable[[str], Mapping[str, Any]],
    source_identity_loader: Callable[[Path], Mapping[str, Any]],
    external_tool_identity_loader: Callable[[], Sequence[Mapping[str, Any]]],
    runtime_identity_loader: Callable[[], Mapping[str, Any]],
    candidate_material_loader: Callable[[Path, str], Mapping[str, Any]],
    progress_blocker: Callable[[], None] | None = None,
    progress_completer: Callable[[], Mapping[str, Any]] | None = None,
    interruption_check: Callable[[], None] | None = None,
) -> tuple[Path, Path]:
    """Write launch before execution and completion after exact candidate validation."""

    if interruption_check is not None and not callable(interruption_check):
        raise CollectorError("collector interruption check is invalid")

    def check_interruption() -> None:
        if interruption_check is not None:
            interruption_check()

    check_interruption()

    if not isinstance(run_id, str) or not run_id:
        raise CollectorError("run_id must be non-empty")
    if transport not in {
        "local_loopback",
        "ssh_local_port_forward",
        "direct_private_network",
        "container_bridge",
    }:
        raise CollectorError("unsupported collector transport")
    config_hash = _validate_hash(config_sha256, "config hash")
    config_file_hash = _validate_hash(config_file_sha256, "config-file hash")
    workload_hash = _validate_hash(workload_sha256, "workload hash")
    environment_hash = _validate_hash(
        environment_attestation_sha256, "environment hash"
    )
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    candidate_id = provenance_bundle_id(
        config_sha256=config_hash,
        run_id_sha256=run_hash,
        formal=False,
        backend="vector-graph",
    )
    launch_store, launch_name, completion_store, completion_name = (
        _preflight_external_outputs(
            project_root, candidate_root, launch_path, completion_path
        )
    )
    if not isinstance(authorization_nonce, bytes) or len(authorization_nonce) < 32:
        raise CollectorError("authorization nonce must contain at least 32 bytes")
    nonce_hash = hashlib.sha256(authorization_nonce).hexdigest()
    if FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(run_hash) != nonce_hash:
        raise CollectorError("launch authorization nonce is not pre-registered")

    source_before = _source_identity_fields(source_identity_loader(project_root))
    command_document = {
        "run_id_sha256": run_hash,
        "config_file_sha256": config_file_hash,
        "environment_attestation_sha256": environment_hash,
        "transport": transport,
        **source_before,
    }
    try:
        validated_command = _validate_command_manifest(
            command_manifest, command_document
        )
        validated_child, _sanitized_child = _validate_child_process(
            child_process, validated_command
        )
        external_tools_before = _validate_external_tools(
            list(external_tool_identity_loader())
        )
        runtime_before = _validate_runtime_manifest(runtime_identity_loader())
    except ValueError as exc:
        raise CollectorError("formal candidate command is invalid") from exc
    if runtime_before != validated_command["runtime_manifest"]:
        raise CollectorError("formal runtime snapshot does not match command")
    if external_tools_before != validated_command["external_tools"]:
        raise CollectorError("formal external tool closure does not match command")
    command_hash = hashlib.sha256(
        canonical_json_bytes(validated_command)
    ).hexdigest()
    check_interruption()
    (
        roles_before,
        proxy_routes_before,
        baseline_a,
        backend_isolation_before,
    ) = _snapshot_components(topology_probe("before"))
    try:
        baseline_a = validate_proxy_counter_snapshot(
            baseline_a, expected_phase="baseline_a"
        )
    except ToxiproxyMetricsError as exc:
        raise CollectorError("formal baseline A proxy counters are invalid") from exc
    check_interruption()
    try:
        raw_activation = network_guard_activate()
        if (
            not isinstance(raw_activation, Mapping)
            or set(raw_activation)
            != {
                "network_guard",
                "proxy_routes",
                "proxy_counters",
                "route_rearmed",
            }
            or not isinstance(raw_activation.get("proxy_routes"), list)
            or not isinstance(raw_activation.get("proxy_counters"), Mapping)
            or raw_activation.get("route_rearmed") is not True
        ):
            raise CollectorError("formal network guard activation is invalid")
        try:
            network_guard_before = _validate_network_guard_attestation(
                raw_activation["network_guard"]
            )
            baseline_b = validate_proxy_counter_snapshot(
                raw_activation["proxy_counters"], expected_phase="baseline_b"
            )
        except ValueError as exc:
            raise CollectorError("formal network guard activation is invalid") from exc
        proxy_routes_boundary = [
            dict(row) for row in raw_activation["proxy_routes"]
        ]
        _validate_toxiproxy_attribution_boundary(
            baseline_a,
            baseline_b,
            proxy_routes_before,
            proxy_routes_boundary,
        )
        for field in (
            "backend_ipv4_subnet_sha256",
            "ingress_ipv4_subnet_sha256",
            "backend_bridge_interface_sha256",
            "ingress_bridge_interface_sha256",
        ):
            if network_guard_before.get(field) != backend_isolation_before.get(
                field
            ):
                raise CollectorError(
                    "formal network guard is not bound to backend isolation"
                )
        shared_before = {
            "collector_id": COLLECTOR_ID,
            "formal_execution_requested": True,
            "run_id_sha256": run_hash,
            "config_sha256": config_hash,
            "config_file_sha256": config_file_hash,
            "workload_sha256": workload_hash,
            "environment_attestation_sha256": environment_hash,
            **source_before,
            "command_manifest": validated_command,
            "command_sha256": command_hash,
            "child_process": validated_child,
            "network_guard": network_guard_before,
            "backend_isolation": backend_isolation_before,
            "transport": transport,
            "matrix_cell_count": 15,
            "repetition_count": 450,
            "operation_sample_count": 14_400,
        }
        launch = {
            "schema": RAW_LAUNCH_SCHEMA,
            **shared_before,
            "roles": roles_before,
            "proxy_routes": proxy_routes_boundary,
            "proxy_counter_baseline_a": baseline_a,
            "proxy_counter_baseline_b": baseline_b,
            "proxy_route_rearm_verified": True,
            "authorization_nonce_sha256": nonce_hash,
        }
        launch["authorization_proof_sha256"] = execution_authorization_proof(
            authorization_nonce, launch
        )
        check_interruption()
        launch_store.write_json_exclusive(
            launch_name, payload=launch, mode=0o600
        )
        launch_raw = launch_store.load_bytes(launch_name)
        launch_hash = hashlib.sha256(launch_raw).hexdigest()
        check_interruption()
    except BaseException:
        raise

    run_failure: BaseException | None = None
    run_result: Any = None
    guard_after: dict[str, Any] | None = None
    final_proxy_routes: list[dict[str, Any]] | None = None
    final_counters: dict[str, Any] | None = None
    proxy_deltas: dict[str, Any] | None = None
    monitor_after: dict[str, Any] | None = None
    monitor_started = False
    monitor_failure: BaseException | None = None
    guard_failure: BaseException | None = None
    cleanup_failure: BaseException | None = None
    check_interruption()
    try:
        execution_monitor_start()
        monitor_started = True
    except BaseException as exc:
        monitor_failure = exc
    if monitor_failure is None:
        try:
            check_interruption()
            run_result = run_candidate()
            check_interruption()
        except BaseException as exc:
            run_failure = exc
    if run_failure is not None:
        raise run_failure
    if monitor_failure is not None:
        raise CollectorError(
            "formal execution integrity monitor failed"
        ) from monitor_failure
    if monitor_started:
        try:
            check_interruption()
            raw_monitor = execution_monitor_finalize()
            _validate_execution_monitor_attestation(raw_monitor)
            monitor_after = dict(raw_monitor)
        except BaseException as exc:
            monitor_failure = exc
    try:
        check_interruption()
        raw_finalization = network_guard_finalize()
        if (
            not isinstance(raw_finalization, Mapping)
            or set(raw_finalization)
            != {"network_guard", "proxy_routes", "proxy_counters"}
            or not isinstance(raw_finalization.get("proxy_routes"), list)
            or not isinstance(raw_finalization.get("proxy_counters"), Mapping)
        ):
            raise CollectorError("formal network guard finalization is invalid")
        final_proxy_routes = [
            dict(row) for row in raw_finalization["proxy_routes"]
        ]
        if final_proxy_routes != proxy_routes_boundary:
            raise CollectorError(
                "formal Toxiproxy routes changed during the attribution window"
            )
        final_counters = validate_proxy_counter_snapshot(
            raw_finalization["proxy_counters"], expected_phase="final"
        )
        proxy_deltas = _derive_toxiproxy_attribution_deltas(
            baseline_b, final_counters
        )
        guard_after = _validate_network_guard_attestation(
            raw_finalization["network_guard"]
        )
    except BaseException as exc:
        guard_failure = exc
    check_interruption()
    try:
        network_guard_deactivate()
    except BaseException as exc:
        cleanup_failure = exc
    if monitor_failure is not None:
        raise CollectorError("formal execution integrity monitor failed") from monitor_failure
    if guard_failure is not None:
        raise CollectorError("formal network guard finalization failed") from guard_failure
    if cleanup_failure is not None:
        raise CollectorError("formal network guard cleanup failed") from cleanup_failure
    check_interruption()
    if guard_after != network_guard_before:
        raise CollectorError("formal network guard changed during execution")
    if (
        final_proxy_routes is None
        or final_counters is None
        or proxy_deltas is None
    ):
        raise CollectorError("formal Toxiproxy final evidence is unavailable")
    if monitor_after is None:
        raise CollectorError("formal execution integrity monitor is unavailable")
    if (
        not isinstance(run_result, tuple)
        or len(run_result) != 2
        or type(run_result[0]) is not int
        or not isinstance(run_result[1], Mapping)
    ):
        raise CollectorError("candidate runner returned an invalid completion result")
    exit_code, raw_receipt = run_result
    candidate_seal: dict[str, Any]
    receipt_material: dict[str, Any] | None = None
    if exit_code == 0:
        check_interruption()
        receipt_material = _validate_candidate_receipt_for_sealing(
            raw_receipt,
            expected_candidate_id=candidate_id,
            expected_run_hash=run_hash,
            expected_config_hash=config_hash,
            expected_config_file_hash=config_file_hash,
            expected_workload_hash=workload_hash,
            expected_environment_hash=environment_hash,
            progress_blocker=progress_blocker,
        )
        if progress_completer is not None:
            def complete_checked_progress() -> Mapping[str, Any]:
                check_interruption()
                return progress_completer()

            _complete_progress_terminal(
                complete_checked_progress,
                progress_blocker,
            )
        try:
            check_interruption()
            candidate_seal, _sanitized_seal = _validate_candidate_seal(
                candidate_sealer(candidate_root, receipt_material),
                expected_completion_receipt_sha256=hashlib.sha256(
                    canonical_json_bytes(receipt_material)
                ).hexdigest(),
            )
        except ValueError as exc:
            raise CollectorError("candidate output could not be sealed") from exc
    else:
        candidate_seal = {
            "schema": "txnmem-provenance-candidate-seal-v1",
            "root_device": 0,
            "root_inode": 1,
            "directory_count": 1,
            "file_count": 1,
            "tree_sha256": "0" * 64,
            "completion_receipt_sha256": "0" * 64,
        }
    check_interruption()
    source_after = _source_identity_fields(source_identity_loader(project_root))
    if source_after != source_before:
        raise CollectorError("formal source identity changed during execution")
    try:
        external_tools_after = _validate_external_tools(
            list(external_tool_identity_loader())
        )
        runtime_after = _validate_runtime_manifest(runtime_identity_loader())
    except ValueError as exc:
        raise CollectorError("formal runtime snapshot became invalid") from exc
    if runtime_after != runtime_before:
        raise CollectorError("formal runtime snapshot changed during execution")
    if external_tools_after != external_tools_before:
        raise CollectorError("formal external tool closure changed during execution")
    check_interruption()
    (
        roles_after,
        proxy_routes_after,
        topology_final_counters,
        backend_isolation_after,
    ) = _snapshot_components(topology_probe("after"))
    if backend_isolation_after != backend_isolation_before:
        raise CollectorError("formal backend isolation changed during execution")
    if proxy_routes_after != final_proxy_routes:
        raise CollectorError("formal final proxy routes changed after guard cleanup")
    if topology_final_counters != final_counters:
        raise CollectorError("formal final proxy counters changed after capture")
    material: dict[str, Any]
    if exit_code == 0:
        check_interruption()
        material = _validate_candidate_material(
            candidate_material_loader(candidate_root, candidate_id),
            expected_candidate_id=candidate_id,
            expected_run_hash=run_hash,
            expected_config_hash=config_hash,
            expected_config_file_hash=config_file_hash,
            expected_workload_hash=workload_hash,
            expected_environment_hash=environment_hash,
        )
        if material != receipt_material:
            raise CollectorError(
                "sealed candidate bytes differ from the child completion receipt"
            )
    else:
        material = {
            "evidence_manifest_sha256": "0" * 64,
            "candidate_operation_samples_sha256": "0" * 64,
            "candidate_repetitions_sha256": "0" * 64,
        }
    shared_after = {
        "collector_id": COLLECTOR_ID,
        "formal_execution_requested": True,
        "run_id_sha256": run_hash,
        "config_sha256": config_hash,
        "config_file_sha256": config_file_hash,
        "workload_sha256": workload_hash,
        "environment_attestation_sha256": environment_hash,
        **source_after,
        "command_manifest": validated_command,
        "command_sha256": command_hash,
        "child_process": validated_child,
        "network_guard": network_guard_before,
        "backend_isolation": backend_isolation_after,
        "transport": transport,
        "matrix_cell_count": 15,
        "repetition_count": 450,
        "operation_sample_count": 14_400,
    }
    completion = {
        "schema": RAW_COMPLETION_SCHEMA,
        **shared_after,
        "launch_file_sha256": launch_hash,
        "exit_code": exit_code,
        "candidate_bundle_id": candidate_id,
        "evidence_manifest_sha256": material["evidence_manifest_sha256"],
        "candidate_operation_samples_sha256": material[
            "candidate_operation_samples_sha256"
        ],
        "candidate_repetitions_sha256": material[
            "candidate_repetitions_sha256"
        ],
        "candidate_seal": candidate_seal,
        "execution_monitor": monitor_after,
        "roles": roles_after,
        "proxy_routes": final_proxy_routes,
        "proxy_counter_baseline_b_sha256": proxy_counter_payload_sha256(
            baseline_b
        ),
        "proxy_counter_final": final_counters,
        "proxy_counter_deltas": proxy_deltas,
        "authorization_nonce_sha256": nonce_hash,
    }
    completion["authorization_proof_sha256"] = execution_authorization_proof(
        authorization_nonce, completion
    )
    check_interruption()
    completion_store.write_json_exclusive(
        completion_name, payload=completion, mode=0o600
    )
    if exit_code != 0:
        raise CollectorError("candidate measurement process failed")
    return launch_store.path(launch_name), completion_store.path(completion_name)


_FORMAL_PROXY_SPECS = {
    "qdrant": {
        "name": "txnmem-qdrant",
        "listen": "0.0.0.0:19000",
        "upstream": "qdrant:6333",
    },
    "neo4j": {
        "name": "txnmem-neo4j",
        "listen": "0.0.0.0:19001",
        "upstream": "neo4j:7687",
    },
}


def _normalize_toxiproxy_proxy(value: Any, *, role: str) -> dict[str, Any]:
    spec = _FORMAL_PROXY_SPECS.get(role)
    if spec is None:
        raise CollectorError("formal proxy role is unsupported")
    expected_listen = str(spec["listen"])
    expected_port = expected_listen.rsplit(":", 1)[1]
    observed_listen = value.get("listen") if isinstance(value, Mapping) else None
    accepted_listens = {expected_listen, f"[::]:{expected_port}"}
    required_keys = {"name", "listen", "upstream", "enabled", "toxics"}
    observed_keys = set(value) if isinstance(value, Mapping) else set()
    logger_extension_valid = observed_keys == required_keys or (
        observed_keys == required_keys | {"Logger"}
        and isinstance(value.get("Logger"), dict)
        and not value["Logger"]
    )
    if (
        not isinstance(value, Mapping)
        or not logger_extension_valid
        or value.get("name") != spec["name"]
        or not isinstance(observed_listen, str)
        or observed_listen not in accepted_listens
        or value.get("upstream") != spec["upstream"]
        or value.get("enabled") is not True
        or not isinstance(value.get("toxics"), list)
        or value["toxics"]
    ):
        raise CollectorError("formal Toxiproxy route configuration drifted")
    return {
        "role": role,
        "proxy_name": spec["name"],
        "listen": spec["listen"],
        "upstream": spec["upstream"],
        "enabled": True,
        "toxics_count": 0,
    }


def _toxiproxy_json_request(
    base_url: str,
    path: str,
    *,
    method: str,
    payload: Mapping[str, Any] | None = None,
    allow_not_found: bool = False,
) -> Any:
    url = base_url.rstrip("/") + path
    data = canonical_json_bytes(dict(payload)) if payload is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(request, timeout=10.0) as response:
            status = int(response.status)
            body = response.read()
    except urllib.error.HTTPError as exc:
        if allow_not_found and exc.code == 404:
            return None
        raise CollectorError("Toxiproxy management request failed") from exc
    except Exception as exc:
        raise CollectorError("Toxiproxy management request failed") from exc
    if status < 200 or status >= 300:
        raise CollectorError("Toxiproxy management returned an unsafe status")
    if method == "DELETE":
        return None
    return _strict_json_bytes(body, "Toxiproxy management response")


def observe_formal_toxiproxy_routes(
    toxiproxy_url: str, *, qdrant_proxy: str, neo4j_proxy: str
) -> list[dict[str, Any]]:
    if (
        qdrant_proxy != _FORMAL_PROXY_SPECS["qdrant"]["name"]
        or neo4j_proxy != _FORMAL_PROXY_SPECS["neo4j"]["name"]
    ):
        raise CollectorError("formal proxy names are not source-registered")
    routes = []
    for role, name in (("qdrant", qdrant_proxy), ("neo4j", neo4j_proxy)):
        document = _toxiproxy_json_request(
            toxiproxy_url,
            "/proxies/" + urllib.parse.quote(name, safe=""),
            method="GET",
        )
        routes.append(_normalize_toxiproxy_proxy(document, role=role))
    return routes


def prepare_isolated_toxiproxy_routes(
    toxiproxy_url: str, *, qdrant_proxy: str, neo4j_proxy: str
) -> list[dict[str, Any]]:
    _formal_endpoint_port(
        toxiproxy_url,
        field="Toxiproxy management endpoint",
        schemes={"http"},
        expected_port=8474,
    )
    if (
        qdrant_proxy != _FORMAL_PROXY_SPECS["qdrant"]["name"]
        or neo4j_proxy != _FORMAL_PROXY_SPECS["neo4j"]["name"]
    ):
        raise CollectorError("formal proxy names are not source-registered")
    for role in ("qdrant", "neo4j"):
        spec = _FORMAL_PROXY_SPECS[role]
        path = "/proxies/" + urllib.parse.quote(str(spec["name"]), safe="")
        _toxiproxy_json_request(
            toxiproxy_url, path, method="DELETE", allow_not_found=True
        )
        created = _toxiproxy_json_request(
            toxiproxy_url,
            "/proxies",
            method="POST",
            payload={
                "name": spec["name"],
                "listen": spec["listen"],
                "upstream": spec["upstream"],
                "enabled": True,
            },
        )
        _normalize_toxiproxy_proxy(created, role=role)
    return observe_formal_toxiproxy_routes(
        toxiproxy_url,
        qdrant_proxy=qdrant_proxy,
        neo4j_proxy=neo4j_proxy,
    )


def attest_committed_source(
    project_root: Path,
    *,
    expected_commit: str | None = None,
    expected_source_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind every formal producer to exact blobs in one Git commit."""

    if (expected_commit is None) != (expected_source_manifest is None):
        raise CollectorError("approved source identity is incomplete")
    if expected_source_manifest is not None and not isinstance(
        expected_source_manifest, Mapping
    ):
        raise CollectorError("approved source manifest is invalid")
    if expected_commit is not None and (
        not isinstance(expected_commit, str)
        or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", expected_commit)
    ):
        raise CollectorError("approved source commit is invalid")
    root = project_root.expanduser().absolute().resolve(strict=True)
    try:
        commit = subprocess.run(
            [
                _FORMAL_GIT_EXECUTABLE,
                "-c",
                f"safe.directory={root}",
                "rev-parse",
                "HEAD",
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CollectorError("cannot resolve formal source commit") from exc
    if not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", commit):
        raise CollectorError("formal source commit has an invalid object id")
    if expected_commit is not None:
        if commit != expected_commit:
            raise CollectorError("repository HEAD changed from the approved commit")
        context = _validate_formal_controller_context(
            {
                "schema": _FORMAL_CONTROLLER_CONTEXT_SCHEMA,
                "source_commit": expected_commit,
                "source_manifest": dict(expected_source_manifest),
                "approval_manifest_sha256": hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "schema": _FORMAL_APPROVAL_SCHEMA,
                            "source_commit": expected_commit,
                            "files": list(expected_source_manifest.get("files", [])),
                        }
                    )
                ).hexdigest(),
            }
        )
        source_paths = [
            str(row["path"])
            for row in context["source_manifest"]["files"]
        ]
    elif _SOURCE_PATHS_FOR_TESTS is None:
        try:
            tracked_output = subprocess.run(
                [
                    _FORMAL_GIT_EXECUTABLE,
                    "-c",
                    f"safe.directory={root}",
                    "ls-tree",
                    "-r",
                    "--name-only",
                    commit,
                    "--",
                    "src",
                ],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            raise CollectorError("cannot enumerate committed source closure") from exc
        source_paths = sorted(
            set(_REQUIRED_SOURCE_PATHS)
            | {
                line.strip()
                for line in tracked_output.splitlines()
                if line.strip().startswith("src/") and line.strip().endswith(".py")
            }
        )
    else:
        source_paths = sorted(_SOURCE_PATHS_FOR_TESTS)
    if not source_paths:
        raise CollectorError("formal source closure is empty")
    entries = []
    for relative in source_paths:
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise CollectorError("formal source file is missing or linked")
        current = path.read_bytes()
        try:
            committed = subprocess.run(
                [
                    _FORMAL_GIT_EXECUTABLE,
                    "-c",
                    f"safe.directory={root}",
                    "show",
                    f"{commit}:{relative}",
                ],
                cwd=root,
                check=True,
                capture_output=True,
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            raise CollectorError("formal source file is absent from the commit") from exc
        if current != committed:
            raise CollectorError("formal source file differs from committed bytes")
        entries.append(
            {"path": relative, "blob_sha256": hashlib.sha256(current).hexdigest()}
        )
    manifest = {
        "schema": "txnmem-provenance-source-manifest-v1",
        "source_commit": commit,
        "files": entries,
    }
    if expected_source_manifest is not None and manifest != dict(
        expected_source_manifest
    ):
        raise CollectorError("committed source differs from controller approval")
    return {
        "source_commit": commit,
        "source_manifest": manifest,
        "source_manifest_sha256": hashlib.sha256(
            canonical_json_bytes(manifest)
        ).hexdigest(),
        "collector_sha256": next(
            row["blob_sha256"]
            for row in entries
            if row["path"] == "src/txnmem_provenance_execution_collector.py"
        ),
        "runner_sha256": next(
            row["blob_sha256"]
            for row in entries
            if row["path"] == "src/txnmem_provenance_runner.py"
        ),
    }


def create_immutable_source_export(
    project_root: Path,
    private_parent: Path,
    source_identity: Mapping[str, Any],
) -> Path:
    """Create a detached, read-only export of every attested source blob."""

    root = project_root.expanduser().absolute().resolve(strict=True)
    parent = _private_directory(private_parent, "source export")
    if _is_within(parent, root):
        raise CollectorError("source export parent must be private and outside repository")
    identity = _source_identity_fields(source_identity)
    manifest = identity["source_manifest"]
    export = parent / f"source-{secrets.token_hex(16)}"
    try:
        export.mkdir(mode=0o700)
        directories: set[Path] = {export}
        for row in manifest["files"]:
            relative = str(row["path"])
            target = export / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            directories.update(
                parent_path
                for parent_path in target.parents
                if parent_path == export or _is_within(parent_path, export)
            )
            committed = subprocess.run(
                [
                    _FORMAL_GIT_EXECUTABLE,
                    "-c",
                    f"safe.directory={root}",
                    "show",
                    f"{identity['source_commit']}:{relative}",
                ],
                cwd=root,
                check=True,
                capture_output=True,
            ).stdout
            if hashlib.sha256(committed).hexdigest() != row["blob_sha256"]:
                raise CollectorError("detached source blob hash mismatch")
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o400,
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(committed)
                stream.flush()
        for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
            directory.chmod(0o500)
    except BaseException:
        if export.exists():
            for path in sorted(export.rglob("*"), key=lambda item: len(item.parts)):
                if path.is_dir() and not path.is_symlink():
                    path.chmod(0o700)
                elif path.is_file() and not path.is_symlink():
                    path.chmod(0o600)
            export.chmod(0o700)
            shutil.rmtree(export)
        raise
    return export


def _create_immutable_input_snapshot(
    source: Path,
    private_parent: Path,
    *,
    expected_file_sha256: str,
    label: str,
) -> Path:
    expected_hash = _validate_hash(expected_file_sha256, f"{label} input")
    if not isinstance(label, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", label):
        raise CollectorError("immutable input label is unsafe")
    parent = _private_directory(private_parent, "immutable input")
    input_path = source.expanduser().absolute()
    if input_path.is_symlink() or not input_path.is_file():
        raise CollectorError("immutable input source must be a real file")
    descriptor = os.open(
        input_path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise CollectorError("immutable input source is not regular")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            raw = stream.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if hashlib.sha256(raw).hexdigest() != expected_hash:
        raise CollectorError(f"{label} input file hash changed")
    destination = parent / f"input-{label}-{secrets.token_hex(16)}.json"
    output = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        with os.fdopen(output, "wb") as stream:
            output = -1
            stream.write(raw)
            stream.flush()
    finally:
        if output >= 0:
            os.close(output)
    return destination


def _http_read(url: str, *, timeout: float = 10.0) -> tuple[bytes, float]:
    started = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read()
    except Exception as exc:
        raise CollectorError("service observation failed") from exc
    elapsed_ms = max(0.0, (time.perf_counter_ns() - started) / 1_000_000.0)
    return body, elapsed_ms


def _linux_memory_total_bytes(*, proc_root: Path = Path("/proc")) -> int:
    try:
        lines = (proc_root / "meminfo").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise CollectorError("Linux memory inventory is unavailable") from exc
    matches = [line for line in lines if line.startswith("MemTotal:")]
    if len(matches) != 1:
        raise CollectorError("Linux memory inventory is malformed")
    fields = matches[0].split()
    if len(fields) != 3 or not fields[1].isdigit() or fields[2] != "kB":
        raise CollectorError("Linux memory inventory is malformed")
    total = int(fields[1]) * 1024
    if total <= 0:
        raise CollectorError("Linux memory inventory is malformed")
    return total


def _linux_load1_milli(*, proc_root: Path = Path("/proc")) -> int:
    try:
        token = (proc_root / "loadavg").read_text(encoding="utf-8").split()[0]
        load = float(token)
    except (OSError, UnicodeError, IndexError, ValueError) as exc:
        raise CollectorError("Linux load observation is unavailable") from exc
    if not math.isfinite(load) or load < 0.0:
        raise CollectorError("Linux load observation is malformed")
    return int(round(load * 1000.0))


def _read_linux_cpu_totals(*, proc_root: Path = Path("/proc")) -> tuple[int, int]:
    try:
        line = (proc_root / "stat").read_text(encoding="utf-8").splitlines()[0]
        fields = line.split()
        values = [int(value) for value in fields[1:]]
    except (OSError, UnicodeError, IndexError, ValueError) as exc:
        raise CollectorError("Linux CPU accounting is unavailable") from exc
    if fields[0] != "cpu" or len(values) < 5 or any(value < 0 for value in values):
        raise CollectorError("Linux CPU accounting is malformed")
    return sum(values), values[3] + values[4]


def _measure_background_cpu_busy_permille(
    *,
    proc_root: Path = Path("/proc"),
    interval_seconds: float = 0.25,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if (
        isinstance(interval_seconds, bool)
        or not isinstance(interval_seconds, (int, float))
        or not math.isfinite(float(interval_seconds))
        or float(interval_seconds) <= 0.0
    ):
        raise CollectorError("CPU observation interval is invalid")
    total_before, idle_before = _read_linux_cpu_totals(proc_root=proc_root)
    sleep(float(interval_seconds))
    total_after, idle_after = _read_linux_cpu_totals(proc_root=proc_root)
    total_delta = total_after - total_before
    idle_delta = idle_after - idle_before
    if total_delta <= 0 or idle_delta < 0 or idle_delta > total_delta:
        raise CollectorError("Linux CPU accounting moved backwards")
    return int(round(1000.0 * (total_delta - idle_delta) / total_delta))


def _linux_disk_medium(
    storage_path: Path,
    *,
    sys_dev_block_root: Path = Path("/sys/dev/block"),
) -> str:
    target = storage_path.expanduser().absolute().resolve(strict=True)
    try:
        device = target.stat().st_dev
        device_link = sys_dev_block_root / f"{os.major(device)}:{os.minor(device)}"
        resolved = device_link.resolve(strict=True)
    except OSError as exc:
        raise CollectorError("Linux storage identity is unavailable") from exc
    candidates = [resolved, *resolved.parents]
    rotational_path = next(
        (candidate / "queue" / "rotational" for candidate in candidates if (candidate / "queue" / "rotational").is_file()),
        None,
    )
    if rotational_path is None:
        raise CollectorError("Linux storage medium is unavailable")
    try:
        rotational = rotational_path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise CollectorError("Linux storage medium is unavailable") from exc
    if rotational == "1":
        return "hdd"
    if rotational != "0":
        raise CollectorError("Linux storage medium is malformed")
    return "nvme" if any(part.startswith("nvme") for part in resolved.parts) else "ssd"


def _parse_toxiproxy_version(payload: bytes) -> str:
    try:
        text = payload.decode("utf-8")
    except (AttributeError, UnicodeError) as exc:
        raise CollectorError("Toxiproxy version response is malformed") from exc
    try:
        parsed = _strict_json_bytes(payload, "Toxiproxy version response")
    except CollectorError as exc:
        if is_registered_service_version("toxiproxy", text):
            return text
        raise CollectorError(
            "observed Toxiproxy version is not registered"
        ) from exc
    if isinstance(parsed, Mapping):
        version = parsed.get("version")
    else:
        version = parsed
    if not is_registered_service_version("toxiproxy", version):
        raise CollectorError("observed Toxiproxy version is not registered")
    return str(version)


def _collect_formal_environment_attestation(
    *, toxiproxy_url: str, storage_path: Path
) -> dict[str, Any]:
    """Collect the formal environment inside the root controller boundary."""

    cpu_count = os.cpu_count()
    if type(cpu_count) is not int or cpu_count <= 0:
        raise CollectorError("logical CPU inventory is unavailable")
    busy_permille = _measure_background_cpu_busy_permille()
    if busy_permille > _FORMAL_BACKGROUND_CPU_BUSY_LIMIT_PERMILLE:
        raise CollectorError("co-tenant load detected before formal execution")
    body, _rtt = _http_read(toxiproxy_url.rstrip("/") + "/version")
    document = {
        "schema": "txnmem-provenance-environment-v1",
        "isolation_verified": True,
        "co_tenant_load_detected": False,
        "source": "collector-observation-v2",
        "cpu_logical_count": cpu_count,
        "memory_total_bytes": _linux_memory_total_bytes(),
        "disk_medium": _linux_disk_medium(storage_path),
        "toxiproxy_version": _parse_toxiproxy_version(body),
    }
    try:
        return validate_environment_attestation(document)
    except ValueError as exc:
        raise CollectorError("collector environment observation is invalid") from exc


def _formal_host_environment_snapshot(storage_path: Path) -> dict[str, Any]:
    cpu_count = os.cpu_count()
    if type(cpu_count) is not int or cpu_count <= 0:
        raise CollectorError("logical CPU inventory is unavailable")
    return {
        "host_identity_sha256": hashlib.sha256(
            _host_identity().encode("utf-8")
        ).hexdigest(),
        "cpu_logical_count": cpu_count,
        "memory_total_bytes": _linux_memory_total_bytes(),
        "disk_medium": _linux_disk_medium(storage_path),
    }


def _write_collected_environment_snapshot(
    private_parent: Path, document: Mapping[str, Any]
) -> tuple[Path, bytes]:
    parent = _private_directory(private_parent, "collected environment")
    try:
        validated = validate_environment_attestation(document)
    except ValueError as exc:
        raise CollectorError("collector environment observation is invalid") from exc
    raw = canonical_json_bytes(validated) + b"\n"
    destination = parent / f"input-environment-{secrets.token_hex(16)}.json"
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return destination, raw


def _docker_owner(container_name: str) -> str:
    if not _SAFE_CONTAINER.fullmatch(container_name):
        raise CollectorError("container name is unsafe")
    try:
        result = subprocess.run(
            [
                _FORMAL_DOCKER_EXECUTABLE,
                "inspect",
                "--format",
                "{{.Id}}|{{.State.Pid}}|{{.State.StartedAt}}",
                container_name,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CollectorError("cannot observe service container identity") from exc
    if not result or len(result) > 512 or any(ord(char) < 32 for char in result):
        raise CollectorError("service container identity is malformed")
    return result


def _validate_formal_docker_network(
    network: Mapping[str, Any],
    *,
    expected_name: str,
    expected_internal: bool,
    role: str,
) -> tuple[str, ipaddress.IPv4Network, str]:
    network_id = network.get("Id") if isinstance(network, Mapping) else None
    if (
        not isinstance(network, Mapping)
        or network.get("Name") != expected_name
        or network.get("Driver") != "bridge"
        or network.get("Scope") != "local"
        or network.get("Internal") is not expected_internal
        or network.get("Attachable") is not False
        or network.get("Ingress") is not False
        or network.get("ConfigOnly") is not False
        or network.get("EnableIPv4") is not True
        or network.get("EnableIPv6") is not False
        or network.get("Options") != {}
        or not isinstance(network_id, str)
        or not re.fullmatch(r"[0-9a-f]{64}", network_id)
        or not isinstance(network.get("Containers"), Mapping)
    ):
        raise CollectorError(f"formal {role} Docker network type is invalid")

    ipam = network.get("IPAM")
    if (
        not isinstance(ipam, Mapping)
        or set(ipam) != {"Driver", "Options", "Config"}
        or ipam.get("Driver") != "default"
        or ipam.get("Options") not in (None, {})
        or not isinstance(ipam.get("Config"), list)
        or len(ipam["Config"]) != 1
        or not isinstance(ipam["Config"][0], Mapping)
        or set(ipam["Config"][0]) != {"Subnet", "IPRange", "Gateway"}
        or ipam["Config"][0].get("IPRange") != ""
        or not isinstance(ipam["Config"][0].get("Subnet"), str)
        or not isinstance(ipam["Config"][0].get("Gateway"), str)
    ):
        raise CollectorError(f"formal {role} Docker network IPAM is invalid")
    try:
        subnet = ipaddress.ip_network(ipam["Config"][0]["Subnet"], strict=True)
        gateway = ipaddress.ip_address(ipam["Config"][0]["Gateway"])
    except ValueError as exc:
        raise CollectorError(
            f"formal {role} Docker network IPAM is invalid"
        ) from exc
    if (
        not isinstance(subnet, ipaddress.IPv4Network)
        or not isinstance(gateway, ipaddress.IPv4Address)
        or gateway not in subnet
        or gateway in {subnet.network_address, subnet.broadcast_address}
    ):
        raise CollectorError(f"formal {role} Docker network IPAM is invalid")
    if not any(subnet.subnet_of(parent) for parent in _RFC1918_IPV4_NETWORKS):
        raise CollectorError(
            f"formal {role} Docker network requires a private RFC1918 subnet"
        )
    return network_id, subnet, f"br-{network_id[:12]}"


def _normalize_docker_network_guard_profile(
    backend_network: Mapping[str, Any],
    ingress_network: Mapping[str, Any],
    *,
    sys_class_net: Path = Path("/sys/class/net"),
) -> dict[str, str]:
    (
        _backend_network_id,
        backend_subnet,
        backend_interface,
    ) = _validate_formal_docker_network(
        backend_network,
        expected_name="txnmem-backend",
        expected_internal=True,
        role="backend",
    )
    (
        _ingress_network_id,
        ingress_subnet,
        ingress_interface,
    ) = _validate_formal_docker_network(
        ingress_network,
        expected_name="txnmem-ingress",
        expected_internal=False,
        role="ingress",
    )
    if backend_subnet.overlaps(ingress_subnet):
        raise CollectorError("formal Docker network IPv4 subnets overlap")
    for interface_name in (backend_interface, ingress_interface):
        if not (sys_class_net / interface_name / "bridge").is_dir():
            raise CollectorError("formal Docker bridge interface is unavailable")
    return {
        "backend_ipv4_subnet": str(backend_subnet),
        "ingress_ipv4_subnet": str(ingress_subnet),
        "backend_bridge_interface": backend_interface,
        "ingress_bridge_interface": ingress_interface,
    }


def _normalize_docker_backend_isolation(
    containers: Mapping[str, Any],
    backend_network: Mapping[str, Any],
    ingress_network: Mapping[str, Any],
) -> dict[str, Any]:
    roles = ("qdrant", "neo4j", "toxiproxy")
    if not isinstance(containers, Mapping) or set(containers) != set(roles):
        raise CollectorError("formal backend container closure is incomplete")
    (
        backend_network_id,
        backend_ipv4_subnet,
        backend_bridge_interface,
    ) = _validate_formal_docker_network(
        backend_network,
        expected_name="txnmem-backend",
        expected_internal=True,
        role="backend",
    )
    (
        ingress_network_id,
        ingress_ipv4_subnet,
        ingress_bridge_interface,
    ) = _validate_formal_docker_network(
        ingress_network,
        expected_name="txnmem-ingress",
        expected_internal=False,
        role="ingress",
    )
    if backend_ipv4_subnet.overlaps(ingress_ipv4_subnet):
        raise CollectorError("formal Docker network IPv4 subnets overlap")
    normalized_containers: list[dict[str, Any]] = []
    container_ids: set[str] = set()
    toxiproxy_container_id: str | None = None
    toxiproxy_ingress_attachment: Mapping[str, Any] | None = None
    for role in roles:
        row = containers[role]
        if not isinstance(row, Mapping):
            raise CollectorError(
                "formal backend container network or identity is invalid"
            )
        container_id = row.get("Id")
        runtime_image_id = row.get("Image")
        config = row.get("Config")
        settings = row.get("NetworkSettings")
        expected_manifest = FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[role]
        if (
            not isinstance(container_id, str)
            or not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or not isinstance(runtime_image_id, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", runtime_image_id)
            or not isinstance(config, Mapping)
            or not isinstance(config.get("Image"), str)
            or not str(config["Image"]).endswith("@sha256:" + expected_manifest)
            or not isinstance(settings, Mapping)
            or not isinstance(settings.get("Networks"), Mapping)
            or set(settings["Networks"])
            != (
                {"txnmem-backend", "txnmem-ingress"}
                if role == "toxiproxy"
                else {"txnmem-backend"}
            )
            or not isinstance(settings["Networks"]["txnmem-backend"], Mapping)
            or settings["Networks"]["txnmem-backend"].get("NetworkID")
            != backend_network_id
            or not isinstance(settings.get("Ports"), Mapping)
        ):
            raise CollectorError(
                "formal backend container network or identity is invalid"
            )
        if role == "toxiproxy" and (
            not isinstance(settings["Networks"]["txnmem-ingress"], Mapping)
            or settings["Networks"]["txnmem-ingress"].get("NetworkID")
            != ingress_network_id
        ):
            raise CollectorError("formal proxy ingress network identity is invalid")
        container_ids.add(container_id)
        if role == "toxiproxy":
            toxiproxy_container_id = container_id
            toxiproxy_ingress_attachment = settings["Networks"][
                "txnmem-ingress"
            ]
        normalized_containers.append(
            {
                "role": role,
                "container_id_sha256": hashlib.sha256(
                    container_id.encode("utf-8")
                ).hexdigest(),
                "runtime_image_id_sha256": hashlib.sha256(
                    runtime_image_id.encode("utf-8")
                ).hexdigest(),
                "manifest_digest": expected_manifest,
            }
        )
    backend_network_container_ids = {
        str(value) for value in backend_network["Containers"].keys()
    }
    if backend_network_container_ids != container_ids:
        raise CollectorError("formal backend Docker network membership drifted")
    ingress_network_container_ids = {
        str(value) for value in ingress_network["Containers"].keys()
    }
    if (
        toxiproxy_container_id is None
        or ingress_network_container_ids != {toxiproxy_container_id}
        or toxiproxy_ingress_attachment is None
    ):
        raise CollectorError("formal ingress network is not proxy-only")
    ingress_membership = ingress_network["Containers"].get(toxiproxy_container_id)
    if not isinstance(ingress_membership, Mapping):
        raise CollectorError("formal proxy ingress membership is invalid")
    endpoint_id = toxiproxy_ingress_attachment.get("EndpointID")
    container_address_text = toxiproxy_ingress_attachment.get("IPAddress")
    container_prefix = toxiproxy_ingress_attachment.get("IPPrefixLen")
    network_endpoint_id = ingress_membership.get("EndpointID")
    network_address_text = ingress_membership.get("IPv4Address")
    if (
        not isinstance(endpoint_id, str)
        or not re.fullmatch(r"[0-9a-f]{64}", endpoint_id)
        or network_endpoint_id != endpoint_id
        or not isinstance(container_address_text, str)
        or type(container_prefix) is not int
        or not 0 <= container_prefix <= 32
        or not isinstance(network_address_text, str)
    ):
        raise CollectorError("formal proxy ingress membership is invalid")
    try:
        ingress_address = ipaddress.IPv4Address(container_address_text)
        ingress_interface = ipaddress.IPv4Interface(network_address_text)
    except ValueError as exc:
        raise CollectorError("formal proxy ingress membership is invalid") from exc
    ingress_gateway = ipaddress.IPv4Address(
        ingress_network["IPAM"]["Config"][0]["Gateway"]
    )
    if (
        ingress_interface.ip != ingress_address
        or ingress_interface.network != ingress_ipv4_subnet
        or ingress_interface.network.prefixlen != container_prefix
        or ingress_address not in ingress_ipv4_subnet
        or ingress_address
        in {
            ingress_ipv4_subnet.network_address,
            ingress_ipv4_subnet.broadcast_address,
            ingress_gateway,
        }
    ):
        raise CollectorError("formal proxy ingress membership is invalid")

    for role in ("qdrant", "neo4j"):
        ports = containers[role]["NetworkSettings"]["Ports"]
        if any(value is not None and value != [] for value in ports.values()):
            raise CollectorError("direct backend port is published")
    expected_proxy_ports = {8474, 19000, 19001}
    proxy_ports = containers["toxiproxy"]["NetworkSettings"]["Ports"]
    observed_proxy_ports: set[int] = set()
    for container_port, bindings in proxy_ports.items():
        if not isinstance(container_port, str) or not container_port.endswith("/tcp"):
            raise CollectorError("formal proxy port mapping is invalid")
        try:
            numeric_container_port = int(container_port.split("/", 1)[0])
        except ValueError as exc:
            raise CollectorError("formal proxy port mapping is invalid") from exc
        if (
            numeric_container_port not in expected_proxy_ports
            or not isinstance(bindings, list)
            or len(bindings) != 1
            or not isinstance(bindings[0], Mapping)
            or bindings[0].get("HostIp") != "127.0.0.1"
            or bindings[0].get("HostPort") != str(numeric_container_port)
        ):
            raise CollectorError("formal proxy ports are not loopback-only")
        observed_proxy_ports.add(numeric_container_port)
    if observed_proxy_ports != expected_proxy_ports:
        raise CollectorError("formal proxy port closure is incomplete")
    return {
        "schema": "txnmem-provenance-backend-isolation-v3",
        "network_name_sha256": hashlib.sha256(
            b"txnmem-backend"
        ).hexdigest(),
        "network_id_sha256": hashlib.sha256(
            backend_network_id.encode("utf-8")
        ).hexdigest(),
        "ingress_network_name_sha256": hashlib.sha256(
            b"txnmem-ingress"
        ).hexdigest(),
        "ingress_network_id_sha256": hashlib.sha256(
            ingress_network_id.encode("utf-8")
        ).hexdigest(),
        "toxiproxy_ingress_ipv4": str(ingress_address),
        "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
            str(ingress_address).encode("utf-8")
        ).hexdigest(),
        "toxiproxy_ingress_endpoint_id_sha256": hashlib.sha256(
            endpoint_id.encode("utf-8")
        ).hexdigest(),
        "toxiproxy_ingress_membership_verified": True,
        "ingress_unique_workload_container_verified": True,
        "backend_network_internal": True,
        "ingress_network_external": True,
        "ingress_proxy_only": True,
        "backend_network_driver": "bridge",
        "ingress_network_driver": "bridge",
        "backend_network_scope": "local",
        "ingress_network_scope": "local",
        "network_driver_options_empty": True,
        "docker_default_ipam_driver_verified": True,
        "private_non_overlapping_ipv4_subnets_verified": True,
        "backend_ipv4_subnet_sha256": hashlib.sha256(
            str(backend_ipv4_subnet).encode("utf-8")
        ).hexdigest(),
        "ingress_ipv4_subnet_sha256": hashlib.sha256(
            str(ingress_ipv4_subnet).encode("utf-8")
        ).hexdigest(),
        "backend_bridge_interface_sha256": hashlib.sha256(
            backend_bridge_interface.encode("utf-8")
        ).hexdigest(),
        "ingress_bridge_interface_sha256": hashlib.sha256(
            ingress_bridge_interface.encode("utf-8")
        ).hexdigest(),
        "networks_non_attachable": True,
        "networks_non_swarm_ingress": True,
        "networks_non_config_only": True,
        "direct_backend_ports_unpublished": True,
        "proxy_ports_loopback_only": True,
        "published_proxy_ports": [8474, 19000, 19001],
        "containers": normalized_containers,
    }


def _validated_backend_ipv4_by_role(
    containers: Mapping[str, Any],
    backend_network: Mapping[str, Any],
    ingress_network: Mapping[str, Any],
) -> dict[str, str]:
    _normalize_docker_backend_isolation(
        containers, backend_network, ingress_network
    )
    (
        _backend_network_id,
        backend_subnet,
        _backend_bridge_interface,
    ) = _validate_formal_docker_network(
        backend_network,
        expected_name="txnmem-backend",
        expected_internal=True,
        role="backend",
    )
    (
        _ingress_network_id,
        ingress_subnet,
        _ingress_bridge_interface,
    ) = _validate_formal_docker_network(
        ingress_network,
        expected_name="txnmem-ingress",
        expected_internal=False,
        role="ingress",
    )
    addresses: dict[str, str] = {}
    for role, network_name, output_role, subnet, network in (
        ("qdrant", "txnmem-backend", "qdrant", backend_subnet, backend_network),
        ("neo4j", "txnmem-backend", "neo4j", backend_subnet, backend_network),
        (
            "toxiproxy",
            "txnmem-ingress",
            "toxiproxy_ingress",
            ingress_subnet,
            ingress_network,
        ),
    ):
        attachment = containers[role]["NetworkSettings"]["Networks"][network_name]
        membership = network["Containers"].get(containers[role]["Id"])
        if (
            not isinstance(attachment, Mapping)
            or not isinstance(membership, Mapping)
            or not isinstance(attachment.get("IPAddress"), str)
            or type(attachment.get("IPPrefixLen")) is not int
            or not isinstance(membership.get("IPv4Address"), str)
        ):
            raise CollectorError("formal backend address attachment is invalid")
        try:
            address = ipaddress.IPv4Address(attachment["IPAddress"])
            membership_interface = ipaddress.IPv4Interface(
                membership["IPv4Address"]
            )
            gateway = ipaddress.IPv4Address(network["IPAM"]["Config"][0]["Gateway"])
        except ValueError as exc:
            raise CollectorError("formal backend address attachment is invalid") from exc
        if (
            not 0 <= attachment["IPPrefixLen"] <= 32
            or membership_interface.ip != address
            or membership_interface.network != subnet
            or membership_interface.network.prefixlen != attachment["IPPrefixLen"]
            or address not in subnet
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address
            in {subnet.network_address, subnet.broadcast_address, gateway}
        ):
            raise CollectorError("formal backend address attachment is invalid")
        addresses[output_role] = str(address)
    return addresses


def _inspect_docker_backend_isolation_documents(
    *, qdrant_container: str, neo4j_container: str, toxiproxy_container: str
) -> tuple[dict[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    names = {
        "qdrant": qdrant_container,
        "neo4j": neo4j_container,
        "toxiproxy": toxiproxy_container,
    }
    if any(not _SAFE_CONTAINER.fullmatch(name) for name in names.values()):
        raise CollectorError("formal backend container name is unsafe")
    try:
        container_result = subprocess.run(
            [
                _FORMAL_DOCKER_EXECUTABLE,
                "inspect",
                names["qdrant"],
                names["neo4j"],
                names["toxiproxy"],
            ],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CollectorError("cannot inspect formal Docker isolation") from exc
    documents = _strict_json_bytes(
        container_result.stdout, "formal Docker container isolation"
    )
    if not isinstance(documents, list) or len(documents) != 3:
        raise CollectorError("formal Docker isolation output is incomplete")
    networks_by_name = _inspect_formal_docker_networks()
    return (
        {role: documents[index] for index, role in enumerate(names)},
        networks_by_name["txnmem-backend"],
        networks_by_name["txnmem-ingress"],
    )


def _collect_docker_backend_isolation(
    *, qdrant_container: str, neo4j_container: str, toxiproxy_container: str
) -> dict[str, Any]:
    containers, backend_network, ingress_network = (
        _inspect_docker_backend_isolation_documents(
            qdrant_container=qdrant_container,
            neo4j_container=neo4j_container,
            toxiproxy_container=toxiproxy_container,
        )
    )
    return _normalize_docker_backend_isolation(
        containers, backend_network, ingress_network
    )


def _inspect_formal_docker_networks() -> dict[str, Mapping[str, Any]]:
    try:
        network_result = subprocess.run(
            [
                _FORMAL_DOCKER_EXECUTABLE,
                "network",
                "inspect",
                "txnmem-backend",
                "txnmem-ingress",
            ],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CollectorError("cannot inspect formal Docker isolation") from exc
    networks = _strict_json_bytes(
        network_result.stdout, "formal Docker network isolation"
    )
    if not isinstance(networks, list) or len(networks) != 2:
        raise CollectorError("formal Docker isolation output is incomplete")
    networks_by_name: dict[str, Mapping[str, Any]] = {}
    for network in networks:
        if not isinstance(network, Mapping) or not isinstance(
            network.get("Name"), str
        ):
            raise CollectorError("formal Docker network identity is invalid")
        name = str(network["Name"])
        if name in networks_by_name:
            raise CollectorError("formal Docker network identity is duplicated")
        networks_by_name[name] = network
    if set(networks_by_name) != {"txnmem-backend", "txnmem-ingress"}:
        raise CollectorError("formal Docker network closure is incomplete")
    return networks_by_name


def _collect_docker_network_guard_profile(
    *, toxiproxy_container: str
) -> dict[str, str]:
    containers, backend_network, ingress_network = (
        _inspect_docker_backend_isolation_documents(
            qdrant_container=_FORMAL_QDRANT_CONTAINER,
            neo4j_container=_FORMAL_NEO4J_CONTAINER,
            toxiproxy_container=toxiproxy_container,
        )
    )
    raw_isolation = _normalize_docker_backend_isolation(
        containers, backend_network, ingress_network
    )
    ingress_address = raw_isolation["toxiproxy_ingress_ipv4"]
    if raw_isolation.get("toxiproxy_ingress_ipv4_sha256") != hashlib.sha256(
        ingress_address.encode("utf-8")
    ).hexdigest():
        raise CollectorError("formal proxy ingress identity hash is invalid")
    profile = _normalize_docker_network_guard_profile(
        backend_network, ingress_network
    )
    return {**profile, "toxiproxy_ingress_ipv4": ingress_address}


def _host_identity() -> str:
    machine_id = Path("/etc/machine-id")
    raw = machine_id.read_text(encoding="utf-8").strip() if machine_id.is_file() else ""
    identity = f"{platform.node()}|{raw}"
    if identity == "|" or len(identity) > 512:
        raise CollectorError("host identity is unavailable")
    return identity


def _observe_formal_child_process(
    pid: int,
    *,
    expected_command: Sequence[str],
    expected_uid: int,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    """Bind launch evidence to the Linux kernel's view of the gated child."""

    if type(pid) is not int or pid <= 0 or type(expected_uid) is not int:
        raise CollectorError("formal child process identity is invalid")
    command = tuple(expected_command)
    if not command or any(not isinstance(item, str) or not item for item in command):
        raise CollectorError("formal child expected command is invalid")
    process_root = proc_root.expanduser().absolute() / str(pid)
    try:
        executable_link = process_root / "exe"
        if not executable_link.is_symlink():
            raise CollectorError("formal child executable is not kernel observed")
        executable = executable_link.resolve(strict=True)
        command_line = _read_regular_file_bytes(
            process_root / "cmdline", "formal child command line"
        )
        status = (process_root / "status").read_text(encoding="utf-8")
        stat_line = (process_root / "stat").read_text(encoding="utf-8").strip()
    except CollectorError:
        raise
    except (OSError, UnicodeError) as exc:
        raise CollectorError("cannot observe formal child process") from exc
    expected_cmdline = b"\0".join(item.encode("utf-8") for item in command) + b"\0"
    if command_line != expected_cmdline:
        raise CollectorError("formal child command line mismatch")
    uid_lines = [line for line in status.splitlines() if line.startswith("Uid:")]
    if len(uid_lines) != 1:
        raise CollectorError("formal child UID is unavailable")
    try:
        observed_uids = [int(item) for item in uid_lines[0].split()[1:]]
    except ValueError as exc:
        raise CollectorError("formal child UID is malformed") from exc
    if len(observed_uids) != 4 or any(uid != expected_uid for uid in observed_uids):
        raise CollectorError("formal child UID mismatch")
    closing_parenthesis = stat_line.rfind(")")
    stat_fields = stat_line[closing_parenthesis + 2 :].split()
    if closing_parenthesis < 0 or len(stat_fields) <= 19:
        raise CollectorError("formal child start identity is malformed")
    start_ticks = stat_fields[19]
    if not start_ticks.isdigit():
        raise CollectorError("formal child start identity is malformed")
    argv_hash = hashlib.sha256("\0".join(command).encode("utf-8")).hexdigest()
    return {
        "pid": pid,
        "start_identity": f"candidate:{pid}:{start_ticks}",
        "uid": expected_uid,
        "executable_sha256": _file_sha256(
            executable, "kernel-observed child executable"
        ),
        "argv_sha256": argv_hash,
        "cmdline_sha256": hashlib.sha256(command_line).hexdigest(),
    }


def _read_process_group_identity(
    pid: int,
    expected_command: Sequence[str],
    *,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    """Re-read the exact process, command, group, session, and start identity."""

    if type(pid) is not int or pid <= 0:
        raise CollectorError("candidate process identity is invalid")
    command = tuple(expected_command)
    if not command or any(not isinstance(item, str) or not item for item in command):
        raise CollectorError("candidate process command is invalid")
    process_root = proc_root.expanduser().absolute() / str(pid)
    try:
        stat_line = (process_root / "stat").read_text(encoding="utf-8").strip()
        command_line = _read_regular_file_bytes(
            process_root / "cmdline", "candidate process command line"
        )
    except CollectorError:
        raise
    except (OSError, UnicodeError):
        raise CollectorError("candidate process identity is unavailable") from None
    closing_parenthesis = stat_line.rfind(")")
    fields = stat_line[closing_parenthesis + 2 :].split()
    if closing_parenthesis < 0 or len(fields) <= 19:
        raise CollectorError("candidate process identity is malformed")
    expected_cmdline = b"\0".join(
        item.encode("utf-8") for item in command
    ) + b"\0"
    if command_line != expected_cmdline:
        raise CollectorError("candidate command line mismatch")
    try:
        pgid = int(fields[2])
        sid = int(fields[3])
    except ValueError:
        raise CollectorError("candidate process identity is malformed") from None
    start_ticks = fields[19]
    if not start_ticks.isdigit():
        raise CollectorError("candidate process identity is malformed")
    return {
        "pid": pid,
        "start_identity": f"candidate:{pid}:{start_ticks}",
        "pgid": pgid,
        "sid": sid,
    }


def _process_group_members(
    pgid: int,
    sid: int,
    *,
    proc_root: Path = Path("/proc"),
) -> dict[int, str]:
    """Return the exact surviving membership of one bound group/session."""

    if type(pgid) is not int or pgid <= 0 or type(sid) is not int or sid <= 0:
        raise CollectorError("candidate process-group identity is invalid")
    root = proc_root.expanduser().absolute()
    if not root.is_dir():
        raise CollectorError("Linux process inventory is unavailable")
    try:
        entries = sorted(
            (entry for entry in root.iterdir() if entry.name.isdigit()),
            key=lambda entry: int(entry.name),
        )
    except OSError as exc:
        raise CollectorError("cannot enumerate Linux process inventory") from exc
    members: dict[int, str] = {}
    for process in entries:
        try:
            stat_line = (process / "stat").read_text(
                encoding="utf-8"
            ).strip()
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError) as exc:
            raise CollectorError("cannot inspect Linux process identity") from exc
        closing_parenthesis = stat_line.rfind(")")
        fields = stat_line[closing_parenthesis + 2 :].split()
        if closing_parenthesis < 0 or len(fields) <= 19:
            raise CollectorError("Linux process identity is malformed")
        try:
            observed_pgid = int(fields[2])
            observed_sid = int(fields[3])
        except ValueError:
            raise CollectorError("Linux process identity is malformed") from None
        if observed_pgid != pgid and observed_sid != sid:
            continue
        if observed_pgid != pgid or observed_sid != sid:
            raise CollectorError("candidate process-group identity is ambiguous")
        start_ticks = fields[19]
        if not start_ticks.isdigit():
            raise CollectorError("Linux process start identity is malformed")
        members[int(process.name)] = start_ticks
    return members


def _formal_uid_processes(
    uid: int, *, proc_root: Path = Path("/proc")
) -> dict[int, str]:
    """Return the kernel-observed PID/start-time set for one dedicated UID."""

    if type(uid) is not int or uid <= 0:
        raise CollectorError("formal runner UID is invalid")
    root = proc_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise CollectorError("Linux process inventory is unavailable")
    observed: dict[int, str] = {}
    try:
        entries = sorted(
            (entry for entry in root.iterdir() if entry.name.isdigit()),
            key=lambda entry: int(entry.name),
        )
    except OSError as exc:
        raise CollectorError("cannot enumerate Linux process inventory") from exc
    for process in entries:
        try:
            status = (process / "status").read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError) as exc:
            raise CollectorError("cannot inspect Linux process UID") from exc
        uid_lines = [line for line in status.splitlines() if line.startswith("Uid:")]
        if len(uid_lines) != 1:
            raise CollectorError("Linux process UID record is malformed")
        try:
            uids = [int(value) for value in uid_lines[0].split()[1:]]
        except ValueError as exc:
            raise CollectorError("Linux process UID record is malformed") from exc
        if len(uids) != 4:
            raise CollectorError("Linux process UID record is malformed")
        if not any(value == uid for value in uids):
            continue
        try:
            stat_line = (process / "stat").read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError) as exc:
            raise CollectorError("cannot inspect Linux process identity") from exc
        closing_parenthesis = stat_line.rfind(")")
        fields = stat_line[closing_parenthesis + 2 :].split()
        if closing_parenthesis < 0 or len(fields) <= 19 or not fields[19].isdigit():
            raise CollectorError("Linux process start identity is malformed")
        observed[int(process.name)] = fields[19]
    return observed


def _require_formal_uid_processes(
    uid: int,
    *,
    expected: Mapping[int, str],
    proc_root: Path = Path("/proc"),
) -> dict[int, str]:
    normalized = dict(expected)
    if any(
        type(pid) is not int
        or pid <= 0
        or not isinstance(start, str)
        or not start.isdigit()
        for pid, start in normalized.items()
    ):
        raise CollectorError("expected formal UID process set is invalid")
    observed = _formal_uid_processes(uid, proc_root=proc_root)
    if observed != normalized:
        raise CollectorError("formal runner UID process set is not isolated")
    return observed


def collect_docker_topology_snapshot(
    phase: str,
    *,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    neo4j_auth: tuple[str, str],
    qdrant_proxy: str,
    neo4j_proxy: str,
    qdrant_container: str,
    neo4j_container: str,
    toxiproxy_container: str,
    client_owner: str,
    client_python_version: str,
    runtime_snapshot: Path,
    frozen_proxy_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Observe Docker identities, service versions, RTT, and proxy counters."""

    if phase not in {"before", "after"}:
        raise CollectorError("topology probe phase is invalid")

    routes: list[dict[str, Any]]
    proxy_counters: dict[str, Any] | None = None
    if phase == "before":
        routes = prepare_isolated_toxiproxy_routes(
            toxiproxy_url,
            qdrant_proxy=qdrant_proxy,
            neo4j_proxy=neo4j_proxy,
        )
    elif frozen_proxy_state is None:
        routes = observe_formal_toxiproxy_routes(
            toxiproxy_url,
            qdrant_proxy=qdrant_proxy,
            neo4j_proxy=neo4j_proxy,
        )
        proxy_counters = capture_toxiproxy_counter_snapshot(
            toxiproxy_url,
            phase="final",
            proxy_routes=routes,
        )
    else:
        if (
            not isinstance(frozen_proxy_state, Mapping)
            or set(frozen_proxy_state) != {"proxy_routes", "proxy_counters"}
            or not isinstance(frozen_proxy_state.get("proxy_routes"), list)
            or not isinstance(frozen_proxy_state.get("proxy_counters"), Mapping)
        ):
            raise CollectorError("frozen Toxiproxy state is invalid")
        routes = [dict(row) for row in frozen_proxy_state["proxy_routes"]]
        try:
            proxy_counters = validate_proxy_counter_snapshot(
                frozen_proxy_state["proxy_counters"], expected_phase="final"
            )
        except ToxiproxyMetricsError as exc:
            raise CollectorError("frozen Toxiproxy state is invalid") from exc
    qdrant_body, qdrant_rtt = _http_read(qdrant_url.rstrip("/") + "/")
    qdrant_document = _strict_json_bytes(qdrant_body, "Qdrant version response")
    if not isinstance(qdrant_document, Mapping):
        raise CollectorError("Qdrant version response is malformed")
    qdrant_version = qdrant_document.get("version")
    try:
        started = time.perf_counter_ns()
        with _locked_neo4j_graph_database(runtime_snapshot) as GraphDatabase:
            driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
            try:
                info = driver.get_server_info()
            finally:
                driver.close()
        neo4j_rtt = max(0.0, (time.perf_counter_ns() - started) / 1_000_000.0)
        agent = str(info.agent)
        match = re.search(r"([0-9]+\.[0-9]+\.[0-9]+)", agent)
        neo4j_version = match.group(1) if match else None
    except Exception as exc:
        raise CollectorError("Neo4j version observation failed") from exc
    toxiproxy_body, toxiproxy_rtt = _http_read(
        toxiproxy_url.rstrip("/") + "/version"
    )
    toxiproxy_version = _parse_toxiproxy_version(toxiproxy_body)
    versions = {
        "client": client_python_version,
        "qdrant": qdrant_version,
        "neo4j": neo4j_version,
        "toxiproxy": toxiproxy_version,
    }
    for role, version in versions.items():
        if not is_registered_service_version(role, version):
            raise CollectorError("observed service version is not registered")
    if phase == "before":
        proxy_counters = capture_toxiproxy_counter_snapshot(
            toxiproxy_url,
            phase="baseline_a",
            proxy_routes=routes,
        )
    assert proxy_counters is not None
    host = _host_identity()
    owners = {
        "client": client_owner,
        "qdrant": _docker_owner(qdrant_container),
        "neo4j": _docker_owner(neo4j_container),
        "toxiproxy": _docker_owner(toxiproxy_container),
    }
    rtts = {
        "client": 0.0,
        "qdrant": qdrant_rtt,
        "neo4j": neo4j_rtt,
        "toxiproxy": toxiproxy_rtt,
    }
    return {
        "schema": SNAPSHOT_SCHEMA,
        "roles": [
            {
                "role": role,
                "host_identity": host,
                "listener_owner": owners[role],
                "service_version": versions[role],
                "rtt_ms": rtts[role],
            }
            for role in ("client", "qdrant", "neo4j", "toxiproxy")
        ],
        "proxy_routes": routes,
        "proxy_counters": proxy_counters,
        "backend_isolation": _collect_docker_backend_isolation(
            qdrant_container=qdrant_container,
            neo4j_container=neo4j_container,
            toxiproxy_container=toxiproxy_container,
        ),
    }


def _parse_formal_toxiproxy_counter_snapshot(
    text: str,
    *,
    phase: str,
    proxy_routes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    try:
        return parse_toxiproxy_byte_counters(
            text, phase=phase, proxy_routes=proxy_routes
        )
    except ToxiproxyMetricsError as exc:
        raise CollectorError("formal Toxiproxy metrics are invalid") from exc


def capture_toxiproxy_counter_snapshot(
    toxiproxy_url: str,
    *,
    phase: str,
    proxy_routes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    body, _rtt = _http_read(toxiproxy_url.rstrip("/") + "/metrics")
    try:
        return _parse_formal_toxiproxy_counter_snapshot(
            body.decode("utf-8"),
            phase=phase,
            proxy_routes=proxy_routes,
        )
    except UnicodeError as exc:
        raise CollectorError("Toxiproxy metrics are not UTF-8") from exc


def _validate_toxiproxy_attribution_boundary(
    baseline_a: Mapping[str, Any],
    baseline_b: Mapping[str, Any],
    routes_a: Sequence[Mapping[str, Any]],
    routes_b: Sequence[Mapping[str, Any]],
) -> None:
    try:
        first = validate_proxy_counter_snapshot(
            baseline_a, expected_phase="baseline_a"
        )
        second = validate_proxy_counter_snapshot(
            baseline_b, expected_phase="baseline_b"
        )
    except ToxiproxyMetricsError as exc:
        raise CollectorError(
            "formal Toxiproxy attribution boundary is invalid"
        ) from exc
    if proxy_counter_values(first) != proxy_counter_values(second):
        raise CollectorError(
            "formal Toxiproxy attribution boundary was not quiescent"
        )
    if list(routes_a) != list(routes_b):
        raise CollectorError(
            "formal Toxiproxy routes changed at the attribution boundary"
        )


def _derive_toxiproxy_attribution_deltas(
    baseline_b: Mapping[str, Any], final: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        deltas = derive_proxy_counter_deltas(baseline_b, final)
    except ToxiproxyMetricsError as exc:
        raise CollectorError("formal Toxiproxy counters were not monotonic") from exc
    role_totals = [row["total_bytes"] for row in deltas["routes"]]
    if any(total <= 0 for total in role_totals):
        raise CollectorError(
            "formal Toxiproxy attribution requires positive backend deltas"
        )
    if deltas["toxiproxy_total_bytes"] != sum(role_totals):
        raise CollectorError("formal Toxiproxy attribution totals do not close")
    return deltas


def _environment_hash(document: Mapping[str, Any]) -> str:
    validate_environment_attestation(document)
    return hashlib.sha256(canonical_json_bytes(dict(document))).hexdigest()


def _read_private_authorization_nonce(path: Path, project_root: Path) -> bytes:
    try:
        return _read_private_nonce_file(
            path, repository_root=project_root
        )
    except ValueError as exc:
        raise CollectorError(str(exc)) from exc


class _ProgressArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CollectorError("formal progress arguments are invalid")


def _require_progress_directory(
    metadata: os.stat_result,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    label: str,
) -> tuple[int, int]:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_gid != expected_gid
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise CollectorError(f"{label} is not protected")
    return int(metadata.st_dev), int(metadata.st_ino)


def _read_registered_formal_progress_view(
    *,
    run_id: str,
    authorization_nonce_path: Path,
    project_root: Path,
    controller_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Read only the identity-derived root-owned progress snapshot."""

    _validate_formal_controller_context(controller_context)
    if type(run_id) is not str or not run_id:
        raise CollectorError("formal progress run identity is invalid")
    if not isinstance(authorization_nonce_path, Path):
        raise CollectorError("formal progress nonce path is invalid")
    if not isinstance(project_root, Path):
        raise CollectorError("formal progress project root is invalid")
    try:
        root = project_root.expanduser().absolute().resolve(strict=True)
    except OSError as exc:
        raise CollectorError("formal progress project root is unavailable") from exc

    authorization_nonce = _read_private_authorization_nonce(
        authorization_nonce_path, root
    )
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    nonce_hash = hashlib.sha256(authorization_nonce).hexdigest()
    if FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(run_hash) != nonce_hash:
        raise CollectorError("formal progress identity is not registered")

    runs_root = _FORMAL_RUNS_ROOT.expanduser().absolute()
    derived_candidate = _formal_candidate_root(
        run_hash,
        nonce_hash,
        runs_root=runs_root,
    )
    candidate = _require_derived_candidate_root(
        derived_candidate,
        run_hash=run_hash,
        nonce_hash=nonce_hash,
        runs_root=runs_root,
    )
    run_root = candidate.parent
    if run_root.parent != runs_root or not run_root.name.startswith("run-"):
        raise CollectorError("formal progress workspace derivation is invalid")

    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    runs_descriptor: int | None = None
    run_descriptor: int | None = None
    try:
        try:
            runs_descriptor = os.open(runs_root, directory_flags)
            runs_metadata = os.fstat(runs_descriptor)
        except OSError as exc:
            raise CollectorError("formal progress runs root is unavailable") from exc
        runs_identity = _require_progress_directory(
            runs_metadata,
            expected_uid=_FORMAL_CONTROLLER_UID,
            expected_gid=FORMAL_RUNNER_GID,
            expected_mode=0o750,
            label="formal progress runs root",
        )
        try:
            run_descriptor = os.open(
                run_root.name,
                directory_flags,
                dir_fd=runs_descriptor,
            )
            run_metadata = os.fstat(run_descriptor)
        except OSError as exc:
            raise CollectorError("formal progress run workspace is unavailable") from exc
        run_identity = _require_progress_directory(
            run_metadata,
            expected_uid=_FORMAL_CONTROLLER_UID,
            expected_gid=FORMAL_RUNNER_GID,
            expected_mode=0o750,
            label="formal progress run workspace",
        )
        try:
            candidate_metadata = os.stat(
                candidate.name,
                dir_fd=run_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise CollectorError("formal progress candidate identity is unavailable") from exc
        candidate_identity = _require_progress_directory(
            candidate_metadata,
            expected_uid=FORMAL_RUNNER_UID,
            expected_gid=FORMAL_RUNNER_GID,
            expected_mode=0o700,
            label="formal progress candidate directory",
        )

        try:
            store = ProgressSnapshotStore(
                run_root / "progress.json",
                expected_uid=_FORMAL_CONTROLLER_UID,
                expected_gid=_FORMAL_CONTROLLER_GID,
                expected_parent_identity=run_identity,
            )
            view = store.read_view()
        except ProgressProtocolError as exc:
            raise CollectorError("formal progress snapshot is unavailable") from exc

        final_runs_metadata = os.fstat(runs_descriptor)
        final_run_metadata = os.fstat(run_descriptor)
        final_candidate_metadata = os.stat(
            candidate.name,
            dir_fd=run_descriptor,
            follow_symlinks=False,
        )
        if (
            _require_progress_directory(
                final_runs_metadata,
                expected_uid=_FORMAL_CONTROLLER_UID,
                expected_gid=FORMAL_RUNNER_GID,
                expected_mode=0o750,
                label="formal progress runs root",
            )
            != runs_identity
            or _require_progress_directory(
                final_run_metadata,
                expected_uid=_FORMAL_CONTROLLER_UID,
                expected_gid=FORMAL_RUNNER_GID,
                expected_mode=0o750,
                label="formal progress run workspace",
            )
            != run_identity
            or _require_progress_directory(
                final_candidate_metadata,
                expected_uid=FORMAL_RUNNER_UID,
                expected_gid=FORMAL_RUNNER_GID,
                expected_mode=0o700,
                label="formal progress candidate directory",
            )
            != candidate_identity
        ):
            raise CollectorError("formal progress workspace identity changed")
    except CollectorError:
        raise
    except (OSError, ValueError, RuntimeError) as exc:
        raise CollectorError("formal progress snapshot cannot be read") from exc
    finally:
        primary_failure = sys.exc_info()[1]
        close_failures: list[BaseException] = []
        for descriptor in (run_descriptor, runs_descriptor):
            if descriptor is None:
                continue
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_failures.append(exc)
        if close_failures and primary_failure is None:
            raise CollectorError(
                "formal progress descriptor cleanup failed"
            ) from close_failures[0]

    expected_fields = SNAPSHOT_FIELDS | (
        {"terminal_reason_class"} if "terminal_reason_class" in view else set()
    )
    if set(view) != expected_fields:
        raise CollectorError("formal progress snapshot closure is invalid")
    age = view.get("last_update_age_seconds")
    if type(age) is not int or age < 0:
        raise CollectorError("formal progress snapshot age is invalid")
    return dict(view)


def read_formal_progress_line(
    argv: Sequence[str] | None = None,
    *,
    _controller_context: Mapping[str, Any] | None = None,
    _controller_project_root: Path | None = None,
) -> bytes:
    """Return one canonical sanitized progress line for the protected controller."""

    arguments = list(() if argv is None else argv)
    parser = _ProgressArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--run-id", action="append", required=True)
    parser.add_argument(
        "--authorization-nonce",
        action="append",
        type=Path,
        required=True,
    )
    try:
        namespace = parser.parse_args(arguments)
    except CollectorError:
        raise
    except (TypeError, ValueError) as exc:
        raise CollectorError("formal progress arguments are invalid") from exc
    run_ids = list(namespace.run_id)
    nonce_paths = list(namespace.authorization_nonce)
    if len(run_ids) != 1 or len(nonce_paths) != 1:
        raise CollectorError("formal progress arguments are ambiguous")
    view = _read_registered_formal_progress_view(
        run_id=run_ids[0],
        authorization_nonce_path=nonce_paths[0],
        project_root=_controller_project_root,
        controller_context=_controller_context,
    )
    try:
        return canonical_snapshot_line(view)
    except ProgressProtocolError as exc:
        raise CollectorError("formal progress output is invalid") from exc


def _deactivate_guard_after_quiescence(*, child: Any, network_guard: Any) -> None:
    if child is None:
        _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
        network_guard.deactivate()
        return
    require_quiescence = getattr(child, "require_quiescence", None)
    if not callable(require_quiescence):
        raise CollectorError("candidate quiescence proof is unavailable")
    require_quiescence()
    network_guard.deactivate()


def _cleanup_formal_execution_resources(
    *,
    execution_monitor: Any,
    network_guard: Any,
    child: Any,
) -> list[BaseException]:
    """Attempt every cleanup operation and return, rather than mask, failures."""

    failures: list[BaseException] = []
    if execution_monitor is not None:
        try:
            execution_monitor.abort()
        except BaseException as exc:
            failures.append(exc)
    child_stopped = child is None
    if child is not None:
        terminate_group = getattr(child, "terminate_validated_group", None)
        try:
            if callable(terminate_group):
                terminate_group(term_seconds=5.0, kill_seconds=5.0)
                require_quiescence = getattr(child, "require_quiescence", None)
                if callable(require_quiescence):
                    require_quiescence()
                child_stopped = True
            else:
                process = getattr(child, "process", None)
                if process is None or process.poll() is None:
                    raise CollectorError(
                        "candidate process identity-bound cleanup is unavailable"
                    )
                process.wait(timeout=0)
                child_stopped = True
        except BaseException as exc:
            failures.append(exc)
        try:
            child.close()
        except BaseException as exc:
            failures.append(exc)
    if (
        child_stopped
        and network_guard is not None
        and bool(getattr(network_guard, "active", False))
    ):
        try:
            _deactivate_guard_after_quiescence(
                child=child, network_guard=network_guard
            )
        except BaseException as exc:
            failures.append(exc)
    return failures


def collect_formal_execution(
    *,
    project_root: Path,
    candidate_root: Path,
    launch_path: Path,
    completion_path: Path,
    authorization_nonce_path: Path,
    run_id: str,
    transport: str,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    neo4j_password: str,
    _controller_context: Mapping[str, Any] | None = None,
) -> tuple[Path, Path]:
    """Run the only production-eligible provenance candidate collection path."""

    controller_context = _validate_formal_controller_context(
        _controller_context
    )
    root = project_root.expanduser().absolute().resolve(strict=True)
    if (
        os.name != "posix"
        or not hasattr(os, "geteuid")
        or os.geteuid() != 0
        or not Path("/proc/self/status").is_file()
    ):
        raise CollectorError(
            "formal provenance collection requires the root Linux controller"
        )
    if transport not in {"local_loopback", "container_bridge"}:
        raise CollectorError("formal cross-host execution requires a remote collector")
    neo4j_user = "neo4j"
    approved_source_root = Path(__file__).resolve().parents[1]
    config_path = (
        approved_source_root
        / "configs"
        / "provenance_performance_matrix.json"
    )
    config_document, config_raw = load_strict_json_document(config_path)
    validate_matrix_config(config_document, formal=True)
    config_hash = formal_matrix_config_sha256()
    config_file_hash = hashlib.sha256(config_raw).hexdigest()
    if config_file_hash != formal_config_file_sha256():
        raise CollectorError("repository formal config bytes changed")
    authorization_nonce = _read_private_authorization_nonce(
        authorization_nonce_path, root
    )
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    nonce_hash = hashlib.sha256(authorization_nonce).hexdigest()
    if FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(run_hash) != nonce_hash:
        raise CollectorError("launch authorization nonce is not pre-registered")

    expected_commit = str(controller_context["source_commit"])
    expected_source_manifest = dict(controller_context["source_manifest"])

    def approved_source_identity(project: Path) -> Mapping[str, Any]:
        return attest_committed_source(
            project,
            expected_commit=expected_commit,
            expected_source_manifest=expected_source_manifest,
        )

    source_identity = approved_source_identity(root)
    derived_candidate = _require_derived_candidate_root(
        candidate_root,
        run_hash=run_hash,
        nonce_hash=nonce_hash,
    )
    workspace = _prepare_formal_run_workspace(run_hash, nonce_hash)
    if workspace.candidate != derived_candidate:
        raise CollectorError("formal candidate derivation is inconsistent")
    FormalStore(workspace.candidate)._require_fd_bound_publication_support(
        _require_credential_match=False
    )
    _preflight_external_outputs(
        root, workspace.candidate, launch_path, completion_path
    )
    private_parent = _create_formal_input_staging(workspace)
    environment_snapshot: Path | None = None
    source_export: Path | None = None
    runtime_snapshot: Path | None = None
    input_tree_manifest: dict[str, Any] | None = None
    child: _GatedCandidate | None = None
    network_guard: _NftNetworkGuard | None = None
    execution_monitor: _ExecutionIntegrityMonitor | None = None
    frozen_proxy_state: dict[str, Any] | None = None
    signal_latch = _SignalLatch()
    try:
        signal_latch.install()
        signal_latch.raise_if_interrupted()
        _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
        environment_document = _collect_formal_environment_attestation(
            toxiproxy_url=toxiproxy_url,
            storage_path=workspace.root,
        )
        environment_snapshot, environment_raw = _write_collected_environment_snapshot(
            private_parent, environment_document
        )
        environment_hash = _environment_hash(environment_document)
        source_export = create_immutable_source_export(
            root, private_parent, source_identity
        )
        python_executable = Path(sys.executable).expanduser().absolute().resolve(
            strict=True
        )
        python_hash = _file_sha256(python_executable, "Python executable")
        runtime_snapshot, runtime_manifest = _create_locked_runtime_snapshot(
            private_parent,
            lock_path=source_export / "configs" / "provenance_runtime_lock.json",
            wheel_directory=_FORMAL_RUNTIME_WHEEL_DIRECTORY,
            python_executable_hash=python_hash,
            require_protected_wheels=True,
        )
        input_tree_manifest = _publish_formal_input_tree(private_parent)
        signal_latch.raise_if_interrupted()
        external_tools = _attest_formal_external_tools(python_executable)
        child_spec = _build_formal_child_spec(
            source_export=source_export,
            runtime_snapshot=runtime_snapshot,
            runtime_manifest=runtime_manifest,
            candidate_root=workspace.candidate,
            environment_attestation_path=environment_snapshot,
            run_id=run_id,
            transport=transport,
            qdrant_url=qdrant_url,
            neo4j_uri=neo4j_uri,
            toxiproxy_url=toxiproxy_url,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
            source_manifest_sha256=source_identity["source_manifest_sha256"],
            runner_sha256=source_identity["runner_sha256"],
            config_file_sha256=config_file_hash,
            environment_attestation_sha256=environment_hash,
            external_tools=external_tools,
            runtime_directory_mode=0o550,
            runtime_file_mode=0o440,
            runtime_owner_uid=0,
            runtime_owner_gid=FORMAL_RUNNER_GID,
        )
        _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
        signal_latch.raise_if_interrupted()
        child = _start_gated_candidate(
            command=child_spec.command,
            cwd=child_spec.cwd,
            environment=child_spec.environment,
            formal_uid=FORMAL_RUNNER_UID,
            formal_gid=FORMAL_RUNNER_GID,
            require_completion_receipt=True,
            require_progress=True,
            progress_binding_sha256=child_spec.command_manifest[
                "progress_binding_sha256"
            ],
            progress_config_sha256=config_hash,
            progress_snapshot_path=workspace.root / "progress.json",
            progress_expected_uid=0,
            progress_expected_gid=0,
        )
        child_process = _observe_formal_child_process(
            child.process.pid,
            expected_command=child_spec.command,
            expected_uid=FORMAL_RUNNER_UID,
        )
        child_start_identity = str(child_process["start_identity"])
        child.bind_process_identity(child_start_identity)
        signal_latch.raise_if_interrupted()
        _require_formal_uid_processes(
            FORMAL_RUNNER_UID,
            expected={
                child.process.pid: child_start_identity.rsplit(":", 1)[-1]
            },
        )
        network_guard_profile = _collect_docker_network_guard_profile(
            toxiproxy_container=_FORMAL_TOXIPROXY_CONTAINER
        )
        network_guard = _NftNetworkGuard(
            _formal_network_table_name(run_hash),
            backend_ipv4_subnet=network_guard_profile["backend_ipv4_subnet"],
            ingress_ipv4_subnet=network_guard_profile["ingress_ipv4_subnet"],
            backend_bridge_interface=network_guard_profile[
                "backend_bridge_interface"
            ],
            ingress_bridge_interface=network_guard_profile[
                "ingress_bridge_interface"
            ],
            toxiproxy_ingress_ipv4=network_guard_profile[
                "toxiproxy_ingress_ipv4"
            ],
        )
        child_start_ticks = child_start_identity.rsplit(":", 1)[-1]

        def monitor_probe() -> Mapping[str, Any]:
            assert child is not None
            assert network_guard is not None
            if child.process.poll() is not None:
                raise _MonitorCandidateExited()
            observed_processes = _formal_uid_processes(FORMAL_RUNNER_UID)
            expected_processes = {child.process.pid: child_start_ticks}
            if observed_processes != expected_processes:
                if child.process.poll() is not None and observed_processes == {}:
                    raise _MonitorCandidateExited()
                raise CollectorError(
                    "execution integrity monitor found formal UID process drift"
                )
            return {
                "network_guard": network_guard.verify(),
                "toxiproxy_routes": observe_formal_toxiproxy_routes(
                    toxiproxy_url,
                    qdrant_proxy=_FORMAL_QDRANT_PROXY,
                    neo4j_proxy=_FORMAL_NEO4J_PROXY,
                ),
                "backend_isolation": _collect_docker_backend_isolation(
                    qdrant_container=_FORMAL_QDRANT_CONTAINER,
                    neo4j_container=_FORMAL_NEO4J_CONTAINER,
                    toxiproxy_container=_FORMAL_TOXIPROXY_CONTAINER,
                ),
                "runner_uid_processes": [
                    {
                        "pid": child.process.pid,
                        "start_identity": child_start_ticks,
                    }
                ],
                "host_environment": _formal_host_environment_snapshot(
                    workspace.root
                ),
                "load1_milli": _linux_load1_milli(),
            }

        def terminal_monitor_probe() -> Mapping[str, Any]:
            assert child is not None
            assert network_guard is not None
            if child.process.poll() is None:
                raise CollectorError(
                    "execution integrity terminal probe preceded child exit"
                )
            _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
            return {
                "network_guard": network_guard.verify(),
                "toxiproxy_routes": observe_formal_toxiproxy_routes(
                    toxiproxy_url,
                    qdrant_proxy=_FORMAL_QDRANT_PROXY,
                    neo4j_proxy=_FORMAL_NEO4J_PROXY,
                ),
                "backend_isolation": _collect_docker_backend_isolation(
                    qdrant_container=_FORMAL_QDRANT_CONTAINER,
                    neo4j_container=_FORMAL_NEO4J_CONTAINER,
                    toxiproxy_container=_FORMAL_TOXIPROXY_CONTAINER,
                ),
                "runner_uid_processes": [],
                "host_environment": _formal_host_environment_snapshot(
                    workspace.root
                ),
                "load1_milli": _linux_load1_milli(),
            }

        execution_monitor = _ExecutionIntegrityMonitor(
            probe=monitor_probe,
            terminal_probe=terminal_monitor_probe,
        )

        def run_candidate() -> tuple[int, Mapping[str, Any]]:
            assert child is not None
            try:
                child.release()
                exit_code, receipt = child.wait_with_receipt(
                    interrupt_latch=signal_latch
                )
                snapshot = child.finish_progress(2.0)
            except _CollectorInterruption:
                try:
                    child.interrupt_progress()
                except BaseException:
                    pass
                raise
            except BaseException:
                _attempt_progress_blocker(child.block_progress)
                raise
            if exit_code != 0:
                child.block_progress()
                return exit_code, receipt
            if (
                snapshot.get("status") != "running"
                or snapshot.get("update_sequence") != 450
                or snapshot.get("completed_repetitions") != 450
                or snapshot.get("completed_samples") != 14400
            ):
                child.block_progress()
                raise CollectorError(
                    "candidate progress did not reach formal completion"
                )
            return exit_code, receipt

        def seal_candidate(
            candidate: Path, receipt: Mapping[str, Any]
        ) -> Mapping[str, Any]:
            _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
            return _seal_candidate_tree(
                candidate,
                expected_owner_uid=FORMAL_RUNNER_UID,
                sealed_owner_uid=0,
                sealed_owner_gid=0,
                completion_receipt=receipt,
            )

        def probe(phase: str) -> Mapping[str, Any]:
            return collect_docker_topology_snapshot(
                phase,
                qdrant_url=qdrant_url,
                neo4j_uri=neo4j_uri,
                toxiproxy_url=toxiproxy_url,
                neo4j_auth=(neo4j_user, neo4j_password),
                qdrant_proxy=_FORMAL_QDRANT_PROXY,
                neo4j_proxy=_FORMAL_NEO4J_PROXY,
                qdrant_container=_FORMAL_QDRANT_CONTAINER,
                neo4j_container=_FORMAL_NEO4J_CONTAINER,
                toxiproxy_container=_FORMAL_TOXIPROXY_CONTAINER,
                client_owner=child_start_identity,
                client_python_version=child_spec.command_manifest[
                    "python_version"
                ],
                runtime_snapshot=runtime_snapshot,
                frozen_proxy_state=(
                    frozen_proxy_state if phase == "after" else None
                ),
            )

        def activate_network_guard() -> Mapping[str, Any]:
            assert network_guard is not None
            guard_snapshot = network_guard.activate()
            routes_b = prepare_isolated_toxiproxy_routes(
                toxiproxy_url,
                qdrant_proxy=_FORMAL_QDRANT_PROXY,
                neo4j_proxy=_FORMAL_NEO4J_PROXY,
            )
            baseline_b = capture_toxiproxy_counter_snapshot(
                toxiproxy_url,
                phase="baseline_b",
                proxy_routes=routes_b,
            )
            return {
                "network_guard": guard_snapshot,
                "proxy_routes": routes_b,
                "proxy_counters": baseline_b,
                "route_rearmed": True,
            }

        def finalize_network_guard() -> Mapping[str, Any]:
            nonlocal frozen_proxy_state
            assert network_guard is not None
            final_routes = observe_formal_toxiproxy_routes(
                toxiproxy_url,
                qdrant_proxy=_FORMAL_QDRANT_PROXY,
                neo4j_proxy=_FORMAL_NEO4J_PROXY,
            )
            final_counters = capture_toxiproxy_counter_snapshot(
                toxiproxy_url,
                phase="final",
                proxy_routes=final_routes,
            )
            frozen_proxy_state = {
                "proxy_routes": final_routes,
                "proxy_counters": final_counters,
            }
            guard_snapshot = network_guard.verify()
            return {
                "network_guard": guard_snapshot,
                "proxy_routes": final_routes,
                "proxy_counters": final_counters,
            }

        def deactivate_network_guard() -> None:
            assert child is not None
            assert network_guard is not None
            _deactivate_guard_after_quiescence(
                child=child, network_guard=network_guard
            )

        def start_execution_monitor() -> None:
            assert execution_monitor is not None
            execution_monitor.start()

        def finalize_execution_monitor() -> Mapping[str, Any]:
            assert execution_monitor is not None
            assert child is not None
            if (
                child.gate_released_monotonic_ns is None
                or child.exit_observed_monotonic_ns is None
            ):
                raise CollectorError(
                    "candidate execution boundary timestamps are unavailable"
                )
            return execution_monitor.finalize(
                gate_release_monotonic_ns=child.gate_released_monotonic_ns,
                child_exit_monotonic_ns=child.exit_observed_monotonic_ns,
            )

        def runtime_identity() -> Mapping[str, Any]:
            assert runtime_snapshot is not None
            assert input_tree_manifest is not None
            observed_inputs = _verify_formal_input_tree(private_parent)
            if observed_inputs != input_tree_manifest:
                raise CollectorError("formal input tree changed during execution")
            return verify_immutable_runtime_snapshot(
                runtime_snapshot,
                runtime_manifest,
                directory_mode=0o550,
                file_mode=0o440,
                expected_uid=0,
                expected_gid=FORMAL_RUNNER_GID,
            )

        def external_tool_identity() -> Sequence[Mapping[str, Any]]:
            return _attest_formal_external_tools(python_executable)

        return _collect_execution_evidence(
            project_root=root,
            candidate_root=workspace.candidate,
            launch_path=launch_path,
            completion_path=completion_path,
            run_id=run_id,
            transport=transport,
            config_sha256=config_hash,
            config_file_sha256=config_file_hash,
            workload_sha256=formal_matrix_workload_sha256(),
            environment_attestation_sha256=environment_hash,
            command_manifest=child_spec.command_manifest,
            child_process=child_process,
            authorization_nonce=authorization_nonce,
            network_guard_activate=activate_network_guard,
            network_guard_finalize=finalize_network_guard,
            network_guard_deactivate=deactivate_network_guard,
            execution_monitor_start=start_execution_monitor,
            execution_monitor_finalize=finalize_execution_monitor,
            run_candidate=run_candidate,
            candidate_sealer=seal_candidate,
            topology_probe=probe,
            source_identity_loader=approved_source_identity,
            external_tool_identity_loader=external_tool_identity,
            runtime_identity_loader=runtime_identity,
            candidate_material_loader=candidate_attestation_material,
            progress_blocker=child.block_progress,
            progress_completer=child.complete_progress,
            interruption_check=signal_latch.raise_if_interrupted,
        )
    except BaseException:
        if child is not None:
            try:
                child.block_progress()
            except BaseException:
                pass
        raise
    finally:
        primary_failure = sys.exc_info()[1]
        cleanup_failures: list[BaseException] = []
        try:
            cleanup_failures.extend(
                _cleanup_formal_execution_resources(
                    execution_monitor=execution_monitor,
                    network_guard=network_guard,
                    child=child,
                )
            )
        except BaseException as exc:
            cleanup_failures.append(exc)
        try:
            cleanup_failures.extend(signal_latch.close())
        except BaseException as exc:
            cleanup_failures.append(exc)
        if cleanup_failures and primary_failure is None:
            if child is not None:
                try:
                    child.block_progress()
                except BaseException as exc:
                    cleanup_failures.append(exc)
            raise CollectorError(
                "formal execution resource cleanup failed"
            ) from cleanup_failures[0]
        # The source/runtime/input snapshot and candidate remain under the
        # protected formal run inode so promotion can re-attest exact bytes.


def main(
    argv: list[str] | None = None,
    *,
    _controller_context: Mapping[str, Any] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description="collect source-bound launch/completion evidence around provenance measurement"
    )
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--launch-out", type=Path, required=True)
    parser.add_argument("--completion-out", type=Path, required=True)
    parser.add_argument("--authorization-nonce", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--transport",
        choices=("local_loopback", "container_bridge"),
        required=True,
    )
    parser.add_argument("--qdrant-url", default="http://127.0.0.1:19000")
    parser.add_argument("--neo4j-uri", default="bolt://127.0.0.1:19001")
    parser.add_argument("--toxiproxy-url", default="http://127.0.0.1:8474")
    args = parser.parse_args(argv)

    try:
        neo4j_password = os.environ.get("TXNMEM_NEO4J_PASSWORD")
        if not neo4j_password:
            raise CollectorError("Neo4j runtime credential is unavailable")
        launch, completion = collect_formal_execution(
            project_root=args.project_root,
            candidate_root=args.candidate_root,
            launch_path=args.launch_out,
            completion_path=args.completion_out,
            authorization_nonce_path=args.authorization_nonce,
            run_id=args.run_id,
            transport=args.transport,
            qdrant_url=args.qdrant_url,
            neo4j_uri=args.neo4j_uri,
            toxiproxy_url=args.toxiproxy_url,
            neo4j_password=neo4j_password,
            _controller_context=_controller_context,
        )
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        print(f"provenance execution collection blocked: {type(exc).__name__}")
        return 2
    print(f"wrote provenance launch evidence -> {launch}")
    print(f"wrote provenance completion evidence -> {completion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
