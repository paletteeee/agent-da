"""Gate-controlled provenance candidate entry point for immutable exports."""

from __future__ import annotations

import ctypes
import enum
import errno
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import sys
import time
import urllib.error
import urllib.request


FORMAL_RUNNER_UID = 65532
FORMAL_RUNNER_GID = 65532
_PR_GET_DUMPABLE = 3
_PR_SET_DUMPABLE = 4
_PR_SET_PDEATHSIG = 1
_PR_GET_NO_NEW_PRIVS = 39
_PR_GET_PDEATHSIG = 2
_SMOKE_V2_SCENARIOS = frozenset(
    {
        "normal_prefix",
        "first_ineligible",
        "backend_timeout",
        "interruption",
    }
)
_SMOKE_V2_RECEIPT_SCHEMA = "txnmem-provenance-smoke-child-receipt-v2"
_CELL_EXECUTION_SCHEMA = "txnmem-provenance-cell-execution-v1"
_CELL_WORKER_RESULT_SCHEMA = "txnmem-provenance-cell-worker-result-v1"
_CELL_HEARTBEAT_SCHEMA = "txnmem-provenance-cell-heartbeat-v1"
_MAX_CELL_RESULT_BYTES = 64 * 1024 * 1024
_MAX_CELL_HEARTBEAT_BYTES = 4096


class _RunnerInterruption(BaseException):
    """The protected runner received a termination request."""


class _IntegratedLifecycleFault(enum.Enum):
    """The sole private fault accepted by the protected lifecycle probe."""

    POINTER_WITHOUT_RECEIPT = "pointer_without_receipt"


def _require_integrated_lifecycle_fault(value) -> _IntegratedLifecycleFault:
    if type(value) is not _IntegratedLifecycleFault:
        raise TypeError("integrated lifecycle fault must be an exact enum member")
    return value


class _RunnerTerminationState:
    """Cooperative pending-SIGTERM state; the collector owns hard bounds."""

    def __init__(self) -> None:
        self.stop_requested = False

    def request_stop(self, _signal_number=None, _frame=None) -> None:
        self.stop_requested = True

    def raise_if_requested(self) -> None:
        try:
            if signal.SIGTERM in signal.sigpending():
                self.stop_requested = True
        except BaseException:
            raise RuntimeError("runner pending signal state is unavailable") from None
        if self.stop_requested:
            raise _RunnerInterruption("formal runner interruption requested")


def _require_controlled_sigterm_mask() -> None:
    """Verify the exact pre-exec mask inherited by every runner thread."""

    if not hasattr(signal, "pthread_sigmask") or not hasattr(signal, "sigpending"):
        raise RuntimeError("runner signal mask support is unavailable")
    try:
        observed = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    except BaseException:
        raise RuntimeError("runner signal mask is unavailable") from None
    if observed != {signal.SIGTERM}:
        raise RuntimeError("runner signal mask is not exact")


def _require_parent_death_sigkill(
    *,
    expected_parent_pid: int | None = None,
    prctl=None,
) -> None:
    """Query the post-drop kernel state and bind it to the current parent."""

    if expected_parent_pid is not None and (
        type(expected_parent_pid) is not int or expected_parent_pid <= 0
    ):
        raise RuntimeError("formal runner parent identity is invalid")
    if not sys.platform.startswith("linux"):
        raise RuntimeError("formal runner parent-death query requires Linux")
    operation = prctl
    if operation is None:
        try:
            raw_prctl = ctypes.CDLL(None, use_errno=True).prctl
        except (AttributeError, OSError) as exc:
            raise RuntimeError("formal runner prctl is unavailable") from exc
        raw_prctl.argtypes = [
            ctypes.c_int,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
        ]
        raw_prctl.restype = ctypes.c_int
        operation = raw_prctl
    observed_signal = ctypes.c_int(0)
    try:
        result = operation(
            _PR_GET_PDEATHSIG,
            ctypes.addressof(observed_signal),
            0,
            0,
            0,
        )
        observed_parent = os.getppid()
    except Exception:
        raise RuntimeError("formal runner parent-death query failed") from None
    if (
        type(result) is not int
        or result != 0
        or observed_signal.value != signal.SIGKILL
        or (
            expected_parent_pid is not None
            and observed_parent != expected_parent_pid
        )
    ):
        raise RuntimeError("formal runner parent-death state is not exact")


def _set_and_require_parent_death_sigkill(expected_parent_pid: int) -> None:
    """Set a descendant's fork-cleared PDEATHSIG and close the parent race."""

    if type(expected_parent_pid) is not int or expected_parent_pid <= 0:
        raise RuntimeError("formal runner parent identity is invalid")
    try:
        operation = ctypes.CDLL(None, use_errno=True).prctl
    except (AttributeError, OSError) as exc:
        raise RuntimeError("formal runner prctl is unavailable") from exc
    operation.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    operation.restype = ctypes.c_int
    try:
        result = operation(
            _PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0
        )
    except Exception:
        raise RuntimeError("formal runner parent-death setup failed") from None
    if (
        type(result) is not int
        or result != 0
        or os.getppid() != expected_parent_pid
    ):
        raise RuntimeError("formal runner parent-death setup failed")
    _require_parent_death_sigkill(
        expected_parent_pid=expected_parent_pid,
        prctl=operation,
    )


def _harden_execd_formal_runner(*, prctl=None) -> None:
    """Re-establish and verify the exact runner state reset by execve."""

    if not sys.platform.startswith("linux"):
        raise RuntimeError("formal runner hardening requires Linux")
    operation = prctl
    if operation is None:
        try:
            raw_prctl = ctypes.CDLL(None, use_errno=True).prctl
        except (AttributeError, OSError) as exc:
            raise RuntimeError("formal runner prctl is unavailable") from exc
        raw_prctl.argtypes = [
            ctypes.c_int,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
        ]
        raw_prctl.restype = ctypes.c_int
        operation = raw_prctl
    try:
        set_dumpable = operation(_PR_SET_DUMPABLE, 0, 0, 0, 0)
        dumpable = operation(_PR_GET_DUMPABLE, 0, 0, 0, 0)
        no_new_privileges = operation(_PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)
    except Exception:
        raise RuntimeError("formal runner prctl verification failed") from None
    if (
        type(set_dumpable) is not int
        or set_dumpable != 0
        or type(dumpable) is not int
        or dumpable != 0
        or type(no_new_privileges) is not int
        or no_new_privileges != 1
    ):
        raise RuntimeError("formal runner kernel state is not exact")
    if (
        os.getuid() != FORMAL_RUNNER_UID
        or os.geteuid() != FORMAL_RUNNER_UID
        or os.getgid() != FORMAL_RUNNER_GID
        or os.getegid() != FORMAL_RUNNER_GID
        or os.getgroups() != []
    ):
        raise RuntimeError("formal runner credentials are not exact")
    _require_parent_death_sigkill(prctl=operation)
    _require_controlled_sigterm_mask()
    source_directory = os.path.dirname(os.path.abspath(__file__))
    if source_directory not in sys.path:
        sys.path.insert(0, source_directory)
    from txnmem_provenance_contract import (
        FORMAL_RUNNER_GID as REGISTERED_FORMAL_RUNNER_GID,
        FORMAL_RUNNER_UID as REGISTERED_FORMAL_RUNNER_UID,
    )

    if (
        FORMAL_RUNNER_UID != REGISTERED_FORMAL_RUNNER_UID
        or FORMAL_RUNNER_GID != REGISTERED_FORMAL_RUNNER_GID
    ):
        raise RuntimeError("formal runner registration changed")


def _argument_value(arguments: list[str], name: str) -> str:
    if arguments.count(name) != 1:
        raise ValueError("formal runner argument is missing or duplicated")
    index = arguments.index(name)
    if index + 1 >= len(arguments) or not arguments[index + 1]:
        raise ValueError("formal runner argument has no value")
    return arguments[index + 1]


def _require_credential_matched_publication_preflight(
    arguments: list[str],
) -> None:
    """Run the decisive publication probe as the hardened exec'd runner."""

    candidate_root = _argument_value(arguments, "--out-dir")
    from txnmem_formal_io import FormalStore

    FormalStore(candidate_root)._require_fd_bound_publication_support()


def _candidate_completion_material(arguments: list[str]) -> dict:
    from txnmem_provenance_performance import (
        candidate_attestation_material,
        formal_matrix_config_sha256,
        provenance_bundle_id,
    )

    run_id = _argument_value(arguments, "--run-id")
    candidate_root = _argument_value(arguments, "--out-dir")
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    bundle_id = provenance_bundle_id(
        config_sha256=formal_matrix_config_sha256(),
        run_id_sha256=run_hash,
        formal=False,
        backend="vector-graph",
    )
    return candidate_attestation_material(candidate_root, bundle_id)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("formal completion receipt write failed")
        view = view[written:]


def _completion_payload(material: dict) -> bytes:
    return json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_cell_payload(value: dict) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise RuntimeError("isolated cell result is not canonical") from exc


def _closed_cell_timeout_class(exc: BaseException) -> str | None:
    current: BaseException | None = exc
    observed: set[int] = set()
    for _depth in range(16):
        if current is None or id(current) in observed:
            break
        observed.add(id(current))
        if type(current) is TimeoutError:
            return "backend_timeout"
        if type(current) is urllib.error.URLError and type(
            getattr(current, "reason", None)
        ) is TimeoutError:
            return "backend_timeout"
        current = current.__cause__ or current.__context__
    return None


def _decode_cell_worker_result(payload: bytes) -> dict:
    if (
        type(payload) is not bytes
        or not payload.endswith(b"\n")
        or payload[:-1].find(b"\n") != -1
        or len(payload) > _MAX_CELL_RESULT_BYTES
    ):
        raise RuntimeError("isolated cell result framing is invalid")

    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError("isolated cell result is invalid")
            result[key] = value
        return result

    try:
        document = json.loads(
            payload[:-1].decode("utf-8", errors="strict"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                RuntimeError("isolated cell result is invalid")
            ),
        )
    except RuntimeError:
        raise
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("isolated cell result is invalid") from exc
    if (
        type(document) is not dict
        or _canonical_cell_payload(document) + b"\n" != payload
        or document.get("schema") != _CELL_WORKER_RESULT_SCHEMA
    ):
        raise RuntimeError("isolated cell result is invalid")
    return document


def _decode_cell_heartbeat(payload: bytes) -> dict:
    if (
        type(payload) is not bytes
        or not payload.endswith(b"\n")
        or payload[:-1].find(b"\n") != -1
        or len(payload) > _MAX_CELL_HEARTBEAT_BYTES
    ):
        raise RuntimeError("isolated cell heartbeat is invalid")
    try:
        document = json.loads(payload[:-1].decode("ascii", errors="strict"))
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("isolated cell heartbeat is invalid") from exc
    if (
        type(document) is not dict
        or set(document)
        != {
            "schema",
            "completed_repetition_count",
            "completed_operation_sample_count",
        }
        or document.get("schema") != _CELL_HEARTBEAT_SCHEMA
        or _canonical_cell_payload(document) + b"\n" != payload
    ):
        raise RuntimeError("isolated cell heartbeat is invalid")
    return document


def _kill_and_reap_stalled_worker(pidfd: int, worker_pid: int) -> tuple[bool, int]:
    """Kill one exact worker and prove whether SIGKILL caused its exit."""

    if (
        type(pidfd) is not int
        or pidfd < 0
        or type(worker_pid) is not int
        or worker_pid <= 0
    ):
        raise RuntimeError("isolated cell worker identity is invalid")
    signal_accepted = True
    try:
        signal.pidfd_send_signal(pidfd, signal.SIGKILL, None, 0)
    except ProcessLookupError:
        signal_accepted = False
    except OSError as exc:
        if exc.errno != errno.ESRCH:
            raise
        signal_accepted = False
    waited_pid, worker_status = os.waitpid(worker_pid, 0)
    if waited_pid != worker_pid:
        raise RuntimeError("isolated cell worker identity changed")
    killed_by_sigkill = bool(
        signal_accepted
        and os.WIFSIGNALED(worker_status)
        and os.WTERMSIG(worker_status) == signal.SIGKILL
    )
    return killed_by_sigkill, worker_status


def _run_isolated_matrix_cell(
    run_cell,
    *,
    cell_index: int,
    cell_count: int,
    graph_size: int,
    concurrency: int,
    repetitions: int,
    operations_per_type: int,
    stall_timeout_seconds: float,
    progress_callback,
    interruption_check,
    close_fds: tuple[int, ...],
) -> dict:
    """Run one cell in an exactly supervised child and bound no-progress stalls."""

    if (
        isinstance(stall_timeout_seconds, bool)
        or type(stall_timeout_seconds) not in {int, float}
        or not math.isfinite(float(stall_timeout_seconds))
        or float(stall_timeout_seconds) <= 0.0
    ):
        raise RuntimeError("isolated cell stall timeout is invalid")
    if not callable(run_cell) or not callable(progress_callback) or not callable(
        interruption_check
    ):
        raise RuntimeError("isolated cell callbacks are invalid")
    for name, value in (
        ("cell_index", cell_index),
        ("cell_count", cell_count),
        ("graph_size", graph_size),
        ("concurrency", concurrency),
        ("repetitions", repetitions),
        ("operations_per_type", operations_per_type),
    ):
        if type(value) is not int or value <= 0:
            raise RuntimeError(f"isolated cell {name} is invalid")
    if cell_index > cell_count:
        raise RuntimeError("isolated cell index is outside the matrix")
    if (
        type(close_fds) is not tuple
        or len(set(close_fds)) != len(close_fds)
        or any(type(value) is not int or value < 0 for value in close_fds)
    ):
        raise RuntimeError("isolated cell descriptor closure is invalid")
    if (
        not sys.platform.startswith("linux")
        or not hasattr(os, "fork")
        or not hasattr(os, "pidfd_open")
        or not hasattr(signal, "pidfd_send_signal")
    ):
        raise RuntimeError("isolated cell supervision requires Linux pidfd support")

    result_read, result_write = os.pipe()
    heartbeat_read, heartbeat_write = os.pipe()
    worker_pid: int | None = None
    pidfd: int | None = None
    waited = False

    def close_descriptor(descriptor: int | None) -> None:
        if descriptor is None:
            return
        try:
            os.close(descriptor)
        except OSError:
            pass

    try:
        worker_pid = os.fork()
        if worker_pid == 0:
            exit_code = 74
            completed_repetitions = 0
            try:
                close_descriptor(result_read)
                close_descriptor(heartbeat_read)
                for descriptor in close_fds:
                    if descriptor not in {result_write, heartbeat_write}:
                        close_descriptor(descriptor)
                parent_pid = os.getppid()
                _set_and_require_parent_death_sigkill(parent_pid)

                def emit_heartbeat(snapshot) -> None:
                    nonlocal completed_repetitions
                    if not isinstance(snapshot, dict):
                        raise RuntimeError("isolated cell progress is invalid")
                    completed = snapshot.get("completed_repetition_count")
                    completed_samples = snapshot.get(
                        "completed_operation_sample_count"
                    )
                    if (
                        type(completed) is not int
                        or completed != completed_repetitions + 1
                        or completed > repetitions
                        or type(completed_samples) is not int
                        or completed_samples
                        != completed * operations_per_type * 4
                    ):
                        raise RuntimeError("isolated cell progress is invalid")
                    heartbeat = {
                        "schema": _CELL_HEARTBEAT_SCHEMA,
                        "completed_repetition_count": completed,
                        "completed_operation_sample_count": completed_samples,
                    }
                    _write_all(
                        heartbeat_write,
                        _canonical_cell_payload(heartbeat) + b"\n",
                    )
                    completed_repetitions = completed

                try:
                    report = run_cell(
                        progress_callback_override=emit_heartbeat
                    )
                    result = {
                        "schema": _CELL_WORKER_RESULT_SCHEMA,
                        "outcome": "completed",
                        "completed_repetition_count": completed_repetitions,
                        "report": report,
                    }
                    exit_code = 0
                except BaseException as exc:
                    timeout_class = _closed_cell_timeout_class(exc)
                    if timeout_class is not None:
                        result = {
                            "schema": _CELL_WORKER_RESULT_SCHEMA,
                            "outcome": "cell_timed_out",
                            "completed_repetition_count": completed_repetitions,
                            "timeout_class": timeout_class,
                        }
                        exit_code = 0
                    else:
                        error_class = type(exc).__name__
                        if re.fullmatch(
                            r"[A-Za-z_][A-Za-z0-9_]{0,127}", error_class
                        ) is None:
                            error_class = "RuntimeError"
                        result = {
                            "schema": _CELL_WORKER_RESULT_SCHEMA,
                            "outcome": "failed",
                            "error_class": error_class,
                        }
                _write_all(
                    result_write,
                    _canonical_cell_payload(result) + b"\n",
                )
            except BaseException:
                exit_code = 74
            finally:
                close_descriptor(heartbeat_write)
                close_descriptor(result_write)
            os._exit(exit_code)

        close_descriptor(result_write)
        result_write = -1
        close_descriptor(heartbeat_write)
        heartbeat_write = -1
        pidfd = os.pidfd_open(worker_pid, 0)
        os.set_blocking(result_read, False)
        os.set_blocking(heartbeat_read, False)
        result_buffer = bytearray()
        heartbeat_buffer = bytearray()
        received_repetitions = 0
        completed_repetitions = 0
        deadline = time.monotonic() + float(stall_timeout_seconds)
        worker_status: int | None = None
        result_open = True
        heartbeat_open = True

        def drain(descriptor: int, target: bytearray, limit: int) -> bool:
            still_open = True
            while True:
                try:
                    chunk = os.read(descriptor, 65536)
                except BlockingIOError:
                    break
                if not chunk:
                    still_open = False
                    break
                target.extend(chunk)
                if len(target) > limit:
                    raise RuntimeError("isolated cell IPC exceeded its bound")
            return still_open

        def consume_heartbeats() -> None:
            nonlocal completed_repetitions, deadline, received_repetitions
            while True:
                newline = heartbeat_buffer.find(b"\n")
                if newline < 0:
                    break
                payload = bytes(heartbeat_buffer[: newline + 1])
                del heartbeat_buffer[: newline + 1]
                heartbeat = _decode_cell_heartbeat(payload)
                expected = received_repetitions + 1
                if (
                    heartbeat["completed_repetition_count"] != expected
                    or expected > repetitions
                ):
                    raise RuntimeError("isolated cell heartbeat order is invalid")
                received_repetitions = expected
                if expected < repetitions:
                    progress_callback(
                        {
                            "completed_repetition_count": expected,
                            "completed_operation_sample_count": heartbeat[
                                "completed_operation_sample_count"
                            ],
                        }
                    )
                    completed_repetitions = expected
                deadline = time.monotonic() + float(stall_timeout_seconds)

        while worker_status is None:
            interruption_check()
            remaining = max(0.0, deadline - time.monotonic())
            watched = [pidfd]
            if result_open:
                watched.append(result_read)
            if heartbeat_open:
                watched.append(heartbeat_read)
            readable, _writable, _exceptional = select.select(
                watched, [], [], min(remaining, 0.25)
            )
            if result_open and result_read in readable:
                result_open = drain(
                    result_read, result_buffer, _MAX_CELL_RESULT_BYTES
                )
            if heartbeat_open and heartbeat_read in readable:
                heartbeat_open = drain(
                    heartbeat_read,
                    heartbeat_buffer,
                    _MAX_CELL_HEARTBEAT_BYTES * repetitions,
                )
                consume_heartbeats()
            if pidfd in readable:
                waited_pid, worker_status = os.waitpid(worker_pid, 0)
                if waited_pid != worker_pid:
                    raise RuntimeError("isolated cell worker identity changed")
                waited = True
                if result_open:
                    result_open = drain(
                        result_read, result_buffer, _MAX_CELL_RESULT_BYTES
                    )
                if heartbeat_open:
                    heartbeat_open = drain(
                        heartbeat_read,
                        heartbeat_buffer,
                        _MAX_CELL_HEARTBEAT_BYTES * repetitions,
                    )
                consume_heartbeats()
                break
            if time.monotonic() >= deadline:
                killed_by_watchdog, worker_status = _kill_and_reap_stalled_worker(
                    pidfd, worker_pid
                )
                waited = True
                if killed_by_watchdog:
                    return {
                        "schema": _CELL_EXECUTION_SCHEMA,
                        "outcome": "cell_timed_out",
                        "completed_repetition_count": completed_repetitions,
                        "timeout_class": "cell_stall_timeout",
                    }
                if result_open:
                    result_open = drain(
                        result_read, result_buffer, _MAX_CELL_RESULT_BYTES
                    )
                if heartbeat_open:
                    heartbeat_open = drain(
                        heartbeat_read,
                        heartbeat_buffer,
                        _MAX_CELL_HEARTBEAT_BYTES * repetitions,
                    )
                consume_heartbeats()
                break

        if heartbeat_buffer:
            raise RuntimeError("isolated cell heartbeat was truncated")
        document = _decode_cell_worker_result(bytes(result_buffer))
        if (
            not os.WIFEXITED(worker_status)
            or os.WEXITSTATUS(worker_status) != 0
        ):
            raise RuntimeError("isolated cell worker failed")
        if document.get("outcome") == "completed":
            if (
                set(document)
                != {
                    "schema",
                    "outcome",
                    "completed_repetition_count",
                    "report",
                }
                or document.get("completed_repetition_count")
                != received_repetitions
                or received_repetitions != repetitions
                or type(document.get("report")) is not dict
            ):
                raise RuntimeError("isolated completed cell result is invalid")
            progress_callback(
                {
                    "completed_repetition_count": repetitions,
                    "completed_operation_sample_count": (
                        repetitions * operations_per_type * 4
                    ),
                }
            )
            completed_repetitions = repetitions
            return {
                "schema": _CELL_EXECUTION_SCHEMA,
                "outcome": "completed",
                "completed_repetition_count": completed_repetitions,
                "report": document["report"],
            }
        if document.get("outcome") == "cell_timed_out":
            if (
                set(document)
                != {
                    "schema",
                    "outcome",
                    "completed_repetition_count",
                    "timeout_class",
                }
                or document.get("completed_repetition_count")
                != received_repetitions
                or completed_repetitions >= repetitions
                or document.get("timeout_class") != "backend_timeout"
            ):
                raise RuntimeError("isolated timed-out cell result is invalid")
            return {
                "schema": _CELL_EXECUTION_SCHEMA,
                "outcome": "cell_timed_out",
                "completed_repetition_count": completed_repetitions,
                "timeout_class": "backend_timeout",
            }
        raise RuntimeError("isolated cell worker failed")
    except BaseException:
        if worker_pid is not None and worker_pid > 0 and not waited:
            if pidfd is not None:
                try:
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL, None, 0)
                except (OSError, ProcessLookupError):
                    pass
            else:
                try:
                    os.kill(worker_pid, signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass
            try:
                waited_pid, _status = os.waitpid(worker_pid, 0)
                waited = waited_pid == worker_pid
            except (ChildProcessError, OSError):
                pass
        raise
    finally:
        for descriptor in (
            result_read,
            None if result_write == -1 else result_write,
            heartbeat_read,
            None if heartbeat_write == -1 else heartbeat_write,
            pidfd,
        ):
            close_descriptor(descriptor)


def _load_smoke_graph_database(runtime_site: Path):
    root = runtime_site.expanduser().absolute().resolve(strict=True)
    module = importlib.import_module("neo4j")
    module_file = Path(str(getattr(module, "__file__", ""))).resolve(strict=True)
    try:
        module_file.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("smoke Neo4j driver escaped locked runtime") from exc
    graph_database = getattr(module, "GraphDatabase", None)
    if graph_database is None:
        raise RuntimeError("smoke Neo4j driver is unavailable")
    return graph_database


def _probe_smoke_qdrant(
    url: str,
    *,
    timeout_seconds: float = 10.0,
) -> bool:
    if url != "http://127.0.0.1:19000/readyz":
        raise ValueError("smoke Qdrant endpoint is not exact loopback")
    if (
        isinstance(timeout_seconds, bool)
        or type(timeout_seconds) not in {int, float}
        or not 0.0 < float(timeout_seconds) <= 10.0
    ):
        raise ValueError("smoke Qdrant timeout is invalid")
    with urllib.request.urlopen(url, timeout=float(timeout_seconds)) as response:
        status = int(response.status)
        body = response.read(4097)
    return 200 <= status < 300 and len(body) <= 4096


def _probe_smoke_neo4j(
    *,
    runtime_site: Path,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
) -> bool:
    if (
        neo4j_uri != "bolt://127.0.0.1:19001"
        or neo4j_user != "neo4j"
        or not isinstance(neo4j_password, str)
        or not neo4j_password
    ):
        raise ValueError("smoke Neo4j endpoint or credential is invalid")
    GraphDatabase = _load_smoke_graph_database(runtime_site)
    with GraphDatabase.driver(
        neo4j_uri, auth=(neo4j_user, neo4j_password)
    ) as driver:
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                record = transaction.run("RETURN 1 AS value").single(strict=True)
                if record is None or record["value"] != 1:
                    return False
                transaction.commit()
    return True


def _provenance_smoke_receipt(
    runtime_site: Path, neo4j_password: str
) -> dict:
    qdrant_ok = _probe_smoke_qdrant("http://127.0.0.1:19000/readyz")
    neo4j_ok = _probe_smoke_neo4j(
        runtime_site=runtime_site,
        neo4j_uri="bolt://127.0.0.1:19001",
        neo4j_user="neo4j",
        neo4j_password=neo4j_password,
    )
    if qdrant_ok is not True or neo4j_ok is not True:
        raise RuntimeError("formal smoke backend probe failed")
    return {
        "schema": "txnmem-provenance-smoke-child-receipt-v1",
        "qdrant_proxy_ok": True,
        "neo4j_proxy_ok": True,
    }


def _smoke_v2_receipt(
    scenario: str,
    *,
    outcome: str,
    completed_repetitions: int,
    qdrant_proxy_ok: bool,
    neo4j_proxy_ok: bool,
) -> dict:
    expected = {
        "normal_prefix": ("succeeded", 2, True, True),
        "first_ineligible": ("formal_ineligible", 0, False, False),
        "backend_timeout": ("backend_timeout", 0, False, False),
    }.get(scenario)
    observed = (
        outcome,
        completed_repetitions,
        qdrant_proxy_ok,
        neo4j_proxy_ok,
    )
    if expected is None or observed != expected:
        raise RuntimeError("formal smoke v2 receipt closure is invalid")
    return {
        "schema": _SMOKE_V2_RECEIPT_SCHEMA,
        "scenario": scenario,
        "outcome": outcome,
        "completed_repetitions": completed_repetitions,
        "qdrant_proxy_ok": qdrant_proxy_ok,
        "neo4j_proxy_ok": neo4j_proxy_ok,
    }


def _is_timeout_failure(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, urllib.error.URLError):
        return isinstance(exc.reason, TimeoutError)
    return False


def _prove_smoke_first_repetition_ineligible() -> None:
    """Exercise the real formal-eligibility gate before any backend operation."""

    from txnmem_provenance_performance import (
        ProvenancePerformanceError,
        build_layered_dag,
        run_matrix_cell,
    )

    environment = {
        "schema": "txnmem-provenance-environment-v1",
        "isolation_verified": False,
        "co_tenant_load_detected": False,
        "source": "collector-observation-v2",
        "cpu_logical_count": 1,
        "memory_total_bytes": 1,
        "disk_medium": "nvme",
        "toxiproxy_version": "2.5.0",
    }
    created_backends = []

    class _EligibilityOnlyBackend:
        def __init__(self) -> None:
            self.closed = False

        def healthcheck(self) -> dict:
            return {
                "qdrant": {"available": True, "version": "1.11.5"},
                "neo4j": {"available": True, "version": "5.22.0"},
            }

        def performance_environment(self) -> dict:
            return dict(environment)

        def close(self) -> None:
            self.closed = True

    def backend_factory(_namespace: str) -> _EligibilityOnlyBackend:
        backend = _EligibilityOnlyBackend()
        created_backends.append(backend)
        return backend

    expected_failure = (
        "formal run requires verified isolation without co-tenant load"
    )
    try:
        run_matrix_cell(
            backend_factory,
            build_layered_dag(1, 0),
            concurrency=1,
            repetitions=1,
            operations_per_type=1,
            run_id="formal-smoke-first-ineligible",
            formal=False,
            require_formal_eligibility=True,
            environment_attestation=environment,
        )
    except ProvenancePerformanceError as exc:
        if str(exc) != expected_failure:
            raise RuntimeError(
                "formal smoke eligibility gate failed unexpectedly"
            ) from None
    else:
        raise RuntimeError("formal smoke ineligible gate unexpectedly passed")
    if created_backends:
        raise RuntimeError(
            "formal smoke eligibility gate touched the backend factory"
        )


def _load_smoke_v2_environment(path: str) -> dict:
    """Load one protected, independently collected environment attestation."""

    if not isinstance(path, str) or not path or not os.path.isabs(path):
        raise RuntimeError("formal smoke environment path is invalid")
    from txnmem_provenance_performance import (
        load_strict_json_document,
        validate_environment_attestation,
    )

    try:
        document, _raw = load_strict_json_document(path)
        if type(document) is not dict:
            raise ValueError("formal smoke environment must be an object")
        validated = validate_environment_attestation(document)
    except (OSError, TypeError, ValueError):
        raise RuntimeError("formal smoke environment is invalid") from None
    if (
        validated.get("isolation_verified") is not True
        or validated.get("co_tenant_load_detected") is not False
    ):
        raise RuntimeError("formal smoke environment is not formally eligible")
    return validated


def _run_smoke_normal_prefix(
    *,
    neo4j_password: str,
    environment_attestation: dict,
    progress_binding: str,
    emit_progress,
    termination_state: _RunnerTerminationState,
) -> None:
    """Execute the real smallest formal cell for two eligible repetitions."""

    from txnmem_provenance_performance import (
        MATRIX_SCHEMA,
        build_layered_dag,
        make_vector_graph_backend_factory,
        run_matrix_cell,
    )

    factory = make_vector_graph_backend_factory(
        qdrant_url="http://127.0.0.1:19000",
        neo4j_uri="bolt://127.0.0.1:19001",
        neo4j_auth=("neo4j", neo4j_password),
        environment_attestation=environment_attestation,
        request_timeout_seconds=30.0,
    )
    primary: BaseException | None = None
    primary_traceback = None
    report = None

    def on_repetition(snapshot: dict) -> None:
        termination_state.raise_if_requested()
        if type(snapshot) is not dict or set(snapshot) != {
            "cell_id",
            "completed_repetition_count",
            "completed_operation_sample_count",
        }:
            raise RuntimeError("formal smoke workload progress is invalid")
        repetition_index = snapshot["completed_repetition_count"]
        expected_samples = repetition_index * 32
        if (
            snapshot["cell_id"] != "n100-c1"
            or type(repetition_index) is not int
            or repetition_index not in {1, 2}
            or snapshot["completed_operation_sample_count"] != expected_samples
        ):
            raise RuntimeError("formal smoke workload progress is invalid")
        emit_progress(repetition_index)

    try:
        report = run_matrix_cell(
            factory,
            build_layered_dag(100, 17),
            concurrency=1,
            repetitions=2,
            operations_per_type=8,
            run_id="formal-smoke-normal-prefix-" + progress_binding,
            formal=False,
            require_formal_eligibility=True,
            environment_attestation=environment_attestation,
            progress_callback=on_repetition,
        )
    except BaseException as exc:
        primary = exc
        primary_traceback = exc.__traceback__
    try:
        close = getattr(factory, "close", None)
        if callable(close):
            close()
    except BaseException as exc:
        if primary is None:
            primary = exc
            primary_traceback = exc.__traceback__
    if primary is not None:
        raise primary.with_traceback(primary_traceback)
    if (
        type(report) is not dict
        or report.get("schema") != MATRIX_SCHEMA
        or report.get("cell_id") != "n100-c1"
        or type(report.get("graph")) is not dict
        or report["graph"].get("node_count") != 100
        or report.get("concurrency") != 1
        or report.get("repetition_count") != 2
        or report.get("operations_per_type") != 8
        or type(report.get("samples")) is not list
        or len(report["samples"]) != 64
        or type(report.get("repetitions")) is not list
        or len(report["repetitions"]) != 2
        or any(
            type(row) is not dict or row.get("eligible_for_formal") is not True
            for row in report["repetitions"]
        )
        or report.get("formal_requested") is not False
        or report.get("formal_eligible") is not True
    ):
        raise RuntimeError("formal smoke workload result is invalid")


def _run_provenance_smoke_v2_scenario(
    scenario: str,
    *,
    runtime_site: Path,
    neo4j_password: str,
    environment_attestation: dict,
    progress_binding: str,
    emit_progress,
    termination_state: _RunnerTerminationState,
) -> dict:
    if scenario not in _SMOKE_V2_SCENARIOS:
        raise RuntimeError("formal smoke v2 scenario is invalid")
    if not isinstance(runtime_site, Path):
        raise RuntimeError("formal smoke v2 runtime is invalid")
    if not isinstance(neo4j_password, str) or not neo4j_password:
        raise RuntimeError("formal smoke v2 credential is unavailable")
    if type(environment_attestation) is not dict:
        raise RuntimeError("formal smoke v2 environment is invalid")
    if re.fullmatch(r"[0-9a-f]{64}", progress_binding) is None:
        raise RuntimeError("formal smoke v2 binding is invalid")
    if not callable(emit_progress):
        raise RuntimeError("formal smoke v2 progress channel is invalid")

    if scenario == "first_ineligible":
        _prove_smoke_first_repetition_ineligible()
        return _smoke_v2_receipt(
            scenario,
            outcome="formal_ineligible",
            completed_repetitions=0,
            qdrant_proxy_ok=False,
            neo4j_proxy_ok=False,
        )
    if scenario == "backend_timeout":
        try:
            _probe_smoke_qdrant(
                "http://127.0.0.1:19000/readyz",
                timeout_seconds=1.0,
            )
        except BaseException as exc:
            if not _is_timeout_failure(exc):
                raise
        else:
            raise RuntimeError("formal smoke timeout probe unexpectedly succeeded")
        return _smoke_v2_receipt(
            scenario,
            outcome="backend_timeout",
            completed_repetitions=0,
            qdrant_proxy_ok=False,
            neo4j_proxy_ok=False,
        )
    if scenario == "interruption":
        while True:
            termination_state.raise_if_requested()
            time.sleep(0.01)

    _run_smoke_normal_prefix(
        neo4j_password=neo4j_password,
        environment_attestation=environment_attestation,
        progress_binding=progress_binding,
        emit_progress=emit_progress,
        termination_state=termination_state,
    )
    return _smoke_v2_receipt(
        scenario,
        outcome="succeeded",
        completed_repetitions=2,
        qdrant_proxy_ok=True,
        neo4j_proxy_ok=True,
    )


def _cleanup_runner_resources(
    *,
    progress_fd: int | None,
    completion_fd: int,
) -> list[BaseException]:
    """Attempt every runner cleanup operation exactly once."""

    failures: list[BaseException] = []
    descriptors = (
        (progress_fd,) if progress_fd is not None else ()
    ) + (completion_fd,)
    for descriptor in descriptors:
        try:
            os.close(descriptor)
        except BaseException as exc:
            failures.append(exc)
    return failures


def _run_protected_linux_integrated_lifecycle_probe(
    candidate_root: Path,
    run_id: str,
    fault: _IntegratedLifecycleFault | None = None,
) -> int:
    """Publish one real fd-bound pointer and deliberately withhold its receipt."""

    selected_fault = _require_integrated_lifecycle_fault(fault)
    if selected_fault is not _IntegratedLifecycleFault.POINTER_WITHOUT_RECEIPT:
        raise RuntimeError("integrated lifecycle fault is unavailable")
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise RuntimeError("integrated lifecycle candidate is invalid")
    if not isinstance(run_id, str) or run_id != "txnmem-integrated-lifecycle":
        raise RuntimeError("integrated lifecycle binding is invalid")
    gate_value = os.environ.pop("TXNMEM_PROVENANCE_START_GATE_FD", None)
    ready_value = os.environ.pop("TXNMEM_PROVENANCE_READY_FD", None)
    completion_value = os.environ.pop(
        "TXNMEM_PROVENANCE_COMPLETION_FD", None
    )
    if (
        gate_value is None
        or not gate_value.isdigit()
        or ready_value is None
        or not ready_value.isdigit()
        or completion_value is None
        or not completion_value.isdigit()
        or len({gate_value, ready_value, completion_value}) != 3
    ):
        raise RuntimeError("integrated lifecycle descriptors are invalid")
    gate_fd = int(gate_value)
    ready_fd = int(ready_value)
    completion_fd = int(completion_value)
    _harden_execd_formal_runner()

    descendant_read, descendant_write = os.pipe()
    descendant_pid = os.fork()
    if descendant_pid == 0:
        try:
            os.close(descendant_read)
            parent_pid = os.getppid()
            _set_and_require_parent_death_sigkill(parent_pid)
            if (
                os.getuid() != FORMAL_RUNNER_UID
                or os.geteuid() != FORMAL_RUNNER_UID
                or os.getgid() != FORMAL_RUNNER_GID
                or os.getegid() != FORMAL_RUNNER_GID
                or os.getgroups() != []
            ):
                os._exit(91)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            if os.write(descendant_write, b"R") != 1:
                os._exit(92)
            os.close(descendant_write)
            ctypes.PyDLL(None).sleep(60)
        except BaseException:
            os._exit(93)
        os._exit(0)
    os.close(descendant_write)
    try:
        if os.read(descendant_read, 1) != b"R":
            raise RuntimeError("integrated lifecycle descendant did not harden")
    finally:
        os.close(descendant_read)
    waited_pid, _waited_status = os.waitpid(descendant_pid, os.WNOHANG)
    if waited_pid != 0:
        raise RuntimeError("integrated lifecycle descendant exited early")
    if os.write(ready_fd, b"R") != 1:
        raise RuntimeError("integrated lifecycle readiness failed")
    os.close(ready_fd)
    if os.read(gate_fd, 1) != b"G":
        raise RuntimeError("integrated lifecycle gate failed")
    os.close(gate_fd)

    from txnmem_backend import InstrumentedMemoryBackend
    from txnmem_provenance_performance import (
        _PrivatePublicationMode,
        _canonical_json_bytes,
        aggregate_matrix,
        build_layered_dag,
        canonical_jsonl_sha256,
        provenance_bundle_id,
        publish_provenance_bundle,
        run_matrix_cell,
    )

    config = {
        "schema": "txnmem-provenance-performance-v2",
        "graph_node_counts": [2],
        "concurrency_levels": [1],
        "repetitions": 1,
        "graph_seed": 17,
        "operations_per_type": 1,
        "bootstrap_repetitions": 10,
        "bootstrap_seed": 17,
        "request_timeout_seconds": 30.0,
    }
    cell = run_matrix_cell(
        lambda _namespace: InstrumentedMemoryBackend(),
        build_layered_dag(2, 17),
        concurrency=1,
        repetitions=1,
        operations_per_type=1,
        run_id=run_id,
        formal=False,
    )
    operation_samples = list(cell["samples"])
    repetitions = list(cell["repetitions"])
    config_hash = hashlib.sha256(_canonical_json_bytes(config)).hexdigest()
    bundle_id = provenance_bundle_id(
        config_sha256=config_hash,
        run_id_sha256=cell["run_id_sha256"],
        formal=False,
        backend="vector-graph",
    )
    report = {
        "schema": "txnmem-provenance-performance-report-v1",
        "backend": "vector-graph",
        "formal_requested": False,
        "bundle_id": bundle_id,
        "publication_status": "complete",
        "production_backend_claim": False,
        "config": config,
        "config_sha256": config_hash,
        "config_file_sha256": "0" * 64,
        "run_id_sha256": cell["run_id_sha256"],
        "matrix_cell_count": 1,
        "repetition_count": 1,
        "operation_sample_count": 4,
        "operation_samples_sha256": canonical_jsonl_sha256(
            operation_samples
        ),
        "repetitions_sha256": canonical_jsonl_sha256(repetitions),
        "graphs": [cell["graph"]],
        "aggregate": aggregate_matrix(
            cell,
            bootstrap_repetitions=10,
            seed=17,
            require_formal=False,
        ),
        "topology_attestation_sha256": None,
    }
    pointer_name = f"{bundle_id}.json"
    precommit_marker = candidate_root / "anonymous-precommit-observed"

    def require_anonymous_precommit() -> None:
        bundles = candidate_root / "bundles"
        observed_names = {path.name for path in bundles.iterdir()}
        if pointer_name in observed_names or any(
            name.startswith(f".{pointer_name}.") for name in observed_names
        ):
            raise RuntimeError(
                "integrated lifecycle pointer became named before commit"
            )
        descriptor = os.open(
            precommit_marker,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, b"anonymous\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    publish_provenance_bundle(
        candidate_root,
        bundle_id=bundle_id,
        operation_samples=operation_samples,
        repetitions=repetitions,
        report=report,
        _precommit_check=require_anonymous_precommit,
        _private_publication_mode=(
            _PrivatePublicationMode.INTEGRATED_POINTER_WITHOUT_RECEIPT
        ),
    )
    pointer = candidate_root / "bundles" / pointer_name
    if not pointer.is_file() or not precommit_marker.is_file():
        raise RuntimeError("integrated lifecycle pointer is unavailable")
    object_id = json.loads(pointer.read_text(encoding="utf-8"))["object_id"]
    if not (
        candidate_root / "bundle_objects" / object_id / "COMPLETED.json"
    ).is_file():
        raise RuntimeError("integrated lifecycle object completion is unavailable")

    # The internal object marker is complete, but the collector receipt remains
    # open and byte-empty until actual parent death kills this process.
    _ = completion_fd
    ctypes.PyDLL(None).sleep(60)
    return 94


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    gate_value = os.environ.pop("TXNMEM_PROVENANCE_START_GATE_FD", None)
    ready_value = os.environ.pop("TXNMEM_PROVENANCE_READY_FD", None)
    completion_value = os.environ.pop("TXNMEM_PROVENANCE_COMPLETION_FD", None)
    progress_value = os.environ.pop("TXNMEM_PROVENANCE_PROGRESS_FD", None)
    progress_binding = os.environ.pop(
        "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256", None
    )
    runtime_site = os.environ.pop("TXNMEM_PROVENANCE_RUNTIME_SITE", None)
    smoke_environment_path = os.environ.pop(
        "TXNMEM_PROVENANCE_SMOKE_ENVIRONMENT_PATH", None
    )
    progress_pair_present = progress_value is not None and progress_binding is not None
    performance_mode = bool(
        arguments and arguments[0] == "provenance-performance"
    )
    smoke_v2_mode = bool(
        arguments and arguments[0] == "provenance-smoke-v2"
    )
    if (
        gate_value is None
        or not gate_value.isdigit()
        or ready_value is None
        or not ready_value.isdigit()
        or completion_value is None
        or not completion_value.isdigit()
        or gate_value == ready_value
        or completion_value in {gate_value, ready_value}
        or (progress_value is None) != (progress_binding is None)
        or ((performance_mode or smoke_v2_mode) and not progress_pair_present)
        or (smoke_v2_mode and smoke_environment_path is None)
        or (not smoke_v2_mode and smoke_environment_path is not None)
        or (
            smoke_environment_path is not None
            and not os.path.isabs(smoke_environment_path)
        )
        or (
            progress_pair_present
            and (
                not progress_value.isdigit()
                or progress_value in {gate_value, ready_value, completion_value}
                or re.fullmatch(r"[0-9a-f]{64}", progress_binding) is None
            )
        )
        or runtime_site is None
        or not os.path.isabs(runtime_site)
        or not os.path.isdir(runtime_site)
    ):
        return 70
    descriptor_values = [gate_value, ready_value, completion_value]
    if progress_value is not None:
        descriptor_values.append(progress_value)
    if len({int(value) for value in descriptor_values}) != len(descriptor_values):
        return 70
    gate_fd = int(gate_value)
    ready_fd = int(ready_value)
    completion_fd = int(completion_value)
    progress_fd = int(progress_value) if progress_pair_present else None
    try:
        _harden_execd_formal_runner()
        if performance_mode:
            _require_credential_matched_publication_preflight(arguments)
    except (OSError, TypeError, ValueError, RuntimeError):
        return 70
    try:
        if os.write(ready_fd, b"R") != 1:
            return 70
    finally:
        os.close(ready_fd)
    try:
        token = os.read(gate_fd, 1)
    finally:
        os.close(gate_fd)
    if token != b"G":
        return 71

    termination_state = _RunnerTerminationState()
    primary_failure: BaseException | None = None
    try:
        if not arguments or arguments[0] not in {
            "provenance-performance",
            "provenance-smoke",
            "provenance-smoke-v2",
        }:
            return 72
        runtime_path = Path(runtime_site).expanduser().absolute().resolve(strict=True)
        sys.path.insert(0, runtime_site)
        sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))
        if arguments[0] == "provenance-smoke":
            if arguments != ["provenance-smoke"]:
                return 72
            neo4j_password = os.environ.pop("TXNMEM_NEO4J_PASSWORD", None)
            if not neo4j_password:
                return 72
            material = _provenance_smoke_receipt(
                runtime_path, neo4j_password
            )
            termination_state.raise_if_requested()
            payload = _completion_payload(material)
            if not payload or len(payload) > 65536:
                return 74
            _write_all(completion_fd, payload)
            return 0
        if arguments[0] == "provenance-smoke-v2":
            if (
                len(arguments) != 2
                or arguments[1] not in _SMOKE_V2_SCENARIOS
                or progress_fd is None
                or progress_binding is None
            ):
                return 72
            neo4j_password = os.environ.pop("TXNMEM_NEO4J_PASSWORD", None)
            if not neo4j_password:
                return 72
            environment_attestation = _load_smoke_v2_environment(
                smoke_environment_path
            )
            from txnmem_provenance_performance import formal_matrix_config_sha256
            from txnmem_provenance_progress import (
                build_progress_event,
                canonical_progress_line,
            )

            config_sha256 = formal_matrix_config_sha256()

            def emit_smoke_progress(repetition_index: int) -> None:
                termination_state.raise_if_requested()
                if type(repetition_index) is not int or repetition_index not in {1, 2}:
                    raise RuntimeError("formal smoke v2 repetition is invalid")
                event = build_progress_event(
                    run_binding_sha256=progress_binding,
                    config_sha256=config_sha256,
                    cell_index=1,
                    graph_size=100,
                    concurrency=1,
                    repetition_index=repetition_index,
                    completed_repetitions=repetition_index,
                    completed_samples=repetition_index * 32,
                    update_sequence=repetition_index,
                )
                _write_all(progress_fd, canonical_progress_line(event))

            material = _run_provenance_smoke_v2_scenario(
                arguments[1],
                runtime_site=runtime_path,
                neo4j_password=neo4j_password,
                environment_attestation=environment_attestation,
                progress_binding=progress_binding,
                emit_progress=emit_smoke_progress,
                termination_state=termination_state,
            )
            termination_state.raise_if_requested()
            payload = _completion_payload(material)
            if not payload or len(payload) > 65536:
                return 74
            _write_all(completion_fd, payload)
            return 0
        if (
            "--formal" in arguments
            or arguments.count("--backend") != 1
            or arguments.index("--backend") + 1 >= len(arguments)
            or arguments[arguments.index("--backend") + 1] != "vector-graph"
        ):
            return 72
        from txnmem_experiment import main as experiment_main
        from txnmem_provenance_performance import formal_matrix_config_sha256
        from txnmem_provenance_progress import (
            build_progress_event,
            canonical_progress_line,
        )

        config_sha256 = formal_matrix_config_sha256()

        def emit_progress(snapshot: dict) -> None:
            termination_state.raise_if_requested()
            if progress_fd is None or progress_binding is None:
                raise RuntimeError("formal progress channel is unavailable")
            event = build_progress_event(
                run_binding_sha256=progress_binding,
                config_sha256=config_sha256,
                cell_index=snapshot["cell_index"],
                graph_size=snapshot["graph_size"],
                concurrency=snapshot["concurrency"],
                repetition_index=snapshot["repetition_index"],
                completed_repetitions=snapshot["completed_repetitions"],
                completed_samples=snapshot["completed_samples"],
                update_sequence=snapshot["update_sequence"],
                outcome=snapshot["outcome"],
                skipped_repetitions=snapshot["skipped_repetitions"],
                timed_out_cell_count=snapshot["timed_out_cell_count"],
            )
            _write_all(progress_fd, canonical_progress_line(event))

        def execute_cell(run_cell, **kwargs):
            return _run_isolated_matrix_cell(
                run_cell,
                interruption_check=termination_state.raise_if_requested,
                close_fds=(completion_fd, progress_fd),
                **kwargs,
            )

        result = experiment_main(
            arguments,
            _progress_callback=emit_progress,
            _require_formal_eligibility=True,
            _interruption_check=termination_state.raise_if_requested,
            _cell_executor=execute_cell,
        )
        if type(result) is not int:
            return 73
        if result != 0:
            termination_state.raise_if_requested()
        if result == 0:
            material = _candidate_completion_material(arguments)
            payload = _completion_payload(material)
            if not payload or len(payload) > 65536:
                return 74
            _write_all(completion_fd, payload)
        return result
    except _RunnerInterruption as exc:
        primary_failure = exc
        return 75
    except (OSError, TypeError, ValueError, RuntimeError) as exc:
        primary_failure = exc
        return 74
    finally:
        active_failure = primary_failure or sys.exc_info()[1]
        cleanup_failures = _cleanup_runner_resources(
            progress_fd=progress_fd,
            completion_fd=completion_fd,
        )
        if cleanup_failures and active_failure is None:
            raise RuntimeError("formal runner cleanup failed") from None


if __name__ == "__main__":
    raise SystemExit(main())
