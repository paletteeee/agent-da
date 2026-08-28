"""Gate-controlled provenance candidate entry point for immutable exports."""

from __future__ import annotations

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
import threading
import urllib.request


class _RunnerInterruption(BaseException):
    """The protected runner received a termination request."""


class _RunnerTerminationWatchdog:
    """Bound cooperative SIGTERM handling with a hard-exit fallback."""

    def __init__(
        self,
        *,
        grace_seconds: float = 5.0,
        hard_exit=os._exit,
    ) -> None:
        if (
            isinstance(grace_seconds, bool)
            or not isinstance(grace_seconds, (int, float))
            or not math.isfinite(float(grace_seconds))
            or float(grace_seconds) <= 0.0
        ):
            raise ValueError("runner watchdog grace is invalid")
        if not callable(hard_exit):
            raise TypeError("runner watchdog hard exit must be callable")
        self.grace_seconds = float(grace_seconds)
        self._hard_exit = hard_exit
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._read_fd, False)
        os.set_blocking(self._write_fd, False)
        self.stop_requested = False
        self._finished = threading.Event()
        self._started = False
        self._closed = False
        self._thread = threading.Thread(
            target=self._watch,
            name="txnmem-runner-termination-watchdog",
            daemon=True,
        )

    def start(self) -> None:
        if self._closed or self._started:
            raise RuntimeError("runner watchdog cannot be started")
        self._started = True
        self._thread.start()

    def request_stop(self, _signal_number=None, _frame=None) -> None:
        self.stop_requested = True
        if self._closed:
            return
        try:
            os.write(self._write_fd, b"T")
        except (BlockingIOError, OSError):
            pass

    def raise_if_requested(self) -> None:
        if self.stop_requested:
            raise _RunnerInterruption("formal runner interruption requested")

    def _watch(self) -> None:
        while not self.stop_requested:
            if self._finished.is_set():
                return
            try:
                readable, _, _ = select.select(
                    [self._read_fd], [], [], 0.05
                )
            except (OSError, ValueError):
                readable = []
            if readable:
                try:
                    os.read(self._read_fd, 4096)
                except (BlockingIOError, OSError):
                    pass
        if not self._finished.wait(self.grace_seconds):
            self._hard_exit(75)

    def finish(self) -> None:
        if self._closed:
            return
        self._finished.set()
        if self._started and threading.current_thread() is not self._thread:
            self._thread.join(timeout=self.grace_seconds + 0.25)
        self._closed = True
        for descriptor in (self._read_fd, self._write_fd):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _argument_value(arguments: list[str], name: str) -> str:
    if arguments.count(name) != 1:
        raise ValueError("formal runner argument is missing or duplicated")
    index = arguments.index(name)
    if index + 1 >= len(arguments) or not arguments[index + 1]:
        raise ValueError("formal runner argument has no value")
    return arguments[index + 1]


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


def _probe_smoke_qdrant(url: str) -> bool:
    if url != "http://127.0.0.1:19000/readyz":
        raise ValueError("smoke Qdrant endpoint is not exact loopback")
    with urllib.request.urlopen(url, timeout=10.0) as response:
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


def _cleanup_runner_resources(
    *,
    progress_fd: int | None,
    completion_fd: int,
    watchdog: _RunnerTerminationWatchdog,
    previous_sigterm,
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
    try:
        watchdog.finish()
    except BaseException as exc:
        failures.append(exc)
    if previous_sigterm is not None:
        try:
            signal.signal(signal.SIGTERM, previous_sigterm)
        except BaseException as exc:
            failures.append(exc)
    return failures


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
    progress_pair_present = progress_value is not None and progress_binding is not None
    performance_mode = bool(
        arguments and arguments[0] == "provenance-performance"
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
        or (performance_mode and not progress_pair_present)
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

    watchdog = _RunnerTerminationWatchdog()
    previous_sigterm = None
    primary_failure: BaseException | None = None
    try:
        watchdog.start()
        previous_sigterm = signal.signal(signal.SIGTERM, watchdog.request_stop)
        if not arguments or arguments[0] not in {
            "provenance-performance",
            "provenance-smoke",
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
            watchdog.raise_if_requested()
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
            watchdog.raise_if_requested()
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
            )
            _write_all(progress_fd, canonical_progress_line(event))

        result = experiment_main(
            arguments,
            _progress_callback=emit_progress,
            _require_formal_eligibility=True,
            _interruption_check=watchdog.raise_if_requested,
        )
        if type(result) is not int:
            return 73
        if result != 0:
            watchdog.raise_if_requested()
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
            watchdog=watchdog,
            previous_sigterm=previous_sigterm,
        )
        if cleanup_failures and active_failure is None:
            raise RuntimeError("formal runner cleanup failed") from None


if __name__ == "__main__":
    raise SystemExit(main())
