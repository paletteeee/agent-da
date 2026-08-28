"""Gate-controlled provenance candidate entry point for immutable exports."""

from __future__ import annotations

import ctypes
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import signal
import sys
import urllib.request


FORMAL_RUNNER_UID = 65532
FORMAL_RUNNER_GID = 65532
_PR_GET_DUMPABLE = 3
_PR_SET_DUMPABLE = 4
_PR_GET_NO_NEW_PRIVS = 39


class _RunnerInterruption(BaseException):
    """The protected runner received a termination request."""


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
            )
            _write_all(progress_fd, canonical_progress_line(event))

        result = experiment_main(
            arguments,
            _progress_callback=emit_progress,
            _require_formal_eligibility=True,
            _interruption_check=termination_state.raise_if_requested,
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
