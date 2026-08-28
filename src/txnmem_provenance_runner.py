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
import time
import urllib.error
import urllib.request


FORMAL_RUNNER_UID = 65532
FORMAL_RUNNER_GID = 65532
_PR_GET_DUMPABLE = 3
_PR_SET_DUMPABLE = 4
_PR_GET_NO_NEW_PRIVS = 39
_SMOKE_V2_SCENARIOS = frozenset(
    {
        "normal_prefix",
        "first_ineligible",
        "backend_timeout",
        "interruption",
    }
)
_SMOKE_V2_RECEIPT_SCHEMA = "txnmem-provenance-smoke-child-receipt-v2"


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
