"""Protected production-path ingress smoke without candidate output.

The root controller, Docker daemon, and root-owned Docker forwarding process
are part of this gate's trusted computing base.  The dedicated formal runner
and every other non-root path remain untrusted.  This module emits only a
sanitized gate receipt; it does not produce latency evidence or a promotable
performance candidate.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import errno
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
import urllib.parse

from txnmem_formal_io import FormalIOError, FormalStore, canonical_json_bytes
from txnmem_provenance_contract import (
    FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
    FORMAL_RUNNER_GID,
    FORMAL_RUNNER_UID,
)
from txnmem_provenance_execution_collector import (
    CollectorError,
    _FORMAL_DOCKER_EXECUTABLE,
    _FORMAL_NEO4J_CONTAINER,
    _FORMAL_NEO4J_PROXY,
    _FORMAL_QDRANT_CONTAINER,
    _FORMAL_QDRANT_PROXY,
    _FORMAL_RUNS_ROOT,
    _FORMAL_RUNTIME_WHEEL_DIRECTORY,
    _FORMAL_TOXIPROXY_CONTAINER,
    _NftNetworkGuard,
    _cleanup_formal_execution_resources,
    _collect_formal_environment_attestation,
    _create_locked_runtime_snapshot,
    _file_sha256,
    _formal_endpoint_port,
    _formal_network_table_name,
    _http_read,
    _inspect_docker_backend_isolation_documents,
    _locked_neo4j_graph_database,
    _normalize_docker_backend_isolation,
    _normalize_docker_network_guard_profile,
    _observe_formal_child_process,
    _parse_toxiproxy_version,
    _publish_formal_input_tree,
    _require_formal_uid_processes,
    _start_gated_candidate,
    _toxiproxy_json_request,
    _validate_formal_controller_context,
    _validated_backend_ipv4_by_role,
    _write_collected_environment_snapshot,
    attest_committed_source,
    capture_toxiproxy_counter_snapshot,
    create_immutable_source_export,
    observe_formal_toxiproxy_routes,
    prepare_isolated_toxiproxy_routes,
    verify_immutable_runtime_snapshot,
)
from txnmem_topology_attestation import (
    _sanitize_backend_isolation,
    _validate_network_guard_attestation,
    _validate_network_guard_backend_binding,
    _validate_raw_backend_isolation,
    _validate_sanitized_backend_isolation,
)
from txnmem_toxiproxy_metrics import proxy_counter_values
from txnmem_provenance_performance import (
    ProvenancePerformanceError,
    _load_provenance_candidate,
    formal_config_file_sha256,
    formal_matrix_config_sha256,
    load_strict_json_document,
    validate_environment_attestation,
)
from txnmem_provenance_progress import canonical_snapshot_line
from txnmem_provenance_execution_collector import (
    _derive_toxiproxy_attribution_deltas,
    _validate_toxiproxy_attribution_boundary,
)


_QDRANT_URL = "http://127.0.0.1:19000"
_NEO4J_URI = "bolt://127.0.0.1:19001"
_TOXIPROXY_URL = "http://127.0.0.1:8474"
_TOXIPROXY_VERSION = "2.5.0"
_REPORT_SCHEMA = "txnmem-formal-provenance-smoke-v2"
_CHILD_RECEIPT_SCHEMA = "txnmem-provenance-smoke-child-receipt-v1"
_CHILD_RECEIPT_V2_SCHEMA = "txnmem-provenance-smoke-child-receipt-v2"
_SMOKE_V2_SCENARIOS = (
    "normal_prefix",
    "first_ineligible",
    "backend_timeout",
    "interruption",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_SMOKE_NAME = re.compile(r"^smoke-[0-9a-f]{32}$")
_SMOKE_PROBE_OWNER_LABEL = "txnmem.formal-smoke.owner"
_SMOKE_WORKSPACE_ROOT = _FORMAL_RUNS_ROOT
_REPORT_FIELDS = frozenset(
    {
        "schema",
        "source_commit",
        "source_manifest_sha256",
        "backend_isolation_sha256",
        "network_guard_sha256",
        "toxiproxy_image_manifest_digest",
        "toxiproxy_version",
        "proxy_metrics_series_count",
        "baseline_values_equal",
        "route_rearmed",
        "root_management_succeeded",
        "runner_qdrant_succeeded",
        "runner_neo4j_succeeded",
        "non_runner_proxy_blocked",
        "direct_backend_blocked",
        "forwarded_bridge_blocked",
        "qdrant_delta_positive",
        "neo4j_delta_positive",
        "guard_stable",
        "guard_removed",
        "candidate_created",
        "progress_monotonic",
        "formal_fail_fast",
        "backend_timeout_bounded",
        "interruption_cleanup",
        "candidate_unpublished",
        "report_sha256",
    }
)
_TRUE_REPORT_FIELDS = frozenset(
    {
        "baseline_values_equal",
        "route_rearmed",
        "root_management_succeeded",
        "runner_qdrant_succeeded",
        "runner_neo4j_succeeded",
        "non_runner_proxy_blocked",
        "direct_backend_blocked",
        "forwarded_bridge_blocked",
        "qdrant_delta_positive",
        "neo4j_delta_positive",
        "guard_stable",
        "guard_removed",
        "progress_monotonic",
        "formal_fail_fast",
        "backend_timeout_bounded",
        "interruption_cleanup",
        "candidate_unpublished",
    }
)


class FormalSmokeError(RuntimeError):
    """The production same-path smoke could not prove its closed gate."""


@dataclass(frozen=True)
class _SmokeWorkspace:
    path: Path
    device: int
    inode: int
    parent_device: int
    parent_inode: int


@dataclass(frozen=True)
class _SmokeChildSpec:
    command: tuple[str, ...]
    cwd: Path
    environment: dict[str, str]


@dataclass(frozen=True)
class _SmokeTopology:
    raw_backend_isolation: dict[str, Any]
    sanitized_backend_isolation: dict[str, Any]
    guard_profile: dict[str, str]
    backend_ipv4_by_role: dict[str, str]
    probe_image_id: str
    toxiproxy_manifest_digest: str


@dataclass(frozen=True)
class _SmokeV2ScenarioSpec:
    scenario: str
    identity_sha256: str
    directory: Path
    progress_path: Path


@dataclass(frozen=True)
class _SmokeV2Execution:
    outcomes: tuple[dict[str, Any], ...]
    normal_receipt: dict[str, Any]
    routes_b: list[dict[str, Any]]
    baseline_b: dict[str, Any]
    final_counters: dict[str, Any]
    initial_guard: dict[str, Any]


def _create_smoke_v2_scenario_specs(
    workspace: Path,
    *,
    source_commit: str,
    controller_uid: int = 0,
    controller_gid: int = FORMAL_RUNNER_GID,
) -> tuple[_SmokeV2ScenarioSpec, ...]:
    if (
        not isinstance(workspace, Path)
        or not isinstance(source_commit, str)
        or _COMMIT.fullmatch(source_commit) is None
        or type(controller_uid) is not int
        or controller_uid < 0
        or type(controller_gid) is not int
        or controller_gid < 0
    ):
        raise FormalSmokeError("formal smoke v2 scenario root is invalid")
    root = workspace.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise FormalSmokeError("formal smoke v2 scenario root is unavailable")
    root = root.resolve(strict=True)
    metadata = root.stat()
    if (
        metadata.st_uid != controller_uid
        or metadata.st_gid != controller_gid
        or stat.S_IMODE(metadata.st_mode) not in {0o550, 0o700}
    ):
        raise FormalSmokeError("formal smoke v2 scenario root is not protected")

    created: list[Path] = []
    specs: list[_SmokeV2ScenarioSpec] = []
    try:
        for index, scenario in enumerate(_SMOKE_V2_SCENARIOS, start=1):
            binding = {
                "schema": "txnmem-formal-smoke-scenario-binding-v1",
                "source_commit": source_commit,
                "workspace_device": int(metadata.st_dev),
                "workspace_inode": int(metadata.st_ino),
                "scenario": scenario,
            }
            identity_sha256 = hashlib.sha256(
                canonical_json_bytes(binding)
            ).hexdigest()
            directory = root / (
                f"diagnostic-{index:02d}-{identity_sha256[:16]}"
            )
            directory.mkdir(mode=0o700)
            created.append(directory)
            if directory.is_symlink():
                raise FormalSmokeError("formal smoke v2 scenario directory is linked")
            observed = directory.stat()
            if observed.st_uid != controller_uid or observed.st_gid != controller_gid:
                os.chown(directory, controller_uid, controller_gid)
            directory.chmod(0o700)
            observed = directory.stat()
            if (
                not stat.S_ISDIR(observed.st_mode)
                or observed.st_uid != controller_uid
                or observed.st_gid != controller_gid
                or stat.S_IMODE(observed.st_mode) != 0o700
            ):
                raise FormalSmokeError(
                    "formal smoke v2 scenario directory is not protected"
                )
            specs.append(
                _SmokeV2ScenarioSpec(
                    scenario=scenario,
                    identity_sha256=identity_sha256,
                    directory=directory,
                    progress_path=directory / "progress.json",
                )
            )
    except BaseException as primary:
        cleanup_failure: BaseException | None = None
        for directory in reversed(created):
            try:
                directory.rmdir()
            except BaseException as exc:
                if cleanup_failure is None:
                    cleanup_failure = exc
        failure = FormalSmokeError("formal smoke v2 scenario creation failed")
        if cleanup_failure is not None:
            try:
                failure.add_note(
                    "formal smoke v2 scenario rollback also failed: "
                    f"{type(cleanup_failure).__name__}"
                )
            except AttributeError:
                pass
        raise failure from primary
    if len({spec.identity_sha256 for spec in specs}) != len(_SMOKE_V2_SCENARIOS):
        raise FormalSmokeError("formal smoke v2 scenario identities collided")
    return tuple(specs)


def _require_root_linux() -> None:
    if (
        os.name != "posix"
        or not hasattr(os, "geteuid")
        or os.geteuid() != 0
        or not Path("/proc/self/status").is_file()
    ):
        raise FormalSmokeError(
            "formal smoke requires the protected root Linux controller"
        )


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _prepare_smoke_output(project_root: Path, out_path: Path) -> tuple[FormalStore, str]:
    if not isinstance(out_path, Path) or not out_path.is_absolute():
        raise FormalSmokeError("formal smoke output must be absolute")
    root = project_root.expanduser().absolute().resolve(strict=True)
    requested = out_path.expanduser().absolute()
    if requested.name in {"", ".", ".."}:
        raise FormalSmokeError("formal smoke output filename is invalid")
    parent = requested.parent
    if parent.is_symlink() or not parent.is_dir():
        raise FormalSmokeError("formal smoke output parent is unavailable")
    resolved_parent = parent.resolve(strict=True)
    if resolved_parent != parent:
        raise FormalSmokeError("formal smoke output parent is not canonical")
    normalized = resolved_parent / requested.name
    if _is_within(normalized, root):
        raise FormalSmokeError("formal smoke output must be outside the repository")
    if normalized.is_symlink() or normalized.exists():
        raise FormalSmokeError("refusing to overwrite formal smoke output")
    return FormalStore(resolved_parent), normalized.name


def _create_smoke_workspace(
    *,
    workspace_root: Path = _SMOKE_WORKSPACE_ROOT,
    controller_uid: int = 0,
    runner_gid: int = FORMAL_RUNNER_GID,
) -> _SmokeWorkspace:
    root = workspace_root.expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise FormalSmokeError("formal smoke workspace root is unavailable")
    root = root.resolve(strict=True)
    metadata = root.stat()
    if (
        metadata.st_uid != controller_uid
        or metadata.st_gid != runner_gid
        or stat.S_IMODE(metadata.st_mode) != 0o750
    ):
        raise FormalSmokeError("formal smoke workspace root is not protected")
    root_descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    name = "smoke-" + secrets.token_hex(16)
    created = False
    try:
        os.mkdir(name, mode=0o700, dir_fd=root_descriptor)
        created = True
        created_metadata = os.stat(
            name, dir_fd=root_descriptor, follow_symlinks=False
        )
        if (
            created_metadata.st_uid != controller_uid
            or created_metadata.st_gid != runner_gid
        ):
            os.chown(
                name,
                controller_uid,
                runner_gid,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
        os.chmod(
            name,
            0o700,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        descriptor = os.open(
            name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=root_descriptor,
        )
        try:
            child = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(child.st_mode)
                or child.st_uid != controller_uid
                or child.st_gid != runner_gid
                or stat.S_IMODE(child.st_mode) != 0o700
            ):
                raise FormalSmokeError("formal smoke workspace identity is invalid")
            return _SmokeWorkspace(
                path=root / name,
                device=int(child.st_dev),
                inode=int(child.st_ino),
                parent_device=int(metadata.st_dev),
                parent_inode=int(metadata.st_ino),
            )
        finally:
            os.close(descriptor)
    except BaseException:
        if created:
            try:
                os.rmdir(name, dir_fd=root_descriptor)
            except OSError as cleanup_error:
                raise FormalSmokeError(
                    "formal smoke workspace rollback failed"
                ) from cleanup_error
        raise
    finally:
        os.close(root_descriptor)


def _remove_workspace_tree(directory_descriptor: int) -> None:
    os.fchmod(directory_descriptor, 0o700)
    try:
        names = sorted(os.listdir(directory_descriptor))
    except OSError as exc:
        raise FormalSmokeError("cannot enumerate formal smoke workspace") from exc
    for name in names:
        if not isinstance(name, str) or name in {"", ".", ".."} or "/" in name:
            raise FormalSmokeError("formal smoke workspace entry is unsafe")
        metadata = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            child = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_descriptor,
            )
            try:
                _remove_workspace_tree(child)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=directory_descriptor)
        elif stat.S_ISREG(metadata.st_mode):
            os.unlink(name, dir_fd=directory_descriptor)
        else:
            raise FormalSmokeError(
                "formal smoke workspace contains a link or special file"
            )


def _remove_smoke_workspace(
    workspace: _SmokeWorkspace,
    *,
    workspace_root: Path = _SMOKE_WORKSPACE_ROOT,
) -> None:
    if not isinstance(workspace, _SmokeWorkspace):
        raise FormalSmokeError("formal smoke cleanup identity is invalid")
    root = workspace_root.expanduser().absolute().resolve(strict=True)
    target = workspace.path.expanduser().absolute()
    if target.parent != root or not _SMOKE_NAME.fullmatch(target.name):
        raise FormalSmokeError("formal smoke cleanup target is unsafe")
    root_descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        parent = os.fstat(root_descriptor)
        if (
            int(parent.st_dev) != workspace.parent_device
            or int(parent.st_ino) != workspace.parent_inode
        ):
            raise FormalSmokeError("formal smoke workspace parent identity changed")
        descriptor = os.open(
            target.name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=root_descriptor,
        )
        try:
            current = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(current.st_mode)
                or int(current.st_dev) != workspace.device
                or int(current.st_ino) != workspace.inode
            ):
                raise FormalSmokeError("formal smoke workspace identity changed")
            _remove_workspace_tree(descriptor)
        finally:
            os.close(descriptor)
        os.rmdir(target.name, dir_fd=root_descriptor)
        try:
            os.stat(target.name, dir_fd=root_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise FormalSmokeError("formal smoke workspace cleanup failed")
    except OSError as exc:
        raise FormalSmokeError("formal smoke workspace cleanup failed") from exc
    finally:
        os.close(root_descriptor)


def _validate_smoke_endpoints(
    qdrant_url: str, neo4j_uri: str, toxiproxy_url: str
) -> None:
    if (
        qdrant_url != _QDRANT_URL
        or neo4j_uri != _NEO4J_URI
        or toxiproxy_url != _TOXIPROXY_URL
    ):
        raise FormalSmokeError("formal smoke endpoints are not exact loopback paths")
    _formal_endpoint_port(
        qdrant_url,
        field="formal smoke Qdrant endpoint",
        schemes={"http"},
        expected_port=19000,
        expected_hosts={"127.0.0.1"},
    )
    _formal_endpoint_port(
        neo4j_uri,
        field="formal smoke Neo4j endpoint",
        schemes={"bolt"},
        expected_port=19001,
        expected_hosts={"127.0.0.1"},
    )
    _formal_endpoint_port(
        toxiproxy_url,
        field="formal smoke Toxiproxy endpoint",
        schemes={"http"},
        expected_port=8474,
        expected_hosts={"127.0.0.1"},
    )


def _build_smoke_child_spec(
    *,
    source_export: Path,
    runtime_snapshot: Path,
    runtime_manifest: Mapping[str, Any],
    runner_sha256: str,
    neo4j_password: str,
    scenario: str,
    environment_attestation_path: Path,
    environment_attestation_sha256: str,
) -> _SmokeChildSpec:
    if type(scenario) is not str or scenario not in _SMOKE_V2_SCENARIOS:
        raise FormalSmokeError("formal smoke v2 scenario is invalid")
    export = source_export.expanduser().absolute().resolve(strict=True)
    runtime = runtime_snapshot.expanduser().absolute().resolve(strict=True)
    environment_path = (
        environment_attestation_path.expanduser().absolute().resolve(strict=True)
    )
    runner = export / "src" / "txnmem_provenance_runner.py"
    if not _SHA256.fullmatch(runner_sha256) or _file_sha256(
        runner, "formal smoke runner"
    ) != runner_sha256:
        raise FormalSmokeError("formal smoke runner hash mismatch")
    if not isinstance(neo4j_password, str) or not neo4j_password:
        raise FormalSmokeError("Neo4j runtime credential is unavailable")
    if not _SHA256.fullmatch(environment_attestation_sha256):
        raise FormalSmokeError("formal smoke environment hash is invalid")
    try:
        environment_document, _environment_raw = load_strict_json_document(
            environment_path
        )
        if type(environment_document) is not dict:
            raise ValueError("formal smoke environment must be an object")
        validated_environment = validate_environment_attestation(
            environment_document
        )
    except (OSError, TypeError, ValueError, ProvenancePerformanceError):
        raise FormalSmokeError("formal smoke environment is invalid") from None
    if (
        hashlib.sha256(
            canonical_json_bytes(validated_environment)
        ).hexdigest()
        != environment_attestation_sha256
        or validated_environment.get("isolation_verified") is not True
        or validated_environment.get("co_tenant_load_detected") is not False
    ):
        raise FormalSmokeError("formal smoke environment hash mismatch")
    environment_metadata = environment_path.lstat()
    if (
        not stat.S_ISREG(environment_metadata.st_mode)
        or environment_metadata.st_uid != 0
        or environment_metadata.st_gid != FORMAL_RUNNER_GID
        or stat.S_IMODE(environment_metadata.st_mode) != 0o440
    ):
        raise FormalSmokeError("formal smoke environment is not protected")
    verify_immutable_runtime_snapshot(
        runtime,
        runtime_manifest,
        directory_mode=0o550,
        file_mode=0o440,
        expected_uid=0,
        expected_gid=FORMAL_RUNNER_GID,
    )
    command = (
        sys.executable,
        "-I",
        "-S",
        "-B",
        str(runner),
        "provenance-smoke-v2",
        scenario,
    )
    return _SmokeChildSpec(
        command=command,
        cwd=export,
        environment={
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
            "TXNMEM_NEO4J_PASSWORD": neo4j_password,
            "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime),
            "TXNMEM_PROVENANCE_SMOKE_ENVIRONMENT_PATH": str(
                environment_path
            ),
        },
    )


_SMOKE_TIMEOUT_TOXIC_NAME = "txnmem-formal-smoke-timeout-v2"


def _install_smoke_timeout_toxic(toxiproxy_url: str) -> None:
    path = (
        "/proxies/"
        + urllib.parse.quote(_FORMAL_QDRANT_PROXY, safe="")
        + "/toxics"
    )
    created = _toxiproxy_json_request(
        toxiproxy_url,
        path,
        method="POST",
        payload={
            "name": _SMOKE_TIMEOUT_TOXIC_NAME,
            "type": "latency",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": {"latency": 2000, "jitter": 0},
        },
    )
    if (
        type(created) is not dict
        or set(created)
        != {"name", "type", "stream", "toxicity", "attributes"}
        or created.get("name") != _SMOKE_TIMEOUT_TOXIC_NAME
        or created.get("type") != "latency"
        or created.get("stream") != "downstream"
        or isinstance(created.get("toxicity"), bool)
        or created.get("toxicity") != 1.0
        or created.get("attributes") != {"latency": 2000, "jitter": 0}
    ):
        raise FormalSmokeError("formal smoke timeout toxic is invalid")


def _remove_smoke_timeout_toxic(
    toxiproxy_url: str,
    *,
    allow_not_found: bool = False,
) -> None:
    if type(allow_not_found) is not bool:
        raise FormalSmokeError("formal smoke timeout toxic cleanup is invalid")
    path = (
        "/proxies/"
        + urllib.parse.quote(_FORMAL_QDRANT_PROXY, safe="")
        + "/toxics/"
        + urllib.parse.quote(_SMOKE_TIMEOUT_TOXIC_NAME, safe="")
    )
    removed = _toxiproxy_json_request(
        toxiproxy_url,
        path,
        method="DELETE",
        allow_not_found=allow_not_found,
    )
    if removed is not None:
        raise FormalSmokeError("formal smoke timeout toxic cleanup is invalid")


def _assert_smoke_scenario_unpublished(spec: _SmokeV2ScenarioSpec) -> int:
    directory = spec.directory.expanduser().absolute()
    if directory.is_symlink() or not directory.is_dir():
        raise FormalSmokeError("formal smoke scenario artifact root is invalid")
    directory = directory.resolve(strict=True)
    try:
        entries = tuple(directory.iterdir())
    except OSError as exc:
        raise FormalSmokeError(
            "formal smoke scenario artifacts are unavailable"
        ) from exc
    if (
        len(entries) != 1
        or entries[0] != spec.progress_path
        or entries[0].is_symlink()
        or not entries[0].is_file()
    ):
        raise FormalSmokeError("formal smoke scenario emitted an artifact")
    bundle_id = (
        "diagnostic-vector_graph-"
        + spec.identity_sha256[:16]
        + "-"
        + spec.identity_sha256[16:32]
    )
    try:
        _load_provenance_candidate(directory, bundle_id)
    except (FileNotFoundError, FormalIOError, ProvenancePerformanceError):
        return 0
    raise FormalSmokeError("formal smoke scenario was promotable")


def _validate_smoke_starting_progress(
    value: Any,
    *,
    spec: _SmokeV2ScenarioSpec,
    config_sha256: str,
) -> dict[str, Any]:
    try:
        canonical_snapshot_line(value)
    except BaseException:
        raise FormalSmokeError(
            "formal smoke starting progress is invalid"
        ) from None
    if (
        type(value) is not dict
        or value.get("run_binding_sha256") != spec.identity_sha256
        or value.get("config_sha256") != config_sha256
        or value.get("status") != "starting"
        or value.get("cell_index") != 1
        or value.get("graph_size") != 100
        or value.get("concurrency") != 1
        or value.get("repetition_index") != 0
        or value.get("completed_repetitions") != 0
        or value.get("completed_samples") != 0
        or value.get("update_sequence") != 0
        or "terminal_reason_class" in value
    ):
        raise FormalSmokeError("formal smoke starting progress is invalid")
    return dict(value)


def _run_smoke_v2_scenarios(
    *,
    scenario_specs: Sequence[_SmokeV2ScenarioSpec],
    source_export: Path,
    runtime_snapshot: Path,
    runtime_manifest: Mapping[str, Any],
    runner_sha256: str,
    neo4j_password: str,
    environment_attestation_path: Path,
    environment_attestation_sha256: str,
    topology: _SmokeTopology,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    routes_a: Sequence[Mapping[str, Any]],
    baseline_a: Mapping[str, Any],
) -> _SmokeV2Execution:
    """Run four isolated diagnostic children and close every lifecycle."""

    specs = tuple(scenario_specs)
    if (
        len(specs) != len(_SMOKE_V2_SCENARIOS)
        or tuple(spec.scenario for spec in specs) != _SMOKE_V2_SCENARIOS
        or len({spec.identity_sha256 for spec in specs}) != len(specs)
        or len({spec.progress_path for spec in specs}) != len(specs)
    ):
        raise FormalSmokeError("formal smoke v2 scenario closure is invalid")
    config_sha256 = formal_matrix_config_sha256()
    config_file_sha256 = formal_config_file_sha256()
    outcomes: list[dict[str, Any]] = []
    normal_receipt: dict[str, Any] | None = None
    normal_routes: list[dict[str, Any]] | None = None
    normal_baseline: dict[str, Any] | None = None
    normal_final: dict[str, Any] | None = None
    normal_guard: dict[str, Any] | None = None

    for spec in specs:
        scenario = spec.scenario
        child: Any = None
        guard: Any = None
        toxic_cleanup_required = False
        toxic_install_completed = False
        primary: BaseException | None = None
        primary_traceback = None
        progress_view: dict[str, Any] | None = None
        receipt: dict[str, Any] | None = None
        scenario_routes: list[dict[str, Any]] | None = None
        guard_attestation: dict[str, Any] | None = None
        blocked_at_ns: int | None = None
        started_ns = time.monotonic_ns()
        verdict_started_ns = started_ns
        try:
            child_spec = _build_smoke_child_spec(
                source_export=source_export,
                runtime_snapshot=runtime_snapshot,
                runtime_manifest=runtime_manifest,
                runner_sha256=runner_sha256,
                neo4j_password=neo4j_password,
                scenario=scenario,
                environment_attestation_path=environment_attestation_path,
                environment_attestation_sha256=(
                    environment_attestation_sha256
                ),
            )
            _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
            child = _start_gated_candidate(
                command=child_spec.command,
                cwd=child_spec.cwd,
                environment=child_spec.environment,
                formal_uid=FORMAL_RUNNER_UID,
                formal_gid=FORMAL_RUNNER_GID,
                require_completion_receipt=True,
                require_progress=True,
                progress_allow_empty=True,
                progress_binding_sha256=spec.identity_sha256,
                progress_config_sha256=config_sha256,
                progress_snapshot_path=spec.progress_path,
                progress_expected_uid=0,
                progress_expected_gid=FORMAL_RUNNER_GID,
            )
            _validate_smoke_starting_progress(
                child.read_progress(), spec=spec, config_sha256=config_sha256
            )
            observed_process = _observe_formal_child_process(
                child.process.pid,
                expected_command=child_spec.command,
                expected_uid=FORMAL_RUNNER_UID,
            )
            start_identity = str(observed_process["start_identity"])
            child.bind_process_identity(start_identity)
            child_ticks = start_identity.rsplit(":", 1)[-1]
            _require_formal_uid_processes(
                FORMAL_RUNNER_UID,
                expected={child.process.pid: child_ticks},
            )
            guard = _NftNetworkGuard(
                _formal_network_table_name(spec.identity_sha256),
                **topology.guard_profile,
            )
            guard_attestation = _validate_smoke_guard(
                guard.activate(), topology
            )
            scenario_routes = prepare_isolated_toxiproxy_routes(
                toxiproxy_url,
                qdrant_proxy=_FORMAL_QDRANT_PROXY,
                neo4j_proxy=_FORMAL_NEO4J_PROXY,
            )

            if scenario == "normal_prefix":
                normal_baseline = capture_toxiproxy_counter_snapshot(
                    toxiproxy_url,
                    phase="baseline_b",
                    proxy_routes=scenario_routes,
                )
                _validate_toxiproxy_attribution_boundary(
                    baseline_a,
                    normal_baseline,
                    routes_a,
                    scenario_routes,
                )
                if _probe_root_management(toxiproxy_url, scenario_routes) is not True:
                    raise FormalSmokeError(
                        "formal smoke root management gate failed"
                    )
                if _probe_root_data_denial(
                    qdrant_url=qdrant_url, neo4j_uri=neo4j_uri
                ) is not True:
                    raise FormalSmokeError(
                        "formal smoke non-runner proxy gate failed"
                    )
                if _probe_direct_backend_denial(
                    topology.backend_ipv4_by_role
                ) is not True:
                    raise FormalSmokeError(
                        "formal smoke direct backend gate failed"
                    )
                if _probe_forward_path_denial(
                    backend_ipv4_by_role=topology.backend_ipv4_by_role,
                    image_id=topology.probe_image_id,
                ) is not True:
                    raise FormalSmokeError(
                        "formal smoke forwarded bridge gate failed"
                    )
            elif scenario == "backend_timeout":
                toxic_cleanup_required = True
                _install_smoke_timeout_toxic(toxiproxy_url)
                toxic_install_completed = True
                verdict_started_ns = time.monotonic_ns()

            child.release()
            if scenario == "interruption":
                signal_delivered = child.terminate_validated_group(
                    term_seconds=5.0,
                    kill_seconds=5.0,
                    require_signal=True,
                )
                if signal_delivered is not True:
                    raise FormalSmokeError(
                        "formal smoke interruption signal was not proven"
                    )
                child.require_quiescence()
                if child.finish_progress(2.0, allow_empty=True) is not None:
                    raise FormalSmokeError(
                        "formal smoke interruption emitted progress"
                    )
                progress_view = child.interrupt_progress()
            else:
                receipt_timeout = 6.0 if scenario == "backend_timeout" else 30.0
                exit_code, raw_receipt = child.wait_with_receipt(
                    timeout=receipt_timeout
                )
                if exit_code != 0:
                    raise FormalSmokeError("formal smoke v2 runner failed")
                receipt = _validate_smoke_v2_child_receipt(
                    raw_receipt, scenario=scenario
                )
                drained = child.finish_progress(2.0, allow_empty=True)
                if scenario == "normal_prefix":
                    if type(drained) is not dict:
                        raise FormalSmokeError(
                            "formal smoke normal progress is unavailable"
                        )
                    progress_view = dict(drained)
                else:
                    if drained is not None:
                        raise FormalSmokeError(
                            "formal smoke fail-fast emitted progress"
                        )
                    reason = (
                        "formal_eligibility_failed"
                        if scenario == "first_ineligible"
                        else "backend_timeout"
                    )
                    progress_view = child.block_progress(reason)
                    if scenario == "backend_timeout":
                        blocked_at_ns = time.monotonic_ns()
            child.require_quiescence()
            _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})

            verified_guard = _validate_smoke_guard(guard.verify(), topology)
            if canonical_json_bytes(verified_guard) != canonical_json_bytes(
                guard_attestation
            ):
                raise FormalSmokeError("formal smoke network guard drifted")
            if scenario == "normal_prefix":
                observed_routes = observe_formal_toxiproxy_routes(
                    toxiproxy_url,
                    qdrant_proxy=_FORMAL_QDRANT_PROXY,
                    neo4j_proxy=_FORMAL_NEO4J_PROXY,
                )
                if observed_routes != scenario_routes:
                    raise FormalSmokeError(
                        "formal smoke routes drifted during execution"
                    )
                normal_final = capture_toxiproxy_counter_snapshot(
                    toxiproxy_url,
                    phase="final",
                    proxy_routes=observed_routes,
                )
                normal_receipt = dict(receipt or {})
                normal_routes = [dict(row) for row in scenario_routes]
                normal_guard = dict(guard_attestation)
            elif scenario != "backend_timeout":
                observed_routes = observe_formal_toxiproxy_routes(
                    toxiproxy_url,
                    qdrant_proxy=_FORMAL_QDRANT_PROXY,
                    neo4j_proxy=_FORMAL_NEO4J_PROXY,
                )
                if observed_routes != scenario_routes:
                    raise FormalSmokeError(
                        "formal smoke routes drifted during execution"
                    )
        except BaseException as exc:
            primary = exc
            primary_traceback = exc.__traceback__

        cleanup_failures: list[BaseException] = []
        child_quiescent = child is None
        if child is not None:
            cleanup_failures.extend(
                _cleanup_formal_execution_resources(
                    execution_monitor=None,
                    network_guard=None,
                    child=child,
                )
            )
            try:
                _verify_smoke_child_quiescence(child)
                child_quiescent = True
            except BaseException as exc:
                cleanup_failures.append(exc)
        if toxic_cleanup_required:
            if child_quiescent:
                try:
                    _remove_smoke_timeout_toxic(
                        toxiproxy_url,
                        allow_not_found=not toxic_install_completed,
                    )
                    observed_routes = observe_formal_toxiproxy_routes(
                        toxiproxy_url,
                        qdrant_proxy=_FORMAL_QDRANT_PROXY,
                        neo4j_proxy=_FORMAL_NEO4J_PROXY,
                    )
                    if observed_routes != scenario_routes:
                        raise FormalSmokeError(
                            "formal smoke routes did not recover after timeout"
                        )
                    toxic_cleanup_required = False
                except BaseException as exc:
                    cleanup_failures.append(exc)
            else:
                cleanup_failures.append(
                    FormalSmokeError(
                        "formal smoke preserved timeout toxic after live child"
                    )
                )
        guard_removed = guard is None
        if guard is not None and bool(getattr(guard, "active", False)):
            if child_quiescent and not toxic_cleanup_required:
                try:
                    guard.deactivate()
                    if bool(getattr(guard, "active", False)):
                        raise FormalSmokeError(
                            "formal smoke guard cleanup failed"
                        )
                    guard_removed = True
                except BaseException as exc:
                    cleanup_failures.append(exc)
            else:
                cleanup_failures.append(
                    FormalSmokeError(
                        "formal smoke preserved guard after live child"
                    )
                )

        if primary is not None:
            if cleanup_failures:
                try:
                    primary.add_note(
                        "formal smoke v2 cleanup also failed: "
                        + type(cleanup_failures[0]).__name__
                    )
                except AttributeError:
                    pass
            raise primary.with_traceback(primary_traceback)
        if cleanup_failures:
            raise FormalSmokeError(
                "formal smoke v2 scenario cleanup failed"
            ) from cleanup_failures[0]
        if (
            progress_view is None
            or child_quiescent is not True
            or guard_removed is not True
        ):
            raise FormalSmokeError("formal smoke v2 scenario did not close")
        candidate_artifact_count = _assert_smoke_scenario_unpublished(spec)
        elapsed_end_ns = blocked_at_ns or time.monotonic_ns()
        elapsed_milliseconds = max(
            1,
            (elapsed_end_ns - verdict_started_ns + 999_999) // 1_000_000,
        )
        outcomes.append(
            {
                "scenario": scenario,
                "identity_sha256": spec.identity_sha256,
                "progress": dict(progress_view),
                "receipt": dict(receipt) if receipt is not None else None,
                "elapsed_milliseconds": int(elapsed_milliseconds),
                "child_quiescent": True,
                "guard_removed": True,
                "candidate_artifact_count": candidate_artifact_count,
            }
        )

    if (
        formal_matrix_config_sha256() != config_sha256
        or formal_config_file_sha256() != config_file_sha256
    ):
        raise FormalSmokeError("formal smoke changed the formal matrix config")
    if any(
        value is None
        for value in (
            normal_receipt,
            normal_routes,
            normal_baseline,
            normal_final,
            normal_guard,
        )
    ):
        raise FormalSmokeError("formal smoke normal scenario is incomplete")
    _validate_smoke_v2_outcomes(outcomes)
    return _SmokeV2Execution(
        outcomes=tuple(dict(row) for row in outcomes),
        normal_receipt=dict(normal_receipt),
        routes_b=[dict(row) for row in normal_routes],
        baseline_b=dict(normal_baseline),
        final_counters=dict(normal_final),
        initial_guard=dict(normal_guard),
    )


def _observe_smoke_topology() -> _SmokeTopology:
    containers, backend_network, ingress_network = (
        _inspect_docker_backend_isolation_documents(
            qdrant_container=_FORMAL_QDRANT_CONTAINER,
            neo4j_container=_FORMAL_NEO4J_CONTAINER,
            toxiproxy_container=_FORMAL_TOXIPROXY_CONTAINER,
        )
    )
    try:
        raw_backend = _validate_raw_backend_isolation(
            _normalize_docker_backend_isolation(
                containers, backend_network, ingress_network
            )
        )
        sanitized_backend = _validate_sanitized_backend_isolation(
            _sanitize_backend_isolation(raw_backend)
        )
    except ValueError as exc:
        raise FormalSmokeError("formal smoke backend isolation is invalid") from exc
    addresses = _validated_backend_ipv4_by_role(
        containers, backend_network, ingress_network
    )
    profile = _normalize_docker_network_guard_profile(
        backend_network, ingress_network
    )
    if addresses.get("toxiproxy_ingress") != raw_backend.get(
        "toxiproxy_ingress_ipv4"
    ):
        raise FormalSmokeError("formal smoke proxy ingress identity drifted")
    profile = {
        **profile,
        "toxiproxy_ingress_ipv4": addresses["toxiproxy_ingress"],
    }
    probe_image_id = containers.get("qdrant", {}).get("Image")
    if not isinstance(probe_image_id, str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", probe_image_id
    ):
        raise FormalSmokeError("formal smoke probe image identity is invalid")
    toxiproxy_rows = [
        row
        for row in sanitized_backend["containers"]
        if row.get("role") == "toxiproxy"
    ]
    if len(toxiproxy_rows) != 1 or toxiproxy_rows[0].get(
        "manifest_digest"
    ) != FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS["toxiproxy"]:
        raise FormalSmokeError("formal smoke Toxiproxy image is not registered")
    return _SmokeTopology(
        raw_backend_isolation=raw_backend,
        sanitized_backend_isolation=sanitized_backend,
        guard_profile=profile,
        backend_ipv4_by_role=addresses,
        probe_image_id=probe_image_id,
        toxiproxy_manifest_digest=FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[
            "toxiproxy"
        ],
    )


def _validate_smoke_guard(
    value: Any, topology: _SmokeTopology
) -> dict[str, Any]:
    try:
        guard = _validate_network_guard_attestation(value)
        _validate_network_guard_backend_binding(
            guard, topology.raw_backend_isolation
        )
    except ValueError as exc:
        raise FormalSmokeError("formal smoke network guard is invalid") from exc
    return guard


def _run_one_neo4j_transaction(
    *, runtime_snapshot: Path, neo4j_uri: str, neo4j_password: str
) -> bool:
    with _locked_neo4j_graph_database(runtime_snapshot) as GraphDatabase:
        driver = GraphDatabase.driver(
            neo4j_uri, auth=("neo4j", neo4j_password)
        )
        session = None
        transaction = None
        committed = False
        try:
            session = driver.session()
            transaction = session.begin_transaction()
            record = transaction.run("RETURN 1 AS value").single(strict=True)
            if record is None or record["value"] != 1:
                return False
            transaction.commit()
            committed = True
            return True
        finally:
            if transaction is not None and not committed:
                try:
                    transaction.rollback()
                except Exception:
                    pass
            if session is not None:
                session.close()
            driver.close()


def _probe_root_health(
    *,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    neo4j_password: str,
    runtime_snapshot: Path,
) -> str:
    _http_read(qdrant_url.rstrip("/") + "/readyz")
    if not _run_one_neo4j_transaction(
        runtime_snapshot=runtime_snapshot,
        neo4j_uri=neo4j_uri,
        neo4j_password=neo4j_password,
    ):
        raise FormalSmokeError("formal smoke Neo4j health probe failed")
    body, _elapsed = _http_read(toxiproxy_url.rstrip("/") + "/version")
    try:
        version = _parse_toxiproxy_version(body)
    except CollectorError as exc:
        raise FormalSmokeError(
            "formal smoke Toxiproxy version response is invalid"
        ) from exc
    if version != _TOXIPROXY_VERSION:
        raise FormalSmokeError("formal smoke Toxiproxy version drifted")
    return version


def _probe_root_management(
    toxiproxy_url: str, expected_routes: Sequence[Mapping[str, Any]]
) -> bool:
    observed = observe_formal_toxiproxy_routes(
        toxiproxy_url,
        qdrant_proxy=_FORMAL_QDRANT_PROXY,
        neo4j_proxy=_FORMAL_NEO4J_PROXY,
    )
    return observed == [dict(row) for row in expected_routes]


def _tcp_connection_denied(host: str, port: int) -> bool:
    connection = None
    try:
        connection = socket.create_connection((host, port), timeout=2.0)
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.ECONNREFUSED:
            return True
        raise FormalSmokeError(
            "formal smoke unexpected connection probe failure"
        ) from exc
    finally:
        if connection is not None:
            connection.close()
    return False


def _probe_root_data_denial(*, qdrant_url: str, neo4j_uri: str) -> bool:
    qdrant_port = _formal_endpoint_port(
        qdrant_url,
        field="formal smoke Qdrant denial endpoint",
        schemes={"http"},
        expected_port=19000,
        expected_hosts={"127.0.0.1"},
    )
    neo4j_port = _formal_endpoint_port(
        neo4j_uri,
        field="formal smoke Neo4j denial endpoint",
        schemes={"bolt"},
        expected_port=19001,
        expected_hosts={"127.0.0.1"},
    )
    return _tcp_connection_denied(
        "127.0.0.1", qdrant_port
    ) and _tcp_connection_denied("127.0.0.1", neo4j_port)


def _normalize_probe_addresses(value: Mapping[str, Any]) -> dict[str, str]:
    roles = ("qdrant", "neo4j", "toxiproxy_ingress")
    if not isinstance(value, Mapping) or set(value) != set(roles):
        raise FormalSmokeError("formal smoke backend address closure is invalid")
    normalized: dict[str, str] = {}
    for role in roles:
        try:
            address = ipaddress.IPv4Address(value[role])
        except (TypeError, ValueError) as exc:
            raise FormalSmokeError(
                "formal smoke backend address is invalid"
            ) from exc
        if (
            str(address) != value[role]
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_unspecified
        ):
            raise FormalSmokeError("formal smoke backend address is invalid")
        normalized[role] = str(address)
    return normalized


def _probe_direct_backend_denial(
    backend_ipv4_by_role: Mapping[str, Any],
) -> bool:
    addresses = _normalize_probe_addresses(backend_ipv4_by_role)
    return _tcp_connection_denied(
        addresses["qdrant"], 6333
    ) and _tcp_connection_denied(addresses["neo4j"], 7687)


def _docker_run(arguments: Sequence[str], *, timeout: float = 15.0):
    try:
        return subprocess.run(
            [str(_FORMAL_DOCKER_EXECUTABLE), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FormalSmokeError("formal smoke container probe failed") from exc


def _validate_probe_container_ref(container_ref: str) -> None:
    if not isinstance(container_ref, str) or not re.fullmatch(
        r"(?:[0-9a-f]{64}|txnmem-smoke-[0-9a-f]{24})", container_ref
    ):
        raise FormalSmokeError("formal smoke container cleanup identity is invalid")


@dataclass(frozen=True)
class _ProbeContainerIdentity:
    container_id: str
    labels: dict[str, str]


def _docker_inspect_is_not_found(result: Any, container_ref: str) -> bool:
    if getattr(result, "returncode", None) != 1:
        return False
    stdout = getattr(result, "stdout", None)
    stderr = getattr(result, "stderr", "")
    if not isinstance(stdout, str) or not isinstance(stderr, str):
        return False
    return (stdout, stderr) in {
        (
            "",
            f"Error response from daemon: No such object: {container_ref}\n",
        ),
        ("[]\n", f"error: no such object: {container_ref}\n"),
    }


def _inspect_probe_container_identity(
    container_ref: str,
) -> _ProbeContainerIdentity | None:
    _validate_probe_container_ref(container_ref)
    inspected = _docker_run(("inspect", container_ref))
    if inspected.returncode != 0:
        if _docker_inspect_is_not_found(inspected, container_ref):
            return None
        raise FormalSmokeError("formal smoke container inspect failed")
    try:
        document = json.loads(inspected.stdout)
    except (TypeError, ValueError) as exc:
        raise FormalSmokeError("formal smoke container inspect failed") from exc
    if not isinstance(document, list) or len(document) != 1:
        raise FormalSmokeError("formal smoke container inspect failed")
    details = document[0]
    if not isinstance(details, Mapping):
        raise FormalSmokeError("formal smoke container inspect failed")
    container_id = details.get("Id")
    if not isinstance(container_id, str) or not re.fullmatch(
        r"[0-9a-f]{64}", container_id
    ):
        raise FormalSmokeError("formal smoke container inspect identity is invalid")
    config = details.get("Config")
    if not isinstance(config, Mapping):
        raise FormalSmokeError("formal smoke container inspect failed")
    labels = config.get("Labels")
    if labels is None:
        normalized_labels: dict[str, str] = {}
    elif not isinstance(labels, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in labels.items()
    ):
        raise FormalSmokeError("formal smoke container inspect failed")
    else:
        normalized_labels = dict(labels)
    return _ProbeContainerIdentity(container_id=container_id, labels=normalized_labels)


def _remove_probe_container(container_ref: str, *, owner_label: str) -> None:
    _validate_probe_container_ref(container_ref)
    if not isinstance(owner_label, str) or not re.fullmatch(
        r"[0-9a-f]{24}", owner_label
    ):
        raise FormalSmokeError("formal smoke container cleanup identity is invalid")
    identity = _inspect_probe_container_identity(container_ref)
    if identity is None:
        return
    if identity.labels.get(_SMOKE_PROBE_OWNER_LABEL) != owner_label:
        raise FormalSmokeError("formal smoke container cleanup ownership mismatch")
    removed = _docker_run(("rm", "--force", identity.container_id))
    if removed.returncode != 0:
        raise FormalSmokeError("formal smoke container cleanup failed")
    if _inspect_probe_container_identity(identity.container_id) is not None:
        raise FormalSmokeError("formal smoke container cleanup failed")


def _probe_forward_path_denial(
    *, backend_ipv4_by_role: Mapping[str, Any], image_id: str
) -> bool:
    addresses = _normalize_probe_addresses(backend_ipv4_by_role)
    if not isinstance(image_id, str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", image_id
    ):
        raise FormalSmokeError("formal smoke probe image identity is invalid")
    owner_label = secrets.token_hex(12)
    name = "txnmem-smoke-" + owner_label
    script = (
        'while [[ "$#" -gt 0 ]]; do '
        'probe_error=$({ exec 3<>"/dev/tcp/$1/$2"; } 2>&1); '
        'probe_status=$?; '
        'if [[ "$probe_status" -eq 0 ]]; then exec 3>&-; exit 91; fi; '
        'case "$probe_error" in *"Connection refused"*) ;; *) exit 92 ;; esac; '
        'shift 2; '
        'done; exit 0'
    )
    cleanup_ref = name
    create_started = False
    container_id: str | None = None
    try:
        create_started = True
        create = _docker_run(
            (
                "create",
                "--pull=never",
                "--name",
                name,
                "--network",
                "bridge",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt",
                "no-new-privileges",
                "--label",
                f"{_SMOKE_PROBE_OWNER_LABEL}={owner_label}",
                "--entrypoint",
                "/bin/bash",
                image_id,
                "--noprofile",
                "--norc",
                "-c",
                script,
                "txnmem-forward-probe",
                addresses["qdrant"],
                "6333",
                addresses["neo4j"],
                "7687",
            )
        )
        if create.returncode == 0:
            observed_id = create.stdout.strip()
            if re.fullmatch(r"[0-9a-f]{64}", observed_id):
                container_id = observed_id
                cleanup_ref = container_id
        if container_id is None:
            raise FormalSmokeError("formal smoke container probe could not start")
        started = _docker_run(("start", "--attach", container_id))
        if started.returncode == 0:
            return True
        if started.returncode == 91:
            return False
        raise FormalSmokeError("formal smoke unexpected container probe failure")
    finally:
        if create_started:
            try:
                _remove_probe_container(cleanup_ref, owner_label=owner_label)
            except FormalSmokeError:
                raise
            except BaseException as exc:
                raise FormalSmokeError(
                    "formal smoke container cleanup failed"
                ) from exc


def _verify_smoke_child_quiescence(child: Any) -> None:
    process = getattr(child, "process", None)
    if process is None or type(getattr(process, "pid", None)) is not int:
        raise FormalSmokeError("formal smoke child identity is unavailable")
    pid = int(process.pid)
    if pid <= 0:
        raise FormalSmokeError("formal smoke child identity is unavailable")
    try:
        running = process.poll() is None
    except BaseException as exc:
        raise FormalSmokeError(
            "formal smoke child process state is unavailable"
        ) from exc
    if running:
        raise FormalSmokeError("formal smoke child process is still running")
    if hasattr(os, "killpg"):
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            pass
        except OSError as exc:
            raise FormalSmokeError(
                "formal smoke child process group quiescence is unproven"
            ) from exc
        else:
            raise FormalSmokeError(
                "formal smoke child process group survived cleanup"
            )
    try:
        _require_formal_uid_processes(FORMAL_RUNNER_UID, expected={})
    except BaseException as exc:
        raise FormalSmokeError(
            "formal smoke runner UID quiescence is unproven"
        ) from exc


def validate_smoke_child_receipt(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {"schema", "qdrant_proxy_ok", "neo4j_proxy_ok"}
        or value.get("schema") != _CHILD_RECEIPT_SCHEMA
        or value.get("qdrant_proxy_ok") is not True
        or value.get("neo4j_proxy_ok") is not True
    ):
        raise FormalSmokeError("formal smoke child receipt is invalid")
    return {
        "schema": _CHILD_RECEIPT_SCHEMA,
        "qdrant_proxy_ok": True,
        "neo4j_proxy_ok": True,
    }


def _validate_smoke_v2_child_receipt(
    value: Any,
    *,
    scenario: str,
) -> dict[str, Any]:
    expected = {
        "normal_prefix": ("succeeded", 2, True, True),
        "first_ineligible": ("formal_ineligible", 0, False, False),
        "backend_timeout": ("backend_timeout", 0, False, False),
    }.get(scenario)
    if (
        expected is None
        or type(value) is not dict
        or set(value)
        != {
            "schema",
            "scenario",
            "outcome",
            "completed_repetitions",
            "qdrant_proxy_ok",
            "neo4j_proxy_ok",
        }
        or value.get("schema") != _CHILD_RECEIPT_V2_SCHEMA
        or value.get("scenario") != scenario
        or (
            value.get("outcome"),
            value.get("completed_repetitions"),
            value.get("qdrant_proxy_ok"),
            value.get("neo4j_proxy_ok"),
        )
        != expected
    ):
        raise FormalSmokeError("formal smoke v2 child receipt is invalid")
    return dict(value)


def _validate_smoke_v2_outcomes(value: Any) -> dict[str, bool]:
    if type(value) not in {list, tuple} or len(value) != len(_SMOKE_V2_SCENARIOS):
        raise FormalSmokeError("formal smoke v2 outcome closure is invalid")
    expected_fields = {
        "scenario",
        "identity_sha256",
        "progress",
        "receipt",
        "elapsed_milliseconds",
        "child_quiescent",
        "guard_removed",
        "candidate_artifact_count",
    }
    rows: dict[str, dict[str, Any]] = {}
    identities: set[str] = set()
    config_sha256: str | None = None
    for expected_scenario, raw in zip(_SMOKE_V2_SCENARIOS, value):
        if (
            type(raw) is not dict
            or set(raw) != expected_fields
            or raw.get("scenario") != expected_scenario
            or not isinstance(raw.get("identity_sha256"), str)
            or _SHA256.fullmatch(raw["identity_sha256"]) is None
            or raw["identity_sha256"] in identities
            or type(raw.get("elapsed_milliseconds")) is not int
            or raw["elapsed_milliseconds"] < 0
            or raw.get("child_quiescent") is not True
            or raw.get("guard_removed") is not True
            or type(raw.get("candidate_artifact_count")) is not int
            or raw["candidate_artifact_count"] != 0
        ):
            raise FormalSmokeError("formal smoke v2 outcome closure is invalid")
        identities.add(raw["identity_sha256"])
        progress_view = raw.get("progress")
        try:
            canonical_snapshot_line(progress_view)
        except BaseException:
            raise FormalSmokeError("formal smoke v2 progress closure is invalid") from None
        if progress_view.get("run_binding_sha256") != raw["identity_sha256"]:
            raise FormalSmokeError("formal smoke v2 progress binding is invalid")
        if config_sha256 is None:
            config_sha256 = progress_view.get("config_sha256")
        elif progress_view.get("config_sha256") != config_sha256:
            raise FormalSmokeError("formal smoke v2 progress config changed")
        rows[expected_scenario] = dict(raw)

    normal = rows["normal_prefix"]
    normal_progress = normal["progress"]
    _validate_smoke_v2_child_receipt(
        normal["receipt"], scenario="normal_prefix"
    )
    progress_monotonic = (
        normal_progress.get("status") == "running"
        and normal_progress.get("cell_index") == 1
        and normal_progress.get("graph_size") == 100
        and normal_progress.get("concurrency") == 1
        and normal_progress.get("repetition_index") == 2
        and normal_progress.get("completed_repetitions") == 2
        and normal_progress.get("completed_samples") == 64
        and normal_progress.get("update_sequence") == 2
        and "terminal_reason_class" not in normal_progress
    )

    ineligible = rows["first_ineligible"]
    ineligible_progress = ineligible["progress"]
    _validate_smoke_v2_child_receipt(
        ineligible["receipt"], scenario="first_ineligible"
    )
    formal_fail_fast = (
        ineligible_progress.get("status") == "blocked"
        and ineligible_progress.get("terminal_reason_class")
        == "formal_eligibility_failed"
        and ineligible_progress.get("repetition_index") == 0
        and ineligible_progress.get("completed_repetitions") == 0
        and ineligible_progress.get("completed_samples") == 0
        and ineligible_progress.get("update_sequence") == 0
    )

    timeout = rows["backend_timeout"]
    timeout_progress = timeout["progress"]
    _validate_smoke_v2_child_receipt(
        timeout["receipt"], scenario="backend_timeout"
    )
    backend_timeout_bounded = (
        timeout_progress.get("status") == "blocked"
        and timeout_progress.get("terminal_reason_class") == "backend_timeout"
        and timeout_progress.get("repetition_index") == 0
        and timeout_progress.get("completed_repetitions") == 0
        and timeout_progress.get("completed_samples") == 0
        and timeout_progress.get("update_sequence") == 0
        and 0 < timeout["elapsed_milliseconds"] <= 6000
    )

    interruption = rows["interruption"]
    interruption_progress = interruption["progress"]
    interruption_cleanup = (
        interruption.get("receipt") is None
        and interruption_progress.get("status") == "interrupted"
        and interruption_progress.get("terminal_reason_class")
        == "collector_interrupted"
        and interruption_progress.get("repetition_index") == 0
        and interruption_progress.get("completed_repetitions") == 0
        and interruption_progress.get("completed_samples") == 0
        and interruption_progress.get("update_sequence") == 0
    )
    proofs = {
        "progress_monotonic": progress_monotonic,
        "formal_fail_fast": formal_fail_fast,
        "backend_timeout_bounded": backend_timeout_bounded,
        "interruption_cleanup": interruption_cleanup,
        "candidate_unpublished": all(
            row["candidate_artifact_count"] == 0 for row in rows.values()
        ),
    }
    if any(value is not True for value in proofs.values()):
        raise FormalSmokeError("formal smoke v2 proof closure is invalid")
    return proofs


def validate_formal_smoke_report(value: Any) -> dict[str, Any]:
    if (
        type(value) is not dict
        or set(value) != _REPORT_FIELDS
        or value.get("schema") != _REPORT_SCHEMA
        or not isinstance(value.get("source_commit"), str)
        or not _COMMIT.fullmatch(value["source_commit"])
        or value.get("toxiproxy_version") != _TOXIPROXY_VERSION
        or type(value.get("proxy_metrics_series_count")) is not int
        or value.get("proxy_metrics_series_count") != 8
        or any(value.get(field) is not True for field in _TRUE_REPORT_FIELDS)
        or value.get("candidate_created") is not False
    ):
        raise FormalSmokeError("formal smoke report schema is invalid")
    for field in (
        "source_manifest_sha256",
        "backend_isolation_sha256",
        "network_guard_sha256",
        "toxiproxy_image_manifest_digest",
        "report_sha256",
    ):
        if not isinstance(value.get(field), str) or not _SHA256.fullmatch(
            value[field]
        ):
            raise FormalSmokeError("formal smoke report digest is invalid")
    unhashed = dict(value)
    report_hash = unhashed.pop("report_sha256")
    try:
        expected = hashlib.sha256(canonical_json_bytes(unhashed)).hexdigest()
    except ValueError as exc:
        raise FormalSmokeError("formal smoke report is not canonical") from exc
    if report_hash != expected:
        raise FormalSmokeError("formal smoke report hash mismatch")
    return dict(value)


def _write_smoke_report(
    store: FormalStore, name: str, report: Mapping[str, Any]
) -> None:
    validated = validate_formal_smoke_report(dict(report))
    store.write_json_exclusive(name, payload=validated, mode=0o600)


def _raise_primary(primary: BaseException) -> None:
    raise primary.with_traceback(primary.__traceback__)


def collect_formal_smoke(
    *,
    project_root: Path,
    out_path: Path,
    qdrant_url: str,
    neo4j_uri: str,
    toxiproxy_url: str,
    neo4j_password: str,
    _controller_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the protected ingress/attribution gate and publish no candidate."""

    _require_root_linux()
    if not isinstance(neo4j_password, str) or not neo4j_password:
        raise FormalSmokeError("Neo4j runtime credential is unavailable")
    root = project_root.expanduser().absolute().resolve(strict=True)
    _validate_smoke_endpoints(qdrant_url, neo4j_uri, toxiproxy_url)
    report_store, report_name = _prepare_smoke_output(root, out_path)
    controller_context = _validate_formal_controller_context(_controller_context)
    expected_commit = str(controller_context["source_commit"])
    expected_manifest = dict(controller_context["source_manifest"])

    workspace: _SmokeWorkspace | None = None
    primary: BaseException | None = None
    primary_traceback = None
    report_without_hash: dict[str, Any] | None = None
    scenario_execution_started = False
    scenario_execution_completed = False
    try:
        workspace = _create_smoke_workspace()
        source_identity = attest_committed_source(
            root,
            expected_commit=expected_commit,
            expected_source_manifest=expected_manifest,
        )
        source_export = create_immutable_source_export(
            root, workspace.path, source_identity
        )
        python_executable = Path(sys.executable).expanduser().absolute().resolve(
            strict=True
        )
        runtime_snapshot, runtime_manifest = _create_locked_runtime_snapshot(
            workspace.path,
            lock_path=source_export
            / "configs"
            / "provenance_runtime_lock.json",
            wheel_directory=_FORMAL_RUNTIME_WHEEL_DIRECTORY,
            python_executable_hash=_file_sha256(
                python_executable, "formal smoke Python executable"
            ),
            require_protected_wheels=True,
        )
        topology = _observe_smoke_topology()
        routes_a = prepare_isolated_toxiproxy_routes(
            toxiproxy_url,
            qdrant_proxy=_FORMAL_QDRANT_PROXY,
            neo4j_proxy=_FORMAL_NEO4J_PROXY,
        )
        toxiproxy_version = _probe_root_health(
            qdrant_url=qdrant_url,
            neo4j_uri=neo4j_uri,
            toxiproxy_url=toxiproxy_url,
            neo4j_password=neo4j_password,
            runtime_snapshot=runtime_snapshot,
        )
        if toxiproxy_version != _TOXIPROXY_VERSION:
            raise FormalSmokeError("formal smoke health gate failed")
        try:
            environment_document = _collect_formal_environment_attestation(
                toxiproxy_url=toxiproxy_url,
                storage_path=workspace.path,
            )
            environment_snapshot, environment_raw = (
                _write_collected_environment_snapshot(
                    workspace.path, environment_document
                )
            )
            canonical_environment = canonical_json_bytes(
                validate_environment_attestation(environment_document)
            )
        except (CollectorError, ProvenancePerformanceError, ValueError) as exc:
            raise FormalSmokeError(
                "formal smoke environment collection failed"
            ) from exc
        if environment_raw != canonical_environment + b"\n":
            raise FormalSmokeError("formal smoke environment snapshot drifted")
        environment_sha256 = hashlib.sha256(
            canonical_environment
        ).hexdigest()
        _publish_formal_input_tree(workspace.path)
        scenario_specs = _create_smoke_v2_scenario_specs(
            workspace.path,
            source_commit=str(source_identity["source_commit"]),
        )
        baseline_a = capture_toxiproxy_counter_snapshot(
            toxiproxy_url,
            phase="baseline_a",
            proxy_routes=routes_a,
        )
        scenario_execution_started = True
        execution = _run_smoke_v2_scenarios(
            scenario_specs=scenario_specs,
            source_export=source_export,
            runtime_snapshot=runtime_snapshot,
            runtime_manifest=runtime_manifest,
            runner_sha256=str(source_identity["runner_sha256"]),
            neo4j_password=neo4j_password,
            environment_attestation_path=environment_snapshot,
            environment_attestation_sha256=environment_sha256,
            topology=topology,
            qdrant_url=qdrant_url,
            neo4j_uri=neo4j_uri,
            toxiproxy_url=toxiproxy_url,
            routes_a=routes_a,
            baseline_a=baseline_a,
        )
        scenario_execution_completed = True
        proofs = _validate_smoke_v2_outcomes(execution.outcomes)
        deltas = _derive_toxiproxy_attribution_deltas(
            execution.baseline_b, execution.final_counters
        )
        delta_by_role = {
            str(row["role"]): int(row["total_bytes"])
            for row in deltas["routes"]
        }
        series_count = len(proxy_counter_values(baseline_a))
        if series_count != 8:
            raise FormalSmokeError("formal smoke metrics series closure is invalid")
        report_without_hash = {
            "schema": _REPORT_SCHEMA,
            "source_commit": str(source_identity["source_commit"]),
            "source_manifest_sha256": str(
                source_identity["source_manifest_sha256"]
            ),
            "backend_isolation_sha256": hashlib.sha256(
                canonical_json_bytes(topology.sanitized_backend_isolation)
            ).hexdigest(),
            "network_guard_sha256": hashlib.sha256(
                canonical_json_bytes(execution.initial_guard)
            ).hexdigest(),
            "toxiproxy_image_manifest_digest": topology.toxiproxy_manifest_digest,
            "toxiproxy_version": toxiproxy_version,
            "proxy_metrics_series_count": series_count,
            "baseline_values_equal": True,
            "route_rearmed": True,
            "root_management_succeeded": True,
            "runner_qdrant_succeeded": execution.normal_receipt[
                "qdrant_proxy_ok"
            ],
            "runner_neo4j_succeeded": execution.normal_receipt[
                "neo4j_proxy_ok"
            ],
            "non_runner_proxy_blocked": True,
            "direct_backend_blocked": True,
            "forwarded_bridge_blocked": True,
            "qdrant_delta_positive": delta_by_role.get("qdrant", 0) > 0,
            "neo4j_delta_positive": delta_by_role.get("neo4j", 0) > 0,
            "guard_stable": True,
            "guard_removed": True,
            "candidate_created": False,
            **proofs,
        }
        if (
            report_without_hash["qdrant_delta_positive"] is not True
            or report_without_hash["neo4j_delta_positive"] is not True
        ):
            raise FormalSmokeError("formal smoke backend delta gate failed")
    except BaseException as exc:
        primary = exc
        primary_traceback = exc.__traceback__

    cleanup_failures: list[BaseException] = []
    if workspace is not None:
        if not scenario_execution_started or scenario_execution_completed:
            try:
                _remove_smoke_workspace(workspace)
            except BaseException as exc:
                cleanup_failures.append(exc)
        else:
            cleanup_failures.append(
                FormalSmokeError(
                    "formal smoke preserved workspace after scenario failure"
                )
            )
    if primary is not None:
        if cleanup_failures:
            try:
                primary.add_note(
                    "formal smoke cleanup also failed: "
                    + type(cleanup_failures[0]).__name__
                )
            except AttributeError:
                pass
        raise primary.with_traceback(primary_traceback)
    if cleanup_failures:
        raise FormalSmokeError("formal smoke cleanup failed") from cleanup_failures[0]
    if report_without_hash is None:
        raise FormalSmokeError("formal smoke report was not derived")
    report = dict(report_without_hash)
    report["report_sha256"] = hashlib.sha256(
        canonical_json_bytes(report_without_hash)
    ).hexdigest()
    validated = validate_formal_smoke_report(report)
    _write_smoke_report(report_store, report_name, validated)
    return validated


def main(
    argv: list[str] | None = None,
    *,
    _controller_context: Mapping[str, Any] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description="run the protected provenance ingress smoke gate",
        allow_abbrev=False,
    )
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, action="append", required=True)
    args = parser.parse_args(argv)
    try:
        if len(args.out) != 1:
            raise FormalSmokeError("formal smoke output path is ambiguous")
        password = os.environ.pop("TXNMEM_NEO4J_PASSWORD", None)
        if not password:
            raise FormalSmokeError("Neo4j runtime credential is unavailable")
        collect_formal_smoke(
            project_root=args.project_root,
            out_path=args.out[0],
            qdrant_url=_QDRANT_URL,
            neo4j_uri=_NEO4J_URI,
            toxiproxy_url=_TOXIPROXY_URL,
            neo4j_password=password,
            _controller_context=_controller_context,
        )
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        print(f"formal provenance smoke blocked: {type(exc).__name__}")
        return 2
    print("formal provenance smoke gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
