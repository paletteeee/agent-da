"""Sanitize and validate independently collected provenance execution evidence."""

from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import stat
from pathlib import Path
from typing import Any, Mapping

from txnmem_provenance_contract import (
    FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
    FORMAL_RUNNER_UID,
    is_registered_service_version,
)


RAW_LAUNCH_SCHEMA = "txnmem-provenance-execution-launch-raw-v3"
RAW_COMPLETION_SCHEMA = "txnmem-provenance-execution-completion-raw-v4"
SANITIZED_SCHEMA = "txnmem-topology-attestation-v5"
COLLECTOR_ID = "txnmem-provenance-execution-collector-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_CANDIDATE_ID = re.compile(
    r"^diagnostic-vector_graph-[0-9a-f]{16}-[0-9a-f]{16}$"
)
_TRANSPORTS = frozenset(
    {
        "local_loopback",
        "ssh_local_port_forward",
        "direct_private_network",
        "container_bridge",
    }
)
_ROLES = ("client", "qdrant", "neo4j", "toxiproxy")
_SHARED_FIELDS = frozenset(
    {
        "collector_id",
        "formal_execution_requested",
        "run_id_sha256",
        "config_sha256",
        "config_file_sha256",
        "workload_sha256",
        "environment_attestation_sha256",
        "source_commit",
        "source_manifest",
        "source_manifest_sha256",
        "collector_sha256",
        "runner_sha256",
        "command_manifest",
        "command_sha256",
        "child_process",
        "network_guard",
        "backend_isolation",
        "transport",
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
    }
)
_LAUNCH_FIELDS = _SHARED_FIELDS | {"schema", "roles", "proxy_routes"}
_LAUNCH_FIELDS = _LAUNCH_FIELDS | {
    "authorization_nonce_sha256",
    "authorization_proof_sha256",
}
_COMPLETION_FIELDS = _SHARED_FIELDS | {
    "schema",
    "launch_file_sha256",
    "exit_code",
    "candidate_bundle_id",
    "evidence_manifest_sha256",
    "candidate_operation_samples_sha256",
    "candidate_repetitions_sha256",
    "candidate_seal",
    "execution_monitor",
    "roles",
    "proxy_routes",
    "authorization_nonce_sha256",
    "authorization_proof_sha256",
}
_ROLE_FIELDS = frozenset(
    {
        "role",
        "host_identity",
        "listener_owner",
        "service_version",
        "rtt_ms",
        "proxy_counter_bytes",
    }
)
_SANITIZED_ROLE_FIELDS = frozenset(
    {
        "role",
        "host_identity_sha256",
        "listener_owner_before_sha256",
        "listener_owner_after_sha256",
        "service_version",
        "rtt_ms_before",
        "rtt_ms_after",
        "proxy_counter_bytes_before",
        "proxy_counter_bytes_after",
        "proxy_counter_bytes_delta",
    }
)
_FORMAL_ARGV_TEMPLATE = (
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
)
_COMMAND_MANIFEST_FIELDS = frozenset(
    {
        "schema",
        "transport",
        "argv_sha256",
        "argv_template",
        "python_executable_path_sha256",
        "python_executable_sha256",
        "python_implementation",
        "python_version",
        "runtime_manifest",
        "runtime_manifest_sha256",
        "runtime_lock_file_sha256",
        "runtime_snapshot_path_sha256",
        "external_tools",
        "working_directory_sha256",
        "source_manifest_sha256",
        "runner_sha256",
        "config_file_sha256",
        "run_id_sha256",
        "candidate_root_sha256",
        "environment_attestation_sha256",
        "environment_attestation_file_sha256",
        "qdrant_endpoint_sha256",
        "qdrant_endpoint_port",
        "neo4j_endpoint_sha256",
        "neo4j_endpoint_port",
        "toxiproxy_endpoint_sha256",
        "literal_environment",
        "hashed_environment",
        "secret_environment_variables",
        "gate_environment_variable",
        "ready_environment_variable",
        "completion_environment_variable",
        "completion_receipt_required",
        "runtime_environment_variable",
        "inherited_environment",
    }
)
_RAW_CHILD_PROCESS_FIELDS = frozenset(
    {
        "pid",
        "start_identity",
        "uid",
        "executable_sha256",
        "argv_sha256",
        "cmdline_sha256",
    }
)
_EXTERNAL_TOOL_FIELDS = frozenset(
    {
        "role",
        "requested_path_sha256",
        "resolved_path_sha256",
        "executable_sha256",
        "owner_uid",
        "mode",
    }
)
_SANITIZED_CHILD_PROCESS_FIELDS = frozenset(
    {
        "pid_sha256",
        "start_identity_sha256",
        "uid_sha256",
        "executable_sha256",
        "argv_sha256",
        "cmdline_sha256",
    }
)
_RAW_CANDIDATE_SEAL_FIELDS = frozenset(
    {
        "schema",
        "root_device",
        "root_inode",
        "directory_count",
        "file_count",
        "tree_sha256",
        "completion_receipt_sha256",
    }
)
_SANITIZED_CANDIDATE_SEAL_FIELDS = frozenset(
    {
        "root_device_sha256",
        "root_inode_sha256",
        "directory_count",
        "file_count",
        "tree_sha256",
        "completion_receipt_sha256",
    }
)
_NETWORK_GUARD_FIELDS = frozenset(
    {
        "schema",
        "table_name_sha256",
        "runner_uid",
        "controller_uid",
        "allowed_ipv4_loopback_ports",
        "management_port_root_only",
        "non_runner_proxy_traffic_blocked",
        "host_bridge_access_blocked",
        "forwarded_bridge_access_blocked",
        "backend_ipv4_subnet_sha256",
        "ingress_ipv4_subnet_sha256",
        "backend_bridge_interface_sha256",
        "ingress_bridge_interface_sha256",
        "policy_sha256",
        "ruleset_sha256",
    }
)
_BACKEND_ISOLATION_FIELDS = frozenset(
    {
        "schema",
        "network_name_sha256",
        "network_id_sha256",
        "ingress_network_name_sha256",
        "ingress_network_id_sha256",
        "toxiproxy_ingress_ipv4",
        "toxiproxy_ingress_ipv4_sha256",
        "toxiproxy_ingress_endpoint_id_sha256",
        "toxiproxy_ingress_membership_verified",
        "ingress_unique_workload_container_verified",
        "backend_network_internal",
        "ingress_network_external",
        "ingress_proxy_only",
        "backend_network_driver",
        "ingress_network_driver",
        "backend_network_scope",
        "ingress_network_scope",
        "network_driver_options_empty",
        "docker_default_ipam_driver_verified",
        "private_non_overlapping_ipv4_subnets_verified",
        "backend_ipv4_subnet_sha256",
        "ingress_ipv4_subnet_sha256",
        "backend_bridge_interface_sha256",
        "ingress_bridge_interface_sha256",
        "networks_non_attachable",
        "networks_non_swarm_ingress",
        "networks_non_config_only",
        "direct_backend_ports_unpublished",
        "proxy_ports_loopback_only",
        "published_proxy_ports",
        "containers",
    }
)
_SANITIZED_BACKEND_ISOLATION_FIELDS = _BACKEND_ISOLATION_FIELDS - frozenset(
    {"toxiproxy_ingress_ipv4"}
)
_BACKEND_CONTAINER_FIELDS = frozenset(
    {
        "role",
        "container_id_sha256",
        "runtime_image_id_sha256",
        "manifest_digest",
    }
)
_EXECUTION_MONITOR_FIELDS = frozenset(
    {
        "schema",
        "sampling_interval_ms",
        "sample_count",
        "first_sample_monotonic_ns",
        "last_sample_monotonic_ns",
        "gate_release_monotonic_ns",
        "child_exit_monotonic_ns",
        "max_observed_gap_ns",
        "violation_count",
        "cpu_logical_count",
        "load1_limit_milli",
        "max_load1_milli",
        "invariants",
        "samples_sha256",
        "first_sample_sha256",
        "last_sample_sha256",
    }
)
_SANITIZED_EXECUTION_MONITOR_FIELDS = frozenset(
    {
        "schema",
        "sampling_interval_ms",
        "sample_count",
        "duration_ns",
        "pre_release_coverage_ns",
        "execution_coverage_ns",
        "terminal_coverage_gap_ns",
        "max_observed_gap_ns",
        "violation_count",
        "cpu_logical_count",
        "load1_limit_milli",
        "max_load1_milli",
        "invariants",
        "samples_sha256",
        "first_sample_sha256",
        "last_sample_sha256",
    }
)
_EXECUTION_MONITOR_INVARIANTS = [
    "backend_isolation",
    "continuous_load_ceiling",
    "host_environment",
    "network_guard",
    "runner_uid_process_set",
    "terminal_process_exit",
    "toxiproxy_routes",
]
_RAW_PROXY_ROUTE_FIELDS = frozenset(
    {"role", "proxy_name", "listen", "upstream", "enabled", "toxics_count"}
)
_SANITIZED_PROXY_ROUTE_FIELDS = frozenset(
    {
        "role",
        "proxy_name",
        "listen_port",
        "upstream_service",
        "upstream_port",
        "enabled",
        "toxics_count",
    }
)
_RUNTIME_FIELDS = frozenset({"schema", "python", "distributions"})
_RUNTIME_PYTHON_FIELDS = frozenset(
    {
        "implementation",
        "version",
        "executable_sha256",
        "build_sha256",
        "compiler_sha256",
        "platform_sha256",
    }
)
_RUNTIME_DISTRIBUTION_FIELDS = frozenset(
    {
        "name",
        "version",
        "files",
        "files_sha256",
        "declared_requirements_sha256",
    }
)
_RUNTIME_FILE_FIELDS = frozenset({"path", "sha256"})
_SANITIZED_FIELDS = frozenset(
    {
        "schema",
        "collector_id",
        "formal_execution_requested",
        "run_id_sha256",
        "config_sha256",
        "config_file_sha256",
        "workload_sha256",
        "environment_attestation_sha256",
        "evidence_manifest_sha256",
        "source_commit",
        "source_manifest",
        "source_manifest_sha256",
        "collector_sha256",
        "runner_sha256",
        "command_manifest",
        "command_sha256",
        "child_process",
        "network_guard",
        "backend_isolation",
        "proxy_routes",
        "authorization_nonce_sha256",
        "launch_authorization_proof_sha256",
        "completion_authorization_proof_sha256",
        "source_continuity_verified",
        "launch_file_sha256",
        "completion_file_sha256",
        "launch_identity_sha256",
        "completion_identity_sha256",
        "candidate_bundle_id",
        "candidate_operation_samples_sha256",
        "candidate_repetitions_sha256",
        "candidate_seal",
        "execution_monitor",
        "exit_code",
        "transport",
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
        "toxiproxy_route_observed",
        "listener_continuity_verified",
        "host_continuity_verified",
        "host_count",
        "roles",
        "attestation_sha256",
    }
)


# Intentionally empty until the exact sanitized completion digest from a
# source-reviewed collector run is independently inspected and registered.
FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN: dict[str, str] = {}
# Pre-run authorization registry.  Values are SHA-256 digests of random
# out-of-tree nonces generated and retained by an independent controller.  The
# nonce itself is never passed to the benchmark child or committed.
FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN: dict[str, str] = {
    # txnmem-provenance-formal-20260822-v1
    "68632b903c7feff62c1996b0b36d238eadaee0bfcdf00b6e60c65ff4d021e5ee":
        "520e62eb73f9293fe539c2a227e0297eed67969453c0d4213656e8319b3f40cd",
}


class TopologyAttestationError(ValueError):
    """Execution/topology evidence is malformed, untrusted, or inconsistent."""


def _read_private_authorization_nonce(
    path: Path, *, repository_root: Path
) -> bytes:
    """Read a controller nonce through a private out-of-tree directory."""

    candidate = path.expanduser().absolute()
    repository = repository_root.expanduser().resolve(strict=True)
    try:
        parent = candidate.parent.resolve(strict=True)
    except OSError as exc:
        raise TopologyAttestationError(
            "authorization nonce parent directory is unavailable"
        ) from exc
    resolved_candidate = parent / candidate.name
    if resolved_candidate == repository or repository in resolved_candidate.parents:
        raise TopologyAttestationError(
            "authorization nonce must be outside the repository"
        )

    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    parent_fd: int | None = None
    nonce_fd: int | None = None
    try:
        parent_fd = os.open(parent, directory_flags)
        parent_stat = os.fstat(parent_fd)
        if not stat.S_ISDIR(parent_stat.st_mode):
            raise TopologyAttestationError(
                "authorization nonce parent must be a directory"
            )
        if stat.S_IMODE(parent_stat.st_mode) != 0o700:
            raise TopologyAttestationError(
                "authorization nonce parent directory mode 0700 is required"
            )
        if hasattr(os, "getuid") and parent_stat.st_uid != os.getuid():
            raise TopologyAttestationError(
                "authorization nonce parent must be owned by the current user"
            )

        nonce_fd = os.open(candidate.name, file_flags, dir_fd=parent_fd)
        nonce_stat = os.fstat(nonce_fd)
        if not stat.S_ISREG(nonce_stat.st_mode):
            raise TopologyAttestationError(
                "authorization nonce must be a regular file"
            )
        if stat.S_IMODE(nonce_stat.st_mode) != 0o600:
            raise TopologyAttestationError(
                "authorization nonce file mode 0600 is required"
            )
        if hasattr(os, "getuid") and nonce_stat.st_uid != os.getuid():
            raise TopologyAttestationError(
                "authorization nonce must be owned by the current user"
            )
        with os.fdopen(nonce_fd, "rb", closefd=True) as stream:
            nonce_fd = None
            nonce = stream.read(4097)
        if not 32 <= len(nonce) <= 4096:
            raise TopologyAttestationError(
                "authorization nonce must contain between 32 and 4096 bytes"
            )
        return nonce
    except TopologyAttestationError:
        raise
    except OSError as exc:
        raise TopologyAttestationError(
            "authorization nonce must be a regular file"
        ) from exc
    finally:
        if nonce_fd is not None:
            os.close(nonce_fd)
        if parent_fd is not None:
            os.close(parent_fd)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TopologyAttestationError("attestation is not finite canonical JSON") from exc


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def execution_authorization_proof(
    nonce: bytes, document: Mapping[str, Any]
) -> str:
    """Authenticate one launch/completion document without persisting nonce."""

    if not isinstance(nonce, bytes) or len(nonce) < 32:
        raise TopologyAttestationError("authorization nonce must contain 32 bytes")
    material = dict(document)
    material.pop("authorization_proof_sha256", None)
    return hmac.new(nonce, _canonical_bytes(material), hashlib.sha256).hexdigest()


def _exact_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise TopologyAttestationError(f"{field} must be a SHA-256 digest")
    return value


def _exact_positive_int(value: Any, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise TopologyAttestationError(f"{field} must be a positive integer")
    return value


def _exact_nonnegative_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise TopologyAttestationError(f"{field} must be a non-negative integer")
    return value


def _private_identity(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise TopologyAttestationError(f"{field} is invalid")
    return value


def _safe_rtt(value: Any) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0.0
    ):
        raise TopologyAttestationError("rtt_ms must be finite and non-negative")
    return value


def _validate_source_manifest(value: Any, source_commit: str) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema", "source_commit", "files"}
        or value.get("schema") != "txnmem-provenance-source-manifest-v1"
        or value.get("source_commit") != source_commit
        or not isinstance(value.get("files"), list)
        or not value["files"]
    ):
        raise TopologyAttestationError("source manifest fields are invalid")
    seen: set[str] = set()
    files: list[dict[str, str]] = []
    for row in value["files"]:
        if not isinstance(row, Mapping) or set(row) != {"path", "blob_sha256"}:
            raise TopologyAttestationError("source manifest file row is invalid")
        path = row.get("path")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or ".." in Path(path).parts
            or path in seen
        ):
            raise TopologyAttestationError("source manifest path is unsafe")
        seen.add(path)
        files.append(
            {
                "path": path,
                "blob_sha256": _exact_hash(row.get("blob_sha256"), "source blob"),
            }
        )
    if [row["path"] for row in files] != sorted(seen):
        raise TopologyAttestationError("source manifest paths are not canonical")
    return {
        "schema": "txnmem-provenance-source-manifest-v1",
        "source_commit": source_commit,
        "files": files,
    }


def _validate_command_manifest(
    value: Any, document: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _COMMAND_MANIFEST_FIELDS:
        raise TopologyAttestationError("command manifest fields do not match schema")
    if value.get("schema") != "txnmem-provenance-command-manifest-v2":
        raise TopologyAttestationError("command manifest schema mismatch")
    if value.get("transport") not in _TRANSPORTS:
        raise TopologyAttestationError("command transport is unsupported")
    for field in (
        "argv_sha256",
        "python_executable_path_sha256",
        "python_executable_sha256",
        "working_directory_sha256",
        "source_manifest_sha256",
        "runner_sha256",
        "config_file_sha256",
        "run_id_sha256",
        "candidate_root_sha256",
        "environment_attestation_sha256",
        "environment_attestation_file_sha256",
        "runtime_lock_file_sha256",
        "qdrant_endpoint_sha256",
        "neo4j_endpoint_sha256",
        "toxiproxy_endpoint_sha256",
    ):
        _exact_hash(value.get(field), f"command {field}")
    qdrant_port = _exact_positive_int(
        value.get("qdrant_endpoint_port"), "Qdrant endpoint port"
    )
    neo4j_port = _exact_positive_int(
        value.get("neo4j_endpoint_port"), "Neo4j endpoint port"
    )
    if qdrant_port != 19000 or neo4j_port != 19001:
        raise TopologyAttestationError("formal service endpoint bypasses Toxiproxy")
    if value.get("argv_template") != list(_FORMAL_ARGV_TEMPLATE):
        raise TopologyAttestationError("formal command template mismatch")
    if value.get("python_implementation") != "CPython" or not is_registered_service_version(
        "client", value.get("python_version")
    ):
        raise TopologyAttestationError("command Python runtime is not registered")
    runtime_manifest = _validate_runtime_manifest(value.get("runtime_manifest"))
    if hashlib.sha256(_canonical_bytes(runtime_manifest)).hexdigest() != _exact_hash(
        value.get("runtime_manifest_sha256"), "runtime manifest"
    ):
        raise TopologyAttestationError("runtime manifest digest mismatch")
    runtime_snapshot_path_hash = _exact_hash(
        value.get("runtime_snapshot_path_sha256"), "runtime snapshot path"
    )
    runtime_python = runtime_manifest["python"]
    if (
        runtime_python["implementation"] != value.get("python_implementation")
        or runtime_python["version"] != value.get("python_version")
        or runtime_python["executable_sha256"]
        != value.get("python_executable_sha256")
    ):
        raise TopologyAttestationError("command Python runtime manifest mismatch")
    external_tools = _validate_external_tools(value.get("external_tools"))
    python_tool = next(row for row in external_tools if row["role"] == "python")
    if python_tool["executable_sha256"] != value.get("python_executable_sha256"):
        raise TopologyAttestationError("command Python executable attestation mismatch")
    if value.get("literal_environment") != {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
    }:
        raise TopologyAttestationError("command literal environment mismatch")
    hashed_environment = value.get("hashed_environment")
    if not isinstance(hashed_environment, Mapping) or set(hashed_environment) != {
        "TXNMEM_NEO4J_URI",
        "TXNMEM_NEO4J_USER",
        "TXNMEM_PROVENANCE_RUNTIME_SITE",
    }:
        raise TopologyAttestationError("command hashed environment mismatch")
    for name, digest in hashed_environment.items():
        _exact_hash(digest, f"command environment {name}")
    if value.get("secret_environment_variables") != ["TXNMEM_NEO4J_PASSWORD"]:
        raise TopologyAttestationError("command secret environment is not closed")
    if (
        hashed_environment["TXNMEM_PROVENANCE_RUNTIME_SITE"]
        != runtime_snapshot_path_hash
    ):
        raise TopologyAttestationError("runtime snapshot environment mismatch")
    if (
        value.get("gate_environment_variable")
        != "TXNMEM_PROVENANCE_START_GATE_FD"
        or value.get("ready_environment_variable")
        != "TXNMEM_PROVENANCE_READY_FD"
        or value.get("completion_environment_variable")
        != "TXNMEM_PROVENANCE_COMPLETION_FD"
        or value.get("completion_receipt_required") is not True
        or value.get("runtime_environment_variable")
        != "TXNMEM_PROVENANCE_RUNTIME_SITE"
        or value.get("inherited_environment") is not False
    ):
        raise TopologyAttestationError("command process environment is not isolated")
    for field in (
        "source_manifest_sha256",
        "runner_sha256",
        "config_file_sha256",
        "run_id_sha256",
        "environment_attestation_sha256",
    ):
        if value.get(field) != document.get(field):
            raise TopologyAttestationError(f"command {field} is not execution-bound")
    if value.get("transport") != document.get("transport"):
        raise TopologyAttestationError("command transport is not execution-bound")
    normalized = copy.deepcopy(dict(value))
    normalized["runtime_manifest"] = runtime_manifest
    normalized["external_tools"] = external_tools
    return normalized


def _validate_external_tools(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != 4:
        raise TopologyAttestationError("formal external tool closure is incomplete")
    normalized: list[dict[str, Any]] = []
    for expected_role, row in zip(("docker", "git", "nft", "python"), value):
        if (
            not isinstance(row, Mapping)
            or set(row) != _EXTERNAL_TOOL_FIELDS
            or row.get("role") != expected_role
        ):
            raise TopologyAttestationError("formal external tool identity is invalid")
        owner_uid = _exact_nonnegative_int(
            row.get("owner_uid"), "formal external tool owner"
        )
        mode = _exact_positive_int(row.get("mode"), "formal external tool mode")
        if owner_uid != 0 or mode & 0o022 or not mode & 0o111 or mode > 0o7777:
            raise TopologyAttestationError("formal external tool is not root protected")
        normalized.append(
            {
                "role": expected_role,
                "requested_path_sha256": _exact_hash(
                    row.get("requested_path_sha256"),
                    "formal external tool requested path",
                ),
                "resolved_path_sha256": _exact_hash(
                    row.get("resolved_path_sha256"),
                    "formal external tool resolved path",
                ),
                "executable_sha256": _exact_hash(
                    row.get("executable_sha256"),
                    "formal external tool executable",
                ),
                "owner_uid": owner_uid,
                "mode": mode,
            }
        )
    return normalized


def _validate_runtime_manifest(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _RUNTIME_FIELDS
        or value.get("schema") != "txnmem-provenance-runtime-manifest-v1"
    ):
        raise TopologyAttestationError("runtime manifest fields do not match schema")
    python = value.get("python")
    if not isinstance(python, Mapping) or set(python) != _RUNTIME_PYTHON_FIELDS:
        raise TopologyAttestationError("runtime Python fields do not match schema")
    if python.get("implementation") != "CPython" or not is_registered_service_version(
        "client", python.get("version")
    ):
        raise TopologyAttestationError("runtime Python identity is not registered")
    for field in (
        "executable_sha256",
        "build_sha256",
        "compiler_sha256",
        "platform_sha256",
    ):
        _exact_hash(python.get(field), f"runtime Python {field}")
    distributions = value.get("distributions")
    if not isinstance(distributions, list) or not distributions:
        raise TopologyAttestationError("runtime distribution closure is empty")
    normalized_distributions: list[dict[str, Any]] = []
    observed_names: list[str] = []
    for distribution in distributions:
        if (
            not isinstance(distribution, Mapping)
            or set(distribution) != _RUNTIME_DISTRIBUTION_FIELDS
        ):
            raise TopologyAttestationError("runtime distribution fields mismatch")
        name = distribution.get("name")
        version = distribution.get("version")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,127}", name)
            or not isinstance(version, str)
            or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+(?:[A-Za-z0-9._+-]*)?", version)
        ):
            raise TopologyAttestationError("runtime distribution identity is unsafe")
        files = distribution.get("files")
        if not isinstance(files, list) or not files:
            raise TopologyAttestationError("runtime distribution files are empty")
        normalized_files: list[dict[str, str]] = []
        observed_paths: list[str] = []
        for row in files:
            if not isinstance(row, Mapping) or set(row) != _RUNTIME_FILE_FIELDS:
                raise TopologyAttestationError("runtime file fields mismatch")
            path = row.get("path")
            if (
                not isinstance(path, str)
                or not path
                or path.startswith("/")
                or ".." in Path(path).parts
            ):
                raise TopologyAttestationError("runtime file path is unsafe")
            observed_paths.append(path)
            normalized_files.append(
                {"path": path, "sha256": _exact_hash(row.get("sha256"), "runtime file")}
            )
        if observed_paths != sorted(set(observed_paths)):
            raise TopologyAttestationError("runtime file paths are not canonical")
        if hashlib.sha256(_canonical_bytes(normalized_files)).hexdigest() != distribution.get(
            "files_sha256"
        ):
            raise TopologyAttestationError("runtime distribution file hash mismatch")
        requirements_hash = _exact_hash(
            distribution.get("declared_requirements_sha256"),
            "runtime declared requirements",
        )
        observed_names.append(name)
        normalized_distributions.append(
            {
                "name": name,
                "version": version,
                "files": normalized_files,
                "files_sha256": distribution["files_sha256"],
                "declared_requirements_sha256": requirements_hash,
            }
        )
    if observed_names != sorted(set(observed_names)) or "neo4j" not in observed_names:
        raise TopologyAttestationError("runtime dependency closure is not canonical")
    return {
        "schema": "txnmem-provenance-runtime-manifest-v1",
        "python": copy.deepcopy(dict(python)),
        "distributions": normalized_distributions,
    }


def _validate_proxy_routes(
    value: Any, command_manifest: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    expected = (
        ("qdrant", "txnmem-qdrant", "0.0.0.0:19000", "qdrant:6333", 19000, 6333),
        ("neo4j", "txnmem-neo4j", "0.0.0.0:19001", "neo4j:7687", 19001, 7687),
    )
    if not isinstance(value, list) or len(value) != len(expected):
        raise TopologyAttestationError("formal proxy route inventory is incomplete")
    raw_routes: list[dict[str, Any]] = []
    safe_routes: list[dict[str, Any]] = []
    for row, (
        role,
        name,
        listen,
        upstream,
        listen_port,
        upstream_port,
    ) in zip(value, expected):
        if not isinstance(row, Mapping) or set(row) != _RAW_PROXY_ROUTE_FIELDS:
            raise TopologyAttestationError("proxy route fields do not match schema")
        if (
            row.get("role") != role
            or row.get("proxy_name") != name
            or row.get("listen") != listen
            or row.get("upstream") != upstream
            or row.get("enabled") is not True
            or type(row.get("toxics_count")) is not int
            or row.get("toxics_count") != 0
            or command_manifest.get(f"{role}_endpoint_port") != listen_port
        ):
            raise TopologyAttestationError("formal proxy route configuration mismatch")
        raw_routes.append(copy.deepcopy(dict(row)))
        safe_routes.append(
            {
                "role": role,
                "proxy_name": name,
                "listen_port": listen_port,
                "upstream_service": role,
                "upstream_port": upstream_port,
                "enabled": True,
                "toxics_count": 0,
            }
        )
    return raw_routes, safe_routes


def _validate_sanitized_proxy_routes(value: Any) -> None:
    expected = (
        ("qdrant", "txnmem-qdrant", 19000, "qdrant", 6333),
        ("neo4j", "txnmem-neo4j", 19001, "neo4j", 7687),
    )
    if not isinstance(value, list) or len(value) != len(expected):
        raise TopologyAttestationError("sanitized proxy routes are incomplete")
    for row, (role, name, listen_port, upstream_service, upstream_port) in zip(
        value, expected
    ):
        if (
            not isinstance(row, Mapping)
            or set(row) != _SANITIZED_PROXY_ROUTE_FIELDS
            or row.get("role") != role
            or row.get("proxy_name") != name
            or row.get("listen_port") != listen_port
            or row.get("upstream_service") != upstream_service
            or row.get("upstream_port") != upstream_port
            or row.get("enabled") is not True
            or row.get("toxics_count") != 0
        ):
            raise TopologyAttestationError("sanitized proxy route mismatch")


def _validate_child_process(
    value: Any, command_manifest: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, str]]:
    if not isinstance(value, Mapping) or set(value) != _RAW_CHILD_PROCESS_FIELDS:
        raise TopologyAttestationError("child process fields do not match schema")
    pid = _exact_positive_int(value.get("pid"), "child pid")
    start_identity = _private_identity(
        value.get("start_identity"), "child process identity"
    )
    uid = _exact_positive_int(value.get("uid"), "child uid")
    executable_hash = _exact_hash(
        value.get("executable_sha256"), "child executable"
    )
    argv_hash = _exact_hash(value.get("argv_sha256"), "child argv")
    cmdline_hash = _exact_hash(
        value.get("cmdline_sha256"), "child command line"
    )
    if (
        uid != FORMAL_RUNNER_UID
        or cmdline_hash == argv_hash
        or executable_hash != command_manifest.get("python_executable_sha256")
        or argv_hash != command_manifest.get("argv_sha256")
    ):
        raise TopologyAttestationError("child process is not command-bound")
    raw = {
        "pid": pid,
        "start_identity": start_identity,
        "uid": uid,
        "executable_sha256": executable_hash,
        "argv_sha256": argv_hash,
        "cmdline_sha256": cmdline_hash,
    }
    sanitized = {
        "pid_sha256": _hash_text(str(pid)),
        "start_identity_sha256": _hash_text(start_identity),
        "uid_sha256": _hash_text(str(uid)),
        "executable_sha256": executable_hash,
        "argv_sha256": argv_hash,
        "cmdline_sha256": cmdline_hash,
    }
    return raw, sanitized


def _validate_candidate_seal(
    value: Any,
    *,
    expected_completion_receipt_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != _RAW_CANDIDATE_SEAL_FIELDS:
        raise TopologyAttestationError("candidate seal fields do not match schema")
    if value.get("schema") != "txnmem-provenance-candidate-seal-v1":
        raise TopologyAttestationError("candidate seal schema mismatch")
    root_device = _exact_nonnegative_int(
        value.get("root_device"), "candidate root device"
    )
    root_inode = _exact_positive_int(
        value.get("root_inode"), "candidate root inode"
    )
    directory_count = _exact_positive_int(
        value.get("directory_count"), "candidate directory count"
    )
    file_count = _exact_positive_int(
        value.get("file_count"), "candidate file count"
    )
    tree_hash = _exact_hash(value.get("tree_sha256"), "candidate tree")
    receipt_hash = _exact_hash(
        value.get("completion_receipt_sha256"), "candidate completion receipt"
    )
    if receipt_hash != _exact_hash(
        expected_completion_receipt_sha256,
        "expected candidate completion receipt",
    ):
        raise TopologyAttestationError("candidate seal receipt is not completion-bound")
    raw = {
        "schema": "txnmem-provenance-candidate-seal-v1",
        "root_device": root_device,
        "root_inode": root_inode,
        "directory_count": directory_count,
        "file_count": file_count,
        "tree_sha256": tree_hash,
        "completion_receipt_sha256": receipt_hash,
    }
    sanitized = {
        "root_device_sha256": _hash_text(str(root_device)),
        "root_inode_sha256": _hash_text(str(root_inode)),
        "directory_count": directory_count,
        "file_count": file_count,
        "tree_sha256": tree_hash,
        "completion_receipt_sha256": receipt_hash,
    }
    return raw, sanitized


def _validate_network_guard_attestation(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _NETWORK_GUARD_FIELDS
        or value.get("schema") != "txnmem-provenance-network-guard-v2"
        or value.get("runner_uid") != FORMAL_RUNNER_UID
        or value.get("controller_uid") != 0
        or value.get("allowed_ipv4_loopback_ports") != [19000, 19001]
        or value.get("management_port_root_only") is not True
        or value.get("non_runner_proxy_traffic_blocked") is not True
        or value.get("host_bridge_access_blocked") is not True
        or value.get("forwarded_bridge_access_blocked") is not True
    ):
        raise TopologyAttestationError("formal network guard is invalid")
    return {
        "schema": "txnmem-provenance-network-guard-v2",
        "table_name_sha256": _exact_hash(
            value.get("table_name_sha256"), "network guard table"
        ),
        "runner_uid": FORMAL_RUNNER_UID,
        "controller_uid": 0,
        "allowed_ipv4_loopback_ports": [19000, 19001],
        "management_port_root_only": True,
        "non_runner_proxy_traffic_blocked": True,
        "host_bridge_access_blocked": True,
        "forwarded_bridge_access_blocked": True,
        "backend_ipv4_subnet_sha256": _exact_hash(
            value.get("backend_ipv4_subnet_sha256"),
            "network guard backend subnet",
        ),
        "ingress_ipv4_subnet_sha256": _exact_hash(
            value.get("ingress_ipv4_subnet_sha256"),
            "network guard ingress subnet",
        ),
        "backend_bridge_interface_sha256": _exact_hash(
            value.get("backend_bridge_interface_sha256"),
            "network guard backend bridge interface",
        ),
        "ingress_bridge_interface_sha256": _exact_hash(
            value.get("ingress_bridge_interface_sha256"),
            "network guard ingress bridge interface",
        ),
        "policy_sha256": _exact_hash(
            value.get("policy_sha256"), "network guard policy"
        ),
        "ruleset_sha256": _exact_hash(
            value.get("ruleset_sha256"), "network guard ruleset"
        ),
    }


def _normalize_backend_isolation_pair(
    value: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _BACKEND_ISOLATION_FIELDS
        or value.get("schema") != "txnmem-provenance-backend-isolation-v3"
        or value.get("toxiproxy_ingress_membership_verified") is not True
        or value.get("ingress_unique_workload_container_verified") is not True
        or value.get("backend_network_internal") is not True
        or value.get("ingress_network_external") is not True
        or value.get("ingress_proxy_only") is not True
        or value.get("backend_network_driver") != "bridge"
        or value.get("ingress_network_driver") != "bridge"
        or value.get("backend_network_scope") != "local"
        or value.get("ingress_network_scope") != "local"
        or value.get("network_driver_options_empty") is not True
        or value.get("docker_default_ipam_driver_verified") is not True
        or value.get("private_non_overlapping_ipv4_subnets_verified") is not True
        or value.get("networks_non_attachable") is not True
        or value.get("networks_non_swarm_ingress") is not True
        or value.get("networks_non_config_only") is not True
        or value.get("direct_backend_ports_unpublished") is not True
        or value.get("proxy_ports_loopback_only") is not True
        or value.get("published_proxy_ports") != [8474, 19000, 19001]
    ):
        raise TopologyAttestationError("formal backend isolation is invalid")
    containers = value.get("containers")
    roles = ("qdrant", "neo4j", "toxiproxy")
    if not isinstance(containers, list) or len(containers) != len(roles):
        raise TopologyAttestationError("formal backend container closure is incomplete")
    normalized_containers: list[dict[str, Any]] = []
    for row, role in zip(containers, roles):
        if (
            not isinstance(row, Mapping)
            or set(row) != _BACKEND_CONTAINER_FIELDS
            or row.get("role") != role
            or row.get("manifest_digest")
            != FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[role]
        ):
            raise TopologyAttestationError("formal backend container identity is invalid")
        normalized_containers.append(
            {
                "role": role,
                "container_id_sha256": _exact_hash(
                    row.get("container_id_sha256"),
                    f"{role} container identity",
                ),
                "runtime_image_id_sha256": _exact_hash(
                    row.get("runtime_image_id_sha256"),
                    f"{role} runtime image identity",
                ),
                "manifest_digest": FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[role],
            }
        )
    ingress_address_text = value.get("toxiproxy_ingress_ipv4")
    if not isinstance(ingress_address_text, str):
        raise TopologyAttestationError("formal proxy ingress identity is invalid")
    try:
        ingress_address = ipaddress.IPv4Address(ingress_address_text)
    except ValueError as exc:
        raise TopologyAttestationError(
            "formal proxy ingress identity is invalid"
        ) from exc
    if str(ingress_address) != ingress_address_text:
        raise TopologyAttestationError("formal proxy ingress identity is invalid")
    ingress_address_hash = _exact_hash(
        value.get("toxiproxy_ingress_ipv4_sha256"), "proxy ingress IPv4"
    )
    if ingress_address_hash != _hash_text(ingress_address_text):
        raise TopologyAttestationError("formal proxy ingress identity is invalid")
    raw = {
        "schema": "txnmem-provenance-backend-isolation-v3",
        "network_name_sha256": _exact_hash(
            value.get("network_name_sha256"), "backend network name"
        ),
        "network_id_sha256": _exact_hash(
            value.get("network_id_sha256"), "backend network identity"
        ),
        "ingress_network_name_sha256": _exact_hash(
            value.get("ingress_network_name_sha256"),
            "ingress network name",
        ),
        "ingress_network_id_sha256": _exact_hash(
            value.get("ingress_network_id_sha256"),
            "ingress network identity",
        ),
        "toxiproxy_ingress_ipv4": ingress_address_text,
        "toxiproxy_ingress_ipv4_sha256": ingress_address_hash,
        "toxiproxy_ingress_endpoint_id_sha256": _exact_hash(
            value.get("toxiproxy_ingress_endpoint_id_sha256"),
            "proxy ingress endpoint identity",
        ),
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
        "backend_ipv4_subnet_sha256": _exact_hash(
            value.get("backend_ipv4_subnet_sha256"),
            "backend IPv4 subnet",
        ),
        "ingress_ipv4_subnet_sha256": _exact_hash(
            value.get("ingress_ipv4_subnet_sha256"),
            "ingress IPv4 subnet",
        ),
        "backend_bridge_interface_sha256": _exact_hash(
            value.get("backend_bridge_interface_sha256"),
            "backend bridge interface",
        ),
        "ingress_bridge_interface_sha256": _exact_hash(
            value.get("ingress_bridge_interface_sha256"),
            "ingress bridge interface",
        ),
        "networks_non_attachable": True,
        "networks_non_swarm_ingress": True,
        "networks_non_config_only": True,
        "direct_backend_ports_unpublished": True,
        "proxy_ports_loopback_only": True,
        "published_proxy_ports": [8474, 19000, 19001],
        "containers": normalized_containers,
    }
    sanitized = dict(raw)
    sanitized.pop("toxiproxy_ingress_ipv4")
    sanitized["schema"] = "txnmem-provenance-backend-isolation-sanitized-v3"
    return raw, sanitized


def _validate_raw_backend_isolation(value: Any) -> dict[str, Any]:
    raw, _sanitized = _normalize_backend_isolation_pair(value)
    return raw


def _sanitize_backend_isolation(value: Any) -> dict[str, Any]:
    _raw, sanitized = _normalize_backend_isolation_pair(value)
    return sanitized


def _validate_sanitized_backend_isolation(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _SANITIZED_BACKEND_ISOLATION_FIELDS
        or value.get("schema")
        != "txnmem-provenance-backend-isolation-sanitized-v3"
    ):
        raise TopologyAttestationError("sanitized backend isolation is invalid")
    ingress_address_hash = _exact_hash(
        value.get("toxiproxy_ingress_ipv4_sha256"), "proxy ingress IPv4"
    )
    raw = dict(value)
    raw["schema"] = "txnmem-provenance-backend-isolation-v3"
    raw["toxiproxy_ingress_ipv4"] = "0.0.0.0"
    raw["toxiproxy_ingress_ipv4_sha256"] = _hash_text("0.0.0.0")
    _raw, sanitized = _normalize_backend_isolation_pair(raw)
    sanitized["toxiproxy_ingress_ipv4_sha256"] = ingress_address_hash
    return sanitized


def _validate_network_guard_backend_binding(
    network_guard: Mapping[str, Any],
    backend_isolation: Mapping[str, Any],
) -> None:
    for field in (
        "backend_ipv4_subnet_sha256",
        "ingress_ipv4_subnet_sha256",
        "backend_bridge_interface_sha256",
        "ingress_bridge_interface_sha256",
    ):
        if network_guard.get(field) != backend_isolation.get(field):
            raise TopologyAttestationError(
                "network guard is not bound to backend isolation"
            )


def _validate_execution_monitor_attestation(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _EXECUTION_MONITOR_FIELDS
        or value.get("schema") != "txnmem-provenance-execution-monitor-v2"
        or value.get("sampling_interval_ms") != 250
        or value.get("violation_count") != 0
        or value.get("invariants") != _EXECUTION_MONITOR_INVARIANTS
    ):
        raise TopologyAttestationError("formal execution monitor is invalid")
    sample_count = _exact_positive_int(
        value.get("sample_count"), "execution monitor sample count"
    )
    first = _exact_positive_int(
        value.get("first_sample_monotonic_ns"), "execution monitor first sample"
    )
    last = _exact_positive_int(
        value.get("last_sample_monotonic_ns"), "execution monitor last sample"
    )
    gate_release = _exact_positive_int(
        value.get("gate_release_monotonic_ns"),
        "execution monitor gate release",
    )
    child_exit = _exact_positive_int(
        value.get("child_exit_monotonic_ns"), "execution monitor child exit"
    )
    max_gap = _exact_positive_int(
        value.get("max_observed_gap_ns"), "execution monitor maximum gap"
    )
    max_load = _exact_nonnegative_int(
        value.get("max_load1_milli"), "execution monitor maximum load"
    )
    cpu_count = _exact_positive_int(
        value.get("cpu_logical_count"), "execution monitor CPU count"
    )
    load_limit = _exact_positive_int(
        value.get("load1_limit_milli"), "execution monitor load limit"
    )
    duration = last - first
    if (
        sample_count < 2
        or duration <= 0
        or not (first <= gate_release < child_exit <= last)
        or max_gap > 2_000_000_000
        or max_gap > duration
        or duration > max_gap * (sample_count - 1)
        or gate_release - first > 2_000_000_000
        or last - child_exit > 2_000_000_000
        or cpu_count > 1024
        or load_limit != cpu_count * 1000
        or max_load > load_limit
    ):
        raise TopologyAttestationError("formal execution monitor coverage is invalid")
    return {
        "schema": "txnmem-provenance-execution-monitor-v2",
        "sampling_interval_ms": 250,
        "sample_count": sample_count,
        "duration_ns": duration,
        "pre_release_coverage_ns": gate_release - first,
        "execution_coverage_ns": child_exit - gate_release,
        "terminal_coverage_gap_ns": last - child_exit,
        "max_observed_gap_ns": max_gap,
        "violation_count": 0,
        "cpu_logical_count": cpu_count,
        "load1_limit_milli": load_limit,
        "max_load1_milli": max_load,
        "invariants": list(_EXECUTION_MONITOR_INVARIANTS),
        "samples_sha256": _exact_hash(
            value.get("samples_sha256"), "execution monitor samples"
        ),
        "first_sample_sha256": _exact_hash(
            value.get("first_sample_sha256"), "execution monitor first hash"
        ),
        "last_sample_sha256": _exact_hash(
            value.get("last_sample_sha256"), "execution monitor last hash"
        ),
    }


def _validate_sanitized_execution_monitor(value: Any) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value) != _SANITIZED_EXECUTION_MONITOR_FIELDS
        or value.get("schema") != "txnmem-provenance-execution-monitor-v2"
        or value.get("sampling_interval_ms") != 250
        or value.get("violation_count") != 0
        or value.get("invariants") != _EXECUTION_MONITOR_INVARIANTS
    ):
        raise TopologyAttestationError("sanitized execution monitor is invalid")
    sample_count = _exact_positive_int(
        value.get("sample_count"), "sanitized monitor sample count"
    )
    duration = _exact_positive_int(
        value.get("duration_ns"), "sanitized monitor duration"
    )
    max_gap = _exact_positive_int(
        value.get("max_observed_gap_ns"), "sanitized monitor maximum gap"
    )
    max_load = _exact_nonnegative_int(
        value.get("max_load1_milli"), "sanitized monitor maximum load"
    )
    cpu_count = _exact_positive_int(
        value.get("cpu_logical_count"), "sanitized monitor CPU count"
    )
    load_limit = _exact_positive_int(
        value.get("load1_limit_milli"), "sanitized monitor load limit"
    )
    pre_release = _exact_nonnegative_int(
        value.get("pre_release_coverage_ns"),
        "sanitized monitor pre-release coverage",
    )
    execution = _exact_positive_int(
        value.get("execution_coverage_ns"),
        "sanitized monitor execution coverage",
    )
    terminal_gap = _exact_nonnegative_int(
        value.get("terminal_coverage_gap_ns"),
        "sanitized monitor terminal coverage",
    )
    if (
        sample_count < 2
        or max_gap > 2_000_000_000
        or max_gap > duration
        or duration > max_gap * (sample_count - 1)
        or pre_release > 2_000_000_000
        or terminal_gap > 2_000_000_000
        or pre_release + execution + terminal_gap != duration
        or cpu_count > 1024
        or load_limit != cpu_count * 1000
        or max_load > load_limit
    ):
        raise TopologyAttestationError("sanitized execution monitor coverage is invalid")
    for field in (
        "samples_sha256",
        "first_sample_sha256",
        "last_sample_sha256",
    ):
        _exact_hash(value.get(field), f"sanitized monitor {field}")


def _validate_shared(document: Mapping[str, Any], *, expected_schema: str) -> None:
    expected_fields = (
        _LAUNCH_FIELDS if expected_schema == RAW_LAUNCH_SCHEMA else _COMPLETION_FIELDS
    )
    if not isinstance(document, Mapping) or set(document) != expected_fields:
        raise TopologyAttestationError("execution attestation fields do not match schema")
    if document.get("schema") != expected_schema:
        raise TopologyAttestationError("execution attestation schema mismatch")
    if document.get("collector_id") != COLLECTOR_ID:
        raise TopologyAttestationError("execution collector identity mismatch")
    if document.get("formal_execution_requested") is not True:
        raise TopologyAttestationError("formal execution was not requested by collector")
    for field in (
        "run_id_sha256",
        "config_sha256",
        "config_file_sha256",
        "workload_sha256",
        "environment_attestation_sha256",
        "source_manifest_sha256",
        "collector_sha256",
        "runner_sha256",
        "command_sha256",
    ):
        _exact_hash(document.get(field), field)
    source_commit = document.get("source_commit")
    if not isinstance(source_commit, str) or not _GIT_COMMIT.fullmatch(source_commit):
        raise TopologyAttestationError("source_commit is not an exact Git object id")
    source_manifest = _validate_source_manifest(
        document.get("source_manifest"), source_commit
    )
    if hashlib.sha256(_canonical_bytes(source_manifest)).hexdigest() != document.get(
        "source_manifest_sha256"
    ):
        raise TopologyAttestationError("source manifest hash mismatch")
    command_manifest = _validate_command_manifest(
        document.get("command_manifest"), document
    )
    if hashlib.sha256(_canonical_bytes(command_manifest)).hexdigest() != document.get(
        "command_sha256"
    ):
        raise TopologyAttestationError("command manifest hash mismatch")
    _validate_child_process(document.get("child_process"), command_manifest)
    network_guard = _validate_network_guard_attestation(
        document.get("network_guard")
    )
    backend_isolation = _validate_raw_backend_isolation(
        document.get("backend_isolation")
    )
    _validate_network_guard_backend_binding(network_guard, backend_isolation)
    if expected_schema == RAW_COMPLETION_SCHEMA:
        _validate_execution_monitor_attestation(document.get("execution_monitor"))
    if document.get("transport") not in _TRANSPORTS:
        raise TopologyAttestationError("unsupported topology transport")
    for field in (
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
    ):
        _exact_positive_int(document.get(field), field)


def _validate_snapshot(
    rows: Any,
) -> tuple[dict[str, Mapping[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(rows, list) or len(rows) != len(_ROLES):
        raise TopologyAttestationError("execution topology roles are incomplete")
    raw_by_role: dict[str, Mapping[str, Any]] = {}
    safe_by_role: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != _ROLE_FIELDS:
            raise TopologyAttestationError("execution role fields do not match schema")
        role = row.get("role")
        if role not in _ROLES or role in raw_by_role:
            raise TopologyAttestationError("execution role is invalid or duplicated")
        role_name = str(role)
        host = _private_identity(row.get("host_identity"), "host_identity")
        owner = _private_identity(row.get("listener_owner"), "listener_owner")
        version = row.get("service_version")
        if not is_registered_service_version(role_name, version):
            raise TopologyAttestationError("service version is not source-registered")
        raw_by_role[role_name] = row
        safe_by_role[role_name] = {
            "host_identity_sha256": _hash_text(host),
            "listener_owner_sha256": _hash_text(owner),
            "service_version": version,
            "rtt_ms": _safe_rtt(row.get("rtt_ms")),
            "proxy_counter_bytes": _exact_nonnegative_int(
                row.get("proxy_counter_bytes"), "proxy_counter_bytes"
            ),
        }
    if set(raw_by_role) != set(_ROLES):
        raise TopologyAttestationError("execution topology roles mismatch")
    return raw_by_role, safe_by_role


def sanitize_topology_attestation(
    launch: Mapping[str, Any],
    completion: Mapping[str, Any],
    *,
    launch_file_sha256: str,
    completion_file_sha256: str,
    authorization_nonce: bytes,
) -> dict[str, Any]:
    """Sanitize two-phase collector evidence and derive continuity facts."""

    _validate_shared(launch, expected_schema=RAW_LAUNCH_SCHEMA)
    _validate_shared(completion, expected_schema=RAW_COMPLETION_SCHEMA)
    launch_file_hash = _exact_hash(launch_file_sha256, "launch_file_sha256")
    completion_file_hash = _exact_hash(
        completion_file_sha256, "completion_file_sha256"
    )
    nonce_hash = hashlib.sha256(authorization_nonce).hexdigest()
    run_hash = str(launch.get("run_id_sha256"))
    if (
        launch.get("authorization_nonce_sha256") != nonce_hash
        or completion.get("authorization_nonce_sha256") != nonce_hash
        or FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(run_hash) != nonce_hash
    ):
        raise TopologyAttestationError("execution authorization nonce is not registered")
    launch_proof = _exact_hash(
        launch.get("authorization_proof_sha256"), "launch authorization proof"
    )
    completion_proof = _exact_hash(
        completion.get("authorization_proof_sha256"),
        "completion authorization proof",
    )
    if not hmac.compare_digest(
        launch_proof, execution_authorization_proof(authorization_nonce, launch)
    ) or not hmac.compare_digest(
        completion_proof,
        execution_authorization_proof(authorization_nonce, completion),
    ):
        raise TopologyAttestationError("execution authorization proof mismatch")
    if completion.get("launch_file_sha256") != launch_file_hash:
        raise TopologyAttestationError("completion does not bind exact launch bytes")
    for field in _SHARED_FIELDS - {"collector_id", "formal_execution_requested"}:
        if completion.get(field) != launch.get(field):
            raise TopologyAttestationError(f"execution {field} changed across phases")
    if completion.get("collector_id") != launch.get("collector_id"):
        raise TopologyAttestationError("collector changed across execution phases")
    if completion.get("formal_execution_requested") is not True:
        raise TopologyAttestationError("completion is not a formal collector run")
    if type(completion.get("exit_code")) is not int or completion.get("exit_code") != 0:
        raise TopologyAttestationError("measured candidate process did not exit cleanly")

    candidate_id = completion.get("candidate_bundle_id")
    if not isinstance(candidate_id, str) or not _CANDIDATE_ID.fullmatch(candidate_id):
        raise TopologyAttestationError("candidate bundle identity is invalid")
    expected_candidate = (
        "diagnostic-vector_graph-"
        f"{str(launch['config_sha256'])[:16]}-{str(launch['run_id_sha256'])[:16]}"
    )
    if candidate_id != expected_candidate:
        raise TopologyAttestationError("candidate bundle identity is not launch-bound")
    for field in (
        "evidence_manifest_sha256",
        "candidate_operation_samples_sha256",
        "candidate_repetitions_sha256",
    ):
        _exact_hash(completion.get(field), field)

    raw_before, before = _validate_snapshot(launch.get("roles"))
    raw_after, after = _validate_snapshot(completion.get("roles"))
    receipt_material = {
        "schema": "txnmem-provenance-candidate-attestation-material-v1",
        "candidate_bundle_id": candidate_id,
        "run_id_sha256": launch["run_id_sha256"],
        "config_sha256": launch["config_sha256"],
        "config_file_sha256": launch["config_file_sha256"],
        "workload_sha256": launch["workload_sha256"],
        "environment_attestation_sha256": launch[
            "environment_attestation_sha256"
        ],
        "evidence_manifest_sha256": completion["evidence_manifest_sha256"],
        "matrix_cell_count": launch["matrix_cell_count"],
        "repetition_count": launch["repetition_count"],
        "operation_sample_count": launch["operation_sample_count"],
        "observed_service_versions": {
            role: raw_after[role]["service_version"]
            for role in ("qdrant", "neo4j", "toxiproxy")
        },
        "candidate_operation_samples_sha256": completion[
            "candidate_operation_samples_sha256"
        ],
        "candidate_repetitions_sha256": completion[
            "candidate_repetitions_sha256"
        ],
    }
    _raw_candidate_seal, sanitized_candidate_seal = _validate_candidate_seal(
        completion.get("candidate_seal"),
        expected_completion_receipt_sha256=hashlib.sha256(
            _canonical_bytes(receipt_material)
        ).hexdigest(),
    )
    command_manifest = _validate_command_manifest(
        launch.get("command_manifest"), launch
    )
    child_process, sanitized_child_process = _validate_child_process(
        launch.get("child_process"), command_manifest
    )
    if (
        raw_before["client"].get("listener_owner")
        != child_process["start_identity"]
        or raw_after["client"].get("listener_owner")
        != child_process["start_identity"]
    ):
        raise TopologyAttestationError("client role is not the measured child process")
    if (
        before["client"]["service_version"]
        != command_manifest["python_version"]
        or after["client"]["service_version"]
        != command_manifest["python_version"]
    ):
        raise TopologyAttestationError("client runtime does not match command manifest")
    raw_routes_before, sanitized_proxy_routes = _validate_proxy_routes(
        launch.get("proxy_routes"), command_manifest
    )
    raw_routes_after, _sanitized_routes_after = _validate_proxy_routes(
        completion.get("proxy_routes"), command_manifest
    )
    if raw_routes_before != raw_routes_after:
        raise TopologyAttestationError("formal proxy route changed during measurement")
    if any(
        int(before[role]["proxy_counter_bytes"]) != 0
        for role in ("qdrant", "neo4j", "toxiproxy")
    ):
        raise TopologyAttestationError("formal proxy counters were not isolated at launch")
    sanitized_roles: list[dict[str, Any]] = []
    host_hashes: set[str] = set()
    listener_continuity = True
    host_continuity = True
    proxy_deltas: dict[str, int] = {}
    for role in _ROLES:
        first = before[role]
        last = after[role]
        if first["service_version"] != last["service_version"]:
            raise TopologyAttestationError("service version changed during measurement")
        counter_before = int(first["proxy_counter_bytes"])
        counter_after = int(last["proxy_counter_bytes"])
        if counter_after < counter_before:
            raise TopologyAttestationError("proxy byte counter moved backwards")
        delta = counter_after - counter_before
        proxy_deltas[role] = delta
        host_hashes.add(str(first["host_identity_sha256"]))
        host_hashes.add(str(last["host_identity_sha256"]))
        listener_continuity = listener_continuity and (
            first["listener_owner_sha256"] == last["listener_owner_sha256"]
        )
        host_continuity = host_continuity and (
            first["host_identity_sha256"] == last["host_identity_sha256"]
        )
        sanitized_roles.append(
            {
                "role": role,
                "host_identity_sha256": first["host_identity_sha256"],
                "listener_owner_before_sha256": first[
                    "listener_owner_sha256"
                ],
                "listener_owner_after_sha256": last[
                    "listener_owner_sha256"
                ],
                "service_version": first["service_version"],
                "rtt_ms_before": first["rtt_ms"],
                "rtt_ms_after": last["rtt_ms"],
                "proxy_counter_bytes_before": counter_before,
                "proxy_counter_bytes_after": counter_after,
                "proxy_counter_bytes_delta": delta,
            }
        )
    proxy_route_observed = bool(
        proxy_deltas.get("qdrant", 0) > 0 and proxy_deltas.get("neo4j", 0) > 0
    )
    source_continuity = all(
        launch.get(field) == completion.get(field)
        for field in (
            "source_commit",
            "source_manifest_sha256",
            "collector_sha256",
            "runner_sha256",
            "command_sha256",
        )
    )
    payload = {
        "schema": SANITIZED_SCHEMA,
        "collector_id": COLLECTOR_ID,
        "formal_execution_requested": True,
        "run_id_sha256": launch["run_id_sha256"],
        "config_sha256": launch["config_sha256"],
        "config_file_sha256": launch["config_file_sha256"],
        "workload_sha256": launch["workload_sha256"],
        "environment_attestation_sha256": launch[
            "environment_attestation_sha256"
        ],
        "evidence_manifest_sha256": completion["evidence_manifest_sha256"],
        "source_commit": launch["source_commit"],
        "source_manifest": copy.deepcopy(launch["source_manifest"]),
        "source_manifest_sha256": launch["source_manifest_sha256"],
        "collector_sha256": launch["collector_sha256"],
        "runner_sha256": launch["runner_sha256"],
        "command_manifest": copy.deepcopy(command_manifest),
        "command_sha256": launch["command_sha256"],
        "child_process": sanitized_child_process,
        "network_guard": _validate_network_guard_attestation(
            launch.get("network_guard")
        ),
        "backend_isolation": _sanitize_backend_isolation(
            launch.get("backend_isolation")
        ),
        "proxy_routes": sanitized_proxy_routes,
        "authorization_nonce_sha256": nonce_hash,
        "launch_authorization_proof_sha256": launch_proof,
        "completion_authorization_proof_sha256": completion_proof,
        "source_continuity_verified": source_continuity,
        "launch_file_sha256": launch_file_hash,
        "completion_file_sha256": completion_file_hash,
        "launch_identity_sha256": hashlib.sha256(_canonical_bytes(launch)).hexdigest(),
        "completion_identity_sha256": hashlib.sha256(
            _canonical_bytes(completion)
        ).hexdigest(),
        "candidate_bundle_id": candidate_id,
        "candidate_operation_samples_sha256": completion[
            "candidate_operation_samples_sha256"
        ],
        "candidate_repetitions_sha256": completion[
            "candidate_repetitions_sha256"
        ],
        "candidate_seal": sanitized_candidate_seal,
        "execution_monitor": _validate_execution_monitor_attestation(
            completion.get("execution_monitor")
        ),
        "exit_code": completion["exit_code"],
        "transport": launch["transport"],
        "matrix_cell_count": launch["matrix_cell_count"],
        "repetition_count": launch["repetition_count"],
        "operation_sample_count": launch["operation_sample_count"],
        "toxiproxy_route_observed": proxy_route_observed,
        "listener_continuity_verified": listener_continuity,
        "host_continuity_verified": host_continuity,
        "host_count": len(host_hashes),
        "roles": sanitized_roles,
    }
    payload["attestation_sha256"] = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
    return payload


def _validate_sanitized_shape(attestation: Mapping[str, Any]) -> None:
    if not isinstance(attestation, Mapping) or set(attestation) != _SANITIZED_FIELDS:
        raise TopologyAttestationError("sanitized topology fields do not match schema")
    if (
        attestation.get("schema") != SANITIZED_SCHEMA
        or attestation.get("collector_id") != COLLECTOR_ID
    ):
        raise TopologyAttestationError("sanitized topology schema mismatch")
    for field in (
        "run_id_sha256",
        "config_sha256",
        "config_file_sha256",
        "workload_sha256",
        "environment_attestation_sha256",
        "evidence_manifest_sha256",
        "source_manifest_sha256",
        "collector_sha256",
        "runner_sha256",
        "command_sha256",
        "authorization_nonce_sha256",
        "launch_authorization_proof_sha256",
        "completion_authorization_proof_sha256",
        "launch_file_sha256",
        "completion_file_sha256",
        "launch_identity_sha256",
        "completion_identity_sha256",
        "candidate_operation_samples_sha256",
        "candidate_repetitions_sha256",
        "attestation_sha256",
    ):
        _exact_hash(attestation.get(field), field)
    source_commit = attestation.get("source_commit")
    if not isinstance(source_commit, str) or not _GIT_COMMIT.fullmatch(source_commit):
        raise TopologyAttestationError("sanitized source commit is invalid")
    source_manifest = _validate_source_manifest(
        attestation.get("source_manifest"), source_commit
    )
    if hashlib.sha256(_canonical_bytes(source_manifest)).hexdigest() != attestation.get(
        "source_manifest_sha256"
    ):
        raise TopologyAttestationError("sanitized source manifest hash mismatch")
    command_manifest = _validate_command_manifest(
        attestation.get("command_manifest"), attestation
    )
    if hashlib.sha256(_canonical_bytes(command_manifest)).hexdigest() != attestation.get(
        "command_sha256"
    ):
        raise TopologyAttestationError("sanitized command manifest hash mismatch")
    child_process = attestation.get("child_process")
    if (
        not isinstance(child_process, Mapping)
        or set(child_process) != _SANITIZED_CHILD_PROCESS_FIELDS
    ):
        raise TopologyAttestationError("sanitized child process fields mismatch")
    for field in _SANITIZED_CHILD_PROCESS_FIELDS:
        _exact_hash(child_process.get(field), f"sanitized child {field}")
    if (
        child_process.get("executable_sha256")
        != command_manifest.get("python_executable_sha256")
        or child_process.get("argv_sha256") != command_manifest.get("argv_sha256")
    ):
        raise TopologyAttestationError("sanitized child process is not command-bound")
    network_guard = _validate_network_guard_attestation(
        attestation.get("network_guard")
    )
    backend_isolation = _validate_sanitized_backend_isolation(
        attestation.get("backend_isolation")
    )
    _validate_network_guard_backend_binding(network_guard, backend_isolation)
    _validate_sanitized_execution_monitor(attestation.get("execution_monitor"))
    candidate_seal = attestation.get("candidate_seal")
    if (
        not isinstance(candidate_seal, Mapping)
        or set(candidate_seal) != _SANITIZED_CANDIDATE_SEAL_FIELDS
    ):
        raise TopologyAttestationError("sanitized candidate seal fields mismatch")
    for field in (
        "root_device_sha256",
        "root_inode_sha256",
        "tree_sha256",
        "completion_receipt_sha256",
    ):
        _exact_hash(candidate_seal.get(field), f"sanitized candidate seal {field}")
    _exact_positive_int(
        candidate_seal.get("directory_count"), "sanitized candidate directories"
    )
    _exact_positive_int(
        candidate_seal.get("file_count"), "sanitized candidate files"
    )
    _validate_sanitized_proxy_routes(attestation.get("proxy_routes"))
    candidate_id = attestation.get("candidate_bundle_id")
    if not isinstance(candidate_id, str) or not _CANDIDATE_ID.fullmatch(candidate_id):
        raise TopologyAttestationError("sanitized candidate identity is invalid")
    if attestation.get("transport") not in _TRANSPORTS:
        raise TopologyAttestationError("unsupported sanitized transport")
    for field in (
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
        "host_count",
    ):
        _exact_positive_int(attestation.get(field), field)
    if type(attestation.get("exit_code")) is not int:
        raise TopologyAttestationError("exit_code must be an exact integer")
    for field in (
        "formal_execution_requested",
        "source_continuity_verified",
        "toxiproxy_route_observed",
        "listener_continuity_verified",
        "host_continuity_verified",
    ):
        if type(attestation.get(field)) is not bool:
            raise TopologyAttestationError(f"{field} must be boolean")
    roles = attestation.get("roles")
    if not isinstance(roles, list) or len(roles) != len(_ROLES):
        raise TopologyAttestationError("sanitized roles are incomplete")
    seen_roles: set[str] = set()
    host_hashes: set[str] = set()
    for row in roles:
        if not isinstance(row, Mapping) or set(row) != _SANITIZED_ROLE_FIELDS:
            raise TopologyAttestationError("sanitized role fields do not match schema")
        role = row.get("role")
        if role not in _ROLES or role in seen_roles:
            raise TopologyAttestationError("sanitized role is invalid or duplicated")
        role_name = str(role)
        seen_roles.add(role_name)
        host_hashes.add(_exact_hash(row.get("host_identity_sha256"), "host hash"))
        _exact_hash(row.get("listener_owner_before_sha256"), "listener hash")
        _exact_hash(row.get("listener_owner_after_sha256"), "listener hash")
        if not is_registered_service_version(role_name, row.get("service_version")):
            raise TopologyAttestationError("sanitized service version is not registered")
        _safe_rtt(row.get("rtt_ms_before"))
        _safe_rtt(row.get("rtt_ms_after"))
        before = _exact_nonnegative_int(
            row.get("proxy_counter_bytes_before"), "proxy counter"
        )
        after = _exact_nonnegative_int(
            row.get("proxy_counter_bytes_after"), "proxy counter"
        )
        delta = _exact_nonnegative_int(
            row.get("proxy_counter_bytes_delta"), "proxy counter delta"
        )
        if after - before != delta:
            raise TopologyAttestationError("sanitized proxy counter delta mismatch")
    if seen_roles != set(_ROLES) or len(host_hashes) != attestation.get("host_count"):
        raise TopologyAttestationError("sanitized role/host inventory mismatch")
    without_hash = dict(attestation)
    supplied_hash = without_hash.pop("attestation_sha256")
    if supplied_hash != hashlib.sha256(_canonical_bytes(without_hash)).hexdigest():
        raise TopologyAttestationError("topology attestation hash mismatch")


def validate_registered_topology_attestation(
    attestation: Mapping[str, Any],
    *,
    expected_run_id_sha256: str,
    expected_config_sha256: str,
    expected_config_file_sha256: str,
    expected_workload_sha256: str,
    expected_environment_attestation_sha256: str,
    expected_evidence_manifest_sha256: str,
    expected_candidate_bundle_id: str,
    expected_candidate_operation_samples_sha256: str,
    expected_candidate_repetitions_sha256: str,
) -> dict[str, Any]:
    """Validate a registered, source-bound launch/completion attestation."""

    _validate_sanitized_shape(attestation)
    expected = {
        "run_id_sha256": _exact_hash(expected_run_id_sha256, "expected run hash"),
        "config_sha256": _exact_hash(expected_config_sha256, "expected config hash"),
        "config_file_sha256": _exact_hash(
            expected_config_file_sha256, "expected config-file hash"
        ),
        "workload_sha256": _exact_hash(
            expected_workload_sha256, "expected workload hash"
        ),
        "environment_attestation_sha256": _exact_hash(
            expected_environment_attestation_sha256, "expected environment hash"
        ),
        "evidence_manifest_sha256": _exact_hash(
            expected_evidence_manifest_sha256, "expected evidence hash"
        ),
        "candidate_operation_samples_sha256": _exact_hash(
            expected_candidate_operation_samples_sha256,
            "expected candidate sample hash",
        ),
        "candidate_repetitions_sha256": _exact_hash(
            expected_candidate_repetitions_sha256,
            "expected candidate repetition hash",
        ),
    }
    if (
        not isinstance(expected_candidate_bundle_id, str)
        or not _CANDIDATE_ID.fullmatch(expected_candidate_bundle_id)
    ):
        raise TopologyAttestationError("expected candidate bundle identity is invalid")
    expected["candidate_bundle_id"] = expected_candidate_bundle_id
    for field, value in expected.items():
        if attestation.get(field) != value:
            raise TopologyAttestationError(f"topology {field} mismatch")
    if (
        attestation.get("formal_execution_requested") is not True
        or attestation.get("source_continuity_verified") is not True
        or attestation.get("listener_continuity_verified") is not True
        or attestation.get("host_continuity_verified") is not True
        or attestation.get("toxiproxy_route_observed") is not True
        or attestation.get("exit_code") != 0
    ):
        raise TopologyAttestationError("execution completion conditions are not verified")
    if attestation.get("transport") not in {"local_loopback", "container_bridge"}:
        raise TopologyAttestationError(
            "formal cross-host transport requires a remote role collector"
        )
    role_deltas = {
        str(row.get("role")): row.get("proxy_counter_bytes_delta")
        for row in attestation.get("roles", [])
        if isinstance(row, Mapping)
    }
    if (
        type(role_deltas.get("qdrant")) is not int
        or role_deltas["qdrant"] <= 0
        or type(role_deltas.get("neo4j")) is not int
        or role_deltas["neo4j"] <= 0
    ):
        raise TopologyAttestationError("proxy counters do not prove both service routes")
    registered = FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN.get(
        expected_run_id_sha256
    )
    registered_nonce = FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(
        expected_run_id_sha256
    )
    if registered_nonce != attestation.get("authorization_nonce_sha256"):
        raise TopologyAttestationError("launch authorization nonce is not registered")
    if registered != attestation.get("attestation_sha256"):
        raise TopologyAttestationError("topology attestation is not registered")
    return copy.deepcopy(dict(attestation))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="sanitize a two-phase TxnMem execution attestation"
    )
    parser.add_argument("--launch", type=Path, required=True)
    parser.add_argument("--completion", type=Path, required=True)
    parser.add_argument("--authorization-nonce", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        from txnmem_formal_io import FormalStore
        from txnmem_provenance_performance import load_strict_json_document

        launch, launch_raw = load_strict_json_document(args.launch)
        completion, completion_raw = load_strict_json_document(args.completion)
        if not isinstance(launch, dict) or not isinstance(completion, dict):
            raise TopologyAttestationError("execution attestations must be mappings")
        authorization_nonce = _read_private_authorization_nonce(
            args.authorization_nonce,
            repository_root=Path(__file__).resolve().parents[1],
        )
        sanitized = sanitize_topology_attestation(
            launch,
            completion,
            launch_file_sha256=hashlib.sha256(launch_raw).hexdigest(),
            completion_file_sha256=hashlib.sha256(completion_raw).hexdigest(),
            authorization_nonce=authorization_nonce,
        )
        output = args.out.expanduser().absolute()
        FormalStore(output.parent).write_json_exclusive(
            output.name, payload=sanitized
        )
    except (OSError, ValueError) as exc:
        print(f"topology attestation blocked: {type(exc).__name__}")
        return 2
    print(f"wrote sanitized topology attestation -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
