"""Deterministic provenance-DAG performance workloads and accounting.

The module deliberately separates measured operations from graph preload and
readback.  Formal runs fail closed unless both persistent stores are healthy,
the namespace is initially empty, the preloaded graph is exact, the final
provenance closure is exact, and the execution environment is attested as
isolated.  Only generated identifiers, counts, hashes and timing metadata are
returned; memory values and service endpoints never enter result artifacts.
"""

from __future__ import annotations

import copy
import enum
import hashlib
import json
import math
import os
import random
import re
import secrets
import stat
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_provenance_contract import is_registered_service_version


GRAPH_SCHEMA = "txnmem-provenance-dag-v1"
MATRIX_SCHEMA = "txnmem-provenance-performance-v2"
OPERATION_TYPES = ("read", "search", "derive", "invalidate_repair")
PROVENANCE_PRELOAD_WORKERS = 8
PROVENANCE_PRELOAD_REPAIR_LIMIT_PER_RECORD = 1
PRELOAD_RETRY_SCOPE = "measured_operations_only"
_SAFE_ERROR_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_ISOLATED_CELL_FAILURE_CLASSES = frozenset(
    {
        "AssertionError",
        "BackendError",
        "BrokenPipeError",
        "ConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "FormalEligibilityError",
        "HTTPError",
        "JSONDecodeError",
        "KeyError",
        "MemoryError",
        "OSError",
        "ProvenancePerformanceError",
        "RuntimeError",
        "TimeoutError",
        "TypeError",
        "URLError",
        "ValueError",
        "VectorGraphBackendError",
        "VectorGraphCommitConflict",
        "_IsolatedCellWorkerFailure",
        "_MeasuredOperationFailure",
        "_ServiceBoundaryFailure",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
ENVIRONMENT_SCHEMA = "txnmem-provenance-environment-v1"
FORMAL_MATRIX_CONFIG = {
    "schema": MATRIX_SCHEMA,
    "graph_node_counts": [100, 1000, 10000],
    "concurrency_levels": [1, 2, 4, 8, 16],
    "repetitions": 30,
    "graph_seed": 17,
    "operations_per_type": 8,
    "bootstrap_repetitions": 10_000,
    "bootstrap_seed": 17,
    "request_timeout_seconds": 30.0,
    "cell_stall_timeout_seconds": 3600.0,
}
PROVENANCE_ABLATION_SCHEMA = "txnmem-provenance-ablation-v1"
PROVENANCE_ABLATION_VARIANTS = ("TxnMem", "MemoryOnly-NoProvenance")
FORMAL_ABLATION_CONFIG = {
    "schema": PROVENANCE_ABLATION_SCHEMA,
    "run_identity": "provenance-ablation-v10",
    "cells": [
        {"graph_node_count": 100, "concurrency": 1},
        {"graph_node_count": 1000, "concurrency": 4},
        {"graph_node_count": 10000, "concurrency": 16},
    ],
    "variants": list(PROVENANCE_ABLATION_VARIANTS),
    "repetitions": 30,
    "repetition_seeds": [17 + 1000 * index for index in range(30)],
    "graph_seed": 17,
    "operations_per_type": 8,
    "bootstrap_repetitions": 10_000,
    "bootstrap_seed": 1701,
    "variant_order_seed": 1702,
    "request_timeout_seconds": 30.0,
    "cell_stall_timeout_seconds": 3600.0,
}
_CONFIG_FIELDS = frozenset(FORMAL_MATRIX_CONFIG)
_DIAGNOSTIC_CONFIG_FIELDS = _CONFIG_FIELDS - {"cell_stall_timeout_seconds"}
_ABLATION_CONFIG_FIELDS = frozenset(FORMAL_ABLATION_CONFIG)
_ENVIRONMENT_FIELDS = frozenset(
    {
        "schema",
        "isolation_verified",
        "co_tenant_load_detected",
        "source",
        "cpu_logical_count",
        "memory_total_bytes",
        "disk_medium",
        "toxiproxy_version",
    }
)
_ENVIRONMENT_SOURCES = frozenset(
    {
        "host-observation-v1",
        "scheduler-observation-v1",
        "cross-host-observation-v1",
        "collector-observation-v2",
    }
)
_DISK_MEDIA = frozenset({"nvme", "ssd", "hdd", "network-block"})


class ProvenancePerformanceError(RuntimeError):
    """Raised when evidence cannot satisfy the formal performance boundary."""


class FormalEligibilityReason(enum.Enum):
    """Closed, publication-safe reasons for formal eligibility failures."""

    ENVIRONMENT_INELIGIBLE = "environment_ineligible"
    NAMESPACE_COLLISION = "namespace_collision"
    SERVICE_HEALTH_UNAVAILABLE = "service_health_unavailable"
    NAMESPACE_NOT_EMPTY = "namespace_not_empty"
    PARALLEL_PRELOAD_LOADER_MISSING = "parallel_preload_loader_missing"
    PRELOAD_RECOVERY_ACCOUNTING_INVALID = (
        "preload_recovery_accounting_invalid"
    )
    PRELOAD_STATE_MISMATCH = "preload_state_mismatch"
    RETRY_POLICY_INELIGIBLE = "retry_policy_ineligible"
    RETRY_METRIC_UNAVAILABLE = "retry_metric_unavailable"
    REPETITION_STATE_INELIGIBLE = "repetition_state_ineligible"


class FormalEligibilityError(ProvenancePerformanceError):
    """Carry one closed reason without publishing exception text."""

    def __init__(
        self,
        reason: FormalEligibilityReason,
        message: str,
    ) -> None:
        if type(reason) is not FormalEligibilityReason:
            raise TypeError("formal eligibility reason must be an exact enum member")
        super().__init__(message)
        self._reason = reason

    @property
    def reason(self) -> FormalEligibilityReason:
        return self._reason

    @property
    def reason_code(self) -> str:
        return self._reason.value


class _IsolatedCellWorkerFailure(ProvenancePerformanceError):
    """Carry only validated, publication-safe failure metadata across IPC."""

    _OPERATIONS = frozenset(
        {"healthcheck", "read", "search", "derive", "invalidate_repair"}
    )
    _SERVICES = frozenset({"qdrant", "neo4j"})

    def __init__(
        self,
        *,
        failure_reason_code: str,
        failure_provenance: Mapping[str, Any],
    ) -> None:
        allowed_reasons = {
            reason.value for reason in FormalEligibilityReason
        } | {"unclassified_failure"}
        if (
            type(failure_reason_code) is not str
            or failure_reason_code not in allowed_reasons
            or type(failure_provenance) is not dict
            or set(failure_provenance)
            != {"error_classes", "operation", "root_error_class", "service"}
        ):
            raise TypeError("isolated worker failure metadata must be closed")
        error_classes = failure_provenance.get("error_classes")
        operation = failure_provenance.get("operation")
        root_error_class = failure_provenance.get("root_error_class")
        service = failure_provenance.get("service")
        if (
            type(error_classes) is not list
            or not 1 <= len(error_classes) <= 8
            or any(
                type(name) is not str
                or name not in _ISOLATED_CELL_FAILURE_CLASSES
                for name in error_classes
            )
            or type(root_error_class) is not str
            or root_error_class != error_classes[-1]
            or (
                operation is not None
                and (
                    type(operation) is not str
                    or operation not in self._OPERATIONS
                )
            )
            or (
                service is not None
                and (type(service) is not str or service not in self._SERVICES)
            )
        ):
            raise TypeError("isolated worker failure metadata must be closed")
        super().__init__("isolated cell worker failed")
        self._failure_reason_code = failure_reason_code
        self._failure_error_classes = tuple(error_classes)
        self._failure_operation = operation
        self._failure_root_error_class = root_error_class
        self._failure_service = service

    @property
    def failure_reason_code(self) -> str:
        return self._failure_reason_code

    @property
    def failure_provenance(self) -> dict[str, Any]:
        return {
            "error_classes": list(self._failure_error_classes),
            "operation": self._failure_operation,
            "root_error_class": self._failure_root_error_class,
            "service": self._failure_service,
        }


def _close_isolated_cell_failure_provenance(
    failure_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Map open exception names to one finite IPC-safe vocabulary."""

    if (
        type(failure_provenance) is not dict
        or set(failure_provenance)
        != {"error_classes", "operation", "root_error_class", "service"}
    ):
        raise TypeError("isolated worker failure metadata must be closed")
    error_classes = failure_provenance.get("error_classes")
    operation = failure_provenance.get("operation")
    root_error_class = failure_provenance.get("root_error_class")
    service = failure_provenance.get("service")
    if (
        type(error_classes) is not list
        or not 1 <= len(error_classes) <= 8
        or any(type(name) is not str for name in error_classes)
        or type(root_error_class) is not str
        or root_error_class != error_classes[-1]
        or (
            operation is not None
            and (
                type(operation) is not str
                or operation not in _IsolatedCellWorkerFailure._OPERATIONS
            )
        )
        or (
            service is not None
            and (
                type(service) is not str
                or service not in _IsolatedCellWorkerFailure._SERVICES
            )
        )
    ):
        raise TypeError("isolated worker failure metadata must be closed")
    closed_classes = [
        name if name in _ISOLATED_CELL_FAILURE_CLASSES else "BackendError"
        for name in error_classes
    ]
    return {
        "error_classes": closed_classes,
        "operation": operation,
        "root_error_class": closed_classes[-1],
        "service": service,
    }


class _MeasuredOperationFailure(ProvenancePerformanceError):
    """Attach one closed operation name without retaining public payload text."""

    def __init__(self, operation: str) -> None:
        if type(operation) is not str or operation not in OPERATION_TYPES:
            raise TypeError("measured failure operation must be closed")
        super().__init__("measured provenance operation failed")
        self._txnmem_operation = operation


class _PrivatePublicationMode(enum.Enum):
    """One invocation-scoped publication mode for the protected lifecycle gate."""

    INTEGRATED_POINTER_WITHOUT_RECEIPT = (
        "integrated_pointer_without_receipt"
    )


def _require_private_publication_mode(value: Any) -> _PrivatePublicationMode:
    if type(value) is not _PrivatePublicationMode:
        raise TypeError("private publication mode must be an exact enum member")
    return value


@dataclass(frozen=True)
class GraphSpec:
    """A canonical, topologically ordered provenance graph."""

    nodes: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    layers: tuple[int, ...]
    seed: int
    graph_sha256: str

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    @property
    def depth(self) -> int:
        return max(self.layers, default=-1) + 1

    def metadata(self) -> dict[str, Any]:
        return {
            "schema": GRAPH_SCHEMA,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "depth": self.depth,
            "seed": self.seed,
            "graph_sha256": self.graph_sha256,
        }


def _strict_positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _strict_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_nonnegative_integer(value: Any, name: str) -> int:
    value = _strict_integer(value, name)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> Any:
    raise ValueError(f"non-finite JSON number: {value}")


def _finite_json_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number: {value}")
    return parsed


def load_strict_json_document(path: str | Path) -> tuple[Any, bytes]:
    """Load one regular JSON file without symlinks, duplicates, or nonfinite values."""

    requested = Path(os.path.abspath(os.fspath(Path(path).expanduser())))
    parts = requested.parts
    if not parts or parts[0] != os.sep or len(parts) < 2:
        raise ValueError("formal JSON input must be an absolute regular file")
    directory_descriptor: int | None = None
    descriptor: int | None = None
    try:
        directory_descriptor = os.open(
            os.sep,
            os.O_RDONLY
            | os.O_DIRECTORY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        for component in parts[1:-1]:
            child = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_descriptor,
            )
            os.close(directory_descriptor)
            directory_descriptor = child
        descriptor = os.open(
            parts[-1],
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_descriptor,
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("formal JSON input must be a regular file")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = None
            raw = stream.read()
    except (OSError, UnicodeError) as exc:
        raise ValueError("cannot read strict JSON input") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory_descriptor is not None:
            os.close(directory_descriptor)
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_nonfinite_json,
            parse_float=_finite_json_float,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("malformed strict JSON input") from exc
    return value, raw


def load_strict_json_file(path: str | Path) -> Any:
    return load_strict_json_document(path)[0]


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("value is not finite canonical JSON") from exc


def validate_matrix_config(config: Mapping[str, Any], *, formal: bool) -> dict[str, Any]:
    """Validate the closed performance configuration before any backend call."""

    if type(formal) is not bool:
        raise ValueError("formal must be a boolean")
    if not isinstance(config, Mapping):
        raise ValueError("provenance performance config must be a mapping")
    actual_fields = set(config)
    if (
        (formal and actual_fields != _CONFIG_FIELDS)
        or (
            not formal
            and actual_fields != _CONFIG_FIELDS
            and actual_fields != _DIAGNOSTIC_CONFIG_FIELDS
        )
    ):
        raise ValueError("provenance performance config fields do not match schema")
    normalized = copy.deepcopy(dict(config))
    normalized.setdefault(
        "cell_stall_timeout_seconds",
        FORMAL_MATRIX_CONFIG["cell_stall_timeout_seconds"],
    )
    if normalized.get("schema") != MATRIX_SCHEMA:
        raise ValueError("provenance performance config schema mismatch")
    expand_matrix(normalized)
    _strict_positive_integer(
        normalized.get("bootstrap_repetitions"), "bootstrap_repetitions"
    )
    _strict_integer(normalized.get("bootstrap_seed"), "bootstrap_seed")
    timeout = normalized.get("request_timeout_seconds")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(float(timeout))
        or float(timeout) <= 0.0
    ):
        raise ValueError("request_timeout_seconds must be a positive finite number")
    cell_timeout = normalized.get("cell_stall_timeout_seconds")
    if (
        isinstance(cell_timeout, bool)
        or not isinstance(cell_timeout, (int, float))
        or not math.isfinite(float(cell_timeout))
        or float(cell_timeout) <= 0.0
    ):
        raise ValueError(
            "cell_stall_timeout_seconds must be a positive finite number"
        )
    _canonical_json_bytes(normalized)
    if formal and _canonical_json_bytes(normalized) != _canonical_json_bytes(
        FORMAL_MATRIX_CONFIG
    ):
        raise ValueError("formal provenance performance config must match the frozen config")
    return normalized


def validate_provenance_ablation_config(
    config: Mapping[str, Any], *, formal: bool
) -> dict[str, Any]:
    """Validate the closed two-variant provenance ablation contract."""

    if type(formal) is not bool:
        raise ValueError("formal must be a boolean")
    if not isinstance(config, Mapping) or set(config) != _ABLATION_CONFIG_FIELDS:
        raise ValueError("provenance ablation config fields do not match schema")
    normalized = copy.deepcopy(dict(config))
    if normalized.get("schema") != PROVENANCE_ABLATION_SCHEMA:
        raise ValueError("provenance ablation config schema mismatch")
    run_identity = normalized.get("run_identity")
    if not isinstance(run_identity, str) or not run_identity.strip():
        raise ValueError("provenance ablation run identity must be non-empty")
    if normalized.get("variants") != list(PROVENANCE_ABLATION_VARIANTS):
        raise ValueError("provenance ablation variants do not match closed domain")

    raw_cells = normalized.get("cells")
    if not isinstance(raw_cells, list) or not raw_cells:
        raise ValueError("provenance ablation cells must be a non-empty list")
    cell_coordinates: list[tuple[int, int]] = []
    for raw_cell in raw_cells:
        if not isinstance(raw_cell, Mapping) or set(raw_cell) != {
            "graph_node_count",
            "concurrency",
        }:
            raise ValueError("provenance ablation cell fields do not match schema")
        cell_coordinates.append(
            (
                _strict_positive_integer(
                    raw_cell.get("graph_node_count"), "graph_node_count"
                ),
                _strict_positive_integer(raw_cell.get("concurrency"), "concurrency"),
            )
        )
    if len(set(cell_coordinates)) != len(cell_coordinates):
        raise ValueError("provenance ablation cells must not contain duplicates")

    repetitions = _strict_positive_integer(
        normalized.get("repetitions"), "repetitions"
    )
    raw_repetition_seeds = normalized.get("repetition_seeds")
    if not isinstance(raw_repetition_seeds, list):
        raise ValueError("repetition_seeds must be a list")
    repetition_seeds = [
        _strict_integer(value, "repetition_seed")
        for value in raw_repetition_seeds
    ]
    if len(repetition_seeds) != repetitions:
        raise ValueError("repetition_seeds must match repetitions")
    if len(set(repetition_seeds)) != len(repetition_seeds):
        raise ValueError("repetition_seeds must not contain duplicates")
    _strict_integer(normalized.get("graph_seed"), "graph_seed")
    _strict_positive_integer(
        normalized.get("operations_per_type"), "operations_per_type"
    )
    _strict_positive_integer(
        normalized.get("bootstrap_repetitions"), "bootstrap_repetitions"
    )
    _strict_integer(normalized.get("bootstrap_seed"), "bootstrap_seed")
    _strict_integer(normalized.get("variant_order_seed"), "variant_order_seed")
    for field in ("request_timeout_seconds", "cell_stall_timeout_seconds"):
        timeout = normalized.get(field)
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout <= 0.0
        ):
            raise ValueError(f"{field} must be a positive finite number")
    _canonical_json_bytes(normalized)
    if formal and _canonical_json_bytes(normalized) != _canonical_json_bytes(
        FORMAL_ABLATION_CONFIG
    ):
        raise ValueError("formal provenance ablation config must match frozen config")
    return normalized


def expand_provenance_ablation(
    config: Mapping[str, Any],
) -> list[dict[str, int | str]]:
    """Expand the closed ablation cells and variants in stable paired order."""

    normalized = validate_provenance_ablation_config(config, formal=False)
    rows: list[dict[str, int | str]] = []
    for raw_cell in normalized["cells"]:
        node_count = int(raw_cell["graph_node_count"])
        concurrency = int(raw_cell["concurrency"])
        for variant in PROVENANCE_ABLATION_VARIANTS:
            rows.append(
                {
                    "cell_id": f"n{node_count}-c{concurrency}",
                    "graph_node_count": node_count,
                    "concurrency": concurrency,
                    "variant": variant,
                    "repetitions": int(normalized["repetitions"]),
                }
            )
    return rows


def formal_matrix_config_sha256() -> str:
    return hashlib.sha256(_canonical_json_bytes(FORMAL_MATRIX_CONFIG)).hexdigest()


def formal_config_file_sha256() -> str:
    path = Path(__file__).resolve().parents[1] / "configs" / "provenance_performance_matrix.json"
    document, raw = load_strict_json_document(path)
    validated = validate_matrix_config(document, formal=True)
    if _canonical_json_bytes(validated) != _canonical_json_bytes(FORMAL_MATRIX_CONFIG):
        raise ProvenancePerformanceError("repository formal config file is not frozen")
    return hashlib.sha256(raw).hexdigest()


def formal_matrix_workload_sha256() -> str:
    config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
    graph_metadata = [
        build_layered_dag(node_count, int(config["graph_seed"])).metadata()
        for node_count in config["graph_node_counts"]
    ]
    material = {
        "schema": "txnmem-provenance-formal-workload-v1",
        "config_sha256": formal_matrix_config_sha256(),
        "graphs": graph_metadata,
        "operation_types": list(OPERATION_TYPES),
    }
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def cell_reports_sha256(reports: Sequence[Mapping[str, Any]]) -> str:
    if isinstance(reports, (str, bytes)) or not isinstance(reports, Sequence):
        raise ValueError("cell reports must be a sequence")
    material = []
    for report in reports:
        if not isinstance(report, Mapping):
            raise ValueError("cell report must be a mapping")
        material.append(copy.deepcopy(dict(report)))
    material.sort(key=lambda report: str(report.get("cell_id", "")))
    return hashlib.sha256(_canonical_json_bytes(material)).hexdigest()


def _canonical_graph_material(
    nodes: Iterable[str], edges: Iterable[Sequence[str]]
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    canonical_nodes = tuple(sorted(str(node) for node in nodes))
    if len(set(canonical_nodes)) != len(canonical_nodes):
        raise ValueError("graph node identifiers must be unique")
    node_set = set(canonical_nodes)
    canonical_edges_list: list[tuple[str, str]] = []
    for raw_edge in edges:
        if len(raw_edge) != 2:
            raise ValueError("each graph edge must have two endpoints")
        source, target = (str(raw_edge[0]), str(raw_edge[1]))
        if source == target:
            raise ValueError("self edges are not permitted")
        if source not in node_set or target not in node_set:
            raise ValueError("graph edge endpoint is missing from nodes")
        canonical_edges_list.append((source, target))
    canonical_edges = tuple(sorted(canonical_edges_list))
    if len(set(canonical_edges)) != len(canonical_edges):
        raise ValueError("graph edges must be unique")
    return canonical_nodes, canonical_edges


def canonical_graph_sha256(
    nodes: Iterable[str], edges: Iterable[Sequence[str]]
) -> str:
    """Hash only graph identity and provenance edges, never payload values."""

    canonical_nodes, canonical_edges = _canonical_graph_material(nodes, edges)
    encoded = json.dumps(
        {
            "schema": GRAPH_SCHEMA,
            "nodes": canonical_nodes,
            "edges": canonical_edges,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_rank(seed: int, target: str, parent: str) -> bytes:
    return hashlib.sha256(
        f"{seed}\0{target}\0{parent}".encode("utf-8")
    ).digest()


def build_layered_dag(node_count: int, seed: int) -> GraphSpec:
    """Build a connected deterministic DAG with one or two prior-layer parents."""

    node_count = _strict_positive_integer(node_count, "node_count")
    seed = _strict_integer(seed, "seed")
    width = max(6, len(str(node_count - 1)))
    nodes = tuple(f"n{index:0{width}d}" for index in range(node_count))
    layers = tuple(int(math.floor(math.log2(index + 1))) for index in range(node_count))
    edges: list[tuple[str, str]] = []
    for target_index in range(1, node_count):
        layer = layers[target_index]
        previous_start = (2 ** (layer - 1)) - 1
        previous_end = min((2**layer) - 2, target_index - 1)
        candidates = list(range(previous_start, previous_end + 1))
        target = nodes[target_index]
        ranked = sorted(
            candidates,
            key=lambda index: (_stable_rank(seed, target, nodes[index]), nodes[index]),
        )
        count_digest = hashlib.sha256(
            f"{seed}\0{target}\0parent-count".encode("utf-8")
        ).digest()
        parent_count = 1 if len(ranked) == 1 else 1 + (count_digest[0] % 2)
        for parent_index in sorted(ranked[:parent_count]):
            edges.append((nodes[parent_index], target))
    canonical_nodes, canonical_edges = _canonical_graph_material(nodes, edges)
    graph_hash = canonical_graph_sha256(canonical_nodes, canonical_edges)
    return GraphSpec(
        nodes=nodes,
        edges=canonical_edges,
        layers=layers,
        seed=seed,
        graph_sha256=graph_hash,
    )


def _positive_axis(config: Mapping[str, Any], key: str) -> tuple[int, ...]:
    raw = config.get(key)
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{key} must be a non-empty list")
    values = tuple(_strict_positive_integer(value, key) for value in raw)
    if len(set(values)) != len(values):
        raise ValueError(f"{key} must not contain duplicates")
    return values


def expand_matrix(config: Mapping[str, Any]) -> list[dict[str, int | str]]:
    """Expand graph-size and concurrency axes in stable source order."""

    if not isinstance(config, Mapping):
        raise ValueError("matrix config must be a mapping")
    node_counts = _positive_axis(config, "graph_node_counts")
    concurrency_levels = _positive_axis(config, "concurrency_levels")
    repetitions = _strict_positive_integer(config.get("repetitions"), "repetitions")
    operations_per_type = _strict_positive_integer(
        config.get("operations_per_type"), "operations_per_type"
    )
    graph_seed = _strict_integer(config.get("graph_seed", 17), "graph_seed")
    cells: list[dict[str, int | str]] = []
    for node_count in node_counts:
        for concurrency in concurrency_levels:
            cells.append(
                {
                    "cell_id": f"n{node_count}-c{concurrency}",
                    "graph_node_count": node_count,
                    "concurrency": concurrency,
                    "repetitions": repetitions,
                    "operations_per_type": operations_per_type,
                    "graph_seed": graph_seed,
                }
            )
    return cells


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_health(raw: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not isinstance(raw, Mapping):
        return result
    for service in ("qdrant", "neo4j"):
        value = raw.get(service)
        if not isinstance(value, Mapping):
            continue
        version = value.get("version")
        safe_version = version if is_registered_service_version(service, version) else None
        result[service] = {
            "available": value.get("available") is True,
            "version": safe_version,
        }
    return result


def _safe_environment(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {
            "schema": ENVIRONMENT_SCHEMA,
            "isolation_verified": False,
            "co_tenant_load_detected": None,
            "source": "unavailable",
            "cpu_logical_count": None,
            "memory_total_bytes": None,
            "disk_medium": None,
            "toxiproxy_version": None,
            "attestation_sha256": None,
        }

    if set(raw) != _ENVIRONMENT_FIELDS:
        raise ProvenancePerformanceError(
            "environment attestation fields do not match the closed schema"
        )
    if raw.get("schema") != ENVIRONMENT_SCHEMA:
        raise ProvenancePerformanceError("environment attestation schema mismatch")
    if type(raw.get("isolation_verified")) is not bool or type(
        raw.get("co_tenant_load_detected")
    ) is not bool:
        raise ProvenancePerformanceError("environment isolation fields must be booleans")
    source = raw.get("source")
    if source not in _ENVIRONMENT_SOURCES:
        raise ProvenancePerformanceError("environment source is not an approved enum")
    try:
        cpu_count = _strict_positive_integer(
            raw.get("cpu_logical_count"), "cpu_logical_count"
        )
        memory_bytes = _strict_positive_integer(
            raw.get("memory_total_bytes"), "memory_total_bytes"
        )
    except ValueError as exc:
        raise ProvenancePerformanceError(str(exc)) from exc
    disk_medium = raw.get("disk_medium")
    if disk_medium not in _DISK_MEDIA:
        raise ProvenancePerformanceError("environment disk_medium is not approved")
    toxiproxy_version = raw.get("toxiproxy_version")
    if not is_registered_service_version("toxiproxy", toxiproxy_version):
        raise ProvenancePerformanceError("unsafe Toxiproxy version")
    try:
        canonical = _canonical_json_bytes(dict(raw))
    except ValueError as exc:
        raise ProvenancePerformanceError("environment attestation is not finite JSON") from exc
    return {
        "schema": ENVIRONMENT_SCHEMA,
        "isolation_verified": raw.get("isolation_verified"),
        "co_tenant_load_detected": raw.get("co_tenant_load_detected"),
        "source": source,
        "cpu_logical_count": cpu_count,
        "memory_total_bytes": memory_bytes,
        "disk_medium": disk_medium,
        "toxiproxy_version": toxiproxy_version,
        "attestation_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def validate_environment_attestation(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a closed raw attestation while retaining only approved fields."""

    _safe_environment(raw)
    return copy.deepcopy(dict(raw))


def _memory_inventory(backend: InstrumentedMemoryBackend) -> dict[str, Any]:
    snapshot = backend.snapshot()
    nodes = sorted(str(node) for node in snapshot)
    edges = sorted(
        (str(source), str(target))
        for target, row in snapshot.items()
        for source in row.get("derived_from", [])
    )
    statuses = [str(row.get("status", "unknown")) for row in snapshot.values()]
    return {
        "classification": "complete",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "graph_sha256": canonical_graph_sha256(nodes, edges),
        "status_counts": {
            status: statuses.count(status) for status in sorted(set(statuses))
        },
    }


def _inventory(backend: Any, limit: int) -> dict[str, Any]:
    limit = _strict_positive_integer(limit, "inventory limit")
    provider = getattr(backend, "provenance_inventory", None)
    if callable(provider):
        try:
            raw = provider(limit=limit)
        except TypeError:
            raw = provider()
    elif isinstance(backend, InstrumentedMemoryBackend):
        raw = _memory_inventory(backend)
    else:
        return {
            "classification": "unknown",
            "node_count": None,
            "edge_count": None,
            "graph_sha256": None,
            "status_counts": {},
        }
    if not isinstance(raw, Mapping):
        return {
            "classification": "unknown",
            "node_count": None,
            "edge_count": None,
            "graph_sha256": None,
            "status_counts": {},
        }
    classification = str(raw.get("classification", "unknown"))
    if classification not in {"complete", "partial", "unknown"}:
        classification = "unknown"
    status_counts = raw.get("status_counts", {})
    if not isinstance(status_counts, Mapping):
        status_counts = {}
    node_count = raw.get("node_count")
    edge_count = raw.get("edge_count")
    graph_sha256 = raw.get("graph_sha256")
    counts_valid = all(
        type(value) is int and value >= 0 for value in (node_count, edge_count)
    )
    hash_valid = isinstance(graph_sha256, str) and _SHA256.fullmatch(graph_sha256)
    statuses_valid = all(
        isinstance(key, str)
        and bool(key)
        and _SAFE_ERROR_CLASS.fullmatch(key)
        and type(value) is int
        and value >= 0
        for key, value in status_counts.items()
    )
    if (
        classification == "complete"
        and (
            not counts_valid
            or not hash_valid
            or not statuses_valid
            or sum(status_counts.values()) != node_count
        )
    ):
        classification = "unknown"
    return {
        "classification": classification,
        "node_count": node_count if counts_valid else None,
        "edge_count": edge_count if counts_valid else None,
        "graph_sha256": graph_sha256 if hash_valid else None,
        "status_counts": {
            key: value
            for key, value in sorted(status_counts.items())
            if isinstance(key, str)
            and _SAFE_ERROR_CLASS.fullmatch(key)
            and type(value) is int
            and value >= 0
        },
    }


def _inventory_matches(
    inventory: Mapping[str, Any],
    nodes: Sequence[str],
    edges: Sequence[Sequence[str]],
    status_counts: Mapping[str, int],
) -> bool:
    if set(inventory) != {
        "classification",
        "node_count",
        "edge_count",
        "graph_sha256",
        "status_counts",
    }:
        return False
    node_count = inventory.get("node_count")
    edge_count = inventory.get("edge_count")
    inventory_statuses = inventory.get("status_counts")
    return bool(
        inventory.get("classification") == "complete"
        and type(node_count) is int
        and type(edge_count) is int
        and node_count == len(nodes)
        and edge_count == len(edges)
        and inventory.get("graph_sha256") == canonical_graph_sha256(nodes, edges)
        and isinstance(inventory_statuses, Mapping)
        and all(type(value) is int for value in inventory_statuses.values())
        and dict(inventory_statuses) == dict(status_counts)
    )


def _namespace(run_id: str, graph: GraphSpec, concurrency: int, repetition: int) -> str:
    digest = hashlib.sha256(
        f"{run_id}\0{graph.graph_sha256}\0{concurrency}\0{repetition}".encode("utf-8")
    ).hexdigest()
    return f"txnmem-prov-{digest[:24]}"


def _preload_graph(backend: Any, graph: GraphSpec) -> dict[str, int]:
    parents: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for source, target in graph.edges:
        parents[target].append(source)
    parallel_preload = getattr(
        backend, "supports_parallel_provenance_preload", False
    )
    preload_record = getattr(backend, "preload_provenance_record", None)
    if parallel_preload is True:
        if not callable(preload_record):
            raise FormalEligibilityError(
                FormalEligibilityReason.PARALLEL_PRELOAD_LOADER_MISSING,
                "parallel preload capability has no record loader"
            )
        nodes_by_layer: dict[int, list[str]] = {}
        for node, layer in zip(graph.nodes, graph.layers):
            nodes_by_layer.setdefault(int(layer), []).append(node)
        worker_count = min(
            PROVENANCE_PRELOAD_WORKERS,
            max(len(nodes) for nodes in nodes_by_layer.values()),
        )
        recovery_count = 0
        active_count = 0
        observed_peak = 0
        activity_lock = threading.Lock()

        def load_record(node: str, source_ids: list[str]) -> int:
            nonlocal active_count, observed_peak
            with activity_lock:
                active_count += 1
                observed_peak = max(observed_peak, active_count)
            try:
                return preload_record(
                    node,
                    source_ids,
                    value=f"provenance:{node}",
                )
            finally:
                with activity_lock:
                    active_count -= 1

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for layer in sorted(nodes_by_layer):
                futures = []
                for node in nodes_by_layer[layer]:
                    source_ids = sorted(parents[node])
                    futures.append(
                        executor.submit(
                            load_record,
                            node,
                            source_ids,
                        )
                    )
                for future in futures:
                    recovered = future.result()
                    if (
                        type(recovered) is not int
                        or recovered < 0
                        or recovered > 1
                    ):
                        raise FormalEligibilityError(
                            FormalEligibilityReason.PRELOAD_RECOVERY_ACCOUNTING_INVALID,
                            "preload recovery accounting is invalid"
                        )
                    recovery_count += recovered
        return {
            "preload_worker_limit": worker_count,
            "preload_observed_peak_concurrency": observed_peak,
            "setup_repair_limit_per_record": (
                PROVENANCE_PRELOAD_REPAIR_LIMIT_PER_RECORD
            ),
            "setup_repair_count": recovery_count,
        }
    for node in graph.nodes:
        source_ids = sorted(parents[node])
        value = f"provenance:{node}"
        if source_ids:
            backend.derive(node, source_ids, value=value)
        else:
            backend.write(node, value=value)
    return {
        "preload_worker_limit": 1,
        "preload_observed_peak_concurrency": 1,
        "setup_repair_limit_per_record": 0,
        "setup_repair_count": 0,
    }


def _operation_plan(
    graph: GraphSpec, operations_per_type: int
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for operation_rank, operation in enumerate(OPERATION_TYPES):
        for index in range(operations_per_type):
            digest = hashlib.sha256(
                f"{graph.seed}\0{graph.graph_sha256}\0{operation}\0{index}".encode(
                    "utf-8"
                )
            ).digest()
            source_index = int.from_bytes(digest[:8], "big") % graph.node_count
            plan.append(
                {
                    "operation": operation,
                    "operation_rank": operation_rank,
                    "operation_index": index,
                    "source": graph.nodes[source_index],
                }
            )
    return plan


def _apply_measured_operation(backend: Any, operation: Mapping[str, Any]) -> None:
    name = str(operation["operation"])
    index = int(operation["operation_index"])
    source = str(operation["source"])
    if name == "read":
        if backend.read(source) is None:
            raise ProvenancePerformanceError("read returned no active record")
        return
    if name == "search":
        if not backend.search(f"provenance:{source}"):
            raise ProvenancePerformanceError("search returned no record")
        return
    if name == "derive":
        memory_id = f"perf-derived-{index:06d}"
        backend.derive(memory_id, [source], value=f"provenance:{memory_id}")
        return
    if name == "invalidate_repair":
        invalid_id = f"perf-invalid-{index:06d}"
        repair_id = f"perf-repair-{index:06d}"
        backend.derive(invalid_id, [source], value=f"provenance:{invalid_id}")
        backend.invalidate(invalid_id)
        backend.derive(repair_id, [source], value=f"provenance:{repair_id}")
        return
    raise ValueError(f"unsupported provenance operation: {name}")


def _safe_error_name(exc: Exception) -> str:
    value = type(exc).__name__
    return value if _SAFE_ERROR_CLASS.fullmatch(value) else "BackendError"


def _expected_final_graph(
    graph: GraphSpec, plan: Sequence[Mapping[str, Any]]
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...], dict[str, int]]:
    nodes = list(graph.nodes)
    edges = list(graph.edges)
    invalid_count = 0
    for operation in plan:
        name = str(operation["operation"])
        index = int(operation["operation_index"])
        source = str(operation["source"])
        if name == "derive":
            target = f"perf-derived-{index:06d}"
            nodes.append(target)
            edges.append((source, target))
        elif name == "invalidate_repair":
            invalid_id = f"perf-invalid-{index:06d}"
            repair_id = f"perf-repair-{index:06d}"
            nodes.extend((invalid_id, repair_id))
            edges.extend(((source, invalid_id), (source, repair_id)))
            invalid_count += 1
    statuses = {"active": len(nodes) - invalid_count}
    if invalid_count:
        statuses["invalid"] = invalid_count
    return tuple(nodes), tuple(sorted(edges)), statuses


def run_matrix_cell(
    backend_factory: Callable[[str], Any],
    graph: GraphSpec,
    concurrency: int,
    repetitions: int,
    *,
    operations_per_type: int = 8,
    run_id: str,
    formal: bool = False,
    require_formal_eligibility: bool = False,
    environment_attestation: Mapping[str, Any]
    | Callable[[Any], Mapping[str, Any]]
    | None = None,
    progress_callback: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one matrix cell and return sanitized samples plus repetition units."""

    concurrency = _strict_positive_integer(concurrency, "concurrency")
    repetitions = _strict_positive_integer(repetitions, "repetitions")
    operations_per_type = _strict_positive_integer(
        operations_per_type, "operations_per_type"
    )
    if not isinstance(graph, GraphSpec):
        raise ValueError("graph must be a GraphSpec")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")
    if type(formal) is not bool:
        raise ValueError("formal must be a boolean")
    if type(require_formal_eligibility) is not bool:
        raise ValueError("require_formal_eligibility must be a boolean")
    enforce_formal_eligibility = formal or require_formal_eligibility
    prevalidated_environment: dict[str, Any] | None = None
    if environment_attestation is not None and not callable(
        environment_attestation
    ):
        prevalidated_environment = _safe_environment(environment_attestation)
        if enforce_formal_eligibility and not (
            prevalidated_environment["isolation_verified"]
            and prevalidated_environment["co_tenant_load_detected"] is False
        ):
            raise FormalEligibilityError(
                FormalEligibilityReason.ENVIRONMENT_INELIGIBLE,
                "formal run requires verified isolation without co-tenant load"
            )
    plan = _operation_plan(graph, operations_per_type)
    expected_nodes, expected_edges, expected_statuses = _expected_final_graph(graph, plan)
    all_samples: list[dict[str, Any]] = []
    repetition_rows: list[dict[str, Any]] = []
    cell_id = f"n{graph.node_count}-c{concurrency}"
    namespace_hashes: set[str] = set()

    for repetition in range(repetitions):
        namespace = _namespace(run_id, graph, concurrency, repetition)
        namespace_sha256 = _hash_text(namespace)
        if namespace_sha256 in namespace_hashes:
            raise FormalEligibilityError(
                FormalEligibilityReason.NAMESPACE_COLLISION,
                "namespace collision within matrix cell",
            )
        namespace_hashes.add(namespace_sha256)
        backend = backend_factory(namespace)
        primary_failure: BaseException | None = None
        try:
            health_provider = getattr(backend, "healthcheck", None)
            health = _safe_health(health_provider() if callable(health_provider) else None)
            services_available = all(
                health.get(service, {}).get("available") is True
                and isinstance(health.get(service, {}).get("version"), str)
                for service in ("qdrant", "neo4j")
            )
            if callable(environment_attestation):
                raw_environment = environment_attestation(backend)
                environment = _safe_environment(raw_environment)
            elif prevalidated_environment is not None:
                environment = dict(prevalidated_environment)
            else:
                environment_provider = getattr(backend, "performance_environment", None)
                raw_environment = (
                    environment_provider() if callable(environment_provider) else None
                )
                environment = _safe_environment(raw_environment)
            isolation_valid = bool(
                environment["isolation_verified"]
                and environment["co_tenant_load_detected"] is False
            )
            if enforce_formal_eligibility and not services_available:
                raise FormalEligibilityError(
                    FormalEligibilityReason.SERVICE_HEALTH_UNAVAILABLE,
                    "formal run requires available Qdrant and Neo4j health checks"
                )
            if enforce_formal_eligibility and not isolation_valid:
                raise FormalEligibilityError(
                    FormalEligibilityReason.ENVIRONMENT_INELIGIBLE,
                    "formal run requires verified isolation without co-tenant load"
                )

            empty_inventory = _inventory(backend, limit=1)
            empty_hash = canonical_graph_sha256((), ())
            namespace_empty = bool(
                empty_inventory.get("classification") == "complete"
                and empty_inventory.get("node_count") == 0
                and empty_inventory.get("edge_count") == 0
                and empty_inventory.get("graph_sha256") == empty_hash
                and empty_inventory.get("status_counts") == {}
            )
            if enforce_formal_eligibility and not namespace_empty:
                raise FormalEligibilityError(
                    FormalEligibilityReason.NAMESPACE_NOT_EMPTY,
                    "formal run requires a new empty namespace"
                )

            setup_started_ns = time.perf_counter_ns()
            preload_metadata = _preload_graph(backend, graph)
            setup_elapsed_ns = max(
                1, time.perf_counter_ns() - setup_started_ns
            )
            preload_inventory = _inventory(backend, limit=graph.node_count + 1)
            preload_closed = _inventory_matches(
                preload_inventory,
                graph.nodes,
                graph.edges,
                {"active": graph.node_count},
            )
            if enforce_formal_eligibility and not preload_closed:
                raise FormalEligibilityError(
                    FormalEligibilityReason.PRELOAD_STATE_MISMATCH,
                    "preloaded graph count, status, or hash mismatch"
                )

            backend_max_retries = getattr(backend, "max_retries", None)
            driver_retry_seconds = getattr(
                backend, "neo4j_max_transaction_retry_time_seconds", None
            )
            driver_retry_policy_valid = bool(
                not isinstance(driver_retry_seconds, bool)
                and isinstance(driver_retry_seconds, (int, float))
                and math.isfinite(float(driver_retry_seconds))
                and float(driver_retry_seconds) == 0.0
            )
            retry_policy_valid = bool(
                type(backend_max_retries) is int
                and backend_max_retries == 0
                and driver_retry_policy_valid
            )
            metrics_provider = getattr(backend, "metrics", None)

            def retry_metric() -> int | None:
                if not callable(metrics_provider):
                    return None
                raw_metrics = metrics_provider()
                if not isinstance(raw_metrics, Mapping):
                    return None
                value = raw_metrics.get("retry_count")
                return value if type(value) is int and value >= 0 else None

            retries_before = retry_metric()
            retry_metric_valid = retries_before is not None
            if enforce_formal_eligibility and not retry_policy_valid:
                raise FormalEligibilityError(
                    FormalEligibilityReason.RETRY_POLICY_INELIGIBLE,
                    "formal run requires attested zero retry at both backend and driver"
                )
            if enforce_formal_eligibility and not retry_metric_valid:
                raise FormalEligibilityError(
                    FormalEligibilityReason.RETRY_METRIC_UNAVAILABLE,
                    "formal run requires an exact backend retry metric"
                )

            activity_lock = threading.Lock()
            active_operations = 0
            observed_peak_concurrency = 0
            concurrency_target = min(concurrency, len(plan))
            start_barrier = threading.Barrier(concurrency_target)

            def measured(operation: Mapping[str, Any]) -> dict[str, Any]:
                nonlocal active_operations, observed_peak_concurrency
                with activity_lock:
                    active_operations += 1
                    observed_peak_concurrency = max(
                        observed_peak_concurrency, active_operations
                    )
                started_ns: int | None = None
                error_class = None
                failure = None
                success = True
                try:
                    ordinal = (
                        int(operation["operation_rank"]) * operations_per_type
                        + int(operation["operation_index"])
                    )
                    if ordinal < concurrency_target:
                        start_barrier.wait(timeout=30.0)
                    started_ns = time.perf_counter_ns()
                    _apply_measured_operation(backend, operation)
                except Exception as exc:  # failures are retained as aggregate-safe classes
                    success = False
                    error_class = _safe_error_name(exc)
                    failure = exc
                finally:
                    latency_ns = (
                        0
                        if started_ns is None
                        else max(0, time.perf_counter_ns() - started_ns)
                    )
                    with activity_lock:
                        active_operations -= 1
                row = {
                    "cell_id": cell_id,
                    "repetition": repetition,
                    "namespace_sha256": namespace_sha256,
                    "operation": str(operation["operation"]),
                    "latency_ns": latency_ns,
                    "success": success,
                    # Formal factories use max_retries=0. Backends with an
                    # operation-local counter may opt in without global races.
                    "retry_count": 0,
                    "_operation_rank": int(operation["operation_rank"]),
                    "_operation_index": int(operation["operation_index"]),
                }
                if error_class is not None:
                    row["error_class"] = error_class
                if failure is not None:
                    row["_failure"] = failure
                return row

            repetition_started_ns = time.perf_counter_ns()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(measured, operation) for operation in plan]
                measured_rows = [future.result() for future in futures]
            elapsed_ns = max(1, time.perf_counter_ns() - repetition_started_ns)
            measured_rows.sort(
                key=lambda row: (row["_operation_rank"], row["_operation_index"])
            )
            measured_failure = next(
                (
                    (str(row["operation"]), row["_failure"])
                    for row in measured_rows
                    if isinstance(row.get("_failure"), Exception)
                ),
                None,
            )
            for row in measured_rows:
                row.pop("_operation_rank", None)
                row.pop("_operation_index", None)
                row.pop("_failure", None)
            if enforce_formal_eligibility and measured_failure is not None:
                failed_operation, failure = measured_failure
                eligibility_error = FormalEligibilityError(
                    FormalEligibilityReason.REPETITION_STATE_INELIGIBLE,
                    "formal repetition has measured operation failures",
                )
                try:
                    raise _MeasuredOperationFailure(failed_operation) from failure
                except _MeasuredOperationFailure as operation_failure:
                    raise eligibility_error from operation_failure

            retries_after = retry_metric()
            if (
                retries_before is None
                or retries_after is None
                or retries_after < retries_before
            ):
                retry_delta = None
            else:
                retry_delta = retries_after - retries_before

            final_inventory = _inventory(backend, limit=len(expected_nodes) + 1)
            state_closed = _inventory_matches(
                final_inventory,
                expected_nodes,
                expected_edges,
                expected_statuses,
            )
            success_count = sum(1 for row in measured_rows if row["success"])
            failure_count = len(measured_rows) - success_count
            sample_retry_count = sum(int(row["retry_count"]) for row in measured_rows)
            retry_count = retry_delta if retry_delta is not None else sample_retry_count
            eligible = bool(
                services_available
                and isolation_valid
                and retry_policy_valid
                and retry_delta == 0
                and observed_peak_concurrency == concurrency_target
                and namespace_empty
                and preload_closed
                and state_closed
                and failure_count == 0
            )
            diagnostic_eligible = bool(
                namespace_empty
                and preload_closed
                and state_closed
                and failure_count == 0
            )
            repetition_row = {
                "cell_id": cell_id,
                "repetition": repetition,
                "namespace_sha256": namespace_sha256,
                "graph_node_count": graph.node_count,
                "graph_edge_count": graph.edge_count,
                "graph_sha256": graph.graph_sha256,
                "concurrency": concurrency,
                "operation_count": len(measured_rows),
                "success_count": success_count,
                "failure_count": failure_count,
                "retry_count": retry_count,
                "backend_max_retries": backend_max_retries,
                "neo4j_driver_max_transaction_retry_time_ms": (
                    0 if driver_retry_policy_valid else None
                ),
                "observed_peak_concurrency": observed_peak_concurrency,
                "elapsed_ns": elapsed_ns,
                "namespace_initially_empty": namespace_empty,
                "preload_method": (
                    "layered-canonical-write-v1"
                    if getattr(
                        backend,
                        "supports_parallel_provenance_preload",
                        False,
                    )
                    is True
                    else "sequential-semantic-v1"
                ),
                "preload_worker_limit": preload_metadata[
                    "preload_worker_limit"
                ],
                "preload_observed_peak_concurrency": preload_metadata[
                    "preload_observed_peak_concurrency"
                ],
                "setup_repair_limit_per_record": preload_metadata[
                    "setup_repair_limit_per_record"
                ],
                "setup_repair_count": preload_metadata[
                    "setup_repair_count"
                ],
                "setup_elapsed_ns": setup_elapsed_ns,
                "retry_scope": PRELOAD_RETRY_SCOPE,
                "preload_closed": preload_closed,
                "state_closed": state_closed,
                "state_classification": final_inventory["classification"],
                "eligible_for_formal": eligible,
                "eligible_for_diagnostic": diagnostic_eligible,
                "service_health": health,
                "environment": environment,
                "preload_inventory": preload_inventory,
                "final_inventory": final_inventory,
            }
            if enforce_formal_eligibility and not eligible:
                raise FormalEligibilityError(
                    FormalEligibilityReason.REPETITION_STATE_INELIGIBLE,
                    "formal repetition has failures or non-closed persistent state"
                )
            all_samples.extend(measured_rows)
            repetition_rows.append(repetition_row)
        except BaseException as exc:
            primary_failure = exc
            raise
        finally:
            close = getattr(backend, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException:
                    # Cleanup is secondary while another failure is already
                    # in flight; never replace its measured-operation chain.
                    if primary_failure is None:
                        raise
        if progress_callback is not None:
            progress_callback(
                {
                    "cell_id": cell_id,
                    "completed_repetition_count": len(repetition_rows),
                    "completed_operation_sample_count": len(all_samples),
                }
            )

    return {
        "schema": MATRIX_SCHEMA,
        "cell_id": cell_id,
        "graph": graph.metadata(),
        "concurrency": concurrency,
        "repetition_count": repetitions,
        "operations_per_type": operations_per_type,
        "operation_mix": list(OPERATION_TYPES),
        "run_id_sha256": _hash_text(run_id),
        "samples": all_samples,
        "repetitions": repetition_rows,
        "formal_requested": bool(formal),
        "formal_eligible": all(row["eligible_for_formal"] for row in repetition_rows),
    }


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ProvenancePerformanceError("cannot compute a percentile without samples")
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _report_sequence(source: Any) -> list[Mapping[str, Any]]:
    if isinstance(source, Mapping):
        reports = [source]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        reports = list(source)
    else:
        raise ValueError("aggregate input must be a report or report sequence")
    if not reports or any(not isinstance(report, Mapping) for report in reports):
        raise ValueError("each aggregate input must be a report mapping")
    return reports


def _flatten_reports(source: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reports = _report_sequence(source)
    samples: list[dict[str, Any]] = []
    repetitions: list[dict[str, Any]] = []
    for report in reports:
        raw_samples = report.get("samples")
        raw_repetitions = report.get("repetitions")
        if not isinstance(raw_samples, list) or not isinstance(raw_repetitions, list):
            raise ValueError("report must include sample and repetition lists")
        samples.extend(copy.deepcopy(raw_samples))
        repetitions.extend(copy.deepcopy(raw_repetitions))
    return samples, repetitions


def _exact_int_field(
    row: Mapping[str, Any],
    field: str,
    *,
    minimum: int = 0,
    expected: int | None = None,
) -> int:
    value = row.get(field)
    if type(value) is not int or value < minimum:
        raise ProvenancePerformanceError(f"{field} must be an exact integer")
    if expected is not None and value != expected:
        raise ProvenancePerformanceError(f"{field} mismatch")
    return value


def _validate_preload_accounting(
    row: Mapping[str, Any],
    *,
    graph: GraphSpec | None = None,
    require_layered: bool = False,
) -> None:
    method = row.get("preload_method")
    if method not in {
        "layered-canonical-write-v1",
        "sequential-semantic-v1",
    }:
        raise ProvenancePerformanceError("preload method is not registered")
    worker_limit = _exact_int_field(
        row, "preload_worker_limit", minimum=1
    )
    if worker_limit > PROVENANCE_PRELOAD_WORKERS:
        raise ProvenancePerformanceError(
            "preload worker limit exceeds the registered maximum"
        )
    observed_peak = _exact_int_field(
        row, "preload_observed_peak_concurrency", minimum=1
    )
    if observed_peak > worker_limit:
        raise ProvenancePerformanceError(
            "preload peak exceeds the worker limit"
        )
    repair_limit = _exact_int_field(
        row, "setup_repair_limit_per_record", minimum=0
    )
    repair_count = _exact_int_field(
        row, "setup_repair_count", minimum=0
    )
    node_count = (
        graph.node_count
        if graph is not None
        else _exact_int_field(row, "graph_node_count", minimum=1)
    )
    if repair_count > node_count * repair_limit:
        raise ProvenancePerformanceError(
            "setup repair count exceeds the registered budget"
        )
    _exact_int_field(row, "setup_elapsed_ns", minimum=1)
    if row.get("retry_scope") != PRELOAD_RETRY_SCOPE:
        raise ProvenancePerformanceError("retry scope is invalid")

    if method == "sequential-semantic-v1":
        if require_layered:
            raise ProvenancePerformanceError(
                "formal preload must use the layered canonical method"
            )
        if (
            worker_limit != 1
            or observed_peak != 1
            or repair_limit != 0
            or repair_count != 0
        ):
            raise ProvenancePerformanceError(
                "sequential preload accounting is inconsistent"
            )
        return

    if repair_limit != PROVENANCE_PRELOAD_REPAIR_LIMIT_PER_RECORD:
        raise ProvenancePerformanceError(
            "layered preload repair limit is inconsistent"
        )
    if graph is None:
        return
    layer_widths = Counter(graph.layers)
    expected_worker_limit = min(
        PROVENANCE_PRELOAD_WORKERS,
        max(layer_widths.values(), default=1),
    )
    if worker_limit != expected_worker_limit:
        raise ProvenancePerformanceError(
            "layered preload worker limit does not match the graph"
        )
    if expected_worker_limit > 1 and observed_peak < 2:
        raise ProvenancePerformanceError(
            "layered preload did not demonstrate parallel execution"
        )


def _formal_environment_valid(environment: Any) -> bool:
    if not isinstance(environment, Mapping):
        return False
    if set(environment) != _ENVIRONMENT_FIELDS | {"attestation_sha256"}:
        return False
    attestation_hash = environment.get("attestation_sha256")
    if not isinstance(attestation_hash, str) or not _SHA256.fullmatch(attestation_hash):
        return False
    raw = {key: environment[key] for key in _ENVIRONMENT_FIELDS}
    try:
        expected = _safe_environment(raw)
        return _canonical_json_bytes(dict(environment)) == _canonical_json_bytes(expected)
    except (ProvenancePerformanceError, ValueError):
        return False


def _formal_health_valid(health: Any) -> bool:
    if not isinstance(health, Mapping) or set(health) != {"qdrant", "neo4j"}:
        return False
    sanitized = _safe_health(health)
    try:
        return bool(
            _canonical_json_bytes(dict(health)) == _canonical_json_bytes(sanitized)
            and all(
                health[service].get("available") is True
                and isinstance(health[service].get("version"), str)
                for service in ("qdrant", "neo4j")
            )
        )
    except (AttributeError, ValueError):
        return False


def _validate_formal_reports(
    reports: Sequence[Mapping[str, Any]], expected_cells: Sequence[Mapping[str, Any]]
) -> None:
    if isinstance(expected_cells, (str, bytes)) or not isinstance(
        expected_cells, Sequence
    ):
        raise ProvenancePerformanceError("formal aggregate requires expected matrix cells")
    expected_by_id: dict[str, dict[str, int | str]] = {}
    for raw_cell in expected_cells:
        if not isinstance(raw_cell, Mapping):
            raise ProvenancePerformanceError("expected matrix cell must be a mapping")
        node_count = _exact_int_field(raw_cell, "graph_node_count", minimum=1)
        concurrency = _exact_int_field(raw_cell, "concurrency", minimum=1)
        repetitions = _exact_int_field(raw_cell, "repetitions", minimum=1)
        operations_per_type = _exact_int_field(
            raw_cell, "operations_per_type", minimum=1
        )
        graph_seed = _exact_int_field(raw_cell, "graph_seed", minimum=0)
        cell_id = raw_cell.get("cell_id")
        expected_id = f"n{node_count}-c{concurrency}"
        if cell_id != expected_id or cell_id in expected_by_id:
            raise ProvenancePerformanceError("invalid or duplicate expected matrix cell")
        expected_by_id[expected_id] = {
            "cell_id": expected_id,
            "graph_node_count": node_count,
            "concurrency": concurrency,
            "repetitions": repetitions,
            "operations_per_type": operations_per_type,
            "graph_seed": graph_seed,
        }
    if not expected_by_id:
        raise ProvenancePerformanceError("formal expected matrix must not be empty")

    report_by_id: dict[str, Mapping[str, Any]] = {}
    for report in reports:
        cell_id = report.get("cell_id")
        if not isinstance(cell_id, str) or cell_id in report_by_id:
            raise ProvenancePerformanceError("invalid or duplicate formal cell report")
        report_by_id[cell_id] = report
    if set(report_by_id) != set(expected_by_id):
        raise ProvenancePerformanceError("formal reports do not cover the expected matrix")

    report_fields = {
        "schema",
        "cell_id",
        "graph",
        "concurrency",
        "repetition_count",
        "operations_per_type",
        "operation_mix",
        "run_id_sha256",
        "samples",
        "repetitions",
        "formal_requested",
        "formal_eligible",
    }
    repetition_fields = {
        "cell_id",
        "repetition",
        "namespace_sha256",
        "graph_node_count",
        "graph_edge_count",
        "graph_sha256",
        "concurrency",
        "operation_count",
        "success_count",
        "failure_count",
        "retry_count",
        "backend_max_retries",
        "neo4j_driver_max_transaction_retry_time_ms",
        "observed_peak_concurrency",
        "elapsed_ns",
        "namespace_initially_empty",
        "preload_method",
        "preload_worker_limit",
        "preload_observed_peak_concurrency",
        "setup_repair_limit_per_record",
        "setup_repair_count",
        "setup_elapsed_ns",
        "retry_scope",
        "preload_closed",
        "state_closed",
        "state_classification",
        "eligible_for_formal",
        "eligible_for_diagnostic",
        "service_health",
        "environment",
        "preload_inventory",
        "final_inventory",
    }
    sample_fields = {
        "cell_id",
        "repetition",
        "namespace_sha256",
        "operation",
        "latency_ns",
        "success",
        "retry_count",
    }
    namespaces: set[str] = set()
    run_hashes: set[str] = set()
    for cell_id, expected in expected_by_id.items():
        report = report_by_id[cell_id]
        if set(report) != report_fields:
            raise ProvenancePerformanceError("formal cell report fields do not match schema")
        if report.get("schema") != MATRIX_SCHEMA:
            raise ProvenancePerformanceError("formal cell report schema mismatch")
        if report.get("formal_requested") is not True or report.get(
            "formal_eligible"
        ) is not True:
            raise ProvenancePerformanceError("diagnostic cell cannot be promoted to formal")
        graph = build_layered_dag(
            int(expected["graph_node_count"]), int(expected["graph_seed"])
        )
        if _canonical_json_bytes(report.get("graph")) != _canonical_json_bytes(
            graph.metadata()
        ):
            raise ProvenancePerformanceError("formal graph metadata mismatch")
        _exact_int_field(
            report, "concurrency", minimum=1, expected=int(expected["concurrency"])
        )
        _exact_int_field(
            report,
            "repetition_count",
            minimum=1,
            expected=int(expected["repetitions"]),
        )
        operations_per_type = _exact_int_field(
            report,
            "operations_per_type",
            minimum=1,
            expected=int(expected["operations_per_type"]),
        )
        if report.get("operation_mix") != list(OPERATION_TYPES):
            raise ProvenancePerformanceError("formal operation mix mismatch")
        run_hash = report.get("run_id_sha256")
        if not isinstance(run_hash, str) or not _SHA256.fullmatch(run_hash):
            raise ProvenancePerformanceError("formal run identity hash is invalid")
        run_hashes.add(run_hash)
        raw_repetitions = report.get("repetitions")
        raw_samples = report.get("samples")
        if not isinstance(raw_repetitions, list) or not isinstance(raw_samples, list):
            raise ProvenancePerformanceError("formal report evidence must be lists")
        if len(raw_repetitions) != int(expected["repetitions"]):
            raise ProvenancePerformanceError("formal repetition count mismatch")

        plan = _operation_plan(graph, operations_per_type)
        expected_nodes, expected_edges, expected_statuses = _expected_final_graph(
            graph, plan
        )
        expected_operation_count = len(OPERATION_TYPES) * operations_per_type
        repetitions_by_id: dict[int, Mapping[str, Any]] = {}
        for repetition_row in raw_repetitions:
            if not isinstance(repetition_row, Mapping) or set(
                repetition_row
            ) != repetition_fields:
                raise ProvenancePerformanceError(
                    "formal repetition fields do not match schema"
                )
            repetition = _exact_int_field(repetition_row, "repetition", minimum=0)
            if repetition in repetitions_by_id:
                raise ProvenancePerformanceError("duplicate formal repetition")
            repetitions_by_id[repetition] = repetition_row
        if set(repetitions_by_id) != set(range(int(expected["repetitions"]))):
            raise ProvenancePerformanceError("formal repetition identities are incomplete")

        samples_by_repetition: dict[int, list[Mapping[str, Any]]] = {
            repetition: [] for repetition in repetitions_by_id
        }
        for sample in raw_samples:
            if not isinstance(sample, Mapping) or set(sample) != sample_fields:
                raise ProvenancePerformanceError("formal sample fields do not match schema")
            repetition = _exact_int_field(sample, "repetition", minimum=0)
            if repetition not in samples_by_repetition:
                raise ProvenancePerformanceError("formal sample repetition is unknown")
            samples_by_repetition[repetition].append(sample)

        for repetition, repetition_row in repetitions_by_id.items():
            if repetition_row.get("cell_id") != cell_id:
                raise ProvenancePerformanceError("formal repetition cell mismatch")
            namespace_hash = repetition_row.get("namespace_sha256")
            if not isinstance(namespace_hash, str) or not _SHA256.fullmatch(
                namespace_hash
            ):
                raise ProvenancePerformanceError("invalid namespace identity hash")
            if namespace_hash in namespaces:
                raise ProvenancePerformanceError("duplicate namespace identity")
            namespaces.add(namespace_hash)
            _exact_int_field(
                repetition_row,
                "graph_node_count",
                minimum=1,
                expected=graph.node_count,
            )
            _exact_int_field(
                repetition_row,
                "graph_edge_count",
                minimum=0,
                expected=graph.edge_count,
            )
            if repetition_row.get("graph_sha256") != graph.graph_sha256:
                raise ProvenancePerformanceError("formal repetition graph hash mismatch")
            _exact_int_field(
                repetition_row,
                "concurrency",
                minimum=1,
                expected=int(expected["concurrency"]),
            )
            _exact_int_field(
                repetition_row,
                "operation_count",
                minimum=1,
                expected=expected_operation_count,
            )
            _exact_int_field(
                repetition_row,
                "success_count",
                minimum=0,
                expected=expected_operation_count,
            )
            _exact_int_field(repetition_row, "failure_count", expected=0)
            _exact_int_field(repetition_row, "retry_count", expected=0)
            _exact_int_field(repetition_row, "backend_max_retries", expected=0)
            _exact_int_field(
                repetition_row,
                "neo4j_driver_max_transaction_retry_time_ms",
                expected=0,
            )
            peak = _exact_int_field(
                repetition_row, "observed_peak_concurrency", minimum=1
            )
            if peak != int(expected["concurrency"]):
                raise ProvenancePerformanceError(
                    "observed concurrency does not match the requested level"
                )
            _exact_int_field(repetition_row, "elapsed_ns", minimum=1)
            _validate_preload_accounting(
                repetition_row,
                graph=graph,
                require_layered=True,
            )
            for field in (
                "namespace_initially_empty",
                "preload_closed",
                "state_closed",
                "eligible_for_formal",
                "eligible_for_diagnostic",
            ):
                if repetition_row.get(field) is not True:
                    raise ProvenancePerformanceError(f"formal repetition {field} is false")
            if repetition_row.get("state_classification") != "complete":
                raise ProvenancePerformanceError("formal state is not complete")
            if not _formal_health_valid(repetition_row.get("service_health")):
                raise ProvenancePerformanceError("formal service health is invalid")
            if not _formal_environment_valid(repetition_row.get("environment")):
                raise ProvenancePerformanceError("formal environment is invalid")
            preload_inventory = repetition_row.get("preload_inventory")
            final_inventory = repetition_row.get("final_inventory")
            if not isinstance(preload_inventory, Mapping) or not _inventory_matches(
                preload_inventory,
                graph.nodes,
                graph.edges,
                {"active": graph.node_count},
            ):
                raise ProvenancePerformanceError("formal preload inventory mismatch")
            if not isinstance(final_inventory, Mapping) or not _inventory_matches(
                final_inventory, expected_nodes, expected_edges, expected_statuses
            ):
                raise ProvenancePerformanceError("formal final inventory mismatch")

            repetition_samples = samples_by_repetition[repetition]
            if len(repetition_samples) != expected_operation_count:
                raise ProvenancePerformanceError("formal sample count mismatch")
            operation_counts: Counter[str] = Counter()
            for sample in repetition_samples:
                if sample.get("cell_id") != cell_id or sample.get(
                    "namespace_sha256"
                ) != namespace_hash:
                    raise ProvenancePerformanceError("formal sample identity mismatch")
                operation = sample.get("operation")
                if operation not in OPERATION_TYPES:
                    raise ProvenancePerformanceError("formal sample operation is invalid")
                operation_counts[str(operation)] += 1
                _exact_int_field(sample, "latency_ns", minimum=0)
                if sample.get("success") is not True:
                    raise ProvenancePerformanceError("formal sample failed")
                _exact_int_field(sample, "retry_count", expected=0)
            if operation_counts != Counter(
                {operation: operations_per_type for operation in OPERATION_TYPES}
            ):
                raise ProvenancePerformanceError("formal per-repetition operation mix mismatch")
    if len(run_hashes) != 1:
        raise ProvenancePerformanceError("formal cell reports disagree on run identity")


def _formal_observed_service_versions(
    reports: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    observed: dict[str, set[str]] = {
        "qdrant": set(),
        "neo4j": set(),
        "toxiproxy": set(),
    }
    for report in reports:
        for row in report.get("repetitions", []):
            if not isinstance(row, Mapping):
                raise ProvenancePerformanceError("formal repetition is malformed")
            health = row.get("service_health")
            environment = row.get("environment")
            if not isinstance(health, Mapping) or not isinstance(environment, Mapping):
                raise ProvenancePerformanceError("formal service evidence is missing")
            for service in ("qdrant", "neo4j"):
                service_health = health.get(service)
                if not isinstance(service_health, Mapping) or not isinstance(
                    service_health.get("version"), str
                ):
                    raise ProvenancePerformanceError("formal service version is missing")
                observed[service].add(str(service_health["version"]))
            toxiproxy_version = environment.get("toxiproxy_version")
            if not isinstance(toxiproxy_version, str):
                raise ProvenancePerformanceError("formal Toxiproxy version is missing")
            observed["toxiproxy"].add(toxiproxy_version)
    if any(len(versions) != 1 for versions in observed.values()):
        raise ProvenancePerformanceError("formal service versions drifted during the run")
    return {service: next(iter(versions)) for service, versions in observed.items()}


def _bootstrap_throughput(
    repetitions: Sequence[Mapping[str, Any]], count: int, seed: int, cell_id: str
) -> dict[str, float]:
    estimate_successes = sum(int(row["success_count"]) for row in repetitions)
    estimate_elapsed = sum(int(row["elapsed_ns"]) for row in repetitions)
    if estimate_elapsed <= 0:
        raise ProvenancePerformanceError("repetition elapsed time must be positive")
    estimate = estimate_successes * 1_000_000_000.0 / estimate_elapsed
    derived_seed = int.from_bytes(
        hashlib.sha256(f"{seed}\0{cell_id}".encode("utf-8")).digest()[:8], "big"
    )
    generator = random.Random(derived_seed)
    draws: list[float] = []
    for _ in range(count):
        selected = [repetitions[generator.randrange(len(repetitions))] for _ in repetitions]
        successes = sum(int(row["success_count"]) for row in selected)
        elapsed = sum(int(row["elapsed_ns"]) for row in selected)
        draws.append(successes * 1_000_000_000.0 / elapsed)
    return {
        "estimate": estimate,
        "lower": _percentile(draws, 0.025),
        "upper": _percentile(draws, 0.975),
    }


_ABLATION_COMMON_OPERATIONS = frozenset({"read", "write", "derive", "propagate"})
_ABLATION_OPERATIONS = _ABLATION_COMMON_OPERATIONS | {"traverse"}
_ABLATION_ERROR_CATEGORIES = frozenset(
    {"backend_error", "correctness_error", "isolation_error"}
)


def _bootstrap_ablation_metric(
    values: Sequence[Any],
    *,
    count: int,
    seed: int,
    identity: str,
    statistic: Callable[[Sequence[Any]], float],
) -> dict[str, float]:
    estimate = statistic(values)
    derived_seed = int.from_bytes(
        hashlib.sha256(f"{seed}\0{identity}".encode("utf-8")).digest()[:8], "big"
    )
    generator = random.Random(derived_seed)
    draws = [
        statistic([values[generator.randrange(len(values))] for _ in values])
        for _ in range(count)
    ]
    return {
        "estimate": estimate,
        "lower": _percentile(draws, 0.025),
        "upper": _percentile(draws, 0.975),
    }


def aggregate_ablation(
    evidence: Mapping[str, Any],
    *,
    bootstrap_repetitions: int = 10_000,
    seed: int = 1701,
    require_formal_contract: bool = False,
) -> dict[str, Any]:
    """Aggregate a two-variant ablation using repetition-level uncertainty."""

    bootstrap_repetitions = _strict_positive_integer(
        bootstrap_repetitions, "bootstrap_repetitions"
    )
    seed = _strict_integer(seed, "seed")
    if type(require_formal_contract) is not bool:
        raise ProvenancePerformanceError("require_formal_contract must be boolean")
    if not isinstance(evidence, Mapping) or set(evidence) != {
        "samples",
        "repetitions",
    }:
        raise ProvenancePerformanceError("ablation evidence fields do not match schema")
    raw_samples = evidence.get("samples")
    raw_repetitions = evidence.get("repetitions")
    if not isinstance(raw_samples, list) or not isinstance(raw_repetitions, list):
        raise ProvenancePerformanceError("ablation evidence rows must be lists")
    if not raw_samples or not raw_repetitions:
        raise ProvenancePerformanceError("ablation evidence must not be empty")

    repetitions: dict[tuple[str, str, int, int], dict[str, Any]] = {}
    coordinates_by_cell: dict[str, set[tuple[int, int]]] = {}
    cell_metadata: dict[str, tuple[int, int]] = {}
    for raw in raw_repetitions:
        if not isinstance(raw, Mapping):
            raise ProvenancePerformanceError("ablation repetition is malformed")
        variant = raw.get("variant")
        if variant not in PROVENANCE_ABLATION_VARIANTS:
            raise ProvenancePerformanceError("ablation repetition variant is invalid")
        cell_id = raw.get("cell_id")
        if not isinstance(cell_id, str) or not cell_id:
            raise ProvenancePerformanceError("ablation repetition cell is invalid")
        repetition = _exact_int_field(raw, "repetition", minimum=0)
        repetition_seed = _exact_int_field(raw, "repetition_seed")
        for field, minimum in (
            ("graph_node_count", 1),
            ("concurrency", 1),
            ("elapsed_ns", 1),
            ("success_count", 0),
            ("failure_count", 0),
            ("timeout_count", 0),
            ("error_count", 0),
        ):
            _exact_int_field(raw, field, minimum=minimum)
        metadata = (int(raw["graph_node_count"]), int(raw["concurrency"]))
        if cell_id != f"n{metadata[0]}-c{metadata[1]}":
            raise ProvenancePerformanceError("ablation cell metadata is inconsistent")
        if cell_id in cell_metadata and cell_metadata[cell_id] != metadata:
            raise ProvenancePerformanceError("ablation cell metadata drifted")
        cell_metadata[cell_id] = metadata
        if type(raw.get("eligible_for_formal")) is not bool:
            raise ProvenancePerformanceError("ablation eligibility must be boolean")
        if int(raw["timeout_count"]) + int(raw["error_count"]) != int(
            raw["failure_count"]
        ):
            raise ProvenancePerformanceError("ablation repetition outcome is inconsistent")
        if raw["eligible_for_formal"] is True and any(
            int(raw[field]) != 0
            for field in ("failure_count", "timeout_count", "error_count")
        ):
            raise ProvenancePerformanceError(
                "eligible ablation repetition must contain no failures"
            )
        key = (cell_id, str(variant), repetition, repetition_seed)
        if key in repetitions:
            raise ProvenancePerformanceError("duplicate ablation repetition identity")
        repetitions[key] = dict(raw)
        coordinates_by_cell.setdefault(cell_id, set()).add(
            (repetition, repetition_seed)
        )

    samples_by_repetition: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {
        key: [] for key in repetitions
    }
    for raw in raw_samples:
        if not isinstance(raw, Mapping):
            raise ProvenancePerformanceError("ablation sample is malformed")
        variant = raw.get("variant")
        cell_id = raw.get("cell_id")
        repetition = raw.get("repetition")
        repetition_seed = raw.get("repetition_seed")
        if (
            variant not in PROVENANCE_ABLATION_VARIANTS
            or not isinstance(cell_id, str)
            or isinstance(repetition, bool)
            or not isinstance(repetition, int)
            or isinstance(repetition_seed, bool)
            or not isinstance(repetition_seed, int)
        ):
            raise ProvenancePerformanceError("ablation sample identity is invalid")
        key = (cell_id, str(variant), repetition, repetition_seed)
        if key not in repetitions:
            raise ProvenancePerformanceError("ablation sample has unknown repetition")
        operation = raw.get("operation")
        if operation not in _ABLATION_OPERATIONS:
            raise ProvenancePerformanceError("ablation sample operation is invalid")
        if variant == "MemoryOnly-NoProvenance" and operation == "traverse":
            raise ProvenancePerformanceError(
                "memory-only control must not contain traversal samples"
            )
        _exact_int_field(raw, "latency_ns", minimum=0)
        operation_id = raw.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            raise ProvenancePerformanceError("ablation operation identity is invalid")
        if type(raw.get("success")) is not bool or type(raw.get("timeout")) is not bool:
            raise ProvenancePerformanceError("ablation sample outcome is invalid")
        error_category = raw.get("error_category")
        if raw["success"] is True:
            if raw["timeout"] is not False or error_category is not None:
                raise ProvenancePerformanceError("ablation sample outcome is contradictory")
        elif raw["timeout"] is True:
            if error_category != "timeout":
                raise ProvenancePerformanceError("ablation timeout outcome is invalid")
        elif error_category not in _ABLATION_ERROR_CATEGORIES:
            raise ProvenancePerformanceError("ablation error category is invalid")
        samples_by_repetition[key].append(dict(raw))

    for key, repetition_row in repetitions.items():
        rows = samples_by_repetition[key]
        successes = sum(row["success"] is True for row in rows)
        failures = len(rows) - successes
        timeouts = sum(row["timeout"] is True for row in rows)
        errors = sum(
            row["success"] is False and row["timeout"] is False for row in rows
        )
        if (
            successes != repetition_row["success_count"]
            or failures != repetition_row["failure_count"]
            or timeouts != repetition_row["timeout_count"]
            or errors != repetition_row["error_count"]
        ):
            raise ProvenancePerformanceError(
                "ablation sample and repetition accounting disagree"
            )

    expected_formal_keys = {
        (
            f"n{cell['graph_node_count']}-c{cell['concurrency']}",
            variant,
            repetition,
            repetition_seed,
        )
        for cell in FORMAL_ABLATION_CONFIG["cells"]
        for variant in PROVENANCE_ABLATION_VARIANTS
        for repetition, repetition_seed in enumerate(
            FORMAL_ABLATION_CONFIG["repetition_seeds"]
        )
    }
    operations_per_type = int(FORMAL_ABLATION_CONFIG["operations_per_type"])
    formal_operation_contract_complete = True
    for key in expected_formal_keys.intersection(repetitions):
        variant = key[1]
        expected_operations = set(_ABLATION_COMMON_OPERATIONS)
        if variant == "TxnMem":
            expected_operations = expected_operations | {"traverse"}
        expected_identity = Counter(
            (operation, f"{operation}:{operation_index}")
            for operation in expected_operations
            for operation_index in range(operations_per_type)
        )
        observed_identity = Counter(
            (str(row["operation"]), str(row["operation_id"]))
            for row in samples_by_repetition[key]
        )
        if observed_identity != expected_identity:
            formal_operation_contract_complete = False
            break
    formal_contract_complete = (
        set(repetitions) == expected_formal_keys
        and len(repetitions) == 180
        and all(row["eligible_for_formal"] is True for row in repetitions.values())
        and formal_operation_contract_complete
    )
    if require_formal_contract and not formal_contract_complete:
        raise ProvenancePerformanceError(
            "formal ablation requires exactly 180 eligible repetitions"
        )

    variant_cells: list[dict[str, Any]] = []
    for cell_id in sorted(coordinates_by_cell):
        for variant in PROVENANCE_ABLATION_VARIANTS:
            cell_rows = sorted(
                (
                    row
                    for key, row in repetitions.items()
                    if key[0] == cell_id and key[1] == variant
                ),
                key=lambda row: (
                    int(row["repetition"]), int(row["repetition_seed"])
                ),
            )
            eligible = [row for row in cell_rows if row["eligible_for_formal"] is True]
            eligible_samples = [
                sample
                for key, rows in samples_by_repetition.items()
                if key[0] == cell_id
                and key[1] == variant
                and repetitions[key]["eligible_for_formal"] is True
                for sample in rows
            ]
            attempted_samples = [
                sample
                for key, rows in samples_by_repetition.items()
                if key[0] == cell_id and key[1] == variant
                for sample in rows
            ]
            successful = [row for row in eligible_samples if row["success"] is True]
            common = [
                row for row in successful if row["operation"] in _ABLATION_COMMON_OPERATIONS
            ]
            traversal = [row for row in successful if row["operation"] == "traverse"]
            throughput_interval = None
            wall_clock_interval = None
            if eligible and sum(int(row["success_count"]) for row in eligible) > 0:
                throughput_interval = _bootstrap_ablation_metric(
                    eligible,
                    count=bootstrap_repetitions,
                    seed=seed,
                    identity=f"variant-throughput\0{cell_id}\0{variant}",
                    statistic=lambda selected: sum(
                        int(row["success_count"]) for row in selected
                    )
                    * 1_000_000_000.0
                    / sum(int(row["elapsed_ns"]) for row in selected),
                )
            if eligible:
                wall_clock_interval = _bootstrap_ablation_metric(
                    eligible,
                    count=bootstrap_repetitions,
                    seed=seed,
                    identity=f"variant-wall-clock\0{cell_id}\0{variant}",
                    statistic=lambda selected: sum(
                        int(row["elapsed_ns"]) for row in selected
                    )
                    / len(selected),
                )
            operation_breakdown = []
            for operation in sorted(_ABLATION_OPERATIONS):
                operation_rows = [
                    row for row in eligible_samples if row["operation"] == operation
                ]
                operation_success = [
                    int(row["latency_ns"])
                    for row in operation_rows
                    if row["success"] is True
                ]
                if not operation_rows:
                    continue
                operation_breakdown.append(
                    {
                        "operation": operation,
                        "successful_count": len(operation_success),
                        "failed_count": len(operation_rows) - len(operation_success),
                        "latency_ns": {
                            "p50": _percentile(operation_success, 0.50),
                            "p95": _percentile(operation_success, 0.95),
                            "p99": _percentile(operation_success, 0.99),
                        }
                        if operation_success
                        else None,
                    }
                )
            first = cell_rows[0] if cell_rows else None
            variant_cells.append(
                {
                    "cell_id": cell_id,
                    "variant": variant,
                    "graph_node_count": int(first["graph_node_count"]) if first else None,
                    "concurrency": int(first["concurrency"]) if first else None,
                    "attempted_repetition_count": len(cell_rows),
                    "eligible_repetition_count": len(eligible),
                    "excluded_repetition_count": len(cell_rows) - len(eligible),
                    "successful_operation_count": len(successful),
                    "attempted_operation_count": len(attempted_samples),
                    "failed_operation_count": sum(
                        row["success"] is False for row in attempted_samples
                    ),
                    "timeout_count": sum(int(row["timeout_count"]) for row in cell_rows),
                    "error_count": sum(int(row["error_count"]) for row in cell_rows),
                    "error_category_counts": dict(
                        sorted(
                            Counter(
                                str(row["error_category"])
                                for row in attempted_samples
                                if row["success"] is False
                                and row["timeout"] is False
                            ).items()
                        )
                    ),
                    "common_operation_sample_count": len(common),
                    "common_latency_ns": {
                        "p50": _percentile([int(row["latency_ns"]) for row in common], 0.50),
                        "p95": _percentile([int(row["latency_ns"]) for row in common], 0.95),
                        "p99": _percentile([int(row["latency_ns"]) for row in common], 0.99),
                    }
                    if common
                    else None,
                    "traversal_sample_count": len(traversal),
                    "traversal_latency_ns": {
                        "p50": _percentile([int(row["latency_ns"]) for row in traversal], 0.50),
                        "p95": _percentile([int(row["latency_ns"]) for row in traversal], 0.95),
                        "p99": _percentile([int(row["latency_ns"]) for row in traversal], 0.99),
                    }
                    if traversal
                    else None,
                    "mechanism_package_wall_clock_ns": sum(
                        int(row["elapsed_ns"]) for row in eligible
                    ),
                    "mechanism_package_wall_clock_95ci_ns": wall_clock_interval,
                    "successful_throughput_ops_per_second": (
                        throughput_interval["estimate"]
                        if throughput_interval is not None
                        else None
                    ),
                    "successful_throughput_95ci": throughput_interval,
                    "operations": operation_breakdown,
                }
            )

    paired_cells: list[dict[str, Any]] = []
    for cell_id in sorted(coordinates_by_cell):
        pairs: list[tuple[int, int, float, float]] = []
        missing = ineligible = incomparable = zero_denominator = 0
        for repetition, repetition_seed in sorted(coordinates_by_cell[cell_id]):
            pair_rows = {
                variant: repetitions.get((cell_id, variant, repetition, repetition_seed))
                for variant in PROVENANCE_ABLATION_VARIANTS
            }
            if any(row is None for row in pair_rows.values()):
                missing += 1
                continue
            full = pair_rows["TxnMem"]
            control = pair_rows["MemoryOnly-NoProvenance"]
            assert full is not None and control is not None
            if not full["eligible_for_formal"] or not control["eligible_for_formal"]:
                ineligible += 1
                continue
            full_common_identity = Counter(
                (str(row["operation"]), str(row["operation_id"]))
                for row in samples_by_repetition[
                    (cell_id, "TxnMem", repetition, repetition_seed)
                ]
                if row["operation"] in _ABLATION_COMMON_OPERATIONS
            )
            control_common_identity = Counter(
                (str(row["operation"]), str(row["operation_id"]))
                for row in samples_by_repetition[
                    (cell_id, "MemoryOnly-NoProvenance", repetition, repetition_seed)
                ]
                if row["operation"] in _ABLATION_COMMON_OPERATIONS
            )
            if (
                not full_common_identity
                or full_common_identity != control_common_identity
                or {operation for operation, _operation_id in full_common_identity}
                != _ABLATION_COMMON_OPERATIONS
            ):
                incomparable += 1
                continue
            common_means: dict[str, float] = {}
            for variant in PROVENANCE_ABLATION_VARIANTS:
                key = (cell_id, variant, repetition, repetition_seed)
                values = [
                    int(row["latency_ns"])
                    for row in samples_by_repetition[key]
                    if row["success"] is True
                    and row["operation"] in _ABLATION_COMMON_OPERATIONS
                ]
                common_means[variant] = sum(values) / len(values) if values else 0.0
            control_throughput = (
                int(control["success_count"]) * 1_000_000_000.0
                / int(control["elapsed_ns"])
            )
            full_throughput = (
                int(full["success_count"]) * 1_000_000_000.0
                / int(full["elapsed_ns"])
            )
            if common_means["MemoryOnly-NoProvenance"] <= 0 or control_throughput <= 0:
                zero_denominator += 1
                continue
            pairs.append(
                (
                    repetition,
                    repetition_seed,
                    (
                        common_means["TxnMem"]
                        - common_means["MemoryOnly-NoProvenance"]
                    )
                    / common_means["MemoryOnly-NoProvenance"]
                    * 100.0,
                    (full_throughput - control_throughput)
                    / control_throughput
                    * 100.0,
                )
            )

        pairs.sort(key=lambda row: (row[0], row[1]))

        latency_interval = throughput_interval = None
        if pairs:
            latency_interval = _bootstrap_ablation_metric(
                pairs,
                count=bootstrap_repetitions,
                seed=seed,
                identity=f"pair-latency\0{cell_id}",
                statistic=lambda selected: sum(row[2] for row in selected)
                / len(selected),
            )
            throughput_interval = _bootstrap_ablation_metric(
                pairs,
                count=bootstrap_repetitions,
                seed=seed,
                identity=f"pair-throughput\0{cell_id}",
                statistic=lambda selected: sum(row[3] for row in selected)
                / len(selected),
            )
        paired_cells.append(
            {
                "cell_id": cell_id,
                "eligible_pair_count": len(pairs),
                "missing_pair_count": missing,
                "ineligible_pair_count": ineligible,
                "incomparable_pair_count": incomparable,
                "zero_denominator_pair_count": zero_denominator,
                "common_latency_overhead_pct": latency_interval,
                "throughput_change_pct": throughput_interval,
            }
        )

    return {
        "schema": "txnmem-provenance-ablation-aggregate-v1",
        "bootstrap_unit": "whole_repetition_pair",
        "bootstrap_repetitions": bootstrap_repetitions,
        "bootstrap_seed": seed,
        "common_operations": sorted(_ABLATION_COMMON_OPERATIONS),
        "traversal_reporting": "txnmem_absolute_only",
        "attempted_repetition_count": len(repetitions),
        "operation_sample_count": len(raw_samples),
        "timeout_count": sum(
            int(row["timeout_count"]) for row in repetitions.values()
        ),
        "error_count": sum(
            int(row["error_count"]) for row in repetitions.values()
        ),
        "eligible_repetition_count": sum(
            row["eligible_for_formal"] is True for row in repetitions.values()
        ),
        "excluded_repetition_count": sum(
            row["eligible_for_formal"] is not True for row in repetitions.values()
        ),
        "eligible_pair_count": sum(
            row["eligible_pair_count"] for row in paired_cells
        ),
        "formal_contract_complete": formal_contract_complete,
        "variant_cells": variant_cells,
        "paired_cells": paired_cells,
    }


def aggregate_matrix(
    samples: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    bootstrap_repetitions: int = 10_000,
    seed: int = 17,
    require_formal: bool = True,
    topology_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregate operation samples with whole-repetition throughput bootstrap."""

    bootstrap_repetitions = _strict_positive_integer(
        bootstrap_repetitions, "bootstrap_repetitions"
    )
    seed = _strict_integer(seed, "seed")
    if type(require_formal) is not bool:
        raise ProvenancePerformanceError("require_formal must be a boolean")
    reports = _report_sequence(samples)
    validated_topology: dict[str, Any] | None = None
    if require_formal:
        config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
        if (
            bootstrap_repetitions != config["bootstrap_repetitions"]
            or seed != config["bootstrap_seed"]
        ):
            raise ProvenancePerformanceError(
                "formal bootstrap settings must match the frozen matrix config"
            )
        expected_cells = expand_matrix(config)
        _validate_formal_reports(reports, expected_cells)
        run_hashes = {str(report.get("run_id_sha256")) for report in reports}
        environment_hashes = {
            str(row.get("environment", {}).get("attestation_sha256"))
            for report in reports
            for row in report.get("repetitions", [])
            if isinstance(row, Mapping)
            and isinstance(row.get("environment"), Mapping)
        }
        if len(run_hashes) != 1 or len(environment_hashes) != 1:
            raise ProvenancePerformanceError(
                "formal reports must bind one run and one environment attestation"
            )
        if not isinstance(topology_attestation, Mapping):
            raise ProvenancePerformanceError(
                "formal aggregate requires a registered completion attestation"
            )
        candidate_operation_rows, candidate_repetition_rows = _flatten_reports(
            reports
        )
        formal_run_hash = next(iter(run_hashes))
        candidate_bundle_id = provenance_bundle_id(
            config_sha256=formal_matrix_config_sha256(),
            run_id_sha256=formal_run_hash,
            formal=False,
            backend="vector-graph",
        )
        try:
            from txnmem_topology_attestation import (
                validate_registered_topology_attestation,
            )

            validated_topology = validate_registered_topology_attestation(
                topology_attestation,
                expected_run_id_sha256=formal_run_hash,
                expected_config_sha256=formal_matrix_config_sha256(),
                expected_config_file_sha256=formal_config_file_sha256(),
                expected_workload_sha256=formal_matrix_workload_sha256(),
                expected_environment_attestation_sha256=next(
                    iter(environment_hashes)
                ),
                expected_evidence_manifest_sha256=cell_reports_sha256(reports),
                expected_candidate_bundle_id=candidate_bundle_id,
                expected_candidate_operation_samples_sha256=canonical_jsonl_sha256(
                    candidate_operation_rows
                ),
                expected_candidate_repetitions_sha256=canonical_jsonl_sha256(
                    candidate_repetition_rows
                ),
            )
        except ValueError as exc:
            raise ProvenancePerformanceError(
                "formal topology completion attestation is invalid"
            ) from exc
        expected_repetitions = sum(int(cell["repetitions"]) for cell in expected_cells)
        expected_samples = sum(
            int(cell["repetitions"])
            * int(cell["operations_per_type"])
            * len(OPERATION_TYPES)
            for cell in expected_cells
        )
        if (
            validated_topology.get("matrix_cell_count") != len(expected_cells)
            or validated_topology.get("repetition_count") != expected_repetitions
            or validated_topology.get("operation_sample_count") != expected_samples
        ):
            raise ProvenancePerformanceError(
                "formal topology counts do not match the frozen matrix"
            )
        observed_versions = _formal_observed_service_versions(reports)
        topology_versions = {
            str(row.get("role")): row.get("service_version")
            for row in validated_topology.get("roles", [])
            if isinstance(row, Mapping)
        }
        if any(
            topology_versions.get(service) != version
            for service, version in observed_versions.items()
        ):
            raise ProvenancePerformanceError(
                "attested service versions do not match observed health evidence"
            )
    operation_rows, repetition_rows = _flatten_reports(samples)
    if not operation_rows or not repetition_rows:
        raise ProvenancePerformanceError("matrix aggregate requires non-empty evidence")
    seen_repetitions: set[tuple[str, int]] = set()
    repetitions_by_cell: dict[str, list[dict[str, Any]]] = {}
    for row in repetition_rows:
        cell_id = str(row.get("cell_id", ""))
        repetition = row.get("repetition")
        if not cell_id or isinstance(repetition, bool) or not isinstance(repetition, int):
            raise ProvenancePerformanceError("invalid repetition identity")
        if repetition < 0:
            raise ProvenancePerformanceError("repetition identity must be non-negative")
        for field, minimum in (
            ("elapsed_ns", 1),
            ("success_count", 0),
            ("failure_count", 0),
            ("retry_count", 0),
            ("graph_node_count", 1),
            ("concurrency", 1),
        ):
            _exact_int_field(row, field, minimum=minimum)
        _validate_preload_accounting(row)
        key = (cell_id, repetition)
        if key in seen_repetitions:
            raise ProvenancePerformanceError("duplicate repetition identity")
        seen_repetitions.add(key)
        eligible = (
            row.get("eligible_for_formal") is True
            if require_formal
            else row.get("eligible_for_diagnostic") is True
            or row.get("eligible_for_formal") is True
        )
        if not eligible:
            raise ProvenancePerformanceError(
                "ineligible repetition cannot enter the requested aggregate"
            )
        repetitions_by_cell.setdefault(cell_id, []).append(row)

    samples_by_cell: dict[str, list[dict[str, Any]]] = {}
    for row in operation_rows:
        cell_id = str(row.get("cell_id", ""))
        repetition = row.get("repetition")
        if (cell_id, repetition) not in seen_repetitions:
            raise ProvenancePerformanceError("sample references an unknown repetition")
        if row.get("operation") not in OPERATION_TYPES:
            raise ProvenancePerformanceError("sample has an unknown operation")
        if type(row.get("success")) is not bool:
            raise ProvenancePerformanceError("sample success must be a boolean")
        latency = row.get("latency_ns")
        if isinstance(latency, bool) or not isinstance(latency, int) or latency < 0:
            raise ProvenancePerformanceError("sample latency must be non-negative nanoseconds")
        _exact_int_field(row, "retry_count", minimum=0)
        if row.get("success") is False:
            error_class = row.get("error_class")
            if not isinstance(error_class, str) or not _SAFE_ERROR_CLASS.fullmatch(
                error_class
            ):
                raise ProvenancePerformanceError("failed sample error class is invalid")
        samples_by_cell.setdefault(cell_id, []).append(row)

    samples_by_repetition: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in operation_rows:
        samples_by_repetition.setdefault(
            (str(row["cell_id"]), int(row["repetition"])), []
        ).append(row)
    for repetition_row in repetition_rows:
        key = (str(repetition_row["cell_id"]), int(repetition_row["repetition"]))
        rows = samples_by_repetition.get(key, [])
        successes = sum(1 for row in rows if row["success"] is True)
        failures = len(rows) - successes
        retries = sum(int(row["retry_count"]) for row in rows)
        if (
            successes != repetition_row["success_count"]
            or failures != repetition_row["failure_count"]
            or retries != repetition_row["retry_count"]
        ):
            raise ProvenancePerformanceError(
                "sample and repetition accounting disagree within a repetition"
            )

    rows: list[dict[str, Any]] = []
    for cell_id in sorted(repetitions_by_cell):
        cell_repetitions = sorted(
            repetitions_by_cell[cell_id], key=lambda row: int(row["repetition"])
        )
        cell_samples = samples_by_cell.get(cell_id, [])
        successful = [row for row in cell_samples if row.get("success") is True]
        failed = [row for row in cell_samples if row.get("success") is not True]
        if not successful:
            raise ProvenancePerformanceError("cell has no successful operation samples")
        expected_successes = sum(int(row["success_count"]) for row in cell_repetitions)
        expected_failures = sum(int(row["failure_count"]) for row in cell_repetitions)
        if len(successful) != expected_successes or len(failed) != expected_failures:
            raise ProvenancePerformanceError("sample and repetition accounting disagree")
        latencies = [int(row["latency_ns"]) for row in successful]
        interval = _bootstrap_throughput(
            cell_repetitions, bootstrap_repetitions, seed, cell_id
        )
        operation_breakdown: list[dict[str, Any]] = []
        for operation in OPERATION_TYPES:
            operation_samples = [
                row for row in cell_samples if row["operation"] == operation
            ]
            operation_success = [
                int(row["latency_ns"])
                for row in operation_samples
                if row.get("success") is True
            ]
            operation_breakdown.append(
                {
                    "operation": operation,
                    "successful_count": len(operation_success),
                    "failed_count": len(operation_samples) - len(operation_success),
                    "p50_latency_ns": _percentile(operation_success, 0.50)
                    if operation_success
                    else None,
                    "p95_latency_ns": _percentile(operation_success, 0.95)
                    if operation_success
                    else None,
                    "p99_latency_ns": _percentile(operation_success, 0.99)
                    if operation_success
                    else None,
                }
            )
        error_counts: dict[str, int] = {}
        for row in failed:
            error = str(row.get("error_class", "BackendError"))
            error_counts[error] = error_counts.get(error, 0) + 1
        first = cell_repetitions[0]
        retry_scopes = {
            str(row.get("retry_scope", "unspecified"))
            for row in cell_repetitions
        }
        rows.append(
            {
                "cell_id": cell_id,
                "graph_node_count": int(first["graph_node_count"]),
                "concurrency": int(first["concurrency"]),
                "repetition_count": len(cell_repetitions),
                "successful_operation_count": len(successful),
                "failed_operation_count": len(failed),
                "retry_count": sum(int(row["retry_count"]) for row in cell_repetitions),
                "retry_scope": (
                    next(iter(retry_scopes))
                    if len(retry_scopes) == 1
                    else "mixed"
                ),
                "setup_repair_count": sum(
                    int(row.get("setup_repair_count", 0))
                    for row in cell_repetitions
                ),
                "setup_elapsed_ns": sum(
                    int(row.get("setup_elapsed_ns", 0))
                    for row in cell_repetitions
                ),
                "p50_latency_ns": _percentile(latencies, 0.50),
                "p95_latency_ns": _percentile(latencies, 0.95),
                "p99_latency_ns": _percentile(latencies, 0.99),
                "successful_throughput_ops_per_second": interval["estimate"],
                "successful_throughput_95ci": interval,
                "error_class_counts": dict(sorted(error_counts.items())),
                "operations": operation_breakdown,
            }
        )
    result = {
        "schema": MATRIX_SCHEMA,
        "bootstrap_unit": "whole_repetition",
        "bootstrap_repetitions": bootstrap_repetitions,
        "bootstrap_seed": seed,
        "throughput_numerator": "successful_operations_only",
        "latency_population": "successful_operations_only",
        "evidence_scope": "formal" if require_formal else "diagnostic",
        "rows": rows,
    }
    if validated_topology is not None:
        result.update(
            {
                "config_sha256": formal_matrix_config_sha256(),
                "workload_sha256": formal_matrix_workload_sha256(),
                "evidence_manifest_sha256": cell_reports_sha256(reports),
                "topology_attestation_sha256": validated_topology[
                    "attestation_sha256"
                ],
            }
        )
    return result


class _Neo4jHealthClient:
    """Read-only Neo4j health boundary with no graph schema initialization."""

    def __init__(
        self,
        uri: str,
        auth: Sequence[str],
        *,
        request_timeout_seconds: float,
    ) -> None:
        try:
            from neo4j import GraphDatabase, Query
        except ImportError as exc:  # pragma: no cover - exercised on remote host
            raise RuntimeError("neo4j driver is required for health attestation") from exc
        self.request_timeout_seconds = request_timeout_seconds
        self.max_transaction_retry_time_seconds = 0.0
        self._Query = Query
        self.driver = GraphDatabase.driver(
            str(uri),
            auth=tuple(auth),
            max_transaction_retry_time=0.0,
            connection_timeout=request_timeout_seconds,
            connection_acquisition_timeout=request_timeout_seconds,
            notifications_min_severity="OFF",
        )

    def healthcheck(self) -> dict[str, Any]:
        query = self._Query(
            "CALL dbms.components() YIELD versions "
            "RETURN 1 AS ok, versions[0] AS version",
            timeout=self.request_timeout_seconds,
        )
        with self.driver.session() as session:
            record = session.run(query).single()
        return {
            "available": bool(record and record.get("ok") == 1),
            "version": record.get("version") if record else None,
        }

    def close(self) -> None:
        self.driver.close()


class _MemoryOnlyNoProvenanceBackend(InstrumentedMemoryBackend):
    """Persist memory objects in Qdrant without any provenance projection."""

    performance_variant = "MemoryOnly-NoProvenance"
    provenance_enabled = False
    max_retries = 0

    def __init__(
        self,
        namespace: str,
        *,
        qdrant_client: Any,
        neo4j_health_client: Any,
        embedder: Callable[[Any], list[float]],
    ) -> None:
        super().__init__()
        self.db_namespace = str(namespace)
        self.qdrant = qdrant_client
        self.neo4j_health = neo4j_health_client
        self.embedder = embedder
        self.neo4j_max_transaction_retry_time_seconds = getattr(
            neo4j_health_client,
            "max_transaction_retry_time_seconds",
            None,
        )
        self._state_lock = threading.RLock()
        self._event_lock = threading.Lock()
        self._event_context = threading.local()
        self._identity_locks_guard = threading.Lock()
        self._identity_locks: dict[str, threading.Lock] = {}
        self._metrics_lock = threading.Lock()
        self._metrics: dict[str, Any] = {
            "request_count": 0,
            "retry_count": 0,
            "error_count": 0,
            "operation_counts": {},
            "timing_ms": {},
        }

    def _event(self, kind: str, **fields: Any) -> dict[str, Any]:
        with self._event_lock:
            event = super()._event(kind, **fields)
            self._event_context.last_write_event = event
            return event

    def _clear_thread_write_event(self) -> None:
        self._event_context.last_write_event = None

    def _rewrite_thread_write_event(self, kind: str, **fields: Any) -> None:
        with self._event_lock:
            event = getattr(self._event_context, "last_write_event", None)
            if event is None:
                return
            event["kind"] = str(kind)
            event.update(copy.deepcopy(fields))

    def _identity_lock(self, memory_id: str) -> threading.Lock:
        with self._identity_locks_guard:
            return self._identity_locks.setdefault(
                str(memory_id),
                threading.Lock(),
            )

    def _key(self, operation: str, memory_id: str) -> str:
        return hashlib.sha256(
            json.dumps(
                [self.db_namespace, str(operation), str(memory_id)],
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _call(self, operation: str, function: Callable[[], Any]) -> Any:
        started = time.perf_counter()
        with self._metrics_lock:
            self._metrics["request_count"] += 1
            counts = self._metrics["operation_counts"]
            counts[operation] = counts.get(operation, 0) + 1
        try:
            return function()
        except Exception:
            with self._metrics_lock:
                self._metrics["error_count"] += 1
            raise
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            with self._metrics_lock:
                self._metrics["timing_ms"].setdefault(operation, []).append(
                    elapsed_ms
                )

    def _retrieve(self, memory_id: str, *, operation: str) -> dict[str, Any] | None:
        row = self._call(
            operation,
            lambda: self.qdrant.retrieve(self.db_namespace, str(memory_id)),
        )
        return copy.deepcopy(dict(row)) if isinstance(row, Mapping) else None

    def write(
        self, memory_id: str, value: Any = None, **fields: Any
    ) -> dict[str, Any]:
        self._clear_thread_write_event()
        memory_id = str(memory_id)
        with self._identity_lock(memory_id):
            previous = self._retrieve(memory_id, operation="memory_version_read")
            memory = {
                "memory_id": memory_id,
                "value": value if value is not None else memory_id,
                "status": "active",
                "agent_id": fields.get("agent_id", "agent_1"),
                "scope": fields.get("scope", "tenant:user_001"),
                "version": int(previous.get("version", 0)) + 1
                if previous is not None
                else 1,
            }
            key = self._key("write", memory_id)
            self._call(
                "memory_write",
                lambda: self.qdrant.upsert(
                    self.db_namespace,
                    memory_id,
                    self.embedder(memory["value"]),
                    memory,
                    key,
                ),
            )
            with self._state_lock:
                self.memories[memory_id] = copy.deepcopy(memory)
            self._event("memory_write", memory_id=memory_id, value=memory["value"])
            return copy.deepcopy(memory)

    def read(
        self, memory_id: str | None = None, **fields: Any
    ) -> dict[str, Any] | None:
        row = (
            self._retrieve(str(memory_id), operation="memory_read")
            if memory_id is not None
            else None
        )
        self._event("memory_read", memory_id=memory_id, **fields)
        if row is None or row.get("status") != "active":
            return None
        with self._state_lock:
            self.memories[str(memory_id)] = copy.deepcopy(row)
        return row

    def search(self, query: str | None = None, **fields: Any) -> list[dict[str, Any]]:
        rows = self._call(
            "memory_search",
            lambda: self.qdrant.search(
                self.db_namespace,
                self.embedder(query),
                1000,
            ),
        )
        matches = [
            copy.deepcopy(dict(row))
            for row in rows
            if isinstance(row, Mapping) and row.get("status") == "active"
        ]
        with self._state_lock:
            for row in matches:
                if row.get("memory_id") is not None:
                    self.memories[str(row["memory_id"])] = copy.deepcopy(row)
        self._event("memory_search", query=query, **fields)
        return matches

    def derive(
        self,
        memory_id: str,
        source_ids: Iterable[str],
        value: Any = None,
        **fields: Any,
    ) -> dict[str, Any]:
        canonical_sources = [str(source_id) for source_id in source_ids]
        for source_id in canonical_sources:
            source = self._retrieve(source_id, operation="derive_source_read")
            if source is None or source.get("status") != "active":
                raise KeyError("derive source is missing")
        memory = self.write(memory_id, value=value, **fields)
        self._rewrite_thread_write_event(
            "memory_derive",
            source_ids=canonical_sources,
        )
        return memory

    def propagate(
        self,
        memory_id: str,
        source_id: str,
        value: Any = None,
        **fields: Any,
    ) -> dict[str, Any]:
        memory = self.derive(memory_id, [source_id], value=value, **fields)
        self._rewrite_thread_write_event(
            "memory_propagate",
            source_id=str(source_id),
        )
        return memory

    def invalidate(self, memory_id: str, **fields: Any) -> None:
        memory_id = str(memory_id)
        with self._identity_lock(memory_id):
            memory = self._retrieve(memory_id, operation="invalidate_read")
            if memory is not None:
                memory["status"] = "invalid"
                memory["version"] = int(memory.get("version", 1)) + 1
                key = self._key("invalidate", memory_id)
                self._call(
                    "memory_invalidate",
                    lambda: self.qdrant.upsert(
                        self.db_namespace,
                        memory_id,
                        self.embedder(memory.get("value")),
                        memory,
                        key,
                    ),
                )
                with self._state_lock:
                    self.memories[memory_id] = copy.deepcopy(memory)
        self._event("invalidate", memory_id=memory_id, **fields)

    def performance_inventory(self, limit: int = 1000) -> dict[str, Any]:
        if type(limit) is not int or limit <= 0:
            raise ValueError("memory inventory limit must be a positive integer")
        scan = getattr(self.qdrant, "scan_namespace", None)
        if not callable(scan):
            return {
                "classification": "unknown",
                "node_count": None,
                "edge_count": None,
                "graph_sha256": None,
                "status_counts": {},
            }
        try:
            result = self._call(
                "memory_inventory",
                lambda: scan(self.db_namespace, limit=limit),
            )
        except Exception:
            result = None
        rows = result.get("rows") if isinstance(result, Mapping) else None
        if (
            not isinstance(result, Mapping)
            or result.get("read_ok") is not True
            or not isinstance(rows, list)
        ):
            return {
                "classification": "unknown",
                "node_count": None,
                "edge_count": None,
                "graph_sha256": None,
                "status_counts": {},
            }
        forbidden = {"derived_from", "source_ids", "supersedes_id"}
        if any(
            not isinstance(row, Mapping)
            or row.get("memory_id") is None
            or forbidden.intersection(row)
            for row in rows
        ):
            return {
                "classification": "unknown",
                "node_count": None,
                "edge_count": None,
                "graph_sha256": None,
                "status_counts": {},
            }
        nodes = sorted(str(row["memory_id"]) for row in rows)
        if len(nodes) != len(set(nodes)):
            return {
                "classification": "unknown",
                "node_count": None,
                "edge_count": None,
                "graph_sha256": None,
                "status_counts": {},
            }
        statuses = [str(row.get("status", "unknown")) for row in rows]
        return {
            "classification": "complete",
            "node_count": len(nodes),
            "edge_count": 0,
            "graph_sha256": canonical_graph_sha256(nodes, ()),
            "status_counts": {
                status: statuses.count(status) for status in sorted(set(statuses))
            },
        }

    def healthcheck(self) -> dict[str, Any]:
        return {
            "namespace": self.db_namespace,
            "qdrant": self.qdrant.healthcheck(),
            "neo4j": self.neo4j_health.healthcheck(),
        }

    def metrics(self) -> dict[str, Any]:
        with self._metrics_lock:
            return copy.deepcopy(self._metrics)

    def close(self) -> None:
        return None


class _ReusableVectorGraphBackendFactory:
    def __init__(
        self,
        *,
        qdrant_url: str,
        neo4j_uri: str,
        neo4j_auth: Sequence[str],
        environment_attestation: Mapping[str, Any],
        request_timeout_seconds: float,
    ):
        from txnmem_vector_graph_backend import (
            _Neo4jBoltClient,
            _QdrantHTTPClient,
        )

        self.qdrant_url = str(qdrant_url)
        self.neo4j_uri = str(neo4j_uri)
        self.neo4j_auth = tuple(str(item) for item in neo4j_auth)
        self.request_timeout_seconds = request_timeout_seconds
        self.attestation = copy.deepcopy(dict(environment_attestation))
        self.qdrant = _QdrantHTTPClient(
            self.qdrant_url,
            timeout_seconds=self.request_timeout_seconds,
        )
        # Benign schema-token notices must not add logging I/O to measured
        # operation latency.  Non-performance clients keep the driver default.
        self.neo4j = _Neo4jBoltClient(
            self.neo4j_uri,
            self.neo4j_auth,
            notifications_min_severity="OFF",
            migrate_legacy=False,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        self._closed = False
        self._close_lock = threading.Lock()

    def __call__(self, namespace: str) -> Any:
        from txnmem_vector_graph_backend import VectorGraphMemoryBackend

        with self._close_lock:
            if self._closed:
                raise RuntimeError("vector graph backend factory is closed")
            backend = VectorGraphMemoryBackend(
                namespace,
                self.qdrant_url,
                self.neo4j_uri,
                self.neo4j_auth,
                qdrant_client=self.qdrant,
                neo4j_client=self.neo4j,
                max_retries=0,
                request_timeout_seconds=self.request_timeout_seconds,
                close_clients=False,
            )
            backend.performance_environment = lambda: copy.deepcopy(
                self.attestation
            )
            return backend

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            close = getattr(self.neo4j, "close", None)
            if callable(close):
                close()
            self._closed = True


class _ReusableMemoryOnlyBackendFactory:
    def __init__(
        self,
        *,
        qdrant_url: str,
        neo4j_uri: str,
        neo4j_auth: Sequence[str],
        environment_attestation: Mapping[str, Any],
        request_timeout_seconds: float,
    ) -> None:
        from txnmem_vector_graph_backend import _QdrantHTTPClient, _embedding

        self.attestation = copy.deepcopy(dict(environment_attestation))
        self.qdrant = _QdrantHTTPClient(
            str(qdrant_url),
            timeout_seconds=request_timeout_seconds,
        )
        self.neo4j = _Neo4jHealthClient(
            str(neo4j_uri),
            tuple(str(item) for item in neo4j_auth),
            request_timeout_seconds=request_timeout_seconds,
        )
        self.embedder = _embedding
        self._closed = False
        self._close_lock = threading.Lock()

    def __call__(self, namespace: str) -> _MemoryOnlyNoProvenanceBackend:
        with self._close_lock:
            if self._closed:
                raise RuntimeError("memory-only backend factory is closed")
            backend = _MemoryOnlyNoProvenanceBackend(
                namespace,
                qdrant_client=self.qdrant,
                neo4j_health_client=self.neo4j,
                embedder=self.embedder,
            )
            backend.performance_environment = lambda: copy.deepcopy(
                self.attestation
            )
            return backend

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            close = getattr(self.neo4j, "close", None)
            if callable(close):
                close()
            self._closed = True


def make_vector_graph_backend_factory(
    *,
    qdrant_url: str,
    neo4j_uri: str,
    neo4j_auth: Sequence[str],
    environment_attestation: Mapping[str, Any],
    request_timeout_seconds: float = 30.0,
) -> _ReusableVectorGraphBackendFactory:
    """Create a zero-retry real-backend factory for unbiased operation timing."""

    if (
        type(request_timeout_seconds) not in (int, float)
        or not math.isfinite(request_timeout_seconds)
        or request_timeout_seconds <= 0.0
    ):
        raise ValueError("request_timeout_seconds must be a positive finite number")
    validate_environment_attestation(environment_attestation)
    return _ReusableVectorGraphBackendFactory(
        qdrant_url=qdrant_url,
        neo4j_uri=neo4j_uri,
        neo4j_auth=neo4j_auth,
        environment_attestation=environment_attestation,
        request_timeout_seconds=request_timeout_seconds,
    )


def make_provenance_ablation_backend_factory(
    *,
    variant: str,
    qdrant_url: str,
    neo4j_uri: str,
    neo4j_auth: Sequence[str],
    environment_attestation: Mapping[str, Any],
    request_timeout_seconds: float = 30.0,
) -> _ReusableVectorGraphBackendFactory | _ReusableMemoryOnlyBackendFactory:
    """Build exactly one registered ablation backend without fallback."""

    if type(variant) is not str or variant not in PROVENANCE_ABLATION_VARIANTS:
        raise ValueError("provenance ablation variant is not registered")
    if variant == "TxnMem":
        return make_vector_graph_backend_factory(
            qdrant_url=qdrant_url,
            neo4j_uri=neo4j_uri,
            neo4j_auth=neo4j_auth,
            environment_attestation=environment_attestation,
            request_timeout_seconds=request_timeout_seconds,
        )
    if (
        type(request_timeout_seconds) not in (int, float)
        or not math.isfinite(request_timeout_seconds)
        or request_timeout_seconds <= 0.0
    ):
        raise ValueError("request_timeout_seconds must be a positive finite number")
    validate_environment_attestation(environment_attestation)
    return _ReusableMemoryOnlyBackendFactory(
        qdrant_url=qdrant_url,
        neo4j_uri=neo4j_uri,
        neo4j_auth=neo4j_auth,
        environment_attestation=environment_attestation,
        request_timeout_seconds=request_timeout_seconds,
    )


def provenance_bundle_id(
    *, config_sha256: str, run_id_sha256: str, formal: bool, backend: str
) -> str:
    if not isinstance(config_sha256, str) or not _SHA256.fullmatch(config_sha256):
        raise ValueError("invalid config hash")
    if not isinstance(run_id_sha256, str) or not _SHA256.fullmatch(run_id_sha256):
        raise ValueError("invalid run hash")
    if type(formal) is not bool:
        raise ValueError("formal must be a boolean")
    if backend not in {"memory", "vector-graph"}:
        raise ValueError("invalid provenance backend")
    scope = "formal" if formal else "diagnostic"
    backend_name = backend.replace("-", "_")
    return f"{scope}-{backend_name}-{config_sha256[:16]}-{run_id_sha256[:16]}"


def _parse_provenance_bundle_id(bundle_id: str) -> tuple[str, str]:
    match = re.fullmatch(
        r"(formal|diagnostic)-(memory|vector_graph)-[0-9a-f]{16}-[0-9a-f]{16}",
        bundle_id if isinstance(bundle_id, str) else "",
    )
    if match is None:
        raise ProvenancePerformanceError("invalid provenance bundle identifier")
    return match.group(1), match.group(2).replace("_", "-")


def preflight_provenance_output(out_dir: str | Path, bundle_id: str):
    """Reject ambiguous legacy paths or an already-published condition."""

    from txnmem_formal_io import FormalIOError, FormalStore

    if not isinstance(bundle_id, str) or not re.fullmatch(
        r"[a-z0-9_]+-[a-z0-9_]+-[0-9a-f]{16}-[0-9a-f]{16}", bundle_id
    ):
        raise FormalIOError("invalid provenance bundle identity")
    store = FormalStore(out_dir)
    for legacy in ("data", "results"):
        if store.entry_kind(legacy) != "missing":
            raise FormalIOError(
                f"legacy provenance output path is ambiguous: {legacy}"
            )
    for container in ("bundles", "bundle_objects"):
        kind = store.entry_kind(container)
        if kind not in {"missing", "directory"}:
            raise FormalIOError(f"provenance output container is unsafe: {container}")
    if store.entry_kind("bundles", f"{bundle_id}.json") != "missing":
        raise FormalIOError("refusing to overwrite an existing provenance bundle")
    return store


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    try:
        return b"".join(
            _canonical_json_bytes(dict(row)) + b"\n" for row in rows
        )
    except (TypeError, ValueError) as exc:
        raise ProvenancePerformanceError("JSONL evidence is not canonical JSON") from exc


def canonical_jsonl_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_jsonl_bytes(rows)).hexdigest()


def _reconstruct_formal_cell_reports(
    operation_samples: Sequence[Mapping[str, Any]],
    repetitions: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
    cells = expand_matrix(config)
    run_hash = report.get("run_id_sha256")
    reports: list[dict[str, Any]] = []
    for cell in cells:
        cell_id = str(cell["cell_id"])
        graph = build_layered_dag(
            int(cell["graph_node_count"]), int(cell["graph_seed"])
        )
        cell_samples = [
            copy.deepcopy(dict(row))
            for row in operation_samples
            if isinstance(row, Mapping) and row.get("cell_id") == cell_id
        ]
        cell_repetitions = [
            copy.deepcopy(dict(row))
            for row in repetitions
            if isinstance(row, Mapping) and row.get("cell_id") == cell_id
        ]
        reports.append(
            {
                "schema": MATRIX_SCHEMA,
                "cell_id": cell_id,
                "graph": graph.metadata(),
                "concurrency": int(cell["concurrency"]),
                "repetition_count": int(cell["repetitions"]),
                "operations_per_type": int(cell["operations_per_type"]),
                "operation_mix": list(OPERATION_TYPES),
                "run_id_sha256": run_hash,
                "samples": cell_samples,
                "repetitions": cell_repetitions,
                "formal_requested": True,
                "formal_eligible": all(
                    row.get("eligible_for_formal") is True
                    for row in cell_repetitions
                ),
            }
        )
    return reports


def _validate_formal_bundle(
    *,
    bundle_id: str,
    operation_samples: Sequence[Mapping[str, Any]],
    repetitions: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    topology_attestation: Mapping[str, Any] | None,
) -> None:
    fields = {
        "schema",
        "backend",
        "formal_requested",
        "bundle_id",
        "publication_status",
        "production_backend_claim",
        "config",
        "config_sha256",
        "config_file_sha256",
        "run_id_sha256",
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
        "operation_samples_sha256",
        "repetitions_sha256",
        "graphs",
        "aggregate",
        "topology_attestation_sha256",
    }
    if set(report) != fields:
        raise ProvenancePerformanceError("formal publication report fields mismatch")
    if (
        report.get("schema") != "txnmem-provenance-performance-report-v1"
        or report.get("backend") != "vector-graph"
        or report.get("formal_requested") is not True
        or report.get("production_backend_claim") is not True
        or report.get("publication_status") != "complete"
        or report.get("bundle_id") != bundle_id
    ):
        raise ProvenancePerformanceError("formal publication identity is invalid")
    config_hash = formal_matrix_config_sha256()
    run_hash = report.get("run_id_sha256")
    if (
        _canonical_json_bytes(report.get("config"))
        != _canonical_json_bytes(FORMAL_MATRIX_CONFIG)
        or
        report.get("config_sha256") != config_hash
        or report.get("config_file_sha256") != formal_config_file_sha256()
        or not isinstance(run_hash, str)
        or not _SHA256.fullmatch(run_hash)
        or provenance_bundle_id(
            config_sha256=config_hash,
            run_id_sha256=run_hash,
            formal=True,
            backend="vector-graph",
        )
        != bundle_id
    ):
        raise ProvenancePerformanceError("formal publication config/run binding mismatch")
    config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
    cells = expand_matrix(config)
    expected_repetitions = sum(int(cell["repetitions"]) for cell in cells)
    expected_samples = sum(
        int(cell["repetitions"])
        * int(cell["operations_per_type"])
        * len(OPERATION_TYPES)
        for cell in cells
    )
    if (
        type(report.get("matrix_cell_count")) is not int
        or report.get("matrix_cell_count") != len(cells)
        or type(report.get("repetition_count")) is not int
        or report.get("repetition_count") != expected_repetitions
        or len(repetitions) != expected_repetitions
        or type(report.get("operation_sample_count")) is not int
        or report.get("operation_sample_count") != expected_samples
        or len(operation_samples) != expected_samples
    ):
        raise ProvenancePerformanceError("formal publication matrix counts mismatch")
    if (
        report.get("operation_samples_sha256")
        != canonical_jsonl_sha256(operation_samples)
        or report.get("repetitions_sha256") != canonical_jsonl_sha256(repetitions)
    ):
        raise ProvenancePerformanceError("formal publication data hash mismatch")
    expected_graphs = [
        build_layered_dag(
            int(cell["graph_node_count"]), int(cell["graph_seed"])
        ).metadata()
        for cell in cells
    ]
    if _canonical_json_bytes(report.get("graphs")) != _canonical_json_bytes(
        expected_graphs
    ):
        raise ProvenancePerformanceError("formal publication graph list mismatch")
    cell_reports = _reconstruct_formal_cell_reports(
        operation_samples, repetitions, report
    )
    recomputed = aggregate_matrix(
        cell_reports,
        bootstrap_repetitions=int(config["bootstrap_repetitions"]),
        seed=int(config["bootstrap_seed"]),
        require_formal=True,
        topology_attestation=topology_attestation,
    )
    if _canonical_json_bytes(report.get("aggregate")) != _canonical_json_bytes(
        recomputed
    ):
        raise ProvenancePerformanceError("formal publication aggregate mismatch")
    if not isinstance(topology_attestation, Mapping) or report.get(
        "topology_attestation_sha256"
    ) != topology_attestation.get("attestation_sha256"):
        raise ProvenancePerformanceError("formal publication topology binding mismatch")


def _validate_diagnostic_bundle(
    *,
    bundle_id: str,
    operation_samples: Sequence[Mapping[str, Any]],
    repetitions: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
) -> None:
    fields = {
        "schema",
        "backend",
        "formal_requested",
        "bundle_id",
        "publication_status",
        "production_backend_claim",
        "config",
        "config_sha256",
        "config_file_sha256",
        "run_id_sha256",
        "matrix_cell_count",
        "repetition_count",
        "operation_sample_count",
        "operation_samples_sha256",
        "repetitions_sha256",
        "graphs",
        "aggregate",
        "topology_attestation_sha256",
    }
    scope, backend_from_id = _parse_provenance_bundle_id(bundle_id)
    backend = report.get("backend")
    if (
        set(report) != fields
        or scope != "diagnostic"
        or backend not in {"memory", "vector-graph"}
        or backend != backend_from_id
        or report.get("schema") != "txnmem-provenance-performance-report-v1"
        or report.get("formal_requested") is not False
        or report.get("production_backend_claim") is not False
        or report.get("bundle_id") != bundle_id
        or report.get("publication_status") != "complete"
        or report.get("topology_attestation_sha256") is not None
    ):
        raise ProvenancePerformanceError("diagnostic publication identity mismatch")
    config_raw = report.get("config")
    if not isinstance(config_raw, Mapping):
        raise ProvenancePerformanceError("diagnostic publication config is missing")
    config = validate_matrix_config(config_raw, formal=False)
    config_hash = hashlib.sha256(_canonical_json_bytes(config)).hexdigest()
    run_hash = report.get("run_id_sha256")
    if (
        report.get("config_sha256") != config_hash
        or not isinstance(report.get("config_file_sha256"), str)
        or not _SHA256.fullmatch(str(report.get("config_file_sha256")))
        or not isinstance(run_hash, str)
        or not _SHA256.fullmatch(run_hash)
        or provenance_bundle_id(
            config_sha256=config_hash,
            run_id_sha256=run_hash,
            formal=False,
            backend=str(backend),
        )
        != bundle_id
    ):
        raise ProvenancePerformanceError("diagnostic config/run binding mismatch")
    cells = expand_matrix(config)
    expected_repetitions = sum(int(cell["repetitions"]) for cell in cells)
    expected_samples = sum(
        int(cell["repetitions"])
        * int(cell["operations_per_type"])
        * len(OPERATION_TYPES)
        for cell in cells
    )
    if (
        not cells
        or type(report.get("matrix_cell_count")) is not int
        or report.get("matrix_cell_count") != len(cells)
        or type(report.get("repetition_count")) is not int
        or report.get("repetition_count") != expected_repetitions
        or len(repetitions) != expected_repetitions
        or type(report.get("operation_sample_count")) is not int
        or report.get("operation_sample_count") != expected_samples
        or len(operation_samples) != expected_samples
        or expected_repetitions <= 0
        or expected_samples <= 0
    ):
        raise ProvenancePerformanceError("diagnostic matrix counts mismatch")
    if (
        report.get("operation_samples_sha256")
        != canonical_jsonl_sha256(operation_samples)
        or report.get("repetitions_sha256") != canonical_jsonl_sha256(repetitions)
    ):
        raise ProvenancePerformanceError("diagnostic publication data hash mismatch")
    expected_graphs = [
        build_layered_dag(
            int(cell["graph_node_count"]), int(cell["graph_seed"])
        ).metadata()
        for cell in cells
    ]
    if _canonical_json_bytes(report.get("graphs")) != _canonical_json_bytes(
        expected_graphs
    ):
        raise ProvenancePerformanceError("diagnostic publication graph list mismatch")
    recomputed = aggregate_matrix(
        {"samples": list(operation_samples), "repetitions": list(repetitions)},
        bootstrap_repetitions=int(config["bootstrap_repetitions"]),
        seed=int(config["bootstrap_seed"]),
        require_formal=False,
    )
    if (
        not isinstance(report.get("aggregate"), Mapping)
        or report["aggregate"].get("evidence_scope") != "diagnostic"
        or _canonical_json_bytes(report["aggregate"])
        != _canonical_json_bytes(recomputed)
    ):
        raise ProvenancePerformanceError("diagnostic aggregate mismatch")


def publish_provenance_bundle(
    out_dir: str | Path,
    *,
    bundle_id: str,
    operation_samples: Sequence[Mapping[str, Any]],
    repetitions: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    topology_attestation: Mapping[str, Any] | None = None,
    _precommit_check: Callable[[], None] | None = None,
    _private_publication_mode: _PrivatePublicationMode | None = None,
) -> Path:
    """Write an immutable object, then atomically publish one exclusive pointer."""

    from txnmem_formal_io import FormalIOError

    if _precommit_check is not None and not callable(_precommit_check):
        raise TypeError("private publication precommit check must be callable")
    private_publication_mode = None
    if _private_publication_mode is not None:
        private_publication_mode = _require_private_publication_mode(
            _private_publication_mode
        )
    sample_bytes = _jsonl_bytes(operation_samples)
    repetition_bytes = _jsonl_bytes(repetitions)
    report_payload = copy.deepcopy(dict(report))
    if report_payload.get("bundle_id") != bundle_id:
        raise FormalIOError("report bundle identity mismatch")
    if report_payload.get("operation_samples_sha256") != hashlib.sha256(
        sample_bytes
    ).hexdigest() or report_payload.get("repetitions_sha256") != hashlib.sha256(
        repetition_bytes
    ).hexdigest():
        raise FormalIOError("report data hashes do not match staged evidence")
    scope, backend_from_id = _parse_provenance_bundle_id(bundle_id)
    if report_payload.get("backend") != backend_from_id:
        raise ProvenancePerformanceError("bundle backend does not match report")
    if scope == "formal":
        _validate_formal_bundle(
            bundle_id=bundle_id,
            operation_samples=operation_samples,
            repetitions=repetitions,
            report=report_payload,
            topology_attestation=topology_attestation,
        )
    elif scope == "diagnostic":
        _validate_diagnostic_bundle(
            bundle_id=bundle_id,
            operation_samples=operation_samples,
            repetitions=repetitions,
            report=report_payload,
        )
    else:  # pragma: no cover - parser is exhaustive
        raise ProvenancePerformanceError("unknown bundle scope")
    if (
        private_publication_mode is not None
        and scope != "diagnostic"
    ):
        raise ProvenancePerformanceError(
            "private publication mode requires a diagnostic bundle"
        )

    store = preflight_provenance_output(out_dir, bundle_id)
    object_id = f"object-{secrets.token_hex(16)}"
    store.ensure_directory("bundles")
    store.ensure_directory("bundle_objects")
    store.create_directory_exclusive("bundle_objects", object_id)
    store.ensure_directory("bundle_objects", object_id, "data")
    store.ensure_directory("bundle_objects", object_id, "results")
    topology_payload: dict[str, Any] | None = None
    topology_canonical_hash: str | None = None
    if scope == "formal":
        if not isinstance(topology_attestation, Mapping):
            raise ProvenancePerformanceError(
                "formal publication requires topology evidence"
            )
        topology_payload = copy.deepcopy(dict(topology_attestation))
        topology_canonical_hash = hashlib.sha256(
            _canonical_json_bytes(topology_payload)
        ).hexdigest()
        store.ensure_directory("bundle_objects", object_id, "evidence")
    with store.open_text_exclusive(
        "bundle_objects", object_id, "data", "provenance_operation_samples.jsonl"
    ) as stream:
        stream.write(sample_bytes.decode("utf-8"))
    with store.open_text_exclusive(
        "bundle_objects", object_id, "data", "provenance_repetitions.jsonl"
    ) as stream:
        stream.write(repetition_bytes.decode("utf-8"))
    store.write_json_exclusive(
        "bundle_objects",
        object_id,
        "results",
        "provenance_performance.json",
        payload=report_payload,
    )
    if topology_payload is not None:
        store.write_json_exclusive(
            "bundle_objects",
            object_id,
            "evidence",
            "topology_attestation.json",
            payload=topology_payload,
        )
    completion = {
        "schema": "txnmem-provenance-performance-bundle-v1",
        "bundle_id": bundle_id,
        "operation_samples_sha256": hashlib.sha256(sample_bytes).hexdigest(),
        "repetitions_sha256": hashlib.sha256(repetition_bytes).hexdigest(),
        "report_canonical_sha256": hashlib.sha256(
            _canonical_json_bytes(report_payload)
        ).hexdigest(),
        "topology_attestation_sha256": (
            topology_payload.get("attestation_sha256")
            if topology_payload is not None
            else None
        ),
        "topology_attestation_canonical_sha256": topology_canonical_hash,
        "publication_status": "complete",
    }
    store.write_json_exclusive(
        "bundle_objects", object_id, "COMPLETED.json", payload=completion
    )
    pointer = {
        "schema": "txnmem-provenance-performance-pointer-v1",
        "bundle_id": bundle_id,
        "object_id": object_id,
        "report_path": f"bundle_objects/{object_id}/results/provenance_performance.json",
        "completion_sha256": hashlib.sha256(
            _canonical_json_bytes(completion)
        ).hexdigest(),
        "publication_status": "complete",
    }
    store._publish_json_exclusive(
        "bundles",
        f"{bundle_id}.json",
        payload=pointer,
        _precommit_check=_precommit_check,
        _allow_named_fallback=(
            scope != "formal" and private_publication_mode is None
        ),
    )
    return store.path(
        "bundle_objects", object_id, "results", "provenance_performance.json"
    )


def _decode_canonical_jsonl(raw: bytes) -> list[dict[str, Any]]:
    if not raw:
        raise ProvenancePerformanceError("candidate JSONL must not be empty")
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise ProvenancePerformanceError("candidate JSONL is not UTF-8") from exc
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        try:
            value = json.loads(
                line,
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json,
                parse_float=_finite_json_float,
            )
        except (ValueError, json.JSONDecodeError) as exc:
            raise ProvenancePerformanceError("candidate JSONL is malformed") from exc
        if not isinstance(value, dict):
            raise ProvenancePerformanceError("candidate JSONL row must be a mapping")
        rows.append(value)
    if _jsonl_bytes(rows) != raw:
        raise ProvenancePerformanceError("candidate JSONL is not canonical")
    return rows


def _load_provenance_candidate(
    candidate_root: str | Path, bundle_id: str
) -> dict[str, Any]:
    from txnmem_formal_io import FormalStore

    scope, _backend = _parse_provenance_bundle_id(bundle_id)
    if scope != "diagnostic":
        raise ProvenancePerformanceError("promotion source must be diagnostic")
    root = Path(candidate_root).expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise ProvenancePerformanceError("candidate root must be a real directory")
    store = FormalStore(root)
    pointer = store.load_json("bundles", f"{bundle_id}.json")
    pointer_fields = {
        "schema",
        "bundle_id",
        "object_id",
        "report_path",
        "completion_sha256",
        "publication_status",
    }
    if not isinstance(pointer, Mapping) or set(pointer) != pointer_fields:
        raise ProvenancePerformanceError("candidate pointer schema mismatch")
    object_id = pointer.get("object_id")
    expected_report_path = (
        f"bundle_objects/{object_id}/results/provenance_performance.json"
    )
    if (
        pointer.get("schema") != "txnmem-provenance-performance-pointer-v1"
        or pointer.get("bundle_id") != bundle_id
        or pointer.get("publication_status") != "complete"
        or not isinstance(object_id, str)
        or not re.fullmatch(r"object-[0-9a-f]{32}", object_id)
        or pointer.get("report_path") != expected_report_path
        or not isinstance(pointer.get("completion_sha256"), str)
        or not _SHA256.fullmatch(str(pointer.get("completion_sha256")))
    ):
        raise ProvenancePerformanceError("candidate pointer identity mismatch")
    completion = store.load_json("bundle_objects", object_id, "COMPLETED.json")
    completion_fields = {
        "schema",
        "bundle_id",
        "operation_samples_sha256",
        "repetitions_sha256",
        "report_canonical_sha256",
        "topology_attestation_sha256",
        "topology_attestation_canonical_sha256",
        "publication_status",
    }
    if (
        not isinstance(completion, Mapping)
        or set(completion) != completion_fields
        or completion.get("schema") != "txnmem-provenance-performance-bundle-v1"
        or completion.get("bundle_id") != bundle_id
        or completion.get("publication_status") != "complete"
        or completion.get("topology_attestation_sha256") is not None
        or completion.get("topology_attestation_canonical_sha256") is not None
        or hashlib.sha256(_canonical_json_bytes(dict(completion))).hexdigest()
        != pointer.get("completion_sha256")
    ):
        raise ProvenancePerformanceError("candidate completion marker mismatch")
    report = store.load_json(
        "bundle_objects", object_id, "results", "provenance_performance.json"
    )
    sample_raw = store.load_bytes(
        "bundle_objects", object_id, "data", "provenance_operation_samples.jsonl"
    )
    repetition_raw = store.load_bytes(
        "bundle_objects", object_id, "data", "provenance_repetitions.jsonl"
    )
    samples = _decode_canonical_jsonl(sample_raw)
    repetitions = _decode_canonical_jsonl(repetition_raw)
    if (
        completion.get("operation_samples_sha256")
        != hashlib.sha256(sample_raw).hexdigest()
        or completion.get("repetitions_sha256")
        != hashlib.sha256(repetition_raw).hexdigest()
        or completion.get("report_canonical_sha256")
        != hashlib.sha256(_canonical_json_bytes(report)).hexdigest()
    ):
        raise ProvenancePerformanceError("candidate object hash mismatch")
    if not isinstance(report, Mapping):
        raise ProvenancePerformanceError("candidate report must be a mapping")
    _validate_diagnostic_bundle(
        bundle_id=bundle_id,
        operation_samples=samples,
        repetitions=repetitions,
        report=report,
    )
    return {
        "report": copy.deepcopy(dict(report)),
        "operation_samples": samples,
        "repetitions": repetitions,
        "sample_bytes_sha256": hashlib.sha256(sample_raw).hexdigest(),
        "repetition_bytes_sha256": hashlib.sha256(repetition_raw).hexdigest(),
    }


def _candidate_formal_reports(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    report = candidate.get("report")
    samples = candidate.get("operation_samples")
    repetitions = candidate.get("repetitions")
    if (
        not isinstance(report, Mapping)
        or not isinstance(samples, list)
        or not isinstance(repetitions, list)
    ):
        raise ProvenancePerformanceError("candidate evidence is incomplete")
    if (
        report.get("backend") != "vector-graph"
        or _canonical_json_bytes(report.get("config"))
        != _canonical_json_bytes(FORMAL_MATRIX_CONFIG)
        or report.get("config_sha256") != formal_matrix_config_sha256()
        or report.get("config_file_sha256") != formal_config_file_sha256()
    ):
        raise ProvenancePerformanceError(
            "candidate is not bound to the frozen real-backend config"
        )
    return _reconstruct_formal_cell_reports(samples, repetitions, report)


def candidate_attestation_material(
    candidate_root: str | Path, bundle_id: str
) -> dict[str, Any]:
    """Return only sanitized hashes/counts needed by the out-of-tree collector."""

    candidate = _load_provenance_candidate(candidate_root, bundle_id)
    reports = _candidate_formal_reports(candidate)
    _validate_formal_reports(reports, expand_matrix(FORMAL_MATRIX_CONFIG))
    observed_versions = _formal_observed_service_versions(reports)
    environment_hashes = {
        str(row["environment"]["attestation_sha256"])
        for report in reports
        for row in report["repetitions"]
    }
    if len(environment_hashes) != 1:
        raise ProvenancePerformanceError("candidate environment attestation drifted")
    config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
    cells = expand_matrix(config)
    return {
        "schema": "txnmem-provenance-candidate-attestation-material-v1",
        "candidate_bundle_id": bundle_id,
        "run_id_sha256": reports[0]["run_id_sha256"],
        "config_sha256": formal_matrix_config_sha256(),
        "config_file_sha256": formal_config_file_sha256(),
        "workload_sha256": formal_matrix_workload_sha256(),
        "environment_attestation_sha256": next(iter(environment_hashes)),
        "evidence_manifest_sha256": cell_reports_sha256(reports),
        "matrix_cell_count": len(cells),
        "repetition_count": sum(int(cell["repetitions"]) for cell in cells),
        "operation_sample_count": sum(
            int(cell["repetitions"])
            * int(cell["operations_per_type"])
            * len(OPERATION_TYPES)
            for cell in cells
        ),
        "observed_service_versions": observed_versions,
        "candidate_operation_samples_sha256": candidate["sample_bytes_sha256"],
        "candidate_repetitions_sha256": candidate["repetition_bytes_sha256"],
    }


def _observe_sealed_candidate_tree(candidate_root: str | Path) -> dict[str, Any]:
    """Recompute the immutable candidate tree identity used by promotion."""

    root = Path(candidate_root).expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise ProvenancePerformanceError("sealed candidate root is invalid")
    root = root.resolve(strict=True)
    rows: list[dict[str, Any]] = []
    directory_count = 0
    file_count = 0
    for path in [root, *sorted(root.rglob("*"), key=lambda item: item.as_posix())]:
        metadata = path.lstat()
        relative = "." if path == root else path.relative_to(root).as_posix()
        if stat.S_ISLNK(metadata.st_mode):
            raise ProvenancePerformanceError("sealed candidate contains a link")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o500:
                raise ProvenancePerformanceError(
                    "sealed candidate directory mode changed"
                )
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
            if stat.S_IMODE(metadata.st_mode) != 0o400 or metadata.st_nlink != 1:
                raise ProvenancePerformanceError(
                    "sealed candidate file identity changed"
                )
            try:
                descriptor = os.open(
                    path,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                )
                with os.fdopen(descriptor, "rb", closefd=True) as stream:
                    payload = stream.read()
            except OSError as exc:
                raise ProvenancePerformanceError(
                    "sealed candidate file is unreadable"
                ) from exc
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
            raise ProvenancePerformanceError(
                "sealed candidate contains a special file"
            )
    root_metadata = root.stat()
    return {
        "root_device_sha256": hashlib.sha256(
            str(int(root_metadata.st_dev)).encode("utf-8")
        ).hexdigest(),
        "root_inode_sha256": hashlib.sha256(
            str(int(root_metadata.st_ino)).encode("utf-8")
        ).hexdigest(),
        "directory_count": directory_count,
        "file_count": file_count,
        "tree_sha256": hashlib.sha256(_canonical_json_bytes(rows)).hexdigest(),
    }


def _require_candidate_seal_matches(
    candidate_root: str | Path, topology_attestation: Mapping[str, Any]
) -> None:
    if not isinstance(topology_attestation, Mapping):
        raise ProvenancePerformanceError("formal topology omits candidate seal")
    seal = topology_attestation.get("candidate_seal")
    if not isinstance(seal, Mapping):
        raise ProvenancePerformanceError("formal topology omits candidate seal")
    observed = _observe_sealed_candidate_tree(candidate_root)
    if any(seal.get(field) != value for field, value in observed.items()):
        raise ProvenancePerformanceError(
            "candidate tree no longer matches completion attestation"
        )


def promote_provenance_candidate(
    candidate_root: str | Path,
    bundle_id: str,
    *,
    topology_attestation: Mapping[str, Any],
    out_dir: str | Path,
) -> Path:
    """Promote the exact immutable candidate bytes without rerunning measurement."""

    _require_candidate_seal_matches(candidate_root, topology_attestation)
    candidate = _load_provenance_candidate(candidate_root, bundle_id)
    reports = _candidate_formal_reports(candidate)
    config = validate_matrix_config(FORMAL_MATRIX_CONFIG, formal=True)
    aggregate = aggregate_matrix(
        reports,
        bootstrap_repetitions=int(config["bootstrap_repetitions"]),
        seed=int(config["bootstrap_seed"]),
        require_formal=True,
        topology_attestation=topology_attestation,
    )
    source_report = candidate["report"]
    operation_samples = candidate["operation_samples"]
    repetitions = candidate["repetitions"]
    run_hash = str(source_report["run_id_sha256"])
    config_hash = formal_matrix_config_sha256()
    formal_id = provenance_bundle_id(
        config_sha256=config_hash,
        run_id_sha256=run_hash,
        formal=True,
        backend="vector-graph",
    )
    cells = expand_matrix(config)
    formal_report = {
        "schema": "txnmem-provenance-performance-report-v1",
        "backend": "vector-graph",
        "formal_requested": True,
        "bundle_id": formal_id,
        "publication_status": "complete",
        "production_backend_claim": True,
        "config": copy.deepcopy(config),
        "config_sha256": config_hash,
        "config_file_sha256": formal_config_file_sha256(),
        "run_id_sha256": run_hash,
        "matrix_cell_count": len(cells),
        "repetition_count": len(repetitions),
        "operation_sample_count": len(operation_samples),
        "operation_samples_sha256": canonical_jsonl_sha256(operation_samples),
        "repetitions_sha256": canonical_jsonl_sha256(repetitions),
        "graphs": [report["graph"] for report in reports],
        "aggregate": aggregate,
        "topology_attestation_sha256": topology_attestation.get(
            "attestation_sha256"
        ),
    }
    return publish_provenance_bundle(
        out_dir,
        bundle_id=formal_id,
        operation_samples=operation_samples,
        repetitions=repetitions,
        report=formal_report,
        topology_attestation=topology_attestation,
    )


def write_provenance_blocked_report(
    out_dir: str | Path, payload: Mapping[str, Any]
) -> Path:
    """Write a redacted blocked marker without following or overwriting paths."""

    from txnmem_formal_io import FormalStore

    store = FormalStore(out_dir)
    store.write_json_exclusive(
        "results", "provenance_performance_blocked.json", payload=dict(payload)
    )
    return store.path("results", "provenance_performance_blocked.json")
