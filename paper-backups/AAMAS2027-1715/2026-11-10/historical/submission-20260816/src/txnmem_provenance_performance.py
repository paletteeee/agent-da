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
import hashlib
import json
import math
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from txnmem_backend import InstrumentedMemoryBackend


GRAPH_SCHEMA = "txnmem-provenance-dag-v1"
MATRIX_SCHEMA = "txnmem-provenance-performance-v1"
OPERATION_TYPES = ("read", "search", "derive", "invalidate_repair")
_SAFE_ERROR_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class ProvenancePerformanceError(RuntimeError):
    """Raised when evidence cannot satisfy the formal performance boundary."""


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
        result[service] = {
            "available": value.get("available") is True,
            "version": None if version is None else str(version)[:128],
        }
    return result


def _safe_environment(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {
            "isolation_verified": False,
            "co_tenant_load_detected": None,
            "source": "unavailable",
        }
    source = str(raw.get("source", "unspecified"))[:128]
    return {
        "isolation_verified": raw.get("isolation_verified") is True,
        "co_tenant_load_detected": raw.get("co_tenant_load_detected"),
        "source": source,
        "attestation_sha256": hashlib.sha256(
            json.dumps(dict(raw), sort_keys=True, default=str, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest(),
    }


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
    return {
        "classification": classification,
        "node_count": raw.get("node_count"),
        "edge_count": raw.get("edge_count"),
        "graph_sha256": raw.get("graph_sha256"),
        "status_counts": {
            str(key): int(value)
            for key, value in sorted(status_counts.items())
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0
        },
    }


def _inventory_matches(
    inventory: Mapping[str, Any],
    nodes: Sequence[str],
    edges: Sequence[Sequence[str]],
    status_counts: Mapping[str, int],
) -> bool:
    return bool(
        inventory.get("classification") == "complete"
        and inventory.get("node_count") == len(nodes)
        and inventory.get("edge_count") == len(edges)
        and inventory.get("graph_sha256") == canonical_graph_sha256(nodes, edges)
        and dict(inventory.get("status_counts", {})) == dict(status_counts)
    )


def _namespace(run_id: str, graph: GraphSpec, concurrency: int, repetition: int) -> str:
    digest = hashlib.sha256(
        f"{run_id}\0{graph.graph_sha256}\0{concurrency}\0{repetition}".encode("utf-8")
    ).hexdigest()
    return f"txnmem-prov-{digest[:24]}"


def _preload_graph(backend: Any, graph: GraphSpec) -> None:
    parents: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for source, target in graph.edges:
        parents[target].append(source)
    for node in graph.nodes:
        source_ids = sorted(parents[node])
        value = f"provenance:{node}"
        if source_ids:
            backend.derive(node, source_ids, value=value)
        else:
            backend.write(node, value=value)


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
    environment_attestation: Mapping[str, Any]
    | Callable[[Any], Mapping[str, Any]]
    | None = None,
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
            raise ProvenancePerformanceError("namespace collision within matrix cell")
        namespace_hashes.add(namespace_sha256)
        backend = backend_factory(namespace)
        try:
            health_provider = getattr(backend, "healthcheck", None)
            health = _safe_health(health_provider() if callable(health_provider) else None)
            services_available = all(
                health.get(service, {}).get("available") is True
                for service in ("qdrant", "neo4j")
            )
            if callable(environment_attestation):
                raw_environment = environment_attestation(backend)
            elif environment_attestation is not None:
                raw_environment = environment_attestation
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
            if formal and not services_available:
                raise ProvenancePerformanceError(
                    "formal run requires available Qdrant and Neo4j health checks"
                )
            if formal and not isolation_valid:
                raise ProvenancePerformanceError(
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
            if formal and not namespace_empty:
                raise ProvenancePerformanceError(
                    "formal run requires a new empty namespace"
                )

            _preload_graph(backend, graph)
            preload_inventory = _inventory(backend, limit=graph.node_count + 1)
            preload_closed = _inventory_matches(
                preload_inventory,
                graph.nodes,
                graph.edges,
                {"active": graph.node_count},
            )
            if formal and not preload_closed:
                raise ProvenancePerformanceError(
                    "preloaded graph count, status, or hash mismatch"
                )

            def measured(operation: Mapping[str, Any]) -> dict[str, Any]:
                started_ns = time.perf_counter_ns()
                error_class = None
                success = True
                try:
                    _apply_measured_operation(backend, operation)
                except Exception as exc:  # failures are retained as aggregate-safe classes
                    success = False
                    error_class = _safe_error_name(exc)
                latency_ns = max(0, time.perf_counter_ns() - started_ns)
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
                return row

            repetition_started_ns = time.perf_counter_ns()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(measured, operation) for operation in plan]
                measured_rows = [future.result() for future in futures]
            elapsed_ns = max(1, time.perf_counter_ns() - repetition_started_ns)
            measured_rows.sort(
                key=lambda row: (row["_operation_rank"], row["_operation_index"])
            )
            for row in measured_rows:
                row.pop("_operation_rank", None)
                row.pop("_operation_index", None)

            final_inventory = _inventory(backend, limit=len(expected_nodes) + 1)
            state_closed = _inventory_matches(
                final_inventory,
                expected_nodes,
                expected_edges,
                expected_statuses,
            )
            success_count = sum(1 for row in measured_rows if row["success"])
            failure_count = len(measured_rows) - success_count
            retry_count = sum(int(row["retry_count"]) for row in measured_rows)
            eligible = bool(
                services_available
                and isolation_valid
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
                "elapsed_ns": elapsed_ns,
                "namespace_initially_empty": namespace_empty,
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
            if formal and not eligible:
                raise ProvenancePerformanceError(
                    "formal repetition has failures or non-closed persistent state"
                )
            all_samples.extend(measured_rows)
            repetition_rows.append(repetition_row)
        finally:
            close = getattr(backend, "close", None)
            if callable(close):
                close()

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


def _flatten_reports(source: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(source, Mapping):
        reports = [source]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        reports = list(source)
    else:
        raise ValueError("aggregate input must be a report or report sequence")
    samples: list[dict[str, Any]] = []
    repetitions: list[dict[str, Any]] = []
    for report in reports:
        if not isinstance(report, Mapping):
            raise ValueError("each aggregate input must be a report mapping")
        raw_samples = report.get("samples")
        raw_repetitions = report.get("repetitions")
        if not isinstance(raw_samples, list) or not isinstance(raw_repetitions, list):
            raise ValueError("report must include sample and repetition lists")
        samples.extend(copy.deepcopy(raw_samples))
        repetitions.extend(copy.deepcopy(raw_repetitions))
    return samples, repetitions


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


def aggregate_matrix(
    samples: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    bootstrap_repetitions: int = 10_000,
    seed: int = 17,
    require_formal: bool = True,
) -> dict[str, Any]:
    """Aggregate operation samples with whole-repetition throughput bootstrap."""

    bootstrap_repetitions = _strict_positive_integer(
        bootstrap_repetitions, "bootstrap_repetitions"
    )
    seed = _strict_integer(seed, "seed")
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
        latency = row.get("latency_ns")
        if isinstance(latency, bool) or not isinstance(latency, int) or latency < 0:
            raise ProvenancePerformanceError("sample latency must be non-negative nanoseconds")
        samples_by_cell.setdefault(cell_id, []).append(row)

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
        rows.append(
            {
                "cell_id": cell_id,
                "graph_node_count": int(first["graph_node_count"]),
                "concurrency": int(first["concurrency"]),
                "repetition_count": len(cell_repetitions),
                "successful_operation_count": len(successful),
                "failed_operation_count": len(failed),
                "retry_count": sum(int(row["retry_count"]) for row in cell_repetitions),
                "p50_latency_ns": _percentile(latencies, 0.50),
                "p95_latency_ns": _percentile(latencies, 0.95),
                "p99_latency_ns": _percentile(latencies, 0.99),
                "successful_throughput_ops_per_second": interval["estimate"],
                "successful_throughput_95ci": interval,
                "error_class_counts": dict(sorted(error_counts.items())),
                "operations": operation_breakdown,
            }
        )
    return {
        "schema": MATRIX_SCHEMA,
        "bootstrap_unit": "whole_repetition",
        "bootstrap_repetitions": bootstrap_repetitions,
        "bootstrap_seed": seed,
        "throughput_numerator": "successful_operations_only",
        "latency_population": "successful_operations_only",
        "evidence_scope": "formal" if require_formal else "diagnostic",
        "rows": rows,
    }


def make_vector_graph_backend_factory(
    *,
    qdrant_url: str,
    neo4j_uri: str,
    neo4j_auth: Sequence[str],
    environment_attestation: Mapping[str, Any],
    request_timeout_seconds: float = 30.0,
) -> Callable[[str], Any]:
    """Create a zero-retry real-backend factory for unbiased operation timing."""

    attestation = copy.deepcopy(dict(environment_attestation))

    def factory(namespace: str) -> Any:
        from txnmem_vector_graph_backend import VectorGraphMemoryBackend

        backend = VectorGraphMemoryBackend(
            namespace,
            qdrant_url,
            neo4j_uri,
            neo4j_auth,
            max_retries=0,
            request_timeout_seconds=request_timeout_seconds,
        )
        backend.performance_environment = lambda: copy.deepcopy(attestation)
        return backend

    return factory
