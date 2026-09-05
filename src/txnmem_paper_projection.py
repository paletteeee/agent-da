"""Shared evidence projections used by manuscript publication builders."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any


CONTROLLED_VARIANT_ORDER = (
    "TxnMem",
    "Naive",
    "TxnMem-NoTxn",
    "TxnMem-NoPolicyCommit",
    "TxnMem-NoRepair",
)


def controlled_result_rows(root: str | Path) -> list[dict[str, Any]]:
    """Project every controlled variant directly from the audited artifact."""

    root = Path(root)
    payload = json.loads(
        (root / "results/paper_evidence/controlled_suite.json").read_text(
            encoding="utf-8"
        )
    )
    instance_count = payload.get("instance_count")
    variants = payload.get("variants")
    if (
        isinstance(instance_count, bool)
        or not isinstance(instance_count, int)
        or instance_count <= 0
        or not isinstance(variants, dict)
        or set(variants) != set(CONTROLLED_VARIANT_ORDER)
    ):
        raise ValueError("controlled-suite variant projection is malformed")
    rows: list[dict[str, Any]] = []
    for variant in CONTROLLED_VARIANT_ORDER:
        item = variants[variant]
        if not isinstance(item, dict) or item.get("row_count") != instance_count:
            raise ValueError(f"controlled-suite row count mismatch: {variant}")
        violation_count = item.get("violation_count")
        oracle_match_count = item.get("oracle_match_count")
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= instance_count
            for value in (violation_count, oracle_match_count)
        ):
            raise ValueError(f"controlled-suite count is malformed: {variant}")
        rows.append(
            {
                "variant": variant,
                "instance_count": instance_count,
                "violation_count": violation_count,
                "oracle_match_count": oracle_match_count,
            }
        )
    return rows


PROVENANCE_PERFORMANCE_V10_SOURCE = (
    "results/provenance_performance_v10_measurements/aggregate.json"
)
PROVENANCE_GRAPH_NODE_COUNTS = (100, 1000, 10000)
PROVENANCE_CONCURRENCY_LEVELS = (1, 2, 4, 8, 16)
PROVENANCE_OPERATIONS = ("read", "search", "derive", "invalidate_repair")
PROVENANCE_MATRIX_COUNTS = {
    "cell_count": 15,
    "repetition_count": 450,
    "sample_count": 14_400,
    "successful_sample_count": 14_400,
    "failed_sample_count": 0,
    "retry_count": 0,
    "setup_repair_count": 0,
}
PROVENANCE_METHODOLOGY = {
    "repetitions_per_cell": 30,
    "samples_per_cell": 960,
    "samples_per_operation_per_cell": 240,
    "latency_unit": "ns",
    "throughput_unit": "successful_operations_per_second",
    "latency_population": "successful_operations_only",
    "throughput_numerator": "successful_operations_only",
    "bootstrap_unit": "whole_repetition",
    "bootstrap_repetitions": 10_000,
    "bootstrap_seed": 17,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rounded(value: Any, places: int, *, divisor: int = 1) -> float:
    quantum = Decimal(1).scaleb(-places)
    normalized = Decimal(str(value)) / Decimal(divisor)
    return float(normalized.quantize(quantum, rounding=ROUND_HALF_UP))


def _percent_change(before: Any, after: Any) -> float:
    change = (Decimal(str(after)) / Decimal(str(before)) - Decimal(1)) * 100
    return float(change.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _is_exact_int(value: Any, expected: int) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value == expected


def _is_positive_finite_number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and value > 0
    )


def _valid_latency_summary(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    percentiles = tuple(value.get(key) for key in ("p50", "p95", "p99"))
    return all(_is_positive_finite_number(item) for item in percentiles) and (
        percentiles[0] <= percentiles[1] <= percentiles[2]
    )


def _flat_numeric_values(*values: Any) -> list[int | float]:
    collected: set[Decimal] = set()

    def collect(value: Any) -> None:
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)):
            normalized = Decimal(str(value))
            collected.add(normalized)
            collected.add(abs(normalized))
        elif isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    for value in values:
        collect(value)
    return [
        int(value) if value == value.to_integral_value() else float(value)
        for value in sorted(collected)
    ]


def provenance_performance_v10_projection(root: str | Path) -> dict[str, Any]:
    """Build the rounded, manuscript-facing projection of the v10 measurements."""

    root = Path(root)
    source_path = root / PROVENANCE_PERFORMANCE_V10_SOURCE
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    matrix = payload.get("measurement_matrix")
    methodology = payload.get("methodology")
    cells = payload.get("cells")
    expected_pairs = {
        (graph_node_count, concurrency)
        for graph_node_count in PROVENANCE_GRAPH_NODE_COUNTS
        for concurrency in PROVENANCE_CONCURRENCY_LEVELS
    }
    if (
        payload.get("schema")
        != "txnmem.provenance_performance.measurement_results.v1"
        or payload.get("dataset_id") != "provenance_performance_v10_measurements"
        or payload.get("result_scope") != "measurement_results"
        or not isinstance(matrix, dict)
        or matrix.get("graph_node_counts") != list(PROVENANCE_GRAPH_NODE_COUNTS)
        or matrix.get("concurrency_levels") != list(PROVENANCE_CONCURRENCY_LEVELS)
        or matrix.get("operations") != list(PROVENANCE_OPERATIONS)
        or any(
            not _is_exact_int(matrix.get(field), expected)
            for field, expected in PROVENANCE_MATRIX_COUNTS.items()
        )
        or not isinstance(methodology, dict)
        or any(
            methodology.get(field) != expected
            for field, expected in PROVENANCE_METHODOLOGY.items()
        )
        or not isinstance(cells, list)
        or len(cells) != PROVENANCE_MATRIX_COUNTS["cell_count"]
    ):
        raise ValueError("v10 provenance-performance aggregate contract is malformed")

    indexed: dict[tuple[int, int], dict[str, Any]] = {}
    for cell in cells:
        if not isinstance(cell, dict):
            raise ValueError("v10 provenance-performance cell is malformed")
        key = (cell.get("graph_node_count"), cell.get("concurrency"))
        latency = cell.get("latency_ns")
        confidence_interval = cell.get("throughput_95ci")
        operations = cell.get("operations")
        throughput = cell.get("successful_throughput_ops_per_second")
        if (
            key not in expected_pairs
            or key in indexed
            or any(isinstance(item, bool) or not isinstance(item, int) for item in key)
            or not _is_exact_int(
                cell.get("repetition_count"),
                PROVENANCE_METHODOLOGY["repetitions_per_cell"],
            )
            or not _is_exact_int(
                cell.get("successful_sample_count"),
                PROVENANCE_METHODOLOGY["samples_per_cell"],
            )
            or any(
                not _is_exact_int(cell.get(field), 0)
                for field in (
                    "failed_sample_count",
                    "retry_count",
                    "setup_repair_count",
                )
            )
            or not _valid_latency_summary(latency)
            or not _is_positive_finite_number(throughput)
            or not isinstance(confidence_interval, dict)
            or not _is_positive_finite_number(confidence_interval.get("lower"))
            or not _is_positive_finite_number(confidence_interval.get("upper"))
            or not confidence_interval["lower"] <= throughput <= confidence_interval["upper"]
            or not isinstance(operations, dict)
            or set(operations) != set(PROVENANCE_OPERATIONS)
        ):
            raise ValueError(f"v10 provenance-performance cell contract mismatch: {key}")
        for operation in PROVENANCE_OPERATIONS:
            operation_result = operations[operation]
            if (
                not isinstance(operation_result, dict)
                or not _is_exact_int(
                    operation_result.get("successful_sample_count"),
                    PROVENANCE_METHODOLOGY["samples_per_operation_per_cell"],
                )
                or not _is_exact_int(operation_result.get("failed_sample_count"), 0)
                or not _valid_latency_summary(operation_result.get("latency_ns"))
            ):
                raise ValueError(
                    f"v10 provenance-performance cell contract mismatch: {key}"
                )
        if (
            sum(
                operations[operation]["successful_sample_count"]
                for operation in PROVENANCE_OPERATIONS
            )
            != cell["successful_sample_count"]
            or sum(
                operations[operation]["failed_sample_count"]
                for operation in PROVENANCE_OPERATIONS
            )
            != cell["failed_sample_count"]
        ):
            raise ValueError(f"v10 provenance-performance cell contract mismatch: {key}")
        indexed[key] = cell
    if set(indexed) != expected_pairs:
        raise ValueError("v10 provenance-performance matrix is incomplete")

    derived_matrix_counts = {
        "cell_count": len(indexed),
        "repetition_count": sum(cell["repetition_count"] for cell in indexed.values()),
        "successful_sample_count": sum(
            cell["successful_sample_count"] for cell in indexed.values()
        ),
        "failed_sample_count": sum(
            cell["failed_sample_count"] for cell in indexed.values()
        ),
        "retry_count": sum(cell["retry_count"] for cell in indexed.values()),
        "setup_repair_count": sum(
            cell["setup_repair_count"] for cell in indexed.values()
        ),
    }
    derived_matrix_counts["sample_count"] = (
        derived_matrix_counts["successful_sample_count"]
        + derived_matrix_counts["failed_sample_count"]
    )
    if any(
        matrix[field] != derived_matrix_counts[field]
        for field in PROVENANCE_MATRIX_COUNTS
    ):
        raise ValueError("v10 provenance-performance aggregate contract is contradictory")

    projected_cells: list[dict[str, Any]] = []
    for graph_node_count in PROVENANCE_GRAPH_NODE_COUNTS:
        for concurrency in PROVENANCE_CONCURRENCY_LEVELS:
            cell = indexed[(graph_node_count, concurrency)]
            projected_cells.append(
                {
                    "graph_node_count": graph_node_count,
                    "concurrency": concurrency,
                    "p50_ms": _rounded(cell["latency_ns"]["p50"], 3, divisor=1_000_000),
                    "p95_ms": _rounded(cell["latency_ns"]["p95"], 3, divisor=1_000_000),
                    "p99_ms": _rounded(cell["latency_ns"]["p99"], 3, divisor=1_000_000),
                    "throughput_ops_per_second": _rounded(
                        cell["successful_throughput_ops_per_second"], 6
                    ),
                    "ci95_lower_ops_per_second": _rounded(
                        cell["throughput_95ci"]["lower"], 6
                    ),
                    "ci95_upper_ops_per_second": _rounded(
                        cell["throughput_95ci"]["upper"], 6
                    ),
                }
            )

    peaks = []
    for graph_node_count in PROVENANCE_GRAPH_NODE_COUNTS:
        peak = max(
            (indexed[(graph_node_count, concurrency)] for concurrency in PROVENANCE_CONCURRENCY_LEVELS),
            key=lambda cell: cell["successful_throughput_ops_per_second"],
        )
        peaks.append(
            {
                "graph_node_count": graph_node_count,
                "concurrency": peak["concurrency"],
                "throughput_ops_per_second": _rounded(
                    peak["successful_throughput_ops_per_second"], 6
                ),
            }
        )

    c1_search = [
        {
            "graph_node_count": graph_node_count,
            "p50_ms": _rounded(
                indexed[(graph_node_count, 1)]["operations"]["search"]["latency_ns"]["p50"],
                3,
                divisor=1_000_000,
            ),
        }
        for graph_node_count in PROVENANCE_GRAPH_NODE_COUNTS
    ]
    graph_10000_c1 = indexed[(10000, 1)]
    operation_p50 = {
        operation: _rounded(
            graph_10000_c1["operations"][operation]["latency_ns"]["p50"],
            3,
            divisor=1_000_000,
        )
        for operation in PROVENANCE_OPERATIONS
    }

    projection = {
        "schema": "txnmem.paper_projection.provenance_performance_v10.v1",
        "result_scope": "measurement_results",
        "source": {
            "path": PROVENANCE_PERFORMANCE_V10_SOURCE,
            "sha256": _sha256(source_path),
        },
        "counts": {
            "cells": matrix["cell_count"],
            "repetitions": matrix["repetition_count"],
            "samples": matrix["sample_count"],
            "successful_samples": matrix["successful_sample_count"],
            "failed_samples": matrix["failed_sample_count"],
        },
        "methodology": {
            "repetitions_per_cell": methodology["repetitions_per_cell"],
            "samples_per_cell": methodology["samples_per_cell"],
            "latency_unit": "ms",
            "source_latency_unit": methodology["latency_unit"],
            "percentiles": [50, 95, 99],
            "confidence_level_percent": 95,
            "bootstrap_unit": methodology["bootstrap_unit"],
            "bootstrap_repetitions": methodology["bootstrap_repetitions"],
        },
        "cells": projected_cells,
        "analysis": {
            "peak_throughput_by_graph": peaks,
            "throughput_change_percent": {
                "graph_100_c1_to_c2": _percent_change(
                    indexed[(100, 1)]["successful_throughput_ops_per_second"],
                    indexed[(100, 2)]["successful_throughput_ops_per_second"],
                ),
                "graph_1000_c1_to_c2": _percent_change(
                    indexed[(1000, 1)]["successful_throughput_ops_per_second"],
                    indexed[(1000, 2)]["successful_throughput_ops_per_second"],
                ),
                "graph_10000_c1_to_c2": _percent_change(
                    indexed[(10000, 1)]["successful_throughput_ops_per_second"],
                    indexed[(10000, 2)]["successful_throughput_ops_per_second"],
                ),
                "graph_10000_c1_to_c16": _percent_change(
                    indexed[(10000, 1)]["successful_throughput_ops_per_second"],
                    indexed[(10000, 16)]["successful_throughput_ops_per_second"],
                ),
                "c1_graph_100_to_10000_drop": -_percent_change(
                    indexed[(100, 1)]["successful_throughput_ops_per_second"],
                    indexed[(10000, 1)]["successful_throughput_ops_per_second"],
                ),
            },
            "search_p50_ms_at_concurrency_1": c1_search,
            "graph_10000_c1_operation_p50_ms": operation_p50,
        },
    }
    projection["manuscript_numeric_values"] = _flat_numeric_values(
        projection["counts"],
        projection["methodology"],
        projection["cells"],
        projection["analysis"],
    )
    return projection


EXTERNAL_BASELINE_ADAPTER_ORDER = (
    "AppendOnly",
    "LastWriteWins",
    "MetadataFiltered",
    "Mem0",
    "LangGraphStore",
)
EXTERNAL_BASELINE_BUNDLE_COMMIT = "79ab85e48196b7d2f4504ee34f3f4d1025e122e4"
EXTERNAL_BASELINE_RUNNER_COMMIT = "540b980c4248830462ceeb2401e818e03b6284f2"
EXTERNAL_BASELINE_INPUT_SHA256 = (
    "d2fb1041989f4d42de6527c67c49e38c23af965bf21dc0a3d3064514f73a12ee"
)
EXTERNAL_BASELINE_SOURCE_HASHES = {
    "run_manifest.json": "cff0f64aeafab8cc62c95cd5acb54574f26396bca0e439291502cf557749043b",
    "summary.json": "c8a2bf895cfd50440a2d91ef89328196b037f73a374b9b2bcbbabfbe9f727370",
    "results.csv": "744ad7c77c539d8e424a9032b8036692a60534cbff7c13d00d59f631153d2888",
}
EXTERNAL_BASELINE_COUNTS = {
    "attempted": 2_000,
    "successful": 1_850,
    "correctness_included": 1_850,
    "excluded": 150,
    "capability_absent_observed": 100,
    "unsupported_mapping": 150,
    "runtime_error": 0,
}
EXTERNAL_BASELINE_CLAIM_BOUNDARY = (
    "observable correctness comparison on the same 400-instance TxnMemBench suite; "
    "capability absence is an interface observation, unsupported/runtime attempts "
    "are excluded from correctness denominators, and results do not establish "
    "third-party security defects or general production behavior"
)


def _external_count_record(value: Any, *, attempted: int) -> dict[str, int]:
    fields = tuple(EXTERNAL_BASELINE_COUNTS)
    if not isinstance(value, dict):
        raise ValueError("external baseline count record is malformed")
    counts = {field: value.get(field) for field in fields}
    if any(isinstance(item, bool) or not isinstance(item, int) for item in counts.values()):
        raise ValueError("external baseline count record is malformed")
    if (
        counts["attempted"] != attempted
        or counts["attempted"]
        != counts["successful"]
        + counts["unsupported_mapping"]
        + counts["runtime_error"]
        or counts["correctness_included"] != counts["successful"]
        or counts["excluded"]
        != counts["unsupported_mapping"] + counts["runtime_error"]
        or not 0
        <= counts["capability_absent_observed"]
        <= counts["successful"]
    ):
        raise ValueError("external baseline count identities are inconsistent")
    return counts


def _external_correctness_record(value: Any, denominator: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("external baseline correctness record is malformed")
    projected: dict[str, Any] = {}
    for prefix in ("violation", "oracle_match"):
        count = value.get(f"{prefix}_count")
        rate = value.get(f"{prefix}_rate")
        interval = value.get(f"{prefix}_interval")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or not 0 <= count <= denominator
            or isinstance(rate, bool)
            or not isinstance(rate, (int, float))
            or not math.isfinite(rate)
            or rate != count / denominator
            or not isinstance(interval, dict)
            or set(interval)
            != {
                "available",
                "confidence",
                "estimate",
                "lower",
                "upper",
                "numerator",
                "denominator",
            }
            or interval.get("available") is not True
            or interval.get("confidence") != 0.95
            or interval.get("numerator") != count
            or interval.get("denominator") != denominator
            or interval.get("estimate") != rate
            or any(
                isinstance(interval.get(field), bool)
                or not isinstance(interval.get(field), (int, float))
                or not math.isfinite(interval[field])
                for field in ("lower", "upper")
            )
            or not 0 <= interval["lower"] <= rate <= interval["upper"] <= 1
        ):
            raise ValueError("external baseline Wilson interval is malformed")
        projected[f"{prefix}_count"] = count
        projected[f"{prefix}_rate"] = rate
        projected[f"{prefix}_interval"] = dict(interval)
    if projected["violation_count"] + projected["oracle_match_count"] != denominator:
        raise ValueError("external baseline correctness counts are inconsistent")
    return projected


def _external_baselines_scale_400_projection(
    summary: dict[str, Any],
    manifest: dict[str, Any],
    source_hashes: dict[str, str],
) -> dict[str, Any]:
    """Project a validated formal bundle without retaining raw attempt data."""

    if source_hashes != EXTERNAL_BASELINE_SOURCE_HASHES:
        raise ValueError("external baseline source hash mismatch")
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != "txnmem-external-runner-v1"
        or manifest.get("run_id") != "formal-native-20260905-v3"
        or not isinstance(summary, dict)
        or summary.get("schema_version") != "txnmem-external-runner-v1"
    ):
        raise ValueError("external baseline source contract is malformed")
    artifacts = manifest.get("artifacts")
    if (
        not isinstance(artifacts, dict)
        or not isinstance(artifacts.get("summary.json"), dict)
        or artifacts["summary.json"].get("sha256")
        != source_hashes["summary.json"]
        or not isinstance(artifacts.get("results.csv"), dict)
        or artifacts["results.csv"].get("sha256")
        != source_hashes["results.csv"]
    ):
        raise ValueError("external baseline aggregate is inconsistent with raw result hash")

    if not isinstance(summary.get("counts"), dict) or summary["counts"].get(
        "attempted"
    ) != 2_000:
        raise ValueError("external baseline source must contain exactly 2,000 attempts")
    summary_counts = _external_count_record(summary.get("counts"), attempted=2_000)
    manifest_counts = _external_count_record(manifest.get("counts"), attempted=2_000)
    if summary_counts != EXTERNAL_BASELINE_COUNTS or manifest_counts != summary_counts:
        raise ValueError("external baseline source must contain exactly 2,000 attempts")
    input_identity = manifest.get("input")
    runner_source = manifest.get("formal_binding", {}).get("source", {})
    if (
        not isinstance(input_identity, dict)
        or input_identity.get("count") != 400
        or input_identity.get("sha256") != EXTERNAL_BASELINE_INPUT_SHA256
        or not isinstance(runner_source, dict)
        or runner_source.get("git_commit") != EXTERNAL_BASELINE_RUNNER_COMMIT
    ):
        raise ValueError("external baseline formal provenance is malformed")

    selected = manifest.get("selected_adapters")
    by_selected = {
        row.get("name"): row for row in selected if isinstance(row, dict)
    } if isinstance(selected, list) else {}
    adapter_counts = summary.get("adapter_counts")
    if (
        list(by_selected) != list(EXTERNAL_BASELINE_ADAPTER_ORDER)
        or not isinstance(adapter_counts, dict)
        or set(adapter_counts) != set(EXTERNAL_BASELINE_ADAPTER_ORDER)
    ):
        raise ValueError("external baseline adapter domain is malformed")

    adapters: list[dict[str, Any]] = []
    for adapter in EXTERNAL_BASELINE_ADAPTER_ORDER:
        metadata = by_selected[adapter]
        if (
            set(metadata)
            != {
                "name",
                "adapter_version",
                "target_adapter_version",
                "backend_mode",
                "backend_available",
            }
            or metadata.get("backend_available") is not True
            or any(
                not isinstance(metadata.get(field), str) or not metadata[field]
                for field in (
                    "adapter_version",
                    "target_adapter_version",
                    "backend_mode",
                )
            )
        ):
            raise ValueError("external baseline adapter metadata is malformed")
        counts = _external_count_record(adapter_counts[adapter], attempted=400)
        correctness = _external_correctness_record(
            adapter_counts[adapter], counts["correctness_included"]
        )
        adapters.append(
            {
                "adapter": adapter,
                "adapter_version": metadata["adapter_version"],
                "target_adapter_version": metadata["target_adapter_version"],
                "backend_mode": metadata["backend_mode"],
                "counts": counts,
                **correctness,
            }
        )
    for field in EXTERNAL_BASELINE_COUNTS:
        if sum(row["counts"][field] for row in adapters) != summary_counts[field]:
            raise ValueError("external baseline adapter counts do not reconcile")

    overall_correctness = _external_correctness_record(
        summary.get("correctness"), summary_counts["correctness_included"]
    )
    if (
        sum(row["violation_count"] for row in adapters)
        != overall_correctness["violation_count"]
        or sum(row["oracle_match_count"] for row in adapters)
        != overall_correctness["oracle_match_count"]
    ):
        raise ValueError("external baseline correctness totals do not reconcile")
    package_versions = manifest.get("environment", {}).get("package_versions")
    if package_versions != {
        "langgraph": "1.2.11",
        "langgraph-checkpoint-postgres": "3.1.2",
        "mem0ai": "2.0.0",
        "psycopg-binary": "3.3.4",
    }:
        raise ValueError("external baseline package versions are malformed")

    bundle_prefix = "results/external_baselines_scale_400/formal_native_v3"
    return {
        "schema": "txnmem.paper_projection.external_baselines_scale_400.v1",
        "result_scope": "observable_correctness_comparison",
        "sources": {
            "bundle_commit": EXTERNAL_BASELINE_BUNDLE_COMMIT,
            "runner_commit": EXTERNAL_BASELINE_RUNNER_COMMIT,
            "run_manifest": {
                "path": f"{bundle_prefix}/run_manifest.json",
                "sha256": source_hashes["run_manifest.json"],
            },
            "summary": {
                "path": f"{bundle_prefix}/summary.json",
                "sha256": source_hashes["summary.json"],
            },
            "raw_results": {
                "path": f"{bundle_prefix}/results.csv",
                "sha256": source_hashes["results.csv"],
            },
            "input": {"record_count": 400, "sha256": EXTERNAL_BASELINE_INPUT_SHA256},
        },
        "counts": summary_counts,
        "overall_correctness": overall_correctness,
        "wilson_interval": {
            "method": "Wilson score interval",
            "confidence": 0.95,
            "denominator": "successful attempts (correctness_included)",
        },
        "reporting_concepts": {
            "capability_absence": {
                "count": summary_counts["capability_absent_observed"],
                "relationship": "orthogonal observation on successful attempts",
            },
            "unsupported_mapping": {
                "count": summary_counts["unsupported_mapping"],
                "relationship": "mutually exclusive run status; excluded from correctness",
            },
            "runtime_error": {
                "count": summary_counts["runtime_error"],
                "relationship": "mutually exclusive run status; excluded from correctness",
            },
            "correctness_violation": {
                "count": overall_correctness["violation_count"],
                "denominator": summary_counts["correctness_included"],
                "relationship": "oracle outcome on successful attempts only",
            },
        },
        "package_versions": dict(package_versions),
        "adapters": adapters,
        "claim_boundary": EXTERNAL_BASELINE_CLAIM_BOUNDARY,
    }


def external_baselines_scale_400_projection(source_dir: str | Path) -> dict[str, Any]:
    """Load and project the pinned formal-native-v3 external baseline bundle."""

    source_dir = Path(source_dir)
    paths = {
        name: source_dir / name
        for name in ("run_manifest.json", "summary.json", "results.csv")
    }
    try:
        source_hashes = {name: _sha256(path) for name, path in paths.items()}
        manifest = json.loads(paths["run_manifest.json"].read_text(encoding="utf-8"))
        summary = json.loads(paths["summary.json"].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("external baseline source is unreadable") from exc
    return _external_baselines_scale_400_projection(summary, manifest, source_hashes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--provenance-v10-out", type=Path)
    parser.add_argument("--external-baselines-source-dir", type=Path)
    parser.add_argument("--external-baselines-out", type=Path)
    args = parser.parse_args()
    if bool(args.external_baselines_source_dir) != bool(args.external_baselines_out):
        parser.error(
            "--external-baselines-source-dir and --external-baselines-out are required together"
        )
    outputs: list[tuple[Path, dict[str, Any]]] = []
    if args.provenance_v10_out:
        outputs.append(
            (
                args.provenance_v10_out,
                provenance_performance_v10_projection(args.root),
            )
        )
    if args.external_baselines_out:
        outputs.append(
            (
                args.external_baselines_out,
                external_baselines_scale_400_projection(
                    args.external_baselines_source_dir
                ),
            )
        )
    if not outputs:
        parser.error("at least one projection output is required")
    for output, payload in outputs:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
