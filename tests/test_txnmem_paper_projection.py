from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from txnmem_paper_projection import (  # noqa: E402
    _external_baselines_scale_400_projection,
    controlled_result_rows,
    external_baselines_scale_400_projection,
    provenance_performance_v10_projection,
)
from build_txnmem_ccfa_docx import _controlled_rows  # noqa: E402


class ControlledResultProjectionTests(unittest.TestCase):
    def test_all_five_markdown_and_docx_rows_match_controlled_artifact(self):
        evidence = json.loads(
            (ROOT / "results/paper_evidence/controlled_suite.json").read_text(
                encoding="utf-8"
            )
        )
        projected = controlled_result_rows(ROOT)
        expected = [
            {
                "variant": row["variant"],
                "violation": f"{evidence['variants'][row['variant']]['violation_count']}/400",
                "oracle": f"{evidence['variants'][row['variant']]['oracle_match_count']}/400",
            }
            for row in projected
        ]

        source = (ROOT / "docs/paper/txnmem_ccfa_draft_zh.md").read_text(
            encoding="utf-8"
        )
        table = source.split("`[[TABLE:controlled_results]]`", 1)[1].split(
            "<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->", 1
        )[0]
        markdown_rows = []
        for line in table.splitlines():
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) == 4 and cells[0] in evidence["variants"]:
                markdown_rows.append(
                    {"variant": cells[0], "violation": cells[1], "oracle": cells[2]}
                )

        _, docx_rows = _controlled_rows(ROOT)
        normalized_docx = [
            {"variant": row[0], "violation": row[1], "oracle": row[2]}
            for row in docx_rows
        ]
        self.assertEqual(len(expected), 5)
        self.assertEqual(markdown_rows, expected)
        self.assertEqual(normalized_docx, expected)
        self.assertIn(
            {"variant": "TxnMem-NoRepair", "violation": "100/400", "oracle": "300/400"},
            expected,
        )


class ExternalBaselinesScale400ProjectionTests(unittest.TestCase):
    SOURCE_HASHES = {
        "run_manifest.json": "cff0f64aeafab8cc62c95cd5acb54574f26396bca0e439291502cf557749043b",
        "summary.json": "c8a2bf895cfd50440a2d91ef89328196b037f73a374b9b2bcbbabfbe9f727370",
        "results.csv": "744ad7c77c539d8e424a9032b8036692a60534cbff7c13d00d59f631153d2888",
    }

    def test_projection_is_source_bound_and_separates_reporting_concepts(self):
        projection = json.loads(
            (
                ROOT
                / "results/paper_evidence/external_baselines_scale_400.json"
            ).read_text(encoding="utf-8")
        )

        self.assertEqual(
            projection["schema"],
            "txnmem.paper_projection.external_baselines_scale_400.v1",
        )
        self.assertEqual(
            projection["sources"],
            {
                "bundle_commit": "79ab85e48196b7d2f4504ee34f3f4d1025e122e4",
                "runner_commit": "540b980c4248830462ceeb2401e818e03b6284f2",
                "run_manifest": {
                    "path": "results/external_baselines_scale_400/formal_native_v3/run_manifest.json",
                    "sha256": self.SOURCE_HASHES["run_manifest.json"],
                },
                "summary": {
                    "path": "results/external_baselines_scale_400/formal_native_v3/summary.json",
                    "sha256": self.SOURCE_HASHES["summary.json"],
                },
                "raw_results": {
                    "path": "results/external_baselines_scale_400/formal_native_v3/results.csv",
                    "sha256": self.SOURCE_HASHES["results.csv"],
                },
                "input": {
                    "record_count": 400,
                    "sha256": "d2fb1041989f4d42de6527c67c49e38c23af965bf21dc0a3d3064514f73a12ee",
                },
            },
        )
        self.assertEqual(
            projection["counts"],
            {
                "attempted": 2000,
                "successful": 1850,
                "correctness_included": 1850,
                "excluded": 150,
                "capability_absent_observed": 100,
                "unsupported_mapping": 150,
                "runtime_error": 0,
            },
        )
        self.assertEqual(
            projection["wilson_interval"],
            {
                "method": "Wilson score interval",
                "confidence": 0.95,
                "denominator": "successful attempts (correctness_included)",
            },
        )
        self.assertEqual(
            set(projection["reporting_concepts"]),
            {
                "capability_absence",
                "unsupported_mapping",
                "runtime_error",
                "correctness_violation",
            },
        )
        self.assertEqual(len(projection["adapters"]), 5)
        langgraph = {
            row["adapter"]: row for row in projection["adapters"]
        }["LangGraphStore"]
        self.assertEqual(
            langgraph["counts"],
            {
                "attempted": 400,
                "successful": 250,
                "correctness_included": 250,
                "excluded": 150,
                "capability_absent_observed": 0,
                "unsupported_mapping": 150,
                "runtime_error": 0,
            },
        )
        self.assertEqual(langgraph["violation_interval"]["denominator"], 250)
        for row in projection["adapters"]:
            self.assertEqual(row["counts"]["attempted"], 400)
            self.assertEqual(
                row["violation_interval"]["denominator"],
                row["counts"]["correctness_included"],
            )
            self.assertEqual(
                row["oracle_match_interval"]["denominator"],
                row["counts"]["correctness_included"],
            )

    def test_projection_rejects_stale_or_raw_hash_inconsistent_aggregate(self):
        committed = json.loads(
            (
                ROOT
                / "results/paper_evidence/external_baselines_scale_400.json"
            ).read_text(encoding="utf-8")
        )
        summary = {
            "schema_version": "txnmem-external-runner-v1",
            "counts": committed["counts"],
            "correctness": committed["overall_correctness"],
            "adapter_counts": {
                row["adapter"]: {
                    **row["counts"],
                    "violation_count": row["violation_count"],
                    "violation_rate": row["violation_rate"],
                    "violation_interval": row["violation_interval"],
                    "oracle_match_count": row["oracle_match_count"],
                    "oracle_match_rate": row["oracle_match_rate"],
                    "oracle_match_interval": row["oracle_match_interval"],
                    "exclusions_by_category": {
                        key: value
                        for key, value in (
                            ("unsupported_mapping", row["counts"]["unsupported_mapping"]),
                            ("runtime_error", row["counts"]["runtime_error"]),
                        )
                        if value
                    },
                }
                for row in committed["adapters"]
            },
        }
        manifest = {
            "schema_version": "txnmem-external-runner-v1",
            "run_id": "formal-native-20260905-v3",
            "counts": committed["counts"],
            "input": {
                "count": 400,
                "sha256": committed["sources"]["input"]["sha256"],
            },
            "formal_binding": {
                "source": {"git_commit": committed["sources"]["runner_commit"]}
            },
            "artifacts": {
                "summary.json": {"sha256": self.SOURCE_HASHES["summary.json"]},
                "results.csv": {"sha256": self.SOURCE_HASHES["results.csv"]},
            },
            "selected_adapters": [
                {
                    "name": row["adapter"],
                    "adapter_version": row["adapter_version"],
                    "target_adapter_version": row["target_adapter_version"],
                    "backend_mode": row["backend_mode"],
                    "backend_available": True,
                }
                for row in committed["adapters"]
            ],
            "environment": {"package_versions": committed["package_versions"]},
        }

        stale = copy.deepcopy(summary)
        stale["counts"]["attempted"] = 400
        with self.assertRaisesRegex(ValueError, "2,000"):
            _external_baselines_scale_400_projection(
                stale, manifest, self.SOURCE_HASHES
            )

        inconsistent = copy.deepcopy(manifest)
        inconsistent["artifacts"]["results.csv"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "raw result hash"):
            _external_baselines_scale_400_projection(
                summary, inconsistent, self.SOURCE_HASHES
            )

    def test_projection_loader_rejects_unpinned_source_bytes(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp)
            (source / "summary.json").write_text("{}\n", encoding="utf-8")
            (source / "run_manifest.json").write_text("{}\n", encoding="utf-8")
            (source / "results.csv").write_text("header\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source hash"):
                external_baselines_scale_400_projection(source)


class ProvenancePerformanceProjectionTests(unittest.TestCase):
    def test_v10_projection_is_complete_rounded_and_source_bound(self):
        projected = provenance_performance_v10_projection(ROOT)

        self.assertEqual(
            projected["schema"],
            "txnmem.paper_projection.provenance_performance_v10.v1",
        )
        self.assertEqual(projected["result_scope"], "measurement_results")
        self.assertEqual(
            projected["source"],
            {
                "path": "results/provenance_performance_v10_measurements/aggregate.json",
                "sha256": "9045c59ad476dc0870fb51fc4b68469935bd79f83cf82b85bcc9845b7230f378",
            },
        )
        self.assertEqual(
            projected["counts"],
            {
                "cells": 15,
                "repetitions": 450,
                "samples": 14400,
                "successful_samples": 14400,
                "failed_samples": 0,
            },
        )
        self.assertEqual(len(projected["cells"]), 15)
        self.assertEqual(
            projected["cells"][0],
            {
                "graph_node_count": 100,
                "concurrency": 1,
                "p50_ms": 59.474,
                "p95_ms": 144.263,
                "p99_ms": 172.244,
                "throughput_ops_per_second": 16.825475,
                "ci95_lower_ops_per_second": 16.12567,
                "ci95_upper_ops_per_second": 17.47755,
            },
        )
        self.assertEqual(
            projected["cells"][-1],
            {
                "graph_node_count": 10000,
                "concurrency": 16,
                "p50_ms": 2037.842,
                "p95_ms": 530274.466,
                "p99_ms": 545677.946,
                "throughput_ops_per_second": 0.063491,
                "ci95_lower_ops_per_second": 0.061534,
                "ci95_upper_ops_per_second": 0.066058,
            },
        )
        self.assertEqual(
            projected["analysis"],
            {
                "peak_throughput_by_graph": [
                    {
                        "graph_node_count": 100,
                        "concurrency": 2,
                        "throughput_ops_per_second": 21.899464,
                    },
                    {
                        "graph_node_count": 1000,
                        "concurrency": 2,
                        "throughput_ops_per_second": 2.843982,
                    },
                    {
                        "graph_node_count": 10000,
                        "concurrency": 1,
                        "throughput_ops_per_second": 0.122943,
                    },
                ],
                "throughput_change_percent": {
                    "graph_100_c1_to_c2": 30.16,
                    "graph_1000_c1_to_c2": 12.2,
                    "graph_10000_c1_to_c2": -19.46,
                    "graph_10000_c1_to_c16": -48.36,
                    "c1_graph_100_to_10000_drop": 99.27,
                },
                "search_p50_ms_at_concurrency_1": [
                    {"graph_node_count": 100, "p50_ms": 130.019},
                    {"graph_node_count": 1000, "p50_ms": 1470.183},
                    {"graph_node_count": 10000, "p50_ms": 32342.042},
                ],
                "graph_10000_c1_operation_p50_ms": {
                    "read": 4.596,
                    "search": 32342.042,
                    "derive": 21.312,
                    "invalidate_repair": 73.928,
                },
            },
        )
        numeric_values = projected["manuscript_numeric_values"]
        self.assertEqual(numeric_values, sorted(set(numeric_values)))
        for displayed_value in (
            0,
            15,
            30,
            95,
            960,
            14400,
            59.474,
            21.899464,
            19.46,
            48.36,
            32342.042,
            545677.946,
        ):
            self.assertIn(displayed_value, numeric_values)

    def test_committed_v10_projection_matches_fresh_recomputation(self):
        committed = json.loads(
            (
                ROOT
                / "results/paper_evidence/provenance_performance_v10.json"
            ).read_text(encoding="utf-8")
        )

        self.assertEqual(committed, provenance_performance_v10_projection(ROOT))

    def test_v10_projection_rejects_contradictory_counts_and_methodology(self):
        source = json.loads(
            (
                ROOT
                / "results/provenance_performance_v10_measurements/aggregate.json"
            ).read_text(encoding="utf-8")
        )
        mutations = (
            ("matrix cell count", ("measurement_matrix", "cell_count"), 14),
            ("matrix sample count", ("measurement_matrix", "sample_count"), 14399),
            ("samples per cell", ("methodology", "samples_per_cell"), 959),
            (
                "throughput unit",
                ("methodology", "throughput_unit"),
                "operations_per_second",
            ),
            (
                "latency population",
                ("methodology", "latency_population"),
                "all_operations",
            ),
            (
                "throughput numerator",
                ("methodology", "throughput_numerator"),
                "all_operations",
            ),
            ("bootstrap seed", ("methodology", "bootstrap_seed"), 18),
        )
        for label, (section, field), replacement in mutations:
            with self.subTest(label=label), TemporaryDirectory() as tmp:
                payload = copy.deepcopy(source)
                payload[section][field] = replacement
                source_path = (
                    Path(tmp)
                    / "results/provenance_performance_v10_measurements/aggregate.json"
                )
                source_path.parent.mkdir(parents=True)
                source_path.write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "aggregate contract"):
                    provenance_performance_v10_projection(tmp)

    def test_v10_projection_rejects_invalid_operation_counts_and_metrics(self):
        source = json.loads(
            (
                ROOT
                / "results/provenance_performance_v10_measurements/aggregate.json"
            ).read_text(encoding="utf-8")
        )
        mutations = (
            (
                "operation sample count",
                lambda payload: payload["cells"][0]["operations"]["read"].__setitem__(
                    "successful_sample_count", 239
                ),
            ),
            (
                "negative throughput",
                lambda payload: (
                    payload["cells"][0].__setitem__(
                        "successful_throughput_ops_per_second", -1
                    ),
                    payload["cells"][0].__setitem__(
                        "throughput_95ci", {"lower": -2, "upper": 0}
                    ),
                ),
            ),
            (
                "unordered operation latency",
                lambda payload: payload["cells"][0]["operations"]["search"].__setitem__(
                    "latency_ns", {"p50": 3, "p95": 2, "p99": 1}
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label), TemporaryDirectory() as tmp:
                payload = copy.deepcopy(source)
                mutate(payload)
                source_path = (
                    Path(tmp)
                    / "results/provenance_performance_v10_measurements/aggregate.json"
                )
                source_path.parent.mkdir(parents=True)
                source_path.write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "cell contract"):
                    provenance_performance_v10_projection(tmp)


if __name__ == "__main__":
    unittest.main()
