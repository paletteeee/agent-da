import copy
import json
import unittest
from pathlib import Path

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_provenance_performance import (
    ProvenancePerformanceError,
    aggregate_matrix,
    build_layered_dag,
    canonical_graph_sha256,
    expand_matrix,
    run_matrix_cell,
)


class _FixtureBackend(InstrumentedMemoryBackend):
    def __init__(
        self,
        namespace,
        *,
        available=True,
        isolated=True,
        inventory_override=None,
    ):
        super().__init__()
        self.namespace = namespace
        self.available = available
        self.isolated = isolated
        self.inventory_override = inventory_override

    def healthcheck(self):
        return {
            "qdrant": {"available": self.available, "version": "fixture-q"},
            "neo4j": {"available": self.available, "version": "fixture-n"},
        }

    def performance_environment(self):
        return {
            "isolation_verified": self.isolated,
            "co_tenant_load_detected": not self.isolated,
            "source": "fixture",
        }

    def provenance_inventory(self, limit=None):
        if self.inventory_override is not None:
            return copy.deepcopy(self.inventory_override)
        nodes = sorted(self.memories)
        edges = sorted(
            (str(source_id), str(memory_id))
            for memory_id, row in self.memories.items()
            for source_id in row.get("derived_from", [])
        )
        return {
            "classification": "complete",
            "node_count": len(nodes),
            "edge_count": len(edges),
            "graph_sha256": canonical_graph_sha256(nodes, edges),
            "status_counts": {
                status: sum(
                    1 for row in self.memories.values() if row.get("status") == status
                )
                for status in sorted(
                    {str(row.get("status")) for row in self.memories.values()}
                )
            },
        }


class ProvenanceGraphTests(unittest.TestCase):
    def test_layered_dag_is_exact_acyclic_and_deterministic(self):
        first = build_layered_dag(100, seed=17)
        second = build_layered_dag(100, seed=17)
        changed = build_layered_dag(100, seed=18)

        self.assertEqual(first, second)
        self.assertEqual(len(first.nodes), 100)
        self.assertEqual(first.node_count, 100)
        self.assertEqual(first.edge_count, len(first.edges))
        positions = {node: index for index, node in enumerate(first.nodes)}
        self.assertTrue(all(positions[source] < positions[target] for source, target in first.edges))
        self.assertEqual(first.graph_sha256, canonical_graph_sha256(first.nodes, first.edges))
        self.assertNotEqual(first.graph_sha256, changed.graph_sha256)

    def test_bad_graph_sizes_are_rejected(self):
        for node_count in (0, -1, True):
            with self.subTest(node_count=node_count):
                with self.assertRaises(ValueError):
                    build_layered_dag(node_count, seed=17)


class ProvenanceMatrixTests(unittest.TestCase):
    def test_repository_formal_config_and_wrapper_are_exact(self):
        root = Path(__file__).resolve().parents[1]
        config_path = root / "configs" / "provenance_performance_matrix.json"
        script_path = root / "scripts" / "run_provenance_performance.sh"
        config = json.loads(config_path.read_text(encoding="utf-8"))

        cells = expand_matrix(config)

        self.assertEqual(config["graph_node_counts"], [100, 1000, 10000])
        self.assertEqual(config["concurrency_levels"], [1, 2, 4, 8, 16])
        self.assertEqual(config["repetitions"], 30)
        self.assertEqual(len(cells), 15)
        script = script_path.read_text(encoding="utf-8")
        self.assertIn("provenance-performance", script)
        self.assertIn("--formal", script)
        self.assertIn("TXNMEM_NEO4J_PASSWORD", script)
        self.assertNotIn("txnmem-local-only", script)

    def test_formal_matrix_has_exactly_fifteen_cells_and_thirty_repetitions(self):
        cells = expand_matrix(
            {
                "graph_node_counts": [100, 1000, 10000],
                "concurrency_levels": [1, 2, 4, 8, 16],
                "repetitions": 30,
                "graph_seed": 17,
                "operations_per_type": 8,
            }
        )
        self.assertEqual(len(cells), 15)
        self.assertEqual({cell["repetitions"] for cell in cells}, {30})
        self.assertEqual(len({cell["cell_id"] for cell in cells}), 15)

    def test_matrix_rejects_duplicate_or_nonpositive_axes(self):
        base = {
            "graph_node_counts": [10, 20],
            "concurrency_levels": [1, 2],
            "repetitions": 2,
            "operations_per_type": 1,
        }
        for key, value in (
            ("graph_node_counts", [10, 10]),
            ("concurrency_levels", [1, 0]),
            ("repetitions", 0),
            ("operations_per_type", -1),
        ):
            config = dict(base)
            config[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    expand_matrix(config)

    def test_small_two_by_two_fixture_closes_state_and_uses_unique_namespaces(self):
        cells = expand_matrix(
            {
                "graph_node_counts": [10, 20],
                "concurrency_levels": [1, 2],
                "repetitions": 2,
                "graph_seed": 17,
                "operations_per_type": 1,
            }
        )
        seen_namespaces = []

        def factory(namespace):
            seen_namespaces.append(namespace)
            return _FixtureBackend(namespace)

        reports = []
        for cell in cells:
            graph = build_layered_dag(cell["graph_node_count"], cell["graph_seed"])
            reports.append(
                run_matrix_cell(
                    factory,
                    graph,
                    concurrency=cell["concurrency"],
                    repetitions=cell["repetitions"],
                    operations_per_type=cell["operations_per_type"],
                    run_id="fixture-run",
                    formal=True,
                )
            )

        self.assertEqual(len(reports), 4)
        self.assertEqual(len(seen_namespaces), 8)
        self.assertEqual(len(set(seen_namespaces)), 8)
        for report in reports:
            self.assertEqual(len(report["repetitions"]), 2)
            self.assertTrue(all(row["state_closed"] for row in report["repetitions"]))
            self.assertTrue(all(row["eligible_for_formal"] for row in report["repetitions"]))
            self.assertEqual(
                {sample["operation"] for sample in report["samples"]},
                {"read", "search", "derive", "invalidate_repair"},
            )
            forbidden = {"value", "payload", "memory_id", "source_ids", "query"}
            self.assertTrue(
                all(not forbidden.intersection(sample) for sample in report["samples"])
            )

    def test_formal_run_fails_closed_on_health_isolation_or_inventory(self):
        graph = build_layered_dag(10, seed=17)
        cases = {
            "health": lambda namespace: _FixtureBackend(namespace, available=False),
            "co_tenant": lambda namespace: _FixtureBackend(namespace, isolated=False),
            "partial": lambda namespace: _FixtureBackend(
                namespace,
                inventory_override={
                    "classification": "partial",
                    "node_count": 0,
                    "edge_count": 0,
                    "graph_sha256": "0" * 64,
                    "status_counts": {},
                },
            ),
        }
        for name, factory in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(ProvenancePerformanceError):
                    run_matrix_cell(
                        factory,
                        graph,
                        concurrency=1,
                        repetitions=1,
                        operations_per_type=1,
                        run_id=f"fail-{name}",
                        formal=True,
                    )


class ProvenanceAggregationTests(unittest.TestCase):
    def _report(self):
        samples = [
            {
                "cell_id": "n10-c1",
                "repetition": 0,
                "operation": "read",
                "latency_ns": 10,
                "success": True,
                "retry_count": 0,
            },
            {
                "cell_id": "n10-c1",
                "repetition": 0,
                "operation": "derive",
                "latency_ns": 20,
                "success": True,
                "retry_count": 1,
            },
            {
                "cell_id": "n10-c1",
                "repetition": 0,
                "operation": "search",
                "latency_ns": 1_000_000_000,
                "success": False,
                "retry_count": 2,
                "error_class": "TimeoutError",
            },
            {
                "cell_id": "n10-c1",
                "repetition": 1,
                "operation": "read",
                "latency_ns": 30,
                "success": True,
                "retry_count": 0,
            },
        ]
        repetitions = [
            {
                "cell_id": "n10-c1",
                "repetition": 0,
                "elapsed_ns": 1_000_000_000,
                "success_count": 2,
                "failure_count": 1,
                "retry_count": 3,
                "eligible_for_formal": True,
                "graph_node_count": 10,
                "concurrency": 1,
            },
            {
                "cell_id": "n10-c1",
                "repetition": 1,
                "elapsed_ns": 2_000_000_000,
                "success_count": 1,
                "failure_count": 0,
                "retry_count": 0,
                "eligible_for_formal": True,
                "graph_node_count": 10,
                "concurrency": 1,
            },
        ]
        return {"samples": samples, "repetitions": repetitions}

    def test_aggregate_uses_only_successes_for_latency_and_throughput_numerator(self):
        aggregate = aggregate_matrix(self._report(), bootstrap_repetitions=200, seed=17)
        row = aggregate["rows"][0]
        self.assertEqual(row["successful_operation_count"], 3)
        self.assertEqual(row["failed_operation_count"], 1)
        self.assertAlmostEqual(row["successful_throughput_ops_per_second"], 1.0)
        self.assertLessEqual(row["p50_latency_ns"], row["p95_latency_ns"])
        self.assertLessEqual(row["p95_latency_ns"], row["p99_latency_ns"])
        self.assertLess(row["p99_latency_ns"], 1_000_000_000)
        self.assertEqual(row["retry_count"], 3)

    def test_repetition_cluster_bootstrap_is_deterministic(self):
        first = aggregate_matrix(self._report(), bootstrap_repetitions=500, seed=17)
        second = aggregate_matrix(self._report(), bootstrap_repetitions=500, seed=17)
        self.assertEqual(first, second)
        interval = first["rows"][0]["successful_throughput_95ci"]
        self.assertLessEqual(interval["lower"], interval["estimate"])
        self.assertLessEqual(interval["estimate"], interval["upper"])

    def test_ineligible_repetition_is_not_silently_aggregated(self):
        report = self._report()
        report["repetitions"][1]["eligible_for_formal"] = False
        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(report, bootstrap_repetitions=100, seed=17)

    def test_diagnostic_aggregate_requires_explicit_opt_in(self):
        report = self._report()
        report["repetitions"][1]["eligible_for_formal"] = False
        report["repetitions"][1]["eligible_for_diagnostic"] = True

        aggregate = aggregate_matrix(
            report,
            bootstrap_repetitions=100,
            seed=17,
            require_formal=False,
        )

        self.assertEqual(aggregate["evidence_scope"], "diagnostic")
        self.assertEqual(aggregate["rows"][0]["repetition_count"], 2)


if __name__ == "__main__":
    unittest.main()
