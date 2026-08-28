import copy
import hashlib
import json
import math
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import txnmem_provenance_performance as provenance_module
import txnmem_provenance_execution_collector as collector_module
from txnmem_backend import InstrumentedMemoryBackend
from txnmem_provenance_performance import (
    ProvenancePerformanceError,
    aggregate_matrix,
    build_layered_dag,
    candidate_attestation_material,
    canonical_graph_sha256,
    expand_matrix,
    formal_config_file_sha256,
    formal_matrix_config_sha256,
    formal_matrix_workload_sha256,
    load_strict_json_file,
    provenance_bundle_id,
    promote_provenance_candidate,
    publish_provenance_bundle,
    run_matrix_cell,
    validate_matrix_config,
)
from txnmem_topology_attestation import (
    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
    FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
    execution_authorization_proof,
    sanitize_topology_attestation,
)
from txnmem_toxiproxy_metrics import (
    derive_proxy_counter_deltas,
    proxy_counter_payload_sha256,
)


def _proxy_snapshot(phase, *, qdrant, neo4j):
    routes = []
    for identity, values in zip(
        (
            ("qdrant", "txnmem-qdrant", "0.0.0.0:19000", "qdrant:6333"),
            ("neo4j", "txnmem-neo4j", "0.0.0.0:19001", "neo4j:7687"),
        ),
        (qdrant, neo4j),
    ):
        routes.append(
            {
                "role": identity[0],
                "proxy_name": identity[1],
                "listener": identity[2],
                "upstream": identity[3],
                "received_upstream_bytes": values[0],
                "sent_upstream_bytes": values[1],
                "received_downstream_bytes": values[2],
                "sent_downstream_bytes": values[3],
                "total_bytes": sum(values),
            }
        )
    document = {
        "schema": "txnmem-provenance-proxy-counters-v1",
        "phase": phase,
        "routes": routes,
        "toxiproxy_total_bytes": sum(row["total_bytes"] for row in routes),
    }
    document["snapshot_sha256"] = hashlib.sha256(
        provenance_module._canonical_json_bytes(document)
    ).hexdigest()
    return document


class _FixtureBackend(InstrumentedMemoryBackend):
    supports_parallel_provenance_preload = True

    def __init__(
        self,
        namespace,
        *,
        available=True,
        isolated=True,
        inventory_override=None,
        qdrant_version="1.15.4",
        neo4j_version="5.26.0",
    ):
        super().__init__()
        self.namespace = namespace
        self.available = available
        self.isolated = isolated
        self.inventory_override = inventory_override
        self.qdrant_version = qdrant_version
        self.neo4j_version = neo4j_version
        self.max_retries = 0
        self.neo4j_max_transaction_retry_time_seconds = 0.0
        self._retry_count = 0
        self._preload_lock = threading.Lock()

    def preload_provenance_record(self, memory_id, source_ids, *, value):
        with self._preload_lock:
            time.sleep(0.001)
            if source_ids:
                self.derive(memory_id, source_ids, value=value)
            else:
                self.write(memory_id, value=value)
        return 0

    def healthcheck(self):
        return {
            "qdrant": {"available": self.available, "version": self.qdrant_version},
            "neo4j": {"available": self.available, "version": self.neo4j_version},
        }

    def performance_environment(self):
        return {
            "schema": "txnmem-provenance-environment-v1",
            "isolation_verified": self.isolated,
            "co_tenant_load_detected": not self.isolated,
            "source": "host-observation-v1",
            "cpu_logical_count": 8,
            "memory_total_bytes": 16 * 1024**3,
            "disk_medium": "ssd",
            "toxiproxy_version": "2.9.0",
        }

    def metrics(self):
        return {"retry_count": self._retry_count}

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
    def _formal_config(self):
        root = Path(__file__).resolve().parents[1]
        return json.loads(
            (root / "configs" / "provenance_performance_matrix.json").read_text(
                encoding="utf-8"
            )
        )

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
        self.assertIn("/opt/txnmem-formal-controller/txnmem_formal_controller.py", script)
        self.assertIn("/usr/bin/python3 -I -S -B", script)
        self.assertIn("/usr/bin/env -i", script)
        self.assertNotIn("PYTHONPATH", script)
        self.assertIn("--launch-out", script)
        self.assertIn("--completion-out", script)
        self.assertNotIn("--environment-attestation", script)
        self.assertNotIn("--formal", script)
        self.assertNotIn("--topology-attestation", script)
        self.assertIn("diagnostic candidate", script)
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

    def test_formal_config_is_closed_exact_and_finite(self):
        config = self._formal_config()
        validated = validate_matrix_config(config, formal=True)
        self.assertEqual(validated, config)

        mutations = []
        for key, value in (
            ("schema", "wrong"),
            ("graph_seed", 18),
            ("operations_per_type", 1),
            ("bootstrap_repetitions", True),
            ("bootstrap_seed", 17.0),
            ("request_timeout_seconds", math.nan),
        ):
            changed = copy.deepcopy(config)
            changed[key] = value
            mutations.append((key, changed))
        extra = copy.deepcopy(config)
        extra["unknown"] = "field"
        mutations.append(("unknown", extra))

        for name, changed in mutations:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    validate_matrix_config(changed, formal=True)

    def test_strict_json_file_rejects_duplicate_keys_nonfinite_and_symlink(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            duplicate = root / "duplicate.json"
            duplicate.write_text('{"schema":"a","schema":"b"}', encoding="utf-8")
            nonfinite = root / "nonfinite.json"
            nonfinite.write_text('{"value":NaN}', encoding="utf-8")
            target = root / "target.json"
            target.write_text('{"value":1}', encoding="utf-8")
            linked = root / "linked.json"
            linked.symlink_to(target)
            real_parent = root / "real-parent"
            real_parent.mkdir()
            parent_target = real_parent / "config.json"
            parent_target.write_text('{"value":1}', encoding="utf-8")
            parent_link = root / "parent-link"
            parent_link.symlink_to(real_parent, target_is_directory=True)

            for source in (duplicate, nonfinite, linked, parent_link / "config.json"):
                with self.subTest(source=source.name):
                    with self.assertRaises(ValueError):
                        load_strict_json_file(source)

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

    def test_matrix_progress_callback_reports_only_completed_repetitions(self):
        snapshots = []

        report = run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            build_layered_dag(2, seed=17),
            concurrency=1,
            repetitions=2,
            operations_per_type=1,
            run_id="progress-fixture",
            formal=True,
            progress_callback=snapshots.append,
        )

        self.assertEqual(len(report["repetitions"]), 2)
        self.assertEqual(len(report["samples"]), 8)
        self.assertEqual(
            snapshots,
            [
                {
                    "cell_id": "n2-c1",
                    "completed_repetition_count": 1,
                    "completed_operation_sample_count": 4,
                },
                {
                    "cell_id": "n2-c1",
                    "completed_repetition_count": 2,
                    "completed_operation_sample_count": 8,
                },
            ],
        )

    def test_protected_diagnostic_stops_at_first_ineligible_repetition(self):
        created = []
        snapshots = []

        def factory(namespace):
            created.append(namespace)
            return _FixtureBackend(namespace, isolated=False)

        with self.assertRaisesRegex(
            ProvenancePerformanceError,
            "verified isolation",
        ):
            run_matrix_cell(
                factory,
                build_layered_dag(2, seed=17),
                concurrency=1,
                repetitions=3,
                operations_per_type=1,
                run_id="protected-first-ineligible",
                formal=False,
                require_formal_eligibility=True,
                progress_callback=snapshots.append,
            )

        self.assertEqual(len(created), 1)
        self.assertEqual(snapshots, [])

    def test_protected_diagnostic_stops_after_completed_repetitions(self):
        created = []
        snapshots = []

        def factory(namespace):
            created.append(namespace)
            return _FixtureBackend(namespace, isolated=len(created) != 3)

        with self.assertRaisesRegex(
            ProvenancePerformanceError,
            "verified isolation",
        ):
            run_matrix_cell(
                factory,
                build_layered_dag(2, seed=17),
                concurrency=1,
                repetitions=4,
                operations_per_type=1,
                run_id="protected-third-ineligible",
                formal=False,
                require_formal_eligibility=True,
                progress_callback=snapshots.append,
            )

        self.assertEqual(len(created), 3)
        self.assertEqual(
            snapshots,
            [
                {
                    "cell_id": "n2-c1",
                    "completed_repetition_count": 1,
                    "completed_operation_sample_count": 4,
                },
                {
                    "cell_id": "n2-c1",
                    "completed_repetition_count": 2,
                    "completed_operation_sample_count": 8,
                },
            ],
        )

    def test_diagnostic_mode_retains_formally_ineligible_repetitions(self):
        created = []

        def factory(namespace):
            created.append(namespace)
            return _FixtureBackend(namespace, isolated=len(created) != 3)

        report = run_matrix_cell(
            factory,
            build_layered_dag(2, seed=17),
            concurrency=1,
            repetitions=3,
            operations_per_type=1,
            run_id="diagnostic-third-ineligible",
            formal=False,
            require_formal_eligibility=False,
        )

        self.assertFalse(report["formal_requested"])
        self.assertEqual(len(created), 3)
        self.assertEqual(len(report["repetitions"]), 3)
        self.assertFalse(report["repetitions"][2]["eligible_for_formal"])

    def test_require_formal_eligibility_requires_an_exact_boolean(self):
        graph = build_layered_dag(2, seed=17)

        for value in (0, 1, "true", None):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "require_formal_eligibility must be a boolean",
                ):
                    run_matrix_cell(
                        lambda namespace: _FixtureBackend(namespace),
                        graph,
                        concurrency=1,
                        repetitions=1,
                        operations_per_type=1,
                        run_id="invalid-require-formal-eligibility",
                        require_formal_eligibility=value,
                    )

    def test_real_backend_preload_uses_fixed_layered_parallelism_and_accounts_repair(self):
        class ParallelPreloadBackend:
            supports_parallel_provenance_preload = True

            def __init__(self):
                self.completed = set()
                self.lock = threading.Lock()
                self.active = 0
                self.peak = 0

            def preload_provenance_record(self, memory_id, source_ids, *, value):
                with self.lock:
                    if not set(source_ids).issubset(self.completed):
                        raise AssertionError("preload crossed a DAG layer boundary")
                    self.active += 1
                    self.peak = max(self.peak, self.active)
                time.sleep(0.002)
                with self.lock:
                    self.completed.add(memory_id)
                    self.active -= 1
                return int(memory_id.endswith("000029"))

        graph = build_layered_dag(100, seed=17)
        backend = ParallelPreloadBackend()

        metadata = provenance_module._preload_graph(backend, graph)

        self.assertEqual(backend.completed, set(graph.nodes))
        self.assertGreater(backend.peak, 1)
        self.assertEqual(
            metadata["preload_worker_limit"],
            provenance_module.PROVENANCE_PRELOAD_WORKERS,
        )
        self.assertGreater(metadata["preload_observed_peak_concurrency"], 1)
        self.assertLessEqual(
            metadata["preload_observed_peak_concurrency"],
            metadata["preload_worker_limit"],
        )
        self.assertEqual(metadata["setup_repair_limit_per_record"], 1)
        self.assertEqual(metadata["setup_repair_count"], 1)

    def test_vector_graph_factory_reuses_clients_and_closes_shared_driver_once(self):
        class SharedNeo4j:
            def __init__(self):
                self.close_count = 0

            def close(self):
                self.close_count += 1

        shared_qdrant = object()
        shared_neo4j = SharedNeo4j()
        constructed = []

        class Backend:
            def __init__(self, *args, **kwargs):
                constructed.append((args, kwargs))

        environment = _FixtureBackend("factory").performance_environment()
        with patch(
            "txnmem_vector_graph_backend._QdrantHTTPClient",
            return_value=shared_qdrant,
        ) as qdrant_constructor, patch(
            "txnmem_vector_graph_backend._Neo4jBoltClient",
            return_value=shared_neo4j,
        ) as neo4j_constructor, patch(
            "txnmem_vector_graph_backend.VectorGraphMemoryBackend", Backend
        ):
            factory = provenance_module.make_vector_graph_backend_factory(
                qdrant_url="http://qdrant",
                neo4j_uri="bolt://neo4j",
                neo4j_auth=("neo4j", "password"),
                environment_attestation=environment,
                request_timeout_seconds=30.0,
            )
            factory("namespace-a")
            factory("namespace-b")
            factory.close()
            factory.close()

        qdrant_constructor.assert_called_once()
        neo4j_constructor.assert_called_once_with(
            "bolt://neo4j",
            ("neo4j", "password"),
            notifications_min_severity="OFF",
            migrate_legacy=False,
            request_timeout_seconds=30.0,
        )
        self.assertEqual(len(constructed), 2)
        self.assertTrue(
            all(call[1]["qdrant_client"] is shared_qdrant for call in constructed)
        )
        self.assertTrue(
            all(call[1]["neo4j_client"] is shared_neo4j for call in constructed)
        )
        self.assertTrue(all(call[1]["close_clients"] is False for call in constructed))
        self.assertEqual(shared_neo4j.close_count, 1)

    def test_vector_graph_factory_rejects_invalid_timeout_before_client_construction(self):
        class TimeoutSubclass(float):
            pass

        invalid_timeouts = (
            True,
            0.0,
            -1.0,
            float("nan"),
            float("inf"),
            TimeoutSubclass(30.0),
        )
        environment = _FixtureBackend("factory-timeout").performance_environment()
        with patch(
            "txnmem_vector_graph_backend._QdrantHTTPClient"
        ) as qdrant_constructor, patch(
            "txnmem_vector_graph_backend._Neo4jBoltClient"
        ) as neo4j_constructor:
            for timeout in invalid_timeouts:
                with self.subTest(timeout=timeout):
                    with self.assertRaisesRegex(
                        ValueError,
                        "request_timeout_seconds",
                    ):
                        provenance_module.make_vector_graph_backend_factory(
                            qdrant_url="http://qdrant",
                            neo4j_uri="bolt://neo4j",
                            neo4j_auth=("neo4j", "password"),
                            environment_attestation=environment,
                            request_timeout_seconds=timeout,
                        )
        qdrant_constructor.assert_not_called()
        neo4j_constructor.assert_not_called()

    def test_vector_graph_factory_close_failure_remains_retryable(self):
        class SharedNeo4j:
            def __init__(self):
                self.close_count = 0

            def close(self):
                self.close_count += 1
                if self.close_count == 1:
                    raise RuntimeError("close boundary failed")

        shared_neo4j = SharedNeo4j()
        environment = _FixtureBackend("factory-close").performance_environment()
        with patch(
            "txnmem_vector_graph_backend._QdrantHTTPClient",
            return_value=object(),
        ), patch(
            "txnmem_vector_graph_backend._Neo4jBoltClient",
            return_value=shared_neo4j,
        ):
            factory = provenance_module.make_vector_graph_backend_factory(
                qdrant_url="http://qdrant",
                neo4j_uri="bolt://neo4j",
                neo4j_auth=("neo4j", "password"),
                environment_attestation=environment,
            )

        with self.assertRaisesRegex(RuntimeError, "close boundary"):
            factory.close()
        factory.close()

        self.assertEqual(shared_neo4j.close_count, 2)

    def test_vector_graph_factory_does_not_close_during_backend_construction(self):
        entered = threading.Event()
        release = threading.Event()

        class SharedNeo4j:
            def __init__(self):
                self.close_count = 0

            def close(self):
                self.close_count += 1

        shared_neo4j = SharedNeo4j()

        class BlockingBackend:
            def __init__(self, *args, **kwargs):
                entered.set()
                if not release.wait(timeout=5):
                    raise AssertionError("backend construction release timed out")

        environment = _FixtureBackend("factory-race").performance_environment()
        with patch(
            "txnmem_vector_graph_backend._QdrantHTTPClient",
            return_value=object(),
        ), patch(
            "txnmem_vector_graph_backend._Neo4jBoltClient",
            return_value=shared_neo4j,
        ), patch(
            "txnmem_vector_graph_backend.VectorGraphMemoryBackend",
            BlockingBackend,
        ):
            factory = provenance_module.make_vector_graph_backend_factory(
                qdrant_url="http://qdrant",
                neo4j_uri="bolt://neo4j",
                neo4j_auth=("neo4j", "password"),
                environment_attestation=environment,
            )
            created = []
            creator = threading.Thread(
                target=lambda: created.append(factory("namespace"))
            )
            closer = threading.Thread(target=factory.close)
            creator.start()
            self.assertTrue(entered.wait(timeout=5))
            closer.start()
            try:
                time.sleep(0.02)
                self.assertEqual(shared_neo4j.close_count, 0)
            finally:
                release.set()
                creator.join(timeout=5)
                closer.join(timeout=5)

        self.assertFalse(creator.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertEqual(len(created), 1)
        self.assertEqual(shared_neo4j.close_count, 1)

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

    def test_formal_environment_attestation_rejects_secret_bearing_fields(self):
        graph = build_layered_dag(10, seed=17)

        with self.assertRaises(ProvenancePerformanceError):
            run_matrix_cell(
                lambda namespace: _FixtureBackend(namespace),
                graph,
                concurrency=1,
                repetitions=1,
                operations_per_type=1,
                run_id="secret-attestation",
                formal=True,
                environment_attestation={
                    "schema": "txnmem-provenance-environment-v1",
                    "isolation_verified": True,
                    "co_tenant_load_detected": False,
                    "source": "host-observation-v1",
                    "cpu_logical_count": 8,
                    "memory_total_bytes": 16 * 1024**3,
                    "disk_medium": "ssd",
                    "toxiproxy_version": "2.9.0",
                    "password": "must-not-be-hashed-into-evidence",
                },
            )

    def test_formal_environment_rejects_sensitive_source_nonfinite_and_bad_versions(self):
        graph = build_layered_dag(10, seed=17)
        base = _FixtureBackend("fixture").performance_environment()
        cases = []
        source_url = copy.deepcopy(base)
        source_url["source"] = "https://alice:secret@203.0.113.10:6333/private"
        cases.append(("source", source_url, {}))
        nonfinite = copy.deepcopy(base)
        nonfinite["memory_total_bytes"] = math.nan
        cases.append(("nonfinite", nonfinite, {}))
        cases.append(
            (
                "health-version",
                base,
                {"qdrant_version": "https://alice:secret@203.0.113.10:6333"},
            )
        )
        cases.append(("health-ip", base, {"qdrant_version": "198.51.100.7"}))
        cases.append(("health-short-ip", base, {"qdrant_version": "127.1"}))
        cases.append(("health-three-part-ip", base, {"qdrant_version": "203.0.113"}))
        toxiproxy_ip = copy.deepcopy(base)
        toxiproxy_ip["toxiproxy_version"] = "203.0.113.10"
        cases.append(("toxiproxy-ip", toxiproxy_ip, {}))
        toxiproxy_secret = copy.deepcopy(base)
        toxiproxy_secret["toxiproxy_version"] = "1.2-password_demoSecret"
        cases.append(("toxiproxy-secret", toxiproxy_secret, {}))
        toxiproxy_three_part_ip = copy.deepcopy(base)
        toxiproxy_three_part_ip["toxiproxy_version"] = "203.0.113"
        cases.append(("toxiproxy-three-part-ip", toxiproxy_three_part_ip, {}))

        for name, attestation, backend_kwargs in cases:
            with self.subTest(name=name):
                with self.assertRaises(ProvenancePerformanceError):
                    run_matrix_cell(
                        lambda namespace: _FixtureBackend(namespace, **backend_kwargs),
                        graph,
                        concurrency=1,
                        repetitions=1,
                        operations_per_type=1,
                        run_id=f"unsafe-{name}",
                        formal=True,
                        environment_attestation=attestation,
                    )

    def test_formal_inventory_rejects_bool_counts(self):
        graph = build_layered_dag(1, seed=17)
        inventory = {
            "classification": "complete",
            "node_count": True,
            "edge_count": False,
            "graph_sha256": graph.graph_sha256,
            "status_counts": {"active": True},
        }
        with self.assertRaises(ProvenancePerformanceError):
            run_matrix_cell(
                lambda namespace: _FixtureBackend(
                    namespace, inventory_override=inventory
                ),
                graph,
                concurrency=1,
                repetitions=1,
                operations_per_type=1,
                run_id="bool-inventory",
                formal=True,
            )

    def test_formal_run_rejects_retry_metric_delta_and_records_peak_concurrency(self):
        graph = build_layered_dag(10, seed=17)

        class RetryingBackend(_FixtureBackend):
            def metrics(self):
                self._retry_count += 1
                return {"retry_count": self._retry_count}

        with self.assertRaises(ProvenancePerformanceError):
            run_matrix_cell(
                lambda namespace: RetryingBackend(namespace),
                graph,
                concurrency=2,
                repetitions=1,
                operations_per_type=1,
                run_id="hidden-retry",
                formal=True,
            )

        report = run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            graph,
            concurrency=2,
            repetitions=1,
            operations_per_type=1,
            run_id="peak-concurrency",
            formal=True,
        )
        peak = report["repetitions"][0]["observed_peak_concurrency"]
        self.assertEqual(peak, 2)


class ProvenanceAggregationTests(unittest.TestCase):
    AUTHORIZATION_NONCE = b"provenance-fixture-authorization-nonce-0001"
    @staticmethod
    def _small_formal_config(concurrency_levels=None):
        return {
            "schema": "txnmem-provenance-performance-v2",
            "graph_node_counts": [10],
            "concurrency_levels": list(concurrency_levels or [1]),
            "repetitions": 2,
            "graph_seed": 17,
            "operations_per_type": 1,
            "bootstrap_repetitions": 100,
            "bootstrap_seed": 17,
            "request_timeout_seconds": 30.0,
        }

    def _topology_attestation(self, reports, config, service_versions=None):
        service_versions = service_versions or {}
        run_hash = reports[0]["run_id_sha256"]
        environment_hash = reports[0]["repetitions"][0]["environment"][
            "attestation_sha256"
        ]
        samples = [row for report in reports for row in report["samples"]]
        repetitions = [
            row for report in reports for row in report["repetitions"]
        ]
        config_hash = formal_matrix_config_sha256()
        try:
            config_file_hash = provenance_module.formal_config_file_sha256()
        except ValueError:
            config_file_hash = "f" * 64
        material = {
            "schema": "txnmem-provenance-candidate-attestation-material-v1",
            "candidate_bundle_id": provenance_bundle_id(
                config_sha256=config_hash,
                run_id_sha256=run_hash,
                formal=False,
                backend="vector-graph",
            ),
            "run_id_sha256": run_hash,
            "config_sha256": config_hash,
            "config_file_sha256": config_file_hash,
            "workload_sha256": formal_matrix_workload_sha256(),
            "environment_attestation_sha256": environment_hash,
            "evidence_manifest_sha256": provenance_module.cell_reports_sha256(reports),
            "matrix_cell_count": len(config["graph_node_counts"])
            * len(config["concurrency_levels"]),
            "repetition_count": len(config["graph_node_counts"])
            * len(config["concurrency_levels"])
            * config["repetitions"],
            "operation_sample_count": len(config["graph_node_counts"])
            * len(config["concurrency_levels"])
            * config["repetitions"]
            * config["operations_per_type"]
            * 4,
            "observed_service_versions": {
                "qdrant": service_versions.get("qdrant", "1.15.4"),
                "neo4j": service_versions.get("neo4j", "5.26.0"),
                "toxiproxy": service_versions.get("toxiproxy", "2.9.0"),
            },
            "candidate_operation_samples_sha256": provenance_module.canonical_jsonl_sha256(
                samples
            ),
            "candidate_repetitions_sha256": provenance_module.canonical_jsonl_sha256(
                repetitions
            ),
        }
        return self._topology_from_candidate_material(material)

    def _topology_from_candidate_material(self, material, candidate_seal=None):
        source_manifest = {
            "schema": "txnmem-provenance-source-manifest-v1",
            "source_commit": "a" * 40,
            "files": [
                {
                    "path": "src/txnmem_experiment.py",
                    "blob_sha256": "b" * 64,
                }
            ],
        }
        shared = {
            "collector_id": "txnmem-provenance-execution-collector-v1",
            "formal_execution_requested": True,
            "run_id_sha256": material["run_id_sha256"],
            "config_sha256": material["config_sha256"],
            "config_file_sha256": material["config_file_sha256"],
            "workload_sha256": material["workload_sha256"],
            "environment_attestation_sha256": material[
                "environment_attestation_sha256"
            ],
            "source_commit": "a" * 40,
            "source_manifest": source_manifest,
            "source_manifest_sha256": hashlib.sha256(
                provenance_module._canonical_json_bytes(source_manifest)
            ).hexdigest(),
            "collector_sha256": "c" * 64,
            "runner_sha256": "d" * 64,
            "transport": "local_loopback",
            "matrix_cell_count": material["matrix_cell_count"],
            "repetition_count": material["repetition_count"],
            "operation_sample_count": material["operation_sample_count"],
        }
        command_manifest = {
            "schema": "txnmem-provenance-command-manifest-v3",
            "transport": "local_loopback",
            "argv_sha256": "e" * 64,
            "argv_template": [
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
            ],
            "python_executable_path_sha256": "f" * 64,
            "python_executable_sha256": "0" * 64,
            "python_implementation": "CPython",
            "python_version": "3.11.9",
            "runtime_manifest": {
                "schema": "txnmem-provenance-runtime-manifest-v1",
                "python": {
                    "implementation": "CPython",
                    "version": "3.11.9",
                    "executable_sha256": "0" * 64,
                    "build_sha256": "8" * 64,
                    "compiler_sha256": "9" * 64,
                    "platform_sha256": "a" * 64,
                },
                "distributions": [
                    {
                        "name": "neo4j",
                        "version": "5.28.1",
                        "files": [
                            {
                                "path": "neo4j/__init__.py",
                                "sha256": "b" * 64,
                            }
                        ],
                        "files_sha256": hashlib.sha256(
                            provenance_module._canonical_json_bytes(
                                [
                                    {
                                        "path": "neo4j/__init__.py",
                                        "sha256": "b" * 64,
                                    }
                                ]
                            )
                        ).hexdigest(),
                        "declared_requirements_sha256": "c" * 64,
                    }
                ],
            },
            "working_directory_sha256": "1" * 64,
            "source_manifest_sha256": shared["source_manifest_sha256"],
            "runner_sha256": shared["runner_sha256"],
            "config_file_sha256": shared["config_file_sha256"],
            "run_id_sha256": shared["run_id_sha256"],
            "candidate_root_sha256": "2" * 64,
            "environment_attestation_sha256": shared[
                "environment_attestation_sha256"
            ],
            "environment_attestation_file_sha256": "d" * 64,
            "qdrant_endpoint_sha256": "3" * 64,
            "qdrant_endpoint_port": 19000,
            "neo4j_endpoint_sha256": "4" * 64,
            "neo4j_endpoint_port": 19001,
            "toxiproxy_endpoint_sha256": "7" * 64,
            "literal_environment": {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            "hashed_environment": {
                "TXNMEM_NEO4J_URI": "5" * 64,
                "TXNMEM_NEO4J_USER": "6" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": "8" * 64,
            },
            "secret_environment_variables": ["TXNMEM_NEO4J_PASSWORD"],
            "gate_environment_variable": "TXNMEM_PROVENANCE_START_GATE_FD",
            "ready_environment_variable": "TXNMEM_PROVENANCE_READY_FD",
            "completion_environment_variable": "TXNMEM_PROVENANCE_COMPLETION_FD",
            "completion_receipt_required": True,
            "progress_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_FD",
            "progress_binding_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256",
            "progress_channel_required": True,
            "backend_timeout_policy": {
                "qdrant_request_seconds": 30.0,
                "neo4j_connection_seconds": 30.0,
                "neo4j_connection_acquisition_seconds": 30.0,
                "neo4j_transaction_query_seconds": 30.0,
            },
            "runtime_environment_variable": "TXNMEM_PROVENANCE_RUNTIME_SITE",
            "inherited_environment": False,
        }
        command_manifest["runtime_manifest_sha256"] = hashlib.sha256(
            provenance_module._canonical_json_bytes(
                command_manifest["runtime_manifest"]
            )
        ).hexdigest()
        command_manifest["runtime_lock_file_sha256"] = "9" * 64
        command_manifest["runtime_snapshot_path_sha256"] = "8" * 64
        command_manifest["external_tools"] = [
            {
                "role": role,
                "requested_path_sha256": value * 64,
                "resolved_path_sha256": value * 64,
                "executable_sha256": "0" * 64 if role == "python" else value * 64,
                "owner_uid": 0,
                "mode": 0o555,
            }
            for role, value in (
                ("docker", "1"),
                ("git", "2"),
                ("nft", "3"),
                ("python", "4"),
            )
        ]
        progress_binding = {
            "schema": "txnmem-provenance-progress-binding-v1",
            "source_manifest_sha256": command_manifest["source_manifest_sha256"],
            "argv_sha256": command_manifest["argv_sha256"],
            "config_file_sha256": command_manifest["config_file_sha256"],
            "run_id_sha256": command_manifest["run_id_sha256"],
            "candidate_root_sha256": command_manifest["candidate_root_sha256"],
        }
        command_manifest["progress_binding_sha256"] = hashlib.sha256(
            provenance_module._canonical_json_bytes(progress_binding)
        ).hexdigest()
        child_identity = "candidate-process:4321:fixture-start"
        shared["command_manifest"] = command_manifest
        shared["command_sha256"] = hashlib.sha256(
            provenance_module._canonical_json_bytes(command_manifest)
        ).hexdigest()
        shared["child_process"] = {
            "pid": 4321,
            "start_identity": child_identity,
            "uid": 65532,
            "executable_sha256": command_manifest["python_executable_sha256"],
            "argv_sha256": command_manifest["argv_sha256"],
            "cmdline_sha256": "a" * 64,
        }
        shared["network_guard"] = {
            "schema": "txnmem-provenance-network-guard-v3",
            "table_name_sha256": "b" * 64,
            "runner_uid": 65532,
            "controller_uid": 0,
            "allowed_ipv4_loopback_ports": [19000, 19001],
            "allowed_root_ingress_ports": [8474, 19000, 19001],
            "root_ingress_destination_exact": True,
            "management_port_root_only": True,
            "non_runner_proxy_traffic_blocked": True,
            "host_bridge_access_blocked": True,
            "forwarded_bridge_access_blocked": True,
            "backend_ipv4_subnet_sha256": "8" * 64,
            "ingress_ipv4_subnet_sha256": "9" * 64,
            "backend_bridge_interface_sha256": "0" * 64,
            "ingress_bridge_interface_sha256": "1" * 64,
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                b"172.20.0.2"
            ).hexdigest(),
            "policy_sha256": "c" * 64,
            "ruleset_sha256": "d" * 64,
        }
        from txnmem_provenance_contract import (
            FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
        )

        shared["backend_isolation"] = {
            "schema": "txnmem-provenance-backend-isolation-v3",
            "network_name_sha256": "e" * 64,
            "network_id_sha256": "f" * 64,
            "ingress_network_name_sha256": "6" * 64,
            "ingress_network_id_sha256": "7" * 64,
            "toxiproxy_ingress_ipv4": "172.20.0.2",
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                b"172.20.0.2"
            ).hexdigest(),
            "toxiproxy_ingress_endpoint_id_sha256": hashlib.sha256(
                ("4" * 64).encode("utf-8")
            ).hexdigest(),
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
            "backend_ipv4_subnet_sha256": "8" * 64,
            "ingress_ipv4_subnet_sha256": "9" * 64,
            "backend_bridge_interface_sha256": "0" * 64,
            "ingress_bridge_interface_sha256": "1" * 64,
            "networks_non_attachable": True,
            "networks_non_swarm_ingress": True,
            "networks_non_config_only": True,
            "direct_backend_ports_unpublished": True,
            "proxy_ports_loopback_only": True,
            "published_proxy_ports": [8474, 19000, 19001],
            "containers": [
                {
                    "role": role,
                    "container_id_sha256": value * 64,
                    "runtime_image_id_sha256": {
                        "qdrant": "d",
                        "neo4j": "e",
                        "toxiproxy": "f",
                    }[role]
                    * 64,
                    "manifest_digest": FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[
                        role
                    ],
                }
                for role, value in (
                    ("qdrant", "a"),
                    ("neo4j", "b"),
                    ("toxiproxy", "c"),
                )
            ],
        }
        versions = {
            "client": "3.11.9",
            **material["observed_service_versions"],
        }
        launch_roles = [
            {
                "role": role,
                "host_identity": "candidate-client"
                if role in {"client", "toxiproxy"}
                else "candidate-backend",
                "listener_owner": (
                    child_identity if role == "client" else f"{role}-owner"
                ),
                "service_version": versions[role],
                "rtt_ms": 0.1,
            }
            for role in ("client", "qdrant", "neo4j", "toxiproxy")
        ]
        completion_roles = copy.deepcopy(launch_roles)
        baseline_a = _proxy_snapshot(
            "baseline_a",
            qdrant=(11, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )
        baseline_b = _proxy_snapshot(
            "baseline_b",
            qdrant=(11, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )
        final = _proxy_snapshot(
            "final",
            qdrant=(21, 33, 47, 69),
            neo4j=(73, 89, 101, 127),
        )
        proxy_deltas = derive_proxy_counter_deltas(baseline_b, final)
        proxy_routes = [
            {
                "role": "qdrant",
                "proxy_name": "txnmem-qdrant",
                "listen": "0.0.0.0:19000",
                "upstream": "qdrant:6333",
                "enabled": True,
                "toxics_count": 0,
            },
            {
                "role": "neo4j",
                "proxy_name": "txnmem-neo4j",
                "listen": "0.0.0.0:19001",
                "upstream": "neo4j:7687",
                "enabled": True,
                "toxics_count": 0,
            },
        ]
        launch = {
            "schema": "txnmem-provenance-execution-launch-raw-v4",
            **shared,
            "roles": launch_roles,
            "proxy_routes": copy.deepcopy(proxy_routes),
            "proxy_counter_baseline_a": baseline_a,
            "proxy_counter_baseline_b": baseline_b,
            "proxy_route_rearm_verified": True,
            "authorization_nonce_sha256": hashlib.sha256(
                self.AUTHORIZATION_NONCE
            ).hexdigest(),
        }
        launch["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, launch
        )
        launch_raw = (
            json.dumps(launch, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        completion = {
            "schema": "txnmem-provenance-execution-completion-raw-v5",
            **shared,
            "launch_file_sha256": hashlib.sha256(launch_raw).hexdigest(),
            "exit_code": 0,
            "candidate_bundle_id": material["candidate_bundle_id"],
            "evidence_manifest_sha256": material["evidence_manifest_sha256"],
            "candidate_operation_samples_sha256": material[
                "candidate_operation_samples_sha256"
            ],
            "candidate_repetitions_sha256": material[
                "candidate_repetitions_sha256"
            ],
            "candidate_seal": candidate_seal
            or {
                "schema": "txnmem-provenance-candidate-seal-v1",
                "root_device": 11,
                "root_inode": 22,
                "directory_count": 3,
                "file_count": 4,
                "tree_sha256": "f" * 64,
                "completion_receipt_sha256": hashlib.sha256(
                    provenance_module._canonical_json_bytes(material)
                ).hexdigest(),
            },
            "execution_monitor": {
                "schema": "txnmem-provenance-execution-monitor-v2",
                "sampling_interval_ms": 250,
                "sample_count": 3,
                "first_sample_monotonic_ns": 1_000_000_000,
                "last_sample_monotonic_ns": 1_500_000_000,
                "gate_release_monotonic_ns": 1_100_000_000,
                "child_exit_monotonic_ns": 1_400_000_000,
                "max_observed_gap_ns": 250_000_000,
                "violation_count": 0,
                "cpu_logical_count": 8,
                "load1_limit_milli": 8000,
                "max_load1_milli": 1250,
                "invariants": [
                    "backend_isolation",
                    "continuous_load_ceiling",
                    "host_environment",
                    "network_guard",
                    "runner_uid_process_set",
                    "terminal_process_exit",
                    "toxiproxy_routes",
                ],
                "samples_sha256": "1" * 64,
                "first_sample_sha256": "2" * 64,
                "last_sample_sha256": "3" * 64,
            },
            "roles": completion_roles,
            "proxy_routes": copy.deepcopy(proxy_routes),
            "proxy_counter_baseline_b_sha256": proxy_counter_payload_sha256(
                baseline_b
            ),
            "proxy_counter_final": final,
            "proxy_counter_deltas": proxy_deltas,
            "authorization_nonce_sha256": hashlib.sha256(
                self.AUTHORIZATION_NONCE
            ).hexdigest(),
        }
        completion["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, completion
        )
        completion_raw = (
            json.dumps(completion, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        ).encode("utf-8")
        with patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                launch["run_id_sha256"]: hashlib.sha256(
                    self.AUTHORIZATION_NONCE
                ).hexdigest()
            },
            clear=True,
        ):
            return sanitize_topology_attestation(
                launch,
                completion,
                launch_file_sha256=hashlib.sha256(launch_raw).hexdigest(),
                completion_file_sha256=hashlib.sha256(completion_raw).hexdigest(),
                authorization_nonce=self.AUTHORIZATION_NONCE,
            )

    def _aggregate_small_formal(self, reports, config=None, topology=None):
        config = config or self._small_formal_config()
        with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config):
            topology = topology or self._topology_attestation(reports, config)
            with patch(
                "txnmem_provenance_performance.formal_config_file_sha256",
                return_value=topology["config_file_sha256"],
            ), patch.dict(
                FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                {
                    topology["run_id_sha256"]: topology[
                        "attestation_sha256"
                    ]
                },
                clear=True,
            ), patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    topology["run_id_sha256"]: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ):
                return aggregate_matrix(
                    reports,
                    bootstrap_repetitions=config["bootstrap_repetitions"],
                    seed=config["bootstrap_seed"],
                    topology_attestation=topology,
                )

    def _valid_formal_report(self):
        graph = build_layered_dag(10, seed=17)
        return run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            graph,
            concurrency=1,
            repetitions=2,
            operations_per_type=1,
            run_id="formal-aggregate-fixture",
            formal=True,
        )

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
                "preload_method": "sequential-semantic-v1",
                "preload_worker_limit": 1,
                "preload_observed_peak_concurrency": 1,
                "setup_repair_limit_per_record": 0,
                "setup_repair_count": 0,
                "setup_elapsed_ns": 1,
                "retry_scope": "measured_operations_only",
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
                "preload_method": "sequential-semantic-v1",
                "preload_worker_limit": 1,
                "preload_observed_peak_concurrency": 1,
                "setup_repair_limit_per_record": 0,
                "setup_repair_count": 0,
                "setup_elapsed_ns": 1,
                "retry_scope": "measured_operations_only",
            },
        ]
        return {"samples": samples, "repetitions": repetitions}

    def test_aggregate_uses_only_successes_for_latency_and_throughput_numerator(self):
        aggregate = aggregate_matrix(
            self._report(),
            bootstrap_repetitions=200,
            seed=17,
            require_formal=False,
        )
        row = aggregate["rows"][0]
        self.assertEqual(row["successful_operation_count"], 3)
        self.assertEqual(row["failed_operation_count"], 1)
        self.assertAlmostEqual(row["successful_throughput_ops_per_second"], 1.0)
        self.assertLessEqual(row["p50_latency_ns"], row["p95_latency_ns"])
        self.assertLessEqual(row["p95_latency_ns"], row["p99_latency_ns"])
        self.assertLess(row["p99_latency_ns"], 1_000_000_000)
        self.assertEqual(row["retry_count"], 3)

    def test_repetition_cluster_bootstrap_is_deterministic(self):
        first = aggregate_matrix(
            self._report(), bootstrap_repetitions=500, seed=17, require_formal=False
        )
        second = aggregate_matrix(
            self._report(), bootstrap_repetitions=500, seed=17, require_formal=False
        )
        self.assertEqual(first, second)
        interval = first["rows"][0]["successful_throughput_95ci"]
        self.assertLessEqual(interval["lower"], interval["estimate"])
        self.assertLessEqual(interval["estimate"], interval["upper"])

    def test_ineligible_repetition_is_not_silently_aggregated(self):
        report = self._report()
        report["repetitions"][1]["eligible_for_formal"] = False
        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(
                report, bootstrap_repetitions=100, seed=17, require_formal=False
            )

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

    def test_formal_aggregate_rejects_samples_shifted_between_repetitions(self):
        report = self._valid_formal_report()
        source = report["repetitions"][0]
        target = report["repetitions"][1]
        shifted = next(
            row for row in report["samples"] if row["repetition"] == source["repetition"]
        )
        shifted["repetition"] = target["repetition"]
        shifted["namespace_sha256"] = target["namespace_sha256"]

        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal([report])

    def test_formal_aggregate_rejects_duplicate_namespace_identity(self):
        report = self._valid_formal_report()
        first = report["repetitions"][0]
        second = report["repetitions"][1]
        second["namespace_sha256"] = first["namespace_sha256"]
        for sample in report["samples"]:
            if sample["repetition"] == second["repetition"]:
                sample["namespace_sha256"] = first["namespace_sha256"]

        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal([report])

    def test_formal_aggregate_rejects_repetition_metadata_drift(self):
        report = self._valid_formal_report()
        report["repetitions"][1]["concurrency"] = 2

        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal([report])

    def test_formal_aggregate_rejects_invalid_preload_accounting(self):
        mutations = (
            ("preload_worker_limit", True),
            ("preload_worker_limit", 0),
            (
                "preload_worker_limit",
                provenance_module.PROVENANCE_PRELOAD_WORKERS + 1,
            ),
            ("preload_observed_peak_concurrency", 0),
            ("setup_repair_limit_per_record", 2),
            ("setup_repair_count", True),
            ("setup_repair_count", -1),
            ("setup_repair_count", 11),
            ("setup_elapsed_ns", 0),
            ("retry_scope", "all_operations"),
        )
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                report = self._valid_formal_report()
                report["repetitions"][0][field] = value
                with self.assertRaises(ProvenancePerformanceError):
                    self._aggregate_small_formal([report])

    def test_formal_aggregate_requires_exact_layered_preload_contract(self):
        mutations = (
            {
                "preload_method": "sequential-semantic-v1",
                "preload_worker_limit": 1,
                "preload_observed_peak_concurrency": 1,
                "setup_repair_limit_per_record": 0,
                "setup_repair_count": 0,
            },
            {"setup_repair_limit_per_record": 0, "setup_repair_count": 0},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                report = self._valid_formal_report()
                report["repetitions"][0].update(mutation)
                with self.assertRaises(ProvenancePerformanceError):
                    self._aggregate_small_formal([report])

        report = self._valid_formal_report()
        current_limit = report["repetitions"][0]["preload_worker_limit"]
        self.assertGreater(current_limit, 1)
        report["repetitions"][0]["preload_worker_limit"] = current_limit - 1
        report["repetitions"][0]["preload_observed_peak_concurrency"] = min(
            report["repetitions"][0]["preload_observed_peak_concurrency"],
            current_limit - 1,
        )
        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal([report])

    def test_diagnostic_aggregate_validates_v2_setup_accounting_exactly(self):
        mutations = (
            ("preload_method", "unregistered"),
            ("preload_worker_limit", True),
            ("preload_observed_peak_concurrency", -1),
            ("setup_repair_limit_per_record", True),
            ("setup_repair_count", -1),
            ("setup_elapsed_ns", True),
            ("retry_scope", "all_operations"),
        )
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                report = self._valid_formal_report()
                report["repetitions"][0][field] = value
                with self.assertRaises(ProvenancePerformanceError):
                    aggregate_matrix(
                        report,
                        bootstrap_repetitions=100,
                        seed=17,
                        require_formal=False,
                    )

        report = self._valid_formal_report()
        report["repetitions"][0].pop("setup_elapsed_ns")
        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(
                report,
                bootstrap_repetitions=100,
                seed=17,
                require_formal=False,
            )

    def test_formal_aggregate_rejects_bool_integer_and_negative_accounting(self):
        report = {
            "samples": [
                {
                    "cell_id": "n1-c1",
                    "repetition": 0,
                    "namespace_sha256": "a" * 64,
                    "operation": "read",
                    "latency_ns": 1,
                    "success": True,
                    "retry_count": 0,
                }
            ],
            "repetitions": [
                {
                    "cell_id": "n1-c1",
                    "repetition": 0,
                    "namespace_sha256": "a" * 64,
                    "elapsed_ns": True,
                    "success_count": True,
                    "failure_count": 0,
                    "retry_count": -1,
                    "eligible_for_formal": True,
                    "graph_node_count": True,
                    "concurrency": True,
                }
            ],
        }

        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(
                report,
                bootstrap_repetitions=100,
                seed=17,
            )

    def test_require_formal_must_be_an_exact_boolean(self):
        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(
                self._valid_formal_report(),
                bootstrap_repetitions=100,
                seed=17,
                require_formal=1,
            )

    def test_formal_aggregate_rejects_diagnostic_producer(self):
        graph = build_layered_dag(10, seed=17)
        report = run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            graph,
            concurrency=1,
            repetitions=2,
            operations_per_type=1,
            run_id="diagnostic-promotion",
            formal=False,
        )

        config = self._small_formal_config()
        with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config):
            topology = self._topology_attestation([report], config)
        report["formal_requested"] = True
        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal([report], config=config, topology=topology)

    def test_production_formal_aggregate_rejects_small_matrix(self):
        report = self._valid_formal_report()
        with self.assertRaises(ProvenancePerformanceError):
            aggregate_matrix(
                report,
                bootstrap_repetitions=10_000,
                seed=17,
            )

    def test_registered_test_contract_accepts_complete_multi_cell_matrix(self):
        graph = build_layered_dag(10, seed=17)
        reports = [
            run_matrix_cell(
                lambda namespace: _FixtureBackend(namespace),
                graph,
                concurrency=concurrency,
                repetitions=2,
                operations_per_type=1,
                run_id="complete-formal-matrix",
                formal=True,
            )
            for concurrency in (1, 2)
        ]

        aggregate = self._aggregate_small_formal(
            reports,
            config=self._small_formal_config(concurrency_levels=[1, 2]),
        )

        self.assertEqual(aggregate["evidence_scope"], "formal")
        self.assertEqual(len(aggregate["rows"]), 2)

    def test_production_formal_contract_is_frozen_to_15_by_30_by_32(self):
        cells = expand_matrix(provenance_module.FORMAL_MATRIX_CONFIG)
        self.assertEqual(len(cells), 15)
        self.assertEqual(sum(cell["repetitions"] for cell in cells), 450)
        self.assertEqual(
            sum(
                cell["repetitions"] * cell["operations_per_type"] * 4
                for cell in cells
            ),
            14_400,
        )
        self.assertEqual(
            provenance_module.FORMAL_MATRIX_CONFIG["bootstrap_repetitions"], 10_000
        )
        root = Path(__file__).resolve().parents[1]
        self.assertEqual(
            formal_config_file_sha256(),
            hashlib.sha256(
                (root / "configs" / "provenance_performance_matrix.json").read_bytes()
            ).hexdigest(),
        )

    def test_formal_aggregate_rejects_unregistered_attestation_and_bootstrap_drift(self):
        report = self._valid_formal_report()
        config = self._small_formal_config()
        with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config):
            topology = self._topology_attestation([report], config)
            with self.assertRaises(ProvenancePerformanceError):
                aggregate_matrix(
                    report,
                    bootstrap_repetitions=100,
                    seed=17,
                    topology_attestation=topology,
                )
            with patch.dict(
                FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                {topology["run_id_sha256"]: topology["attestation_sha256"]},
                clear=True,
            ), patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    topology["run_id_sha256"]: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ):
                with self.assertRaises(ProvenancePerformanceError):
                    aggregate_matrix(
                        report,
                        bootstrap_repetitions=10,
                        seed=17,
                        topology_attestation=topology,
                    )

    def test_formal_aggregate_rejects_peak_below_requested_concurrency(self):
        graph = build_layered_dag(10, seed=17)
        report = run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            graph,
            concurrency=2,
            repetitions=2,
            operations_per_type=1,
            run_id="peak-forgery",
            formal=True,
        )
        report["repetitions"][0]["observed_peak_concurrency"] = 1
        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal(
                [report],
                config=self._small_formal_config(concurrency_levels=[2]),
            )

    def test_formal_aggregate_cross_checks_attested_service_versions(self):
        report = self._valid_formal_report()
        config = self._small_formal_config()
        with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config):
            topology = self._topology_attestation(
                [report], config, service_versions={"qdrant": "1.11.5"}
            )

        with self.assertRaises(ProvenancePerformanceError):
            self._aggregate_small_formal(
                [report], config=config, topology=topology
            )

    def test_publisher_rejects_empty_forged_formal_bundle(self):
        config_hash = formal_matrix_config_sha256()
        run_hash = "a" * 64
        bundle_id = provenance_bundle_id(
            config_sha256=config_hash,
            run_id_sha256=run_hash,
            formal=True,
            backend="vector-graph",
        )
        forged = {
            "bundle_id": bundle_id,
            "formal_requested": True,
            "operation_samples_sha256": provenance_module.canonical_jsonl_sha256(
                []
            ),
            "repetitions_sha256": provenance_module.canonical_jsonl_sha256([]),
        }

        with TemporaryDirectory() as tmp:
            with self.assertRaises((ProvenancePerformanceError, ValueError)):
                publish_provenance_bundle(
                    tmp,
                    bundle_id=bundle_id,
                    operation_samples=[],
                    repetitions=[],
                    report=forged,
                    topology_attestation=None,
                )
            self.assertFalse((Path(tmp) / "bundles" / f"{bundle_id}.json").exists())

    def test_formal_named_bundle_cannot_dispatch_to_diagnostic_validator(self):
        config_hash = formal_matrix_config_sha256()
        run_hash = "b" * 64
        bundle_id = provenance_bundle_id(
            config_sha256=config_hash,
            run_id_sha256=run_hash,
            formal=True,
            backend="vector-graph",
        )
        forged = {
            "schema": "txnmem-provenance-performance-report-v1",
            "backend": "vector-graph",
            "formal_requested": False,
            "bundle_id": bundle_id,
            "publication_status": "complete",
            "production_backend_claim": False,
            "config": copy.deepcopy(provenance_module.FORMAL_MATRIX_CONFIG),
            "config_sha256": config_hash,
            "config_file_sha256": "f" * 64,
            "run_id_sha256": run_hash,
            "matrix_cell_count": 0,
            "repetition_count": 0,
            "operation_sample_count": 0,
            "operation_samples_sha256": provenance_module.canonical_jsonl_sha256(
                []
            ),
            "repetitions_sha256": provenance_module.canonical_jsonl_sha256([]),
            "graphs": [],
            "aggregate": {"evidence_scope": "diagnostic", "rows": []},
            "topology_attestation_sha256": None,
        }

        with TemporaryDirectory() as tmp:
            with self.assertRaises((ProvenancePerformanceError, ValueError)):
                publish_provenance_bundle(
                    tmp,
                    bundle_id=bundle_id,
                    operation_samples=[],
                    repetitions=[],
                    report=forged,
                )
            self.assertFalse((Path(tmp) / "bundles" / f"{bundle_id}.json").exists())

    def test_publisher_revalidates_and_exclusively_points_to_valid_formal_object(self):
        config = self._small_formal_config()
        report = self._valid_formal_report()
        reports = [report]
        operation_samples = list(report["samples"])
        repetitions = list(report["repetitions"])
        with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config), patch(
            "txnmem_provenance_performance.formal_config_file_sha256",
            return_value="e" * 64,
        ):
            topology = self._topology_attestation(reports, config)
            with patch.dict(
                FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                {topology["run_id_sha256"]: topology["attestation_sha256"]},
                clear=True,
            ), patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    topology["run_id_sha256"]: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ):
                aggregate = aggregate_matrix(
                    reports,
                    bootstrap_repetitions=100,
                    seed=17,
                    topology_attestation=topology,
                )
                config_hash = formal_matrix_config_sha256()
                bundle_id = provenance_bundle_id(
                    config_sha256=config_hash,
                    run_id_sha256=report["run_id_sha256"],
                    formal=True,
                    backend="vector-graph",
                )
                publication = {
                    "schema": "txnmem-provenance-performance-report-v1",
                    "backend": "vector-graph",
                    "formal_requested": True,
                    "bundle_id": bundle_id,
                    "publication_status": "complete",
                    "production_backend_claim": True,
                    "config": copy.deepcopy(config),
                    "config_sha256": config_hash,
                    "config_file_sha256": "e" * 64,
                    "run_id_sha256": report["run_id_sha256"],
                    "matrix_cell_count": 1,
                    "repetition_count": 2,
                    "operation_sample_count": 8,
                    "operation_samples_sha256": provenance_module.canonical_jsonl_sha256(
                        operation_samples
                    ),
                    "repetitions_sha256": provenance_module.canonical_jsonl_sha256(
                        repetitions
                    ),
                    "graphs": [report["graph"]],
                    "aggregate": aggregate,
                    "topology_attestation_sha256": topology["attestation_sha256"],
                }
                with TemporaryDirectory() as tmp:
                    output_path = publish_provenance_bundle(
                        tmp,
                        bundle_id=bundle_id,
                        operation_samples=operation_samples,
                        repetitions=repetitions,
                        report=publication,
                        topology_attestation=topology,
                    )
                    output_exists = output_path.is_file()
                    pointer = Path(tmp) / "bundles" / f"{bundle_id}.json"
                    before = pointer.read_bytes()
                    with self.assertRaises(ValueError):
                        publish_provenance_bundle(
                            tmp,
                            bundle_id=bundle_id,
                            operation_samples=operation_samples,
                            repetitions=repetitions,
                            report=publication,
                            topology_attestation=topology,
                        )
                    after = pointer.read_bytes()

        self.assertTrue(output_exists)
        self.assertEqual(before, after)

    def test_two_stage_candidate_attestation_and_promotion_reuses_exact_bytes(self):
        config = self._small_formal_config()
        graph = build_layered_dag(10, seed=17)
        diagnostic_cell = run_matrix_cell(
            lambda namespace: _FixtureBackend(namespace),
            graph,
            concurrency=1,
            repetitions=2,
            operations_per_type=1,
            run_id="two-stage-candidate",
            formal=False,
        )
        samples = list(diagnostic_cell["samples"])
        repetitions = list(diagnostic_cell["repetitions"])
        diagnostic_aggregate = aggregate_matrix(
            diagnostic_cell,
            bootstrap_repetitions=100,
            seed=17,
            require_formal=False,
        )
        config_hash = hashlib.sha256(
            provenance_module._canonical_json_bytes(config)
        ).hexdigest()
        run_hash = diagnostic_cell["run_id_sha256"]
        candidate_id = provenance_bundle_id(
            config_sha256=config_hash,
            run_id_sha256=run_hash,
            formal=False,
            backend="vector-graph",
        )
        candidate_report = {
            "schema": "txnmem-provenance-performance-report-v1",
            "backend": "vector-graph",
            "formal_requested": False,
            "bundle_id": candidate_id,
            "publication_status": "complete",
            "production_backend_claim": False,
            "config": copy.deepcopy(config),
            "config_sha256": config_hash,
            "config_file_sha256": "e" * 64,
            "run_id_sha256": run_hash,
            "matrix_cell_count": 1,
            "repetition_count": 2,
            "operation_sample_count": 8,
            "operation_samples_sha256": provenance_module.canonical_jsonl_sha256(
                samples
            ),
            "repetitions_sha256": provenance_module.canonical_jsonl_sha256(
                repetitions
            ),
            "graphs": [graph.metadata()],
            "aggregate": diagnostic_aggregate,
            "topology_attestation_sha256": None,
        }

        with TemporaryDirectory() as candidate_tmp, TemporaryDirectory() as formal_tmp:
            with patch.object(provenance_module, "FORMAL_MATRIX_CONFIG", config), patch(
                "txnmem_provenance_performance.formal_config_file_sha256",
                return_value="e" * 64,
            ):
                publish_provenance_bundle(
                    candidate_tmp,
                    bundle_id=candidate_id,
                    operation_samples=samples,
                    repetitions=repetitions,
                    report=candidate_report,
                )
                candidate_pointer = (
                    Path(candidate_tmp) / "bundles" / f"{candidate_id}.json"
                )
                candidate_before = candidate_pointer.read_bytes()
                candidate_pointer_payload = json.loads(candidate_before)
                candidate_object_root = (
                    Path(candidate_tmp)
                    / "bundle_objects"
                    / candidate_pointer_payload["object_id"]
                )
                candidate_sample_bytes = (
                    candidate_object_root
                    / "data"
                    / "provenance_operation_samples.jsonl"
                ).read_bytes()
                candidate_repetition_bytes = (
                    candidate_object_root
                    / "data"
                    / "provenance_repetitions.jsonl"
                ).read_bytes()
                material = candidate_attestation_material(
                    candidate_tmp, candidate_id
                )
                candidate_seal = collector_module._seal_candidate_tree(
                    Path(candidate_tmp),
                    expected_owner_uid=collector_module.os.getuid(),
                    sealed_owner_uid=collector_module.os.getuid(),
                    sealed_owner_gid=collector_module.os.getgid(),
                    completion_receipt=material,
                )
                topology = self._topology_from_candidate_material(
                    material, candidate_seal=candidate_seal
                )
                with patch.dict(
                    FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                    {run_hash: topology["attestation_sha256"]},
                    clear=True,
                ), patch.dict(
                    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                    {
                        run_hash: hashlib.sha256(
                            self.AUTHORIZATION_NONCE
                        ).hexdigest()
                    },
                    clear=True,
                ):
                    formal_path = promote_provenance_candidate(
                        candidate_tmp,
                        candidate_id,
                        topology_attestation=topology,
                        out_dir=formal_tmp,
                    )
                candidate_after = candidate_pointer.read_bytes()
                formal_pointers = list((Path(formal_tmp) / "bundles").glob("*.json"))
                formal_pointer_payload = json.loads(
                    formal_pointers[0].read_text(encoding="utf-8")
                )
                formal_object_root = (
                    Path(formal_tmp)
                    / "bundle_objects"
                    / formal_pointer_payload["object_id"]
                )
                formal_sample_bytes = (
                    formal_object_root
                    / "data"
                    / "provenance_operation_samples.jsonl"
                ).read_bytes()
                formal_repetition_bytes = (
                    formal_object_root
                    / "data"
                    / "provenance_repetitions.jsonl"
                ).read_bytes()
                retained_topology = json.loads(
                    (
                        formal_object_root
                        / "evidence"
                        / "topology_attestation.json"
                    ).read_text(encoding="utf-8")
                )
                formal_completion = json.loads(
                    (formal_object_root / "COMPLETED.json").read_text(
                        encoding="utf-8"
                    )
                )
                for path in sorted(
                    Path(candidate_tmp).rglob("*"),
                    key=lambda item: len(item.parts),
                    reverse=True,
                ):
                    path.chmod(0o700 if path.is_dir() else 0o600)
                Path(candidate_tmp).chmod(0o700)

        self.assertEqual(candidate_before, candidate_after)
        self.assertEqual(candidate_sample_bytes, formal_sample_bytes)
        self.assertEqual(candidate_repetition_bytes, formal_repetition_bytes)
        self.assertEqual(retained_topology, topology)
        self.assertEqual(
            retained_topology["command_manifest"]["runtime_manifest"],
            topology["command_manifest"]["runtime_manifest"],
        )
        self.assertEqual(
            retained_topology["source_manifest"], topology["source_manifest"]
        )
        self.assertEqual(
            retained_topology["proxy_routes"], topology["proxy_routes"]
        )
        self.assertEqual(
            formal_completion["topology_attestation_sha256"],
            topology["attestation_sha256"],
        )
        self.assertEqual(
            formal_completion["topology_attestation_canonical_sha256"],
            hashlib.sha256(
                provenance_module._canonical_json_bytes(topology)
            ).hexdigest(),
        )
        self.assertTrue(material["evidence_manifest_sha256"])
        self.assertEqual(len(formal_pointers), 1)
        self.assertTrue(str(formal_path).endswith("provenance_performance.json"))


if __name__ == "__main__":
    unittest.main()
