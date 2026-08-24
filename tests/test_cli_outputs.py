import subprocess
import sys
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class TxnMemCliOutputTests(unittest.TestCase):
    @staticmethod
    def _provenance_environment():
        return {
            "schema": "txnmem-provenance-environment-v1",
            "isolation_verified": True,
            "co_tenant_load_detected": False,
            "source": "host-observation-v1",
            "cpu_logical_count": 8,
            "memory_total_bytes": 16 * 1024**3,
            "disk_medium": "ssd",
            "toxiproxy_version": "2.9.0",
        }

    def test_provenance_blocked_report_does_not_persist_backend_exception_text(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        class FailingBackend:
            def healthcheck(self):
                cause = TimeoutError("token=private-cause")
                failure = RuntimeError(
                    "password=private at http://203.0.113.10:6333"
                )
                failure._txnmem_service = "neo4j"
                failure._txnmem_operation = "healthcheck"
                raise failure from cause

            def close(self):
                return None

        with TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"TXNMEM_NEO4J_PASSWORD": "runtime-only"}, clear=False
        ), patch(
            "txnmem_provenance_performance.make_vector_graph_backend_factory",
            return_value=lambda _namespace: FailingBackend(),
        ):
            root = Path(tmp).resolve()
            attestation = root / "environment.json"
            attestation.write_text(
                json.dumps(self._provenance_environment()),
                encoding="utf-8",
            )
            out_dir = root / "out"

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "vector-graph",
                    "--config",
                    str(ROOT / "configs" / "provenance_performance_matrix.json"),
                    "--run-id",
                    "blocked-fixture",
                    "--environment-attestation",
                    str(attestation),
                    "--out-dir",
                    str(out_dir),
                ]
            )
            blocked_text = (
                out_dir / "results" / "provenance_performance_blocked.json"
            ).read_text(encoding="utf-8")

        self.assertEqual(exit_code, 2)
        self.assertNotIn("private", blocked_text)
        self.assertNotIn("203.0.113.10", blocked_text)
        blocked = json.loads(blocked_text)
        self.assertEqual(
            blocked["schema"], "txnmem-provenance-performance-blocked-v2"
        )
        self.assertEqual(blocked["error_class"], "RuntimeError")
        self.assertEqual(blocked["reason_code"], "formal_preflight_or_execution_failed")
        self.assertEqual(
            blocked["failure_provenance"],
            {
                "error_classes": ["RuntimeError", "TimeoutError"],
                "operation": "healthcheck",
                "root_error_class": "TimeoutError",
                "service": "neo4j",
            },
        )

    def test_provenance_formal_config_rejects_duplicate_keys_before_backend(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        formal = json.loads(
            (ROOT / "configs" / "provenance_performance_matrix.json").read_text(
                encoding="utf-8"
            )
        )
        duplicate_text = json.dumps(formal)[:-1] + ',"graph_seed":17}'
        with TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"TXNMEM_NEO4J_PASSWORD": "runtime-only"}, clear=False
        ), patch(
            "txnmem_provenance_performance.make_vector_graph_backend_factory"
        ) as factory:
            root = Path(tmp).resolve()
            config = root / "config.json"
            config.write_text(duplicate_text, encoding="utf-8")
            attestation = root / "environment.json"
            attestation.write_text(
                json.dumps(self._provenance_environment()), encoding="utf-8"
            )
            topology = root / "topology.json"
            topology.write_text("{}", encoding="utf-8")
            out_dir = root / "out"

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "vector-graph",
                    "--config",
                    str(config),
                    "--run-id",
                    "duplicate-config",
                    "--environment-attestation",
                    str(attestation),
                    "--topology-attestation",
                    str(topology),
                    "--formal",
                    "--out-dir",
                    str(out_dir),
                ]
            )

        self.assertEqual(exit_code, 2)
        factory.assert_not_called()

    def test_provenance_one_stage_formal_mode_is_disabled_before_backend(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"TXNMEM_NEO4J_PASSWORD": "runtime-only"}, clear=False
        ), patch(
            "txnmem_provenance_performance.make_vector_graph_backend_factory"
        ) as factory:
            root = Path(tmp).resolve()
            attestation = root / "environment.json"
            attestation.write_text(
                json.dumps(self._provenance_environment()), encoding="utf-8"
            )
            topology = root / "topology.json"
            topology.write_text("{}", encoding="utf-8")

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "vector-graph",
                    "--config",
                    str(ROOT / "configs" / "provenance_performance_matrix.json"),
                    "--run-id",
                    "one-stage-disabled",
                    "--environment-attestation",
                    str(attestation),
                    "--topology-attestation",
                    str(topology),
                    "--formal",
                    "--out-dir",
                    str(root / "out"),
                ]
            )

        self.assertEqual(exit_code, 2)
        factory.assert_not_called()

    def test_provenance_formal_output_is_an_immutable_published_bundle(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "schema": "txnmem-provenance-performance-v2",
                        "graph_node_counts": [10],
                        "concurrency_levels": [1],
                        "repetitions": 1,
                        "graph_seed": 17,
                        "operations_per_type": 1,
                        "bootstrap_repetitions": 10,
                        "bootstrap_seed": 17,
                        "request_timeout_seconds": 30.0,
                    }
                ),
                encoding="utf-8",
            )
            out_dir = root / "out"
            arguments = [
                "provenance-performance",
                "--backend",
                "memory",
                "--config",
                str(config),
                "--run-id",
                "immutable-bundle",
                "--out-dir",
                str(out_dir),
            ]

            first = main(arguments)
            bundles = list((out_dir / "bundles").glob("*.json"))
            second = main(arguments)

        self.assertEqual(first, 0)
        self.assertEqual(len(bundles), 1)
        self.assertEqual(second, 2)

    def test_provenance_formal_output_refuses_legacy_and_symlink_targets(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"TXNMEM_NEO4J_PASSWORD": "runtime-only"}, clear=False
        ):
            root = Path(tmp).resolve()
            attestation = root / "environment.json"
            attestation.write_text(
                json.dumps(self._provenance_environment()), encoding="utf-8"
            )
            external = root / "external.json"
            external.write_text("sentinel", encoding="utf-8")
            out_dir = root / "out"
            (out_dir / "results").mkdir(parents=True)
            (out_dir / "results" / "provenance_performance.json").symlink_to(external)

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "memory",
                    "--config",
                    str(ROOT / "configs" / "provenance_performance_matrix.json"),
                    "--run-id",
                    "unsafe-output",
                    "--out-dir",
                    str(out_dir),
                ]
            )
            external_text = external.read_text(encoding="utf-8")
            data_exists = (out_dir / "data").exists()

        self.assertEqual(exit_code, 2)
        self.assertEqual(external_text, "sentinel")
        self.assertFalse(data_exists)

    def test_provenance_blocked_report_refuses_symlink_without_partial_data(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = root / "bad.json"
            config.write_text('{"schema":"x","schema":"y"}', encoding="utf-8")
            out_dir = root / "out"
            (out_dir / "results").mkdir(parents=True)
            external = root / "external.json"
            external.write_text("sentinel", encoding="utf-8")
            (out_dir / "results" / "provenance_performance_blocked.json").symlink_to(
                external
            )

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "memory",
                    "--config",
                    str(config),
                    "--run-id",
                    "blocked-symlink",
                    "--out-dir",
                    str(out_dir),
                ]
            )
            external_text = external.read_text(encoding="utf-8")
            bundles_exist = (out_dir / "bundles").exists()

        self.assertEqual(exit_code, 2)
        self.assertEqual(external_text, "sentinel")
        self.assertFalse(bundles_exist)

    def test_appworld_group_selection_requires_a_source_bound_native_agent_bundle(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            events = root / "events.jsonl"
            selection = root / "selection.json"
            config = root / "realism.json"
            out_dir = root / "out"
            events.write_text(
                json.dumps(
                    {
                        "task_id": "family001_1",
                        "event_id": "family001_1:reference_api:0001",
                        "sequence": 1,
                        "method": "get",
                        "url": "/resource",
                        "official_split": "wrong",
                        "family_id": "family001",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            selection.write_text(
                json.dumps(
                    {
                        "group_key": "family_id",
                        "official_split": "wrong",
                        "evaluation_family_ids": ["family001"],
                        "calibration_family_ids": ["family999"],
                    }
                ),
                encoding="utf-8",
            )
            config.write_text(
                json.dumps(
                    {
                        "synthetic": {"workloads": ["atomic_multi_write"], "seeds": [0]},
                        "statistics": {},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "source-bound native Agent bundle"):
                main(
                    [
                        "trace-replay",
                        "--events",
                        str(events),
                        "--adapter",
                        "appworld",
                        "--group-key",
                        "family_id",
                        "--group-selection",
                        str(selection),
                        "--realism-config",
                        str(config),
                        "--out-dir",
                        str(out_dir),
                    ]
                )
            self.assertFalse(out_dir.exists())

    def test_provenance_performance_cli_runs_small_diagnostic_matrix(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        config = {
            "schema": "txnmem-provenance-performance-v2",
            "graph_node_counts": [10, 20],
            "concurrency_levels": [1, 2],
            "repetitions": 2,
            "graph_seed": 17,
            "operations_per_type": 1,
            "bootstrap_repetitions": 100,
            "bootstrap_seed": 17,
            "request_timeout_seconds": 30.0,
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config_path = root / "matrix.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            out_dir = root / "out"

            exit_code = main(
                [
                    "provenance-performance",
                    "--backend",
                    "memory",
                    "--config",
                    str(config_path),
                    "--run-id",
                    "cli-fixture",
                    "--out-dir",
                    str(out_dir),
                ]
            )

            self.assertEqual(exit_code, 0)
            pointer_paths = list((out_dir / "bundles").glob("*.json"))
            self.assertEqual(len(pointer_paths), 1)
            pointer = json.loads(pointer_paths[0].read_text(encoding="utf-8"))
            report_path = out_dir / pointer["report_path"]
            aggregate = json.loads(report_path.read_text(encoding="utf-8"))
            bundle_root = report_path.parents[1]
            samples = (
                bundle_root / "data" / "provenance_operation_samples.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            repetitions = (
                bundle_root / "data" / "provenance_repetitions.jsonl"
            ).read_text(encoding="utf-8").splitlines()

        self.assertEqual(aggregate["backend"], "memory")
        self.assertEqual(aggregate["aggregate"]["evidence_scope"], "diagnostic")
        self.assertEqual(len(aggregate["aggregate"]["rows"]), 4)
        self.assertEqual(len(samples), 32)
        self.assertEqual(len(repetitions), 8)

    def test_provenance_candidate_material_cli_writes_once(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        material = {
            "schema": "txnmem-provenance-candidate-attestation-material-v1",
            "candidate_bundle_id": "diagnostic-vector_graph-"
            + "a" * 16
            + "-"
            + "b" * 16,
            "run_id_sha256": "1" * 64,
            "config_sha256": "2" * 64,
            "config_file_sha256": "3" * 64,
            "workload_sha256": "4" * 64,
            "environment_attestation_sha256": "5" * 64,
            "evidence_manifest_sha256": "6" * 64,
            "matrix_cell_count": 15,
            "repetition_count": 450,
            "operation_sample_count": 14_400,
            "observed_service_versions": {
                "qdrant": "1.15.4",
                "neo4j": "5.26.0",
                "toxiproxy": "2.9.0",
            },
            "candidate_operation_samples_sha256": "7" * 64,
            "candidate_repetitions_sha256": "8" * 64,
        }
        bundle_id = "diagnostic-vector_graph-" + "a" * 16 + "-" + "b" * 16
        with TemporaryDirectory() as tmp, patch(
            "txnmem_provenance_performance.candidate_attestation_material",
            return_value=material,
        ) as build_material:
            root = Path(tmp).resolve()
            candidate_root = root / "candidate"
            candidate_root.mkdir()
            output = root / "attestation-material.json"
            arguments = [
                "provenance-candidate-material",
                "--candidate-root",
                str(candidate_root),
                "--bundle-id",
                bundle_id,
                "--out",
                str(output),
            ]

            first = main(arguments)
            before = output.read_bytes()
            second = main(arguments)
            after = output.read_bytes()

        self.assertEqual(first, 0)
        self.assertEqual(second, 2)
        self.assertEqual(before, after)
        self.assertEqual(json.loads(before), material)
        self.assertEqual(build_material.call_count, 2)
        build_material.assert_called_with(candidate_root, bundle_id)

    def test_provenance_promote_cli_loads_strict_topology_without_rerun(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        bundle_id = "diagnostic-vector_graph-" + "c" * 16 + "-" + "d" * 16
        topology = {"schema": "fixture-topology", "attestation_sha256": "e" * 64}
        with TemporaryDirectory() as tmp, patch(
            "txnmem_provenance_performance.promote_provenance_candidate",
            return_value=Path(tmp) / "formal" / "report.json",
        ) as promote, patch(
            "txnmem_provenance_performance.run_matrix_cell"
        ) as rerun:
            root = Path(tmp).resolve()
            candidate_root = root / "candidate"
            candidate_root.mkdir()
            topology_path = root / "topology.json"
            topology_path.write_text(json.dumps(topology), encoding="utf-8")
            out_dir = root / "formal"

            exit_code = main(
                [
                    "provenance-promote",
                    "--candidate-root",
                    str(candidate_root),
                    "--bundle-id",
                    bundle_id,
                    "--topology-attestation",
                    str(topology_path),
                    "--out-dir",
                    str(out_dir),
                ]
            )

        self.assertEqual(exit_code, 0)
        promote.assert_called_once_with(
            candidate_root,
            bundle_id,
            topology_attestation=topology,
            out_dir=out_dir,
        )
        rerun.assert_not_called()

    def test_vector_graph_fault_cli_binds_scenario_to_toxiproxy_requester(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        captured = {}

        class FakeVectorGraphBackend:
            def __init__(
                self,
                namespace,
                qdrant_url,
                neo4j_uri,
                auth,
                proxy_requester=None,
                **kwargs,
            ):
                captured.update(
                    {
                        "namespace": namespace,
                        "qdrant_url": qdrant_url,
                        "neo4j_uri": neo4j_uri,
                        "proxy_requester": proxy_requester,
                        "kwargs": kwargs,
                    }
                )

            def close(self):
                return None

            def healthcheck(self):
                captured["healthcheck_called"] = True
                return {
                    "qdrant": {"available": True, "version": "1.11.5"},
                    "neo4j": {"available": True, "version": "5.22.0"},
                }

        def fake_fault_matrix(factory, scenarios, workload, repetitions):
            delay = next(scenario for scenario in scenarios if scenario.name == "delay")
            backend = factory(scenario=delay)
            backend.close()
            return {
                "benchmark": "backend_fault_matrix",
                "scenarios": {},
                "all_scenarios_state_verified": True,
                "all_observed_states_consistent": True,
                "all_scenarios_evidence_valid": True,
                "production_latency_claim": False,
            }

        with TemporaryDirectory() as tmp, patch.dict(
            "os.environ",
            {
                "TXNMEM_NEO4J_URI": "bolt://127.0.0.1:19001",
                "TXNMEM_TOXIPROXY_URL": "http://127.0.0.1:8474",
            },
            clear=False,
        ), patch(
            "txnmem_experiment.benchmark_backend",
            return_value={"benchmark": "backend_only", "rows": []},
        ), patch(
            "txnmem_experiment.run_fault_matrix", side_effect=fake_fault_matrix
        ), patch(
            "txnmem_vector_graph_backend.VectorGraphMemoryBackend",
            FakeVectorGraphBackend,
        ):
            exit_code = main(
                [
                    "backend-performance",
                    "--backend",
                    "vector-graph",
                    "--service-url",
                    "http://127.0.0.1:19000",
                    "--events",
                    "1",
                    "--repetitions",
                    "1",
                    "--out-dir",
                    tmp,
                ]
            )

        self.assertEqual(exit_code, 0)
        controller = captured["proxy_requester"]
        self.assertIsNotNone(controller)
        self.assertEqual(controller.scenario["name"], "delay")
        self.assertEqual(captured["qdrant_url"], "http://127.0.0.1:19000")
        self.assertEqual(captured["neo4j_uri"], "bolt://127.0.0.1:19001")
        self.assertEqual(captured["kwargs"]["max_retries"], 0)
        self.assertEqual(captured["kwargs"]["request_timeout_seconds"], 2.0)
        self.assertTrue(captured["healthcheck_called"])

    def test_benchmark_native_parser_accepts_persistent_memory_backend(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import _build_parser

        args = _build_parser().parse_args(
            [
                "benchmark-native-smoke",
                "--benchmark",
                "locomo",
                "--manifest",
                "configs/real_model_locomo.json",
                "--memory-backend",
                "sqlite",
            ]
        )
        self.assertEqual(args.memory_backend, "sqlite")

    def test_appworld_batch_accepts_instruction_inferred_tool_strategy(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import _build_parser

        args = _build_parser().parse_args(
            [
                "benchmark-native-batch",
                "--benchmark",
                "appworld",
                "--manifest",
                "manifest.json",
                "--appworld-tool-strategy",
                "instruction_inferred",
            ]
        )

        self.assertEqual(args.appworld_tool_strategy, "instruction_inferred")

    def test_appworld_smoke_accepts_instruction_inferred_tool_strategy(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import _build_parser

        args = _build_parser().parse_args(
            [
                "benchmark-native-smoke",
                "--benchmark",
                "appworld",
                "--manifest",
                "manifest.json",
                "--appworld-tool-strategy",
                "instruction_inferred",
            ]
        )

        self.assertEqual(args.appworld_tool_strategy, "instruction_inferred")

    def test_appworld_native_paths_propagate_instruction_inferred_strategy(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        manifest = {"tasks": [{"task_id": "task-1", "instruction": "Use Venmo."}]}
        observed_strategies = {}

        def run_smoke(manifest, model, adapter_factory, out_dir, **_kwargs):
            observed_strategies["smoke"] = adapter_factory(manifest["tasks"][0]).tool_strategy
            (out_dir / "results").mkdir(parents=True, exist_ok=True)
            return {}

        def run_batch(manifest, model, out_dir, *, adapter_factory, **_kwargs):
            observed_strategies["batch"] = adapter_factory(manifest["tasks"][0]).tool_strategy
            (out_dir / "results").mkdir(parents=True, exist_ok=True)
            return {}

        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            with patch(
                "txnmem_experiment.load_task_manifest",
                return_value=(manifest, "manifest-sha256"),
            ), patch(
                "txnmem_experiment.run_benchmark_experiment_manifest",
                side_effect=run_smoke,
            ), patch(
                "txnmem_experiment.run_benchmark_batch",
                side_effect=run_batch,
            ):
                self.assertEqual(
                    main(
                        [
                            "benchmark-native-smoke",
                            "--benchmark",
                            "appworld",
                            "--manifest",
                            "manifest.json",
                            "--offline-fixture",
                            "--out-dir",
                            str(out_dir / "smoke"),
                            "--appworld-tool-strategy",
                            "instruction_inferred",
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "benchmark-native-batch",
                            "--benchmark",
                            "appworld",
                            "--manifest",
                            "manifest.json",
                            "--offline-fixture",
                            "--out-dir",
                            str(out_dir / "batch"),
                            "--appworld-tool-strategy",
                            "instruction_inferred",
                        ]
                    ),
                    0,
                )
                batch_report = json.loads(
                    (out_dir / "batch" / "results" / "native_batch_summary.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    batch_report["condition"]["appworld_model_tool_strategy"],
                    "instruction_inferred",
                )
                self.assertEqual(
                    batch_report["treatment"],
                    {
                        "prompt_profile": "baseline",
                        "trusted_preflight_enabled": False,
                        "app_tool_strategy": "instruction_inferred",
                    },
                )

        self.assertEqual(
            observed_strategies,
            {"smoke": "instruction_inferred", "batch": "instruction_inferred"},
        )

    def test_appworld_tool_strategy_changes_shared_condition_fingerprint(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_conditions import canonical_fingerprint
        from txnmem_experiment import _paired_benchmark_condition

        arguments = {
            "benchmark": "appworld",
            "manifest_sha256": "a" * 64,
            "model_id": "qwen2.5-7b-instruct",
            "model_execution_mode": "remote_endpoint",
            "memory_backend": "sqlite",
            "repetitions": 1,
            "max_tokens": 1024,
            "timeout_seconds": 300.0,
            "model_revision": "b" * 64,
            "model_server_build": "vllm:0.8.5.post1",
        }
        baseline = _paired_benchmark_condition(
            **arguments, appworld_tool_strategy="instruction_inferred"
        )
        tuned = _paired_benchmark_condition(
            **arguments, appworld_tool_strategy="instruction_inferred"
        )
        all_public = _paired_benchmark_condition(
            **arguments, appworld_tool_strategy="all_public"
        )
        not_applicable = _paired_benchmark_condition(
            **{**arguments, "benchmark": "locomo"}, appworld_tool_strategy="all_public"
        )

        self.assertEqual(baseline["appworld_model_tool_strategy"], "instruction_inferred")
        self.assertEqual(canonical_fingerprint(baseline), canonical_fingerprint(tuned))
        self.assertNotEqual(canonical_fingerprint(baseline), canonical_fingerprint(all_public))
        self.assertEqual(not_applicable["appworld_model_tool_strategy"], "not_applicable")

    def test_tau_batch_rejects_manifest_runtime_scope_mismatch_before_execution(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        manifest = {
            "manifest_version": 1,
            "dataset_name": "tau-bench-retail-test",
            "benchmark": "tau-bench",
            "domain": "retail",
            "split": "test",
            "parent_manifest_hash": "a" * 64,
            "tasks": [{"task_id": "task-1", "prompt": "fixture"}],
        }

        def run_batch(_manifest, _model, out_dir, **_kwargs):
            (out_dir / "results").mkdir(parents=True, exist_ok=True)
            return {}

        for option, value in (("--tau-domain", "airline"), ("--tau-split", "dev")):
            with self.subTest(option=option), TemporaryDirectory() as tmp, patch(
                "txnmem_experiment.load_task_manifest",
                return_value=(manifest, "b" * 64),
            ), patch(
                "txnmem_experiment.run_benchmark_batch", side_effect=run_batch
            ) as run_batch_mock:
                arguments = [
                    "benchmark-native-batch",
                    "--benchmark",
                    "tau-bench",
                    "--manifest",
                    "manifest.json",
                    "--offline-fixture",
                    "--out-dir",
                    tmp,
                    "--tau-domain",
                    "retail",
                    "--tau-split",
                    "test",
                    option,
                    value,
                ]

                self.assertEqual(main(arguments), 2)
                run_batch_mock.assert_not_called()

    def test_shard_batch_condition_records_frozen_domain_and_split(self):
        sys.path.insert(0, str(ROOT / "src"))
        from txnmem_experiment import main

        manifest = {
            "manifest_version": 1,
            "dataset_name": "tau-bench-retail-test",
            "benchmark": "tau-bench",
            "domain": "retail",
            "split": "test",
            "parent_manifest_hash": "a" * 64,
            "tasks": [{"task_id": "task-1", "prompt": "fixture"}],
        }

        def run_batch(_manifest, _model, out_dir, **_kwargs):
            (out_dir / "results").mkdir(parents=True, exist_ok=True)
            return {}

        with TemporaryDirectory() as tmp, patch(
            "txnmem_experiment.load_task_manifest",
            return_value=(manifest, "b" * 64),
        ), patch(
            "txnmem_experiment.run_benchmark_batch", side_effect=run_batch
        ):
            self.assertEqual(
                main(
                    [
                        "benchmark-native-batch",
                        "--benchmark",
                        "tau-bench",
                        "--manifest",
                        "manifest.json",
                        "--offline-fixture",
                        "--out-dir",
                        tmp,
                        "--tau-domain",
                        "retail",
                        "--tau-split",
                        "test",
                    ]
                ),
                0,
            )
            report = json.loads(
                (Path(tmp) / "results" / "native_batch_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(report["condition"]["domain"], "retail")
        self.assertEqual(report["condition"]["split"], "test")

    def test_experiment_command_writes_all_artifacts(self):
        with TemporaryDirectory() as tmp:
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "experiment",
                    "--out-dir",
                    tmp,
                    "--seeds",
                    "1",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("wrote", completed.stdout)
            self.assertTrue((Path(tmp) / "data/generated_instances.jsonl").exists())
            self.assertTrue((Path(tmp) / "data/reference_oracles.jsonl").exists())
            self.assertTrue((Path(tmp) / "results/experiment_results.csv").exists())
            self.assertTrue((Path(tmp) / "results/summary.json").exists())
            self.assertTrue((Path(tmp) / "results/coverage.json").exists())
            self.assertTrue((Path(tmp) / "results/mutation_report.json").exists())
            self.assertTrue((Path(tmp) / "results/schedule_baseline.json").exists())
            self.assertTrue((Path(tmp) / "results/realism.json").exists())
            self.assertTrue((Path(tmp) / "results/saturation.json").exists())
            self.assertTrue((Path(tmp) / "results/diversity.json").exists())
            self.assertTrue((Path(tmp) / "results/figures/saturation.svg").exists())
            self.assertTrue((Path(tmp) / "run_manifest.json").exists())
            self.assertTrue(list((Path(tmp) / "results/figures").glob("*.svg")))
            manifest = json.loads((Path(tmp) / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], 1)
            self.assertEqual(manifest["runner_version"], "controlled-experiment/1")
            self.assertEqual(manifest["oracle_version"], "0.4")
            self.assertRegex(manifest["source"]["commit"], r"^[0-9a-f]{40}$")
            self.assertRegex(manifest["source"]["fingerprint"], r"^[0-9a-f]{64}$")
            self.assertIsInstance(manifest["source"]["contained_in_commit"], bool)
            self.assertEqual(
                set(manifest["source"]["components"]),
                {
                    "configs/workload_families.yaml",
                    "src/txnmem_conditions.py",
                    "src/txnmem_differential.py",
                    "src/txnmem_experiment.py",
                    "src/txnmem_invariants.py",
                    "src/txnmem_metrics.py",
                    "src/txnmem_reference.py",
                    "src/txnmem_schedules.py",
                    "src/txnmem_schema.py",
                    "src/txnmem_simulator.py",
                    "src/txnmem_statistics.py",
                    "src/txnmem_workloads.py",
                },
            )
            self.assertEqual(manifest["config"]["relative_path"], "configs/workload_families.yaml")
            self.assertEqual(manifest["domains"]["seeds"], [0])
            self.assertEqual(manifest["counts"]["instances"], 8)
            self.assertEqual(manifest["counts"]["variant_results"], 40)
            for name in (
                "generated_instances.jsonl",
                "reference_oracles.jsonl",
                "experiment_results.csv",
                "saturation.json",
                "diversity.json",
                "saturation.svg",
            ):
                artifact = manifest["artifacts"][name]
                self.assertNotIn(str(Path(tmp)), json.dumps(artifact))
                self.assertRegex(artifact["sha256"], r"^[0-9a-f]{64}$")

    def test_experiment_config_changes_generated_operation_and_config_distribution(self):
        """Ignoring --config would make these intentionally opposite ranges identical."""

        base = json.loads((ROOT / "configs/workload_families.yaml").read_text(encoding="utf-8"))
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            low_config = {**base, "parameter_ranges": {
                "txn_size": [1, 1], "provenance_depth": [1, 1],
                "branch_factor": [1, 1], "policy_churn": [0, 0], "concurrency": [1, 1],
            }}
            high_config = {**base, "parameter_ranges": {
                "txn_size": [4, 4], "provenance_depth": [4, 4],
                "branch_factor": [3, 3], "policy_churn": [2, 2], "concurrency": [3, 3],
            }}
            low_path = tmp_path / "low.json"
            high_path = tmp_path / "high.json"
            low_path.write_text(json.dumps(low_config), encoding="utf-8")
            high_path.write_text(json.dumps(high_config), encoding="utf-8")
            for name, config_path in (("low", low_path), ("high", high_path)):
                completed = subprocess.run(
                    [
                        sys.executable, "src/txnmem_experiment.py", "experiment",
                        "--config", str(config_path), "--out-dir", str(tmp_path / name),
                        "--seeds", "1", "--variants", "TxnMem",
                    ],
                    cwd=ROOT, capture_output=True, text=True,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
            low_rows = [json.loads(line) for line in (tmp_path / "low/data/generated_instances.jsonl").read_text(encoding="utf-8").splitlines()]
            high_rows = [json.loads(line) for line in (tmp_path / "high/data/generated_instances.jsonl").read_text(encoding="utf-8").splitlines()]

        self.assertNotEqual(
            [row["config"] for row in low_rows], [row["config"] for row in high_rows]
        )
        self.assertNotEqual(
            [row["operations"] for row in low_rows], [row["operations"] for row in high_rows]
        )

    def test_experiment_rejects_non_positive_seed_count_before_writing_artifacts(self):
        with TemporaryDirectory() as tmp:
            completed = subprocess.run(
                [
                    sys.executable, "src/txnmem_experiment.py", "experiment",
                    "--out-dir", tmp, "--seeds", "0",
                ],
                cwd=ROOT, capture_output=True, text=True,
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertFalse((Path(tmp) / "data/generated_instances.jsonl").exists())

    def test_formal_source_gate_rejects_uncontained_config_before_artifact_writes(self):
        base = json.loads((ROOT / "configs/controlled_scale_200.json").read_text(encoding="utf-8"))
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            external_config = tmp_path / "external.json"
            external_config.write_text(json.dumps(base), encoding="utf-8")
            out_dir = tmp_path / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "experiment",
                    "--config",
                    str(external_config),
                    "--out-dir",
                    str(out_dir),
                    "--seeds",
                    "1",
                    "--require-clean-source",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("contained", completed.stderr)
            self.assertFalse(out_dir.exists())

    def test_mutation_witnesses_command_writes_replayable_report(self):
        with TemporaryDirectory() as tmp:
            instances = Path(tmp) / "instances.jsonl"
            subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "generate",
                    "--out",
                    str(instances),
                    "--seeds",
                    "1",
                    "--workloads",
                    "atomic_multi_write",
                    "revoke_before_commit",
                    "provenance_chain_repair",
                    "scope_bypass",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            report_path = Path(tmp) / "minimal_mutant_witnesses.json"

            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "mutation-witnesses",
                    "--instances",
                    str(instances),
                    "--out",
                    str(report_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["witness_count"], 4)
            self.assertTrue(report["all_prefix_minimal"])
            self.assertEqual(len(report["source_instances_sha256"]), 64)

    def test_trace_replay_command_writes_trace_grounded_artifacts(self):
        with TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            events.write_text(
                '{"episode_id":"ep1","event_id":"e1","kind":"memory_write","memory_id":"m1"}\n'
                '{"episode_id":"ep1","event_id":"e2","kind":"memory_read","memory_id":"m1"}\n',
                encoding="utf-8",
            )
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "trace-replay",
                    "--events",
                    str(events),
                    "--adapter",
                    "normalized",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("adapted 2 records into 1 trace instances", completed.stdout)
            self.assertTrue((out_dir / "data/trace_grounded_instances.jsonl").exists())
            self.assertTrue((out_dir / "results/trace_replay.csv").exists())
            self.assertTrue((out_dir / "results/trace_realism.json").exists())
            realism = json.loads((out_dir / "results/trace_realism.json").read_text(encoding="utf-8"))
            self.assertIn("evidence", realism)
            self.assertFalse(realism["evidence"]["trace_ground_truth_native"])

    def test_trace_replay_can_compare_saved_synthetic_instances_with_trace_features(self):
        with TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            events.write_text(
                '{"episode_id":"ep1","event_id":"e1","kind":"memory_write","memory_id":"m1"}\n',
                encoding="utf-8",
            )
            synthetic = Path(tmp) / "synthetic.jsonl"
            synthetic.write_text(
                json.dumps(
                    {
                        "operations": [
                            {"type": "write", "memory_id": "m1", "agent_id": "agent_1"}
                        ],
                        "failure_schedule": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            out_dir = Path(tmp) / "out"
            subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "trace-replay",
                    "--events",
                    str(events),
                    "--adapter",
                    "normalized",
                    "--synthetic-instances",
                    str(synthetic),
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            realism = json.loads((out_dir / "results/trace_realism.json").read_text(encoding="utf-8"))
            self.assertEqual(realism["synthetic_count"], 1)
            self.assertEqual(realism["trace_count"], 1)
            self.assertIn("synthetic_source", realism)
            self.assertIn("mean_abs_diff_interval", realism["features"]["operation_count"])

    def test_trace_replay_calibrates_on_train_and_tests_only_holdout_instances(self):
        with TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            events.write_text(
                "".join(
                    json.dumps(
                        {
                            "episode_id": f"ep{index}",
                            "event_id": f"e{index}",
                            "kind": "memory_write",
                            "memory_id": f"m{index}",
                        }
                    )
                    + "\n"
                    for index in range(5)
                ),
                encoding="utf-8",
            )
            out_dir = Path(tmp) / "out"
            subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "trace-replay",
                    "--events",
                    str(events),
                    "--adapter",
                    "normalized",
                    "--out-dir",
                    str(out_dir),
                    "--holdout-fraction",
                    "0.2",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

            realism = json.loads(
                (out_dir / "results/trace_realism.json").read_text(encoding="utf-8")
            )
            self.assertEqual(realism["split"]["train_instance_count"], 4)
            self.assertEqual(realism["split"]["holdout_instance_count"], 1)
            self.assertEqual(realism["trace_count"], 1)
            self.assertEqual(realism["calibration"]["source_instance_count"], 4)

    def test_process_smoke_command_writes_aggregate_only_report(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "process-smoke",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("process concurrency", completed.stdout)
            report = json.loads(
                (out_dir / "results/process_concurrency.json").read_text(encoding="utf-8")
            )
            self.assertTrue(report["completed"])
            self.assertEqual(report["event_count"], 3)
            serialized = json.dumps(report)
            for sensitive_key in ("value", "arguments", "data", "password", "token"):
                self.assertNotIn(sensitive_key, serialized.lower())

    def test_real_model_smoke_fixture_writes_native_trace_and_sanitized_summary(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "real-model-smoke",
                    "--manifest",
                    "configs/real_model_smoke.json",
                    "--offline-fixture",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("native model", completed.stdout)
            self.assertTrue((out_dir / "data/native_model_traces.jsonl").exists())
            summary_path = out_dir / "results/native_model_summary.json"
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertTrue(summary["trace_ground_truth_native"])
            self.assertTrue(summary["task_summaries"][0]["task_evaluator"]["success"])
            serialized = json.dumps(summary, ensure_ascii=False).lower()
            for sensitive_key in ("value", "content", "arguments", "messages", "events"):
                self.assertNotIn(sensitive_key, serialized)

    def test_public_native_smoke_reports_blocked_runtime_without_projection_fallback(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "public-native-smoke",
                    "--dataset",
                    "locomo",
                    "--source",
                    "external_data/raw/locomo10.json",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("blocked", completed.stdout)
            report = json.loads((out_dir / "results/blocked_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "blocked")
            self.assertFalse(report["projection_fallback"])

    def test_process_protocol_smoke_writes_independent_fault_coverage(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            completed = subprocess.run(
                [
                    sys.executable,
                    "src/txnmem_experiment.py",
                    "process-protocol-smoke",
                    "--out-dir",
                    str(out_dir),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("protocol", completed.stdout)
            report = json.loads(
                (out_dir / "results/process_protocol.json").read_text(encoding="utf-8")
            )
            self.assertEqual(report["invariant_coverage"]["coverage_rate"], 1.0)
            self.assertEqual(report["minimal_counterexamples"], [])


if __name__ == "__main__":
    unittest.main()
