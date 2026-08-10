import subprocess
import sys
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class TxnMemCliOutputTests(unittest.TestCase):
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
                "all_scenarios_no_partial_commit": True,
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
            self.assertTrue(list((Path(tmp) / "results/figures").glob("*.svg")))

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
