from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_evidence_aggregates import (  # noqa: E402
    aggregate_e2e_submission_evidence,
    aggregate_tau_submission_evidence,
    aggregate_toxiproxy_submission_evidence,
)


MODEL_REVISION = "7" * 64
SOURCE_COMMIT = "a" * 40


class SubmissionEvidenceAggregateTests(unittest.TestCase):
    def test_toxiproxy_aggregate_requires_consistent_post_operation_state(self):
        source = self._state_verified_toxiproxy_source()
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "toxiproxy.json", source)
            result = aggregate_toxiproxy_submission_evidence(
                path,
                expected_repetitions=2,
                toxiproxy_version="2.5.0",
                source_commit=SOURCE_COMMIT,
                run_command="python txnmem_experiment.py backend-performance",
                runtime_attestation=self._runtime_attestation(path),
            )

        self.assertEqual(result["schema_version"], 2)
        self.assertEqual(result["evidence_id"], "toxiproxy_state_verified_30")
        self.assertEqual(result["status"], "complete_state_verified_fault_observations")
        self.assertEqual(result["workload_events"], 2)
        self.assertEqual(result["total_repetitions"], 10)
        self.assertEqual(result["state_totals"], {"complete": 6, "absent": 4, "partial": 0, "unknown": 0})
        self.assertTrue(result["all_scenarios_state_verified"])
        self.assertTrue(result["all_observed_states_consistent"])
        self.assertEqual(len(result["runtime_attestation"]["sha256"]), 64)
        self.assertEqual(set(result["runtime_attestation"]["image_digests"]), {"qdrant", "neo4j", "toxiproxy"})
        self.assertEqual(set(result["runtime_attestation"]), {"sha256", "image_digests", "host_identity_sha256"})

    def test_toxiproxy_aggregate_rejects_invalid_state_oracle_evidence(self):
        cases = {
            "partial state": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0].__setitem__("classification", "partial"),
            "unknown state": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0].__setitem__("classification", "unknown"),
            "Qdrant read failure": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0]["items"][0]["qdrant"].__setitem__("read_ok", False),
            "Neo4j read failure": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0]["items"][0]["neo4j"].__setitem__("read_ok", False),
            "present and matches disagreement": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0]["items"][0]["neo4j"].__setitem__("matches", False),
            "duplicate expected memory IDs": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"][0]["items"][1].__setitem__("memory_id", "normal-m0"),
            "state-verification row count mismatch": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_verifications"].pop(),
            "state count mismatch": lambda source: source["fault_matrix"]["scenarios"]["normal"]["persistent_state_classification_counts"].__setitem__("complete", 1),
            "memory-event count mismatch": lambda source: source["performance"]["rows"][0].__setitem__("workload_events", 3),
            "scenario repetition mismatch": lambda source: source["fault_matrix"]["scenarios"]["normal"].__setitem__("repetitions", 1),
            "aggregate all_scenarios_state_verified false": lambda source: source["fault_matrix"].__setitem__("all_scenarios_state_verified", False),
            "aggregate all_observed_states_consistent false": lambda source: source["fault_matrix"].__setitem__("all_observed_states_consistent", False),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                source = self._state_verified_toxiproxy_source()
                mutate(source)
                path = self._write(Path(tmp), "toxiproxy.json", source)
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT,
                        run_command="python txnmem_experiment.py backend-performance",
                        runtime_attestation=self._runtime_attestation(path),
                    )

    def test_toxiproxy_aggregate_rejects_invalid_runtime_attestation(self):
        cases = {
            "missing image digest": lambda attestation: attestation["services"]["qdrant"].pop("image_digest"),
            "mismatched source SHA-256": lambda attestation: attestation.__setitem__("source_artifact_sha256", "b" * 64),
            "non-zero exit code": lambda attestation: attestation.__setitem__("exit_code", 1),
            "missing runtime version": lambda attestation: attestation["runtime"].pop("docker"),
            "secret-bearing command text": lambda attestation: attestation.__setitem__("run_command", "runner --secret=redacted"),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                path = self._write(Path(tmp), "toxiproxy.json", self._state_verified_toxiproxy_source())
                attestation = self._runtime_attestation(path)
                mutate(attestation)
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT,
                        run_command="python txnmem_experiment.py backend-performance",
                        runtime_attestation=attestation,
                    )

    def test_toxiproxy_aggregate_rejects_contradictory_raw_proxy_evidence(self):
        cases = {
            "unverified target route": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0]["proxy_routes"]["qdrant"].__setitem__("verified", False),
            "mismatched target upstream": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0]["proxy_routes"]["qdrant"].__setitem__("upstream", "wrong:6333"),
            "wrong trigger ordinal": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0]["events"][0].__setitem__("request_ordinal", 2),
            "truthy trigger flag": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0].__setitem__("trigger_fired", 1),
            "contradictory event fault flag": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0]["events"][0].__setitem__("fault_observed", False),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                source = self._state_verified_toxiproxy_source()
                mutate(source)
                path = self._write(Path(tmp), "toxiproxy.json", source)
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT,
                        run_command="python txnmem_experiment.py backend-performance",
                        runtime_attestation=self._runtime_attestation(path),
                    )

    def test_toxiproxy_aggregate_reconciles_every_summary_counter(self):
        cases = {
            "success_count": ("normal", "success_count"),
            "error_count": ("timeout", "error_count"),
            "abort_count": ("timeout", "abort_count"),
            "retry_count": ("retry_success", "retry_count"),
            "retry_success_count": ("retry_success", "retry_success_count"),
            "fault_evidence_count": ("normal", "fault_evidence_count"),
            "trigger_fired_count": ("delay", "trigger_fired_count"),
            "toxic_installed_count": ("delay", "toxic_installed_count"),
            "toxic_cleared_count": ("delay", "toxic_cleared_count"),
            "proxy_path_verified_count": ("normal", "proxy_path_verified_count"),
            "fault_observed_count": ("delay", "fault_observed_count"),
            "evidence_valid_count": ("normal", "evidence_valid_count"),
        }
        for name, (scenario, field) in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                source = self._state_verified_toxiproxy_source()
                source["fault_matrix"]["scenarios"][scenario][field] -= 1
                path = self._write(Path(tmp), "toxiproxy.json", source)
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT,
                        run_command="python txnmem_experiment.py backend-performance",
                        runtime_attestation=self._runtime_attestation(path),
                    )

    def test_toxiproxy_aggregate_rejects_unbound_request_ordinals(self):
        cases = {
            "wrong positive ordinal": lambda source: source["fault_matrix"]["scenarios"]["delay"]
            ["repetition_evidence"][0]["request_ordinals"].__setitem__("qdrant:write", 999),
            "extra ordinal key": lambda source: source["fault_matrix"]["scenarios"]["normal"]
            ["repetition_evidence"][0]["request_ordinals"].__setitem__("qdrant:delete_compensation", 1),
            "missing required ordinal": lambda source: source["fault_matrix"]["scenarios"]["connection_drop"]
            ["repetition_evidence"][0]["request_ordinals"].pop("neo4j:delete_compensation"),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                source = self._state_verified_toxiproxy_source()
                mutate(source)
                path = self._write(Path(tmp), "toxiproxy.json", source)
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT,
                        run_command="python txnmem_experiment.py backend-performance",
                        runtime_attestation=self._runtime_attestation(path),
                    )

    def test_toxiproxy_aggregate_rejects_extra_state_classifications(self):
        with TemporaryDirectory() as tmp:
            source = self._state_verified_toxiproxy_source()
            source["fault_matrix"]["scenarios"]["normal"][
                "persistent_state_classification_counts"
            ]["unrecognized"] = 0
            path = self._write(Path(tmp), "toxiproxy.json", source)
            with self.assertRaises(ValueError):
                aggregate_toxiproxy_submission_evidence(
                    path, expected_repetitions=2, toxiproxy_version="2.5.0",
                    source_commit=SOURCE_COMMIT,
                    run_command="python txnmem_experiment.py backend-performance",
                    runtime_attestation=self._runtime_attestation(path),
                )

    def test_toxiproxy_aggregate_rejects_private_run_commands_and_output_paths(self):
        unsafe_commands = (
            "python " + str(Path.home() / "private" / "runner.py"),
            "curl http://" + ".".join(("192", "0", "2", "1")) + "/run",
            "ssh " + "audit" + "@" + "host" + " python runner.py",
            "python " + "runner" + "." + "internal",
            "curl https://" + "name" + ":" + "value" + "@" + "site" + ".test/run",
            "python \"" + str(Path.home() / "private" / "runner.py") + "\"",
            "python '" + "/" + "home" + "/private/runner.py'",
        )
        for command in unsafe_commands:
            with self.subTest(command_type=command.split()[0]), TemporaryDirectory() as tmp:
                path = self._write(Path(tmp), "toxiproxy.json", self._state_verified_toxiproxy_source())
                with self.assertRaises(ValueError):
                    aggregate_toxiproxy_submission_evidence(
                        path, expected_repetitions=2, toxiproxy_version="2.5.0",
                        source_commit=SOURCE_COMMIT, run_command=command,
                        runtime_attestation=self._runtime_attestation(path, run_command=command),
                    )
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "toxiproxy.json", self._state_verified_toxiproxy_source())
            result = aggregate_toxiproxy_submission_evidence(
                path, expected_repetitions=2, toxiproxy_version="2.5.0",
                source_commit=SOURCE_COMMIT,
                run_command="python txnmem_experiment.py backend-performance",
                runtime_attestation=self._runtime_attestation(path),
            )
        self.assertEqual(result["source_artifact"]["basename"], path.name)
        self.assertFalse(str(Path.home()) in json.dumps(result, sort_keys=True))

    def test_toxiproxy_cli_requires_runtime_attestation(self):
        command = [
            sys.executable,
            str(ROOT / "scripts" / "aggregate_submission_evidence.py"),
            "toxiproxy",
            "--source", "source.json",
            "--out", "aggregate.json",
            "--toxiproxy-version", "2.5.0",
            "--source-commit", SOURCE_COMMIT,
            "--run-command", "python runner.py",
        ]
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(ROOT / "src")
        result = subprocess.run(
            command, capture_output=True, text=True, check=False, cwd=ROOT, env=environment
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--runtime-attestation", result.stderr)

    def _state_verified_toxiproxy_source(self, repetitions: int = 2) -> dict:
        scenario_specs = {
            "normal": ("none", "none", "none"),
            "delay": ("qdrant", "write", "delay"),
            "timeout": ("qdrant", "write", "timeout"),
            "connection_drop": ("neo4j", "commit", "connection_drop"),
            "retry_success": ("qdrant", "write", "connection_drop"),
        }
        request_ordinals = {
            "normal": {"qdrant:write": 2, "neo4j:commit": 2},
            "delay": {"qdrant:write": 2, "neo4j:commit": 2},
            "retry_success": {"qdrant:write": 2, "neo4j:commit": 2},
            "timeout": {"qdrant:write": 1},
            "connection_drop": {
                "qdrant:write": 1,
                "neo4j:commit": 1,
                "qdrant:delete_compensation": 1,
                "neo4j:delete_compensation": 1,
            },
        }
        scenarios = {}
        for name in ("normal", "delay", "timeout", "connection_drop", "retry_success"):
            non_normal = name != "normal"
            retry = name == "retry_success"
            abort = name in {"timeout", "connection_drop"}
            expected_state = "absent" if abort else "complete"
            service, operation, action = scenario_specs[name]
            verification = {
                "classification": expected_state,
                "items": [
                    {
                        "classification": expected_state,
                        "memory_id": f"{name}-m{index}",
                        "qdrant": {"read_ok": True, "present": not abort, "matches": not abort},
                        "neo4j": {"read_ok": True, "present": not abort, "matches": not abort},
                    }
                    for index in range(2)
                ],
            }
            evidence = [
                {
                    "trigger_fired": non_normal, "toxic_installed": non_normal,
                    "toxic_cleared": non_normal, "proxy_path_verified": True,
                    "fault_observed": non_normal, "evidence_valid": True,
                    "retry_count": int(retry), "retry_success_count": int(retry),
                    "scenario": name,
                    "request_ordinals": copy.deepcopy(request_ordinals[name]),
                    "proxy_routes": {
                        route_service: {
                            "service": route_service,
                            "proxy_name": f"txnmem-{route_service}",
                            "client_endpoint": f"proxy://{route_service}:1900{int(route_service == 'neo4j')}",
                            "listen": f":1900{int(route_service == 'neo4j')}",
                            "upstream": f"{route_service}:{'6333' if route_service == 'qdrant' else '7687'}",
                            "verified": True,
                        }
                        for route_service in ("qdrant", "neo4j")
                    },
                    "events": [] if not non_normal else [{
                        "scenario": name, "service": service, "operation": operation,
                        "request_ordinal": 1, "action": action,
                        "recovery_action": "retry_once" if retry else ("abort" if abort else "continue"),
                        "proxy_name": f"txnmem-{service}",
                        "toxic_installed": True, "toxic_cleared": True,
                        "proxy_path_verified": True, "fault_observed": True,
                        **({"retry_success": True} if retry else {}),
                        **({"observed_exception": "Fault"} if abort or retry else {}),
                    }],
                }
                for _ in range(repetitions)
            ]
            scenarios[name] = {
                "repetitions": repetitions, "success_count": repetitions * int(not abort),
                "error_count": repetitions * int(abort), "retry_success_count": repetitions * int(retry),
                "abort_count": repetitions * int(abort), "retry_count": repetitions * int(retry),
                "fault_evidence_count": repetitions, "trigger_fired_count": repetitions * int(non_normal),
                "toxic_installed_count": repetitions * int(non_normal), "toxic_cleared_count": repetitions * int(non_normal),
                "proxy_path_verified_count": repetitions, "fault_observed_count": repetitions * int(non_normal),
                "evidence_valid_count": repetitions, "repetition_evidence": evidence, "evidence_valid": True,
                "persistent_state_verifications": [copy.deepcopy(verification) for _ in range(repetitions)],
                "persistent_state_classification_counts": {
                    "complete": repetitions * int(expected_state == "complete"),
                    "absent": repetitions * int(expected_state == "absent"), "partial": 0, "unknown": 0,
                },
                "name": name, "service": service, "trigger_operation": operation,
                "trigger_ordinal": 1, "action": action,
                "recovery_action": "none" if not non_normal else (
                    "retry_once" if retry else ("abort" if abort else "continue")
                ),
            }
        return {
            "backend": "vector-graph",
            "backend_health": {"qdrant": {"available": True, "version": "1.11.5"}, "neo4j": {"available": True, "version": "5.22.0"}},
            "fault_matrix": {
                "all_scenarios_evidence_valid": True, "all_scenarios_state_verified": True,
                "all_observed_states_consistent": True, "scenarios": scenarios,
            },
            "performance": {"rows": [{"workload_events": 2}]},
            "production_latency_claim": False,
        }

    def _runtime_attestation(
        self, source_path: Path, *, run_command: str = "python txnmem_experiment.py backend-performance"
    ) -> dict:
        return {
            "schema_version": 1, "captured_at": "2026-08-16T00:00:00Z",
            "execution_scope": "single_host_real_services", "source_commit": SOURCE_COMMIT,
            "source_artifact_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "exit_code": 0, "run_command": run_command,
            "runtime": {"python": "3.12.0", "docker": "29.1.3", "compose": "2.40.3", "kernel": "test-kernel"},
            "services": {
                "qdrant": {"version": "1.11.5", "tag": "qdrant/qdrant:v1.11.5", "image_digest": "1" * 64, "pull_source": "registry/qdrant"},
                "neo4j": {"version": "5.22.0", "tag": "neo4j:5.22.0", "image_digest": "2" * 64, "pull_source": "registry/neo4j"},
                "toxiproxy": {"version": "2.5.0", "tag": "shopify/toxiproxy:2.5.0", "image_digest": "3" * 64, "pull_source": "registry/toxiproxy"},
            },
            "network_boundary": {"data_services_directly_published": False, "client_data_path": "toxiproxy"},
            "host_identity_sha256": "4" * 64,
        }

    def _write(self, root: Path, name: str, payload: dict) -> Path:
        path = root / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _tau_source(self) -> dict:
        rows = [
            {
                "task_id": "tau-0000",
                "status": "completed",
                "native_event_count": 2,
                "official": {"status": "available", "reward": 1.0},
            },
            {
                "task_id": "tau-0001",
                "status": "failed",
                "failure_code": "max_steps_exceeded",
                "native_event_count": 3,
                "official": {"status": "available", "reward": 0.0},
            },
        ]
        return {
            "status": "available",
            "dataset": "tau-bench-airline-test",
            "task_count": 2,
            "unique_task_count": 2,
            "native_event_count": 5,
            "model_id": "qwen2.5-7b-instruct",
            "model_execution_mode": "remote_endpoint",
            "memory_backend": "sqlite",
            "primary_manifest_sha256": "b" * 64,
            "replaced_network_error_tasks": [],
            "retry_manifests": [],
            "official": {
                "official_evaluator_status": "available",
                "evaluator_available_task_count": 2,
                "task_count": 2,
                "trials": 2,
                "reward_sum": 1.0,
                "reward_mean": 0.5,
                "event_count": 5,
            },
            "task_summaries": rows,
            "production_latency_claim": False,
        }

    def _e2e_source(self) -> dict:
        return {
            "status": "available",
            "benchmark": "tau-bench-airline",
            "task_count": 2,
            "model": "qwen2.5-7b-instruct",
            "model_revision": MODEL_REVISION,
            "model_server_build": "vllm:0.8.5.post1",
            "source_commit": SOURCE_COMMIT,
            "backend_health": {
                "qdrant": {"available": True, "version": "1.11.5"},
                "neo4j": {"available": True, "version": "5.22.0"},
            },
            "manifest_sha256": "b" * 64,
            "rows": [
                {
                    "task_id": "tau-0000",
                    "elapsed_ms": 10.0,
                    "status": "completed",
                    "native_event_count": 2,
                    "official": {"status": "available", "reward": 1.0},
                },
                {
                    "task_id": "tau-0001",
                    "elapsed_ms": 30.0,
                    "status": "completed",
                    "native_event_count": 3,
                    "official": {"status": "available", "reward": 0.0},
                },
            ],
            "mean_ms": 20.0,
            "p50_ms": 20.0,
            "production_latency_claim": False,
        }

    def test_tau_aggregate_recomputes_all_totals_and_records_attestation(self):
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "tau.json", self._tau_source())
            result = aggregate_tau_submission_evidence(
                path,
                expected_task_count=2,
                model_revision=MODEL_REVISION,
                model_server_build="vllm:0.8.5.post1",
                source_commit=SOURCE_COMMIT,
                run_command="python txnmem_experiment.py benchmark-native-batch ...",
                runtime_attestation={"server_pid": 123, "server_continuity": True},
            )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["task_count"], 2)
        self.assertEqual(result["native_event_count"], 5)
        self.assertEqual(result["evaluator_available_task_count"], 2)
        self.assertEqual(result["reward_sum"], 1.0)
        self.assertEqual(result["reward_mean"], 0.5)
        self.assertEqual(result["max_steps_exceeded_count"], 1)
        self.assertEqual(result["model_revision"], MODEL_REVISION)
        self.assertEqual(result["source_commit"], SOURCE_COMMIT)
        self.assertEqual(len(result["source_artifact"]["sha256"]), 64)
        self.assertFalse(result["production_latency_claim"])

    def test_tau_aggregate_rejects_duplicate_or_incomplete_evaluator_tasks(self):
        source = self._tau_source()
        source["task_summaries"][1]["task_id"] = "tau-0000"
        source["task_summaries"][1].pop("official")
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "tau.json", source)
            with self.assertRaisesRegex(ValueError, "unique task IDs"):
                aggregate_tau_submission_evidence(
                    path,
                    expected_task_count=2,
                    model_revision=MODEL_REVISION,
                    model_server_build="vllm:0.8.5.post1",
                    source_commit=SOURCE_COMMIT,
                    run_command="command",
                    runtime_attestation={"server_continuity": True},
                )

    def test_e2e_aggregate_recomputes_latency_completion_and_service_health(self):
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "e2e.json", self._e2e_source())
            result = aggregate_e2e_submission_evidence(
                path,
                expected_task_count=2,
                source_commit=SOURCE_COMMIT,
                run_command="python scripts/run_e2e_real_backend.py",
            )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(result["native_event_count"], 5)
        self.assertEqual(result["mean_ms"], 20.0)
        self.assertEqual(result["p50_ms"], 20.0)
        self.assertEqual(result["backend_health"]["qdrant"]["version"], "1.11.5")
        self.assertEqual(result["model_revision"], MODEL_REVISION)
        self.assertFalse(result["production_latency_claim"])

    def test_e2e_aggregate_rejects_missing_health_or_nonfinite_latency(self):
        source = self._e2e_source()
        source["backend_health"]["neo4j"]["available"] = False
        source["rows"][0]["elapsed_ms"] = math.inf
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "e2e.json", source)
            with self.assertRaisesRegex(ValueError, "finite positive latency"):
                aggregate_e2e_submission_evidence(
                    path,
                    expected_task_count=2,
                    source_commit=SOURCE_COMMIT,
                    run_command="command",
                )

    def test_e2e_aggregate_rejects_source_commit_mismatch(self):
        with TemporaryDirectory() as tmp:
            path = self._write(Path(tmp), "e2e.json", self._e2e_source())
            with self.assertRaisesRegex(ValueError, "source_commit"):
                aggregate_e2e_submission_evidence(
                    path,
                    expected_task_count=2,
                    source_commit="c" * 40,
                    run_command="command",
                )


if __name__ == "__main__":
    unittest.main()
