import hashlib
import json
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
from threading import Lock
import unittest

from txnmem_model_load import run_model_load
from txnmem_model_protocol import ModelResponse, TokenUsage
from txnmem_experiment import _build_parser


class _UsageModel:
    model = "fixture-model"
    endpoint = "http://127.0.0.1:18001/v1/chat/completions"
    timeout_s = 123.0
    max_tokens = 456

    def __init__(self):
        self._lock = Lock()
        self.calls = 0

    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        with self._lock:
            self.calls += 1
        return ModelResponse(
            "done",
            [],
            TokenUsage(prompt_tokens=10, completion_tokens=2, total_tokens=12),
        )


class TxnMemModelLoadTests(unittest.TestCase):
    def test_cli_exposes_concurrency_cycle_duration_and_generation_bounds(self):
        args = _build_parser().parse_args(
            [
                "real-model-load",
                "--manifest",
                "manifest.json",
                "--endpoint",
                "http://model.test/v1",
                "--model",
                "qwen",
                "--model-revision",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--model-server-build",
                "vllm:fixture",
                "--concurrency",
                "8",
                "--minimum-cycles",
                "5",
                "--minimum-duration-seconds",
                "60",
                "--max-tokens",
                "512",
                "--execution-scope",
                "cross_host_client_server",
                "--host-count",
                "2",
                "--network-transport",
                "ssh_local_port_forward",
                "--tunnel-process-id",
                "4242",
                "--observed-model-host-identity-sha256",
                "b" * 64,
            ]
        )

        self.assertEqual(args.command, "real-model-load")
        self.assertEqual(args.concurrency, 8)
        self.assertEqual(args.minimum_cycles, 5)
        self.assertEqual(args.minimum_duration_seconds, 60.0)
        self.assertEqual(args.max_tokens, 512)
        self.assertEqual(args.execution_scope, "cross_host_client_server")
        self.assertEqual(args.host_count, 2)
        self.assertEqual(args.network_transport, "ssh_local_port_forward")
        self.assertEqual(args.tunnel_process_id, 4242)
        self.assertEqual(args.observed_model_host_identity_sha256, "b" * 64)
        self.assertEqual(args.model_revision, "a" * 64)
        self.assertEqual(args.model_server_build, "vllm:fixture")

    def test_multi_agent_cycles_report_usage_latency_and_claim_boundary(self):
        manifest = {
            "dataset_name": "load-fixture",
            "tasks": [
                {"task_id": "task-1", "prompt": "private one"},
                {"task_id": "task-2", "prompt": "private two"},
            ],
        }
        with TemporaryDirectory() as tmp:
            report = run_model_load(
                manifest,
                _UsageModel(),
                Path(tmp),
                concurrency=2,
                minimum_cycles=3,
                max_steps=7,
            )
            summary_text = (Path(tmp) / "results" / "model_load_summary.json").read_text(
                encoding="utf-8"
            )
            raw_text = (Path(tmp) / "data" / "model_load_traces.jsonl").read_text(
                encoding="utf-8"
            )

        self.assertEqual(report["attempt_count"], 6)
        self.assertEqual(report["completed_attempt_count"], 6)
        self.assertEqual(report["configured_concurrency"], 2)
        self.assertEqual(report["completed_cycles"], 3)
        self.assertEqual(report["model_usage"]["request_count"], 6)
        self.assertEqual(report["model_usage"]["total_tokens"], 72)
        self.assertTrue(report["token_usage_complete"])
        self.assertEqual(report["execution_scope"], "single_host_multi_agent")
        self.assertFalse(report["cross_host_network_claim"])
        self.assertGreaterEqual(report["latency_ms"]["p95"], 0.0)
        self.assertGreaterEqual(report["observed_peak_in_flight"], 1)
        self.assertIn("started_at_utc", report)
        self.assertIn("ended_at_utc", report)
        self.assertEqual(
            report["generation_parameters"],
            {"max_steps": 7, "max_tokens": 456, "timeout_seconds": 123.0},
        )
        self.assertNotIn("private one", summary_text)
        self.assertIn("private one", raw_text)
        json.loads(summary_text)

    def test_load_runner_validates_concurrency_and_cycle_bounds(self):
        manifest = {"tasks": [{"task_id": "task-1", "prompt": "x"}]}
        with TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                run_model_load(
                    manifest,
                    _UsageModel(),
                    Path(tmp),
                    concurrency=0,
                    minimum_cycles=1,
                )
            with self.assertRaises(ValueError):
                run_model_load(
                    manifest,
                    _UsageModel(),
                    Path(tmp),
                    concurrency=1,
                    minimum_cycles=0,
                )

    def test_cross_host_client_server_scope_does_not_claim_multi_host_workers(self):
        manifest = {"tasks": [{"task_id": "task-1", "prompt": "x"}]}
        with TemporaryDirectory() as tmp:
            report = run_model_load(
                manifest,
                _UsageModel(),
                Path(tmp),
                concurrency=1,
                minimum_cycles=1,
                execution_scope="cross_host_client_server",
                host_count=2,
                network_transport="ssh_local_port_forward",
                tunnel_process_id=4242,
                tunnel_command_for_test=(
                    "ssh -N -L 18001:127.0.0.1:8000 "
                    "gpu-user@remote.example"
                ),
                observed_model_host_identity_sha256="b" * 64,
                model_revision="a" * 64,
                model_server_build="vllm:fixture",
            )

        self.assertEqual(report["execution_scope"], "cross_host_client_server")
        self.assertEqual(report["host_count"], 2)
        self.assertTrue(report["cross_host_network_claim"])
        self.assertFalse(report["cross_host_multi_agent_workers_claim"])
        self.assertEqual(report["agent_worker_host_count"], 1)
        self.assertEqual(report["model_server_host_count"], 1)
        self.assertEqual(report["network_transport"], "ssh_local_port_forward")
        self.assertTrue(report["topology_attested"])
        self.assertEqual(
            report["topology_attestation"]["status"], "process_observed"
        )
        self.assertNotEqual(
            report["topology_attestation"]["agent_host_identity_sha256"],
            "",
        )
        self.assertNotEqual(
            report["topology_attestation"]["model_host_identity_sha256"],
            "gpu-user@remote.example",
        )
        self.assertEqual(report["execution_identity"]["model_revision"], "a" * 64)
        self.assertEqual(
            report["execution_identity"]["model_revision_status"], "sha256"
        )
        self.assertTrue(
            report["topology_attestation"]["host_identities_distinct"]
        )
        self.assertEqual(
            report["topology_attestation"]["model_host_identity_source"],
            "ssh_remote_hostname_sha256_observation",
        )

    def test_cross_host_claim_requires_distinct_observed_remote_hostname(self):
        manifest = {"tasks": [{"task_id": "task-1", "prompt": "x"}]}
        command = "ssh -N -L 18001:127.0.0.1:8000 user@localhost"
        local_hash = hashlib.sha256(socket.gethostname().encode("utf-8")).hexdigest()
        with TemporaryDirectory() as tmp:
            missing = run_model_load(
                manifest,
                _UsageModel(),
                Path(tmp) / "missing",
                execution_scope="cross_host_client_server",
                host_count=2,
                network_transport="ssh_local_port_forward",
                tunnel_process_id=4242,
                tunnel_command_for_test=command,
            )
            same = run_model_load(
                manifest,
                _UsageModel(),
                Path(tmp) / "same",
                execution_scope="cross_host_client_server",
                host_count=2,
                network_transport="ssh_local_port_forward",
                tunnel_process_id=4243,
                tunnel_command_for_test=command,
                observed_model_host_identity_sha256=local_hash,
            )

        self.assertFalse(missing["cross_host_network_claim"])
        self.assertEqual(
            missing["topology_attestation"]["status"],
            "process_observed_but_model_host_unattested",
        )
        self.assertFalse(same["cross_host_network_claim"])
        self.assertEqual(
            same["topology_attestation"]["status"],
            "process_observed_but_model_host_not_distinct",
        )


if __name__ == "__main__":
    unittest.main()
