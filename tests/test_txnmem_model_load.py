import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from threading import Lock
import unittest
from unittest.mock import patch

from txnmem_model_load import _observe_ssh_tunnel, run_model_load
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
    def test_cli_exposes_topology_inputs_but_rejects_caller_supplied_host_identity(self):
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
        self.assertEqual(args.model_revision, "a" * 64)
        self.assertEqual(args.model_server_build, "vllm:fixture")
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                _build_parser().parse_args(
                    [
                        "real-model-load",
                        "--manifest",
                        "manifest.json",
                        "--endpoint",
                        "http://model.test/v1",
                        "--model",
                        "qwen",
                        "--observed-model-host-identity-sha256",
                        "b" * 64,
                    ]
                )
        with TemporaryDirectory() as tmp:
            with self.assertRaises(TypeError):
                run_model_load(
                    {"tasks": [{"task_id": "task-1", "prompt": "x"}]},
                    _UsageModel(),
                    Path(tmp),
                    observed_model_host_identity_sha256="b" * 64,
                )

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

    def test_cross_host_client_server_scope_requires_exactly_two_hosts(self):
        manifest = {"tasks": [{"task_id": "task-1", "prompt": "x"}]}
        with TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "host_count=2"):
                run_model_load(
                    manifest,
                    _UsageModel(),
                    Path(tmp),
                    execution_scope="cross_host_client_server",
                    host_count=3,
                )

    def test_tunnel_without_controlmaster_or_with_localhost_cannot_claim_cross_host(self):
        no_master = _observe_ssh_tunnel(
            "http://127.0.0.1:18001/v1",
            4242,
            command_override="ssh -N -L 18001:127.0.0.1:8000 user@remote.example",
        )
        localhost = _observe_ssh_tunnel(
            "http://127.0.0.1:18001/v1",
            4242,
            command_override=(
                "ssh -M -S /private/tmp/txnmem-control -N "
                "-L 18001:127.0.0.1:8000 user@localhost"
            ),
        )

        self.assertEqual(no_master["status"], "process_observed_but_controlmaster_required")
        self.assertFalse(no_master["host_identities_distinct"])
        self.assertEqual(localhost["status"], "process_observed_but_remote_target_loopback")
        self.assertFalse(localhost["host_identities_distinct"])

    def test_matching_controlmaster_pid_observes_remote_hostname_over_same_socket(self):
        command = (
            "ssh -M -S /private/tmp/txnmem-control -N "
            "-L 18001:127.0.0.1:8000 user@remote.example"
        )
        with patch(
            "txnmem_model_load.subprocess.run",
            side_effect=[
                subprocess.CompletedProcess([], 0, "Master running (pid=4242)\n", ""),
                subprocess.CompletedProcess([], 0, "remote-model-host\n", ""),
            ],
        ) as run:
            attestation = _observe_ssh_tunnel(
                "http://127.0.0.1:18001/v1",
                4242,
                command_override=command,
            )

        self.assertEqual(attestation["status"], "process_observed")
        self.assertEqual(
            attestation["model_host_identity_sha256"],
            hashlib.sha256(b"remote-model-host").hexdigest(),
        )
        self.assertEqual(
            attestation["model_host_identity_source"],
            "ssh_controlmaster_bound_remote_hostname_sha256",
        )
        self.assertTrue(attestation["host_identities_distinct"])
        self.assertEqual(run.call_count, 2)
        self.assertIn("-S", run.call_args_list[0].args[0])
        self.assertIn("-S", run.call_args_list[1].args[0])
        serialized = json.dumps(attestation, sort_keys=True)
        self.assertNotIn("remote-model-host", serialized)
        self.assertNotIn("remote.example", serialized)
        self.assertNotIn("txnmem-control", serialized)

    def test_controlmaster_pid_mismatch_rejects_remote_identity_claim(self):
        command = (
            "ssh -M -S /private/tmp/txnmem-control -N "
            "-L 18001:127.0.0.1:8000 user@remote.example"
        )
        with patch(
            "txnmem_model_load.subprocess.run",
            return_value=subprocess.CompletedProcess(
                [], 0, "Master running (pid=4243)\n", ""
            ),
        ) as run:
            attestation = _observe_ssh_tunnel(
                "http://127.0.0.1:18001/v1",
                4242,
                command_override=command,
            )

        self.assertEqual(attestation["status"], "process_observed_but_controlmaster_pid_mismatch")
        self.assertIsNone(attestation["model_host_identity_sha256"])
        self.assertFalse(attestation["host_identities_distinct"])
        self.assertEqual(run.call_count, 1)

    def test_controlmaster_observation_preserves_port_and_accepts_pid_on_stderr(self):
        command = (
            "ssh -tt -M -S /private/tmp/txnmem-control -o ControlPersist=no "
            "-p 32222 -L 18002:127.0.0.1:8000 -N user@remote.example"
        )
        with patch(
            "txnmem_model_load.subprocess.run",
            side_effect=[
                subprocess.CompletedProcess([], 0, "", "Master running (pid=4242)\n"),
                subprocess.CompletedProcess([], 0, "remote-model-host\n", ""),
            ],
        ) as run:
            attestation = _observe_ssh_tunnel(
                "http://127.0.0.1:18002/v1",
                4242,
                command_override=command,
            )

        self.assertEqual(attestation["status"], "process_observed")
        self.assertTrue(attestation["controlmaster_pid_matches_tunnel"])
        self.assertEqual(run.call_args_list[0].args[0][3:5], ["-p", "32222"])
        self.assertEqual(run.call_args_list[1].args[0][3:5], ["-p", "32222"])


if __name__ == "__main__":
    unittest.main()
