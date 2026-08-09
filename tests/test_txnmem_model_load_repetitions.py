import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_model_load_repetitions import _load_summary, aggregate_model_load_repetitions


def _summary(index: int) -> dict:
    task_summaries = []
    for task_index in range(80):
        if task_index < 60:
            status = "completed"
            failure_code = None
        elif task_index < 70:
            status = "failed"
            failure_code = "injected_crash"
        else:
            status = "failed"
            failure_code = "policy_denied"
        prompt_tokens = 13 if task_index < 40 else 12
        completion_tokens = 3 if task_index < 40 else 2
        request_count = 2 if task_index < 20 else 1
        task_summaries.append(
            {
                "status": status,
                "failure_code": failure_code,
                "native_event_count": 3 if task_index < 40 else 2,
                "model_usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                    "request_count": request_count,
                    "responses_with_usage": request_count,
                },
                "task_evaluator": {"success": True},
            }
        )
    return {
        "dataset": "native-load",
        "manifest_sha256": "same-manifest",
        "model_id": "qwen2.5-7b-instruct",
        "execution_identity": {
            "model_revision": "revision",
            "model_server_build": "vllm:version",
            "runner_source_identity": {"fingerprint": "runner"},
        },
        "configured_concurrency": 4,
        "observed_peak_in_flight": 4,
        "generation_parameters": {
            "max_steps": 12,
            "max_tokens": 512,
            "timeout_seconds": 300.0,
        },
        "execution_scope": "cross_host_client_server",
        "host_count": 2,
        "agent_worker_host_count": 1,
        "model_server_host_count": 1,
        "network_transport": "ssh_local_port_forward",
        "task_count_per_cycle": 8,
        "minimum_cycles": 1,
        "minimum_duration_seconds": 600.0,
        "duration_target_met": True,
        "elapsed_seconds": 605.0 + index,
        "completed_cycles": 10,
        "attempt_count": 80,
        "completed_attempt_count": 60,
        "failed_attempt_count": 20,
        "failure_counts": {"injected_crash": 10, "policy_denied": 10},
        "native_event_count": 200,
        "model_usage": {
            "prompt_tokens": 1000,
            "completion_tokens": 200,
            "total_tokens": 1200,
            "request_count": 100,
            "responses_with_usage": 100,
        },
        "token_usage_complete": True,
        "started_at_utc": f"2026-08-09T0{index}:00:00+00:00",
        "ended_at_utc": f"2026-08-09T0{index}:10:06+00:00",
        "topology_attested": True,
        "cross_host_network_claim": True,
        "cross_host_multi_agent_workers_claim": False,
        "topology_attestation": {
            "status": "process_observed",
            "process_id": 1000 + index,
            "agent_host_identity_sha256": "agent-host",
            "model_host_identity_sha256": "model-host",
            "process_command_sha256": "ssh-command",
            "local_forward_matches_model_endpoint": True,
        },
        "latency_ms": {"p50": 10.0 + index, "p95": 20.0 + index, "p99": 30.0 + index},
        "task_summaries": task_summaries,
    }


class ModelLoadRepetitionTests(unittest.TestCase):
    def test_aggregate_requires_and_sums_attested_exact_repetitions(self):
        report = aggregate_model_load_repetitions([_summary(1), _summary(2), _summary(3)])

        self.assertEqual(report["repetition_count"], 3)
        self.assertEqual(report["total_attempt_count"], 240)
        self.assertEqual(report["total_contract_success_count"], 240)
        self.assertEqual(report["total_elapsed_seconds"], 1821.0)
        self.assertEqual(report["model_usage"]["total_tokens"], 3600)
        self.assertTrue(report["token_usage_complete"])
        self.assertTrue(report["all_repetitions_topology_attested"])
        self.assertEqual(report["failure_counts"], {"injected_crash": 30, "policy_denied": 30})
        self.assertEqual(report["duration_design"], "3_independent_repetitions_x_600_seconds")
        self.assertFalse(report["single_continuous_tunnel_claim"])
        self.assertFalse(report["cross_host_multi_agent_workers_claim"])

    def test_aggregate_rejects_condition_mismatch(self):
        changed = _summary(2)
        changed["configured_concurrency"] = 8

        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            aggregate_model_load_repetitions([_summary(1), changed])

        changed_generation = _summary(2)
        changed_generation["generation_parameters"]["timeout_seconds"] = 60.0
        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            aggregate_model_load_repetitions([_summary(1), changed_generation])

    def test_aggregate_rejects_unattested_or_incomplete_repetition(self):
        unattested = _summary(2)
        unattested["topology_attested"] = False
        with self.assertRaisesRegex(ValueError, "topology attestation"):
            aggregate_model_load_repetitions([_summary(1), unattested])

        incomplete = _summary(2)
        incomplete["token_usage_complete"] = False
        with self.assertRaisesRegex(ValueError, "token usage"):
            aggregate_model_load_repetitions([_summary(1), incomplete])

    def test_aggregate_rejects_internally_inconsistent_counts(self):
        inconsistent_usage = _summary(2)
        inconsistent_usage["model_usage"]["responses_with_usage"] = 99
        with self.assertRaisesRegex(ValueError, "usage counts"):
            aggregate_model_load_repetitions([_summary(1), inconsistent_usage])

        inconsistent_attempts = _summary(2)
        inconsistent_attempts["failed_attempt_count"] = 19
        with self.assertRaisesRegex(ValueError, "attempt counts"):
            aggregate_model_load_repetitions([_summary(1), inconsistent_attempts])

        inconsistent_tasks = _summary(2)
        inconsistent_tasks["task_summaries"].pop()
        with self.assertRaisesRegex(ValueError, "task summary counts"):
            aggregate_model_load_repetitions([_summary(1), inconsistent_tasks])

    def test_aggregate_rejects_duplicate_or_overlapping_runs(self):
        first = _summary(1)
        with self.assertRaisesRegex(ValueError, "duplicate repetition"):
            aggregate_model_load_repetitions([first, first])

        overlapping = _summary(2)
        overlapping["started_at_utc"] = "2026-08-09T01:05:00+00:00"
        overlapping["ended_at_utc"] = "2026-08-09T01:15:06+00:00"
        with self.assertRaisesRegex(ValueError, "time intervals overlap"):
            aggregate_model_load_repetitions([_summary(1), overlapping])

        reused_tunnel = _summary(2)
        reused_tunnel["topology_attestation"]["process_id"] = 1001
        with self.assertRaisesRegex(ValueError, "tunnel process"):
            aggregate_model_load_repetitions([_summary(1), reused_tunnel])

    def test_aggregate_rejects_non_boolean_and_non_finite_fields(self):
        malformed = _summary(2)
        malformed["duration_target_met"] = "false"
        with self.assertRaisesRegex(ValueError, "duration target"):
            aggregate_model_load_repetitions([_summary(1), malformed])

        malformed = _summary(2)
        malformed["elapsed_seconds"] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite JSON"):
            aggregate_model_load_repetitions([_summary(1), malformed])

    def test_claim_boundary_uses_actual_repetition_count(self):
        report = aggregate_model_load_repetitions([_summary(1), _summary(2)])
        self.assertIn("2 independently attested", report["claim_boundary"])

    def test_summary_loader_hashes_the_same_bytes_it_parses(self):
        original = b'{"value": 1}\n'
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "summary.json"
            path.write_bytes(original)
            payload, digest = _load_summary(path)
            path.write_text('{"value": 2}\n', encoding="utf-8")

        self.assertEqual(payload, {"value": 1})
        self.assertEqual(digest, hashlib.sha256(original).hexdigest())


if __name__ == "__main__":
    unittest.main()
