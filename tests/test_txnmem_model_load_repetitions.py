import unittest

from txnmem_model_load_repetitions import aggregate_model_load_repetitions


def _summary(index: int) -> dict:
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
        "execution_scope": "cross_host_client_server",
        "host_count": 2,
        "agent_worker_host_count": 1,
        "model_server_host_count": 1,
        "network_transport": "ssh_local_port_forward",
        "task_count_per_cycle": 8,
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
        "topology_attested": True,
        "cross_host_network_claim": True,
        "cross_host_multi_agent_workers_claim": False,
        "topology_attestation": {"status": "process_observed"},
        "latency_ms": {"p50": 10.0 + index, "p95": 20.0 + index, "p99": 30.0 + index},
        "task_summaries": [
            {"task_evaluator": {"success": True}} for _ in range(80)
        ],
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


if __name__ == "__main__":
    unittest.main()
