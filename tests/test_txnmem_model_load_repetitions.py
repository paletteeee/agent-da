import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_model_load_repetitions import _load_summary, aggregate_model_load_repetitions


def _attestation(index: int) -> dict:
    return {
        "status": "process_observed",
        "process_id": 1000 + index,
        "agent_host_identity_sha256": "a" * 64,
        "model_host_identity_sha256": "b" * 64,
        "model_host_identity_source": (
            "ssh_controlmaster_bound_remote_hostname_sha256"
        ),
        "ssh_target_identity_sha256": "c" * 64,
        "host_identities_distinct": True,
        "controlmaster_precheck_verified": True,
        "controlmaster_precheck_pid_matches_tunnel": True,
        "controlmaster_postcheck_verified": True,
        "controlmaster_postcheck_pid_matches_tunnel": True,
        "controlmaster_session_verified": True,
        "controlmaster_pid_matches_tunnel": True,
        "process_command_sha256": "d" * 64,
        "exit_on_forward_failure": True,
        "local_forward_matches_model_endpoint": True,
        "forwarding_binding": {
            "local_address_class": "ipv4_loopback",
            "local_port": 18001,
            "remote_address_class": "ipv4_loopback",
            "remote_port": 8000,
        },
        "listener_binding": {
            "address_class": "ipv4_loopback",
            "port": 18001,
            "owned_by_tunnel_process": True,
        },
    }


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
                "attempt_id": (
                    f"cycle_{task_index // 8 + 1:04d}:task-{task_index % 8 + 1}"
                ),
                "cycle": task_index // 8 + 1,
                "source_task_id": f"task-{task_index % 8 + 1}",
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
        "topology_continuity_verified": True,
        "cross_host_network_claim": True,
        "cross_host_multi_agent_workers_claim": False,
        "production_latency_claim": False,
        "topology_preflight_attestation": _attestation(index),
        "topology_attestation": _attestation(index),
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
        self.assertIn("production_latency_claim", report)
        self.assertFalse(report["production_latency_claim"])

    def test_aggregate_requires_explicit_false_production_latency_claim(self):
        missing = [_summary(1), _summary(2), _summary(3)]
        for row in missing:
            row.pop("production_latency_claim")
        with self.assertRaisesRegex(ValueError, "production latency claim"):
            aggregate_model_load_repetitions(missing)

        claimed = [_summary(1), _summary(2), _summary(3)]
        for row in claimed:
            row["production_latency_claim"] = True
        with self.assertRaisesRegex(ValueError, "production latency claim"):
            aggregate_model_load_repetitions(claimed)

    def test_aggregate_rejects_condition_mismatch(self):
        changed = _summary(2)
        changed["configured_concurrency"] = 8

        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            aggregate_model_load_repetitions([_summary(1), changed, _summary(3)])

        changed_generation = _summary(2)
        changed_generation["generation_parameters"]["timeout_seconds"] = 60.0
        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            aggregate_model_load_repetitions(
                [_summary(1), changed_generation, _summary(3)]
            )

    def test_aggregate_rejects_unattested_or_incomplete_repetition(self):
        unattested = _summary(2)
        unattested["topology_attested"] = False
        with self.assertRaisesRegex(ValueError, "topology attestation"):
            aggregate_model_load_repetitions([_summary(1), unattested, _summary(3)])

        incomplete = _summary(2)
        incomplete["token_usage_complete"] = False
        with self.assertRaisesRegex(ValueError, "token usage"):
            aggregate_model_load_repetitions([_summary(1), incomplete, _summary(3)])

    def test_aggregate_rejects_internally_inconsistent_counts(self):
        inconsistent_usage = _summary(2)
        inconsistent_usage["model_usage"]["responses_with_usage"] = 99
        with self.assertRaisesRegex(ValueError, "usage counts"):
            aggregate_model_load_repetitions(
                [_summary(1), inconsistent_usage, _summary(3)]
            )

        inconsistent_attempts = _summary(2)
        inconsistent_attempts["failed_attempt_count"] = 19
        with self.assertRaisesRegex(ValueError, "attempt counts"):
            aggregate_model_load_repetitions(
                [_summary(1), inconsistent_attempts, _summary(3)]
            )

        inconsistent_tasks = _summary(2)
        inconsistent_tasks["task_summaries"].pop()
        with self.assertRaisesRegex(ValueError, "task summary counts"):
            aggregate_model_load_repetitions(
                [_summary(1), inconsistent_tasks, _summary(3)]
            )

    def test_aggregate_rejects_duplicate_or_overlapping_runs(self):
        first = _summary(1)
        with self.assertRaisesRegex(ValueError, "duplicate repetition"):
            aggregate_model_load_repetitions([first, first, _summary(3)])

        overlapping = _summary(2)
        overlapping["started_at_utc"] = "2026-08-09T01:05:00+00:00"
        overlapping["ended_at_utc"] = "2026-08-09T01:15:06+00:00"
        with self.assertRaisesRegex(ValueError, "time intervals overlap"):
            aggregate_model_load_repetitions([_summary(1), overlapping, _summary(3)])

        reused_tunnel = _summary(2)
        reused_tunnel["topology_attestation"]["process_id"] = 1001
        reused_tunnel["topology_preflight_attestation"]["process_id"] = 1001
        with self.assertRaisesRegex(ValueError, "tunnel process"):
            aggregate_model_load_repetitions([_summary(1), reused_tunnel, _summary(3)])

    def test_aggregate_rejects_non_boolean_and_non_finite_fields(self):
        malformed = _summary(2)
        malformed["duration_target_met"] = "false"
        with self.assertRaisesRegex(ValueError, "duration target"):
            aggregate_model_load_repetitions([_summary(1), malformed, _summary(3)])

        malformed = _summary(2)
        malformed["elapsed_seconds"] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite JSON"):
            aggregate_model_load_repetitions([_summary(1), malformed, _summary(3)])

    def test_formal_policy_rejects_two_one_second_repetitions(self):
        summaries = [_summary(1), _summary(2)]
        for row in summaries:
            row["minimum_duration_seconds"] = 1.0

        with self.assertRaisesRegex(ValueError, "exactly three"):
            aggregate_model_load_repetitions(summaries)

    def test_formal_policy_rejects_three_short_repetitions(self):
        summaries = [_summary(1), _summary(2), _summary(3)]
        for row in summaries:
            row["minimum_duration_seconds"] = 599.0

        with self.assertRaisesRegex(ValueError, "at least 600"):
            aggregate_model_load_repetitions(summaries)

    def test_aggregate_rejects_nonzero_utc_offsets(self):
        summaries = [_summary(1), _summary(2), _summary(3)]
        for row in summaries:
            row["started_at_utc"] = row["started_at_utc"].replace("+00:00", "+01:00")
            row["ended_at_utc"] = row["ended_at_utc"].replace("+00:00", "+01:00")

        with self.assertRaisesRegex(ValueError, "zero UTC offset"):
            aggregate_model_load_repetitions(summaries)

    def test_aggregate_requires_preflight_listener_and_postcheck_continuity(self):
        missing_preflight = _summary(2)
        missing_preflight.pop("topology_preflight_attestation")
        with self.assertRaisesRegex(ValueError, "preflight topology"):
            aggregate_model_load_repetitions(
                [_summary(1), missing_preflight, _summary(3)]
            )

        unowned_listener = _summary(2)
        unowned_listener["topology_attestation"]["listener_binding"][
            "owned_by_tunnel_process"
        ] = False
        with self.assertRaisesRegex(ValueError, "listener ownership"):
            aggregate_model_load_repetitions(
                [_summary(1), unowned_listener, _summary(3)]
            )

        failed_postcheck = _summary(2)
        failed_postcheck["topology_attestation"][
            "controlmaster_postcheck_verified"
        ] = False
        with self.assertRaisesRegex(ValueError, "topology identity"):
            aggregate_model_load_repetitions(
                [_summary(1), failed_postcheck, _summary(3)]
            )

        no_continuity = _summary(2)
        no_continuity["topology_continuity_verified"] = False
        with self.assertRaisesRegex(ValueError, "topology continuity"):
            aggregate_model_load_repetitions(
                [_summary(1), no_continuity, _summary(3)]
            )

    def test_normalized_forwarding_binding_is_a_repetition_condition(self):
        changed = _summary(2)
        for key in ("topology_preflight_attestation", "topology_attestation"):
            changed[key]["forwarding_binding"]["remote_port"] = 8001

        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            aggregate_model_load_repetitions([_summary(1), changed, _summary(3)])

    def test_summary_loader_hashes_the_same_bytes_it_parses(self):
        original = b'{"value": 1}\n'
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "summary.json"
            path.write_bytes(original)
            payload, digest = _load_summary(path)
            path.write_text('{"value": 2}\n', encoding="utf-8")

        self.assertEqual(payload, {"value": 1})
        self.assertEqual(digest, hashlib.sha256(original).hexdigest())

    def test_aggregate_rejects_non_cross_host_or_zero_work_summaries(self):
        single_host = [_summary(1), _summary(2), _summary(3)]
        for row in single_host:
            row["execution_scope"] = "single_host_multi_agent"
            row["host_count"] = 1
            row["agent_worker_host_count"] = 1
            row["model_server_host_count"] = 0
            row["network_transport"] = "loopback_or_unspecified"
        with self.assertRaisesRegex(ValueError, "cross-host condition"):
            aggregate_model_load_repetitions(single_host)

        empty = [_summary(1), _summary(2), _summary(3)]
        for row in empty:
            row["completed_cycles"] = 0
            row["attempt_count"] = 0
            row["completed_attempt_count"] = 0
            row["failed_attempt_count"] = 0
            row["failure_counts"] = {}
            row["native_event_count"] = 0
            row["observed_peak_in_flight"] = 0
            row["task_summaries"] = []
            row["model_usage"] = {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "request_count": 0,
                "responses_with_usage": 0,
            }
        with self.assertRaisesRegex(ValueError, "completed cycles"):
            aggregate_model_load_repetitions(empty)

    def test_aggregate_rejects_multiple_model_hosts_even_when_counts_add_up(self):
        summaries = [_summary(1), _summary(2), _summary(3)]
        for row in summaries:
            row["host_count"] = 3
            row["agent_worker_host_count"] = 1
            row["model_server_host_count"] = 2

        with self.assertRaisesRegex(ValueError, "cross-host condition"):
            aggregate_model_load_repetitions(summaries)

    def test_aggregate_requires_attempt_grid_and_strict_evaluator_boolean(self):
        missing_ids = _summary(2)
        for row in missing_ids["task_summaries"]:
            row.pop("attempt_id")
        with self.assertRaisesRegex(ValueError, "attempt IDs"):
            aggregate_model_load_repetitions([_summary(1), missing_ids, _summary(3)])

        malformed_evaluator = _summary(2)
        malformed_evaluator["task_summaries"][0]["task_evaluator"]["success"] = 1
        with self.assertRaisesRegex(ValueError, "task evaluator"):
            aggregate_model_load_repetitions(
                [_summary(1), malformed_evaluator, _summary(3)]
            )

    def test_aggregate_rejects_invalid_generation_parameters_even_when_matched(self):
        invalid = [_summary(1), _summary(2), _summary(3)]
        for row in invalid:
            row["generation_parameters"]["max_steps"] = 0
            row["generation_parameters"]["max_tokens"] = None
            row["generation_parameters"]["timeout_seconds"] = "300"
        with self.assertRaisesRegex(ValueError, "generation parameters"):
            aggregate_model_load_repetitions(invalid)

    def test_aggregate_rejects_zero_work_attempt_or_invalid_topology_identity(self):
        zero_attempt = _summary(2)
        usage = zero_attempt["task_summaries"][0]["model_usage"]
        for key, value in usage.items():
            zero_attempt["model_usage"][key] -= value
            usage[key] = 0
        with self.assertRaisesRegex(ValueError, "attempt usage"):
            aggregate_model_load_repetitions([_summary(1), zero_attempt, _summary(3)])

        invalid_topology = [_summary(1), _summary(2), _summary(3)]
        for row in invalid_topology:
            row["topology_attestation"]["process_id"] = 0
            row["topology_attestation"]["model_host_identity_sha256"] = "short"
        with self.assertRaisesRegex(ValueError, "topology identity"):
            aggregate_model_load_repetitions(invalid_topology)


if __name__ == "__main__":
    unittest.main()
