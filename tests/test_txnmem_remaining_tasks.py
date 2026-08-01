import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_backend import InstrumentedMemoryBackend, AgentReplayRunner  # noqa: E402
from txnmem_bench_adapters import adapt_records  # noqa: E402
from txnmem_concurrency import run_concurrent_action_sequences  # noqa: E402
from txnmem_distributed import run_process_action_sequences  # noqa: E402
from txnmem_event_contract import EventContractError, validate_event, validate_events  # noqa: E402
from txnmem_interleavings import enumerate_interleavings, micro_witness_report  # noqa: E402
from txnmem_performance import benchmark_replay  # noqa: E402
from txnmem_repair import incremental_repair, repair_failure_matrix  # noqa: E402
from txnmem_realism import calibrate_config, split_holdout, trace_evidence_summary  # noqa: E402
from txnmem_trace_pipeline import load_trace_records, build_trace_instances, trace_inventory  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemRemainingTaskTests(unittest.TestCase):
    def test_native_event_contract_accepts_write_and_preserves_derive_sources(self):
        write = validate_event(
            {
                "event_id": "e1",
                "kind": "memory_write",
                "agent_id": "agent_1",
                "step": 1,
                "memory_id": "m1",
                "value": "safe",
            }
        )
        derive = validate_event(
            {
                "event_id": "e2",
                "kind": "memory_derive",
                "agent_id": "agent_1",
                "step": 2,
                "memory_id": "m2",
                "source_ids": ["m1"],
            }
        )
        self.assertEqual(write["memory_id"], "m1")
        self.assertEqual(derive["source_ids"], ["m1"])

    def test_native_event_contract_rejects_invalid_shape_with_stable_codes(self):
        cases = [
            ({"kind": "memory_write", "agent_id": "a", "step": 1, "memory_id": "m"}, "missing_event_id"),
            ({"event_id": "e", "kind": "unknown", "agent_id": "a", "step": 1}, "unsupported_kind"),
            ({"event_id": "e", "kind": "memory_write", "agent_id": "a", "step": 0, "memory_id": "m"}, "invalid_step"),
            ({"event_id": "e", "kind": "memory_derive", "agent_id": "a", "step": 1, "memory_id": "m"}, "missing_source_ids"),
        ]
        for event, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(EventContractError) as raised:
                    validate_event(event)
                self.assertEqual(raised.exception.code, code)

    def test_native_event_contract_rejects_duplicate_or_non_monotonic_events(self):
        events = [
            {"event_id": "e1", "kind": "memory_write", "agent_id": "a", "step": 2, "memory_id": "m1"},
            {"event_id": "e1", "kind": "memory_write", "agent_id": "a", "step": 1, "memory_id": "m2"},
        ]
        with self.assertRaises(EventContractError) as duplicate:
            validate_events(events)
        self.assertEqual(duplicate.exception.code, "duplicate_event_id")

        with self.assertRaises(EventContractError) as order:
            validate_events(
                [
                    {**events[0], "event_id": "e2"},
                    {**events[1], "event_id": "e3"},
                ]
            )
        self.assertEqual(order.exception.code, "non_monotonic_step")

    def test_instrumented_backend_exposes_validated_events(self):
        backend = InstrumentedMemoryBackend()
        backend.write("m0", value="v0")
        validated = backend.validated_events()
        self.assertEqual(validated[0]["step"], 1)
        self.assertEqual(validated[0]["agent_id"], "agent_1")

    def test_benchmark_adapters_map_native_shapes_without_inventing_events(self):
        tau = adapt_records(
            "tau-bench",
            [
                {"id": "c1", "tool_name": "memory_read", "arguments": {"memory_id": "m0"}},
                {"id": "c2", "tool_name": "memory_write", "arguments": {"memory_id": "m1", "value": "v1"}},
                {"id": "p1", "policy_guideline": "deny writes", "agent_id": "agent_1"},
                {"id": "chat", "role": "user", "content": "hello"},
            ],
        )
        self.assertEqual([event["kind"] for event in tau.events], ["memory_read", "memory_write", "policy_revoke"])
        self.assertEqual(tau.skipped_events, 1)
        self.assertEqual(tau.events[1]["memory_id"], "m1")

        loco = adapt_records(
            "locomo",
            [{"turn_id": 4, "event_type": "memory_update", "old_memory_id": "m0", "new_memory_id": "m1"}],
        )
        self.assertEqual(loco.events[0]["kind"], "memory_supersede")
        self.assertEqual(loco.events[0]["old_memory_id"], "m0")

        appworld = adapt_records(
            "appworld",
            [{"api_name": "write_memory", "args": {"memory_id": "m2", "value": "v2"}}],
        )
        self.assertEqual(appworld.events[0]["kind"], "memory_write")

        appworld_log = adapt_records(
            "appworld",
            [{"task_id": "t1", "method": "get", "url": "/spotify/library/playlists", "data": {}},
             {"task_id": "t1", "method": "post", "url": "/supervisor/complete_task", "data": {}}],
        )
        self.assertEqual([event["kind"] for event in appworld_log.events], ["memory_search", "memory_write"])
        self.assertTrue(appworld_log.events[1]["memory_id"].startswith("appworld:"))

    def test_adapters_project_official_tau_and_locomo_shapes_explicitly(self):
        tau = adapt_records(
            "tau-bench",
            [
                {
                    "task_id": 7,
                    "traj": [
                        {"role": "system", "content": "# Airline Agent Policy"},
                        {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "function": {"name": "get_user_details", "arguments": "{\"user_id\": \"u1\"}"},
                                }
                            ],
                        },
                    ],
                }
            ],
        )
        self.assertEqual([event["kind"] for event in tau.events], ["policy_change", "memory_read"])
        self.assertEqual(tau.events[1]["projection"], "tau_api_tool_call")

        locomo = adapt_records(
            "locomo",
            [
                {
                    "sample_id": "sample_1",
                    "conversation": {"session_1_date_time": "2023-01-01"},
                    "session_summary": {"session_1_summary": "A temporal fact."},
                }
            ],
        )
        self.assertEqual(len(locomo.events), 1)
        self.assertEqual(locomo.events[0]["projection"], "locomo_session_summary")

    def test_trace_pipeline_loads_jsonl_and_builds_trace_grounded_instance(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            path.write_text(
                "\n".join(
                    [
                        json.dumps({"event_id": "e1", "kind": "memory_write", "memory_id": "m1"}),
                        json.dumps({"event_id": "e2", "kind": "memory_read", "memory_id": "m1"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            records = load_trace_records(path)
            instances = build_trace_instances(records, "normalized", source="fixture")
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]["workload"], "trace_grounded_replay")
        self.assertEqual(instances[0]["provenance_edges"], [])
        self.assertEqual(instances[0]["trace_metadata"]["source"], "fixture")

    def test_trace_pipeline_wraps_external_episode_in_replay_transaction(self):
        records = [
            {"event_id": "e1", "kind": "memory_write", "memory_id": "m1"},
            {"event_id": "e2", "kind": "memory_read", "memory_id": "m1"},
        ]
        instance = build_trace_instances(records, "normalized", source="fixture")[0]
        self.assertEqual(
            [operation["type"] for operation in instance["operations"]],
            ["begin_txn", "write", "read", "commit"],
        )
        self.assertEqual(instance["operations"][1]["step"], 2)
        self.assertEqual(instance["operations"][-1]["txn_id"], "txn_trace")
        self.assertEqual(trace_inventory([instance])["event_count"], 2)

    def test_trace_pipeline_keeps_tau_trials_separate(self):
        records = [
            {"task_id": 1, "trial": 0, "traj": [{"role": "assistant", "tool_calls": [{"function": {"name": "get_user_details", "arguments": "{}"}}]}]},
            {"task_id": 1, "trial": 1, "traj": [{"role": "assistant", "tool_calls": [{"function": {"name": "get_user_details", "arguments": "{}"}}]}]},
        ]
        instances = build_trace_instances(records, "tau-bench", source="fixture")
        self.assertEqual(len(instances), 2)

    def test_calibration_and_holdout_are_deterministic(self):
        records = [{"episode_id": str(i), "operation_count": i + 1} for i in range(10)]
        first = split_holdout(records, holdout_fraction=0.2, seed=7)
        second = split_holdout(records, holdout_fraction=0.2, seed=7)
        self.assertEqual(first, second)
        config = calibrate_config(
            [
                {"transaction_size": 3, "provenance_depth": 4, "branch_factor": 2, "agent_count": 2},
                {"transaction_size": 5, "provenance_depth": 2, "branch_factor": 4, "agent_count": 3},
            ]
        )
        self.assertEqual(config["txn_size"], 4)
        self.assertEqual(config["provenance_depth"], 3)
        self.assertEqual(config["branch_factor"], 3)

        train, holdout = split_holdout(
            [{"sample_id": str(i), "value": i} for i in range(5)], holdout_fraction=0.2, seed=2
        )
        self.assertEqual(len(train), 4)
        self.assertEqual(len(holdout), 1)

        tau_trials = [{"task_id": 0, "trial": trial, "value": trial} for trial in range(4)]
        tau_train, tau_holdout = split_holdout(tau_trials, holdout_fraction=0.25, seed=2)
        self.assertEqual(len(tau_train), 3)
        self.assertEqual(len(tau_holdout), 1)

    def test_exhaustive_micro_witness_enumerates_all_linearizations(self):
        left = [{"op_id": "a1", "type": "read"}, {"op_id": "a2", "type": "write"}]
        right = [{"op_id": "b1", "type": "read"}, {"op_id": "b2", "type": "write"}]
        interleavings = enumerate_interleavings([left, right])
        self.assertEqual(len(interleavings), 6)
        self.assertEqual([item["op_id"] for item in interleavings[0]], ["a1", "a2", "b1", "b2"])

        instance = generate_instance("atomic_multi_write", 0, {"txn_size": 1})
        report = micro_witness_report(instance, [[left[0]], [right[0]]])
        self.assertEqual(report["interleaving_count"], 2)
        self.assertIn("oracle_outcome_count", report)

        bypass = generate_instance("scope_bypass", 0)
        mismatch_report = micro_witness_report(bypass, [bypass["operations"]], variant="Naive")
        self.assertEqual(mismatch_report["oracle_mismatch_count"], 1)

    def test_incremental_repair_exposes_safe_and_unsafe_crash_points(self):
        memories = {memory_id: {"memory_id": memory_id, "status": "active"} for memory_id in ["m0", "m1", "m2"]}
        edges = [
            {"source_id": "m0", "derived_id": "m1"},
            {"source_id": "m1", "derived_id": "m2"},
        ]
        partial = incremental_repair(memories, edges, ["m0"], crash_after=1)
        self.assertTrue(partial["crashed"])
        self.assertIn("m2", partial["unsafe_active_ids"])
        matrix = repair_failure_matrix(memories, edges, ["m0"])
        self.assertEqual(matrix["repair_step_count"], 3)
        self.assertEqual(matrix["first_unsafe_crash_after"], 0)

    def test_instrumented_backend_records_events_and_replay_is_injectable(self):
        backend = InstrumentedMemoryBackend()
        runner = AgentReplayRunner(backend)
        runner.run(
            [
                {"type": "write", "memory_id": "m0", "value": "v0"},
                {"type": "read", "memory_id": "m0"},
                {"type": "derive", "memory_id": "m1", "source_ids": ["m0"], "value": "v1"},
            ]
        )
        self.assertEqual([event["kind"] for event in backend.events], ["memory_write", "memory_read", "memory_derive"])
        self.assertEqual(backend.memories["m1"]["derived_from"], ["m0"])
        self.assertEqual(runner.to_instance("agent_fixture")["workload"], "trace_grounded_replay")

    def test_threaded_backend_records_a_real_linearization_order(self):
        result = run_concurrent_action_sequences(
            [
                [{"type": "write", "memory_id": "m_a", "value": "a", "agent_id": "agent_a"}],
                [{"type": "write", "memory_id": "m_b", "value": "b", "agent_id": "agent_b"}],
            ]
        )
        self.assertEqual(result["agent_count"], 2)
        self.assertEqual(result["event_count"], 2)
        self.assertTrue(result["unique_event_ids"])
        self.assertEqual([event["linearization_index"] for event in result["events"]], [1, 2])

    def test_process_harness_preserves_worker_order_and_reports_linearization(self):
        result = run_process_action_sequences(
            [
                [
                    {"type": "write", "memory_id": "p_a1", "value": "a1"},
                    {"type": "write", "memory_id": "p_a2", "value": "a2"},
                ],
                [{"type": "write", "memory_id": "p_b1", "value": "b1"}],
            ]
        )
        self.assertTrue(result["completed"])
        self.assertEqual(result["worker_count"], 2)
        self.assertEqual(result["submitted_operation_count"], 3)
        self.assertEqual(result["event_count"], 3)
        self.assertTrue(result["unique_event_ids"])
        self.assertEqual(
            [event["linearization_index"] for event in result["events"]],
            [1, 2, 3],
        )
        for worker_id in ("worker_0", "worker_1"):
            local_indexes = [
                event["local_index"]
                for event in result["events"]
                if event["worker_id"] == worker_id
            ]
            self.assertEqual(local_indexes, sorted(local_indexes))

    def test_process_harness_reports_worker_failure_without_silent_drop(self):
        result = run_process_action_sequences(
            [[{"type": "unsupported_action", "op_id": "bad_op"}]],
        )
        self.assertFalse(result["completed"])
        self.assertEqual(result["failed_worker_ids"], ["worker_0"])
        self.assertEqual(result["unacknowledged_operation_ids"], ["bad_op"])

    def test_local_performance_benchmark_reports_timing_without_production_claim(self):
        report = benchmark_replay([generate_instance("atomic_multi_write", 0)], ["TxnMem"], repetitions=2)
        self.assertFalse(report["production_latency_claim"])
        self.assertEqual(report["rows"][0]["repetitions"], 2)
        self.assertGreaterEqual(report["rows"][0]["mean_ms"], 0.0)

    def test_trace_evidence_summary_separates_source_and_envelope_operations(self):
        instances = [
            {
                "operations": [
                    {"type": "begin_txn"},
                    {"type": "write"},
                    {"type": "commit"},
                ],
                "trace_metadata": {"event_count": 1},
            }
        ]
        rows = [
            {"variant": "TxnMem", "oracle_match": 1},
            {"variant": "Naive", "oracle_match": 0},
        ]
        summary = trace_evidence_summary(instances, rows)
        self.assertEqual(summary["source_operation_count"], 1)
        self.assertEqual(summary["replay_operation_count"], 3)
        self.assertEqual(summary["replay_envelope_operation_count"], 2)
        self.assertEqual(summary["oracle_match_by_variant"]["TxnMem"]["matched"], 1)
        self.assertFalse(summary["trace_ground_truth_native"])
        self.assertFalse(summary["production_latency_claim"])


if __name__ == "__main__":
    unittest.main()
