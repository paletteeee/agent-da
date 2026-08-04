import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from txnmem_model_protocol import (
    ModelProtocolError,
    ModelResponse,
    OpenAICompatibleClient,
    ToolCall,
    parse_chat_completion,
)
from txnmem_backend import InstrumentedMemoryBackend
from txnmem_real_agent import NativeMemoryToolGateway, run_real_agent
from txnmem_real_experiment import (
    RealExperimentError,
    evaluate_native_trace,
    evaluate_task_contract,
    load_task_manifest,
    run_experiment_manifest,
    sanitize_run_report,
    split_task_manifest,
)
from txnmem_failure_controller import FailureController, FailureInjectionError


class _FakeHTTPResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


class _FakeURLopener:
    response_payload = {
        "choices": [
            {
                "message": {
                    "content": "I will update memory.",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "memory_write",
                                "arguments": '{"memory_id":"m1","value":"safe"}',
                            },
                        }
                    ],
                }
            }
        ]
    }

    def __init__(self):
        self.last_request = None

    def __call__(self, request, timeout):
        self.last_request = {
            "headers": dict(request.header_items()),
            "body": json.loads(request.data.decode("utf-8")),
            "timeout": timeout,
        }
        return _FakeHTTPResponse(self.response_payload)


class TxnMemRealModelProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fake_urlopen = _FakeURLopener()

    def test_client_parses_tool_calls_and_sends_fixed_request_metadata(self):
        endpoint = "http://model.test/v1/chat/completions"
        client = OpenAICompatibleClient(endpoint, model="local-test", api_key="secret-key")
        with patch("txnmem_model_protocol.urlopen", self.fake_urlopen):
            response = client.complete(
                messages=[{"role": "user", "content": "remember this"}],
                tools=[{"type": "function", "function": {"name": "memory_write"}}],
                seed=7,
                temperature=0.0,
            )
        self.assertEqual(response.text, "I will update memory.")
        self.assertEqual(response.tool_calls[0].name, "memory_write")
        self.assertEqual(response.tool_calls[0].arguments["memory_id"], "m1")
        request = self.fake_urlopen.last_request
        self.assertEqual(request["body"]["model"], "local-test")
        self.assertEqual(request["body"]["seed"], 7)
        self.assertEqual(request["body"]["temperature"], 0.0)
        self.assertEqual(request["headers"]["Authorization"], "Bearer secret-key")
        metadata = client.request_metadata(seed=7, temperature=0.0, message_count=1, tool_count=1)
        self.assertNotIn("secret-key", json.dumps(metadata))
        self.assertEqual(metadata["model"], "local-test")

    def test_parser_rejects_malformed_tool_arguments_with_stable_code(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_bad",
                                "function": {"name": "memory_write", "arguments": "{"},
                            }
                        ],
                    }
                }
            ]
        }
        with self.assertRaises(ModelProtocolError) as raised:
            parse_chat_completion(payload)
        self.assertEqual(raised.exception.code, "invalid_tool_arguments")

    def test_parser_rejects_missing_choices_with_stable_code(self):
        with self.assertRaises(ModelProtocolError) as raised:
            parse_chat_completion({"choices": []})
        self.assertEqual(raised.exception.code, "missing_choices")


class _ScriptedModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        self.calls.append({"messages": messages, "tools": tools, "seed": seed, "temperature": temperature})
        return self.responses.pop(0)


class TxnMemRealAgentTests(unittest.TestCase):
    def test_tool_loop_records_actual_derive_sources(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "source", "value": "s"})]),
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "c2",
                            "memory_derive",
                            {"memory_id": "derived", "source_ids": ["source"], "value": "d"},
                        )
                    ],
                ),
                ModelResponse("done", []),
            ]
        )
        report = run_real_agent(
            {"task_id": "task_1", "prompt": "remember and derive", "agent_id": "agent_model"},
            model,
            InstrumentedMemoryBackend(),
            max_steps=5,
            seed=11,
        )
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["steps"], 3)
        derive = next(event for event in report["events"] if event["kind"] == "memory_derive")
        self.assertEqual(derive["source_ids"], ["source"])
        self.assertEqual(derive["agent_id"], "agent_model")
        self.assertEqual(model.calls[0]["seed"], 11)
        self.assertTrue(NativeMemoryToolGateway.schemas())

    def test_unknown_tool_stops_with_stable_failure_code(self):
        model = _ScriptedModel([ModelResponse("", [ToolCall("bad", "memory_delete", {})])])
        report = run_real_agent(
            {"task_id": "task_2", "prompt": "delete", "agent_id": "agent_model"},
            model,
            InstrumentedMemoryBackend(),
        )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failure_code"], "unknown_tool")

    def test_tool_loop_reports_max_steps_without_silent_success(self):
        model = _ScriptedModel(
            [ModelResponse("", [ToolCall("c", "memory_read", {"memory_id": "missing"})]) for _ in range(3)]
        )
        report = run_real_agent(
            {"task_id": "task_3", "prompt": "loop", "agent_id": "agent_model"},
            model,
            InstrumentedMemoryBackend(),
            max_steps=2,
        )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failure_code"], "max_steps_exceeded")

    def test_tool_loop_applies_task_failure_schedule_after_native_write(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "v"})]),
                ModelResponse("done", []),
            ]
        )
        report = run_real_agent(
            {
                "task_id": "task_crash",
                "prompt": "write then stop",
                "agent_id": "agent_model",
                "failure_schedule": [
                    {"trigger": {"kind": "memory_write", "count": 1}, "action": {"type": "crash"}}
                ],
            },
            model,
            InstrumentedMemoryBackend(),
        )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failure_code"], "injected_crash")
        self.assertEqual(report["events"][0]["kind"], "memory_write")


class TxnMemRealExperimentTests(unittest.TestCase):
    def test_native_trace_is_evaluated_by_all_variants_and_keeps_source_count(self):
        backend = InstrumentedMemoryBackend()
        backend.write("source", value="private source", agent_id="agent_model")
        backend.derive(
            "derived",
            source_ids=["source"],
            value="derived fact",
            agent_id="agent_model",
        )
        report = evaluate_native_trace(backend.validated_events(), "native_task", seed=4)
        self.assertEqual(report["evidence"]["source_operation_count"], 2)
        self.assertEqual(len(report["rows"]), 5)
        self.assertIn("TxnMem", report["evidence"]["oracle_match_by_variant"])
        self.assertTrue(report["evidence"]["trace_ground_truth_native"])

    def test_sanitized_report_removes_raw_content_and_events(self):
        report = sanitize_run_report(
            {
                "task_id": "task_1",
                "status": "completed",
                "steps": 2,
                "value": "private",
                "messages": [{"content": "private prompt"}],
                "events": [{"kind": "memory_write", "value": "private"}],
                "evidence": {"source_operation_count": 1},
            }
        )
        serialized = json.dumps(report, ensure_ascii=False)
        self.assertNotIn("private", serialized)
        self.assertNotIn("messages", report)
        self.assertNotIn("events", report)
        self.assertEqual(report["evidence"]["source_operation_count"], 1)

    def test_manifest_requires_a_model_before_creating_results(self):
        with TemporaryDirectory() as tmp:
            with self.assertRaises(RealExperimentError) as raised:
                run_experiment_manifest({"tasks": []}, None, Path(tmp))
        self.assertEqual(raised.exception.code, "missing_model")

    def test_manifest_writes_local_raw_trace_and_sanitized_summary(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "private"})]),
                ModelResponse("done", []),
            ]
        )
        with TemporaryDirectory() as tmp:
            result = run_experiment_manifest(
                {"tasks": [{"task_id": "task_manifest", "prompt": "write", "agent_id": "agent_model"}]},
                model,
                Path(tmp),
            )
            raw = (Path(tmp) / "data" / "native_model_traces.jsonl").read_text(encoding="utf-8")
            summary = (Path(tmp) / "results" / "native_model_summary.json").read_text(encoding="utf-8")
        self.assertIn("native_event_count", result)
        self.assertIn("private", raw)
        self.assertNotIn("private", summary)
        self.assertNotIn("events", result)

    def test_manifest_records_replay_errors_without_aborting_later_tasks(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "old_fact", "value": "old"})]),
                ModelResponse("", [ToolCall("c2", "memory_write", {"memory_id": "new_fact", "value": "new"})]),
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "c3",
                            "memory_supersede",
                            {
                                "old_memory_id": "old_fact",
                                "new_memory_id": "new_fact",
                                "value": "new",
                            },
                        )
                    ],
                ),
                ModelResponse("done", []),
                ModelResponse("", [ToolCall("c4", "memory_write", {"memory_id": "after_error", "value": "ok"})]),
                ModelResponse("done", []),
            ]
        )
        with TemporaryDirectory() as tmp:
            result = run_experiment_manifest(
                {
                    "tasks": [
                        {"task_id": "task_replay_error", "prompt": "supersede", "agent_id": "agent_model"},
                        {"task_id": "task_after_error", "prompt": "supersede", "agent_id": "agent_model"},
                    ]
                },
                model,
                Path(tmp),
            )
        self.assertEqual(result["evaluation_error_count"], 1)
        self.assertEqual(len(result["task_summaries"]), 2)
        self.assertEqual(result["task_summaries"][0]["evaluation_status"], "error")
        self.assertEqual(result["task_summaries"][1]["status"], "completed")

    def test_task_contract_evaluator_checks_actual_native_events(self):
        backend = InstrumentedMemoryBackend()
        backend.write("source", value="s", agent_id="agent_model")
        backend.derive("derived", source_ids=["source"], value="d", agent_id="agent_model")
        task = {
            "task_id": "task_eval",
            "acceptance": {
                "required_event_kinds": ["memory_write", "memory_derive"],
                "required_memory_ids": ["source", "derived"],
                "required_provenance": [{"derived_id": "derived", "source_id": "source"}],
            },
        }
        result = evaluate_task_contract(task, {"status": "completed", "events": backend.validated_events()})
        self.assertTrue(result["success"])
        failed = evaluate_task_contract(task, {"status": "failed", "events": backend.validated_events()})
        self.assertFalse(failed["success"])
        self.assertIn("run_not_completed", failed["reasons"])

    def test_task_contract_accepts_declared_expected_failure(self):
        task = {
            "task_id": "task_expected_crash",
            "acceptance": {
                "expected_status": "failed",
                "required_failure_code": "injected_crash",
                "required_event_kinds": ["memory_write"],
            },
        }
        result = evaluate_task_contract(
            task,
            {
                "status": "failed",
                "failure_code": "injected_crash",
                "events": [{"kind": "memory_write", "memory_id": "m1"}],
            },
        )
        self.assertTrue(result["success"])

    def test_task_manifest_hash_and_episode_holdout_are_deterministic(self):
        manifest = {
            "manifest_version": 1,
            "tasks": [
                {"task_id": "t1", "prompt": "p1"},
                {"task_id": "t2", "prompt": "p2"},
                {"task_id": "t3", "prompt": "p3"},
                {"task_id": "t4", "prompt": "p4"},
                {"task_id": "t5", "prompt": "p5"},
            ],
        }
        normalized, digest = load_task_manifest(manifest)
        train_a, holdout_a = split_task_manifest(normalized, 0.2, seed=17)
        train_b, holdout_b = split_task_manifest(normalized, 0.2, seed=17)
        self.assertEqual(digest, load_task_manifest(manifest)[1])
        self.assertEqual(train_a, train_b)
        self.assertEqual(holdout_a, holdout_b)
        self.assertEqual(len(holdout_a), 1)

    def test_failure_controller_crashes_after_first_matching_write(self):
        controller = FailureController(
            [{"trigger": {"kind": "memory_write", "count": 1}, "action": {"type": "crash"}}]
        )
        with self.assertRaises(FailureInjectionError) as raised:
            controller.observe({"kind": "memory_write", "step": 1})
        self.assertEqual(raised.exception.code, "injected_crash")

    def test_failure_controller_revoke_records_policy_event_and_denies_write(self):
        backend = InstrumentedMemoryBackend()
        gateway = NativeMemoryToolGateway(
            backend,
            agent_id="agent_model",
            failure_controller=FailureController(
                [{
                    "trigger": {"kind": "memory_read", "count": 1},
                    "action": {"type": "policy_revoke", "target": "write"},
                }]
            ),
        )
        gateway.call("memory_read", {"memory_id": "source"})
        self.assertEqual(backend.events[-1]["kind"], "policy_revoke")
        with self.assertRaises(Exception) as raised:
            gateway.call("memory_write", {"memory_id": "m1", "value": "v"})
        self.assertIn("policy", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
