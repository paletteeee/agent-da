import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from txnmem_model_protocol import (
    ModelProtocolError,
    ModelResponse,
    OpenAICompatibleClient,
    TokenUsage,
    ToolCall,
    parse_chat_completion,
)
from txnmem_backend import InstrumentedMemoryBackend, SQLiteInstrumentedMemoryBackend
from txnmem_benchmark_bridge import (
    BenchmarkEnvAdapter,
    adapt_appworld_arguments,
    appworld_tool_allowed,
    build_benchmark_system_prompt,
    infer_appworld_app_names,
    run_benchmark_agent,
)
from txnmem_real_agent import NativeMemoryToolGateway, run_real_agent
from txnmem_real_experiment import (
    RealExperimentError,
    evaluate_native_trace,
    evaluate_task_contract,
    load_task_manifest,
    run_experiment_manifest,
    run_benchmark_batch,
    run_benchmark_experiment_manifest,
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


class _NoopBenchmarkAdapter(BenchmarkEnvAdapter):
    dataset = "fixture"

    def tool_schemas(self):
        return []

    def reset(self, task):
        return str(task["instruction"])

    def execute(self, name, arguments):
        raise AssertionError("fixture should not call benchmark tools")

    def evaluate(self, run_report):
        return {"success": True}


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

    def test_parser_preserves_openai_compatible_token_usage(self):
        response = parse_chat_completion(
            {
                "choices": [{"message": {"content": "done", "tool_calls": []}}],
                "usage": {
                    "prompt_tokens": 101,
                    "completion_tokens": 7,
                    "total_tokens": 108,
                },
            }
        )

        self.assertEqual(response.usage.prompt_tokens, 101)
        self.assertEqual(response.usage.completion_tokens, 7)
        self.assertEqual(response.usage.total_tokens, 108)

    def test_client_can_bound_generation_tokens(self):
        client = OpenAICompatibleClient(
            "http://model.test/v1/chat/completions",
            model="local-test",
            max_tokens=256,
        )
        with patch("txnmem_model_protocol.urlopen", self.fake_urlopen):
            client.complete([{"role": "user", "content": "hello"}], [])

        self.assertEqual(self.fake_urlopen.last_request["body"]["max_tokens"], 256)


class _ScriptedModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, messages, tools, *, seed=None, temperature=0.0):
        self.calls.append({"messages": messages, "tools": tools, "seed": seed, "temperature": temperature})
        return self.responses.pop(0)


class TxnMemRealAgentTests(unittest.TestCase):
    def test_tool_loop_aggregates_model_usage_across_steps(self):
        model = _ScriptedModel(
            [
                ModelResponse(
                    "",
                    [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "v"})],
                    TokenUsage(prompt_tokens=20, completion_tokens=5, total_tokens=25),
                ),
                ModelResponse(
                    "done",
                    [],
                    TokenUsage(prompt_tokens=30, completion_tokens=4, total_tokens=34),
                ),
            ]
        )

        report = run_real_agent(
            {"task_id": "usage", "prompt": "remember", "agent_id": "agent_model"},
            model,
            InstrumentedMemoryBackend(),
        )

        self.assertEqual(
            report["model_usage"],
            {
                "request_count": 2,
                "responses_with_usage": 2,
                "prompt_tokens": 50,
                "completion_tokens": 9,
                "total_tokens": 59,
            },
        )

    def test_tuned_appworld_prompt_is_explicit_about_tools_and_verification(self):
        prompt = build_benchmark_system_prompt(
            {"system_prompt": "Keep actions safe."},
            dataset="appworld",
            prompt_profile="tuned",
        )

        self.assertIn("Keep actions safe.", prompt)
        self.assertIn("exact function schema", prompt)
        self.assertIn("verify", prompt.lower())
        self.assertIn("do not change AppWorld state", prompt)
        self.assertIn("publicly means private=false", prompt.lower())
        self.assertIn("answer=null", prompt.lower())
        self.assertIn("phone contacts", prompt.lower())

    def test_tuned_appworld_app_selection_recovers_instruction_apps(self):
        self.assertEqual(
            infer_appworld_app_names(
                'Request $13 publicly on Venmo with a note "meal".',
                supplied_app_names=["supervisor"],
            ),
            ["venmo", "supervisor"],
        )
        self.assertEqual(
            infer_appworld_app_names(
                "Request money on Venmo from my friend Stacy.",
                supplied_app_names=["supervisor"],
            ),
            ["phone", "venmo", "supervisor"],
        )
        self.assertEqual(
            infer_appworld_app_names(
                "Email the Spotify playlist to my phone contact.",
                supplied_app_names=["supervisor"],
            ),
            ["gmail", "phone", "spotify", "supervisor"],
        )

    def test_tuned_appworld_allowlist_never_removes_supervisor_tools(self):
        allowlist = ["amazon__search_products"]
        self.assertTrue(
            appworld_tool_allowed(
                "supervisor__show_account_passwords",
                allowlist,
                always_allow_supervisor=True,
            )
        )
        self.assertTrue(
            appworld_tool_allowed(
                "amazon__search_products",
                allowlist,
                always_allow_supervisor=True,
            )
        )
        self.assertFalse(
            appworld_tool_allowed(
                "spotify__search_tracks",
                allowlist,
                always_allow_supervisor=True,
            )
        )

    def test_tuned_profile_resets_appworld_before_loading_tool_schemas(self):
        order = []

        class _OrderingAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def tool_schemas(self):
                order.append("schemas")
                return []

            def reset(self, task):
                order.append("reset")
                return str(task["instruction"])

        report = run_benchmark_agent(
            {
                "task_id": "appworld-order",
                "instruction": "complete the workflow",
                "prompt_profile": "tuned",
            },
            _ScriptedModel([ModelResponse("done", [])]),
            InstrumentedMemoryBackend(),
            _OrderingAdapter(),
        )

        self.assertEqual(order[:2], ["reset", "schemas"])
        self.assertEqual(report["prompt_profile"], "tuned")

    def test_tuned_appworld_strategy_prefetches_supervisor_identity_and_credentials(self):
        calls = []

        class _PreflightAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def tool_schemas(self):
                return [
                    {
                        "type": "function",
                        "function": {
                            "name": name,
                            "parameters": {"type": "object", "properties": {}},
                        },
                    }
                    for name in (
                        "supervisor__show_profile",
                        "supervisor__show_account_passwords",
                    )
                ]

            def execute(self, name, arguments):
                calls.append(name)
                return "private observation", {"ok": True}

        model = _ScriptedModel([ModelResponse("done", [])])
        report = run_benchmark_agent(
            {
                "task_id": "appworld-preflight",
                "instruction": "complete the workflow",
                "prompt_profile": "tuned",
            },
            model,
            InstrumentedMemoryBackend(),
            _PreflightAdapter(),
        )

        self.assertEqual(
            calls,
            ["supervisor__show_profile", "supervisor__show_account_passwords"],
        )
        self.assertEqual(report["preflight_tool_count"], 2)
        model_tool_names = {
            item["function"]["name"] for item in model.calls[0]["tools"]
        }
        self.assertNotIn("supervisor__show_profile", model_tool_names)
        self.assertNotIn("supervisor__show_account_passwords", model_tool_names)
        self.assertTrue(
            any(
                "read-only preflight" in str(message.get("content", "")).lower()
                for message in model.calls[0]["messages"]
            )
        )

    def test_tuned_appworld_strategy_prelogs_in_and_prunes_unsupported_arguments(self):
        calls = []

        class _PreloginAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def tool_schemas(self):
                return [
                    {
                        "type": "function",
                        "function": {
                            "name": name,
                            "parameters": {
                                "type": "object",
                                "properties": properties,
                                "required": required,
                            },
                        },
                    }
                    for name, properties, required in (
                        ("supervisor__show_profile", {}, []),
                        ("supervisor__show_account_passwords", {}, []),
                        (
                            "venmo__login",
                            {
                                "username": {"type": "string", "description": "Your account email."},
                                "password": {"type": "string"},
                            },
                            ["username", "password"],
                        ),
                        (
                            "venmo__show_profile",
                            {
                                "access_token": {"type": "string"},
                                "email": {"type": "string"},
                            },
                            ["access_token"],
                        ),
                    )
                ]

            def execute(self, name, arguments):
                calls.append((name, dict(arguments)))
                if name == "supervisor__show_profile":
                    return str({"email": "agent@example.test", "phone_number": "+10000000000"}), {"ok": True}
                if name == "supervisor__show_account_passwords":
                    return str([{"account_name": "venmo", "password": "correct-password"}]), {"ok": True}
                if name == "venmo__login":
                    return str({"access_token": "official-token", "token_type": "Bearer"}), {"ok": True}
                return "profile", {"ok": True}

        model = _ScriptedModel(
            [
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "call-1",
                            "venmo__show_profile",
                            {
                                "access_token": "official-token",
                                "email": "agent@example.test",
                                "unsupported": "must-be-removed",
                            },
                        )
                    ],
                ),
                ModelResponse("done", []),
            ]
        )
        report = run_benchmark_agent(
            {
                "task_id": "appworld-prelogin",
                "instruction": "Use Venmo to complete the workflow.",
                "prompt_profile": "tuned",
            },
            model,
            InstrumentedMemoryBackend(),
            _PreloginAdapter(),
        )

        self.assertEqual(
            calls[:3],
            [
                ("supervisor__show_profile", {}),
                ("supervisor__show_account_passwords", {}),
                (
                    "venmo__login",
                    {"username": "agent@example.test", "password": "correct-password"},
                ),
            ],
        )
        self.assertEqual(
            calls[3],
            (
                "venmo__show_profile",
                {"access_token": "official-token", "email": "agent@example.test"},
            ),
        )
        self.assertEqual(report["preflight_login_count"], 1)
        self.assertEqual(report["preflight_tool_count"], 3)
        visible_messages = json.dumps(model.calls[0]["messages"], ensure_ascii=False)
        self.assertNotIn("correct-password", visible_messages)
        self.assertIn("official-token", visible_messages)
        self.assertEqual(
            [item["name"] for item in report["benchmark_tool_trace"]],
            [
                "supervisor__show_profile",
                "supervisor__show_account_passwords",
                "venmo__login",
                "venmo__show_profile",
            ],
        )
        self.assertEqual(
            report["benchmark_tool_trace"][-1]["argument_keys"],
            ["access_token", "email"],
        )

    def test_appworld_argument_adapter_keeps_only_schema_properties(self):
        self.assertEqual(
            adapt_appworld_arguments(
                {"username": "u", "password": "p", "access_token": "wrong-extra"},
                {"username", "password"},
            ),
            {"username": "u", "password": "p"},
        )

    def test_tuned_appworld_reprompts_empty_turn_until_complete_task(self):
        calls = []

        class _CompletionAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def tool_schemas(self):
                return [
                    {
                        "type": "function",
                        "function": {
                            "name": "supervisor__complete_task",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "answer": {},
                                    "status": {"type": "string"},
                                },
                            },
                        },
                    }
                ]

            def execute(self, name, arguments):
                calls.append((name, dict(arguments)))
                return str({"message": "Task completed successfully."}), {"ok": True}

        model = _ScriptedModel(
            [
                ModelResponse("I am done.", []),
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "complete-1",
                            "supervisor__complete_task",
                            {"answer": None, "status": "success"},
                        )
                    ],
                ),
            ]
        )
        report = run_benchmark_agent(
            {
                "task_id": "appworld-completion-loop",
                "instruction": "Complete the workflow.",
                "prompt_profile": "tuned",
            },
            model,
            InstrumentedMemoryBackend(),
            _CompletionAdapter(),
            max_steps=4,
        )

        self.assertEqual(
            calls,
            [("supervisor__complete_task", {"answer": None, "status": "success"})],
        )
        self.assertEqual(report["steps"], 2)
        self.assertEqual(report["status"], "completed")
        self.assertTrue(
            any(
                "call at least one function" in str(message.get("content", "")).lower()
                for message in model.calls[1]["messages"]
            )
        )

    def test_tuned_appworld_prefetches_named_contact_after_phone_login(self):
        calls = []

        class _ContactAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def tool_schemas(self):
                function_specs = {
                    "supervisor__show_profile": ({}, []),
                    "supervisor__show_account_passwords": ({}, []),
                    "phone__login": (
                        {
                            "username": {"type": "string", "description": "Your account phone_number."},
                            "password": {"type": "string"},
                        },
                        ["username", "password"],
                    ),
                    "phone__search_contacts": (
                        {
                            "access_token": {"type": "string"},
                            "query": {"type": "string"},
                            "relationship": {"type": "string"},
                            "page_index": {"type": "integer"},
                            "page_limit": {"type": "integer"},
                        },
                        ["access_token"],
                    ),
                }
                return [
                    {
                        "type": "function",
                        "function": {
                            "name": name,
                            "parameters": {
                                "type": "object",
                                "properties": properties,
                                "required": required,
                            },
                        },
                    }
                    for name, (properties, required) in function_specs.items()
                ]

            def execute(self, name, arguments):
                calls.append((name, dict(arguments)))
                if name == "supervisor__show_profile":
                    return str({"email": "agent@example.test", "phone_number": "+10000000000"}), {"ok": True}
                if name == "supervisor__show_account_passwords":
                    return str([{"account_name": "phone", "password": "phone-password"}]), {"ok": True}
                if name == "phone__login":
                    return str({"access_token": "phone-token", "token_type": "Bearer"}), {"ok": True}
                return str([{"first_name": "Stacy", "relationship": "friend"}]), {"ok": True}

        report = run_benchmark_agent(
            {
                "task_id": "appworld-contact-prefetch",
                "instruction": "Request money from my friend, Stacy, on Venmo.",
                "prompt_profile": "tuned",
            },
            _ScriptedModel([ModelResponse("done", [])]),
            InstrumentedMemoryBackend(),
            _ContactAdapter(),
        )

        self.assertEqual(
            calls[-1],
            (
                "phone__search_contacts",
                {
                    "access_token": "phone-token",
                    "query": "Stacy",
                    "relationship": "friend",
                    "page_index": 0,
                    "page_limit": 20,
                },
            ),
        )
        self.assertEqual(report["preflight_contact_lookup_count"], 1)
        self.assertEqual(report["preflight_tool_count"], 4)

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
        self.assertEqual(report["evidence"]["oracle_match_by_variant"]["TxnMem"]["matched"], 1)
        self.assertTrue(report["evidence"]["trace_ground_truth_native"])

    def test_native_supersede_trace_matches_oracle_when_writes_are_buffered(self):
        events = [
            {"event_id": "e1", "kind": "memory_write", "agent_id": "agent_model", "step": 1, "memory_id": "old_fact"},
            {"event_id": "e2", "kind": "memory_write", "agent_id": "agent_model", "step": 2, "memory_id": "corrected_fact"},
            {
                "event_id": "e3",
                "kind": "memory_supersede",
                "agent_id": "agent_model",
                "step": 3,
                "old_memory_id": "old_fact",
                "new_memory_id": "corrected_fact",
            },
        ]
        report = evaluate_native_trace(events, "native_supersede", seed=5)

        self.assertEqual(report["evidence"]["oracle_match_by_variant"]["TxnMem"]["matched"], 1)

    def test_native_propagate_trace_matches_oracle_with_explicit_source_id(self):
        events = [
            {"event_id": "e1", "kind": "memory_write", "agent_id": "agent_model", "step": 1, "memory_id": "source_fact"},
            {
                "event_id": "e2",
                "kind": "memory_propagate",
                "agent_id": "agent_model",
                "step": 2,
                "memory_id": "task_memory",
                "source_id": "source_fact",
            },
        ]
        report = evaluate_native_trace(events, "native_propagate", seed=6)

        self.assertEqual(report["evidence"]["oracle_match_by_variant"]["TxnMem"]["matched"], 1)

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

    def test_benchmark_manifest_can_use_persistent_memory_backend_factory(self):
        model = _ScriptedModel(
            [
                ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "private"})]),
                ModelResponse("done", []),
            ]
        )
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            result = run_benchmark_experiment_manifest(
                {
                    "tasks": [
                        {
                            "task_id": "persistent_task",
                            "instruction": "write",
                            "prompt": "write",
                            "agent_id": "agent_model",
                        }
                    ]
                },
                model,
                lambda: _NoopBenchmarkAdapter(),
                out_dir,
                backend_factory=lambda index, root: SQLiteInstrumentedMemoryBackend(
                    root / "data" / f"memory_{index:04d}.sqlite"
                ),
            )
            self.assertEqual(result["native_event_count"], 1)
            database = out_dir / "data" / "memory_0001.sqlite"
            self.assertTrue(database.exists())
            reopened = SQLiteInstrumentedMemoryBackend(database)
            self.assertEqual(reopened.read("m1")["value"], "private")
            reopened.close()

    def test_benchmark_manifest_closes_adapter_after_each_task(self):
        closed = []

        class _ClosingAdapter(_NoopBenchmarkAdapter):
            def close(self):
                closed.append(True)

        with TemporaryDirectory() as tmp:
            run_benchmark_experiment_manifest(
                {
                    "tasks": [
                        {
                            "task_id": "close_adapter",
                            "instruction": "complete",
                            "prompt": "complete",
                        }
                    ]
                },
                _ScriptedModel([ModelResponse("done", [])]),
                lambda: _ClosingAdapter(),
                Path(tmp),
            )

        self.assertEqual(closed, [True])

    def test_benchmark_batch_aggregates_usage_and_prompt_profile(self):
        model = _ScriptedModel(
            [
                ModelResponse(
                    "done",
                    [],
                    TokenUsage(prompt_tokens=10, completion_tokens=2, total_tokens=12),
                ),
                ModelResponse(
                    "done",
                    [],
                    TokenUsage(prompt_tokens=11, completion_tokens=3, total_tokens=14),
                ),
            ]
        )
        manifest = {
            "dataset_name": "appworld",
            "tasks": [
                {
                    "task_id": "appworld-1",
                    "instruction": "complete",
                    "prompt": "complete",
                    "prompt_profile": "tuned",
                }
            ],
        }

        with TemporaryDirectory() as tmp:
            report = run_benchmark_batch(
                manifest,
                model,
                Path(tmp),
                adapter_factory=lambda: _NoopBenchmarkAdapter(),
                repetitions=2,
            )

        self.assertEqual(report["prompt_profiles"], ["tuned"])
        self.assertEqual(report["model_usage"]["request_count"], 2)
        self.assertEqual(report["model_usage"]["total_tokens"], 26)
        self.assertTrue(report["token_usage_complete"])
        self.assertEqual(report["task_summaries"][0]["benchmark_tool_trace"], [])

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
            with patch(
                "txnmem_real_experiment.evaluate_native_trace",
                side_effect=[KeyError("replay"), {"evidence": {}, "variant_summary": {}}],
            ):
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
