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
    AppWorldAdapter,
    BenchmarkEnvAdapter,
    adapt_appworld_arguments,
    appworld_tool_allowed,
    build_benchmark_system_prompt,
    infer_appworld_app_names,
    resolve_appworld_app_names,
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
from txnmem_task_transaction import InMemoryTransactionBackend, TaskTransactionGateway
from txnmem_transaction_journal import TransactionJournal


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


class _PreflightAdapter(_NoopBenchmarkAdapter):
    dataset = "appworld"

    def tool_schemas(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": name,
                    "parameters": {
                        "type": "object",
                        "properties": (
                            {
                                "zeta": {
                                    "type": "string",
                                    "description": "fixture-zeta-schema-description",
                                },
                                "alpha": {
                                    "type": "string",
                                    "description": "fixture-alpha-schema-description",
                                },
                            }
                            if name == "venmo__get_profile"
                            else {}
                        ),
                    },
                },
            }
            for name in (
                "venmo__get_profile",
                "supervisor__show_profile",
                "supervisor__show_account_passwords",
            )
        ]

    def execute(self, name, arguments):
        if name == "venmo__get_profile":
            return "fixture-public-observation", {"ok": True}
        return super().execute(name, arguments)

    def execute_trusted_preflight(self, name, arguments):
        if name == "supervisor__show_profile":
            return '{"first_name":"Alex"}', {"ok": True}
        return '{"venmo":"fixture-secret-password"}', {"ok": True}


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
    @staticmethod
    def _write_derive_responses():
        return [
            ModelResponse(
                "",
                [ToolCall("c1", "memory_write", {"memory_id": "source", "value": "s"})],
            ),
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

    def test_explicit_direct_mode_is_report_and_snapshot_compatible_with_default(self):
        task = {"task_id": "direct_compat", "prompt": "remember", "agent_id": "agent_model"}
        default_backend = InstrumentedMemoryBackend()
        direct_backend = InstrumentedMemoryBackend()

        default_report = run_real_agent(
            task,
            _ScriptedModel(self._write_derive_responses()),
            default_backend,
            max_steps=5,
        )
        with TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / "must_not_exist.sqlite3"
            direct_report = run_real_agent(
                task,
                _ScriptedModel(self._write_derive_responses()),
                direct_backend,
                max_steps=5,
                transaction_mode="direct",
                transaction_journal_path=journal_path,
            )
            self.assertFalse(journal_path.exists())

        for field in ("status", "failure_code", "events", "steps"):
            self.assertEqual(direct_report.get(field), default_report.get(field))
        self.assertEqual(direct_backend.snapshot(), default_backend.snapshot())
        self.assertNotIn("transaction", default_report)
        self.assertNotIn("transaction", direct_report)

    def test_explicit_direct_mode_keeps_event_failure_schedule_boundary(self):
        backend = InstrumentedMemoryBackend()
        report = run_real_agent(
            {
                "task_id": "direct_failure",
                "prompt": "write",
                "failure_schedule": [
                    {"trigger": {"kind": "memory_write", "count": 1}, "action": {"type": "crash"}}
                ],
            },
            _ScriptedModel(
                [ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "v"})])]
            ),
            backend,
            transaction_mode="direct",
        )

        self.assertEqual(report["failure_code"], "injected_crash")
        self.assertEqual([event["kind"] for event in report["events"]], ["memory_write"])
        self.assertNotIn("transaction", report)

    def test_task_mode_commits_write_derive_before_completed_status(self):
        backend = InMemoryTransactionBackend()
        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {"task_id": "task_commit", "prompt": "remember", "agent_id": "agent_model"},
                _ScriptedModel(self._write_derive_responses()),
                backend,
                max_steps=5,
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
                transaction_id="txn_commit",
            )

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["transaction"]["decision"], "committed")
        self.assertEqual(
            [event["kind"] for event in report["events"]],
            ["begin_txn", "memory_write", "memory_derive", "commit"],
        )
        self.assertEqual(report["transaction"]["intent_count"], 2)
        self.assertEqual(len(report["transaction"]["state_digest"]), 64)
        self.assertEqual(backend.read_committed("derived")["derived_from"], ["source"])

    def test_task_mode_requires_a_journal_and_never_falls_back(self):
        with self.assertRaises(ValueError):
            run_real_agent(
                {"task_id": "task_missing_journal", "prompt": "remember"},
                _ScriptedModel([ModelResponse("done", [])]),
                InMemoryTransactionBackend(),
                transaction_mode="task",
            )

        with self.assertRaises(ValueError):
            run_real_agent(
                {"task_id": "task_bad_mode", "prompt": "remember"},
                _ScriptedModel([ModelResponse("done", [])]),
                InstrumentedMemoryBackend(),
                transaction_mode="best_effort",
            )

    def test_after_mutation_hook_is_one_based_in_direct_and_task_modes(self):
        task = {"task_id": "mutation_hooks", "prompt": "write and derive"}
        direct_phases = []
        task_phases = []

        direct_report = run_real_agent(
            task,
            _ScriptedModel(self._write_derive_responses()),
            InstrumentedMemoryBackend(),
            transaction_mode="direct",
            transaction_phase_hook=lambda phase, evidence: direct_phases.append(
                (phase, dict(evidence))
            ),
        )
        with TemporaryDirectory() as tmp:
            task_report = run_real_agent(
                task,
                _ScriptedModel(self._write_derive_responses()),
                InMemoryTransactionBackend(),
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
                transaction_phase_hook=lambda phase, evidence: task_phases.append(
                    (phase, dict(evidence))
                ),
            )

        self.assertEqual(direct_report["status"], "completed")
        self.assertEqual(
            [evidence["mutation_count"] for phase, evidence in direct_phases if phase == "after_mutation"],
            [1, 2],
        )
        self.assertEqual(task_report["status"], "completed")
        self.assertEqual(
            [phase for phase, _evidence in task_phases],
            [
                "after_mutation",
                "after_mutation",
                "after_prepare",
                "after_qdrant_stage",
                "after_neo4j_stage",
                "after_stage_verify",
                "after_commit_decision",
                "after_finalize",
            ],
        )

    def test_after_mutation_failure_schedule_is_shared_by_direct_and_task_modes(self):
        schedule = [
            {"trigger": {"phase": "after_mutation", "count": 1}, "action": {"type": "crash"}}
        ]
        for mode, backend in (
            ("direct", InstrumentedMemoryBackend()),
            ("task", InMemoryTransactionBackend()),
        ):
            with self.subTest(mode=mode), TemporaryDirectory() as tmp:
                kwargs = {"transaction_mode": mode}
                if mode == "task":
                    kwargs["transaction_journal_path"] = Path(tmp) / "journal.sqlite3"
                report = run_real_agent(
                    {
                        "task_id": f"mutation_failure_{mode}",
                        "prompt": "write",
                        "failure_schedule": schedule,
                    },
                    _ScriptedModel(
                        [
                            ModelResponse(
                                "",
                                [ToolCall("c1", "memory_write", {"memory_id": "m1", "value": "v"})],
                            )
                        ]
                    ),
                    backend,
                    **kwargs,
                )
                self.assertEqual(report["failure_code"], "injected_crash")
                if mode == "task":
                    self.assertEqual(report["transaction"]["decision"], "aborted")
                    self.assertEqual(report["events"][-1]["kind"], "abort")

    def test_task_phase_policy_revoke_updates_commit_snapshot_and_aborts(self):
        controller = FailureController(
            [
                {
                    "trigger": {"phase": "after_mutation", "count": 1},
                    "action": {"type": "policy_revoke", "target": "write"},
                }
            ]
        )
        backend = InMemoryTransactionBackend()
        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {
                    "task_id": "task_phase_revoke",
                    "prompt": "write",
                    "failure_controller": controller,
                },
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})],
                        ),
                        ModelResponse("final text", []),
                    ]
                ),
                backend,
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
            )

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failure_code"], "policy_revalidation_failed")
        self.assertEqual(report["transaction"]["decision"], "aborted")
        self.assertEqual(report["events"][-1]["kind"], "abort")
        self.assertIsNone(backend.read_committed("pending"))

    def test_task_phase_invalidation_changes_committed_source_and_aborts_derive(self):
        backend = InMemoryTransactionBackend(
            {
                "source": {
                    "memory_id": "source",
                    "value": "source",
                    "status": "active",
                    "version": 4,
                    "derived_from": [],
                }
            }
        )
        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {
                    "task_id": "task_phase_invalidate",
                    "prompt": "derive",
                    "failure_schedule": [
                        {
                            "trigger": {"phase": "after_mutation", "count": 1},
                            "action": {"type": "invalidate", "target": "source"},
                        }
                    ],
                },
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [
                                ToolCall(
                                    "c1",
                                    "memory_derive",
                                    {
                                        "memory_id": "derived",
                                        "source_ids": ["source"],
                                        "value": "derived",
                                    },
                                )
                            ],
                        ),
                        ModelResponse("final text", []),
                    ]
                ),
                backend,
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
            )

        self.assertEqual(report["failure_code"], "source_invalidated")
        self.assertEqual(report["transaction"]["decision"], "aborted")
        self.assertEqual(report["events"][-1]["kind"], "abort")
        self.assertIsNone(backend.read_committed("source"))
        self.assertEqual(backend.current_version("source"), 5)
        self.assertIsNone(backend.read_committed("derived"))

    def test_task_phase_action_failure_is_coded_and_aborts_active_transaction(self):
        class FailingInvalidationBackend(InMemoryTransactionBackend):
            def invalidate_committed(self, memory_id):
                raise RuntimeError(f"cannot invalidate {memory_id}")

        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {
                    "task_id": "task_action_failure",
                    "prompt": "write",
                    "failure_schedule": [
                        {
                            "trigger": {"phase": "after_mutation", "count": 1},
                            "action": {"type": "invalidate", "target": "source"},
                        }
                    ],
                },
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})],
                        ),
                        ModelResponse("done", []),
                    ]
                ),
                FailingInvalidationBackend(),
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
            )

        self.assertEqual(report["failure_code"], "failure_action_failed")
        self.assertEqual(report["transaction"]["decision"], "aborted")
        self.assertEqual(report["events"][-1]["kind"], "abort")

    def test_task_mode_observes_legacy_event_kind_and_aborts_on_first_write(self):
        controller = FailureController(
            [{"trigger": {"kind": "memory_write", "count": 1}, "action": {"type": "crash"}}]
        )
        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {
                    "task_id": "task_event_crash",
                    "prompt": "write",
                    "failure_controller": controller,
                },
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})],
                        ),
                        ModelResponse("done", []),
                    ]
                ),
                InMemoryTransactionBackend(),
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
            )

        self.assertEqual(report.get("failure_code"), "injected_crash")
        self.assertEqual(report["transaction"]["decision"], "aborted")
        self.assertEqual(
            [event["kind"] for event in report["events"]],
            ["begin_txn", "memory_write", "abort"],
        )
        self.assertEqual(controller.counts["memory_write"], 1)

    def test_task_mode_observes_each_logical_event_exactly_once(self):
        controller = FailureController(
            [{"trigger": {"kind": "memory_write", "count": 2}, "action": {"type": "crash"}}]
        )
        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {
                    "task_id": "task_event_once",
                    "prompt": "write",
                    "failure_controller": controller,
                },
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})],
                        ),
                        ModelResponse("done", []),
                    ]
                ),
                InMemoryTransactionBackend(),
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
            )

        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["transaction"]["decision"], "committed")
        self.assertEqual(controller.counts["memory_write"], 1)

    def test_task_mode_aborts_unknown_tool_and_hides_pending_state(self):
        backend = InMemoryTransactionBackend()
        with TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / "journal.sqlite3"
            report = run_real_agent(
                {"task_id": "task_unknown", "prompt": "write then fail"},
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "secret"})],
                        ),
                        ModelResponse("", [ToolCall("bad", "memory_delete", {})]),
                    ]
                ),
                backend,
                transaction_mode="task",
                transaction_journal_path=journal_path,
                transaction_id="txn_unknown",
            )
            journal = TransactionJournal(journal_path)
            self.addCleanup(journal.close)
            observer = TaskTransactionGateway(
                journal=journal,
                backend=backend,
                task_id="observer",
                agent_id="agent_model",
                txn_id="txn_observer",
                policy_snapshot_provider=lambda: {
                    "version": 1,
                    "denied_actions": [],
                    "scope_overrides": {},
                },
            )
            self.assertIsNone(observer.call("memory_read", {"memory_id": "pending"}))

        self.assertEqual(report["failure_code"], "unknown_tool")
        self.assertEqual(report["transaction"]["decision"], "aborted")
        self.assertEqual(report["events"][-1]["kind"], "abort")

    def test_task_mode_aborts_model_protocol_non_json_and_max_step_failures(self):
        class ProtocolFailureModel:
            def complete(self, *_args, **_kwargs):
                raise ModelProtocolError("missing_choices", "missing")

        cases = [
            (
                "model",
                ProtocolFailureModel(),
                InMemoryTransactionBackend(),
                2,
                "model_missing_choices",
            ),
            (
                "non_json",
                _ScriptedModel(
                    [ModelResponse("", [ToolCall("c1", "memory_read", {"memory_id": "opaque"})])]
                ),
                InMemoryTransactionBackend(
                    {"opaque": {"memory_id": "opaque", "value": object(), "version": 1}}
                ),
                2,
                "non_json_tool_result",
            ),
            (
                "max_steps",
                _ScriptedModel(
                    [ModelResponse("", [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})])]
                ),
                InMemoryTransactionBackend(),
                1,
                "max_steps_exceeded",
            ),
        ]
        for name, model, backend, max_steps, failure_code in cases:
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                report = run_real_agent(
                    {"task_id": f"task_{name}", "prompt": "run"},
                    model,
                    backend,
                    max_steps=max_steps,
                    transaction_mode="task",
                    transaction_journal_path=Path(tmp) / "journal.sqlite3",
                    transaction_id=f"txn_{name}",
                )
                self.assertEqual(report["failure_code"], failure_code)
                self.assertEqual(report["transaction"]["decision"], "aborted")
                self.assertEqual(report["events"][-1]["kind"], "abort")
                self.assertIsNone(backend.read_committed("pending"))

    def test_task_commit_revalidation_failure_keeps_final_text_but_fails_run(self):
        snapshots = 0

        def policy_snapshot():
            nonlocal snapshots
            snapshots += 1
            return {
                "version": snapshots,
                "denied_actions": ["write"] if snapshots >= 3 else [],
                "scope_overrides": {},
            }

        with TemporaryDirectory() as tmp:
            report = run_real_agent(
                {"task_id": "task_revalidation", "prompt": "write"},
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [ToolCall("c1", "memory_write", {"memory_id": "pending", "value": "v"})],
                        ),
                        ModelResponse("diagnostic final", []),
                    ]
                ),
                InMemoryTransactionBackend(),
                transaction_mode="task",
                transaction_journal_path=Path(tmp) / "journal.sqlite3",
                policy_snapshot_provider=policy_snapshot,
            )

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failure_code"], "policy_revalidation_failed")
        self.assertEqual(report["final_text"], "diagnostic final")
        self.assertEqual(report["transaction"]["decision"], "aborted")
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

    def test_instruction_inferred_tool_strategy_is_prompt_profile_independent(self):
        expected = ["phone", "venmo", "supervisor"]
        self.assertEqual(
            resolve_appworld_app_names(
                "instruction_inferred",
                "Request money on Venmo from my friend Stacy.",
                ["supervisor"],
            ),
            expected,
        )

    def test_appworld_strategy_does_not_depend_on_prompt_profile(self):
        baseline = resolve_appworld_app_names(
            "instruction_inferred", "Use Venmo.", ["supervisor"]
        )
        tuned = resolve_appworld_app_names(
            "instruction_inferred", "Use Venmo.", ["supervisor"]
        )

        self.assertEqual(baseline, tuned)

    def test_manifest_scoped_tool_strategy_preserves_manifest_apps(self):
        self.assertEqual(
            resolve_appworld_app_names("manifest_scoped", "Use Venmo.", ["supervisor"]),
            ["supervisor"],
        )

    def test_appworld_adapter_rejects_unsupported_tool_strategy(self):
        with self.assertRaisesRegex(ValueError, "unsupported AppWorld tool strategy"):
            AppWorldAdapter(tool_strategy="unsupported")

    def test_instruction_inferred_tool_strategy_allows_omitted_manifest_apps(self):
        self.assertEqual(
            resolve_appworld_app_names("instruction_inferred", "Use Venmo."),
            ["venmo", "supervisor"],
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

    def test_appworld_baseline_and_tuned_reset_before_schema_loading(self):
        for profile in ("baseline", "tuned"):
            order = []

            class _OrderingAdapter(_NoopBenchmarkAdapter):
                dataset = "appworld"

                def reset(self, task):
                    order.append("reset")
                    return str(task["instruction"])

                def tool_schemas(self):
                    order.append("schemas")
                    return []

            report = run_benchmark_agent(
                {
                    "task_id": profile,
                    "instruction": "Use Venmo.",
                    "prompt_profile": profile,
                },
                _ScriptedModel([ModelResponse("done", [])]),
                InstrumentedMemoryBackend(),
                _OrderingAdapter(),
            )

            self.assertEqual(order[:2], ["reset", "schemas"])
            self.assertEqual(report["prompt_profile"], profile)

    def test_supervisor_preflight_tools_are_never_model_visible(self):
        model = _ScriptedModel([ModelResponse("done", [])])
        report = run_benchmark_agent(
            {
                "task_id": "tuned",
                "instruction": "Use Venmo.",
                "prompt_profile": "tuned",
            },
            model,
            InstrumentedMemoryBackend(),
            _PreflightAdapter(),
        )

        names = {tool["function"]["name"] for tool in model.calls[0]["tools"]}
        self.assertNotIn("supervisor__show_profile", names)
        self.assertNotIn("supervisor__show_account_passwords", names)
        self.assertEqual(report["trusted_preflight_enabled"], True)

    def test_tool_set_digest_is_stable_across_prompt_profiles(self):
        reports = {}
        for profile in ("baseline", "tuned"):
            reports[profile] = run_benchmark_agent(
                {
                    "task_id": profile,
                    "instruction": "Use Venmo.",
                    "prompt_profile": profile,
                },
                _ScriptedModel([ModelResponse("done", [])]),
                InstrumentedMemoryBackend(),
                _PreflightAdapter(),
            )

        self.assertEqual(
            reports["baseline"]["model_visible_benchmark_tool_names_sha256"],
            reports["tuned"]["model_visible_benchmark_tool_names_sha256"],
        )
        self.assertEqual(reports["baseline"]["model_visible_benchmark_tool_count"], 1)
        self.assertEqual(reports["baseline"]["trusted_preflight_enabled"], False)

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
        sensitive_value = "SENSITIVE_PAYLOAD_DO_NOT_SERIALIZE_7F5C"
        model = _ScriptedModel(
            [
                ModelResponse(
                    "",
                    [
                        ToolCall(
                            "c1",
                            "memory_write",
                            {"memory_id": "m1", "value": sensitive_value},
                        )
                    ],
                ),
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
            parsed_summary = json.loads(summary)
        self.assertIn("native_event_count", result)
        self.assertIn(sensitive_value, raw)
        self.assertNotIn(sensitive_value, summary)
        self.assertEqual(
            parsed_summary["raw_trace_path"],
            str(Path(tmp) / "data" / "native_model_traces.jsonl"),
        )
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

    def test_benchmark_batch_propagates_task_mode_with_unique_repetition_transactions(self):
        class _BatchTransactionBackend(InMemoryTransactionBackend):
            def __init__(self):
                super().__init__()
                self.events = []

            def validated_events(self):
                return list(self.events)

        manifest = {
            "dataset_name": "stub",
            "transaction_mode": "task",
            "transaction_id": "manifest-transaction",
            "tasks": [
                {
                    "task_id": "repeatable-task",
                    "instruction": "complete",
                    "prompt": "complete",
                }
            ],
        }
        model = _ScriptedModel(
            [ModelResponse("done one", []), ModelResponse("done two", [])]
        )

        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            report = run_benchmark_batch(
                manifest,
                model,
                out_dir,
                backend_factory=lambda _index, _root: _BatchTransactionBackend(),
                adapter_factory=lambda: _NoopBenchmarkAdapter(),
                repetitions=2,
            )
            journals = sorted(
                str(path.relative_to(out_dir))
                for path in out_dir.rglob("*.sqlite3")
            )

        self.assertTrue(
            all("transaction" in summary for summary in report["task_summaries"])
        )
        transaction_ids = [
            summary["transaction"]["txn_id"]
            for summary in report["task_summaries"]
        ]
        self.assertEqual(len(set(transaction_ids)), 2)
        self.assertEqual(
            transaction_ids,
            [
                "manifest-transaction__repeatable-task__rep_01",
                "manifest-transaction__repeatable-task__rep_02",
            ],
        )
        self.assertEqual(
            journals,
            [
                "rep_01/journals/repeatable-task__rep_01.sqlite3",
                "rep_02/journals/repeatable-task__rep_02.sqlite3",
            ],
        )

    def test_benchmark_batch_preserves_sanitized_model_visible_tool_attestation(self):
        manifest = {
            "dataset_name": "appworld",
            "tasks": [
                {
                    "task_id": "appworld-attestation",
                    "instruction": "Use Venmo.",
                    "prompt": "Use Venmo.",
                    "prompt_profile": "tuned",
                }
            ],
        }

        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            report = run_benchmark_batch(
                manifest,
                _ScriptedModel(
                    [
                        ModelResponse(
                            "",
                            [
                                ToolCall(
                                    "audit-call",
                                    "venmo__get_profile",
                                    {
                                        "zeta": "fixture-argument-z",
                                        "alpha": "fixture-argument-a",
                                    },
                                )
                            ],
                        ),
                        ModelResponse("done", []),
                    ]
                ),
                out_dir,
                adapter_factory=lambda: _PreflightAdapter(),
            )
            persisted = json.loads(
                (out_dir / "results" / "native_batch_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        for task_summary in (
            report["task_summaries"][0],
            persisted["task_summaries"][0],
        ):
            self.assertEqual(
                task_summary["model_visible_benchmark_tool_names_sha256"],
                "c616c879dd4a61cd2d35da08c7b8559010309492bd3ec5b32df655a52f555b24",
            )
            self.assertIs(
                type(task_summary["model_visible_benchmark_tool_names_sha256"]), str
            )
            self.assertEqual(task_summary["model_visible_benchmark_tool_count"], 1)
            self.assertIs(type(task_summary["model_visible_benchmark_tool_count"]), int)
            self.assertIs(task_summary["trusted_preflight_enabled"], True)
            self.assertNotIn("model_visible_benchmark_tool_names", task_summary)
            trace = task_summary["benchmark_tool_trace"]
            allowed_trace_keys = {
                "name",
                "origin",
                "step",
                "argument_keys",
                "observation_status",
            }
            for row in trace:
                self.assertEqual(set(row), allowed_trace_keys)
                self.assertEqual(row["argument_keys"], sorted(row["argument_keys"]))
            trace_by_name = {row["name"]: row for row in trace}
            for trusted_name in (
                "supervisor__show_profile",
                "supervisor__show_account_passwords",
            ):
                self.assertIn(trusted_name, trace_by_name)
                self.assertEqual(
                    trace_by_name[trusted_name]["origin"], "trusted_preflight"
                )
            self.assertEqual(
                trace_by_name["venmo__get_profile"]["argument_keys"],
                ["alpha", "zeta"],
            )
            serialized = json.dumps(task_summary, ensure_ascii=False)
            self.assertNotIn('"function"', serialized)
            self.assertNotIn('"parameters"', serialized)
            self.assertNotIn('"arguments"', serialized)
            self.assertNotIn('"messages"', serialized)
            for private_value in (
                "fixture-zeta-schema-description",
                "fixture-alpha-schema-description",
                "fixture-argument-z",
                "fixture-argument-a",
                "fixture-public-observation",
                "Alex",
                "fixture-secret-password",
            ):
                self.assertNotIn(private_value, serialized)

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
