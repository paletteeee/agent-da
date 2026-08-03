import json
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


if __name__ == "__main__":
    unittest.main()
