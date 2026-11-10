"""Dependency-light OpenAI-compatible chat completion protocol client."""

from __future__ import annotations

import json
from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ModelProtocolError(RuntimeError):
    """A model endpoint or response violation with a stable machine code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ToolCall:
    call_id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class TokenUsage:
    """Token counters returned by an OpenAI-compatible endpoint."""

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass(frozen=True)
class ModelResponse:
    text: str
    tool_calls: list[ToolCall]
    usage: TokenUsage | None = None


def empty_usage_summary() -> dict[str, int]:
    """Return a stable aggregate schema for model-request accounting."""

    return {
        "request_count": 0,
        "responses_with_usage": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }


def add_response_usage(summary: dict[str, int], response: ModelResponse) -> None:
    """Accumulate one response's endpoint-provided counters in place."""

    usage = response.usage
    if usage is None:
        return
    summary["responses_with_usage"] += 1
    summary["prompt_tokens"] += usage.prompt_tokens
    summary["completion_tokens"] += usage.completion_tokens
    summary["total_tokens"] += usage.total_tokens


def merge_usage_summaries(summaries: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Sum already-sanitized usage summaries without estimating missing data."""

    result = empty_usage_summary()
    for summary in summaries:
        for field in result:
            value = summary.get(field, 0)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            result[field] += int(value)
    return result


def _required_string(value: Any, field: str, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ModelProtocolError(code, f"{field} must be a non-empty string")
    return value.strip()


def _completion_url(endpoint: str) -> str:
    base = _required_string(endpoint, "endpoint", "missing_endpoint").rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


def parse_chat_completion(payload: Mapping[str, Any]) -> ModelResponse:
    """Parse one OpenAI-compatible response without accepting hidden payloads."""

    if not isinstance(payload, Mapping):
        raise ModelProtocolError("invalid_response_shape", "response must be a mapping")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ModelProtocolError("missing_choices", "response choices must be non-empty")
    first = choices[0]
    if not isinstance(first, Mapping) or not isinstance(first.get("message"), Mapping):
        raise ModelProtocolError("missing_message", "response choice must contain a message")
    message = first["message"]
    content = message.get("content")
    if content is None:
        text = ""
    elif isinstance(content, str):
        text = content
    else:
        raise ModelProtocolError("invalid_content", "message content must be a string or null")

    raw_calls = message.get("tool_calls", [])
    if raw_calls is None:
        raw_calls = []
    if not isinstance(raw_calls, list):
        raise ModelProtocolError("invalid_tool_calls", "tool_calls must be a list")
    calls: list[ToolCall] = []
    for raw_call in raw_calls:
        if not isinstance(raw_call, Mapping) or not isinstance(raw_call.get("function"), Mapping):
            raise ModelProtocolError("invalid_tool_call", "tool call must contain function")
        function = raw_call["function"]
        call_id = _required_string(raw_call.get("id"), "tool_call.id", "missing_tool_call_id")
        name = _required_string(function.get("name"), "tool_call.name", "missing_tool_name")
        raw_arguments = function.get("arguments", {})
        if isinstance(raw_arguments, str):
            try:
                arguments = json.loads(raw_arguments)
            except json.JSONDecodeError as exc:
                raise ModelProtocolError("invalid_tool_arguments", "tool arguments are not valid JSON") from exc
        else:
            arguments = raw_arguments
        if not isinstance(arguments, Mapping):
            raise ModelProtocolError("invalid_tool_arguments", "tool arguments must be a JSON object")
        calls.append(ToolCall(call_id=call_id, name=name, arguments=dict(arguments)))
    usage = None
    raw_usage = payload.get("usage")
    if raw_usage is not None:
        if not isinstance(raw_usage, Mapping):
            raise ModelProtocolError("invalid_usage", "response usage must be a mapping")
        counters: dict[str, int] = {}
        for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
            value = raw_usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ModelProtocolError("invalid_usage", f"usage.{field} must be a non-negative integer")
            counters[field] = value
        usage = TokenUsage(**counters)
    return ModelResponse(text=text, tool_calls=calls, usage=usage)


class OpenAICompatibleClient:
    """Call a local or remote OpenAI-compatible chat completion endpoint."""

    def __init__(
        self,
        endpoint: str,
        model: str,
        api_key: str | None = None,
        timeout_s: float = 60.0,
        max_tokens: int | None = None,
    ):
        self.endpoint = _completion_url(endpoint)
        self.model = _required_string(model, "model", "missing_model")
        self.api_key = api_key
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        self.timeout_s = float(timeout_s)
        if max_tokens is not None and (isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or max_tokens < 1):
            raise ValueError("max_tokens must be a positive integer")
        self.max_tokens = max_tokens

    def request_metadata(
        self,
        *,
        seed: int | None,
        temperature: float,
        message_count: int,
        tool_count: int,
    ) -> dict[str, Any]:
        """Return auditable request metadata without credentials or payloads."""

        return {
            "endpoint": self.endpoint,
            "model": self.model,
            "seed": seed,
            "temperature": float(temperature),
            "message_count": int(message_count),
            "tool_count": int(tool_count),
        }

    def complete(
        self,
        messages: Sequence[Mapping[str, Any]],
        tools: Sequence[Mapping[str, Any]],
        *,
        seed: int | None = None,
        temperature: float = 0.0,
    ) -> ModelResponse:
        if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes)):
            raise ModelProtocolError("invalid_messages", "messages must be a sequence")
        if not isinstance(tools, Sequence) or isinstance(tools, (str, bytes)):
            raise ModelProtocolError("invalid_tools", "tools must be a sequence")
        if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
            raise ModelProtocolError("invalid_temperature", "temperature must be numeric")
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": list(messages),
            "tools": list(tools),
            "temperature": float(temperature),
        }
        if seed is not None:
            payload["seed"] = int(seed)
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = Request(
            self.endpoint,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_s) as response:
                raw_response = response.read()
        except HTTPError as exc:
            raise ModelProtocolError("http_error", f"model endpoint returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise ModelProtocolError("network_error", "model endpoint could not be reached") from exc
        try:
            response_payload = json.loads(raw_response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModelProtocolError("invalid_response_json", "model endpoint returned invalid JSON") from exc
        return parse_chat_completion(response_payload)
