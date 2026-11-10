"""Strict LongMemEval-S ingestion and deterministic session retrieval.

The public benchmark answer is never used by the retriever or model prompt.
Official QA scores remain blocked until a separately executed, pinned official
evaluator supplies a successful evidence report.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
from statistics import mean
from typing import Any

from txnmem_conditions import canonical_fingerprint, file_sha256
from txnmem_model_protocol import (
    ModelResponse,
    add_response_usage,
    empty_usage_summary,
    merge_usage_summaries,
)


LONGMEMEVAL_DATASET_REVISION = "98d7416c24c778c2fee6e6f3006e7a073259d48f"
LONGMEMEVAL_S_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
LONGMEMEVAL_S_SIZE_BYTES = 277_383_467
LONGMEMEVAL_ORACLE_SHA256 = "821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c"
LONGMEMEVAL_ORACLE_SIZE_BYTES = 15_388_478
LONGMEMEVAL_OFFICIAL_REPO_COMMIT = "9e0b455f4ef0e2ab8f2e582289761153549043fc"
LONGMEMEVAL_OFFICIAL_EVALUATOR_SHA256 = (
    "ecce9c4c79dc89d99534ac17b383a5cbb5b9f0c69ee98adaf0684742e3d95251"
)
LONGMEMEVAL_OFFICIAL_METRICS_SHA256 = (
    "e9283933a0cefb7a0ded7365e436ae3d1be5aac41853325e6155d83bf07607f0"
)
FORMAL_QUESTION_COUNT = 500
FORMAL_ABSTENTION_COUNT = 30

QUESTION_TYPES = frozenset(
    {
        "single-session-user",
        "single-session-assistant",
        "single-session-preference",
        "temporal-reasoning",
        "knowledge-update",
        "multi-session",
    }
)
TURN_ROLES = frozenset({"user", "assistant"})
_DATE_RE = re.compile(
    r"^(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2}) "
    r"\((?P<weekday>Mon|Tue|Wed|Thu|Fri|Sat|Sun)\) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2})$"
)
_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


@dataclass(frozen=True)
class LongMemEvalTurn:
    role: str
    content: str
    has_answer: bool | None = None


@dataclass(frozen=True)
class LongMemEvalItem:
    question_id: str
    question_type: str
    question: str
    answer: str
    question_date: str
    answer_session_ids: tuple[str, ...]
    haystack_session_ids: tuple[str, ...]
    haystack_dates: tuple[str, ...]
    haystack_sessions: tuple[tuple[LongMemEvalTurn, ...], ...]
    is_abstention: bool


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _answer_string(value: Any, field: str) -> str:
    if isinstance(value, str):
        return _required_string(value, field)
    if (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    ):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    raise ValueError(f"{field} must be a non-empty string or finite number")


def _turn_content(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _string_list(
    value: Any,
    field: str,
    *,
    unique: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    result = tuple(_required_string(item, f"{field} item") for item in value)
    if unique and len(set(result)) != len(result):
        raise ValueError(f"{field} must contain unique values")
    return result


def _parse_release_date(value: Any, field: str) -> datetime:
    text = _required_string(value, field)
    match = _DATE_RE.fullmatch(text)
    if match is None:
        raise ValueError(f"{field} has invalid date format")
    try:
        parsed = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
        )
    except ValueError as exc:
        raise ValueError(f"{field} has invalid date value") from exc
    if _WEEKDAYS[parsed.weekday()] != match.group("weekday"):
        raise ValueError(f"{field} date weekday is inconsistent")
    return parsed


def _load_turns(value: Any, field: str) -> tuple[LongMemEvalTurn, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list of turns")
    turns: list[LongMemEvalTurn] = []
    for index, raw_turn in enumerate(value):
        if not isinstance(raw_turn, Mapping):
            raise ValueError(f"{field}[{index}] must be a mapping")
        role = _required_string(raw_turn.get("role"), f"{field}[{index}].role")
        if role not in TURN_ROLES:
            raise ValueError(f"{field}[{index}].role is unsupported")
        content = _turn_content(raw_turn.get("content"), f"{field}[{index}].content")
        has_answer = raw_turn.get("has_answer")
        if has_answer is not None and type(has_answer) is not bool:
            raise ValueError(f"{field}[{index}].has_answer must be boolean")
        turns.append(LongMemEvalTurn(role=role, content=content, has_answer=has_answer))
    return tuple(turns)


def _load_item(raw: Any, index: int) -> LongMemEvalItem:
    if not isinstance(raw, Mapping):
        raise ValueError(f"item {index} must be a mapping")
    prefix = f"item {index}"
    question_id = _required_string(raw.get("question_id"), f"{prefix}.question_id")
    question_type = _required_string(raw.get("question_type"), f"{prefix}.question_type")
    if question_type not in QUESTION_TYPES:
        raise ValueError(f"{prefix}.question_type is unsupported")
    question = _required_string(raw.get("question"), f"{prefix}.question")
    answer = _answer_string(raw.get("answer"), f"{prefix}.answer")
    question_date = _required_string(raw.get("question_date"), f"{prefix}.question_date")
    _parse_release_date(question_date, f"{prefix}.question_date")
    answer_session_ids = _string_list(
        raw.get("answer_session_ids"), f"{prefix}.answer_session_ids"
    )
    session_ids = _string_list(
        raw.get("haystack_session_ids"),
        f"{prefix}.haystack_session_ids",
        unique=False,
    )
    raw_dates = raw.get("haystack_dates")
    raw_sessions = raw.get("haystack_sessions")
    if not isinstance(raw_dates, list):
        raise ValueError(f"{prefix}.haystack_dates must be a list")
    if not isinstance(raw_sessions, list):
        raise ValueError(f"{prefix}.haystack_sessions must be a list")
    if not session_ids:
        raise ValueError(f"{prefix}.haystack_session_ids must be non-empty")
    if not (len(session_ids) == len(raw_dates) == len(raw_sessions)):
        raise ValueError(f"{prefix} haystack arrays must have equal lengths")
    dates = tuple(
        _required_string(value, f"{prefix}.haystack_dates[{position}]")
        for position, value in enumerate(raw_dates)
    )
    parsed_dates = tuple(
        _parse_release_date(value, f"{prefix}.haystack_dates[{position}]")
        for position, value in enumerate(dates)
    )
    calendar_dates = tuple(value.date() for value in parsed_dates)
    if calendar_dates != tuple(sorted(calendar_dates)):
        raise ValueError(f"{prefix}.haystack_dates must be chronological by calendar day")
    sessions = tuple(
        _load_turns(value, f"{prefix}.haystack_sessions[{position}]")
        for position, value in enumerate(raw_sessions)
    )
    unknown_evidence = set(answer_session_ids) - set(session_ids)
    if unknown_evidence:
        raise ValueError(f"{prefix}.answer_session_ids are not in the haystack")
    is_abstention = question_id.endswith("_abs")
    if not is_abstention and not answer_session_ids:
        raise ValueError(f"{prefix}.answer_session_ids must be non-empty")
    return LongMemEvalItem(
        question_id=question_id,
        question_type=question_type,
        question=question,
        answer=answer,
        question_date=question_date,
        answer_session_ids=answer_session_ids,
        haystack_session_ids=session_ids,
        haystack_dates=dates,
        haystack_sessions=sessions,
        is_abstention=is_abstention,
    )


def load_longmemeval_s(
    path: str | Path,
    *,
    formal: bool = True,
) -> list[LongMemEvalItem]:
    """Load the cleaned S split and fail closed on schema/cardinality drift."""

    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as stream:
            raw_items = json.load(stream, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("LongMemEval source is not valid UTF-8 JSON") from exc
    if not isinstance(raw_items, list):
        raise ValueError("LongMemEval source must contain a JSON list")
    if formal and len(raw_items) != FORMAL_QUESTION_COUNT:
        raise ValueError("formal LongMemEval-S must contain exactly 500 questions")
    items = [_load_item(raw, index) for index, raw in enumerate(raw_items)]
    question_ids = [item.question_id for item in items]
    if len(set(question_ids)) != len(question_ids):
        raise ValueError("LongMemEval question IDs must be unique")
    if formal:
        abstention_count = sum(item.is_abstention for item in items)
        if abstention_count != FORMAL_ABSTENTION_COUNT:
            raise ValueError("formal LongMemEval-S must contain exactly 30 abstention questions")
    return items


def preflight_longmemeval_oracle(
    path: str | Path,
    *,
    expected_question_ids: Sequence[str],
    formal: bool = True,
    require_pinned_source: bool = True,
) -> dict[str, Any]:
    """Validate the separate reference file consumed by the official QA judge."""

    source = Path(path)
    size_bytes = source.stat().st_size
    sha256 = file_sha256(source)
    if require_pinned_source and (
        size_bytes != LONGMEMEVAL_ORACLE_SIZE_BYTES
        or sha256 != LONGMEMEVAL_ORACLE_SHA256
    ):
        raise ValueError("LongMemEval oracle does not match the pinned cleaned release")
    try:
        with source.open("r", encoding="utf-8") as stream:
            raw_items = json.load(stream, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("LongMemEval oracle is not valid UTF-8 JSON") from exc
    if not isinstance(raw_items, list):
        raise ValueError("LongMemEval oracle must contain a JSON list")
    if formal and len(raw_items) != FORMAL_QUESTION_COUNT:
        raise ValueError("formal LongMemEval oracle must contain exactly 500 questions")
    question_ids: list[str] = []
    type_counts: Counter[str] = Counter()
    for index, raw in enumerate(raw_items):
        if not isinstance(raw, Mapping):
            raise ValueError(f"oracle item {index} must be a mapping")
        question_id = _required_string(
            raw.get("question_id"), f"oracle item {index}.question_id"
        )
        question_type = _required_string(
            raw.get("question_type"), f"oracle item {index}.question_type"
        )
        if question_type not in QUESTION_TYPES:
            raise ValueError(f"oracle item {index}.question_type is unsupported")
        _required_string(raw.get("question"), f"oracle item {index}.question")
        _answer_string(raw.get("answer"), f"oracle item {index}.answer")
        question_ids.append(question_id)
        type_counts[question_type] += 1
    if len(set(question_ids)) != len(question_ids):
        raise ValueError("LongMemEval oracle question IDs must be unique")
    expected_ids = tuple(expected_question_ids)
    if (
        any(not isinstance(value, str) or not value for value in expected_ids)
        or len(set(expected_ids)) != len(expected_ids)
        or set(question_ids) != set(expected_ids)
    ):
        raise ValueError("LongMemEval oracle question IDs do not match the S split")
    return {
        "source_sha256": sha256,
        "source_size_bytes": size_bytes,
        "pinned_source_match": (
            size_bytes == LONGMEMEVAL_ORACLE_SIZE_BYTES
            and sha256 == LONGMEMEVAL_ORACLE_SHA256
        ),
        "question_count": len(question_ids),
        "question_id_set_sha256": canonical_fingerprint(sorted(question_ids)),
        "question_type_counts": dict(sorted(type_counts.items())),
    }


def longmemeval_namespace(question_id: str) -> str:
    """Return a stable opaque namespace for exactly one benchmark question."""

    return "longmemeval:" + canonical_fingerprint(
        {"benchmark": "longmemeval_s_cleaned", "question_id": question_id}
    )


def _render_session(item: LongMemEvalItem, position: int) -> str:
    lines = [
        f"DATE: {item.haystack_dates[position]}",
        f"SESSION_ID: {item.haystack_session_ids[position]}",
    ]
    lines.extend(
        f"{turn.role.upper()}: {turn.content}"
        for turn in item.haystack_sessions[position]
    )
    return "\n".join(lines)


def _normalize_token(token: str) -> str:
    token = token.lower()
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


def _tokens(text: str) -> list[str]:
    return [_normalize_token(token) for token in _TOKEN_RE.findall(text)]


def _rank_sessions(
    question: str,
    sessions: Sequence[tuple[int, str, str]],
    top_k: int,
) -> list[tuple[int, str, str]]:
    if type(top_k) is not int or top_k < 1:
        raise ValueError("top_k must be a positive integer")
    query_tokens = tuple(dict.fromkeys(_tokens(question)))
    document_tokens = [Counter(_tokens(content)) for _, _, content in sessions]
    document_frequency = Counter(
        token for counts in document_tokens for token in counts
    )
    average_length = (
        sum(sum(counts.values()) for counts in document_tokens) / len(document_tokens)
        if document_tokens
        else 1.0
    )
    scored: list[tuple[float, int, str, str]] = []
    for (position, session_id, content), counts in zip(sessions, document_tokens):
        length = max(1, sum(counts.values()))
        score = 0.0
        for token in query_tokens:
            frequency = counts.get(token, 0)
            if not frequency:
                continue
            frequency_weight = (
                frequency * 2.2
                / (frequency + 1.2 * (0.25 + 0.75 * length / average_length))
            )
            inverse_frequency = math.log(
                1.0
                + (len(sessions) - document_frequency[token] + 0.5)
                / (document_frequency[token] + 0.5)
            )
            score += inverse_frequency * frequency_weight
        scored.append((-score, position, session_id, content))
    scored.sort()
    return [
        (position, session_id, content)
        for _, position, session_id, content in scored[:top_k]
    ]


def run_longmemeval_item(
    item: LongMemEvalItem,
    backend: Any,
    model: Any,
    *,
    top_k: int = 5,
    seed: int = 17,
) -> dict[str, Any]:
    """Ingest one item's sessions, retrieve deterministically, and answer once."""

    namespace = longmemeval_namespace(item.question_id)
    namespace_hash = namespace.split(":", 1)[1]
    session_by_memory_id: dict[str, tuple[int, str, str]] = {}
    for position, session_id in enumerate(item.haystack_session_ids):
        content = _render_session(item, position)
        memory_id = (
            f"longmemeval:{namespace_hash}:{position:04d}:"
            + canonical_fingerprint({"session_id": session_id})[:16]
        )
        session_by_memory_id[memory_id] = (position, session_id, content)
        backend.write(
            memory_id,
            value=content,
            agent_id=namespace,
            scope="benchmark:longmemeval_s_cleaned",
            projection="longmemeval_session_ingest",
            source_position=position,
        )

    returned = backend.search(
        query=None,
        agent_id=namespace,
        projection="longmemeval_session_retrieval",
    )
    if not isinstance(returned, list):
        raise ValueError("memory backend search must return a list")
    visible: dict[str, tuple[int, str, str]] = {}
    for record in returned:
        if not isinstance(record, Mapping):
            continue
        if record.get("agent_id") != namespace:
            continue
        memory_id = str(record.get("memory_id", ""))
        session = session_by_memory_id.get(memory_id)
        value = record.get("value")
        if (
            session is not None
            and isinstance(value, str)
            and str(record.get("status", "active")) == "active"
        ):
            position, session_id, _source_content = session
            visible[memory_id] = (position, session_id, value)
    ranked = _rank_sessions(item.question, list(visible.values()), top_k)
    context = "\n\n".join(content for _, _, content in ranked)
    if not context:
        context = "[No sessions were retrieved.]"
    messages = [
        {
            "role": "system",
            "content": (
                "Answer the question using only the retrieved dated sessions. "
                "Return only a concise answer; if the sessions do not contain the answer, "
                "say that the information is unavailable."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Retrieved sessions:\n{context}\n\n"
                f"Question date: {item.question_date}\n"
                f"Question: {item.question}"
            ),
        },
    ]
    response = model.complete(messages, [], seed=int(seed), temperature=0.0)
    if isinstance(response, ModelResponse):
        hypothesis = response.text.strip()
        usage = empty_usage_summary()
        usage["request_count"] = 1
        add_response_usage(usage, response)
    elif isinstance(response, str):
        hypothesis = response.strip()
        usage = empty_usage_summary()
        usage["request_count"] = 1
    else:
        raise ValueError("model completion must return ModelResponse or string")
    retrieved_session_ids = [session_id for _, session_id, _ in ranked]
    evidence = set(item.answer_session_ids)
    retrieved_evidence_count = len(evidence.intersection(retrieved_session_ids))
    evidence_recall = (
        None
        if item.is_abstention
        else retrieved_evidence_count / len(item.answer_session_ids)
    )
    return {
        "status": "completed" if hypothesis else "empty_hypothesis",
        "question_id": item.question_id,
        "question_type": item.question_type,
        "hypothesis": hypothesis,
        "is_abstention": item.is_abstention,
        "namespace_sha256": namespace_hash,
        "source_session_count": len(item.haystack_session_ids),
        "write_attempt_count": len(session_by_memory_id),
        "stored_session_count": len(visible),
        "retrieved_session_count": len(ranked),
        "retrieved_session_ids": retrieved_session_ids,
        "evidence_session_count": len(item.answer_session_ids),
        "retrieved_evidence_session_count": retrieved_evidence_count,
        "evidence_session_recall": evidence_recall,
        "model_usage": usage,
    }


def _validated_official_evaluator_report(
    report: Mapping[str, Any] | None,
    question_ids: Sequence[str],
    abstention_count: int,
) -> dict[str, Any] | None:
    """Validate official QA from immutable artifacts, never caller summaries."""

    if not isinstance(report, Mapping):
        return None
    if not (
        report.get("status") == "available"
        and report.get("command_succeeded") is True
        and type(report.get("returncode")) is int
        and report.get("returncode") == 0
        and report.get("evaluator_commit") == LONGMEMEVAL_OFFICIAL_REPO_COMMIT
        and report.get("metric_model") == "gpt-4o-2024-08-06"
        and len(question_ids) == FORMAL_QUESTION_COUNT
        and len(set(question_ids)) == FORMAL_QUESTION_COUNT
        and abstention_count == FORMAL_ABSTENTION_COUNT
    ):
        return None

    def artifact_path(field: str) -> Path:
        value = report.get(field)
        if not isinstance(value, (str, os.PathLike)):
            raise ValueError(f"official evaluator report needs {field}")
        path = Path(value)
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"official evaluator {field} must be a regular file")
        return path

    def jsonl(path: Path, label: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, start=1):
                    if not line.strip():
                        raise ValueError(f"{label} contains a blank line")
                    item = json.loads(line, object_pairs_hook=_reject_duplicate_keys)
                    if not isinstance(item, dict):
                        raise ValueError(f"{label} row {line_number} must be a mapping")
                    rows.append(item)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"{label} is not valid UTF-8 JSONL") from exc
        return rows

    try:
        hypotheses_path = artifact_path("hypotheses_path")
        evaluation_log_path = artifact_path("evaluation_log_path")
        reference_path = artifact_path("reference_path")
        evaluator_path = artifact_path("evaluator_path")
        metrics_script_path = artifact_path("metrics_script_path")
        if file_sha256(evaluator_path) != LONGMEMEVAL_OFFICIAL_EVALUATOR_SHA256:
            raise ValueError("official evaluator script hash mismatch")
        if file_sha256(metrics_script_path) != LONGMEMEVAL_OFFICIAL_METRICS_SHA256:
            raise ValueError("official metrics script hash mismatch")

        expected_ids = list(question_ids)
        preflight_longmemeval_oracle(
            reference_path,
            expected_question_ids=expected_ids,
            formal=True,
            require_pinned_source=True,
        )
        with reference_path.open("r", encoding="utf-8") as stream:
            reference_rows = json.load(stream, object_pairs_hook=_reject_duplicate_keys)
        reference_types = {
            str(item["question_id"]): str(item["question_type"])
            for item in reference_rows
        }

        hypotheses = jsonl(hypotheses_path, "official hypotheses")
        logs = jsonl(evaluation_log_path, "official evaluation log")
        if len(hypotheses) != FORMAL_QUESTION_COUNT or len(logs) != FORMAL_QUESTION_COUNT:
            raise ValueError("official evaluator artifacts must contain exactly 500 rows")
        hypothesis_ids: list[str] = []
        hypothesis_by_id: dict[str, str] = {}
        for position, item in enumerate(hypotheses):
            if set(item) != {"question_id", "hypothesis"}:
                raise ValueError(f"official hypothesis {position} has unexpected fields")
            question_id = _required_string(
                item.get("question_id"), f"official hypothesis {position}.question_id"
            )
            hypothesis = item.get("hypothesis")
            if not isinstance(hypothesis, str):
                raise ValueError(f"official hypothesis {position}.hypothesis must be a string")
            hypothesis_ids.append(question_id)
            hypothesis_by_id[question_id] = hypothesis
        if (
            len(set(hypothesis_ids)) != FORMAL_QUESTION_COUNT
            or set(hypothesis_ids) != set(expected_ids)
        ):
            raise ValueError("official hypotheses do not match the formal question ID set")

        labels_by_id: dict[str, bool] = {}
        log_ids: list[str] = []
        for position, item in enumerate(logs):
            if set(item) != {"question_id", "hypothesis", "autoeval_label"}:
                raise ValueError(f"official evaluation log {position} has unexpected fields")
            question_id = _required_string(
                item.get("question_id"), f"official evaluation log {position}.question_id"
            )
            if item.get("hypothesis") != hypothesis_by_id.get(question_id):
                raise ValueError("official evaluation log hypothesis mismatch")
            label = item.get("autoeval_label")
            if (
                not isinstance(label, Mapping)
                or set(label) != {"model", "label"}
                or label.get("model") != "gpt-4o-2024-08-06"
                or type(label.get("label")) is not bool
            ):
                raise ValueError("official evaluation log has an invalid fixed-judge label")
            log_ids.append(question_id)
            labels_by_id[question_id] = label["label"]
        if (
            log_ids != hypothesis_ids
            or len(labels_by_id) != FORMAL_QUESTION_COUNT
            or set(labels_by_id) != set(expected_ids)
        ):
            raise ValueError("official evaluation log does not match the hypotheses")

        type_labels: dict[str, list[bool]] = {name: [] for name in QUESTION_TYPES}
        for question_id in expected_ids:
            type_labels[reference_types[question_id]].append(labels_by_id[question_id])
        if any(not values for values in type_labels.values()):
            raise ValueError("official evaluation log is missing a question type")
        abstention_labels = [
            labels_by_id[question_id]
            for question_id in expected_ids
            if question_id.endswith("_abs")
        ]
        if len(abstention_labels) != FORMAL_ABSTENTION_COUNT:
            raise ValueError("official evaluation log must contain 30 abstention questions")
        metrics = {
            "overall_accuracy": mean(
                float(labels_by_id[question_id]) for question_id in expected_ids
            ),
            "task_averaged_accuracy": mean(
                mean(float(label) for label in type_labels[name])
                for name in sorted(type_labels)
            ),
            "abstention_accuracy": mean(float(label) for label in abstention_labels),
        }
    except (KeyError, OSError, TypeError, ValueError):
        return None

    return {
        "evaluator_commit": LONGMEMEVAL_OFFICIAL_REPO_COMMIT,
        "evaluator_sha256": file_sha256(evaluator_path),
        "metrics_script_sha256": file_sha256(metrics_script_path),
        "reference_sha256": file_sha256(reference_path),
        "metric_model": report["metric_model"],
        "hypotheses_sha256": file_sha256(hypotheses_path),
        "evaluation_log_sha256": file_sha256(evaluation_log_path),
        "evaluated_question_count": len(logs),
        "abstention_count": len(abstention_labels),
        "metrics": metrics,
    }


def aggregate_longmemeval(
    rows: Sequence[Mapping[str, Any]],
    *,
    official_evaluator_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregate retrieval facts without inventing an official QA score."""

    question_ids: list[str] = []
    abstention_count = 0
    retrieval_recalls: list[float] = []
    evidence_denominator = 0
    evidence_retrieved = 0
    usage_summaries: list[Mapping[str, Any]] = []
    for position, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"row {position} must be a mapping")
        question_id = _required_string(row.get("question_id"), f"row {position}.question_id")
        question_ids.append(question_id)
        is_abstention = row.get("is_abstention")
        if type(is_abstention) is not bool:
            raise ValueError(f"row {position}.is_abstention must be boolean")
        usage = row.get("model_usage", {})
        if not isinstance(usage, Mapping):
            raise ValueError(f"row {position}.model_usage must be a mapping")
        usage_summaries.append(usage)
        if is_abstention:
            abstention_count += 1
            continue
        evidence_count = row.get("evidence_session_count")
        retrieved_count = row.get("retrieved_evidence_session_count")
        recall = row.get("evidence_session_recall")
        if (
            type(evidence_count) is not int
            or evidence_count < 1
            or type(retrieved_count) is not int
            or retrieved_count < 0
            or retrieved_count > evidence_count
            or isinstance(recall, bool)
            or not isinstance(recall, (int, float))
            or not math.isfinite(float(recall))
            or abs(float(recall) - retrieved_count / evidence_count) > 1e-12
        ):
            raise ValueError(f"row {position} has invalid retrieval evidence")
        evidence_denominator += evidence_count
        evidence_retrieved += retrieved_count
        retrieval_recalls.append(float(recall))
    if len(set(question_ids)) != len(question_ids):
        raise ValueError("LongMemEval aggregate question IDs must be unique")
    aggregate: dict[str, Any] = {
        "question_count": len(question_ids),
        "abstention_question_count": abstention_count,
        "retrieval_question_count": len(retrieval_recalls),
        "evidence_session_denominator": evidence_denominator,
        "retrieved_evidence_session_count": evidence_retrieved,
        "evidence_session_recall_micro": (
            evidence_retrieved / evidence_denominator if evidence_denominator else 0.0
        ),
        "evidence_session_recall_macro": (
            sum(retrieval_recalls) / len(retrieval_recalls)
            if retrieval_recalls
            else 0.0
        ),
        "model_usage": merge_usage_summaries(usage_summaries),
        "official_qa_status": "blocked",
        "official_qa_reason": "pinned_official_evaluator_has_not_succeeded",
    }
    official = _validated_official_evaluator_report(
        official_evaluator_report, question_ids, abstention_count
    )
    if official is not None:
        aggregate["official_qa_status"] = "available"
        aggregate.pop("official_qa_reason", None)
        aggregate["official_qa_evaluator_commit"] = official["evaluator_commit"]
        aggregate["official_qa_evaluator_sha256"] = official["evaluator_sha256"]
        aggregate["official_qa_metrics_script_sha256"] = official[
            "metrics_script_sha256"
        ]
        aggregate["official_qa_reference_sha256"] = official["reference_sha256"]
        aggregate["official_qa_metric_model"] = official["metric_model"]
        aggregate["official_qa_hypotheses_sha256"] = official["hypotheses_sha256"]
        aggregate["official_qa_evaluation_log_sha256"] = official[
            "evaluation_log_sha256"
        ]
        aggregate["official_qa_evaluated_question_count"] = official[
            "evaluated_question_count"
        ]
        aggregate["official_qa_abstention_count"] = official["abstention_count"]
        aggregate["official_qa_metrics"] = official["metrics"]
    return aggregate


def write_official_hypotheses(
    rows: Sequence[Mapping[str, Any]],
    path: str | Path,
    *,
    expected_question_ids: Sequence[str],
) -> None:
    """Write only a complete, exact formal question set for official QA."""

    expected_ids = [
        _required_string(value, f"expected_question_ids[{index}]")
        for index, value in enumerate(expected_question_ids)
    ]
    if (
        len(expected_ids) != FORMAL_QUESTION_COUNT
        or len(set(expected_ids)) != FORMAL_QUESTION_COUNT
        or sum(value.endswith("_abs") for value in expected_ids)
        != FORMAL_ABSTENTION_COUNT
    ):
        raise ValueError("expected_question_ids must be the exact formal question ID set")

    payloads: list[dict[str, str]] = []
    question_ids: list[str] = []
    for position, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"row {position} must be a mapping")
        question_id = _required_string(row.get("question_id"), f"row {position}.question_id")
        hypothesis = row.get("hypothesis")
        if not isinstance(hypothesis, str):
            raise ValueError(f"row {position}.hypothesis must be a string")
        question_ids.append(question_id)
        payloads.append({"question_id": question_id, "hypothesis": hypothesis})
    if len(set(question_ids)) != len(question_ids):
        raise ValueError("official hypotheses contain duplicate question IDs")
    if len(question_ids) != FORMAL_QUESTION_COUNT or set(question_ids) != set(expected_ids):
        raise ValueError("official hypotheses must match the exact formal question ID set")
    encoded = "".join(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        for payload in payloads
    ).encode("utf-8")
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(destination, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(encoded)
            stream.flush()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def preflight_longmemeval_s(
    path: str | Path,
    *,
    require_pinned_source: bool = True,
) -> dict[str, Any]:
    """Return a sanitized source inventory after strict formal validation."""

    source = Path(path)
    size_bytes = source.stat().st_size
    sha256 = file_sha256(source)
    if require_pinned_source and (
        size_bytes != LONGMEMEVAL_S_SIZE_BYTES or sha256 != LONGMEMEVAL_S_SHA256
    ):
        raise ValueError("LongMemEval-S source does not match the pinned cleaned release")
    items = load_longmemeval_s(source, formal=True)
    type_counts = Counter(item.question_type for item in items)
    duplicate_session_counts = [
        len(item.haystack_session_ids) - len(set(item.haystack_session_ids))
        for item in items
    ]
    same_day_time_inversion_count = sum(
        any(
            right < left
            for left, right in zip(
                (
                    _parse_release_date(value, "haystack date")
                    for value in item.haystack_dates
                ),
                (
                    _parse_release_date(value, "haystack date")
                    for value in item.haystack_dates[1:]
                ),
            )
        )
        for item in items
    )
    return {
        "benchmark": "LongMemEval-S cleaned",
        "dataset_revision": LONGMEMEVAL_DATASET_REVISION,
        "source_sha256": sha256,
        "source_size_bytes": size_bytes,
        "pinned_source_match": (
            size_bytes == LONGMEMEVAL_S_SIZE_BYTES and sha256 == LONGMEMEVAL_S_SHA256
        ),
        "question_count": len(items),
        "abstention_question_count": sum(item.is_abstention for item in items),
        "retrieval_question_count": sum(not item.is_abstention for item in items),
        "question_type_counts": dict(sorted(type_counts.items())),
        "session_count": sum(len(item.haystack_session_ids) for item in items),
        "duplicate_session_question_count": sum(
            count > 0 for count in duplicate_session_counts
        ),
        "duplicate_session_occurrence_count": sum(duplicate_session_counts),
        "same_day_time_inversion_question_count": same_day_time_inversion_count,
        "turn_count": sum(
            len(session)
            for item in items
            for session in item.haystack_sessions
        ),
        "empty_turn_count": sum(
            turn.content == ""
            for item in items
            for session in item.haystack_sessions
            for turn in session
        ),
        "evidence_session_count": sum(
            len(item.answer_session_ids) for item in items if not item.is_abstention
        ),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--data", required=True)
    preflight.add_argument("--oracle")
    preflight.add_argument("--allow-unpinned-source", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "preflight":
        report = preflight_longmemeval_s(
            args.data,
            require_pinned_source=not args.allow_unpinned_source,
        )
        if args.oracle:
            items = load_longmemeval_s(args.data, formal=True)
            report["official_qa_reference"] = preflight_longmemeval_oracle(
                args.oracle,
                expected_question_ids=[item.question_id for item in items],
                formal=True,
                require_pinned_source=not args.allow_unpinned_source,
            )
            report["official_qa_reference_status"] = "ready"
        else:
            report["official_qa_reference_status"] = "not_checked"
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
