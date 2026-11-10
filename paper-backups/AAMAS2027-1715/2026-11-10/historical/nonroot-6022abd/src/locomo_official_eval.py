"""Run the released LoCoMo QA evaluator against an OpenAI-compatible model.

This is deliberately separate from the native contextual-agent trace runner:
the former measures the public QA task, while the latter measures memory
events.  The only score in the report comes from LoCoMo's own
``eval_question_answering`` implementation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


def _session_sort_key(key: str) -> tuple[int, str]:
    suffix = key.removeprefix("session_")
    return (int(suffix), key) if suffix.isdigit() else (10**9, key)


def iter_conversation_sessions(sample: Mapping[str, Any]):
    """Yield every LoCoMo session in source chronology without truncation."""

    if not isinstance(sample, Mapping):
        raise ValueError("LoCoMo sample must be a mapping")
    conversation = sample.get("conversation", sample)
    if not isinstance(conversation, Mapping):
        raise ValueError("LoCoMo conversation must be a mapping")
    session_keys = sorted(
        (
            str(key)
            for key in conversation
            if str(key).startswith("session_")
            and not str(key).endswith("_date_time")
        ),
        key=_session_sort_key,
    )
    for source_position, key in enumerate(session_keys):
        turns = conversation.get(key)
        if not isinstance(turns, list):
            raise ValueError(f"LoCoMo {key} must be a list of turns")
        session_number = key.removeprefix("session_")
        date_time = str(conversation.get(f"session_{session_number}_date_time", ""))
        lines = [f"DATE: {date_time}", "CONVERSATION:"]
        for turn_index, turn in enumerate(turns):
            if not isinstance(turn, Mapping):
                raise ValueError(f"LoCoMo {key} turn {turn_index} must be a mapping")
            speaker = str(turn.get("speaker", "speaker"))
            text = turn.get("text", turn.get("content", ""))
            if text not in (None, ""):
                lines.append(f'{speaker} said, "{text}"')
        content = "\n".join(lines)
        yield {
            "session_id": key,
            "source_position": source_position,
            "date_time": date_time,
            "content": content,
            "char_count": len(content),
            "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        }


def parse_batch_answers(text: str, count: int) -> dict[int, str]:
    """Parse a model's numbered JSON answer object, with a line fallback."""

    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError:
        value = None
    if isinstance(value, list):
        return {index: str(answer).strip() for index, answer in enumerate(value[:count])}
    if isinstance(value, Mapping):
        parsed: dict[int, str] = {}
        for key, answer in value.items():
            try:
                index = int(str(key))
            except (TypeError, ValueError):
                continue
            if 0 <= index < count:
                parsed[index] = str(answer).strip()
        if parsed:
            return parsed
    parsed = {}
    matches = list(
        re.finditer(
            r"(?:^|\n)\s*(\d+)\s*[.)：:]\s*(.+?)(?=\n\s*\d+\s*[.)：:]|$)",
            cleaned,
            re.S,
        )
    )
    zero_based = any(int(match.group(1)) == 0 for match in matches)
    for match in matches:
        number = int(match.group(1))
        index = number if zero_based else number - 1
        if 0 <= index < count:
            parsed[index] = " ".join(match.group(2).split())
    return parsed


def aggregate_scores(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate scores returned by the official LoCoMo evaluator."""

    category_scores: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        category_scores[str(row["category"])].append(float(row["score"]))
    all_scores = [score for values in category_scores.values() for score in values]
    return {
        "question_count": len(all_scores),
        "mean_f1": sum(all_scores) / len(all_scores) if all_scores else 0.0,
        "category_counts": {key: len(values) for key, values in sorted(category_scores.items())},
        "category_mean_f1": {
            key: sum(values) / len(values) for key, values in sorted(category_scores.items())
        },
    }


def normalize_qa_for_evaluator(qa: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the released category-5 schema for the official evaluator."""

    row = dict(qa)
    if "answer" not in row:
        row["answer"] = row.get("adversarial_answer", "")
    return row


def _conversation_context(conversation: Mapping[str, Any], max_chars: int) -> str:
    context = "\n".join(
        session["content"] for session in iter_conversation_sessions(conversation)
    )
    if max_chars <= 0 or len(context) <= max_chars:
        return context
    marker = "\n[conversation middle omitted for context budget]\n"
    budget = max_chars - len(marker)
    head = max(1, budget // 2)
    return context[:head] + marker + context[-(budget - head):]


def _question_for_model(qa: Mapping[str, Any], rng: random.Random) -> tuple[str, dict[str, str] | None]:
    category = int(qa.get("category", 0))
    question = str(qa.get("question", ""))
    if category == 2:
        question += " Use DATE of CONVERSATION to answer with an approximate date."
    if category != 5:
        return question, None
    answer = str(qa.get("answer", ""))
    if rng.random() < 0.5:
        return f"{question} (a) No information available (b) {answer}. Reply with (a) or (b).", {"a": "No information available", "b": answer}
    return f"{question} (a) {answer} (b) No information available. Reply with (a) or (b).", {"a": answer, "b": "No information available"}


def _request(endpoint: str, model: str, messages: Sequence[Mapping[str, Any]], max_tokens: int, timeout: float) -> str:
    url = endpoint.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions" if url.endswith("/v1") else "/v1/chat/completions"
    payload = {"model": model, "messages": list(messages), "temperature": 0.0, "max_tokens": int(max_tokens)}
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read().decode("utf-8"))
    return str(body["choices"][0]["message"].get("content") or "")


def run_official_eval(
    data_file: str | Path,
    out_dir: str | Path,
    endpoint: str,
    model: str,
    *,
    limit: int | None = None,
    batch_size: int = 8,
    max_context_chars: int = 90000,
    max_tokens: int = 512,
    timeout: float = 120.0,
    seed: int = 17,
) -> dict[str, Any]:
    from task_eval.evaluation import eval_question_answering

    samples = json.loads(Path(data_file).read_text(encoding="utf-8"))
    if not isinstance(samples, list):
        raise ValueError("LoCoMo data file must contain a JSON list")
    selected = samples[:limit] if limit is not None else samples
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    raw_predictions: list[dict[str, Any]] = []
    score_rows: list[dict[str, Any]] = []
    for sample_index, sample in enumerate(selected):
        qas = [normalize_qa_for_evaluator(qa) for qa in sample.get("qa", [])]
        context = _conversation_context(sample.get("conversation", {}), max_context_chars)
        for batch_start in range(0, len(qas), batch_size):
            batch = qas[batch_start : batch_start + batch_size]
            questions: list[str] = []
            options: list[dict[str, str] | None] = []
            rng = random.Random(seed + sample_index * 10000 + batch_start)
            for qa in batch:
                question, choice_map = _question_for_model(qa, rng)
                questions.append(question)
                options.append(choice_map)
            numbered = "\n".join(f"{idx}: {question}" for idx, question in enumerate(questions))
            prompt = (
                "Based on the conversation below, answer each question briefly. "
                "Return only a JSON object mapping the question number to its answer.\n\n"
                f"{context}\n\nQuestions:\n{numbered}"
            )
            try:
                response = _request(
                    endpoint,
                    model,
                    [{"role": "user", "content": prompt}],
                    max_tokens,
                    timeout,
                )
                answers = parse_batch_answers(response, len(batch))
            except Exception as exc:
                answers = {}
                for index in range(len(batch)):
                    batch[index]["_error"] = f"{type(exc).__name__}: {exc}"
            for local_index, qa in enumerate(batch):
                answer = answers.get(local_index, "")
                choice_map = options[local_index]
                if choice_map:
                    match = re.search(r"\b([ab])\b", answer.lower())
                    answer = choice_map.get(match.group(1), "No information available") if match else answer
                qa["qwen2.5-7b-instruct_prediction"] = answer
        scores, _, _ = eval_question_answering(qas, "qwen2.5-7b-instruct_prediction")
        sample_rows = []
        for qa, score in zip(qas, scores):
            row = {"sample_id": sample.get("sample_id"), "category": int(qa["category"]), "score": float(score)}
            score_rows.append(row)
            sample_rows.append(row)
        raw_predictions.append({"sample_id": sample.get("sample_id"), "qa": qas, "scores": sample_rows})
    (out_path / "locomo_qa_predictions.json").write_text(json.dumps(raw_predictions, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = aggregate_scores(score_rows)
    summary.update({"status": "available", "evaluator": "locomo.task_eval.evaluation.eval_question_answering", "model": model, "sample_count": len(selected), "production_latency_claim": False})
    (out_path / "locomo_qa_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-file", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-context-chars", type=int, default=90000)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    print(json.dumps(run_official_eval(**vars(args)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
