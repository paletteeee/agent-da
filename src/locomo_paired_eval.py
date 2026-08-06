"""Run LoCoMo QA through the same native memory backend used for ingestion.

The released LoCoMo QA evaluator is still the scoring authority.  This
runner differs from :mod:`locomo_official_eval` in one important way: it first
asks the model to process the conversation and persist salient facts through
TxnMem memory tools, then asks the QA batches without replaying the raw
conversation.  The native event log and QA score therefore belong to the same
conversation-level run, while remaining separate evidence fields.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any, Mapping, Sequence

from locomo_official_eval import (
    _conversation_context,
    _question_for_model,
    aggregate_scores,
    normalize_qa_for_evaluator,
    parse_batch_answers,
)
from txnmem_backend import SQLiteInstrumentedMemoryBackend
from txnmem_model_protocol import OpenAICompatibleClient
from txnmem_real_agent import run_real_agent


def build_paired_question_prompt(
    qas: Sequence[Mapping[str, Any]],
    *,
    rng: random.Random | None = None,
) -> tuple[str, list[dict[str, str] | None]]:
    """Build a memory-only QA prompt and return category-5 choice maps."""

    random_source = rng or random.Random(17)
    questions: list[str] = []
    choice_maps: list[dict[str, str] | None] = []
    for qa in qas:
        question, choice_map = _question_for_model(qa, random_source)
        questions.append(question)
        choice_maps.append(choice_map)
    numbered = "\n".join(f"{index}: {question}" for index, question in enumerate(questions))
    prompt = (
        "Answer each question. Use the memory tools and the facts already stored in this "
        "conversation's memory backend. Do not use the original conversation because it is not "
        "included in this phase. Return only a JSON object mapping each question number to a "
        "brief answer.\n\nQuestions:\n"
        f"{numbered}"
    )
    return prompt, choice_maps


def aggregate_paired_scores(
    rows: Sequence[Mapping[str, Any]],
    *,
    native_event_count: int,
    sample_count: int,
    ingestion_completed: int,
    qa_batch_count: int = 0,
) -> dict[str, Any]:
    """Aggregate official QA scores with explicit paired-run metadata."""

    result = aggregate_scores(rows)
    result.update(
        {
            "native_event_count": int(native_event_count),
            "sample_count": int(sample_count),
            "ingestion_completed": int(ingestion_completed),
            "qa_batch_count": int(qa_batch_count),
            "paired_native_memory": True,
            "production_latency_claim": False,
        }
    )
    return result


def format_memory_context(memories: Sequence[Mapping[str, Any]]) -> str:
    """Serialize records returned by the backend for the QA model only."""

    rows: list[str] = []
    for memory in memories:
        if not isinstance(memory, Mapping):
            continue
        memory_id = str(memory.get("memory_id", ""))
        value = memory.get("value", "")
        if memory_id:
            rows.append(f"- memory_id={memory_id}; value={value}")
    return "\n".join(rows) if rows else "(no active memories were returned)"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_paired_eval(
    data_file: str | Path,
    out_dir: str | Path,
    endpoint: str,
    model: str,
    *,
    limit: int | None = None,
    batch_size: int = 8,
    max_context_chars: int = 24000,
    max_ingest_steps: int = 12,
    max_qa_steps: int = 6,
    max_tokens: int = 512,
    timeout: float = 180.0,
    seed: int = 17,
) -> dict[str, Any]:
    """Run conversation ingestion and QA using one backend per sample."""

    from task_eval.evaluation import eval_question_answering

    samples = json.loads(Path(data_file).read_text(encoding="utf-8"))
    if not isinstance(samples, list):
        raise ValueError("LoCoMo data file must contain a JSON list")
    selected = samples[:limit] if limit is not None else samples
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    client = OpenAICompatibleClient(endpoint, model, timeout_s=timeout)
    score_rows: list[dict[str, Any]] = []
    raw_predictions: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []
    native_event_count = 0
    ingestion_completed = 0
    qa_batch_count = 0

    for sample_index, sample in enumerate(selected):
        sample_id = str(sample.get("sample_id", sample_index))
        qas = [normalize_qa_for_evaluator(qa) for qa in sample.get("qa", [])]
        context = _conversation_context(sample.get("conversation", {}), max_context_chars)
        database_path = out_path / "data" / f"memory_{sample_index:04d}.sqlite"
        backend = SQLiteInstrumentedMemoryBackend(database_path)
        try:
            ingest = run_real_agent(
                {
                    "task_id": f"{sample_id}:ingest",
                    "agent_id": "locomo_paired_agent",
                    "system_prompt": (
                        "You are the ingestion phase of a long-term memory agent. Store salient "
                        "facts from the conversation with memory_write or memory_derive. Use only "
                        "facts present in the conversation and finish with READY."
                    ),
                    "prompt": f"Conversation to ingest:\n{context}",
                    "max_steps": max_ingest_steps,
                },
                client,
                backend,
                max_steps=max_ingest_steps,
                seed=seed + sample_index,
                temperature=0.0,
            )
            ingest_ok = ingest.get("status") == "completed"
            ingestion_completed += int(ingest_ok)
            phase_rows.append(
                {
                    "sample_id": sample_id,
                    "ingestion_status": ingest.get("status"),
                    "ingestion_failure_code": ingest.get("failure_code"),
                }
            )

            predictions: list[str] = [""] * len(qas)
            for batch_start in range(0, len(qas), batch_size):
                batch = qas[batch_start : batch_start + batch_size]
                prompt, choice_maps = build_paired_question_prompt(
                    batch,
                    rng=random.Random(seed + sample_index * 10000 + batch_start),
                )
                retrieved = backend.search(
                    query=None,
                    agent_id="locomo_paired_agent",
                    projection="paired_memory_retrieval",
                )
                retrieval_prompt = (
                    "Retrieved active memory records from the instrumented backend:\n"
                    f"{format_memory_context(retrieved)}\n\n{prompt}"
                )
                try:
                    response = client.complete(
                        [
                            {
                                "role": "system",
                                "content": (
                                    "You are answering LoCoMo questions from backend-retrieved "
                                    "memory records. Do not use the original conversation."
                                ),
                            },
                            {"role": "user", "content": retrieval_prompt},
                        ],
                        [],
                        seed=seed + sample_index * 10000 + batch_start,
                        temperature=0.0,
                    )
                    answer_text = response.text
                except Exception:
                    answer_text = ""
                answers = parse_batch_answers(str(answer_text), len(batch))
                for local_index, answer in answers.items():
                    choice_map = choice_maps[local_index]
                    if choice_map:
                        lowered = answer.lower()
                        selected_option = next((option for option in ("a", "b") if option in lowered), None)
                        answer = choice_map.get(selected_option, answer) if selected_option else answer
                    predictions[batch_start + local_index] = answer
                qa_batch_count += 1

            for qa, prediction in zip(qas, predictions):
                qa["txnmem_paired_prediction"] = prediction
            scores, _, _ = eval_question_answering(qas, "txnmem_paired_prediction")
            sample_rows: list[dict[str, Any]] = []
            for question_index, (qa, score) in enumerate(zip(qas, scores)):
                row = {
                    "sample_id": sample_id,
                    "question_index": question_index,
                    "category": int(qa["category"]),
                    "score": float(score),
                }
                score_rows.append(row)
                sample_rows.append(row)
            raw_predictions.append({"sample_id": sample_id, "qa": qas, "scores": sample_rows})
        finally:
            native_event_count += len(backend.validated_events())
            backend.close()

    summary = aggregate_paired_scores(
        score_rows,
        native_event_count=native_event_count,
        sample_count=len(selected),
        ingestion_completed=ingestion_completed,
        qa_batch_count=qa_batch_count,
    )
    summary.update(
        {
            "status": "available",
            "evaluator": "locomo.task_eval.evaluation.eval_question_answering",
            "model": model,
            "batch_size": int(batch_size),
            "max_context_chars": int(max_context_chars),
            "phase_rows": phase_rows,
        }
    )
    _write_json(out_path / "locomo_paired_predictions.json", raw_predictions)
    _write_json(out_path / "locomo_paired_summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-file", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-context-chars", type=int, default=24000)
    parser.add_argument("--max-ingest-steps", type=int, default=12)
    parser.add_argument("--max-qa-steps", type=int, default=6)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()
    result = run_paired_eval(**vars(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
