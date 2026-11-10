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
from collections import Counter
from importlib.metadata import PackageNotFoundError, version as package_version
import json
import random
import re
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

from locomo_official_eval import (
    _conversation_context,
    _question_for_model,
    aggregate_scores,
    normalize_qa_for_evaluator,
    parse_batch_answers,
)
from txnmem_backend import SQLiteInstrumentedMemoryBackend
from txnmem_conditions import canonical_fingerprint, file_sha256, source_identity
from txnmem_model_protocol import (
    ModelProtocolError,
    OpenAICompatibleClient,
    add_response_usage,
    empty_usage_summary,
    merge_usage_summaries,
)
from txnmem_real_agent import run_real_agent


def build_paired_question_prompt(
    qas: Sequence[Mapping[str, Any]],
    *,
    rng: random.Random | None = None,
    prompt_profile: str = "baseline",
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
    if prompt_profile == "baseline":
        instructions = (
            "Answer each question. Use the memory tools and the facts already stored in this "
            "conversation's memory backend. Do not use the original conversation because it is not "
            "included in this phase. Return only a JSON object mapping each question number to a "
            "brief answer."
        )
    elif prompt_profile == "tuned":
        instructions = (
            "Answer each question only from the backend-retrieved memory records. Resolve temporal "
            "updates and superseded facts before answering, cross-check related records, preserve "
            "names and dates exactly, and do not invent missing details. The original conversation "
            "is unavailable. Return only a JSON object mapping each question number to one brief answer."
        )
    else:
        raise ValueError(f"unsupported prompt profile: {prompt_profile}")
    prompt = f"{instructions}\n\nQuestions:\n{numbered}"
    return prompt, choice_maps


def build_ingestion_system_prompt(prompt_profile: str = "baseline") -> str:
    """Return the fixed ingestion instruction for one paired-QA condition."""

    if prompt_profile == "baseline":
        return (
            "You are the ingestion phase of a long-term memory agent. Store salient facts from "
            "the conversation with memory_write or memory_derive. Use only facts present in the "
            "conversation and finish with READY."
        )
    if prompt_profile == "tuned":
        return (
            "You are the ingestion phase of a long-term memory agent. Store atomic facts with a "
            "stable memory_id that names the entity and attribute; keep dates, people, places, and "
            "preferences explicit. When a later statement corrects an earlier fact, use "
            "memory_supersede. Use memory_derive only for summaries whose source_ids are explicit. "
            "Never add facts absent from the conversation, and finish with READY."
        )
    raise ValueError(f"unsupported prompt profile: {prompt_profile}")


def resolve_category5_answer(answer: str, choice_map: Mapping[str, str]) -> str:
    """Resolve only an isolated A/B label, never a letter inside ordinary text."""

    match = re.search(r"(?<![A-Za-z0-9])([ABab])(?![A-Za-z0-9])", str(answer))
    if match is None:
        return str(answer)
    return str(choice_map.get(match.group(1).lower(), answer))


def aggregate_repetition_summaries(
    summaries: Sequence[Mapping[str, Any]],
    *,
    prompt_profile: str,
    model: str,
) -> dict[str, Any]:
    """Aggregate independent paired-QA repetitions without raw predictions."""

    if not summaries:
        raise ValueError("at least one repetition summary is required")
    available_summaries = [summary for summary in summaries if summary.get("status") == "available"]
    scores: list[float | None] = [
        float(summary.get("mean_f1", 0.0))
        if summary.get("status") == "available"
        else None
        for summary in summaries
    ]
    available_scores = [score for score in scores if score is not None]
    question_counts = [int(summary.get("question_count", 0)) for summary in summaries]
    sample_counts = [int(summary.get("sample_count", 0)) for summary in summaries]
    category_names = sorted(
        {
            str(category)
            for summary in available_summaries
            for category in summary.get("category_mean_f1", {})
        }
    )
    category_f1_mean = {
        category: mean(
            float(summary.get("category_mean_f1", {}).get(category, 0.0))
            for summary in available_summaries
        )
        for category in category_names
    }
    usage = merge_usage_summaries(
        [summary.get("model_usage", {}) for summary in summaries]
    )
    condition_fingerprints = {
        str(summary.get("condition_fingerprint"))
        for summary in summaries
        if summary.get("condition_fingerprint")
    }
    if len(condition_fingerprints) > 1:
        raise ValueError("repetition condition fingerprints differ")
    result = {
        "status": (
            "available"
            if len(available_summaries) == len(summaries)
            else "partial"
            if available_summaries
            else "error"
        ),
        "evaluator": "locomo.task_eval.evaluation.eval_question_answering",
        "model": model,
        "prompt_profile": prompt_profile,
        "repetition_count": len(summaries),
        "successful_repetition_count": len(available_summaries),
        "sample_count_per_repetition": sample_counts,
        "question_count_per_repetition": question_counts,
        "total_question_evaluations": sum(question_counts),
        "native_event_count_total": sum(int(summary.get("native_event_count", 0)) for summary in summaries),
        "mean_f1_by_repetition": scores,
        "mean_f1_mean": mean(available_scores) if available_scores else None,
        "mean_f1_std": pstdev(available_scores) if available_scores else None,
        "category_f1_mean": category_f1_mean,
        "qa_batch_attempt_count_total": sum(
            int(summary.get("qa_batch_attempt_count", summary.get("qa_batch_count", 0)) or 0)
            for summary in summaries
        ),
        "qa_batch_success_count_total": sum(
            int(summary.get("qa_batch_success_count", 0) or 0) for summary in summaries
        ),
        "qa_batch_failure_count_total": sum(
            int(summary.get("qa_batch_failure_count", 0) or 0) for summary in summaries
        ),
        "qa_batch_parse_failure_count_total": sum(
            int(summary.get("qa_batch_parse_failure_count", 0) or 0) for summary in summaries
        ),
        "qa_question_attempt_count_total": sum(
            int(summary.get("qa_question_attempt_count", summary.get("question_count", 0)) or 0)
            for summary in summaries
        ),
        "qa_question_successful_response_count_total": sum(
            int(summary.get("qa_question_successful_response_count", 0) or 0)
            for summary in summaries
        ),
        "qa_parsed_answer_count_total": sum(
            int(summary.get("qa_parsed_answer_count", 0) or 0) for summary in summaries
        ),
        "model_usage": usage,
        "token_usage_complete": bool(usage["request_count"])
        and usage["request_count"] == usage["responses_with_usage"],
        "paired_native_memory": True,
        "raw_reports_committed": False,
        "production_latency_claim": False,
    }
    if condition_fingerprints:
        result["condition_fingerprint"] = next(iter(condition_fingerprints))
        result["condition"] = summaries[0].get("condition")
    result["treatment"] = {"prompt_profile": prompt_profile}
    return result


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
    prompt_profile: str = "baseline",
    model_revision: str = "unspecified",
) -> dict[str, Any]:
    """Run conversation ingestion and QA using one backend per sample."""

    import locomo_official_eval as official_eval_module
    import txnmem_backend as backend_module
    import txnmem_model_protocol as model_protocol_module
    import txnmem_real_agent as real_agent_module
    from task_eval import evaluation as task_evaluation_module

    eval_question_answering = task_evaluation_module.eval_question_answering

    samples = json.loads(Path(data_file).read_text(encoding="utf-8"))
    if not isinstance(samples, list):
        raise ValueError("LoCoMo data file must contain a JSON list")
    selected = samples[:limit] if limit is not None else samples
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    build_ingestion_system_prompt(prompt_profile)
    client = OpenAICompatibleClient(
        endpoint,
        model,
        timeout_s=timeout,
        max_tokens=max_tokens,
    )
    score_rows: list[dict[str, Any]] = []
    raw_predictions: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []
    native_event_count = 0
    ingestion_completed = 0
    qa_batch_count = 0
    qa_batch_success_count = 0
    qa_batch_failure_count = 0
    qa_batch_parse_failure_count = 0
    qa_question_attempt_count = 0
    qa_question_successful_response_count = 0
    qa_parsed_answer_count = 0
    qa_failure_counts: Counter[str] = Counter()
    model_usage = empty_usage_summary()
    sample_manifest = [
        {
            "sample_id": str(sample.get("sample_id", index)),
            "question_count": len(sample.get("qa", [])),
        }
        for index, sample in enumerate(selected)
    ]
    code_identity = source_identity(
        {
            "locomo_paired_eval": Path(__file__),
            "locomo_official_eval": Path(official_eval_module.__file__),
            "txnmem_backend": Path(backend_module.__file__),
            "txnmem_model_protocol": Path(model_protocol_module.__file__),
            "txnmem_real_agent": Path(real_agent_module.__file__),
            "locomo_official_task_evaluator": Path(task_evaluation_module.__file__),
        }
    )
    try:
        model_server_build = f"vllm:{package_version('vllm')}"
    except PackageNotFoundError:
        model_server_build = "vllm:unknown"
    condition = {
        "dataset": "locomo",
        "source_sha256": file_sha256(data_file),
        "sample_manifest_sha256": canonical_fingerprint(sample_manifest),
        "sample_count": len(selected),
        "question_count": sum(item["question_count"] for item in sample_manifest),
        "model": model,
        "model_revision": str(model_revision),
        "model_revision_status": (
            "sha256"
            if re.fullmatch(r"[0-9a-fA-F]{64}", str(model_revision))
            else "unspecified_or_non_hash"
        ),
        "model_server_build": model_server_build,
        "runner_evaluator_source_identity": code_identity,
        "evaluator": "locomo.task_eval.evaluation.eval_question_answering",
        "memory_backend": "SQLiteInstrumentedMemoryBackend",
        "retrieval_projection": "paired_memory_retrieval",
        "batch_size": int(batch_size),
        "max_context_chars": int(max_context_chars),
        "max_ingest_steps": int(max_ingest_steps),
        "max_qa_steps": int(max_qa_steps),
        "max_tokens": int(max_tokens),
        "timeout_seconds": float(timeout),
        "temperature": 0.0,
        "seed_policy": "repetition_seed_plus_sample_and_batch_offsets",
    }

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
                    "system_prompt": build_ingestion_system_prompt(prompt_profile),
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
            ingest_usage = ingest.get("model_usage", {})
            for field in model_usage:
                model_usage[field] += int(ingest_usage.get(field, 0) or 0)
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
                qa_batch_count += 1
                qa_question_attempt_count += len(batch)
                prompt, choice_maps = build_paired_question_prompt(
                    batch,
                    rng=random.Random(seed + sample_index * 10000 + batch_start),
                    prompt_profile=prompt_profile,
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
                    model_usage["request_count"] += 1
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
                    add_response_usage(model_usage, response)
                    answer_text = response.text
                    qa_batch_success_count += 1
                    qa_question_successful_response_count += len(batch)
                except Exception as exc:
                    qa_batch_failure_count += 1
                    code = (
                        f"model_{exc.code}"
                        if isinstance(exc, ModelProtocolError)
                        else type(exc).__name__
                    )
                    qa_failure_counts[code] += 1
                    answer_text = ""
                answers = parse_batch_answers(str(answer_text), len(batch))
                parsed_in_batch = sum(bool(str(answer).strip()) for answer in answers.values())
                qa_parsed_answer_count += parsed_in_batch
                if parsed_in_batch < len(batch):
                    qa_batch_parse_failure_count += 1
                for local_index, answer in answers.items():
                    choice_map = choice_maps[local_index]
                    if choice_map:
                        answer = resolve_category5_answer(answer, choice_map)
                    predictions[batch_start + local_index] = answer

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
            "status": (
                "available"
                if ingestion_completed == len(selected)
                and qa_batch_failure_count == 0
                and qa_batch_parse_failure_count == 0
                else "error"
                if qa_batch_count and qa_batch_success_count == 0
                else "partial"
            ),
            "evaluator": "locomo.task_eval.evaluation.eval_question_answering",
            "model": model,
            "batch_size": int(batch_size),
            "max_context_chars": int(max_context_chars),
            "prompt_profile": prompt_profile,
            "condition": condition,
            "condition_fingerprint": canonical_fingerprint(condition),
            "treatment": {"prompt_profile": prompt_profile},
            "model_usage": model_usage,
            "token_usage_complete": bool(model_usage["request_count"])
            and model_usage["request_count"] == model_usage["responses_with_usage"],
            "qa_batch_attempt_count": qa_batch_count,
            "qa_batch_success_count": qa_batch_success_count,
            "qa_batch_failure_count": qa_batch_failure_count,
            "qa_batch_parse_failure_count": qa_batch_parse_failure_count,
            "qa_question_attempt_count": qa_question_attempt_count,
            "qa_question_successful_response_count": qa_question_successful_response_count,
            "qa_parsed_answer_count": qa_parsed_answer_count,
            "qa_failure_counts": dict(sorted(qa_failure_counts.items())),
            "phase_rows": phase_rows,
        }
    )
    _write_json(out_path / "data" / "locomo_paired_predictions.json", raw_predictions)
    _write_json(out_path / "locomo_paired_summary.json", summary)
    return summary


def run_paired_repetitions(
    data_file: str | Path,
    out_dir: str | Path,
    endpoint: str,
    model: str,
    *,
    repetitions: int,
    prompt_profile: str = "baseline",
    seed: int = 17,
    **kwargs: Any,
) -> dict[str, Any]:
    """Run isolated repetitions and persist one sanitized aggregate."""

    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    root = Path(out_dir)
    summaries = []
    repetition_seeds = [seed + repetition * 1000 for repetition in range(repetitions)]
    for repetition in range(repetitions):
        summaries.append(
            run_paired_eval(
                data_file,
                root / f"rep_{repetition + 1:02d}",
                endpoint,
                model,
                seed=seed + repetition * 1000,
                prompt_profile=prompt_profile,
                **kwargs,
            )
        )
    aggregate = aggregate_repetition_summaries(
        summaries,
        prompt_profile=prompt_profile,
        model=model,
    )
    aggregate["base_seed"] = seed
    aggregate["repetition_seeds"] = repetition_seeds
    aggregate["raw_reports_location"] = "rep_*/locomo_paired_summary.json"
    _write_json(root / "locomo_paired_repetition_summary.json", aggregate)
    return aggregate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-file", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--model-revision", default="unspecified")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-context-chars", type=int, default=24000)
    parser.add_argument("--max-ingest-steps", type=int, default=12)
    parser.add_argument("--max-qa-steps", type=int, default=6)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--prompt-profile", choices=("baseline", "tuned"), default="baseline")
    parser.add_argument("--repetitions", type=int, default=1)
    args = parser.parse_args()
    arguments = vars(args)
    repetitions = arguments.pop("repetitions")
    if repetitions == 1:
        result = run_paired_eval(**arguments)
    else:
        result = run_paired_repetitions(repetitions=repetitions, **arguments)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
