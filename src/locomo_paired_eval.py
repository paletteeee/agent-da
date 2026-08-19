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
import hashlib
import json
import random
import re
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

from locomo_official_eval import (
    _question_for_model,
    aggregate_scores,
    iter_conversation_sessions,
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
from txnmem_resampling import cluster_bootstrap_interval


FORMAL_PAIRED_SEEDS = (17, 1017, 2017, 3017, 4017)


def conversation_namespace(sample_id: str, prompt_profile: str, seed: int) -> str:
    """Return an isolated backend namespace for one paired condition."""

    if prompt_profile not in {"baseline", "tuned"}:
        raise ValueError(f"unsupported prompt profile: {prompt_profile}")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    return "locomo:" + canonical_fingerprint(
        {
            "sample_id": str(sample_id),
            "prompt_profile": prompt_profile,
            "repetition_seed": seed,
        }
    )[:24]


def _stream_sha256(chunks: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for chunk in chunks:
        encoded = chunk.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def ingest_session_stream(
    sample: Mapping[str, Any],
    ingest_session: Any,
    *,
    max_session_chars: int,
) -> dict[str, Any]:
    """Pass the complete ordered session stream through bounded requests."""

    if isinstance(max_session_chars, bool) or not isinstance(max_session_chars, int):
        raise ValueError("max_session_chars must be a positive integer")
    if max_session_chars < 1:
        raise ValueError("max_session_chars must be a positive integer")
    if not callable(ingest_session):
        raise ValueError("ingest_session must be callable")

    sessions = list(iter_conversation_sessions(sample))
    source_chunks: list[str] = []
    attempted_chunks: list[str] = []
    completed_char_count = 0
    completed_chunk_count = 0
    attempted_sessions: set[str] = set()
    completed_sessions: set[str] = set()
    session_summaries: list[dict[str, Any]] = []
    for session in sessions:
        content = str(session["content"])
        chunks = [
            content[offset : offset + max_session_chars]
            for offset in range(0, len(content), max_session_chars)
        ] or [""]
        source_chunks.extend(chunks)
        session_completed = True
        session_completed_chars = 0
        for chunk_index, chunk in enumerate(chunks):
            payload = {
                "session_id": session["session_id"],
                "session_id_sha256": hashlib.sha256(
                    str(session["session_id"]).encode("utf-8")
                ).hexdigest(),
                "session_position": session["source_position"],
                "date_time": session["date_time"],
                "session_sha256": session["sha256"],
                "chunk_index": chunk_index,
                "chunk_count": len(chunks),
                "content": chunk,
                "char_count": len(chunk),
                "chunk_sha256": hashlib.sha256(chunk.encode("utf-8")).hexdigest(),
            }
            attempted_sessions.add(str(session["session_id"]))
            attempted_chunks.append(chunk)
            try:
                outcome = ingest_session(payload)
                completed = isinstance(outcome, Mapping) and outcome.get("status") == "completed"
            except Exception:
                completed = False
            if completed:
                completed_chunk_count += 1
                completed_char_count += len(chunk)
                session_completed_chars += len(chunk)
            else:
                session_completed = False
        if session_completed:
            completed_sessions.add(str(session["session_id"]))
        session_summaries.append(
            {
                "session_id_sha256": hashlib.sha256(
                    str(session["session_id"]).encode("utf-8")
                ).hexdigest(),
                "source_position": session["source_position"],
                "source_char_count": len(content),
                "attempted_char_count": len(content),
                "completed_char_count": session_completed_chars,
                "chunk_count": len(chunks),
                "completed": session_completed,
                "session_sha256": session["sha256"],
            }
        )

    source_char_count = sum(len(chunk) for chunk in source_chunks)
    attempted_char_count = sum(len(chunk) for chunk in attempted_chunks)
    source_chunk_count = len(source_chunks)
    attempted_chunk_count = len(attempted_chunks)
    return {
        "status": (
            "completed"
            if completed_chunk_count == source_chunk_count
            else "partial"
            if completed_chunk_count
            else "error"
        ),
        "source_session_count": len(sessions),
        "attempted_session_count": len(attempted_sessions),
        "completed_session_count": len(completed_sessions),
        "source_chunk_count": source_chunk_count,
        "attempted_chunk_count": attempted_chunk_count,
        "completed_chunk_count": completed_chunk_count,
        "source_char_count": source_char_count,
        "attempted_char_count": attempted_char_count,
        "completed_char_count": completed_char_count,
        "session_attempt_coverage": (
            len(attempted_sessions) / len(sessions) if sessions else 1.0
        ),
        "session_completion_coverage": (
            len(completed_sessions) / len(sessions) if sessions else 1.0
        ),
        "character_attempt_coverage": (
            attempted_char_count / source_char_count if source_char_count else 1.0
        ),
        "character_completion_coverage": (
            completed_char_count / source_char_count if source_char_count else 1.0
        ),
        "source_stream_sha256": _stream_sha256(source_chunks),
        "attempted_stream_sha256": _stream_sha256(attempted_chunks),
        "session_summaries": session_summaries,
    }


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


def _validated_model_identity(value: Any, expected_model: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("repetition model identities differ or are missing")
    required = ("model", "model_revision", "model_server_build")
    if any(
        not isinstance(value.get(field), str)
        or not value.get(field).strip()
        or value.get(field) != value.get(field).strip()
        for field in required
    ):
        raise ValueError(
            "repetition model identities differ: required fields are missing or empty"
        )
    if value.get("model") != expected_model:
        raise ValueError(
            "repetition model identities differ: identity model disagrees with the model ID"
        )
    return dict(value)


def aggregate_repetition_summaries(
    summaries: Sequence[Mapping[str, Any]],
    *,
    prompt_profile: str,
    model: str,
) -> dict[str, Any]:
    """Aggregate independent paired-QA repetitions without raw predictions."""

    if not summaries:
        raise ValueError("at least one repetition summary is required")
    if (
        not isinstance(model, str)
        or not model.strip()
        or model != model.strip()
    ):
        raise ValueError("repetition model ID is missing or noncanonical")
    task_manifest_values = [summary.get("task_manifest_sha256") for summary in summaries]
    if not all(isinstance(value, str) and value for value in task_manifest_values):
        raise ValueError("repetition task manifests differ or are missing")
    task_manifests = set(task_manifest_values)
    if len(task_manifests) != 1:
        raise ValueError("repetition task manifests differ")
    if any(summary.get("model") != model for summary in summaries):
        raise ValueError("repetition model IDs differ")
    model_identity_values = [
        _validated_model_identity(summary.get("model_identity"), model)
        for summary in summaries
    ]
    model_identities = {
        canonical_fingerprint(value) for value in model_identity_values
    }
    if len(model_identities) != 1:
        raise ValueError("repetition model identities differ")
    available_summaries = [summary for summary in summaries if summary.get("status") == "available"]
    scores: list[float | None] = [
        float(summary.get("mean_f1", 0.0))
        if summary.get("status") == "available"
        else None
        for summary in summaries
    ]
    available_scores = [score for score in scores if score is not None]
    question_counts = [summary.get("question_count") for summary in summaries]
    sample_counts = [summary.get("sample_count") for summary in summaries]
    if any(type(value) is not int or value < 1 for value in question_counts):
        raise ValueError("repetition question denominators are invalid")
    if any(type(value) is not int or value < 1 for value in sample_counts):
        raise ValueError("repetition sample denominators are invalid")
    if len(set(question_counts)) != 1 or len(set(sample_counts)) != 1:
        raise ValueError("repetition denominators differ")
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
    condition_values = [summary.get("condition_fingerprint") for summary in summaries]
    condition_fingerprints = {
        str(value) for value in condition_values if isinstance(value, str) and value
    }
    if len(condition_fingerprints) != 1 or not all(condition_values):
        raise ValueError("repetition condition fingerprints differ or are missing")

    cluster_rows: list[dict[str, Any]] = []
    combined_conversations: dict[str, dict[str, Any]] = {}
    expected_conversation_ids: list[str] | None = None
    conversation_summary_sets = [
        summary.get("conversation_score_summaries") for summary in summaries
    ]
    if not all(isinstance(item, list) and item for item in conversation_summary_sets):
        raise ValueError("repetition conversation score summaries are incomplete")
    if conversation_summary_sets:
        for repetition_index, items in enumerate(conversation_summary_sets):
            assert isinstance(items, list)
            conversation_ids: list[str] = []
            repetition_question_count = 0
            repetition_score_sum = 0.0
            for item in items:
                if not isinstance(item, Mapping):
                    raise ValueError("invalid conversation score summary")
                conversation_id = item.get("conversation_id_sha256")
                question_count = item.get("question_count")
                score_sum = item.get("score_sum")
                if not isinstance(conversation_id, str) or not conversation_id:
                    raise ValueError("invalid conversation score identity")
                if (
                    isinstance(question_count, bool)
                    or not isinstance(question_count, int)
                    or question_count < 1
                ):
                    raise ValueError("invalid conversation question count")
                if isinstance(score_sum, bool) or not isinstance(score_sum, (int, float)):
                    raise ValueError("invalid conversation score sum")
                conversation_ids.append(conversation_id)
                conversation_mean = float(score_sum) / question_count
                repetition_question_count += question_count
                repetition_score_sum += float(score_sum)
                if summaries[repetition_index].get("status") == "available":
                    combined = combined_conversations.setdefault(
                        conversation_id,
                        {
                            "conversation_id_sha256": conversation_id,
                            "question_evaluation_count": 0,
                            "score_sum": 0.0,
                        },
                    )
                    combined["question_evaluation_count"] += question_count
                    combined["score_sum"] += float(score_sum)
                    cluster_rows.extend(
                        {
                            "conversation_id_sha256": conversation_id,
                            "score": conversation_mean,
                            "repetition": repetition_index + 1,
                        }
                        for _ in range(question_count)
                    )
            if len(conversation_ids) != len(set(conversation_ids)):
                raise ValueError("duplicate conversation score identity")
            if len(conversation_ids) != sample_counts[repetition_index]:
                raise ValueError("conversation count disagrees with sample denominator")
            if repetition_question_count != question_counts[repetition_index]:
                raise ValueError("conversation questions disagree with repetition denominator")
            if summaries[repetition_index].get("status") == "available" and abs(
                repetition_score_sum / repetition_question_count
                - float(summaries[repetition_index].get("mean_f1", 0.0))
            ) > 1e-12:
                raise ValueError("conversation scores disagree with repetition mean")
            if expected_conversation_ids is None:
                expected_conversation_ids = conversation_ids
            elif conversation_ids != expected_conversation_ids:
                raise ValueError("repetition conversation task IDs differ")
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
    result["task_manifest_sha256"] = next(iter(task_manifests))
    result["model_identity"] = summaries[0]["model_identity"]
    if cluster_rows:
        result["conversation_count"] = len(expected_conversation_ids or [])
        result["conversation_score_summaries"] = []
        for conversation_id in expected_conversation_ids or []:
            item = dict(combined_conversations[conversation_id])
            item["mean_f1"] = item["score_sum"] / item["question_evaluation_count"]
            result["conversation_score_summaries"].append(item)
        result["cluster_bootstrap_interval"] = cluster_bootstrap_interval(
            cluster_rows,
            "conversation_id_sha256",
            "score",
            repetitions=10000,
            seed=17,
        )
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
    max_session_chars: int = 12000,
    max_context_chars: int | None = None,
    max_ingest_steps: int = 12,
    max_qa_steps: int = 6,
    max_tokens: int = 512,
    timeout: float = 180.0,
    seed: int = 17,
    prompt_profile: str = "baseline",
    model_revision: str = "unspecified",
) -> dict[str, Any]:
    """Run conversation ingestion and QA using one backend per sample."""

    if max_context_chars is not None:
        max_session_chars = int(max_context_chars)

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
    conversation_score_summaries: list[dict[str, Any]] = []
    ingestion_coverage_rows: list[dict[str, Any]] = []
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
        "ingestion_mode": "complete_ordered_session_stream_v1",
        "max_session_chars": int(max_session_chars),
        "max_ingest_steps": int(max_ingest_steps),
        "max_qa_steps": int(max_qa_steps),
        "max_tokens": int(max_tokens),
        "timeout_seconds": float(timeout),
        "temperature": 0.0,
        "seed_policy": "repetition_seed_plus_sample_and_batch_offsets",
    }

    for sample_index, sample in enumerate(selected):
        sample_id = str(sample.get("sample_id", sample_index))
        sample_id_sha256 = hashlib.sha256(sample_id.encode("utf-8")).hexdigest()
        namespace = conversation_namespace(sample_id, prompt_profile, seed)
        qas = [normalize_qa_for_evaluator(qa) for qa in sample.get("qa", [])]
        database_path = out_path / "data" / f"memory_{sample_index:04d}.sqlite"
        backend = SQLiteInstrumentedMemoryBackend(database_path)
        try:
            def ingest_chunk(payload: Mapping[str, Any]) -> Mapping[str, Any]:
                chunk_seed = (
                    seed
                    + sample_index * 10000
                    + int(payload["session_position"]) * 100
                    + int(payload["chunk_index"])
                )
                ingest = run_real_agent(
                    {
                        "task_id": (
                            f"{sample_id}:ingest:{payload['session_position']}:"
                            f"{payload['chunk_index']}"
                        ),
                        "agent_id": namespace,
                        "system_prompt": build_ingestion_system_prompt(prompt_profile),
                        "prompt": (
                            "Chronological LoCoMo session chunk "
                            f"{int(payload['chunk_index']) + 1}/{payload['chunk_count']} "
                            f"for session {int(payload['session_position']) + 1}. "
                            "Store all supported facts from this chunk.\n\n"
                            f"{payload['content']}"
                        ),
                        "max_steps": max_ingest_steps,
                    },
                    client,
                    backend,
                    max_steps=max_ingest_steps,
                    seed=chunk_seed,
                    temperature=0.0,
                )
                ingest_usage = ingest.get("model_usage", {})
                for field in model_usage:
                    model_usage[field] += int(ingest_usage.get(field, 0) or 0)
                return {
                    "status": ingest.get("status"),
                    "failure_code": ingest.get("failure_code"),
                }

            stream_report = ingest_session_stream(
                sample,
                ingest_chunk,
                max_session_chars=max_session_chars,
            )
            ingest_ok = stream_report["status"] == "completed"
            ingestion_completed += int(ingest_ok)
            ingestion_coverage_rows.append(
                {
                    "conversation_id_sha256": sample_id_sha256,
                    "namespace_sha256": hashlib.sha256(namespace.encode("utf-8")).hexdigest(),
                    **stream_report,
                }
            )
            phase_rows.append(
                {
                    "conversation_id_sha256": sample_id_sha256,
                    "namespace_sha256": hashlib.sha256(namespace.encode("utf-8")).hexdigest(),
                    "ingestion_status": stream_report["status"],
                    "source_session_count": stream_report["source_session_count"],
                    "source_char_count": stream_report["source_char_count"],
                    "attempted_char_count": stream_report["attempted_char_count"],
                    "completed_char_count": stream_report["completed_char_count"],
                    "source_stream_sha256": stream_report["source_stream_sha256"],
                    "attempted_stream_sha256": stream_report["attempted_stream_sha256"],
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
                    agent_id=namespace,
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
            score_sum = sum(float(row["score"]) for row in sample_rows)
            conversation_score_summaries.append(
                {
                    "conversation_id_sha256": sample_id_sha256,
                    "question_count": len(sample_rows),
                    "score_sum": score_sum,
                    "mean_f1": score_sum / len(sample_rows) if sample_rows else 0.0,
                }
            )
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
    total_source_sessions = sum(
        int(row["source_session_count"]) for row in ingestion_coverage_rows
    )
    total_attempted_sessions = sum(
        int(row["attempted_session_count"]) for row in ingestion_coverage_rows
    )
    total_completed_sessions = sum(
        int(row["completed_session_count"]) for row in ingestion_coverage_rows
    )
    total_source_chars = sum(
        int(row["source_char_count"]) for row in ingestion_coverage_rows
    )
    total_attempted_chars = sum(
        int(row["attempted_char_count"]) for row in ingestion_coverage_rows
    )
    total_completed_chars = sum(
        int(row["completed_char_count"]) for row in ingestion_coverage_rows
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
            "max_session_chars": int(max_session_chars),
            "prompt_profile": prompt_profile,
            "condition": condition,
            "condition_fingerprint": canonical_fingerprint(condition),
            "task_manifest_sha256": canonical_fingerprint(sample_manifest),
            "model_identity": {
                "model": model,
                "model_revision": str(model_revision),
                "model_server_build": model_server_build,
            },
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
            "conversation_score_summaries": conversation_score_summaries,
            "ingestion_coverage": {
                "conversation_count": len(ingestion_coverage_rows),
                "source_session_count": total_source_sessions,
                "attempted_session_count": total_attempted_sessions,
                "completed_session_count": total_completed_sessions,
                "source_char_count": total_source_chars,
                "attempted_char_count": total_attempted_chars,
                "completed_char_count": total_completed_chars,
                "session_attempt_coverage": (
                    total_attempted_sessions / total_source_sessions
                    if total_source_sessions
                    else 1.0
                ),
                "session_completion_coverage": (
                    total_completed_sessions / total_source_sessions
                    if total_source_sessions
                    else 1.0
                ),
                "character_attempt_coverage": (
                    total_attempted_chars / total_source_chars
                    if total_source_chars
                    else 1.0
                ),
                "character_completion_coverage": (
                    total_completed_chars / total_source_chars
                    if total_source_chars
                    else 1.0
                ),
                "source_stream_set_sha256": canonical_fingerprint(
                    [row["source_stream_sha256"] for row in ingestion_coverage_rows]
                ),
                "attempted_stream_set_sha256": canonical_fingerprint(
                    [row["attempted_stream_sha256"] for row in ingestion_coverage_rows]
                ),
            },
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

    if repetitions != len(FORMAL_PAIRED_SEEDS) or seed != FORMAL_PAIRED_SEEDS[0]:
        raise ValueError(
            "formal LoCoMo paired evaluation requires exactly five repetitions "
            "with seeds 17, 1017, 2017, 3017, and 4017"
        )
    root = Path(out_dir)
    summaries = []
    repetition_seeds = list(FORMAL_PAIRED_SEEDS)
    for repetition, repetition_seed in enumerate(repetition_seeds):
        summaries.append(
            run_paired_eval(
                data_file,
                root / f"rep_{repetition + 1:02d}",
                endpoint,
                model,
                seed=repetition_seed,
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
    parser.add_argument(
        "--max-session-chars",
        "--max-context-chars",
        dest="max_session_chars",
        type=int,
        default=12000,
    )
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
