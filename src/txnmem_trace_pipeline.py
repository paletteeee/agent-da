"""File-backed trace ingestion, holdout splitting, and replay helpers."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any, Iterable

from txnmem_bench_adapters import adapt_records
from txnmem_trace import trace_to_instance
from txnmem_schema import validate_instance
from txnmem_simulator import VARIANTS, run_instance
from txnmem_metrics import result_row


def load_trace_records(path: Path) -> list[dict[str, Any]]:
    """Load JSON, JSONL, or an object containing an ``events`` array."""

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
        if isinstance(loaded, list):
            return [item for item in loaded if isinstance(item, dict)]
        if isinstance(loaded, dict):
            events = loaded.get("events") or loaded.get("records")
            if isinstance(events, list):
                return [item for item in events if isinstance(item, dict)]
            return [loaded]
    except json.JSONDecodeError:
        pass
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number} of {path}") from exc
        if isinstance(item, dict):
            records.append(item)
    return records


def _episode_key(record: dict[str, Any]) -> str:
    if record.get("task_id") is not None and record.get("trial") is not None:
        return f"{record['task_id']}:trial:{record['trial']}"
    for name in (
        "episode_id",
        "task_id",
        "conversation_id",
        "trajectory_id",
        "dialogue_id",
        "sample_id",
    ):
        if record.get(name) is not None:
            return str(record[name])
    return "episode_0001"


def build_trace_instances(
    records: Iterable[dict[str, Any]],
    adapter: str,
    *,
    source: str,
    seed: int = 0,
) -> list[dict[str, Any]]:
    """Adapt records by episode and materialize validated trace instances."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_episode_key(record), []).append(record)
    instances: list[dict[str, Any]] = []
    for index, (episode_id, episode_records) in enumerate(sorted(grouped.items()), start=1):
        adapted = adapt_records(adapter, episode_records, episode_id=episode_id)
        if not adapted.events:
            continue
        instance = trace_to_instance(adapted.events, f"{adapter}_{episode_id}", seed=seed + index - 1)
        if not instance.get("operations"):
            # A policy-only or dialogue-only episode has no executable memory
            # history; retain it in skipped-event metadata rather than making
            # an invalid synthetic trigger for a nonexistent operation.
            continue
        _add_replay_transaction_envelope(instance)
        instance["trace_metadata"] = {
            "source": source,
            "adapter": adapter,
            "episode_id": episode_id,
            "skipped_events": adapted.skipped_events,
            "warnings": adapted.warnings,
            "event_count": sum(
                operation.get("type") not in {"begin_txn", "commit"}
                for operation in instance.get("operations", [])
            ),
            "adapted_record_count": len(adapted.events),
            "transaction_envelope": "episode_projection",
        }
        validate_instance(instance)
        instances.append(instance)
    return instances


def _add_replay_transaction_envelope(instance: dict[str, Any]) -> None:
    """Make a transaction-free external episode executable by the replay engine.

    Public workflow/API logs rarely expose TxnMem transaction boundaries.  The
    adapter therefore projects one episode into one transaction only when the
    source did not already provide ``begin_txn``/``commit`` events.  This is an
    explicit replay assumption, not a claim about the source benchmark.
    """

    operations = instance.get("operations", [])
    if any(operation.get("type") in {"begin_txn", "commit"} for operation in operations):
        return
    if not operations:
        return
    txn_id = next((operation.get("txn_id") for operation in operations if operation.get("txn_id")), "txn_trace")
    agent_id = operations[0].get("agent_id", "agent_1")
    for operation in operations:
        operation["txn_id"] = txn_id
        operation["step"] = int(operation.get("step", 0)) + 1
    begin = {
        "op_id": f"{instance['instance_id']}:begin",
        "step": 1,
        "agent_id": agent_id,
        "txn_id": txn_id,
        "type": "begin_txn",
    }
    commit = {
        "op_id": f"{instance['instance_id']}:commit",
        "step": max(int(operation.get("step", 0)) for operation in operations) + 1,
        "agent_id": agent_id,
        "txn_id": txn_id,
        "type": "commit",
    }
    instance["operations"] = [begin, *operations, commit]
    instance.setdefault("config", {})["txn_size"] = max(
        1,
        sum(operation.get("type") in {"write", "derive", "propagate", "supersede"} for operation in operations),
    )


def split_holdout(
    records: Iterable[dict[str, Any]], holdout_fraction: float = 0.2, seed: int = 0
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Deterministically split complete episodes, preserving record order."""

    materialized = list(records)
    if not 0.0 <= holdout_fraction < 1.0:
        raise ValueError("holdout_fraction must be in [0, 1)")
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in materialized:
        groups.setdefault(_episode_key(record), []).append(record)
    keys = sorted(groups)
    random.Random(seed).shuffle(keys)
    holdout_count = int(round(len(keys) * holdout_fraction)) if keys else 0
    if holdout_fraction > 0 and keys:
        holdout_count = max(1, holdout_count)
    holdout_keys = set(keys[:holdout_count])
    train = [record for key in sorted(groups) if key not in holdout_keys for record in groups[key]]
    holdout = [record for key in sorted(groups) if key in holdout_keys for record in groups[key]]
    return train, holdout


def replay_trace_instances(
    instances: Iterable[dict[str, Any]], variants: Iterable[str] = VARIANTS
) -> list[dict[str, Any]]:
    """Replay trace-grounded instances without changing their recorded events."""

    return [
        result_row(instance, run_instance(instance, variant))
        for instance in instances
        for variant in variants
    ]


def trace_inventory(instances: Iterable[dict[str, Any]]) -> dict[str, Any]:
    materialized = list(instances)
    event_count = 0
    for instance in materialized:
        metadata = instance.get("trace_metadata", {})
        if "event_count" in metadata:
            event_count += int(metadata["event_count"])
        else:
            event_count += sum(
                operation.get("type") not in {"begin_txn", "commit"}
                for operation in instance.get("operations", [])
            )
    return {
        "instance_count": len(materialized),
        "event_count": event_count,
        "sources": sorted({instance.get("trace_metadata", {}).get("source", "unknown") for instance in materialized}),
        "adapters": sorted({instance.get("trace_metadata", {}).get("adapter", "unknown") for instance in materialized}),
        "status": "trace_supplied" if materialized else "not_supplied",
    }
