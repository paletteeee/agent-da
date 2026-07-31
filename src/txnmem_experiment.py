#!/usr/bin/env python3
"""Minimal, deterministic TxnMemBench pilot experiment.

This module is intentionally dependency-free.  It provides a reference
simulator for the first three controlled workloads so that dataset generation,
invariant checking, and result export can be tested before integrating a real
memory backend or LLM workflow.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import random
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable

from txnmem_invariants import check_invariants as core_check_invariants
from txnmem_metrics import (
    result_row,
    summarize,
    write_repair_figure,
    write_summary,
    write_violation_figure,
)
from txnmem_schema import load_workload_config
from txnmem_simulator import VARIANTS as CORE_VARIANTS
from txnmem_simulator import run_instance as core_run_instance
from txnmem_workloads import WORKLOADS as CORE_WORKLOADS
from txnmem_workloads import generate_suite as generate_core_suite


WORKLOADS = (
    "atomic_multi_write",
    "revoke_before_commit",
    "provenance_chain_repair",
)
VARIANTS = (
    "Naive",
    "TxnMem-NoTxn",
    "TxnMem-NoPolicyCommit",
    "TxnMem-NoRepair",
    "TxnMem",
)
NO_TRANSACTION_VARIANTS = {"Naive", "TxnMem-NoTxn"}
POLICY_REVALIDATION_VARIANTS = {"TxnMem", "TxnMem-NoTxn", "TxnMem-NoRepair"}
REPAIR_VARIANTS = {"TxnMem", "TxnMem-NoTxn", "TxnMem-NoPolicyCommit"}
DEFAULT_CONFIG = {
    "agent_count": 2,
    "txn_size": 2,
    "provenance_depth": 2,
    "branch_factor": 1,
    "concurrency": 1,
    "policy_churn": 0,
}


def _merged_config(config: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(DEFAULT_CONFIG)
    if config:
        merged.update(config)
    for name in ("agent_count", "txn_size", "provenance_depth", "branch_factor"):
        if int(merged[name]) < 1:
            raise ValueError(f"{name} must be >= 1")
    return merged


def _memory(memory_id: str, status: str = "active", **extra: Any) -> dict[str, Any]:
    value = {
        "memory_id": memory_id,
        "agent_id": extra.pop("agent_id", "agent_a"),
        "scope": extra.pop("scope", "tenant:user_001"),
        "entity_id": extra.pop("entity_id", "user_001"),
        "attribute": extra.pop("attribute", "fact"),
        "value": extra.pop("value", memory_id),
        "status": status,
        "policy_version": extra.pop("policy_version", 1),
        "supersedes_id": extra.pop("supersedes_id", None),
        "derived_from": extra.pop("derived_from", []),
    }
    value.update(extra)
    return value


def generate_instance(
    workload: str, seed: int, config: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Generate one deterministic TxnMemBench instance."""

    if workload not in WORKLOADS:
        raise ValueError(f"unsupported workload: {workload}")
    merged = _merged_config(config)
    rng = random.Random(seed)
    agent = f"agent_{rng.randrange(merged['agent_count']) + 1}"
    instance: dict[str, Any] = {
        "instance_id": f"{workload}_seed_{seed}",
        "workload": workload,
        "seed": seed,
        "config": merged,
        "initial_memories": [],
        "operations": [],
        "policies": [
            {
                "policy_id": "p_write",
                "version": 1,
                "agent_id": agent,
                "action": "write",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            }
        ],
        "failure_schedule": [],
        "provenance_edges": [],
        "expected_outcome": {},
    }

    if workload == "atomic_multi_write":
        txn_size = int(merged["txn_size"])
        instance["operations"].append(
            {"op_id": "op_001", "step": 1, "agent_id": agent, "txn_id": "txn_001", "type": "begin_txn"}
        )
        for index in range(txn_size):
            step = index + 2
            instance["operations"].append(
                {
                    "op_id": f"op_{step:03d}",
                    "step": step,
                    "agent_id": agent,
                    "txn_id": "txn_001",
                    "type": "write",
                    "memory_id": f"m_write_{index + 1}",
                    "source_ids": [],
                    "policy_version": 1,
                }
            )
        instance["operations"].append(
            {
                "op_id": f"op_{txn_size + 2:03d}",
                "step": txn_size + 2,
                "agent_id": agent,
                "txn_id": "txn_001",
                "type": "commit",
            }
        )
        instance["failure_schedule"] = [
            {"step": 2, "type": "crash", "target": "txn_001"}
        ]
        instance["expected_outcome"] = {
            "transaction_state": "abort",
            "committed_memory_ids": [],
            "invariants": {"atomicity": True},
        }

    elif workload == "revoke_before_commit":
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "txn_id": "txn_001", "type": "begin_txn"},
            {
                "op_id": "op_002",
                "step": 2,
                "agent_id": agent,
                "txn_id": "txn_001",
                "type": "write",
                "memory_id": "m_protected_write",
                "source_ids": [],
                "policy_version": 1,
            },
            {"op_id": "op_003", "step": 3, "agent_id": agent, "txn_id": "txn_001", "type": "commit"},
        ]
        instance["failure_schedule"] = [
            {"step": 3, "type": "revoke", "target": "write"}
        ]
        instance["expected_outcome"] = {
            "transaction_state": "abort",
            "committed_memory_ids": [],
            "invariants": {"commit_authorization": True},
        }

    elif workload == "provenance_chain_repair":
        depth = int(merged["provenance_depth"])
        root_id = "m_root"
        instance["initial_memories"].append(_memory(root_id, value="source_v1"))
        previous = root_id
        for index in range(1, depth + 1):
            current = f"m_derived_{index}"
            instance["initial_memories"].append(
                _memory(current, value=f"derived_v{index}", derived_from=[previous])
            )
            instance["provenance_edges"].append(
                {"source_id": previous, "derived_id": current, "relation": "derived_from"}
            )
            previous = current
        instance["operations"] = [
            {
                "op_id": "op_001",
                "step": 1,
                "agent_id": agent,
                "txn_id": "txn_repair",
                "type": "invalidate",
                "memory_id": root_id,
            }
        ]
        instance["failure_schedule"] = []
        instance["expected_outcome"] = {
            "transaction_state": "repaired",
            "root_memory_id": root_id,
            "invariants": {"provenance_closure": True},
        }

    return instance


def _descendants(instance: dict[str, Any], source_id: str) -> set[str]:
    children: dict[str, list[str]] = defaultdict(list)
    for edge in instance["provenance_edges"]:
        children[edge["source_id"]].append(edge["derived_id"])
    found: set[str] = set()
    queue = deque(children.get(source_id, []))
    while queue:
        current = queue.popleft()
        if current in found:
            continue
        found.add(current)
        queue.extend(children.get(current, []))
    return found


def _apply_repair(instance: dict[str, Any], memories: dict[str, dict[str, Any]]) -> int:
    repaired = 0
    invalid_sources = [
        memory_id
        for memory_id, memory in memories.items()
        if memory.get("status") == "invalid"
    ]
    for source_id in invalid_sources:
        for descendant_id in _descendants(instance, source_id):
            if memories[descendant_id]["status"] != "invalid":
                memories[descendant_id]["status"] = "invalid"
                repaired += 1
    return repaired


def run_instance(instance: dict[str, Any], variant: str) -> dict[str, Any]:
    """Replay one instance using a baseline, ablation, or full semantics."""

    if variant not in VARIANTS:
        raise ValueError(f"unsupported variant: {variant}")
    uses_transaction = variant not in NO_TRANSACTION_VARIANTS
    memories = {
        memory["memory_id"]: copy.deepcopy(memory)
        for memory in instance["initial_memories"]
    }
    buffered_writes: list[dict[str, Any]] = []
    committed_memory_ids: list[str] = []
    trace: list[dict[str, Any]] = []
    current_policy_version = 1
    begin_policy_version = 1
    write_allowed = True
    transaction_state = "active"
    repair_count = 0

    scheduled_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in instance["failure_schedule"]:
        scheduled_by_step[int(event["step"])].append(event)

    for operation in instance["operations"]:
        step = int(operation["step"])
        for event in scheduled_by_step.get(step, []):
            if event["type"] == "revoke":
                current_policy_version += 1
                write_allowed = False
                trace.append({"step": step, "event": "revoke", "policy_version": current_policy_version})

        op_type = operation["type"]
        trace.append({"step": step, "operation": op_type})
        if op_type == "begin_txn":
            begin_policy_version = current_policy_version
        elif op_type == "write":
            memory = _memory(
                operation["memory_id"],
                agent_id=operation["agent_id"],
                policy_version=current_policy_version,
            )
            if not uses_transaction:
                memories[memory["memory_id"]] = memory
                committed_memory_ids.append(memory["memory_id"])
            else:
                buffered_writes.append(memory)
        elif op_type == "commit":
            if variant in POLICY_REVALIDATION_VARIANTS and (
                current_policy_version != begin_policy_version or not write_allowed
            ):
                buffered_writes.clear()
                transaction_state = "aborted"
            elif not uses_transaction:
                transaction_state = "committed"
            else:
                for memory in buffered_writes:
                    memories[memory["memory_id"]] = memory
                    committed_memory_ids.append(memory["memory_id"])
                buffered_writes.clear()
                transaction_state = "committed"
        elif op_type == "invalidate":
            memory_id = operation["memory_id"]
            if memory_id not in memories:
                raise KeyError(f"unknown memory_id: {memory_id}")
            memories[memory_id]["status"] = "invalid"
            if variant in REPAIR_VARIANTS:
                repair_count += _apply_repair(instance, memories)
                transaction_state = "repaired"
            else:
                transaction_state = "invalidated"

        if any(event["type"] == "crash" for event in scheduled_by_step.get(step, [])):
            if uses_transaction:
                buffered_writes.clear()
                transaction_state = "aborted"
            elif committed_memory_ids:
                transaction_state = "partial_commit"
            else:
                transaction_state = "crashed"
            trace.append({"step": step, "event": "crash"})
            break

    return {
        "variant": variant,
        "transaction_state": transaction_state,
        "final_memories": memories,
        "committed_memory_ids": committed_memory_ids,
        "trace": trace,
        "metrics": {
            "operation_count": len(trace),
            "repair_count": repair_count,
            "policy_version_at_end": current_policy_version,
        },
    }


def check_invariants(instance: dict[str, Any], result: dict[str, Any]) -> list[str]:
    """Return stable names for violations in one replay result."""

    violations: list[str] = []
    workload = instance["workload"]
    expected = instance["expected_outcome"]
    committed = result["committed_memory_ids"]

    if workload == "atomic_multi_write":
        expected_size = int(instance["config"]["txn_size"])
        if result["transaction_state"] == "partial_commit":
            violations.append("atomicity_violation")
        elif result["transaction_state"] == "committed" and len(committed) not in (0, expected_size):
            violations.append("atomicity_violation")
        if expected["transaction_state"] == "abort" and result["transaction_state"] == "committed":
            violations.append("unexpected_commit")

    if workload == "revoke_before_commit":
        if result["transaction_state"] == "committed":
            violations.append("invalid_commit_violation")
        elif "m_protected_write" in result["final_memories"]:
            violations.append("stale_write_violation")

    if workload == "provenance_chain_repair":
        for memory_id in _descendants(instance, instance["expected_outcome"]["root_memory_id"]):
            memory = result["final_memories"].get(memory_id)
            if memory is not None and memory.get("status") == "active":
                violations.append("provenance_closure_violation")
                break

    return violations


def _repair_recall(instance: dict[str, Any], result: dict[str, Any]) -> float:
    if instance["workload"] != "provenance_chain_repair":
        return 0.0
    affected = _descendants(instance, instance["expected_outcome"]["root_memory_id"])
    if not affected:
        return 1.0
    repaired = sum(
        result["final_memories"].get(memory_id, {}).get("status") == "invalid"
        for memory_id in affected
    )
    return repaired / len(affected)


def _result_row(instance: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    violations = check_invariants(instance, result)
    return {
        "instance_id": instance["instance_id"],
        "workload": instance["workload"],
        "seed": instance["seed"],
        "variant": result["variant"],
        "transaction_state": result["transaction_state"],
        "partial_update_rate": float("atomicity_violation" in violations),
        "invalid_commit_rate": float("invalid_commit_violation" in violations),
        "stale_write_rate": float("stale_write_violation" in violations),
        "repair_recall": _repair_recall(instance, result),
        "any_violation": int(bool(violations)),
        "violations": ";".join(violations),
        "committed_count": len(result["committed_memory_ids"]),
        "operation_count": result["metrics"]["operation_count"],
        "repair_count": result["metrics"]["repair_count"],
    }


def run_suite(
    workloads: Iterable[str] = WORKLOADS,
    seeds: Iterable[int] = range(10),
    variants: Iterable[str] = VARIANTS,
    config: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate and replay a suite; return instances and flat result rows."""

    instances: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for workload in workloads:
        for seed in seeds:
            instance = generate_instance(workload, int(seed), config=config)
            instances.append(instance)
            for variant in variants:
                rows.append(_result_row(instance, run_instance(instance, variant)))
    return instances, rows


def write_jsonl(records: Iterable[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _run_instances(instances: list[dict[str, Any]], variants: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instance in instances:
        for variant in variants:
            rows.append(_result_row(instance, run_instance(instance, variant)))
    return rows


def _run_core_instances(instances: list[dict[str, Any]], variants: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instance in instances:
        for variant in variants:
            result = core_run_instance(instance, variant)
            rows.append(result_row(instance, result))
    return rows


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate JSONL instances")
    generate.add_argument("--out", type=Path, required=True)
    generate.add_argument("--seeds", type=int, default=10)
    generate.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS))

    run = subparsers.add_parser("run", help="replay JSONL instances")
    run.add_argument("--instances", type=Path, required=True)
    run.add_argument("--results", type=Path, required=True)
    run.add_argument("--variants", nargs="+", choices=VARIANTS, default=list(VARIANTS))

    pilot = subparsers.add_parser("pilot", help="generate and run the pilot suite")
    pilot.add_argument("--out-dir", type=Path, default=Path("."))
    pilot.add_argument("--seeds", type=int, default=10)

    experiment = subparsers.add_parser("experiment", help="generate, replay, and summarize W1-W8")
    experiment.add_argument("--config", type=Path, default=Path("configs/workload_families.yaml"))
    experiment.add_argument("--out-dir", type=Path, default=Path("."))
    experiment.add_argument("--seeds", type=int, default=10)
    experiment.add_argument("--variants", nargs="+", choices=CORE_VARIANTS, default=list(CORE_VARIANTS))

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "experiment":
        load_workload_config(args.config)
        instances = generate_core_suite(workloads=CORE_WORKLOADS, seeds=range(args.seeds))
        rows = _run_core_instances(instances, args.variants)
        data_path = args.out_dir / "data" / "generated_instances.jsonl"
        result_path = args.out_dir / "results" / "experiment_results.csv"
        summary_path = args.out_dir / "results" / "summary.json"
        figures_dir = args.out_dir / "results" / "figures"
        write_jsonl(instances, data_path)
        write_csv(rows, result_path)
        summary = summarize(rows, ("workload", "variant"))
        write_summary(summary, summary_path)
        write_violation_figure(summary, figures_dir / "violation_rate.svg")
        write_repair_figure(summary, figures_dir / "repair_recall.svg")
        print(f"generated {len(instances)} instances -> {data_path}")
        print(f"wrote {len(rows)} result rows -> {result_path}")
        print(f"wrote summary and figures -> {args.out_dir / 'results'}")
        return 0

    if args.command == "generate":
        instances, _ = run_suite(workloads=args.workloads, seeds=range(args.seeds), variants=[])
        write_jsonl(instances, args.out)
        print(f"generated {len(instances)} instances -> {args.out}")
        return 0

    if args.command == "run":
        instances = _load_jsonl(args.instances)
        rows = _run_instances(instances, args.variants)
        write_csv(rows, args.results)
        print(f"wrote {len(rows)} result rows -> {args.results}")
        return 0

    if args.command == "pilot":
        data_path = args.out_dir / "data" / "pilot_instances.jsonl"
        result_path = args.out_dir / "results" / "pilot_results.csv"
        instances, rows = run_suite(seeds=range(args.seeds))
        write_jsonl(instances, data_path)
        write_csv(rows, result_path)
        print(f"generated {len(instances)} instances -> {data_path}")
        print(f"wrote {len(rows)} result rows -> {result_path}")
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
