#!/usr/bin/env python3
"""Compatibility API and CLI for the modular TxnMemBench core."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from txnmem_invariants import check_invariants
from txnmem_metrics import (
    result_row,
    summarize,
    write_repair_figure,
    write_summary,
    write_violation_figure,
)
from txnmem_coverage import coverage_report
from txnmem_coverage import schedule_effectiveness
from txnmem_distributed import run_process_action_sequences
from txnmem_mutation import run_mutation_campaign
from txnmem_performance import benchmark_replay
from txnmem_realism import (
    calibrate_config,
    compare_distributions,
    extract_trace_features,
    split_holdout,
    trace_evidence_summary,
)
from txnmem_reference import reference_outcome
from txnmem_schema import DEFAULT_CONFIG, load_workload_config
from txnmem_simulator import VARIANTS, run_instance
from txnmem_trace_pipeline import (
    build_trace_instances,
    load_trace_records,
    replay_trace_instances,
    trace_inventory,
)
from txnmem_workloads import WORKLOADS, generate_instance, generate_suite


CORE_WORKLOADS = (
    "atomic_multi_write",
    "revoke_before_commit",
    "provenance_chain_repair",
)
FORMAL_WORKLOADS = WORKLOADS


def write_jsonl(instances: Iterable[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for instance in instances:
            handle.write(json.dumps(instance, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(rows: Iterable[dict[str, Any]], path: Path) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not materialized:
        path.write_text("\n", encoding="utf-8")
        return
    preferred = [
        "instance_id",
        "workload",
        "seed",
        "variant",
        "transaction_state",
        "partial_update_rate",
        "invalid_commit_rate",
        "stale_write_rate",
        "repair_recall",
        "leak_rate",
        "supersession_consistency",
        "scope_bypass_rate",
        "latency",
        "any_violation",
        "violations",
        "committed_count",
        "operation_count",
        "repair_count",
        "oracle_version",
        "oracle_match",
        "allowed_outcome_count",
        "oracle_mismatches",
    ]
    fields = [field for field in preferred if any(field in row for row in materialized)]
    fields.extend(sorted({field for row in materialized for field in row} - set(fields)))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(materialized)


def run_suite(
    workloads: Iterable[str] = WORKLOADS,
    seeds: Iterable[int] = range(10),
    variants: Iterable[str] = VARIANTS,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    instances = generate_suite(workloads, seeds, config=config)
    for instance in instances:
        for variant in variants:
            result = run_instance(instance, variant)
            rows.append(result_row(instance, result))
    return rows


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    instances: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            instances.append(json.loads(line))
    return instances


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TxnMemBench deterministic experiment runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate workload instances")
    generate.add_argument("--out", type=Path, required=True)
    generate.add_argument("--seeds", type=int, default=10)
    generate.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS))

    run = subparsers.add_parser("run", help="replay saved workload instances")
    run.add_argument("--instances", type=Path, required=True)
    run.add_argument("--results", type=Path, required=True)
    run.add_argument("--variants", nargs="+", choices=VARIANTS, default=list(VARIANTS))

    for name, workloads, default_instances, default_results in (
        ("pilot", CORE_WORKLOADS, "pilot_instances.jsonl", "pilot_results.csv"),
        ("formal", FORMAL_WORKLOADS, "formal_instances.jsonl", "formal_results.csv"),
    ):
        command = subparsers.add_parser(name, help=f"run the {name} workload suite")
        command.add_argument("--out-dir", type=Path, default=Path("."))
        command.add_argument("--seeds", type=int, default=10 if name == "pilot" else 20)
        command.set_defaults(workloads=workloads, instances_name=default_instances, results_name=default_results)

    experiment = subparsers.add_parser("experiment", help="generate, replay, and summarize W1-W8")
    experiment.add_argument("--config", type=Path, default=Path("configs/workload_families.yaml"))
    experiment.add_argument("--out-dir", type=Path, default=Path("."))
    experiment.add_argument("--seeds", type=int, default=10)
    experiment.add_argument("--variants", nargs="+", choices=VARIANTS, default=list(VARIANTS))

    trace_replay = subparsers.add_parser(
        "trace-replay", help="adapt and replay externally supplied Agent memory traces"
    )
    trace_replay.add_argument("--events", type=Path, required=True)
    trace_replay.add_argument(
        "--adapter", choices=("normalized", "tau-bench", "appworld", "locomo"), default="normalized"
    )
    trace_replay.add_argument("--source", default="external")
    trace_replay.add_argument("--out-dir", type=Path, default=Path("."))
    trace_replay.add_argument("--holdout-fraction", type=float, default=0.2)
    trace_replay.add_argument("--seed", type=int, default=0)

    performance = subparsers.add_parser(
        "performance", help="measure local deterministic replay timing"
    )
    performance.add_argument("--out-dir", type=Path, default=Path("."))
    performance.add_argument("--seeds", type=int, default=3)
    performance.add_argument("--repetitions", type=int, default=3)

    process_smoke = subparsers.add_parser(
        "process-smoke", help="run the dependency-free process linearization smoke test"
    )
    process_smoke.add_argument("--out-dir", type=Path, default=Path("."))
    return parser


def _run_core_experiment(
    instances: list[dict[str, Any]], variants: Iterable[str], out_dir: Path
) -> int:
    rows: list[dict[str, Any]] = []
    for instance in instances:
        for variant in variants:
            rows.append(result_row(instance, run_instance(instance, variant)))
    write_jsonl(instances, out_dir / "data" / "generated_instances.jsonl")
    write_jsonl(
        [reference_outcome(instance) for instance in instances],
        out_dir / "data" / "reference_oracles.jsonl",
    )
    write_csv(rows, out_dir / "results" / "experiment_results.csv")
    summary = summarize(rows, ("workload", "variant"))
    write_summary(summary, out_dir / "results" / "summary.json")
    write_summary(
        coverage_report(instances, "TxnMem"),
        out_dir / "results" / "coverage.json",
    )
    write_summary(
        run_mutation_campaign(instances),
        out_dir / "results" / "mutation_report.json",
    )
    write_summary(
        schedule_effectiveness(instances, "Naive", random_seeds=range(10)),
        out_dir / "results" / "schedule_baseline.json",
    )
    synthetic_features = [
        extract_trace_features(instance["operations"], instance["failure_schedule"])
        for instance in instances
    ]
    realism = compare_distributions(synthetic_features, [])
    realism["trace_grounded_status"] = "not_supplied"
    write_summary(realism, out_dir / "results" / "realism.json")
    write_violation_figure(summary, out_dir / "results" / "figures" / "violation_rate.svg")
    write_repair_figure(summary, out_dir / "results" / "figures" / "repair_recall.svg")
    print(f"generated {len(instances)} instances -> {out_dir / 'data' / 'generated_instances.jsonl'}")
    print(f"wrote {len(rows)} result rows -> {out_dir / 'results' / 'experiment_results.csv'}")
    print(f"wrote summary and figures -> {out_dir / 'results'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "generate":
        instances = generate_suite(args.workloads, range(args.seeds))
        write_jsonl(instances, args.out)
        print(f"wrote {len(instances)} instances -> {args.out}")
        return 0
    if args.command == "run":
        instances = _read_jsonl(args.instances)
        rows = [
            result_row(instance, run_instance(instance, variant))
            for instance in instances
            for variant in args.variants
        ]
        write_csv(rows, args.results)
        print(f"wrote {len(rows)} result rows -> {args.results}")
        return 0
    if args.command in {"pilot", "formal"}:
        instances = generate_suite(args.workloads, range(args.seeds))
        rows = [
            result_row(instance, run_instance(instance, variant))
            for instance in instances
            for variant in VARIANTS
        ]
        write_jsonl(instances, args.out_dir / "data" / args.instances_name)
        write_csv(rows, args.out_dir / "results" / args.results_name)
        print(f"wrote {len(instances)} instances and {len(rows)} result rows -> {args.out_dir}")
        return 0
    if args.command == "experiment":
        load_workload_config(args.config)
        instances = generate_suite(WORKLOADS, range(args.seeds))
        return _run_core_experiment(instances, args.variants, args.out_dir)
    if args.command == "trace-replay":
        records = load_trace_records(args.events)
        instances = build_trace_instances(
            records,
            args.adapter,
            source=args.source,
            seed=args.seed,
        )
        train, holdout = split_holdout(records, args.holdout_fraction, seed=args.seed)
        rows = replay_trace_instances(instances, VARIANTS)
        write_jsonl(instances, args.out_dir / "data" / "trace_grounded_instances.jsonl")
        write_csv(rows, args.out_dir / "results" / "trace_replay.csv")
        trace_features = [
            extract_trace_features(instance["operations"], instance["failure_schedule"])
            for instance in instances
        ]
        realism = compare_distributions([], trace_features)
        realism["trace_grounded_status"] = "trace_supplied" if instances else "not_supplied"
        realism["trace_inventory"] = trace_inventory(instances)
        realism["calibration"] = calibrate_config(trace_features)
        realism["split"] = {
            "train_record_count": len(train),
            "holdout_record_count": len(holdout),
            "holdout_fraction": args.holdout_fraction,
            "seed": args.seed,
        }
        realism["evidence"] = trace_evidence_summary(instances, rows)
        write_summary(realism, args.out_dir / "results" / "trace_realism.json")
        print(f"adapted {len(records)} records into {len(instances)} trace instances")
        print(f"wrote trace replay artifacts -> {args.out_dir}")
        return 0
    if args.command == "performance":
        instances = generate_suite(WORKLOADS, range(args.seeds))
        performance = benchmark_replay(instances, VARIANTS, repetitions=args.repetitions)
        write_summary(performance, args.out_dir / "results" / "performance.json")
        print(f"wrote local performance benchmark -> {args.out_dir / 'results' / 'performance.json'}")
        return 0
    if args.command == "process-smoke":
        raw_report = run_process_action_sequences(
            [
                [
                    {"type": "write", "memory_id": "process_smoke_a", "value": "a"},
                    {"type": "write", "memory_id": "process_smoke_a2", "value": "a2"},
                ],
                [{"type": "write", "memory_id": "process_smoke_b", "value": "b"}],
            ]
        )
        report = {
            key: raw_report[key]
            for key in (
                "concurrency_model",
                "worker_count",
                "submitted_operation_count",
                "event_count",
                "unique_event_ids",
                "completed",
                "failed_worker_ids",
                "unacknowledged_operation_ids",
            )
        }
        report["linearization_indexes"] = [
            event["linearization_index"] for event in raw_report.get("events", [])
        ]
        report["worker_event_counts"] = dict(
            sorted(Counter(event["worker_id"] for event in raw_report.get("events", [])).items())
        )
        report["trace_ground_truth_native"] = False
        report["production_latency_claim"] = False
        write_summary(report, args.out_dir / "results" / "process_concurrency.json")
        print(f"wrote process concurrency smoke report -> {args.out_dir / 'results' / 'process_concurrency.json'}")
        return 0
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
