#!/usr/bin/env python3
"""Compatibility API and CLI for the modular TxnMemBench core."""

from __future__ import annotations

import argparse
import csv
import json
import os
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
from txnmem_distributed_protocol import run_protocol_matrix
from txnmem_backend_performance import FaultScenario, benchmark_backend, run_fault_matrix
from txnmem_service_faults import deterministic_fault_matrix
from txnmem_mutation import run_mutation_campaign
from txnmem_model_protocol import ModelResponse, OpenAICompatibleClient, ToolCall
from txnmem_performance import benchmark_replay
from txnmem_realism import (
    calibrate_config,
    compare_distributions,
    extract_trace_features,
    split_holdout,
    trace_evidence_summary,
)
from txnmem_real_experiment import (
    RealExperimentError,
    load_task_manifest,
    run_benchmark_batch,
    run_benchmark_experiment_manifest,
    run_experiment_manifest,
)
from txnmem_public_native import run_public_native_manifest
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

    backend_performance = subparsers.add_parser(
        "backend-performance", help="run backend-only timing and deterministic fault matrix"
    )
    backend_performance.add_argument("--backend", choices=("memory", "sqlite", "vector-graph"), default="sqlite")
    backend_performance.add_argument("--service-url", default="http://127.0.0.1:6333")
    backend_performance.add_argument("--fault-matrix", type=Path, default=None)
    backend_performance.add_argument("--events", type=int, nargs="+", default=[50, 200, 1000])
    backend_performance.add_argument("--repetitions", type=int, default=30)
    backend_performance.add_argument("--out-dir", type=Path, default=Path("."))

    process_smoke = subparsers.add_parser(
        "process-smoke", help="run the dependency-free process linearization smoke test"
    )
    process_smoke.add_argument("--out-dir", type=Path, default=Path("."))
    protocol_smoke = subparsers.add_parser(
        "process-protocol-smoke", help="run deterministic distributed protocol fault schedules"
    )
    protocol_smoke.add_argument("--out-dir", type=Path, default=Path("."))

    real_model = subparsers.add_parser(
        "real-model-smoke", help="run a native memory trace against a model endpoint or offline fixture"
    )
    real_model.add_argument("--manifest", type=Path, required=True)
    real_model.add_argument("--out-dir", type=Path, default=Path("."))
    real_model.add_argument("--endpoint", default=None, help="OpenAI-compatible base or completion endpoint")
    real_model.add_argument("--model", default=None, help="model id served by the endpoint")
    real_model.add_argument("--api-key-env", default="OPENAI_API_KEY")
    real_model.add_argument("--timeout", type=float, default=60.0)
    real_model.add_argument("--offline-fixture", action="store_true")
    benchmark_native = subparsers.add_parser(
        "benchmark-native-smoke", help="run a benchmark task through merged benchmark+memory tools"
    )
    benchmark_native.add_argument("--benchmark", choices=("tau-bench", "appworld", "locomo"), required=True)
    benchmark_native.add_argument("--manifest", type=Path, required=True)
    benchmark_native.add_argument("--tau-domain", default="airline", choices=("airline", "retail"))
    benchmark_native.add_argument("--tau-split", default="test", choices=("test", "train", "dev"))
    benchmark_native.add_argument(
        "--tau-user-strategy",
        default="scripted",
        choices=("scripted", "human"),
        help="tau-bench user boundary; scripted is reproducible and non-interactive",
    )
    benchmark_native.add_argument("--appworld-root", type=Path, default=Path("external_data/deps/appworld-data"))
    benchmark_native.add_argument(
        "--appworld-apps",
        default=None,
        help="comma-separated AppWorld apps to expose; use a task-specific list to bound tool context",
    )
    benchmark_native.add_argument("--out-dir", type=Path, default=Path("."))
    benchmark_native.add_argument(
        "--memory-backend",
        choices=("memory", "sqlite"),
        default="memory",
        help="memory backend for the merged benchmark+memory loop; sqlite persists state per task",
    )
    benchmark_native.add_argument("--endpoint", default=None, help="OpenAI-compatible base or completion endpoint")
    benchmark_native.add_argument("--model", default=None, help="model id served by the endpoint")
    benchmark_native.add_argument("--api-key-env", default="OPENAI_API_KEY")
    benchmark_native.add_argument("--timeout", type=float, default=60.0)
    benchmark_native.add_argument("--offline-fixture", action="store_true")
    benchmark_batch = subparsers.add_parser(
        "benchmark-native-batch", help="run a fixed public benchmark manifest with task-level aggregation"
    )
    benchmark_batch.add_argument("--benchmark", choices=("tau-bench", "appworld", "locomo"), required=True)
    benchmark_batch.add_argument("--manifest", type=Path, required=True)
    benchmark_batch.add_argument("--tau-domain", default="airline", choices=("airline", "retail"))
    benchmark_batch.add_argument("--tau-split", default="test", choices=("test", "train", "dev"))
    benchmark_batch.add_argument("--tau-user-strategy", default="scripted", choices=("scripted", "human"))
    benchmark_batch.add_argument("--appworld-root", type=Path, default=Path("external_data/deps/appworld-data"))
    benchmark_batch.add_argument("--appworld-apps", default=None)
    benchmark_batch.add_argument("--locomo-evaluator-command", default=None, help="JSON argv array for official QA evaluator")
    benchmark_batch.add_argument("--out-dir", type=Path, default=Path("."))
    benchmark_batch.add_argument("--memory-backend", choices=("memory", "sqlite"), default="sqlite")
    benchmark_batch.add_argument("--repetitions", type=int, default=1)
    benchmark_batch.add_argument("--endpoint", default=None)
    benchmark_batch.add_argument("--model", default=None)
    benchmark_batch.add_argument("--api-key-env", default="OPENAI_API_KEY")
    benchmark_batch.add_argument("--timeout", type=float, default=60.0)
    benchmark_batch.add_argument("--offline-fixture", action="store_true")
    public_native = subparsers.add_parser(
        "public-native-smoke", help="run a public workflow through the native-agent boundary"
    )
    public_native.add_argument("--dataset", choices=("tau-bench", "appworld", "locomo"), required=True)
    public_native.add_argument("--source", type=Path, required=True)
    public_native.add_argument("--limit", type=int, default=1)
    public_native.add_argument("--out-dir", type=Path, default=Path("."))
    public_native.add_argument("--endpoint", default=None)
    public_native.add_argument("--model", default=None)
    public_native.add_argument("--api-key-env", default="OPENAI_API_KEY")
    public_native.add_argument("--timeout", type=float, default=60.0)
    return parser


class _OfflineFixtureModel:
    """Deterministic protocol fixture; never reported as a real model run."""

    def __init__(self):
        self.step = 0

    def complete(self, _messages, _tools, *, seed=None, temperature=0.0):
        self.step += 1
        if self.step == 1:
            return ModelResponse(
                "",
                [ToolCall("fixture_write", "memory_write", {"memory_id": "fixture_source", "value": "generic source"})],
            )
        if self.step == 2:
            return ModelResponse(
                "",
                [
                    ToolCall(
                        "fixture_derive",
                        "memory_derive",
                        {
                            "memory_id": "fixture_derived",
                            "source_ids": ["fixture_source"],
                            "value": "generic derived",
                        },
                    )
                ],
            )
        return ModelResponse("offline fixture completed", [])


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
    if args.command == "backend-performance":
        try:
            from txnmem_backend import InstrumentedMemoryBackend, SQLiteInstrumentedMemoryBackend

            backend_counter = {"value": 0}

            def backend_factory(size=None, scenario=None):
                backend_counter["value"] += 1
                if args.backend == "memory":
                    return InstrumentedMemoryBackend()
                if args.backend == "sqlite":
                    path = args.out_dir / "data" / f"backend_perf_{backend_counter['value']:05d}.sqlite"
                    return SQLiteInstrumentedMemoryBackend(path)
                from txnmem_vector_graph_backend import VectorGraphMemoryBackend

                neo4j_uri = os.environ.get("TXNMEM_NEO4J_URI", "bolt://127.0.0.1:7687")
                neo4j_user = os.environ.get("TXNMEM_NEO4J_USER", "neo4j")
                neo4j_password = os.environ.get("TXNMEM_NEO4J_PASSWORD", "txnmem-local-only")
                return VectorGraphMemoryBackend(
                    f"perf-{backend_counter['value']:05d}",
                    args.service_url,
                    neo4j_uri,
                    (neo4j_user, neo4j_password),
                )

            performance = benchmark_backend(
                backend_factory,
                workload_sizes=args.events,
                repetitions=args.repetitions,
            )
            if args.fault_matrix is None:
                raw_scenarios = deterministic_fault_matrix(seed=17)
            else:
                raw_scenarios = json.loads(args.fault_matrix.read_text(encoding="utf-8"))
            scenarios = [
                FaultScenario(
                    name=str(item["name"]),
                    service=str(item["service"]),
                    trigger_operation=str(item["trigger_operation"]),
                    action=str(item["action"]),
                    seed=int(item.get("seed", 17)),
                )
                for item in raw_scenarios
            ]
            workload = [
                {"type": "write", "memory_id": "fault_m0", "value": "fault_v0"},
                {"type": "write", "memory_id": "fault_m1", "value": "fault_v1"},
            ]
            faults = run_fault_matrix(backend_factory, scenarios, workload, repetitions=args.repetitions)
            report = {
                "backend": args.backend,
                "service_url": args.service_url if args.backend == "vector-graph" else None,
                "performance": performance,
                "fault_matrix": faults,
                "production_latency_claim": False,
            }
            write_summary(report, args.out_dir / "results" / "backend_performance.json")
        except (OSError, ValueError, ImportError, RuntimeError) as exc:
            blocked = {
                "status": "blocked",
                "backend": args.backend,
                "reason": f"{type(exc).__name__}: {exc}",
                "production_latency_claim": False,
            }
            write_summary(blocked, args.out_dir / "results" / "backend_performance_blocked.json")
            print(f"backend performance blocked: {exc}")
            return 2
        print(f"wrote backend performance report -> {args.out_dir / 'results' / 'backend_performance.json'}")
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
    if args.command == "process-protocol-smoke":
        schedules = [
            [{"type": "prepare"}, {"type": "commit"}],
            [{"type": "prepare"}, {"type": "abort"}],
            [{"type": "prepare"}, {"type": "crash_after_prepare", "participant": "p2"}],
            [
                {"type": "prepare"},
                {"type": "network_drop", "participant": "p2", "phase": "commit"},
                {"type": "commit"},
                {"type": "retry_commit"},
                {"type": "retry_commit"},
            ],
        ]
        report = run_protocol_matrix(schedules)
        write_summary(report, args.out_dir / "results" / "process_protocol.json")
        print(f"wrote distributed protocol smoke report -> {args.out_dir / 'results' / 'process_protocol.json'}")
        return 0
    if args.command == "real-model-smoke":
        try:
            manifest, manifest_sha256 = load_task_manifest(args.manifest)
            if args.offline_fixture:
                model = _OfflineFixtureModel()
                execution_mode = "offline_fixture"
                model_id = "offline-fixture"
            else:
                if not args.endpoint or not args.model:
                    raise RealExperimentError(
                        "missing_endpoint_or_model",
                        "real endpoint mode requires --endpoint and --model",
                    )
                model = OpenAICompatibleClient(
                    args.endpoint,
                    args.model,
                    api_key=os.environ.get(args.api_key_env),
                    timeout_s=args.timeout,
                )
                execution_mode = "remote_endpoint"
                model_id = args.model
            report = run_experiment_manifest(manifest, model, args.out_dir)
            report["model_execution_mode"] = execution_mode
            report["model_id"] = model_id
            report["manifest_sha256"] = manifest_sha256
            summary_path = args.out_dir / "results" / "native_model_summary.json"
            summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        except (OSError, json.JSONDecodeError, RealExperimentError) as exc:
            print(f"real model experiment configuration error: {exc}")
            return 2
        print(f"wrote native model trace and summary -> {args.out_dir}")
        return 0
    if args.command == "benchmark-native-smoke":
        try:
            manifest, manifest_sha256 = load_task_manifest(args.manifest)
            if args.offline_fixture:
                model = _OfflineFixtureModel()
                execution_mode = "offline_fixture"
                model_id = "offline-fixture"
            else:
                if not args.endpoint or not args.model:
                    raise RealExperimentError(
                        "missing_endpoint_or_model",
                        "real endpoint mode requires --endpoint and --model",
                    )
                model = OpenAICompatibleClient(
                    args.endpoint,
                    args.model,
                    api_key=os.environ.get(args.api_key_env),
                    timeout_s=args.timeout,
                )
                execution_mode = "remote_endpoint"
                model_id = args.model
            from txnmem_benchmark_bridge import (
                AppWorldAdapter,
                LoCoMoAdapter,
                TauBenchAdapter,
                _official_tau_user_strategy,
            )

            if args.benchmark == "tau-bench":
                if args.offline_fixture:
                    from unittest import mock as _mock

                    _mock.patch(
                        "builtins.input",
                        side_effect=lambda *a, **k: "I want to book a flight. ###STOP###",
                    ).start()

                def adapter_factory(task=None):
                    from tau_bench.envs.airline.env import MockAirlineDomainEnv
                    from tau_bench.envs.retail.env import MockRetailDomainEnv

                    if args.tau_domain == "airline":
                        env_cls = MockAirlineDomainEnv
                    else:
                        env_cls = MockRetailDomainEnv
                    env = env_cls(
                        user_strategy=_official_tau_user_strategy(args.tau_user_strategy),
                        task_split=args.tau_split,
                        task_index=(task.get("task_index") if isinstance(task, dict) else None),
                    )
                    return TauBenchAdapter(
                        lambda: env,
                        task_split=args.tau_split,
                        user_strategy=args.tau_user_strategy,
                    )
            elif args.benchmark == "appworld":
                default_app_names = (
                        [name.strip() for name in args.appworld_apps.split(",") if name.strip()]
                        if args.appworld_apps
                        else None
                    )

                def adapter_factory(task=None):
                    task_app_names = task.get("app_names") if isinstance(task, dict) else None
                    task_api_allowlist = task.get("api_name_allowlist") if isinstance(task, dict) else None
                    effective_app_names = task_app_names or default_app_names
                    return AppWorldAdapter(
                        appworld_root=args.appworld_root,
                        app_names=effective_app_names,
                        api_name_allowlist=task_api_allowlist,
                    )
            else:
                def adapter_factory():
                    return LoCoMoAdapter()

            backend_factory = None
            if args.memory_backend == "sqlite":
                from txnmem_backend import SQLiteInstrumentedMemoryBackend

                def backend_factory(index: int, root: Path) -> SQLiteInstrumentedMemoryBackend:
                    return SQLiteInstrumentedMemoryBackend(root / "data" / f"memory_{index:04d}.sqlite")

            report = run_benchmark_experiment_manifest(
                manifest,
                model,
                adapter_factory,
                args.out_dir,
                backend_factory=backend_factory,
            )
            report["model_execution_mode"] = execution_mode
            report["model_id"] = model_id
            report["manifest_sha256"] = manifest_sha256
            report["benchmark"] = args.benchmark
            report["memory_backend"] = args.memory_backend
            summary_path = args.out_dir / "results" / "native_model_summary.json"
            summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        except (OSError, json.JSONDecodeError, RealExperimentError) as exc:
            print(f"benchmark native experiment configuration error: {exc}")
            return 2
        print(f"wrote benchmark native trace and summary -> {args.out_dir}")
        return 0
    if args.command == "benchmark-native-batch":
        try:
            manifest, manifest_sha256 = load_task_manifest(args.manifest)
            if args.offline_fixture:
                model = _OfflineFixtureModel()
                execution_mode = "offline_fixture"
                model_id = "offline-fixture"
            else:
                if not args.endpoint or not args.model:
                    raise RealExperimentError(
                        "missing_endpoint_or_model",
                        "real endpoint mode requires --endpoint and --model",
                    )
                model = OpenAICompatibleClient(
                    args.endpoint,
                    args.model,
                    api_key=os.environ.get(args.api_key_env),
                    timeout_s=args.timeout,
                )
                execution_mode = "remote_endpoint"
                model_id = args.model
            from txnmem_benchmark_bridge import (
                AppWorldAdapter,
                LoCoMoAdapter,
                TauBenchAdapter,
                _official_tau_user_strategy,
            )

            if args.benchmark == "tau-bench":
                def adapter_factory(task=None):
                    from tau_bench.envs.airline.env import MockAirlineDomainEnv
                    from tau_bench.envs.retail.env import MockRetailDomainEnv

                    env_cls = MockAirlineDomainEnv if args.tau_domain == "airline" else MockRetailDomainEnv
                    env = env_cls(
                        user_strategy=_official_tau_user_strategy(args.tau_user_strategy),
                        task_split=args.tau_split,
                        task_index=(task.get("task_index") if isinstance(task, dict) else None),
                    )
                    return TauBenchAdapter(
                        lambda: env,
                        task_split=args.tau_split,
                        user_strategy=args.tau_user_strategy,
                    )
            elif args.benchmark == "appworld":
                default_app_names = (
                    [name.strip() for name in args.appworld_apps.split(",") if name.strip()]
                    if args.appworld_apps
                    else None
                )

                def adapter_factory(task=None):
                    task_app_names = task.get("app_names") if isinstance(task, dict) else None
                    task_api_allowlist = task.get("api_name_allowlist") if isinstance(task, dict) else None
                    effective_app_names = task_app_names or default_app_names
                    return AppWorldAdapter(
                        appworld_root=args.appworld_root,
                        app_names=effective_app_names,
                        api_name_allowlist=task_api_allowlist,
                    )
            else:
                evaluator_command = None
                if args.locomo_evaluator_command:
                    parsed = json.loads(args.locomo_evaluator_command)
                    if not isinstance(parsed, list) or not parsed or not all(isinstance(item, str) for item in parsed):
                        raise RealExperimentError(
                            "invalid_locomo_evaluator_command",
                            "--locomo-evaluator-command must be a JSON argv array",
                        )
                    evaluator_command = parsed

                def adapter_factory():
                    return LoCoMoAdapter(evaluator_command=evaluator_command, evaluator_timeout=args.timeout)

            backend_factory = None
            if args.memory_backend == "sqlite":
                from txnmem_backend import SQLiteInstrumentedMemoryBackend

                def backend_factory(index: int, root: Path) -> SQLiteInstrumentedMemoryBackend:
                    return SQLiteInstrumentedMemoryBackend(root / "data" / f"memory_{index:04d}.sqlite")

            report = run_benchmark_batch(
                manifest,
                model,
                args.out_dir,
                backend_factory=backend_factory,
                adapter_factory=adapter_factory,
                repetitions=args.repetitions,
            )
            report["model_execution_mode"] = execution_mode
            report["model_id"] = model_id
            report["manifest_sha256"] = manifest_sha256
            report["benchmark"] = args.benchmark
            report["memory_backend"] = args.memory_backend
            summary_path = args.out_dir / "results" / "native_batch_summary.json"
            summary_path.write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except (OSError, json.JSONDecodeError, RealExperimentError, ImportError) as exc:
            print(f"benchmark native batch configuration error: {exc}")
            return 2
        print(f"wrote benchmark native batch summary -> {args.out_dir / 'results' / 'native_batch_summary.json'}")
        return 0
    if args.command == "public-native-smoke":
        model = None
        if args.endpoint and args.model:
            model = OpenAICompatibleClient(
                args.endpoint,
                args.model,
                api_key=os.environ.get(args.api_key_env),
                timeout_s=args.timeout,
            )
        report = run_public_native_manifest(
            {
                "dataset": args.dataset,
                "source": str(args.source),
                "limit": args.limit,
            },
            model,
            args.out_dir,
        )
        if report.get("status") == "blocked":
            print(f"public native run blocked: {report.get('reason')}")
        else:
            print(f"wrote public native summary -> {args.out_dir}")
        return 0
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
