#!/usr/bin/env python3
"""Compatibility API and CLI for the modular TxnMemBench core."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
from collections import Counter
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Iterable

from txnmem_invariants import check_invariants
from txnmem_metrics import (
    result_row,
    summarize,
    write_repair_figure,
    write_saturation_figure,
    write_summary,
    write_violation_figure,
)
from txnmem_coverage import coverage_report
from txnmem_coverage import schedule_effectiveness
from txnmem_distributed import run_process_action_sequences
from txnmem_distributed_protocol import run_protocol_matrix
from txnmem_backend_performance import FaultScenario, benchmark_backend, run_fault_matrix
from txnmem_benchmark_bridge import APPWORLD_TOOL_STRATEGIES
from txnmem_conditions import (
    canonical_fingerprint,
    file_sha256,
    source_identity,
    verify_git_source_containment,
)
from txnmem_service_faults import ToxiproxyFaultController, deterministic_fault_matrix
from txnmem_mutation import (
    build_minimal_mutant_witnesses,
    run_mutation_campaign,
    validate_minimal_mutant_witnesses,
)
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
from txnmem_reference import ORACLE_VERSION, reference_outcome
from txnmem_schema import DEFAULT_CONFIG, load_workload_config
from txnmem_simulator import VARIANTS, run_instance
from txnmem_trace_pipeline import (
    build_trace_instances,
    load_trace_records,
    replay_trace_instances,
    trace_inventory,
)
from txnmem_workloads import WORKLOADS, generate_instance, generate_suite
from txnmem_statistics import controlled_diversity, controlled_violation_saturation


CORE_WORKLOADS = (
    "atomic_multi_write",
    "revoke_before_commit",
    "provenance_chain_repair",
)
FORMAL_WORKLOADS = WORKLOADS


def _benchmark_runtime_version(benchmark: str) -> str:
    distribution = {
        "appworld": "appworld",
        "tau-bench": "tau-bench",
    }.get(benchmark)
    if distribution is None:
        return "repository_source"
    try:
        return package_version(distribution)
    except PackageNotFoundError:
        return "unknown"


def _paired_benchmark_condition(
    *,
    benchmark: str,
    manifest_sha256: str,
    model_id: str,
    model_execution_mode: str,
    memory_backend: str,
    repetitions: int,
    max_tokens: int | None,
    timeout_seconds: float,
    model_revision: str,
    model_server_build: str,
    appworld_tool_strategy: str,
) -> dict[str, object]:
    import txnmem_benchmark_bridge as benchmark_bridge_module
    import txnmem_model_protocol as model_protocol_module
    import txnmem_real_experiment as real_experiment_module

    source_paths: dict[str, Path] = {
        "txnmem_experiment": Path(__file__),
        "txnmem_benchmark_bridge": Path(benchmark_bridge_module.__file__),
        "txnmem_model_protocol": Path(model_protocol_module.__file__),
        "txnmem_real_experiment": Path(real_experiment_module.__file__),
    }
    if benchmark == "appworld":
        try:
            import appworld.common.evaluation as appworld_common_evaluation_module
            import appworld.environment as appworld_environment_module
            import appworld.evaluator as appworld_evaluator_module
        except ImportError:
            pass
        else:
            source_paths["appworld_environment"] = Path(
                appworld_environment_module.__file__
            )
            source_paths["appworld_evaluator"] = Path(
                appworld_evaluator_module.__file__
            )
            source_paths["appworld_common_evaluation"] = Path(
                appworld_common_evaluation_module.__file__
            )
    return {
        "benchmark": benchmark,
        "manifest_sha256": manifest_sha256,
        "model_id": model_id,
        "model_revision": str(model_revision),
        "model_revision_status": (
            "sha256"
            if len(str(model_revision)) == 64
            and all(character in "0123456789abcdefABCDEF" for character in str(model_revision))
            else "unspecified_or_non_hash"
        ),
        "model_server_build": str(model_server_build),
        "runner_evaluator_source_identity": source_identity(source_paths),
        "model_execution_mode": model_execution_mode,
        "memory_backend": memory_backend,
        "repetitions": int(repetitions),
        "max_tokens": max_tokens,
        "timeout_seconds": float(timeout_seconds),
        "generation_parameters": "seed_temperature_and_max_steps_from_fixed_manifest",
        "official_evaluator": (
            "appworld.TestTracker.success_and_task_completed"
            if benchmark == "appworld"
            else f"{benchmark}_official_runtime"
        ),
        "runtime_version": _benchmark_runtime_version(benchmark),
        "appworld_model_tool_strategy": (
            appworld_tool_strategy if benchmark == "appworld" else "not_applicable"
        ),
    }


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


def _seed_range(seed_count: int) -> range:
    if seed_count <= 0:
        raise ValueError("seed count must be positive")
    return range(seed_count)


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
    experiment.add_argument("--require-clean-source", action="store_true")

    mutation_witnesses = subparsers.add_parser(
        "mutation-witnesses",
        help="derive and replay one prefix-minimal witness for each major mutant",
    )
    mutation_witnesses.add_argument("--instances", type=Path, required=True)
    mutation_witnesses.add_argument("--out", type=Path, required=True)

    trace_replay = subparsers.add_parser(
        "trace-replay", help="adapt and replay externally supplied Agent memory traces"
    )
    trace_replay.add_argument("--events", type=Path, required=True)
    trace_replay.add_argument(
        "--synthetic-instances",
        type=Path,
        default=None,
        help="optional generated-instance JSONL for feature-distribution comparison",
    )
    trace_replay.add_argument(
        "--adapter", choices=("normalized", "tau-bench", "appworld", "locomo"), default="normalized"
    )
    trace_replay.add_argument("--source", default="external")
    trace_replay.add_argument("--out-dir", type=Path, default=Path("."))
    trace_replay.add_argument("--holdout-fraction", type=float, default=0.2)
    trace_replay.add_argument("--seed", type=int, default=0)
    trace_replay.add_argument("--bootstrap-repetitions", type=int, default=2000)
    trace_replay.add_argument("--joint-permutations", type=int, default=999)
    trace_replay.add_argument("--joint-rff-dimensions", type=int, default=64)

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
    real_model.add_argument("--max-tokens", type=int, default=1024)
    real_model.add_argument("--offline-fixture", action="store_true")
    model_load = subparsers.add_parser(
        "real-model-load",
        help="run concurrent real-model Agent cycles with endpoint token accounting",
    )
    model_load.add_argument("--manifest", type=Path, required=True)
    model_load.add_argument("--out-dir", type=Path, default=Path("."))
    model_load.add_argument("--endpoint", required=True)
    model_load.add_argument("--model", required=True)
    model_load.add_argument("--model-revision", default="unspecified")
    model_load.add_argument("--model-server-build", default="unknown")
    model_load.add_argument("--api-key-env", default="OPENAI_API_KEY")
    model_load.add_argument("--timeout", type=float, default=180.0)
    model_load.add_argument("--max-tokens", type=int, default=1024)
    model_load.add_argument("--max-steps", type=int, default=12)
    model_load.add_argument("--concurrency", type=int, default=4)
    model_load.add_argument("--minimum-cycles", type=int, default=1)
    model_load.add_argument("--minimum-duration-seconds", type=float, default=0.0)
    model_load.add_argument(
        "--execution-scope",
        choices=("single_host_multi_agent", "cross_host_client_server"),
        default="single_host_multi_agent",
    )
    model_load.add_argument("--host-count", type=int, default=1)
    model_load.add_argument("--network-transport", default="loopback_or_unspecified")
    model_load.add_argument("--tunnel-process-id", type=int, default=None)
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
    benchmark_native.add_argument(
        "--appworld-tool-strategy",
        choices=APPWORLD_TOOL_STRATEGIES,
        default="manifest_scoped",
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
    benchmark_native.add_argument("--model-revision", default="unspecified")
    benchmark_native.add_argument("--model-server-build", default="unknown")
    benchmark_native.add_argument("--api-key-env", default="OPENAI_API_KEY")
    benchmark_native.add_argument("--timeout", type=float, default=60.0)
    benchmark_native.add_argument("--max-tokens", type=int, default=1024)
    benchmark_native.add_argument(
        "--prompt-profile", choices=("baseline", "tuned"), default="baseline"
    )
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
    benchmark_batch.add_argument(
        "--appworld-tool-strategy",
        choices=APPWORLD_TOOL_STRATEGIES,
        default="manifest_scoped",
    )
    benchmark_batch.add_argument("--locomo-evaluator-command", default=None, help="JSON argv array for official QA evaluator")
    benchmark_batch.add_argument("--out-dir", type=Path, default=Path("."))
    benchmark_batch.add_argument("--memory-backend", choices=("memory", "sqlite"), default="sqlite")
    benchmark_batch.add_argument("--repetitions", type=int, default=1)
    benchmark_batch.add_argument("--endpoint", default=None)
    benchmark_batch.add_argument("--model", default=None)
    benchmark_batch.add_argument("--model-revision", default="unspecified")
    benchmark_batch.add_argument("--model-server-build", default="unknown")
    benchmark_batch.add_argument("--api-key-env", default="OPENAI_API_KEY")
    benchmark_batch.add_argument("--timeout", type=float, default=60.0)
    benchmark_batch.add_argument("--max-tokens", type=int, default=1024)
    benchmark_batch.add_argument(
        "--prompt-profile", choices=("baseline", "tuned"), default="baseline"
    )
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


_CONTROLLED_SOURCE_PATHS = (
    "configs/workload_families.yaml",
    "src/txnmem_conditions.py",
    "src/txnmem_differential.py",
    "src/txnmem_experiment.py",
    "src/txnmem_invariants.py",
    "src/txnmem_metrics.py",
    "src/txnmem_reference.py",
    "src/txnmem_schedules.py",
    "src/txnmem_schema.py",
    "src/txnmem_simulator.py",
    "src/txnmem_statistics.py",
    "src/txnmem_workloads.py",
)


def _controlled_source_manifest(config_path: Path) -> tuple[dict[str, Any], str | None]:
    source_root = Path(__file__).resolve().parent
    repository_root = source_root.parent
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    commit = completed.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ValueError("controlled evidence requires a full lowercase Git source commit")
    try:
        config_relative = config_path.resolve().relative_to(repository_root.resolve()).as_posix()
    except ValueError:
        config_relative = None
    source_paths = [path for path in _CONTROLLED_SOURCE_PATHS if path != "configs/workload_families.yaml"]
    if config_relative is not None:
        source_paths.append(config_relative)
    components = {
        path: file_sha256(repository_root / path)
        for path in sorted(set(source_paths))
        if (repository_root / path).is_file()
    }
    containment = verify_git_source_containment(repository_root, commit, components)
    expected_paths = set(source_paths)
    contained = (
        config_relative is not None
        and containment["contained_in_commit"]
        and set(components) == expected_paths
    )
    return ({
        "commit": commit,
        "components": components,
        "fingerprint": canonical_fingerprint(components),
        "contained_in_commit": contained,
    }, config_relative)


def _controlled_artifact_entry(path: Path, relative_path: str, **counts: int) -> dict[str, Any]:
    return {
        "relative_path": relative_path,
        "sha256": file_sha256(path),
        **counts,
    }


def _run_core_experiment(
    instances: list[dict[str, Any]],
    variants: Iterable[str],
    out_dir: Path,
    *,
    config_path: Path,
    workload_config: dict[str, Any],
    source_manifest: dict[str, Any],
    config_relative_path: str | None,
) -> int:
    variant_domain = list(variants)
    rows: list[dict[str, Any]] = []
    for instance in instances:
        for variant in variant_domain:
            rows.append(result_row(instance, run_instance(instance, variant)))
    instances_path = out_dir / "data" / "generated_instances.jsonl"
    oracles_path = out_dir / "data" / "reference_oracles.jsonl"
    results_path = out_dir / "results" / "experiment_results.csv"
    saturation_path = out_dir / "results" / "saturation.json"
    diversity_path = out_dir / "results" / "diversity.json"
    saturation_figure_path = out_dir / "results" / "figures" / "saturation.svg"
    write_jsonl(instances, instances_path)
    write_jsonl(
        [reference_outcome(instance) for instance in instances],
        oracles_path,
    )
    write_csv(rows, results_path)
    seed_count = len({int(instance["seed"]) for instance in instances})
    checkpoints = [value for value in (10, 25, 50, 100, 150, 200) if value <= seed_count]
    if not checkpoints or checkpoints[-1] != seed_count:
        checkpoints.append(seed_count)
    saturation = controlled_violation_saturation(
        rows,
        checkpoints,
        approved_variants=variant_domain,
    )
    diversity = controlled_diversity(instances)
    write_summary(saturation, saturation_path)
    write_summary(diversity, diversity_path)
    write_saturation_figure(saturation, saturation_figure_path)
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
    family_domain = sorted({str(instance["workload"]) for instance in instances})
    seed_domain = sorted({int(instance["seed"]) for instance in instances})
    manifest = {
        "schema_version": 1,
        "runner_version": "controlled-experiment/1",
        "source": source_manifest,
        "oracle_version": ORACLE_VERSION,
        "config": {
            "relative_path": config_relative_path,
            "sha256": file_sha256(config_path),
            "canonical_fingerprint": canonical_fingerprint(workload_config),
        },
        "domains": {
            "families": family_domain,
            "seeds": seed_domain,
            "variants": variant_domain,
        },
        "counts": {
            "families": len(family_domain),
            "seeds_per_family": len(seed_domain),
            "instances": len(instances),
            "variant_results": len(rows),
        },
        "artifacts": {
            "generated_instances.jsonl": _controlled_artifact_entry(
                instances_path, "data/generated_instances.jsonl", line_count=len(instances)
            ),
            "reference_oracles.jsonl": _controlled_artifact_entry(
                oracles_path, "data/reference_oracles.jsonl", line_count=len(instances)
            ),
            "experiment_results.csv": _controlled_artifact_entry(
                results_path, "results/experiment_results.csv", row_count=len(rows)
            ),
            "saturation.json": _controlled_artifact_entry(
                saturation_path, "results/saturation.json"
            ),
            "diversity.json": _controlled_artifact_entry(
                diversity_path, "results/diversity.json"
            ),
            "saturation.svg": _controlled_artifact_entry(
                saturation_figure_path, "results/figures/saturation.svg"
            ),
        },
    }
    write_summary(manifest, out_dir / "run_manifest.json")
    print(f"generated {len(instances)} instances -> {out_dir / 'data' / 'generated_instances.jsonl'}")
    print(f"wrote {len(rows)} result rows -> {out_dir / 'results' / 'experiment_results.csv'}")
    print(f"wrote summary and figures -> {out_dir / 'results'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "generate":
        instances = generate_suite(args.workloads, _seed_range(args.seeds))
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
        instances = generate_suite(args.workloads, _seed_range(args.seeds))
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
        workload_config = load_workload_config(args.config)
        source_manifest, config_relative_path = _controlled_source_manifest(args.config)
        if args.require_clean_source and not source_manifest["contained_in_commit"]:
            raise ValueError("formal controlled evidence source is not contained in the declared commit")
        instances = generate_suite(
            WORKLOADS,
            _seed_range(args.seeds),
            parameter_ranges=workload_config.get("parameter_ranges"),
        )
        return _run_core_experiment(
            instances,
            args.variants,
            args.out_dir,
            config_path=args.config,
            workload_config=workload_config,
            source_manifest=source_manifest,
            config_relative_path=config_relative_path,
        )
    if args.command == "mutation-witnesses":
        instances = _read_jsonl(args.instances)
        report = build_minimal_mutant_witnesses(instances)
        validate_minimal_mutant_witnesses(report)
        report["source_instances_path"] = str(args.instances)
        report["source_instances_sha256"] = hashlib.sha256(
            args.instances.read_bytes()
        ).hexdigest()
        report["validation"] = {
            "status": "passed",
            "method": "replay_target_violation_and_one_step_shorter_prefix",
        }
        write_summary(report, args.out)
        print(f"wrote {report['witness_count']} minimal mutant witnesses -> {args.out}")
        return 0
    if args.command == "trace-replay":
        records = load_trace_records(args.events)
        train_records, holdout_records = split_holdout(
            records, args.holdout_fraction, seed=args.seed
        )
        train_instances = build_trace_instances(
            train_records,
            args.adapter,
            source=args.source,
            seed=args.seed,
        )
        holdout_instances = build_trace_instances(
            holdout_records,
            args.adapter,
            source=args.source,
            seed=args.seed + 100000,
        )
        for instance in train_instances:
            instance.setdefault("trace_metadata", {})["split"] = "train"
        for instance in holdout_instances:
            instance.setdefault("trace_metadata", {})["split"] = "holdout"
        instances = [*train_instances, *holdout_instances]
        rows = replay_trace_instances(instances, VARIANTS)
        write_jsonl(instances, args.out_dir / "data" / "trace_grounded_instances.jsonl")
        write_csv(rows, args.out_dir / "results" / "trace_replay.csv")
        calibration_features = [
            extract_trace_features(instance["operations"], instance["failure_schedule"])
            for instance in train_instances
        ]
        holdout_features = [
            extract_trace_features(instance["operations"], instance["failure_schedule"])
            for instance in holdout_instances
        ]
        synthetic_features = []
        if args.synthetic_instances is not None:
            synthetic_instances = _read_jsonl(args.synthetic_instances)
            synthetic_features = [
                extract_trace_features(
                    instance.get("operations", []), instance.get("failure_schedule", [])
                )
                for instance in synthetic_instances
            ]
        realism = compare_distributions(
            synthetic_features,
            holdout_features,
            bootstrap_repetitions=args.bootstrap_repetitions,
            joint_test_permutations=args.joint_permutations,
            joint_test_dimensions=args.joint_rff_dimensions,
            seed=args.seed,
        )
        realism["trace_grounded_status"] = "trace_supplied" if instances else "not_supplied"
        realism["synthetic_source"] = str(args.synthetic_instances) if args.synthetic_instances else None
        realism["joint_feature_comparison"] = (
            realism["multivariate_test"]["status"] == "available"
        )
        realism["trace_inventory"] = trace_inventory(instances)
        realism["train_trace_inventory"] = trace_inventory(train_instances)
        realism["holdout_trace_inventory"] = trace_inventory(holdout_instances)
        realism["calibration"] = calibrate_config(calibration_features)
        realism["calibration"]["source_instance_count"] = len(train_instances)
        realism["comparison_split"] = "holdout_only"
        realism["split"] = {
            "train_record_count": len(train_records),
            "holdout_record_count": len(holdout_records),
            "train_instance_count": len(train_instances),
            "holdout_instance_count": len(holdout_instances),
            "calibration_instance_count": len(train_instances),
            "test_instance_count": len(holdout_instances),
            "unit": "episode",
            "holdout_fraction": args.holdout_fraction,
            "seed": args.seed,
        }
        realism["evidence"] = trace_evidence_summary(instances, rows)
        holdout_ids = {str(instance.get("instance_id")) for instance in holdout_instances}
        holdout_rows = [
            row for row in rows if str(row.get("instance_id")) in holdout_ids
        ]
        realism["holdout_evidence"] = trace_evidence_summary(
            holdout_instances, holdout_rows
        )
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

            toxiproxy_url = os.environ.get(
                "TXNMEM_TOXIPROXY_URL", "http://127.0.0.1:8474"
            )

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
                proxy_routes = {
                    "qdrant": {
                        "proxy_name": os.environ.get(
                            "TXNMEM_QDRANT_PROXY_NAME", "txnmem-qdrant"
                        ),
                        "client_endpoint": args.service_url,
                        "listen": os.environ.get(
                            "TXNMEM_QDRANT_PROXY_LISTEN", "0.0.0.0:19000"
                        ),
                        "upstream": os.environ.get(
                            "TXNMEM_QDRANT_PROXY_UPSTREAM", "qdrant:6333"
                        ),
                    },
                    "neo4j": {
                        "proxy_name": os.environ.get(
                            "TXNMEM_NEO4J_PROXY_NAME", "txnmem-neo4j"
                        ),
                        "client_endpoint": neo4j_uri,
                        "listen": os.environ.get(
                            "TXNMEM_NEO4J_PROXY_LISTEN", "0.0.0.0:19001"
                        ),
                        "upstream": os.environ.get(
                            "TXNMEM_NEO4J_PROXY_UPSTREAM", "neo4j:7687"
                        ),
                    },
                }
                controller = None
                if scenario is not None:
                    controller = ToxiproxyFaultController(
                        scenario.as_dict(),
                        management_url=toxiproxy_url,
                        proxy_routes=proxy_routes,
                    )
                return VectorGraphMemoryBackend(
                    f"perf-{backend_counter['value']:05d}",
                    args.service_url,
                    neo4j_uri,
                    (neo4j_user, neo4j_password),
                    proxy_requester=controller,
                    max_retries=0 if scenario is not None else 1,
                    request_timeout_seconds=2.0 if scenario is not None else 15.0,
                )

            backend_health = None
            if args.backend == "vector-graph":
                health_backend = backend_factory(size=0)
                try:
                    backend_health = health_backend.healthcheck()
                finally:
                    health_backend.close()
                if not all(
                    bool(backend_health.get(service, {}).get("available"))
                    for service in ("qdrant", "neo4j")
                ):
                    raise RuntimeError("vector/graph backend healthcheck failed")

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
                    recovery_action=str(item.get("recovery_action", "abort")),
                    trigger_ordinal=int(item.get("trigger_ordinal", 1)),
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
                "backend_health": backend_health,
                "toxiproxy": {
                    "management_url": toxiproxy_url,
                    "qdrant_proxy_name": os.environ.get(
                        "TXNMEM_QDRANT_PROXY_NAME", "txnmem-qdrant"
                    ),
                    "neo4j_proxy_name": os.environ.get(
                        "TXNMEM_NEO4J_PROXY_NAME", "txnmem-neo4j"
                    ),
                }
                if args.backend == "vector-graph"
                else None,
                "production_latency_claim": False,
            }
            write_summary(report, args.out_dir / "results" / "backend_performance.json")
            if args.backend == "vector-graph" and not faults.get(
                "all_scenarios_evidence_valid", False
            ):
                print("backend fault evidence invalid: one or more scenarios did not traverse Toxiproxy")
                return 2
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
                    max_tokens=args.max_tokens,
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
    if args.command == "real-model-load":
        try:
            manifest, manifest_sha256 = load_task_manifest(args.manifest)
            model = OpenAICompatibleClient(
                args.endpoint,
                args.model,
                api_key=os.environ.get(args.api_key_env),
                timeout_s=args.timeout,
                max_tokens=args.max_tokens,
            )
            from txnmem_model_load import run_model_load

            report = run_model_load(
                manifest,
                model,
                args.out_dir,
                concurrency=args.concurrency,
                minimum_cycles=args.minimum_cycles,
                minimum_duration_s=args.minimum_duration_seconds,
                max_steps=args.max_steps,
                execution_scope=args.execution_scope,
                host_count=args.host_count,
                network_transport=args.network_transport,
                tunnel_process_id=args.tunnel_process_id,
                model_revision=args.model_revision,
                model_server_build=args.model_server_build,
            )
            report["manifest_sha256"] = manifest_sha256
            report["model_execution_mode"] = "remote_endpoint"
            summary_path = args.out_dir / "results" / "model_load_summary.json"
            summary_path.write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except (OSError, json.JSONDecodeError, RealExperimentError, ValueError) as exc:
            print(f"real model load configuration error: {exc}")
            return 2
        print(f"wrote real model load summary -> {args.out_dir / 'results' / 'model_load_summary.json'}")
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
                    max_tokens=args.max_tokens,
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
                    return AppWorldAdapter(
                        appworld_root=args.appworld_root,
                        tool_strategy=args.appworld_tool_strategy,
                        supplied_app_names=task_app_names or default_app_names,
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
                {
                    **manifest,
                    "tasks": [
                        {**task, "prompt_profile": args.prompt_profile}
                        for task in manifest["tasks"]
                    ],
                },
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
            report["prompt_profile"] = args.prompt_profile
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
                    max_tokens=args.max_tokens,
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
                    return AppWorldAdapter(
                        appworld_root=args.appworld_root,
                        tool_strategy=args.appworld_tool_strategy,
                        supplied_app_names=task_app_names or default_app_names,
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
                {
                    **manifest,
                    "tasks": [
                        {**task, "prompt_profile": args.prompt_profile}
                        for task in manifest["tasks"]
                    ],
                },
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
            report["prompt_profile"] = args.prompt_profile
            condition = _paired_benchmark_condition(
                benchmark=args.benchmark,
                manifest_sha256=manifest_sha256,
                model_id=model_id,
                model_execution_mode=execution_mode,
                memory_backend=args.memory_backend,
                repetitions=args.repetitions,
                max_tokens=args.max_tokens,
                timeout_seconds=args.timeout,
                model_revision=args.model_revision,
                model_server_build=args.model_server_build,
                appworld_tool_strategy=args.appworld_tool_strategy,
            )
            report["condition"] = condition
            report["condition_fingerprint"] = canonical_fingerprint(condition)
            report["treatment"] = {
                "prompt_profile": args.prompt_profile,
                "trusted_preflight_enabled": (
                    args.benchmark == "appworld" and args.prompt_profile == "tuned"
                ),
                "app_tool_strategy": (
                    args.appworld_tool_strategy
                    if args.benchmark == "appworld"
                    else "benchmark_default_tools"
                ),
            }
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
