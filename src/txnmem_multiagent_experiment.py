"""Controlled exhaustive and seeded multi-agent correctness experiments."""

from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from txnmem_metrics import result_row, write_summary
from txnmem_multiagent_invariants import MULTI_AGENT_VIOLATION_ORDER
from txnmem_multiagent_workloads import (
    MULTI_AGENT_WORKLOADS,
    exhaustive_multi_agent_instances,
    generate_multi_agent_suite,
)
from txnmem_mutation import (
    MULTI_AGENT_MUTANTS,
    build_minimal_mutant_witnesses,
    run_mutation_campaign,
    validate_minimal_mutant_witnesses,
)
from txnmem_reference import reference_outcome
from txnmem_schema import validate_instance
from txnmem_simulator import VARIANTS, run_instance
from txnmem_statistics import binomial_interval


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_unique_signatures(instances: list[dict[str, Any]]) -> None:
    signatures: dict[str, set[str]] = defaultdict(set)
    for instance in instances:
        validate_instance(instance)
        workload = str(instance["workload"])
        signature = str(instance.get("structure_signature", ""))
        if signature in signatures[workload]:
            raise ValueError(
                f"duplicate structure signature in {workload}: {signature}"
            )
        signatures[workload].add(signature)


def _execute_once(
    seed_count: int,
    variants: tuple[str, ...],
) -> dict[str, Any]:
    instances = generate_multi_agent_suite(range(seed_count))
    _validate_unique_signatures(instances)
    oracles = [reference_outcome(copy.deepcopy(instance)) for instance in instances]
    rows = [
        result_row(
            instance,
            run_instance(copy.deepcopy(instance), variant),
        )
        for instance in instances
        for variant in variants
    ]

    exhaustive_instances = exhaustive_multi_agent_instances()
    for instance in exhaustive_instances:
        validate_instance(instance)
    exhaustive_oracles = [
        reference_outcome(copy.deepcopy(instance))
        for instance in exhaustive_instances
    ]
    exhaustive_rows = [
        result_row(
            instance,
            run_instance(copy.deepcopy(instance), variant),
        )
        for instance in exhaustive_instances
        for variant in variants
    ]
    return {
        "instances": instances,
        "oracles": oracles,
        "rows": rows,
        "exhaustive_instances": exhaustive_instances,
        "exhaustive_oracles": exhaustive_oracles,
        "exhaustive_rows": exhaustive_rows,
    }


def _rate(successes: int, trials: int) -> dict[str, Any]:
    return {
        "count": int(successes),
        "total": int(trials),
        "interval_95": binomial_interval(successes, trials),
    }


def _summarize_tier(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[f"{row['workload']}/{row['variant']}"].append(row)
    output: dict[str, Any] = {}
    for key, records in sorted(groups.items()):
        violation_counts = Counter()
        for row in records:
            violation_counts.update(
                name for name in str(row.get("violations", "")).split(";") if name
            )
        output[key] = {
            "workload": records[0]["workload"],
            "variant": records[0]["variant"],
            "observation_count": len(records),
            "violations": _rate(
                sum(int(row.get("any_violation", 0)) for row in records),
                len(records),
            ),
            "oracle_matches": _rate(
                sum(int(row.get("oracle_match", 0)) for row in records),
                len(records),
            ),
            "violation_counts": {
                name: int(violation_counts.get(name, 0))
                for name in MULTI_AGENT_VIOLATION_ORDER
            },
        }
    return output


def _coverage(
    instances: list[dict[str, Any]],
    exhaustive_instances: list[dict[str, Any]],
    mutation_report: dict[str, Any],
) -> dict[str, Any]:
    parameters: dict[str, dict[str, list[Any]]] = {}
    for workload in MULTI_AGENT_WORKLOADS:
        values: dict[str, set[str]] = defaultdict(set)
        family = [item for item in instances if item["workload"] == workload]
        for instance in family:
            for name, value in instance["config"]["sampled_parameters"].items():
                values[name].add(json.dumps(value, ensure_ascii=False, sort_keys=True))
        parameters[workload] = {
            name: [json.loads(value) for value in sorted(encoded_values)]
            for name, encoded_values in sorted(values.items())
        }
    exhaustive_counts = Counter(
        str(instance["workload"]) for instance in exhaustive_instances
    )
    return {
        "schema_version": 1,
        "workload_families": list(MULTI_AGENT_WORKLOADS),
        "seeded_instance_count": len(instances),
        "unique_signature_count": len(
            {instance["structure_signature"] for instance in instances}
        ),
        "parameter_values": parameters,
        "exhaustive_interleaving_count": len(exhaustive_instances),
        "exhaustive_interleavings_by_family": dict(sorted(exhaustive_counts.items())),
        "invariants": list(MULTI_AGENT_VIOLATION_ORDER),
        "mutants": {
            name: {
                "target_violation": MULTI_AGENT_MUTANTS[name],
                **mutation_report.get("mutants", {}).get(name, {}),
            }
            for name in MULTI_AGENT_MUTANTS
        },
    }


def _full_counts(rows: list[dict[str, Any]]) -> tuple[int, int]:
    full = [row for row in rows if row.get("variant") == "TxnMem"]
    return (
        sum(int(row.get("any_violation", 0)) for row in full),
        sum(1 - int(row.get("oracle_match", 0)) for row in full),
    )


def run_multi_agent_controlled(
    out_dir: Path,
    seeds: int = 50,
    replications: int = 3,
    variants: Iterable[str] = VARIANTS,
) -> dict[str, Any]:
    """Run paired multi-agent conditions and write auditable artifacts."""

    if isinstance(seeds, bool) or not isinstance(seeds, int) or not 1 <= seeds <= 50:
        raise ValueError("seeds must be an integer in [1, 50]")
    if (
        isinstance(replications, bool)
        or not isinstance(replications, int)
        or replications < 1
    ):
        raise ValueError("replications must be a positive integer")
    selected_variants = tuple(variants)
    if not selected_variants or len(set(selected_variants)) != len(selected_variants):
        raise ValueError("variants must be a non-empty list without duplicates")
    unknown = set(selected_variants) - set(VARIANTS)
    if unknown:
        raise ValueError(f"unsupported variants: {sorted(unknown)}")

    executions = [
        _execute_once(seeds, selected_variants) for _ in range(replications)
    ]
    primary = executions[0]
    replication_records = [
        {
            "replication": index,
            "seeded_input_sha256": _canonical_sha256(execution["instances"]),
            "seeded_oracle_sha256": _canonical_sha256(execution["oracles"]),
            "seeded_results_sha256": _canonical_sha256(execution["rows"]),
            "exhaustive_input_sha256": _canonical_sha256(
                execution["exhaustive_instances"]
            ),
            "exhaustive_results_sha256": _canonical_sha256(
                execution["exhaustive_rows"]
            ),
        }
        for index, execution in enumerate(executions, start=1)
    ]
    reproducible = all(
        {
            key: value
            for key, value in record.items()
            if key != "replication"
        }
        == {
            key: value
            for key, value in replication_records[0].items()
            if key != "replication"
        }
        for record in replication_records[1:]
    )
    reproducibility = {
        "schema_version": 1,
        "replication_count": replications,
        "all_hashes_identical": reproducible,
        "replications": replication_records,
    }
    if not reproducible:
        raise ValueError("controlled experiment replication hashes differ")

    instances = primary["instances"]
    rows = primary["rows"]
    exhaustive_instances = primary["exhaustive_instances"]
    exhaustive_rows = primary["exhaustive_rows"]
    mutation_report = run_mutation_campaign(instances)
    witness_catalog = generate_multi_agent_suite(range(50))
    witnesses = build_minimal_mutant_witnesses(witness_catalog)
    validate_minimal_mutant_witnesses(witnesses)
    coverage = _coverage(instances, exhaustive_instances, mutation_report)
    summary = {
        "schema_version": 1,
        "seeded": _summarize_tier(rows),
        "exhaustive": _summarize_tier(exhaustive_rows),
    }

    seeded_violations, seeded_mismatches = _full_counts(rows)
    exhaustive_violations, exhaustive_mismatches = _full_counts(exhaustive_rows)
    controlled_gates = {
        "seeded_exact_count": len(instances) == len(MULTI_AGENT_WORKLOADS) * seeds,
        "seeded_unique_signatures": coverage["unique_signature_count"]
        == len(instances),
        "full_txnmem_seeded_zero_violations": seeded_violations == 0,
        "full_txnmem_seeded_oracle_match": seeded_mismatches == 0,
        "full_txnmem_exhaustive_zero_violations": exhaustive_violations == 0,
        "full_txnmem_exhaustive_oracle_match": exhaustive_mismatches == 0,
        "all_multi_agent_mutants_have_witnesses": set(witnesses["witnesses"])
        == set(MULTI_AGENT_MUTANTS),
        "replication_hashes_identical": reproducible,
    }
    controlled_gates_passed = all(controlled_gates.values())
    report = {
        "schema_version": 1,
        "seeded_instance_count": len(instances),
        "seeded_observation_count": len(rows),
        "exhaustive_instance_count": len(exhaustive_instances),
        "exhaustive_observation_count": len(exhaustive_rows),
        "replication_count": replications,
        "variant_count": len(selected_variants),
        "variants": list(selected_variants),
        "full_txnmem_seeded_violations": seeded_violations,
        "full_txnmem_seeded_oracle_mismatches": seeded_mismatches,
        "full_txnmem_exhaustive_violations": exhaustive_violations,
        "full_txnmem_exhaustive_oracle_mismatches": exhaustive_mismatches,
        "controlled_gates": controlled_gates,
        "controlled_gates_passed": controlled_gates_passed,
    }
    claim_status = {
        "schema_version": 1,
        "status": "inactive",
        "claim": (
            "In the defined operation model and covered fault schedules, "
            "TxnMem preserves transactional, actor-policy, and cross-agent "
            "provenance consistency for shared memory."
        ),
        "controlled_only": True,
        "gates": {**controlled_gates, "real_storage_evidence": False},
        "failed_gates": ["real_storage_evidence"],
    }

    from txnmem_experiment import write_csv, write_jsonl

    root = Path(out_dir)
    write_jsonl(instances, root / "data" / "generated_instances.jsonl")
    write_jsonl(primary["oracles"], root / "data" / "reference_oracles.jsonl")
    write_jsonl(
        exhaustive_instances,
        root / "data" / "exhaustive_interleavings.jsonl",
    )
    write_jsonl(
        primary["exhaustive_oracles"],
        root / "data" / "exhaustive_reference_oracles.jsonl",
    )
    write_csv(rows, root / "results" / "experiment_results.csv")
    write_csv(exhaustive_rows, root / "results" / "exhaustive_results.csv")
    write_summary(report, root / "results" / "controlled_report.json")
    write_summary(summary, root / "results" / "summary.json")
    write_summary(coverage, root / "results" / "coverage.json")
    write_summary(mutation_report, root / "results" / "mutation_report.json")
    write_summary(witnesses, root / "results" / "minimal_witnesses.json")
    write_summary(reproducibility, root / "results" / "reproducibility.json")
    write_summary(claim_status, root / "results" / "claim_status.json")
    return report
