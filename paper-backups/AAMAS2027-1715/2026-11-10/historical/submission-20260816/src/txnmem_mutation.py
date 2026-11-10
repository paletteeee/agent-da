"""Mutation-style campaigns backed by the independent differential oracle."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any, Iterable

from txnmem_coverage import prefix_instance
from txnmem_differential import compare_result_to_oracle
from txnmem_invariants import check_invariants
from txnmem_reference import reference_outcome
from txnmem_simulator import run_instance


MUTANTS: dict[str, dict[str, Any]] = {
    "partial_commit": {
        "variant": "TxnMem-NoTxn",
        "target_workloads": {"atomic_multi_write", "mixed_stress"},
        "target_violation": "atomicity_violation",
        "witness_predicate": "partial_commit",
    },
    "remove_commit_revalidation": {
        "variant": "TxnMem-NoPolicyCommit",
        "target_workloads": {"revoke_before_commit", "mixed_stress"},
        "target_violation": "invalid_commit_violation",
        "witness_predicate": "target_violation",
    },
    "disable_provenance_traversal": {
        "variant": "TxnMem-NoRepair",
        "target_workloads": {"provenance_chain_repair", "provenance_branch_repair"},
        "target_violation": "provenance_closure_violation",
        "witness_predicate": "target_violation",
    },
    "bypass_scope_check": {
        "variant": "Naive",
        "target_workloads": {"scope_bypass"},
        "target_violation": "scope_leak_violation",
        "witness_predicate": "target_violation",
    },
}


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _mutant_observation(
    instance: dict[str, Any], spec: dict[str, Any]
) -> dict[str, Any]:
    result = run_instance(instance, spec["variant"])
    comparison = compare_result_to_oracle(instance, result)
    violations = check_invariants(instance, result)
    target = str(spec["target_violation"])
    reproduces = not comparison["matches"] and target in violations
    if spec.get("witness_predicate") == "partial_commit":
        committed = list(result.get("committed_memory_ids", []))
        reproduces = reproduces and bool(committed) and (
            result.get("transaction_state") == "partial_commit"
            or len(committed) < int(instance.get("config", {}).get("txn_size", len(committed)))
        )
    return {
        "reproduces": bool(reproduces),
        "violations": violations,
        "oracle_mismatches": comparison["mismatches"],
        "candidate": comparison["candidate"],
        "transaction_state": result.get("transaction_state"),
        "committed_memory_ids": list(result.get("committed_memory_ids", [])),
    }


def _compact_reference(instance: dict[str, Any]) -> dict[str, Any]:
    oracle = reference_outcome(instance)
    fields = (
        "txn_states",
        "committed_memory_ids",
        "visible_memory_ids",
        "invalid_memory_ids",
        "superseded_memory_ids",
    )
    return {
        "oracle_version": oracle.get("oracle_version"),
        "allowed_outcome_count": len(oracle.get("allowed_outcomes", [])),
        "allowed_outcomes": [
            {field: outcome.get(field, {}) if field == "txn_states" else outcome.get(field, []) for field in fields}
            for outcome in oracle.get("allowed_outcomes", [])
        ],
    }


def _minimal_witness(
    mutant_name: str,
    instance: dict[str, Any],
    spec: dict[str, Any],
) -> dict[str, Any] | None:
    shrink_trace: list[dict[str, Any]] = []
    for operation_count in range(1, len(instance.get("operations", [])) + 1):
        prefix = prefix_instance(instance, operation_count)
        observation = _mutant_observation(prefix, spec)
        shrink_trace.append(
            {
                "operation_count": operation_count,
                "reproduces_target_violation": observation["reproduces"],
                "violations": observation["violations"],
                "oracle_mismatches": observation["oracle_mismatches"],
            }
        )
        if not observation["reproduces"]:
            continue
        predecessor_reproduces = bool(
            len(shrink_trace) > 1
            and shrink_trace[-2]["reproduces_target_violation"]
        )
        return {
            "mutant": mutant_name,
            "variant": spec["variant"],
            "target_violation": spec["target_violation"],
            "source_instance_id": instance.get("instance_id"),
            "source_instance_sha256": _canonical_sha256(instance),
            "source_operation_count": len(instance.get("operations", [])),
            "minimal_operation_count": operation_count,
            "minimal_instance_sha256": _canonical_sha256(prefix),
            "minimal_instance": prefix,
            "reference_expected": _compact_reference(prefix),
            "observed": observation,
            "shrink_trace": shrink_trace,
            "minimality": {
                "method": "shortest_operation_prefix_reproducing_target_violation",
                "predecessor_operation_count": max(0, operation_count - 1),
                "predecessor_reproduces_target_violation": predecessor_reproduces,
            },
        }
    return None


def build_minimal_mutant_witnesses(
    instances: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Build one deterministic prefix-minimal witness per major mutant."""

    materialized = list(instances)
    witnesses: dict[str, dict[str, Any]] = {}
    for mutant_name, spec in MUTANTS.items():
        candidates = []
        for instance in sorted(materialized, key=lambda item: str(item.get("instance_id"))):
            if instance.get("workload") not in spec["target_workloads"]:
                continue
            candidate = _minimal_witness(mutant_name, instance, spec)
            if candidate is not None:
                candidates.append(candidate)
        if not candidates:
            raise ValueError(f"no target-specific minimal witness found for {mutant_name}")
        witnesses[mutant_name] = min(
            candidates,
            key=lambda item: (
                item["minimal_operation_count"],
                item["source_operation_count"],
                item["source_instance_id"],
            ),
        )
    report = {
        "schema_version": 1,
        "source_instance_count": len(materialized),
        "mutant_count": len(MUTANTS),
        "witness_count": len(witnesses),
        "witnesses": witnesses,
    }
    report["all_prefix_minimal"] = all(
        not witness["minimality"]["predecessor_reproduces_target_violation"]
        for witness in witnesses.values()
    )
    return report


def validate_minimal_mutant_witnesses(report: dict[str, Any]) -> bool:
    """Replay all published witnesses and fail closed on a stale witness."""

    witnesses = report.get("witnesses")
    if not isinstance(witnesses, dict) or set(witnesses) != set(MUTANTS):
        raise ValueError("minimal witness report does not cover all mutants")
    for mutant_name, witness in witnesses.items():
        spec = MUTANTS[mutant_name]
        instance = witness.get("minimal_instance")
        if not isinstance(instance, dict):
            raise ValueError(f"minimal instance missing for {mutant_name}")
        if _canonical_sha256(instance) != witness.get("minimal_instance_sha256"):
            raise ValueError(f"minimal instance hash mismatch for {mutant_name}")
        if not _mutant_observation(instance, spec)["reproduces"]:
            raise ValueError(f"minimal witness no longer reproduces {mutant_name}")
        operation_count = len(instance.get("operations", []))
        if operation_count > 1:
            predecessor = prefix_instance(instance, operation_count - 1)
            if _mutant_observation(predecessor, spec)["reproduces"]:
                raise ValueError(f"witness is not prefix-minimal for {mutant_name}")
    return True


def run_mutation_campaign(instances: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Run named mutants and report how often the oracle detects them."""

    materialized = list(instances)
    report: dict[str, Any] = {"mutants": {}, "cases": []}
    total_cases = 0
    total_killed = 0
    for mutant_name, spec in MUTANTS.items():
        targeted = [
            instance
            for instance in materialized
            if instance.get("workload") in spec["target_workloads"]
        ]
        killed = 0
        for instance in targeted:
            comparison = compare_result_to_oracle(instance, run_instance(instance, spec["variant"]))
            is_killed = not comparison["matches"]
            killed += int(is_killed)
            report["cases"].append(
                {
                    "mutant": mutant_name,
                    "instance_id": instance["instance_id"],
                    "killed": is_killed,
                    "mismatches": comparison["mismatches"],
                }
            )
        total_cases += len(targeted)
        total_killed += killed
        report["mutants"][mutant_name] = {
            "variant": spec["variant"],
            "target_case_count": len(targeted),
            "killed": killed,
            "kill_rate": killed / len(targeted) if targeted else 0.0,
        }
    report["kill_rate"] = total_killed / total_cases if total_cases else 0.0
    return report
