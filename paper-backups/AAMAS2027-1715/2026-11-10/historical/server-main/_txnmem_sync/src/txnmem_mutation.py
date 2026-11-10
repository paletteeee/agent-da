"""Mutation-style campaigns backed by the independent differential oracle."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from txnmem_differential import compare_result_to_oracle
from txnmem_simulator import run_instance


MUTANTS: dict[str, dict[str, Any]] = {
    "partial_commit": {
        "variant": "TxnMem-NoTxn",
        "target_workloads": {"atomic_multi_write", "mixed_stress"},
    },
    "remove_commit_revalidation": {
        "variant": "TxnMem-NoPolicyCommit",
        "target_workloads": {"revoke_before_commit", "mixed_stress"},
    },
    "disable_provenance_traversal": {
        "variant": "TxnMem-NoRepair",
        "target_workloads": {"provenance_chain_repair", "provenance_branch_repair"},
    },
    "bypass_scope_check": {
        "variant": "Naive",
        "target_workloads": {"scope_bypass"},
    },
}


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
