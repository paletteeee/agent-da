"""Exhaustive micro-witness linearizations for small concurrent workloads."""

from __future__ import annotations

import copy
from typing import Any, Iterable

from txnmem_differential import compare_result_to_oracle
from txnmem_reference import reference_outcome
from txnmem_simulator import run_instance


def enumerate_interleavings(
    sequences: Iterable[Iterable[dict[str, Any]]], max_count: int | None = None
) -> list[list[dict[str, Any]]]:
    """Enumerate all linear extensions that preserve each agent sequence."""

    materialized = [list(sequence) for sequence in sequences]
    if any(not isinstance(sequence, list) for sequence in materialized):
        raise ValueError("sequences must contain operation lists")
    total = sum(len(sequence) for sequence in materialized)
    if max_count is not None and max_count < 1:
        return []
    output: list[list[dict[str, Any]]] = []

    def visit(positions: list[int], prefix: list[dict[str, Any]]) -> None:
        if max_count is not None and len(output) >= max_count:
            return
        if len(prefix) == total:
            output.append(copy.deepcopy(prefix))
            return
        for sequence_index, sequence in enumerate(materialized):
            position = positions[sequence_index]
            if position >= len(sequence):
                continue
            positions[sequence_index] += 1
            prefix.append(sequence[position])
            visit(positions, prefix)
            prefix.pop()
            positions[sequence_index] -= 1

    visit([0] * len(materialized), [])
    return output


def _linearized_instance(instance: dict[str, Any], operations: list[dict[str, Any]]) -> dict[str, Any]:
    result = copy.deepcopy(instance)
    result["operations"] = []
    for step, operation in enumerate(operations, start=1):
        item = copy.deepcopy(operation)
        item["step"] = step
        if not item.get("op_id"):
            item["op_id"] = f"interleaving_op_{step:04d}"
        result["operations"].append(item)
    # A micro-witness schedule must be attached to operation ids in this
    # linearization.  The helper is intended for witnesses with no external
    # failure events; callers can supply a pre-linearized instance instead.
    result["failure_schedule"] = []
    return result


def micro_witness_report(
    instance: dict[str, Any],
    sequences: Iterable[Iterable[dict[str, Any]]],
    variant: str = "TxnMem",
    max_count: int | None = None,
) -> dict[str, Any]:
    """Run every admissible serialization and compare it with the oracle."""

    records: list[dict[str, Any]] = []
    oracle_keys: set[str] = set()
    mismatches = 0
    for index, operations in enumerate(enumerate_interleavings(sequences, max_count=max_count), start=1):
        linearized = _linearized_instance(instance, operations)
        oracle = reference_outcome(linearized)
        result = run_instance(linearized, variant)
        comparison = compare_result_to_oracle(linearized, result)
        oracle_keys.update(repr(outcome) for outcome in oracle.get("allowed_outcomes", []))
        comparison = {
            "interleaving_id": index,
            "operation_ids": [operation.get("op_id") for operation in operations],
            "oracle_outcome_count": len(oracle.get("allowed_outcomes", [])),
            "oracle_match": comparison["matches"],
            "oracle_mismatches": comparison["mismatches"],
        }
        records.append(comparison)
        if result.get("oracle_match") is False:
            mismatches += 1
    return {
        "concurrency_model": "exhaustive_serializations",
        "interleaving_count": len(records),
        "oracle_outcome_count": len(oracle_keys),
        "variant": variant,
        "oracle_mismatch_count": mismatches,
        "interleavings": records,
    }
