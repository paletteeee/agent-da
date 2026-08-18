"""Differential comparison between an implementation result and reference oracle."""

from __future__ import annotations

from typing import Any

from txnmem_reference import reference_outcome


def _explicit_txn_ids(instance: dict[str, Any]) -> list[str]:
    return sorted(
        {
            str(operation["txn_id"])
            for operation in instance.get("operations", [])
            if operation.get("txn_id")
            and operation.get("type") in {"begin_txn", "write", "stage_write", "derive", "propagate", "supersede", "commit"}
        }
    )


def _implementation_snapshot(instance: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    final_memories = result.get("final_memories", {})
    txn_ids = _explicit_txn_ids(instance)
    transaction_state = result.get("transaction_state")
    txn_states = {txn_id: transaction_state for txn_id in txn_ids} if txn_ids else {}
    if result.get("transaction_states"):
        txn_states = dict(result["transaction_states"])
    has_read_operation = any(
        operation.get("type") in {"read", "search", "get_by_id"}
        for operation in instance.get("operations", [])
    )
    if has_read_operation:
        visible_ids = sorted(
            memory_id
            for memory_id in result.get("metrics", {}).get("exposed_memory_ids", [])
            if final_memories.get(memory_id, {}).get("status") == "active"
        )
    else:
        visible_ids = sorted(
            memory_id
            for memory_id, memory in final_memories.items()
            if memory.get("status") == "active"
        )
    return {
        "txn_states": txn_states,
        "committed_memory_ids": list(result.get("committed_memory_ids", [])),
        "visible_memory_ids": visible_ids,
        "invalid_memory_ids": sorted(
            memory_id
            for memory_id, memory in final_memories.items()
            if memory.get("status") == "invalid"
        ),
        "superseded_memory_ids": sorted(
            memory_id
            for memory_id, memory in final_memories.items()
            if memory.get("status") == "superseded"
        ),
    }


def _matches_outcome(candidate: dict[str, Any], outcome: dict[str, Any]) -> tuple[bool, list[str]]:
    mismatches: list[str] = []
    if outcome.get("txn_states"):
        for txn_id, expected_state in outcome["txn_states"].items():
            if txn_id == "implicit" and txn_id not in candidate["txn_states"]:
                continue
            if candidate["txn_states"].get(txn_id) != expected_state:
                mismatches.append("transaction_state")
                break
    for field in (
        "committed_memory_ids",
        "visible_memory_ids",
        "invalid_memory_ids",
        "superseded_memory_ids",
    ):
        if candidate[field] != outcome.get(field, []):
            mismatches.append(field)
    return not mismatches, mismatches


def compare_result_to_oracle(
    instance: dict[str, Any], result: dict[str, Any], oracle: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Compare implementation state against all allowed reference outcomes."""

    oracle = oracle or reference_outcome(instance)
    candidate = _implementation_snapshot(instance, result)
    comparisons = [_matches_outcome(candidate, outcome) for outcome in oracle.get("allowed_outcomes", [])]
    matches = any(item[0] for item in comparisons)
    mismatches: list[str] = []
    if not matches and comparisons:
        for field in comparisons[0][1]:
            if field not in mismatches:
                mismatches.append(field)
    return {
        "matches": matches,
        "oracle_version": oracle.get("oracle_version"),
        "allowed_outcome_count": len(oracle.get("allowed_outcomes", [])),
        "mismatches": mismatches,
        "candidate": candidate,
    }
