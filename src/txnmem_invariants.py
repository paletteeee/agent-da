"""Stable invariant checks for TxnMemBench replay results."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from txnmem_differential import compare_result_to_oracle


VIOLATION_ORDER = (
    "atomicity_violation",
    "unexpected_commit",
    "recovery_consistency_violation",
    "invalid_commit_violation",
    "stale_write_violation",
    "scope_leak_violation",
    "supersession_consistency_violation",
    "provenance_closure_violation",
)


def _descendants(
    instance: dict[str, Any], source_id: str, result: dict[str, Any] | None = None
) -> set[str]:
    children: dict[str, list[str]] = defaultdict(list)
    edges = (result or {}).get("provenance_edges") or instance.get("provenance_edges", [])
    for edge in edges:
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


def check_invariants(instance: dict[str, Any], result: dict[str, Any]) -> list[str]:
    """Return stable, deduplicated violation names for one replay result."""

    violations: list[str] = []
    workload = instance["workload"]
    committed = result.get("committed_memory_ids", [])
    final_memories = result.get("final_memories", {})

    def add(name: str) -> None:
        if name not in violations:
            violations.append(name)

    if workload in {"atomic_multi_write", "mixed_stress"}:
        expected_size = int(instance["config"].get("txn_size", len(committed)))
        if result.get("transaction_state") == "partial_commit":
            add("atomicity_violation")
        elif result.get("transaction_state") == "committed" and len(committed) not in (0, expected_size):
            add("atomicity_violation")

    if workload == "crash_during_commit":
        if result.get("transaction_state") == "partial_commit":
            add("recovery_consistency_violation")
        elif len(committed) not in (0, 1):
            add("recovery_consistency_violation")

    if workload in {"revoke_before_commit", "mixed_stress"}:
        if result.get("transaction_state") == "committed":
            add("invalid_commit_violation")
        protected_id = "m_protected_write"
        if workload == "mixed_stress":
            protected_id = "m_mix_1"
        if protected_id in final_memories and final_memories[protected_id].get("status") == "active":
            add("stale_write_violation")

    if workload == "scope_bypass":
        if result.get("metrics", {}).get("exposed_memory_ids"):
            add("scope_leak_violation")

    if workload == "supersession_consistency":
        old_id = "m_old"
        new_id = "m_new"
        old = final_memories.get(old_id)
        new = final_memories.get(new_id)
        if (
            old is None
            or new is None
            or old.get("status") != "superseded"
            or new.get("supersedes_id") != old_id
            or new.get("status") != "active"
        ):
            add("supersession_consistency_violation")

    if workload in {"provenance_chain_repair", "provenance_branch_repair"}:
        root_id = next(
            (
                operation.get("memory_id")
                for operation in instance.get("operations", [])
                if operation.get("type") == "invalidate"
            ),
            "m_root",
        )
        for memory_id in _descendants(instance, root_id, result):
            memory = final_memories.get(memory_id)
            if memory is not None and memory.get("status") == "active":
                add("provenance_closure_violation")
                break

    comparison = compare_result_to_oracle(instance, result)
    if not comparison["matches"]:
        mismatches = set(comparison["mismatches"])
        if "visible_memory_ids" in mismatches:
            if workload == "scope_bypass":
                add("scope_leak_violation")
        if "invalid_memory_ids" in mismatches:
            if workload in {"provenance_chain_repair", "provenance_branch_repair"}:
                add("provenance_closure_violation")
        if "superseded_memory_ids" in mismatches:
            if workload == "supersession_consistency":
                add("supersession_consistency_violation")
        if "committed_memory_ids" in mismatches or "transaction_state" in mismatches:
            if workload in {"atomic_multi_write", "mixed_stress"}:
                add("atomicity_violation")
            elif workload == "revoke_before_commit":
                add("invalid_commit_violation")
            elif workload == "crash_during_commit":
                add("recovery_consistency_violation")

    rank = {name: index for index, name in enumerate(VIOLATION_ORDER)}
    return sorted(violations, key=lambda name: rank.get(name, len(rank)))
