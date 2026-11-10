"""Incremental provenance repair semantics and crash-point analysis."""

from __future__ import annotations

import copy
from collections import defaultdict, deque
from typing import Any, Iterable


def repair_plan(edges: Iterable[dict[str, Any]], root_ids: Iterable[str]) -> list[str]:
    children: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        children[str(edge["source_id"])].append(str(edge["derived_id"]))
    for values in children.values():
        values.sort()
    ordered: list[str] = []
    seen: set[str] = set()
    queue = deque(sorted({str(root_id) for root_id in root_ids if root_id is not None}))
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        ordered.append(current)
        queue.extend(children.get(current, []))
    return ordered


def _descendants(edges: Iterable[dict[str, Any]], roots: Iterable[str]) -> set[str]:
    plan = repair_plan(edges, roots)
    return set(plan) - {str(root) for root in roots}


def incremental_repair(
    memories: dict[str, dict[str, Any]],
    edges: Iterable[dict[str, Any]],
    root_ids: Iterable[str],
    crash_after: int | None = None,
) -> dict[str, Any]:
    """Invalidate a graph one node at a time and optionally crash.

    Roots become invalid at the invalidation boundary.  Descendants are
    repaired in deterministic breadth-first order.  ``crash_after`` counts
    completed repair steps, so zero means a crash before the first step.
    """

    if crash_after is not None and crash_after < 0:
        raise ValueError("crash_after must be non-negative or None")
    result = copy.deepcopy(memories)
    roots = [str(root_id) for root_id in root_ids]
    plan = repair_plan(edges, roots)
    for root_id in roots:
        if root_id in result:
            result[root_id]["status"] = "invalid"
    completed: list[str] = []
    crashed = False
    for memory_id in plan:
        if crash_after is not None and len(completed) >= crash_after:
            crashed = True
            break
        if memory_id in result:
            result[memory_id]["status"] = "invalid"
        completed.append(memory_id)
    if crash_after is not None and len(completed) >= crash_after:
        crashed = True
    all_affected = _descendants(edges, roots) | set(roots)
    unsafe = sorted(
        memory_id
        for memory_id in all_affected - set(roots)
        if result.get(memory_id, {}).get("status") == "active"
    )
    return {
        "memories": result,
        "plan": plan,
        "completed_steps": completed,
        "remaining_steps": [memory_id for memory_id in plan if memory_id not in completed],
        "crashed": crashed,
        "unsafe_active_ids": unsafe,
        "safe": not unsafe,
    }


def repair_failure_matrix(
    memories: dict[str, dict[str, Any]], edges: Iterable[dict[str, Any]], root_ids: Iterable[str]
) -> dict[str, Any]:
    plan = repair_plan(edges, root_ids)
    cases = []
    for crash_after in range(len(plan)):
        result = incremental_repair(memories, edges, root_ids, crash_after=crash_after)
        cases.append(
            {
                "crash_after": crash_after,
                "safe": result["safe"],
                "unsafe_active_ids": result["unsafe_active_ids"],
            }
        )
    unsafe_points = [case["crash_after"] for case in cases if not case["safe"]]
    return {
        "repair_step_count": len(plan),
        "repair_plan": plan,
        "crash_cases": cases,
        "first_unsafe_crash_after": min(unsafe_points) if unsafe_points else None,
        "full_repair_safe": incremental_repair(memories, edges, root_ids)["safe"],
    }
