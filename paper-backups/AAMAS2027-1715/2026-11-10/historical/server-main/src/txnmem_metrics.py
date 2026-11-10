"""Metrics, summaries, and dependency-free SVG output for TxnMemBench."""

from __future__ import annotations

import html
import json
from collections import defaultdict, deque
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable

from txnmem_differential import compare_result_to_oracle
from txnmem_invariants import check_invariants


NUMERIC_FIELDS = (
    "partial_update_rate",
    "invalid_commit_rate",
    "stale_write_rate",
    "repair_recall",
    "leak_rate",
    "supersession_consistency",
    "scope_bypass_rate",
    "latency",
    "any_violation",
    "committed_count",
    "operation_count",
    "repair_count",
    "oracle_match",
    "allowed_outcome_count",
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


def _repair_recall(instance: dict[str, Any], result: dict[str, Any]) -> float:
    if instance["workload"] not in {"provenance_chain_repair", "provenance_branch_repair"}:
        return 0.0
    root_id = next(
        (
            operation.get("memory_id")
            for operation in instance.get("operations", [])
            if operation.get("type") == "invalidate"
        ),
        "m_root",
    )
    affected = _descendants(instance, root_id, result)
    if not affected:
        return 1.0
    repaired = sum(
        result.get("final_memories", {}).get(memory_id, {}).get("status") == "invalid"
        for memory_id in affected
    )
    return repaired / len(affected)


def result_row(instance: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    violations = check_invariants(instance, result)
    workload = instance["workload"]
    scope_violation = "scope_leak_violation" in violations
    supersession_violation = "supersession_consistency_violation" in violations
    oracle_comparison = compare_result_to_oracle(instance, result)
    return {
        "instance_id": instance["instance_id"],
        "workload": workload,
        "seed": instance["seed"],
        "variant": result["variant"],
        "transaction_state": result["transaction_state"],
        "partial_update_rate": float("atomicity_violation" in violations),
        "invalid_commit_rate": float("invalid_commit_violation" in violations),
        "stale_write_rate": float("stale_write_violation" in violations),
        "repair_recall": _repair_recall(instance, result),
        "leak_rate": float(scope_violation),
        "supersession_consistency": float(
            workload == "supersession_consistency" and not supersession_violation
        ),
        "scope_bypass_rate": float(workload == "scope_bypass" and scope_violation),
        "latency": float(result.get("metrics", {}).get("operation_count", len(result.get("trace", [])))),
        "any_violation": int(bool(violations)),
        "violations": ";".join(violations),
        "committed_count": len(result.get("committed_memory_ids", [])),
        "operation_count": result.get("metrics", {}).get("operation_count", len(result.get("trace", []))),
        "repair_count": result.get("metrics", {}).get("repair_count", 0),
        "oracle_version": oracle_comparison["oracle_version"],
        "oracle_match": int(oracle_comparison["matches"]),
        "allowed_outcome_count": oracle_comparison["allowed_outcome_count"],
        "oracle_mismatches": ";".join(oracle_comparison["mismatches"]),
    }


def summarize(rows: Iterable[dict[str, Any]], group_keys: tuple[str, ...]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = "/".join(str(row[name]) for name in group_keys)
        groups[key].append(row)
    output: dict[str, Any] = {"group_keys": list(group_keys), "groups": {}}
    for key in sorted(groups):
        records = groups[key]
        stats: dict[str, Any] = {name: records[0][name] for name in group_keys}
        stats["count"] = len(records)
        for field in NUMERIC_FIELDS:
            values = [float(row[field]) for row in records if field in row]
            if values:
                stats[field] = {"mean": mean(values), "std": pstdev(values)}
        output["groups"][key] = stats
    return output


def write_summary(summary: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_bar_figure(summary: dict[str, Any], path: Path, metric: str, title: str) -> None:
    groups = summary.get("groups", {})
    values = []
    for key, stats in sorted(groups.items()):
        value = stats.get(metric, {})
        values.append((key, float(value.get("mean", 0.0)) if isinstance(value, dict) else float(value)))
    width = 720
    height = 420
    chart_height = 280
    chart_top = 70
    max_value = max([value for _, value in values] or [1.0])
    bar_width = max(12.0, 600.0 / max(1, len(values)))
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<title>{html.escape(title)}</title>',
        f'<text x="20" y="30" font-family="sans-serif" font-size="18">{html.escape(title)}</text>',
        '<line x1="50" y1="350" x2="680" y2="350" stroke="#333"/>',
    ]
    for index, (label, value) in enumerate(values):
        x = 60 + index * (bar_width + 8)
        bar_height = chart_height * value / max_value if max_value else 0
        y = chart_top + chart_height - bar_height
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" fill="#3568a8"/>')
        parts.append(f'<text x="{x + bar_width / 2:.1f}" y="370" text-anchor="middle" font-family="sans-serif" font-size="9">{html.escape(label)}</text>')
        parts.append(f'<text x="{x + bar_width / 2:.1f}" y="{max(60, y - 5):.1f}" text-anchor="middle" font-family="sans-serif" font-size="9">{value:.3f}</text>')
    parts.append("</svg>\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")


def write_violation_figure(summary: dict[str, Any], path: Path) -> None:
    _write_bar_figure(summary, path, "any_violation", "Mean invariant violation rate")


def write_repair_figure(summary: dict[str, Any], path: Path) -> None:
    _write_bar_figure(summary, path, "repair_recall", "Mean provenance repair recall")
