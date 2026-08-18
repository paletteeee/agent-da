"""Metrics, summaries, and dependency-free SVG output for TxnMemBench."""

from __future__ import annotations

import html
import json
from collections.abc import Mapping
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
        "transaction_states": json.dumps(
            dict(sorted(result.get("transaction_states", {}).items())),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
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


def write_saturation_figure(saturation: Mapping[str, Any], path: Path) -> None:
    """Render a deterministic dependency-free SVG from saturation JSON data."""

    checkpoints = list(saturation.get("checkpoints", []))
    if not checkpoints:
        raise ValueError("saturation report has no checkpoints")
    seed_counts = [int(checkpoint["checkpoint_seed_count"]) for checkpoint in checkpoints]
    variant_domain = list(saturation.get("variant_domain", []))
    if not variant_domain:
        raise ValueError("saturation report has no variants")
    width, height = 900, 520
    left, right, top, bottom = 80.0, 30.0, 60.0, 100.0
    chart_width = width - left - right
    chart_height = height - top - bottom
    minimum_seed = min(seed_counts)
    maximum_seed = max(seed_counts)

    def x_position(seed_count: int) -> float:
        if maximum_seed == minimum_seed:
            return left + chart_width / 2.0
        return left + chart_width * (seed_count - minimum_seed) / (maximum_seed - minimum_seed)

    def y_position(rate: float) -> float:
        return top + chart_height * (1.0 - rate)

    colors = ("#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#17becf")
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<title>Controlled evidence saturation</title>',
        '<text x="20" y="30" font-family="sans-serif" font-size="20">Controlled evidence saturation</text>',
        f'<line x1="{left:.1f}" y1="{top:.1f}" x2="{left:.1f}" y2="{top + chart_height:.1f}" stroke="#333"/>',
        f'<line x1="{left:.1f}" y1="{top + chart_height:.1f}" x2="{left + chart_width:.1f}" y2="{top + chart_height:.1f}" stroke="#333"/>',
    ]
    for index in range(5):
        rate = index / 4.0
        y = y_position(rate)
        parts.append(f'<line x1="{left:.1f}" y1="{y:.1f}" x2="{left + chart_width:.1f}" y2="{y:.1f}" stroke="#ddd"/>')
        parts.append(f'<text x="{left - 12:.1f}" y="{y + 4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11">{rate:.2f}</text>')
    for seed_count in seed_counts:
        x = x_position(seed_count)
        parts.append(f'<text x="{x:.1f}" y="{top + chart_height + 22:.1f}" text-anchor="middle" font-family="sans-serif" font-size="11">{seed_count}</text>')
    parts.append(f'<text x="{left + chart_width / 2:.1f}" y="{height - 52}" text-anchor="middle" font-family="sans-serif" font-size="12">checkpoint seeds per family</text>')
    parts.append(f'<text x="18" y="{top + chart_height / 2:.1f}" transform="rotate(-90 18 {top + chart_height / 2:.1f})" text-anchor="middle" font-family="sans-serif" font-size="12">rate</text>')

    by_checkpoint = {
        int(checkpoint["checkpoint_seed_count"]): {
            str(row["variant"]): row for row in checkpoint.get("variants", [])
        }
        for checkpoint in checkpoints
    }
    for variant_index, variant in enumerate(variant_domain):
        color = colors[variant_index % len(colors)]
        for metric, dash in (("violation_rate", ""), ("oracle_match_rate", ' stroke-dasharray="6 4"')):
            points = []
            for seed_count in seed_counts:
                row = by_checkpoint[seed_count].get(str(variant))
                if row is None:
                    raise ValueError(f"saturation checkpoint missing variant: {variant}")
                rate = float(row[metric])
                if not 0.0 <= rate <= 1.0:
                    raise ValueError(f"invalid saturation rate: {metric}")
                points.append(f"{x_position(seed_count):.3f},{y_position(rate):.3f}")
            parts.append(
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2"{dash}/>'
            )
        legend_y = height - 30 + (variant_index // 3) * 18
        legend_x = 85 + (variant_index % 3) * 270
        parts.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 22}" y2="{legend_y}" stroke="{color}" stroke-width="3"/>')
        parts.append(f'<text x="{legend_x + 28}" y="{legend_y + 4}" font-family="sans-serif" font-size="11">{html.escape(str(variant))}</text>')
    parts.append('</svg>\n')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")
