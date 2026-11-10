"""Deterministic group-aware resampling for benchmark evidence."""

from __future__ import annotations

import math
import random
from collections.abc import Hashable, Mapping, Sequence
from statistics import mean
from typing import Any


def _group_rows(
    rows: Sequence[Mapping[str, Any]], group_key: str
) -> dict[Hashable, list[Mapping[str, Any]]]:
    grouped: dict[Hashable, list[Mapping[str, Any]]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"row {index} must be a mapping")
        if group_key not in row:
            raise ValueError(f"row {index} has no group key {group_key!r}")
        group = row[group_key]
        if isinstance(group, bool) or not isinstance(group, Hashable):
            raise ValueError(f"row {index} has an invalid group value")
        grouped.setdefault(group, []).append(row)
    if not grouped:
        raise ValueError("cluster bootstrap requires at least one group")
    return grouped


def _resample_whole_clusters(
    rows: Sequence[Mapping[str, Any]],
    *,
    group_key: str,
    rng: Any,
) -> list[Mapping[str, Any]]:
    """Draw groups with replacement and copy every row in each draw."""

    grouped = _group_rows(rows, group_key)
    groups = list(grouped)
    selected = rng.choices(groups, k=len(groups))
    return [row for group in selected for row in grouped[group]]


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(probability * len(ordered)) - 1))
    return ordered[index]


def cluster_bootstrap_interval(
    rows: Sequence[Mapping[str, Any]],
    group_key: str,
    value_key: str,
    repetitions: int = 10000,
    seed: int = 17,
) -> dict[str, Any]:
    """Bootstrap a row-weighted mean while resampling complete groups."""

    if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions < 1:
        raise ValueError("repetitions must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    grouped = _group_rows(rows, group_key)
    values: list[float] = []
    for index, row in enumerate(rows):
        value = row.get(value_key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"row {index} has a non-numeric value")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"row {index} has a non-finite value")
        values.append(numeric)

    group_totals = {
        group: sum(float(row[value_key]) for row in group_rows)
        for group, group_rows in grouped.items()
    }
    group_counts = {group: len(group_rows) for group, group_rows in grouped.items()}
    groups = list(grouped)
    rng = random.Random(seed)
    bootstrap_means = []
    for _ in range(repetitions):
        selected = rng.choices(groups, k=len(groups))
        bootstrap_means.append(
            sum(group_totals[group] for group in selected)
            / sum(group_counts[group] for group in selected)
        )
    alpha = 0.025
    estimate = mean(values)
    return {
        "method": "percentile_cluster_bootstrap",
        "sampling_unit": "whole_group",
        "confidence": 0.95,
        "estimate": estimate,
        "lower": _percentile(bootstrap_means, alpha),
        "upper": _percentile(bootstrap_means, 1.0 - alpha),
        "group_count": len(grouped),
        "row_count": len(rows),
        "bootstrap_repetitions": repetitions,
        "seed": seed,
    }
