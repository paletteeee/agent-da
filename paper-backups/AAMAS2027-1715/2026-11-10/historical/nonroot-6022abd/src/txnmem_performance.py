"""Lightweight local replay timing, explicitly separate from production claims."""

from __future__ import annotations

from math import ceil
from time import perf_counter
from typing import Any, Iterable

from txnmem_simulator import VARIANTS, run_instance


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(quantile * len(ordered)) - 1))
    return ordered[index]


def benchmark_replay(
    instances: Iterable[dict[str, Any]],
    variants: Iterable[str] = VARIANTS,
    repetitions: int = 3,
) -> dict[str, Any]:
    """Time deterministic replay over a fixed instance set."""

    if repetitions < 1:
        raise ValueError("repetitions must be >= 1")
    materialized = list(instances)
    rows: list[dict[str, Any]] = []
    for variant in variants:
        samples: list[float] = []
        operation_count = sum(len(instance.get("operations", [])) for instance in materialized)
        for _ in range(repetitions):
            start = perf_counter()
            for instance in materialized:
                run_instance(instance, variant)
            samples.append(perf_counter() - start)
        total_operations = operation_count * repetitions
        total_seconds = sum(samples)
        rows.append(
            {
                "variant": variant,
                "instance_count": len(materialized),
                "repetitions": repetitions,
                "operation_count": operation_count,
                "total_seconds": total_seconds,
                "mean_ms": total_seconds / repetitions * 1000.0,
                "p50_ms": _percentile(samples, 0.50) * 1000.0,
                "p95_ms": _percentile(samples, 0.95) * 1000.0,
                "operations_per_second": total_operations / total_seconds if total_seconds else 0.0,
            }
        )
    return {
        "benchmark": "deterministic_serial_replay",
        "timing_source": "time.perf_counter",
        "production_latency_claim": False,
        "rows": rows,
    }
