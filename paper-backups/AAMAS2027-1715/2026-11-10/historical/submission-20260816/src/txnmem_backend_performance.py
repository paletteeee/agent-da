"""Backend-only performance and deterministic fault-matrix runners."""

from __future__ import annotations

import copy
import time
from dataclasses import asdict, dataclass
from collections.abc import Callable, Iterable, Mapping
from math import ceil
from typing import Any


@dataclass(frozen=True)
class FaultScenario:
    name: str
    service: str
    trigger_operation: str
    action: str
    seed: int
    recovery_action: str = "abort"
    trigger_ordinal: int = 1

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(quantile * len(ordered)) - 1))
    return ordered[index]


def _new_backend(factory: Callable[..., Any], scenario: FaultScenario | None = None, size: int | None = None) -> Any:
    kwargs: dict[str, Any] = {}
    if scenario is not None:
        kwargs["scenario"] = scenario
    if size is not None:
        kwargs["size"] = size
    return factory(**kwargs)


def _apply_action(backend: Any, action: Mapping[str, Any]) -> None:
    operation = str(action.get("type", action.get("kind", "")))
    args = {key: value for key, value in action.items() if key not in {"type", "kind"}}
    if operation == "commit":
        return
    method = getattr(backend, operation, None)
    if not callable(method):
        raise ValueError(f"unsupported backend workload operation: {operation}")
    method(**args)


def run_fault_matrix(
    backend_factory: Callable[..., Any],
    scenarios: Iterable[FaultScenario | Mapping[str, Any]],
    workload: Iterable[Mapping[str, Any]],
    repetitions: int = 1,
) -> dict[str, Any]:
    """Run fault scenarios and classify recovered persistent state explicitly."""

    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    actions = [dict(action) for action in workload]
    scenario_rows: dict[str, dict[str, Any]] = {}
    for raw_scenario in scenarios:
        scenario = raw_scenario if isinstance(raw_scenario, FaultScenario) else FaultScenario(**dict(raw_scenario))
        row = {
            **scenario.as_dict(),
            "repetitions": repetitions,
            "success_count": 0,
            "error_count": 0,
            "retry_success_count": 0,
            "abort_count": 0,
            "persistent_state_classification_counts": {
                "complete": 0,
                "absent": 0,
                "partial": 0,
                "unknown": 0,
            },
            "persistent_state_verifications": [],
            "retry_count": 0,
            "fault_evidence_count": 0,
            "trigger_fired_count": 0,
            "toxic_installed_count": 0,
            "toxic_cleared_count": 0,
            "proxy_path_verified_count": 0,
            "fault_observed_count": 0,
            "evidence_valid_count": 0,
            "repetition_evidence": [],
        }
        for _ in range(repetitions):
            backend = _new_backend(backend_factory, scenario=scenario)
            failed = False
            retried = False
            local_retry_count = 0
            local_retry_success_count = 0
            try:
                for action in actions:
                    try:
                        _apply_action(backend, action)
                    except Exception:
                        failed = True
                        row["error_count"] += 1
                        can_retry = scenario.recovery_action == "retry_once"
                        if can_retry and not retried:
                            retried = True
                            local_retry_count += 1
                            try:
                                _apply_action(backend, action)
                            except Exception:
                                row["abort_count"] += 1
                                break
                            else:
                                local_retry_success_count += 1
                                failed = False
                                continue
                        row["abort_count"] += 1
                        break
                if not failed:
                    row["success_count"] += 1
                verifier = getattr(backend, "verify_persistent_state", None)
                if not callable(verifier):
                    state_verification: dict[str, Any] = {
                        "classification": "unknown",
                        "items": [],
                        "reason": "persistent_state_verifier_unavailable",
                    }
                else:
                    try:
                        raw_verification = verifier(actions)
                    except Exception as exc:
                        state_verification = {
                            "classification": "unknown",
                            "items": [],
                            "reason": "persistent_state_verification_failed",
                            "error": type(exc).__name__,
                        }
                    else:
                        if (
                            not isinstance(raw_verification, Mapping)
                            or raw_verification.get("classification")
                            not in {"complete", "absent", "partial", "unknown"}
                            or not isinstance(raw_verification.get("items"), list)
                        ):
                            state_verification = {
                                "classification": "unknown",
                                "items": [],
                                "reason": "persistent_state_verification_malformed",
                            }
                        else:
                            state_verification = copy.deepcopy(
                                dict(raw_verification)
                            )
                classification = str(state_verification["classification"])
                row["persistent_state_classification_counts"][classification] += 1
                row["persistent_state_verifications"].append(state_verification)
                evidence_provider = getattr(backend, "fault_evidence", None)
                evidence = evidence_provider() if callable(evidence_provider) else None
                if isinstance(evidence, Mapping):
                    sanitized = copy.deepcopy(dict(evidence))
                    row["repetition_evidence"].append(sanitized)
                    row["fault_evidence_count"] += 1
                    for key in (
                        "trigger_fired",
                        "toxic_installed",
                        "toxic_cleared",
                        "proxy_path_verified",
                        "fault_observed",
                        "evidence_valid",
                    ):
                        row[f"{key}_count"] += int(bool(evidence.get(key)))
                    row["retry_count"] += int(evidence.get("retry_count", 0) or 0)
                    row["retry_success_count"] += int(
                        evidence.get("retry_success_count", 0) or 0
                    )
                else:
                    row["retry_count"] += local_retry_count
                    row["retry_success_count"] += local_retry_success_count
            finally:
                close = getattr(backend, "close", None)
                if callable(close):
                    close()
        row["evidence_valid"] = row["evidence_valid_count"] == repetitions
        scenario_rows[scenario.name] = row
    return {
        "benchmark": "backend_fault_matrix",
        "scenarios": scenario_rows,
        "all_scenarios_state_verified": all(
            row["persistent_state_classification_counts"]["unknown"] == 0
            for row in scenario_rows.values()
        ),
        "all_observed_states_consistent": all(
            row["persistent_state_classification_counts"]["partial"] == 0
            and row["persistent_state_classification_counts"]["unknown"] == 0
            for row in scenario_rows.values()
        ),
        "all_scenarios_evidence_valid": all(
            bool(row["evidence_valid"]) for row in scenario_rows.values()
        ),
        "production_latency_claim": False,
    }


def _workload(size: int) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for index in range(size):
        actions.append({"type": "write", "memory_id": f"m{index}", "value": f"v{index}"})
    return actions


def benchmark_backend(
    backend_factory: Callable[..., Any],
    workload_sizes: Iterable[int] = (50, 200, 1000),
    repetitions: int = 30,
) -> dict[str, Any]:
    """Measure fixed backend workloads with model time excluded."""

    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    rows: list[dict[str, Any]] = []
    for raw_size in workload_sizes:
        size = int(raw_size)
        if size <= 0:
            raise ValueError("workload sizes must be positive")
        actions = _workload(size)
        samples: list[float] = []
        errors = 0
        retries = 0
        warmup_backend = _new_backend(backend_factory, size=size)
        try:
            for action in actions:
                _apply_action(warmup_backend, action)
        finally:
            close = getattr(warmup_backend, "close", None)
            if callable(close):
                close()
        for _ in range(repetitions):
            backend = _new_backend(backend_factory, size=size)
            started = time.perf_counter()
            try:
                for action in actions:
                    _apply_action(backend, action)
            except Exception:
                errors += 1
            finally:
                elapsed = time.perf_counter() - started
                samples.append(elapsed)
                metrics = getattr(backend, "metrics", lambda: {})()
                retries += int(metrics.get("retry_count", 0) or 0)
                close = getattr(backend, "close", None)
                if callable(close):
                    close()
        total_seconds = sum(samples)
        rows.append(
            {
                "workload_events": size,
                "repetitions": repetitions,
                "p50_ms": _percentile(samples, 0.50) * 1000.0,
                "p95_ms": _percentile(samples, 0.95) * 1000.0,
                "p99_ms": _percentile(samples, 0.99) * 1000.0,
                "throughput_ops_per_second": (size * repetitions) / total_seconds if total_seconds else 0.0,
                "error_count": errors,
                "retry_count": retries,
            }
        )
    return {
        "benchmark": "backend_only",
        "timing_source": "time.perf_counter",
        "rows": rows,
        "production_latency_claim": False,
    }
