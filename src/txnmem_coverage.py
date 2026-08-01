"""Coverage, random-schedule baseline, and minimal-counterexample utilities."""

from __future__ import annotations

import copy
import random
from collections import Counter
from typing import Any, Iterable

from txnmem_differential import compare_result_to_oracle
from txnmem_invariants import check_invariants
from txnmem_schedules import schedule_coverage
from txnmem_simulator import run_instance


WORKLOAD_TARGETS = {
    "atomic_multi_write": {"atomicity"},
    "crash_during_commit": {"recovery_consistency"},
    "revoke_before_commit": {"commit_authorization"},
    "scope_bypass": {"scope_safety"},
    "supersession_consistency": {"supersession_consistency"},
    "provenance_chain_repair": {"provenance_closure"},
    "provenance_branch_repair": {"provenance_closure"},
    "mixed_stress": {"atomicity", "commit_authorization"},
}


def randomize_schedule(instance: dict[str, Any], seed: int, event_count: int = 1) -> dict[str, Any]:
    """Create a deterministic random schedule using causal operation triggers."""

    if event_count < 0:
        raise ValueError("event_count must be non-negative")
    result = copy.deepcopy(instance)
    rng = random.Random(seed)
    operations = list(result.get("operations", []))
    candidates = [operation for operation in operations if operation.get("type") != "begin_txn"]
    events: list[dict[str, Any]] = []
    for _ in range(min(event_count, len(candidates))):
        operation = rng.choice(candidates)
        action = rng.choice(["crash", "revoke", "delay"])
        if action == "revoke":
            events.append(
                {
                    "trigger": {"before_operation": operation["op_id"]},
                    "type": "revoke",
                    "target": "write",
                    "phase": "before_validate",
                }
            )
        elif action == "crash":
            events.append(
                {
                    "trigger": {"after_operation": operation["op_id"]},
                    "type": "crash",
                    "target": operation.get("txn_id", operation.get("type")),
                    "phase": "after_operation",
                }
            )
        else:
            events.append(
                {
                    "trigger": {"before_operation": operation["op_id"]},
                    "type": "delay",
                    "target": operation.get("txn_id", operation.get("type")),
                    "phase": "before_operation",
                }
            )
    result["failure_schedule"] = events
    return result


def _prefix_instance(instance: dict[str, Any], operation_count: int) -> dict[str, Any]:
    prefix = copy.deepcopy(instance)
    prefix["operations"] = list(instance.get("operations", []))[:operation_count]
    operation_ids = {operation.get("op_id") for operation in prefix["operations"]}
    filtered_schedule = []
    for event in instance.get("failure_schedule", []):
        trigger = event.get("trigger", {})
        if isinstance(trigger, dict) and trigger:
            if all(operation_id in operation_ids for operation_id in trigger.values()):
                filtered_schedule.append(copy.deepcopy(event))
        elif int(event.get("step", 0)) <= int(prefix["operations"][-1].get("step", 0)) if prefix["operations"] else False:
            filtered_schedule.append(copy.deepcopy(event))
    prefix["failure_schedule"] = filtered_schedule
    return prefix


def find_minimal_counterexample(instance: dict[str, Any], variant: str) -> dict[str, Any] | None:
    """Return the shortest operation prefix rejected by the oracle, if any."""

    full_result = run_instance(instance, variant)
    if compare_result_to_oracle(instance, full_result)["matches"]:
        return None
    for operation_count in range(1, len(instance.get("operations", [])) + 1):
        prefix = _prefix_instance(instance, operation_count)
        result = run_instance(prefix, variant)
        comparison = compare_result_to_oracle(prefix, result)
        if not comparison["matches"]:
            return {
                "instance_id": instance.get("instance_id"),
                "variant": variant,
                "operation_count": operation_count,
                "operation_ids": [operation.get("op_id") for operation in prefix["operations"]],
                "failure_schedule": prefix["failure_schedule"],
                "violations": check_invariants(prefix, result),
                "oracle_mismatches": comparison["mismatches"],
            }
    return None


def coverage_report(instances: Iterable[dict[str, Any]], variant: str) -> dict[str, Any]:
    materialized = list(instances)
    action_counts: Counter[str] = Counter()
    trigger_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    total_events = 0
    targets: set[str] = set()
    for instance in materialized:
        coverage = schedule_coverage(instance)
        total_events += coverage["event_count"]
        action_counts.update(coverage["actions"])
        trigger_counts.update(coverage["trigger_kinds"])
        phase_counts.update(coverage["phases"])
        targets.update(WORKLOAD_TARGETS.get(instance.get("workload"), set()))
    counterexamples = [
        item
        for instance in materialized
        for item in [find_minimal_counterexample(instance, variant)]
        if item is not None
    ]
    return {
        "instance_count": len(materialized),
        "schedule_coverage": {
            "event_count": total_events,
            "actions": dict(sorted(action_counts.items())),
            "trigger_kinds": dict(sorted(trigger_counts.items())),
            "phases": dict(sorted(phase_counts.items())),
        },
        "invariant_coverage": {
            "covered": sorted(targets),
            "target_count": len(targets),
            "coverage_rate": 1.0 if targets else 0.0,
        },
        "minimal_counterexamples": counterexamples,
    }


def schedule_effectiveness(
    instances: Iterable[dict[str, Any]],
    variant: str,
    random_seeds: Iterable[int] = range(10),
) -> dict[str, Any]:
    """Compare detection on causal schedules with seeded random schedules."""

    materialized = list(instances)
    causal_detections = sum(
        not compare_result_to_oracle(instance, run_instance(instance, variant))["matches"]
        for instance in materialized
    )
    seeds = list(random_seeds)
    random_detections = 0
    random_cases = 0
    for seed in seeds:
        for instance in materialized:
            randomized = randomize_schedule(instance, seed=seed)
            random_detections += int(
                not compare_result_to_oracle(randomized, run_instance(randomized, variant))["matches"]
            )
            random_cases += 1
    return {
        "causal_case_count": len(materialized),
        "causal_detection_rate": causal_detections / len(materialized) if materialized else 0.0,
        "random_runs": len(seeds),
        "random_case_count": random_cases,
        "random_detection_rate": random_detections / random_cases if random_cases else 0.0,
    }
