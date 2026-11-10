"""Paired multi-agent experiments over fixture or real vector/graph storage."""

from __future__ import annotations

import copy
import hashlib
import threading
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_multiagent_storage import MultiAgentStorageCoordinator


REAL_STORAGE_SCENARIOS = (
    "concurrent_atomic_backend_failure",
    "selective_actor_revocation",
    "cross_agent_provenance_invalidation",
    "concurrent_mixed_failure",
)
CONDITIONS = ("TxnMem", "Naive")


class _FixtureFaultRequester:
    def __init__(self, scenario: Mapping[str, Any]):
        self.scenario = dict(scenario)
        self.ordinals: Counter[str] = Counter()
        self.fired = False
        self.events: list[dict[str, Any]] = []

    def __call__(self, service, operation, function, request_key):
        key = f"{service}:{operation}"
        self.ordinals[key] += 1
        target = (
            service == self.scenario.get("service")
            and operation == self.scenario.get("trigger_operation")
            and self.ordinals[key] == int(self.scenario.get("trigger_ordinal", 1))
            and self.scenario.get("action") != "none"
        )
        if target and not self.fired:
            self.fired = True
            event = {
                "service": service,
                "operation": operation,
                "request_ordinal": self.ordinals[key],
                "action": self.scenario.get("action"),
                "request_key_sha256": hashlib.sha256(
                    str(request_key).encode("utf-8")
                ).hexdigest(),
                "fault_observed": True,
            }
            self.events.append(event)
            raise ConnectionError("fixture storage fault")
        return function()

    def evidence(self) -> dict[str, Any]:
        requires_fault = self.scenario.get("action") != "none"
        return {
            "scenario": self.scenario.get("name"),
            "trigger_fired": self.fired,
            "fault_observed": self.fired,
            "proxy_path_verified": True,
            "request_ordinals": dict(sorted(self.ordinals.items())),
            "events": copy.deepcopy(self.events),
            "evidence_valid": self.fired if requires_fault else True,
            "fixture": True,
        }


class _FixtureBackend(InstrumentedMemoryBackend):
    def __init__(self, namespace: str, requester: Any | None):
        super().__init__()
        self.namespace = str(namespace)
        self.requester = requester

    def _request(self, operation: str, function: Callable[[], Any], key: str) -> Any:
        if self.requester is None:
            return function()
        return self.requester("qdrant", operation, function, key)

    def write(self, memory_id: str, value: Any = None, **fields: Any):
        return self._request(
            "write",
            lambda: InstrumentedMemoryBackend.write(
                self, memory_id, value=value, **fields
            ),
            f"{self.namespace}:write:{memory_id}",
        )

    def invalidate(self, memory_id: str, **fields: Any) -> None:
        self._request(
            "invalidate",
            lambda: InstrumentedMemoryBackend.invalidate(
                self, memory_id, **fields
            ),
            f"{self.namespace}:invalidate:{memory_id}",
        )

    def healthcheck(self) -> dict[str, Any]:
        return {
            "qdrant": {"available": True, "version": "fixture"},
            "neo4j": {"available": True, "version": "fixture"},
        }

    def fault_evidence(self) -> dict[str, Any]:
        provider = getattr(self.requester, "evidence", None)
        return provider() if callable(provider) else {"evidence_valid": True}

    def close(self) -> None:
        return None


def fixture_backend_factory(namespace: str, fault_requester: Any | None) -> Any:
    """Create the offline backend while keeping the production matrix path."""

    return _FixtureBackend(namespace, fault_requester)


def _fault_spec(scenario: str, repetition: int) -> dict[str, Any]:
    injects_failure = scenario in {
        "concurrent_atomic_backend_failure",
        "concurrent_mixed_failure",
    }
    return {
        "name": f"{scenario}_rep_{repetition:02d}",
        "service": "qdrant" if injects_failure else "none",
        "trigger_operation": "write" if injects_failure else "none",
        "trigger_ordinal": 2,
        "action": "connection_drop" if injects_failure else "none",
        "seed": repetition,
        "recovery_action": "abort" if injects_failure else "none",
    }


def _prepared_concurrent_commit(
    coordinator: MultiAgentStorageCoordinator,
    transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    barrier = threading.Barrier(len(transactions))
    errors: list[str] = []

    def worker(txn_id: str) -> None:
        try:
            barrier.wait()
            coordinator.commit(txn_id)
        except Exception as exc:
            errors.append(type(exc).__name__)

    threads = [
        threading.Thread(target=worker, args=(str(item["txn_id"]),))
        for item in transactions
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    report = coordinator.report()
    report["thread_errors"] = sorted(errors)
    return report


def _stage_transactions(
    coordinator: MultiAgentStorageCoordinator,
    transactions: list[dict[str, Any]],
) -> None:
    for transaction in transactions:
        coordinator.begin(str(transaction["txn_id"]), str(transaction["agent_id"]))
        for action in transaction["actions"]:
            coordinator.stage(str(transaction["txn_id"]), action)


def _write_transactions(prefix: str, action_count: int = 2) -> list[dict[str, Any]]:
    return [
        {
            "txn_id": f"txn_agent_{agent_index}",
            "agent_id": f"agent_{agent_index}",
            "actions": [
                {
                    "type": "write",
                    "memory_id": f"{prefix}_a{agent_index}_{action_index}",
                    "value": f"value_{agent_index}_{action_index}",
                    "scope": "tenant:shared",
                }
                for action_index in range(1, action_count + 1)
            ],
        }
        for agent_index in (1, 2)
    ]


def _run_scenario(
    name: str,
    condition: str,
    backend: Any,
) -> tuple[dict[str, Any], str]:
    coordinator = MultiAgentStorageCoordinator(backend, variant=condition)
    policy_violation = False
    provenance_violation = False

    if name == "concurrent_atomic_backend_failure":
        transactions = _write_transactions("atomic")
        _stage_transactions(coordinator, transactions)
        runtime_report = _prepared_concurrent_commit(coordinator, transactions)

    elif name == "selective_actor_revocation":
        transactions = _write_transactions("policy", action_count=1)
        _stage_transactions(coordinator, transactions)
        coordinator.revoke("agent_1", "write", "tenant:shared")
        runtime_report = _prepared_concurrent_commit(coordinator, transactions)
        policy_violation = any(
            memory.get("agent_id") == "agent_1"
            and memory.get("status") == "active"
            for memory in backend.snapshot().values()
        )

    elif name == "cross_agent_provenance_invalidation":
        transactions = [
            {
                "txn_id": "txn_agent_1",
                "agent_id": "agent_1",
                "actions": [
                    {
                        "type": "write",
                        "memory_id": "provenance_root",
                        "value": "root",
                        "scope": "tenant:shared",
                    }
                ],
            },
            {
                "txn_id": "txn_agent_2",
                "agent_id": "agent_2",
                "actions": [
                    {
                        "type": "derive",
                        "memory_id": "provenance_child",
                        "source_ids": ["provenance_root"],
                        "value": "child",
                        "scope": "tenant:shared",
                    }
                ],
            },
        ]
        coordinator.begin("txn_agent_1", "agent_1")
        coordinator.stage("txn_agent_1", transactions[0]["actions"][0])
        coordinator.commit("txn_agent_1")
        coordinator.begin("txn_agent_2", "agent_2")
        coordinator.stage("txn_agent_2", transactions[1]["actions"][0])
        coordinator.commit("txn_agent_2")
        coordinator.invalidate("agent_1", "provenance_root")
        runtime_report = coordinator.report()
        provenance_violation = (
            backend.snapshot().get("provenance_child", {}).get("status")
            == "active"
        )

    elif name == "concurrent_mixed_failure":
        backend.write(
            "mixed_root",
            value="root",
            agent_id="agent_1",
            scope="tenant:shared",
        )
        transactions = [
            {
                "txn_id": f"txn_agent_{agent_index}",
                "agent_id": f"agent_{agent_index}",
                "actions": [
                    {
                        "type": "derive",
                        "memory_id": f"mixed_child_{agent_index}",
                        "source_ids": ["mixed_root"],
                        "value": f"child_{agent_index}",
                        "scope": "tenant:shared",
                    },
                    {
                        "type": "write",
                        "memory_id": f"mixed_write_{agent_index}",
                        "value": f"write_{agent_index}",
                        "scope": "tenant:shared",
                    },
                ],
            }
            for agent_index in (1, 2)
        ]
        _stage_transactions(coordinator, transactions)
        coordinator.revoke("agent_1", "derive", "tenant:shared")
        runtime_report = _prepared_concurrent_commit(coordinator, transactions)
        coordinator.invalidate("agent_2", "mixed_root")
        runtime_report = coordinator.report()
        snapshot = backend.snapshot()
        policy_violation = any(
            memory.get("agent_id") == "agent_1"
            and memory.get("status") == "active"
            for memory_id, memory in snapshot.items()
            if memory_id != "mixed_root"
        )
        provenance_violation = any(
            memory.get("status") == "active"
            and memory.get("derived_from") == ["mixed_root"]
            for memory in snapshot.values()
        )
    else:
        raise ValueError(f"unknown real-storage scenario: {name}")

    outcome_classes = [
        str(outcome.get("classification"))
        for outcome in runtime_report.get("outcomes", [])
    ]
    if runtime_report.get("thread_errors") or "unknown" in outcome_classes:
        classification = "unknown"
    elif "partial_visible" in outcome_classes:
        classification = "partial_visible"
    elif policy_violation:
        classification = "policy_violation"
    elif provenance_violation:
        classification = "provenance_violation"
    elif outcome_classes and all(
        item == "aborted_clean" for item in outcome_classes
    ):
        classification = "aborted_clean"
    else:
        classification = "committed_complete"
    return runtime_report, classification


def _safe_fault_evidence(backend: Any) -> dict[str, Any]:
    provider = getattr(backend, "fault_evidence", None)
    raw = provider() if callable(provider) else None
    if not isinstance(raw, Mapping):
        return {"evidence_valid": False, "reason": "not_available"}
    allowed = {
        "scenario",
        "trigger_fired",
        "toxic_installed",
        "toxic_cleared",
        "proxy_path_verified",
        "fault_observed",
        "retry_count",
        "retry_success_count",
        "request_ordinals",
        "events",
        "evidence_valid",
        "fixture",
    }
    return {
        key: copy.deepcopy(value)
        for key, value in raw.items()
        if key in allowed
    }


def run_real_storage_matrix(
    backend_factory: Callable[[str, Callable[..., Any] | None], Any],
    repetitions: int = 30,
    *,
    fault_requester_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    real_service_evidence: bool = False,
) -> dict[str, Any]:
    """Run four paired two-agent scenarios and classify persistent readback."""

    if (
        isinstance(repetitions, bool)
        or not isinstance(repetitions, int)
        or repetitions < 1
    ):
        raise ValueError("repetitions must be a positive integer")
    requester_factory = fault_requester_factory or _FixtureFaultRequester

    health_backend = backend_factory("healthcheck", None)
    try:
        health_provider = getattr(health_backend, "healthcheck", None)
        service_versions = (
            health_provider() if callable(health_provider) else {"fixture": True}
        )
    finally:
        close = getattr(health_backend, "close", None)
        if callable(close):
            close()

    observations: list[dict[str, Any]] = []
    for repetition in range(1, repetitions + 1):
        for scenario in REAL_STORAGE_SCENARIOS:
            fault_spec = _fault_spec(scenario, repetition)
            for condition in CONDITIONS:
                namespace = (
                    f"multi-agent-{scenario}-r{repetition:02d}-{condition.lower()}"
                )
                requester = requester_factory(copy.deepcopy(fault_spec))
                backend = backend_factory(namespace, requester)
                try:
                    runtime_report, classification = _run_scenario(
                        scenario, condition, backend
                    )
                    snapshot = backend.snapshot()
                    observations.append(
                        {
                            "scenario": scenario,
                            "repetition": repetition,
                            "condition": condition,
                            "agent_count": 2,
                            "actual_commit_order": copy.deepcopy(
                                runtime_report.get("commit_order", [])
                            ),
                            "classification": classification,
                            "state_readback": {
                                memory_id: memory.get("status")
                                for memory_id, memory in sorted(snapshot.items())
                            },
                            "fault_evidence": _safe_fault_evidence(backend),
                        }
                    )
                finally:
                    close = getattr(backend, "close", None)
                    if callable(close):
                        close()

    txnmem = [row for row in observations if row["condition"] == "TxnMem"]
    txnmem_counts = {
        "observation_count": len(txnmem),
        "unknown_count": sum(row["classification"] == "unknown" for row in txnmem),
        "partial_visible_count": sum(
            row["classification"] == "partial_visible" for row in txnmem
        ),
        "policy_violation_count": sum(
            row["classification"] == "policy_violation" for row in txnmem
        ),
        "provenance_violation_count": sum(
            row["classification"] == "provenance_violation" for row in txnmem
        ),
    }
    all_readable = all(row["classification"] != "unknown" for row in observations)
    all_fault_evidence_valid = all(
        row["fault_evidence"].get("evidence_valid") is True
        for row in observations
    )
    return {
        "schema_version": 1,
        "status": "passed" if all_readable and all_fault_evidence_valid else "failed",
        "scenarios": list(REAL_STORAGE_SCENARIOS),
        "conditions": list(CONDITIONS),
        "scenario_count": len(REAL_STORAGE_SCENARIOS),
        "condition_count": len(CONDITIONS),
        "repetition_count": repetitions,
        "observation_count": len(observations),
        "agent_count": 2,
        "real_service_evidence": bool(real_service_evidence),
        "service_versions": copy.deepcopy(service_versions),
        "observations": observations,
        "txnmem_counts": txnmem_counts,
        "unknown_count": txnmem_counts["unknown_count"],
        "partial_visible_count": txnmem_counts["partial_visible_count"],
        "policy_violation_count": txnmem_counts["policy_violation_count"],
        "provenance_violation_count": txnmem_counts[
            "provenance_violation_count"
        ],
        "all_state_readbacks_complete": all_readable,
        "all_fault_evidence_valid": all_fault_evidence_valid,
    }
