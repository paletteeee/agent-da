"""Transaction-aware coordination over instrumented or vector/graph backends."""

from __future__ import annotations

import copy
import json
import threading
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from typing import Any


class MultiAgentStorageCoordinator:
    """Stage actor-owned transactions and linearize verified backend commits."""

    def __init__(
        self,
        backend: Any,
        *,
        variant: str = "TxnMem",
        policies: Iterable[Mapping[str, Any]] | None = None,
    ):
        if variant not in {"TxnMem", "Naive"}:
            raise ValueError("storage coordinator variant must be TxnMem or Naive")
        self.backend = backend
        self.variant = variant
        self.policies = [dict(policy) for policy in (policies or [])]
        self._lock = threading.RLock()
        self._transactions: dict[str, dict[str, Any]] = {}
        self._policy_versions: dict[str, int] = defaultdict(lambda: 1)
        self._revoked: set[tuple[str, str, str]] = set()
        self._commit_order: list[dict[str, Any]] = []
        self._outcomes: list[dict[str, Any]] = []

    def begin(self, txn_id: str, agent_id: str) -> None:
        with self._lock:
            if txn_id in self._transactions:
                raise ValueError(f"duplicate transaction: {txn_id}")
            self._transactions[txn_id] = {
                "agent_id": str(agent_id),
                "begin_policy_version": self._policy_versions[str(agent_id)],
                "actions": [],
                "applied_actions": [],
                "prior_states": {},
                "read_versions": {},
                "state": "active",
                "apply_error": None,
            }

    def _transaction(self, txn_id: str) -> dict[str, Any]:
        if txn_id not in self._transactions:
            raise ValueError(f"unknown transaction: {txn_id}")
        return self._transactions[txn_id]

    @staticmethod
    def _memory_id(action: Mapping[str, Any]) -> str | None:
        value = action.get("memory_id") or action.get("output_id")
        return str(value) if value is not None else None

    @staticmethod
    def _fingerprint(memory: Mapping[str, Any] | None) -> str | None:
        if memory is None:
            return None
        return json.dumps(
            dict(memory),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    def _raw_memory(self, memory_id: str) -> dict[str, Any] | None:
        snapshot = self.backend.snapshot()
        memory = snapshot.get(memory_id)
        return copy.deepcopy(memory) if isinstance(memory, Mapping) else None

    def _remember_version(self, txn: dict[str, Any], memory_id: str) -> None:
        txn["read_versions"].setdefault(
            memory_id,
            self._fingerprint(self._raw_memory(memory_id)),
        )

    def stage(self, txn_id: str, action: Mapping[str, Any]) -> None:
        with self._lock:
            txn = self._transaction(txn_id)
            if txn["state"] != "active":
                raise ValueError(f"transaction is not active: {txn_id}")
            item = copy.deepcopy(dict(action))
            action_type = str(item.get("type") or item.get("kind") or "")
            if action_type not in {
                "write",
                "derive",
                "propagate",
                "invalidate",
                "delete",
            }:
                raise ValueError(f"unsupported staged action: {action_type}")
            item["type"] = action_type
            item.pop("kind", None)
            if item.get("agent_id") not in {None, txn["agent_id"]}:
                raise ValueError(f"transaction actor mismatch: {txn_id}")
            item["agent_id"] = txn["agent_id"]
            item.setdefault("scope", "tenant:user_001")
            for source_id in item.get("source_ids", []):
                self._remember_version(txn, str(source_id))
            txn["actions"].append(item)
            if self.variant == "Naive":
                memory_id = self._memory_id(item)
                if memory_id is not None:
                    txn["prior_states"].setdefault(
                        memory_id, self._raw_memory(memory_id)
                    )
                try:
                    self._apply(item)
                except Exception as exc:
                    txn["apply_error"] = type(exc).__name__
                else:
                    txn["applied_actions"].append(item)

    def read(
        self, txn_id: str, memory_id: str, scope: str
    ) -> dict[str, Any] | None:
        with self._lock:
            txn = self._transaction(txn_id)
            for action in reversed(txn["actions"]):
                if self._memory_id(action) != memory_id:
                    continue
                if action["type"] in {"write", "derive", "propagate"}:
                    if action.get("scope") != scope:
                        return None
                    return {
                        "memory_id": memory_id,
                        "value": action.get("value", memory_id),
                        "status": "active",
                        "agent_id": txn["agent_id"],
                        "scope": scope,
                        "derived_from": list(action.get("source_ids", [])),
                    }
            memory = self.backend.read(
                memory_id,
                agent_id=txn["agent_id"],
                scope=scope,
                txn_id=txn_id,
            )
            if memory is None or memory.get("scope") != scope:
                return None
            self._remember_version(txn, memory_id)
            return memory

    def revoke(self, agent_id: str, action: str, scope: str) -> None:
        with self._lock:
            actor = str(agent_id)
            self._policy_versions[actor] += 1
            self._revoked.add((actor, str(action), str(scope)))
            recorder = getattr(self.backend, "record_control_event", None)
            if callable(recorder):
                recorder(
                    "policy_revoke",
                    agent_id=actor,
                    action=str(action),
                    scope=str(scope),
                    policy_version=self._policy_versions[actor],
                )

    def _is_revoked(self, txn: Mapping[str, Any], action: Mapping[str, Any]) -> bool:
        actor = str(txn["agent_id"])
        action_type = str(action["type"])
        scope = str(action.get("scope") or "*")
        if any(
            (candidate_actor, action_type, candidate_scope) in self._revoked
            for candidate_actor in (actor, "*")
            for candidate_scope in (scope, "*")
        ):
            return True
        if not self.policies:
            return False
        candidates = [
            policy
            for policy in self.policies
            if policy.get("agent_id") in {None, actor}
            and policy.get("action") == action_type
            and policy.get("scope") in {None, scope}
        ]
        return not candidates or not any(
            policy.get("effect") == "allow" for policy in candidates
        )

    def _apply(self, action: Mapping[str, Any]) -> None:
        action_type = str(action["type"])
        arguments = {
            key: copy.deepcopy(value)
            for key, value in action.items()
            if key != "type"
        }
        if action_type == "propagate" and "source_id" not in arguments:
            source_ids = list(arguments.pop("source_ids", []))
            if not source_ids:
                raise ValueError("propagate requires source_id")
            arguments["source_id"] = source_ids[0]
        getattr(self.backend, action_type)(**arguments)

    def _capture_prior_states(self, txn: dict[str, Any]) -> None:
        for action in txn["actions"]:
            memory_id = self._memory_id(action)
            if memory_id is not None:
                txn["prior_states"].setdefault(
                    memory_id, self._raw_memory(memory_id)
                )

    def _restore_memory(
        self, memory_id: str, prior: Mapping[str, Any] | None
    ) -> None:
        if prior is None:
            self.backend.delete(memory_id, agent_id="txnmem_compensator")
            return
        self.backend.delete(memory_id, agent_id="txnmem_compensator")
        fields = {
            key: copy.deepcopy(value)
            for key, value in prior.items()
            if key not in {"memory_id", "value", "derived_from"}
        }
        fields["source_ids"] = list(prior.get("derived_from", []))
        self.backend.write(
            memory_id,
            value=copy.deepcopy(prior.get("value")),
            **fields,
        )

    def _verify_prior(self, txn: Mapping[str, Any]) -> str:
        try:
            for memory_id, prior in txn["prior_states"].items():
                if self._fingerprint(self._raw_memory(memory_id)) != self._fingerprint(
                    prior
                ):
                    return "partial_visible"
        except Exception:
            return "unknown"
        return "aborted_clean"

    def _verify_actions(self, actions: list[dict[str, Any]]) -> tuple[str, Any]:
        verifier = getattr(self.backend, "verify_persistent_state", None)
        if callable(verifier):
            try:
                readback = verifier(actions)
            except Exception as exc:
                return "unknown", {"error": type(exc).__name__}
            mapping = {
                "complete": "committed_complete",
                "unknown": "unknown",
                "partial": "partial_visible",
                "absent": "partial_visible",
            }
            return mapping.get(str(readback.get("classification")), "unknown"), readback

        items = []
        try:
            snapshot = self.backend.snapshot()
        except Exception as exc:
            return "unknown", {"error": type(exc).__name__}
        for action in actions:
            memory_id = self._memory_id(action)
            if memory_id is None:
                continue
            memory = snapshot.get(memory_id)
            if action["type"] == "delete":
                matches = memory is None
            elif action["type"] == "invalidate":
                matches = memory is not None and memory.get("status") == "invalid"
            else:
                matches = memory is not None and memory.get("status") == "active"
            items.append(
                {
                    "memory_id": memory_id,
                    "present": memory is not None,
                    "matches": bool(matches),
                }
            )
        if items and all(item["matches"] for item in items):
            return "committed_complete", {"classification": "complete", "items": items}
        return "partial_visible", {"classification": "partial", "items": items}

    def _record_outcome(
        self,
        txn_id: str,
        txn: dict[str, Any],
        classification: str,
        state_readback: Any,
    ) -> dict[str, Any]:
        txn["state"] = classification
        linearization_index = len(self._commit_order) + 1
        order = {
            "linearization_index": linearization_index,
            "txn_id": txn_id,
            "agent_id": txn["agent_id"],
            "classification": classification,
        }
        self._commit_order.append(order)
        outcome = {
            **order,
            "state_readback": copy.deepcopy(state_readback),
            "applied_action_count": len(txn["applied_actions"]),
        }
        self._outcomes.append(copy.deepcopy(outcome))
        return outcome

    def commit(self, txn_id: str) -> dict[str, Any]:
        with self._lock:
            txn = self._transaction(txn_id)
            if txn["state"] != "active":
                raise ValueError(f"transaction already resolved: {txn_id}")

            if self.variant == "Naive":
                classification, readback = self._verify_actions(txn["actions"])
                if txn["apply_error"] is not None and classification == "unknown":
                    classification = "unknown"
                return self._record_outcome(
                    txn_id, txn, classification, readback
                )

            if any(self._is_revoked(txn, action) for action in txn["actions"]):
                return self._record_outcome(
                    txn_id,
                    txn,
                    "aborted_clean",
                    {"classification": "absent", "reason": "policy_revoked"},
                )
            if any(
                self._fingerprint(self._raw_memory(memory_id)) != recorded
                for memory_id, recorded in txn["read_versions"].items()
            ):
                return self._record_outcome(
                    txn_id,
                    txn,
                    "aborted_clean",
                    {"classification": "absent", "reason": "read_version_changed"},
                )

            self._capture_prior_states(txn)
            attempted_actions: list[dict[str, Any]] = []
            try:
                for action in txn["actions"]:
                    attempted_actions.append(action)
                    self._apply(action)
                    txn["applied_actions"].append(action)
            except Exception as apply_error:
                compensation_failed = False
                restored: set[str] = set()
                for action in reversed(attempted_actions):
                    memory_id = self._memory_id(action)
                    if memory_id is None or memory_id in restored:
                        continue
                    restored.add(memory_id)
                    try:
                        self._restore_memory(
                            memory_id, txn["prior_states"].get(memory_id)
                        )
                    except Exception:
                        compensation_failed = True
                classification = (
                    "unknown" if compensation_failed else self._verify_prior(txn)
                )
                return self._record_outcome(
                    txn_id,
                    txn,
                    classification,
                    {
                        "classification": classification,
                        "apply_error": type(apply_error).__name__,
                        "compensation_failed": compensation_failed,
                    },
                )

            classification, readback = self._verify_actions(txn["actions"])
            return self._record_outcome(txn_id, txn, classification, readback)

    def invalidate(self, agent_id: str, memory_id: str) -> dict[str, Any]:
        with self._lock:
            snapshot = self.backend.snapshot()
            if memory_id not in snapshot:
                return {
                    "classification": "unknown",
                    "invalidated_memory_ids": [],
                    "state_readback": {"reason": "root_missing"},
                }
            children: dict[str, list[str]] = defaultdict(list)
            for candidate_id, memory in snapshot.items():
                for source_id in memory.get("derived_from", []):
                    children[str(source_id)].append(str(candidate_id))
            invalidated: list[str] = []
            queue = deque([str(memory_id)])
            try:
                while queue:
                    current = queue.popleft()
                    if current in invalidated:
                        continue
                    self.backend.invalidate(current, agent_id=agent_id)
                    invalidated.append(current)
                    if self.variant == "TxnMem":
                        queue.extend(children.get(current, []))
                verifier = getattr(self.backend, "verify_persistent_state", None)
                if callable(verifier):
                    readback = verifier(
                        [
                            {"type": "invalidate", "memory_id": item}
                            for item in invalidated
                        ]
                    )
                    complete = readback.get("classification") == "complete"
                else:
                    readback = self.backend.snapshot()
                    complete = all(
                        readback.get(item, {}).get("status") == "invalid"
                        for item in invalidated
                    )
            except Exception as exc:
                return {
                    "classification": "unknown",
                    "invalidated_memory_ids": invalidated,
                    "state_readback": {"error": type(exc).__name__},
                }
            return {
                "classification": (
                    "invalidated_complete" if complete else "partial_visible"
                ),
                "invalidated_memory_ids": invalidated,
                "state_readback": copy.deepcopy(readback),
            }

    def report(self) -> dict[str, Any]:
        with self._lock:
            return {
                "variant": self.variant,
                "transactions": {
                    txn_id: txn["state"]
                    for txn_id, txn in sorted(self._transactions.items())
                },
                "commit_order": copy.deepcopy(self._commit_order),
                "outcomes": copy.deepcopy(self._outcomes),
            }


def run_concurrent_storage_scenario(
    scenario: Mapping[str, Any],
    coordinator: MultiAgentStorageCoordinator,
) -> dict[str, Any]:
    """Stage two programs, release their commits together, and record order."""

    transactions = [dict(item) for item in scenario.get("transactions", [])]
    if len(transactions) < 2:
        raise ValueError("concurrent storage scenario requires at least two transactions")
    for transaction in transactions:
        coordinator.begin(
            str(transaction["txn_id"]), str(transaction["agent_id"])
        )
        for action in transaction.get("actions", []):
            coordinator.stage(str(transaction["txn_id"]), action)

    barrier = threading.Barrier(len(transactions))
    outcomes: list[dict[str, Any]] = []
    outcomes_lock = threading.Lock()

    def worker(txn_id: str) -> None:
        barrier.wait()
        outcome = coordinator.commit(txn_id)
        with outcomes_lock:
            outcomes.append(outcome)

    threads = [
        threading.Thread(
            target=worker,
            args=(str(transaction["txn_id"]),),
            name=f"txnmem-{transaction['txn_id']}",
        )
        for transaction in transactions
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    report = coordinator.report()
    report["scenario"] = str(scenario.get("name", "concurrent"))
    report["thread_outcomes"] = sorted(
        outcomes, key=lambda item: int(item["linearization_index"])
    )
    return report
