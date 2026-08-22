"""Controlled traditional shared-memory baselines for TxnMemBench."""

from __future__ import annotations

import copy
from collections import defaultdict
from typing import Any

from txnmem_adapter_contract import MemoryAdapter, ReplayObservation
from txnmem_schema import validate_instance


def _memory_from_operation(operation: dict[str, Any]) -> dict[str, Any]:
    return {
        "memory_id": operation["memory_id"],
        "agent_id": operation.get("agent_id", "agent_1"),
        "scope": operation.get("scope", "tenant:user_001"),
        "entity_id": operation.get("entity_id", "user_001"),
        "attribute": operation.get("attribute", "fact"),
        "value": operation.get("value", operation["memory_id"]),
        "status": "active",
        "policy_version": operation.get("policy_version", 1),
        "supersedes_id": operation.get("supersedes_id"),
        "derived_from": list(operation.get("source_ids", [])),
    }


def _matches(memory: dict[str, Any], operation: dict[str, Any]) -> bool:
    query = operation.get("query")
    return query is None or query in {
        memory.get("value"),
        memory.get("memory_id"),
        memory.get("attribute"),
    }


def _logical_key(memory: dict[str, Any]) -> tuple[Any, Any, Any]:
    return (memory.get("scope"), memory.get("entity_id"), memory.get("attribute"))


class _ControlledBaselineAdapter(MemoryAdapter):
    """Single-record replay shared by deliberately non-transactional baselines."""

    filters_metadata = False
    uses_last_write_wins = False
    preserves_first_write = False

    def _read_candidates(
        self,
        memories: dict[str, dict[str, Any]],
        logical_index: dict[tuple[Any, Any, Any], str],
        operation: dict[str, Any],
    ) -> list[dict[str, Any] | None]:
        requested_id = operation.get("memory_id")
        if requested_id is not None:
            return [memories.get(requested_id)]
        if not self.uses_last_write_wins:
            return list(memories.values())
        return [memories[memory_id] for memory_id in logical_index.values()]

    def _is_visible(self, memory: dict[str, Any], operation: dict[str, Any]) -> bool:
        if not self.filters_metadata:
            return True
        return (
            memory.get("agent_id") == operation.get("agent_id", memory.get("agent_id"))
            and memory.get("scope") == operation.get("scope", memory.get("scope"))
        )

    def run(self, instance: dict[str, Any]) -> ReplayObservation:
        """Replay one instance without a shared atomic commit or repair pass."""

        validate_instance(instance)
        memories = {
            memory["memory_id"]: copy.deepcopy(memory)
            for memory in instance["initial_memories"]
        }
        logical_index = {
            _logical_key(memory): memory_id for memory_id, memory in memories.items()
        }
        committed_memory_ids: list[str] = []
        trace: list[dict[str, Any]] = []
        current_policy_version = 1
        transaction_state = "active"
        exposed_memory_ids: list[str] = []
        denied_reads = 0
        supersession_updates = 0

        scheduled_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for event in instance["failure_schedule"]:
            scheduled_by_step[int(event["step"])].append(event)

        for operation in instance["operations"]:
            step = int(operation["step"])
            step_events = scheduled_by_step.get(step, [])
            for event in step_events:
                if event["type"] == "revoke":
                    current_policy_version += 1
                    trace.append(
                        {"step": step, "event": "revoke", "policy_version": current_policy_version}
                    )
                elif event["type"] == "delay":
                    trace.append({"step": step, "event": "delay"})

            op_type = operation["type"]
            trace.append({"step": step, "operation": op_type})

            if op_type == "write":
                memory = _memory_from_operation(operation)
                if self.preserves_first_write and memory["memory_id"] in memories:
                    trace.append(
                        {
                            "step": step,
                            "event": "capability_absent",
                            "capability": "duplicate_memory_id",
                            "memory_id": memory["memory_id"],
                        }
                    )
                else:
                    memories[memory["memory_id"]] = memory
                    if self.uses_last_write_wins:
                        logical_index[_logical_key(memory)] = memory["memory_id"]
                    committed_memory_ids.append(memory["memory_id"])

            elif op_type in {"search", "read", "get_by_id"}:
                for memory in self._read_candidates(memories, logical_index, operation):
                    if memory is None or memory.get("status") not in {"active", "pending"}:
                        continue
                    if not _matches(memory, operation):
                        continue
                    if not self._is_visible(memory, operation):
                        denied_reads += 1
                        trace.append(
                            {"step": step, "event": "denied_read", "memory_id": memory["memory_id"]}
                        )
                        continue
                    if memory["memory_id"] not in exposed_memory_ids:
                        exposed_memory_ids.append(memory["memory_id"])
                    trace.append(
                        {"step": step, "event": "exposed_read", "memory_id": memory["memory_id"]}
                    )
                    break

            elif op_type == "supersede":
                if not self.uses_last_write_wins:
                    trace.append(
                        {"step": step, "event": "capability_absent", "capability": "supersession"}
                    )
                else:
                    old_id = operation["old_memory_id"]
                    new_id = operation["new_memory_id"]
                    if old_id not in memories or new_id not in memories:
                        raise KeyError(f"unknown memory_id: {old_id if old_id not in memories else new_id}")
                    memories[old_id]["status"] = "superseded"
                    memories[new_id]["status"] = "active"
                    memories[new_id]["supersedes_id"] = old_id
                    logical_index[_logical_key(memories[new_id])] = new_id
                    supersession_updates += 1

            elif op_type == "invalidate":
                memory_id = operation["memory_id"]
                if memory_id not in memories:
                    raise KeyError(f"unknown memory_id: {memory_id}")
                memories[memory_id]["status"] = "invalid"
                transaction_state = "invalidated"

            elif op_type == "propagate":
                trace.append({"step": step, "event": "capability_absent", "capability": "provenance_propagation"})

            elif op_type == "commit":
                transaction_state = "committed"

            if any(event["type"] == "crash" for event in step_events):
                transaction_state = "partial_commit" if committed_memory_ids else "crashed"
                trace.append({"step": step, "event": "crash"})
                break

        if transaction_state == "active":
            transaction_state = "completed"

        return ReplayObservation(
            transaction_state=transaction_state,
            final_memories=memories,
            committed_memory_ids=committed_memory_ids,
            trace=trace,
            metrics={
                "operation_count": len(trace),
                "repair_count": 0,
                "policy_version_at_end": current_policy_version,
                "exposed_memory_ids": exposed_memory_ids,
                "denied_reads": denied_reads,
                "supersession_updates": supersession_updates,
            },
        )


class AppendOnlyAdapter(_ControlledBaselineAdapter):
    """Immediate append-only record storage without policy filtering."""

    preserves_first_write = True


class LastWriteWinsAdapter(_ControlledBaselineAdapter):
    """Immediate storage with one current record per logical memory field."""

    uses_last_write_wins = True


class MetadataFilteredAdapter(_ControlledBaselineAdapter):
    """Immediate storage with scope metadata checks on all read paths."""

    filters_metadata = True
