"""Deterministic two-agent workloads with meaningful seeded variation."""

from __future__ import annotations

import copy
import hashlib
import json
import random
from functools import lru_cache
from typing import Any, Iterable, Mapping

from txnmem_interleavings import enumerate_interleavings
from txnmem_schema import DEFAULT_CONFIG, validate_instance


MULTI_AGENT_WORKLOADS = (
    "multi_agent_visibility",
    "interleaved_atomicity",
    "concurrent_supersession",
    "actor_policy_scope",
    "cross_agent_provenance",
    "multi_agent_mixed",
)

PARAMETER_GRID: dict[str, dict[str, tuple[Any, ...]]] = {
    "multi_agent_visibility": {
        "writer": ("agent_1", "agent_2"),
        "txn_size": (1, 2, 3, 4),
        "reader_probe_count": (1, 2),
    },
    "interleaved_atomicity": {
        "txn_size_a": (1, 2, 3, 4),
        "txn_size_b": (1, 2, 3, 4),
        "crash_victim": ("agent_1", "agent_2"),
        "crash_phase": ("before_linearize", "after_linearize"),
    },
    "concurrent_supersession": {
        "first_committer": ("agent_1", "agent_2"),
        "precommit_reads": (1, 2),
        "new_value_count": (1, 2, 3, 4),
    },
    "actor_policy_scope": {
        "revoked_agent": ("agent_1", "agent_2"),
        "revoked_action": ("write", "derive"),
        "probe_type": ("search", "get_by_id"),
        "trigger_slot": (1, 2, 3, 4),
    },
    "cross_agent_provenance": {
        "source_agent": ("agent_1", "agent_2"),
        "depth": (1, 2, 3, 4),
        "branch_factor": (1, 2, 3),
        "invalidation_phase": (
            "before_derived_commit",
            "after_derived_commit",
        ),
    },
    "multi_agent_mixed": {
        "txn_size_a": (1, 2, 3, 4),
        "txn_size_b": (1, 2, 3, 4),
        "revoked_agent": ("agent_1", "agent_2"),
        "depth": (1, 2, 3, 4),
        "crash_phase": ("before_linearize", "after_linearize"),
    },
}


def _stable_seed(workload: str, candidate_index: int) -> int:
    digest = hashlib.sha256(f"{workload}:{candidate_index}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _parameters(workload: str, candidate_index: int) -> dict[str, Any]:
    quotient = candidate_index
    result: dict[str, Any] = {}
    for name, values in PARAMETER_GRID[workload].items():
        result[name] = values[quotient % len(values)]
        quotient //= len(values)
    return result


def _operation(
    agent_id: str,
    program_index: int,
    operation_type: str,
    *,
    txn_id: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    operation = {
        "op_id": f"{agent_id}_op_{program_index:03d}",
        "step": program_index,
        "program_index": program_index,
        "agent_id": agent_id,
        "type": operation_type,
    }
    if txn_id is not None:
        operation["txn_id"] = txn_id
    operation.update(fields)
    return operation


def _memory(
    memory_id: str,
    agent_id: str,
    *,
    scope: str = "tenant:shared",
    value: Any | None = None,
    attribute: str = "fact",
    status: str = "active",
    memory_role: str | None = None,
) -> dict[str, Any]:
    return {
        "memory_id": memory_id,
        "memory_role": memory_role or memory_id,
        "agent_id": agent_id,
        "scope": scope,
        "entity_id": "shared_entity",
        "attribute": attribute,
        "value": memory_id if value is None else value,
        "status": status,
        "version": 1,
        "policy_version": 1,
        "supersedes_id": None,
        "derived_from": [],
    }


def _policies(scopes_by_agent: Mapping[str, Iterable[str]]) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    actions = (
        "write",
        "read",
        "search",
        "derive",
        "propagate",
        "supersede",
        "invalidate",
    )
    for agent_id, scopes in scopes_by_agent.items():
        for scope in scopes:
            for action in actions:
                policies.append(
                    {
                        "policy_id": f"allow_{agent_id}_{action}_{scope.replace(':', '_')}",
                        "version": 1,
                        "agent_id": agent_id,
                        "action": action,
                        "scope": scope,
                        "effect": "allow",
                        "effective_step": 0,
                    }
                )
    return policies


def _topological_merge(
    sequences: Mapping[str, list[dict[str, Any]]],
    precedence: Iterable[tuple[str, str]],
    *,
    seed: int,
) -> list[dict[str, Any]]:
    nodes = {
        operation["op_id"]: copy.deepcopy(operation)
        for sequence in sequences.values()
        for operation in sequence
    }
    predecessors: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    successors: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    for sequence in sequences.values():
        for left, right in zip(sequence, sequence[1:]):
            predecessors[right["op_id"]].add(left["op_id"])
            successors[left["op_id"]].add(right["op_id"])
    for left, right in precedence:
        predecessors[right].add(left)
        successors[left].add(right)

    rng = random.Random(seed)
    ready = sorted(node_id for node_id, incoming in predecessors.items() if not incoming)
    ordered: list[dict[str, Any]] = []
    while ready:
        selected_index = rng.randrange(len(ready))
        node_id = ready.pop(selected_index)
        ordered.append(nodes[node_id])
        for successor in sorted(successors[node_id]):
            predecessors[successor].discard(node_id)
            if not predecessors[successor] and successor not in {
                item["op_id"] for item in ordered
            } and successor not in ready:
                ready.append(successor)
        ready.sort()
    if len(ordered) != len(nodes):
        raise ValueError("multi-agent precedence graph contains a cycle")
    for step, operation in enumerate(ordered, start=1):
        operation["step"] = step
    return ordered


def _base_config(parameters: Mapping[str, Any]) -> dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    config.update(
        {
            "agent_count": 2,
            "concurrency": 2,
            "txn_size": max(
                int(parameters.get("txn_size", 1)),
                int(parameters.get("txn_size_a", 1)),
                int(parameters.get("txn_size_b", 1)),
            ),
            "provenance_depth": int(parameters.get("depth", 1)),
            "branch_factor": int(parameters.get("branch_factor", 1)),
            "policy_churn": int("revoked_agent" in parameters),
            "sampled_parameters": dict(parameters),
        }
    )
    return config


def _visibility_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    writer = str(parameters["writer"])
    reader = "agent_2" if writer == "agent_1" else "agent_1"
    writer_txn = f"txn_{writer}"
    reader_txn = f"txn_{reader}"
    writer_ops = [_operation(writer, 1, "begin_txn", txn_id=writer_txn)]
    for index in range(1, int(parameters["txn_size"]) + 1):
        writer_ops.append(
            _operation(
                writer,
                index + 1,
                "write",
                txn_id=writer_txn,
                memory_id=f"m_visibility_{index}",
                memory_role=f"writer_item_{index}",
                value=f"visibility_value_{index}",
                scope="tenant:shared",
                source_ids=[],
                policy_version=1,
            )
        )
    writer_commit = _operation(
        writer, len(writer_ops) + 1, "commit", txn_id=writer_txn
    )
    writer_ops.append(writer_commit)

    reader_ops = [_operation(reader, 1, "begin_txn", txn_id=reader_txn)]
    for index in range(1, int(parameters["reader_probe_count"]) + 1):
        reader_ops.append(
            _operation(
                reader,
                len(reader_ops) + 1,
                "get_by_id",
                txn_id=reader_txn,
                memory_id="m_visibility_1",
                memory_role="writer_item_1",
                scope="tenant:shared",
                probe_role="before_writer_commit",
            )
        )
    post_probe = _operation(
        reader,
        len(reader_ops) + 1,
        "get_by_id",
        txn_id=reader_txn,
        memory_id="m_visibility_1",
        memory_role="writer_item_1",
        scope="tenant:shared",
        probe_role="after_writer_commit",
    )
    reader_ops.append(post_probe)
    reader_ops.append(
        _operation(reader, len(reader_ops) + 1, "commit", txn_id=reader_txn)
    )
    precedence = [
        (writer_ops[-2]["op_id"], reader_ops[1]["op_id"]),
        (reader_ops[-3]["op_id"], writer_commit["op_id"]),
        (writer_commit["op_id"], post_probe["op_id"]),
    ]
    return {writer: writer_ops, reader: reader_ops}, precedence, [], []


def _atomicity_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    sequences: dict[str, list[dict[str, Any]]] = {}
    for agent_id, size_key in (("agent_1", "txn_size_a"), ("agent_2", "txn_size_b")):
        txn_id = f"txn_{agent_id}"
        operations = [_operation(agent_id, 1, "begin_txn", txn_id=txn_id)]
        for index in range(1, int(parameters[size_key]) + 1):
            operations.append(
                _operation(
                    agent_id,
                    index + 1,
                    "write",
                    txn_id=txn_id,
                    memory_id=f"m_{agent_id}_{index}",
                    memory_role=f"{agent_id}_item_{index}",
                    value=f"{agent_id}_value_{index}",
                    scope="tenant:shared",
                    source_ids=[],
                    policy_version=1,
                )
            )
        operations.append(
            _operation(agent_id, len(operations) + 1, "commit", txn_id=txn_id)
        )
        sequences[agent_id] = operations
    victim = str(parameters["crash_victim"])
    survivor = "agent_2" if victim == "agent_1" else "agent_1"
    victim_commit = sequences[victim][-1]
    survivor_commit = sequences[survivor][-1]
    phase = str(parameters["crash_phase"])
    trigger_key = "before_operation" if phase == "before_linearize" else "after_operation"
    failure_schedule = [
        {
            "trigger": {trigger_key: victim_commit["op_id"]},
            "type": "crash",
            "target": f"txn_{victim}",
            "agent_id": victim,
            "phase": phase,
        }
    ]
    return sequences, [(survivor_commit["op_id"], victim_commit["op_id"])], [], failure_schedule


def _supersession_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    sequences: dict[str, list[dict[str, Any]]] = {}
    for agent_id in ("agent_1", "agent_2"):
        txn_id = f"txn_{agent_id}"
        operations = [_operation(agent_id, 1, "begin_txn", txn_id=txn_id)]
        for probe in range(int(parameters["precommit_reads"])):
            operations.append(
                _operation(
                    agent_id,
                    len(operations) + 1,
                    "read",
                    txn_id=txn_id,
                    memory_id="m_shared_old",
                    memory_role="shared_old",
                    scope="tenant:shared",
                    probe_role=f"base_version_read_{probe + 1}",
                )
            )
        new_id = f"m_{agent_id}_new"
        operations.append(
            _operation(
                agent_id,
                len(operations) + 1,
                "write",
                txn_id=txn_id,
                memory_id=new_id,
                memory_role=f"{agent_id}_new",
                value=f"{agent_id}_candidate_{parameters['new_value_count']}",
                entity_id="shared_entity",
                attribute="preference",
                base_version=1,
                scope="tenant:shared",
                source_ids=[],
                policy_version=1,
                supersedes_id="m_shared_old",
            )
        )
        operations.append(
            _operation(
                agent_id,
                len(operations) + 1,
                "supersede",
                txn_id=txn_id,
                old_memory_id="m_shared_old",
                new_memory_id=new_id,
                old_memory_role="shared_old",
                new_memory_role=f"{agent_id}_new",
            )
        )
        operations.append(
            _operation(agent_id, len(operations) + 1, "commit", txn_id=txn_id)
        )
        sequences[agent_id] = operations
    first = str(parameters["first_committer"])
    second = "agent_2" if first == "agent_1" else "agent_1"
    first_commit = sequences[first][-1]
    second_commit = sequences[second][-1]
    precedence = [
        (sequences["agent_1"][int(parameters["precommit_reads"])]["op_id"], first_commit["op_id"]),
        (sequences["agent_2"][int(parameters["precommit_reads"])]["op_id"], first_commit["op_id"]),
        (first_commit["op_id"], second_commit["op_id"]),
    ]
    initial = [
        _memory(
            "m_shared_old",
            "agent_1",
            value="old_preference",
            attribute="preference",
            memory_role="shared_old",
        )
    ]
    return sequences, precedence, initial, []


def _policy_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    sequences: dict[str, list[dict[str, Any]]] = {}
    initial = [
        _memory(
            "m_private_1",
            "agent_1",
            scope="tenant:user_001",
            value="private_1",
            memory_role="private_1",
        ),
        _memory(
            "m_private_2",
            "agent_2",
            scope="tenant:user_002",
            value="private_2",
            memory_role="private_2",
        ),
    ]
    for agent_id, own_scope, other_index in (
        ("agent_1", "tenant:user_001", 2),
        ("agent_2", "tenant:user_002", 1),
    ):
        txn_id = f"txn_{agent_id}"
        operations = [_operation(agent_id, 1, "begin_txn", txn_id=txn_id)]
        if parameters["probe_type"] == "search":
            operations.append(
                _operation(
                    agent_id,
                    2,
                    "search",
                    txn_id=txn_id,
                    query=f"private_{other_index}",
                    scope=own_scope,
                    probe_role="cross_scope_probe",
                )
            )
        else:
            operations.append(
                _operation(
                    agent_id,
                    2,
                    "get_by_id",
                    txn_id=txn_id,
                    memory_id=f"m_private_{other_index}",
                    memory_role=f"private_{other_index}",
                    scope=own_scope,
                    probe_role="cross_scope_probe",
                )
            )
        if parameters["revoked_action"] == "derive":
            operations.append(
                _operation(
                    agent_id,
                    3,
                    "derive",
                    txn_id=txn_id,
                    memory_id=f"m_{agent_id}_derived",
                    memory_role=f"{agent_id}_derived",
                    source_ids=[f"m_private_{1 if agent_id == 'agent_1' else 2}"],
                    source_roles=[f"private_{1 if agent_id == 'agent_1' else 2}"],
                    value=f"derived_by_{agent_id}",
                    scope=own_scope,
                    policy_version=1,
                )
            )
        else:
            operations.append(
                _operation(
                    agent_id,
                    3,
                    "write",
                    txn_id=txn_id,
                    memory_id=f"m_{agent_id}_write",
                    memory_role=f"{agent_id}_write",
                    value=f"written_by_{agent_id}",
                    scope=own_scope,
                    source_ids=[],
                    policy_version=1,
                )
            )
        operations.append(_operation(agent_id, 4, "commit", txn_id=txn_id))
        sequences[agent_id] = operations
    revoked = str(parameters["revoked_agent"])
    revoked_scope = "tenant:user_001" if revoked == "agent_1" else "tenant:user_002"
    failure_schedule = [
        {
            "trigger": {"before_operation": sequences[revoked][-1]["op_id"]},
            "type": "revoke",
            "target": str(parameters["revoked_action"]),
            "agent_id": revoked,
            "scope": revoked_scope,
            "phase": "before_validate",
            "trigger_slot": int(parameters["trigger_slot"]),
        }
    ]
    return sequences, [], initial, failure_schedule


def _provenance_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    source_agent = str(parameters["source_agent"])
    derived_agent = "agent_2" if source_agent == "agent_1" else "agent_1"
    source_txn = f"txn_{source_agent}"
    derived_txn = f"txn_{derived_agent}"
    source_ops = [
        _operation(source_agent, 1, "begin_txn", txn_id=source_txn),
        _operation(
            source_agent,
            2,
            "write",
            txn_id=source_txn,
            memory_id="m_cross_root",
            memory_role="cross_root",
            value="cross_agent_source",
            scope="tenant:shared",
            source_ids=[],
            policy_version=1,
        ),
        _operation(source_agent, 3, "commit", txn_id=source_txn),
    ]
    derived_ops = [_operation(derived_agent, 1, "begin_txn", txn_id=derived_txn)]
    for branch in range(1, int(parameters["branch_factor"]) + 1):
        previous_id = "m_cross_root"
        previous_role = "cross_root"
        for level in range(1, int(parameters["depth"]) + 1):
            current_id = f"m_cross_b{branch}_d{level}"
            current_role = f"branch_{branch}_depth_{level}"
            derived_ops.append(
                _operation(
                    derived_agent,
                    len(derived_ops) + 1,
                    "derive",
                    txn_id=derived_txn,
                    memory_id=current_id,
                    memory_role=current_role,
                    source_ids=[previous_id],
                    source_roles=[previous_role],
                    value=current_role,
                    scope="tenant:shared",
                    policy_version=1,
                )
            )
            previous_id = current_id
            previous_role = current_role
    derived_ops.append(
        _operation(derived_agent, len(derived_ops) + 1, "commit", txn_id=derived_txn)
    )
    precedence = [(source_ops[-1]["op_id"], derived_ops[1]["op_id"])]
    trigger_key = (
        "before_operation"
        if parameters["invalidation_phase"] == "before_derived_commit"
        else "after_operation"
    )
    failure_schedule = [
        {
            "trigger": {trigger_key: derived_ops[-1]["op_id"]},
            "type": "invalidate",
            "target": "m_cross_root",
            "memory_id": "m_cross_root",
            "memory_role": "cross_root",
            "agent_id": source_agent,
            "phase": str(parameters["invalidation_phase"]),
        }
    ]
    return {source_agent: source_ops, derived_agent: derived_ops}, precedence, [], failure_schedule


def _mixed_program(parameters: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], list[tuple[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    sequences: dict[str, list[dict[str, Any]]] = {}
    for agent_id, size_key in (("agent_1", "txn_size_a"), ("agent_2", "txn_size_b")):
        txn_id = f"txn_{agent_id}"
        operations = [
            _operation(agent_id, 1, "begin_txn", txn_id=txn_id),
            _operation(
                agent_id,
                2,
                "read",
                txn_id=txn_id,
                memory_id="m_mix_root",
                memory_role="mix_root",
                scope="tenant:shared",
                probe_role="mixed_source_read",
            ),
        ]
        for index in range(1, int(parameters[size_key]) + 1):
            operations.append(
                _operation(
                    agent_id,
                    len(operations) + 1,
                    "write",
                    txn_id=txn_id,
                    memory_id=f"m_mix_{agent_id}_{index}",
                    memory_role=f"{agent_id}_mixed_item_{index}",
                    value=f"mixed_{agent_id}_{index}",
                    scope="tenant:shared",
                    source_ids=[],
                    policy_version=1,
                )
            )
        operations.append(
            _operation(
                agent_id,
                len(operations) + 1,
                "derive",
                txn_id=txn_id,
                memory_id=f"m_mix_{agent_id}_derived",
                memory_role=f"{agent_id}_mixed_derived",
                source_ids=["m_mix_root"],
                source_roles=["mix_root"],
                value=f"mixed_derived_{agent_id}_{parameters['depth']}",
                scope="tenant:shared",
                policy_version=1,
            )
        )
        operations.append(
            _operation(agent_id, len(operations) + 1, "commit", txn_id=txn_id)
        )
        sequences[agent_id] = operations
    revoked = str(parameters["revoked_agent"])
    survivor = "agent_2" if revoked == "agent_1" else "agent_1"
    precedence = [
        (sequences["agent_1"][-2]["op_id"], sequences[revoked][-1]["op_id"]),
        (sequences["agent_2"][-2]["op_id"], sequences[revoked][-1]["op_id"]),
        (sequences[revoked][-1]["op_id"], sequences[survivor][-1]["op_id"]),
    ]
    crash_phase = str(parameters["crash_phase"])
    crash_trigger = "before_operation" if crash_phase == "before_linearize" else "after_operation"
    failure_schedule = [
        {
            "trigger": {"before_operation": sequences[revoked][-1]["op_id"]},
            "type": "revoke",
            "target": "write",
            "agent_id": revoked,
            "scope": "tenant:shared",
            "phase": "before_validate",
        },
        {
            "trigger": {"before_operation": sequences[revoked][-1]["op_id"]},
            "type": "invalidate",
            "target": "m_mix_root",
            "memory_id": "m_mix_root",
            "memory_role": "mix_root",
            "phase": "before_validate",
        },
        {
            "trigger": {crash_trigger: sequences[survivor][-1]["op_id"]},
            "type": "crash",
            "target": f"txn_{survivor}",
            "agent_id": survivor,
            "phase": crash_phase,
        },
    ]
    initial = [
        _memory(
            "m_mix_root",
            "agent_1",
            value="mixed_root",
            memory_role="mix_root",
        )
    ]
    return sequences, precedence, initial, failure_schedule


PROGRAM_BUILDERS = {
    "multi_agent_visibility": _visibility_program,
    "interleaved_atomicity": _atomicity_program,
    "concurrent_supersession": _supersession_program,
    "actor_policy_scope": _policy_program,
    "cross_agent_provenance": _provenance_program,
    "multi_agent_mixed": _mixed_program,
}


def _build_candidate(workload: str, candidate_index: int) -> dict[str, Any]:
    parameters = _parameters(workload, candidate_index)
    sequences, precedence, initial_memories, failure_schedule = PROGRAM_BUILDERS[workload](parameters)
    operations = _topological_merge(
        sequences,
        precedence,
        seed=_stable_seed(workload, candidate_index),
    )
    if workload == "actor_policy_scope":
        scopes = {
            "agent_1": ("tenant:user_001",),
            "agent_2": ("tenant:user_002",),
        }
    else:
        scopes = {"agent_1": ("tenant:shared",), "agent_2": ("tenant:shared",)}
    instance = {
        "instance_id": f"{workload}_candidate_{candidate_index}",
        "workload": workload,
        "seed": candidate_index,
        "evidence_tier": "seeded",
        "config": _base_config(parameters),
        "initial_memories": initial_memories,
        "operations": operations,
        "policies": _policies(scopes),
        "failure_schedule": failure_schedule,
        "provenance_edges": [],
    }
    instance["structure_signature"] = structural_signature(instance)
    validate_instance(instance)
    return instance


def structural_signature(instance: Mapping[str, Any]) -> str:
    operations = list(instance.get("operations", []))
    operation_positions = {
        operation.get("op_id"): index for index, operation in enumerate(operations)
    }
    signature_view = {
        "workload": instance.get("workload"),
        "parameters": dict(instance.get("config", {}).get("sampled_parameters", {})),
        "initial": [
            {
                "agent_id": memory.get("agent_id"),
                "memory_role": memory.get("memory_role"),
                "scope": memory.get("scope"),
                "attribute": memory.get("attribute"),
                "status": memory.get("status"),
            }
            for memory in instance.get("initial_memories", [])
        ],
        "operations": [
            {
                "agent_id": operation.get("agent_id"),
                "type": operation.get("type"),
                "txn_id": operation.get("txn_id"),
                "program_index": operation.get("program_index"),
                "scope": operation.get("scope"),
                "probe_role": operation.get("probe_role"),
                "memory_role": operation.get("memory_role"),
                "old_memory_role": operation.get("old_memory_role"),
                "new_memory_role": operation.get("new_memory_role"),
                "source_roles": operation.get("source_roles", []),
                "source_count": len(operation.get("source_ids", [])),
                "base_version": operation.get("base_version"),
            }
            for operation in operations
        ],
        "failure_schedule": [
            {
                "type": event.get("type"),
                "phase": event.get("phase"),
                "agent_id": event.get("agent_id"),
                "target": event.get("target"),
                "memory_role": event.get("memory_role"),
                "trigger": {
                    key: operation_positions.get(value)
                    for key, value in event.get("trigger", {}).items()
                },
            }
            for event in instance.get("failure_schedule", [])
        ],
    }
    encoded = json.dumps(
        signature_view,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@lru_cache(maxsize=None)
def _family_candidates(workload: str) -> tuple[dict[str, Any], ...]:
    if workload not in MULTI_AGENT_WORKLOADS:
        raise ValueError(f"unsupported multi-agent workload: {workload}")
    candidates: list[dict[str, Any]] = []
    signatures: set[str] = set()
    for candidate_index in range(20000):
        instance = _build_candidate(workload, candidate_index)
        signature = instance["structure_signature"]
        if signature in signatures:
            continue
        signatures.add(signature)
        candidates.append(instance)
        if len(candidates) == 50:
            return tuple(candidates)
    raise ValueError(f"fewer than fifty unique candidates for {workload}")


def generate_multi_agent_instance(workload: str, seed: int) -> dict[str, Any]:
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 50:
        raise ValueError("multi-agent seed must be an integer in [0, 49]")
    instance = copy.deepcopy(_family_candidates(workload)[seed])
    instance["seed"] = seed
    instance["instance_id"] = f"{workload}_seed_{seed}"
    validate_instance(instance)
    return instance


def generate_multi_agent_suite(
    seeds: Iterable[int] = range(50),
) -> list[dict[str, Any]]:
    return [
        generate_multi_agent_instance(workload, int(seed))
        for workload in MULTI_AGENT_WORKLOADS
        for seed in seeds
    ]


def _valid_exhaustive_order(workload: str, operations: list[dict[str, Any]], base: Mapping[str, Any]) -> bool:
    positions = {operation["op_id"]: index for index, operation in enumerate(operations)}
    parameters = base["config"]["sampled_parameters"]
    if workload == "multi_agent_visibility":
        writer = parameters["writer"]
        writer_ops = [op for op in operations if op["agent_id"] == writer]
        pre = [op for op in operations if op.get("probe_role") == "before_writer_commit"]
        post = [op for op in operations if op.get("probe_role") == "after_writer_commit"]
        return bool(pre and post) and positions[writer_ops[-2]["op_id"]] < positions[pre[0]["op_id"]] < positions[writer_ops[-1]["op_id"]] < positions[post[0]["op_id"]]
    if workload == "interleaved_atomicity":
        victim = parameters["crash_victim"]
        survivor = "agent_2" if victim == "agent_1" else "agent_1"
        victim_commit = next(op for op in operations if op["agent_id"] == victim and op["type"] == "commit")
        survivor_commit = next(op for op in operations if op["agent_id"] == survivor and op["type"] == "commit")
        return positions[survivor_commit["op_id"]] < positions[victim_commit["op_id"]]
    if workload == "concurrent_supersession":
        first = parameters["first_committer"]
        second = "agent_2" if first == "agent_1" else "agent_1"
        first_commit = next(op for op in operations if op["agent_id"] == first and op["type"] == "commit")
        second_commit = next(op for op in operations if op["agent_id"] == second and op["type"] == "commit")
        reads = [op for op in operations if op["type"] == "read"]
        return all(positions[op["op_id"]] < positions[first_commit["op_id"]] for op in reads) and positions[first_commit["op_id"]] < positions[second_commit["op_id"]]
    if workload == "cross_agent_provenance":
        source_agent = parameters["source_agent"]
        source_commit = next(op for op in operations if op["agent_id"] == source_agent and op["type"] == "commit")
        first_derive = next(op for op in operations if op["type"] == "derive")
        return positions[source_commit["op_id"]] < positions[first_derive["op_id"]]
    return True


def exhaustive_multi_agent_instances() -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    core_workloads = MULTI_AGENT_WORKLOADS[:-1]
    for workload in core_workloads:
        base = generate_multi_agent_instance(workload, 0)
        sequences = [
            sorted(
                [op for op in base["operations"] if op["agent_id"] == agent_id],
                key=lambda operation: operation["program_index"],
            )
            for agent_id in ("agent_1", "agent_2")
        ]
        interleavings = enumerate_interleavings(sequences)
        for interleaving_index, operations in enumerate(interleavings):
            if not _valid_exhaustive_order(workload, operations, base):
                continue
            instance = copy.deepcopy(base)
            instance["operations"] = []
            for step, operation in enumerate(operations, start=1):
                item = copy.deepcopy(operation)
                item["step"] = step
                instance["operations"].append(item)
            instance["seed"] = interleaving_index
            instance["instance_id"] = f"{workload}_exhaustive_{interleaving_index:04d}"
            instance["evidence_tier"] = "exhaustive"
            instance["config"]["sampled_parameters"]["exhaustive_program"] = 1
            instance["structure_signature"] = structural_signature(instance)
            try:
                validate_instance(instance)
            except ValueError:
                continue
            output.append(instance)
    return output
