"""Deterministic TxnMemBench workload generators."""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Mapping, Sequence
from typing import Any, Iterable

from txnmem_schema import DEFAULT_CONFIG, validate_instance, validate_parameter_ranges


WORKLOADS = (
    "atomic_multi_write",
    "crash_during_commit",
    "revoke_before_commit",
    "scope_bypass",
    "supersession_consistency",
    "provenance_chain_repair",
    "provenance_branch_repair",
    "mixed_stress",
)

WORKLOAD_SEMANTIC_PARAMETERS = {
    "atomic_multi_write": ("txn_size",),
    "crash_during_commit": (),
    "revoke_before_commit": ("policy_churn", "concurrency"),
    "scope_bypass": (),
    "supersession_consistency": ("concurrency",),
    "provenance_chain_repair": ("provenance_depth", "concurrency"),
    "provenance_branch_repair": ("provenance_depth", "branch_factor", "concurrency"),
    "mixed_stress": (),
}


def _merged_config(config: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(DEFAULT_CONFIG)
    if config:
        merged.update(config)
    for name in ("agent_count", "txn_size", "provenance_depth", "branch_factor", "concurrency"):
        if int(merged[name]) < 1:
            raise ValueError(f"{name} must be >= 1")
    if int(merged["policy_churn"]) < 0:
        raise ValueError("policy_churn must be >= 0")
    return merged


def sample_semantic_config(
    workload: str, seed: int, ranges: Mapping[str, Sequence[int]]
) -> dict[str, int]:
    """Sample every inclusive range with a process-stable SHA-256 source."""

    validated = validate_parameter_ranges(ranges)
    sampled: dict[str, int] = {}
    for name in WORKLOAD_SEMANTIC_PARAMETERS.get(workload, ()):
        if name not in validated:
            continue
        low, high = validated[name]
        digest = hashlib.sha256(f"{workload}\0{seed}\0{name}".encode("utf-8")).digest()
        sampled[name] = low + (int.from_bytes(digest, "big") % (high - low + 1))
    return sampled


def _semantic_id_category(name: str) -> str | None:
    if name in {"memory_id", "old_memory_id", "new_memory_id", "supersedes_id", "source_id", "derived_id", "output_id"}:
        return "memory"
    if name in {"source_ids", "derived_from"}:
        return "memory"
    if name in {"op_id", "operation_id", "before_operation", "after_operation"}:
        return "operation"
    if name == "txn_id":
        return "transaction"
    if name == "policy_id":
        return "policy"
    if name == "agent_id":
        return "agent"
    if name == "entity_id":
        return "entity"
    return None


def _known_semantic_labels(instance: Mapping[str, Any]) -> dict[str, set[str]]:
    known: dict[str, set[str]] = {}

    def collect(value: Any, name: str | None = None) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                collect(item, str(key))
            return
        if isinstance(value, list):
            for item in value:
                collect(item, name)
            return
        category = _semantic_id_category(name or "")
        if category is not None and isinstance(value, str):
            known.setdefault(category, set()).add(value)

    collect(instance)
    return known


def _target_reference_category(event: Mapping[str, Any], known: Mapping[str, set[str]]) -> str | None:
    target = event.get("target")
    if not isinstance(target, str):
        return None
    event_type = event.get("type") or event.get("action")
    if event_type in {"crash", "crash_during_commit"}:
        for category in ("transaction", "operation"):
            if target in known.get(category, set()):
                return category
    if event_type == "invalidate" and target in known.get("memory", set()):
        return "memory"
    return None


def _normalized_semantic_shape(instance: Mapping[str, Any]) -> dict[str, Any]:
    labels: dict[str, dict[str, str]] = {}
    known = _known_semantic_labels(instance)

    def normalize(value: Any, name: str | None = None, category: str | None = None) -> Any:
        if name in {"instance_id", "seed", "semantic_fingerprint"}:
            return None
        if isinstance(value, Mapping):
            target_category = _target_reference_category(value, known)
            return {
                str(key): normalize(
                    item,
                    str(key),
                    target_category if key == "target" else None,
                )
                for key, item in sorted(value.items(), key=lambda item: str(item[0]))
                if key not in {"instance_id", "seed", "semantic_fingerprint"}
            }
        if isinstance(value, list):
            return [normalize(item, name, category) for item in value]
        category = category or _semantic_id_category(name or "")
        if category is not None and isinstance(value, str):
            category_labels = labels.setdefault(category, {})
            if value not in category_labels:
                category_labels[value] = f"{category}_{len(category_labels) + 1:03d}"
            return category_labels[value]
        return value

    return normalize(instance)


def semantic_fingerprint(instance: Mapping[str, Any]) -> str:
    """Fingerprint the instance shape without incidental IDs, seeds, or agent labels."""

    encoded = json.dumps(
        _normalized_semantic_shape(instance), ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _memory(memory_id: str, status: str = "active", **extra: Any) -> dict[str, Any]:
    value = {
        "memory_id": memory_id,
        "agent_id": extra.pop("agent_id", "agent_1"),
        "scope": extra.pop("scope", "tenant:user_001"),
        "entity_id": extra.pop("entity_id", "user_001"),
        "attribute": extra.pop("attribute", "fact"),
        "value": extra.pop("value", memory_id),
        "status": status,
        "policy_version": extra.pop("policy_version", 1),
        "supersedes_id": extra.pop("supersedes_id", None),
        "derived_from": extra.pop("derived_from", []),
    }
    value.update(extra)
    return value


def _operation(number: int, step: int, agent: str, op_type: str, **extra: Any) -> dict[str, Any]:
    value = {
        "op_id": f"op_{number:03d}",
        "step": step,
        "agent_id": agent,
        "type": op_type,
    }
    value.update(extra)
    return value


def _base_instance(workload: str, seed: int, config: dict[str, Any]) -> dict[str, Any]:
    rng = random.Random(seed)
    agent = f"agent_{rng.randrange(config['agent_count']) + 1}"
    return {
        "instance_id": f"{workload}_seed_{seed}",
        "workload": workload,
        "seed": seed,
        "config": config,
        "initial_memories": [],
        "operations": [],
        "policies": [
            {
                "policy_id": "p_write",
                "version": 1,
                "agent_id": agent,
                "action": "write",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            },
            {
                "policy_id": "p_read",
                "version": 1,
                "agent_id": agent,
                "action": "read",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            },
            {
                "policy_id": "p_derive",
                "version": 1,
                "agent_id": agent,
                "action": "derive",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            },
            {
                "policy_id": "p_propagate",
                "version": 1,
                "agent_id": agent,
                "action": "propagate",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            },
        ],
        "failure_schedule": [],
        "provenance_edges": [],
    }


def _interleave_concurrent_transactions(instance: dict[str, Any], concurrency: int) -> None:
    if concurrency <= 1:
        return
    operations = instance["operations"]
    primary_begin_index = next(
        index for index, operation in enumerate(operations) if operation["type"] == "begin_txn"
    )
    primary_begin = operations[primary_begin_index]
    primary_txn_id = str(primary_begin["txn_id"])
    agent = primary_begin["agent_id"]
    probes: list[dict[str, Any]] = []
    for lane in range(2, concurrency + 1):
        txn_id = f"{primary_txn_id}_concurrent_{lane}"
        probes.extend(
            [
                {"op_id": f"concurrent_{lane}_begin", "agent_id": agent, "type": "begin_txn", "txn_id": txn_id},
                {
                    "op_id": f"concurrent_{lane}_read",
                    "agent_id": agent,
                    "type": "read",
                    "txn_id": txn_id,
                    "query": "__concurrent_probe__",
                    "scope": "tenant:user_001",
                },
                {"op_id": f"concurrent_{lane}_commit", "agent_id": agent, "type": "commit", "txn_id": txn_id},
            ]
        )
    starts = [operation for operation in probes if operation["type"] == "begin_txn"]
    reads = [operation for operation in probes if operation["type"] == "read"]
    commits = [operation for operation in probes if operation["type"] == "commit"]
    instance["operations"] = [
        *operations[: primary_begin_index + 1],
        *starts,
        *reads,
        *commits,
        *operations[primary_begin_index + 1 :],
    ]
    for step, operation in enumerate(instance["operations"], start=1):
        operation["step"] = step


def _apply_parameterized_schedules(
    instance: dict[str, Any], semantic_parameters: Mapping[str, int]
) -> None:
    """Add replay-consumed interleavings and policy schedules for sampled semantics."""

    concurrency = semantic_parameters.get("concurrency")
    if concurrency is not None:
        _interleave_concurrent_transactions(instance, concurrency)

    policy_churn = semantic_parameters.get("policy_churn")
    if policy_churn is not None:
        commit_operation = next(
            operation["op_id"]
            for operation in reversed(instance["operations"])
            if operation["type"] == "commit"
        )
        for _ in range(policy_churn):
            instance["failure_schedule"].append(
                {
                    "trigger": {"before_operation": commit_operation},
                    "type": "revoke",
                    "target": "write",
                    "phase": "before_validate",
                }
            )


def _generate_chain(instance: dict[str, Any], agent: str, depth: int) -> None:
    root_id = "m_root"
    instance["initial_memories"].append(_memory(root_id, agent_id=agent, value="source_v1"))
    txn_id = "txn_derive"
    operation_number = len(instance["operations"]) + 1
    instance["operations"].append(_operation(operation_number, 1, agent, "begin_txn", txn_id=txn_id))
    previous = root_id
    for index in range(1, depth + 1):
        current = f"m_derived_{index}"
        read_number = len(instance["operations"]) + 1
        instance["operations"].append(
            _operation(
                read_number,
                len(instance["operations"]) + 1,
                agent,
                "read",
                txn_id=txn_id,
                memory_id=previous,
                scope="tenant:user_001",
            )
        )
        derive_number = len(instance["operations"]) + 1
        instance["operations"].append(
            _operation(
                derive_number,
                len(instance["operations"]) + 1,
                agent,
                "derive",
                txn_id=txn_id,
                memory_id=current,
                source_ids=[previous],
                value=f"derived_v{index}",
                scope="tenant:user_001",
            )
        )
        previous = current
    instance["operations"].append(
        _operation(len(instance["operations"]) + 1, len(instance["operations"]) + 1, agent, "commit", txn_id=txn_id)
    )


def _generate_branches(instance: dict[str, Any], agent: str, branch_factor: int, depth: int) -> None:
    root_id = "m_root"
    instance["initial_memories"].append(_memory(root_id, agent_id=agent, value="source_v1"))
    txn_id = "txn_derive"
    instance["operations"].append(_operation(1, 1, agent, "begin_txn", txn_id=txn_id))
    operation_number = 1
    for branch in range(1, branch_factor + 1):
        previous = root_id
        for level in range(1, depth + 1):
            current = f"m_branch_{branch}_{level}"
            operation_number += 1
            instance["operations"].append(
                _operation(
                    operation_number,
                    operation_number,
                    agent,
                    "read",
                    txn_id=txn_id,
                    memory_id=previous,
                    scope="tenant:user_001",
                )
            )
            operation_number += 1
            instance["operations"].append(
                _operation(
                    operation_number,
                    operation_number,
                    agent,
                    "derive",
                    txn_id=txn_id,
                    memory_id=current,
                    source_ids=[previous],
                    value=f"branch_{branch}_v{level}",
                    scope="tenant:user_001",
                )
            )
            previous = current
    operation_number += 1
    instance["operations"].append(_operation(operation_number, operation_number, agent, "commit", txn_id=txn_id))


def generate_instance(
    workload: str, seed: int, config: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Generate one deterministic workload instance and validate it."""

    if workload not in WORKLOADS:
        raise ValueError(f"unsupported workload: {workload}")
    merged = _merged_config(config)
    instance = _base_instance(workload, seed, merged)
    agent = instance["policies"][0]["agent_id"]

    if workload == "atomic_multi_write":
        txn_size = int(merged["txn_size"])
        instance["operations"].append(_operation(1, 1, agent, "begin_txn", txn_id="txn_001"))
        for index in range(txn_size):
            instance["operations"].append(
                _operation(
                    index + 2,
                    index + 2,
                    agent,
                    "write",
                    txn_id="txn_001",
                    memory_id=f"m_write_{index + 1}",
                    source_ids=[],
                    policy_version=1,
                )
            )
        instance["operations"].append(
            _operation(txn_size + 2, txn_size + 2, agent, "commit", txn_id="txn_001")
        )
        instance["failure_schedule"] = [
            {
                "trigger": {"after_operation": "op_002"},
                "type": "crash",
                "target": "txn_001",
                "phase": "after_operation",
            }
        ]

    elif workload == "crash_during_commit":
        instance["operations"] = [
            _operation(1, 1, agent, "begin_txn", txn_id="txn_001"),
            _operation(2, 2, agent, "write", txn_id="txn_001", memory_id="m_commit", source_ids=[], policy_version=1),
            _operation(3, 3, agent, "commit", txn_id="txn_001"),
        ]
        instance["failure_schedule"] = [
            {"trigger": {"before_operation": "op_003"}, "type": "crash", "target": "commit"}
        ]

    elif workload == "revoke_before_commit":
        instance["operations"] = [
            _operation(1, 1, agent, "begin_txn", txn_id="txn_001"),
            _operation(2, 2, agent, "write", txn_id="txn_001", memory_id="m_protected_write", source_ids=[], policy_version=1),
            _operation(3, 3, agent, "commit", txn_id="txn_001"),
        ]
        instance["failure_schedule"] = [
            {
                "trigger": {"before_operation": "op_003"},
                "type": "revoke",
                "target": "write",
                "phase": "before_validate",
            }
        ]

    elif workload == "scope_bypass":
        private_id = "m_private"
        instance["initial_memories"].append(
            _memory(private_id, agent_id=agent, scope="tenant:user_001", value="private_fact")
        )
        instance["operations"] = [
            _operation(1, 1, agent, "search", query="private_fact", scope="tenant:user_002"),
            _operation(2, 2, agent, "get_by_id", memory_id=private_id, scope="tenant:user_002"),
        ]
        instance["policies"].append(
            {
                "policy_id": "p_scope",
                "version": 1,
                "agent_id": agent,
                "action": "search",
                "scope": "tenant:user_002",
                "effect": "allow",
                "effective_step": 0,
            }
        )

    elif workload == "supersession_consistency":
        instance["initial_memories"] = [
            _memory("m_old", agent_id=agent, value="old_fact"),
            _memory("m_new", agent_id=agent, status="pending", value="new_fact", supersedes_id="m_old"),
        ]
        instance["policies"].append(
            {
                "policy_id": "p_supersede",
                "version": 1,
                "agent_id": agent,
                "action": "supersede",
                "scope": "tenant:user_001",
                "effect": "allow",
                "effective_step": 0,
            }
        )
        instance["operations"] = [
            _operation(1, 1, agent, "begin_txn", txn_id="txn_super"),
            _operation(2, 2, agent, "write", txn_id="txn_super", memory_id="m_new", source_ids=[], policy_version=1, supersedes_id="m_old"),
            _operation(3, 3, agent, "supersede", txn_id="txn_super", old_memory_id="m_old", new_memory_id="m_new"),
            _operation(4, 4, agent, "commit", txn_id="txn_super"),
        ]

    elif workload == "provenance_chain_repair":
        _generate_chain(instance, agent, int(merged["provenance_depth"]))
        instance["operations"].append(
            _operation(len(instance["operations"]) + 1, len(instance["operations"]) + 1, agent, "invalidate", memory_id="m_root", txn_id="txn_repair")
        )

    elif workload == "provenance_branch_repair":
        _generate_branches(instance, agent, int(merged["branch_factor"]), int(merged["provenance_depth"]))
        instance["operations"].append(
            _operation(len(instance["operations"]) + 1, len(instance["operations"]) + 1, agent, "invalidate", memory_id="m_root", txn_id="txn_repair")
        )

    elif workload == "mixed_stress":
        instance["initial_memories"].append(_memory("m_mix_root", agent_id=agent, value="mixed_root"))
        instance["operations"] = [
            _operation(1, 1, agent, "begin_txn", txn_id="txn_mix"),
            _operation(2, 2, agent, "write", txn_id="txn_mix", memory_id="m_mix_1", source_ids=[], policy_version=1),
            _operation(3, 3, agent, "write", txn_id="txn_mix", memory_id="m_mix_2", source_ids=[], policy_version=1),
            _operation(4, 4, agent, "commit", txn_id="txn_mix"),
        ]
        instance["failure_schedule"] = [
            {
                "trigger": {"before_operation": "op_003"},
                "type": "revoke",
                "target": "write",
                "phase": "before_validate",
            },
            {"trigger": {"before_operation": "op_004"}, "type": "crash", "target": "commit"},
        ]

    validate_instance(instance)
    return instance


def generate_suite(
    workloads: Iterable[str] = WORKLOADS,
    seeds: Iterable[int] = range(10),
    config: dict[str, Any] | None = None,
    parameter_ranges: Mapping[str, Sequence[int]] | None = None,
) -> list[dict[str, Any]]:
    instances: list[dict[str, Any]] = []
    validated_ranges = (
        validate_parameter_ranges(parameter_ranges) if parameter_ranges is not None else None
    )
    for workload in workloads:
        for seed in seeds:
            normalized_seed = int(seed)
            if validated_ranges is None:
                instances.append(generate_instance(workload, normalized_seed, config=config))
                continue
            semantic_parameters = sample_semantic_config(workload, normalized_seed, validated_ranges)
            instance = generate_instance(
                workload,
                normalized_seed,
                config={**(config or {}), **semantic_parameters},
            )
            _apply_parameterized_schedules(instance, semantic_parameters)
            instance["semantic_parameters"] = semantic_parameters
            instance["semantic_fingerprint"] = semantic_fingerprint(instance)
            validate_instance(instance)
            instances.append(instance)
    return instances
