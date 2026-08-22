"""Dependency-free contract shared by external-memory baseline adapters."""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CapabilitySupport:
    """One adapter capability and whether its native semantics are available."""

    capability: str
    supported: bool
    detail: str | None = None


class AdapterRunError(RuntimeError):
    """A replay failure with a stable, machine-readable category."""

    category = "runtime_error"


class CapabilityAbsentError(AdapterRunError):
    """Raised when the backend has no native operation with required semantics."""

    category = "capability_absent"


class UnsupportedMappingError(AdapterRunError):
    """Raised when an operation cannot be mapped to the selected adapter."""

    category = "unsupported_mapping"


class RuntimeAdapterError(AdapterRunError):
    """Raised when a selected native call fails at runtime."""

    category = "runtime_error"


CRASH_OUTCOME_STATES = frozenset({"crashed", "partial_commit"})


def _normalize_value(value: Any, path: str) -> Any:
    """Copy only JSON-shaped values across the adapter/oracle boundary."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested_value in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} is not normalized: mapping keys must be strings")
            normalized[key] = _normalize_value(nested_value, f"{path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [
            _normalize_value(nested_value, f"{path}[{index}]")
            for index, nested_value in enumerate(value)
        ]
    raise TypeError(f"{path} is not normalized: {type(value).__name__} is adapter-native")


def _normalize_memories(
    source: Mapping[str, Mapping[str, Any]] | Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    if isinstance(source, Mapping):
        memory_items = source.items()
        records_include_ids = False
    elif isinstance(source, (str, bytes)):
        raise TypeError("final_memories must be a mapping or iterable of memories")
    else:
        memory_items = enumerate(source)
        records_include_ids = True

    normalized_memories: dict[str, dict[str, Any]] = {}
    for source_id, memory in memory_items:
        if not isinstance(memory, Mapping):
            raise TypeError("final_memories entries must be mappings")
        normalized_memory = _normalize_value(memory, "final_memories")
        if records_include_ids:
            memory_id = normalized_memory.get("memory_id")
        else:
            if not isinstance(source_id, str):
                raise TypeError("final_memories mapping keys must be strings")
            memory_id = normalized_memory.setdefault("memory_id", source_id)
            if memory_id != source_id:
                raise ValueError("final_memories key must match memory_id")
        if not isinstance(memory_id, str):
            raise ValueError("memory_id must be a string")
        if memory_id in normalized_memories:
            raise ValueError(f"duplicate memory_id: {memory_id}")
        if "status" not in normalized_memory:
            raise ValueError(f"memory {memory_id} is missing status")
        normalized_memories[memory_id] = normalized_memory
    return normalized_memories


class MemoryAdapter(ABC):
    """Adapter boundary used by the external baseline runner."""

    capabilities: tuple[CapabilitySupport, ...] = ()

    @abstractmethod
    def run(self, instance: dict[str, Any]) -> "ReplayObservation":
        """Replay one dataset instance and return a normalized observation."""


@dataclass(frozen=True)
class ReplayObservation:
    """Adapter output represented only with benchmark-visible values."""

    transaction_state: str
    final_memories: Mapping[str, Mapping[str, Any]] | Iterable[Mapping[str, Any]]
    committed_memory_ids: Iterable[str]
    trace: list[dict[str, Any]]
    metrics: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.transaction_state, str):
            raise TypeError("transaction_state must be a string")
        if not isinstance(self.trace, list):
            raise ValueError("trace must be a list")
        if not isinstance(self.metrics, Mapping):
            raise TypeError("metrics must be a mapping")

        normalized_memories = _normalize_memories(self.final_memories)
        if isinstance(self.committed_memory_ids, (str, bytes)):
            raise TypeError("committed_memory_ids must be an iterable of strings")
        committed_memory_ids = list(self.committed_memory_ids)
        if not all(isinstance(memory_id, str) for memory_id in committed_memory_ids):
            raise TypeError("committed_memory_ids must contain strings")
        if self.transaction_state not in CRASH_OUTCOME_STATES:
            missing_ids = [
                memory_id
                for memory_id in committed_memory_ids
                if memory_id not in normalized_memories
            ]
            if missing_ids:
                raise ValueError(
                    f"committed memory_id is absent from final_memories: {missing_ids[0]}"
                )

        object.__setattr__(self, "final_memories", normalized_memories)
        object.__setattr__(self, "committed_memory_ids", committed_memory_ids)
        object.__setattr__(self, "trace", _normalize_value(self.trace, "trace"))
        object.__setattr__(self, "metrics", _normalize_value(self.metrics, "metrics"))

    def to_oracle_result(self, variant: str) -> dict[str, Any]:
        """Return the result shape consumed by the independent metric oracle."""

        return {
            "variant": variant,
            "transaction_state": self.transaction_state,
            "final_memories": copy.deepcopy(self.final_memories),
            "committed_memory_ids": list(self.committed_memory_ids),
            "trace": copy.deepcopy(self.trace),
            "metrics": copy.deepcopy(self.metrics),
        }


def normalize_result(observation: ReplayObservation, variant: str) -> dict[str, Any]:
    """Normalize one adapter observation for the unchanged metric oracle."""

    return observation.to_oracle_result(variant)
