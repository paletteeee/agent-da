"""Native Mem0 OSS replay adapter for TxnMemBench.

Mem0 generates its own UUIDs, so this adapter keeps the benchmark ID only in
metadata and translates it to the native UUID before every ``get``, ``update``
and ``delete`` call.  It intentionally does not add transactions, policy
revalidation, or provenance traversal around the individual native calls.
"""

from __future__ import annotations

import copy
import hashlib
import os
from collections import defaultdict
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from txnmem_adapter_contract import (
    CapabilitySupport,
    MemoryAdapter,
    ReplayObservation,
    RuntimeAdapterError,
    UnsupportedMappingError,
)
from txnmem_schema import validate_instance

if TYPE_CHECKING:
    from mem0 import Memory


_METADATA_KEYS = (
    "benchmark_memory_id",
    "instance_id",
    "scope",
    "entity_id",
    "attribute",
    "status",
    "policy_version",
    "supersedes_id",
    "derived_from",
)


def mem0_capabilities(*, persistent_reopen: bool = False) -> tuple[CapabilitySupport, ...]:
    """Return the formal eight-dimension capability matrix for Mem0 replay."""

    return (
        CapabilitySupport("single_record_read_write", True, "Uses native Mem0 add, get, search, and update calls."),
        CapabilitySupport("atomic_multi_record_commit", False, "Mem0 writes are individual records."),
        CapabilitySupport("commit_policy_revalidation", False, "Commit is only a trace marker."),
        CapabilitySupport("shared_scope_isolation", True, "Mapping-layer agent_id and scope metadata checks gate reads; this is not a TxnMem claim."),
        CapabilitySupport("version_supersession", True, "Supersession is two ordered native updates, not an atomic operation."),
        CapabilitySupport("provenance_propagation", False, "Mem0 has no native provenance propagation operation."),
        CapabilitySupport(
            "recursive_provenance_invalidation",
            False,
            "Invalidation updates only the named record; descendants are not traversed.",
        ),
        CapabilitySupport(
            "crash_recovery",
            persistent_reopen,
            "Backend close/reopen observes persistent state only; it does not guarantee atomic transaction recovery and correctness remains oracle-determined."
            if persistent_reopen else "No injected persistent reopen factory is configured.",
        ),
    )


def _json_value(value: Any, path: str) -> Any:
    """Copy only JSON-native values at the SDK/benchmark boundary."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} has a non-string key")
            normalized[key] = _json_value(nested, f"{path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [_json_value(nested, f"{path}[]") for nested in value]
    raise TypeError(f"{path} is not JSON-native")


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


def _matches(memory: Mapping[str, Any], operation: Mapping[str, Any]) -> bool:
    query = operation.get("query")
    return query is None or query in {
        memory.get("value"),
        memory.get("memory_id"),
        memory.get("attribute"),
    }


def close_mem0_memory(memory: Any) -> None:
    """Close a local embedded-Qdrant client when the SDK exposes one."""

    close = getattr(memory, "close", None)
    if callable(close):
        close()
    client = getattr(getattr(memory, "vector_store", None), "client", None)
    close = getattr(client, "close", None)
    if callable(close):
        close()


class Mem0Adapter(MemoryAdapter):
    """Replay one workload with the installed synchronous Mem0 API.

    ``memory_factory`` receives the benchmark instance ID and must return an
    isolated ``Memory`` object or a shared object which honours the per-run
    ``user_id`` supplied by the adapter.
    """

    capabilities = mem0_capabilities()

    def __init__(
        self,
        memory_factory: Callable[[str], "Memory"],
    ) -> None:
        self._memory_factory = memory_factory

    @staticmethod
    def _runtime(instance_id: str, operation_id: str, detail: str) -> RuntimeAdapterError:
        return RuntimeAdapterError(f"Mem0 {detail} for instance {instance_id}, operation {operation_id}")

    @staticmethod
    def _has_memory_methods(memory: Any) -> bool:
        return all(callable(getattr(memory, method, None)) for method in ("add", "get", "search", "update", "delete"))

    @classmethod
    def _validate_memory(cls, memory: Any) -> None:
        if not cls._has_memory_methods(memory):
            raise TypeError("Mem0 Memory must provide callable add, get, search, update, and delete methods")

    @classmethod
    def _call(cls, instance_id: str, operation_id: str, call: Callable[[], Any]) -> Any:
        try:
            return call()
        except (RuntimeAdapterError, UnsupportedMappingError, TypeError, AttributeError, KeyError):
            raise
        except Exception as exc:
            raise cls._runtime(instance_id, operation_id, "call failed") from exc

    @classmethod
    def _metadata(cls, memory: Mapping[str, Any], instance_id: str) -> dict[str, Any]:
        metadata = {
            "benchmark_memory_id": memory["memory_id"],
            "instance_id": instance_id,
            "scope": memory["scope"],
            "entity_id": memory["entity_id"],
            "attribute": memory["attribute"],
            "status": memory["status"],
            "policy_version": memory["policy_version"],
            "supersedes_id": memory["supersedes_id"],
            "derived_from": memory["derived_from"],
        }
        return _json_value(metadata, "metadata")

    @classmethod
    def _add_response_id(cls, response: Any, instance_id: str, operation_id: str) -> str:
        if not isinstance(response, Mapping) or set(response) != {"results"}:
            raise cls._runtime(instance_id, operation_id, "returned an unknown add response envelope")
        results = response.get("results")
        if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], Mapping):
            raise cls._runtime(instance_id, operation_id, "returned an unknown add response envelope")
        sdk_id = results[0].get("id")
        if not isinstance(sdk_id, str) or not sdk_id:
            raise cls._runtime(instance_id, operation_id, "returned an add response without a UUID")
        return sdk_id

    @classmethod
    def _success_response(cls, response: Any, instance_id: str, operation_id: str, op_name: str) -> None:
        if not isinstance(response, Mapping) or not isinstance(response.get("message"), str):
            raise cls._runtime(instance_id, operation_id, f"returned an unknown {op_name} response envelope")

    @classmethod
    def _record(
        cls,
        response: Any,
        *,
        expected_metadata: Mapping[str, Any],
        expected_agent_id: str,
        expected_sdk_id: str,
        instance_id: str,
        operation_id: str,
    ) -> dict[str, Any] | None:
        if response is None:
            return None
        if not isinstance(response, Mapping):
            raise cls._runtime(instance_id, operation_id, "returned a non-dictionary record")
        try:
            sdk_id = response["id"]
            content = response["memory"]
            metadata = response["metadata"]
            agent_id = response["agent_id"]
            if (
                not isinstance(sdk_id, str)
                or sdk_id != expected_sdk_id
                or not isinstance(metadata, Mapping)
                or not isinstance(agent_id, str)
                or agent_id != expected_agent_id
            ):
                raise ValueError("unexpected record identity")
            normalized_metadata = _json_value(metadata, "metadata")
            if any(key not in normalized_metadata for key in _METADATA_KEYS):
                raise ValueError("missing benchmark metadata")
            if any(normalized_metadata[key] != expected_metadata[key] for key in _METADATA_KEYS):
                raise ValueError("conflicting benchmark metadata")
            value = _json_value(content, "memory")
        except (KeyError, TypeError, ValueError):
            raise cls._runtime(instance_id, operation_id, "returned missing or conflicting benchmark metadata") from None
        return {
            "memory_id": normalized_metadata["benchmark_memory_id"],
            "agent_id": agent_id,
            "scope": normalized_metadata["scope"],
            "entity_id": normalized_metadata["entity_id"],
            "attribute": normalized_metadata["attribute"],
            "value": value,
            "status": normalized_metadata["status"],
            "policy_version": normalized_metadata["policy_version"],
            "supersedes_id": normalized_metadata["supersedes_id"],
            "derived_from": normalized_metadata["derived_from"],
        }

    @classmethod
    def _search_records(cls, response: Any, instance_id: str, operation_id: str) -> list[Any]:
        if not isinstance(response, Mapping) or set(response) != {"results"} or not isinstance(response.get("results"), list):
            raise cls._runtime(instance_id, operation_id, "returned an unknown search response envelope")
        return response["results"]

    @classmethod
    def _get_all_records(cls, response: Any, instance_id: str) -> list[Any]:
        if isinstance(response, list):
            return response
        if not isinstance(response, Mapping) or set(response) != {"results"} or not isinstance(response.get("results"), list):
            raise cls._runtime(instance_id, "final_state", "returned an unknown get_all response envelope")
        return response["results"]

    @staticmethod
    def _allowed(memory: Mapping[str, Any], operation: Mapping[str, Any]) -> bool:
        return (
            memory.get("agent_id") == operation.get("agent_id", "agent_1")
            and memory.get("scope") == operation.get("scope", "tenant:user_001")
            and memory.get("status") in {"active", "pending"}
            and _matches(memory, operation)
        )

    @staticmethod
    def _close(memory: Any) -> None:
        close_mem0_memory(memory)

    def _supports_reopen(self) -> bool:
        return bool(
            getattr(self._memory_factory, "persistent", False)
            and callable(getattr(self._memory_factory, "reopen", None))
        )

    def run(self, instance: dict[str, Any]) -> ReplayObservation:
        validate_instance(instance)
        instance_id = instance["instance_id"]
        try:
            memory = self._memory_factory(instance_id)
        except Exception as exc:
            raise self._runtime(instance_id, "initialization", "initialization failed") from exc
        self._validate_memory(memory)
        opened_memories = [memory]
        try:
            return self._run_with_memory(memory, instance, opened_memories)
        finally:
            closed_ids: set[int] = set()
            for opened in reversed(opened_memories):
                if id(opened) not in closed_ids:
                    self._close(opened)
                    closed_ids.add(id(opened))

    def _run_with_memory(
        self,
        memory: Any,
        instance: dict[str, Any],
        opened_memories: list[Any],
    ) -> ReplayObservation:
        instance_id = instance["instance_id"]
        user_id = f"txnmembench:{instance_id}"
        benchmark_to_sdk: dict[str, str] = {}
        sdk_to_benchmark: dict[str, str] = {}
        expected_metadata: dict[str, dict[str, Any]] = {}
        known_memories: dict[str, dict[str, Any]] = {}
        committed_memory_ids: list[str] = []
        trace: list[dict[str, Any]] = []
        exposed_memory_ids: list[str] = []
        denied_reads = 0
        supersession_updates = 0
        current_policy_version = 1
        transaction_state = "active"

        def put(record: dict[str, Any], operation_id: str) -> None:
            benchmark_id = record["memory_id"]
            metadata = self._metadata(record, instance_id)
            if benchmark_id in benchmark_to_sdk:
                sdk_id = benchmark_to_sdk[benchmark_id]
                response = self._call(
                    instance_id,
                    operation_id,
                    lambda: memory.update(sdk_id, text=record["value"], metadata=metadata),
                )
                self._success_response(response, instance_id, operation_id, "update")
            else:
                response = self._call(
                    instance_id,
                    operation_id,
                    lambda: memory.add(
                        record["value"],
                        user_id=user_id,
                        agent_id=record["agent_id"],
                        metadata=metadata,
                        infer=False,
                    ),
                )
                sdk_id = self._add_response_id(response, instance_id, operation_id)
                benchmark_to_sdk[benchmark_id] = sdk_id
                sdk_to_benchmark[sdk_id] = benchmark_id
            expected_metadata[benchmark_id] = metadata
            known_memories[benchmark_id] = copy.deepcopy(record)

        def get(benchmark_id: str, operation_id: str) -> dict[str, Any] | None:
            sdk_id = benchmark_to_sdk.get(benchmark_id)
            if sdk_id is None:
                return None
            response = self._call(instance_id, operation_id, lambda: memory.get(sdk_id))
            return self._record(
                response,
                expected_metadata=expected_metadata[benchmark_id],
                expected_agent_id=known_memories[benchmark_id]["agent_id"],
                expected_sdk_id=sdk_id,
                instance_id=instance_id,
                operation_id=operation_id,
            )

        for initial in instance["initial_memories"]:
            put(copy.deepcopy(initial), "initial_memory")

        scheduled_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for event in instance["failure_schedule"]:
            scheduled_by_step[int(event["step"])].append(event)

        for operation in instance["operations"]:
            step = int(operation["step"])
            operation_id = operation["op_id"]
            step_events = scheduled_by_step.get(step, [])
            for event in step_events:
                if event["type"] == "revoke":
                    current_policy_version += 1
                    trace.append({"step": step, "event": "revoke", "policy_version": current_policy_version})
                elif event["type"] == "delay":
                    trace.append({"step": step, "event": "delay"})

            op_type = operation["type"]
            trace.append({"step": step, "operation": op_type})
            if op_type == "write":
                put(_memory_from_operation(operation), operation_id)
                committed_memory_ids.append(operation["memory_id"])

            elif op_type in {"get_by_id", "read"} and operation.get("memory_id") is not None:
                benchmark_id = operation["memory_id"]
                record = get(benchmark_id, operation_id)
                if record is not None and self._allowed(record, operation):
                    if benchmark_id not in exposed_memory_ids:
                        exposed_memory_ids.append(benchmark_id)
                    trace.append({"step": step, "event": "exposed_read", "memory_id": benchmark_id})
                elif benchmark_id in known_memories:
                    denied_reads += 1
                    trace.append({"step": step, "event": "denied_read", "memory_id": benchmark_id})

            elif op_type in {"search", "read"}:
                response = self._call(
                    instance_id,
                    operation_id,
                    lambda: memory.search(operation.get("query"), filters={"user_id": user_id}),
                )
                for item in self._search_records(response, instance_id, operation_id):
                    if not isinstance(item, Mapping):
                        raise self._runtime(instance_id, operation_id, "returned a non-dictionary search record")
                    sdk_id = item.get("id")
                    benchmark_id = sdk_to_benchmark.get(sdk_id)
                    if not isinstance(sdk_id, str) or benchmark_id is None:
                        raise self._runtime(instance_id, operation_id, "returned a search record without a known UUID mapping")
                    record = self._record(
                        item,
                        expected_metadata=expected_metadata[benchmark_id],
                        expected_agent_id=known_memories[benchmark_id]["agent_id"],
                        expected_sdk_id=sdk_id,
                        instance_id=instance_id,
                        operation_id=operation_id,
                    )
                    if record is not None and self._allowed(record, operation):
                        if benchmark_id not in exposed_memory_ids:
                            exposed_memory_ids.append(benchmark_id)
                        trace.append({"step": step, "event": "exposed_read", "memory_id": benchmark_id})
                        break
                else:
                    denied = next(
                        (
                            benchmark_id
                            for benchmark_id, candidate in known_memories.items()
                            if _matches(candidate, operation)
                            and not self._allowed(candidate, operation)
                        ),
                        None,
                    )
                    if denied is not None:
                        denied_reads += 1
                        trace.append({"step": step, "event": "denied_read", "memory_id": denied})

            elif op_type == "supersede":
                old_id, new_id = operation["old_memory_id"], operation["new_memory_id"]
                old_record, new_record = get(old_id, operation_id), get(new_id, operation_id)
                if old_record is None or new_record is None:
                    raise KeyError(f"unknown memory_id: {old_id if old_record is None else new_id}")
                old_record["status"] = "superseded"
                new_record["status"] = "active"
                new_record["supersedes_id"] = old_id
                put(old_record, operation_id)
                put(new_record, operation_id)
                supersession_updates += 1
                trace.append({"step": step, "event": "ordered_supersession_updates"})

            elif op_type == "invalidate":
                benchmark_id = operation["memory_id"]
                record = get(benchmark_id, operation_id)
                if record is None:
                    raise KeyError(f"unknown memory_id: {benchmark_id}")
                record["status"] = "invalid"
                put(record, operation_id)
                transaction_state = "invalidated"

            elif op_type == "propagate":
                trace.append({"step": step, "event": "capability_absent", "capability": "provenance_propagation"})

            elif op_type == "commit":
                transaction_state = "committed"

            if any(event["type"] == "crash" for event in step_events):
                if self._supports_reopen():
                    original_memory = memory
                    self._close(original_memory)
                    opened_memories[:] = [
                        opened for opened in opened_memories if opened is not original_memory
                    ]
                    try:
                        reopened = self._memory_factory.reopen(instance_id)
                        self._validate_memory(reopened)
                    except Exception as exc:
                        raise self._runtime(instance_id, operation_id, "reopen failed") from exc
                    memory = reopened
                    opened_memories.append(memory)
                else:
                    trace.append({"step": step, "event": "capability_absent", "capability": "crash_recovery"})
                transaction_state = "partial_commit" if committed_memory_ids else "crashed"
                trace.append({"step": step, "event": "crash"})
                break

        final_response = self._call(
            instance_id,
            "final_state",
            lambda: memory.get_all(
                filters={"user_id": user_id},
                top_k=max(1, len(benchmark_to_sdk)),
            ),
        )
        final_memories: dict[str, dict[str, Any]] = {}
        for item in self._get_all_records(final_response, instance_id):
            if not isinstance(item, Mapping):
                raise self._runtime(instance_id, "final_state", "returned a non-dictionary get_all record")
            sdk_id = item.get("id")
            benchmark_id = sdk_to_benchmark.get(sdk_id)
            if not isinstance(sdk_id, str) or benchmark_id is None:
                raise self._runtime(instance_id, "final_state", "returned a get_all record without a known UUID mapping")
            record = self._record(
                item,
                expected_metadata=expected_metadata[benchmark_id],
                expected_agent_id=known_memories[benchmark_id]["agent_id"],
                expected_sdk_id=sdk_id,
                instance_id=instance_id,
                operation_id="final_state",
            )
            if record is not None:
                final_memories[benchmark_id] = record

        if transaction_state == "active":
            transaction_state = "completed"
        return ReplayObservation(
            transaction_state=transaction_state,
            final_memories=final_memories,
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


class DeterministicHashEmbedding:
    """A local content-sensitive embedding with a fixed, recorded 64-D hash."""

    dimension = 64
    algorithm = "sha256-counter-v1"

    def embed(self, text: str, memory_action: str | None = None) -> list[float]:
        source = str(text).encode("utf-8")
        values: list[float] = []
        for counter in range(2):
            digest = hashlib.sha256(counter.to_bytes(1, "big") + source).digest()
            values.extend((byte / 127.5) - 1.0 for byte in digest)
        return values


class DeterministicMem0Factory:
    """Explicitly persistent Mem0 factory backed by caller-owned local paths."""

    persistent = True

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def __call__(self, instance_id: str) -> "Memory":
        return self._open(instance_id)

    def reopen(self, instance_id: str) -> "Memory":
        return self._open(instance_id)

    def _open(self, instance_id: str) -> "Memory":
        os.environ["MEM0_TELEMETRY"] = "false"
        from mem0 import Memory  # optional dependency; intentionally lazy

        digest = hashlib.sha256(instance_id.encode("utf-8")).hexdigest()[:16]
        instance_root = self.root / digest
        qdrant_path = instance_root / "qdrant"
        instance_root.mkdir(parents=True, exist_ok=True)
        config = {
            "history_db_path": str(instance_root / "history.db"),
            "vector_store": {
                "provider": "qdrant",
                "config": {
                    "collection_name": f"txnmem_{digest}",
                    "path": str(qdrant_path),
                    "embedding_model_dims": DeterministicHashEmbedding.dimension,
                },
            },
            "llm": {
                "provider": "vllm",
                "config": {"model": "unused-local-model", "vllm_base_url": "http://127.0.0.1:9"},
            },
            "embedder": {
                "provider": "openai",
                "config": {"model": "text-embedding-3-small", "api_key": "not-a-secret-bootstrap-only"},
            },
        }
        local_memory = Memory.from_config(config)
        local_memory.embedding_model = DeterministicHashEmbedding()
        return local_memory


def deterministic_mem0_factory(root: str | Path) -> DeterministicMem0Factory:
    """Build an embedded-Qdrant Mem0 factory for a caller-owned formal-run root.

    The factory imports Mem0 lazily, disables telemetry before that import, and
    replaces the SDK-required inert embedding client with the fixed SHA-256
    embedding above. ``infer=False`` in :class:`Mem0Adapter` means the local
    inert LLM configuration is never invoked.
    """

    return DeterministicMem0Factory(root)
