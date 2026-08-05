"""Persistent vector/provenance backend used by the production-evidence runs.

The backend intentionally keeps the storage seam small: local tests inject
fake Qdrant/Neo4j clients, while the default clients use Qdrant HTTP and the
Neo4j Bolt driver.  A canonical event is appended only after both stores have
accepted a mutation.
"""

from __future__ import annotations

import copy
import hashlib
import json
import time
from uuid import NAMESPACE_URL, uuid5
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from txnmem_backend import InstrumentedMemoryBackend


class VectorGraphBackendError(RuntimeError):
    """A storage or compensation failure with aggregate-safe metadata."""


def _qdrant_point_id(namespace: str, memory_id: str) -> str:
    """Map arbitrary application IDs to Qdrant's UUID-compatible point IDs."""

    return str(uuid5(NAMESPACE_URL, f"txnmem:{namespace}:{memory_id}"))


def _embedding(value: Any, dimension: int = 32) -> list[float]:
    """Create a deterministic storage fixture embedding without model calls."""

    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    vector: list[float] = []
    for index in range(dimension):
        digest = hashlib.sha256(raw + index.to_bytes(2, "big")).digest()
        vector.append((int.from_bytes(digest[:4], "big") / 2**32) * 2.0 - 1.0)
    return vector


class _QdrantHTTPClient:
    def __init__(self, base_url: str, dimension: int = 32):
        self.base_url = str(base_url).rstrip("/")
        self.dimension = int(dimension)
        self.collection = "txnmem_memory"

    def _request(self, method: str, path: str, payload: Mapping[str, Any] | None = None) -> Any:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=15) as response:  # noqa: S310 - endpoint is explicit experiment config
            raw = response.read()
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _ensure_collection(self) -> None:
        try:
            self._request("PUT", f"/collections/{self.collection}", {"vectors": {"size": self.dimension, "distance": "Cosine"}})
        except Exception as exc:
            if "409" not in str(exc):
                raise

    def upsert(self, namespace, point_id, vector, payload, idempotency_key):
        self._ensure_collection()
        self._request(
            "PUT",
            f"/collections/{self.collection}/points",
            {"points": [{"id": _qdrant_point_id(namespace, point_id), "vector": vector, "payload": {**payload, "namespace": namespace}}]},
        )

    def retrieve(self, namespace, point_id):
        result = self._request("POST", f"/collections/{self.collection}/points", {"ids": [_qdrant_point_id(namespace, point_id)], "with_payload": True})
        rows = result.get("result", []) if isinstance(result, Mapping) else []
        for row in rows:
            payload = row.get("payload", {})
            if payload.get("namespace") == namespace:
                return dict(payload)
        return None

    def search(self, namespace, vector, limit):
        result = self._request(
            "POST",
            f"/collections/{self.collection}/points/search",
            {"vector": vector, "limit": int(limit), "with_payload": True, "filter": {"must": [{"key": "namespace", "match": {"value": namespace}}]}},
        )
        rows = result.get("result", []) if isinstance(result, Mapping) else []
        return [dict(row.get("payload", {})) for row in rows]

    def delete(self, namespace, point_id, idempotency_key):
        self._request("POST", f"/collections/{self.collection}/points/delete", {"points": [_qdrant_point_id(namespace, point_id)]})

    def healthcheck(self):
        result = self._request("GET", "/")
        return {"available": True, "version": result.get("version") if isinstance(result, Mapping) else None}


class _Neo4jBoltClient:
    def __init__(self, uri: str, auth: Sequence[str]):
        try:
            from neo4j import GraphDatabase
        except ImportError as exc:  # pragma: no cover - exercised on remote host
            raise RuntimeError("neo4j driver is required for the real graph backend") from exc
        self.driver = GraphDatabase.driver(str(uri), auth=tuple(auth))

    def upsert_memory(self, namespace, memory_id, payload, source_ids, supersedes_id, idempotency_key):
        with self.driver.session() as session:
            session.run(
                "MERGE (m:Memory {namespace:$namespace, memory_id:$memory_id}) SET m.status=$status",
                namespace=namespace,
                memory_id=memory_id,
                status=payload.get("status", "active"),
            ).consume()
            for source_id in source_ids:
                session.run(
                    "MATCH (s:Memory {namespace:$namespace, memory_id:$source_id}) "
                    "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) "
                    "MERGE (m)-[:DERIVED_FROM]->(s)",
                    namespace=namespace,
                    source_id=source_id,
                    memory_id=memory_id,
                ).consume()
            if supersedes_id:
                session.run(
                    "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) "
                    "MATCH (o:Memory {namespace:$namespace, memory_id:$old_id}) "
                    "MERGE (m)-[:SUPERSEDES]->(o)",
                    namespace=namespace,
                    memory_id=memory_id,
                    old_id=supersedes_id,
                ).consume()

    def update_status(self, namespace, memory_id, status, idempotency_key):
        with self.driver.session() as session:
            session.run(
                "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) SET m.status=$status",
                namespace=namespace,
                memory_id=memory_id,
                status=status,
            ).consume()

    def delete_memory(self, namespace, memory_id, idempotency_key):
        with self.driver.session() as session:
            session.run(
                "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) DETACH DELETE m",
                namespace=namespace,
                memory_id=memory_id,
            ).consume()

    def healthcheck(self):
        with self.driver.session() as session:
            record = session.run("RETURN 1 AS ok").single()
        return {"available": bool(record and record.get("ok") == 1), "version": "neo4j-bolt"}

    def close(self):
        self.driver.close()


class VectorGraphMemoryBackend(InstrumentedMemoryBackend):
    """Instrumented memory backend backed by vector and graph services."""

    def __init__(
        self,
        db_namespace: str,
        qdrant_url: str,
        neo4j_uri: str,
        neo4j_auth: Sequence[str],
        proxy_requester: Callable[..., Any] | None = None,
        *,
        qdrant_client: Any | None = None,
        neo4j_client: Any | None = None,
        embedder: Callable[[Any], list[float]] | None = None,
        max_retries: int = 1,
    ):
        super().__init__()
        self.db_namespace = str(db_namespace)
        self.qdrant_url = str(qdrant_url)
        self.neo4j_uri = str(neo4j_uri)
        self.neo4j_auth = tuple(str(item) for item in neo4j_auth)
        self.qdrant = qdrant_client or _QdrantHTTPClient(self.qdrant_url)
        self.neo4j = neo4j_client or _Neo4jBoltClient(self.neo4j_uri, self.neo4j_auth)
        self.proxy_requester = proxy_requester
        self.embedder = embedder or _embedding
        self.max_retries = max(0, int(max_retries))
        self._committed_keys: dict[str, str] = {}
        self._metrics: dict[str, Any] = {
            "request_count": 0,
            "retry_count": 0,
            "rollback_count": 0,
            "error_count": 0,
            "operation_counts": {},
            "timing_ms": {},
        }

    def _key(self, operation: str, memory_id: str, source_ids: Iterable[str] = ()) -> str:
        encoded = json.dumps(
            [self.db_namespace, operation, memory_id, sorted(str(item) for item in source_ids)],
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _call(self, service: str, operation: str, function: Callable[[], Any], key: str) -> Any:
        attempts = 0
        started = time.perf_counter()
        while True:
            attempts += 1
            self._metrics["request_count"] += 1
            self._metrics["operation_counts"][f"{service}:{operation}"] = self._metrics["operation_counts"].get(f"{service}:{operation}", 0) + 1
            try:
                if self.proxy_requester is not None:
                    result = self.proxy_requester(service, operation, function, key)
                else:
                    result = function()
                break
            except Exception:
                if attempts > self.max_retries:
                    self._metrics["error_count"] += 1
                    raise
                self._metrics["retry_count"] += 1
        elapsed = (time.perf_counter() - started) * 1000.0
        self._metrics["timing_ms"].setdefault(operation, []).append(elapsed)
        return result

    def write(self, memory_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        memory_id = str(memory_id)
        source_ids = list(fields.get("source_ids", []))
        supersedes_id = fields.get("supersedes_id")
        key = self._key("write", memory_id, source_ids)
        if key in self._committed_keys and memory_id in self.memories:
            return copy.deepcopy(self.memories[memory_id])
        memory = {
            "memory_id": memory_id,
            "value": value if value is not None else memory_id,
            "status": "active",
            "agent_id": fields.get("agent_id", "agent_1"),
            "scope": fields.get("scope", "tenant:user_001"),
            "derived_from": source_ids,
        }
        vector = self.embedder(memory["value"])
        try:
            self._call(
                "qdrant",
                "upsert",
                lambda: self.qdrant.upsert(self.db_namespace, memory_id, vector, memory, key),
                key,
            )
            try:
                self._call(
                    "neo4j",
                    "upsert",
                    lambda: self.neo4j.upsert_memory(
                        self.db_namespace, memory_id, memory, source_ids, supersedes_id, key
                    ),
                    key,
                )
            except Exception:
                self._metrics["rollback_count"] += 1
                self._call(
                    "qdrant",
                    "delete_compensation",
                    lambda: self.qdrant.delete(self.db_namespace, memory_id, key),
                    key,
                )
                raise
        except Exception as exc:
            if not isinstance(exc, VectorGraphBackendError):
                raise
            raise
        self.memories[memory_id] = copy.deepcopy(memory)
        self._committed_keys[key] = memory_id
        self._event(
            "memory_write",
            memory_id=memory_id,
            value=memory["value"],
            source_ids=source_ids or None,
            **{key: value for key, value in fields.items() if key not in {"source_ids", "supersedes_id"}},
        )
        return copy.deepcopy(memory)

    def read(self, memory_id: str | None = None, **fields: Any) -> dict[str, Any] | None:
        memory = None
        if memory_id is not None:
            memory = self._call(
                "qdrant",
                "retrieve",
                lambda: self.qdrant.retrieve(self.db_namespace, str(memory_id)),
                self._key("read", str(memory_id)),
            )
            if memory is None:
                memory = self.memories.get(str(memory_id))
            elif str(memory_id) in self.memories and self.memories[str(memory_id)].get("status") != "active":
                memory = self.memories[str(memory_id)]
            if memory is not None:
                self.memories[str(memory_id)] = copy.deepcopy(memory)
        self._event("memory_read", memory_id=memory_id, **fields)
        return copy.deepcopy(memory) if memory and memory.get("status") == "active" else None

    def search(self, query: str | None = None, **fields: Any) -> list[dict[str, Any]]:
        rows = self._call(
            "qdrant",
            "search",
            lambda: self.qdrant.search(self.db_namespace, self.embedder(query or ""), 100),
            self._key("search", str(query or "")),
        )
        matches = [
            copy.deepcopy(row)
            for row in rows
            if isinstance(row, Mapping)
            and self.memories.get(str(row.get("memory_id")), row).get("status") == "active"
            and (query is None or query in {row.get("memory_id"), row.get("value"), row.get("attribute")})
        ]
        self._event("memory_search", query=query, **fields)
        return matches

    def derive(self, memory_id: str, source_ids: Iterable[str], value: Any = None, **fields: Any) -> dict[str, Any]:
        source_ids = list(source_ids)
        if any(source_id not in self.memories for source_id in source_ids):
            raise KeyError("derive source is missing")
        memory = self.write(memory_id, value=value, source_ids=source_ids, **fields)
        self.events[-1]["kind"] = "memory_derive"
        self.events[-1]["source_ids"] = source_ids
        return memory

    def propagate(self, memory_id: str, source_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        memory = self.write(memory_id, value=value, source_ids=[source_id], **fields)
        self.events[-1]["kind"] = "memory_propagate"
        self.events[-1]["source_id"] = source_id
        self.events[-1]["source_ids"] = [source_id]
        return memory

    def supersede(self, old_memory_id: str, new_memory_id: str, value: Any = None, **fields: Any) -> dict[str, Any]:
        if old_memory_id not in self.memories:
            raise KeyError(old_memory_id)
        memory = self.write(new_memory_id, value=value, supersedes_id=old_memory_id, **fields)
        old = self.memories[old_memory_id]
        old["status"] = "superseded"
        key = self._key("supersede", old_memory_id, [new_memory_id])
        if hasattr(self.neo4j, "update_status"):
            self._call("neo4j", "update_status", lambda: self.neo4j.update_status(self.db_namespace, old_memory_id, "superseded", key), key)
        self._event("memory_supersede", old_memory_id=old_memory_id, new_memory_id=new_memory_id, **fields)
        return memory

    def invalidate(self, memory_id: str, **fields: Any) -> None:
        if memory_id in self.memories:
            self.memories[memory_id]["status"] = "invalid"
        key = self._key("invalidate", str(memory_id))
        if hasattr(self.neo4j, "update_status"):
            self._call("neo4j", "update_status", lambda: self.neo4j.update_status(self.db_namespace, memory_id, "invalid", key), key)
        self._event("invalidate", memory_id=memory_id, **fields)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return copy.deepcopy(self.memories)

    def healthcheck(self) -> dict[str, Any]:
        return {
            "namespace": self.db_namespace,
            "qdrant": self.qdrant.healthcheck(),
            "neo4j": self.neo4j.healthcheck(),
        }

    def metrics(self) -> dict[str, Any]:
        return copy.deepcopy(self._metrics)

    def close(self) -> None:
        close = getattr(self.neo4j, "close", None)
        if callable(close):
            close()
