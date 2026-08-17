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
from txnmem_task_transaction import (
    _expected_edges,
    _expected_ids,
    _intent_memory_id,
    _latest_record_intents,
    _replay_staged_records,
)


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
    def __init__(self, base_url: str, dimension: int = 32, timeout_seconds: float = 15.0):
        self.base_url = str(base_url).rstrip("/")
        self.dimension = int(dimension)
        self.timeout_seconds = float(timeout_seconds)
        self.collection = "txnmem_memory"

    def _request(self, method: str, path: str, payload: Mapping[str, Any] | None = None) -> Any:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - endpoint is explicit experiment config
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
            f"/collections/{self.collection}/points?wait=true",
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
        self._request("POST", f"/collections/{self.collection}/points/delete?wait=true", {"points": [_qdrant_point_id(namespace, point_id)]})

    @staticmethod
    def _must_filter(**matches: str) -> dict[str, Any]:
        return {
            "must": [
                {"key": key, "match": {"value": value}}
                for key, value in matches.items()
            ]
        }

    def _scroll(self, filters: Mapping[str, Any], limit: int = 1000) -> list[dict[str, Any]]:
        self._ensure_collection()
        result = self._request(
            "POST",
            f"/collections/{self.collection}/points/scroll",
            {
                "filter": dict(filters),
                "limit": int(limit),
                "with_payload": True,
                "with_vector": False,
            },
        )
        page = result.get("result", {}) if isinstance(result, Mapping) else {}
        rows = page.get("points", []) if isinstance(page, Mapping) else []
        return [dict(row.get("payload", {})) for row in rows]

    def retrieve_many_by_txn(self, namespace, txn_id):
        try:
            rows = self._scroll(self._must_filter(namespace=namespace, txn_id=txn_id))
        except Exception as exc:
            return {"read_ok": False, "error": type(exc).__name__}
        return {"read_ok": True, "rows": rows}

    def retrieve_many_by_memory(self, namespace, memory_id):
        try:
            rows = self._scroll(
                self._must_filter(namespace=namespace, memory_id=memory_id)
            )
        except Exception as exc:
            return {"read_ok": False, "error": type(exc).__name__}
        return {"read_ok": True, "rows": rows}

    def scan_namespace(self, namespace, limit=1000):
        try:
            rows = self._scroll(
                self._must_filter(namespace=namespace), limit=int(limit)
            )
        except Exception as exc:
            return {"read_ok": False, "error": type(exc).__name__}
        return {"read_ok": True, "rows": rows}

    def delete_many_by_txn(self, namespace, txn_id, idempotency_key):
        self._request(
            "POST",
            f"/collections/{self.collection}/points/delete?wait=true",
            {"filter": self._must_filter(namespace=namespace, txn_id=txn_id)},
        )

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
        txn_id = payload.get("txn_id")
        if txn_id is not None:
            with self.driver.session() as session:
                session.run(
                    "MERGE (m:TxnMemory {namespace:$namespace, memory_id:$memory_id, "
                    "txn_id:$txn_id, sequence:$sequence, record_kind:$record_kind}) "
                    "SET m.status=$status, m.target_status=$target_status, "
                    "m.payload_hash=$payload_hash, m.version=$version, "
                    "m.operation=$operation, m.agent_id=$agent_id, m.scope=$scope, "
                    "m.derived_from=$source_ids, m.supersedes_id=$supersedes_id",
                    namespace=namespace,
                    memory_id=memory_id,
                    txn_id=txn_id,
                    sequence=int(payload["sequence"]),
                    record_kind=payload["record_kind"],
                    status=payload["status"],
                    target_status=payload["target_status"],
                    payload_hash=payload["payload_hash"],
                    version=int(payload["version"]),
                    operation=payload["operation"],
                    agent_id=payload.get("agent_id"),
                    scope=payload.get("scope"),
                    source_ids=list(source_ids),
                    supersedes_id=supersedes_id,
                ).consume()
                session.run(
                    "MATCH (m:TxnMemory {namespace:$namespace, txn_id:$txn_id, "
                    "memory_id:$memory_id, sequence:$sequence, record_kind:$record_kind}) "
                    "OPTIONAL MATCH (m)-[r:DERIVED_FROM|SUPERSEDES {txn_id:$txn_id}]->() "
                    "DELETE r",
                    namespace=namespace,
                    txn_id=txn_id,
                    memory_id=memory_id,
                    sequence=int(payload["sequence"]),
                    record_kind=payload["record_kind"],
                ).consume()
                for source_id in source_ids:
                    session.run(
                        "MATCH (m:TxnMemory {namespace:$namespace, txn_id:$txn_id, "
                        "memory_id:$memory_id, sequence:$sequence, record_kind:$record_kind}) "
                        "MERGE (s:MemoryReference {namespace:$namespace, memory_id:$source_id}) "
                        "MERGE (m)-[r:DERIVED_FROM {namespace:$namespace, txn_id:$txn_id, source_id:$source_id, "
                        "target_id:$memory_id}]->(s) SET r.status=$status",
                        namespace=namespace,
                        txn_id=txn_id,
                        memory_id=memory_id,
                        sequence=int(payload["sequence"]),
                        record_kind=payload["record_kind"],
                        source_id=source_id,
                        status=payload["status"],
                    ).consume()
                if supersedes_id:
                    session.run(
                        "MATCH (m:TxnMemory {namespace:$namespace, txn_id:$txn_id, "
                        "memory_id:$memory_id, sequence:$sequence, record_kind:$record_kind}) "
                        "MERGE (o:MemoryReference {namespace:$namespace, memory_id:$old_id}) "
                        "MERGE (m)-[r:SUPERSEDES {namespace:$namespace, txn_id:$txn_id, source_id:$memory_id, "
                        "target_id:$old_id}]->(o) SET r.status=$status",
                        namespace=namespace,
                        txn_id=txn_id,
                        memory_id=memory_id,
                        sequence=int(payload["sequence"]),
                        record_kind=payload["record_kind"],
                        old_id=supersedes_id,
                        status=payload["status"],
                    ).consume()
            return
        with self.driver.session() as session:
            session.run(
                "MERGE (m:Memory {namespace:$namespace, memory_id:$memory_id}) "
                "SET m.status=$status, m.version=$version",
                namespace=namespace,
                memory_id=memory_id,
                status=payload.get("status", "active"),
                version=int(payload.get("version", 1)),
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

    def update_status(self, namespace, memory_id, status, idempotency_key, version=None):
        with self.driver.session() as session:
            session.run(
                "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) "
                "SET m.status=$status, m.version=coalesce($version, m.version)",
                namespace=namespace,
                memory_id=memory_id,
                status=status,
                version=version,
            ).consume()

    def delete_memory(self, namespace, memory_id, idempotency_key):
        with self.driver.session() as session:
            session.run(
                "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) DETACH DELETE m",
                namespace=namespace,
                memory_id=memory_id,
            ).consume()

    def retrieve_memory(self, namespace, memory_id):
        """Read the persisted node and provenance edges after fault recovery."""

        with self.driver.session() as session:
            record = session.run(
                "MATCH (m:Memory {namespace:$namespace, memory_id:$memory_id}) "
                "OPTIONAL MATCH (m)-[:DERIVED_FROM]->(s:Memory {namespace:$namespace}) "
                "OPTIONAL MATCH (m)-[:SUPERSEDES]->(o:Memory {namespace:$namespace}) "
                "RETURN m.status AS status, m.version AS version, "
                "collect(DISTINCT s.memory_id) AS source_ids, "
                "head(collect(DISTINCT o.memory_id)) AS supersedes_id",
                namespace=namespace,
                memory_id=memory_id,
            ).single()
        if record is None:
            return None
        return {
            "status": record.get("status"),
            "version": record.get("version", 1),
            "source_ids": sorted(
                str(source_id)
                for source_id in (record.get("source_ids") or [])
                if source_id is not None
            ),
            "supersedes_id": record.get("supersedes_id"),
        }

    def retrieve_many_by_txn(self, namespace, txn_id):
        try:
            with self.driver.session() as session:
                nodes = [
                    dict(record.get("payload") or {})
                    for record in session.run(
                        "MATCH (m:TxnMemory {namespace:$namespace, txn_id:$txn_id}) "
                        "RETURN properties(m) AS payload "
                        "ORDER BY m.sequence, m.record_kind, m.memory_id",
                        namespace=namespace,
                        txn_id=txn_id,
                    )
                ]
                edges = [
                    {
                        "txn_id": str(record.get("txn_id")),
                        "kind": str(record.get("kind")),
                        "source_id": str(record.get("source_id")),
                        "target_id": str(record.get("target_id")),
                        "status": str(record.get("status")),
                    }
                    for record in session.run(
                        "MATCH ()-[r]->() WHERE r.namespace=$namespace AND r.txn_id=$txn_id "
                        "RETURN r.txn_id AS txn_id, type(r) AS kind, "
                        "r.source_id AS source_id, r.target_id AS target_id, "
                        "r.status AS status "
                        "ORDER BY kind, source_id, target_id",
                        namespace=namespace,
                        txn_id=txn_id,
                    )
                ]
        except Exception as exc:
            return {"read_ok": False, "error": type(exc).__name__}
        return {"read_ok": True, "nodes": nodes, "edges": edges}

    def delete_many_by_txn(self, namespace, txn_id, idempotency_key):
        with self.driver.session() as session:
            session.run(
                "MATCH (m:TxnMemory {namespace:$namespace, txn_id:$txn_id}) "
                "DETACH DELETE m",
                namespace=namespace,
                txn_id=txn_id,
            ).consume()
            session.run(
                "MATCH (r:MemoryReference {namespace:$namespace}) "
                "WHERE NOT (r)--() DELETE r",
                namespace=namespace,
            ).consume()

    def healthcheck(self):
        with self.driver.session() as session:
            record = session.run(
                "CALL dbms.components() YIELD versions "
                "RETURN 1 AS ok, versions[0] AS version"
            ).single()
        return {
            "available": bool(record and record.get("ok") == 1),
            "version": record.get("version") if record else None,
        }

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
        request_timeout_seconds: float = 15.0,
        decision_resolver: Callable[[str], str | None] | None = None,
    ):
        super().__init__()
        self.db_namespace = str(db_namespace)
        self.qdrant_url = str(qdrant_url)
        self.neo4j_uri = str(neo4j_uri)
        self.neo4j_auth = tuple(str(item) for item in neo4j_auth)
        self.qdrant = qdrant_client or _QdrantHTTPClient(
            self.qdrant_url, timeout_seconds=request_timeout_seconds
        )
        self.neo4j = neo4j_client or _Neo4jBoltClient(self.neo4j_uri, self.neo4j_auth)
        self.proxy_requester = proxy_requester
        self.embedder = embedder or _embedding
        self.max_retries = max(0, int(max_retries))
        self._decision_resolver = decision_resolver or (lambda txn_id: None)
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

    def bind_decision_resolver(
        self, resolver: Callable[[str], str | None]
    ) -> None:
        self._decision_resolver = resolver

    def _transaction_key(self, txn_id: str, sequence: int, operation: str) -> str:
        encoded = json.dumps(
            [self.db_namespace, str(txn_id), int(sequence), str(operation)],
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _payload_hash(value: Any) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _transaction_rows(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        durable_bases: dict[str, int] = {}
        try:
            existing = self.qdrant.retrieve_many_by_txn(
                self.db_namespace, str(txn_id)
            )
        except Exception:
            existing = None
        if isinstance(existing, Mapping) and existing.get("read_ok", False):
            for row in existing.get("rows", []):
                if isinstance(row, Mapping) and row.get("base_version") is not None:
                    durable_bases.setdefault(
                        str(row["memory_id"]), int(row["base_version"])
                    )

        def version_reader(memory_id: str) -> int | None:
            if memory_id in durable_bases:
                return durable_bases[memory_id] or None
            return self._current_version_excluding(memory_id, str(txn_id))

        records = _replay_staged_records(intents, version_reader)
        latest = _latest_record_intents(intents)
        rows: list[dict[str, Any]] = []
        for memory_id, record in records.items():
            intent = latest[memory_id]
            row = copy.deepcopy(record)
            row.update(
                {
                    "txn_id": str(txn_id),
                    "record_kind": "memory",
                    "sequence": int(intent["sequence"]),
                    "operation": str(intent["tool_name"]),
                    "payload_hash": self._payload_hash(record.get("value")),
                    "base_version": version_reader(memory_id) or 0,
                }
            )
            rows.append(row)

        versions: dict[str, int] = {}
        record_tools = {
            "memory_write",
            "memory_derive",
            "memory_propagate",
            "memory_supersede",
        }
        for intent in sorted(intents, key=lambda item: int(item["sequence"])):
            tool_name = str(intent["tool_name"])
            memory_id = _intent_memory_id(intent)
            if tool_name in record_tools and memory_id is not None:
                versions.setdefault(memory_id, version_reader(memory_id) or 0)
                versions[memory_id] += 1
                continue
            if tool_name != "status_overlay":
                continue
            arguments = intent["arguments"]
            memory_id = str(arguments["memory_id"])
            versions.setdefault(memory_id, version_reader(memory_id) or 0)
            versions[memory_id] += 1
            target_status = str(arguments["target_status"])
            rows.append(
                {
                    "txn_id": str(txn_id),
                    "record_kind": "status_overlay",
                    "sequence": int(intent["sequence"]),
                    "operation": "status_overlay",
                    "memory_id": memory_id,
                    "status": "pending",
                    "target_status": target_status,
                    "version": versions[memory_id],
                    "base_version": version_reader(memory_id) or 0,
                    "derived_from": [],
                    "supersedes_id": None,
                    "payload_hash": self._payload_hash(
                        [memory_id, target_status, int(intent["sequence"])]
                    ),
                }
            )
        return sorted(
            rows,
            key=lambda row: (
                int(row["sequence"]),
                str(row["record_kind"]),
                str(row["memory_id"]),
            ),
        )

    @staticmethod
    def _transaction_point_name(row: Mapping[str, Any]) -> str:
        return "txn:{txn_id}:{sequence}:{record_kind}:{memory_id}".format(**row)

    def _safe_transaction_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        safe_fields = (
            "txn_id",
            "record_kind",
            "sequence",
            "operation",
            "memory_id",
            "payload_hash",
            "status",
            "target_status",
            "version",
            "agent_id",
            "scope",
            "derived_from",
            "supersedes_id",
        )
        safe = {
            field: copy.deepcopy(row.get(field))
            for field in safe_fields
            if field in row
        }
        if row.get("record_kind") == "memory" and "value" in row:
            safe["payload_hash"] = self._payload_hash(row["value"])
        safe.setdefault("derived_from", [])
        safe.setdefault("supersedes_id", None)
        return safe

    @staticmethod
    def _safe_edge(edge: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "kind": str(edge["kind"]),
            "source_id": str(edge["source_id"]),
            "status": str(edge["status"]),
            "target_id": str(edge["target_id"]),
            "txn_id": str(edge["txn_id"]),
        }

    @staticmethod
    def _read_failure(exc: Exception) -> dict[str, Any]:
        return {"read_ok": False, "error": type(exc).__name__}

    def _qdrant_transaction_state(self, txn_id: str) -> dict[str, Any]:
        try:
            result = self.qdrant.retrieve_many_by_txn(self.db_namespace, txn_id)
        except Exception as exc:
            return self._read_failure(exc)
        if not isinstance(result, Mapping) or not result.get("read_ok", False):
            error = result.get("error", "readback_failed") if isinstance(result, Mapping) else "invalid_readback"
            return {"read_ok": False, "error": str(error)}
        rows = [
            self._safe_transaction_row(row)
            for row in result.get("rows", [])
            if isinstance(row, Mapping)
        ]
        rows.sort(
            key=lambda row: (
                int(row.get("sequence", 0)),
                str(row.get("record_kind", "")),
                str(row.get("memory_id", "")),
            )
        )
        return {"read_ok": True, "objects": rows}

    def _neo4j_transaction_state(self, txn_id: str) -> dict[str, Any]:
        try:
            result = self.neo4j.retrieve_many_by_txn(self.db_namespace, txn_id)
        except Exception as exc:
            return self._read_failure(exc)
        if not isinstance(result, Mapping) or not result.get("read_ok", False):
            error = result.get("error", "readback_failed") if isinstance(result, Mapping) else "invalid_readback"
            return {"read_ok": False, "error": str(error)}
        nodes = [
            self._safe_transaction_row(row)
            for row in result.get("nodes", [])
            if isinstance(row, Mapping)
        ]
        nodes.sort(
            key=lambda row: (
                int(row.get("sequence", 0)),
                str(row.get("record_kind", "")),
                str(row.get("memory_id", "")),
            )
        )
        edges = [
            self._safe_edge(edge)
            for edge in result.get("edges", [])
            if isinstance(edge, Mapping)
        ]
        edges.sort(
            key=lambda edge: (
                edge["kind"],
                edge["source_id"],
                edge["target_id"],
            )
        )
        return {"read_ok": True, "nodes": nodes, "edges": edges}

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

    def _decision(self, txn_id: str) -> str | None:
        try:
            decision = self._decision_resolver(str(txn_id))
        except Exception:
            return None
        return str(decision).upper() if decision is not None else None

    def _rows_for_memory(self, memory_id: str) -> list[dict[str, Any]]:
        method = getattr(self.qdrant, "retrieve_many_by_memory", None)
        if callable(method):
            result = method(self.db_namespace, memory_id)
            if not isinstance(result, Mapping) or not result.get("read_ok", False):
                raise VectorGraphBackendError("Qdrant memory readback is unknown")
            rows = [
                copy.deepcopy(dict(row))
                for row in result.get("rows", [])
                if isinstance(row, Mapping)
            ]
        else:
            row = self.qdrant.retrieve(self.db_namespace, memory_id)
            rows = [copy.deepcopy(dict(row))] if isinstance(row, Mapping) else []
            for candidate in self.qdrant.search(
                self.db_namespace, self.embedder(memory_id), 1000
            ):
                if (
                    isinstance(candidate, Mapping)
                    and str(candidate.get("memory_id")) == memory_id
                ):
                    rows.append(copy.deepcopy(dict(candidate)))
        if memory_id in self.memories:
            rows = [row for row in rows if row.get("txn_id") is not None]
            rows.append(copy.deepcopy(self.memories[memory_id]))
        return rows

    def _all_rows(self) -> list[dict[str, Any]]:
        method = getattr(self.qdrant, "scan_namespace", None)
        if callable(method):
            result = method(self.db_namespace, 1000)
            if not isinstance(result, Mapping) or not result.get("read_ok", False):
                raise VectorGraphBackendError("Qdrant namespace readback is unknown")
            rows = [
                copy.deepcopy(dict(row))
                for row in result.get("rows", [])
                if isinstance(row, Mapping)
            ]
        else:
            rows = [
                copy.deepcopy(dict(row))
                for row in self.qdrant.search(
                    self.db_namespace, self.embedder(""), 1000
                )
                if isinstance(row, Mapping)
            ]
        local_ids = set(self.memories)
        rows = [
            row
            for row in rows
            if row.get("txn_id") is not None
            or str(row.get("memory_id")) not in local_ids
        ]
        rows.extend(copy.deepcopy(list(self.memories.values())))
        return rows

    @staticmethod
    def _public_record(row: Mapping[str, Any], status: str) -> dict[str, Any]:
        record = copy.deepcopy(dict(row))
        record["status"] = status
        for field in (
            "txn_id",
            "record_kind",
            "sequence",
            "operation",
            "payload_hash",
            "target_status",
            "base_version",
        ):
            record.pop(field, None)
        return record

    def _effective_record(
        self, memory_id: str, rows: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any] | None:
        staged = [
            row
            for row in rows
            if row.get("txn_id") is not None
            and row.get("record_kind") == "memory"
            and self._decision(str(row["txn_id"])) == "COMMITTED"
        ]
        if staged:
            staged_row = max(
                staged,
                key=lambda item: (
                    int(item.get("version", 0)),
                    int(item.get("sequence", 0)),
                    str(item.get("txn_id", "")),
                ),
            )
            direct = [row for row in rows if row.get("txn_id") is None]
            direct_row = (
                max(direct, key=lambda item: int(item.get("version", 1)))
                if direct
                else None
            )
            row = (
                direct_row
                if direct_row is not None
                and int(direct_row.get("version", 1))
                > int(staged_row.get("version", 0))
                else staged_row
            )
            physical_status = str(row.get("status", "pending"))
            status = (
                str(row.get("target_status", "active"))
                if physical_status == "pending"
                else physical_status
            )
            return self._public_record(row, status) if status == "active" else None

        overlays = [
            row
            for row in rows
            if row.get("txn_id") is not None
            and row.get("record_kind") == "status_overlay"
            and self._decision(str(row["txn_id"])) == "COMMITTED"
        ]
        if overlays:
            overlay = max(
                overlays,
                key=lambda item: (
                    str(item.get("txn_id", "")),
                    int(item.get("sequence", 0)),
                ),
            )
            if str(overlay.get("target_status")) != "active":
                return None

        direct = [row for row in rows if row.get("txn_id") is None]
        if not direct:
            return None
        row = max(direct, key=lambda item: int(item.get("version", 1)))
        status = str(row.get("status", "active"))
        return self._public_record(row, status) if status == "active" else None

    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None:
        memory_id = str(memory_id)
        return self._effective_record(memory_id, self._rows_for_memory(memory_id))

    def search_committed(
        self, query: str | None = None
    ) -> list[Mapping[str, Any]]:
        rows = self._all_rows()
        ids = sorted(
            {
                str(row["memory_id"])
                for row in rows
                if row.get("memory_id") is not None
            }
        )
        matches: list[Mapping[str, Any]] = []
        for memory_id in ids:
            candidates = [
                row for row in rows if str(row.get("memory_id")) == memory_id
            ]
            record = self._effective_record(memory_id, candidates)
            if record is None:
                continue
            if query is None or any(
                query == candidate
                for candidate in (
                    record.get("memory_id"),
                    record.get("value"),
                    record.get("attribute"),
                )
            ):
                matches.append(record)
        return matches

    def current_version(self, memory_id: str) -> int | None:
        return self._current_version_excluding(str(memory_id), None)

    def _current_version_excluding(
        self, memory_id: str, excluded_txn_id: str | None
    ) -> int | None:
        rows = self._rows_for_memory(str(memory_id))
        eligible = [
            row
            for row in rows
            if (
                row.get("txn_id") is None
                or (
                    str(row.get("txn_id")) != excluded_txn_id
                    and self._decision(str(row.get("txn_id"))) == "COMMITTED"
                )
            )
        ]
        versions = [int(row.get("version", 1)) for row in eligible]
        return max(versions) if versions else None

    def _compensate_failed_commit(
        self,
        memory_id: str,
        key: str,
        action: Mapping[str, Any],
    ) -> None:
        """Delete both possible writes and require readable proof of absence."""

        for service, operation, function in (
            (
                "qdrant",
                "delete_compensation",
                lambda: self.qdrant.delete(self.db_namespace, memory_id, key),
            ),
            (
                "neo4j",
                "delete_compensation",
                lambda: self.neo4j.delete_memory(self.db_namespace, memory_id, key),
            ),
        ):
            try:
                self._call(service, operation, function, key)
            except Exception:
                # A lost delete response is also ambiguous.  The persisted reads
                # below, rather than the response, decide whether compensation
                # completed safely.
                continue

        verification = self.verify_persistent_state([action])
        classification = verification.get("classification")
        if classification != "absent":
            if classification == "unknown":
                detail = "persistent state is unknown after compensation"
            else:
                detail = f"persistent state is {classification} after compensation"
            raise VectorGraphBackendError(detail)

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
            "version": 1,
        }
        vector = self.embedder(memory["value"])
        try:
            self._call(
                "qdrant",
                "write",
                lambda: self.qdrant.upsert(self.db_namespace, memory_id, vector, memory, key),
                key,
            )
            try:
                self._call(
                    "neo4j",
                    "commit",
                    lambda: self.neo4j.upsert_memory(
                        self.db_namespace, memory_id, memory, source_ids, supersedes_id, key
                    ),
                    key,
                )
            except Exception:
                self._metrics["rollback_count"] += 1
                self._compensate_failed_commit(
                    memory_id,
                    key,
                    {
                        "type": "write",
                        "memory_id": memory_id,
                        "value": memory["value"],
                        "agent_id": memory["agent_id"],
                        "scope": memory["scope"],
                        "source_ids": source_ids,
                        "supersedes_id": supersedes_id,
                    },
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
        memory = (
            self._call(
                "qdrant",
                "retrieve",
                lambda: self.read_committed(str(memory_id)),
                self._key("read", str(memory_id)),
            )
            if memory_id is not None
            else None
        )
        self._event("memory_read", memory_id=memory_id, **fields)
        return copy.deepcopy(memory) if memory is not None else None

    def search(self, query: str | None = None, **fields: Any) -> list[dict[str, Any]]:
        matches = self._call(
            "qdrant",
            "search",
            lambda: [
                copy.deepcopy(dict(row)) for row in self.search_committed(query)
            ],
            self._key("search", str(query or "")),
        )
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
        if memory_id in self.memories or self.qdrant.retrieve(
            self.db_namespace, memory_id
        ) is not None:
            self.invalidate_committed(memory_id)
        self._event("invalidate", memory_id=memory_id, **fields)

    def invalidate_committed(self, memory_id: str) -> Mapping[str, Any]:
        memory_id = str(memory_id)
        stored = self.qdrant.retrieve(self.db_namespace, memory_id)
        if not isinstance(stored, Mapping):
            stored = self.memories.get(memory_id)
        if not isinstance(stored, Mapping):
            raise KeyError(memory_id)
        record = copy.deepcopy(dict(stored))
        record["status"] = "invalid"
        record["version"] = int(record.get("version", 1)) + 1
        key = self._key("invalidate_committed", memory_id)
        self._call(
            "qdrant",
            "invalidate_committed",
            lambda: self.qdrant.upsert(
                self.db_namespace,
                memory_id,
                self.embedder(record.get("value", memory_id)),
                record,
                key,
            ),
            key,
        )
        if hasattr(self.neo4j, "update_status"):
            self._call(
                "neo4j",
                "invalidate_committed",
                lambda: self.neo4j.update_status(
                    self.db_namespace,
                    memory_id,
                    "invalid",
                    key,
                    version=int(record["version"]),
                ),
                key,
            )
        self.memories[memory_id] = copy.deepcopy(record)
        return copy.deepcopy(record)

    def _expected_transaction_state(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        rows = self._transaction_rows(txn_id, intents)
        edges = [
            {
                **edge,
                "status": "pending",
                "txn_id": str(txn_id),
            }
            for edge in _expected_edges(intents)
        ]
        return {
            "rows": rows,
            "safe_rows": [self._safe_transaction_row(row) for row in rows],
            "edges": sorted(
                edges,
                key=lambda edge: (
                    edge["kind"], edge["source_id"], edge["target_id"]
                ),
            ),
        }

    @staticmethod
    def _canonical_verification_row(row: Mapping[str, Any]) -> dict[str, Any]:
        normalized = copy.deepcopy(dict(row))
        if normalized.get("status") in {
            "pending",
            normalized.get("target_status"),
        }:
            normalized["status"] = "durable"
        return normalized

    @staticmethod
    def _canonical_verification_edge(
        edge: Mapping[str, Any], target_status: str
    ) -> dict[str, Any]:
        normalized = copy.deepcopy(dict(edge))
        if normalized.get("status") in {"pending", target_status}:
            normalized["status"] = "durable"
        return normalized

    def _qdrant_matches(
        self, state: Mapping[str, Any], expected: Mapping[str, Any]
    ) -> bool:
        if not state.get("read_ok", False):
            return False
        actual_rows = [
            self._canonical_verification_row(row)
            for row in state.get("objects", [])
        ]
        expected_rows = [
            self._canonical_verification_row(row)
            for row in expected["safe_rows"]
        ]
        return actual_rows == expected_rows

    def _neo4j_matches(
        self, state: Mapping[str, Any], expected: Mapping[str, Any]
    ) -> bool:
        if not state.get("read_ok", False):
            return False
        actual_nodes = [
            self._canonical_verification_row(row)
            for row in state.get("nodes", [])
        ]
        expected_nodes = [
            self._canonical_verification_row(row)
            for row in expected["safe_rows"]
        ]
        target_statuses = {
            str(row["memory_id"]): str(row["target_status"])
            for row in expected["safe_rows"]
            if row.get("record_kind") == "memory"
        }
        actual_edges = [
            self._canonical_verification_edge(
                edge,
                target_statuses.get(
                    str(
                        edge["target_id"]
                        if edge["kind"] == "DERIVED_FROM"
                        else edge["source_id"]
                    ),
                    "active",
                ),
            )
            for edge in state.get("edges", [])
        ]
        expected_edges = [
            self._canonical_verification_edge(
                edge,
                target_statuses.get(
                    str(
                        edge["target_id"]
                        if edge["kind"] == "DERIVED_FROM"
                        else edge["source_id"]
                    ),
                    "active",
                ),
            )
            for edge in expected["edges"]
        ]
        return actual_nodes == expected_nodes and actual_edges == expected_edges

    def stage_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        phase_hook: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        expected = self._expected_transaction_state(txn_id, intents)
        qdrant_errors: list[Exception] = []
        for row in expected["rows"]:
            key = self._transaction_key(
                txn_id, int(row["sequence"]), str(row["operation"])
            )
            try:
                self._call(
                    "qdrant",
                    "stage",
                    lambda row=row, key=key: self.qdrant.upsert(
                        self.db_namespace,
                        self._transaction_point_name(row),
                        self.embedder(
                            row.get("value", row.get("memory_id", ""))
                        ),
                        row,
                        key,
                    ),
                    key,
                )
            except Exception as exc:
                qdrant_errors.append(exc)
        if qdrant_errors and not self._qdrant_matches(
            self._qdrant_transaction_state(txn_id), expected
        ):
            raise qdrant_errors[0]
        qdrant_evidence = {
            "txn_id": txn_id,
            "memory_ids": _expected_ids(intents),
            "row_count": len(expected["rows"]),
        }
        if phase_hook:
            phase_hook("after_qdrant_stage", qdrant_evidence)

        neo4j_errors: list[Exception] = []
        for row in expected["rows"]:
            key = self._transaction_key(
                txn_id, int(row["sequence"]), str(row["operation"])
            )
            try:
                self._call(
                    "neo4j",
                    "stage",
                    lambda row=row, key=key: self.neo4j.upsert_memory(
                        self.db_namespace,
                        str(row["memory_id"]),
                        row,
                        list(row.get("derived_from", [])),
                        row.get("supersedes_id"),
                        key,
                    ),
                    key,
                )
            except Exception as exc:
                neo4j_errors.append(exc)
        if neo4j_errors and not self._neo4j_matches(
            self._neo4j_transaction_state(txn_id), expected
        ):
            raise neo4j_errors[0]
        neo4j_evidence = {
            "txn_id": txn_id,
            "memory_ids": _expected_ids(intents),
            "edge_count": len(expected["edges"]),
            "row_count": len(expected["rows"]),
        }
        if phase_hook:
            phase_hook("after_neo4j_stage", neo4j_evidence)
        return {"qdrant": qdrant_evidence, "neo4j": neo4j_evidence}

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        expected = self._expected_transaction_state(txn_id, intents)
        qdrant = self._qdrant_transaction_state(txn_id)
        neo4j = self._neo4j_transaction_state(txn_id)
        if not qdrant.get("read_ok", False) or not neo4j.get("read_ok", False):
            status = "unknown"
        else:
            qdrant_empty = not qdrant.get("objects", [])
            neo4j_empty = not neo4j.get("nodes", []) and not neo4j.get("edges", [])
            if self._qdrant_matches(qdrant, expected) and self._neo4j_matches(
                neo4j, expected
            ):
                status = "complete"
            elif qdrant_empty and neo4j_empty:
                status = "absent"
            else:
                status = "partial"
        return {"status": status, "txn_id": txn_id}

    def finalize_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        verification = self.verify_transaction(txn_id, intents)
        if verification["status"] != "complete":
            return verification
        expected = self._expected_transaction_state(txn_id, intents)
        for row in expected["rows"]:
            finalized = copy.deepcopy(row)
            finalized["status"] = str(finalized["target_status"])
            key = self._transaction_key(
                txn_id,
                int(row["sequence"]),
                f"finalize:{row['operation']}",
            )
            self._call(
                "qdrant",
                "finalize",
                lambda finalized=finalized, key=key: self.qdrant.upsert(
                    self.db_namespace,
                    self._transaction_point_name(finalized),
                    self.embedder(
                        finalized.get("value", finalized.get("memory_id", ""))
                    ),
                    finalized,
                    key,
                ),
                key,
            )
            self._call(
                "neo4j",
                "finalize",
                lambda finalized=finalized, key=key: self.neo4j.upsert_memory(
                    self.db_namespace,
                    str(finalized["memory_id"]),
                    finalized,
                    list(finalized.get("derived_from", [])),
                    finalized.get("supersedes_id"),
                    key,
                ),
                key,
            )
            if finalized["record_kind"] == "memory":
                committed = self._public_record(
                    finalized, str(finalized["target_status"])
                )
                canonical_key = self._transaction_key(
                    txn_id,
                    int(row["sequence"]),
                    f"finalize_canonical:{row['operation']}",
                )
                self._call(
                    "qdrant",
                    "finalize_canonical",
                    lambda committed=committed, canonical_key=canonical_key: self.qdrant.upsert(
                        self.db_namespace,
                        str(committed["memory_id"]),
                        self.embedder(
                            committed.get("value", committed["memory_id"])
                        ),
                        committed,
                        canonical_key,
                    ),
                    canonical_key,
                )
                self._call(
                    "neo4j",
                    "finalize_canonical",
                    lambda committed=committed, canonical_key=canonical_key: self.neo4j.upsert_memory(
                        self.db_namespace,
                        str(committed["memory_id"]),
                        committed,
                        list(committed.get("derived_from", [])),
                        committed.get("supersedes_id"),
                        canonical_key,
                    ),
                    canonical_key,
                )
                self.memories[str(committed["memory_id"])] = copy.deepcopy(
                    committed
                )

        staged_ids = {
            str(row["memory_id"])
            for row in expected["rows"]
            if row["record_kind"] == "memory"
        }
        overlays_by_memory: dict[str, list[dict[str, Any]]] = {}
        for row in expected["rows"]:
            if row["record_kind"] == "status_overlay":
                overlays_by_memory.setdefault(str(row["memory_id"]), []).append(row)
        for memory_id, overlays in overlays_by_memory.items():
            if memory_id in staged_ids:
                continue
            overlay = max(overlays, key=lambda row: int(row["sequence"]))
            stored = self.qdrant.retrieve(self.db_namespace, memory_id)
            if not isinstance(stored, Mapping):
                stored = self.memories.get(memory_id)
            if not isinstance(stored, Mapping):
                continue
            committed = copy.deepcopy(dict(stored))
            committed["status"] = str(overlay["target_status"])
            committed["version"] = int(overlay["version"])
            key = self._transaction_key(
                txn_id, int(overlay["sequence"]), "finalize:status_overlay"
            )
            self._call(
                "qdrant",
                "finalize_overlay",
                lambda committed=committed, key=key: self.qdrant.upsert(
                    self.db_namespace,
                    memory_id,
                    self.embedder(committed.get("value", memory_id)),
                    committed,
                    key,
                ),
                key,
            )
            if hasattr(self.neo4j, "update_status"):
                self._call(
                    "neo4j",
                    "finalize_overlay",
                    lambda committed=committed, key=key: self.neo4j.update_status(
                        self.db_namespace,
                        memory_id,
                        committed["status"],
                        key,
                        version=int(committed["version"]),
                    ),
                    key,
                )
            if memory_id in self.memories:
                self.memories[memory_id] = copy.deepcopy(committed)
        return {"status": "complete", "txn_id": txn_id}

    def cleanup_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        key = self._transaction_key(txn_id, 0, "cleanup")
        for service, function in (
            (
                "qdrant",
                lambda: self.qdrant.delete_many_by_txn(
                    self.db_namespace, txn_id, key
                ),
            ),
            (
                "neo4j",
                lambda: self.neo4j.delete_many_by_txn(
                    self.db_namespace, txn_id, key
                ),
            ),
        ):
            try:
                self._call(service, "cleanup", function, key)
            except Exception:
                continue
        raw = self.raw_transaction_state(txn_id, intents)
        if not raw["qdrant"].get("read_ok", False) or not raw["neo4j"].get(
            "read_ok", False
        ):
            status = "unknown"
        elif (
            not raw["qdrant"].get("objects", [])
            and not raw["neo4j"].get("nodes", [])
            and not raw["neo4j"].get("edges", [])
        ):
            status = "clean"
        else:
            status = "partial"
        return {"status": status, "txn_id": txn_id}

    def raw_transaction_state(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        qdrant = self._qdrant_transaction_state(txn_id)
        neo4j = self._neo4j_transaction_state(txn_id)
        object_ids = [
            str(row["memory_id"])
            for row in qdrant.get("objects", [])
            if row.get("record_kind") == "memory"
        ]
        visible: list[str] = []
        if qdrant.get("read_ok", False) and self._decision(txn_id) == "COMMITTED":
            for memory_id in sorted(set(object_ids)):
                try:
                    record = self.read_committed(memory_id)
                except Exception:
                    continue
                if record is not None:
                    visible.append(memory_id)
        return {
            "qdrant": qdrant,
            "neo4j": neo4j,
            "gateway_visible": visible,
        }

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return copy.deepcopy(self.memories)

    def verify_persistent_state(
        self, actions: Iterable[Mapping[str, Any]]
    ) -> dict[str, Any]:
        """Classify post-fault Qdrant/Neo4j state without optimistic defaults."""

        items: list[dict[str, Any]] = []
        for action in actions:
            if str(action.get("type", action.get("kind", ""))) != "write":
                continue
            memory_id = str(action.get("memory_id", ""))
            source_ids = sorted(str(item) for item in action.get("source_ids", []))
            expected_qdrant = {
                "memory_id": memory_id,
                "value": action.get("value", memory_id),
                "status": "active",
                "agent_id": action.get("agent_id", "agent_1"),
                "scope": action.get("scope", "tenant:user_001"),
                "derived_from": source_ids,
            }
            expected_neo4j = {
                "status": "active",
                "source_ids": source_ids,
                "supersedes_id": action.get("supersedes_id"),
            }
            item: dict[str, Any] = {"memory_id": memory_id}
            try:
                qdrant_state = self.qdrant.retrieve(self.db_namespace, memory_id)
            except Exception as exc:
                item["qdrant"] = {
                    "read_ok": False,
                    "error": type(exc).__name__,
                }
            else:
                qdrant_present = qdrant_state is not None
                item["qdrant"] = {
                    "read_ok": True,
                    "present": qdrant_present,
                    "matches": bool(
                        qdrant_present
                        and isinstance(qdrant_state, Mapping)
                        and all(
                            qdrant_state.get(field) == value
                            for field, value in expected_qdrant.items()
                        )
                    ),
                }
            try:
                neo4j_state = self.neo4j.retrieve_memory(
                    self.db_namespace, memory_id
                )
            except Exception as exc:
                item["neo4j"] = {
                    "read_ok": False,
                    "error": type(exc).__name__,
                }
            else:
                neo4j_present = neo4j_state is not None
                item["neo4j"] = {
                    "read_ok": True,
                    "present": neo4j_present,
                    "matches": bool(
                        neo4j_present
                        and isinstance(neo4j_state, Mapping)
                        and all(
                            neo4j_state.get(field) == value
                            for field, value in expected_neo4j.items()
                        )
                    ),
                }

            qdrant = item["qdrant"]
            neo4j = item["neo4j"]
            if not qdrant["read_ok"] or not neo4j["read_ok"]:
                classification = "unknown"
            elif not qdrant["present"] and not neo4j["present"]:
                classification = "absent"
            elif (
                qdrant["present"]
                and neo4j["present"]
                and qdrant["matches"]
                and neo4j["matches"]
            ):
                classification = "complete"
            else:
                classification = "partial"
            item["classification"] = classification
            items.append(item)

        classifications = [item["classification"] for item in items]
        if not classifications or "unknown" in classifications:
            overall = "unknown"
        elif "partial" in classifications:
            overall = "partial"
        elif all(value == "complete" for value in classifications):
            overall = "complete"
        elif all(value == "absent" for value in classifications):
            overall = "absent"
        else:
            overall = "partial"
        return {"classification": overall, "items": items}

    def healthcheck(self) -> dict[str, Any]:
        return {
            "namespace": self.db_namespace,
            "qdrant": self.qdrant.healthcheck(),
            "neo4j": self.neo4j.healthcheck(),
        }

    def metrics(self) -> dict[str, Any]:
        return copy.deepcopy(self._metrics)

    def fault_evidence(self) -> dict[str, Any] | None:
        provider = getattr(self.proxy_requester, "evidence", None)
        return copy.deepcopy(provider()) if callable(provider) else None

    def close(self) -> None:
        close = getattr(self.neo4j, "close", None)
        if callable(close):
            close()
