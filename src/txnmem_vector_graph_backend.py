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
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """Promote the immediately-prior identity schema before constraining it."""

        with self.driver.session() as session:
            session.run(
                "MATCH (m:Memory) SET m:MemoryIdentity, "
                "m.canonical=coalesce(m.canonical, true)"
            ).consume()
            for relationship in ("DERIVED_FROM", "SUPERSEDES"):
                session.run(
                    f"MATCH (legacy:MemoryReference)-[old:{relationship}]->(target) "
                    "MATCH (canonical:MemoryIdentity:Memory) "
                    "WHERE canonical.namespace=legacy.namespace "
                    "AND canonical.memory_id=legacy.memory_id "
                    "AND elementId(legacy) <> elementId(canonical) "
                    f"MERGE (canonical)-[replacement:{relationship}]->(target) "
                    "SET replacement += properties(old) DELETE old"
                ).consume()
                session.run(
                    f"MATCH (source)-[old:{relationship}]->(legacy:MemoryReference) "
                    "MATCH (canonical:MemoryIdentity:Memory) "
                    "WHERE canonical.namespace=legacy.namespace "
                    "AND canonical.memory_id=legacy.memory_id "
                    "AND elementId(legacy) <> elementId(canonical) "
                    f"MERGE (source)-[replacement:{relationship}]->(canonical) "
                    "SET replacement += properties(old) DELETE old"
                ).consume()
            session.run(
                "MATCH (legacy:MemoryReference) "
                "MATCH (canonical:MemoryIdentity:Memory) "
                "WHERE canonical.namespace=legacy.namespace "
                "AND canonical.memory_id=legacy.memory_id "
                "AND elementId(legacy) <> elementId(canonical) "
                "DETACH DELETE legacy"
            ).consume()
            session.run(
                "MATCH (r:MemoryReference) SET r:MemoryIdentity "
                "REMOVE r:MemoryReference"
            ).consume()
            session.run(
                "CREATE CONSTRAINT memory_identity_unique IF NOT EXISTS "
                "FOR (m:MemoryIdentity) REQUIRE (m.namespace, m.memory_id) IS UNIQUE"
            ).consume()
            session.run(
                "CREATE CONSTRAINT memory_write_claim_unique IF NOT EXISTS "
                "FOR (c:MemoryWriteClaim) REQUIRE "
                "(c.namespace, c.memory_id, c.target_version) IS UNIQUE"
            ).consume()

    def claim_transaction_writes(
        self, namespace, txn_id, claims, idempotency_key
    ):
        """Atomically reserve every logical identity changed by a task txn."""

        normalized = sorted(
            (
                {
                    "memory_id": str(claim["memory_id"]),
                    "expected_version": int(claim["expected_version"]),
                    "target_version": int(claim["target_version"]),
                    "claim_hash": str(claim["claim_hash"]),
                }
                for claim in claims
            ),
            key=lambda claim: claim["memory_id"],
        )

        def reserve(tx):
            current_rows = []
            for claim in normalized:
                current = tx.run(
                    "MERGE (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                    "SET m._claim_lock=coalesce(m._claim_lock, 0) + 1 "
                    "RETURN coalesce(m.canonical, false) AS canonical, "
                    "m.version AS version",
                    namespace=namespace,
                    memory_id=claim["memory_id"],
                ).single()
                canonical_version = (
                    int(current.get("version") or 1)
                    if current and current.get("canonical")
                    else 0
                )
                if canonical_version > claim["expected_version"]:
                    return {
                        "status": "conflict",
                        "memory_ids": [claim["memory_id"]],
                    }
                if canonical_version < claim["expected_version"]:
                    prior = tx.run(
                        "MATCH (prior:MemoryWriteClaim {namespace:$namespace, "
                        "memory_id:$memory_id}) "
                        "WHERE prior.txn_id IS NOT NULL "
                        "AND prior.target_version > $canonical_version "
                        "AND prior.target_version <= $expected_version "
                        "RETURN collect(prior.target_version) AS versions",
                        namespace=namespace,
                        memory_id=claim["memory_id"],
                        canonical_version=canonical_version,
                        expected_version=claim["expected_version"],
                    ).single()
                    versions = sorted(
                        int(version)
                        for version in ((prior or {}).get("versions") or [])
                    )
                    if versions != list(
                        range(canonical_version + 1, claim["expected_version"] + 1)
                    ):
                        return {
                            "status": "conflict",
                            "memory_ids": [claim["memory_id"]],
                        }
                reservation = tx.run(
                    "MERGE (c:MemoryWriteClaim {namespace:$namespace, "
                    "memory_id:$memory_id, target_version:$target_version}) "
                    "SET c._claim_lock=coalesce(c._claim_lock, 0) + 1 "
                    "RETURN c.txn_id AS claim_txn_id, c.claim_hash AS claim_hash",
                    namespace=namespace,
                    memory_id=claim["memory_id"],
                    target_version=claim["target_version"],
                ).single()
                owner = reservation.get("claim_txn_id") if reservation else None
                same_claim = bool(
                    owner == txn_id
                    and reservation.get("claim_hash") == claim["claim_hash"]
                )
                if owner is not None and not same_claim:
                    return {
                        "status": "conflict",
                        "memory_ids": [claim["memory_id"]],
                    }
                current_rows.append(claim)
            for claim in current_rows:
                tx.run(
                    "MATCH (c:MemoryWriteClaim {namespace:$namespace, "
                    "memory_id:$memory_id, target_version:$target_version}) "
                    "SET c.txn_id=$txn_id, c.claim_hash=$claim_hash",
                    namespace=namespace,
                    memory_id=claim["memory_id"],
                    txn_id=txn_id,
                    target_version=claim["target_version"],
                    claim_hash=claim["claim_hash"],
                ).consume()
            return {"status": "claimed", "memory_ids": []}

        with self.driver.session() as session:
            execute_write = getattr(session, "execute_write", None)
            if callable(execute_write):
                return execute_write(reserve)
            return session.write_transaction(reserve)

    def release_transaction_claims(
        self, namespace, txn_id, idempotency_key
    ):
        with self.driver.session() as session:
            session.run(
                "MATCH (c:MemoryWriteClaim {namespace:$namespace, txn_id:$txn_id}) "
                "DELETE c",
                namespace=namespace,
                txn_id=txn_id,
            ).consume()
        return {"status": "released"}

    def upsert_memory(self, namespace, memory_id, payload, source_ids, supersedes_id, idempotency_key):
        source_ids = sorted({str(source_id) for source_id in source_ids})
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
                        "MERGE (s:MemoryIdentity {namespace:$namespace, memory_id:$source_id}) "
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
                        "MERGE (o:MemoryIdentity {namespace:$namespace, memory_id:$old_id}) "
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
                "MERGE (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "SET m:Memory, m.canonical=true, m.status=$status, m.version=$version",
                namespace=namespace,
                memory_id=memory_id,
                status=payload.get("status", "active"),
                version=int(payload.get("version", 1)),
            ).consume()
            session.run(
                "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "OPTIONAL MATCH (m)-[r:DERIVED_FROM|SUPERSEDES]->() DELETE r",
                namespace=namespace,
                memory_id=memory_id,
            ).consume()
            for source_id in source_ids:
                session.run(
                    "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                    "MERGE (s:MemoryIdentity {namespace:$namespace, memory_id:$source_id}) "
                    "MERGE (m)-[r:DERIVED_FROM]->(s) "
                    "SET r.namespace=$namespace",
                    namespace=namespace,
                    source_id=source_id,
                    memory_id=memory_id,
                ).consume()
            if supersedes_id:
                session.run(
                    "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                    "MERGE (o:MemoryIdentity {namespace:$namespace, memory_id:$old_id}) "
                    "MERGE (m)-[r:SUPERSEDES]->(o) "
                    "SET r.namespace=$namespace",
                    namespace=namespace,
                    memory_id=memory_id,
                    old_id=supersedes_id,
                ).consume()

    def compare_and_set_memory(
        self,
        namespace,
        memory_id,
        payload,
        source_ids,
        supersedes_id,
        expected_version,
        idempotency_key,
        claim_txn_id=None,
    ):
        """Linearize a canonical transition on one Neo4j identity node."""

        source_ids = sorted({str(source_id) for source_id in source_ids})
        desired_version = int(payload.get("version", 1))
        desired_status = str(payload.get("status", "active"))
        desired_hash = str(payload["_canonical_state_hash"])

        def transition(tx):
            current = tx.run(
                "MERGE (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "SET m._cas_lock=coalesce(m._cas_lock, 0) + 1 "
                "WITH m OPTIONAL MATCH (claim:MemoryWriteClaim "
                "{namespace:$namespace, memory_id:$memory_id, "
                "target_version:$target_version}) "
                "RETURN coalesce(m.canonical, false) AS canonical, "
                "m.status AS status, m.version AS version, "
                "m.canonical_state_hash AS state_hash, "
                "m.canonical_source_ids AS source_ids, "
                "m.canonical_supersedes_id AS supersedes_id, "
                "m.canonical_operation_id AS operation_id, "
                "claim.txn_id AS claim_txn_id",
                namespace=namespace,
                memory_id=memory_id,
                target_version=desired_version,
            ).single()
            canonical = bool(current and current.get("canonical"))
            current_version = (
                int(current.get("version") or 1) if canonical else 0
            )
            current_record = None
            current_claim = current.get("claim_txn_id") if current else None
            if current_claim is not None and current_claim != claim_txn_id:
                return {"status": "conflict", "record": None}
            if canonical:
                current_record = {
                    "status": current.get("status"),
                    "version": current_version,
                    "source_ids": sorted(
                        str(item) for item in (current.get("source_ids") or [])
                    ),
                    "supersedes_id": current.get("supersedes_id"),
                    "state_hash": current.get("state_hash"),
                    "operation_id": current.get("operation_id"),
                }
            if current_version > desired_version:
                return {"status": "newer", "record": current_record}
            if current_version == desired_version:
                exact = bool(
                    current_record is not None
                    and str(current_record.get("status")) == desired_status
                    and current_record.get("source_ids") == source_ids
                    and current_record.get("supersedes_id") == supersedes_id
                    and current_record.get("state_hash") == desired_hash
                )
                if exact and current_claim == claim_txn_id:
                    tx.run(
                        "MATCH (claim:MemoryWriteClaim {namespace:$namespace, "
                        "memory_id:$memory_id, target_version:$target_version, "
                        "txn_id:$txn_id}) DELETE claim",
                        namespace=namespace,
                        memory_id=memory_id,
                        target_version=desired_version,
                        txn_id=claim_txn_id,
                    ).consume()
                return {
                    "status": "matched" if exact else "conflict",
                    "record": current_record,
                }
            if current_version != int(expected_version):
                return {"status": "conflict", "record": current_record}
            tx.run(
                "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "SET m:Memory, m.canonical=true, m.status=$status, "
                "m.version=$version, m.canonical_state_hash=$state_hash, "
                "m.canonical_source_ids=$source_ids, "
                "m.canonical_supersedes_id=$supersedes_id, "
                "m.canonical_operation_id=$operation_id",
                namespace=namespace,
                memory_id=memory_id,
                status=desired_status,
                version=desired_version,
                state_hash=desired_hash,
                source_ids=source_ids,
                supersedes_id=supersedes_id,
                operation_id=idempotency_key,
            ).consume()
            if claim_txn_id is not None:
                tx.run(
                    "MATCH (claim:MemoryWriteClaim {namespace:$namespace, "
                    "memory_id:$memory_id, target_version:$target_version, "
                    "txn_id:$txn_id}) DELETE claim",
                    namespace=namespace,
                    memory_id=memory_id,
                    target_version=desired_version,
                    txn_id=claim_txn_id,
                ).consume()
            tx.run(
                "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "OPTIONAL MATCH (m)-[r:DERIVED_FROM|SUPERSEDES]->() DELETE r",
                namespace=namespace,
                memory_id=memory_id,
            ).consume()
            for source_id in source_ids:
                tx.run(
                    "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                    "MERGE (s:MemoryIdentity {namespace:$namespace, memory_id:$source_id}) "
                    "MERGE (m)-[r:DERIVED_FROM]->(s) SET r.namespace=$namespace",
                    namespace=namespace,
                    memory_id=memory_id,
                    source_id=source_id,
                ).consume()
            if supersedes_id:
                tx.run(
                    "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                    "MERGE (o:MemoryIdentity {namespace:$namespace, memory_id:$old_id}) "
                    "MERGE (m)-[r:SUPERSEDES]->(o) SET r.namespace=$namespace",
                    namespace=namespace,
                    memory_id=memory_id,
                    old_id=supersedes_id,
                ).consume()
            return {
                "status": "applied",
                "record": {
                    "status": desired_status,
                    "version": desired_version,
                    "source_ids": source_ids,
                    "supersedes_id": supersedes_id,
                    "state_hash": desired_hash,
                    "operation_id": idempotency_key,
                },
            }

        with self.driver.session() as session:
            execute_write = getattr(session, "execute_write", None)
            if callable(execute_write):
                return execute_write(transition)
            return session.write_transaction(transition)

    def project_if_current(
        self, namespace, memory_id, operation_id, projector
    ):
        """Hold the identity-node write lock while projecting to Qdrant."""

        def project(tx):
            current = tx.run(
                "MATCH (m:MemoryIdentity {namespace:$namespace, memory_id:$memory_id}) "
                "SET m._projection_lock=coalesce(m._projection_lock, 0) + 1 "
                "RETURN m.canonical_operation_id AS operation_id",
                namespace=namespace,
                memory_id=memory_id,
            ).single()
            if current is None or current.get("operation_id") != operation_id:
                return {"status": "newer"}
            projector()
            return {"status": "projected"}

        with self.driver.session() as session:
            execute_write = getattr(session, "execute_write", None)
            if callable(execute_write):
                return execute_write(project)
            return session.write_transaction(project)

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
                "MATCH (m:MemoryIdentity:Memory {namespace:$namespace, memory_id:$memory_id}) "
                "WHERE coalesce(m.canonical, true) "
                "OPTIONAL MATCH (m)-[:DERIVED_FROM]->(s:MemoryIdentity {namespace:$namespace}) "
                "OPTIONAL MATCH (m)-[:SUPERSEDES]->(o:MemoryIdentity {namespace:$namespace}) "
                "RETURN m.status AS status, m.version AS version, "
                "collect(DISTINCT s.memory_id) AS source_ids, "
                "head(collect(DISTINCT o.memory_id)) AS supersedes_id, "
                "m.canonical_state_hash AS state_hash, "
                "m.canonical_operation_id AS operation_id",
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
            "state_hash": record.get("state_hash"),
            "operation_id": record.get("operation_id"),
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
        if memory_id in self.memories and not any(
            row.get("txn_id") is None for row in rows
        ):
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
        stored_canonical_ids = {
            str(row.get("memory_id"))
            for row in rows
            if row.get("txn_id") is None and row.get("memory_id") is not None
        }
        rows.extend(
            copy.deepcopy(memory)
            for memory_id, memory in self.memories.items()
            if memory_id not in stored_canonical_ids
        )
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
            "_canonical_state_hash",
            "_canonical_operation_id",
        ):
            record.pop(field, None)
        return record

    def _effective_record(
        self, memory_id: str, rows: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any] | None:
        eligible = [
            row
            for row in rows
            if row.get("txn_id") is None
            or self._decision(str(row.get("txn_id"))) == "COMMITTED"
        ]
        records = [
            row
            for row in eligible
            if row.get("txn_id") is None or row.get("record_kind") == "memory"
        ]
        if not records:
            return None
        record_version = max(int(row.get("version", 1)) for row in records)
        latest_records = [
            row
            for row in records
            if int(row.get("version", 1)) == record_version
        ]
        resolved_records: list[dict[str, Any]] = []
        for row in latest_records:
            physical_status = str(row.get("status", "active"))
            status = (
                str(row.get("target_status", "active"))
                if row.get("txn_id") is not None and physical_status == "pending"
                else physical_status
            )
            resolved_records.append(self._public_record(row, status))
        fingerprints = {
            json.dumps(
                record,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            )
            for record in resolved_records
        }
        if len(fingerprints) == 1:
            selected = resolved_records[0]
        else:
            canonical_records = [
                self._public_record(row, str(row.get("status", "active")))
                for row in latest_records
                if row.get("txn_id") is None
            ]
            canonical_fingerprints = {
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                )
                for record in canonical_records
            }
            if len(canonical_fingerprints) != 1:
                return None
            selected = canonical_records[0]
        status = str(selected.get("status", "active"))

        overlays = [
            row for row in eligible if row.get("record_kind") == "status_overlay"
        ]
        if overlays:
            overlay_version = max(int(row.get("version", 1)) for row in overlays)
            if overlay_version >= record_version:
                targets = {
                    str(row.get("target_status"))
                    for row in overlays
                    if int(row.get("version", 1)) == overlay_version
                }
                if len(targets) != 1:
                    return None
                overlay_status = next(iter(targets))
                if overlay_version == record_version and status != overlay_status:
                    return None
                status = overlay_status
                selected["version"] = overlay_version
                selected["status"] = status
        return selected if status == "active" else None

    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None:
        memory_id = str(memory_id)
        rows = self._rows_for_memory(memory_id)
        record = self._effective_record(memory_id, rows)
        if record is None:
            return None
        return record if self._visible_record_is_supported(record, rows) else None

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
            if record is None or not self._visible_record_is_supported(
                record, candidates
            ):
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

    @staticmethod
    def _qdrant_canonical_matches(
        current: Mapping[str, Any], desired: Mapping[str, Any]
    ) -> bool:
        return all(
            current.get(field) == value
            for field, value in desired.items()
            if field not in {"namespace"}
        )

    @classmethod
    def _qdrant_states_equal(
        cls,
        current: Mapping[str, Any] | None,
        expected: Mapping[str, Any] | None,
    ) -> bool:
        if current is None or expected is None:
            return current is None and expected is None
        return cls._qdrant_canonical_matches(current, expected)

    @staticmethod
    def _neo4j_states_equal(
        current: Mapping[str, Any] | None,
        expected: Mapping[str, Any] | None,
    ) -> bool:
        if current is None or expected is None:
            return current is None and expected is None
        return (
            str(current.get("status")) == str(expected.get("status"))
            and int(current.get("version", 1)) == int(expected.get("version", 1))
            and sorted(str(item) for item in current.get("source_ids", []))
            == sorted(str(item) for item in expected.get("source_ids", []))
            and current.get("supersedes_id") == expected.get("supersedes_id")
        )

    @staticmethod
    def _neo4j_canonical_matches(
        current: Mapping[str, Any], desired: Mapping[str, Any]
    ) -> bool:
        expected = {
            "status": desired.get("status", "active"),
            "version": int(desired.get("version", 1)),
            "source_ids": sorted(
                str(source_id) for source_id in desired.get("derived_from", [])
            ),
            "supersedes_id": desired.get("supersedes_id"),
            "state_hash": desired.get("_canonical_state_hash"),
        }
        return all(current.get(field) == value for field, value in expected.items())

    @staticmethod
    def _canonical_state_material(
        record: Mapping[str, Any]
    ) -> dict[str, Any]:
        required = (
            "memory_id",
            "value",
            "status",
            "agent_id",
            "scope",
            "derived_from",
            "version",
        )
        missing = [field for field in required if field not in record]
        if missing:
            raise ValueError(
                f"canonical record missing fields: {','.join(missing)}"
            )
        if not isinstance(record["memory_id"], str) or not record[
            "memory_id"
        ]:
            raise ValueError("canonical memory_id is invalid")
        if not isinstance(record["status"], str) or not record["status"]:
            raise ValueError("canonical status is invalid")
        if not isinstance(record["agent_id"], str) or not record["agent_id"]:
            raise ValueError("canonical agent_id is invalid")
        if not isinstance(record["scope"], str) or not record["scope"]:
            raise ValueError("canonical scope is invalid")
        source_ids = record["derived_from"]
        if isinstance(source_ids, (str, bytes)) or not isinstance(
            source_ids, Sequence
        ):
            raise ValueError("canonical provenance is invalid")
        if any(
            not isinstance(source_id, str) or not source_id
            for source_id in source_ids
        ):
            raise ValueError("canonical provenance is invalid")
        canonical_source_ids = sorted(set(source_ids))
        if list(source_ids) != canonical_source_ids:
            raise ValueError("canonical provenance is not canonical")
        version = record["version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValueError("canonical version is invalid")
        supersedes_id = record.get("supersedes_id")
        if supersedes_id is not None and (
            not isinstance(supersedes_id, str) or not supersedes_id
        ):
            raise ValueError("canonical supersedes_id is invalid")
        return {
            "memory_id": record["memory_id"],
            "value": copy.deepcopy(record["value"]),
            "status": record["status"],
            "agent_id": record["agent_id"],
            "scope": record["scope"],
            "derived_from": canonical_source_ids,
            "supersedes_id": supersedes_id,
            "version": version,
        }

    @classmethod
    def _canonical_state_hash(cls, record: Mapping[str, Any]) -> str:
        public = cls._canonical_state_material(record)
        encoded = json.dumps(
            public,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _canonical_payload(
        self, desired: Mapping[str, Any], operation_id: str
    ) -> dict[str, Any]:
        payload = copy.deepcopy(dict(desired))
        payload["_canonical_state_hash"] = self._canonical_state_hash(payload)
        payload["_canonical_operation_id"] = str(operation_id)
        return payload

    def _visible_record_is_supported(
        self,
        record: Mapping[str, Any],
        rows: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Require graph evidence for whichever vector row made a record visible."""

        memory_id = str(record["memory_id"])
        version = int(record.get("version", 1))
        staged = [
            row
            for row in rows
            if row.get("txn_id") is not None
            and int(row.get("version", 1)) == version
            and self._decision(str(row.get("txn_id"))) == "COMMITTED"
            and (
                (
                    row.get("record_kind") == "memory"
                    and self._public_record(
                        row,
                        str(
                            row.get("target_status", "active")
                            if row.get("status") == "pending"
                            else row.get("status", "active")
                        ),
                    )
                    == dict(record)
                )
                or (
                    row.get("record_kind") == "status_overlay"
                    and str(row.get("target_status"))
                    == str(record.get("status"))
                )
            )
        ]
        for row in staged:
            try:
                raw = self.neo4j.retrieve_many_by_txn(
                    self.db_namespace, str(row["txn_id"])
                )
            except Exception:
                continue
            if not isinstance(raw, Mapping) or not raw.get("read_ok", False):
                continue
            expected = self._safe_transaction_row(row)
            if any(
                self._safe_transaction_row(node) == expected
                for node in raw.get("nodes", [])
                if isinstance(node, Mapping)
            ):
                return True
        try:
            canonical = self.neo4j.retrieve_memory(self.db_namespace, memory_id)
        except Exception:
            return False
        desired = self._canonical_payload(record, "visibility")
        desired.pop("_canonical_operation_id", None)
        if canonical is None:
            return False
        expected_hash = self._canonical_state_hash(record)
        current_hash = canonical.get("state_hash")
        if current_hash is not None and current_hash != expected_hash:
            return False
        desired["_canonical_state_hash"] = current_hash
        return self._neo4j_canonical_matches(canonical, desired)

    @staticmethod
    def _canonical_transition_action(
        current: Mapping[str, Any] | None,
        desired: Mapping[str, Any],
        matches: Callable[[Mapping[str, Any], Mapping[str, Any]], bool],
    ) -> str:
        if current is None:
            return "apply"
        current_version = int(current.get("version", 1))
        desired_version = int(desired.get("version", 1))
        if current_version > desired_version:
            return "newer"
        if current_version == desired_version:
            return "matched" if matches(current, desired) else "conflict"
        return "apply"

    def _apply_canonical_transition(
        self,
        desired: Mapping[str, Any],
        *,
        key: str,
        operation: str,
        claim_txn_id: str | None = None,
    ) -> str:
        memory_id = str(desired["memory_id"])
        canonical = self._canonical_payload(desired, key)
        desired_version = int(canonical.get("version", 1))
        compare_and_set = getattr(self.neo4j, "compare_and_set_memory", None)
        if not callable(compare_and_set):
            raise VectorGraphBackendError("Neo4j canonical CAS is unavailable")
        try:
            result = self._call(
                "neo4j",
                f"{operation}_cas",
                lambda: compare_and_set(
                    self.db_namespace,
                    memory_id,
                    canonical,
                    list(canonical.get("derived_from", [])),
                    canonical.get("supersedes_id"),
                    desired_version - 1,
                    key,
                    claim_txn_id,
                ),
                key,
            )
            status = (
                str(result.get("status"))
                if isinstance(result, Mapping)
                else "unknown"
            )
            if status == "newer":
                return "skipped"
            if status not in {"applied", "matched"}:
                raise VectorGraphBackendError(
                    f"canonical version conflict for {memory_id}"
                )
            qdrant_action = "matched"

            def project() -> None:
                nonlocal qdrant_action
                qdrant_current = self._call(
                    "qdrant",
                    f"{operation}_read",
                    lambda: self.qdrant.retrieve(
                        self.db_namespace, memory_id
                    ),
                    key,
                )
                qdrant_action = self._canonical_transition_action(
                    qdrant_current,
                    canonical,
                    self._qdrant_canonical_matches,
                )
                if (
                    qdrant_action == "conflict"
                    and isinstance(qdrant_current, Mapping)
                    and self._qdrant_canonical_matches(
                        qdrant_current,
                        {
                            field: value
                            for field, value in canonical.items()
                            if field
                            not in {
                                "_canonical_state_hash",
                                "_canonical_operation_id",
                            }
                        },
                    )
                ):
                    qdrant_action = "apply"
                if qdrant_action == "newer":
                    raise VectorGraphBackendError(
                        f"canonical projection is newer than authority for {memory_id}"
                    )
                if qdrant_action == "conflict":
                    raise VectorGraphBackendError(
                        f"canonical version conflict for {memory_id}"
                    )
                if qdrant_action == "apply":
                    self._call(
                        "qdrant",
                        operation,
                        lambda: self.qdrant.upsert(
                            self.db_namespace,
                            memory_id,
                            self.embedder(
                                canonical.get("value", memory_id)
                            ),
                            canonical,
                            key,
                        ),
                        key,
                    )

            project_if_current = getattr(
                self.neo4j, "project_if_current", None
            )
            if not callable(project_if_current):
                raise VectorGraphBackendError(
                    "Neo4j projection guard is unavailable"
                )
            projection = self._call(
                "neo4j",
                f"{operation}_projection_guard",
                lambda: project_if_current(
                    self.db_namespace, memory_id, key, project
                ),
                key,
            )
            projection_status = (
                str(projection.get("status"))
                if isinstance(projection, Mapping)
                else "unknown"
            )
            if projection_status == "newer":
                return "skipped"
            if projection_status != "projected":
                raise VectorGraphBackendError(
                    f"canonical projection is recoverable for {memory_id}"
                )
            qdrant_after = self._call(
                "qdrant",
                f"{operation}_verify",
                lambda: self.qdrant.retrieve(self.db_namespace, memory_id),
                key,
            )
            neo4j_after = self._call(
                "neo4j",
                f"{operation}_verify",
                lambda: self.neo4j.retrieve_memory(
                    self.db_namespace, memory_id
                ),
                key,
            )
            if not isinstance(qdrant_after, Mapping) or not isinstance(
                neo4j_after, Mapping
            ):
                raise VectorGraphBackendError(
                    f"canonical transition is incomplete for {memory_id}"
                )
            if not self._qdrant_canonical_matches(
                qdrant_after, canonical
            ) or not self._neo4j_canonical_matches(neo4j_after, canonical):
                raise VectorGraphBackendError(
                    f"canonical transition is incomplete for {memory_id}"
                )
            return "applied" if status == "applied" or qdrant_action == "apply" else "matched"
        except VectorGraphBackendError:
            raise
        except Exception as exc:
            raise VectorGraphBackendError(
                f"canonical transition is recoverable for {memory_id}"
            ) from exc

    def _raw_canonical_state(
        self, memory_id: str, *, key: str, operation: str
    ) -> dict[str, Any]:
        reads: dict[str, Any] = {}
        for service, function in (
            (
                "qdrant",
                lambda: self.qdrant.retrieve(self.db_namespace, memory_id),
            ),
            (
                "neo4j",
                lambda: self.neo4j.retrieve_memory(
                    self.db_namespace, memory_id
                ),
            ),
        ):
            try:
                reads[service] = self._call(
                    service, f"{operation}_raw_read", function, key
                )
            except Exception as exc:
                reads[service] = {"read_ok": False, "error": type(exc).__name__}
        if any(
            isinstance(reads[service], Mapping)
            and reads[service].get("read_ok") is False
            for service in ("qdrant", "neo4j")
        ):
            status = "unknown"
        elif reads["qdrant"] is None and reads["neo4j"] is None:
            status = "absent"
        elif reads["qdrant"] is None or reads["neo4j"] is None:
            status = "partial"
        else:
            qdrant = reads["qdrant"]
            neo4j = reads["neo4j"]
            try:
                recomputed_hash = self._canonical_state_hash(qdrant)
                stored_hash = qdrant["_canonical_state_hash"]
            except (KeyError, TypeError, ValueError):
                status = "partial"
            else:
                expected = {
                    "status": qdrant["status"],
                    "version": int(qdrant["version"]),
                    "derived_from": list(qdrant["derived_from"]),
                    "supersedes_id": qdrant.get("supersedes_id"),
                    "_canonical_state_hash": recomputed_hash,
                }
                status = (
                    "complete"
                    if stored_hash == recomputed_hash
                    and neo4j.get("state_hash") == recomputed_hash
                    and self._neo4j_canonical_matches(neo4j, expected)
                    else "partial"
                )
        return {"status": status, **reads}

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
        source_ids = sorted({str(item) for item in fields.get("source_ids", [])})
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
        if supersedes_id is not None:
            memory["supersedes_id"] = str(supersedes_id)
        try:
            transition = self._apply_canonical_transition(
                memory,
                key=key,
                operation="write",
            )
            if transition == "skipped":
                raise VectorGraphBackendError(
                    f"canonical version advanced during write for {memory_id}"
                )
        except Exception:
            self._metrics["rollback_count"] += 1
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
        memory_id = str(memory_id)
        key = self._key("invalidate_lookup", memory_id)
        existing = self._call(
            "qdrant",
            "invalidate_lookup",
            lambda: self.qdrant.retrieve(self.db_namespace, memory_id),
            key,
        )
        if existing is not None or memory_id in self.memories:
            self.invalidate_committed(memory_id)
        self._event("invalidate", memory_id=memory_id, **fields)

    def invalidate_committed(self, memory_id: str) -> Mapping[str, Any]:
        memory_id = str(memory_id)
        read_key = self._key("invalidate_committed_read", memory_id)
        try:
            qdrant_before = self._call(
                "qdrant",
                "invalidate_committed_canonical_read",
                lambda: self.qdrant.retrieve(self.db_namespace, memory_id),
                read_key,
            )
            neo4j_before = self._call(
                "neo4j",
                "invalidate_committed_canonical_read",
                lambda: self.neo4j.retrieve_memory(
                    self.db_namespace, memory_id
                ),
                read_key,
            )
        except Exception as exc:
            raise VectorGraphBackendError(
                f"invalidate recovery read is unavailable for {memory_id}"
            ) from exc

        recovery_record: dict[str, Any] | None = None
        if isinstance(qdrant_before, Mapping) and isinstance(
            neo4j_before, Mapping
        ):
            if (
                str(qdrant_before.get("status")) == "invalid"
                and str(neo4j_before.get("status")) == "invalid"
                and int(qdrant_before.get("version", 1))
                == int(neo4j_before.get("version", 1))
                and neo4j_before.get("operation_id")
                == self._key(
                    "invalidate_committed",
                    memory_id,
                    [str(int(neo4j_before.get("version", 1)))],
                )
                and qdrant_before.get("_canonical_state_hash")
                == neo4j_before.get("state_hash")
                and self._neo4j_canonical_matches(
                    neo4j_before,
                    {
                        "status": qdrant_before.get("status"),
                        "version": int(qdrant_before.get("version", 1)),
                        "derived_from": list(
                            qdrant_before.get("derived_from", [])
                        ),
                        "supersedes_id": qdrant_before.get("supersedes_id"),
                        "_canonical_state_hash": qdrant_before.get(
                            "_canonical_state_hash"
                        ),
                    },
                )
            ):
                result = self._public_record(qdrant_before, "invalid")
                self.memories[memory_id] = copy.deepcopy(result)
                return result
            if int(neo4j_before.get("version", 1)) > int(
                qdrant_before.get("version", 1)
            ):
                candidate = self._public_record(
                    qdrant_before, str(neo4j_before.get("status", "invalid"))
                )
                candidate["version"] = int(neo4j_before.get("version", 1))
                candidate["derived_from"] = list(
                    neo4j_before.get("source_ids", [])
                )
                if neo4j_before.get("supersedes_id") is not None:
                    candidate["supersedes_id"] = neo4j_before.get(
                        "supersedes_id"
                    )
                if (
                    neo4j_before.get("operation_id")
                    == self._key(
                        "invalidate_committed",
                        memory_id,
                        [str(int(neo4j_before.get("version", 1)))],
                    )
                    and str(neo4j_before.get("status")) == "invalid"
                    and neo4j_before.get("state_hash")
                    == self._canonical_state_hash(candidate)
                ):
                    recovery_record = candidate
                else:
                    raise VectorGraphBackendError(
                        f"canonical version advanced during invalidate for {memory_id}"
                    )

        stored = self._call(
            "qdrant",
            "invalidate_committed_read",
            lambda: self.read_committed(memory_id),
            read_key,
        )
        if recovery_record is not None:
            record = recovery_record
        elif isinstance(stored, Mapping):
            if qdrant_before is None and neo4j_before is None:
                base_key = self._key("invalidate_committed_base", memory_id)
                staged_owner = None
                owner_rows = self._call(
                    "qdrant",
                    "invalidate_committed_owner_read",
                    lambda: self._rows_for_memory(memory_id),
                    base_key,
                )
                for row in owner_rows:
                    if (
                        row.get("txn_id") is not None
                        and row.get("record_kind") == "memory"
                        and self._decision(str(row["txn_id"])) == "COMMITTED"
                        and self._public_record(
                            row,
                            str(
                                row.get("target_status", "active")
                                if row.get("status") == "pending"
                                else row.get("status", "active")
                            ),
                        )
                        == dict(stored)
                    ):
                        staged_owner = str(row["txn_id"])
                        break
                self._apply_canonical_transition(
                    stored,
                    key=base_key,
                    operation="invalidate_committed_base",
                    claim_txn_id=staged_owner,
                )
            record = copy.deepcopy(dict(stored))
            record["status"] = "invalid"
            record["version"] = int(record.get("version", 1)) + 1
        elif isinstance(qdrant_before, Mapping) and str(
            qdrant_before.get("status")
        ) == "invalid":
            record = self._public_record(qdrant_before, "invalid")
        else:
            raise KeyError(memory_id)

        key = self._key(
            "invalidate_committed",
            memory_id,
            [str(int(record.get("version", 1)))],
        )
        transition = self._apply_canonical_transition(
            record,
            key=key,
            operation="invalidate_committed",
        )
        if transition == "skipped":
            raise VectorGraphBackendError(
                f"canonical version advanced during invalidate for {memory_id}"
            )
        record = self._public_record(record, "invalid")
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

    def _transaction_claims(
        self, expected: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        rows_by_memory: dict[str, list[dict[str, Any]]] = {}
        for row in expected["rows"]:
            rows_by_memory.setdefault(str(row["memory_id"]), []).append(
                self._safe_transaction_row(row)
            )
        claims = []
        for memory_id, rows in sorted(rows_by_memory.items()):
            rows.sort(
                key=lambda row: (
                    int(row.get("sequence", 0)),
                    str(row.get("record_kind", "")),
                )
            )
            source_rows = [
                row
                for row in expected["rows"]
                if str(row["memory_id"]) == memory_id
            ]
            claims.append(
                {
                    "memory_id": memory_id,
                    "expected_version": min(
                        int(row.get("base_version", 0)) for row in source_rows
                    ),
                    "target_version": max(
                        int(row.get("version", 1)) for row in source_rows
                    ),
                    "claim_hash": self._payload_hash(rows),
                }
            )
        return claims

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
        claims = self._transaction_claims(expected)
        if claims:
            claim_writes = getattr(
                self.neo4j, "claim_transaction_writes", None
            )
            if not callable(claim_writes):
                raise VectorGraphBackendError(
                    "Neo4j transaction write claims are unavailable"
                )
            claim_key = self._transaction_key(txn_id, 0, "claim")
            claimed = self._call(
                "neo4j",
                "stage_claim",
                lambda: claim_writes(
                    self.db_namespace, txn_id, claims, claim_key
                ),
                claim_key,
            )
            if not isinstance(claimed, Mapping) or claimed.get(
                "status"
            ) != "claimed":
                raise VectorGraphBackendError(
                    f"transaction write conflict for {txn_id}"
                )
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
        try:
            expected = self._expected_transaction_state(txn_id, intents)
        except Exception:
            return {"status": "unknown", "txn_id": txn_id}
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
                transition = self._apply_canonical_transition(
                    committed,
                    key=canonical_key,
                    operation="finalize_canonical",
                    claim_txn_id=txn_id,
                )
                if transition == "skipped":
                    return {"status": "conflict", "txn_id": txn_id}
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
            key = self._transaction_key(
                txn_id, int(overlay["sequence"]), "finalize:status_overlay"
            )
            raw = self._raw_canonical_state(
                memory_id, key=key, operation="finalize_overlay"
            )
            if raw["status"] != "complete":
                return {"status": raw["status"], "txn_id": txn_id}
            stored = raw["qdrant"]
            assert isinstance(stored, Mapping)
            committed = self._public_record(
                stored, str(stored.get("status", "active"))
            )
            committed["status"] = str(overlay["target_status"])
            committed["version"] = int(overlay["version"])
            transition = self._apply_canonical_transition(
                committed,
                key=key,
                operation="finalize_overlay",
                claim_txn_id=txn_id,
            )
            if transition == "skipped":
                return {"status": "conflict", "txn_id": txn_id}
            self.memories[memory_id] = copy.deepcopy(committed)
        return {"status": "complete", "txn_id": txn_id}

    def cleanup_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        key = self._transaction_key(txn_id, 0, "cleanup")
        cleanup_unknown = False
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
        release_claims = getattr(
            self.neo4j, "release_transaction_claims", None
        )
        if callable(release_claims):
            try:
                released = self._call(
                    "neo4j",
                    "cleanup_claim",
                    lambda: release_claims(
                        self.db_namespace, txn_id, key
                    ),
                    key,
                )
                if not isinstance(released, Mapping) or released.get(
                    "status"
                ) != "released":
                    cleanup_unknown = True
            except Exception:
                cleanup_unknown = True
        elif intents:
            cleanup_unknown = True
        raw = self.raw_transaction_state(txn_id, intents)
        if cleanup_unknown or not raw["qdrant"].get("read_ok", False) or not raw["neo4j"].get(
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
