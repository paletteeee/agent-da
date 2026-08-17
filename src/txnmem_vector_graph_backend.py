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

    code = "backend_state_unknown"


class VectorGraphCommitConflict(VectorGraphBackendError):
    """A deterministic canonical CAS/claim conflict, not response loss."""

    code = "backend_commit_conflict"


class _ServiceBoundaryFailure(VectorGraphBackendError):
    """Fallback wrapper when an exhausted exception cannot carry provenance."""

    def __init__(self, service: str, operation: str, cause: Exception):
        super().__init__(f"{service}:{operation} failed: {cause}")
        self.service = str(service)
        self.operation = str(operation)
        self.__cause__ = cause


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
        """Atomically reconcile every legacy identity before installing constraints."""

        def migrate(tx):
            candidates = [
                {
                    "element_id": str(row["element_id"]),
                    "labels": {str(label) for label in row["labels"]},
                    "properties": copy.deepcopy(dict(row["properties"])),
                }
                for row in tx.run(
                    "MATCH (candidate) "
                    "WHERE (candidate:MemoryIdentity OR candidate:Memory "
                    "OR candidate:MemoryReference) "
                    "AND candidate.namespace IS NOT NULL "
                    "AND candidate.memory_id IS NOT NULL "
                    "SET candidate._migration_lock="
                    "coalesce(candidate._migration_lock, 0) + 1 "
                    "RETURN elementId(candidate) AS element_id, "
                    "labels(candidate) AS labels, properties(candidate) AS properties "
                    "ORDER BY candidate.namespace, candidate.memory_id, element_id"
                )
            ]
            groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for candidate in candidates:
                properties = candidate["properties"]
                key = (
                    str(properties["namespace"]),
                    str(properties["memory_id"]),
                )
                groups.setdefault(key, []).append(candidate)

            winners: dict[tuple[str, str], dict[str, Any]] = {}
            replacement_ids: dict[str, str] = {}
            losers: list[dict[str, str]] = []
            for key, group in sorted(groups.items()):
                self._validate_legacy_canonical_group(key, group)
                winner = min(group, key=self._legacy_winner_key)
                merged_labels, merged_properties = self._legacy_merged_winner(
                    winner, group
                )
                winner["merged_labels"] = merged_labels
                winner["merged_properties"] = merged_properties
                winners[key] = winner
                for candidate in group:
                    if candidate["element_id"] == winner["element_id"]:
                        continue
                    replacement_ids[candidate["element_id"]] = winner["element_id"]
                    losers.append(
                        {
                            "element_id": candidate["element_id"],
                            "namespace": key[0],
                            "memory_id": key[1],
                        }
                    )

            loser_ids = sorted(replacement_ids)
            if loser_ids:
                participant_ids = sorted(
                    set(loser_ids) | set(replacement_ids.values())
                )
                relationship_rows = list(
                    tx.run(
                        "MATCH (source)-[relationship:DERIVED_FROM|SUPERSEDES]->(target) "
                        "WHERE elementId(source) IN $participant_ids "
                        "OR elementId(target) IN $participant_ids "
                        "RETURN elementId(relationship) AS relationship_id, "
                        "type(relationship) AS relationship_type, "
                        "elementId(source) AS source_id, elementId(target) AS target_id, "
                        "properties(relationship) AS properties "
                        "ORDER BY relationship_id",
                        participant_ids=participant_ids,
                    )
                )
                tx.run(
                    "MATCH ()-[relationship]->() "
                    "WHERE elementId(relationship) IN $relationship_ids "
                    "DELETE relationship",
                    relationship_ids=[
                        str(row["relationship_id"])
                        for row in relationship_rows
                    ],
                ).consume()
                relationships_by_fingerprint: dict[str, dict[str, Any]] = {}
                for row in relationship_rows:
                    original_source_id = str(row["source_id"])
                    original_target_id = str(row["target_id"])
                    source_id = replacement_ids.get(
                        original_source_id, original_source_id
                    )
                    target_id = replacement_ids.get(
                        original_target_id, original_target_id
                    )
                    if (
                        source_id == target_id
                        and original_source_id != original_target_id
                    ):
                        continue
                    relationship = {
                        "relationship_type": str(row["relationship_type"]),
                        "source_id": source_id,
                        "target_id": target_id,
                        "properties": copy.deepcopy(dict(row["properties"])),
                    }
                    fingerprint = json.dumps(
                        relationship,
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                        separators=(",", ":"),
                    )
                    relationships_by_fingerprint.setdefault(
                        fingerprint, relationship
                    )
                relationships = [
                    relationships_by_fingerprint[fingerprint]
                    for fingerprint in sorted(relationships_by_fingerprint)
                ]
                for relationship_type in ("DERIVED_FROM", "SUPERSEDES"):
                    rewired = [
                        {
                            "source_id": relationship["source_id"],
                            "target_id": relationship["target_id"],
                            "properties": relationship["properties"],
                        }
                        for relationship in relationships
                        if relationship["relationship_type"] == relationship_type
                    ]
                    if not rewired:
                        continue
                    tx.run(
                        "UNWIND $relationships AS edge "
                        "MATCH (source) WHERE elementId(source)=edge.source_id "
                        "MATCH (target) WHERE elementId(target)=edge.target_id "
                        f"CREATE (source)-[replacement:{relationship_type}]->(target) "
                        "SET replacement = edge.properties",
                        relationships=rewired,
                    ).consume()

            for key, winner in sorted(winners.items()):
                labels = sorted(
                    set(winner["merged_labels"]) - {"MemoryIdentity"}
                )
                label_clause = "".join(
                    f":`{label.replace('`', '``')}`" for label in labels
                )
                tx.run(
                    "MATCH (winner) WHERE elementId(winner)=$winner_id "
                    "AND winner.namespace=$namespace "
                    "AND winner.memory_id=$memory_id "
                    f"SET winner:MemoryIdentity{label_clause}, "
                    "winner += $merged_properties, "
                    "winner.canonical=CASE WHEN $canonical "
                    "THEN true "
                    "ELSE coalesce(winner.canonical, false) END "
                    "REMOVE winner._migration_lock",
                    winner_id=winner["element_id"],
                    namespace=key[0],
                    memory_id=key[1],
                    canonical=self._legacy_is_canonical(winner),
                    merged_labels=sorted(winner["merged_labels"]),
                    merged_properties=winner["merged_properties"],
                ).consume()
            if losers:
                tx.run(
                    "UNWIND $losers AS loser_row MATCH (loser) "
                    "WHERE elementId(loser)=loser_row.element_id "
                    "AND loser.namespace=loser_row.namespace "
                    "AND loser.memory_id=loser_row.memory_id "
                    "AND (loser:MemoryIdentity OR loser:Memory "
                    "OR loser:MemoryReference) DETACH DELETE loser",
                    losers=sorted(losers, key=lambda row: row["element_id"]),
                ).consume()
            tx.run(
                "MATCH (claim:MemoryWriteClaim) "
                "SET claim.base_version=coalesce(claim.base_version, "
                "claim.target_version - 1), "
                "claim.final_version=coalesce(claim.final_version, "
                "claim.target_version)"
            ).consume()
            return {"winner_count": len(winners), "loser_count": len(losers)}

        with self.driver.session() as session:
            execute_write = getattr(session, "execute_write", None)
            if callable(execute_write):
                execute_write(migrate)
            else:  # pragma: no cover - compatibility with older driver shims
                session.write_transaction(migrate)
        with self.driver.session() as session:
            session.run(
                "CREATE CONSTRAINT memory_identity_unique IF NOT EXISTS "
                "FOR (m:MemoryIdentity) REQUIRE (m.namespace, m.memory_id) IS UNIQUE"
            ).consume()
            session.run(
                "DROP CONSTRAINT memory_write_claim_unique IF EXISTS"
            ).consume()
            session.run(
                "CREATE CONSTRAINT memory_write_claim_unique IF NOT EXISTS "
                "FOR (c:MemoryWriteClaim) REQUIRE "
                "(c.namespace, c.memory_id, c.base_version, c.final_version) IS UNIQUE"
            ).consume()

    @staticmethod
    def _legacy_is_canonical(candidate: Mapping[str, Any]) -> bool:
        properties = candidate["properties"]
        state_hash = properties.get("canonical_state_hash") or properties.get(
            "_canonical_state_hash"
        )
        canonical_evidence = (
            state_hash is not None
            and properties.get("status") is not None
            and properties.get("version") is not None
        )
        return bool(
            properties.get("canonical") is True
            or "Memory" in candidate["labels"]
            or canonical_evidence
        )

    @staticmethod
    def _legacy_version(candidate: Mapping[str, Any]) -> int:
        try:
            return int(candidate["properties"].get("version") or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _legacy_winner_key(cls, candidate: Mapping[str, Any]) -> tuple[Any, ...]:
        labels = set(candidate["labels"])
        properties = candidate["properties"]
        canonical_fields = (
            "canonical_state_hash",
            "_canonical_state_hash",
            "status",
            "value",
            "canonical_source_ids",
            "derived_from",
            "canonical_supersedes_id",
            "supersedes_id",
        )
        richness = sum(properties.get(field) is not None for field in canonical_fields)
        state_hash = properties.get("canonical_state_hash") or properties.get(
            "_canonical_state_hash"
        )
        label_rank = (
            4
            if {"MemoryIdentity", "Memory"}.issubset(labels)
            else 3
            if "Memory" in labels
            else 2
            if "MemoryIdentity" in labels
            else 1
        )
        return (
            -int(cls._legacy_is_canonical(candidate)),
            -cls._legacy_version(candidate),
            -int(state_hash is not None),
            -richness,
            -label_rank,
            str(candidate["element_id"]),
        )

    @classmethod
    def _legacy_merged_winner(
        cls,
        winner: Mapping[str, Any],
        group: Sequence[Mapping[str, Any]],
    ) -> tuple[set[str], dict[str, Any]]:
        labels = {
            str(label)
            for candidate in group
            for label in candidate["labels"]
        }
        labels.add("MemoryIdentity")
        if cls._legacy_is_canonical(winner):
            labels.add("Memory")

        ignored = {
            "_migration_lock",
            "_claim_lock",
            "_cas_lock",
            "_projection_lock",
        }
        canonical_fields = (
            {
                "namespace",
                "memory_id",
                "canonical",
                "version",
                "status",
                "value",
                "agent_id",
                "scope",
                "canonical_state_hash",
                "_canonical_state_hash",
                "canonical_source_ids",
                "derived_from",
                "canonical_supersedes_id",
                "supersedes_id",
                "canonical_operation_id",
                "_canonical_operation_id",
            }
            if cls._legacy_is_canonical(winner)
            else set()
        )
        merged = {
            str(field): copy.deepcopy(value)
            for field, value in winner["properties"].items()
            if field not in ignored
        }
        fields = sorted(
            {
                str(field)
                for candidate in group
                for field in candidate["properties"]
                if field not in ignored
            }
        )
        for field in fields:
            if field in canonical_fields:
                continue
            if merged.get(field) is not None:
                continue
            values: dict[str, Any] = {}
            for candidate in group:
                value = candidate["properties"].get(field)
                if value is None:
                    continue
                fingerprint = json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                )
                values.setdefault(fingerprint, value)
            if len(values) == 1:
                merged[field] = copy.deepcopy(next(iter(values.values())))
        if cls._legacy_is_canonical(winner):
            merged["canonical"] = True
        return labels, merged

    @staticmethod
    def _legacy_canonical_evidence(candidate: Mapping[str, Any]) -> dict[str, Any]:
        properties = candidate["properties"]
        source_ids = properties.get("canonical_source_ids")
        if source_ids is None:
            source_ids = properties.get("derived_from")
        if isinstance(source_ids, (list, tuple, set)):
            source_ids = sorted(str(item) for item in source_ids)
        return {
            "state_hash": properties.get("canonical_state_hash")
            or properties.get("_canonical_state_hash"),
            "status": properties.get("status"),
            "value": properties.get("value"),
            "scope": properties.get("scope"),
            "agent_id": properties.get("agent_id"),
            "source_ids": source_ids,
            "supersedes_id": properties.get("canonical_supersedes_id")
            if properties.get("canonical_supersedes_id") is not None
            else properties.get("supersedes_id"),
        }

    @classmethod
    def _validate_legacy_canonical_group(
        cls,
        key: tuple[str, str],
        group: Sequence[Mapping[str, Any]],
    ) -> None:
        by_version: dict[int, list[Mapping[str, Any]]] = {}
        for candidate in group:
            if cls._legacy_is_canonical(candidate):
                by_version.setdefault(cls._legacy_version(candidate), []).append(candidate)
        for version, candidates in by_version.items():
            evidence = [cls._legacy_canonical_evidence(item) for item in candidates]
            for left_index, left in enumerate(evidence):
                for right in evidence[left_index + 1 :]:
                    divergent = any(
                        left[field] is not None
                        and right[field] is not None
                        and left[field] != right[field]
                        for field in left
                    )
                    if divergent:
                        raise VectorGraphBackendError(
                            "divergent canonical legacy identities for "
                            f"{key[0]}:{key[1]} at version {version}"
                        )

    def claim_transaction_writes(
        self, namespace, txn_id, claims, idempotency_key
    ):
        """Atomically reserve every logical identity changed by a task txn."""

        normalized = sorted(
            (
                {
                    "memory_id": str(claim["memory_id"]),
                    "base_version": int(claim["base_version"]),
                    "final_version": int(claim["final_version"]),
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
                if canonical_version > claim["base_version"]:
                    return {
                        "status": "conflict",
                        "memory_ids": [claim["memory_id"]],
                    }
                reservations = list(tx.run(
                    "MATCH (c:MemoryWriteClaim {namespace:$namespace, "
                    "memory_id:$memory_id}) WHERE c.txn_id IS NOT NULL "
                    "RETURN coalesce(c.base_version, c.target_version - 1) AS base_version, "
                    "coalesce(c.final_version, c.target_version) AS final_version, "
                    "c.txn_id AS claim_txn_id, c.claim_hash AS claim_hash "
                    "ORDER BY base_version, final_version, claim_txn_id",
                    namespace=namespace,
                    memory_id=claim["memory_id"],
                ))
                existing = [
                    {
                        "base_version": int(row["base_version"]),
                        "final_version": int(row["final_version"]),
                        "txn_id": str(row["claim_txn_id"]),
                        "claim_hash": str(row["claim_hash"]),
                    }
                    for row in reservations
                ]
                if canonical_version < claim["base_version"]:
                    cursor = canonical_version
                    while cursor < claim["base_version"]:
                        continuations = [
                            row
                            for row in existing
                            if row["base_version"] == cursor
                            and row["final_version"] <= claim["base_version"]
                        ]
                        if len(continuations) != 1:
                            break
                        cursor = continuations[0]["final_version"]
                    if cursor != claim["base_version"]:
                        return {
                            "status": "conflict",
                            "memory_ids": [claim["memory_id"]],
                        }
                overlapping = [
                    row
                    for row in existing
                    if row["base_version"] < claim["final_version"]
                    and claim["base_version"] < row["final_version"]
                ]
                same_claim = [
                    row
                    for row in overlapping
                    if row["base_version"] == claim["base_version"]
                    and row["final_version"] == claim["final_version"]
                    and row["txn_id"] == txn_id
                    and row["claim_hash"] == claim["claim_hash"]
                ]
                if overlapping and len(same_claim) != len(overlapping):
                    return {
                        "status": "conflict",
                        "memory_ids": [claim["memory_id"]],
                    }
                current_rows.append(claim)
            for claim in current_rows:
                tx.run(
                    "MERGE (c:MemoryWriteClaim {namespace:$namespace, "
                    "memory_id:$memory_id, base_version:$base_version, "
                    "final_version:$final_version}) "
                    "SET c.txn_id=$txn_id, c.claim_hash=$claim_hash, "
                    "c.target_version=$final_version",
                    namespace=namespace,
                    memory_id=claim["memory_id"],
                    txn_id=txn_id,
                    base_version=claim["base_version"],
                    final_version=claim["final_version"],
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
                    "m.base_version=$base_version, "
                    "m.staged_state_hash=$staged_state_hash, "
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
                    staged_state_hash=payload["staged_state_hash"],
                    version=int(payload["version"]),
                    base_version=int(payload.get("base_version", 0)),
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
                "{namespace:$namespace, memory_id:$memory_id}) "
                "WHERE claim IS NULL OR ("
                "coalesce(claim.base_version, claim.target_version - 1) "
                "< $final_version AND $base_version < "
                "coalesce(claim.final_version, claim.target_version)) "
                "RETURN coalesce(m.canonical, false) AS canonical, "
                "m.status AS status, m.version AS version, "
                "m.canonical_state_hash AS state_hash, "
                "m.canonical_source_ids AS source_ids, "
                "m.canonical_supersedes_id AS supersedes_id, "
                "m.canonical_operation_id AS operation_id, "
                "collect(claim.txn_id) AS claim_txn_ids",
                namespace=namespace,
                memory_id=memory_id,
                base_version=int(expected_version),
                final_version=desired_version,
            ).single()
            canonical = bool(current and current.get("canonical"))
            current_version = (
                int(current.get("version") or 1) if canonical else 0
            )
            current_record = None
            current_claims = {
                str(owner)
                for owner in ((current or {}).get("claim_txn_ids") or [])
                if owner is not None
            }
            if any(owner != claim_txn_id for owner in current_claims):
                return {"status": "conflict", "record": None}
            current_claim = (
                claim_txn_id if claim_txn_id in current_claims else None
            )
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
                        "memory_id:$memory_id, base_version:$base_version, "
                        "final_version:$final_version, "
                        "txn_id:$txn_id}) DELETE claim",
                        namespace=namespace,
                        memory_id=memory_id,
                        base_version=int(expected_version),
                        final_version=desired_version,
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
                    "memory_id:$memory_id, base_version:$base_version, "
                    "final_version:$final_version, "
                    "txn_id:$txn_id}) DELETE claim",
                    namespace=namespace,
                    memory_id=memory_id,
                    base_version=int(expected_version),
                    final_version=desired_version,
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

    def retrieve_txn_ids_by_memory(self, namespace, memory_id):
        try:
            with self.driver.session() as session:
                record = session.run(
                    "MATCH (m:TxnMemory {namespace:$namespace, memory_id:$memory_id}) "
                    "RETURN collect(DISTINCT m.txn_id) AS txn_ids",
                    namespace=namespace,
                    memory_id=memory_id,
                ).single()
        except Exception as exc:
            return {"read_ok": False, "error": type(exc).__name__}
        return {
            "read_ok": True,
            "txn_ids": sorted(
                str(txn_id)
                for txn_id in ((record or {}).get("txn_ids") or [])
                if txn_id is not None
            ),
        }

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

    def _staged_state_material(
        self, row: Mapping[str, Any]
    ) -> dict[str, Any]:
        required = {
            "txn_id",
            "record_kind",
            "sequence",
            "operation",
            "memory_id",
            "payload_hash",
            "status",
            "target_status",
            "version",
            "base_version",
            "derived_from",
            "supersedes_id",
        }
        if not required.issubset(row):
            raise ValueError("incomplete staged state")
        record_kind = str(row["record_kind"])
        if record_kind not in {"memory", "status_overlay"}:
            raise ValueError("unsupported staged record kind")
        target_status = str(row["target_status"])
        physical_status = str(row["status"])
        if physical_status not in {"pending", target_status}:
            raise ValueError("invalid staged physical status")
        source_ids = row["derived_from"]
        if not isinstance(source_ids, (list, tuple, set)):
            raise ValueError("invalid staged provenance")

        material = {
            str(field): copy.deepcopy(value)
            for field, value in row.items()
            if field not in {"namespace", "staged_state_hash", "value"}
        }
        material["namespace"] = self.db_namespace
        material["txn_id"] = str(row["txn_id"])
        material["record_kind"] = record_kind
        material["sequence"] = int(row["sequence"])
        material["operation"] = str(row["operation"])
        material["memory_id"] = str(row["memory_id"])
        material["status"] = "durable"
        material["target_status"] = target_status
        material["version"] = int(row["version"])
        material["base_version"] = int(row["base_version"])
        material["derived_from"] = sorted(
            {str(source_id) for source_id in source_ids}
        )
        material["supersedes_id"] = (
            str(row["supersedes_id"])
            if row.get("supersedes_id") is not None
            else None
        )
        if record_kind == "memory":
            if "value" not in row:
                raise ValueError("staged memory value is missing")
            payload_hash = self._payload_hash(row["value"])
        else:
            payload_hash = self._payload_hash(
                [
                    material["memory_id"],
                    target_status,
                    material["sequence"],
                ]
            )
        if str(row["payload_hash"]) != payload_hash:
            raise ValueError("staged payload hash mismatch")
        material["payload_hash"] = payload_hash
        return material

    def _staged_state_hash(self, row: Mapping[str, Any]) -> str:
        return self._payload_hash(self._staged_state_material(row))

    def _staged_row_evidence(
        self,
        row: Mapping[str, Any],
        *,
        qdrant_payload: bool,
        normalize_status: bool,
    ) -> dict[str, Any] | None:
        required = {
            "txn_id",
            "record_kind",
            "sequence",
            "operation",
            "memory_id",
            "payload_hash",
            "status",
            "target_status",
            "version",
            "base_version",
            "derived_from",
            "staged_state_hash",
        }
        if not required.issubset(row):
            return None
        try:
            record_kind = str(row["record_kind"])
            if record_kind not in {"memory", "status_overlay"}:
                return None
            target_status = str(row["target_status"])
            physical_status = str(row["status"])
            if physical_status not in {"pending", target_status}:
                return None
            source_ids = row["derived_from"]
            if not isinstance(source_ids, (list, tuple, set)):
                return None
            if qdrant_payload:
                payload_hash = self._staged_state_material(row)["payload_hash"]
                if str(row["staged_state_hash"]) != self._staged_state_hash(row):
                    return None
            else:
                payload_hash = str(row["payload_hash"])
            return {
                "txn_id": str(row["txn_id"]),
                "record_kind": record_kind,
                "sequence": int(row["sequence"]),
                "operation": str(row["operation"]),
                "memory_id": str(row["memory_id"]),
                "payload_hash": payload_hash,
                "staged_state_hash": str(row["staged_state_hash"]),
                "status": "durable" if normalize_status else physical_status,
                "target_status": target_status,
                "version": int(row["version"]),
                "base_version": int(row["base_version"]),
                "agent_id": row.get("agent_id")
                if record_kind == "memory"
                else None,
                "scope": row.get("scope")
                if record_kind == "memory"
                else None,
                "derived_from": sorted(
                    {str(source_id) for source_id in source_ids}
                ),
                "supersedes_id": (
                    str(row["supersedes_id"])
                    if row.get("supersedes_id") is not None
                    else None
                ),
            }
        except (KeyError, TypeError, ValueError):
            return None

    def _transaction_rows(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        durable_bases: dict[str, int] = {}
        try:
            existing = self._call(
                "qdrant",
                "stage_base_read",
                lambda: self._require_readback(
                    self.qdrant.retrieve_many_by_txn(
                        self.db_namespace, str(txn_id)
                    ),
                    "Qdrant staged base readback is unknown",
                ),
                self._transaction_key(str(txn_id), 0, "stage_base_read"),
            )
        except Exception:
            existing = None
        if isinstance(existing, Mapping) and existing.get("read_ok", False):
            for row in existing.get("rows", []):
                if isinstance(row, Mapping) and row.get("base_version") is not None:
                    durable_bases.setdefault(
                        str(row["memory_id"]), int(row["base_version"])
                    )

        observed_versions: dict[str, int | None] = {}

        def version_reader(memory_id: str) -> int | None:
            if memory_id in durable_bases:
                return durable_bases[memory_id] or None
            if memory_id not in observed_versions:
                observed_versions[memory_id] = self._current_version_excluding(
                    memory_id, str(txn_id)
                )
            return observed_versions[memory_id]

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
        for row in rows:
            row["staged_state_hash"] = self._staged_state_hash(row)
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
            "base_version",
            "staged_state_hash",
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

    @staticmethod
    def _require_readback(result: Any, detail: str) -> Mapping[str, Any]:
        if not isinstance(result, Mapping) or not result.get("read_ok", False):
            raise VectorGraphBackendError(detail)
        return result

    def _qdrant_transaction_evidence(
        self, txn_id: str, *, operation: str = "transaction_readback"
    ) -> dict[str, Any]:
        key = self._transaction_key(txn_id, 0, "qdrant_readback")
        try:
            result = self._call(
                "qdrant",
                operation,
                lambda: self._require_readback(
                    self.qdrant.retrieve_many_by_txn(
                        self.db_namespace, txn_id
                    ),
                    "Qdrant transaction readback is unknown",
                ),
                key,
            )
        except Exception as exc:
            return self._read_failure(exc)
        if not isinstance(result, Mapping) or not result.get("read_ok", False):
            error = result.get("error", "readback_failed") if isinstance(result, Mapping) else "invalid_readback"
            return {"read_ok": False, "error": str(error)}
        rows = [
            copy.deepcopy(dict(row))
            for row in result.get("rows", [])
            if isinstance(row, Mapping)
        ]
        rows.sort(
            key=lambda row: (
                str(row.get("sequence", "")),
                str(row.get("record_kind", "")),
                str(row.get("memory_id", "")),
            )
        )
        return {"read_ok": True, "rows": rows}

    def _neo4j_transaction_evidence(
        self, txn_id: str, *, operation: str = "transaction_readback"
    ) -> dict[str, Any]:
        key = self._transaction_key(txn_id, 0, "neo4j_readback")
        try:
            result = self._call(
                "neo4j",
                operation,
                lambda: self._require_readback(
                    self.neo4j.retrieve_many_by_txn(
                        self.db_namespace, txn_id
                    ),
                    "Neo4j transaction readback is unknown",
                ),
                key,
            )
        except Exception as exc:
            return self._read_failure(exc)
        if not isinstance(result, Mapping) or not result.get("read_ok", False):
            error = result.get("error", "readback_failed") if isinstance(result, Mapping) else "invalid_readback"
            return {"read_ok": False, "error": str(error)}
        nodes = [
            copy.deepcopy(dict(row))
            for row in result.get("nodes", [])
            if isinstance(row, Mapping)
        ]
        nodes.sort(
            key=lambda row: (
                str(row.get("sequence", "")),
                str(row.get("record_kind", "")),
                str(row.get("memory_id", "")),
            )
        )
        edges = [
            copy.deepcopy(dict(edge))
            for edge in result.get("edges", [])
            if isinstance(edge, Mapping)
        ]
        edges.sort(
            key=lambda edge: (
                str(edge.get("kind", "")),
                str(edge.get("source_id", "")),
                str(edge.get("target_id", "")),
            )
        )
        return {"read_ok": True, "nodes": nodes, "edges": edges}

    def _qdrant_transaction_state(self, txn_id: str) -> dict[str, Any]:
        evidence = self._qdrant_transaction_evidence(txn_id)
        if not evidence.get("read_ok", False):
            return evidence
        return {
            "read_ok": True,
            "objects": [
                self._safe_transaction_row(row)
                for row in evidence.get("rows", [])
            ],
        }

    def _neo4j_transaction_state(self, txn_id: str) -> dict[str, Any]:
        evidence = self._neo4j_transaction_evidence(txn_id)
        if not evidence.get("read_ok", False):
            return evidence
        return {
            "read_ok": True,
            "nodes": [
                self._safe_transaction_row(row)
                for row in evidence.get("nodes", [])
            ],
            "edges": [
                self._safe_edge(edge)
                for edge in evidence.get("edges", [])
            ],
        }

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
            except Exception as exc:
                if getattr(exc, "_txnmem_service", None) is not None or isinstance(
                    exc, _ServiceBoundaryFailure
                ):
                    raise
                if attempts > self.max_retries:
                    self._metrics["error_count"] += 1
                    try:
                        setattr(exc, "_txnmem_service", str(service))
                        setattr(exc, "_txnmem_operation", str(operation))
                    except Exception:
                        raise _ServiceBoundaryFailure(
                            service, operation, exc
                        ) from exc
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

    def _rows_for_memory(
        self, memory_id: str, *, operation: str = "memory"
    ) -> list[dict[str, Any]]:
        key = self._key(f"{operation}_rows", memory_id)
        method = getattr(self.qdrant, "retrieve_many_by_memory", None)
        if callable(method):
            result = self._call(
                "qdrant",
                f"{operation}_rows",
                lambda: self._require_readback(
                    method(self.db_namespace, memory_id),
                    "Qdrant memory readback is unknown",
                ),
                key,
            )
            if not isinstance(result, Mapping) or not result.get("read_ok", False):
                raise VectorGraphBackendError("Qdrant memory readback is unknown")
            rows = [
                copy.deepcopy(dict(row))
                for row in result.get("rows", [])
                if isinstance(row, Mapping)
            ]
        else:
            row = self._call(
                "qdrant",
                f"{operation}_canonical_row",
                lambda: self.qdrant.retrieve(self.db_namespace, memory_id),
                key,
            )
            rows = [copy.deepcopy(dict(row))] if isinstance(row, Mapping) else []
            searched = self._call(
                "qdrant",
                f"{operation}_staged_rows",
                lambda: self.qdrant.search(
                    self.db_namespace, self.embedder(memory_id), 1000
                ),
                key,
            )
            for candidate in searched:
                if (
                    isinstance(candidate, Mapping)
                    and str(candidate.get("memory_id")) == memory_id
                ):
                    rows.append(copy.deepcopy(dict(candidate)))
        unique: dict[str, dict[str, Any]] = {}
        for row in rows:
            fingerprint = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            )
            unique.setdefault(fingerprint, row)
        return list(unique.values())

    def _all_rows(self, *, operation: str = "search") -> list[dict[str, Any]]:
        key = self._key(f"{operation}_rows", "*")
        method = getattr(self.qdrant, "scan_namespace", None)
        if callable(method):
            result = self._call(
                "qdrant",
                f"{operation}_rows",
                lambda: self._require_readback(
                    method(self.db_namespace, 1000),
                    "Qdrant namespace readback is unknown",
                ),
                key,
            )
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
                for row in self._call(
                    "qdrant",
                    f"{operation}_rows",
                    lambda: self.qdrant.search(
                        self.db_namespace, self.embedder(""), 1000
                    ),
                    key,
                )
                if isinstance(row, Mapping)
            ]
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
            "staged_state_hash",
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

    @staticmethod
    def _record_fingerprint(record: Mapping[str, Any]) -> str:
        return json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )

    def _canonical_evidence(
        self,
        memory_id: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        operation: str,
    ) -> dict[str, Any]:
        canonical_rows = [row for row in rows if row.get("txn_id") is None]
        canonical_fingerprints = {
            self._record_fingerprint(row) for row in canonical_rows
        }
        if len(canonical_fingerprints) > 1:
            return {"status": "divergent", "record": None, "version": None}
        qdrant = canonical_rows[0] if canonical_rows else None
        key = self._key(f"{operation}_canonical", memory_id)
        try:
            neo4j = self._call(
                "neo4j",
                f"{operation}_canonical",
                lambda: self.neo4j.retrieve_memory(
                    self.db_namespace, memory_id
                ),
                key,
            )
        except Exception:
            return {"status": "unknown", "record": None, "version": None}
        if qdrant is None and neo4j is None:
            return {"status": "absent", "record": None, "version": None}
        if qdrant is None or not isinstance(neo4j, Mapping):
            return {"status": "partial", "record": None, "version": None}
        try:
            recomputed_hash = self._canonical_state_hash(qdrant)
            stored_hash = str(qdrant["_canonical_state_hash"])
            version = int(qdrant["version"])
            desired = {
                "status": str(qdrant["status"]),
                "version": version,
                "derived_from": list(qdrant["derived_from"]),
                "supersedes_id": qdrant.get("supersedes_id"),
                "_canonical_state_hash": recomputed_hash,
            }
        except (KeyError, TypeError, ValueError):
            return {"status": "partial", "record": None, "version": None}
        if (
            stored_hash != recomputed_hash
            or neo4j.get("state_hash") != recomputed_hash
            or not self._neo4j_canonical_matches(neo4j, desired)
        ):
            return {"status": "partial", "record": None, "version": None}
        return {
            "status": "complete",
            "record": self._public_record(qdrant, str(qdrant["status"])),
            "version": version,
        }

    def _verified_staged_groups(
        self,
        memory_id: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        operation: str,
        excluded_txn_id: str | None,
        discover_missing: bool,
    ) -> tuple[list[dict[str, Any]], bool]:
        committed_ids: set[str] = set()
        for row in rows:
            txn_id = row.get("txn_id")
            if txn_id is None or str(txn_id) == excluded_txn_id:
                continue
            if self._decision(str(txn_id)) != "COMMITTED":
                continue
            committed_ids.add(str(txn_id))
        groups: list[dict[str, Any]] = []
        unverified = False
        if discover_missing:
            discover = getattr(self.neo4j, "retrieve_txn_ids_by_memory", None)
            if callable(discover):
                key = self._key(f"{operation}_staged_ids", memory_id)
                try:
                    discovered = self._call(
                        "neo4j",
                        f"{operation}_staged_ids",
                        lambda: self._require_readback(
                            discover(self.db_namespace, memory_id),
                            "Neo4j staged identity readback is unknown",
                        ),
                        key,
                    )
                except Exception:
                    unverified = True
                else:
                    for txn_id in discovered.get("txn_ids", []):
                        txn_id = str(txn_id)
                        if (
                            txn_id != excluded_txn_id
                            and self._decision(txn_id) == "COMMITTED"
                        ):
                            committed_ids.add(txn_id)

        for txn_id in sorted(committed_ids):
            qdrant = self._qdrant_transaction_evidence(
                txn_id, operation=f"{operation}_staged"
            )
            neo4j = self._neo4j_transaction_evidence(
                txn_id, operation=f"{operation}_staged"
            )
            if not self._transaction_evidence_matches(
                qdrant, neo4j, normalize_status=True
            ):
                unverified = True
                continue
            qdrant_rows = qdrant.get("rows", [])
            relevant_qdrant = [
                row
                for row in qdrant_rows
                if isinstance(row, Mapping)
                and str(row.get("memory_id")) == memory_id
                and row.get("record_kind") in {"memory", "status_overlay"}
            ]
            if not relevant_qdrant:
                unverified = True
                continue
            memory_rows = [
                row for row in relevant_qdrant if row.get("record_kind") == "memory"
            ]
            overlay_rows = [
                row
                for row in relevant_qdrant
                if row.get("record_kind") == "status_overlay"
            ]
            memory_row = (
                max(memory_rows, key=lambda row: int(row.get("sequence", 0)))
                if memory_rows
                else None
            )
            overlay_row = (
                max(overlay_rows, key=lambda row: int(row.get("sequence", 0)))
                if overlay_rows
                else None
            )
            groups.append(
                {
                    "txn_id": txn_id,
                    "base_version": min(
                        int(row.get("base_version", 0))
                        for row in relevant_qdrant
                    ),
                    "final_version": max(
                        int(row.get("version", 1))
                        for row in relevant_qdrant
                    ),
                    "memory_row": memory_row,
                    "overlay_row": overlay_row,
                }
            )
        return groups, unverified

    def _apply_staged_group(
        self,
        current: Mapping[str, Any] | None,
        group: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        memory_row = group.get("memory_row")
        if isinstance(memory_row, Mapping):
            physical_status = str(memory_row.get("status", "pending"))
            status = (
                str(memory_row.get("target_status", "active"))
                if physical_status == "pending"
                else physical_status
            )
            return self._public_record(memory_row, status)
        overlay = group.get("overlay_row")
        if current is None or not isinstance(overlay, Mapping):
            return None
        result = copy.deepcopy(dict(current))
        result["status"] = str(overlay.get("target_status"))
        result["version"] = int(group["final_version"])
        return result

    def _memory_evidence(
        self,
        memory_id: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        operation: str,
        excluded_txn_id: str | None = None,
    ) -> dict[str, Any]:
        canonical = self._canonical_evidence(
            memory_id, rows, operation=operation
        )
        if canonical["status"] in {"unknown", "partial", "divergent"}:
            return canonical
        groups, unverified = self._verified_staged_groups(
            memory_id,
            rows,
            operation=operation,
            excluded_txn_id=excluded_txn_id,
            discover_missing=canonical["status"] == "absent",
        )
        current = (
            copy.deepcopy(canonical["record"])
            if isinstance(canonical.get("record"), Mapping)
            else None
        )
        version = int(canonical.get("version") or 0)
        applied = False
        owner_txn_id: str | None = None
        owner_base_version: int | None = None
        remaining = list(groups)
        while True:
            candidates = [
                group
                for group in remaining
                if int(group["base_version"]) == version
                and int(group["final_version"]) > version
            ]
            if not candidates:
                break
            outcomes = []
            for group in candidates:
                outcome = self._apply_staged_group(current, group)
                if outcome is None:
                    return {
                        "status": "unknown",
                        "record": None,
                        "version": None,
                    }
                outcomes.append((group, outcome))
            fingerprints = {
                self._record_fingerprint(outcome)
                for _group, outcome in outcomes
            }
            if len(fingerprints) != 1:
                return {
                    "status": "divergent",
                    "record": None,
                    "version": None,
                }
            selected_group, selected = min(
                outcomes, key=lambda item: str(item[0]["txn_id"])
            )
            selected_version = int(selected_group["final_version"])
            if any(
                int(group["final_version"]) != selected_version
                for group, _outcome in outcomes
            ):
                return {
                    "status": "divergent",
                    "record": None,
                    "version": None,
                }
            current = selected
            version = selected_version
            applied = True
            owner_txn_id = str(selected_group["txn_id"])
            owner_base_version = int(selected_group["base_version"])
            remaining = [group for group in remaining if group not in candidates]

        if any(int(group["base_version"]) > version for group in remaining):
            return {"status": "unknown", "record": None, "version": None}
        if canonical["status"] == "absent" and unverified:
            return {"status": "unknown", "record": None, "version": None}
        if canonical["status"] == "absent" and not applied:
            return {"status": "absent", "record": None, "version": None}
        return {
            "status": "complete",
            "record": current,
            "version": version,
            "owner_txn_id": owner_txn_id,
            "owner_base_version": owner_base_version,
        }

    def _read_memory_evidence(
        self, memory_id: str, *, operation: str
    ) -> dict[str, Any]:
        rows = self._rows_for_memory(memory_id, operation=operation)
        return self._memory_evidence(
            memory_id, rows, operation=operation
        )

    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None:
        memory_id = str(memory_id)
        evidence = self._read_memory_evidence(memory_id, operation="read")
        record = evidence.get("record")
        if evidence.get("status") != "complete" or not isinstance(
            record, Mapping
        ):
            return None
        return record if str(record.get("status", "active")) == "active" else None

    def search_committed(
        self, query: str | None = None
    ) -> list[Mapping[str, Any]]:
        rows = self._all_rows(operation="search")
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
            evidence = self._memory_evidence(
                memory_id, candidates, operation="search"
            )
            record = evidence.get("record")
            if (
                evidence.get("status") != "complete"
                or not isinstance(record, Mapping)
                or str(record.get("status", "active")) != "active"
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
        """Resolve vector and graph evidence before accepting a visible record."""

        evidence = self._memory_evidence(
            str(record["memory_id"]), rows, operation="visibility"
        )
        supported = evidence.get("record")
        return bool(
            evidence.get("status") == "complete"
            and isinstance(supported, Mapping)
            and dict(supported) == dict(record)
        )

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
        expected_version: int | None = None,
    ) -> str:
        memory_id = str(desired["memory_id"])
        canonical = self._canonical_payload(desired, key)
        desired_version = int(canonical.get("version", 1))
        base_version = (
            desired_version - 1
            if expected_version is None
            else int(expected_version)
        )
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
                    base_version,
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
                raise VectorGraphCommitConflict(
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
                    raise VectorGraphCommitConflict(
                        f"canonical projection is newer than authority for {memory_id}"
                    )
                if qdrant_action == "conflict":
                    raise VectorGraphCommitConflict(
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
        memory_id = str(memory_id)
        rows = self._rows_for_memory(memory_id, operation="current_version")
        evidence = self._memory_evidence(
            memory_id,
            rows,
            operation="current_version",
            excluded_txn_id=excluded_txn_id,
        )
        if evidence.get("status") == "absent":
            return None
        if evidence.get("status") != "complete":
            raise VectorGraphBackendError(
                f"verified version is unavailable for {memory_id}"
            )
        return int(evidence["version"])

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
        memory = self.read_committed(str(memory_id)) if memory_id is not None else None
        self._event("memory_read", memory_id=memory_id, **fields)
        return copy.deepcopy(memory) if memory is not None else None

    def search(self, query: str | None = None, **fields: Any) -> list[dict[str, Any]]:
        matches = [
            copy.deepcopy(dict(row)) for row in self.search_committed(query)
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

        visible_evidence = self._read_memory_evidence(
            memory_id, operation="invalidate_committed"
        )
        visible_record = visible_evidence.get("record")
        stored = (
            visible_record
            if visible_evidence.get("status") == "complete"
            and isinstance(visible_record, Mapping)
            and str(visible_record.get("status", "active")) == "active"
            else None
        )
        if recovery_record is not None:
            record = recovery_record
        elif isinstance(stored, Mapping):
            if qdrant_before is None and neo4j_before is None:
                base_key = self._key("invalidate_committed_base", memory_id)
                self._apply_canonical_transition(
                    stored,
                    key=base_key,
                    operation="invalidate_committed_base",
                    claim_txn_id=visible_evidence.get("owner_txn_id"),
                    expected_version=visible_evidence.get(
                        "owner_base_version"
                    ),
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
            evidence = self._staged_row_evidence(
                row, qdrant_payload=True, normalize_status=True
            )
            if evidence is None:
                raise VectorGraphBackendError(
                    "expected staged claim evidence is invalid"
                )
            rows_by_memory.setdefault(str(row["memory_id"]), []).append(evidence)
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
                    "base_version": min(
                        int(row.get("base_version", 0)) for row in source_rows
                    ),
                    "final_version": max(
                        int(row.get("version", 1)) for row in source_rows
                    ),
                    "claim_hash": self._payload_hash(rows),
                }
            )
        return claims

    def _canonical_staged_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        qdrant_payload: bool,
        normalize_status: bool,
    ) -> list[dict[str, Any]] | None:
        canonical: list[dict[str, Any]] = []
        for row in rows:
            evidence = self._staged_row_evidence(
                row,
                qdrant_payload=qdrant_payload,
                normalize_status=normalize_status,
            )
            if evidence is None:
                return None
            canonical.append(evidence)
        canonical.sort(
            key=lambda row: (
                int(row["sequence"]),
                str(row["record_kind"]),
                str(row["memory_id"]),
            )
        )
        return canonical

    @staticmethod
    def _canonical_staged_edge(
        edge: Mapping[str, Any], target_statuses: Mapping[str, str]
    ) -> dict[str, Any] | None:
        required = {"txn_id", "kind", "source_id", "target_id", "status"}
        if not required.issubset(edge):
            return None
        kind = str(edge["kind"])
        if kind not in {"DERIVED_FROM", "SUPERSEDES"}:
            return None
        owner_id = str(
            edge["target_id"] if kind == "DERIVED_FROM" else edge["source_id"]
        )
        target_status = str(target_statuses.get(owner_id, "active"))
        if str(edge["status"]) not in {"pending", target_status}:
            return None
        return {
            "txn_id": str(edge["txn_id"]),
            "kind": kind,
            "source_id": str(edge["source_id"]),
            "target_id": str(edge["target_id"]),
            "status": "durable",
        }

    def _canonical_staged_edges(
        self,
        edges: Iterable[Mapping[str, Any]],
        target_statuses: Mapping[str, str],
    ) -> list[dict[str, Any]] | None:
        canonical: list[dict[str, Any]] = []
        for edge in edges:
            evidence = self._canonical_staged_edge(edge, target_statuses)
            if evidence is None:
                return None
            canonical.append(evidence)
        canonical.sort(
            key=lambda edge: (
                edge["kind"],
                edge["source_id"],
                edge["target_id"],
                edge["txn_id"],
            )
        )
        return canonical

    @staticmethod
    def _staged_edges_from_rows(
        rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        edges: list[dict[str, Any]] = []
        for row in rows:
            if row.get("record_kind") != "memory":
                continue
            txn_id = str(row["txn_id"])
            memory_id = str(row["memory_id"])
            status = str(row["status"])
            for source_id in sorted(
                {str(item) for item in row.get("derived_from", [])}
            ):
                edges.append(
                    {
                        "txn_id": txn_id,
                        "kind": "DERIVED_FROM",
                        "source_id": source_id,
                        "target_id": memory_id,
                        "status": status,
                    }
                )
            if row.get("supersedes_id") is not None:
                edges.append(
                    {
                        "txn_id": txn_id,
                        "kind": "SUPERSEDES",
                        "source_id": memory_id,
                        "target_id": str(row["supersedes_id"]),
                        "status": status,
                    }
                )
        return edges

    def _qdrant_matches(
        self,
        state: Mapping[str, Any],
        expected: Mapping[str, Any],
        *,
        normalize_status: bool = False,
    ) -> bool:
        if not state.get("read_ok", False):
            return False
        actual_rows = self._canonical_staged_rows(
            state.get("rows", []),
            qdrant_payload=True,
            normalize_status=normalize_status,
        )
        expected_rows = self._canonical_staged_rows(
            expected["rows"],
            qdrant_payload=True,
            normalize_status=normalize_status,
        )
        return actual_rows is not None and actual_rows == expected_rows

    def _neo4j_matches(
        self,
        state: Mapping[str, Any],
        expected: Mapping[str, Any],
        *,
        normalize_status: bool = False,
    ) -> bool:
        if not state.get("read_ok", False):
            return False
        actual_nodes = self._canonical_staged_rows(
            state.get("nodes", []),
            qdrant_payload=False,
            normalize_status=normalize_status,
        )
        expected_nodes = self._canonical_staged_rows(
            expected["rows"],
            qdrant_payload=True,
            normalize_status=normalize_status,
        )
        if actual_nodes is None or expected_nodes is None:
            return False
        target_statuses = {
            str(row["memory_id"]): str(row["target_status"])
            for row in expected["rows"]
            if row.get("record_kind") == "memory"
        }
        actual_edges = self._canonical_staged_edges(
            state.get("edges", []), target_statuses
        )
        expected_edges = self._canonical_staged_edges(
            expected["edges"], target_statuses
        )
        return actual_nodes == expected_nodes and actual_edges == expected_edges

    def _transaction_evidence_matches(
        self,
        qdrant: Mapping[str, Any],
        neo4j: Mapping[str, Any],
        *,
        normalize_status: bool,
    ) -> bool:
        if not qdrant.get("read_ok", False) or not neo4j.get("read_ok", False):
            return False
        expected = {
            "rows": qdrant.get("rows", []),
            "edges": self._staged_edges_from_rows(qdrant.get("rows", [])),
        }
        return self._qdrant_matches(
            qdrant, expected, normalize_status=normalize_status
        ) and self._neo4j_matches(
            neo4j, expected, normalize_status=normalize_status
        )

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
                raise VectorGraphCommitConflict(
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
            self._qdrant_transaction_evidence(txn_id), expected
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
            self._neo4j_transaction_evidence(txn_id), expected
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

    def _verify_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        *,
        normalize_status: bool,
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        try:
            expected = self._expected_transaction_state(txn_id, intents)
        except Exception:
            return {"status": "unknown", "txn_id": txn_id}
        qdrant = self._qdrant_transaction_evidence(txn_id)
        neo4j = self._neo4j_transaction_evidence(txn_id)
        if not qdrant.get("read_ok", False) or not neo4j.get("read_ok", False):
            status = "unknown"
        else:
            qdrant_empty = not qdrant.get("rows", [])
            neo4j_empty = not neo4j.get("nodes", []) and not neo4j.get("edges", [])
            if self._qdrant_matches(
                qdrant, expected, normalize_status=normalize_status
            ) and self._neo4j_matches(
                neo4j, expected, normalize_status=normalize_status
            ):
                status = "complete"
            elif qdrant_empty and neo4j_empty:
                status = "absent"
            else:
                status = "partial"
        return {"status": status, "txn_id": txn_id}

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        return self._verify_transaction(
            txn_id,
            intents,
            normalize_status=self._decision(txn_id) == "COMMITTED",
        )

    def finalize_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        txn_id = str(txn_id)
        verification = self._verify_transaction(
            txn_id, intents, normalize_status=True
        )
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
                    expected_version=int(finalized.get("base_version", 0)),
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
                expected_version=int(overlay.get("base_version", 0)),
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
            read_key = self._key("verify_persistent_state", memory_id)
            try:
                qdrant_state = self._call(
                    "qdrant",
                    "verify_persistent_state",
                    lambda: self.qdrant.retrieve(
                        self.db_namespace, memory_id
                    ),
                    read_key,
                )
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
                neo4j_state = self._call(
                    "neo4j",
                    "verify_persistent_state",
                    lambda: self.neo4j.retrieve_memory(
                        self.db_namespace, memory_id
                    ),
                    read_key,
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
