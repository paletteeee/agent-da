"""Contract tests for the Qdrant/Neo4j memory backend seam."""

from __future__ import annotations

import sys
import unittest
import copy
import hashlib
import json
import threading
import tempfile
import time
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_vector_graph_backend import (
    VectorGraphBackendError,
    VectorGraphMemoryBackend,
    _Neo4jBoltClient,
    _QdrantHTTPClient,
    _qdrant_point_id,
)
from txnmem_backend import InstrumentedMemoryBackend
from txnmem_provenance_performance import (
    ProvenancePerformanceError,
    _preload_graph,
    build_layered_dag,
    canonical_graph_sha256,
    run_matrix_cell,
)
from txnmem_task_transaction import TaskTransactionError, TaskTransactionGateway
from txnmem_transaction_journal import TransactionJournal


class _FakeQdrant:
    def __init__(self):
        self.points = {}
        self.upsert_count = 0
        self.fail_upsert = False
        self.fail_upsert_after = False
        self.fail_readback = False
        self.fail_cleanup = False
        self.fail_cleanup_after = False
        self.transaction_keys = []
        self.cleanup_count = 0

    def upsert(self, namespace, point_id, vector, payload, idempotency_key):
        self.upsert_count += 1
        if self.fail_upsert:
            raise RuntimeError("qdrant unavailable")
        self.points[(namespace, point_id)] = {"vector": vector, "payload": dict(payload)}
        if payload.get("txn_id"):
            self.transaction_keys.append(idempotency_key)
        if self.fail_upsert_after:
            raise ConnectionResetError("qdrant response lost")

    def retrieve(self, namespace, point_id):
        row = self.points.get((namespace, point_id))
        return None if row is None else dict(row["payload"])

    def search(self, namespace, vector, limit):
        return [dict(row["payload"]) for (space, _), row in self.points.items() if space == namespace][:limit]

    def retrieve_many_by_memory(self, namespace, memory_id):
        if self.fail_readback:
            raise TimeoutError("qdrant readback unavailable")
        return {
            "read_ok": True,
            "rows": [
                dict(row["payload"])
                for (space, _), row in self.points.items()
                if space == namespace and row["payload"].get("memory_id") == memory_id
            ],
        }

    def scan_namespace(self, namespace, limit=1000):
        if self.fail_readback:
            raise TimeoutError("qdrant scan unavailable")
        return {
            "read_ok": True,
            "rows": [
                dict(row["payload"])
                for (space, _), row in self.points.items()
                if space == namespace
            ][:limit],
        }

    def retrieve_many_by_txn(self, namespace, txn_id):
        if self.fail_readback:
            raise TimeoutError("qdrant transaction readback unavailable")
        return {
            "read_ok": True,
            "rows": [
                dict(row["payload"])
                for (space, _), row in self.points.items()
                if space == namespace and row["payload"].get("txn_id") == txn_id
            ],
        }

    def delete_many_by_txn(self, namespace, txn_id, idempotency_key):
        self.cleanup_count += 1
        if self.fail_cleanup:
            raise RuntimeError("qdrant cleanup unavailable")
        doomed = [
            key
            for key, row in self.points.items()
            if key[0] == namespace and row["payload"].get("txn_id") == txn_id
        ]
        for key in doomed:
            self.points.pop(key, None)
        if self.fail_cleanup_after:
            raise ConnectionResetError("qdrant cleanup response lost")

    def delete(self, namespace, point_id, idempotency_key):
        self.points.pop((namespace, point_id), None)

    def healthcheck(self):
        return {"available": True, "version": "1.15.4"}


class _FakeNeo4j:
    def __init__(self):
        self.max_transaction_retry_time_seconds = 0.0
        self.memories = {}
        self.edges = []
        self.staged_memories = {}
        self.staged_edges = []
        self.fail_upsert = False
        self.fail_upsert_after = False
        self.fail_readback = False
        self.fail_cleanup = False
        self.fail_cleanup_after = False
        self.transaction_keys = []
        self.cleanup_count = 0
        self.cas_barrier = None
        self.cas_started_event = None
        self.claim_barrier = None
        self.enforce_claims = True
        self.claims = {}
        self.claim_requests = []
        self.cas_requests = []
        self._cas_lock = threading.Lock()

    def claim_transaction_writes(
        self, namespace, txn_id, claims, idempotency_key
    ):
        self.claim_requests.append(copy.deepcopy(list(claims)))
        if self.claim_barrier is not None:
            self.claim_barrier.wait(timeout=5)
        with self._cas_lock:
            conflicts = []
            for claim in claims:
                if self.enforce_claims:
                    memory_id = str(claim["memory_id"])
                    base_version = int(claim["base_version"])
                    final_version = int(claim["final_version"])
                    current = self.retrieve_memory(
                        namespace, memory_id
                    )
                    canonical_version = (
                        int(current.get("version", 1))
                        if current is not None
                        else 0
                    )
                    existing = [
                        (key, value)
                        for key, value in self.claims.items()
                        if key[0] == namespace and key[1] == memory_id
                    ]
                    cursor = canonical_version
                    while cursor < base_version:
                        continuations = [
                            key
                            for key, _value in existing
                            if key[2] == cursor and key[3] <= base_version
                        ]
                        if len(continuations) != 1:
                            break
                        cursor = continuations[0][3]
                    overlapping = [
                        (key, value)
                        for key, value in existing
                        if key[2] < final_version and base_version < key[3]
                    ]
                    same = [
                        (key, value)
                        for key, value in overlapping
                        if key[2:] == (base_version, final_version)
                        and value["txn_id"] == txn_id
                        and value["claim_hash"] == claim["claim_hash"]
                    ]
                    if (
                        canonical_version > base_version
                        or cursor != base_version
                        or (overlapping and len(same) != len(overlapping))
                    ):
                        conflicts.append(memory_id)
            if conflicts:
                return {"status": "conflict", "memory_ids": sorted(conflicts)}
            if self.enforce_claims:
                for claim in claims:
                    self.claims[
                        (
                            namespace,
                            str(claim["memory_id"]),
                            int(claim["base_version"]),
                            int(claim["final_version"]),
                        )
                    ] = {
                        "txn_id": txn_id,
                        "claim_hash": str(claim["claim_hash"]),
                    }
            return {"status": "claimed", "memory_ids": []}

    def release_transaction_claims(
        self, namespace, txn_id, idempotency_key
    ):
        with self._cas_lock:
            self.claims = {
                key: claim
                for key, claim in self.claims.items()
                if not (
                    key[0] == namespace and claim["txn_id"] == txn_id
                )
            }
        return {"status": "released"}

    def upsert_memory(self, namespace, memory_id, payload, source_ids, supersedes_id, idempotency_key):
        if self.fail_upsert:
            raise RuntimeError("neo4j unavailable")
        if payload.get("txn_id"):
            key = (
                namespace,
                memory_id,
                payload["txn_id"],
                int(payload["sequence"]),
                payload["record_kind"],
            )
            self.staged_memories[key] = dict(payload)
            self.staged_edges = [
                edge
                for edge in self.staged_edges
                if not (
                    edge["txn_id"] == payload["txn_id"]
                    and payload["record_kind"] == "memory"
                    and (
                        (
                            edge["kind"] == "DERIVED_FROM"
                            and edge["target_id"] == memory_id
                        )
                        or (
                            edge["kind"] == "SUPERSEDES"
                            and edge["source_id"] == memory_id
                        )
                    )
                )
            ]
            for source_id in dict.fromkeys(source_ids):
                self.staged_edges.append(
                    {
                        "txn_id": payload["txn_id"],
                        "kind": "DERIVED_FROM",
                        "source_id": source_id,
                        "target_id": memory_id,
                        "status": payload["status"],
                    }
                )
            if supersedes_id:
                self.staged_edges.append(
                    {
                        "txn_id": payload["txn_id"],
                        "kind": "SUPERSEDES",
                        "source_id": memory_id,
                        "target_id": supersedes_id,
                        "status": payload["status"],
                    }
                )
            self.transaction_keys.append(idempotency_key)
            if self.fail_upsert_after:
                raise ConnectionResetError("neo4j response lost")
            return
        self.memories[(namespace, memory_id)] = dict(payload)
        for source_id in source_ids:
            self.edges.append((namespace, source_id, memory_id, "DERIVED_FROM"))
        if supersedes_id:
            self.edges.append((namespace, memory_id, supersedes_id, "SUPERSEDES"))
        if self.fail_upsert_after:
            raise ConnectionResetError("neo4j response lost")

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
        self.cas_requests.append(
            {
                "memory_id": memory_id,
                "expected_version": int(expected_version),
                "desired_version": int(payload.get("version", 1)),
                "claim_txn_id": claim_txn_id,
            }
        )
        if self.cas_barrier is not None:
            self.cas_barrier.wait(timeout=5)
        if self.cas_started_event is not None:
            self.cas_started_event.set()
        with self._cas_lock:
            if self.fail_upsert:
                raise RuntimeError("neo4j unavailable")
            desired_version = int(payload.get("version", 1))
            claim_key = (
                namespace,
                memory_id,
                int(expected_version),
                desired_version,
            )
            claim = self.claims.get(claim_key)
            claim_owner = claim["txn_id"] if claim is not None else None
            overlapping_owners = {
                value["txn_id"]
                for key, value in self.claims.items()
                if key[0] == namespace
                and key[1] == memory_id
                and key[2] < desired_version
                and int(expected_version) < key[3]
            }
            if any(owner != claim_txn_id for owner in overlapping_owners):
                return {"status": "conflict", "record": None}
            current = self.retrieve_memory(namespace, memory_id)
            current_version = (
                int(current.get("version", 1)) if current is not None else 0
            )
            desired_hash = payload.get("_canonical_state_hash")
            current_payload = self.memories.get((namespace, memory_id))
            if current_version > desired_version:
                return {"status": "newer", "record": current}
            if current_version == desired_version:
                exact = bool(
                    current is not None
                    and current.get("status") == payload.get("status", "active")
                    and current.get("source_ids") == sorted(set(source_ids))
                    and current.get("supersedes_id") == supersedes_id
                    and current_payload is not None
                    and current_payload.get("_canonical_state_hash") == desired_hash
                )
                if exact and claim_owner == claim_txn_id:
                    self.claims.pop(claim_key, None)
                return {
                    "status": "matched" if exact else "conflict",
                    "record": current,
                }
            if current_version != int(expected_version):
                return {"status": "conflict", "record": current}
            stored = dict(payload)
            stored["_canonical_operation_id"] = idempotency_key
            self.memories[(namespace, memory_id)] = stored
            if claim_owner == claim_txn_id:
                self.claims.pop(claim_key, None)
            self.edges = [
                edge
                for edge in self.edges
                if not (
                    edge[0] == namespace
                    and (
                        (edge[3] == "DERIVED_FROM" and edge[2] == memory_id)
                        or (edge[3] == "SUPERSEDES" and edge[1] == memory_id)
                    )
                )
            ]
            for source_id in sorted(set(source_ids)):
                self.edges.append(
                    (namespace, source_id, memory_id, "DERIVED_FROM")
                )
            if supersedes_id:
                self.edges.append(
                    (namespace, memory_id, supersedes_id, "SUPERSEDES")
                )
            if self.fail_upsert_after:
                raise ConnectionResetError("neo4j response lost")
            return {
                "status": "applied",
                "record": self.retrieve_memory(namespace, memory_id),
            }

    def project_if_current(
        self, namespace, memory_id, operation_id, projector
    ):
        with self._cas_lock:
            current = self.memories.get((namespace, memory_id))
            if current is None or current.get("_canonical_operation_id") != operation_id:
                return {"status": "newer"}
            projector()
            return {"status": "projected"}

    def update_status(self, namespace, memory_id, status, idempotency_key, version=None):
        if (namespace, memory_id) in self.memories:
            self.memories[(namespace, memory_id)]["status"] = status
            if version is not None:
                self.memories[(namespace, memory_id)]["version"] = version

    def delete_memory(self, namespace, memory_id, idempotency_key):
        self.memories.pop((namespace, memory_id), None)

    def retrieve_memory(self, namespace, memory_id):
        row = self.memories.get((namespace, memory_id))
        if row is None:
            return None
        return {
            "status": row.get("status"),
            "version": row.get("version", 1),
            "source_ids": sorted(
                source_id
                for edge_namespace, source_id, target_id, kind in self.edges
                if edge_namespace == namespace
                and target_id == memory_id
                and kind == "DERIVED_FROM"
            ),
            "supersedes_id": next(
                (
                    target_id
                    for edge_namespace, source_id, target_id, kind in self.edges
                    if edge_namespace == namespace
                    and source_id == memory_id
                    and kind == "SUPERSEDES"
                ),
                None,
            ),
            "state_hash": row.get("_canonical_state_hash"),
            "operation_id": row.get("_canonical_operation_id"),
        }

    def scan_namespace(self, namespace, limit=1000):
        if self.fail_readback:
            raise TimeoutError("neo4j scan unavailable")
        rows = []
        for (space, memory_id), row in sorted(self.memories.items()):
            if space != namespace:
                continue
            rows.append(
                {
                    "memory_id": memory_id,
                    "status": row.get("status"),
                    "source_ids": sorted(
                        source_id
                        for edge_namespace, source_id, target_id, kind in self.edges
                        if edge_namespace == namespace
                        and target_id == memory_id
                        and kind == "DERIVED_FROM"
                    ),
                }
            )
        return {"read_ok": True, "rows": rows[:limit]}

    def retrieve_many_by_txn(self, namespace, txn_id):
        if self.fail_readback:
            raise TimeoutError("neo4j transaction readback unavailable")
        return {
            "read_ok": True,
            "nodes": [
                dict(row)
                for key, row in self.staged_memories.items()
                if key[0] == namespace and row.get("txn_id") == txn_id
            ],
            "edges": [
                dict(edge)
                for edge in self.staged_edges
                if edge["txn_id"] == txn_id
            ],
        }

    def retrieve_txn_ids_by_memory(self, namespace, memory_id):
        if self.fail_readback:
            raise TimeoutError("neo4j staged identity readback unavailable")
        return {
            "read_ok": True,
            "txn_ids": sorted(
                {
                    str(row["txn_id"])
                    for key, row in self.staged_memories.items()
                    if key[0] == namespace
                    and str(row.get("memory_id")) == str(memory_id)
                }
            ),
        }

    def delete_many_by_txn(self, namespace, txn_id, idempotency_key):
        self.cleanup_count += 1
        if self.fail_cleanup:
            raise RuntimeError("neo4j cleanup unavailable")
        self.staged_memories = {
            key: row
            for key, row in self.staged_memories.items()
            if not (key[0] == namespace and row.get("txn_id") == txn_id)
        }
        self.staged_edges = [
            edge for edge in self.staged_edges if edge["txn_id"] != txn_id
        ]
        if self.fail_cleanup_after:
            raise ConnectionResetError("neo4j cleanup response lost")

    def healthcheck(self):
        return {"available": True, "version": "5.26.0"}


class _MigrationRows:
    def __init__(self, rows=()):
        self.rows = [copy.deepcopy(row) for row in rows]

    def __iter__(self):
        return iter(self.rows)

    def single(self):
        return copy.deepcopy(self.rows[0]) if self.rows else None

    def consume(self):
        return None


class _MigrationTransaction:
    def __init__(self, nodes, relationships):
        self.nodes = nodes
        self.relationships = relationships

    def run(self, query, **parameters):
        if "RETURN elementId(candidate) AS element_id" in query:
            rows = []
            for element_id, node in sorted(self.nodes.items()):
                labels = set(node["labels"])
                if not labels.intersection(
                    {"MemoryIdentity", "Memory", "MemoryReference"}
                ):
                    continue
                properties = node["properties"]
                if properties.get("namespace") is None or properties.get("memory_id") is None:
                    continue
                properties["_migration_lock"] = int(
                    properties.get("_migration_lock", 0)
                ) + 1
                rows.append(
                    {
                        "element_id": element_id,
                        "labels": sorted(labels),
                        "properties": copy.deepcopy(properties),
                    }
                )
            return _MigrationRows(rows)
        if "RETURN elementId(relationship) AS relationship_id" in query:
            participant_ids = set(
                parameters.get("participant_ids", parameters.get("loser_ids", []))
            )
            rows = []
            for relationship_id, relationship in sorted(self.relationships.items()):
                if relationship["type"] not in {"DERIVED_FROM", "SUPERSEDES"}:
                    continue
                if (
                    relationship["source_id"] not in participant_ids
                    and relationship["target_id"] not in participant_ids
                ):
                    continue
                rows.append(
                    {
                        "relationship_id": relationship_id,
                        "relationship_type": relationship["type"],
                        "source_id": relationship["source_id"],
                        "target_id": relationship["target_id"],
                        "properties": copy.deepcopy(relationship.get("properties", {})),
                    }
                )
            return _MigrationRows(rows)
        if "WHERE elementId(relationship) IN $relationship_ids" in query:
            for relationship_id in parameters["relationship_ids"]:
                self.relationships.pop(relationship_id, None)
            return _MigrationRows()
        if "UNWIND $relationships AS edge" in query:
            relationship_type = (
                "DERIVED_FROM" if "[replacement:DERIVED_FROM]" in query else "SUPERSEDES"
            )
            next_index = len(self.relationships)
            for edge in parameters["relationships"]:
                relationship_id = f"migrated-{next_index:04d}"
                next_index += 1
                self.relationships[relationship_id] = {
                    "type": relationship_type,
                    "source_id": edge["source_id"],
                    "target_id": edge["target_id"],
                    "properties": copy.deepcopy(edge["properties"]),
                }
            return _MigrationRows()
        if "SET winner:MemoryIdentity" in query:
            winner = self.nodes[parameters["winner_id"]]
            winner["labels"].update(parameters.get("merged_labels", []))
            winner["properties"].update(
                copy.deepcopy(parameters.get("merged_properties", {}))
            )
            if parameters["canonical"]:
                winner["properties"]["canonical"] = True
            winner["properties"].pop("_migration_lock", None)
            return _MigrationRows()
        if "DETACH DELETE loser" in query:
            for loser in parameters["losers"]:
                node = self.nodes.get(loser["element_id"])
                if node is None:
                    continue
                properties = node["properties"]
                if (
                    properties.get("namespace") != loser["namespace"]
                    or properties.get("memory_id") != loser["memory_id"]
                ):
                    raise AssertionError("migration attempted to delete an unverified node")
                self.nodes.pop(loser["element_id"])
            return _MigrationRows()
        if "MATCH (claim:MemoryWriteClaim)" in query:
            return _MigrationRows()
        raise AssertionError(f"unexpected migration transaction query: {query}")


class _MigrationDriver:
    def __init__(self, nodes, relationships=()):
        self.nodes = copy.deepcopy(nodes)
        self.relationships = {
            relationship["relationship_id"]: copy.deepcopy(relationship)
            for relationship in relationships
        }
        self.constraints = set()
        self.events = []
        self.rollback_count = 0

    class _Session:
        def __init__(self, driver):
            self.driver = driver

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute_write(self, function):
            self.driver.events.append("migration_begin")
            working_nodes = copy.deepcopy(self.driver.nodes)
            working_relationships = copy.deepcopy(self.driver.relationships)
            transaction = _MigrationTransaction(working_nodes, working_relationships)
            try:
                result = function(transaction)
            except Exception:
                self.driver.rollback_count += 1
                self.driver.events.append("migration_rollback")
                raise
            self.driver.nodes = working_nodes
            self.driver.relationships = working_relationships
            self.driver.events.append("migration_commit")
            return result

        class _ExplicitTransaction(_MigrationTransaction):
            def __init__(self, driver):
                self.driver = driver
                self.working_nodes = copy.deepcopy(driver.nodes)
                self.working_relationships = copy.deepcopy(driver.relationships)
                super().__init__(self.working_nodes, self.working_relationships)
                self.finished = False
                self.driver.events.append("migration_begin")

            def commit(self):
                self.driver.nodes = self.working_nodes
                self.driver.relationships = self.working_relationships
                self.driver.events.append("migration_commit")
                self.finished = True

            def rollback(self):
                self.driver.rollback_count += 1
                self.driver.events.append("migration_rollback")
                self.finished = True

            def close(self):
                return None

        def begin_transaction(self):
            return self._ExplicitTransaction(self.driver)

        def run(self, query, **_parameters):
            if query.startswith(
                "MATCH (m:MemoryIdentity:Memory {namespace:$namespace, memory_id:$memory_id})"
            ):
                namespace = _parameters["namespace"]
                memory_id = _parameters["memory_id"]
                for element_id, node in self.driver.nodes.items():
                    properties = node["properties"]
                    if (
                        {"MemoryIdentity", "Memory"}.issubset(node["labels"])
                        and properties.get("namespace") == namespace
                        and properties.get("memory_id") == memory_id
                        and properties.get("canonical", True)
                    ):
                        source_ids = sorted(
                            self.driver.nodes[edge["target_id"]]["properties"]["memory_id"]
                            for edge in self.driver.relationships.values()
                            if edge["type"] == "DERIVED_FROM"
                            and edge["source_id"] == element_id
                        )
                        supersedes_ids = sorted(
                            self.driver.nodes[edge["target_id"]]["properties"]["memory_id"]
                            for edge in self.driver.relationships.values()
                            if edge["type"] == "SUPERSEDES"
                            and edge["source_id"] == element_id
                        )
                        return _MigrationRows(
                            [
                                {
                                    "status": properties.get("status"),
                                    "version": properties.get("version", 1),
                                    "source_ids": source_ids,
                                    "supersedes_id": supersedes_ids[0]
                                    if supersedes_ids
                                    else None,
                                    "state_hash": properties.get(
                                        "canonical_state_hash"
                                    ),
                                    "operation_id": properties.get(
                                        "canonical_operation_id"
                                    ),
                                }
                            ]
                        )
                return _MigrationRows()
            if query.startswith("DROP CONSTRAINT memory_write_claim_unique"):
                self.driver.constraints.discard("memory_write_claim_unique")
                self.driver.events.append("claim_constraint_drop")
                return _MigrationRows()
            if query.startswith("CREATE CONSTRAINT memory_identity_unique"):
                identities = [
                    (
                        node["properties"]["namespace"],
                        node["properties"]["memory_id"],
                    )
                    for node in self.driver.nodes.values()
                    if "MemoryIdentity" in node["labels"]
                ]
                if len(identities) != len(set(identities)):
                    raise RuntimeError("identity constraint created before deduplication")
                self.driver.constraints.add("memory_identity_unique")
                self.driver.events.append("identity_constraint")
                return _MigrationRows()
            if query.startswith("CREATE CONSTRAINT memory_write_claim_unique"):
                self.driver.constraints.add("memory_write_claim_unique")
                self.driver.events.append("claim_constraint")
                return _MigrationRows()
            raise AssertionError(f"query ran outside migration transaction: {query}")

    def session(self):
        return self._Session(self)


class VectorGraphMemoryBackendTests(unittest.TestCase):
    @staticmethod
    def _intent(txn_id, sequence, tool_name, **arguments):
        return {
            "txn_id": txn_id,
            "sequence": sequence,
            "tool_name": tool_name,
            "arguments": arguments,
        }

    def _assert_staged_fail_closed(self, backend, memory_id):
        self.assertIsNone(backend.read_committed(memory_id))
        self.assertNotIn(
            memory_id,
            [record["memory_id"] for record in backend.search_committed()],
        )
        with self.assertRaises(VectorGraphBackendError) as raised:
            backend.current_version(memory_id)
        self.assertEqual(raised.exception.code, "backend_state_unknown")

    def test_neo4j_driver_and_writes_disable_implicit_retry(self):
        observed = {}

        class Driver:
            pass

        class GraphDatabase:
            @staticmethod
            def driver(uri, **kwargs):
                observed.update({"uri": uri, **kwargs})
                return Driver()

        module = ModuleType("neo4j")
        module.GraphDatabase = GraphDatabase
        with patch.dict(sys.modules, {"neo4j": module}), patch.object(
            _Neo4jBoltClient, "_initialize_schema"
        ):
            client = _Neo4jBoltClient("bolt://proxy", ("neo4j", "secret"))

        self.assertEqual(observed["max_transaction_retry_time"], 0.0)
        self.assertEqual(client.max_transaction_retry_time_seconds, 0.0)

        events = []

        class Transaction:
            def commit(self):
                events.append("commit")

            def rollback(self):
                events.append("rollback")

            def close(self):
                events.append("close")

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def begin_transaction(self):
                events.append("begin")
                return Transaction()

            def execute_write(self, _work):
                raise AssertionError("managed transaction retry path was used")

        class ExplicitDriver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = ExplicitDriver()
        self.assertEqual(client._execute_write_once(lambda _tx: "ok"), "ok")
        self.assertEqual(events, ["begin", "commit", "close"])

        events.clear()
        with self.assertRaisesRegex(RuntimeError, "transient"):
            client._execute_write_once(
                lambda _tx: (_ for _ in ()).throw(RuntimeError("transient"))
            )
        self.assertEqual(events, ["begin", "rollback", "close"])

    def test_neo4j_healthcheck_reports_server_version(self):
        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query):
                self.query = query
                return self

            def single(self):
                return {"ok": 1, "version": "5.22.0"}

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()

        self.assertEqual(
            client.healthcheck(),
            {"available": True, "version": "5.22.0"},
        )

    def test_neo4j_transaction_node_and_edge_readback_are_namespace_scoped(self):
        calls = []

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query, **parameters):
                calls.append((query, parameters))
                return []

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()

        self.assertEqual(
            client.retrieve_many_by_txn("tenant-a", "txn-shared"),
            {"read_ok": True, "nodes": [], "edges": []},
        )
        self.assertEqual(
            [parameters for _query, parameters in calls],
            [
                {"namespace": "tenant-a", "txn_id": "txn-shared"},
                {"namespace": "tenant-a", "txn_id": "txn-shared"},
            ],
        )

    def test_neo4j_canonical_rewrite_replaces_only_outgoing_memory_edges(self):
        edges = {
            ("DERIVED_FROM", "other", "m"),
            ("AUDITS", "m", "audit"),
        }

        class Result:
            def consume(self):
                return None

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query, **parameters):
                if (
                    "OPTIONAL MATCH (m)-[r:DERIVED_FROM|SUPERSEDES]->()" in query
                    and "TxnMemory" not in query
                ):
                    edges.difference_update(
                        edge
                        for edge in set(edges)
                        if edge[1] == parameters["memory_id"]
                        and edge[0] in {"DERIVED_FROM", "SUPERSEDES"}
                    )
                elif (
                    "MERGE (m)-[:DERIVED_FROM]->(s)" in query
                    or "MERGE (m)-[r:DERIVED_FROM]->(s)" in query
                ):
                    edges.add(
                        (
                            "DERIVED_FROM",
                            parameters["memory_id"],
                            parameters["source_id"],
                        )
                    )
                return Result()

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()
        payload = {"status": "active", "version": 1}

        client.upsert_memory("tenant", "m", payload, ["s1"], None, "first")
        client.upsert_memory("tenant", "m", payload, ["s2"], None, "second")

        self.assertEqual(
            edges,
            {
                ("DERIVED_FROM", "other", "m"),
                ("AUDITS", "m", "audit"),
                ("DERIVED_FROM", "m", "s2"),
            },
        )

    def test_neo4j_reference_identity_survives_later_source_canonicalization(self):
        identities = set()
        canonical = set()
        edges = set()

        class Result:
            def consume(self):
                return None

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query, **parameters):
                memory_id = parameters.get("memory_id")
                if "MERGE (m:MemoryReference" in query:
                    identities.add(parameters.get("source_id") or parameters.get("old_id"))
                if "MERGE (m:MemoryIdentity" in query:
                    identities.add(memory_id)
                if "MERGE (m:Memory {" in query:
                    identities.add(memory_id)
                    canonical.add(memory_id)
                if "SET m:Memory" in query:
                    canonical.add(memory_id)
                if "MERGE (s:MemoryIdentity" in query:
                    identities.add(parameters["source_id"])
                if "MERGE (m)-[:DERIVED_FROM]->(s)" in query:
                    if parameters["source_id"] in canonical:
                        edges.add((memory_id, parameters["source_id"]))
                if "MERGE (m)-[r:DERIVED_FROM" in query and "TxnMemory" not in query:
                    edges.add((memory_id, parameters["source_id"]))
                return Result()

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()
        payload = {"status": "active", "version": 1}

        client.upsert_memory("tenant", "target", payload, ["source"], None, "target")
        client.upsert_memory("tenant", "source", payload, [], None, "source")

        self.assertIn("source", identities)
        self.assertEqual(edges, {("target", "source")})

    def test_neo4j_legacy_migration_deduplicates_all_label_shapes_and_rewires_both_directions(self):
        def node(labels, memory_id, **properties):
            return {
                "labels": set(labels),
                "properties": {
                    "namespace": "tenant",
                    "memory_id": memory_id,
                    **properties,
                },
            }

        driver = _MigrationDriver(
            {
                "shared-identity": node(
                    {"MemoryIdentity"}, "shared", canonical=False
                ),
                "shared-canonical": node(
                    {"Memory", "MemoryReference"},
                    "shared",
                    canonical=True,
                    version=4,
                    status="active",
                    value="canonical-value",
                    custom_property="preserved",
                ),
                "hybrid": node(
                    {"Memory", "MemoryReference"},
                    "hybrid",
                    canonical=True,
                    version=2,
                    value="hybrid-value",
                ),
                "hybrid-reference": node(
                    {"Memory", "MemoryReference"},
                    "hybrid",
                    canonical=True,
                    version=2,
                    value="hybrid-value",
                ),
                "plain-a": node({"Memory"}, "plain", version=1, value="old"),
                "plain-b": node({"Memory"}, "plain", version=2, value="new"),
                "plain-c": node({"Memory"}, "plain", version=2, value="new"),
                "reference-a": node({"MemoryReference"}, "reference"),
                "reference-b": node({"MemoryReference"}, "reference"),
                "external": node({"Memory"}, "external", version=1),
            },
            [
                {
                    "relationship_id": "r1",
                    "type": "DERIVED_FROM",
                    "source_id": "external",
                    "target_id": "shared-identity",
                    "properties": {"direction": "incoming"},
                },
                {
                    "relationship_id": "r2",
                    "type": "SUPERSEDES",
                    "source_id": "shared-identity",
                    "target_id": "reference-b",
                    "properties": {"direction": "outgoing"},
                },
                {
                    "relationship_id": "r3",
                    "type": "DERIVED_FROM",
                    "source_id": "plain-a",
                    "target_id": "external",
                    "properties": {"plain": "source"},
                },
                {
                    "relationship_id": "r4",
                    "type": "SUPERSEDES",
                    "source_id": "external",
                    "target_id": "plain-c",
                    "properties": {"plain": "target"},
                },
                {
                    "relationship_id": "r5",
                    "type": "DERIVED_FROM",
                    "source_id": "hybrid-reference",
                    "target_id": "shared-identity",
                    "properties": {"hybrid": True},
                },
            ],
        )
        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = driver

        client._initialize_schema()

        self.assertEqual(
            set(driver.nodes),
            {"shared-canonical", "hybrid", "plain-b", "reference-a", "external"},
        )
        self.assertTrue(
            all("MemoryIdentity" in node["labels"] for node in driver.nodes.values())
        )
        self.assertEqual(
            driver.nodes["shared-canonical"]["properties"]["custom_property"],
            "preserved",
        )
        self.assertIn("MemoryReference", driver.nodes["shared-canonical"]["labels"])
        migrated_edges = {
            (
                edge["type"],
                edge["source_id"],
                edge["target_id"],
                tuple(sorted(edge["properties"].items())),
            )
            for edge in driver.relationships.values()
        }
        self.assertEqual(
            migrated_edges,
            {
                ("DERIVED_FROM", "external", "shared-canonical", (("direction", "incoming"),)),
                ("SUPERSEDES", "shared-canonical", "reference-a", (("direction", "outgoing"),)),
                ("DERIVED_FROM", "plain-b", "external", (("plain", "source"),)),
                ("SUPERSEDES", "external", "plain-b", (("plain", "target"),)),
                ("DERIVED_FROM", "hybrid", "shared-canonical", (("hybrid", True),)),
            },
        )
        self.assertLess(
            driver.events.index("migration_commit"),
            driver.events.index("identity_constraint"),
        )
        self.assertEqual(
            driver.constraints,
            {"memory_identity_unique", "memory_write_claim_unique"},
        )

        first_state = (copy.deepcopy(driver.nodes), copy.deepcopy(driver.relationships))
        client._initialize_schema()
        self.assertEqual(
            (driver.nodes, driver.relationships),
            first_state,
        )

    def test_neo4j_legacy_migration_conflict_rolls_back_without_constraints(self):
        nodes = {
            "left": {
                "labels": {"Memory"},
                "properties": {
                    "namespace": "tenant",
                    "memory_id": "conflict",
                    "canonical": True,
                    "version": 7,
                    "value": "left",
                    "canonical_state_hash": "left-hash",
                },
            },
            "right": {
                "labels": {"MemoryIdentity", "Memory"},
                "properties": {
                    "namespace": "tenant",
                    "memory_id": "conflict",
                    "canonical": True,
                    "version": 7,
                    "value": "right",
                    "canonical_state_hash": "right-hash",
                },
            },
        }
        driver = _MigrationDriver(nodes)
        before = copy.deepcopy(driver.nodes)
        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = driver

        with self.assertRaisesRegex(VectorGraphBackendError, "divergent canonical"):
            client._initialize_schema()

        self.assertEqual(driver.nodes, before)
        self.assertEqual(driver.relationships, {})
        self.assertEqual(driver.constraints, set())
        self.assertEqual(driver.rollback_count, 1)
        self.assertEqual(driver.events, ["migration_begin", "migration_rollback"])

    def test_neo4j_legacy_migration_preserves_canonical_winner_for_retrieval_and_reopen(self):
        def node(labels, **properties):
            return {
                "labels": set(labels),
                "properties": {
                    "namespace": "tenant",
                    "memory_id": "shared",
                    **properties,
                },
            }

        driver = _MigrationDriver(
            {
                "canonical-identity": node(
                    {"MemoryIdentity", "IdentityMetadata"},
                    canonical=True,
                    version=5,
                    status="active",
                    value="new-value",
                    agent_id="agent-new",
                    scope="tenant:new",
                    canonical_state_hash="hash-v5",
                    canonical_source_ids=[],
                    canonical_supersedes_id=None,
                    identity_marker="keep-identity",
                ),
                "same-version-complement": node(
                    {"Memory", "CanonicalMetadata"},
                    canonical=True,
                    version=5,
                    status="active",
                    value="new-value",
                    agent_id="agent-new",
                    scope="tenant:new",
                    canonical_state_hash="hash-v5",
                    audit_marker="merge-audit",
                    ambiguous_metadata="left",
                ),
                "older-memory": node(
                    {"Memory", "MemoryReference", "LegacyMetadata"},
                    canonical=True,
                    version=4,
                    status="active",
                    value="old-value",
                    agent_id="agent-old",
                    scope="tenant:old",
                    canonical_state_hash="hash-v4",
                    legacy_marker="merge-legacy",
                    ambiguous_metadata="right",
                ),
            }
        )
        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = driver

        client._initialize_schema()

        self.assertEqual(set(driver.nodes), {"canonical-identity"})
        winner = driver.nodes["canonical-identity"]
        self.assertEqual(
            winner["labels"],
            {
                "MemoryIdentity",
                "Memory",
                "MemoryReference",
                "IdentityMetadata",
                "CanonicalMetadata",
                "LegacyMetadata",
            },
        )
        self.assertEqual(
            {
                field: winner["properties"].get(field)
                for field in (
                    "version",
                    "value",
                    "agent_id",
                    "scope",
                    "canonical_state_hash",
                    "identity_marker",
                    "audit_marker",
                    "legacy_marker",
                )
            },
            {
                "version": 5,
                "value": "new-value",
                "agent_id": "agent-new",
                "scope": "tenant:new",
                "canonical_state_hash": "hash-v5",
                "identity_marker": "keep-identity",
                "audit_marker": "merge-audit",
                "legacy_marker": "merge-legacy",
            },
        )
        self.assertNotIn("ambiguous_metadata", winner["properties"])
        self.assertNotIn("_migration_lock", winner["properties"])
        self.assertEqual(
            client.retrieve_memory("tenant", "shared"),
            {
                "status": "active",
                "version": 5,
                "source_ids": [],
                "supersedes_id": None,
                "state_hash": "hash-v5",
                "operation_id": None,
            },
        )
        self.assertEqual(
            driver.constraints,
            {"memory_identity_unique", "memory_write_claim_unique"},
        )

        first_state = copy.deepcopy(driver.nodes)
        client._initialize_schema()

        self.assertEqual(driver.nodes, first_state)
        self.assertEqual(
            client.retrieve_memory("tenant", "shared")["version"], 5
        )

    def test_neo4j_legacy_migration_does_not_backfill_newer_canonical_fields(self):
        def node(element_labels, **properties):
            return {
                "labels": set(element_labels),
                "properties": {
                    "namespace": "tenant",
                    "memory_id": "shared",
                    **properties,
                },
            }

        driver = _MigrationDriver(
            {
                "newer": node(
                    {"MemoryIdentity"},
                    version=6,
                    status="active",
                    value="new-value",
                    canonical_state_hash="new-hash",
                    newer_marker="keep-newer",
                ),
                "older": node(
                    {"Memory", "LegacyMetadata"},
                    canonical=True,
                    version=5,
                    status="active",
                    value="old-value",
                    agent_id="old-agent",
                    scope="tenant:old",
                    canonical_state_hash="old-hash",
                    canonical_source_ids=["old-source"],
                    canonical_supersedes_id="old-memory",
                    legacy_marker="merge-safe-extra",
                ),
            }
        )
        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = driver

        client._initialize_schema()

        self.assertEqual(set(driver.nodes), {"newer"})
        winner = driver.nodes["newer"]
        self.assertEqual(
            winner["labels"],
            {"MemoryIdentity", "Memory", "LegacyMetadata"},
        )
        self.assertEqual(winner["properties"]["version"], 6)
        self.assertEqual(winner["properties"]["value"], "new-value")
        self.assertTrue(winner["properties"]["canonical"])
        self.assertEqual(
            winner["properties"]["canonical_state_hash"], "new-hash"
        )
        self.assertEqual(
            winner["properties"]["legacy_marker"], "merge-safe-extra"
        )
        for field in (
            "agent_id",
            "scope",
            "canonical_source_ids",
            "canonical_supersedes_id",
        ):
            self.assertNotIn(field, winner["properties"])
        self.assertEqual(
            client.retrieve_memory("tenant", "shared"),
            {
                "status": "active",
                "version": 6,
                "source_ids": [],
                "supersedes_id": None,
                "state_hash": "new-hash",
                "operation_id": None,
            },
        )

    def test_neo4j_legacy_migration_drops_collapsed_self_loops_and_duplicate_edges(self):
        def node(memory_id):
            return {
                "labels": {"MemoryIdentity", "Memory"},
                "properties": {
                    "namespace": "tenant",
                    "memory_id": memory_id,
                    "canonical": True,
                    "version": 2,
                    "status": "active",
                    "value": memory_id,
                },
            }

        driver = _MigrationDriver(
            {
                "shared-a": node("shared"),
                "shared-b": node("shared"),
                "external": node("external"),
            },
            [
                {
                    "relationship_id": "collapsed-derived",
                    "type": "DERIVED_FROM",
                    "source_id": "shared-a",
                    "target_id": "shared-b",
                    "properties": {"reason": "collapse"},
                },
                {
                    "relationship_id": "collapsed-supersedes",
                    "type": "SUPERSEDES",
                    "source_id": "shared-b",
                    "target_id": "shared-a",
                    "properties": {"reason": "collapse"},
                },
                {
                    "relationship_id": "derived-existing",
                    "type": "DERIVED_FROM",
                    "source_id": "shared-a",
                    "target_id": "external",
                    "properties": {"direction": "outgoing", "weight": 1},
                },
                {
                    "relationship_id": "derived-duplicate",
                    "type": "DERIVED_FROM",
                    "source_id": "shared-b",
                    "target_id": "external",
                    "properties": {"direction": "outgoing", "weight": 1},
                },
                {
                    "relationship_id": "supersedes-existing",
                    "type": "SUPERSEDES",
                    "source_id": "external",
                    "target_id": "shared-a",
                    "properties": {"direction": "incoming", "weight": 2},
                },
                {
                    "relationship_id": "supersedes-duplicate",
                    "type": "SUPERSEDES",
                    "source_id": "external",
                    "target_id": "shared-b",
                    "properties": {"direction": "incoming", "weight": 2},
                },
            ],
        )
        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = driver

        client._initialize_schema()

        self.assertEqual(set(driver.nodes), {"shared-a", "external"})
        relationships = [
            (
                edge["type"],
                edge["source_id"],
                edge["target_id"],
                edge["properties"],
            )
            for edge in driver.relationships.values()
        ]
        self.assertEqual(
            relationships,
            [
                (
                    "DERIVED_FROM",
                    "shared-a",
                    "external",
                    {"direction": "outgoing", "weight": 1},
                ),
                (
                    "SUPERSEDES",
                    "external",
                    "shared-a",
                    {"direction": "incoming", "weight": 2},
                ),
            ],
        )
        self.assertTrue(
            all(source_id != target_id for _, source_id, target_id, _ in relationships)
        )

        first_state = copy.deepcopy(driver.relationships)
        client._initialize_schema()
        self.assertEqual(driver.relationships, first_state)

    def test_neo4j_transaction_cleanup_does_not_sweep_other_orphan_references(self):
        references = {"reference-owned-by-txn-b"}

        class Result:
            def consume(self):
                return None

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query, **parameters):
                if "MATCH (r:MemoryReference" in query:
                    references.clear()
                return Result()

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()

        client.delete_many_by_txn("tenant", "txn-a", "cleanup-key")

        self.assertEqual(references, {"reference-owned-by-txn-b"})

    def test_qdrant_point_id_is_stable_uuid_for_arbitrary_memory_ids(self):
        first = _qdrant_point_id("tenant", "real_a")
        self.assertEqual(first, _qdrant_point_id("tenant", "real_a"))
        self.assertNotEqual(first, _qdrant_point_id("tenant", "real_b"))
        self.assertRegex(first, r"^[0-9a-f-]{36}$")

    def test_qdrant_raw_readback_creates_a_missing_collection_before_scroll(self):
        client = _QdrantHTTPClient("http://qdrant")
        created = False

        def request(method, path, payload=None):
            nonlocal created
            if method == "PUT" and path == "/collections/txnmem_memory":
                created = True
                return {}
            if method == "GET" and path == "/collections/txnmem_memory":
                return {
                    "result": {
                        "config": {
                            "params": {
                                "vectors": {"size": 32, "distance": "Cosine"}
                            }
                        }
                    }
                }
            if path.endswith("/points/scroll"):
                if not created:
                    raise RuntimeError("collection missing")
                return {"result": {"points": []}}
            raise AssertionError((method, path, payload))

        client._request = request

        self.assertEqual(
            client.retrieve_many_by_txn("tenant", "txn-empty"),
            {"read_ok": True, "rows": []},
        )

    def test_qdrant_collection_readiness_is_cached_after_exact_conflict(self):
        client = _QdrantHTTPClient("http://qdrant")
        requests = []

        def request(method, path, payload=None):
            requests.append((method, path, payload))
            if method == "PUT":
                raise HTTPError(
                    "http://qdrant/collections/txnmem_memory",
                    409,
                    "Conflict",
                    None,
                    None,
                )
            return {
                "result": {
                    "config": {
                        "params": {
                            "vectors": {"size": 32, "distance": "Cosine"}
                        }
                    }
                }
            }

        client._request = request

        client._ensure_collection()
        client._ensure_collection()

        self.assertEqual(
            [(method, path) for method, path, _payload in requests],
            [
                ("PUT", "/collections/txnmem_memory"),
                ("GET", "/collections/txnmem_memory"),
            ],
        )

    def test_qdrant_collection_readiness_rejects_lookalike_409_text(self):
        client = _QdrantHTTPClient("http://qdrant")
        attempts = 0

        def request(_method, _path, _payload=None):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("unrelated diagnostic token 409")

        client._request = request

        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "409"):
                client._ensure_collection()

        self.assertEqual(attempts, 2)

    def test_qdrant_collection_readiness_rejects_wrong_existing_vector_config(self):
        client = _QdrantHTTPClient("http://qdrant")
        requests = []

        def request(method, path, payload=None):
            requests.append((method, path, payload))
            if method == "PUT":
                raise HTTPError(path, 409, "Conflict", None, None)
            return {
                "result": {
                    "config": {
                        "params": {
                            "vectors": {"size": 64, "distance": "Dot"}
                        }
                    }
                }
            }

        client._request = request

        for _ in range(2):
            with self.assertRaises(VectorGraphBackendError):
                client._ensure_collection()

        self.assertEqual(
            [(method, path) for method, path, _payload in requests],
            [
                ("PUT", "/collections/txnmem_memory"),
                ("GET", "/collections/txnmem_memory"),
                ("PUT", "/collections/txnmem_memory"),
                ("GET", "/collections/txnmem_memory"),
            ],
        )

    def test_qdrant_collection_readiness_does_not_cache_unknown_failure(self):
        client = _QdrantHTTPClient("http://qdrant")
        attempts = 0

        def request(_method, _path, _payload=None):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("collection probe timed out")
            if attempts == 2:
                return {}
            return {
                "result": {
                    "config": {
                        "params": {
                            "vectors": {"size": 32, "distance": "Cosine"}
                        }
                    }
                }
            }

        client._request = request

        with self.assertRaises(TimeoutError):
            client._ensure_collection()
        client._ensure_collection()
        client._ensure_collection()

        self.assertEqual(attempts, 3)

    def test_qdrant_namespace_scan_paginates_beyond_one_thousand_rows(self):
        client = _QdrantHTTPClient("http://qdrant")
        offsets = []

        def request(method, path, payload=None):
            if method == "PUT":
                return {}
            if method == "GET":
                return {
                    "result": {
                        "config": {
                            "params": {
                                "vectors": {"size": 32, "distance": "Cosine"}
                            }
                        }
                    }
                }
            self.assertTrue(path.endswith("/points/scroll"))
            offset = payload.get("offset")
            offsets.append(offset)
            start = 0 if offset is None else int(offset)
            stop = min(start + int(payload["limit"]), 1500)
            points = [
                {"payload": {"memory_id": f"m{index}"}}
                for index in range(start, stop)
            ]
            return {
                "result": {
                    "points": points,
                    "next_page_offset": stop if stop < 1500 else None,
                }
            }

        client._request = request

        result = client.scan_namespace("tenant", limit=1501)

        self.assertTrue(result["read_ok"])
        self.assertEqual(len(result["rows"]), 1500)
        self.assertEqual(offsets, [None, 1000])

    def test_namespace_scans_reject_bool_and_fractional_limits(self):
        qdrant = _QdrantHTTPClient("http://qdrant")
        neo4j = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        for client in (qdrant, neo4j):
            for limit in (True, 1.5):
                with self.subTest(client=type(client).__name__, limit=limit):
                    with self.assertRaises(ValueError):
                        client.scan_namespace("tenant", limit=limit)

    def test_neo4j_namespace_scan_returns_canonical_rows_only(self):
        calls = []

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, query, **parameters):
                calls.append((query, parameters))
                return [
                    {
                        "memory_id": "m1",
                        "status": "active",
                        "source_ids": ["m0", None],
                    }
                ]

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()

        result = client.scan_namespace("tenant", limit=11)

        self.assertEqual(
            result,
            {
                "read_ok": True,
                "rows": [
                    {"memory_id": "m1", "status": "active", "source_ids": ["m0"]}
                ],
            },
        )
        self.assertEqual(calls[0][1], {"namespace": "tenant", "limit": 11})
        self.assertIn("MemoryIdentity:Memory", calls[0][0])

    def test_qdrant_mutations_wait_for_operation_after_readback(self):
        client = _QdrantHTTPClient("http://qdrant")
        requests = []
        client._ensure_collection = lambda: None
        client._request = lambda method, path, payload=None: requests.append(
            (method, path, payload)
        )

        client.upsert("tenant", "memory", [0.0] * 32, {"memory_id": "memory"}, "key")
        client.delete("tenant", "memory", "key")

        self.assertEqual(requests[0][1], "/collections/txnmem_memory/points?wait=true")
        self.assertEqual(requests[1][1], "/collections/txnmem_memory/points/delete?wait=true")

    def setUp(self):
        self.qdrant = _FakeQdrant()
        self.neo4j = _FakeNeo4j()
        self.backend = VectorGraphMemoryBackend(
            "episode-1",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
        )

    def test_write_read_search_derive_provenance_and_supersede(self):
        self.backend.write("m0", value="source")
        self.assertEqual(self.backend.read("m0")["value"], "source")
        self.assertEqual(self.backend.search("source")[0]["memory_id"], "m0")
        self.backend.derive("m1", ["m0"], value="derived")
        self.backend.propagate("m2", "m1", value="propagated")
        self.backend.supersede("m0", "m3", value="corrected")
        self.assertEqual(self.backend.memories["m0"]["status"], "superseded")
        self.assertTrue(any(edge[3] == "DERIVED_FROM" for edge in self.neo4j.edges))
        self.assertTrue(any(edge[3] == "SUPERSEDES" for edge in self.neo4j.edges))

    def test_preload_record_repairs_one_verified_partial_projection(self):
        class OneShotProjectionFailure(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.fail_once = True

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                if self.fail_once:
                    self.fail_once = False
                    self.upsert_count += 1
                    raise ConnectionResetError("projection response boundary failed")
                return super().upsert(
                    namespace, point_id, vector, payload, idempotency_key
                )

        qdrant = OneShotProjectionFailure()
        neo4j = _FakeNeo4j()
        backend = VectorGraphMemoryBackend(
            "preload-repair",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            max_retries=0,
        )

        recovery_count = backend.preload_provenance_record(
            "m0", [], value="provenance:m0"
        )

        self.assertEqual(recovery_count, 1)
        self.assertEqual(qdrant.retrieve("preload-repair", "m0")["memory_id"], "m0")
        self.assertEqual(
            neo4j.retrieve_memory("preload-repair", "m0")["status"], "active"
        )
        self.assertEqual(backend.metrics()["retry_count"], 0)
        self.assertEqual(backend.metrics()["rollback_count"], 1)

    def test_preload_record_never_repairs_a_deterministic_conflict(self):
        backend = VectorGraphMemoryBackend(
            "preload-conflict",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            max_retries=0,
        )
        backend.write("m0", value="first")
        conflicting_backend = VectorGraphMemoryBackend(
            "preload-conflict",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            max_retries=0,
        )

        with self.assertRaises(VectorGraphBackendError):
            conflicting_backend.preload_provenance_record(
                "m0", [], value="different"
            )

        self.assertEqual(self.qdrant.upsert_count, 1)

    def test_preload_record_fails_closed_when_partial_state_is_unreadable(self):
        class UnreadableProjection(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.upsert_attempts = 0

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                self.upsert_attempts += 1
                raise ConnectionResetError("projection boundary failed")

            def retrieve(self, namespace, point_id):
                raise TimeoutError("projection readback unavailable")

        qdrant = UnreadableProjection()
        neo4j = _FakeNeo4j()
        backend = VectorGraphMemoryBackend(
            "preload-unknown",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            max_retries=0,
        )

        with self.assertRaises(VectorGraphBackendError):
            backend.preload_provenance_record(
                "m0", [], value="provenance:m0"
            )

        self.assertEqual(qdrant.upsert_attempts, 0)
        self.assertIsNotNone(neo4j.retrieve_memory("preload-unknown", "m0"))

    def test_preload_record_repairs_one_verified_lost_success_response(self):
        class OneShotLostResponse(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.lose_once = True

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                super().upsert(
                    namespace, point_id, vector, payload, idempotency_key
                )
                if self.lose_once:
                    self.lose_once = False
                    raise ConnectionResetError("projection success response lost")

        qdrant = OneShotLostResponse()
        backend = VectorGraphMemoryBackend(
            "preload-lost-response",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )

        recovered = backend.preload_provenance_record(
            "m0", [], value="provenance:m0"
        )

        self.assertEqual(recovered, 1)
        self.assertEqual(qdrant.upsert_count, 1)
        self.assertEqual(backend.metrics()["retry_count"], 0)

    def test_preload_record_repairs_one_verified_readback_boundary_failure(self):
        class OneShotVerifyFailure(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.retrieve_count = 0

            def retrieve(self, namespace, point_id):
                self.retrieve_count += 1
                if self.retrieve_count == 2:
                    raise TimeoutError("projection verification unavailable")
                return super().retrieve(namespace, point_id)

        qdrant = OneShotVerifyFailure()
        backend = VectorGraphMemoryBackend(
            "preload-verify-repair",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )

        recovered = backend.preload_provenance_record(
            "m0", [], value="provenance:m0"
        )

        self.assertEqual(recovered, 1)
        self.assertEqual(qdrant.upsert_count, 1)
        self.assertEqual(backend.metrics()["retry_count"], 0)

    def test_preload_record_fails_closed_when_one_repair_is_exhausted(self):
        class RepeatedProjectionFailure(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.upsert_attempts = 0

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                self.upsert_attempts += 1
                raise ConnectionResetError("projection boundary remains unavailable")

        qdrant = RepeatedProjectionFailure()
        backend = VectorGraphMemoryBackend(
            "preload-budget",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )

        with self.assertRaises(VectorGraphBackendError):
            backend.preload_provenance_record(
                "m0", [], value="provenance:m0"
            )

        self.assertEqual(qdrant.upsert_attempts, 2)

    def test_parallel_and_sequential_preload_close_the_same_canonical_graph(self):
        graph = build_layered_dag(100, seed=17)
        parallel = VectorGraphMemoryBackend(
            "preload-parallel",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )
        sequential = VectorGraphMemoryBackend(
            "preload-sequential",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )
        sequential.supports_parallel_provenance_preload = False

        parallel_metadata = _preload_graph(parallel, graph)
        sequential_metadata = _preload_graph(sequential, graph)

        self.assertEqual(
            parallel.provenance_inventory(limit=101),
            sequential.provenance_inventory(limit=101),
        )
        self.assertEqual(parallel_metadata["setup_repair_count"], 0)
        self.assertEqual(sequential_metadata["setup_repair_count"], 0)

    def test_parallel_preload_serializes_local_event_and_state_bookkeeping(self):
        graph = build_layered_dag(100, seed=17)
        backend = VectorGraphMemoryBackend(
            "preload-thread-state",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )

        def deliberately_racy_event(self, kind, **fields):
            step = len(self.events) + 1
            time.sleep(0.002)
            event = {
                "event_id": f"backend_event_{step:04d}",
                "kind": kind,
                "step": step,
                "agent_id": fields.get("agent_id", "agent_1"),
            }
            event.update(
                {key: value for key, value in fields.items() if value is not None}
            )
            self.events.append(event)
            return event

        with patch.object(
            InstrumentedMemoryBackend,
            "_event",
            deliberately_racy_event,
        ):
            _preload_graph(backend, graph)

        self.assertEqual(len(backend.memories), graph.node_count)
        self.assertEqual(len(backend.events), graph.node_count)
        self.assertEqual(
            [event["step"] for event in backend.events],
            list(range(1, graph.node_count + 1)),
        )
        self.assertEqual(
            len({event["event_id"] for event in backend.events}),
            graph.node_count,
        )
        metrics = backend.metrics()
        self.assertEqual(
            metrics["request_count"],
            sum(metrics["operation_counts"].values()),
        )

    def test_search_handles_structured_memory_values_without_hashing_them(self):
        self.backend.write("m-structured", value={"tool": "search", "result": [1, 2]})

        self.assertEqual(self.backend.search("not-present"), [])
        self.assertEqual(
            self.backend.search("m-structured")[0]["memory_id"],
            "m-structured",
        )

    def test_duplicate_request_is_idempotent_and_does_not_duplicate_event(self):
        first = self.backend.write("m0", value="source")
        second = self.backend.write("m0", value="source")
        self.assertEqual(first, second)
        self.assertEqual(len(self.backend.validated_events()), 1)
        self.assertEqual(self.qdrant.upsert_count, 1)

    def test_graph_failure_compensates_vector_and_emits_no_commit_event(self):
        self.neo4j.fail_upsert = True
        with self.assertRaises(RuntimeError):
            self.backend.write("m0", value="source")
        self.assertNotIn(("episode-1", "m0"), self.qdrant.points)
        self.assertNotIn("m0", self.backend.memories)
        self.assertEqual(self.backend.validated_events(), [])
        self.assertEqual(self.backend.metrics()["rollback_count"], 1)

    def test_commit_response_loss_retries_the_same_cas_and_materializes_both_stores(self):
        original = self.neo4j.compare_and_set_memory
        lost = False

        def commit_then_raise(*args, **kwargs):
            nonlocal lost
            result = original(*args, **kwargs)
            if not lost:
                lost = True
                raise ConnectionResetError("response lost after commit")
            return result

        self.neo4j.compare_and_set_memory = commit_then_raise

        result = self.backend.write("m-ambiguous", value="source")

        verification = self.backend.verify_persistent_state(
            [{"type": "write", "memory_id": "m-ambiguous", "value": "source"}]
        )
        self.assertEqual(result["value"], "source")
        self.assertEqual(verification["classification"], "complete")

    def test_ambiguous_commit_recovery_fails_closed_when_absence_cannot_be_read(self):
        def unreadable_after_commit(*_args, **_kwargs):
            raise TimeoutError("graph read unavailable")

        self.neo4j.retrieve_memory = unreadable_after_commit

        with self.assertRaisesRegex(
            VectorGraphBackendError,
            "canonical transition is recoverable",
        ):
            self.backend.write("m-unknown", value="source")

    def test_persistent_state_read_failure_is_unknown_not_absent(self):
        self.backend.write("m0", value="source")

        def fail_read(*_args, **_kwargs):
            raise TimeoutError("qdrant read unavailable")

        self.qdrant.retrieve = fail_read
        verification = self.backend.verify_persistent_state(
            [{"type": "write", "memory_id": "m0", "value": "source"}]
        )

        self.assertEqual(verification["classification"], "unknown")
        self.assertFalse(verification["items"][0]["qdrant"]["read_ok"])
        self.assertNotIn("present", verification["items"][0]["qdrant"])

    def test_healthcheck_and_invalidation_are_reported(self):
        health = self.backend.healthcheck()
        self.assertTrue(health["qdrant"]["available"])
        self.assertTrue(health["neo4j"]["available"])
        self.backend.write("m0", value="source")
        self.backend.invalidate("m0")
        self.assertEqual(self.backend.read("m0"), None)

    def test_provenance_inventory_matches_both_persistent_stores(self):
        self.backend.write("m0", value="source")
        self.backend.derive("m1", ["m0"], value="derived")
        self.backend.invalidate("m1")

        inventory = self.backend.provenance_inventory(limit=10)

        self.assertEqual(inventory["classification"], "complete")
        self.assertEqual(inventory["node_count"], 2)
        self.assertEqual(inventory["edge_count"], 1)
        self.assertEqual(
            inventory["graph_sha256"],
            canonical_graph_sha256(["m0", "m1"], [("m0", "m1")]),
        )
        self.assertEqual(inventory["status_counts"], {"active": 1, "invalid": 1})
        self.assertNotIn("nodes", inventory)
        self.assertNotIn("edges", inventory)

    def test_provenance_inventory_fails_closed_on_cross_store_mismatch(self):
        self.backend.write("m0", value="source")
        self.backend.derive("m1", ["m0"], value="derived")
        self.qdrant.points[("episode-1", "m1")]["payload"]["derived_from"] = []

        inventory = self.backend.provenance_inventory(limit=10)

        self.assertEqual(inventory["classification"], "partial")
        self.assertIsNone(inventory["graph_sha256"])

    def test_provenance_inventory_fails_closed_on_unreadable_store(self):
        self.backend.write("m0", value="source")
        self.neo4j.fail_readback = True

        inventory = self.backend.provenance_inventory(limit=10)

        self.assertEqual(inventory["classification"], "unknown")
        self.assertIsNone(inventory["node_count"])

    def test_provenance_inventory_rejects_duplicate_physical_rows(self):
        self.backend.write("m0", value="source")
        original = self.qdrant.scan_namespace

        def duplicate_scan(namespace, limit=1000):
            result = original(namespace, limit=limit)
            result["rows"].append(copy.deepcopy(result["rows"][0]))
            return result

        self.qdrant.scan_namespace = duplicate_scan

        inventory = self.backend.provenance_inventory(limit=10)

        self.assertNotEqual(inventory["classification"], "complete")

    def test_provenance_inventory_rejects_bool_and_fractional_limits(self):
        for limit in (True, 1.5):
            with self.subTest(limit=limit):
                with self.assertRaises(ValueError):
                    self.backend.provenance_inventory(limit=limit)

    def test_provenance_matrix_cell_closes_real_backend_seam(self):
        namespaces = []

        def factory(namespace):
            namespaces.append(namespace)
            return VectorGraphMemoryBackend(
                namespace,
                "http://qdrant",
                "bolt://neo4j",
                ("neo4j", "fixture"),
                qdrant_client=_FakeQdrant(),
                neo4j_client=_FakeNeo4j(),
                max_retries=0,
            )

        report = run_matrix_cell(
            factory,
            build_layered_dag(10, seed=17),
            concurrency=2,
            repetitions=1,
            operations_per_type=1,
            run_id="vector-graph-fixture",
            formal=True,
            environment_attestation={
                "schema": "txnmem-provenance-environment-v1",
                "isolation_verified": True,
                "co_tenant_load_detected": False,
                "source": "host-observation-v1",
                "cpu_logical_count": 8,
                "memory_total_bytes": 16 * 1024**3,
                "disk_medium": "ssd",
                "toxiproxy_version": "2.9.0",
            },
        )

        self.assertEqual(len(namespaces), 1)
        self.assertTrue(report["formal_eligible"])
        self.assertTrue(report["repetitions"][0]["state_closed"])

    def test_preload_repair_is_accounted_without_changing_measured_retry_zero(self):
        class OneShotProjectionFailure(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.fail_once = True
                self.failure_lock = threading.Lock()

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                with self.failure_lock:
                    if self.fail_once:
                        self.fail_once = False
                        raise ConnectionResetError("preload projection boundary failed")
                return super().upsert(
                    namespace, point_id, vector, payload, idempotency_key
                )

        def factory(namespace):
            return VectorGraphMemoryBackend(
                namespace,
                "http://qdrant",
                "bolt://neo4j",
                ("neo4j", "fixture"),
                qdrant_client=OneShotProjectionFailure(),
                neo4j_client=_FakeNeo4j(),
                max_retries=0,
            )

        report = run_matrix_cell(
            factory,
            build_layered_dag(10, seed=17),
            concurrency=2,
            repetitions=1,
            operations_per_type=1,
            run_id="preload-repair-accounting",
            formal=True,
            environment_attestation={
                "schema": "txnmem-provenance-environment-v1",
                "isolation_verified": True,
                "co_tenant_load_detected": False,
                "source": "host-observation-v1",
                "cpu_logical_count": 8,
                "memory_total_bytes": 16 * 1024**3,
                "disk_medium": "ssd",
                "toxiproxy_version": "2.9.0",
            },
        )

        repetition = report["repetitions"][0]
        self.assertEqual(repetition["setup_repair_count"], 1)
        self.assertEqual(repetition["retry_count"], 0)
        self.assertEqual(repetition["retry_scope"], "measured_operations_only")
        self.assertTrue(repetition["eligible_for_formal"])

    def test_measured_failure_is_never_absorbed_by_setup_repair(self):
        class MeasuredProjectionFailure(_FakeQdrant):
            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                if str(payload.get("memory_id", "")).startswith("perf-derived-"):
                    raise ConnectionResetError("measured projection boundary failed")
                return super().upsert(
                    namespace, point_id, vector, payload, idempotency_key
                )

        def factory(namespace):
            return VectorGraphMemoryBackend(
                namespace,
                "http://qdrant",
                "bolt://neo4j",
                ("neo4j", "fixture"),
                qdrant_client=MeasuredProjectionFailure(),
                neo4j_client=_FakeNeo4j(),
                max_retries=0,
            )

        with self.assertRaises(ProvenancePerformanceError):
            run_matrix_cell(
                factory,
                build_layered_dag(10, seed=17),
                concurrency=1,
                repetitions=1,
                operations_per_type=1,
                run_id="measured-failure-no-setup-repair",
                formal=True,
                environment_attestation={
                    "schema": "txnmem-provenance-environment-v1",
                    "isolation_verified": True,
                    "co_tenant_load_detected": False,
                    "source": "host-observation-v1",
                    "cpu_logical_count": 8,
                    "memory_total_bytes": 16 * 1024**3,
                    "disk_medium": "ssd",
                    "toxiproxy_version": "2.9.0",
                },
            )

    def test_proxy_requester_observes_semantic_write_and_commit_boundaries(self):
        observed = []

        def requester(service, operation, function, key):
            observed.append((service, operation, key))
            return function()

        backend = VectorGraphMemoryBackend(
            "episode-proxy",
            "http://qdrant-proxy:19000",
            "bolt://neo4j-proxy:19001",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )

        backend.write("m0", value="source")

        self.assertEqual(
            [(service, operation) for service, operation, _ in observed],
            [
                ("neo4j", "write_cas"),
                ("neo4j", "write_projection_guard"),
                ("qdrant", "write_read"),
                ("qdrant", "write"),
                ("qdrant", "write_verify"),
                ("neo4j", "write_verify"),
            ],
        )

    def test_qdrant_failure_does_not_multiply_or_relabel_neo4j_guard_retries(self):
        observed = []

        def requester(service, operation, function, key):
            observed.append((service, operation))
            if service == "qdrant" and operation == "write":
                raise TimeoutError("persistent qdrant projection failure")
            return function()

        backend = VectorGraphMemoryBackend(
            "projection-qdrant-failure",
            "http://qdrant-proxy",
            "bolt://neo4j-proxy",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=1,
        )

        with self.assertRaises(VectorGraphBackendError):
            backend.write("shared", value="value")

        self.assertEqual(observed.count(("qdrant", "write")), 2)
        self.assertEqual(
            observed.count(("neo4j", "write_projection_guard")), 1
        )
        self.assertEqual(observed.count(("qdrant", "write_read")), 1)
        metrics = backend.metrics()
        self.assertEqual(metrics["retry_count"], 1)
        self.assertEqual(metrics["error_count"], 1)
        self.assertEqual(metrics["operation_counts"]["qdrant:write"], 2)
        self.assertEqual(
            metrics["operation_counts"]["neo4j:write_projection_guard"], 1
        )

    def test_neo4j_guard_origin_failure_keeps_neo4j_retry_behavior(self):
        observed = []
        failures = {"remaining": 1}

        def requester(service, operation, function, key):
            observed.append((service, operation))
            if (
                service == "neo4j"
                and operation == "write_projection_guard"
                and failures["remaining"]
            ):
                failures["remaining"] -= 1
                raise TimeoutError("one-shot neo4j guard failure")
            return function()

        backend = VectorGraphMemoryBackend(
            "projection-neo4j-failure",
            "http://qdrant-proxy",
            "bolt://neo4j-proxy",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=1,
        )

        self.assertEqual(backend.write("shared", value="value")["value"], "value")

        self.assertEqual(
            observed.count(("neo4j", "write_projection_guard")), 2
        )
        self.assertEqual(observed.count(("qdrant", "write")), 1)
        metrics = backend.metrics()
        self.assertEqual(metrics["retry_count"], 1)
        self.assertEqual(metrics["error_count"], 0)
        self.assertEqual(
            metrics["operation_counts"]["neo4j:write_projection_guard"], 2
        )

    def test_direct_read_and_search_keep_proxy_and_request_metrics_boundaries(self):
        observed = []

        def requester(service, operation, function, key):
            observed.append((service, operation, key))
            return function()

        backend = VectorGraphMemoryBackend(
            "episode-proxy-reads",
            "http://qdrant-proxy:19000",
            "bolt://neo4j-proxy:19001",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=0,
        )
        backend.write("m0", value="source")
        requests_after_write = backend.metrics()["request_count"]

        backend.read("m0")
        backend.search("source")

        self.assertEqual(
            [(service, operation) for service, operation, _ in observed[-4:]],
            [
                ("qdrant", "read_rows"),
                ("neo4j", "read_canonical"),
                ("qdrant", "search_rows"),
                ("neo4j", "search_canonical"),
            ],
        )
        self.assertEqual(
            backend.metrics()["request_count"],
            requests_after_write + 4,
        )

    def test_read_retries_qdrant_and_neo4j_at_their_own_proxy_boundaries(self):
        observed = []
        failures = {"qdrant": 1, "neo4j": 1}

        def requester(service, operation, function, key):
            observed.append((service, operation))
            if operation in {"read_rows", "read_canonical"} and failures[service]:
                failures[service] -= 1
                raise TimeoutError(f"one-shot {service} read fault")
            return function()

        backend = VectorGraphMemoryBackend(
            "episode-proxy-read-retries",
            "http://qdrant-proxy:19000",
            "bolt://neo4j-proxy:19001",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            max_retries=1,
        )
        backend.write("m0", value="source")
        observed.clear()
        retries_before = backend.metrics()["retry_count"]

        self.assertEqual(backend.read("m0")["value"], "source")

        self.assertEqual(
            observed,
            [
                ("qdrant", "read_rows"),
                ("qdrant", "read_rows"),
                ("neo4j", "read_canonical"),
                ("neo4j", "read_canonical"),
            ],
        )
        self.assertEqual(backend.metrics()["retry_count"], retries_before + 2)

    def test_staged_visibility_retries_neo4j_on_its_own_boundary(self):
        decisions = {"txn-visible": None}
        observed = []
        staged_faults = {"remaining": 1}

        def requester(service, operation, function, key):
            observed.append((service, operation))
            if (
                service == "neo4j"
                and operation == "read_staged"
                and staged_faults["remaining"]
            ):
                staged_faults["remaining"] -= 1
                raise TimeoutError("one-shot Neo4j staged read fault")
            return function()

        backend = VectorGraphMemoryBackend(
            "episode-proxy-staged-read",
            "http://qdrant-proxy:19000",
            "bolt://neo4j-proxy:19001",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=_FakeQdrant(),
            neo4j_client=_FakeNeo4j(),
            decision_resolver=decisions.get,
            max_retries=1,
        )
        intents = [
            self._intent(
                "txn-visible", 1, "memory_write", memory_id="m0", value="source"
            )
        ]
        backend.stage_transaction("txn-visible", intents)
        decisions["txn-visible"] = "COMMITTED"
        observed.clear()
        retries_before = backend.metrics()["retry_count"]

        self.assertEqual(backend.read_committed("m0")["value"], "source")

        self.assertEqual(
            observed,
            [
                ("qdrant", "read_rows"),
                ("neo4j", "read_canonical"),
                ("neo4j", "read_staged_ids"),
                ("qdrant", "read_staged"),
                ("neo4j", "read_staged"),
                ("neo4j", "read_staged"),
            ],
        )
        self.assertEqual(backend.metrics()["retry_count"], retries_before + 1)

    def test_public_invalidate_retries_and_instruments_its_existence_read(self):
        class OneShotTimeoutQdrant(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.timeout_once = False

            def retrieve(self, namespace, point_id):
                if self.timeout_once:
                    self.timeout_once = False
                    raise TimeoutError("one-shot existence read timeout")
                return super().retrieve(namespace, point_id)

        observed = []
        qdrant = OneShotTimeoutQdrant()

        def requester(service, operation, function, key):
            observed.append((service, operation))
            return function()

        backend = VectorGraphMemoryBackend(
            "invalidate-instrumented",
            "http://qdrant-proxy",
            "bolt://neo4j-proxy",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=qdrant,
            neo4j_client=_FakeNeo4j(),
            max_retries=1,
        )
        backend.write("existing", value="value")
        observed.clear()
        retries_before = backend.metrics()["retry_count"]
        qdrant.timeout_once = True

        backend.invalidate("existing")
        backend.invalidate("missing")

        self.assertEqual(backend.metrics()["retry_count"], retries_before + 1)
        self.assertGreaterEqual(
            observed.count(("qdrant", "invalidate_lookup")), 3
        )
        self.assertEqual(
            [event["memory_id"] for event in backend.validated_events() if event["kind"] == "invalidate"],
            ["existing", "missing"],
        )
        self.assertIsNone(backend.read_committed("existing"))

    def test_staged_records_are_raw_pending_and_fail_closed_until_committed(self):
        decisions = {}
        backend = VectorGraphMemoryBackend(
            "txn-visible",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        intents = [
            self._intent("txn-a", 1, "memory_write", memory_id="m-a", value="secret-a"),
            self._intent(
                "txn-a",
                2,
                "memory_derive",
                memory_id="m-b",
                source_ids=["m-a"],
                value="secret-b",
            ),
        ]

        backend.stage_transaction("txn-a", intents)

        raw = backend.raw_transaction_state("txn-a", intents)
        self.assertEqual(
            [(item["memory_id"], item["status"]) for item in raw["qdrant"]["objects"] if item["record_kind"] == "memory"],
            [("m-a", "pending"), ("m-b", "pending")],
        )
        self.assertEqual(
            [(item["memory_id"], item["status"]) for item in raw["neo4j"]["nodes"] if item["record_kind"] == "memory"],
            [("m-a", "pending"), ("m-b", "pending")],
        )
        self.assertIsNone(backend.read("m-a"))
        self.assertEqual(backend.search(), [])

        decisions["txn-a"] = "COMMITTED"
        committed = backend.read_committed("m-a")
        self.assertEqual(committed["value"], "secret-a")
        self.assertNotIn("base_version", committed)
        self.assertNotIn("txn_id", committed)
        self.assertEqual(
            [item["memory_id"] for item in backend.search_committed()],
            ["m-a", "m-b"],
        )
        self.assertEqual(
            backend.raw_transaction_state("txn-a", intents)["gateway_visible"],
            ["m-a", "m-b"],
        )

        aborted = [
            self._intent("txn-b", 1, "memory_write", memory_id="m-c", value="secret-c")
        ]
        backend.stage_transaction("txn-b", aborted)
        decisions["txn-b"] = "ABORTED"
        self.assertIsNone(backend.read_committed("m-c"))
        self.assertNotIn("m-c", [item["memory_id"] for item in backend.search_committed()])

    def test_decision_visibility_rejects_full_state_tampering_and_redacts_raw_evidence(self):
        mutations = {
            "agent_id": lambda row: row.__setitem__("agent_id", "agent-forged"),
            "scope": lambda row: row.__setitem__("scope", "tenant:forged"),
            "value": lambda row: row.__setitem__("value", "forged-value"),
            "target_status": lambda row: row.__setitem__(
                "target_status", "invalid"
            ),
            "sequence": lambda row: row.__setitem__("sequence", 99),
            "operation": lambda row: row.__setitem__(
                "operation", "memory_supersede"
            ),
            "provenance": lambda row: row.__setitem__(
                "derived_from", ["forged-source"]
            ),
            "state_hash": lambda row: row.__setitem__(
                "staged_state_hash", "forged-state-hash"
            ),
        }
        for index, (field, mutate) in enumerate(mutations.items()):
            with self.subTest(field=field):
                qdrant = _FakeQdrant()
                neo4j = _FakeNeo4j()
                txn_id = f"txn-tamper-{index}"
                backend = VectorGraphMemoryBackend(
                    f"staged-tamper-{index}",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                    decision_resolver={txn_id: "COMMITTED"}.get,
                )
                intents = [
                    self._intent(
                        txn_id,
                        1,
                        "memory_write",
                        memory_id="shared",
                        value="original-value",
                        agent_id="agent-original",
                        scope="tenant:original",
                    )
                ]
                backend.stage_transaction(txn_id, intents)
                qdrant_row = next(
                    point["payload"]
                    for point in qdrant.points.values()
                    if point["payload"].get("txn_id") == txn_id
                )
                neo4j_row = next(
                    row
                    for row in neo4j.staged_memories.values()
                    if row.get("txn_id") == txn_id
                )
                original_state_hash = qdrant_row.get("staged_state_hash")

                mutate(qdrant_row)

                self._assert_staged_fail_closed(backend, "shared")
                self.assertIsInstance(original_state_hash, str)
                self.assertEqual(
                    original_state_hash,
                    neo4j_row["staged_state_hash"],
                )
                raw = backend.raw_transaction_state(txn_id, intents)
                serialized = json.dumps(raw, sort_keys=True)
                self.assertNotIn("original-value", serialized)
                self.assertNotIn("agent-original", serialized)
                self.assertNotIn("tenant:original", serialized)

    def test_decision_visibility_rejects_a_missing_staged_node_in_either_store(self):
        for missing_store in ("qdrant", "neo4j"):
            with self.subTest(missing_store=missing_store):
                qdrant = _FakeQdrant()
                neo4j = _FakeNeo4j()
                txn_id = f"txn-missing-{missing_store}"
                backend = VectorGraphMemoryBackend(
                    f"staged-missing-{missing_store}",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                    decision_resolver={txn_id: "COMMITTED"}.get,
                )
                intents = [
                    self._intent(
                        txn_id,
                        1,
                        "memory_write",
                        memory_id="shared",
                        value="value",
                    )
                ]
                backend.stage_transaction(txn_id, intents)
                if missing_store == "qdrant":
                    qdrant.points = {
                        key: row
                        for key, row in qdrant.points.items()
                        if row["payload"].get("txn_id") != txn_id
                    }
                else:
                    neo4j.staged_memories = {
                        key: row
                        for key, row in neo4j.staged_memories.items()
                        if row.get("txn_id") != txn_id
                    }

                self._assert_staged_fail_closed(backend, "shared")

        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        decisions = {
            "txn-complete": "COMMITTED",
            "txn-neo4j-only": "COMMITTED",
        }
        backend = VectorGraphMemoryBackend(
            "staged-missing-among-multiple",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        intents = [
            self._intent(
                "txn-complete",
                1,
                "memory_write",
                memory_id="shared",
                value="value",
            )
        ]
        backend.stage_transaction("txn-complete", intents)
        complete_key, complete_row = next(iter(neo4j.staged_memories.items()))
        neo4j.staged_memories[
            (complete_key[0], complete_key[1], "txn-neo4j-only", *complete_key[3:])
        ] = {
            **complete_row,
            "txn_id": "txn-neo4j-only",
        }

        self._assert_staged_fail_closed(backend, "shared")

    def test_decision_visibility_accepts_neo4j_omission_of_null_optional_properties(self):
        txn_id = "txn-null-optional"
        backend = VectorGraphMemoryBackend(
            "staged-null-optional",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver={txn_id: "COMMITTED"}.get,
        )
        intents = [
            self._intent(
                txn_id,
                1,
                "memory_write",
                memory_id="shared",
                value="value",
            )
        ]
        backend.stage_transaction(txn_id, intents)
        staged = next(
            row
            for row in self.neo4j.staged_memories.values()
            if row.get("txn_id") == txn_id
        )
        staged.pop("supersedes_id")

        self.assertEqual(backend.read_committed("shared")["value"], "value")
        self.assertEqual(
            [record["memory_id"] for record in backend.search_committed()],
            ["shared"],
        )
        self.assertEqual(backend.current_version("shared"), 1)

    def test_decision_visibility_accepts_qdrant_storage_namespace_envelope(self):
        class NamespaceInjectingQdrant(_FakeQdrant):
            def upsert(
                self,
                namespace,
                point_id,
                vector,
                payload,
                idempotency_key,
            ):
                super().upsert(
                    namespace,
                    point_id,
                    vector,
                    {**payload, "namespace": namespace},
                    idempotency_key,
                )

        txn_id = "txn-storage-namespace"
        backend = VectorGraphMemoryBackend(
            "staged-storage-namespace",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=NamespaceInjectingQdrant(),
            neo4j_client=_FakeNeo4j(),
            decision_resolver={txn_id: "COMMITTED"}.get,
        )
        intents = [
            self._intent(
                txn_id,
                1,
                "memory_write",
                memory_id="shared",
                value="value",
            )
        ]

        backend.stage_transaction(txn_id, intents)

        self.assertEqual(backend.read_committed("shared")["value"], "value")
        self.assertEqual(
            [record["memory_id"] for record in backend.search_committed()],
            ["shared"],
        )
        self.assertEqual(backend.current_version("shared"), 1)

    def test_predecision_verification_rejects_a_physically_finalized_staged_row(self):
        txn_id = "txn-early-finalization"
        backend = VectorGraphMemoryBackend(
            "staged-early-finalization",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
        )
        intents = [
            self._intent(
                txn_id,
                1,
                "memory_write",
                memory_id="shared",
                value="value",
            )
        ]
        backend.stage_transaction(txn_id, intents)
        staged = next(
            point["payload"]
            for point in self.qdrant.points.values()
            if point["payload"].get("txn_id") == txn_id
        )
        staged["status"] = "active"

        self.assertEqual(
            backend.verify_transaction(txn_id, intents)["status"],
            "partial",
        )

    def test_decision_visibility_requires_the_exact_staged_edge_set(self):
        for mutation in ("missing", "extra"):
            with self.subTest(mutation=mutation):
                qdrant = _FakeQdrant()
                neo4j = _FakeNeo4j()
                txn_id = f"txn-edge-{mutation}"
                backend = VectorGraphMemoryBackend(
                    f"staged-edge-{mutation}",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                    decision_resolver={txn_id: "COMMITTED"}.get,
                )
                backend.write("source", value="source")
                intents = [
                    self._intent(
                        txn_id,
                        1,
                        "memory_derive",
                        memory_id="target",
                        source_ids=["source"],
                        value="derived",
                    )
                ]
                backend.stage_transaction(txn_id, intents)
                if mutation == "missing":
                    neo4j.staged_edges = [
                        edge
                        for edge in neo4j.staged_edges
                        if not (
                            edge["txn_id"] == txn_id
                            and edge["kind"] == "DERIVED_FROM"
                        )
                    ]
                else:
                    neo4j.staged_edges.append(
                        {
                            "txn_id": txn_id,
                            "kind": "SUPERSEDES",
                            "source_id": "target",
                            "target_id": "unexpected",
                            "status": "pending",
                        }
                    )

                self._assert_staged_fail_closed(backend, "target")

    def test_aborted_rewrite_does_not_report_the_old_committed_row_as_txn_visible(self):
        decisions = {"txn-rewrite": "ABORTED"}
        backend = VectorGraphMemoryBackend(
            "txn-rewrite",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        backend.write("shared", value="old")
        intents = [
            self._intent(
                "txn-rewrite",
                1,
                "memory_write",
                memory_id="shared",
                value="new",
            )
        ]

        backend.stage_transaction("txn-rewrite", intents)

        self.assertEqual(backend.read_committed("shared")["value"], "old")
        self.assertEqual(
            backend.raw_transaction_state("txn-rewrite", intents)["gateway_visible"],
            [],
        )

    def test_empty_transaction_stage_verifies_complete(self):
        phases = []

        receipt = self.backend.stage_transaction(
            "txn-empty", [], phase_hook=lambda phase, _evidence: phases.append(phase)
        )

        self.assertEqual(
            self.backend.verify_transaction("txn-empty", []),
            {"status": "complete", "txn_id": "txn-empty"},
        )
        self.assertEqual(
            phases,
            ["after_qdrant_stage", "after_neo4j_stage"],
        )
        self.assertEqual(receipt["qdrant"]["memory_ids"], [])

    def test_commit_decision_applies_overlay_visibility_before_idempotent_finalize(self):
        decisions = {}
        backend = VectorGraphMemoryBackend(
            "txn-overlay",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        backend.write("old", value="old-value")
        intents = [
            self._intent(
                "txn-overlay",
                1,
                "memory_supersede",
                old_memory_id="old",
                new_memory_id="new",
                value="new-value",
            ),
            self._intent(
                "txn-overlay",
                2,
                "status_overlay",
                memory_id="old",
                target_status="superseded",
            ),
        ]

        backend.stage_transaction("txn-overlay", intents)
        self.assertEqual(self.qdrant.retrieve("txn-overlay", "old")["status"], "active")
        self.assertEqual(backend.read_committed("old")["value"], "old-value")
        self.assertIsNone(backend.read_committed("new"))

        decisions["txn-overlay"] = "COMMITTED"
        self.assertIsNone(backend.read_committed("old"))
        self.assertEqual(backend.read_committed("new")["status"], "active")
        self.assertEqual(self.qdrant.retrieve("txn-overlay", "old")["status"], "active")

        first = backend.finalize_transaction("txn-overlay", intents)
        old_after_first = self.qdrant.retrieve("txn-overlay", "old")
        second = backend.finalize_transaction("txn-overlay", intents)
        old_after_second = self.qdrant.retrieve("txn-overlay", "old")

        self.assertEqual(first["status"], "complete")
        self.assertEqual(second["status"], "complete")
        self.assertEqual(old_after_first, old_after_second)
        self.assertEqual(old_after_second["status"], "superseded")
        self.assertEqual(old_after_second["version"], 2)
        raw = backend.raw_transaction_state("txn-overlay", intents)
        self.assertEqual(
            next(item for item in raw["qdrant"]["objects"] if item["record_kind"] == "memory")["status"],
            "active",
        )

    def test_committed_split_finalization_stays_logically_visible_and_retry_converges(self):
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        failure_armed = {"value": True}

        def phase_hook(phase, _evidence):
            if phase == "after_commit_decision" and failure_armed["value"]:
                neo4j.fail_upsert = True

        with tempfile.TemporaryDirectory() as directory:
            journal = TransactionJournal(Path(directory) / "split.sqlite3")
            self.addCleanup(journal.close)
            backend = VectorGraphMemoryBackend(
                "txn-split-finalize",
                "http://qdrant",
                "bolt://neo4j",
                ("neo4j", "password"),
                qdrant_client=qdrant,
                neo4j_client=neo4j,
                max_retries=0,
            )
            gateway = TaskTransactionGateway(
                journal=journal,
                backend=backend,
                task_id="task-split",
                agent_id="agent",
                txn_id="txn-split",
                policy_snapshot_provider=lambda: {
                    "version": 1,
                    "denied_actions": [],
                    "scope_overrides": {},
                },
                phase_hook=phase_hook,
            )
            gateway.call(
                "memory_write",
                {"memory_id": "shared", "value": "committed-value"},
            )

            with self.assertRaises(TaskTransactionError) as raised:
                gateway.commit()
            self.assertEqual(
                raised.exception.code, "commit_decided_response_lost"
            )
            self.assertEqual(journal.load("txn-split").state, "COMMITTED")
            self.assertNotIn(
                "finalize_complete",
                [phase["phase"] for phase in journal.phases("txn-split")],
            )

            split = backend.raw_transaction_state(
                "txn-split", journal.intents("txn-split")
            )
            self.assertEqual(split["qdrant"]["objects"][0]["status"], "active")
            self.assertEqual(split["neo4j"]["nodes"][0]["status"], "pending")
            self.assertEqual(
                backend.read_committed("shared")["value"], "committed-value"
            )
            self.assertEqual(
                [record["memory_id"] for record in backend.search_committed()],
                ["shared"],
            )
            self.assertEqual(backend.current_version("shared"), 1)

            successor_intents = [
                self._intent(
                    "txn-successor",
                    1,
                    "memory_write",
                    memory_id="shared",
                    value="successor-value",
                )
            ]
            neo4j.fail_upsert = False
            backend.stage_transaction("txn-successor", successor_intents)
            successor_raw = backend.raw_transaction_state(
                "txn-successor", successor_intents
            )
            self.assertEqual(
                (
                    successor_raw["qdrant"]["objects"][0]["base_version"],
                    successor_raw["qdrant"]["objects"][0]["version"],
                ),
                (1, 2),
            )
            self.assertEqual(
                backend.cleanup_transaction(
                    "txn-successor", successor_intents
                )["status"],
                "clean",
            )

            failure_armed["value"] = False
            neo4j.fail_upsert = False
            self.assertEqual(gateway.commit()["decision"], "COMMITTED")

            converged = backend.raw_transaction_state(
                "txn-split", journal.intents("txn-split")
            )
            self.assertEqual(converged["qdrant"]["objects"][0]["status"], "active")
            self.assertEqual(converged["neo4j"]["nodes"][0]["status"], "active")
            self.assertEqual(backend.current_version("shared"), 1)
            self.assertEqual(backend.read_committed("shared")["version"], 1)
            self.assertEqual(len(neo4j.cas_requests), 1)
            self.assertEqual(
                [phase["phase"] for phase in journal.phases("txn-split")].count(
                    "finalize_complete"
                ),
                1,
            )

    def test_newer_committed_overlay_hides_an_unfinalized_transaction_created_record(self):
        decisions = {"txn-create": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-overlay-order",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        created = [
            self._intent(
                "txn-create", 1, "memory_write", memory_id="created", value="value"
            )
        ]
        overlay = [
            self._intent(
                "txn-overlay",
                1,
                "status_overlay",
                memory_id="created",
                target_status="invalid",
            )
        ]
        backend.stage_transaction("txn-create", created)
        backend.stage_transaction("txn-overlay", overlay)

        self.assertEqual(backend.read_committed("created")["value"], "value")
        decisions["txn-overlay"] = "COMMITTED"

        self.assertIsNone(backend.read_committed("created"))
        self.assertEqual(backend.current_version("created"), 2)

    def test_equal_version_committed_record_conflicts_fail_closed(self):
        decisions = {}
        self.neo4j.enforce_claims = False
        backend = VectorGraphMemoryBackend(
            "txn-equal-visible",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        first = [
            self._intent("txn-a", 1, "memory_write", memory_id="shared", value="a")
        ]
        second = [
            self._intent("txn-b", 1, "memory_write", memory_id="shared", value="b")
        ]
        backend.stage_transaction("txn-a", first)
        backend.stage_transaction("txn-b", second)
        decisions.update({"txn-a": "COMMITTED", "txn-b": "COMMITTED"})

        self.assertIsNone(backend.read_committed("shared"))
        self.assertNotIn(
            "shared", [record["memory_id"] for record in backend.search_committed()]
        )
        with self.assertRaises(VectorGraphBackendError) as raised:
            backend.current_version("shared")
        self.assertEqual(raised.exception.code, "backend_state_unknown")

    def test_tampered_canonical_hash_authority_cannot_be_bypassed_by_staged_loser(self):
        decisions = {"txn-loser": "COMMITTED"}
        self.neo4j.enforce_claims = False
        backend = VectorGraphMemoryBackend(
            "txn-canonical-authority",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        loser = [
            self._intent(
                "txn-loser", 1, "memory_write", memory_id="shared", value="loser"
            )
        ]
        backend.stage_transaction("txn-loser", loser)
        backend.write("shared", value="winner", agent_id="agent_model")
        canonical = self.qdrant.points[
            ("txn-canonical-authority", "shared")
        ]["payload"]
        original_hash = canonical["_canonical_state_hash"]
        canonical["value"] = "loser"
        self.assertEqual(canonical["_canonical_state_hash"], original_hash)

        self.assertIsNone(backend.read_committed("shared"))
        self.assertNotIn(
            "shared", [record["memory_id"] for record in backend.search_committed()]
        )
        with self.assertRaises(VectorGraphBackendError) as raised:
            backend.current_version("shared")
        self.assertEqual(raised.exception.code, "backend_state_unknown")

    def test_qdrant_only_high_version_does_not_poison_verified_successor(self):
        decisions = {"txn-poison": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-qdrant-only-poison",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        backend.write("shared", value="verified")
        poison = [
            self._intent(
                "txn-poison", 1, "memory_write", memory_id="shared", value="poison"
            )
        ]
        backend.stage_transaction("txn-poison", poison)
        poison_payload = next(
            row["payload"]
            for (namespace, _point_id), row in self.qdrant.points.items()
            if namespace == "txn-qdrant-only-poison"
            and row["payload"].get("txn_id") == "txn-poison"
        )
        poison_payload["version"] = 99
        self.neo4j.staged_memories.clear()
        self.neo4j.staged_edges.clear()
        self.neo4j.claims.clear()

        self.assertEqual(backend.current_version("shared"), 1)
        self.assertEqual(backend.read_committed("shared")["value"], "verified")

        with tempfile.TemporaryDirectory() as directory:
            journal = TransactionJournal(Path(directory) / "successor.sqlite3")
            self.addCleanup(journal.close)
            gateway = TaskTransactionGateway(
                journal=journal,
                backend=backend,
                task_id="task-successor",
                agent_id="agent",
                txn_id="txn-successor",
                policy_snapshot_provider=lambda: {
                    "version": 1,
                    "denied_actions": [],
                    "scope_overrides": {},
                },
            )
            backend.bind_decision_resolver(
                lambda txn_id: (
                    "COMMITTED"
                    if txn_id == "txn-poison"
                    else journal.load(txn_id).decision
                )
            )
            gateway.call(
                "memory_write", {"memory_id": "shared", "value": "successor"}
            )
            self.assertEqual(gateway.commit()["decision"], "COMMITTED")

        self.assertEqual(backend.current_version("shared"), 2)
        self.assertEqual(backend.read_committed("shared")["value"], "successor")

    def test_unverified_qdrant_only_version_aborts_new_mutation_stably(self):
        decisions = {"txn-poison": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-qdrant-only-unknown",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        payload = {
            "txn_id": "txn-poison",
            "record_kind": "memory",
            "sequence": 1,
            "operation": "memory_write",
            "memory_id": "shared",
            "value": "poison",
            "payload_hash": backend._payload_hash("poison"),
            "status": "pending",
            "target_status": "active",
            "version": 99,
            "base_version": 0,
            "derived_from": [],
            "supersedes_id": None,
        }
        self.qdrant.upsert(
            "txn-qdrant-only-unknown",
            "txn:txn-poison:1:memory:shared",
            [0.0] * 32,
            payload,
            "poison",
        )
        with self.assertRaises(VectorGraphBackendError) as version_error:
            backend.current_version("shared")
        self.assertEqual(version_error.exception.code, "backend_state_unknown")

        with tempfile.TemporaryDirectory() as directory:
            journal = TransactionJournal(Path(directory) / "unknown.sqlite3")
            self.addCleanup(journal.close)
            gateway = TaskTransactionGateway(
                journal=journal,
                backend=backend,
                task_id="task-unknown",
                agent_id="agent",
                txn_id="txn-new",
                policy_snapshot_provider=lambda: {
                    "version": 1,
                    "denied_actions": [],
                    "scope_overrides": {},
                },
            )
            backend.bind_decision_resolver(
                lambda txn_id: (
                    "COMMITTED"
                    if txn_id == "txn-poison"
                    else journal.load(txn_id).decision
                )
            )

            with self.assertRaises(TaskTransactionError) as raised:
                gateway.call(
                    "memory_write", {"memory_id": "shared", "value": "new"}
                )
            self.assertEqual(raised.exception.code, "backend_state_unknown")
            self.assertEqual(journal.intents("txn-new"), [])

    def test_invalidate_committed_updates_status_and_version_in_both_stores(self):
        self.backend.write("source", value="source-value")

        result = self.backend.invalidate_committed("source")

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["version"], 2)
        self.assertEqual(self.qdrant.retrieve("episode-1", "source")["version"], 2)
        self.assertEqual(self.neo4j.memories[("episode-1", "source")]["version"], 2)
        self.assertIsNone(self.backend.read_committed("source"))

    def test_invalidate_committed_fails_before_projection_when_authority_is_unavailable(self):
        observed = []

        def requester(service, operation, function, key):
            observed.append((service, operation))
            return function()

        backend = VectorGraphMemoryBackend(
            "txn-invalidate-atomic",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            proxy_requester=requester,
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            max_retries=0,
        )
        backend.write("source", value="source-value")
        observed.clear()
        requests_before = backend.metrics()["request_count"]
        self.neo4j.fail_upsert = True

        with self.assertRaises(VectorGraphBackendError):
            backend.invalidate_committed("source")

        vector = self.qdrant.retrieve("txn-invalidate-atomic", "source")
        graph = self.neo4j.memories[("txn-invalidate-atomic", "source")]
        self.assertEqual((vector["status"], vector["version"]), ("active", 1))
        self.assertEqual((graph["status"], graph["version"]), ("active", 1))
        self.assertEqual(backend.read_committed("source")["value"], "source-value")
        self.assertEqual(
            backend.metrics()["request_count"] - requests_before,
            len(observed),
        )
        self.assertEqual(
            observed[:4],
            [
                ("qdrant", "invalidate_committed_canonical_read"),
                ("neo4j", "invalidate_committed_canonical_read"),
                ("qdrant", "invalidate_committed_rows"),
                ("neo4j", "invalidate_committed_canonical"),
            ],
        )
        self.assertNotIn(
            ("qdrant", "invalidate_committed_read"), observed
        )
        self.assertNotIn(
            ("qdrant", "invalidate_committed_owner_read"), observed
        )
        self.assertFalse(any("rollback" in operation for _, operation in observed))

    def test_invalidate_committed_handles_decision_visible_unfinalized_record(self):
        decisions = {"txn-create": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-invalidate-unfinalized",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        intents = [
            self._intent(
                "txn-create", 1, "memory_write", memory_id="source", value="value"
            )
        ]
        backend.stage_transaction("txn-create", intents)

        invalidated = backend.invalidate_committed("source")

        self.assertEqual((invalidated["status"], invalidated["version"]), ("invalid", 2))
        self.assertIsNone(backend.read_committed("source"))
        self.assertEqual(backend.current_version("source"), 2)
        raw = backend.raw_transaction_state("txn-create", intents)
        self.assertEqual(raw["qdrant"]["objects"][0]["status"], "pending")
        self.assertEqual(raw["qdrant"]["objects"][0]["version"], 1)

    def test_invalidate_committed_advances_a_finalized_transaction_record(self):
        decisions = {"txn-final": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-finalized",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        intents = [
            self._intent(
                "txn-final", 1, "memory_write", memory_id="final", value="value"
            )
        ]
        backend.stage_transaction("txn-final", intents)
        backend.finalize_transaction("txn-final", intents)

        result = backend.invalidate_committed("final")

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["version"], 2)
        self.assertIsNone(backend.read_committed("final"))
        raw = backend.raw_transaction_state("txn-final", intents)
        self.assertEqual(raw["qdrant"]["objects"][0]["status"], "active")
        self.assertEqual(raw["qdrant"]["objects"][0]["version"], 1)
        self.assertEqual(
            self.neo4j.memories[("txn-finalized", "final")]["status"],
            "invalid",
        )

    def test_invalidate_response_loss_is_forward_recoverable_and_never_exposes_stale_active(self):
        class LostResponseQdrant(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.armed = False
                self.readback_down = False

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                super().upsert(namespace, point_id, vector, payload, idempotency_key)
                if self.armed and payload.get("status") == "invalid":
                    self.armed = False
                    self.readback_down = True
                    raise ConnectionResetError("invalidate response lost")

            def retrieve(self, namespace, point_id):
                if self.readback_down:
                    raise TimeoutError("qdrant readback unavailable")
                return super().retrieve(namespace, point_id)

            def retrieve_many_by_memory(self, namespace, memory_id):
                if self.readback_down:
                    raise TimeoutError("qdrant readback unavailable")
                return super().retrieve_many_by_memory(namespace, memory_id)

        qdrant = LostResponseQdrant()
        neo4j = _FakeNeo4j()
        observed = []

        def requester(service, operation, function, key):
            observed.append((service, operation))
            return function()

        backend = VectorGraphMemoryBackend(
            "txn-forward-recovery",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            proxy_requester=requester,
            max_retries=0,
        )
        backend.write("source", value="value")
        observed.clear()
        qdrant.armed = True

        with self.assertRaises(VectorGraphBackendError):
            backend.invalidate_committed("source")

        self.assertEqual(
            (neo4j.memories[("txn-forward-recovery", "source")]["status"],
             neo4j.memories[("txn-forward-recovery", "source")]["version"]),
            ("invalid", 2),
        )
        self.assertNotIn("rollback", " ".join(operation for _, operation in observed))
        qdrant.readback_down = False
        self.assertIsNone(backend.read_committed("source"))

        recovered = backend.invalidate_committed("source")

        self.assertEqual((recovered["status"], recovered["version"]), ("invalid", 2))
        self.assertEqual(
            (qdrant.retrieve("txn-forward-recovery", "source")["status"],
             qdrant.retrieve("txn-forward-recovery", "source")["version"]),
            ("invalid", 2),
        )

    def test_invalidate_retry_completes_a_split_forward_transition(self):
        self.backend.write("source", value="value")
        vector = self.qdrant.points[("episode-1", "source")]["payload"]
        vector.update({"status": "invalid", "version": 2})

        recovered = self.backend.invalidate_committed("source")

        self.assertEqual((recovered["status"], recovered["version"]), ("invalid", 2))
        graph = self.neo4j.memories[("episode-1", "source")]
        self.assertEqual((graph["status"], graph["version"]), ("invalid", 2))

    def test_invalidate_retry_projects_an_authoritative_transition_after_qdrant_failure(self):
        self.backend.write("source", value="value")
        self.qdrant.fail_upsert = True

        with self.assertRaises(VectorGraphBackendError):
            self.backend.invalidate_committed("source")

        graph = self.neo4j.retrieve_memory("episode-1", "source")
        vector = self.qdrant.retrieve("episode-1", "source")
        self.assertEqual((graph["status"], graph["version"]), ("invalid", 2))
        self.assertEqual((vector["status"], vector["version"]), ("active", 1))
        self.qdrant.fail_upsert = False

        recovered = self.backend.invalidate_committed("source")

        self.assertEqual((recovered["status"], recovered["version"]), ("invalid", 2))
        vector = self.qdrant.retrieve("episode-1", "source")
        self.assertEqual((vector["status"], vector["version"]), ("invalid", 2))

    def test_invalidate_recovery_never_rolls_back_a_concurrent_newer_version(self):
        self.backend.write("source", value="value")
        self.qdrant.points[("episode-1", "source")]["payload"].update(
            {"status": "invalid", "version": 2}
        )
        self.neo4j.memories[("episode-1", "source")].update(
            {"status": "active", "version": 3}
        )

        with self.assertRaises(VectorGraphBackendError):
            self.backend.invalidate_committed("source")

        self.assertEqual(
            (self.neo4j.memories[("episode-1", "source")]["status"],
             self.neo4j.memories[("episode-1", "source")]["version"]),
            ("active", 3),
        )
        self.assertIsNone(self.backend.read_committed("source"))

    def test_conflicting_equal_version_finalizers_are_linearized_by_storage_cas(self):
        decisions = {}
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        neo4j.enforce_claims = False
        first_backend = VectorGraphMemoryBackend(
            "txn-concurrent",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        second_backend = VectorGraphMemoryBackend(
            "txn-concurrent",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        first = [self._intent("txn-a", 1, "memory_write", memory_id="shared", value="a")]
        second = [self._intent("txn-b", 1, "memory_write", memory_id="shared", value="b")]
        first_backend.stage_transaction("txn-a", first)
        second_backend.stage_transaction("txn-b", second)
        decisions.update({"txn-a": "COMMITTED", "txn-b": "COMMITTED"})
        neo4j.cas_barrier = threading.Barrier(2)
        outcomes = []

        def finalize(backend, txn_id, intents):
            try:
                outcomes.append(backend.finalize_transaction(txn_id, intents)["status"])
            except VectorGraphBackendError:
                outcomes.append("conflict")

        threads = [
            threading.Thread(target=finalize, args=(first_backend, "txn-a", first)),
            threading.Thread(target=finalize, args=(second_backend, "txn-b", second)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sorted(outcomes), ["complete", "conflict"])
        vector = qdrant.retrieve("txn-concurrent", "shared")
        graph = neo4j.retrieve_memory("txn-concurrent", "shared")
        self.assertEqual(vector["version"], graph["version"])
        self.assertEqual(vector["status"], graph["status"])

    def test_repeated_same_id_commit_recovers_after_decision_and_allows_successor(self):
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        fail_after_decision = {"enabled": True}

        def phase_hook(phase, _evidence):
            if phase == "after_commit_decision" and fail_after_decision["enabled"]:
                raise RuntimeError("simulated coordinator crash")

        policy = lambda: {
            "version": 1,
            "denied_actions": [],
            "scope_overrides": {},
        }
        with tempfile.TemporaryDirectory() as directory:
            first_journal = TransactionJournal(Path(directory) / "first.sqlite3")
            self.addCleanup(first_journal.close)
            backend = VectorGraphMemoryBackend(
                "txn-repeat-recovery",
                "http://qdrant",
                "bolt://neo4j",
                ("neo4j", "password"),
                qdrant_client=qdrant,
                neo4j_client=neo4j,
            )
            first = TaskTransactionGateway(
                journal=first_journal,
                backend=backend,
                task_id="task-first",
                agent_id="agent",
                txn_id="txn-first",
                policy_snapshot_provider=policy,
                phase_hook=phase_hook,
            )
            first.call("memory_write", {"memory_id": "shared", "value": "one"})
            first.call("memory_write", {"memory_id": "shared", "value": "two"})

            with self.assertRaises(TaskTransactionError) as raised:
                first.commit()
            self.assertEqual(raised.exception.code, "commit_decided_response_lost")
            self.assertEqual(first_journal.load("txn-first").state, "COMMITTED")

            fail_after_decision["enabled"] = False
            self.assertEqual(first.commit()["decision"], "COMMITTED")
            self.assertEqual(backend.read_committed("shared")["value"], "two")
            self.assertEqual(backend.read_committed("shared")["version"], 2)
            self.assertEqual(neo4j.claims, {})
            self.assertEqual(
                neo4j.cas_requests[-1],
                {
                    "memory_id": "shared",
                    "expected_version": 0,
                    "desired_version": 2,
                    "claim_txn_id": "txn-first",
                },
            )

            successor_journal = TransactionJournal(
                Path(directory) / "successor.sqlite3"
            )
            self.addCleanup(successor_journal.close)
            successor = TaskTransactionGateway(
                journal=successor_journal,
                backend=backend,
                task_id="task-successor",
                agent_id="agent",
                txn_id="txn-successor",
                policy_snapshot_provider=policy,
            )
            successor.call(
                "memory_write", {"memory_id": "shared", "value": "three"}
            )
            self.assertEqual(successor.commit()["decision"], "COMMITTED")
            self.assertEqual(backend.read_committed("shared")["version"], 3)
            self.assertEqual(backend.read_committed("shared")["value"], "three")
            self.assertEqual(neo4j.claims, {})

    def test_repeated_write_derive_supersede_uses_one_base_to_final_claim(self):
        decisions = {"txn-combined": "COMMITTED"}
        self.backend.bind_decision_resolver(decisions.get)
        self.backend.write("old", value="old-value")
        intents = [
            self._intent(
                "txn-combined", 1, "memory_write", memory_id="target", value="one"
            ),
            self._intent(
                "txn-combined",
                2,
                "memory_derive",
                memory_id="target",
                source_ids=["old"],
                value="two",
            ),
            self._intent(
                "txn-combined",
                3,
                "memory_supersede",
                old_memory_id="old",
                new_memory_id="target",
                value="three",
            ),
            self._intent(
                "txn-combined",
                4,
                "status_overlay",
                memory_id="old",
                target_status="superseded",
            ),
        ]

        self.backend.stage_transaction("txn-combined", intents)
        claims = {
            claim["memory_id"]: claim for claim in self.neo4j.claim_requests[-1]
        }
        self.assertEqual(
            (claims["target"]["base_version"], claims["target"]["final_version"]),
            (0, 3),
        )
        self.assertEqual(
            (claims["old"]["base_version"], claims["old"]["final_version"]),
            (1, 2),
        )

        self.assertEqual(
            self.backend.finalize_transaction("txn-combined", intents)["status"],
            "complete",
        )
        target = self.backend.read_committed("target")
        self.assertEqual((target["value"], target["version"]), ("three", 3))
        self.assertEqual(target["supersedes_id"], "old")
        self.assertIsNone(self.backend.read_committed("old"))
        self.assertEqual(self.backend.current_version("old"), 2)
        self.assertEqual(self.neo4j.claims, {})

    def test_repeated_write_interval_claim_blocks_an_overlapping_direct_cas(self):
        decisions = {"txn-reserved": "COMMITTED"}
        self.backend.bind_decision_resolver(decisions.get)
        intents = [
            self._intent(
                "txn-reserved", 1, "memory_write", memory_id="shared", value="one"
            ),
            self._intent(
                "txn-reserved", 2, "memory_write", memory_id="shared", value="two"
            ),
        ]
        self.backend.stage_transaction("txn-reserved", intents)

        with self.assertRaises(VectorGraphBackendError) as raised:
            self.backend.write("shared", value="overlap")
        self.assertEqual(raised.exception.code, "backend_commit_conflict")

        self.assertEqual(
            self.backend.finalize_transaction("txn-reserved", intents)["status"],
            "complete",
        )
        self.assertEqual(self.backend.read_committed("shared")["value"], "two")
        self.assertEqual(self.backend.current_version("shared"), 2)

    def test_repeated_write_abort_cleanup_releases_interval_for_successor(self):
        decisions = {"txn-aborted": "ABORTED", "txn-successor": "COMMITTED"}
        self.backend.bind_decision_resolver(decisions.get)
        aborted = [
            self._intent(
                "txn-aborted", 1, "memory_write", memory_id="shared", value="one"
            ),
            self._intent(
                "txn-aborted", 2, "memory_write", memory_id="shared", value="two"
            ),
        ]
        self.backend.stage_transaction("txn-aborted", aborted)
        self.assertTrue(self.neo4j.claims)

        self.assertEqual(
            self.backend.cleanup_transaction("txn-aborted", aborted)["status"],
            "clean",
        )
        self.assertEqual(self.neo4j.claims, {})

        successor = [
            self._intent(
                "txn-successor",
                1,
                "memory_write",
                memory_id="shared",
                value="successor",
            )
        ]
        self.backend.stage_transaction("txn-successor", successor)
        self.assertEqual(
            self.backend.finalize_transaction("txn-successor", successor)["status"],
            "complete",
        )
        self.assertEqual(self.backend.current_version("shared"), 1)
        self.assertEqual(self.backend.read_committed("shared")["value"], "successor")

    def test_coordinator_claims_new_identity_before_only_one_commit_decision(self):
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        prepare_barrier = threading.Barrier(2)
        outcomes = []

        with tempfile.TemporaryDirectory() as directory:
            def phase_hook(phase, _evidence):
                if phase == "after_prepare":
                    prepare_barrier.wait(timeout=5)

            def commit(index, value):
                journal = TransactionJournal(
                    Path(directory) / f"journal-{index}.sqlite3"
                )
                try:
                    backend = VectorGraphMemoryBackend(
                        "txn-claimed-new-id",
                        "http://qdrant",
                        "bolt://neo4j",
                        ("neo4j", "password"),
                        qdrant_client=qdrant,
                        neo4j_client=neo4j,
                    )
                    gateway = TaskTransactionGateway(
                        journal=journal,
                        backend=backend,
                        task_id=f"task-{index}",
                        agent_id="agent",
                        txn_id=f"txn-{index}",
                        policy_snapshot_provider=lambda: {
                            "version": 1,
                            "denied_actions": [],
                            "scope_overrides": {},
                        },
                        phase_hook=phase_hook,
                    )
                    gateway.call(
                        "memory_write", {"memory_id": "shared", "value": value}
                    )
                    outcomes.append(gateway.commit()["decision"])
                except TaskTransactionError as exc:
                    outcomes.append(exc.code)
                finally:
                    journal.close()

            threads = [
                threading.Thread(target=commit, args=(0, "a")),
                threading.Thread(target=commit, args=(1, "b")),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

            states = []
            for index in range(2):
                journal = TransactionJournal(
                    Path(directory) / f"journal-{index}.sqlite3"
                )
                try:
                    states.append(journal.load(f"txn-{index}").state)
                finally:
                    journal.close()
            states.sort()

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(states, ["ABORTED", "COMMITTED"])
        self.assertIn("COMMITTED", outcomes)
        canonical = qdrant.retrieve("txn-claimed-new-id", "shared")
        self.assertIn(canonical["value"], {"a", "b"})
        self.assertEqual(neo4j.retrieve_memory("txn-claimed-new-id", "shared")["version"], 1)

    def test_adversarial_dual_committed_rows_keep_exact_canonical_winner_visible(self):
        decisions = {}
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        neo4j.enforce_claims = False
        backend = VectorGraphMemoryBackend(
            "txn-dual-committed",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        first = [self._intent("txn-a", 1, "memory_write", memory_id="shared", value="winner")]
        second = [self._intent("txn-b", 1, "memory_write", memory_id="shared", value="loser")]
        backend.stage_transaction("txn-a", first)
        backend.stage_transaction("txn-b", second)
        decisions.update({"txn-a": "COMMITTED", "txn-b": "COMMITTED"})
        self.assertEqual(backend.finalize_transaction("txn-a", first)["status"], "complete")

        with self.assertRaisesRegex(VectorGraphBackendError, "version conflict") as raised:
            backend.finalize_transaction("txn-b", second)
        self.assertEqual(raised.exception.code, "backend_commit_conflict")

        self.assertEqual(backend.read_committed("shared")["value"], "winner")
        self.assertEqual(
            [row["value"] for row in backend.search_committed() if row["memory_id"] == "shared"],
            ["winner"],
        )

    def test_dual_committed_coordinator_loser_has_stable_commit_conflict(self):
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        neo4j.enforce_claims = False

        with tempfile.TemporaryDirectory() as directory:
            journals = [
                TransactionJournal(Path(directory) / f"journal-{index}.sqlite3")
                for index in range(2)
            ]
            self.addCleanup(journals[0].close)
            self.addCleanup(journals[1].close)
            gateways = []
            for index, value in enumerate(("winner-or-loser-a", "winner-or-loser-b")):
                backend = VectorGraphMemoryBackend(
                    "txn-dual-coordinator",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                )

                gateway = TaskTransactionGateway(
                    journal=journals[index],
                    backend=backend,
                    task_id=f"task-{index}",
                    agent_id="agent",
                    txn_id=f"txn-{index}",
                    policy_snapshot_provider=lambda: {
                        "version": 1,
                        "denied_actions": [],
                        "scope_overrides": {},
                    },
                )
                gateway.call(
                    "memory_write", {"memory_id": "shared", "value": value}
                )
                gateways.append(gateway)

            for index, gateway in enumerate(gateways):
                txn_id = f"txn-{index}"
                frozen = journals[index].freeze(txn_id)
                journals[index].prepare(txn_id)
                gateway.coordinator.backend.stage_transaction(
                    txn_id, frozen["intents"]
                )
                self.assertEqual(
                    gateway.coordinator.backend.verify_transaction(
                        txn_id, frozen["intents"]
                    )["status"],
                    "complete",
                )
                journals[index].decide(txn_id, "COMMITTED")

            self.assertEqual(gateways[0].commit()["decision"], "COMMITTED")
            with self.assertRaises(TaskTransactionError) as loser:
                gateways[1].commit()
            self.assertEqual(loser.exception.code, "commit_conflict")
            self.assertEqual(
                [journal.load(f"txn-{index}").state for index, journal in enumerate(journals)],
                ["COMMITTED", "COMMITTED"],
            )

            with self.assertRaises(TaskTransactionError) as retried:
                gateways[1].commit()
            self.assertEqual(retried.exception.code, "commit_conflict")

        visible = VectorGraphMemoryBackend(
            "txn-dual-coordinator",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
        ).read_committed("shared")
        self.assertIn(visible["value"], {"winner-or-loser-a", "winner-or-loser-b"})

    def test_conflicting_direct_writes_are_linearized_before_vector_projection(self):
        qdrant = _FakeQdrant()
        neo4j = _FakeNeo4j()
        first_backend = VectorGraphMemoryBackend(
            "direct-concurrent",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
        )
        second_backend = VectorGraphMemoryBackend(
            "direct-concurrent",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
        )
        neo4j.cas_barrier = threading.Barrier(2)
        outcomes = []

        def write(backend, value):
            try:
                outcomes.append(backend.write("shared", value=value)["value"])
            except VectorGraphBackendError:
                outcomes.append("conflict")

        threads = [
            threading.Thread(target=write, args=(first_backend, "a")),
            threading.Thread(target=write, args=(second_backend, "b")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(outcomes.count("conflict"), 1)
        vector = qdrant.retrieve("direct-concurrent", "shared")
        graph = neo4j.retrieve_memory("direct-concurrent", "shared")
        self.assertEqual(vector["version"], graph["version"])
        self.assertEqual(
            vector["_canonical_state_hash"], graph["state_hash"]
        )

    def test_direct_write_cannot_report_success_over_a_newer_canonical_version(self):
        self.backend.write("shared", value="first")
        self.backend.invalidate_committed("shared")
        restarted = VectorGraphMemoryBackend(
            "episode-1",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
        )

        with self.assertRaisesRegex(VectorGraphBackendError, "advanced"):
            restarted.write("shared", value="stale")

        vector = self.qdrant.retrieve("episode-1", "shared")
        graph = self.neo4j.retrieve_memory("episode-1", "shared")
        self.assertEqual((vector["status"], vector["version"]), ("invalid", 2))
        self.assertEqual((graph["status"], graph["version"]), ("invalid", 2))
        self.assertEqual(restarted.validated_events(), [])

    def test_delayed_older_projection_cannot_overwrite_a_newer_canonical_version(self):
        class DelayedQdrant(_FakeQdrant):
            def __init__(self):
                super().__init__()
                self.old_projection_started = threading.Event()
                self.release_old_projection = threading.Event()

            def upsert(self, namespace, point_id, vector, payload, idempotency_key):
                if (
                    point_id == "shared"
                    and payload.get("txn_id") is None
                    and payload.get("value") == "first"
                ):
                    self.old_projection_started.set()
                    self.release_old_projection.wait(timeout=5)
                return super().upsert(
                    namespace, point_id, vector, payload, idempotency_key
                )

        decisions = {"txn-a": "COMMITTED", "txn-b": "COMMITTED"}
        qdrant = DelayedQdrant()
        neo4j = _FakeNeo4j()
        first_backend = VectorGraphMemoryBackend(
            "txn-projection-order",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        second_backend = VectorGraphMemoryBackend(
            "txn-projection-order",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=qdrant,
            neo4j_client=neo4j,
            decision_resolver=decisions.get,
        )
        first = [self._intent("txn-a", 1, "memory_write", memory_id="shared", value="first")]
        second = [self._intent("txn-b", 1, "memory_write", memory_id="shared", value="second")]
        first_backend.stage_transaction("txn-a", first)
        second_backend.stage_transaction("txn-b", second)
        outcomes = []

        def finalize(backend, txn_id, intents):
            try:
                outcomes.append(backend.finalize_transaction(txn_id, intents)["status"])
            except VectorGraphBackendError:
                outcomes.append("recoverable")

        old_thread = threading.Thread(
            target=finalize, args=(first_backend, "txn-a", first)
        )
        old_thread.start()
        self.assertTrue(qdrant.old_projection_started.wait(timeout=5))
        neo4j.cas_started_event = threading.Event()
        new_thread = threading.Thread(
            target=finalize, args=(second_backend, "txn-b", second)
        )
        new_thread.start()
        self.assertTrue(neo4j.cas_started_event.wait(timeout=5))
        qdrant.release_old_projection.set()
        old_thread.join(timeout=5)
        new_thread.join(timeout=5)

        self.assertFalse(old_thread.is_alive() or new_thread.is_alive())
        vector = qdrant.retrieve("txn-projection-order", "shared")
        graph = neo4j.retrieve_memory("txn-projection-order", "shared")
        self.assertEqual((vector["value"], vector["version"]), ("second", 2))
        self.assertEqual(graph["version"], 2)
        self.assertEqual(second_backend.read_committed("shared")["value"], "second")

    def test_incomplete_overlay_finalize_remains_recoverable_without_finalize_phase(self):
        backend = VectorGraphMemoryBackend(
            "txn-overlay-recovery",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
        )
        backend.write("source", value="value")
        with tempfile.TemporaryDirectory() as directory:
            journal = TransactionJournal(Path(directory) / "journal.sqlite3")
            self.addCleanup(journal.close)

            def phase_hook(phase, _evidence):
                if phase == "after_commit_decision":
                    self.neo4j.memories.pop(("txn-overlay-recovery", "source"), None)

            gateway = TaskTransactionGateway(
                journal=journal,
                backend=backend,
                task_id="overlay-task",
                agent_id="agent",
                txn_id="txn-overlay",
                policy_snapshot_provider=lambda: {
                    "version": 1,
                    "denied_actions": [],
                    "scope_overrides": {},
                },
                phase_hook=phase_hook,
            )
            gateway.call("memory_invalidate", {"memory_id": "source"})

            with self.assertRaisesRegex(TaskTransactionError, "finalize"):
                gateway.commit()

            record = journal.load("txn-overlay")
            phases = [phase["phase"] for phase in journal.phases("txn-overlay")]

        self.assertEqual(record.decision, "COMMITTED")
        self.assertNotIn("finalize_complete", phases)
        self.assertEqual(
            self.qdrant.retrieve("txn-overlay-recovery", "source")["status"],
            "active",
        )

    def test_retrying_an_older_record_finalize_cannot_overwrite_a_newer_commit(self):
        decisions = {"txn-a": "COMMITTED", "txn-b": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-stale-record",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        first = [
            self._intent(
                "txn-a", 1, "memory_write", memory_id="shared", value="first"
            )
        ]
        second = [
            self._intent(
                "txn-b", 1, "memory_write", memory_id="shared", value="second"
            )
        ]
        backend.stage_transaction("txn-a", first)
        backend.finalize_transaction("txn-a", first)
        backend.stage_transaction("txn-b", second)
        backend.finalize_transaction("txn-b", second)

        backend.finalize_transaction("txn-a", first)

        self.assertEqual(backend.read_committed("shared")["value"], "second")
        self.assertEqual(
            self.qdrant.retrieve("txn-stale-record", "shared")["version"], 2
        )
        self.assertEqual(
            self.neo4j.memories[("txn-stale-record", "shared")]["version"], 2
        )

    def test_retrying_an_older_overlay_finalize_cannot_reverse_a_newer_overlay(self):
        decisions = {"txn-a": "COMMITTED", "txn-b": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-stale-overlay",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        backend.write("shared", value="value")
        first = [
            self._intent(
                "txn-a",
                1,
                "status_overlay",
                memory_id="shared",
                target_status="superseded",
            )
        ]
        second = [
            self._intent(
                "txn-b",
                1,
                "status_overlay",
                memory_id="shared",
                target_status="invalid",
            )
        ]
        backend.stage_transaction("txn-a", first)
        backend.finalize_transaction("txn-a", first)
        backend.stage_transaction("txn-b", second)
        backend.finalize_transaction("txn-b", second)

        stale_retry = backend.finalize_transaction("txn-a", first)

        canonical = self.qdrant.retrieve("txn-stale-overlay", "shared")
        self.assertIn(stale_retry["status"], {"partial", "conflict"})
        self.assertEqual((canonical["status"], canonical["version"]), ("invalid", 3))
        graph = self.neo4j.memories[("txn-stale-overlay", "shared")]
        self.assertEqual((graph["status"], graph["version"]), ("invalid", 3))

    def test_equal_version_finalize_conflict_fails_closed_without_overwrite(self):
        decisions = {"txn-a": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-equal-conflict",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        intents = [
            self._intent(
                "txn-a", 1, "memory_write", memory_id="shared", value="expected"
            )
        ]
        backend.stage_transaction("txn-a", intents)
        backend.finalize_transaction("txn-a", intents)
        self.qdrant.points[("txn-equal-conflict", "shared")]["payload"][
            "value"
        ] = "conflicting"

        with self.assertRaisesRegex(VectorGraphBackendError, "version conflict"):
            backend.finalize_transaction("txn-a", intents)

        self.assertEqual(
            self.qdrant.retrieve("txn-equal-conflict", "shared")["value"],
            "conflicting",
        )

    def test_overlay_finalize_rejects_tampered_or_incomplete_qdrant_canonical_payload(self):
        for mutation in (
            "tampered-value",
            "tampered-provenance",
            "tampered-status",
            "tampered-version",
            "missing-scope",
        ):
            with self.subTest(mutation=mutation):
                qdrant = _FakeQdrant()
                neo4j = _FakeNeo4j()
                backend = VectorGraphMemoryBackend(
                    f"canonical-integrity-{mutation}",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                    decision_resolver=lambda txn_id: (
                        "COMMITTED" if txn_id == "txn-overlay" else None
                    ),
                )
                backend.write("shared", value="original")
                intents = [
                    self._intent(
                        "txn-overlay",
                        1,
                        "status_overlay",
                        memory_id="shared",
                        target_status="invalid",
                    )
                ]
                backend.stage_transaction("txn-overlay", intents)
                payload = qdrant.points[
                    (f"canonical-integrity-{mutation}", "shared")
                ]["payload"]
                if mutation == "tampered-value":
                    payload["value"] = "attacker-controlled"
                elif mutation == "tampered-provenance":
                    payload["derived_from"] = ["attacker-source"]
                elif mutation == "tampered-status":
                    payload["status"] = "superseded"
                elif mutation == "tampered-version":
                    payload["version"] = 9
                else:
                    payload.pop("scope")

                result = backend.finalize_transaction("txn-overlay", intents)

                self.assertIn(result["status"], {"partial", "unknown"})
                graph = neo4j.memories[
                    (f"canonical-integrity-{mutation}", "shared")
                ]
                self.assertEqual((graph["status"], graph["version"]), ("active", 1))
                self.assertEqual(graph["value"], "original")

    def test_exact_verification_covers_objects_nodes_and_direct_edges_without_values(self):
        decisions = {"txn-all": "COMMITTED"}
        backend = VectorGraphMemoryBackend(
            "txn-exact",
            "http://qdrant",
            "bolt://neo4j",
            ("neo4j", "password"),
            qdrant_client=self.qdrant,
            neo4j_client=self.neo4j,
            decision_resolver=decisions.get,
        )
        backend.write("old", value="old-secret")
        intents = [
            self._intent("txn-all", 1, "memory_write", memory_id="written", value="write-secret"),
            self._intent(
                "txn-all", 2, "memory_derive", memory_id="derived", source_ids=["written"], value="derive-secret"
            ),
            self._intent(
                "txn-all", 3, "memory_propagate", memory_id="propagated", source_ids=["derived"], value="propagate-secret"
            ),
            self._intent(
                "txn-all", 4, "memory_supersede", old_memory_id="old", new_memory_id="new", value="new-secret"
            ),
            self._intent(
                "txn-all", 5, "status_overlay", memory_id="old", target_status="superseded"
            ),
        ]

        receipt = backend.stage_transaction("txn-all", intents)
        verification = backend.verify_transaction("txn-all", intents)
        raw = backend.raw_transaction_state("txn-all", intents)

        self.assertEqual(verification, {"status": "complete", "txn_id": "txn-all"})
        self.assertEqual(receipt["qdrant"]["memory_ids"], ["derived", "new", "propagated", "written"])
        self.assertEqual(
            raw["neo4j"]["edges"],
            [
                {"kind": "DERIVED_FROM", "source_id": "derived", "status": "pending", "target_id": "propagated", "txn_id": "txn-all"},
                {"kind": "DERIVED_FROM", "source_id": "written", "status": "pending", "target_id": "derived", "txn_id": "txn-all"},
                {"kind": "SUPERSEDES", "source_id": "new", "status": "pending", "target_id": "old", "txn_id": "txn-all"},
            ],
        )
        serialized = json.dumps(raw, sort_keys=True)
        for secret in ("old-secret", "write-secret", "derive-secret", "propagate-secret", "new-secret"):
            self.assertNotIn(secret, serialized)
        self.assertTrue(all(len(item["payload_hash"]) == 64 for item in raw["qdrant"]["objects"] if item["record_kind"] == "memory"))

        expected_first_key = hashlib.sha256(
            json.dumps(["txn-exact", "txn-all", 1, "memory_write"], separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self.assertEqual(self.qdrant.transaction_keys[0], expected_first_key)
        self.assertEqual(self.neo4j.transaction_keys[0], expected_first_key)

    def test_raw_transaction_state_redacts_caller_controlled_identity_and_scope(self):
        intents = [
            self._intent(
                "txn-redacted",
                1,
                "memory_write",
                memory_id="memory",
                value="SECRET_VALUE",
                agent_id="SECRET_AGENT_ID",
                scope="SECRET_SCOPE",
            )
        ]
        self.backend.stage_transaction("txn-redacted", intents)

        raw = self.backend.raw_transaction_state("txn-redacted", intents)

        serialized = json.dumps(raw, sort_keys=True)
        self.assertNotIn("SECRET_VALUE", serialized)
        self.assertNotIn("SECRET_AGENT_ID", serialized)
        self.assertNotIn("SECRET_SCOPE", serialized)
        self.assertEqual(raw["qdrant"]["objects"][0]["memory_id"], "memory")
        self.assertEqual(len(raw["qdrant"]["objects"][0]["payload_hash"]), 64)

    def test_duplicate_transaction_sources_match_neo4j_merge_semantics(self):
        intents = [
            self._intent(
                "txn-duplicate-sources",
                1,
                "memory_derive",
                memory_id="derived",
                source_ids=["source", "source"],
                value="value",
            )
        ]

        self.backend.stage_transaction("txn-duplicate-sources", intents)

        self.assertEqual(
            self.backend.verify_transaction("txn-duplicate-sources", intents)[
                "status"
            ],
            "complete",
        )
        raw = self.backend.raw_transaction_state("txn-duplicate-sources", intents)
        self.assertEqual(len(raw["neo4j"]["edges"]), 1)

    def test_verification_distinguishes_partial_absent_and_unknown_readback(self):
        intents = [
            self._intent("txn-check", 1, "memory_write", memory_id="m", value="value")
        ]
        self.assertEqual(
            self.backend.verify_transaction("txn-check", intents)["status"],
            "absent",
        )
        self.backend.stage_transaction("txn-check", intents)
        self.neo4j.staged_memories.clear()
        self.assertEqual(
            self.backend.verify_transaction("txn-check", intents)["status"],
            "partial",
        )
        self.neo4j.fail_readback = True
        self.assertEqual(
            self.backend.verify_transaction("txn-check", intents)["status"],
            "unknown",
        )

    def test_verification_returns_unknown_when_expected_version_readback_fails(self):
        intents = [
            self._intent(
                "txn-unreadable", 1, "memory_write", memory_id="m", value="value"
            )
        ]
        self.qdrant.fail_readback = True

        self.assertEqual(
            self.backend.verify_transaction("txn-unreadable", intents),
            {"status": "unknown", "txn_id": "txn-unreadable"},
        )

    def test_verification_recomputes_qdrant_hash_from_the_stored_value(self):
        intents = [
            self._intent(
                "txn-tampered", 1, "memory_write", memory_id="m", value="original"
            )
        ]
        self.backend.stage_transaction("txn-tampered", intents)
        staged = next(
            row
            for row in self.qdrant.points.values()
            if row["payload"].get("txn_id") == "txn-tampered"
        )
        staged["payload"]["value"] = "tampered"

        self.assertEqual(
            self.backend.verify_transaction("txn-tampered", intents)["status"],
            "partial",
        )

    def test_operation_after_response_loss_is_verified_complete_without_cleanup(self):
        intents = [
            self._intent("txn-loss", 1, "memory_write", memory_id="m", value="value")
        ]
        for service in ("qdrant", "neo4j"):
            with self.subTest(service=service):
                qdrant = _FakeQdrant()
                neo4j = _FakeNeo4j()
                setattr(qdrant if service == "qdrant" else neo4j, "fail_upsert_after", True)
                backend = VectorGraphMemoryBackend(
                    f"txn-loss-{service}",
                    "http://qdrant",
                    "bolt://neo4j",
                    ("neo4j", "password"),
                    qdrant_client=qdrant,
                    neo4j_client=neo4j,
                    max_retries=0,
                )

                backend.stage_transaction("txn-loss", intents)

                self.assertEqual(backend.verify_transaction("txn-loss", intents)["status"], "complete")
                self.assertEqual(qdrant.cleanup_count, 0)
                self.assertEqual(neo4j.cleanup_count, 0)

    def test_cleanup_uses_readback_for_success_partial_and_unknown(self):
        intents = [
            self._intent("txn-clean", 1, "memory_write", memory_id="m", value="value")
        ]
        self.backend.stage_transaction("txn-clean", intents)
        self.qdrant.fail_cleanup_after = True
        self.neo4j.fail_cleanup_after = True

        self.assertEqual(
            self.backend.cleanup_transaction("txn-clean", intents)["status"],
            "clean",
        )
        self.assertEqual(
            self.backend.cleanup_transaction("txn-clean", intents)["status"],
            "clean",
        )

        self.qdrant.fail_cleanup_after = False
        self.neo4j.fail_cleanup_after = False
        self.backend.stage_transaction("txn-clean", intents)
        self.neo4j.fail_cleanup = True
        self.assertEqual(
            self.backend.cleanup_transaction("txn-clean", intents)["status"],
            "partial",
        )
        self.neo4j.fail_readback = True
        self.assertEqual(
            self.backend.cleanup_transaction("txn-clean", intents)["status"],
            "unknown",
        )


if __name__ == "__main__":
    unittest.main()
