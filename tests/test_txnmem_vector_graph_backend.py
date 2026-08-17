"""Contract tests for the Qdrant/Neo4j memory backend seam."""

from __future__ import annotations

import sys
import unittest
import copy
import hashlib
import json
import threading
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_vector_graph_backend import (
    VectorGraphBackendError,
    VectorGraphMemoryBackend,
    _Neo4jBoltClient,
    _QdrantHTTPClient,
    _qdrant_point_id,
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
        return {"available": True, "version": "fake-qdrant"}


class _FakeNeo4j:
    def __init__(self):
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
        self._cas_lock = threading.Lock()

    def claim_transaction_writes(
        self, namespace, txn_id, claims, idempotency_key
    ):
        if self.claim_barrier is not None:
            self.claim_barrier.wait(timeout=5)
        with self._cas_lock:
            conflicts = []
            for claim in claims:
                key = (
                    namespace,
                    str(claim["memory_id"]),
                    int(claim["target_version"]),
                )
                owner = self.claims.get(key)
                if self.enforce_claims and owner not in {None, txn_id}:
                    conflicts.append(str(claim["memory_id"]))
                    continue
                if self.enforce_claims:
                    current = self.retrieve_memory(
                        namespace, str(claim["memory_id"])
                    )
                    canonical_version = (
                        int(current.get("version", 1))
                        if current is not None
                        else 0
                    )
                    expected_version = int(claim["expected_version"])
                    chained = all(
                        (
                            namespace,
                            str(claim["memory_id"]),
                            version,
                        )
                        in self.claims
                        for version in range(
                            canonical_version + 1, expected_version + 1
                        )
                    )
                    if canonical_version > expected_version or (
                        canonical_version < expected_version and not chained
                    ):
                        conflicts.append(str(claim["memory_id"]))
            if conflicts:
                return {"status": "conflict", "memory_ids": sorted(conflicts)}
            if self.enforce_claims:
                for claim in claims:
                    self.claims[
                        (
                            namespace,
                            str(claim["memory_id"]),
                            int(claim["target_version"]),
                        )
                    ] = txn_id
            return {"status": "claimed", "memory_ids": []}

    def release_transaction_claims(
        self, namespace, txn_id, idempotency_key
    ):
        with self._cas_lock:
            self.claims = {
                key: owner
                for key, owner in self.claims.items()
                if not (key[0] == namespace and owner == txn_id)
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
        if self.cas_barrier is not None:
            self.cas_barrier.wait(timeout=5)
        if self.cas_started_event is not None:
            self.cas_started_event.set()
        with self._cas_lock:
            if self.fail_upsert:
                raise RuntimeError("neo4j unavailable")
            desired_version = int(payload.get("version", 1))
            claim_key = (namespace, memory_id, desired_version)
            claim_owner = self.claims.get(claim_key)
            if claim_owner is not None and claim_owner != claim_txn_id:
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
        return {"available": True, "version": "fake-neo4j"}


class VectorGraphMemoryBackendTests(unittest.TestCase):
    @staticmethod
    def _intent(txn_id, sequence, tool_name, **arguments):
        return {
            "txn_id": txn_id,
            "sequence": sequence,
            "tool_name": tool_name,
            "arguments": arguments,
        }

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

    def test_neo4j_legacy_identity_migration_is_ordered_and_idempotent(self):
        nodes = {
            "canonical": {
                "labels": {"Memory", "MemoryReference"},
                "namespace": "tenant",
                "memory_id": "shared",
            },
            "duplicate": {
                "labels": {"MemoryReference"},
                "namespace": "tenant",
                "memory_id": "shared",
            },
            "source": {
                "labels": {"MemoryReference"},
                "namespace": "tenant",
                "memory_id": "source",
            },
            "target": {
                "labels": {"Memory"},
                "namespace": "tenant",
                "memory_id": "target",
            },
        }
        edges = {
            ("DERIVED_FROM", "target", "duplicate"),
            ("SUPERSEDES", "duplicate", "source"),
        }
        constraints = []

        class Result:
            def consume(self):
                return None

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            @staticmethod
            def _canonical_for(node_key):
                node = nodes[node_key]
                return next(
                    (
                        key
                        for key, candidate in nodes.items()
                        if key != node_key
                        and "Memory" in candidate["labels"]
                        and candidate["namespace"] == node["namespace"]
                        and candidate["memory_id"] == node["memory_id"]
                    ),
                    None,
                )

            def run(self, query, **_parameters):
                if query.startswith("MATCH (m:Memory) SET m:MemoryIdentity"):
                    for node in nodes.values():
                        if "Memory" in node["labels"]:
                            node["labels"].add("MemoryIdentity")
                elif "(legacy:MemoryReference)-[old:DERIVED_FROM]->(target)" in query:
                    for kind, source, target in list(edges):
                        canonical = self._canonical_for(source)
                        if kind == "DERIVED_FROM" and canonical is not None:
                            edges.remove((kind, source, target))
                            edges.add((kind, canonical, target))
                elif "(source)-[old:DERIVED_FROM]->(legacy:MemoryReference)" in query:
                    for kind, source, target in list(edges):
                        canonical = self._canonical_for(target)
                        if kind == "DERIVED_FROM" and canonical is not None:
                            edges.remove((kind, source, target))
                            edges.add((kind, source, canonical))
                elif "(legacy:MemoryReference)-[old:SUPERSEDES]->(target)" in query:
                    for kind, source, target in list(edges):
                        canonical = self._canonical_for(source)
                        if kind == "SUPERSEDES" and canonical is not None:
                            edges.remove((kind, source, target))
                            edges.add((kind, canonical, target))
                elif "(source)-[old:SUPERSEDES]->(legacy:MemoryReference)" in query:
                    for kind, source, target in list(edges):
                        canonical = self._canonical_for(target)
                        if kind == "SUPERSEDES" and canonical is not None:
                            edges.remove((kind, source, target))
                            edges.add((kind, source, canonical))
                elif "DETACH DELETE legacy" in query:
                    for key in list(nodes):
                        if (
                            "MemoryReference" in nodes[key]["labels"]
                            and self._canonical_for(key) is not None
                        ):
                            nodes.pop(key)
                            edges.difference_update(
                                edge for edge in set(edges) if key in edge[1:]
                            )
                elif query.startswith("MATCH (r:MemoryReference) SET r:MemoryIdentity"):
                    for node in nodes.values():
                        if "MemoryReference" in node["labels"]:
                            node["labels"].add("MemoryIdentity")
                            node["labels"].discard("MemoryReference")
                elif query.startswith("CREATE CONSTRAINT memory_identity_unique"):
                    identities = [
                        (node["namespace"], node["memory_id"])
                        for node in nodes.values()
                        if "MemoryIdentity" in node["labels"]
                    ]
                    if len(identities) != len(set(identities)):
                        raise RuntimeError("constraint created before deduplication")
                    constraints.append("memory_identity_unique")
                return Result()

        class Driver:
            def session(self):
                return Session()

        client = _Neo4jBoltClient.__new__(_Neo4jBoltClient)
        client.driver = Driver()

        client._initialize_schema()
        first_state = (copy.deepcopy(nodes), set(edges))
        client._initialize_schema()

        identities = sorted(
            (node["namespace"], node["memory_id"])
            for node in nodes.values()
            if "MemoryIdentity" in node["labels"]
        )
        self.assertEqual(
            identities,
            [("tenant", "shared"), ("tenant", "source"), ("tenant", "target")],
        )
        self.assertEqual(
            edges,
            {
                ("DERIVED_FROM", "target", "canonical"),
                ("SUPERSEDES", "canonical", "source"),
            },
        )
        self.assertEqual((nodes, edges), first_state)
        self.assertEqual(constraints, ["memory_identity_unique", "memory_identity_unique"])

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
            [(service, operation) for service, operation, _ in observed[-2:]],
            [("qdrant", "retrieve"), ("qdrant", "search")],
        )
        self.assertEqual(
            backend.metrics()["request_count"],
            requests_after_write + 2,
        )

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
            observed[:3],
            [
                ("qdrant", "invalidate_committed_canonical_read"),
                ("neo4j", "invalidate_committed_canonical_read"),
                ("qdrant", "invalidate_committed_read"),
            ],
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

        with self.assertRaisesRegex(VectorGraphBackendError, "version conflict"):
            backend.finalize_transaction("txn-b", second)

        self.assertEqual(backend.read_committed("shared")["value"], "winner")
        self.assertEqual(
            [row["value"] for row in backend.search_committed() if row["memory_id"] == "shared"],
            ["winner"],
        )

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
