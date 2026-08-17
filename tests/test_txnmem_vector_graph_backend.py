"""Contract tests for the Qdrant/Neo4j memory backend seam."""

from __future__ import annotations

import sys
import unittest
import hashlib
import json
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
            for source_id in source_ids:
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

    def test_commit_then_client_exception_compensates_and_verifies_both_stores_absent(self):
        original = self.neo4j.upsert_memory

        def commit_then_raise(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionResetError("response lost after commit")

        self.neo4j.upsert_memory = commit_then_raise

        with self.assertRaises(ConnectionResetError):
            self.backend.write("m-ambiguous", value="source")

        verification = self.backend.verify_persistent_state(
            [{"type": "write", "memory_id": "m-ambiguous", "value": "source"}]
        )
        self.assertEqual(verification["classification"], "absent")
        self.assertEqual(
            verification["items"],
            [
                {
                    "memory_id": "m-ambiguous",
                    "classification": "absent",
                    "qdrant": {"read_ok": True, "present": False, "matches": False},
                    "neo4j": {"read_ok": True, "present": False, "matches": False},
                }
            ],
        )

    def test_ambiguous_commit_recovery_fails_closed_when_absence_cannot_be_read(self):
        original = self.neo4j.upsert_memory

        def commit_then_raise(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionResetError("response lost after commit")

        def unreadable_after_compensation(*_args, **_kwargs):
            raise TimeoutError("graph read unavailable")

        self.neo4j.upsert_memory = commit_then_raise
        self.neo4j.retrieve_memory = unreadable_after_compensation

        with self.assertRaisesRegex(
            VectorGraphBackendError,
            "persistent state is unknown after compensation",
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
            [("qdrant", "write"), ("neo4j", "commit")],
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

    def test_invalidate_committed_updates_status_and_version_in_both_stores(self):
        self.backend.write("source", value="source-value")

        result = self.backend.invalidate_committed("source")

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["version"], 2)
        self.assertEqual(self.qdrant.retrieve("episode-1", "source")["version"], 2)
        self.assertEqual(self.neo4j.memories[("episode-1", "source")]["version"], 2)
        self.assertIsNone(self.backend.read_committed("source"))

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
