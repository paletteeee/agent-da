"""Contract tests for the Qdrant/Neo4j memory backend seam."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_vector_graph_backend import (
    VectorGraphBackendError,
    VectorGraphMemoryBackend,
    _Neo4jBoltClient,
    _qdrant_point_id,
)


class _FakeQdrant:
    def __init__(self):
        self.points = {}
        self.upsert_count = 0
        self.fail_upsert = False

    def upsert(self, namespace, point_id, vector, payload, idempotency_key):
        self.upsert_count += 1
        if self.fail_upsert:
            raise RuntimeError("qdrant unavailable")
        self.points[(namespace, point_id)] = {"vector": vector, "payload": dict(payload)}

    def retrieve(self, namespace, point_id):
        row = self.points.get((namespace, point_id))
        return None if row is None else dict(row["payload"])

    def search(self, namespace, vector, limit):
        return [dict(row["payload"]) for (space, _), row in self.points.items() if space == namespace][:limit]

    def delete(self, namespace, point_id, idempotency_key):
        self.points.pop((namespace, point_id), None)

    def healthcheck(self):
        return {"available": True, "version": "fake-qdrant"}


class _FakeNeo4j:
    def __init__(self):
        self.memories = {}
        self.edges = []
        self.fail_upsert = False

    def upsert_memory(self, namespace, memory_id, payload, source_ids, supersedes_id, idempotency_key):
        if self.fail_upsert:
            raise RuntimeError("neo4j unavailable")
        self.memories[(namespace, memory_id)] = dict(payload)
        for source_id in source_ids:
            self.edges.append((namespace, source_id, memory_id, "DERIVED_FROM"))
        if supersedes_id:
            self.edges.append((namespace, memory_id, supersedes_id, "SUPERSEDES"))

    def update_status(self, namespace, memory_id, status, idempotency_key):
        if (namespace, memory_id) in self.memories:
            self.memories[(namespace, memory_id)]["status"] = status

    def delete_memory(self, namespace, memory_id, idempotency_key):
        self.memories.pop((namespace, memory_id), None)

    def retrieve_memory(self, namespace, memory_id):
        row = self.memories.get((namespace, memory_id))
        if row is None:
            return None
        return {
            "status": row.get("status"),
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

    def healthcheck(self):
        return {"available": True, "version": "fake-neo4j"}


class VectorGraphMemoryBackendTests(unittest.TestCase):
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

    def test_qdrant_point_id_is_stable_uuid_for_arbitrary_memory_ids(self):
        first = _qdrant_point_id("tenant", "real_a")
        self.assertEqual(first, _qdrant_point_id("tenant", "real_a"))
        self.assertNotEqual(first, _qdrant_point_id("tenant", "real_b"))
        self.assertRegex(first, r"^[0-9a-f-]{36}$")

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


if __name__ == "__main__":
    unittest.main()
