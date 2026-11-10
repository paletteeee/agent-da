from __future__ import annotations

import tempfile
import unittest
import json
import sqlite3
from pathlib import Path

from txnmem_backend import InstrumentedMemoryBackend, SQLiteInstrumentedMemoryBackend


class InstrumentedMemoryBackendVersionTests(unittest.TestCase):
    def test_legacy_records_default_to_version_one_without_losing_fields(self) -> None:
        backend = InstrumentedMemoryBackend(
            {
                "legacy": {
                    "memory_id": "legacy",
                    "value": "kept",
                    "status": "active",
                    "scope": "tenant:user_001",
                    "custom": "preserved",
                }
            }
        )

        self.assertEqual(
            backend.snapshot()["legacy"],
            {
                "memory_id": "legacy",
                "value": "kept",
                "status": "active",
                "scope": "tenant:user_001",
                "custom": "preserved",
                "version": 1,
            },
        )

    def test_write_starts_at_one_and_rewrite_increments_version(self) -> None:
        backend = InstrumentedMemoryBackend()

        first = backend.write("memory_a", value="first")
        second = backend.write("memory_a", value="second")

        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        self.assertEqual(backend.snapshot()["memory_a"]["version"], 2)
        self.assertEqual(
            [event["kind"] for event in backend.validated_events()],
            ["memory_write", "memory_write"],
        )

    def test_supersede_and_invalidate_increment_changed_committed_records(self) -> None:
        backend = InstrumentedMemoryBackend(
            {
                "old": {
                    "memory_id": "old",
                    "value": "old",
                    "status": "active",
                    "version": 8,
                }
            }
        )

        new = backend.supersede("old", "new", value="new")
        backend.invalidate("new")

        snapshot = backend.snapshot()
        self.assertEqual(snapshot["old"]["status"], "superseded")
        self.assertEqual(snapshot["old"]["version"], 9)
        self.assertEqual(new["version"], 1)
        self.assertEqual(snapshot["new"]["status"], "invalid")
        self.assertEqual(snapshot["new"]["version"], 2)


class SQLiteInstrumentedMemoryBackendVersionTests(unittest.TestCase):
    def test_persisted_legacy_record_without_version_defaults_to_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                "CREATE TABLE memories (memory_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO memories(memory_id, payload, status) VALUES (?, ?, ?)",
                (
                    "legacy",
                    json.dumps(
                        {"memory_id": "legacy", "value": "kept", "status": "active"}
                    ),
                    "active",
                ),
            )
            connection.commit()
            connection.close()

            backend = SQLiteInstrumentedMemoryBackend(path)
            self.addCleanup(backend.close)

            self.assertEqual(backend.snapshot()["legacy"]["version"], 1)

    def test_versions_survive_reopen_and_direct_snapshot_shape_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "memory.sqlite3"
            backend = SQLiteInstrumentedMemoryBackend(path)
            backend.write("memory_a", value="first", attribute="fact")
            backend.write("memory_a", value="second", attribute="fact")
            before = backend.snapshot()
            backend.close()

            reopened = SQLiteInstrumentedMemoryBackend(path)
            self.addCleanup(reopened.close)
            after = reopened.snapshot()

        self.assertEqual(before, after)
        self.assertEqual(after["memory_a"]["version"], 2)
        self.assertEqual(after["memory_a"]["derived_from"], [])
        self.assertEqual(after["memory_a"]["scope"], "tenant:user_001")


if __name__ == "__main__":
    unittest.main()
