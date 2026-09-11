import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from txnmem_backend import (
    InstrumentedMemoryBackend,
    SQLiteInstrumentedMemoryBackend,
)


class BackendDeleteTests(unittest.TestCase):
    def test_instrumented_delete_removes_memory(self):
        backend = InstrumentedMemoryBackend()
        backend.write("m1", value="one")

        backend.delete("m1", agent_id="agent_1")

        self.assertNotIn("m1", backend.snapshot())

    def test_sqlite_delete_is_persistent(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "memory.sqlite"
            with SQLiteInstrumentedMemoryBackend(path) as backend:
                backend.write("m1", value="one")
                backend.delete("m1", agent_id="agent_1")
            with SQLiteInstrumentedMemoryBackend(path) as reopened:
                self.assertIsNone(reopened.read("m1"))


if __name__ == "__main__":
    unittest.main()
