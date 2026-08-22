"""Dependency contract for the optional external-memory baselines."""

import importlib.util
import sys
import unittest


class ExternalDependencyContractTests(unittest.TestCase):
    def test_python_version_is_supported(self):
        self.assertGreaterEqual(sys.version_info, (3, 10))

    def test_mem0_exposes_memory_adapter_import(self):
        if importlib.util.find_spec("mem0") is None:
            self.skipTest(
                "mem0ai is an optional external-baseline dependency; "
                "install requirements-baselines.txt before running this contract"
            )

        from mem0 import Memory

        self.assertTrue(callable(Memory))

    def test_langgraph_exposes_postgres_store_adapter_import(self):
        if importlib.util.find_spec("langgraph") is None:
            self.skipTest(
                "langgraph and langgraph-checkpoint-postgres are optional "
                "external-baseline dependencies; install requirements-baselines.txt "
                "before running this contract"
            )

        from langgraph.store.postgres import PostgresStore

        self.assertTrue(callable(PostgresStore))


if __name__ == "__main__":
    unittest.main()
