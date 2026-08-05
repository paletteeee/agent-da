"""Deterministic backend-only timing and fault aggregation tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_backend_performance import FaultScenario, benchmark_backend, run_fault_matrix


class BackendPerformanceTests(unittest.TestCase):
    def test_percentiles_and_production_boundary_are_reported(self):
        report = benchmark_backend(
            lambda size=None: InstrumentedMemoryBackend(),
            workload_sizes=(5,),
            repetitions=3,
        )
        row = report["rows"][0]
        self.assertLessEqual(row["p50_ms"], row["p95_ms"])
        self.assertLessEqual(row["p95_ms"], row["p99_ms"])
        self.assertGreater(row["throughput_ops_per_second"], 0.0)
        self.assertFalse(report["production_latency_claim"])

    def test_fault_matrix_records_abort_without_partial_commit(self):
        scenarios = [
            FaultScenario("normal", "none", "write", "none", 17),
            FaultScenario("timeout", "qdrant", "write", "timeout", 17),
        ]

        def factory(scenario):
            backend = InstrumentedMemoryBackend()
            if scenario.action == "timeout":
                original = backend.write

                def failing(*args, **kwargs):
                    if not hasattr(backend, "_failed_once"):
                        backend._failed_once = True
                        raise TimeoutError("synthetic timeout")
                    return original(*args, **kwargs)

                backend.write = failing
            return backend

        report = run_fault_matrix(
            factory,
            scenarios,
            [{"type": "write", "memory_id": "m0", "value": "v0"}],
            repetitions=2,
        )
        timeout = report["scenarios"]["timeout"]
        self.assertEqual(timeout["repetitions"], 2)
        self.assertEqual(timeout["partial_commit_count"], 0)
        self.assertEqual(timeout["retry_success_count"], 2)
        self.assertTrue(report["all_scenarios_no_partial_commit"])


if __name__ == "__main__":
    unittest.main()
