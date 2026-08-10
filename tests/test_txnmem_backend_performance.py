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
            FaultScenario("normal", "none", "none", "none", 17, "none"),
            FaultScenario("timeout", "qdrant", "write", "timeout", 17, "retry_once"),
        ]

        def factory(*, scenario=None):
            backend = InstrumentedMemoryBackend()
            if scenario.action == "timeout":
                original = backend.write

                def failing(*args, **kwargs):
                    if not hasattr(backend, "_failed_once"):
                        backend._failed_once = True
                        raise TimeoutError("synthetic timeout")
                    return original(*args, **kwargs)

                backend.write = failing
            backend.fault_evidence = lambda: {
                "scenario": scenario.name,
                "trigger_fired": scenario.name != "normal",
                "toxic_installed": scenario.name != "normal",
                "toxic_cleared": scenario.name != "normal",
                "proxy_path_verified": True,
                "retry_count": int(scenario.name == "timeout"),
                "retry_success_count": int(scenario.name == "timeout"),
                "evidence_valid": True,
            }
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
        self.assertTrue(timeout["evidence_valid"])
        self.assertTrue(report["all_scenarios_evidence_valid"])
        self.assertTrue(report["all_scenarios_no_partial_commit"])

    def test_backend_factory_receives_scenario_by_keyword(self):
        observed = []

        def factory(*, scenario=None):
            observed.append(None if scenario is None else scenario.name)
            backend = InstrumentedMemoryBackend()
            backend.fault_evidence = lambda: {
                "scenario": scenario.name,
                "trigger_fired": False,
                "toxic_installed": False,
                "toxic_cleared": False,
                "proxy_path_verified": True,
                "retry_count": 0,
                "retry_success_count": 0,
                "evidence_valid": True,
            }
            return backend

        run_fault_matrix(
            factory,
            [FaultScenario("normal", "none", "none", "none", 17, "none")],
            [{"type": "write", "memory_id": "m0", "value": "v0"}],
        )
        self.assertEqual(observed, ["normal"])

    def test_non_normal_scenario_without_trigger_evidence_fails_closed(self):
        report = run_fault_matrix(
            lambda *, scenario=None: InstrumentedMemoryBackend(),
            [FaultScenario("drop", "qdrant", "write", "connection_drop", 17, "abort")],
            [{"type": "write", "memory_id": "m0", "value": "v0"}],
        )

        self.assertFalse(report["scenarios"]["drop"]["evidence_valid"])
        self.assertFalse(report["all_scenarios_evidence_valid"])


if __name__ == "__main__":
    unittest.main()
