import unittest

from txnmem_distributed_protocol import (
    ProtocolCoordinator,
    check_protocol_invariants,
    run_protocol_matrix,
)


class DistributedProtocolTests(unittest.TestCase):
    def test_prepare_and_commit_is_atomic_for_all_participants(self):
        report = ProtocolCoordinator(["p1", "p2"]).execute(
            [{"type": "prepare"}, {"type": "commit"}]
        )
        self.assertEqual(report["final_states"], {"p1": "COMMITTED", "p2": "COMMITTED"})
        self.assertEqual(check_protocol_invariants(report)["violation_count"], 0)

    def test_abort_after_prepare_leaves_no_visible_committed_write(self):
        report = ProtocolCoordinator(["p1", "p2"]).execute(
            [{"type": "prepare"}, {"type": "abort"}]
        )
        self.assertEqual(report["final_states"], {"p1": "ABORTED", "p2": "ABORTED"})
        self.assertEqual(report["committed_ids"], [])
        self.assertEqual(check_protocol_invariants(report)["violation_count"], 0)

    def test_crash_after_prepare_aborts_without_half_commit(self):
        report = ProtocolCoordinator(["p1", "p2"]).execute(
            [{"type": "prepare"}, {"type": "crash_after_prepare", "participant": "p2"}, {"type": "commit"}]
        )
        self.assertEqual(report["final_states"], {"p1": "ABORTED", "p2": "ABORTED"})
        self.assertEqual(report["committed_ids"], [])
        self.assertEqual(check_protocol_invariants(report)["violation_count"], 0)

    def test_network_drop_requires_retry_and_retry_commit_is_idempotent(self):
        report = ProtocolCoordinator(["p1", "p2"]).execute(
            [
                {"type": "prepare"},
                {"type": "network_drop", "participant": "p2", "phase": "commit"},
                {"type": "commit"},
                {"type": "retry_commit"},
                {"type": "retry_commit"},
            ]
        )
        self.assertEqual(report["final_states"], {"p1": "COMMITTED", "p2": "COMMITTED"})
        self.assertEqual(len(report["committed_ids"]), 2)
        self.assertEqual(check_protocol_invariants(report)["violation_count"], 0)
        self.assertEqual(report["metrics"]["retry_count"], 1)

    def test_matrix_reports_schedule_and_invariant_coverage(self):
        summary = run_protocol_matrix(
            [
                [{"type": "prepare"}, {"type": "commit"}],
                [{"type": "prepare"}, {"type": "abort"}],
                [{"type": "prepare"}, {"type": "crash_after_prepare", "participant": "p2"}],
                [
                    {"type": "prepare"},
                    {"type": "network_drop", "participant": "p2", "phase": "commit"},
                    {"type": "commit"},
                    {"type": "retry_commit"},
                ],
            ]
        )
        self.assertEqual(summary["schedule_count"], 4)
        self.assertEqual(summary["invariant_coverage"]["coverage_rate"], 1.0)
        self.assertEqual(summary["minimal_counterexamples"], [])
        self.assertGreaterEqual(summary["schedule_coverage"]["actions"]["network_drop"], 1)
