import json
import tempfile
import unittest
from pathlib import Path

from src.infertxn.clock import LogicalClock
from src.infertxn.coordinator import DecisionLog, TwoPhaseCoordinator
from src.infertxn.models import TransactionState
from src.infertxn.mvcc import MVCCStore
from src.infertxn.participant import Participant


class TwoPhaseCommitTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.log_path = Path(self.temp_dir.name) / "decisions.jsonl"
        self.clock = LogicalClock()
        self.route = Participant("route", MVCCStore("route"))
        self.request = Participant("request", MVCCStore("request"))
        self.route.store.seed("route/r1", {"node": "a"}, self.clock.tick())
        self.request.store.seed("request/r1", {"owner": "a"}, self.clock.tick())
        self.coordinator = TwoPhaseCoordinator(
            self.clock,
            {"route": self.route, "request": self.request},
            DecisionLog(self.log_path),
        )

    def test_commit_changes_every_participant(self):
        result = self.coordinator.execute(
            {
                "route": {"route/r1": {"node": "b"}},
                "request": {"request/r1": {"owner": "b"}},
            }
        )

        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(self.route.store.read("route/r1"), {"node": "b"})
        self.assertEqual(self.request.store.read("request/r1"), {"owner": "b"})

    def test_prepare_rejection_aborts_every_participant(self):
        self.request.fail_next_prepare = True

        result = self.coordinator.execute(
            {
                "route": {"route/r1": {"node": "b"}},
                "request": {"request/r1": {"owner": "b"}},
            }
        )

        self.assertEqual(result.state, TransactionState.ABORTED)
        self.assertEqual(self.route.store.read("route/r1"), {"node": "a"})
        self.assertEqual(self.request.store.read("request/r1"), {"owner": "a"})
        self.assertEqual(self.route.status(result.tx_id), TransactionState.ABORTED)

    def test_decision_is_flushed_before_participants_receive_commit(self):
        observed = []
        original_commit = self.route.commit

        def inspecting_commit(tx_id, commit_ts):
            records = [json.loads(line) for line in self.log_path.read_text().splitlines()]
            observed.append(records[-1]["state"])
            return original_commit(tx_id, commit_ts)

        self.route.commit = inspecting_commit
        self.coordinator.execute({"route": {"route/r1": {"node": "b"}}})

        self.assertEqual(observed, ["committed"])

    def test_recovery_replays_commit_after_acknowledgement_is_lost(self):
        self.request.drop_next_commit_ack = True
        result = self.coordinator.execute(
            {
                "route": {"route/r1": {"node": "b"}},
                "request": {"request/r1": {"owner": "b"}},
            }
        )

        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(result.reason, "commit acknowledgement pending: request")
        recovered = self.coordinator.recover()
        self.assertEqual(recovered[0].state, TransactionState.COMMITTED)
        self.assertIsNone(recovered[0].reason)
        self.assertEqual(self.request.store.version_count("request/r1"), 2)

    def test_prepare_timeout_aborts_participants_that_already_prepared(self):
        def timeout(*args, **kwargs):
            raise TimeoutError("request shard unavailable")

        self.request.prepare = timeout

        result = self.coordinator.execute(
            {
                "route": {"route/r1": {"node": "b"}},
                "request": {"request/r1": {"owner": "b"}},
            }
        )

        self.assertEqual(result.state, TransactionState.ABORTED)
        self.assertEqual(self.route.status(result.tx_id), TransactionState.ABORTED)
        self.assertEqual(self.route.store.read("route/r1"), {"node": "a"})

    def test_commit_transport_error_does_not_skip_later_participants(self):
        original_commit = self.route.commit

        def commit_then_timeout(tx_id, commit_ts):
            original_commit(tx_id, commit_ts)
            raise TimeoutError("ack timed out")

        self.route.commit = commit_then_timeout

        result = self.coordinator.execute(
            {
                "route": {"route/r1": {"node": "b"}},
                "request": {"request/r1": {"owner": "b"}},
            }
        )

        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(result.reason, "commit acknowledgement pending: route")
        self.assertEqual(self.request.store.read("request/r1"), {"owner": "b"})

    def test_fresh_coordinator_recovery_restores_clock_before_next_commit(self):
        first = self.coordinator.execute(
            {"route": {"route/r1": {"node": "b"}}}
        )
        fresh_clock = LogicalClock()
        recovered_coordinator = TwoPhaseCoordinator(
            fresh_clock,
            {"route": self.route, "request": self.request},
            DecisionLog(self.log_path),
        )

        recovered_coordinator.recover()
        second = recovered_coordinator.execute(
            {"route": {"route/r1": {"node": "c"}}}
        )

        self.assertGreater(second.commit_ts, first.commit_ts)
        self.assertEqual(self.route.store.read("route/r1"), {"node": "c"})

    def test_fresh_coordinator_requires_recovery_before_new_transactions(self):
        self.coordinator.execute({"route": {"route/r1": {"node": "b"}}})
        fresh = TwoPhaseCoordinator(
            LogicalClock(),
            {"route": self.route, "request": self.request},
            DecisionLog(self.log_path),
        )

        result = fresh.execute({"request": {"request/new": {"owner": "c"}}})

        self.assertEqual(result.state, TransactionState.ABORTED)
        self.assertEqual(
            result.reason, "coordinator recovery required before new transactions"
        )
        self.assertIsNone(self.request.store.read("request/new"))


if __name__ == "__main__":
    unittest.main()
