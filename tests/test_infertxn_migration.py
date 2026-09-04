import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.infertxn.clock import LogicalClock
from src.infertxn.coordinator import DecisionLog, TwoPhaseCoordinator
from src.infertxn.migration import InferenceMetadataDB, StaleEpoch
from src.infertxn.models import TransactionState
from src.infertxn.mvcc import MVCCStore
from src.infertxn.participant import Participant


class InferenceMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        clock = LogicalClock()
        participants = {
            name: Participant(name, MVCCStore(name))
            for name in ("route", "kv", "request")
        }
        coordinator = TwoPhaseCoordinator(
            clock,
            participants,
            DecisionLog(Path(self.temp_dir.name) / "decisions.jsonl"),
        )
        self.db = InferenceMetadataDB(clock, participants, coordinator)
        self.db.initialize_request("r1", "decode-a", 41, generated_tokens=128)

    def test_migration_atomically_updates_all_metadata_shards(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        result = self.db.migrate("r1", "decode-a", "decode-b")

        state = self.db.read_state("r1")
        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(state["route"], {"decode_node": "decode-b", "epoch": 2})
        self.assertEqual(
            state["kv"],
            {
                "location": "decode-b",
                "cache_version": 42,
                "state": "ready",
                "epoch": 2,
            },
        )
        self.assertEqual(state["request"]["owner"], "decode-b")
        self.assertEqual(state["request"]["generated_tokens"], 128)
        self.assertTrue(state["consistent"])

    def test_migration_aborts_when_target_cache_is_not_ready(self):
        result = self.db.migrate("r1", "decode-a", "decode-b")

        self.assertEqual(result.state, TransactionState.ABORTED)
        self.assertEqual(self.db.read_state("r1")["route"]["decode_node"], "decode-a")

    def test_two_concurrent_migrations_cannot_both_commit(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        self.db.stage_target_cache("r1", "decode-c", cache_version=43)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(self.db.migrate, "r1", "decode-a", target)
                for target in ("decode-b", "decode-c")
            ]
        results = [future.result() for future in futures]

        self.assertEqual(
            sum(result.state is TransactionState.COMMITTED for result in results), 1
        )
        self.assertTrue(self.db.read_state("r1")["consistent"])

    def test_old_decode_epoch_is_fenced_after_migration(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        self.db.migrate("r1", "decode-a", "decode-b")

        with self.assertRaises(StaleEpoch):
            self.db.advance_tokens("r1", "decode-a", epoch=1, count=1)

        result = self.db.advance_tokens("r1", "decode-b", epoch=2, count=3)
        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(self.db.read_state("r1")["request"]["generated_tokens"], 131)

    def test_readers_do_not_observe_a_partially_applied_commit(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        kv = self.db.participants["kv"]
        original_commit = kv.commit
        route_committed = threading.Event()
        allow_kv_commit = threading.Event()

        original_route_commit = self.db.participants["route"].commit

        def signal_route(tx_id, commit_ts):
            result = original_route_commit(tx_id, commit_ts)
            route_committed.set()
            return result

        def block_kv(tx_id, commit_ts):
            allow_kv_commit.wait(timeout=2)
            return original_commit(tx_id, commit_ts)

        self.db.participants["route"].commit = signal_route
        kv.commit = block_kv
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.db.migrate, "r1", "decode-a", "decode-b")
            self.assertTrue(route_committed.wait(timeout=2))
            during_commit = self.db.read_state("r1")
            allow_kv_commit.set()
            result = future.result(timeout=2)

        self.assertTrue(during_commit["consistent"])
        self.assertEqual(during_commit["route"]["decode_node"], "decode-a")
        self.assertEqual(result.state, TransactionState.COMMITTED)

    def test_consumed_target_cache_cannot_be_reused_by_later_migration(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        self.assertEqual(
            self.db.migrate("r1", "decode-a", "decode-b").state,
            TransactionState.COMMITTED,
        )
        self.db.stage_target_cache("r1", "decode-a", cache_version=43)
        self.assertEqual(
            self.db.migrate("r1", "decode-b", "decode-a").state,
            TransactionState.COMMITTED,
        )

        stale_reuse = self.db.migrate("r1", "decode-a", "decode-b")

        self.assertEqual(stale_reuse.state, TransactionState.ABORTED)
        self.assertIn("not ready for epoch", stale_reuse.reason)

    def test_cache_staged_before_more_tokens_cannot_be_migrated(self):
        self.db.stage_target_cache("r1", "decode-b", cache_version=42)
        self.db.advance_tokens("r1", "decode-a", epoch=1, count=3)

        result = self.db.migrate("r1", "decode-a", "decode-b")

        self.assertEqual(result.state, TransactionState.ABORTED)
        self.assertIn("not ready for current progress", result.reason)
        self.assertEqual(self.db.read_state("r1")["request"]["generated_tokens"], 131)


if __name__ == "__main__":
    unittest.main()
