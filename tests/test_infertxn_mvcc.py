import unittest

from src.infertxn.clock import LogicalClock
from src.infertxn.models import Vote
from src.infertxn.mvcc import MVCCStore


class MVCCStoreTests(unittest.TestCase):
    def setUp(self):
        self.clock = LogicalClock()
        self.store = MVCCStore("route")
        self.store.seed("route/r1", {"node": "a"}, self.clock.tick())

    def test_snapshot_read_does_not_see_a_newer_commit(self):
        snapshot = self.clock.tick()
        start = self.clock.tick()
        self.assertEqual(
            self.store.prepare("tx-1", start, {"route/r1": {"node": "b"}}),
            Vote.YES,
        )
        self.store.commit("tx-1", self.clock.tick())

        self.assertEqual(self.store.read("route/r1", snapshot), {"node": "a"})
        self.assertEqual(self.store.read("route/r1"), {"node": "b"})

    def test_prepared_write_is_not_visible_before_commit(self):
        start = self.clock.tick()
        self.store.prepare("tx-1", start, {"route/r1": {"node": "b"}})

        self.assertEqual(self.store.read("route/r1"), {"node": "a"})

    def test_prepare_rejects_writer_older_than_latest_commit(self):
        stale_start = self.clock.tick()
        fresh_start = self.clock.tick()
        self.store.prepare("fresh", fresh_start, {"route/r1": {"node": "b"}})
        self.store.commit("fresh", self.clock.tick())

        vote = self.store.prepare(
            "stale", stale_start, {"route/r1": {"node": "c"}}
        )

        self.assertEqual(vote, Vote.NO)
        self.assertEqual(self.store.read("route/r1"), {"node": "b"})

    def test_replaying_commit_does_not_create_a_second_version(self):
        start = self.clock.tick()
        self.store.prepare("tx-1", start, {"route/r1": {"node": "b"}})
        commit_ts = self.clock.tick()

        self.assertTrue(self.store.commit("tx-1", commit_ts))
        self.assertTrue(self.store.commit("tx-1", commit_ts))
        self.assertEqual(self.store.version_count("route/r1"), 2)


if __name__ == "__main__":
    unittest.main()
