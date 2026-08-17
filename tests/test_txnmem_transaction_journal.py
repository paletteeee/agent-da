from __future__ import annotations

import multiprocessing
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_transaction_journal import (  # noqa: E402
    TransactionDecisionError,
    TransactionJournal,
    TransactionRecord,
)


def _competing_decision(path: str, decision: str, ready: multiprocessing.Event, queue: multiprocessing.Queue) -> None:
    journal = TransactionJournal(path)
    try:
        ready.wait(5)
        queue.put(("ok", journal.decide("txn_race", decision).decision))
    except TransactionDecisionError as exc:
        queue.put(("error", exc.code))
    finally:
        journal.close()


def _prepare_then_exit(path: str) -> None:
    journal = TransactionJournal(path)
    journal.begin(txn_id="txn_exit", task_id="task", agent_id="agent", begin_policy_version=1)
    journal.append_intent("txn_exit", sequence=1, tool_name="memory_write", arguments={"memory_id": "m1"})
    journal.prepare("txn_exit")
    os._exit(0)


class TransactionJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "journal.sqlite"
        self.journal = TransactionJournal(self.path)

    def tearDown(self) -> None:
        self.journal.close()
        self.temp_dir.cleanup()

    def _begin(self, txn_id: str = "txn_1") -> TransactionRecord:
        return self.journal.begin(
            txn_id=txn_id,
            task_id="task_1",
            agent_id="agent_1",
            begin_policy_version=7,
        )

    def _direct_state_update(self, txn_id: str, state: str, decision: str | None) -> None:
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                "UPDATE transactions SET state = ?, decision = ? WHERE txn_id = ?",
                (state, decision, txn_id),
            )
            connection.commit()
        finally:
            connection.close()

    def test_begin_creates_active_transaction_and_is_idempotent(self) -> None:
        record = self._begin()
        self.assertEqual(
            record,
            TransactionRecord("txn_1", "task_1", "agent_1", 7, "ACTIVE", None),
        )
        self.assertEqual(self._begin(), record)

    def test_state_machine_allows_only_required_transitions(self) -> None:
        self._begin("txn_commit")
        self.assertEqual(self.journal.prepare("txn_commit").state, "PREPARED")
        self.assertEqual(self.journal.decide("txn_commit", "COMMITTED").decision, "COMMITTED")
        self.assertEqual(self.journal.decide("txn_commit", "COMMITTED").state, "COMMITTED")
        with self.assertRaisesRegex(TransactionDecisionError, "terminal") as caught:
            self.journal.decide("txn_commit", "ABORTED")
        self.assertEqual(caught.exception.code, "terminal_decision_conflict")
        with self.assertRaisesRegex(TransactionDecisionError, "terminal"):
            self.journal.prepare("txn_commit")

        self._begin("txn_abort_active")
        self.assertEqual(self.journal.decide("txn_abort_active", "ABORTED").state, "ABORTED")
        self.assertEqual(self.journal.decide("txn_abort_active", "ABORTED").decision, "ABORTED")

        self._begin("txn_abort_prepared")
        self.journal.prepare("txn_abort_prepared")
        self.assertEqual(self.journal.decide("txn_abort_prepared", "ABORTED").state, "ABORTED")
        with self.assertRaisesRegex(TransactionDecisionError, "terminal") as caught:
            self.journal.decide("txn_abort_prepared", "COMMITTED")
        self.assertEqual(caught.exception.code, "terminal_decision_conflict")

        self._begin("txn_not_prepared")
        with self.assertRaises(TransactionDecisionError) as caught:
            self.journal.decide("txn_not_prepared", "COMMITTED")
        self.assertEqual(caught.exception.code, "commit_requires_prepared")

    def test_database_rejects_active_to_committed_direct_update(self) -> None:
        self._begin("txn_direct_commit")
        with self.assertRaises(sqlite3.IntegrityError):
            self._direct_state_update("txn_direct_commit", "COMMITTED", "COMMITTED")
        self.assertEqual(self.journal.load("txn_direct_commit").state, "ACTIVE")

    def test_database_rejects_terminal_reopening_direct_update(self) -> None:
        self._begin("txn_reopen")
        self.journal.prepare("txn_reopen")
        self.journal.decide("txn_reopen", "COMMITTED")
        with self.assertRaises(sqlite3.IntegrityError):
            self._direct_state_update("txn_reopen", "ACTIVE", None)
        self.assertEqual(self.journal.load("txn_reopen").state, "COMMITTED")

    def test_database_rejects_committed_to_aborted_direct_update(self) -> None:
        self._begin("txn_flip_committed")
        self.journal.prepare("txn_flip_committed")
        self.journal.decide("txn_flip_committed", "COMMITTED")
        with self.assertRaises(sqlite3.IntegrityError):
            self._direct_state_update("txn_flip_committed", "ABORTED", "ABORTED")
        self.assertEqual(self.journal.load("txn_flip_committed").decision, "COMMITTED")

    def test_database_rejects_aborted_to_committed_direct_update(self) -> None:
        self._begin("txn_flip_aborted")
        self.journal.decide("txn_flip_aborted", "ABORTED")
        with self.assertRaises(sqlite3.IntegrityError):
            self._direct_state_update("txn_flip_aborted", "COMMITTED", "COMMITTED")
        self.assertEqual(self.journal.load("txn_flip_aborted").decision, "ABORTED")

    def test_database_allows_legal_and_same_decision_direct_updates(self) -> None:
        self._begin("txn_legal")
        self._direct_state_update("txn_legal", "ACTIVE", None)
        self._direct_state_update("txn_legal", "PREPARED", None)
        self._direct_state_update("txn_legal", "COMMITTED", "COMMITTED")
        self._direct_state_update("txn_legal", "COMMITTED", "COMMITTED")
        self.assertEqual(self.journal.load("txn_legal").decision, "COMMITTED")

    def test_intents_and_reads_are_idempotent_but_conflicts_are_coded(self) -> None:
        self._begin()
        first = self.journal.append_intent(
            "txn_1", sequence=1, tool_name="memory_write", arguments={"b": 2, "a": 1}
        )
        second = self.journal.append_intent(
            "txn_1", sequence=1, tool_name="memory_write", arguments={"a": 1, "b": 2}
        )
        self.assertEqual(first, second)
        self.assertEqual(len(self.journal.intents("txn_1")), 1)
        with self.assertRaises(TransactionDecisionError) as caught:
            self.journal.append_intent(
                "txn_1", sequence=1, tool_name="memory_write", arguments={"a": 3}
            )
        self.assertEqual(caught.exception.code, "intent_sequence_conflict")

        self.journal.record_read("txn_1", memory_id="m1", observed_version=2, scope="team")
        self.journal.record_read("txn_1", memory_id="m1", observed_version=2, scope="team")
        self.assertEqual(len(self.journal.read_set("txn_1")), 1)
        with self.assertRaises(TransactionDecisionError) as caught:
            self.journal.record_read("txn_1", memory_id="m1", observed_version=3, scope="team")
        self.assertEqual(caught.exception.code, "read_set_conflict")

    def test_phases_and_receipts_are_idempotent_and_persist(self) -> None:
        self._begin()
        self.journal.append_intent(
            "txn_1", sequence=1, tool_name="memory_write", arguments={"memory_id": "m1"}
        )
        self.journal.record_read("txn_1", memory_id="m0", observed_version=2, scope="team")
        self.journal.record_phase("txn_1", "qdrant_staged", {"count": 2, "ids": ["m1", "m2"]})
        self.journal.record_phase("txn_1", "qdrant_staged", {"ids": ["m1", "m2"], "count": 2})
        self.assertEqual(len(self.journal.phases("txn_1")), 1)
        with self.assertRaises(TransactionDecisionError) as caught:
            self.journal.record_phase("txn_1", "qdrant_staged", {"count": 3})
        self.assertEqual(caught.exception.code, "phase_evidence_conflict")

        self.journal.record_backend_receipt(
            "txn_1", backend="qdrant", operation_key="write:m1", phase="stage", evidence={"etag": "x"}
        )
        self.journal.record_backend_receipt(
            "txn_1", backend="qdrant", operation_key="write:m1", phase="stage", evidence={"etag": "x"}
        )
        self.assertEqual(len(self.journal.backend_receipts("txn_1")), 1)
        with self.assertRaises(TransactionDecisionError) as caught:
            self.journal.record_backend_receipt(
                "txn_1", backend="qdrant", operation_key="write:m1", phase="stage", evidence={"etag": "y"}
            )
        self.assertEqual(caught.exception.code, "backend_receipt_conflict")

        self.journal.close()
        self.journal = TransactionJournal(self.path)
        self.assertEqual(self.journal.load("txn_1").state, "ACTIVE")
        self.assertEqual(len(self.journal.intents("txn_1")), 1)
        self.assertEqual(len(self.journal.read_set("txn_1")), 1)
        self.assertEqual(len(self.journal.phases("txn_1")), 1)
        self.assertEqual(len(self.journal.backend_receipts("txn_1")), 1)

    def test_recoverable_ids_require_terminal_completion_phase(self) -> None:
        self._begin("txn_active")
        self._begin("txn_prepared")
        self.journal.prepare("txn_prepared")
        self._begin("txn_committed")
        self.journal.prepare("txn_committed")
        self.journal.decide("txn_committed", "COMMITTED")
        self._begin("txn_aborted")
        self.journal.decide("txn_aborted", "ABORTED")
        self.assertEqual(
            self.journal.recoverable_transaction_ids(),
            ["txn_aborted", "txn_active", "txn_committed", "txn_prepared"],
        )
        self.journal.record_phase("txn_committed", "finalize_complete")
        self.journal.record_phase("txn_aborted", "cleanup_complete")
        self.assertEqual(self.journal.recoverable_transaction_ids(), ["txn_active", "txn_prepared"])

    def test_state_digest_excludes_idempotent_recovery_metadata(self) -> None:
        self._begin()
        self.journal.prepare("txn_1")
        before = self.journal.state_digest("txn_1")
        self.journal.record_phase("txn_1", "recovery_started", {"attempt": 1})
        once = self.journal.state_digest("txn_1")
        self.journal.record_phase("txn_1", "recovery_started", {"attempt": 1})
        self.assertNotEqual(before, once)
        self.assertEqual(once, self.journal.state_digest("txn_1"))
        self.journal.close()
        self.journal = TransactionJournal(self.path)
        self.assertEqual(once, self.journal.state_digest("txn_1"))

    def test_opposite_decisions_from_processes_produce_one_terminal_winner(self) -> None:
        self._begin("txn_race")
        self.journal.prepare("txn_race")
        ready = multiprocessing.Event()
        queue: multiprocessing.Queue = multiprocessing.Queue()
        committed = multiprocessing.Process(target=_competing_decision, args=(str(self.path), "COMMITTED", ready, queue))
        aborted = multiprocessing.Process(target=_competing_decision, args=(str(self.path), "ABORTED", ready, queue))
        committed.start()
        aborted.start()
        ready.set()
        committed.join(10)
        aborted.join(10)
        self.assertFalse(committed.is_alive())
        self.assertFalse(aborted.is_alive())
        outcomes = sorted((queue.get(timeout=2), queue.get(timeout=2)))
        self.assertEqual(len([item for item in outcomes if item[0] == "ok"]), 1)
        self.assertEqual(len([item for item in outcomes if item == ("error", "terminal_decision_conflict")]), 1)
        self.assertIn(self.journal.load("txn_race").decision, {"COMMITTED", "ABORTED"})

    def test_prepared_intents_survive_abrupt_worker_exit(self) -> None:
        self.journal.close()
        worker = multiprocessing.Process(target=_prepare_then_exit, args=(str(self.path),))
        worker.start()
        worker.join(10)
        self.assertEqual(worker.exitcode, 0)
        self.journal = TransactionJournal(self.path)
        self.assertEqual(self.journal.load("txn_exit").state, "PREPARED")
        self.assertEqual(self.journal.intents("txn_exit")[0]["arguments"], {"memory_id": "m1"})


if __name__ == "__main__":
    unittest.main()
