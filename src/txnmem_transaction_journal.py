"""Durable SQLite source of truth for task transaction decisions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, TypeVar


_T = TypeVar("_T")
_STATES = frozenset({"ACTIVE", "PREPARED", "COMMITTED", "ABORTED"})
_DECISIONS = frozenset({"COMMITTED", "ABORTED"})


@dataclass(frozen=True)
class TransactionRecord:
    txn_id: str
    task_id: str
    agent_id: str
    begin_policy_version: int
    state: str
    decision: str | None


class TransactionDecisionError(RuntimeError):
    """A journal decision or evidence conflict with a stable code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class TransactionJournal:
    """SQLite-backed, single-writer decision journal for one or more tasks."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._connection = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                begin_policy_version INTEGER NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'PREPARED', 'COMMITTED', 'ABORTED')),
                decision TEXT CHECK (decision IS NULL OR decision IN ('COMMITTED', 'ABORTED')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (
                    (state IN ('ACTIVE', 'PREPARED') AND decision IS NULL)
                    OR (state = 'COMMITTED' AND decision = 'COMMITTED')
                    OR (state = 'ABORTED' AND decision = 'ABORTED')
                )
            );
            CREATE TRIGGER IF NOT EXISTS transactions_enforce_state_transition
            BEFORE UPDATE OF state, decision ON transactions
            FOR EACH ROW
            WHEN NOT (
                (OLD.state = NEW.state AND OLD.decision IS NEW.decision)
                OR (
                    OLD.state = 'ACTIVE' AND OLD.decision IS NULL
                    AND NEW.state = 'PREPARED' AND NEW.decision IS NULL
                )
                OR (
                    OLD.state = 'ACTIVE' AND OLD.decision IS NULL
                    AND NEW.state = 'ABORTED' AND NEW.decision = 'ABORTED'
                )
                OR (
                    OLD.state = 'PREPARED' AND OLD.decision IS NULL
                    AND NEW.state = 'COMMITTED' AND NEW.decision = 'COMMITTED'
                )
                OR (
                    OLD.state = 'PREPARED' AND OLD.decision IS NULL
                    AND NEW.state = 'ABORTED' AND NEW.decision = 'ABORTED'
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid transaction state transition');
            END;
            CREATE TABLE IF NOT EXISTS intents (
                txn_id TEXT NOT NULL REFERENCES transactions(txn_id),
                sequence INTEGER NOT NULL CHECK (sequence >= 0),
                tool_name TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (txn_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS read_set (
                txn_id TEXT NOT NULL REFERENCES transactions(txn_id),
                memory_id TEXT NOT NULL,
                observed_version INTEGER NOT NULL,
                scope TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (txn_id, memory_id)
            );
            CREATE TABLE IF NOT EXISTS transaction_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                txn_id TEXT NOT NULL REFERENCES transactions(txn_id),
                phase TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (txn_id, phase)
            );
            CREATE TABLE IF NOT EXISTS backend_receipts (
                receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                txn_id TEXT NOT NULL REFERENCES transactions(txn_id),
                backend TEXT NOT NULL,
                operation_key TEXT NOT NULL,
                phase TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (txn_id, backend, operation_key, phase)
            );
            """
        )

    @contextmanager
    def _write(self) -> Iterator[None]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self._connection.execute("ROLLBACK")
            raise
        else:
            self._connection.execute("COMMIT")

    def _in_write(self, action: Callable[[], _T]) -> _T:
        with self._write():
            return action()

    def _record_from_row(self, row: sqlite3.Row) -> TransactionRecord:
        return TransactionRecord(
            txn_id=row["txn_id"],
            task_id=row["task_id"],
            agent_id=row["agent_id"],
            begin_policy_version=row["begin_policy_version"],
            state=row["state"],
            decision=row["decision"],
        )

    def _load_row(self, txn_id: str) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT txn_id, task_id, agent_id, begin_policy_version, state, decision "
            "FROM transactions WHERE txn_id = ?",
            (txn_id,),
        ).fetchone()
        if row is None:
            raise TransactionDecisionError("transaction_not_found", f"unknown transaction: {txn_id}")
        return row

    @staticmethod
    def _json_mapping(value: Mapping[str, Any] | None, code: str) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise TransactionDecisionError(code, "evidence must be a mapping")
        try:
            return json.loads(_canonical_json(dict(value)))
        except (TypeError, ValueError) as exc:
            raise TransactionDecisionError(code, "evidence must be JSON serializable") from exc

    def begin(
        self, *, txn_id: str, task_id: str, agent_id: str, begin_policy_version: int
    ) -> TransactionRecord:
        def action() -> TransactionRecord:
            row = self._connection.execute(
                "SELECT txn_id, task_id, agent_id, begin_policy_version, state, decision "
                "FROM transactions WHERE txn_id = ?",
                (txn_id,),
            ).fetchone()
            if row is not None:
                record = self._record_from_row(row)
                requested = (task_id, agent_id, begin_policy_version)
                stored = (record.task_id, record.agent_id, record.begin_policy_version)
                if requested != stored:
                    raise TransactionDecisionError("begin_conflict", f"conflicting begin metadata for {txn_id}")
                return record
            now = _utc_timestamp()
            self._connection.execute(
                "INSERT INTO transactions "
                "(txn_id, task_id, agent_id, begin_policy_version, state, decision, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'ACTIVE', NULL, ?, ?)",
                (txn_id, task_id, agent_id, begin_policy_version, now, now),
            )
            return TransactionRecord(txn_id, task_id, agent_id, begin_policy_version, "ACTIVE", None)

        return self._in_write(action)

    def append_intent(
        self, txn_id: str, *, sequence: int, tool_name: str, arguments: Mapping[str, Any]
    ) -> dict[str, Any]:
        payload = self._json_mapping({"tool_name": tool_name, "arguments": arguments}, "invalid_intent")
        payload_json = _canonical_json(payload)
        payload_digest = _digest(payload)

        def action() -> dict[str, Any]:
            self._load_row(txn_id)
            row = self._connection.execute(
                "SELECT tool_name, arguments_json, payload_digest FROM intents "
                "WHERE txn_id = ? AND sequence = ?",
                (txn_id, sequence),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO intents (txn_id, sequence, tool_name, arguments_json, payload_digest, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (txn_id, sequence, tool_name, _canonical_json(payload["arguments"]), payload_digest, _utc_timestamp()),
                )
            elif row["payload_digest"] != payload_digest:
                raise TransactionDecisionError(
                    "intent_sequence_conflict", f"conflicting intent at sequence {sequence}"
                )
            return {
                "txn_id": txn_id,
                "sequence": sequence,
                "tool_name": payload["tool_name"],
                "arguments": payload["arguments"],
                "payload_digest": payload_digest,
            }

        return self._in_write(action)

    def record_read(
        self, txn_id: str, *, memory_id: str, observed_version: int, scope: str
    ) -> None:
        def action() -> None:
            self._load_row(txn_id)
            row = self._connection.execute(
                "SELECT observed_version, scope FROM read_set WHERE txn_id = ? AND memory_id = ?",
                (txn_id, memory_id),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO read_set (txn_id, memory_id, observed_version, scope, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (txn_id, memory_id, observed_version, scope, _utc_timestamp()),
                )
            elif (row["observed_version"], row["scope"]) != (observed_version, scope):
                raise TransactionDecisionError(
                    "read_set_conflict", f"conflicting observed read for {memory_id}"
                )

        self._in_write(action)

    def record_phase(
        self, txn_id: str, phase: str, evidence: Mapping[str, Any] | None = None
    ) -> None:
        normalized = self._json_mapping(evidence, "invalid_phase_evidence")
        evidence_json = _canonical_json(normalized)
        evidence_digest = _digest(normalized)

        def action() -> None:
            self._load_row(txn_id)
            row = self._connection.execute(
                "SELECT evidence_digest FROM transaction_events WHERE txn_id = ? AND phase = ?",
                (txn_id, phase),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO transaction_events "
                    "(txn_id, phase, evidence_json, evidence_digest, created_at) VALUES (?, ?, ?, ?, ?)",
                    (txn_id, phase, evidence_json, evidence_digest, _utc_timestamp()),
                )
            elif row["evidence_digest"] != evidence_digest:
                raise TransactionDecisionError(
                    "phase_evidence_conflict", f"conflicting evidence for phase {phase}"
                )

        self._in_write(action)

    def record_backend_receipt(
        self, txn_id: str, *, backend: str, operation_key: str, phase: str, evidence: Mapping[str, Any]
    ) -> None:
        normalized = self._json_mapping(evidence, "invalid_backend_receipt")
        evidence_json = _canonical_json(normalized)
        evidence_digest = _digest(normalized)

        def action() -> None:
            self._load_row(txn_id)
            row = self._connection.execute(
                "SELECT evidence_digest FROM backend_receipts "
                "WHERE txn_id = ? AND backend = ? AND operation_key = ? AND phase = ?",
                (txn_id, backend, operation_key, phase),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO backend_receipts "
                    "(txn_id, backend, operation_key, phase, evidence_json, evidence_digest, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (txn_id, backend, operation_key, phase, evidence_json, evidence_digest, _utc_timestamp()),
                )
            elif row["evidence_digest"] != evidence_digest:
                raise TransactionDecisionError(
                    "backend_receipt_conflict", f"conflicting receipt {backend}/{operation_key}/{phase}"
                )

        self._in_write(action)

    def prepare(self, txn_id: str) -> TransactionRecord:
        def action() -> TransactionRecord:
            record = self._record_from_row(self._load_row(txn_id))
            if record.state == "ACTIVE":
                self._connection.execute(
                    "UPDATE transactions SET state = 'PREPARED', updated_at = ? WHERE txn_id = ?",
                    (_utc_timestamp(), txn_id),
                )
                return TransactionRecord(
                    record.txn_id,
                    record.task_id,
                    record.agent_id,
                    record.begin_policy_version,
                    "PREPARED",
                    None,
                )
            if record.state == "PREPARED":
                return record
            raise TransactionDecisionError("terminal_state", f"cannot prepare terminal transaction {txn_id}")

        return self._in_write(action)

    def decide(self, txn_id: str, decision: str) -> TransactionRecord:
        decision = decision.upper()
        if decision not in _DECISIONS:
            raise TransactionDecisionError("invalid_decision", f"unsupported decision: {decision}")

        def action() -> TransactionRecord:
            record = self._record_from_row(self._load_row(txn_id))
            if record.state in _DECISIONS:
                if record.decision == decision:
                    return record
                raise TransactionDecisionError(
                    "terminal_decision_conflict",
                    f"terminal decision conflict for {txn_id}: {record.decision} versus {decision}",
                )
            if decision == "COMMITTED" and record.state != "PREPARED":
                raise TransactionDecisionError(
                    "commit_requires_prepared", f"COMMITTED requires PREPARED for {txn_id}"
                )
            self._connection.execute(
                "UPDATE transactions SET state = ?, decision = ?, updated_at = ? WHERE txn_id = ?",
                (decision, decision, _utc_timestamp(), txn_id),
            )
            return TransactionRecord(
                record.txn_id,
                record.task_id,
                record.agent_id,
                record.begin_policy_version,
                decision,
                decision,
            )

        return self._in_write(action)

    def load(self, txn_id: str) -> TransactionRecord:
        return self._record_from_row(self._load_row(txn_id))

    def intents(self, txn_id: str) -> list[dict[str, Any]]:
        self._load_row(txn_id)
        rows = self._connection.execute(
            "SELECT sequence, tool_name, arguments_json, payload_digest FROM intents "
            "WHERE txn_id = ? ORDER BY sequence",
            (txn_id,),
        ).fetchall()
        return [
            {
                "txn_id": txn_id,
                "sequence": row["sequence"],
                "tool_name": row["tool_name"],
                "arguments": json.loads(row["arguments_json"]),
                "payload_digest": row["payload_digest"],
            }
            for row in rows
        ]

    def read_set(self, txn_id: str) -> list[dict[str, Any]]:
        self._load_row(txn_id)
        rows = self._connection.execute(
            "SELECT memory_id, observed_version, scope FROM read_set WHERE txn_id = ? ORDER BY memory_id",
            (txn_id,),
        ).fetchall()
        return [
            {
                "txn_id": txn_id,
                "memory_id": row["memory_id"],
                "observed_version": row["observed_version"],
                "scope": row["scope"],
            }
            for row in rows
        ]

    def phases(self, txn_id: str) -> list[dict[str, Any]]:
        self._load_row(txn_id)
        rows = self._connection.execute(
            "SELECT phase, evidence_json, evidence_digest FROM transaction_events "
            "WHERE txn_id = ? ORDER BY phase",
            (txn_id,),
        ).fetchall()
        return [
            {
                "txn_id": txn_id,
                "phase": row["phase"],
                "evidence": json.loads(row["evidence_json"]),
                "evidence_digest": row["evidence_digest"],
            }
            for row in rows
        ]

    def backend_receipts(self, txn_id: str) -> list[dict[str, Any]]:
        self._load_row(txn_id)
        rows = self._connection.execute(
            "SELECT backend, operation_key, phase, evidence_json, evidence_digest FROM backend_receipts "
            "WHERE txn_id = ? ORDER BY backend, operation_key, phase",
            (txn_id,),
        ).fetchall()
        return [
            {
                "txn_id": txn_id,
                "backend": row["backend"],
                "operation_key": row["operation_key"],
                "phase": row["phase"],
                "evidence": json.loads(row["evidence_json"]),
                "evidence_digest": row["evidence_digest"],
            }
            for row in rows
        ]

    def recoverable_transaction_ids(self) -> list[str]:
        rows = self._connection.execute(
            """
            SELECT txn_id FROM transactions AS t
            WHERE state IN ('ACTIVE', 'PREPARED')
               OR (state = 'COMMITTED' AND NOT EXISTS (
                    SELECT 1 FROM transaction_events AS e
                    WHERE e.txn_id = t.txn_id AND e.phase = 'finalize_complete'
               ))
               OR (state = 'ABORTED' AND NOT EXISTS (
                    SELECT 1 FROM transaction_events AS e
                    WHERE e.txn_id = t.txn_id AND e.phase = 'cleanup_complete'
               ))
            ORDER BY txn_id
            """
        ).fetchall()
        return [row["txn_id"] for row in rows]

    def state_digest(self, txn_id: str) -> str:
        record = self.load(txn_id)
        state = {
            "transaction": {
                "txn_id": record.txn_id,
                "task_id": record.task_id,
                "agent_id": record.agent_id,
                "begin_policy_version": record.begin_policy_version,
                "state": record.state,
                "decision": record.decision,
            },
            "intents": self.intents(txn_id),
            "read_set": self.read_set(txn_id),
            "phases": self.phases(txn_id),
            "backend_receipts": self.backend_receipts(txn_id),
        }
        return _digest(state)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
