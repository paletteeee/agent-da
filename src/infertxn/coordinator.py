import json
import os
import uuid
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Mapping, Optional

from .clock import LogicalClock
from .models import TransactionResult, TransactionState, Vote
from .participant import CommitAcknowledgementLost, Participant


class DecisionLog:
    """Append-only JSONL log whose latest record is authoritative per tx."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()

    def record(
        self,
        tx_id: str,
        state: TransactionState,
        participants: List[str],
        commit_ts: Optional[int] = None,
    ) -> None:
        record = {
            "tx_id": tx_id,
            "state": state.value,
            "participants": list(participants),
            "commit_ts": commit_ts,
        }
        with self._lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())

    def all(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            if not self.path.exists():
                return {}
            records: Dict[str, Dict[str, Any]] = {}
            with self.path.open(encoding="utf-8") as stream:
                for line in stream:
                    if line.strip():
                        record = json.loads(line)
                        records[record["tx_id"]] = record
            return records

    def get(self, tx_id: str) -> Optional[Dict[str, Any]]:
        return self.all().get(tx_id)


class TwoPhaseCoordinator:
    def __init__(
        self,
        clock: LogicalClock,
        participants: Mapping[str, Participant],
        decision_log: DecisionLog,
    ) -> None:
        self.clock = clock
        self.participants = dict(participants)
        self.decision_log = decision_log
        recovery_required = bool(decision_log.all())
        self._visible_ts = 0 if recovery_required else clock.now()
        self._pending_commit = recovery_required
        self._tx_lock = RLock()

    def snapshot_ts(self) -> int:
        # Publishing one integer is atomic in the supported CPython runtime.
        # Readers must not wait behind sequential participant commit calls:
        # they continue at the prior visibility watermark until every ack.
        return self._visible_ts

    def mark_visible(self, commit_ts: int) -> None:
        """Publish bootstrap data written outside the transaction coordinator."""
        with self._tx_lock:
            self._visible_ts = max(self._visible_ts, commit_ts)

    def execute(
        self,
        writes_by_shard: Mapping[str, Mapping[str, Any]],
        start_ts: Optional[int] = None,
        tx_id: Optional[str] = None,
    ) -> TransactionResult:
        with self._tx_lock:
            return self._execute_locked(writes_by_shard, start_ts, tx_id)

    def _execute_locked(
        self,
        writes_by_shard: Mapping[str, Mapping[str, Any]],
        start_ts: Optional[int],
        tx_id: Optional[str],
    ) -> TransactionResult:
        transaction_id = tx_id or uuid.uuid4().hex
        if self._pending_commit:
            return TransactionResult(
                transaction_id,
                TransactionState.ABORTED,
                reason="coordinator recovery required before new transactions",
            )
        snapshot = self._visible_ts if start_ts is None else start_ts
        names = list(writes_by_shard)

        for name in names:
            participant = self.participants[name]
            try:
                vote = participant.prepare(
                    transaction_id, snapshot, writes_by_shard[name]
                )
            except Exception as error:
                self.decision_log.record(
                    transaction_id, TransactionState.ABORTED, names
                )
                self._apply_abort(transaction_id, names)
                return TransactionResult(
                    transaction_id,
                    TransactionState.ABORTED,
                    reason=f"prepare error at {name}: {type(error).__name__}",
                )
            if vote is Vote.NO:
                self.decision_log.record(
                    transaction_id, TransactionState.ABORTED, names
                )
                self._apply_abort(transaction_id, names)
                return TransactionResult(
                    transaction_id,
                    TransactionState.ABORTED,
                    reason=f"prepare rejected: {name}",
                )
        commit_ts = self.clock.tick()
        self.decision_log.record(
            transaction_id, TransactionState.COMMITTED, names, commit_ts
        )
        pending = self._apply_commit(transaction_id, commit_ts, names)
        reason = None
        if pending:
            self._pending_commit = True
            reason = "commit acknowledgement pending: " + ", ".join(pending)
        else:
            self._visible_ts = max(self._visible_ts, commit_ts)
        return TransactionResult(
            transaction_id, TransactionState.COMMITTED, commit_ts, reason
        )

    def _apply_commit(
        self, tx_id: str, commit_ts: int, names: List[str]
    ) -> List[str]:
        pending = []
        for name in names:
            try:
                if not self.participants[name].commit(tx_id, commit_ts):
                    pending.append(name)
            except Exception:
                pending.append(name)
        return pending

    def _apply_abort(self, tx_id: str, names: List[str]) -> None:
        for name in names:
            try:
                self.participants[name].abort(tx_id)
            except Exception:
                # The durable ABORT decision lets recovery retry unavailable shards.
                continue

    def recover(self) -> List[TransactionResult]:
        with self._tx_lock:
            results = []
            still_pending = False
            recovered_visible_ts = self._visible_ts
            records = self.decision_log.all()
            durable_clock_floor = max(
                (
                    int(record["commit_ts"])
                    for record in records.values()
                    if record.get("commit_ts") is not None
                ),
                default=self.clock.now(),
            )
            self.clock.advance_to(durable_clock_floor)
            for tx_id, record in records.items():
                state = TransactionState(record["state"])
                names = record["participants"]
                if state is TransactionState.COMMITTED:
                    commit_ts = int(record["commit_ts"])
                    pending = self._apply_commit(tx_id, commit_ts, names)
                    reason = None
                    if pending:
                        still_pending = True
                        reason = "commit acknowledgement pending: " + ", ".join(pending)
                    else:
                        recovered_visible_ts = max(recovered_visible_ts, commit_ts)
                    results.append(TransactionResult(tx_id, state, commit_ts, reason))
                else:
                    self._apply_abort(tx_id, names)
                    results.append(TransactionResult(tx_id, state))
            if not still_pending:
                self._visible_ts = recovered_visible_ts
            self._pending_commit = still_pending
            return results
