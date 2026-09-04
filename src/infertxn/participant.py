from typing import Any, Mapping

from .models import TransactionState, Vote
from .mvcc import MVCCStore


class CommitAcknowledgementLost(ConnectionError):
    """The participant committed, but its acknowledgement was lost."""


class Participant:
    """A transaction participant with deterministic demo fault injection."""

    def __init__(self, name: str, store: MVCCStore) -> None:
        self.name = name
        self.store = store
        self.fail_next_prepare = False
        self.drop_next_commit_ack = False

    def prepare(
        self, tx_id: str, start_ts: int, writes: Mapping[str, Any]
    ) -> Vote:
        if self.fail_next_prepare:
            self.fail_next_prepare = False
            return Vote.NO
        return self.store.prepare(tx_id, start_ts, writes)

    def commit(self, tx_id: str, commit_ts: int) -> bool:
        committed = self.store.commit(tx_id, commit_ts)
        if self.drop_next_commit_ack:
            self.drop_next_commit_ack = False
            raise CommitAcknowledgementLost(self.name)
        return committed

    def abort(self, tx_id: str) -> bool:
        return self.store.abort(tx_id)

    def status(self, tx_id: str) -> TransactionState:
        return self.store.status(tx_id)

    def read(self, key: str, timestamp=None):
        return self.store.read(key, timestamp)

