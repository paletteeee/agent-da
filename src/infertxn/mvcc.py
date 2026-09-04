from copy import deepcopy
from dataclasses import dataclass
from threading import RLock
from typing import Any, Dict, List, Mapping, Optional

from .models import TransactionState, Vote


@dataclass(frozen=True)
class Version:
    commit_ts: int
    value: Any


@dataclass
class PreparedWrite:
    start_ts: int
    writes: Dict[str, Any]


class MVCCStore:
    """In-memory snapshot-isolation store with prepared-key locking."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._versions: Dict[str, List[Version]] = {}
        self._prepared: Dict[str, PreparedWrite] = {}
        self._locks: Dict[str, str] = {}
        self._decisions: Dict[str, TransactionState] = {}
        self._commit_timestamps: Dict[str, int] = {}
        self._mutex = RLock()

    def seed(self, key: str, value: Any, commit_ts: int) -> None:
        with self._mutex:
            versions = self._versions.setdefault(key, [])
            if versions and commit_ts <= versions[-1].commit_ts:
                raise ValueError("seed timestamp must be newer than existing versions")
            versions.append(Version(commit_ts, deepcopy(value)))

    def read(self, key: str, timestamp: Optional[int] = None) -> Any:
        with self._mutex:
            versions = self._versions.get(key, [])
            for version in reversed(versions):
                if timestamp is None or version.commit_ts <= timestamp:
                    return deepcopy(version.value)
            return None

    def prepare(
        self, tx_id: str, start_ts: int, writes: Mapping[str, Any]
    ) -> Vote:
        with self._mutex:
            state = self._decisions.get(tx_id)
            if state in (TransactionState.PREPARED, TransactionState.COMMITTED):
                return Vote.YES
            if state is TransactionState.ABORTED:
                return Vote.NO

            for key in writes:
                owner = self._locks.get(key)
                if owner is not None and owner != tx_id:
                    return Vote.NO
                versions = self._versions.get(key, [])
                if versions and versions[-1].commit_ts > start_ts:
                    return Vote.NO

            copied = {key: deepcopy(value) for key, value in writes.items()}
            self._prepared[tx_id] = PreparedWrite(start_ts, copied)
            for key in copied:
                self._locks[key] = tx_id
            self._decisions[tx_id] = TransactionState.PREPARED
            return Vote.YES

    def commit(self, tx_id: str, commit_ts: int) -> bool:
        with self._mutex:
            state = self._decisions.get(tx_id)
            if state is TransactionState.COMMITTED:
                return self._commit_timestamps[tx_id] == commit_ts
            if state is not TransactionState.PREPARED:
                return False

            prepared = self._prepared.pop(tx_id)
            for key, value in prepared.writes.items():
                self._versions.setdefault(key, []).append(
                    Version(commit_ts, deepcopy(value))
                )
                self._locks.pop(key, None)
            self._decisions[tx_id] = TransactionState.COMMITTED
            self._commit_timestamps[tx_id] = commit_ts
            return True

    def abort(self, tx_id: str) -> bool:
        with self._mutex:
            state = self._decisions.get(tx_id)
            if state is TransactionState.COMMITTED:
                return False
            if state is TransactionState.ABORTED:
                return True
            prepared = self._prepared.pop(tx_id, None)
            if prepared:
                for key in prepared.writes:
                    if self._locks.get(key) == tx_id:
                        self._locks.pop(key)
            self._decisions[tx_id] = TransactionState.ABORTED
            return True

    def status(self, tx_id: str) -> TransactionState:
        with self._mutex:
            return self._decisions.get(tx_id, TransactionState.UNKNOWN)

    def version_count(self, key: str) -> int:
        with self._mutex:
            return len(self._versions.get(key, []))
