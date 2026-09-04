from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Vote(str, Enum):
    YES = "yes"
    NO = "no"


class TransactionState(str, Enum):
    UNKNOWN = "unknown"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class TransactionResult:
    tx_id: str
    state: TransactionState
    commit_ts: Optional[int] = None
    reason: Optional[str] = None

