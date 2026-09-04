import uuid
from typing import Dict, Mapping

from .clock import LogicalClock
from .coordinator import TwoPhaseCoordinator
from .models import TransactionResult, TransactionState
from .participant import Participant


class StaleEpoch(RuntimeError):
    pass


class InferenceMetadataDB:
    """Inference-specific transaction facade over three metadata shards."""

    def __init__(
        self,
        clock: LogicalClock,
        participants: Mapping[str, Participant],
        coordinator: TwoPhaseCoordinator,
    ) -> None:
        required = {"route", "kv", "request"}
        if set(participants) != required:
            raise ValueError(f"participants must be exactly {sorted(required)}")
        self.clock = clock
        self.participants = dict(participants)
        self.coordinator = coordinator

    def initialize_request(
        self,
        request_id: str,
        node: str,
        cache_version: int,
        generated_tokens: int = 0,
    ) -> None:
        snapshot = self.coordinator.snapshot_ts()
        if self.participants["request"].read(
            self._request_key(request_id), snapshot
        ) is not None:
            raise ValueError(f"request already exists: {request_id}")
        result = self.coordinator.execute(
            {
                "route": {
                    self._route_key(request_id): {"decode_node": node, "epoch": 1}
                },
                "kv": {
                    self._kv_key(request_id): {
                        "location": node,
                        "cache_version": cache_version,
                        "state": "ready",
                        "epoch": 1,
                    }
                },
                "request": {
                    self._request_key(request_id): {
                        "owner": node,
                        "phase": "decoding",
                        "generated_tokens": generated_tokens,
                        "epoch": 1,
                    }
                },
            },
            start_ts=snapshot,
        )
        if result.state is not TransactionState.COMMITTED or result.reason:
            raise RuntimeError(f"request initialization failed: {result.reason}")

    def stage_target_cache(
        self, request_id: str, target: str, cache_version: int
    ) -> TransactionResult:
        """Record that tensor transfer completed before metadata ownership moves."""
        snapshot = self.coordinator.snapshot_ts()
        current = self._read_at(request_id, snapshot)
        return self.coordinator.execute(
            {
                "kv": {
                    self._kv_copy_key(request_id, target): {
                        "location": target,
                        "cache_version": cache_version,
                        "state": "ready",
                        "source_epoch": current["request"]["epoch"],
                        "source_cache_version": current["kv"]["cache_version"],
                        "generated_tokens": current["request"]["generated_tokens"],
                        "target_epoch": current["request"]["epoch"] + 1,
                    }
                }
            },
            start_ts=snapshot,
        )

    def migrate(
        self,
        request_id: str,
        source: str,
        target: str,
    ) -> TransactionResult:
        if source == target:
            return self._rejected("source and target must differ")

        start_ts = self.coordinator.snapshot_ts()
        state = self._read_at(request_id, start_ts)
        staged_cache = self.participants["kv"].read(
            self._kv_copy_key(request_id, target), start_ts
        )
        if not state["consistent"]:
            return self._rejected("metadata shards are inconsistent")
        if state["route"]["decode_node"] != source:
            return self._rejected("source no longer owns request")

        next_epoch = state["request"]["epoch"] + 1
        if (
            staged_cache is None
            or staged_cache.get("state") != "ready"
            or staged_cache.get("source_epoch") != state["request"]["epoch"]
            or staged_cache.get("source_cache_version")
            != state["kv"]["cache_version"]
            or staged_cache.get("target_epoch") != next_epoch
        ):
            return self._rejected(f"target KV cache is not ready for epoch {next_epoch}")
        if staged_cache.get("generated_tokens") != state["request"]["generated_tokens"]:
            return self._rejected("target KV cache is not ready for current progress")
        consumed_cache = {
            **staged_cache,
            "state": "consumed",
            "consumed_epoch": next_epoch,
        }
        writes = {
            "route": {
                self._route_key(request_id): {
                    "decode_node": target,
                    "epoch": next_epoch,
                }
            },
            "kv": {
                self._kv_key(request_id): {
                    "location": target,
                    "cache_version": staged_cache["cache_version"],
                    "state": "ready",
                    "epoch": next_epoch,
                },
                self._kv_copy_key(request_id, target): consumed_cache,
            },
            "request": {
                self._request_key(request_id): {
                    **state["request"],
                    "owner": target,
                    "epoch": next_epoch,
                }
            },
        }
        return self.coordinator.execute(writes, start_ts=start_ts)

    def advance_tokens(
        self, request_id: str, node: str, epoch: int, count: int
    ) -> TransactionResult:
        if count <= 0:
            raise ValueError("count must be positive")
        start_ts = self.coordinator.snapshot_ts()
        current = self.participants["request"].read(
            self._request_key(request_id), start_ts
        )
        if current is None:
            raise KeyError(request_id)
        if current["owner"] != node or current["epoch"] != epoch:
            raise StaleEpoch(
                f"writer ({node}, epoch={epoch}) does not own current epoch"
            )
        updated = {**current, "generated_tokens": current["generated_tokens"] + count}
        return self.coordinator.execute(
            {"request": {self._request_key(request_id): updated}},
            start_ts=start_ts,
        )

    def read_state(self, request_id: str) -> Dict[str, object]:
        return self._read_at(request_id, self.coordinator.snapshot_ts())

    def _read_at(self, request_id: str, timestamp):
        route = self.participants["route"].read(self._route_key(request_id), timestamp)
        kv = self.participants["kv"].read(self._kv_key(request_id), timestamp)
        request = self.participants["request"].read(
            self._request_key(request_id), timestamp
        )
        if route is None or kv is None or request is None:
            raise KeyError(request_id)
        consistent = (
            route["decode_node"] == kv["location"] == request["owner"]
            and route["epoch"] == kv["epoch"] == request["epoch"]
            and kv["state"] == "ready"
        )
        return {"route": route, "kv": kv, "request": request, "consistent": consistent}

    @staticmethod
    def _rejected(reason: str) -> TransactionResult:
        return TransactionResult(
            uuid.uuid4().hex, TransactionState.ABORTED, reason=reason
        )

    @staticmethod
    def _route_key(request_id: str) -> str:
        return f"route/{request_id}"

    @staticmethod
    def _kv_key(request_id: str) -> str:
        return f"kv/{request_id}"

    @staticmethod
    def _request_key(request_id: str) -> str:
        return f"request/{request_id}"

    @staticmethod
    def _kv_copy_key(request_id: str, node: str) -> str:
        return f"kv-copy/{request_id}/{node}"
