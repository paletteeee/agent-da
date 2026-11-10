"""Threaded Agent replay harness with explicit backend linearization logging."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, RLock
from typing import Any, Iterable

from txnmem_backend import AgentReplayRunner, InstrumentedMemoryBackend


class ThreadSafeBackend:
    """Serialize each backend call while preserving the actual lock order."""

    def __init__(self, backend: InstrumentedMemoryBackend | None = None):
        self.backend = backend or InstrumentedMemoryBackend()
        self._lock = RLock()

    @property
    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.backend.events)

    @property
    def memories(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return self.backend.snapshot()

    def __getattr__(self, name: str) -> Any:
        method = getattr(self.backend, name)
        if not callable(method):
            return method

        def guarded(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return method(*args, **kwargs)

        return guarded


def run_concurrent_action_sequences(
    sequences: Iterable[Iterable[dict[str, Any]]],
    backend: InstrumentedMemoryBackend | None = None,
) -> dict[str, Any]:
    """Run per-agent action sequences concurrently and return the lock order."""

    materialized = [list(sequence) for sequence in sequences]
    if not materialized:
        return {
            "concurrency_model": "threaded_backend_linearization",
            "agent_count": 0,
            "operation_count": 0,
            "event_count": 0,
            "events": [],
            "final_memories": {},
        }
    safe_backend = ThreadSafeBackend(backend)
    start_barrier = Barrier(len(materialized))

    def worker(actions: list[dict[str, Any]]) -> None:
        start_barrier.wait()
        AgentReplayRunner(safe_backend).run(actions)

    with ThreadPoolExecutor(max_workers=len(materialized)) as executor:
        futures = [executor.submit(worker, actions) for actions in materialized]
        for future in futures:
            future.result()
    events = safe_backend.events
    for index, event in enumerate(events, start=1):
        event["linearization_index"] = index
    return {
        "concurrency_model": "threaded_backend_linearization",
        "agent_count": len(materialized),
        "operation_count": sum(len(sequence) for sequence in materialized),
        "event_count": len(events),
        "unique_event_ids": len({event["event_id"] for event in events}) == len(events),
        "events": events,
        "final_memories": safe_backend.memories,
    }
