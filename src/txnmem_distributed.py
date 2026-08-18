"""Dependency-free cross-process backend linearization smoke harness."""

from __future__ import annotations

import copy
import multiprocessing
import time
from typing import Any, Iterable

from txnmem_backend import AgentReplayRunner, InstrumentedMemoryBackend


def _worker_main(
    worker_id: str,
    actions: list[dict[str, Any]],
    command_queue: Any,
    response_queue: Any,
) -> None:
    for local_index, action in enumerate(actions, start=1):
        operation_id = str(action.get("op_id") or f"{worker_id}:op:{local_index:04d}")
        command_queue.put(
            {
                "type": "action",
                "worker_id": worker_id,
                "local_index": local_index,
                "operation_id": operation_id,
                "action": dict(action),
            }
        )
        response = response_queue.get()
        if response.get("ok"):
            continue
        command_queue.put(
            {
                "type": "worker_failed",
                "worker_id": worker_id,
                "operation_id": operation_id,
            }
        )
        return
    command_queue.put({"type": "worker_done", "worker_id": worker_id})


def _owner_main(
    command_queue: Any,
    response_queues: dict[str, Any],
    worker_count: int,
    submitted_operation_count: int,
    report_queue: Any,
) -> None:
    backend = InstrumentedMemoryBackend()
    runner = AgentReplayRunner(backend)
    completed_workers: set[str] = set()
    failed_workers: set[str] = set()
    unacknowledged: set[str] = set()
    linearization_index = 0

    while len(completed_workers) < worker_count:
        message = command_queue.get()
        message_type = message.get("type")
        worker_id = str(message.get("worker_id"))
        if message_type == "action":
            operation_id = str(message["operation_id"])
            local_index = int(message["local_index"])
            try:
                before_count = len(backend.events)
                runner.run([message["action"]])
                new_events = backend.events[before_count:]
                for event in new_events:
                    linearization_index += 1
                    event["linearization_index"] = linearization_index
                    event["worker_id"] = worker_id
                    event["operation_id"] = operation_id
                    event["local_index"] = local_index
                response_queues[worker_id].put({"ok": True})
            except Exception as exc:  # pragma: no cover - exact exception is backend-dependent
                failed_workers.add(worker_id)
                unacknowledged.add(operation_id)
                response_queues[worker_id].put(
                    {
                        "ok": False,
                        "error_type": type(exc).__name__,
                    }
                )
        elif message_type == "worker_failed":
            failed_workers.add(worker_id)
            unacknowledged.add(str(message["operation_id"]))
            completed_workers.add(worker_id)
        elif message_type == "worker_done":
            completed_workers.add(worker_id)

    events = copy.deepcopy(backend.events)
    report_queue.put(
        {
            "concurrency_model": "process_backend_linearization",
            "worker_count": worker_count,
            "submitted_operation_count": submitted_operation_count,
            "event_count": len(events),
            "unique_event_ids": len({event["event_id"] for event in events}) == len(events),
            "completed": not failed_workers and len(completed_workers) == worker_count,
            "failed_worker_ids": sorted(failed_workers),
            "unacknowledged_operation_ids": sorted(unacknowledged),
            "events": events,
            "final_memories": backend.snapshot(),
        }
    )


def _empty_report() -> dict[str, Any]:
    return {
        "concurrency_model": "process_backend_linearization",
        "worker_count": 0,
        "submitted_operation_count": 0,
        "event_count": 0,
        "unique_event_ids": True,
        "completed": True,
        "failed_worker_ids": [],
        "unacknowledged_operation_ids": [],
        "events": [],
        "final_memories": {},
    }


def run_process_action_sequences(
    sequences: Iterable[Iterable[dict[str, Any]]], timeout_s: float = 5.0
) -> dict[str, Any]:
    """Run worker sequences and return the owner process's linearization report."""

    materialized = [list(sequence) for sequence in sequences]
    if not materialized:
        return _empty_report()
    if timeout_s <= 0:
        raise ValueError("timeout_s must be positive")

    context = multiprocessing.get_context("spawn")
    command_queue = context.Queue()
    response_queues = {f"worker_{index}": context.Queue() for index in range(len(materialized))}
    report_queue = context.Queue()
    owner = context.Process(
        target=_owner_main,
        args=(
            command_queue,
            response_queues,
            len(materialized),
            sum(len(actions) for actions in materialized),
            report_queue,
        ),
        name="txnmem-backend-owner",
    )
    workers = [
        context.Process(
            target=_worker_main,
            args=(f"worker_{index}", actions, command_queue, response_queues[f"worker_{index}"]),
            name=f"txnmem-worker-{index}",
        )
        for index, actions in enumerate(materialized)
    ]
    owner.start()
    for worker in workers:
        worker.start()

    deadline = time.monotonic() + timeout_s
    for worker in workers:
        worker.join(max(0.0, deadline - time.monotonic()))
    owner.join(max(0.0, deadline - time.monotonic()))

    alive = [process for process in [owner, *workers] if process.is_alive()]
    if alive:
        for process in alive:
            process.terminate()
        for process in alive:
            process.join()
        return {
            "concurrency_model": "process_backend_linearization",
            "worker_count": len(materialized),
            "submitted_operation_count": sum(len(actions) for actions in materialized),
            "event_count": 0,
            "unique_event_ids": True,
            "completed": False,
            "failed_worker_ids": [process.name for process in alive],
            "unacknowledged_operation_ids": [
                str(action.get("op_id") or f"worker_{index}:op:{local_index:04d}")
                for index, actions in enumerate(materialized)
                for local_index, action in enumerate(actions, start=1)
            ],
            "events": [],
            "final_memories": {},
        }
    try:
        return report_queue.get(timeout=1.0)
    except Exception:
        return {
            "concurrency_model": "process_backend_linearization",
            "worker_count": len(materialized),
            "submitted_operation_count": sum(len(actions) for actions in materialized),
            "event_count": 0,
            "unique_event_ids": True,
            "completed": False,
            "failed_worker_ids": [worker.name for worker in workers if worker.exitcode not in {0, None}],
            "unacknowledged_operation_ids": [],
            "events": [],
            "final_memories": {},
        }
