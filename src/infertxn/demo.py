import json
import multiprocessing
import tempfile
from pathlib import Path
from typing import Any, Dict

from .clock import LogicalClock
from .coordinator import DecisionLog, TwoPhaseCoordinator
from .http_service import HTTPParticipantClient, ParticipantHTTPServer
from .migration import InferenceMetadataDB
from .mvcc import MVCCStore
from .participant import Participant


def _serve(
    name: str,
    key: str,
    value: Dict[str, Any],
    connection,
    stop_event,
) -> None:
    store = MVCCStore(name)
    store.seed(key, value, 1)
    server = ParticipantHTTPServer("127.0.0.1", 0, Participant(name, store))
    server.start()
    connection.send(server.url)
    connection.close()
    stop_event.wait()
    server.close()


def run_demo() -> Dict[str, Any]:
    initial = {
        "route": ("route/demo-request", {"decode_node": "decode-a", "epoch": 1}),
        "kv": (
            "kv/demo-request",
            {
                "location": "decode-a",
                "cache_version": 7,
                "state": "ready",
                "epoch": 1,
            },
        ),
        "request": (
            "request/demo-request",
            {
                "owner": "decode-a",
                "phase": "decoding",
                "generated_tokens": 64,
                "epoch": 1,
            },
        ),
    }
    processes = []
    stops = []
    clients = {}
    try:
        for name, (key, value) in initial.items():
            parent, child = multiprocessing.Pipe(duplex=False)
            stop = multiprocessing.Event()
            process = multiprocessing.Process(
                target=_serve, args=(name, key, value, child, stop), daemon=True
            )
            process.start()
            clients[name] = HTTPParticipantClient(name, parent.recv())
            parent.close()
            processes.append(process)
            stops.append(stop)

        clock = LogicalClock(initial=1)
        with tempfile.TemporaryDirectory() as directory:
            coordinator = TwoPhaseCoordinator(
                clock,
                clients,
                DecisionLog(Path(directory) / "decisions.jsonl"),
            )
            database = InferenceMetadataDB(clock, clients, coordinator)
            database.stage_target_cache("demo-request", "decode-b", cache_version=8)
            result = database.migrate("demo-request", "decode-a", "decode-b")
            return {
                "transaction": {
                    "id": result.tx_id,
                    "state": result.state.value,
                    "commit_ts": result.commit_ts,
                },
                "metadata": database.read_state("demo-request"),
            }
    finally:
        for stop in stops:
            stop.set()
        for process in processes:
            process.join(timeout=3)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)


def main() -> None:
    print(json.dumps(run_demo(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
