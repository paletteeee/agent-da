import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Dict, Mapping, Optional
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from .models import TransactionState, Vote
from .participant import CommitAcknowledgementLost, Participant


class _ParticipantHandler(BaseHTTPRequestHandler):
    participant: Participant

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/read":
            query = parse_qs(parsed.query)
            timestamp = query.get("timestamp", [None])[0]
            value = self.participant.read(
                query["key"][0], None if timestamp is None else int(timestamp)
            )
            self._send(200, {"value": value})
            return
        if parsed.path.startswith("/status/"):
            tx_id = parsed.path.split("/", 2)[2]
            self._send(200, {"state": self.participant.status(tx_id).value})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        payload = self._read_json()
        try:
            if self.path == "/prepare":
                vote = self.participant.prepare(
                    payload["tx_id"], int(payload["start_ts"]), payload["writes"]
                )
                self._send(200, {"vote": vote.value})
                return
            if self.path == "/commit":
                committed = self.participant.commit(
                    payload["tx_id"], int(payload["commit_ts"])
                )
                self._send(200, {"committed": committed})
                return
            if self.path == "/abort":
                self._send(
                    200, {"aborted": self.participant.abort(payload["tx_id"])}
                )
                return
            if self.path == "/faults":
                self.participant.fail_next_prepare = bool(
                    payload.get("fail_next_prepare", False)
                )
                self.participant.drop_next_commit_ack = bool(
                    payload.get("drop_next_commit_ack", False)
                )
                self._send(200, {"configured": True})
                return
        except CommitAcknowledgementLost:
            # Deliberately close without an HTTP response: the client cannot
            # distinguish a lost acknowledgement from a failed commit.
            self.close_connection = True
            return
        self._send(404, {"error": "not found"})

    def _read_json(self) -> Dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(size) or b"{}")

    def _send(self, status: int, payload: Mapping[str, Any]) -> None:
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        return


class ParticipantHTTPServer:
    def __init__(self, host: str, port: int, participant: Participant) -> None:
        handler = type(
            f"{participant.name.title()}Handler",
            (_ParticipantHandler,),
            {"participant": participant},
        )
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread: Optional[Thread] = None

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("server already started")
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)


class HTTPParticipantClient:
    def __init__(self, name: str, base_url: str, timeout: float = 2.0) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def prepare(
        self, tx_id: str, start_ts: int, writes: Mapping[str, Any]
    ) -> Vote:
        data = self._post(
            "/prepare", {"tx_id": tx_id, "start_ts": start_ts, "writes": writes}
        )
        return Vote(data["vote"])

    def commit(self, tx_id: str, commit_ts: int) -> bool:
        try:
            data = self._post(
                "/commit", {"tx_id": tx_id, "commit_ts": commit_ts}
            )
        except HTTPError as error:
            if error.code == 503:
                raise CommitAcknowledgementLost(self.name) from error
            raise
        return bool(data["committed"])

    def abort(self, tx_id: str) -> bool:
        return bool(self._post("/abort", {"tx_id": tx_id})["aborted"])

    def status(self, tx_id: str) -> TransactionState:
        return TransactionState(self._get(f"/status/{quote(tx_id)}")["state"])

    def read(self, key: str, timestamp: Optional[int] = None) -> Any:
        path = f"/read?key={quote(key)}"
        if timestamp is not None:
            path += f"&timestamp={timestamp}"
        return self._get(path)["value"]

    def configure_faults(
        self, fail_next_prepare: bool = False, drop_next_commit_ack: bool = False
    ) -> None:
        self._post(
            "/faults",
            {
                "fail_next_prepare": fail_next_prepare,
                "drop_next_commit_ack": drop_next_commit_ack,
            },
        )

    def _get(self, path: str) -> Dict[str, Any]:
        with urlopen(self.base_url + path, timeout=self.timeout) as response:
            return json.load(response)

    def _post(self, path: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        request = Request(
            self.base_url + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.load(response)
