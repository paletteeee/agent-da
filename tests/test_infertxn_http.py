import tempfile
import unittest
from pathlib import Path

from src.infertxn.clock import LogicalClock
from src.infertxn.coordinator import DecisionLog, TwoPhaseCoordinator
from src.infertxn.http_service import HTTPParticipantClient, ParticipantHTTPServer
from src.infertxn.migration import InferenceMetadataDB
from src.infertxn.models import TransactionState, Vote
from src.infertxn.mvcc import MVCCStore
from src.infertxn.participant import Participant


class ParticipantHTTPTests(unittest.TestCase):
    def test_real_http_client_prepares_commits_and_reads(self):
        store = MVCCStore("route")
        store.seed("route/r1", {"decode_node": "a", "epoch": 1}, 1)
        server = ParticipantHTTPServer(
            "127.0.0.1", 0, Participant("route", store)
        )
        server.start()
        self.addCleanup(server.close)
        client = HTTPParticipantClient("route", server.url)

        vote = client.prepare(
            "tx-http",
            2,
            {"route/r1": {"decode_node": "b", "epoch": 2}},
        )
        committed = client.commit("tx-http", 3)

        self.assertEqual(vote, Vote.YES)
        self.assertTrue(committed)
        self.assertEqual(client.status("tx-http"), TransactionState.COMMITTED)
        self.assertEqual(
            client.read("route/r1"), {"decode_node": "b", "epoch": 2}
        )

    def test_http_backed_database_initializes_all_shards_transactionally(self):
        servers = []
        clients = {}
        for name in ("route", "kv", "request"):
            server = ParticipantHTTPServer(
                "127.0.0.1", 0, Participant(name, MVCCStore(name))
            )
            server.start()
            servers.append(server)
            clients[name] = HTTPParticipantClient(name, server.url)
        self.addCleanup(lambda: [server.close() for server in servers])

        with tempfile.TemporaryDirectory() as directory:
            clock = LogicalClock()
            coordinator = TwoPhaseCoordinator(
                clock, clients, DecisionLog(Path(directory) / "decisions.jsonl")
            )
            database = InferenceMetadataDB(clock, clients, coordinator)
            database.initialize_request("r-http", "decode-a", 4)

            state = database.read_state("r-http")

        self.assertTrue(state["consistent"])
        self.assertEqual(state["request"]["owner"], "decode-a")

    def test_lost_http_commit_response_does_not_skip_later_shard(self):
        route_store = MVCCStore("route")
        request_store = MVCCStore("request")
        route_store.seed("route/r1", {"node": "a"}, 1)
        request_store.seed("request/r1", {"owner": "a"}, 1)
        route_server = ParticipantHTTPServer(
            "127.0.0.1", 0, Participant("route", route_store)
        )
        request_server = ParticipantHTTPServer(
            "127.0.0.1", 0, Participant("request", request_store)
        )
        route_server.start()
        request_server.start()
        self.addCleanup(route_server.close)
        self.addCleanup(request_server.close)
        clients = {
            "route": HTTPParticipantClient("route", route_server.url),
            "request": HTTPParticipantClient("request", request_server.url),
        }
        clients["route"].configure_faults(drop_next_commit_ack=True)

        with tempfile.TemporaryDirectory() as directory:
            coordinator = TwoPhaseCoordinator(
                LogicalClock(1),
                clients,
                DecisionLog(Path(directory) / "decisions.jsonl"),
            )
            result = coordinator.execute(
                {
                    "route": {"route/r1": {"node": "b"}},
                    "request": {"request/r1": {"owner": "b"}},
                }
            )

        self.assertEqual(result.state, TransactionState.COMMITTED)
        self.assertEqual(result.reason, "commit acknowledgement pending: route")
        self.assertEqual(clients["request"].read("request/r1"), {"owner": "b"})


if __name__ == "__main__":
    unittest.main()
