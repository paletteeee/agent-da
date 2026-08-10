"""Fault schedule validation tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_service_faults import ToxiproxyFaultController, deterministic_fault_matrix


class _FakeToxiproxyAPI:
    def __init__(self):
        self.calls = []
        self.proxies = {
            "txnmem-qdrant": {
                "name": "txnmem-qdrant",
                "listen": "0.0.0.0:19000",
                "upstream": "qdrant:6333",
                "enabled": True,
            },
            "txnmem-neo4j": {
                "name": "txnmem-neo4j",
                "listen": "0.0.0.0:19001",
                "upstream": "neo4j:7687",
                "enabled": True,
            },
        }

    def __call__(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        parts = path.strip("/").split("/")
        if method == "GET" and len(parts) == 2:
            return dict(self.proxies[parts[1]])
        if method == "POST" and parts[-1] == "toxics":
            return dict(payload or {})
        if method == "DELETE" and "toxics" in parts:
            return {}
        raise AssertionError((method, path, payload))


def _routes():
    return {
        "qdrant": {
            "proxy_name": "txnmem-qdrant",
            "client_endpoint": "http://127.0.0.1:19000",
            "listen": "0.0.0.0:19000",
            "upstream": "qdrant:6333",
        },
        "neo4j": {
            "proxy_name": "txnmem-neo4j",
            "client_endpoint": "bolt://127.0.0.1:19001",
            "listen": "0.0.0.0:19001",
            "upstream": "neo4j:7687",
        },
    }


class ServiceFaultTests(unittest.TestCase):
    def test_fault_matrix_has_triggered_actions_and_stable_seed(self):
        first = deterministic_fault_matrix(seed=17)
        second = deterministic_fault_matrix(seed=17)
        self.assertEqual(first, second)
        self.assertEqual([item["name"] for item in first], ["normal", "delay", "timeout", "connection_drop", "retry_success"])
        self.assertTrue(all("trigger_operation" in item for item in first))

    def test_controller_fires_on_operation_ordinal_not_wall_clock(self):
        controller = ToxiproxyFaultController(
            {"name": "drop-on-second-write", "service": "qdrant", "trigger_operation": "write", "trigger_ordinal": 2, "action": "connection_drop", "seed": 17}
        )
        self.assertIsNone(controller.observe("qdrant", "write"))
        fired = controller.observe("qdrant", "write")
        self.assertEqual(fired["request_ordinal"], 2)
        self.assertEqual(fired["action"], "connection_drop")
        self.assertIsNone(controller.observe("qdrant", "write"))

    def test_requester_installs_toxic_on_verified_proxy_and_retries_only_after_clear(self):
        api = _FakeToxiproxyAPI()
        controller = ToxiproxyFaultController(
            {
                "name": "retry-success",
                "service": "qdrant",
                "trigger_operation": "write",
                "action": "connection_drop",
                "seed": 17,
                "recovery_action": "retry_once",
            },
            proxy_routes=_routes(),
            api_requester=api,
        )
        attempts = []

        def operation():
            attempts.append("call")
            if len(attempts) == 1:
                raise ConnectionResetError("injected reset")
            return "committed"

        self.assertEqual(
            controller.request("qdrant", "write", operation, "request-key"),
            "committed",
        )
        evidence = controller.evidence()

        self.assertEqual(len(attempts), 2)
        self.assertTrue(evidence["trigger_fired"])
        self.assertTrue(evidence["toxic_installed"])
        self.assertTrue(evidence["toxic_cleared"])
        self.assertTrue(evidence["proxy_path_verified"])
        self.assertEqual(evidence["retry_count"], 1)
        self.assertEqual(evidence["retry_success_count"], 1)
        self.assertTrue(evidence["evidence_valid"])
        install_index = next(
            index for index, call in enumerate(api.calls)
            if call[0] == "POST" and call[1].endswith("/toxics")
        )
        clear_index = next(
            index for index, call in enumerate(api.calls)
            if call[0] == "DELETE" and "/toxics/" in call[1]
        )
        self.assertLess(install_index, clear_index)

    def test_wrong_client_port_fails_proxy_path_evidence_closed(self):
        api = _FakeToxiproxyAPI()
        routes = _routes()
        routes["qdrant"]["client_endpoint"] = "http://127.0.0.1:6333"
        controller = ToxiproxyFaultController(
            {
                "name": "delay",
                "service": "qdrant",
                "trigger_operation": "write",
                "action": "delay",
                "seed": 17,
                "recovery_action": "continue",
            },
            proxy_routes=routes,
            api_requester=api,
        )

        with self.assertRaisesRegex(RuntimeError, "does not traverse configured proxy"):
            controller.request("qdrant", "write", lambda: None, "key")
        self.assertFalse(controller.evidence()["evidence_valid"])


if __name__ == "__main__":
    unittest.main()
