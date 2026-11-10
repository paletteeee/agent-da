"""Fault schedule validation tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import ParseResult, urlparse as stdlib_urlparse

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

    def test_proxy_path_accepts_python_3_10_bare_listen_parse(self):
        api = _FakeToxiproxyAPI()
        controller = ToxiproxyFaultController(
            {
                "name": "delay",
                "service": "qdrant",
                "trigger_operation": "write",
                "action": "delay",
                "seed": 17,
                "recovery_action": "continue",
            },
            proxy_routes=_routes(),
            api_requester=api,
        )

        def python_3_10_urlparse(value):
            if value == "0.0.0.0:19000":
                return ParseResult("0.0.0.0", "", "19000", "", "", "")
            return stdlib_urlparse(value)

        with patch("txnmem_service_faults.urlparse", side_effect=python_3_10_urlparse):
            evidence = controller.verify_proxy_path("qdrant")

        self.assertTrue(evidence["verified"])

    def test_proxy_path_rejects_malformed_client_endpoints_even_when_ports_match(self):
        invalid_endpoints = {
            "scheme without authority": "http:127.0.0.1:19000",
            "path": "junk/path:19000",
            "zero port": "0.0.0.0:0",
            "out of range port": "0.0.0.0:99999",
            "credentials": "http://user:pass@127.0.0.1:19000",
            "query": "http://127.0.0.1:19000?query=value",
            "fragment": "http://127.0.0.1:19000#fragment",
            "parameters": "http://127.0.0.1:19000;param",
        }
        for description, endpoint in invalid_endpoints.items():
            with self.subTest(description=description):
                api = _FakeToxiproxyAPI()
                routes = _routes()
                routes["qdrant"]["client_endpoint"] = endpoint
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
                    controller.verify_proxy_path("qdrant")

    def test_proxy_path_rejects_malformed_listen_and_upstream_endpoints(self):
        invalid_endpoints = (
            "http:127.0.0.1:19000",
            "junk/path:19000",
            "0.0.0.0:0",
            "0.0.0.0:99999",
        )
        for field in ("listen", "upstream"):
            for endpoint in invalid_endpoints:
                with self.subTest(field=field, endpoint=endpoint):
                    api = _FakeToxiproxyAPI()
                    routes = _routes()
                    routes["qdrant"][field] = endpoint
                    api.proxies["txnmem-qdrant"][field] = endpoint
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
                        controller.verify_proxy_path("qdrant")

    def test_proxy_path_requires_enabled_and_complete_matching_upstream(self):
        cases = {
            "missing enabled": lambda api, routes: api.proxies["txnmem-qdrant"].pop("enabled"),
            "disabled": lambda api, routes: api.proxies["txnmem-qdrant"].update(enabled=False),
            "missing expected upstream": lambda api, routes: routes["qdrant"].update(upstream=""),
            "missing observed upstream": lambda api, routes: api.proxies["txnmem-qdrant"].pop("upstream"),
            "both upstreams missing": lambda api, routes: (
                routes["qdrant"].update(upstream=""),
                api.proxies["txnmem-qdrant"].pop("upstream"),
            ),
            "wrong upstream": lambda api, routes: api.proxies["txnmem-qdrant"].update(upstream="other:6333"),
        }
        for description, configure in cases.items():
            with self.subTest(description=description):
                api = _FakeToxiproxyAPI()
                routes = _routes()
                configure(api, routes)
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
                    controller.verify_proxy_path("qdrant")

    def test_proxy_path_requires_matching_listen_host(self):
        api = _FakeToxiproxyAPI()
        routes = _routes()
        api.proxies["txnmem-qdrant"]["listen"] = "127.0.0.1:19000"
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
            controller.verify_proxy_path("qdrant")

    def test_proxy_path_normalizes_valid_endpoint_hostnames(self):
        api = _FakeToxiproxyAPI()
        routes = _routes()
        routes["qdrant"]["upstream"] = "http://QDRANT:6333"
        api.proxies["txnmem-qdrant"]["upstream"] = "http://qdrant:6333"
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

        self.assertTrue(controller.verify_proxy_path("qdrant")["verified"])

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
