"""Deterministic service-fault specifications for backend experiments."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen


def deterministic_fault_matrix(seed: int = 17) -> list[dict[str, Any]]:
    """Return the fixed trigger-to-action matrix used by remote runs."""

    return [
        {"name": "normal", "service": "none", "trigger_operation": "none", "action": "none", "seed": int(seed), "recovery_action": "none"},
        {"name": "delay", "service": "qdrant", "trigger_operation": "write", "action": "delay", "seed": int(seed), "recovery_action": "continue"},
        {"name": "timeout", "service": "qdrant", "trigger_operation": "write", "action": "timeout", "seed": int(seed), "recovery_action": "abort"},
        {"name": "connection_drop", "service": "neo4j", "trigger_operation": "commit", "action": "connection_drop", "seed": int(seed), "recovery_action": "abort"},
        {"name": "retry_success", "service": "qdrant", "trigger_operation": "write", "action": "connection_drop", "seed": int(seed), "recovery_action": "retry_once"},
    ]


class ToxiproxyFaultController:
    """Trigger Toxiproxy actions by service/operation request ordinal."""

    def __init__(
        self,
        scenario: Mapping[str, Any],
        management_url: str | None = None,
        *,
        proxy_routes: Mapping[str, Mapping[str, Any]] | None = None,
        api_requester: Any | None = None,
    ):
        required = ("name", "service", "trigger_operation", "action", "seed")
        if any(key not in scenario for key in required):
            raise ValueError("fault scenario is missing required fields")
        self.scenario = dict(scenario)
        self.management_url = str(management_url or "http://127.0.0.1:8474").rstrip("/")
        self.proxy_routes = {
            str(service): dict(route) for service, route in (proxy_routes or {}).items()
        }
        self.api_requester = api_requester
        self.request_ordinals: dict[tuple[str, str], int] = {}
        self.fired = False
        self._route_evidence: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []
        self._retry_count = 0
        self._retry_success_count = 0
        self._toxic_installed = False
        self._toxic_cleared = False

    def _api_request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.api_requester is not None:
            result = self.api_requester(method, path, payload)
            return dict(result or {})
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.management_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=10) as response:  # noqa: S310 - explicit experiment config
            raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}

    @staticmethod
    def _port(value: str) -> int | None:
        parsed = urlparse(value)
        if parsed.netloc:
            return parsed.port
        match = re.search(r":(\d+)$", value)
        return int(match.group(1)) if match else None

    def verify_proxy_path(self, service: str) -> dict[str, Any]:
        """Verify that the client endpoint enters the configured proxy."""

        service = str(service)
        if service in self._route_evidence:
            evidence = self._route_evidence[service]
            if not evidence["verified"]:
                raise RuntimeError(f"{service} request does not traverse configured proxy")
            return dict(evidence)
        route = self.proxy_routes.get(service)
        if not route:
            evidence = {"service": service, "verified": False, "reason": "missing_proxy_route"}
            self._route_evidence[service] = evidence
            raise RuntimeError(f"{service} request does not traverse configured proxy")
        proxy_name = str(route.get("proxy_name", ""))
        observed = self._api_request("GET", f"/proxies/{proxy_name}")
        expected_listen = str(route.get("listen", ""))
        expected_upstream = str(route.get("upstream", ""))
        client_endpoint = str(route.get("client_endpoint", ""))
        observed_listen = str(observed.get("listen", ""))
        observed_upstream = str(observed.get("upstream", ""))
        verified = bool(
            proxy_name
            and observed.get("name") == proxy_name
            and observed.get("enabled", True) is True
            and self._port(client_endpoint) is not None
            and self._port(client_endpoint) == self._port(observed_listen)
            and self._port(expected_listen) == self._port(observed_listen)
            and expected_upstream == observed_upstream
        )
        evidence = {
            "service": service,
            "proxy_name": proxy_name,
            "client_endpoint": client_endpoint,
            "listen": observed_listen,
            "upstream": observed_upstream,
            "verified": verified,
        }
        self._route_evidence[service] = evidence
        if not verified:
            raise RuntimeError(f"{service} request does not traverse configured proxy")
        return dict(evidence)

    def observe(self, service: str, operation: str) -> dict[str, Any] | None:
        key = (str(service), str(operation))
        ordinal = self.request_ordinals.get(key, 0) + 1
        self.request_ordinals[key] = ordinal
        target_service = str(self.scenario["service"])
        target_operation = str(self.scenario["trigger_operation"])
        target_ordinal = int(self.scenario.get("trigger_ordinal", 1))
        if self.fired or service != target_service or operation != target_operation or ordinal != target_ordinal:
            return None
        self.fired = True
        return {
            "scenario": str(self.scenario["name"]),
            "service": str(service),
            "operation": str(operation),
            "request_ordinal": ordinal,
            "action": str(self.scenario["action"]),
            "seed": int(self.scenario["seed"]),
            "recovery_action": self.scenario.get("recovery_action", "abort"),
        }

    def install(self, proxy_name: str, toxic_name: str = "txnmem_fault") -> dict[str, Any]:
        """Install the selected toxic through Toxiproxy's HTTP API."""

        action = str(self.scenario["action"])
        if action == "delay":
            toxic = {"name": toxic_name, "type": "latency", "stream": "downstream", "toxicity": 1.0, "attributes": {"latency": 100}}
        elif action == "timeout":
            toxic = {"name": toxic_name, "type": "timeout", "stream": "downstream", "toxicity": 1.0, "attributes": {"timeout": 1000}}
        elif action == "connection_drop":
            toxic = {"name": toxic_name, "type": "reset_peer", "stream": "downstream", "toxicity": 1.0, "attributes": {}}
        else:
            return {"installed": False, "action": action, "reason": "no_toxic_for_action"}
        response = self._api_request(
            "POST", f"/proxies/{proxy_name}/toxics", toxic
        )
        self._toxic_installed = True
        return {"installed": True, "action": action, "response": response, "toxic": toxic}

    def clear(self, proxy_name: str, toxic_name: str = "txnmem_fault") -> None:
        self._api_request(
            "DELETE", f"/proxies/{proxy_name}/toxics/{toxic_name}"
        )
        self._toxic_cleared = True

    def request(
        self,
        service: str,
        operation: str,
        function: Any,
        request_key: str,
    ) -> Any:
        """Run one backend request, installing a toxic at the exact trigger."""

        route = self.verify_proxy_path(service)
        trigger = self.observe(service, operation)
        if trigger is None:
            return function()

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", str(self.scenario["name"]))
        toxic_name = f"txnmem_{safe_name}"
        installed = self.install(str(route["proxy_name"]), toxic_name)
        event = {
            **trigger,
            "request_key_sha256": hashlib.sha256(
                str(request_key).encode("utf-8")
            ).hexdigest(),
            "proxy_name": route["proxy_name"],
            "toxic_name": toxic_name,
            "toxic_type": installed.get("toxic", {}).get("type"),
            "toxic_installed": bool(installed.get("installed")),
            "proxy_path_verified": bool(route["verified"]),
            "fault_observed": False,
        }
        started = time.perf_counter()
        try:
            result = function()
        except Exception as exc:
            event["fault_observed"] = True
            event["observed_exception"] = type(exc).__name__
            self.clear(str(route["proxy_name"]), toxic_name)
            event["toxic_cleared"] = True
            if str(trigger.get("recovery_action")) != "retry_once":
                event["operation_elapsed_ms"] = (time.perf_counter() - started) * 1000.0
                self._events.append(event)
                raise
            self._retry_count += 1
            try:
                result = function()
            except Exception as retry_exc:
                event["retry_exception"] = type(retry_exc).__name__
                event["operation_elapsed_ms"] = (time.perf_counter() - started) * 1000.0
                self._events.append(event)
                raise
            self._retry_success_count += 1
            event["retry_success"] = True
        else:
            event["fault_observed"] = str(self.scenario["action"]) == "delay"
            self.clear(str(route["proxy_name"]), toxic_name)
            event["toxic_cleared"] = True
        event["operation_elapsed_ms"] = (time.perf_counter() - started) * 1000.0
        self._events.append(event)
        return result

    def __call__(self, service: str, operation: str, function: Any, request_key: str) -> Any:
        return self.request(service, operation, function, request_key)

    def evidence(self) -> dict[str, Any]:
        normal = str(self.scenario["action"]) == "none"
        route_verified = bool(self._route_evidence) and all(
            bool(item.get("verified")) for item in self._route_evidence.values()
        )
        target_route_verified = normal or bool(
            self._route_evidence.get(str(self.scenario["service"]), {}).get("verified")
        )
        fault_observed = any(bool(event.get("fault_observed")) for event in self._events)
        if normal:
            evidence_valid = route_verified and not self.fired
        else:
            evidence_valid = bool(
                self.fired
                and self._toxic_installed
                and self._toxic_cleared
                and target_route_verified
                and fault_observed
            )
        return {
            "scenario": str(self.scenario["name"]),
            "trigger_fired": self.fired,
            "toxic_installed": self._toxic_installed,
            "toxic_cleared": self._toxic_cleared,
            "proxy_path_verified": route_verified and target_route_verified,
            "fault_observed": fault_observed,
            "retry_count": self._retry_count,
            "retry_success_count": self._retry_success_count,
            "request_ordinals": {
                f"{service}:{operation}": ordinal
                for (service, operation), ordinal in sorted(self.request_ordinals.items())
            },
            "proxy_routes": {
                service: dict(item) for service, item in sorted(self._route_evidence.items())
            },
            "events": [dict(event) for event in self._events],
            "evidence_valid": evidence_valid,
        }
