"""Deterministic service-fault specifications for backend experiments."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
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

    def __init__(self, scenario: Mapping[str, Any], management_url: str | None = None):
        required = ("name", "service", "trigger_operation", "action", "seed")
        if any(key not in scenario for key in required):
            raise ValueError("fault scenario is missing required fields")
        self.scenario = dict(scenario)
        self.management_url = str(management_url or "http://127.0.0.1:8474").rstrip("/")
        self.request_ordinals: dict[tuple[str, str], int] = {}
        self.fired = False

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
        request = Request(
            f"{self.management_url}/proxies/{proxy_name}/toxics",
            data=json.dumps(toxic).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=10) as response:  # noqa: S310 - management URL is explicit config
            payload = response.read().decode("utf-8")
        return {"installed": True, "action": action, "response": json.loads(payload) if payload else {}}

    def clear(self, proxy_name: str, toxic_name: str = "txnmem_fault") -> None:
        request = Request(
            f"{self.management_url}/proxies/{proxy_name}/toxics/{toxic_name}",
            method="DELETE",
        )
        with urlopen(request, timeout=10):  # noqa: S310 - management URL is explicit config
            return
