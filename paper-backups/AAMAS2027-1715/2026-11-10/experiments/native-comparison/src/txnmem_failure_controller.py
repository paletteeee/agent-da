"""Trigger-based fault and policy injection for native Agent runs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any


class FailureInjectionError(RuntimeError):
    """A deterministic failure schedule action was fired."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


_ACTIONS = frozenset({"crash", "policy_revoke", "invalidate", "delay"})


def validate_failure_schedule(schedule: Any) -> list[dict[str, Any]]:
    if not isinstance(schedule, list):
        raise ValueError("failure_schedule must be a list")
    normalized = []
    for index, entry in enumerate(schedule, start=1):
        if not isinstance(entry, Mapping):
            raise ValueError(f"failure schedule entry {index} must be a mapping")
        trigger = entry.get("trigger")
        action = entry.get("action")
        if not isinstance(trigger, Mapping) or not isinstance(action, Mapping):
            raise ValueError(f"failure schedule entry {index} needs trigger and action mappings")
        kind = trigger.get("kind")
        count = trigger.get("count", 1)
        action_type = action.get("type")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError(f"failure schedule entry {index} has invalid trigger kind")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError(f"failure schedule entry {index} has invalid trigger count")
        if action_type not in _ACTIONS:
            raise ValueError(f"failure schedule entry {index} has unsupported action")
        normalized.append({"trigger": dict(trigger), "action": dict(action)})
    return normalized


class FailureController:
    """Observe canonical events and fire each matching schedule entry once."""

    def __init__(self, schedule: list[Mapping[str, Any]] | None = None):
        self.schedule = validate_failure_schedule(list(schedule or []))
        self.counts: Counter[str] = Counter()
        self.fired: set[int] = set()

    def is_revoked(self, action: str, gateway: Any) -> bool:
        return action in getattr(gateway, "revoked_actions", set())

    def observe(self, event: Mapping[str, Any], *, backend: Any = None, gateway: Any = None) -> list[dict[str, Any]]:
        if not isinstance(event, Mapping):
            raise ValueError("observed event must be a mapping")
        kind = event.get("kind")
        if not isinstance(kind, str):
            return []
        self.counts[kind] += 1
        fired: list[dict[str, Any]] = []
        for index, entry in enumerate(self.schedule):
            if index in self.fired:
                continue
            trigger = entry["trigger"]
            if trigger.get("kind") != kind or int(trigger.get("count", 1)) != self.counts[kind]:
                continue
            if trigger.get("after_step") is not None and int(event.get("step", 0)) < int(trigger["after_step"]):
                continue
            self.fired.add(index)
            action = entry["action"]
            action_type = action["type"]
            record = {"schedule_index": index, "action": action_type, "trigger_event_id": event.get("event_id")}
            fired.append(record)
            if action_type == "crash":
                raise FailureInjectionError("injected_crash", "failure schedule injected crash")
            if action_type == "policy_revoke":
                target = str(action.get("target", "write"))
                if gateway is None:
                    raise FailureInjectionError("missing_gateway", "policy revoke requires a tool gateway")
                gateway.revoke_policy(target, trigger_event=event)
            elif action_type == "invalidate":
                target = action.get("target")
                if not isinstance(target, str) or not target:
                    raise FailureInjectionError("missing_invalidation_target", "invalidate requires target")
                if backend is None:
                    raise FailureInjectionError("missing_backend", "invalidate requires backend")
                backend.invalidate(target, agent_id=event.get("agent_id", "agent_model"), projection="failure_injection")
        return fired
