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
_PHASES = frozenset(
    {
        "after_prepare",
        "after_qdrant_stage",
        "after_neo4j_stage",
        "after_stage_verify",
        "after_commit_decision",
        "after_finalize",
        "after_mutation",
    }
)


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
        phase = trigger.get("phase")
        count = trigger.get("count", 1)
        action_type = action.get("type")
        has_kind = isinstance(kind, str) and bool(kind.strip())
        has_phase = isinstance(phase, str) and bool(phase.strip())
        if has_kind == has_phase:
            raise ValueError(
                f"failure schedule entry {index} needs exactly one trigger kind or phase"
            )
        if has_phase and phase not in _PHASES:
            raise ValueError(f"failure schedule entry {index} has unsupported trigger phase")
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
        self.phase_counts: Counter[str] = Counter()
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
            fired.append(
                self._fire(index, entry["action"], event, backend=backend, gateway=gateway)
            )
        return fired

    def observe_phase(
        self,
        phase: str,
        evidence: Mapping[str, Any],
        *,
        backend: Any = None,
        gateway: Any = None,
    ) -> list[dict[str, Any]]:
        if phase not in _PHASES:
            raise ValueError(f"unsupported transaction phase: {phase}")
        if not isinstance(evidence, Mapping):
            raise ValueError("phase evidence must be a mapping")
        self.phase_counts[phase] += 1
        fired: list[dict[str, Any]] = []
        for index, entry in enumerate(self.schedule):
            if index in self.fired:
                continue
            trigger = entry["trigger"]
            if (
                trigger.get("phase") != phase
                or int(trigger.get("count", 1)) != self.phase_counts[phase]
            ):
                continue
            trigger_evidence = dict(evidence)
            trigger_evidence["phase"] = phase
            fired.append(
                self._fire(
                    index,
                    entry["action"],
                    trigger_evidence,
                    backend=backend,
                    gateway=gateway,
                )
            )
        return fired

    def _fire(
        self,
        index: int,
        action: Mapping[str, Any],
        trigger_evidence: Mapping[str, Any],
        *,
        backend: Any,
        gateway: Any,
    ) -> dict[str, Any]:
        self.fired.add(index)
        action_type = action["type"]
        record = {
            "schedule_index": index,
            "action": action_type,
            "trigger_event_id": trigger_evidence.get("event_id"),
        }
        if trigger_evidence.get("phase") is not None:
            record["trigger_phase"] = trigger_evidence["phase"]
        if action_type == "crash":
            raise FailureInjectionError("injected_crash", "failure schedule injected crash")
        if action_type == "policy_revoke":
            target = str(action.get("target", "write"))
            if gateway is None:
                raise FailureInjectionError("missing_gateway", "policy revoke requires a tool gateway")
            try:
                gateway.revoke_policy(target, trigger_event=trigger_evidence)
            except FailureInjectionError:
                raise
            except Exception as exc:
                raise FailureInjectionError(
                    "failure_action_failed", "policy revoke action failed"
                ) from exc
        elif action_type == "invalidate":
            target = action.get("target")
            if not isinstance(target, str) or not target:
                raise FailureInjectionError("missing_invalidation_target", "invalidate requires target")
            if backend is None:
                raise FailureInjectionError("missing_backend", "invalidate requires backend")
            try:
                invalidate_committed = getattr(backend, "invalidate_committed", None)
                if callable(invalidate_committed):
                    invalidate_committed(target)
                else:
                    backend.invalidate(
                        target,
                        agent_id=trigger_evidence.get("agent_id", "agent_model"),
                        projection="failure_injection",
                    )
            except FailureInjectionError:
                raise
            except Exception as exc:
                raise FailureInjectionError(
                    "failure_action_failed", "invalidate action failed"
                ) from exc
        return record
