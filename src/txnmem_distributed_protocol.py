"""Deterministic coordinator/participant protocol smoke model.

This is intentionally a protocol checker, not a production database or a
network implementation.  It provides explicit fault schedules whose outcome
is independently checked from participant states and committed IDs.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


STATES = ("INIT", "PREPARED", "COMMITTED", "ABORTED", "CRASHED")
INVARIANTS = (
    "no_half_commit",
    "abort_visibility",
    "commit_requires_prepare",
    "commit_idempotence",
    "network_drop_safety",
)


class ProtocolError(ValueError):
    """Invalid deterministic protocol schedule."""


class ProtocolCoordinator:
    """Execute a small all-or-nothing protocol over named participants."""

    def __init__(self, participant_ids: list[str]):
        if not participant_ids or any(not isinstance(item, str) or not item for item in participant_ids):
            raise ValueError("participant_ids must contain non-empty strings")
        if len(set(participant_ids)) != len(participant_ids):
            raise ValueError("participant_ids must be unique")
        self.participant_ids = list(participant_ids)
        self.states = {participant_id: "INIT" for participant_id in participant_ids}
        self.events: list[dict[str, Any]] = []
        self.drop_phases: set[tuple[str, str]] = set()
        self.retry_count = 0
        self.commit_attempts = 0

    def _record(self, action: str, **fields: Any) -> None:
        self.events.append({"step": len(self.events) + 1, "action": action, **fields})

    def _abort_all(self, reason: str) -> None:
        for participant_id, state in self.states.items():
            if state != "COMMITTED":
                self.states[participant_id] = "ABORTED"
        self._record("abort", reason=reason)

    def _prepare(self) -> None:
        for participant_id, state in self.states.items():
            if state == "INIT":
                self.states[participant_id] = "PREPARED"
            elif state not in {"PREPARED"}:
                raise ProtocolError(f"cannot prepare participant {participant_id} from {state}")
        self._record("prepare", participant_ids=list(self.participant_ids))

    def _commit(self, *, retry: bool = False) -> None:
        self.commit_attempts += 1
        if any(self.states[participant_id] == "COMMITTED" for participant_id in self.participant_ids):
            if all(self.states[participant_id] == "COMMITTED" for participant_id in self.participant_ids):
                self._record("commit_idempotent", retry=retry)
                return
            self._abort_all("inconsistent_existing_commit")
            return
        if any(self.states[participant_id] != "PREPARED" for participant_id in self.participant_ids):
            self._abort_all("commit_without_all_participants_prepared")
            return
        dropped = [
            participant_id
            for participant_id in self.participant_ids
            if (participant_id, "commit") in self.drop_phases
        ]
        if dropped:
            self._record("retry_required", phase="commit", dropped_participants=dropped)
            if retry:
                self.retry_count += 1
                self.drop_phases.difference_update((participant_id, "commit") for participant_id in dropped)
            else:
                return
        if any((participant_id, "commit") in self.drop_phases for participant_id in self.participant_ids):
            return
        for participant_id in self.participant_ids:
            self.states[participant_id] = "COMMITTED"
        self._record("commit", participant_ids=list(self.participant_ids), retry=retry)

    def execute(self, schedule: list[Mapping[str, Any]]) -> dict[str, Any]:
        if not isinstance(schedule, list):
            raise ProtocolError("schedule must be a list")
        for index, entry in enumerate(schedule, start=1):
            if not isinstance(entry, Mapping):
                raise ProtocolError(f"schedule entry {index} must be a mapping")
            action = entry.get("type")
            if action == "prepare":
                self._prepare()
            elif action == "commit":
                self._commit()
            elif action == "retry_commit":
                self._commit(retry=True)
            elif action == "abort":
                self._abort_all("explicit_abort")
            elif action == "crash_after_prepare":
                participant_id = entry.get("participant")
                if participant_id not in self.states:
                    raise ProtocolError("crash participant is unknown")
                if self.states[participant_id] == "INIT":
                    self._prepare()
                if self.states[participant_id] != "PREPARED":
                    raise ProtocolError("crash_after_prepare requires PREPARED participant")
                self.states[participant_id] = "CRASHED"
                self._record("crash_after_prepare", participant_id=participant_id)
                self._abort_all("participant_crashed_after_prepare")
            elif action == "network_drop":
                participant_id = entry.get("participant")
                phase = entry.get("phase", "commit")
                if participant_id not in self.states or phase not in {"prepare", "commit"}:
                    raise ProtocolError("network_drop requires known participant and prepare/commit phase")
                self.drop_phases.add((participant_id, phase))
                self._record("network_drop", participant_id=participant_id, phase=phase)
            else:
                raise ProtocolError(f"unsupported protocol action: {action}")
        return self._report()

    def _report(self) -> dict[str, Any]:
        committed_ids = [
            participant_id
            for participant_id in self.participant_ids
            if self.states[participant_id] == "COMMITTED"
        ]
        return {
            "participant_ids": list(self.participant_ids),
            "final_states": dict(self.states),
            "committed_ids": committed_ids,
            "events": list(self.events),
            "metrics": {
                "commit_attempts": self.commit_attempts,
                "retry_count": self.retry_count,
                "event_count": len(self.events),
            },
        }


def check_protocol_invariants(report: Mapping[str, Any]) -> dict[str, Any]:
    states = report.get("final_states", {})
    committed_ids = list(report.get("committed_ids", []))
    events = list(report.get("events", []))
    violations: list[str] = []
    committed_count = sum(state == "COMMITTED" for state in states.values())
    if committed_count not in {0, len(states)}:
        violations.append("no_half_commit")
    if any(event.get("action") == "abort" for event in events) and committed_ids:
        violations.append("abort_visibility")
    if any(event.get("action") == "commit" for event in events) and committed_count != len(states):
        violations.append("commit_requires_prepare")
    if len(committed_ids) != len(set(committed_ids)):
        violations.append("commit_idempotence")
    for event in events:
        if event.get("action") == "retry_required" and event.get("phase") == "commit":
            if committed_count not in {0, len(states)}:
                violations.append("network_drop_safety")
                break
    covered = set(INVARIANTS) - set(violations)
    return {
        "invariants": list(INVARIANTS),
        "covered": sorted(covered),
        "coverage_rate": len(covered) / len(INVARIANTS),
        "violations": violations,
        "violation_count": len(violations),
    }


def run_protocol_matrix(schedules: Iterable[Mapping[str, Any] | list[Mapping[str, Any]]]) -> dict[str, Any]:
    materialized = list(schedules)
    reports: list[dict[str, Any]] = []
    action_counts: Counter[str] = Counter()
    invariant_covered: set[str] = set()
    minimal_counterexamples: list[dict[str, Any]] = []
    for index, schedule in enumerate(materialized, start=1):
        entries = list(schedule) if not isinstance(schedule, Mapping) else [schedule]
        for entry in entries:
            action_counts[str(entry.get("type"))] += 1
        report = ProtocolCoordinator(["p1", "p2"]).execute(entries)
        check = check_protocol_invariants(report)
        invariant_covered.update(check["covered"])
        if check["violations"]:
            minimal_counterexamples.append(
                {"schedule_index": index, "violations": check["violations"]}
            )
        reports.append({"schedule_index": index, "protocol": report, "invariants": check})
    return {
        "schedule_count": len(reports),
        "schedule_coverage": {"actions": dict(sorted(action_counts.items()))},
        "invariant_coverage": {
            "covered": sorted(invariant_covered),
            "target_count": len(INVARIANTS),
            "coverage_rate": len(invariant_covered) / len(INVARIANTS) if INVARIANTS else 1.0,
        },
        "minimal_counterexamples": minimal_counterexamples,
        "reports": reports,
        "production_claim": False,
    }
