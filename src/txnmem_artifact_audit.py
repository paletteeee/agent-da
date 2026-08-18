"""Fail closed when raw traces or secret-bearing fields enter result artifacts."""

from __future__ import annotations

import argparse
import ipaddress
import json
import math
from pathlib import Path
import re
import subprocess
from typing import Iterable

from txnmem_reference import ORACLE_VERSION
from txnmem_schema import DEFAULT_CONFIG
from txnmem_workloads import (
    MEMORY_SHAPE_KEYS,
    NESTED_NEW_MEMORY_SHAPE_KEYS,
    OPERATION_SHAPE_KEYS,
    POLICY_SHAPE_KEYS,
    PROVENANCE_EDGE_SHAPE_KEYS,
    SCHEDULE_SHAPE_KEYS,
    TRIGGER_SHAPE_KEYS,
    WORKLOADS,
    WORKLOAD_SEMANTIC_PARAMETERS,
    semantic_fingerprint,
)
from txnmem_statistics import APPROVED_CONTROLLED_PARAMETER_INTERVALS


_RAW_FILENAMES = {
    "locomo_paired_predictions.json",
    "model_load_traces.jsonl",
    "native_model_traces.jsonl",
}
_CONTROLLED_PUBLIC_ALLOWLIST = {
    ("final_controlled", "data", "generated_instances.jsonl"),
    ("final_controlled", "data", "reference_oracles.jsonl"),
    ("final_controlled_200", "data", "generated_instances.jsonl"),
    ("final_controlled_200", "data", "reference_oracles.jsonl"),
}
_RAW_CAPABLE_COMPONENTS = {
    "raw", "trace", "traces", "event", "events", "prompt", "prompts",
    "message", "messages", "payload", "payloads", "conversation",
    "conversations", "transcript", "transcripts", "chat", "chats",
    "dialogue", "dialogues", "tool_arg", "tool_args", "tool_argument",
    "tool_arguments", "arguments", "native",
}
_SAFE_AGGREGATE_FILENAMES = {
    "trace_realism.json",
    "trace_replay.csv",
    "native_model_summary.json",
    "native_batch_summary.json",
    "native_memory_replay_summary.json",
    "native_smoke_summary.json",
    "blocked_report.json",
    "performance.json",
    "repetition_report.json",
    "appworld_prompt_comparison.json",
    "locomo_prompt_comparison.json",
    "locomo_paired_repetition_summary.json",
}
_SAFE_AGGREGATE_ANCESTORS = {
    "native_memory_replay",
    "native_repetitions5",
    "official_trace_runs",
    "prompt_profile_formal_v4",
    "public_native",
}
_RAW_PAYLOAD_KEYS = {
    "raw", "trace", "traces", "event", "events", "prompt", "prompts",
    "message", "messages", "payload", "payloads", "conversation",
    "conversations", "transcript", "transcripts", "chat", "chats",
    "dialogue", "dialogues", "customer", "customers", "arguments",
    "tool_arg", "tool_args", "tool_argument", "tool_arguments", "content",
    "task_payload", "native_payload", "benchmark_payload",
}
_TRACE_REPLAY_COLUMNS = {
    "instance_id", "workload", "seed", "variant", "transaction_state",
    "partial_update_rate", "invalid_commit_rate", "stale_write_rate",
    "repair_recall", "leak_rate", "supersession_consistency",
    "scope_bypass_rate", "latency", "any_violation", "violations",
    "committed_count", "operation_count", "repair_count", "oracle_version",
    "oracle_match", "allowed_outcome_count", "oracle_mismatches",
}
_ORACLE_SAFETY_KEYS = {
    "atomicity",
    "commit_authorization",
    "no_invalid_visibility",
    "supersession_consistency",
    "provenance_closure",
    "graph_validity",
}
_ORACLE_OUTCOME_KEYS = {
    "txn_states",
    "committed_memory_ids",
    "visible_memory_ids",
    "invalid_memory_ids",
    "superseded_memory_ids",
    "provenance_edges",
    "policy_version",
    "invariants",
}
_ORACLE_TRACE_KEYS = {
    "event_id",
    "operation_id",
    "txn_id",
    "event_type",
    "policy_version",
    "decision",
    "reason_codes",
    "affected_memory_ids",
    "affected_edge_ids",
}
_RAW_VALUE_MARKERS = {
    "customer",
    "customers",
    "conversation",
    "conversations",
    "dialogue",
    "dialogues",
    "message",
    "messages",
    "payload",
    "payloads",
    "prompt",
    "prompts",
    "transcript",
    "transcripts",
    "turn",
    "turns",
}
_SENSITIVE_KEY = re.compile(
    r'"(?:password|api_key|access_token|secret|messages|arguments|tool_args|prompt|content)"\s*:',
    flags=re.IGNORECASE,
)
_TOKEN_VALUE = re.compile(
    r"(?:\bsk-[A-Za-z0-9_-]{8,}\b|\bBearer\s+[A-Za-z0-9._-]{8,}|-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    flags=re.IGNORECASE,
)
_IPV4_VALUE = re.compile(r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])")


def _contains_sensitive_value(text: str) -> bool:
    if _TOKEN_VALUE.search(text):
        return True
    for match in _IPV4_VALUE.finditer(text):
        try:
            address = ipaddress.ip_address(match.group(0))
        except ValueError:
            continue
        if address.is_global:
            return True
    return False


def _contains_raw_payload(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            str(key).lower() in _RAW_PAYLOAD_KEYS or _contains_raw_payload(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(_contains_raw_payload(item) for item in value)
    return False


def _safe_aggregate(path: Path) -> bool:
    if path.name not in _SAFE_AGGREGATE_FILENAMES:
        return False
    if path.suffix.lower() == ".csv":
        try:
            header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
        except (OSError, IndexError):
            return False
        return set(header) == _TRACE_REPLAY_COLUMNS and len(header) == len(_TRACE_REPLAY_COLUMNS)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(document, dict) and not _contains_raw_payload(document)


def _plain_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int)


def _string(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _optional_string(value: object) -> bool:
    return value is None or _string(value)


def _string_list(value: object) -> bool:
    return isinstance(value, list) and all(_string(item) for item in value)


def _contains_raw_value_marker(value: object) -> bool:
    if isinstance(value, str):
        tokens = {
            token
            for token in re.split(r"[^a-z0-9]+", value.lower())
            if token
        }
        return bool(tokens & _RAW_VALUE_MARKERS)
    if isinstance(value, dict):
        return any(_contains_raw_value_marker(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_raw_value_marker(item) for item in value)
    return False


def _synthetic_scalar(value: object) -> bool:
    if value is None or isinstance(value, bool) or _plain_int(value):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if not isinstance(value, str):
        return False
    return not _contains_raw_value_marker(value)


def _memory_record_valid(value: object) -> bool:
    if (
        not isinstance(value, dict)
        or not {"memory_id"} <= set(value) <= MEMORY_SHAPE_KEYS
    ):
        return False
    string_fields = {"memory_id", "agent_id", "scope", "entity_id", "attribute", "status"}
    if any(name in value and not _string(value[name]) for name in string_fields):
        return False
    if "value" in value and not _synthetic_scalar(value["value"]):
        return False
    if any(name in value and not _plain_int(value[name]) for name in {"version", "policy_version"}):
        return False
    if "supersedes_id" in value and not _optional_string(value["supersedes_id"]):
        return False
    if "derived_from" in value and not _string_list(value["derived_from"]):
        return False
    return not _contains_raw_payload(value)


def _new_memory_record_valid(value: object) -> bool:
    if (
        not isinstance(value, dict)
        or not set(value) <= NESTED_NEW_MEMORY_SHAPE_KEYS
        or not ({"memory_id", "output_id"} & set(value))
    ):
        return False
    string_fields = {
        "memory_id", "output_id", "agent_id", "scope", "entity_id", "attribute"
    }
    if any(name in value and not _string(value[name]) for name in string_fields):
        return False
    if "value" in value and not _synthetic_scalar(value["value"]):
        return False
    if "policy_version" in value and not _plain_int(value["policy_version"]):
        return False
    if "source_ids" in value and not _string_list(value["source_ids"]):
        return False
    return not _contains_raw_payload(value)


def _operation_record_valid(value: object) -> bool:
    required = {"op_id", "step", "agent_id", "type"}
    if (
        not isinstance(value, dict)
        or not required <= set(value) <= OPERATION_SHAPE_KEYS
        or not _plain_int(value.get("step"))
    ):
        return False
    string_fields = {
        "op_id", "agent_id", "type", "txn_id", "memory_id", "output_id",
        "source_id", "scope", "target_scope", "entity_id", "attribute",
        "supersedes_id", "old_memory_id", "old_id", "new_memory_id", "new_id",
        "abort_reason", "root_id",
    }
    if any(name in value and not _string(value[name]) for name in string_fields):
        return False
    if "policy_version" in value and not _plain_int(value["policy_version"]):
        return False
    if any(name in value and not _string_list(value[name]) for name in {"source_ids", "root_ids"}):
        return False
    if any(name in value and not _synthetic_scalar(value[name]) for name in {"value", "query"}):
        return False
    if "new_memory" in value and not _new_memory_record_valid(value["new_memory"]):
        return False
    return not _contains_raw_payload(value)


def _policy_record_valid(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != POLICY_SHAPE_KEYS:
        return False
    if not all(
        _string(value[name])
        for name in {"policy_id", "agent_id", "action", "scope", "effect"}
    ):
        return False
    return _plain_int(value["version"]) and _plain_int(value["effective_step"])


def _trigger_record_valid(value: object) -> bool:
    return (
        isinstance(value, dict)
        and len(value) == 1
        and set(value) <= TRIGGER_SHAPE_KEYS
        and all(_string(item) for item in value.values())
    )


def _schedule_record_valid(value: object) -> bool:
    if not isinstance(value, dict) or not set(value) <= SCHEDULE_SHAPE_KEYS:
        return False
    if not any(name in value for name in {"type", "action"}):
        return False
    if any(name in value and not _string(value[name]) for name in {"type", "action", "target", "memory_id", "phase"}):
        return False
    if "trigger" in value:
        if "step" in value or not _trigger_record_valid(value["trigger"]):
            return False
    elif "step" not in value or not _plain_int(value["step"]):
        return False
    return not _contains_raw_payload(value)


def _provenance_edge_record_valid(value: object) -> bool:
    if (
        not isinstance(value, dict)
        or not {"source_id", "derived_id"} <= set(value) <= PROVENANCE_EDGE_SHAPE_KEYS
    ):
        return False
    return all(_string(item) for item in value.values()) and not _contains_raw_payload(value)


def _controlled_instance_containers_valid(row: dict[str, object]) -> bool:
    config = row.get("config")
    if (
        not isinstance(config, dict)
        or set(config) != set(DEFAULT_CONFIG)
        or any(isinstance(value, bool) or not isinstance(value, int) for value in config.values())
    ):
        return False
    specifications = (
        ("initial_memories", _memory_record_valid),
        ("operations", _operation_record_valid),
        ("policies", _policy_record_valid),
        ("failure_schedule", _schedule_record_valid),
        ("provenance_edges", _provenance_edge_record_valid),
    )
    for field, validator in specifications:
        values = row.get(field)
        if not isinstance(values, list) or any(
            not validator(item) for item in values
        ):
            return False
    return not _contains_raw_payload(row) and not _contains_raw_value_marker(row)


def controlled_generated_record_valid(row: object, *, scaled: bool) -> bool:
    required = {
        "instance_id", "workload", "seed", "config", "initial_memories",
        "operations", "policies", "failure_schedule", "provenance_edges",
    }
    if scaled:
        required |= {"semantic_parameters", "semantic_fingerprint"}
    return (
        isinstance(row, dict)
        and set(row) == required
        and _string(row.get("instance_id"))
        and _string(row.get("workload"))
        and _plain_int(row.get("seed"))
        and _controlled_instance_containers_valid(row)
        and not _contains_raw_value_marker(row)
    )


def _oracle_safety_valid(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _ORACLE_SAFETY_KEYS
        and all(item is True or item is False for item in value.values())
    )


def _oracle_outcome_valid(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != _ORACLE_OUTCOME_KEYS:
        return False
    txn_states = value.get("txn_states")
    if not isinstance(txn_states, dict) or not all(
        _string(key) and _string(item) for key, item in txn_states.items()
    ):
        return False
    for name in {
        "committed_memory_ids", "visible_memory_ids", "invalid_memory_ids",
        "superseded_memory_ids",
    }:
        if not _string_list(value.get(name)):
            return False
    edges = value.get("provenance_edges")
    return (
        isinstance(edges, list)
        and all(_provenance_edge_record_valid(edge) for edge in edges)
        and _plain_int(value.get("policy_version"))
        and _oracle_safety_valid(value.get("invariants"))
        and not _contains_raw_payload(value)
    )


def _oracle_trace_event_valid(value: object) -> bool:
    required = {
        "event_id", "operation_id", "txn_id", "event_type", "policy_version",
        "decision", "reason_codes",
    }
    if (
        not isinstance(value, dict)
        or not required <= set(value) <= _ORACLE_TRACE_KEYS
        or not _string(value.get("event_id"))
        or not _optional_string(value.get("operation_id"))
        or not _optional_string(value.get("txn_id"))
        or not _string(value.get("event_type"))
        or not _plain_int(value.get("policy_version"))
        or not _string(value.get("decision"))
        or not _string_list(value.get("reason_codes"))
    ):
        return False
    return all(
        name not in value or _string_list(value[name])
        for name in {"affected_memory_ids", "affected_edge_ids"}
    ) and not _contains_raw_payload(value)


def controlled_oracle_record_valid(
    row: object,
    *,
    expected_oracle_version: str,
) -> bool:
    required = {
        "instance_id", "oracle_version", "allowed_outcomes", "event_trace",
        "minimal_counterexample", "safety_invariants",
    }
    if (
        not isinstance(row, dict)
        or set(row) != required
        or not _string(row.get("instance_id"))
        or row.get("oracle_version") != expected_oracle_version
        or row.get("minimal_counterexample") is not None
        or not _oracle_safety_valid(row.get("safety_invariants"))
        or _contains_raw_payload(row)
        or _contains_raw_value_marker(row)
    ):
        return False
    outcomes = row.get("allowed_outcomes")
    trace = row.get("event_trace")
    return (
        isinstance(outcomes, list)
        and bool(outcomes)
        and all(_oracle_outcome_valid(outcome) for outcome in outcomes)
        and isinstance(trace, list)
        and all(_oracle_trace_event_valid(event) for event in trace)
    )


def _controlled_artifact_valid(path: Path, relative_result_path: tuple[str, ...]) -> bool:
    if relative_result_path not in _CONTROLLED_PUBLIC_ALLOWLIST:
        return False
    tree, _, name = relative_result_path
    seed_count = 200 if tree == "final_controlled_200" else 50
    expected_coordinates = {(family, seed) for family in WORKLOADS for seed in range(seed_count)}
    expected_count = len(expected_coordinates)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        if len(lines) != expected_count or any(not line.strip() for line in lines):
            return False
        rows = [json.loads(line) for line in lines]
    except (OSError, json.JSONDecodeError):
        return False
    if not all(isinstance(row, dict) for row in rows):
        return False
    seen: set[tuple[str, int]] = set()
    if name == "generated_instances.jsonl":
        for row in rows:
            if not controlled_generated_record_valid(
                row, scaled=tree == "final_controlled_200"
            ):
                return False
            coordinate = (row.get("workload"), row.get("seed"))
            if coordinate not in expected_coordinates or coordinate in seen:
                return False
            if row.get("instance_id") != f"{coordinate[0]}_seed_{coordinate[1]}":
                return False
            if not _controlled_instance_containers_valid(row):
                return False
            if tree == "final_controlled_200":
                parameters = row.get("semantic_parameters")
                fingerprint = row.get("semantic_fingerprint")
                if (
                    not isinstance(parameters, dict)
                    or set(parameters) != set(WORKLOAD_SEMANTIC_PARAMETERS[coordinate[0]])
                    or not isinstance(fingerprint, str)
                    or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
                    or fingerprint != semantic_fingerprint(row)
                ):
                    return False
                for parameter, value in parameters.items():
                    low, high = APPROVED_CONTROLLED_PARAMETER_INTERVALS[parameter]
                    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high or row["config"].get(parameter) != value:
                        return False
            seen.add(coordinate)
    else:
        expected_oracle = ORACLE_VERSION if tree == "final_controlled_200" else "0.1"
        id_coordinates = {
            f"{family}_seed_{seed}": (family, seed)
            for family, seed in expected_coordinates
        }
        for row in rows:
            coordinate = id_coordinates.get(row.get("instance_id"))
            if (
                not controlled_oracle_record_valid(
                    row, expected_oracle_version=expected_oracle
                )
                or coordinate is None
                or coordinate in seen
            ):
                return False
            seen.add(coordinate)
    return seen == expected_coordinates


def audit_result_paths(paths: Iterable[str | Path]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for source in paths:
        path = Path(source)
        parts = path.parts
        if "results" not in parts:
            continue
        result_index = parts.index("results")
        result_parts = parts[result_index + 1 :]
        relative_result_path = tuple(result_parts)
        if ".." in parts:
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        audit_root = path.parents[len(result_parts)]
        resolved_path = path.resolve(strict=False)
        try:
            resolved_relative = resolved_path.relative_to(audit_root.resolve(strict=False))
        except ValueError:
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        if resolved_relative != path.relative_to(audit_root):
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        ancestor_tokens = {
            token
            for part in result_parts[:-1]
            if Path(part).stem.lower() not in _SAFE_AGGREGATE_ANCESTORS
            for token in re.split(r"[^a-z0-9]+", Path(part).stem.lower())
            if token
        }
        filename_tokens = {
            token
            for token in re.split(r"[^a-z0-9]+", path.stem.lower())
            if token
        }
        if (
            ("data" in result_parts and relative_result_path not in _CONTROLLED_PUBLIC_ALLOWLIST)
            or path.name in _RAW_FILENAMES
            or bool(ancestor_tokens & _RAW_CAPABLE_COMPONENTS)
            or (
                bool(filename_tokens & _RAW_CAPABLE_COMPONENTS)
                and not _safe_aggregate(path)
            )
        ):
            findings.append({"code": "raw_result_path", "path": str(path)})
        if relative_result_path in _CONTROLLED_PUBLIC_ALLOWLIST and not _controlled_artifact_valid(path, relative_result_path):
            findings.append({"code": "controlled_artifact_schema", "path": str(path)})
        if not path.is_file() or path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if _SENSITIVE_KEY.search(text):
            findings.append({"code": "sensitive_result_key", "path": str(path)})
        if _contains_sensitive_value(text):
            findings.append({"code": "sensitive_result_value", "path": str(path)})
    return findings


def _git_result_paths(root: Path) -> list[Path]:
    commands = (
        ["git", "ls-files", "results"],
        ["git", "ls-files", "--others", "--exclude-standard", "results"],
    )
    paths: set[Path] = set()
    for command in commands:
        completed = subprocess.run(
            command,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        for line in completed.stdout.splitlines():
            if line.strip():
                paths.add(root / line.strip())
    return sorted(paths)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    findings = audit_result_paths(_git_result_paths(root))
    print(json.dumps({"finding_count": len(findings), "findings": findings}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
