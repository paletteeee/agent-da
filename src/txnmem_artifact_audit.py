"""Fail closed when raw traces or secret-bearing fields enter result artifacts."""

from __future__ import annotations

import argparse
import ipaddress
import json
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


def _closed_scalar_value(value: object) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    return isinstance(value, list) and all(
        item is None or isinstance(item, (str, int, float, bool))
        for item in value
    )


def _closed_mapping(
    value: object,
    allowed_keys: frozenset[str],
    *,
    nested_key: str | None = None,
    nested_allowed_keys: frozenset[str] | None = None,
) -> bool:
    if not isinstance(value, dict) or not set(value) <= allowed_keys:
        return False
    for key, item in value.items():
        if key == nested_key:
            if nested_allowed_keys is None or not _closed_mapping(
                item, nested_allowed_keys
            ):
                return False
        elif not _closed_scalar_value(item):
            return False
    return not _contains_raw_payload(value)


def _controlled_instance_containers_valid(row: dict[str, object]) -> bool:
    config = row.get("config")
    if (
        not isinstance(config, dict)
        or set(config) != set(DEFAULT_CONFIG)
        or any(isinstance(value, bool) or not isinstance(value, int) for value in config.values())
    ):
        return False
    specifications = (
        ("initial_memories", MEMORY_SHAPE_KEYS, None, None),
        ("operations", OPERATION_SHAPE_KEYS, "new_memory", NESTED_NEW_MEMORY_SHAPE_KEYS),
        ("policies", POLICY_SHAPE_KEYS, None, None),
        ("failure_schedule", SCHEDULE_SHAPE_KEYS, "trigger", TRIGGER_SHAPE_KEYS),
        ("provenance_edges", PROVENANCE_EDGE_SHAPE_KEYS, None, None),
    )
    for field, allowed, nested_key, nested_allowed in specifications:
        values = row.get(field)
        if not isinstance(values, list) or any(
            not _closed_mapping(
                item,
                allowed,
                nested_key=nested_key,
                nested_allowed_keys=nested_allowed,
            )
            for item in values
        ):
            return False
    return not _contains_raw_payload(row)


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
        required = {
            "instance_id", "workload", "seed", "config", "initial_memories",
            "operations", "policies", "failure_schedule", "provenance_edges",
        }
        if tree == "final_controlled_200":
            required |= {"semantic_parameters", "semantic_fingerprint"}
        for row in rows:
            if set(row) != required or _contains_raw_payload(row):
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
        required = {
            "instance_id", "oracle_version", "allowed_outcomes", "event_trace",
            "minimal_counterexample", "safety_invariants",
        }
        expected_oracle = ORACLE_VERSION if tree == "final_controlled_200" else "0.1"
        id_coordinates = {
            f"{family}_seed_{seed}": (family, seed)
            for family, seed in expected_coordinates
        }
        for row in rows:
            coordinate = id_coordinates.get(row.get("instance_id"))
            if (
                set(row) != required
                or coordinate is None
                or coordinate in seen
                or row.get("oracle_version") != expected_oracle
                or _contains_raw_payload(row)
                or not isinstance(row.get("allowed_outcomes"), list)
                or not isinstance(row.get("event_trace"), list)
                or not isinstance(row.get("safety_invariants"), dict)
                or set(row["safety_invariants"]) != _ORACLE_SAFETY_KEYS
                or any(value is not True and value is not False for value in row["safety_invariants"].values())
                or not (
                    row.get("minimal_counterexample") is None
                    or isinstance(row.get("minimal_counterexample"), dict)
                )
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
        raw_tokens = {
            token
            for part in result_parts
            for token in re.split(r"[^a-z0-9]+", Path(part).stem.lower())
            if token
        }
        if (
            ("data" in result_parts and relative_result_path not in _CONTROLLED_PUBLIC_ALLOWLIST)
            or path.name in _RAW_FILENAMES
            or (
                bool(raw_tokens & _RAW_CAPABLE_COMPONENTS)
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
