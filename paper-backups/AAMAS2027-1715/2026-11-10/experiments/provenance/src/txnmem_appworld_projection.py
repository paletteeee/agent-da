"""Regenerate redacted AppWorld reference-API projection source events."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
from tempfile import TemporaryDirectory
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from txnmem_benchmark_manifests import _canonical_hash, shard_manifest
from txnmem_conditions import canonical_fingerprint
from txnmem_event_contract import validate_events
from txnmem_formal_io import (
    FormalIOError,
    FormalStore,
    bind_native_shard_report,
    canonical_json_equal,
    validate_parent_manifest,
)


APPWORLD_FORMAL_SPLIT = "test_normal"
APPWORLD_FORMAL_SPLIT_SHA256 = (
    "c3af41497b6f2f0860a2ff8c09b335dca527e2cf48e59b4aabdb301b6b68db8f"
)
APPWORLD_FORMAL_PARENT_MANIFEST_SHA256 = (
    "f1b5946abf818fab5cfba9319b50f006878ed1cef1c4a48989a0d8d704803c61"
)
APPWORLD_FORMAL_TASK_COUNT = 168
APPWORLD_FORMAL_FAMILY_COUNT = 56
APPWORLD_FORMAL_EVALUATION_FAMILY_COUNT = 50
APPWORLD_FORMAL_CALIBRATION_FAMILY_COUNT = 6
APPWORLD_FORMAL_SELECTION_SEED = 17
# Filled only after an independently reviewed Task-11 execution attestation is
# registered in source control.  The key is the canonical treatment
# fingerprint and the value is the exact SHA-256 of the out-of-tree
# launch/completion attestation.  An empty registry deliberately keeps every
# native bundle in candidate/blocked status.
APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT: dict[str, str] = {}


def _data_root(path: Path) -> Path:
    path = Path(path)
    if (path / "tasks").is_dir():
        return path
    return path / "data"


def _appworld_family(
    task_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    metadata = metadata or {}
    for field in ("family_id", "scenario_id", "generator_id"):
        value = metadata.get(field)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            raise ValueError(f"AppWorld {field} must be a canonical non-empty string")
        return value, f"official_{field}"

    untagged = task_id.split(":", 1)[0]
    match = re.fullmatch(r"(.+)_([1-9][0-9]*)", untagged)
    if match:
        return match.group(1), "audited_appworld_generator_prefix"
    return untagged, "task_id_identity_fallback"


def _appworld_dataset_identity(
    dataset_path: Path, official_split: str
) -> tuple[list[str], dict[str, Any]]:
    source = Path(dataset_path)
    if source.is_symlink() or not source.is_file():
        raise ValueError("AppWorld dataset list must be a regular file")
    raw = source.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("AppWorld dataset list must be UTF-8") from exc
    if official_split != "unknown" and source.stem != official_split:
        raise ValueError("official_split must match the AppWorld dataset filename")
    task_ids = [
        line.strip().split(":", 1)[0]
        for line in text.splitlines()
        if line.strip()
    ]
    if not task_ids or len(set(task_ids)) != len(task_ids):
        raise ValueError("AppWorld dataset list must contain unique task IDs")
    return task_ids, {
        "dataset_name": source.stem,
        "dataset_file_sha256": hashlib.sha256(raw).hexdigest(),
        "dataset_file_size_bytes": len(raw),
        "dataset_task_count": len(task_ids),
    }


def select_appworld_realism_families(
    task_ids: Sequence[str],
    *,
    evaluation_family_count: int = 50,
    calibration_family_count: int | None = None,
    seed: int = 17,
    official_split: str,
    task_metadata_by_id: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Select deterministic disjoint AppWorld generator families."""

    normalized_ids = [str(task_id).strip().split(":", 1)[0] for task_id in task_ids]
    if (
        not normalized_ids
        or any(not task_id for task_id in normalized_ids)
        or len(set(normalized_ids)) != len(normalized_ids)
    ):
        raise ValueError("AppWorld task ids must be non-empty and unique")
    if not isinstance(official_split, str) or not official_split.strip():
        raise ValueError("official_split must be a non-empty string")
    if official_split != official_split.strip():
        raise ValueError("official_split must be canonical")
    for name, value in (("evaluation_family_count", evaluation_family_count),):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    if calibration_family_count is not None and (
        isinstance(calibration_family_count, bool)
        or not isinstance(calibration_family_count, int)
        or calibration_family_count < 1
    ):
        raise ValueError("calibration_family_count must be a positive integer or None")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")

    metadata_map = task_metadata_by_id or {}
    task_families: dict[str, str] = {}
    derivations: Counter[str] = Counter()
    for task_id in normalized_ids:
        metadata = metadata_map.get(task_id, {})
        if not isinstance(metadata, Mapping):
            raise ValueError(f"AppWorld metadata for {task_id} must be a mapping")
        family_id, derivation = _appworld_family(task_id, metadata)
        task_families[task_id] = family_id
        derivations[derivation] += 1
    families = sorted(set(task_families.values()))
    if calibration_family_count is None and evaluation_family_count >= len(families):
        raise ValueError(
            "AppWorld selection needs at least one disjoint calibration family"
        )
    effective_calibration_count = (
        len(families) - evaluation_family_count
        if calibration_family_count is None
        else calibration_family_count
    )
    required_count = evaluation_family_count + effective_calibration_count
    if len(families) < required_count:
        raise ValueError(
            f"AppWorld selection needs at least {required_count} disjoint families"
        )
    random.Random(seed).shuffle(families)
    evaluation = set(families[:evaluation_family_count])
    calibration = set(families[evaluation_family_count:required_count])
    return {
        "selection_method": "seeded_disjoint_whole_appworld_generator_family",
        "group_key": "family_id",
        "seed": seed,
        "official_split": official_split,
        "source_task_count": len(normalized_ids),
        "source_family_count": len(set(task_families.values())),
        "evaluation_family_count": len(evaluation),
        "calibration_family_count": len(calibration),
        "calibration_selection": (
            "all_remaining_families"
            if calibration_family_count is None
            else "fixed_family_count"
        ),
        "evaluation_family_ids": sorted(evaluation),
        "calibration_family_ids": sorted(calibration),
        "evaluation_task_ids": [
            task_id for task_id in normalized_ids if task_families[task_id] in evaluation
        ],
        "calibration_task_ids": [
            task_id for task_id in normalized_ids if task_families[task_id] in calibration
        ],
        "family_overlap_count": len(evaluation.intersection(calibration)),
        "family_derivation_counts": dict(sorted(derivations.items())),
    }


def select_appworld_realism_families_from_dataset(
    dataset_path: Path,
    *,
    evaluation_family_count: int = 50,
    calibration_family_count: int | None = None,
    seed: int = 17,
    official_split: str,
) -> dict[str, Any]:
    """Bind a family selection to the exact official split-list bytes."""

    task_ids, identity = _appworld_dataset_identity(dataset_path, official_split)
    selection = select_appworld_realism_families(
        task_ids,
        evaluation_family_count=evaluation_family_count,
        calibration_family_count=calibration_family_count,
        seed=seed,
        official_split=official_split,
    )
    selection.update(
        {
            "source": "AppWorld official dataset split task list",
            **identity,
        }
    )
    return selection


def regenerate_appworld_projection(
    appworld_root: Path,
    task_ids: Sequence[str],
    output_path: Path,
    *,
    official_split: str = "unknown",
    task_metadata_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    dataset_path: Path | None = None,
) -> dict[str, Any]:
    """Write method/URL-only source records from official ``api_calls.json``.

    Request ``data`` values are intentionally excluded because official calls
    may contain credentials or private task state.  Source and output hashes
    make the regeneration auditable without committing those values.
    """

    normalized_ids = [
        str(task_id).strip().split(":", 1)[0]
        for task_id in task_ids
        if str(task_id).strip()
    ]
    if not normalized_ids:
        raise ValueError("at least one AppWorld task id is required")
    if len(set(normalized_ids)) != len(normalized_ids):
        raise ValueError("AppWorld task ids must be unique")
    if not isinstance(official_split, str) or not official_split.strip():
        raise ValueError("official_split must be a non-empty string")
    if official_split != official_split.strip():
        raise ValueError("official_split must be canonical")
    dataset_identity: dict[str, Any] = {
        "dataset_name": None,
        "dataset_file_sha256": None,
        "dataset_file_size_bytes": None,
        "dataset_task_count": None,
    }
    split_membership_verified = False
    if dataset_path is not None:
        dataset_ids, dataset_identity = _appworld_dataset_identity(
            dataset_path, official_split
        )
        missing_from_split = sorted(set(normalized_ids) - set(dataset_ids))
        if missing_from_split:
            raise ValueError("task IDs are outside the declared AppWorld split")
        split_membership_verified = True
    metadata_map = task_metadata_by_id or {}
    data_root = _data_root(Path(appworld_root))
    records: list[dict[str, Any]] = []
    source_hashes: dict[str, str] = {}
    method_counts: Counter[str] = Counter()
    per_task_counts: dict[str, int] = {}
    task_families: dict[str, str] = {}
    family_derivations: dict[str, str] = {}
    derivation_counts: Counter[str] = Counter()
    for task_id in normalized_ids:
        metadata = metadata_map.get(task_id, {})
        if not isinstance(metadata, Mapping):
            raise ValueError(f"AppWorld metadata for {task_id} must be a mapping")
        family_id, family_derivation = _appworld_family(task_id, metadata)
        task_families[task_id] = family_id
        family_derivations[task_id] = family_derivation
        derivation_counts[family_derivation] += 1
        source_path = data_root / "tasks" / task_id / "ground_truth" / "api_calls.json"
        raw = source_path.read_bytes()
        source_hashes[task_id] = hashlib.sha256(raw).hexdigest()
        try:
            calls = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid AppWorld api_calls.json for task {task_id}") from exc
        if not isinstance(calls, list):
            raise ValueError(f"AppWorld api_calls.json for task {task_id} must be a list")
        per_task_counts[task_id] = len(calls)
        for sequence, call in enumerate(calls, start=1):
            if not isinstance(call, dict):
                raise ValueError(f"AppWorld API call {task_id}:{sequence} must be a mapping")
            method = str(call.get("method", "")).strip().lower()
            url = str(call.get("url", "")).strip()
            if not method or not url:
                raise ValueError(f"AppWorld API call {task_id}:{sequence} needs method and url")
            method_counts[method] += 1
            records.append(
                {
                    "task_id": task_id,
                    "event_id": f"{task_id}:reference_api:{sequence:04d}",
                    "sequence": sequence,
                    "method": method,
                    "url": url,
                    "official_split": official_split,
                    "family_id": family_id,
                    "family_derivation_method": family_derivation,
                    "source_projection": "appworld_official_reference_api_call_redacted",
                }
            )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    serialized = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in records
    )
    output.write_text(serialized, encoding="utf-8")
    return {
        "status": "regenerated",
        "source": "AppWorld official ground_truth/api_calls.json",
        "projection": "method_url_only_redacted_source_records",
        "task_ids": normalized_ids,
        "task_count": len(normalized_ids),
        "official_split": official_split,
        "split_membership_verified": split_membership_verified,
        **dataset_identity,
        "family_count": len(set(task_families.values())),
        "family_ids": sorted(set(task_families.values())),
        "task_family_by_id": task_families,
        "family_derivation_by_task": family_derivations,
        "family_derivation_counts": dict(sorted(derivation_counts.items())),
        "event_count": len(records),
        "zero_event_count": sum(count == 0 for count in per_task_counts.values()),
        "zero_event_task_ids": sorted(
            task_id for task_id, count in per_task_counts.items() if count == 0
        ),
        "per_task_event_counts": per_task_counts,
        "method_counts": dict(sorted(method_counts.items())),
        "source_sha256_by_task": source_hashes,
        "output_sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "request_data_values_retained": False,
        "raw_official_request_values_included_in_projection": False,
        "trace_ground_truth_native": False,
        "production_latency_claim": False,
    }


def _load_json_mapping(path: Path, label: str) -> tuple[dict[str, Any], bytes]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"{label} must be a regular file")
    raw = source.read_bytes()
    value = _strict_json_line(raw, label)
    return value, raw


def _strict_json_line(raw: bytes, label: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"{label} contains a duplicate JSON key")
            value[key] = item
        return value

    def reject_constant(value: str) -> Any:
        raise ValueError(f"{label} contains a non-finite number: {value}")

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError(f"{label} contains a non-finite number")
        return parsed

    try:
        item = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is not strict UTF-8 JSON") from exc
    if not isinstance(item, dict):
        raise ValueError(f"{label} must be a JSON mapping")
    return item


def _canonical_artifact_parts(relative_path: str) -> tuple[str, ...]:
    if not isinstance(relative_path, str) or not relative_path:
        raise ValueError("native trace artifact path must be non-empty")
    path = PurePosixPath(relative_path)
    if (
        path.is_absolute()
        or "\\" in relative_path
        or path.as_posix() != relative_path
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("native trace artifact path must be canonical and relative")
    return tuple(path.parts)


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _require_exact_nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _require_exact_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _resolved_output_candidate(output_path: Path, label: str) -> tuple[Path, Path]:
    requested = Path(output_path).expanduser().absolute()
    if requested.is_symlink():
        raise ValueError(f"{label} must not be a symlink")
    if not requested.name or requested.name in {".", ".."}:
        raise ValueError(f"{label} must name a file")
    try:
        resolved = requested.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"{label} cannot be resolved safely") from exc
    return requested, resolved


def _external_output_target(
    native_run_root: Path,
    output_path: Path,
    label: str,
) -> tuple[FormalStore, str, Path]:
    """Resolve one new no-follow output outside the protected run tree."""

    requested, resolved_candidate = _resolved_output_candidate(output_path, label)
    if _is_within(resolved_candidate, native_run_root):
        raise ValueError(f"{label} must be outside the native run root")
    try:
        output_store = FormalStore(requested.parent)
    except FormalIOError as exc:
        raise ValueError(f"{label} has an unsafe parent directory") from exc
    target = output_store.path(requested.name)
    if _is_within(target, native_run_root):
        raise ValueError(f"{label} must be outside the native run root")
    try:
        kind = output_store.entry_kind(requested.name)
    except FormalIOError as exc:
        raise ValueError(f"{label} must not be a symlink") from exc
    if kind != "missing":
        raise ValueError(f"refusing to overwrite existing {label}")
    return output_store, requested.name, target


def _load_external_task11_attestation(
    path: Path,
    *,
    native_run_root: Path,
) -> tuple[dict[str, Any], bytes]:
    requested = Path(path).expanduser().absolute()
    if requested.is_symlink() or not requested.is_file():
        raise ValueError("Task-11 execution attestation must be a regular file")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise ValueError("Task-11 execution attestation cannot be resolved") from exc
    if _is_within(resolved, native_run_root):
        raise ValueError("Task-11 execution attestation must be outside the native run root")
    raw = requested.read_bytes()
    return _strict_json_line(raw, "Task-11 execution attestation"), raw


def _validate_task11_execution_attestation(
    path: Path,
    *,
    native_run_root: Path,
    treatment: Mapping[str, Any],
    expected_launch: Mapping[str, Any],
    expected_completion: Mapping[str, Any],
) -> str:
    attestation, raw = _load_external_task11_attestation(
        path, native_run_root=native_run_root
    )
    if set(attestation) != {"schema", "launch", "completion"}:
        raise ValueError("Task-11 execution attestation has unknown fields")
    if attestation.get("schema") != "txnmem-task11-appworld-execution-attestation-v1":
        raise ValueError("Task-11 execution attestation schema is unsupported")
    launch = attestation.get("launch")
    completion = attestation.get("completion")
    expected_launch_keys = set(expected_launch) | {"launch_id_sha256"}
    if not isinstance(launch, Mapping) or set(launch) != expected_launch_keys:
        raise ValueError("Task-11 launch attestation is incomplete")
    _require_sha256(launch.get("launch_id_sha256"), "Task-11 launch identity")
    launch_without_identity = {
        key: value for key, value in launch.items() if key != "launch_id_sha256"
    }
    if not canonical_json_equal(launch_without_identity, expected_launch):
        raise ValueError("Task-11 launch attestation does not match the native run")
    if not canonical_json_equal(completion, expected_completion):
        raise ValueError("Task-11 completion attestation does not match the native run")

    treatment_fingerprint = canonical_fingerprint(dict(treatment))
    registered = APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT.get(
        treatment_fingerprint
    )
    if registered is None:
        raise ValueError(
            "Task-11 execution attestation is not pre-registered for this treatment"
        )
    _require_sha256(registered, "registered Task-11 execution attestation hash")
    observed = hashlib.sha256(raw).hexdigest()
    if observed != registered:
        raise ValueError("Task-11 execution attestation hash is not pre-registered")
    return observed


def _validate_execution_condition(
    raw_summary: Mapping[str, Any], parent_manifest_hash: str
) -> tuple[dict[str, Any], str]:
    condition = raw_summary.get("condition")
    if not isinstance(condition, Mapping):
        raise ValueError("AppWorld native summary has no execution condition")
    normalized = dict(condition)
    fingerprint = _require_sha256(
        raw_summary.get("condition_fingerprint"),
        "AppWorld execution condition fingerprint",
    )
    if canonical_fingerprint(normalized) != fingerprint:
        raise ValueError("AppWorld execution condition fingerprint is stale")
    required_equal = {
        "benchmark": "appworld",
        "split": APPWORLD_FORMAL_SPLIT,
        "manifest_sha256": parent_manifest_hash,
        "model_execution_mode": "remote_endpoint",
        "model_revision_status": "sha256",
        "official_evaluator": "appworld.TestTracker.success_and_task_completed",
    }
    for field, expected in required_equal.items():
        if normalized.get(field) != expected:
            raise ValueError(f"AppWorld execution condition has invalid {field}")
    for field in ("model_id", "model_server_build", "runtime_version", "memory_backend"):
        value = normalized.get(field)
        if not isinstance(value, str) or not value.strip() or value in {"unknown", "unspecified"}:
            raise ValueError(f"AppWorld execution condition has no verified {field}")
    _require_sha256(normalized.get("model_revision"), "AppWorld model revision")
    runner_identity = normalized.get("runner_evaluator_source_identity")
    if not isinstance(runner_identity, Mapping):
        raise ValueError("AppWorld runner/evaluator source identity is missing")
    components = runner_identity.get("component_sha256")
    required_components = {
        "txnmem_experiment",
        "txnmem_benchmark_bridge",
        "txnmem_model_protocol",
        "txnmem_real_experiment",
        "appworld_environment",
        "appworld_evaluator",
        "appworld_common_evaluation",
    }
    if not isinstance(components, Mapping) or set(components) != required_components:
        raise ValueError("AppWorld runner/evaluator source identity is incomplete")
    normalized_components = dict(components)
    for name, digest in normalized_components.items():
        _require_sha256(digest, f"AppWorld source component {name}")
    if runner_identity.get("fingerprint") != canonical_fingerprint(normalized_components):
        raise ValueError("AppWorld runner/evaluator source fingerprint is stale")
    if raw_summary.get("model_execution_mode") != normalized["model_execution_mode"]:
        raise ValueError("AppWorld summary execution mode disagrees with its condition")
    if raw_summary.get("model_id") != normalized["model_id"]:
        raise ValueError("AppWorld summary model ID disagrees with its condition")
    if raw_summary.get("memory_backend") != normalized["memory_backend"]:
        raise ValueError("AppWorld summary memory backend disagrees with its condition")
    condition_repetitions = _require_exact_positive_int(
        normalized.get("repetitions"),
        "AppWorld execution condition repetitions",
    )
    summary_repetitions = _require_exact_positive_int(
        raw_summary.get("repetitions"),
        "AppWorld native repetitions",
    )
    if condition_repetitions != summary_repetitions:
        raise ValueError(
            "AppWorld execution condition repetitions disagree with its summary"
        )
    return normalized, fingerprint


def _redact_native_events(
    events: Sequence[Mapping[str, Any]],
    *,
    task_id: str,
    family_id: str,
    repetition: int,
) -> list[dict[str, Any]]:
    aliases: dict[str, dict[str, str]] = {
        "memory": {},
        "agent": {},
        "transaction": {},
        "scope": {},
    }

    def alias(namespace: str, value: Any, prefix: str) -> str:
        canonical = str(value)
        values = aliases[namespace]
        if canonical not in values:
            values[canonical] = f"{prefix}_{len(values) + 1:04d}"
        return values[canonical]

    redacted: list[dict[str, Any]] = []
    for event_index, event in enumerate(events, start=1):
        item: dict[str, Any] = {
            "task_id": task_id,
            "trial": repetition,
            "family_id": family_id,
            "event_id": f"{task_id}:rep{repetition:02d}:native:{event_index:04d}",
            "sequence": event_index,
            "step": int(event["step"]),
            "kind": str(event["kind"]),
            "agent_id": alias("agent", event["agent_id"], "agent"),
            "official_split": APPWORLD_FORMAL_SPLIT,
            "projection": "appworld_native_agent_memory_event_redacted",
            "source_projection": "appworld_native_agent_memory_event_redacted",
        }
        if event.get("txn_id") is not None:
            item["txn_id"] = alias(
                "transaction", event["txn_id"], "txn"
            )
        for field in (
            "memory_id",
            "output_id",
            "source_id",
            "old_memory_id",
            "new_memory_id",
        ):
            if event.get(field) is not None:
                item[field] = alias("memory", event[field], "memory")
        if event.get("source_ids") is not None:
            item["source_ids"] = [
                alias("memory", source_id, "memory")
                for source_id in event["source_ids"]
            ]
        for field in ("scope", "target_scope"):
            if event.get(field) is not None:
                item[field] = alias("scope", event[field], "scope")
        if event["kind"] in {"memory_write", "memory_derive", "memory_propagate"}:
            item["value"] = {"redacted": True}
        if event["kind"] == "memory_search" and event.get("query") is not None:
            item["query"] = "<redacted>"
        if event.get("attribute") is not None:
            item["attribute"] = "<redacted>"
        if event["kind"] in {"policy_change", "policy_revoke"} and event.get("target") in {
            "read",
            "search",
            "write",
            "derive",
            "propagate",
            "supersede",
        }:
            item["target"] = event["target"]
        redacted.append(item)
    validate_events(redacted)
    return redacted


def regenerate_appworld_native_realism_projection(
    native_run_root: Path,
    output_path: Path,
    *,
    task11_attestation_path: Path | None = None,
) -> dict[str, Any]:
    """Derive a payload-free realism stream from one native Agent run.

    A self-consistent run tree is only a candidate.  Formal promotion requires
    an out-of-tree Task-11 launch/completion attestation whose exact byte hash
    was registered independently in source control.
    """

    requested_root = Path(native_run_root).expanduser().absolute()
    if requested_root.is_symlink() or not requested_root.is_dir():
        raise ValueError("AppWorld native run root must be a real directory")
    store = FormalStore(requested_root)
    output_store, output_name, _ = _external_output_target(
        store.root,
        Path(output_path),
        "AppWorld native realism output",
    )
    parent = store.load_json("manifests", "appworld", "parent.json")
    validate_parent_manifest(parent)
    if parent.get("manifest_hash") != APPWORLD_FORMAL_PARENT_MANIFEST_SHA256:
        raise ValueError("AppWorld native run does not use the frozen official parent manifest")
    if parent.get("benchmark") != "appworld" or parent.get("split") != APPWORLD_FORMAL_SPLIT:
        raise ValueError("AppWorld native parent must be the test_normal split")
    if parent.get("task_count") != APPWORLD_FORMAL_TASK_COUNT or parent.get(
        "source_task_count"
    ) != APPWORLD_FORMAL_TASK_COUNT:
        raise ValueError("AppWorld native parent must contain exactly 168 Test-N tasks")
    source_identity = parent.get("source_identity")
    if not isinstance(source_identity, Mapping):
        raise ValueError("AppWorld native parent has no source identity")
    source_material = {
        key: value for key, value in source_identity.items() if key != "fingerprint"
    }
    if source_identity.get("fingerprint") != _canonical_hash(source_material):
        raise ValueError("AppWorld native parent source identity is stale")
    split_identity = source_identity.get("split_file")
    if (
        not isinstance(split_identity, Mapping)
        or split_identity.get("path") != "datasets/test_normal.txt"
        or split_identity.get("sha256") != APPWORLD_FORMAL_SPLIT_SHA256
    ):
        raise ValueError("AppWorld native parent is not bound to the frozen Test-N split")

    tasks = list(parent["tasks"])
    raw_task_ids: list[str] = []
    task_by_id: dict[str, Mapping[str, Any]] = {}
    family_by_task: dict[str, str] = {}
    for position, task in enumerate(tasks):
        task_id = task.get("task_id") if isinstance(task, Mapping) else None
        raw_task_id = task.get("raw_task_id") if isinstance(task, Mapping) else None
        if (
            not isinstance(task_id, str)
            or not isinstance(raw_task_id, str)
            or task_id != f"appworld-{raw_task_id}"
        ):
            raise ValueError("AppWorld native parent has a malformed task identity")
        if task_id in task_by_id or raw_task_id in raw_task_ids:
            raise ValueError("AppWorld native parent has duplicate task identities")
        if task.get("source_position") != position or task.get("source_index") != position:
            raise ValueError("AppWorld native parent task order is not frozen")
        family_id, derivation = _appworld_family(raw_task_id)
        if derivation != "audited_appworld_generator_prefix":
            raise ValueError("AppWorld Test-N family cannot be derived from task identity")
        raw_task_ids.append(raw_task_id)
        task_by_id[task_id] = task
        family_by_task[task_id] = family_id
    families = sorted(set(family_by_task.values()))
    if len(families) != APPWORLD_FORMAL_FAMILY_COUNT:
        raise ValueError("AppWorld native parent must contain exactly 56 families")
    public_identity = parent.get("public_split_identity")
    ordered_ids = json.dumps(
        raw_task_ids, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    if (
        not isinstance(public_identity, Mapping)
        or public_identity.get("benchmark") != "appworld"
        or public_identity.get("split") != APPWORLD_FORMAL_SPLIT
        or public_identity.get("source_task_count") != APPWORLD_FORMAL_TASK_COUNT
        or public_identity.get("selected_task_count") != APPWORLD_FORMAL_TASK_COUNT
        or public_identity.get("ordered_raw_task_ids_sha256")
        != hashlib.sha256(ordered_ids).hexdigest()
    ):
        raise ValueError("AppWorld native parent public split identity is stale")
    if parent.get("seed") != APPWORLD_FORMAL_SELECTION_SEED:
        raise ValueError("AppWorld native parent seed is not the frozen seed 17")
    expected_parent_condition = _canonical_hash(
        {
            "benchmark": "appworld",
            "domain": None,
            "split": APPWORLD_FORMAL_SPLIT,
            "seed": APPWORLD_FORMAL_SELECTION_SEED,
            "source_identity": dict(source_identity),
            "public_split_identity": dict(public_identity),
        }
    )
    if parent.get("condition_fingerprint") != expected_parent_condition:
        raise ValueError("AppWorld native parent condition fingerprint is stale")

    manifest_directory = store.path("manifests", "appworld")
    shard_indices: list[int] = []
    for entry in manifest_directory.iterdir():
        match = re.fullmatch(r"shard_([0-9]{3})\.json", entry.name)
        if match:
            if entry.is_symlink() or not entry.is_file():
                raise ValueError("AppWorld shard manifest must be a regular file")
            shard_indices.append(int(match.group(1)))
        elif entry.name.startswith("shard_"):
            raise ValueError("AppWorld native shard manifest name is unexpected")
    shard_indices.sort()
    if not shard_indices or shard_indices != list(range(len(shard_indices))):
        raise ValueError("AppWorld native shard manifests must be contiguous")
    expected_shards = shard_manifest(parent, len(shard_indices))
    run_directory = store.path("runs", "appworld")
    if run_directory.is_symlink() or not run_directory.is_dir():
        raise ValueError("AppWorld native run directory is unavailable")
    expected_run_names = {
        f"shard_{index:03d}" for index in range(len(expected_shards))
    }
    observed_run_names: set[str] = set()
    for entry in run_directory.iterdir():
        if entry.name.startswith("shard_"):
            if entry.is_symlink() or not entry.is_dir():
                raise ValueError("AppWorld native shard run must be a real directory")
            observed_run_names.add(entry.name)
    if observed_run_names != expected_run_names:
        raise ValueError("AppWorld native shard run directories are incomplete or unexpected")

    projection_rows: list[tuple[int, int, int, dict[str, Any]]] = []
    trace_artifacts: list[dict[str, Any]] = []
    task_event_counts: Counter[str] = Counter()
    task_status_counts: Counter[str] = Counter()
    seen_task_repetitions: set[tuple[str, int]] = set()
    execution_condition: dict[str, Any] | None = None
    execution_fingerprint: str | None = None
    treatment: dict[str, Any] | None = None
    prompt_profile: str | None = None
    shard_manifest_hashes: list[str] = []
    attested_shards: list[dict[str, Any]] = []

    for shard_index, expected_shard in enumerate(expected_shards):
        shard_name = f"shard_{shard_index:03d}"
        shard = store.load_json("manifests", "appworld", f"{shard_name}.json")
        if not canonical_json_equal(shard, expected_shard):
            raise ValueError("AppWorld shard manifest does not match the frozen parent")
        shard_manifest_hashes.append(str(shard["manifest_hash"]))
        raw_summary_bytes = store.load_bytes(
            "runs", "appworld", shard_name, "results", "native_batch_summary.json"
        )
        raw_summary = _strict_json_line(
            raw_summary_bytes,
            f"AppWorld native summary {shard_index}",
        )
        bound_report_bytes = store.load_bytes(
            "runs", "appworld", shard_name, "shard_report.json"
        )
        bound_report = _strict_json_line(
            bound_report_bytes,
            f"AppWorld bound shard report {shard_index}",
        )
        if not canonical_json_equal(
            bound_report, bind_native_shard_report(shard, raw_summary)
        ):
            raise ValueError("AppWorld bound shard report is stale")
        condition, fingerprint = _validate_execution_condition(
            raw_summary, str(parent["manifest_hash"])
        )
        if execution_condition is None:
            execution_condition = condition
            execution_fingerprint = fingerprint
        elif not canonical_json_equal(execution_condition, condition):
            raise ValueError("AppWorld native shards used different execution conditions")
        current_treatment = raw_summary.get("treatment")
        current_profile = raw_summary.get("prompt_profile")
        if not isinstance(current_treatment, Mapping) or not isinstance(
            current_profile, str
        ) or not current_profile:
            raise ValueError("AppWorld native summary has no treatment identity")
        if set(current_treatment) != {
            "prompt_profile",
            "trusted_preflight_enabled",
            "app_tool_strategy",
        }:
            raise ValueError("AppWorld native treatment has unregistered fields")
        if current_profile not in {"baseline", "tuned"}:
            raise ValueError("AppWorld native treatment has an invalid prompt profile")
        if type(current_treatment.get("trusted_preflight_enabled")) is not bool:
            raise ValueError("AppWorld native treatment preflight flag must be boolean")
        if current_treatment.get("app_tool_strategy") not in {
            "instruction_inferred",
            "manifest_scoped",
            "all_public",
        }:
            raise ValueError("AppWorld native treatment has an invalid tool strategy")
        if current_treatment.get("prompt_profile") != current_profile:
            raise ValueError("AppWorld treatment and prompt profile disagree")
        expected_preflight = current_profile == "tuned"
        if current_treatment.get("trusted_preflight_enabled") is not expected_preflight:
            raise ValueError(
                "AppWorld treatment preflight flag violates the prompt profile contract"
            )
        if current_treatment.get("app_tool_strategy") != condition.get(
            "appworld_model_tool_strategy"
        ):
            raise ValueError("AppWorld treatment and execution tool strategy disagree")
        if treatment is None:
            treatment = dict(current_treatment)
            prompt_profile = current_profile
        elif not canonical_json_equal(treatment, current_treatment) or prompt_profile != current_profile:
            raise ValueError("AppWorld native shards used different treatments")

        repetitions = _require_exact_positive_int(
            raw_summary.get("repetitions"), "AppWorld native repetitions"
        )
        if condition.get("repetitions") != repetitions:
            raise ValueError(
                "AppWorld execution condition repetitions disagree with its summary"
            )
        reported_profiles = raw_summary.get("prompt_profiles")
        if reported_profiles is not None and reported_profiles != [current_profile]:
            raise ValueError("AppWorld native prompt profile aggregate is stale")
        shard_run_directory = store.path("runs", "appworld", shard_name)
        expected_repetition_directories = (
            set()
            if repetitions == 1
            else {f"rep_{index:02d}" for index in range(1, repetitions + 1)}
        )
        observed_repetition_directories: set[str] = set()
        for entry in shard_run_directory.iterdir():
            if entry.name.startswith("rep_"):
                if entry.is_symlink() or not entry.is_dir():
                    raise ValueError("AppWorld repetition entry must be a real directory")
                observed_repetition_directories.add(entry.name)
        if observed_repetition_directories != expected_repetition_directories:
            raise ValueError("AppWorld native run has an unexpected repetition directory")
        artifacts = raw_summary.get("native_trace_artifacts")
        if not isinstance(artifacts, list) or len(artifacts) != repetitions:
            raise ValueError("AppWorld native summary has incomplete trace artifacts")
        summary_rows = raw_summary.get("task_summaries")
        if not isinstance(summary_rows, list):
            raise ValueError("AppWorld native summary has malformed task rows")
        shard_native_event_count = 0
        shard_attested_artifacts: list[dict[str, Any]] = []
        for repetition in range(1, repetitions + 1):
            artifact = artifacts[repetition - 1]
            expected_relative = (
                "data/native_model_traces.jsonl"
                if repetitions == 1
                else f"rep_{repetition:02d}/data/native_model_traces.jsonl"
            )
            if (
                not isinstance(artifact, Mapping)
                or set(artifact)
                != {"relative_path", "sha256", "size_bytes", "line_count"}
                or artifact.get("relative_path") != expected_relative
            ):
                raise ValueError("AppWorld native trace artifact path is unexpected")
            expected_hash = _require_sha256(
                artifact.get("sha256"), "AppWorld native trace hash"
            )
            expected_size = _require_exact_nonnegative_int(
                artifact.get("size_bytes"), "AppWorld native trace size"
            )
            expected_lines = _require_exact_nonnegative_int(
                artifact.get("line_count"), "AppWorld native trace line count"
            )
            artifact_parts = _canonical_artifact_parts(expected_relative)
            try:
                trace_bytes = store.load_bytes(
                    "runs", "appworld", shard_name, *artifact_parts
                )
            except (FormalIOError, OSError) as exc:
                raise ValueError(
                    "AppWorld native trace must be a regular file"
                ) from exc
            if len(trace_bytes) != expected_size:
                raise ValueError("AppWorld native trace size does not match its run summary")
            if hashlib.sha256(trace_bytes).hexdigest() != expected_hash:
                raise ValueError("AppWorld native trace hash does not match its run summary")
            nonempty_lines = [line for line in trace_bytes.splitlines() if line.strip()]
            if len(nonempty_lines) != expected_lines or expected_lines != len(shard["tasks"]):
                raise ValueError("AppWorld native trace does not cover its exact shard tasks")
            trace_artifacts.append(
                {
                    "shard_index": shard_index,
                    "repetition": repetition,
                    "relative_path": (
                        f"runs/appworld/{shard_name}/{expected_relative}"
                    ),
                    "sha256": expected_hash,
                    "size_bytes": expected_size,
                    "line_count": expected_lines,
                }
            )
            shard_attested_artifacts.append(
                {
                    "relative_path": expected_relative,
                    "sha256": expected_hash,
                    "size_bytes": expected_size,
                    "line_count": expected_lines,
                }
            )
            for task_offset, (line, task) in enumerate(
                zip(nonempty_lines, shard["tasks"])
            ):
                trace_row = _strict_json_line(
                    line,
                    f"AppWorld native trace row {shard_index}:{repetition}:{task_offset}",
                )
                task_id = task["task_id"]
                if trace_row.get("task_id") != task_id:
                    raise ValueError("AppWorld native trace task order does not match its shard")
                run = trace_row.get("run")
                if not isinstance(run, Mapping):
                    raise ValueError("AppWorld native trace row has no run mapping")
                summary_index = (repetition - 1) * len(shard["tasks"]) + task_offset
                summary_row = summary_rows[summary_index]
                if not isinstance(summary_row, Mapping) or summary_row.get("task_id") != task_id:
                    raise ValueError("AppWorld native task summary order is stale")
                if run.get("status") != summary_row.get("status"):
                    raise ValueError("AppWorld native trace status disagrees with its summary")
                for treatment_field, expected_value in (
                    ("prompt_profile", current_profile),
                    ("app_tool_strategy", current_treatment["app_tool_strategy"]),
                ):
                    if not canonical_json_equal(
                        summary_row.get(treatment_field), expected_value
                    ):
                        raise ValueError(
                            "AppWorld native task summary treatment disagrees with its shard"
                        )
                    if not canonical_json_equal(run.get(treatment_field), expected_value):
                        raise ValueError(
                            "AppWorld native trace treatment disagrees with its shard"
                        )
                summary_preflight = summary_row.get("trusted_preflight_enabled")
                run_preflight = run.get("trusted_preflight_enabled")
                if type(summary_preflight) is not bool or type(run_preflight) is not bool:
                    raise ValueError(
                        "AppWorld native task preflight evidence must be boolean"
                    )
                if summary_preflight is not expected_preflight or run_preflight is not expected_preflight:
                    raise ValueError(
                        "AppWorld native task preflight evidence disagrees with its shard"
                    )
                raw_events = run.get("events", [])
                if not isinstance(raw_events, list):
                    raise ValueError("AppWorld native trace events must be a list")
                validated = validate_events(raw_events)
                summary_event_count = _require_exact_nonnegative_int(
                    summary_row.get("native_event_count"),
                    "AppWorld native event count",
                )
                if summary_event_count != len(validated):
                    raise ValueError("AppWorld native event count disagrees with its summary")
                task_key = (task_id, repetition)
                if task_key in seen_task_repetitions:
                    raise ValueError("AppWorld native trace repeats a task repetition")
                seen_task_repetitions.add(task_key)
                shard_native_event_count += len(validated)
                task_event_counts[task_id] += len(validated)
                task_status_counts[str(run.get("status"))] += 1
                redacted = _redact_native_events(
                    validated,
                    task_id=str(task["raw_task_id"]),
                    family_id=family_by_task[task_id],
                    repetition=repetition,
                )
                for event_index, event in enumerate(redacted, start=1):
                    projection_rows.append(
                        (
                            int(task["source_position"]),
                            repetition,
                            event_index,
                            event,
                        )
                    )
        if raw_summary.get("native_event_count") != shard_native_event_count:
            raise ValueError("AppWorld shard native event count is stale")
        attested_shards.append(
            {
                "shard_index": shard_index,
                "manifest_hash": shard["manifest_hash"],
                "raw_summary_sha256": hashlib.sha256(
                    raw_summary_bytes
                ).hexdigest(),
                "bound_report_sha256": hashlib.sha256(
                    bound_report_bytes
                ).hexdigest(),
                "native_trace_artifacts": shard_attested_artifacts,
            }
        )

    if execution_condition is None or execution_fingerprint is None or treatment is None:
        raise ValueError("AppWorld native run has no complete execution evidence")
    expected_repetitions = _require_exact_positive_int(
        execution_condition.get("repetitions"),
        "AppWorld execution condition repetitions",
    )
    expected_pairs = {
        (task_id, repetition)
        for task_id in task_by_id
        for repetition in range(1, expected_repetitions + 1)
    }
    if seen_task_repetitions != expected_pairs:
        raise ValueError("AppWorld native run does not cover every task repetition")
    event_families = {
        family_by_task[task_id]
        for task_id, count in task_event_counts.items()
        if count > 0
    }
    if event_families != set(families):
        raise ValueError("AppWorld native traces do not represent every Test-N family")

    projection_rows.sort(key=lambda row: row[:3])
    serialized = "".join(
        json.dumps(row[3], ensure_ascii=False, sort_keys=True) + "\n"
        for row in projection_rows
    )
    family_selection = select_appworld_realism_families(
        raw_task_ids,
        evaluation_family_count=APPWORLD_FORMAL_EVALUATION_FAMILY_COUNT,
        calibration_family_count=None,
        seed=APPWORLD_FORMAL_SELECTION_SEED,
        official_split=APPWORLD_FORMAL_SPLIT,
    )
    assert treatment is not None
    assert prompt_profile is not None
    expected_launch = {
        "benchmark": "appworld",
        "split": APPWORLD_FORMAL_SPLIT,
        "parent_manifest_hash": parent["manifest_hash"],
        "shard_count": len(expected_shards),
        "repetitions": expected_repetitions,
        "shard_manifest_hashes": shard_manifest_hashes,
        "execution_condition": execution_condition,
        "execution_condition_fingerprint": execution_fingerprint,
        "prompt_profile": prompt_profile,
        "treatment": treatment,
    }
    expected_completion = {
        "status": "completed",
        "shards": attested_shards,
    }
    treatment_fingerprint = canonical_fingerprint(treatment)
    attestation_sha256: str | None = None
    if task11_attestation_path is not None:
        attestation_sha256 = _validate_task11_execution_attestation(
            Path(task11_attestation_path),
            native_run_root=store.root,
            treatment=treatment,
            expected_launch=expected_launch,
            expected_completion=expected_completion,
        )
    formally_eligible = attestation_sha256 is not None
    with output_store.open_text_exclusive(output_name) as output_handle:
        output_handle.write(serialized)
    return {
        "schema": "appworld-native-realism-inventory-v1",
        "evidence_scope": (
            "trace_grounded_native_agent_execution"
            if formally_eligible
            else "candidate_native_bundle"
        ),
        "promotion_status": "eligible" if formally_eligible else "blocked",
        "blocking_reason": (
            None
            if formally_eligible
            else "unregistered_task11_execution_attestation"
        ),
        "task11_execution_attestation_sha256": attestation_sha256,
        "task11_treatment_fingerprint": treatment_fingerprint,
        "benchmark": "appworld",
        "official_split": APPWORLD_FORMAL_SPLIT,
        "dataset_file_sha256": APPWORLD_FORMAL_SPLIT_SHA256,
        "parent_manifest_hash": parent["manifest_hash"],
        "parent_condition_fingerprint": parent["condition_fingerprint"],
        "source_identity": dict(source_identity),
        "task_count": APPWORLD_FORMAL_TASK_COUNT,
        "task_repetition_count": len(seen_task_repetitions),
        "family_count": APPWORLD_FORMAL_FAMILY_COUNT,
        "event_count": len(projection_rows),
        "zero_event_count": sum(
            task_event_counts[task_id] == 0 for task_id in task_by_id
        ),
        "zero_event_task_ids": sorted(
            str(task_by_id[task_id]["raw_task_id"])
            for task_id in task_by_id
            if task_event_counts[task_id] == 0
        ),
        "status_counts": dict(sorted(task_status_counts.items())),
        "native_trace_file_count": len(trace_artifacts),
        "native_trace_artifacts": trace_artifacts,
        "execution_condition_fingerprint": execution_fingerprint,
        "execution": {
            "model_id": execution_condition["model_id"],
            "model_revision": execution_condition["model_revision"],
            "model_server_build": execution_condition["model_server_build"],
            "runtime_version": execution_condition["runtime_version"],
            "memory_backend": execution_condition["memory_backend"],
            "model_execution_mode": execution_condition["model_execution_mode"],
            "runner_evaluator_source_identity": execution_condition[
                "runner_evaluator_source_identity"
            ],
            "prompt_profile": prompt_profile,
            "treatment": treatment,
        },
        "family_selection": family_selection,
        "output_sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "output_line_count": len(projection_rows),
        "request_data_values_retained": False,
        "native_trace_structure_rederived": True,
        "formal_evidence_eligible": formally_eligible,
        "production_latency_claim": False,
    }


def validate_appworld_native_realism_bundle(
    *,
    events_path: Path,
    selection_path: Path,
    inventory_path: Path,
    native_run_root: Path,
    task11_attestation_path: Path | None = None,
) -> dict[str, Any]:
    """Re-derive an AppWorld native bundle and report its promotion status."""

    selection, selection_raw = _load_json_mapping(
        Path(selection_path), "AppWorld family selection"
    )
    inventory, inventory_raw = _load_json_mapping(
        Path(inventory_path), "AppWorld native realism inventory"
    )
    events_source = Path(events_path)
    if events_source.is_symlink() or not events_source.is_file():
        raise ValueError("AppWorld native realism events must be a regular file")
    events_raw = events_source.read_bytes()
    with TemporaryDirectory(prefix="txnmem-appworld-native-verify-") as temporary:
        regenerated_path = Path(temporary) / "events.jsonl"
        expected_inventory = regenerate_appworld_native_realism_projection(
            Path(native_run_root),
            regenerated_path,
            task11_attestation_path=task11_attestation_path,
        )
        regenerated_raw = regenerated_path.read_bytes()
    if events_raw != regenerated_raw:
        raise ValueError("AppWorld native realism events do not match trace re-derivation")
    if not canonical_json_equal(inventory, expected_inventory):
        raise ValueError("AppWorld native realism inventory does not match trace re-derivation")
    expected_selection = {
        **expected_inventory["family_selection"],
        "dataset_file_sha256": APPWORLD_FORMAL_SPLIT_SHA256,
    }
    if not canonical_json_equal(selection, expected_selection):
        raise ValueError(
            "AppWorld family selection must exactly equal the frozen native 50/6 partition"
        )
    return {
        "binding_schema": "appworld-test-normal-native-realism-bundle-v1",
        "evidence_scope": expected_inventory["evidence_scope"],
        "promotion_status": expected_inventory["promotion_status"],
        "blocking_reason": expected_inventory["blocking_reason"],
        "task11_execution_attestation_sha256": expected_inventory[
            "task11_execution_attestation_sha256"
        ],
        "task11_treatment_fingerprint": expected_inventory[
            "task11_treatment_fingerprint"
        ],
        "official_split": APPWORLD_FORMAL_SPLIT,
        "dataset_file_sha256": APPWORLD_FORMAL_SPLIT_SHA256,
        "parent_manifest_hash": expected_inventory["parent_manifest_hash"],
        "execution_condition_fingerprint": expected_inventory[
            "execution_condition_fingerprint"
        ],
        "task_count": expected_inventory["task_count"],
        "task_repetition_count": expected_inventory["task_repetition_count"],
        "family_count": expected_inventory["family_count"],
        "evaluation_family_count": APPWORLD_FORMAL_EVALUATION_FAMILY_COUNT,
        "calibration_family_count": APPWORLD_FORMAL_CALIBRATION_FAMILY_COUNT,
        "family_overlap_count": 0,
        "event_count": expected_inventory["event_count"],
        "zero_event_count": expected_inventory["zero_event_count"],
        "native_trace_file_count": expected_inventory["native_trace_file_count"],
        "selection_sha256": hashlib.sha256(selection_raw).hexdigest(),
        "inventory_sha256": hashlib.sha256(inventory_raw).hexdigest(),
        "projection_sha256": hashlib.sha256(events_raw).hexdigest(),
        "execution": expected_inventory["execution"],
        "family_selection": expected_inventory["family_selection"],
        "native_trace_rederived": True,
        "request_data_values_retained": False,
    }


def validate_appworld_realism_bundle(
    *,
    events_path: Path,
    selection_path: Path,
    projection_inventory_path: Path,
    dataset_path: Path,
    appworld_root: Path,
    expected_dataset_sha256: str = APPWORLD_FORMAL_SPLIT_SHA256,
) -> dict[str, Any]:
    """Reject promotion of reference API projections to formal Agent evidence."""

    raise ValueError(
        "AppWorld reference projection is diagnostic only; formal realism requires "
        "a source-bound native Agent trace bundle"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--appworld-root", type=Path)
    parser.add_argument("--task-ids", nargs="+")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--official-split", default="unknown")
    parser.add_argument("--dataset-file", type=Path)
    parser.add_argument("--selection-output", type=Path)
    parser.add_argument("--evaluation-family-count", type=int, default=50)
    parser.add_argument("--calibration-family-count", type=int, default=None)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--native-run-root", type=Path)
    parser.add_argument("--native-output", type=Path)
    parser.add_argument("--native-inventory", type=Path)
    parser.add_argument("--native-task11-execution-attestation", type=Path)
    args = parser.parse_args(argv)
    native_values = (
        args.native_run_root,
        args.native_output,
        args.native_inventory,
        args.native_task11_execution_attestation,
    )
    if any(value is not None for value in native_values):
        if any(
            value is None
            for value in (
                args.native_run_root,
                args.native_output,
                args.native_inventory,
            )
        ):
            parser.error(
                "--native-run-root, --native-output, and --native-inventory "
                "must be supplied together"
            )
        if any(
            value is not None
            for value in (
                args.appworld_root,
                args.task_ids,
                args.output,
                args.inventory,
                args.dataset_file,
                args.selection_output,
            )
        ):
            parser.error("native export and reference projection arguments are mutually exclusive")
        requested_native_root = Path(args.native_run_root).expanduser().absolute()
        if requested_native_root.is_symlink() or not requested_native_root.is_dir():
            parser.error("--native-run-root must be an existing real directory")
        native_store = FormalStore(requested_native_root)
        _, inventory_candidate = _resolved_output_candidate(
            args.native_inventory,
            "AppWorld native realism inventory",
        )
        _, events_candidate = _resolved_output_candidate(
            args.native_output,
            "AppWorld native realism output",
        )
        if _is_within(events_candidate, inventory_candidate) or _is_within(
            inventory_candidate, events_candidate
        ):
            parser.error(
                "native output and inventory must be distinct non-ancestor files"
            )
        inventory_store, inventory_name, inventory_target = _external_output_target(
            native_store.root,
            args.native_inventory,
            "AppWorld native realism inventory",
        )
        _, _, events_target = _external_output_target(
            native_store.root,
            args.native_output,
            "AppWorld native realism output",
        )
        if inventory_target == events_target:
            parser.error("native output and inventory must be distinct files")
        native_inventory = regenerate_appworld_native_realism_projection(
            args.native_run_root,
            args.native_output,
            task11_attestation_path=args.native_task11_execution_attestation,
        )
        inventory_store.write_json_exclusive(
            inventory_name,
            payload=native_inventory,
        )
        print(json.dumps(native_inventory, ensure_ascii=False, sort_keys=True))
        return 0
    if args.appworld_root is None:
        parser.error("reference projection requires --appworld-root")
    if args.selection_output is not None:
        if args.dataset_file is None or args.selection_output is None:
            parser.error("--dataset-file and --selection-output must be supplied together")
        if args.task_ids is not None or args.output is not None or args.inventory is not None:
            parser.error("family selection and projection arguments are mutually exclusive")
        selection = select_appworld_realism_families_from_dataset(
            args.dataset_file,
            evaluation_family_count=args.evaluation_family_count,
            calibration_family_count=args.calibration_family_count,
            seed=args.seed,
            official_split=args.official_split,
        )
        args.selection_output.parent.mkdir(parents=True, exist_ok=True)
        args.selection_output.write_text(
            json.dumps(selection, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(selection, ensure_ascii=False, sort_keys=True))
        return 0
    if args.task_ids is None or args.output is None or args.inventory is None:
        parser.error("projection requires --task-ids, --output, and --inventory")
    inventory = regenerate_appworld_projection(
        args.appworld_root,
        args.task_ids,
        args.output,
        official_split=args.official_split,
        dataset_path=args.dataset_file,
    )
    args.inventory.parent.mkdir(parents=True, exist_ok=True)
    args.inventory.write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(inventory, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
