"""Regenerate redacted AppWorld reference-API projection source events."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


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
        "raw_official_request_values_committed": False,
        "trace_ground_truth_native": False,
        "production_latency_claim": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--appworld-root", type=Path, required=True)
    parser.add_argument("--task-ids", nargs="+")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--official-split", default="unknown")
    parser.add_argument("--dataset-file", type=Path)
    parser.add_argument("--selection-output", type=Path)
    parser.add_argument("--evaluation-family-count", type=int, default=50)
    parser.add_argument("--calibration-family-count", type=int, default=None)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args(argv)
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
