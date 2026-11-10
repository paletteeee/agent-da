"""Manifest generators for native benchmark task runs.

Each generator produces a TxnMem task manifest (manifest_version=1) with
task_id, prompt/instruction, failure_schedule, acceptance and fixed seeds,
consumable by txnmem_real_experiment.run_experiment_manifest.
"""

from __future__ import annotations

import json
import hashlib
import importlib
import importlib.metadata
import random
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _canonical_manifest(dataset_name: str, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = {
        "manifest_version": 1,
        "dataset_name": dataset_name,
        "tasks": tasks,
    }
    return normalized


def _canonical_hash(value: Mapping[str, Any]) -> str:
    normalized = {key: item for key, item in value.items() if key != "manifest_hash"}
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_identity(path: Path, relative_path: str) -> dict[str, str]:
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"required AppWorld identity file is missing: {relative_path}") from exc
    return {
        "path": relative_path,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _resolve_appworld_data_root(root: str | Path) -> Path:
    supplied = Path(root)
    candidates = []
    if (supplied / "tasks").is_dir() and (supplied / "datasets").is_dir():
        candidates.append(supplied)
    nested = supplied / "data"
    if (nested / "tasks").is_dir() and (nested / "datasets").is_dir():
        candidates.append(nested)
    unique = list(dict.fromkeys(path.resolve() for path in candidates))
    if not unique:
        raise ValueError(f"AppWorld root does not contain an official data tree: {supplied}")
    if len(unique) != 1:
        raise ValueError(f"ambiguous AppWorld data root: {supplied}")
    return unique[0]


def _task(
    task_id: str,
    instruction: str,
    *,
    agent_id: str = "agent_1",
    seed: int = 0,
    temperature: float = 0.0,
    max_steps: int = 30,
    failure_schedule: list[dict[str, Any]] | None = None,
    acceptance: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_id": task_id,
        "instruction": instruction,
        "prompt": instruction,
        "agent_id": agent_id,
        "seed": seed,
        "temperature": temperature,
        "max_steps": max_steps,
        "failure_schedule": failure_schedule or [],
        "acceptance": acceptance or {"expected_status": "completed"},
    }
    if extra:
        task.update(extra)
    return task


def generate_tau_bench_manifest(
    *,
    domain: str = "airline",
    task_split: str = "test",
    max_tasks: int | None = None,
    seed: int = 0,
    max_steps: int = 30,
) -> dict[str, Any]:
    """Generate a manifest from the official tau-bench task lists."""
    sources = {
        ("airline", "test"): ("tau_bench.envs.airline.tasks_test", "TASKS"),
        ("retail", "train"): ("tau_bench.envs.retail.tasks_train", "TASKS_TRAIN"),
        ("retail", "dev"): ("tau_bench.envs.retail.tasks_dev", "TASKS_DEV"),
        ("retail", "test"): ("tau_bench.envs.retail.tasks_test", "TASKS_TEST"),
    }
    try:
        module_name, task_symbol = sources[(domain, task_split)]
    except KeyError as exc:
        if domain not in {item[0] for item in sources}:
            raise ValueError(f"unsupported tau-bench domain: {domain}") from exc
        raise ValueError(
            f"unsupported tau-bench split {task_split!r} for domain {domain!r}"
        ) from exc
    dependency_root = "external_data/deps/tau-bench"
    if dependency_root not in sys.path:
        sys.path.insert(0, dependency_root)
    module = importlib.import_module(module_name)
    TASKS = getattr(module, task_symbol, None)
    if not isinstance(TASKS, (list, tuple)):
        raise ValueError(f"malformed tau-bench task source: {module_name}.{task_symbol}")
    source_path_value = getattr(module, "__file__", None)
    if not isinstance(source_path_value, str):
        raise ValueError(f"tau-bench task source has no file identity: {module_name}")
    source_path = Path(source_path_value)
    try:
        source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ValueError(f"tau-bench task source is unreadable: {module_name}") from exc
    try:
        package_version = importlib.metadata.version("tau-bench")
    except importlib.metadata.PackageNotFoundError as exc:
        raise ValueError("tau-bench package identity is unavailable") from exc
    task_source = {
        "module": module_name,
        "path": module_name.replace(".", "/") + ".py",
        "sha256": source_hash,
    }
    identity_material = {
        "task_source": task_source,
        "package": {"distribution": "tau-bench", "version": package_version},
    }
    source_identity = {
        **identity_material,
        "fingerprint": _canonical_hash(identity_material),
    }

    tasks = []
    for index, task in enumerate(TASKS):
        if max_tasks is not None and index >= max_tasks:
            break
        instruction = getattr(task, "instruction", "") or (task.get("instruction", "") if isinstance(task, dict) else "")
        if isinstance(task, Mapping) and "task_id" in task:
            raw_task_id = task["task_id"]
        elif hasattr(task, "task_id"):
            raw_task_id = getattr(task, "task_id")
        else:
            raw_task_id = index
        task_id = f"tau-{domain}-{task_split}-{index:04d}"
        tasks.append(
            _task(
                task_id,
                str(instruction),
                seed=seed,
                max_steps=max_steps,
                extra={
                    "domain": domain,
                    "task_split": task_split,
                    "task_index": index,
                    "raw_task_id": raw_task_id,
                    "source_index": index,
                    "source_position": index,
                },
            )
        )
    manifest = _canonical_manifest(f"tau-bench-{domain}-{task_split}", tasks)
    manifest.update(
        {
            "benchmark": "tau-bench",
            "domain": domain,
            "split": task_split,
            "source_task_count": len(TASKS),
            "source_identity": source_identity,
        }
    )
    return manifest


def generate_appworld_manifest(
    data_root: str | Path = "external_data/deps/appworld-data/data",
    *,
    task_split: str = "test_normal",
    max_tasks: int | None = None,
    seed: int = 0,
    max_steps: int = 30,
) -> dict[str, Any]:
    """Generate a manifest from the official AppWorld task specs."""
    if not isinstance(task_split, str) or not task_split or Path(task_split).name != task_split:
        raise ValueError("task_split must be a non-empty split name")
    data_root = _resolve_appworld_data_root(data_root)
    split_relative = f"datasets/{task_split}.txt"
    split_path = data_root / split_relative
    split_identity = _file_identity(split_path, split_relative)
    version_identity = _file_identity(data_root / "version.txt", "version.txt")
    try:
        raw_ids = split_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"AppWorld split file is not valid UTF-8: {split_relative}") from exc
    task_ids = []
    for raw_task_id in raw_ids:
        if (
            raw_task_id != raw_task_id.strip()
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", raw_task_id) is None
        ):
            raise ValueError(f"malformed AppWorld split task ID: {raw_task_id!r}")
        task_ids.append(raw_task_id)
    seen_ids: set[str] = set()
    for task_id in task_ids:
        if task_id in seen_ids:
            raise ValueError(f"duplicate AppWorld split task ID: {task_id}")
        seen_ids.add(task_id)
    missing_ids = [
        task_id for task_id in task_ids if not (data_root / "tasks" / task_id).is_dir()
    ]
    if missing_ids:
        raise ValueError(f"missing AppWorld task directory: {missing_ids[0]}")

    tasks = []
    selected_ids = task_ids if max_tasks is None else task_ids[:max_tasks]
    for index, raw_task_id in enumerate(selected_ids):
        task_dir = data_root / "tasks" / raw_task_id
        if max_tasks is not None and index >= max_tasks:
            break
        specs_path = task_dir / "specs.json"
        if not specs_path.exists():
            raise ValueError(f"malformed AppWorld specs for {raw_task_id}: missing specs.json")
        try:
            specs = json.loads(specs_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"malformed AppWorld specs for {raw_task_id}") from exc
        if not isinstance(specs, Mapping) or not isinstance(specs.get("instruction"), str):
            raise ValueError(f"malformed AppWorld specs for {raw_task_id}")
        instruction = specs.get("instruction", "")
        task_id = f"appworld-{raw_task_id}"
        app_names = []
        dbs_dir = task_dir / "dbs"
        if dbs_dir.is_dir():
            for db_path in sorted(dbs_dir.glob("*.jsonl")):
                try:
                    has_data = db_path.stat().st_size > 0
                except OSError:
                    has_data = False
                if has_data and db_path.stem != "admin":
                    app_names.append(db_path.stem)
        api_name_allowlist = None
        if "amazon" in app_names:
            api_name_allowlist = [
                "amazon__search_products",
                "amazon__show_product",
                "amazon__show_cart",
                "amazon__show_wish_list",
                "amazon__show_product_rating_distribution",
                "amazon__move_product_from_cart_to_wish_list",
                "amazon__move_product_from_wish_list_to_cart",
            ]
        tasks.append(
            _task(
                task_id,
                instruction,
                seed=seed,
                max_steps=max_steps,
                extra={
                    "task_dir": raw_task_id,
                    "raw_task_id": raw_task_id,
                    "source_index": index,
                    "source_position": index,
                    "task_split": task_split,
                    "app_names": app_names,
                    "api_name_allowlist": api_name_allowlist,
                    "supervisor": specs.get("supervisor"),
                    "datetime": specs.get("datetime"),
                },
            )
        )
    identity_material = {
        "split_file": split_identity,
        "version_file": version_identity,
    }
    source_identity = {
        **identity_material,
        "fingerprint": _canonical_hash(identity_material),
    }
    manifest = _canonical_manifest(f"appworld-{task_split}", tasks)
    manifest.update(
        {
            "benchmark": "appworld",
            "split": task_split,
            "source_task_count": len(task_ids),
            "source_identity": source_identity,
        }
    )
    return manifest


def shard_manifest(
    manifest: Mapping[str, Any], shard_count: int
) -> list[dict[str, Any]]:
    """Partition an ordered parent manifest exactly once by source position."""

    if not isinstance(manifest, Mapping):
        raise ValueError("manifest must be a mapping")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("manifest.tasks must be a non-empty list")
    if (
        isinstance(shard_count, bool)
        or not isinstance(shard_count, int)
        or shard_count < 1
        or shard_count > len(tasks)
    ):
        raise ValueError("shard_count must be between 1 and the task count")
    parent_hash = manifest.get("manifest_hash")
    if not isinstance(parent_hash, str) or parent_hash != _canonical_hash(manifest):
        raise ValueError("parent manifest_hash does not match the canonical manifest")
    task_ids: list[str] = []
    positions: list[int] = []
    normalized_tasks: list[dict[str, Any]] = []
    for index, task in enumerate(tasks):
        if not isinstance(task, Mapping):
            raise ValueError(f"manifest task {index} must be a mapping")
        task_id = task.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            raise ValueError(f"manifest task {index} has an invalid task_id")
        if task_id in task_ids:
            raise ValueError(f"duplicate task ID: {task_id}")
        position = task.get("source_position", index)
        if isinstance(position, bool) or not isinstance(position, int):
            raise ValueError(f"task {task_id} has an invalid source_position")
        task_ids.append(task_id)
        positions.append(position)
        item = dict(task)
        item["source_position"] = position
        normalized_tasks.append(item)
    if positions != list(range(len(tasks))):
        raise ValueError("source positions must be unique, contiguous, and ordered")

    required = ("benchmark", "split", "source_identity", "condition_fingerprint")
    for field in required:
        if field not in manifest:
            raise ValueError(f"parent manifest is missing {field}")
    shards: list[dict[str, Any]] = []
    for shard_index in range(shard_count):
        shard_tasks = [
            dict(task)
            for task in normalized_tasks
            if task["source_position"] % shard_count == shard_index
        ]
        shard: dict[str, Any] = {
            "manifest_version": int(manifest.get("manifest_version", 1)),
            "dataset_name": str(manifest.get("dataset_name", "benchmark")),
            "benchmark": manifest["benchmark"],
            "split": manifest["split"],
            "source_identity": manifest["source_identity"],
            "condition_fingerprint": manifest["condition_fingerprint"],
            "parent_manifest_hash": parent_hash,
            "shard_index": shard_index,
            "shard_count": shard_count,
            "task_count": len(shard_tasks),
            "tasks": shard_tasks,
        }
        if "domain" in manifest:
            shard["domain"] = manifest["domain"]
        shard["manifest_hash"] = _canonical_hash(shard)
        shards.append(shard)
    return shards


def generate_locomo_manifest(
    *,
    source: str | Path = "external_data/raw/locomo10.json",
    max_tasks: int | None = None,
    seed: int = 0,
    max_steps: int = 30,
) -> dict[str, Any]:
    """Generate a manifest from the LoCoMo conversation samples."""
    source = Path(source)
    data = json.loads(source.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        samples = data.get("samples") or data.get("data") or [data]
    elif isinstance(data, list):
        samples = data
    else:
        raise ValueError(f"unsupported LoCoMo source structure: {type(data)}")
    tasks = []
    for index, sample in enumerate(samples):
        if max_tasks is not None and index >= max_tasks:
            break
        sample_id = str(sample.get("sample_id") or f"locomo_{index:04d}")
        conversation = sample.get("conversation", {})
        first_session = None
        for key in sorted(conversation):
            if key.startswith("session_") and key.endswith("_date_time"):
                first_session = conversation[key]
                break
        instruction = (
            f"Continue the long-term multi-session conversation of user {sample_id}. "
            f"Session history and summaries are available through memory tools."
        )
        if first_session is not None:
            instruction += f" First session started at {first_session}."
        tasks.append(
            _task(
                f"locomo-{sample_id}",
                instruction,
                seed=seed,
                max_steps=max_steps,
                extra={"sample_id": sample_id},
            )
        )
    return _canonical_manifest("locomo", tasks)


def build_native_scale_manifest(
    benchmark: str,
    source: str | Path | None,
    limit: int,
    seed: int = 17,
    split: str = "test",
) -> dict[str, Any]:
    """Build a fixed, hashed task-level manifest for a public batch run.

    ``source`` is a benchmark-specific source: tau-bench domain name,
    AppWorld data root, or LoCoMo JSON path.  The raw task text remains an
    input to the remote run; the returned hash and split metadata are the
    reproducibility boundary used by aggregate reports.
    """

    if not isinstance(benchmark, str) or benchmark not in {"tau-bench", "appworld", "locomo"}:
        raise ValueError("benchmark must be tau-bench, appworld, or locomo")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    if not isinstance(split, str) or not split.strip():
        raise ValueError("split must be a non-empty string")

    if benchmark == "tau-bench":
        domain = str(source or "airline")
        manifest = generate_tau_bench_manifest(
            domain=domain, task_split=split, max_tasks=limit, seed=seed
        )
    elif benchmark == "appworld":
        data_root = Path(source or "external_data/deps/appworld-data/data")
        manifest = generate_appworld_manifest(
            data_root=data_root,
            task_split=split,
            max_tasks=limit,
            seed=seed,
        )
    else:
        locomo_source = Path(source or "external_data/raw/locomo10.json")
        manifest = generate_locomo_manifest(source=locomo_source, max_tasks=limit, seed=seed)

    tasks = [dict(task) for task in manifest.get("tasks", [])]
    if len(tasks) != limit:
        raise ValueError(f"{benchmark} source provided {len(tasks)} tasks, expected {limit}")
    for position, task in enumerate(tasks):
        task.setdefault("source_position", position)
        task.setdefault("source_index", position)
        task.setdefault("raw_task_id", task.get("task_id"))
    task_ids = [str(task.get("task_id")) for task in tasks]
    if any(not task_id or task_id == "None" for task_id in task_ids):
        raise ValueError("every task must have a non-empty task_id")
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("duplicate task IDs are not allowed")

    shuffled = list(task_ids)
    random.Random(seed).shuffle(shuffled)
    holdout_count = max(1, round(len(shuffled) * 0.2))
    holdout_set = set(shuffled[:holdout_count])
    holdout_ids = [task_id for task_id in task_ids if task_id in holdout_set]
    train_ids = [task_id for task_id in task_ids if task_id not in holdout_set]
    normalized = dict(manifest)
    normalized["tasks"] = tasks
    normalized["benchmark"] = benchmark
    if benchmark == "tau-bench":
        normalized["domain"] = str(source or "airline")
    normalized["seed"] = int(seed)
    normalized["split"] = split
    normalized["task_count"] = len(tasks)
    normalized["task_level_split"] = {
        "seed": int(seed),
        "source_split": split,
        "train_task_ids": train_ids,
        "holdout_task_ids": holdout_ids,
    }
    raw_task_ids = [task["raw_task_id"] for task in tasks]
    ordered_raw_ids = json.dumps(
        raw_task_ids,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    normalized["public_split_identity"] = {
        "benchmark": benchmark,
        "split": split,
        "source_task_count": int(manifest.get("source_task_count", len(tasks))),
        "selected_task_count": len(tasks),
        "ordered_raw_task_ids_sha256": hashlib.sha256(ordered_raw_ids).hexdigest(),
    }
    if "domain" in normalized:
        normalized["public_split_identity"]["domain"] = normalized["domain"]
    if isinstance(manifest.get("source_identity"), Mapping):
        normalized["source_identity"] = dict(manifest["source_identity"])
        normalized["source_sha256"] = str(manifest["source_identity"]["fingerprint"])
    elif isinstance(source, (str, Path)) and Path(str(source)).is_file():
        source_bytes = Path(str(source)).read_bytes()
        normalized["source_sha256"] = hashlib.sha256(source_bytes).hexdigest()
        normalized["source_identity"] = {
            "source_file": {
                "path": Path(str(source)).name,
                "sha256": normalized["source_sha256"],
            },
            "fingerprint": normalized["source_sha256"],
        }
    else:
        normalized["source_sha256"] = hashlib.sha256(str(source or benchmark).encode("utf-8")).hexdigest()
        normalized["source_identity"] = {
            "source_label_sha256": normalized["source_sha256"],
            "fingerprint": normalized["source_sha256"],
        }
    condition = {
        "benchmark": benchmark,
        "domain": normalized.get("domain"),
        "split": split,
        "seed": int(seed),
        "source_identity": normalized["source_identity"],
        "public_split_identity": normalized["public_split_identity"],
    }
    normalized["condition_fingerprint"] = _canonical_hash(condition)
    normalized["manifest_hash"] = _canonical_hash(normalized)
    return normalized


def write_manifest(manifest: dict[str, Any], out_path: str | Path) -> str:
    """Write a manifest and return its canonical SHA-256 digest."""
    digest = _canonical_hash(manifest)
    provided_digest = manifest.get("manifest_hash")
    if provided_digest is not None and provided_digest != digest:
        raise ValueError("manifest_hash does not match the canonical manifest")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return digest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate TxnMem benchmark task manifests.")
    parser.add_argument("--benchmark", choices=["tau-bench", "appworld", "locomo"], required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-tasks", type=int, default=None)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tau-domain", default="airline", choices=["airline", "retail"])
    parser.add_argument("--tau-split", default="test", choices=["test", "train", "dev"])
    parser.add_argument("--locomo-source", type=Path, default=Path("external_data/raw/locomo10.json"))
    args = parser.parse_args()

    if args.benchmark == "tau-bench":
        manifest = generate_tau_bench_manifest(
            domain=args.tau_domain, task_split=args.tau_split, max_tasks=args.max_tasks, seed=args.seed
        )
    elif args.benchmark == "appworld":
        manifest = generate_appworld_manifest(max_tasks=args.max_tasks, seed=args.seed)
    else:
        manifest = generate_locomo_manifest(source=args.locomo_source, max_tasks=args.max_tasks, seed=args.seed)

    digest = write_manifest(manifest, args.out)
    print(f"wrote {args.out}: {len(manifest['tasks'])} tasks, sha256={digest}")
