"""Manifest generators for native benchmark task runs.

Each generator produces a TxnMem task manifest (manifest_version=1) with
task_id, prompt/instruction, failure_schedule, acceptance and fixed seeds,
consumable by txnmem_real_experiment.run_experiment_manifest.
"""

from __future__ import annotations

import json
import hashlib
import os
import random
from pathlib import Path
from typing import Any


def _canonical_manifest(dataset_name: str, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = {
        "manifest_version": 1,
        "dataset_name": dataset_name,
        "tasks": tasks,
    }
    return normalized


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
    import sys

    if domain == "airline":
        sys.path.insert(0, "external_data/deps/tau-bench")
        from tau_bench.envs.airline.tasks_test import TASKS  # type: ignore
    elif domain == "retail":
        sys.path.insert(0, "external_data/deps/tau-bench")
        if task_split == "train":
            from tau_bench.envs.retail.tasks_train import TASKS_TRAIN as TASKS  # type: ignore
        elif task_split == "dev":
            from tau_bench.envs.retail.tasks_dev import TASKS_DEV as TASKS  # type: ignore
        else:
            from tau_bench.envs.retail.tasks_test import TASKS_TEST as TASKS  # type: ignore
    else:
        raise ValueError(f"unsupported tau-bench domain: {domain}")

    tasks = []
    for index, task in enumerate(TASKS):
        if max_tasks is not None and index >= max_tasks:
            break
        instruction = getattr(task, "instruction", "") or (task.get("instruction", "") if isinstance(task, dict) else "")
        task_id = f"tau-{domain}-{task_split}-{index:04d}"
        tasks.append(
            _task(
                task_id,
                str(instruction),
                seed=seed,
                max_steps=max_steps,
                extra={"domain": domain, "task_split": task_split, "task_index": index},
            )
        )
    return _canonical_manifest(f"tau-bench-{domain}-{task_split}", tasks)


def generate_appworld_manifest(
    *,
    data_root: str | Path = "external_data/deps/appworld-data/data",
    max_tasks: int | None = None,
    seed: int = 0,
    max_steps: int = 30,
) -> dict[str, Any]:
    """Generate a manifest from the official AppWorld task specs."""
    data_root = Path(data_root)
    task_dirs = sorted(
        [path for path in (data_root / "tasks").iterdir() if path.is_dir()]
    )
    tasks = []
    for index, task_dir in enumerate(task_dirs):
        if max_tasks is not None and index >= max_tasks:
            break
        specs_path = task_dir / "specs.json"
        if not specs_path.exists():
            continue
        try:
            specs = json.loads(specs_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        instruction = specs.get("instruction", "")
        task_id = f"appworld-{task_dir.name}"
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
        tasks.append(
            _task(
                task_id,
                instruction,
                seed=seed,
                max_steps=max_steps,
                extra={
                    "task_dir": task_dir.name,
                    "app_names": app_names,
                    "supervisor": specs.get("supervisor"),
                    "datetime": specs.get("datetime"),
                },
            )
        )
    return _canonical_manifest("appworld", tasks)


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
        if data_root.name != "data" and (data_root / "data" / "tasks").is_dir():
            data_root = data_root / "data"
        manifest = generate_appworld_manifest(data_root=data_root, max_tasks=limit, seed=seed)
    else:
        locomo_source = Path(source or "external_data/raw/locomo10.json")
        manifest = generate_locomo_manifest(source=locomo_source, max_tasks=limit, seed=seed)

    tasks = list(manifest.get("tasks", []))
    if len(tasks) != limit:
        raise ValueError(f"{benchmark} source provided {len(tasks)} tasks, expected {limit}")
    task_ids = [str(task.get("task_id")) for task in tasks]
    if any(not task_id or task_id == "None" for task_id in task_ids):
        raise ValueError("every task must have a non-empty task_id")
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("duplicate task IDs are not allowed")

    shuffled = list(task_ids)
    random.Random(seed).shuffle(shuffled)
    holdout_count = max(1, round(len(shuffled) * 0.2))
    holdout_ids = sorted(shuffled[:holdout_count])
    holdout_set = set(holdout_ids)
    train_ids = sorted(task_id for task_id in task_ids if task_id not in holdout_set)
    normalized = dict(manifest)
    normalized["seed"] = int(seed)
    normalized["split"] = split
    normalized["task_count"] = len(tasks)
    normalized["task_level_split"] = {
        "seed": int(seed),
        "source_split": split,
        "train_task_ids": train_ids,
        "holdout_task_ids": holdout_ids,
    }
    if isinstance(source, (str, Path)) and Path(str(source)).is_file():
        source_bytes = Path(str(source)).read_bytes()
        normalized["source_sha256"] = hashlib.sha256(source_bytes).hexdigest()
    else:
        normalized["source_sha256"] = hashlib.sha256(str(source or benchmark).encode("utf-8")).hexdigest()
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    normalized["manifest_hash"] = hashlib.sha256(encoded).hexdigest()
    return normalized


def write_manifest(manifest: dict[str, Any], out_path: str | Path) -> str:
    """Write a manifest and return its canonical SHA-256 digest."""
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
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
