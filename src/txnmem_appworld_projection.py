"""Regenerate redacted AppWorld reference-API projection source events."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def _data_root(path: Path) -> Path:
    path = Path(path)
    if (path / "tasks").is_dir():
        return path
    return path / "data"


def regenerate_appworld_projection(
    appworld_root: Path,
    task_ids: Sequence[str],
    output_path: Path,
) -> dict[str, Any]:
    """Write method/URL-only source records from official ``api_calls.json``.

    Request ``data`` values are intentionally excluded because official calls
    may contain credentials or private task state.  Source and output hashes
    make the regeneration auditable without committing those values.
    """

    normalized_ids = [str(task_id).strip() for task_id in task_ids if str(task_id).strip()]
    if not normalized_ids:
        raise ValueError("at least one AppWorld task id is required")
    if len(set(normalized_ids)) != len(normalized_ids):
        raise ValueError("AppWorld task ids must be unique")
    data_root = _data_root(Path(appworld_root))
    records: list[dict[str, Any]] = []
    source_hashes: dict[str, str] = {}
    method_counts: Counter[str] = Counter()
    per_task_counts: dict[str, int] = {}
    for task_id in normalized_ids:
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
        "event_count": len(records),
        "per_task_event_counts": per_task_counts,
        "method_counts": dict(sorted(method_counts.items())),
        "source_sha256_by_task": source_hashes,
        "output_sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "request_data_values_retained": False,
        "raw_official_request_values_committed": False,
        "trace_ground_truth_native": False,
        "production_latency_claim": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--appworld-root", type=Path, required=True)
    parser.add_argument("--task-ids", nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    args = parser.parse_args()
    inventory = regenerate_appworld_projection(
        args.appworld_root,
        args.task_ids,
        args.output,
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
