"""Fail-closed evidence derivation and paper-claim auditing helpers."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _binary_integer(value: Any, field: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be 0 or 1") from exc
    if normalized not in {0, 1}:
        raise ValueError(f"{field} must be 0 or 1")
    return normalized


def build_controlled_suite_evidence(
    instances_path: str | Path,
    results_path: str | Path,
) -> dict[str, Any]:
    """Derive controlled-suite counts from raw instance and result rows.

    The function deliberately has no parameters for claimed totals: every
    count is derived from the two source artifacts and the Cartesian product
    is verified before a report is returned.
    """

    instances_path = Path(instances_path)
    results_path = Path(results_path)
    instances: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        instances_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid instance JSON on line {line_number}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"instance line {line_number} is not an object")
        instances.append(row)
    if not instances:
        raise ValueError("controlled suite has no instances")

    instance_ids = [str(row.get("instance_id", "")) for row in instances]
    if any(not instance_id for instance_id in instance_ids):
        raise ValueError("every instance must have an instance_id")
    if len(set(instance_ids)) != len(instance_ids):
        raise ValueError("duplicate instance_id in controlled suite")
    instance_id_set = set(instance_ids)

    with results_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("controlled suite has no variant rows")

    pairs: set[tuple[str, str]] = set()
    by_variant: dict[str, dict[str, int]] = defaultdict(
        lambda: {"row_count": 0, "violation_count": 0, "oracle_match_count": 0}
    )
    for row in rows:
        instance_id = str(row.get("instance_id", ""))
        variant = str(row.get("variant", ""))
        if instance_id not in instance_id_set:
            raise ValueError(f"result row references unknown instance_id: {instance_id}")
        if not variant:
            raise ValueError("every result row must have a variant")
        pair = (instance_id, variant)
        if pair in pairs:
            raise ValueError(f"duplicate instance/variant row: {instance_id}/{variant}")
        pairs.add(pair)
        metrics = by_variant[variant]
        metrics["row_count"] += 1
        metrics["violation_count"] += _binary_integer(
            row.get("any_violation"), "any_violation"
        )
        metrics["oracle_match_count"] += _binary_integer(
            row.get("oracle_match"), "oracle_match"
        )

    variants = sorted(by_variant)
    expected_pairs = {
        (instance_id, variant)
        for instance_id in instance_ids
        for variant in variants
    }
    if pairs != expected_pairs:
        missing = sorted(expected_pairs - pairs)
        extra = sorted(pairs - expected_pairs)
        raise ValueError(
            "results are not a complete instance-by-variant Cartesian product "
            f"(missing={len(missing)}, extra={len(extra)})"
        )

    workloads = {str(row.get("workload", "")) for row in instances}
    seeds = {str(row.get("seed", "")) for row in instances}
    if "" in workloads or "" in seeds:
        raise ValueError("every instance must have workload and seed fields")

    return {
        "schema_version": 1,
        "evidence_id": "controlled_suite",
        "derivation": "generated_instances.jsonl × observed CSV variants",
        "instance_count": len(instances),
        "workload_family_count": len(workloads),
        "seed_count": len(seeds),
        "variant_count": len(variants),
        "variant_row_count": len(rows),
        "variants": {variant: dict(by_variant[variant]) for variant in variants},
        "sources": {
            "instances": {
                "path": str(instances_path),
                "sha256": _sha256(instances_path),
                "line_count": len(instances),
            },
            "results": {
                "path": str(results_path),
                "sha256": _sha256(results_path),
                "row_count": len(rows),
            },
        },
        "production_latency_claim": False,
    }


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit TxnMem paper evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)
    controlled = subparsers.add_parser(
        "controlled", help="derive the controlled-suite evidence summary"
    )
    controlled.add_argument("--instances", type=Path, required=True)
    controlled.add_argument("--results", type=Path, required=True)
    controlled.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.command == "controlled":
        evidence = build_controlled_suite_evidence(args.instances, args.results)
        _write_json(evidence, args.out)
        print(
            f"wrote controlled evidence: {evidence['instance_count']} instances, "
            f"{evidence['variant_row_count']} variant rows -> {args.out}"
        )
        return 0
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
