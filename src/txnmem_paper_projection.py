"""Shared evidence projections used by manuscript publication builders."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONTROLLED_VARIANT_ORDER = (
    "TxnMem",
    "Naive",
    "TxnMem-NoTxn",
    "TxnMem-NoPolicyCommit",
    "TxnMem-NoRepair",
)


def controlled_result_rows(root: str | Path) -> list[dict[str, Any]]:
    """Project every controlled variant directly from the audited artifact."""

    root = Path(root)
    payload = json.loads(
        (root / "results/paper_evidence/controlled_suite.json").read_text(
            encoding="utf-8"
        )
    )
    instance_count = payload.get("instance_count")
    variants = payload.get("variants")
    if (
        isinstance(instance_count, bool)
        or not isinstance(instance_count, int)
        or instance_count <= 0
        or not isinstance(variants, dict)
        or set(variants) != set(CONTROLLED_VARIANT_ORDER)
    ):
        raise ValueError("controlled-suite variant projection is malformed")
    rows: list[dict[str, Any]] = []
    for variant in CONTROLLED_VARIANT_ORDER:
        item = variants[variant]
        if not isinstance(item, dict) or item.get("row_count") != instance_count:
            raise ValueError(f"controlled-suite row count mismatch: {variant}")
        violation_count = item.get("violation_count")
        oracle_match_count = item.get("oracle_match_count")
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= instance_count
            for value in (violation_count, oracle_match_count)
        ):
            raise ValueError(f"controlled-suite count is malformed: {variant}")
        rows.append(
            {
                "variant": variant,
                "instance_count": instance_count,
                "violation_count": violation_count,
                "oracle_match_count": oracle_match_count,
            }
        )
    return rows
