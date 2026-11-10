"""Post-run analysis for stable and network-outage windows in model load tests."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping


_ENDPOINT_OR_TRANSPORT_FAILURE_CODES = {
    "model_http_error",
    "model_network_error",
    "model_timeout",
}


def analyze_model_load_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    rows = summary.get("task_summaries")
    if not isinstance(rows, list):
        raise ValueError("task_summaries must be a list")
    cycle_rows: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    endpoint_or_transport_rows: list[Mapping[str, Any]] = []
    failure_counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("task_summaries must contain mappings")
        cycle = int(row.get("cycle", 0) or 0)
        cycle_rows[cycle].append(row)
        code = str(row.get("failure_code") or "none")
        failure_counts[code] += 1
        if code in _ENDPOINT_OR_TRANSPORT_FAILURE_CODES:
            endpoint_or_transport_rows.append(row)
    failure_cycles = sorted(
        {int(row.get("cycle", 0) or 0) for row in endpoint_or_transport_rows}
    )
    first_failure_cycle = failure_cycles[0] if failure_cycles else None
    stable_rows = [
        row
        for row in rows
        if first_failure_cycle is None
        or int(row.get("cycle", 0) or 0) < first_failure_cycle
    ]
    full_failure_cycles = [
        cycle
        for cycle in failure_cycles
        if cycle_rows[cycle]
        and all(
            str(row.get("failure_code") or "")
            in _ENDPOINT_OR_TRANSPORT_FAILURE_CODES
            for row in cycle_rows[cycle]
        )
    ]
    partial_failure_cycles = [
        cycle for cycle in failure_cycles if cycle not in full_failure_cycles
    ]
    usage = summary.get("model_usage", {})
    request_count = int(usage.get("request_count", 0) or 0)
    responses_with_usage = int(usage.get("responses_with_usage", 0) or 0)
    token_usage_complete = responses_with_usage == request_count
    return {
        "analysis": "cross_host_model_load_stable_and_endpoint_or_transport_failure_windows",
        "completed_cycles": int(summary.get("completed_cycles", 0) or 0),
        "attempt_count": len(rows),
        "first_endpoint_or_transport_failure_cycle": first_failure_cycle,
        "stable_prefix_cycle_count": (
            (first_failure_cycle - 1) if first_failure_cycle else len(cycle_rows)
        ),
        "stable_prefix_attempt_count": len(stable_rows),
        "stable_prefix_contract_success_count": sum(
            bool(row.get("task_evaluator", {}).get("success")) for row in stable_rows
        ),
        "endpoint_or_transport_failure_attempt_count": len(endpoint_or_transport_rows),
        "endpoint_or_transport_failure_codes": {
            code: count
            for code, count in sorted(failure_counts.items())
            if code in _ENDPOINT_OR_TRANSPORT_FAILURE_CODES
        },
        "partial_failure_cycles": partial_failure_cycles,
        "full_failure_cycles": full_failure_cycles,
        "model_request_count": request_count,
        "responses_with_usage": responses_with_usage,
        "token_usage_complete": token_usage_complete,
        "endpoint_reported_total_tokens": int(usage.get("total_tokens", 0) or 0),
        "reported_token_total_is_lower_bound_for_all_requests": not token_usage_complete,
        "claim_boundary": (
            "HTTP failures are endpoint-or-transport failures unless separate network-loss evidence exists; "
            "token totals are lower bounds whenever any request lacks endpoint usage, and stable-prefix "
            "results do not imply multi-host Agent workers"
        ),
        "raw_traces_committed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    summary = json.loads(args.summary.read_text(encoding="utf-8"))
    report = analyze_model_load_summary(summary)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
