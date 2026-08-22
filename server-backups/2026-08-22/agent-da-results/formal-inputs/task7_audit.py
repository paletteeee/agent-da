#!/usr/bin/env python3
"""Read-only acceptance audit for the Task 7 formal experiment artifacts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


ADAPTERS = (
    "AppendOnly",
    "LastWriteWins",
    "MetadataFiltered",
    "Mem0",
    "LangGraphStore",
)
WORKLOADS = (
    "atomic_multi_write",
    "crash_during_commit",
    "mixed_stress",
    "provenance_branch_repair",
    "provenance_chain_repair",
    "revoke_before_commit",
    "scope_bypass",
    "supersession_consistency",
)
CAPABILITIES = (
    "single_record_read_write",
    "atomic_multi_record_commit",
    "commit_policy_revalidation",
    "shared_scope_isolation",
    "version_supersession",
    "provenance_propagation",
    "recursive_provenance_invalidation",
    "crash_recovery",
)
ORACLE_FIELDS = (
    "transaction_state",
    "partial_update_rate",
    "invalid_commit_rate",
    "stale_write_rate",
    "repair_recall",
    "leak_rate",
    "supersession_consistency",
    "scope_bypass_rate",
    "latency",
    "any_violation",
    "violations",
    "committed_count",
    "operation_count",
    "repair_count",
)
ARTIFACTS = (
    "results.csv",
    "summary.json",
    "capabilities.csv",
    "capabilities.json",
    "environment.json",
    "errors.jsonl",
)
EXPECTED_HEAD = "1db4f092769f3a7cf60c472a0ad97176eca6268f"
EXPECTED_RUN_ID = "formal-1db4f09-20260821"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def as_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise AssertionError(f"invalid boolean cell: {value!r}")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def row_counts(rows: list[dict], key: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for value in sorted({row[key] for row in rows}):
        selected = [row for row in rows if row[key] == value]
        categories = Counter(
            row["error_category"] for row in selected if row["run_status"] == "excluded"
        )
        result[value] = {
            "attempted": len(selected),
            "successful": sum(row["run_status"] == "success" for row in selected),
            "correctness_included": sum(as_bool(row["correctness_included"]) for row in selected),
            "excluded": sum(row["run_status"] == "excluded" for row in selected),
            "capability_absent_observed": sum(
                as_bool(row["capability_absent_observed"]) for row in selected
            ),
            "exclusions_by_category": dict(sorted(categories.items())),
        }
    return result


def audit(repo: Path, core_rerun: Path) -> dict:
    external = repo / "results" / "external"
    results_path = external / "results.csv"
    raw_result_lines = results_path.read_bytes().splitlines()
    assert len(raw_result_lines) == 401
    with results_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert len(rows) == 400
    assert len({(row["instance_id"], row["adapter"]) for row in rows}) == 400
    assert set(row["workload"] for row in rows) == set(WORKLOADS)
    assert Counter(row["adapter"] for row in rows) == Counter({name: 80 for name in ADAPTERS})
    first_seen = tuple(dict.fromkeys(row["adapter"] for row in rows))
    assert first_seen == ADAPTERS

    instances_path = repo / "data" / "generated_instances.jsonl"
    instances = load_jsonl(instances_path)
    assert len(instances) == 80
    by_id = {item["instance_id"]: item for item in instances}
    assert len(by_id) == 80
    assert set(by_id) == {row["instance_id"] for row in rows}
    crash_ids = {
        item["instance_id"]
        for item in instances
        if any(event.get("type") == "crash" for event in item.get("failure_schedule", []))
    }
    assert len(crash_ids) == 30

    included = [row for row in rows if as_bool(row["correctness_included"])]
    excluded = [row for row in rows if row["run_status"] == "excluded"]
    successful = [row for row in rows if row["run_status"] == "success"]
    assert len(included) == len(successful) == 370
    assert len(excluded) == 30
    assert all(row["adapter_version"] for row in successful)
    assert all(row["run_status"] == "success" and not row["error_category"] for row in included)
    assert all(row["error_category"] != "runtime_error" for row in included)
    assert not any(row["error_category"] == "runtime_error" for row in rows)
    assert all(not row[field] for row in excluded for field in ORACLE_FIELDS)
    assert all(not as_bool(row["correctness_included"]) for row in excluded)
    assert all(
        row["adapter"] == "LangGraphStore"
        and row["backend_mode"] == "in_memory_fallback"
        and row["error_category"] == "unsupported_mapping"
        and row["instance_id"] in crash_ids
        for row in excluded
    )
    assert {row["instance_id"] for row in excluded} == crash_ids
    langgraph_success = {
        row["instance_id"]
        for row in rows
        if row["adapter"] == "LangGraphStore" and row["run_status"] == "success"
    }
    assert langgraph_success == set(by_id) - crash_ids

    errors = load_jsonl(external / "errors.jsonl")
    assert len(errors) == 30
    assert {(item["instance_id"], item["adapter"]) for item in errors} == {
        (row["instance_id"], row["adapter"]) for row in excluded
    }
    assert all(
        item["adapter"] == "LangGraphStore"
        and item["backend_mode"] == "in_memory_fallback"
        and item["error_category"] == "unsupported_mapping"
        and item["error_type"] == "UnsupportedMappingError"
        and item["instance_id"] in crash_ids
        for item in errors
    )

    summary = load_json(external / "summary.json")
    expected_counts = {
        "attempted": 400,
        "successful": 370,
        "correctness_included": 370,
        "excluded": 30,
        "capability_absent_observed": 20,
    }
    assert summary["counts"] == expected_counts
    assert summary["exclusions_by_category"] == {"unsupported_mapping": 30}
    adapters_from_rows = row_counts(rows, "adapter")
    workloads_from_rows = row_counts(rows, "workload")
    assert summary["adapter_counts"] == adapters_from_rows
    assert summary["workload_counts"] == workloads_from_rows
    assert len(summary["groups"]) == len(ADAPTERS) * len(WORKLOADS)
    for adapter in ADAPTERS:
        for workload in WORKLOADS:
            selected = [
                row for row in rows if row["adapter"] == adapter and row["workload"] == workload
            ]
            group = summary["groups"][f"{workload}/{adapter}"]
            assert group["attempted"] == len(selected) == 10
            assert group["correctness_included"] == sum(
                as_bool(row["correctness_included"]) for row in selected
            )
            assert group["excluded"] == sum(row["run_status"] == "excluded" for row in selected)
            assert group["capability_absent_observed"] == sum(
                as_bool(row["capability_absent_observed"]) for row in selected
            )
            selected_included = [row for row in selected if as_bool(row["correctness_included"])]
            if selected_included:
                expected_mean = sum(int(row["any_violation"]) for row in selected_included) / len(
                    selected_included
                )
                assert abs(group["any_violation"]["mean"] - expected_mean) < 1e-12
            else:
                assert "any_violation" not in group

    capabilities_path = external / "capabilities.csv"
    with capabilities_path.open(encoding="utf-8", newline="") as handle:
        capabilities_reader = csv.DictReader(handle)
        capability_rows = list(capabilities_reader)
    assert len(capability_rows) == 40
    assert tuple(dict.fromkeys(row["adapter"] for row in capability_rows)) == ADAPTERS
    for adapter in ADAPTERS:
        selected = [row for row in capability_rows if row["adapter"] == adapter]
        assert tuple(row["capability"] for row in selected) == CAPABILITIES
        assert all(row["adapter_version"] and row["backend_mode"] and row["detail"] for row in selected)
        assert all(row["supported"] in {"true", "false"} for row in selected)
    capabilities_json = load_json(external / "capabilities.json")
    assert [item["adapter"] for item in capabilities_json["adapters"]] == list(ADAPTERS)
    for item in capabilities_json["adapters"]:
        assert [entry["capability"] for entry in item["capabilities"]] == list(CAPABILITIES)
        csv_rows = [row for row in capability_rows if row["adapter"] == item["adapter"]]
        normalized = [
            {**row, "supported": as_bool(row["supported"])}
            for row in csv_rows
        ]
        assert item["capabilities"] == normalized

    manifest = load_json(external / "run_manifest.json")
    environment = load_json(external / "environment.json")
    assert manifest["run_id"] == EXPECTED_RUN_ID
    assert manifest["git"] == {"dirty": False, "head": EXPECTED_HEAD}
    assert environment["git"] == manifest["git"]
    assert manifest["input"]["count"] == 80
    assert manifest["input"]["bytes"] == instances_path.stat().st_size
    assert manifest["input"]["sha256"] == sha256(instances_path)
    assert [item["name"] for item in manifest["selected_adapters"]] == list(ADAPTERS)
    assert manifest["counts"] == expected_counts
    assert manifest["backend_state"]["langgraph_store"] == {
        "mode": "in_memory_fallback",
        "reason": "postgres_dsn_not_configured",
    }
    assert manifest["backend_state"]["mem0"] == {
        "base_root": "/data/agent-da-results/mem0-formal",
        "mode": "embedded_qdrant",
        "root": "/data/agent-da-results/mem0-formal/formal-1db4f09-20260821",
    }
    assert set(manifest["artifacts"]) == set(ARTIFACTS)
    for name in ARTIFACTS:
        path = external / name
        assert manifest["artifacts"][name] == {
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        }

    core_pairs = (
        (repo / "data" / "generated_instances.jsonl", core_rerun / "data" / "generated_instances.jsonl"),
        (repo / "results" / "experiment_results.csv", core_rerun / "results" / "experiment_results.csv"),
        (repo / "results" / "summary.json", core_rerun / "results" / "summary.json"),
    )
    core_hashes = {}
    for tracked, rerun in core_pairs:
        assert sha256(tracked) == sha256(rerun)
        core_hashes[str(tracked.relative_to(repo))] = sha256(tracked)
    assert len((core_rerun / "data" / "generated_instances.jsonl").read_bytes().splitlines()) == 80
    with (core_rerun / "results" / "experiment_results.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        core_rows = list(csv.DictReader(handle))
    assert len(core_rows) == 400
    assert len({row["instance_id"] for row in core_rows}) == 80
    assert Counter(row["variant"] for row in core_rows) == Counter(
        {
            "Naive": 80,
            "TxnMem-NoTxn": 80,
            "TxnMem-NoPolicyCommit": 80,
            "TxnMem-NoRepair": 80,
            "TxnMem": 80,
        }
    )
    txnmem_rows = [row for row in core_rows if row["variant"] == "TxnMem"]
    assert len(txnmem_rows) == 80
    assert sum(int(row["any_violation"]) for row in txnmem_rows) == 0

    formal_log = (external / "formal_run.log").read_text(encoding="utf-8")
    assert "Ran 99 tests" in formal_log and "\nOK\n" in formal_log
    assert "wrote 400 attempts to results/external" in formal_log
    assert "generated 80 instances" in formal_log
    assert "wrote 400 result rows" in formal_log

    by_adapter_workload: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for adapter in ADAPTERS:
        for workload in WORKLOADS:
            selected = [
                row for row in rows if row["adapter"] == adapter and row["workload"] == workload
            ]
            selected_included = [row for row in selected if as_bool(row["correctness_included"])]
            by_adapter_workload[adapter][workload] = {
                "included": len(selected_included),
                "excluded": len(selected) - len(selected_included),
                "violations": sum(int(row["any_violation"]) for row in selected_included),
            }

    artifact_hashes = {
        name: sha256(external / name)
        for name in (*ARTIFACTS, "run_manifest.json", "formal_run.log")
    }
    adapter_results = {}
    for adapter in ADAPTERS:
        selected = [row for row in rows if row["adapter"] == adapter]
        selected_included = [row for row in selected if as_bool(row["correctness_included"])]
        adapter_results[adapter] = {
            "attempted": len(selected),
            "included": len(selected_included),
            "excluded": len(selected) - len(selected_included),
            "violations": sum(int(row["any_violation"]) for row in selected_included),
        }

    return {
        "status": "PASS",
        "head": EXPECTED_HEAD,
        "run_id": EXPECTED_RUN_ID,
        "attempts": len(rows),
        "workloads": list(WORKLOADS),
        "scheduled_crash_instances": len(crash_ids),
        "adapter_results": adapter_results,
        "adapter_workload_results": by_adapter_workload,
        "core_rerun": {
            "instances": 80,
            "rows": 400,
            "txnmem_rows": 80,
            "txnmem_violations": 0,
            "hashes": core_hashes,
        },
        "artifact_hashes": artifact_hashes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--core-rerun", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.repo.resolve(), args.core_rerun.resolve()), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
