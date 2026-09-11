"""Fail-closed auditing and claim gates for multi-agent evidence."""

from __future__ import annotations

import csv
import hashlib
import ipaddress
import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from txnmem_multiagent_workloads import MULTI_AGENT_WORKLOADS
from txnmem_mutation import MULTI_AGENT_MUTANTS
from txnmem_schema import validate_instance
from txnmem_simulator import VARIANTS


_AUDITED_PATHS = (
    "data/generated_instances.jsonl",
    "data/reference_oracles.jsonl",
    "data/exhaustive_interleavings.jsonl",
    "data/exhaustive_reference_oracles.jsonl",
    "results/experiment_results.csv",
    "results/exhaustive_results.csv",
    "results/controlled_report.json",
    "results/summary.json",
    "results/coverage.json",
    "results/mutation_report.json",
    "results/minimal_witnesses.json",
    "results/reproducibility.json",
)

_SECRET_PATTERN = re.compile(
    r"(?i)(password|api[_-]?key|access[_-]?token|secret)\s*[\"']?\s*[:=]"
)
_HOME_PATTERN = re.compile(r"/(?:Users|home)/[^\s\"']+")
_IP_PATTERN = re.compile(r"(?<![0-9.])(?:\d{1,3}\.){3}\d{1,3}(?![0-9.])")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"JSON artifact must contain an object: {path.name}")
    return loaded


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        item = json.loads(line)
        if not isinstance(item, dict):
            raise ValueError(
                f"JSONL row must contain an object: {path.name}:{line_number}"
            )
        records.append(item)
    return records


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _assert_safe_text(text: str, label: str) -> None:
    if _SECRET_PATTERN.search(text) or _HOME_PATTERN.search(text):
        raise ValueError(f"sensitive or host-specific content in {label}")
    for candidate in _IP_PATTERN.findall(text):
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if address.is_global:
            raise ValueError(f"sensitive or host-specific content in {label}")


def _audit_privacy(root: Path, relative_paths: tuple[str, ...]) -> None:
    for relative in relative_paths:
        path = root / relative
        _assert_safe_text(path.read_text(encoding="utf-8"), relative)


def _int(row: Mapping[str, Any], name: str) -> int:
    try:
        return int(row.get(name, 0))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid integer field {name}") from exc


def _audit_seeded_instances(
    instances: list[dict[str, Any]], expected_seeds: int
) -> dict[str, dict[str, Any]]:
    expected_count = len(MULTI_AGENT_WORKLOADS) * expected_seeds
    if len(instances) != expected_count:
        raise ValueError(
            f"seeded instance count must equal {expected_count}, got {len(instances)}"
        )
    by_id: dict[str, dict[str, Any]] = {}
    family_signatures: dict[str, set[str]] = defaultdict(set)
    family_seeds: dict[str, set[int]] = defaultdict(set)
    family_counts: Counter[str] = Counter()
    for instance in instances:
        validate_instance(instance)
        instance_id = str(instance["instance_id"])
        if instance_id in by_id:
            raise ValueError(f"duplicate seeded instance ID: {instance_id}")
        by_id[instance_id] = instance
        workload = str(instance["workload"])
        family_counts[workload] += 1
        family_signatures[workload].add(str(instance.get("structure_signature", "")))
        family_seeds[workload].add(int(instance["seed"]))
        if instance.get("evidence_tier") != "seeded":
            raise ValueError(f"seeded artifact contains non-seeded instance: {instance_id}")
    for workload in MULTI_AGENT_WORKLOADS:
        if family_counts[workload] != expected_seeds:
            raise ValueError(f"seeded family count mismatch for {workload}")
        if len(family_signatures[workload]) != expected_seeds:
            raise ValueError(
                f"unique structure signatures mismatch for {workload}"
            )
        if family_seeds[workload] != set(range(expected_seeds)):
            raise ValueError(f"seed sequence mismatch for {workload}")
    return by_id


def _audit_result_rows(
    rows: list[dict[str, str]],
    instances: Mapping[str, Mapping[str, Any]],
    *,
    label: str,
) -> dict[str, int]:
    expected_keys = {
        (instance_id, variant)
        for instance_id in instances
        for variant in VARIANTS
    }
    observed_keys: list[tuple[str, str]] = [
        (str(row.get("instance_id")), str(row.get("variant"))) for row in rows
    ]
    if len(observed_keys) != len(set(observed_keys)):
        raise ValueError(f"duplicate {label} variant rows")
    if set(observed_keys) != expected_keys:
        raise ValueError(f"{label} variant rows do not cover every instance and variant")
    for row in rows:
        instance = instances[str(row["instance_id"])]
        if row.get("workload") != str(instance["workload"]):
            raise ValueError(f"{label} workload mismatch")
        if _int(row, "seed") != int(instance["seed"]):
            raise ValueError(f"{label} seed mismatch")
        if row.get("structure_signature") != str(
            instance.get("structure_signature", "")
        ):
            raise ValueError(f"{label} structure signature mismatch")
        if _int(row, "agent_count") != 2 or _int(row, "transaction_count") < 2:
            raise ValueError(f"{label} rows must represent two agents and two transactions")
    full = [row for row in rows if row.get("variant") == "TxnMem"]
    return {
        "observation_count": len(rows),
        "full_txnmem_violation_count": sum(
            _int(row, "any_violation") for row in full
        ),
        "full_txnmem_oracle_mismatch_count": sum(
            1 - _int(row, "oracle_match") for row in full
        ),
    }


def _audit_oracle_alignment(
    oracles: list[dict[str, Any]], instance_ids: set[str], label: str
) -> None:
    oracle_ids = [str(oracle.get("instance_id")) for oracle in oracles]
    if len(oracle_ids) != len(set(oracle_ids)) or set(oracle_ids) != instance_ids:
        raise ValueError(f"{label} oracle instance IDs do not align with inputs")


def _audit_witnesses(report: Mapping[str, Any]) -> None:
    witnesses = report.get("witnesses")
    if not isinstance(witnesses, Mapping) or set(witnesses) != set(
        MULTI_AGENT_MUTANTS
    ):
        raise ValueError("mutation witnesses are missing")
    if report.get("all_prefix_minimal") is not True:
        raise ValueError("mutation witnesses are not prefix-minimal")
    for name, target in MULTI_AGENT_MUTANTS.items():
        witness = witnesses[name]
        if not isinstance(witness, Mapping):
            raise ValueError(f"mutation witness is malformed: {name}")
        observed = witness.get("observed")
        minimality = witness.get("minimality")
        if witness.get("target_violation") != target:
            raise ValueError(f"mutation witness target mismatch: {name}")
        if not isinstance(observed, Mapping) or observed.get("reproduces") is not True:
            raise ValueError(f"mutation witness is inactive: {name}")
        if (
            not isinstance(minimality, Mapping)
            or minimality.get("predecessor_reproduces_target_violation") is not False
        ):
            raise ValueError(f"mutation witness is not minimal: {name}")


def _audit_reproducibility(report: Mapping[str, Any]) -> int:
    replications = report.get("replications")
    if not isinstance(replications, list) or not replications:
        raise ValueError("replication hashes are missing")
    normalized = [
        {key: value for key, value in record.items() if key != "replication"}
        for record in replications
        if isinstance(record, Mapping)
    ]
    if (
        len(normalized) != len(replications)
        or any(record != normalized[0] for record in normalized[1:])
        or report.get("all_hashes_identical") is not True
        or _int(report, "replication_count") != len(replications)
    ):
        raise ValueError("replication hashes are inconsistent")
    for record in normalized:
        if any(
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in record.values()
        ):
            raise ValueError("replication hashes are malformed")
    return len(replications)


def audit_controlled_artifacts(
    root: Path, expected_seeds: int
) -> dict[str, Any]:
    """Recompute controlled evidence counts and reject malformed artifacts."""

    if (
        isinstance(expected_seeds, bool)
        or not isinstance(expected_seeds, int)
        or expected_seeds < 1
    ):
        raise ValueError("expected_seeds must be a positive integer")
    root = Path(root)
    for relative in _AUDITED_PATHS:
        if not (root / relative).is_file():
            raise ValueError(f"required controlled artifact is missing: {relative}")
    _audit_privacy(root, _AUDITED_PATHS)

    instances = _read_jsonl(root / "data/generated_instances.jsonl")
    instance_map = _audit_seeded_instances(instances, expected_seeds)
    oracles = _read_jsonl(root / "data/reference_oracles.jsonl")
    _audit_oracle_alignment(oracles, set(instance_map), "seeded")
    rows = _read_csv(root / "results/experiment_results.csv")
    seeded_counts = _audit_result_rows(
        rows, instance_map, label="seeded"
    )

    exhaustive_instances = _read_jsonl(
        root / "data/exhaustive_interleavings.jsonl"
    )
    if not exhaustive_instances:
        raise ValueError("exhaustive instance set is empty")
    exhaustive_map: dict[str, dict[str, Any]] = {}
    for instance in exhaustive_instances:
        validate_instance(instance)
        instance_id = str(instance["instance_id"])
        if instance.get("evidence_tier") != "exhaustive" or instance_id in exhaustive_map:
            raise ValueError("exhaustive instance IDs or tiers are invalid")
        exhaustive_map[instance_id] = instance
    exhaustive_oracles = _read_jsonl(
        root / "data/exhaustive_reference_oracles.jsonl"
    )
    _audit_oracle_alignment(
        exhaustive_oracles, set(exhaustive_map), "exhaustive"
    )
    exhaustive_rows = _read_csv(root / "results/exhaustive_results.csv")
    exhaustive_counts = _audit_result_rows(
        exhaustive_rows, exhaustive_map, label="exhaustive"
    )

    controlled_report = _read_json(root / "results/controlled_report.json")
    recomputed = {
        "seeded_instance_count": len(instances),
        "seeded_observation_count": seeded_counts["observation_count"],
        "exhaustive_instance_count": len(exhaustive_instances),
        "exhaustive_observation_count": exhaustive_counts["observation_count"],
        "full_txnmem_seeded_violations": seeded_counts[
            "full_txnmem_violation_count"
        ],
        "full_txnmem_seeded_oracle_mismatches": seeded_counts[
            "full_txnmem_oracle_mismatch_count"
        ],
        "full_txnmem_exhaustive_violations": exhaustive_counts[
            "full_txnmem_violation_count"
        ],
        "full_txnmem_exhaustive_oracle_mismatches": exhaustive_counts[
            "full_txnmem_oracle_mismatch_count"
        ],
    }
    for name, value in recomputed.items():
        if name in controlled_report and _int(controlled_report, name) != value:
            raise ValueError(f"controlled report disagrees with CSV: {name}")

    witnesses = _read_json(root / "results/minimal_witnesses.json")
    _audit_witnesses(witnesses)
    replication_count = _audit_reproducibility(
        _read_json(root / "results/reproducibility.json")
    )
    return {
        "schema_version": 1,
        "status": "passed",
        "expected_seeds": expected_seeds,
        **recomputed,
        "replication_count": replication_count,
        "all_multi_agent_mutants_have_witnesses": True,
        "file_sha256": {
            relative: _sha256(root / relative) for relative in _AUDITED_PATHS
        },
    }


def audit_storage_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Validate aggregate real-storage evidence and reject unknown states."""

    if not isinstance(report, Mapping):
        raise ValueError("real-storage report must be a mapping")
    _assert_safe_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True),
        "real-storage report",
    )
    fields = (
        "observation_count",
        "unknown_count",
        "partial_visible_count",
        "policy_violation_count",
        "provenance_violation_count",
    )
    counts = {name: _int(report, name) for name in fields}
    if any(value < 0 for value in counts.values()):
        raise ValueError("real-storage counts must be non-negative")
    if counts["unknown_count"]:
        raise ValueError("real-storage report contains unknown states")
    if report.get("status") not in {None, "passed"}:
        raise ValueError("real-storage report did not pass its own audit")
    return {
        "schema_version": 1,
        "status": "passed",
        **counts,
    }


def evaluate_claim_status(
    controlled: Mapping[str, Any],
    storage: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Activate the bounded paper claim only when every evidence gate passes."""

    audited_storage = audit_storage_report(storage) if storage is not None else None
    gates = {
        "controlled_artifact_integrity": controlled.get("status") == "passed",
        "formal_seed_count": _int(controlled, "expected_seeds") == 50,
        "seeded_case_count": _int(controlled, "seeded_instance_count") == 300,
        "seeded_observation_count": _int(
            controlled, "seeded_observation_count"
        )
        == 1500,
        "full_txnmem_seeded_zero_violations": _int(
            controlled, "full_txnmem_seeded_violations"
        )
        == 0,
        "full_txnmem_seeded_oracle_match": _int(
            controlled, "full_txnmem_seeded_oracle_mismatches"
        )
        == 0,
        "full_txnmem_exhaustive_zero_violations": _int(
            controlled, "full_txnmem_exhaustive_violations"
        )
        == 0,
        "full_txnmem_exhaustive_oracle_match": _int(
            controlled, "full_txnmem_exhaustive_oracle_mismatches"
        )
        == 0,
        "mutation_witnesses": controlled.get(
            "all_multi_agent_mutants_have_witnesses"
        )
        is True,
        "three_identical_replications": _int(controlled, "replication_count")
        >= 3,
        "real_storage_evidence": audited_storage is not None
        and audited_storage["observation_count"] == 240,
        "real_storage_zero_partial_visibility": audited_storage is not None
        and audited_storage["partial_visible_count"] == 0,
        "real_storage_zero_policy_violations": audited_storage is not None
        and audited_storage["policy_violation_count"] == 0,
        "real_storage_zero_provenance_violations": audited_storage is not None
        and audited_storage["provenance_violation_count"] == 0,
        "real_storage_zero_unknown": audited_storage is not None
        and audited_storage["unknown_count"] == 0,
    }
    failed = sorted(name for name, passed in gates.items() if not passed)
    return {
        "schema_version": 1,
        "status": "active" if not failed else "inactive",
        "claim": (
            "In the defined operation model and covered fault schedules, "
            "TxnMem preserves transactional, actor-policy, and cross-agent "
            "provenance consistency for shared memory."
        ),
        "gates": gates,
        "failed_gates": failed,
        "scope_boundary": (
            "finite exhaustive micro-programs, 300 seeded controlled cases, "
            "and 240 state-verified real-storage observations"
        ),
    }


def write_paper_summary(
    claim_status: Mapping[str, Any],
    controlled: Mapping[str, Any],
    storage: Mapping[str, Any] | None,
    path: Path,
) -> None:
    """Write a count-derived Chinese summary suitable for paper drafting."""

    storage_count = _int(storage or {}, "observation_count")
    failed = list(claim_status.get("failed_gates", []))
    lines = [
        "# TxnMem 多智能体证据摘要",
        "",
        f"主张状态：{claim_status.get('status', 'inactive')}。",
        "",
        (
            f"受控实验包含 {controlled.get('seeded_instance_count', 0)} 个结构化实例、"
            f"{controlled.get('seeded_observation_count', 0)} 条配对观察，以及 "
            f"{controlled.get('exhaustive_instance_count', 0)} 个穷举交错实例。"
        ),
        (
            "完整 TxnMem 的受控违规数为 "
            f"{controlled.get('full_txnmem_seeded_violations', 0)}，"
            "独立参照不一致数为 "
            f"{controlled.get('full_txnmem_seeded_oracle_mismatches', 0)}。"
        ),
        f"真实存储观察数为 {storage_count}。",
        "",
        "未通过的闸门：" + ("、".join(failed) if failed else "无") + "。",
        "",
        "适用边界：" + str(claim_status.get("scope_boundary", "")) + "。",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
