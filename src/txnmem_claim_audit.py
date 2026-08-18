"""Fail-closed evidence derivation and paper-claim auditing helpers."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


_KNOWN_CLAIM_STATUSES = {"active"}
_KNOWN_ARTIFACT_FORMATS = {"json"}
_KNOWN_ASSERTION_OPERATORS = {
    "equals",
    "not_equals",
    "greater_equal",
    "less_equal",
    "length_equals",
}
_KNOWN_VALIDATION_PROFILES = {
    "tau_bench_50",
    "toxiproxy_fault_path",
    "toxiproxy_state_verified",
    "qwen_vector_graph_e2e_5",
    "minimal_mutant_witnesses",
    "controlled_scale_200",
}
_ACTIVE_CLAIM_FIELDS = {
    "claim_id",
    "status",
    "paper_location",
    "artifact_path",
    "artifact_format",
    "artifact_sha256",
    "assertions",
    "run_command",
    "manifest",
    "source_commit",
    "claim_boundary",
    "validation_profile",
    "controlled_evidence",
}


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


def _finding(
    code: str,
    message: str,
    *,
    claim_id: str | None = None,
    field: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {"code": code, "message": message}
    if claim_id is not None:
        item["claim_id"] = claim_id
    if field is not None:
        item["field"] = field
    return item


def _rooted_path(root: Path, value: str) -> Path:
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path escapes audit root: {value}") from exc
    return candidate


def _json_pointer(document: Any, pointer: str) -> Any:
    if pointer == "":
        return document
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise KeyError("JSON pointer must be empty or start with '/'")
    current = document
    for raw_token in pointer[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token not in current:
                raise KeyError(token)
            current = current[token]
        elif isinstance(current, list):
            try:
                index = int(token)
            except ValueError as exc:
                raise KeyError(token) from exc
            if index < 0 or index >= len(current):
                raise KeyError(token)
            current = current[index]
        else:
            raise KeyError(token)
    return current


def _assertion_matches(actual: Any, operator: str, expected: Any) -> bool:
    if operator == "equals":
        if isinstance(actual, bool) != isinstance(expected, bool):
            return False
        return actual == expected
    if operator == "not_equals":
        return actual != expected
    if operator == "greater_equal":
        return bool(actual >= expected)
    if operator == "less_equal":
        return bool(actual <= expected)
    if operator == "length_equals":
        return len(actual) == expected
    raise ValueError(f"unsupported assertion operator: {operator}")


def _semantic_findings(
    claim_id: str,
    profile: str | None,
    document: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if profile == "toxiproxy_fault_path":
        scenarios = document.get("scenarios", {})
        repetitions = document.get("repetitions_per_scenario")
        if not isinstance(scenarios, dict):
            scenarios = {}
        for name, row in scenarios.items():
            if name == "normal":
                continue
            if not isinstance(row, dict) or any(
                row.get(field) != repetitions
                for field in (
                    "trigger_fired_count",
                    "toxic_installed_count",
                    "proxy_path_verified_count",
                )
            ):
                findings.append(
                    _finding(
                        "toxiproxy_trigger_evidence_incomplete",
                        f"scenario {name} lacks per-repetition trigger/toxic/proxy evidence",
                        claim_id=claim_id,
                    )
                )
    elif profile == "toxiproxy_state_verified":
        expected_states = {
            "normal": {"complete": 30, "absent": 0},
            "delay": {"complete": 30, "absent": 0},
            "retry_success": {"complete": 30, "absent": 0},
            "timeout": {"complete": 0, "absent": 30},
            "connection_drop": {"complete": 0, "absent": 30},
        }
        expected_responses = {
            "normal": {"success_count": 30, "error_count": 0, "abort_count": 0, "retry_count": 0, "retry_success_count": 0},
            "delay": {"success_count": 30, "error_count": 0, "abort_count": 0, "retry_count": 0, "retry_success_count": 0},
            "retry_success": {"success_count": 30, "error_count": 0, "abort_count": 0, "retry_count": 30, "retry_success_count": 30},
            "timeout": {"success_count": 0, "error_count": 30, "abort_count": 30, "retry_count": 0, "retry_success_count": 0},
            "connection_drop": {"success_count": 0, "error_count": 30, "abort_count": 30, "retry_count": 0, "retry_success_count": 0},
        }
        scenarios = document.get("scenarios")
        state_totals = document.get("state_totals")
        complete = (
            document.get("scenario_count") == 5
            and document.get("repetitions_per_scenario") == 30
            and document.get("total_repetitions") == 150
            and document.get("all_scenarios_evidence_valid") is True
            and document.get("all_scenarios_state_verified") is True
            and document.get("all_observed_states_consistent") is True
            and document.get("production_latency_claim") is False
            and isinstance(state_totals, dict)
            and state_totals == {
                "complete": 90,
                "absent": 60,
                "partial": 0,
                "unknown": 0,
            }
            and isinstance(scenarios, dict)
            and set(scenarios) == set(expected_states)
        )
        if complete:
            for name, expected_state_counts in expected_states.items():
                row = scenarios[name]
                if not isinstance(row, dict):
                    complete = False
                    break
                expected_counts = {
                    **expected_state_counts,
                    "partial": 0,
                    "unknown": 0,
                }
                if (
                    row.get("repetitions") != 30
                    or row.get("evidence_valid_count") != 30
                    or row.get("state_counts") != expected_counts
                    or any(
                        row.get(field) != value
                        for field, value in expected_responses[name].items()
                    )
                ):
                    complete = False
                    break
                if name == "normal":
                    if any(
                        row.get(field) != value
                        for field, value in (
                            ("fault_observed_count", 0),
                            ("toxic_installed_count", 0),
                            ("toxic_cleared_count", 0),
                            ("trigger_fired_count", 0),
                        )
                    ):
                        complete = False
                        break
                elif any(
                    row.get(field) != 30
                    for field in (
                        "fault_evidence_count",
                        "fault_observed_count",
                        "trigger_fired_count",
                        "toxic_installed_count",
                        "toxic_cleared_count",
                        "proxy_path_verified_count",
                    )
                ):
                    complete = False
                    break
        if not complete:
            findings.append(
                _finding(
                    "toxiproxy_state_evidence_incomplete",
                    "state-verified Toxiproxy evidence lacks the complete five-scenario state and response contract",
                    claim_id=claim_id,
                )
            )
    elif profile == "tau_bench_50":
        task_ids = document.get("task_ids", [])
        if (
            document.get("task_count") != 50
            or document.get("unique_task_count") != 50
            or len(task_ids) != 50
            or len(set(task_ids)) != 50
            or document.get("evaluator_available_task_count") != 50
        ):
            findings.append(
                _finding(
                    "tau_task_set_incomplete",
                    "tau-bench evidence is not a unique evaluator-backed 50-task set",
                    claim_id=claim_id,
                )
            )
    elif profile == "qwen_vector_graph_e2e_5":
        task_ids = document.get("task_ids", [])
        health = document.get("backend_health", {})
        if (
            document.get("task_count") != 5
            or document.get("completed_count") != 5
            or len(task_ids) != 5
            or len(set(task_ids)) != 5
            or not all(
                bool(health.get(service, {}).get("available"))
                for service in ("qdrant", "neo4j")
            )
        ):
            findings.append(
                _finding(
                    "e2e_evidence_incomplete",
                    "E2E evidence lacks five unique completed tasks or healthy backends",
                    claim_id=claim_id,
                )
            )
    elif profile == "minimal_mutant_witnesses":
        witnesses = document.get("witnesses", {})
        if (
            document.get("witness_count") != 4
            or len(witnesses) != 4
            or document.get("all_prefix_minimal") is not True
            or any(
                witness.get("observed", {}).get("reproduces") is not True
                or witness.get("minimality", {}).get(
                    "predecessor_reproduces_target_violation"
                )
                is not False
                for witness in witnesses.values()
            )
        ):
            findings.append(
                _finding(
                    "mutant_witness_incomplete",
                    "mutant evidence does not contain four replayable prefix-minimal witnesses",
                    claim_id=claim_id,
                )
            )
    return findings


def _active_claim_schema_findings(
    claim: dict[str, Any], claim_id: str
) -> list[dict[str, Any]]:
    """Validate active-claim containers and scalar types before content checks."""

    findings: list[dict[str, Any]] = []

    def invalid(field: str, message: str) -> None:
        findings.append(
            _finding(
                "claim_schema_invalid",
                message,
                claim_id=claim_id,
                field=field,
            )
        )

    unknown_fields = sorted(set(claim) - _ACTIVE_CLAIM_FIELDS)
    if unknown_fields:
        invalid(
            "claim",
            f"active claim contains unknown fields: {', '.join(unknown_fields)}",
        )
    if not isinstance(claim.get("claim_id"), str) or not claim.get("claim_id"):
        invalid("claim_id", "claim_id must be a nonempty string")
    status = claim.get("status")
    if not isinstance(status, str) or status not in _KNOWN_CLAIM_STATUSES:
        invalid("status", f"unknown or malformed claim status: {status!r}")

    paper_location = claim.get("paper_location")
    if (
        not isinstance(paper_location, list)
        or not paper_location
        or not all(isinstance(item, str) and item for item in paper_location)
    ):
        invalid("paper_location", "paper_location must be a nonempty string list")

    for field in ("artifact_path", "run_command", "claim_boundary"):
        if not isinstance(claim.get(field), str) or not claim.get(field):
            invalid(field, f"{field} must be a nonempty string")

    artifact_format = claim.get("artifact_format")
    if (
        not isinstance(artifact_format, str)
        or artifact_format not in _KNOWN_ARTIFACT_FORMATS
    ):
        invalid(
            "artifact_format",
            f"unknown or malformed artifact_format: {artifact_format!r}",
        )

    artifact_hash = claim.get("artifact_sha256")
    if (
        not isinstance(artifact_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", artifact_hash) is None
    ):
        invalid("artifact_sha256", "artifact_sha256 must be a lowercase SHA-256")

    source_commit = claim.get("source_commit")
    if (
        not isinstance(source_commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", source_commit) is None
    ):
        invalid("source_commit", "source_commit must be a lowercase 40-hex Git id")

    manifest = claim.get("manifest")
    if not isinstance(manifest, dict):
        invalid("manifest", "manifest must be an object with path and sha256")
    else:
        if set(manifest) != {"path", "sha256"}:
            invalid("manifest", "manifest must contain exactly path and sha256")
        if not isinstance(manifest.get("path"), str) or not manifest.get("path"):
            invalid("manifest", "manifest.path must be a nonempty string")
        manifest_hash = manifest.get("sha256")
        if (
            not isinstance(manifest_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", manifest_hash) is None
        ):
            invalid("manifest", "manifest.sha256 must be a lowercase SHA-256")

    assertions = claim.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        invalid("assertions", "assertions must be a nonempty object list")
    else:
        for index, assertion in enumerate(assertions):
            if not isinstance(assertion, dict):
                findings.append(
                    _finding(
                        "assertion_invalid",
                        f"assertion {index} must be an object",
                        claim_id=claim_id,
                        field="assertions",
                    )
                )
                continue
            if set(assertion) != {"pointer", "operator", "expected"}:
                findings.append(
                    _finding(
                        "assertion_invalid",
                        f"assertion {index} must contain exactly pointer/operator/expected",
                        claim_id=claim_id,
                        field="assertions",
                    )
                )
                continue
            pointer = assertion.get("pointer")
            if not isinstance(pointer, str) or (
                pointer != "" and not pointer.startswith("/")
            ):
                findings.append(
                    _finding(
                        "assertion_invalid",
                        f"assertion {index} has a malformed JSON pointer",
                        claim_id=claim_id,
                        field="assertions",
                    )
                )
            operator = assertion.get("operator")
            if (
                not isinstance(operator, str)
                or operator not in _KNOWN_ASSERTION_OPERATORS
            ):
                findings.append(
                    _finding(
                        "assertion_invalid",
                        f"assertion {index} has an unknown operator",
                        claim_id=claim_id,
                        field="assertions",
                    )
                )

    profile = claim.get("validation_profile")
    if profile is not None and (
        not isinstance(profile, str) or profile not in _KNOWN_VALIDATION_PROFILES
    ):
        invalid(
            "validation_profile",
            f"unknown or malformed validation_profile: {profile!r}",
        )
    return findings


def _controlled_evidence_findings(
    root: Path,
    claim: dict[str, Any],
    claim_id: str,
    manifest_path: Path | None,
    manifest_document: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if claim.get("validation_profile") != "controlled_scale_200":
        return [], []
    findings: list[dict[str, Any]] = []
    checked: list[dict[str, str]] = []
    bundle = claim.get("controlled_evidence")
    if not isinstance(bundle, dict):
        return [
            _finding(
                "controlled_evidence_missing",
                "scaled controlled claims require saturation and diversity evidence",
                claim_id=claim_id,
                field="controlled_evidence",
            )
        ], []
    if set(bundle) != {"saturation", "diversity"}:
        findings.append(
            _finding(
                "controlled_evidence_missing",
                "controlled_evidence must contain exactly saturation and diversity",
                claim_id=claim_id,
                field="controlled_evidence",
            )
        )
    manifest_artifacts = (
        manifest_document.get("artifacts", {})
        if isinstance(manifest_document, dict)
        else {}
    )
    if not isinstance(manifest_artifacts, dict):
        manifest_artifacts = {}
    for name in ("saturation", "diversity"):
        reference = bundle.get(name)
        if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
            findings.append(
                _finding(
                    "controlled_evidence_missing",
                    f"controlled {name} reference must contain exactly path and sha256",
                    claim_id=claim_id,
                    field="controlled_evidence",
                )
            )
            continue
        value = reference.get("path")
        expected_hash = reference.get("sha256")
        if not isinstance(value, str) or not value or not isinstance(expected_hash, str) or re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None:
            findings.append(
                _finding(
                    "controlled_evidence_missing",
                    f"controlled {name} path/hash is malformed",
                    claim_id=claim_id,
                    field="controlled_evidence",
                )
            )
            continue
        try:
            evidence_path = _rooted_path(root, value)
        except ValueError as exc:
            findings.append(_finding("invalid_path", str(exc), claim_id=claim_id))
            continue
        if not evidence_path.is_file():
            findings.append(
                _finding(
                    "controlled_evidence_missing",
                    f"controlled evidence file not found: {value}",
                    claim_id=claim_id,
                )
            )
            continue
        actual_hash = _sha256(evidence_path)
        checked.append({"path": value, "sha256": actual_hash})
        if actual_hash != expected_hash:
            findings.append(
                _finding(
                    "controlled_evidence_hash_mismatch",
                    f"controlled evidence SHA-256 mismatch: {value}",
                    claim_id=claim_id,
                )
            )
        try:
            parsed = json.loads(evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            findings.append(
                _finding(
                    "controlled_evidence_parse_error",
                    f"cannot parse controlled {name} JSON: {exc}",
                    claim_id=claim_id,
                )
            )
        else:
            if not isinstance(parsed, dict):
                findings.append(
                    _finding(
                        "controlled_evidence_parse_error",
                        f"controlled {name} evidence must be a JSON object",
                        claim_id=claim_id,
                    )
                )
        manifest_entry = manifest_artifacts.get(f"{name}.json")
        expected_relative = None
        if manifest_path is not None:
            try:
                expected_relative = evidence_path.relative_to(manifest_path.parent).as_posix()
            except ValueError:
                expected_relative = None
        if (
            not isinstance(manifest_entry, dict)
            or manifest_entry.get("sha256") != expected_hash
            or manifest_entry.get("relative_path") != expected_relative
        ):
            findings.append(
                _finding(
                    "controlled_manifest_hash_mismatch",
                    f"run manifest does not bind controlled {name} evidence",
                    claim_id=claim_id,
                )
            )
    if isinstance(manifest_document, dict):
        manifest_commit = manifest_document.get("source", {}).get("commit") if isinstance(manifest_document.get("source"), dict) else None
        if manifest_commit != claim.get("source_commit"):
            findings.append(
                _finding(
                    "controlled_manifest_source_mismatch",
                    "run manifest source commit does not match the active claim",
                    claim_id=claim_id,
                )
            )
    else:
        findings.append(
            _finding(
                "controlled_manifest_invalid",
                "scaled controlled claim manifest is not valid JSON evidence",
                claim_id=claim_id,
            )
        )
    return findings, checked


def audit_claim_ledger(
    root: str | Path,
    ledger_path: str | Path,
) -> dict[str, Any]:
    """Audit every active paper claim against immutable local evidence.

    Findings are accumulated instead of raising so one invocation gives a
    complete repair list. Malformed top-level ledger JSON still raises because
    there is no trustworthy claim set to inspect.
    """

    root = Path(root).resolve()
    ledger_path = Path(ledger_path)
    if not ledger_path.is_absolute():
        ledger_path = _rooted_path(root, str(ledger_path))
    else:
        ledger_path = ledger_path.resolve()
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    if not isinstance(ledger, dict) or not isinstance(ledger.get("claims"), list):
        raise ValueError("claim ledger must be an object with a claims array")

    findings: list[dict[str, Any]] = []
    superseded: set[str] = set()
    supersession_value = ledger.get("supersession_index")
    supersession_count = 0
    if not isinstance(supersession_value, str) or not supersession_value:
        findings.append(
            _finding(
                "ledger_metadata_missing",
                "supersession_index is required",
                field="supersession_index",
            )
        )
    else:
        try:
            supersession_path = _rooted_path(root, supersession_value)
        except ValueError as exc:
            findings.append(_finding("invalid_path", str(exc)))
        else:
            if not supersession_path.is_file():
                findings.append(
                    _finding(
                        "supersession_index_missing",
                        f"supersession index not found: {supersession_value}",
                    )
                )
            else:
                supersession_payload = json.loads(
                    supersession_path.read_text(encoding="utf-8")
                )
                entries = supersession_payload.get("superseded_artifacts", [])
                if not isinstance(entries, list):
                    findings.append(
                        _finding(
                            "supersession_index_invalid",
                            "superseded_artifacts must be an array",
                        )
                    )
                else:
                    supersession_count = len(entries)
                    for entry in entries:
                        if not isinstance(entry, dict):
                            findings.append(
                                _finding(
                                    "supersession_entry_invalid",
                                    "supersession entry must be an object",
                                )
                            )
                            continue
                        required = (
                            "artifact_path",
                            "replacement_path",
                            "reason",
                            "superseded_on",
                        )
                        if any(not entry.get(field) for field in required):
                            findings.append(
                                _finding(
                                    "supersession_entry_invalid",
                                    "supersession entry lacks required metadata",
                                )
                            )
                            continue
                        superseded.add(str(entry["artifact_path"]))

    claims = ledger["claims"]
    claim_ids: set[str] = set()
    checked_assertions = 0
    checked_artifacts: list[dict[str, str]] = []
    required_fields = (
        "claim_id",
        "status",
        "paper_location",
        "artifact_path",
        "artifact_format",
        "artifact_sha256",
        "assertions",
        "run_command",
        "manifest",
        "source_commit",
        "claim_boundary",
    )
    expected_active_claim_count = ledger.get("expected_active_claim_count")
    expected_assertion_count = ledger.get("expected_assertion_count")
    for field, value in (
        ("expected_active_claim_count", expected_active_claim_count),
        ("expected_assertion_count", expected_assertion_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            findings.append(
                _finding(
                    "ledger_schema_invalid",
                    f"{field} must be a positive integer",
                    field=field,
                )
            )
    for index, claim in enumerate(claims):
        if not isinstance(claim, dict):
            findings.append(
                _finding("claim_invalid", f"claim at index {index} is not an object")
            )
            continue
        claim_id = str(claim.get("claim_id") or f"claim_index_{index}")
        for field in required_fields:
            if field not in claim or claim[field] in (None, "", []):
                findings.append(
                    _finding(
                        "claim_metadata_missing",
                        f"required claim field is missing: {field}",
                        claim_id=claim_id,
                        field=field,
                    )
                )
        findings.extend(_active_claim_schema_findings(claim, claim_id))
        if claim_id in claim_ids:
            findings.append(
                _finding(
                    "duplicate_claim_id",
                    f"duplicate claim id: {claim_id}",
                    claim_id=claim_id,
                )
            )
        claim_ids.add(claim_id)
        if claim.get("status") != "active":
            continue

        artifact_value = claim.get("artifact_path")
        if isinstance(artifact_value, str) and artifact_value in superseded:
            findings.append(
                _finding(
                    "active_claim_uses_superseded_artifact",
                    f"active claim points to superseded artifact: {artifact_value}",
                    claim_id=claim_id,
                )
            )
        if not isinstance(artifact_value, str) or not artifact_value:
            continue
        try:
            artifact_path = _rooted_path(root, artifact_value)
        except ValueError as exc:
            findings.append(_finding("invalid_path", str(exc), claim_id=claim_id))
            continue
        if not artifact_path.is_file():
            findings.append(
                _finding(
                    "artifact_missing",
                    f"artifact not found: {artifact_value}",
                    claim_id=claim_id,
                )
            )
            continue
        actual_artifact_hash = _sha256(artifact_path)
        checked_artifacts.append(
            {"path": artifact_value, "sha256": actual_artifact_hash}
        )
        if actual_artifact_hash != claim.get("artifact_sha256"):
            findings.append(
                _finding(
                    "artifact_hash_mismatch",
                    f"artifact SHA-256 mismatch: {artifact_value}",
                    claim_id=claim_id,
                )
            )

        document: Any = None
        if claim.get("artifact_format") != "json":
            findings.append(
                _finding(
                    "unsupported_artifact_format",
                    f"unsupported artifact format: {claim.get('artifact_format')}",
                    claim_id=claim_id,
                )
            )
        else:
            try:
                document = json.loads(artifact_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                findings.append(
                    _finding(
                        "artifact_parse_error",
                        f"cannot parse artifact JSON: {exc}",
                        claim_id=claim_id,
                    )
                )
        assertions = claim.get("assertions", [])
        if isinstance(assertions, list) and document is not None:
            for assertion in assertions:
                checked_assertions += 1
                if not isinstance(assertion, dict):
                    findings.append(
                        _finding(
                            "assertion_invalid",
                            "claim assertion is not an object",
                            claim_id=claim_id,
                        )
                    )
                    continue
                pointer = assertion.get("pointer")
                try:
                    actual = _json_pointer(document, pointer)
                except (KeyError, TypeError):
                    findings.append(
                        _finding(
                            "json_pointer_missing",
                            f"JSON pointer not found: {pointer}",
                            claim_id=claim_id,
                        )
                    )
                    continue
                operator = str(assertion.get("operator", "equals"))
                expected = assertion.get("expected")
                try:
                    matches = _assertion_matches(actual, operator, expected)
                except (TypeError, ValueError) as exc:
                    findings.append(
                        _finding(
                            "assertion_invalid",
                            str(exc),
                            claim_id=claim_id,
                        )
                    )
                    continue
                if not matches:
                    findings.append(
                        _finding(
                            "assertion_mismatch",
                            f"{pointer} {operator} {expected!r}, observed {actual!r}",
                            claim_id=claim_id,
                        )
                    )
        if isinstance(document, dict):
            findings.extend(
                _semantic_findings(
                    claim_id,
                    claim.get("validation_profile"),
                    document,
                )
            )

        manifest_path: Path | None = None
        manifest_document: Any = None
        manifest = claim.get("manifest")
        if isinstance(manifest, dict):
            manifest_value = manifest.get("path")
            manifest_hash = manifest.get("sha256")
            if not manifest_value or not manifest_hash:
                findings.append(
                    _finding(
                        "claim_metadata_missing",
                        "manifest path and sha256 are required",
                        claim_id=claim_id,
                        field="manifest",
                    )
                )
            else:
                try:
                    manifest_path = _rooted_path(root, str(manifest_value))
                except ValueError as exc:
                    findings.append(
                        _finding("invalid_path", str(exc), claim_id=claim_id)
                    )
                else:
                    if not manifest_path.is_file():
                        findings.append(
                            _finding(
                                "manifest_missing",
                                f"manifest not found: {manifest_value}",
                                claim_id=claim_id,
                            )
                        )
                    elif _sha256(manifest_path) != manifest_hash:
                        findings.append(
                            _finding(
                                "manifest_hash_mismatch",
                                f"manifest SHA-256 mismatch: {manifest_value}",
                                claim_id=claim_id,
                            )
                        )
                    else:
                        try:
                            manifest_document = json.loads(
                                manifest_path.read_text(encoding="utf-8")
                            )
                        except (OSError, json.JSONDecodeError):
                            manifest_document = None
        controlled_findings, controlled_checked = _controlled_evidence_findings(
            root,
            claim,
            claim_id,
            manifest_path,
            manifest_document,
        )
        findings.extend(controlled_findings)
        checked_artifacts.extend(controlled_checked)
        source_commit = claim.get("source_commit")
        if isinstance(source_commit, str) and source_commit and not re.fullmatch(r"[0-9a-f]{40}", source_commit):
            findings.append(
                _finding(
                    "source_commit_invalid",
                    "source_commit must be a full lowercase Git object id",
                    claim_id=claim_id,
                )
            )

    active_claim_count = sum(
        1
        for claim in claims
        if isinstance(claim, dict) and claim.get("status") == "active"
    )
    declared_assertion_count = sum(
        len(claim.get("assertions", []))
        for claim in claims
        if isinstance(claim, dict)
        and claim.get("status") == "active"
        and isinstance(claim.get("assertions"), list)
    )
    if isinstance(expected_active_claim_count, int) and not isinstance(
        expected_active_claim_count, bool
    ) and active_claim_count != expected_active_claim_count:
        findings.append(
            _finding(
                "active_claim_count_mismatch",
                f"expected {expected_active_claim_count} active claims, found {active_claim_count}",
                field="expected_active_claim_count",
            )
        )
    if isinstance(expected_assertion_count, int) and not isinstance(
        expected_assertion_count, bool
    ) and declared_assertion_count != expected_assertion_count:
        findings.append(
            _finding(
                "assertion_count_mismatch",
                f"expected {expected_assertion_count} active assertions, found {declared_assertion_count}",
                field="expected_assertion_count",
            )
        )

    unique_checked_artifacts = {
        item["path"]: item["sha256"] for item in checked_artifacts
    }
    return {
        "schema_version": 1,
        "evidence_id": "paper_claim_audit",
        "ledger_path": str(ledger_path.relative_to(root)),
        "ledger_sha256": _sha256(ledger_path),
        "claim_count": len(claims),
        "active_claim_count": active_claim_count,
        "expected_active_claim_count": expected_active_claim_count,
        "declared_assertion_count": declared_assertion_count,
        "expected_assertion_count": expected_assertion_count,
        "checked_assertion_count": checked_assertions,
        "checked_artifacts": [
            {"path": path, "sha256": sha256}
            for path, sha256 in sorted(unique_checked_artifacts.items())
        ],
        "superseded_artifact_count": supersession_count,
        "finding_count": len(findings),
        "findings": findings,
        "status": "passed" if not findings else "failed",
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
    audit = subparsers.add_parser(
        "audit", help="fail closed when a paper claim is not traceable to evidence"
    )
    audit.add_argument("--root", type=Path, default=Path("."))
    audit.add_argument("--ledger", type=Path, required=True)
    audit.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.command == "controlled":
        evidence = build_controlled_suite_evidence(args.instances, args.results)
        _write_json(evidence, args.out)
        print(
            f"wrote controlled evidence: {evidence['instance_count']} instances, "
            f"{evidence['variant_row_count']} variant rows -> {args.out}"
        )
        return 0
    if args.command == "audit":
        report = audit_claim_ledger(args.root, args.ledger)
        _write_json(report, args.out)
        print(
            f"claim audit {report['status']}: {report['claim_count']} claims, "
            f"{report['finding_count']} findings -> {args.out}"
        )
        return 0 if report["status"] == "passed" else 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
