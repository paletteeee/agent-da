"""Fail-closed checks for the evidence used in the TxnMem manuscript."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from txnmem_claim_audit import audit_claim_ledger


_CLAIM_MARKER = re.compile(r"\[\[CLAIM:([A-Za-z0-9_-]+)\]\]")
_AUTHOR_ANNOTATION_BEGIN = "<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->"
_AUTHOR_ANNOTATION_END = "<!-- TXNMEM-AUTHOR-ANNOTATIONS:END -->"
_AUTHOR_ANNOTATION_BLOCK = re.compile(
    rf"{re.escape(_AUTHOR_ANNOTATION_BEGIN)}.*?{re.escape(_AUTHOR_ANNOTATION_END)}",
    re.DOTALL,
)
_HEADING_LINE = re.compile(r"^\s{0,3}#{1,6}\s+.*$", re.MULTILINE)
_VERSION_NUMBER = re.compile(r"(?<![\w.])\d+(?:\.\d+){2,}(?![\w.])")
_NUMBER = re.compile(r"(?<![\w.])\d{1,3}(?:,\d{3})+(?:\.\d+)?|(?<![\w.])\d+(?:\.\d+)?(?![\w.])")


def strip_author_annotations(text: str) -> str:
    """Return reader-facing Markdown without fail-closed audit annotations.

    Manuscript auditing deliberately consumes the unstripped source so claim
    boundaries remain available to the evidence checks. Renderers should call
    this projection before producing reader-facing output.
    """

    begins = text.count(_AUTHOR_ANNOTATION_BEGIN)
    ends = text.count(_AUTHOR_ANNOTATION_END)
    if begins != ends:
        raise ValueError("author annotation delimiters must be paired")
    stripped, block_count = _AUTHOR_ANNOTATION_BLOCK.subn("", text)
    if block_count != begins:
        raise ValueError("author annotation delimiters must form complete blocks")
    return stripped


def _finding(code: str, message: str, **details: Any) -> dict[str, Any]:
    finding: dict[str, Any] = {"code": code, "message": message}
    finding.update({key: value for key, value in details.items() if value is not None})
    return finding


def load_paper_config(path: Path) -> dict[str, Any]:
    """Load the versioned manuscript contract and reject malformed JSON."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("paper config must be a JSON object")
    return payload


def _check_required_sections(text: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    required_sections = config.get("required_sections")
    if not isinstance(required_sections, list) or not all(
        isinstance(section, str) and section for section in required_sections
    ):
        return [_finding("paper_config_invalid", "required_sections must be a string array")]

    optional_sections = config.get("drafting_optional_sections", [])
    if not isinstance(optional_sections, list) or not all(
        isinstance(section, str) and section for section in optional_sections
    ):
        return [
            _finding(
                "paper_config_invalid",
                "drafting_optional_sections must be a string array",
            )
        ]
    if config.get("drafting_mode") is not True:
        optional_sections = []
    elif optional_sections and required_sections[-len(optional_sections) :] != optional_sections:
        return [
            _finding(
                "paper_config_invalid",
                "drafting_optional_sections must be a suffix of required_sections",
            )
        ]

    findings: list[dict[str, Any]] = []
    for section in required_sections:
        if section in optional_sections:
            continue
        pattern = re.compile(rf"^\s{{0,3}}#{{1,6}}\s+{re.escape(section)}\s*$", re.MULTILINE)
        if not pattern.search(text):
            findings.append(
                _finding(
                    "missing_required_section",
                    f"required manuscript section is absent: {section}",
                    section=section,
                )
            )
    return findings


def _superseded_artifacts(root: Path, config: dict[str, Any]) -> tuple[set[str], list[dict[str, Any]]]:
    index_value = config.get("supersession_index_path")
    if not isinstance(index_value, str) or not index_value:
        return set(), [
            _finding(
                "paper_config_invalid",
                "supersession_index_path is required",
            )
        ]
    index_path = root / index_value
    if not index_path.is_file():
        return set(), [
            _finding(
                "supersession_index_missing",
                f"supersession index not found: {index_value}",
            )
        ]
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return set(), [
            _finding("supersession_index_invalid", f"cannot parse supersession index: {exc}")
        ]
    entries = payload.get("superseded_artifacts") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return set(), [
            _finding(
                "supersession_index_invalid",
                "superseded_artifacts must be an array",
            )
        ]
    artifacts = {
        str(entry["artifact_path"])
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("artifact_path"), str)
    }
    if len(artifacts) != len(entries):
        return artifacts, [
            _finding(
                "supersession_index_invalid",
                "every supersession entry must declare artifact_path",
            )
        ]
    return artifacts, []


def _check_forbidden_artifacts(text: str, root: Path, config: dict[str, Any]) -> list[dict[str, Any]]:
    configured = config.get("forbidden_artifacts")
    if not isinstance(configured, list) or not all(
        isinstance(artifact, str) and artifact for artifact in configured
    ):
        return [_finding("paper_config_invalid", "forbidden_artifacts must be a string array")]
    superseded, findings = _superseded_artifacts(root, config)
    forbidden = superseded | set(configured)
    for artifact in sorted(forbidden):
        if artifact in text:
            findings.append(
                _finding(
                    "superseded_artifact",
                    f"manuscript cites a superseded artifact: {artifact}",
                    artifact_path=artifact,
                )
            )
    return findings


def _check_required_boundaries(text: str, boundaries: Any) -> list[dict[str, Any]]:
    if not isinstance(boundaries, list) or not all(
        isinstance(boundary, str) and boundary for boundary in boundaries
    ):
        return [
            _finding(
                "paper_config_invalid",
                "required_claim_boundaries must be a string array",
            )
        ]
    return [
        _finding(
            "missing_claim_boundary",
            "required claim boundary is absent from manuscript",
            claim_boundary=boundary,
        )
        for boundary in boundaries
        if boundary not in text
    ]


def _check_claim_audit(root: Path, config: dict[str, Any]) -> list[dict[str, Any]]:
    audit_value = config.get("claim_audit_path")
    if not isinstance(audit_value, str) or not audit_value:
        return [_finding("paper_config_invalid", "claim_audit_path is required")]
    audit_path = root / audit_value
    if not audit_path.is_file():
        return [
            _finding("claim_audit_missing", f"claim audit not found: {audit_value}")
        ]
    try:
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [_finding("claim_audit_invalid", f"cannot parse claim audit: {exc}")]
    if not isinstance(payload, dict):
        return [_finding("claim_audit_invalid", "claim audit must be a JSON object")]
    ledger_value = config.get("claim_ledger_path", "configs/paper_claims.json")
    if not isinstance(ledger_value, str) or not ledger_value:
        return [_finding("paper_config_invalid", "claim_ledger_path is required")]
    ledger_path = root / ledger_value
    if not ledger_path.is_file():
        return [
            _finding("claim_ledger_missing", f"claim ledger not found: {ledger_value}")
        ]
    recorded_digest = payload.get("ledger_sha256")
    if not isinstance(recorded_digest, str) or not recorded_digest:
        return [
            _finding(
                "claim_audit_ledger_sha256_missing",
                "claim audit must record the audited claim-ledger SHA-256",
            )
        ]
    current_digest = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    if recorded_digest != current_digest:
        return [
            _finding(
                "claim_audit_ledger_sha256_mismatch",
                "claim audit ledger SHA-256 does not match the current claim ledger",
            )
        ]
    try:
        fresh_payload = audit_claim_ledger(root, ledger_value)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return [
            _finding(
                "fresh_claim_audit_failed",
                f"current evidence could not be audited: {exc}",
            )
        ]
    if fresh_payload.get("status") != "passed" or fresh_payload.get("finding_count") != 0:
        return [
            _finding(
                "fresh_claim_audit_failed",
                "current evidence bytes do not pass the claim audit",
                claim_audit_finding_codes=[
                    item.get("code")
                    for item in fresh_payload.get("findings", [])
                    if isinstance(item, dict)
                ],
            )
        ]
    if payload != fresh_payload:
        return [
            _finding(
                "claim_audit_stale_report",
                "stored claim audit does not equal a fresh audit of current evidence",
            )
        ]
    configured_ids = config.get("active_claim_ids")
    if isinstance(configured_ids, list) and fresh_payload.get("active_claim_count") != len(
        configured_ids
    ):
        return [
            _finding(
                "claim_audit_count_mismatch",
                "claim audit active count does not match the manuscript contract",
            )
        ]
    return []


def _check_figure_manifest(root: Path, config: dict[str, Any]) -> list[dict[str, Any]]:
    manifest_value = config.get(
        "figure_manifest_path", "paper_assets/figures/manifest.json"
    )
    if not isinstance(manifest_value, str) or not manifest_value:
        return [_finding("paper_config_invalid", "figure_manifest_path is required")]
    manifest_path = root / manifest_value
    if not manifest_path.is_file():
        return [
            _finding(
                "figure_manifest_missing",
                f"figure manifest not found: {manifest_value}",
            )
        ]
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [
            _finding("figure_manifest_invalid", f"cannot parse figure manifest: {exc}")
        ]
    figures = payload.get("figures") if isinstance(payload, dict) else None
    if not isinstance(figures, dict) or not figures:
        return [
            _finding("figure_manifest_invalid", "figures must be a nonempty object")
        ]

    findings: list[dict[str, Any]] = []
    for figure_id, figure in sorted(figures.items()):
        if not isinstance(figure_id, str) or not isinstance(figure, dict):
            findings.append(
                _finding(
                    "figure_manifest_invalid",
                    "each figure entry must be a named object",
                )
            )
            continue
        sources = figure.get("sources")
        if not isinstance(sources, list) or not sources:
            findings.append(
                _finding(
                    "figure_manifest_invalid",
                    "figure sources must be a nonempty array",
                    figure_id=figure_id,
                )
            )
        else:
            for source in sources:
                if not isinstance(source, dict):
                    findings.append(
                        _finding(
                            "figure_manifest_invalid",
                            "figure source must be an object",
                            figure_id=figure_id,
                        )
                    )
                    continue
                source_value = source.get("path")
                expected_hash = source.get("sha256")
                if not isinstance(source_value, str) or not source_value or not isinstance(
                    expected_hash, str
                ):
                    findings.append(
                        _finding(
                            "figure_manifest_invalid",
                            "figure source requires string path and sha256",
                            figure_id=figure_id,
                        )
                    )
                    continue
                try:
                    source_path = (root / source_value).resolve()
                    source_path.relative_to(root)
                    actual_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
                except (OSError, ValueError):
                    findings.append(
                        _finding(
                            "figure_source_unreadable",
                            f"figure source cannot be read: {source_value}",
                            figure_id=figure_id,
                        )
                    )
                    continue
                if actual_hash != expected_hash:
                    findings.append(
                        _finding(
                            "figure_source_hash_mismatch",
                            f"figure source SHA-256 mismatch: {source_value}",
                            figure_id=figure_id,
                        )
                    )

        output_value = figure.get("file")
        expected_output_hash = figure.get("output_sha256")
        if not isinstance(output_value, str) or not output_value or not isinstance(
            expected_output_hash, str
        ):
            findings.append(
                _finding(
                    "figure_manifest_invalid",
                    "figure output requires string file and output_sha256",
                    figure_id=figure_id,
                )
            )
            continue
        try:
            output_path = (manifest_path.parent / output_value).resolve()
            output_path.relative_to(root)
            actual_output_hash = hashlib.sha256(output_path.read_bytes()).hexdigest()
        except (OSError, ValueError):
            findings.append(
                _finding(
                    "figure_output_unreadable",
                    f"figure output cannot be read: {output_value}",
                    figure_id=figure_id,
                )
            )
            continue
        if actual_output_hash != expected_output_hash:
            findings.append(
                _finding(
                    "figure_output_hash_mismatch",
                    f"figure output SHA-256 mismatch: {output_value}",
                    figure_id=figure_id,
                )
            )
    return findings


def _active_claims(root: Path, config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ledger_value = config.get("claim_ledger_path", "configs/paper_claims.json")
    if not isinstance(ledger_value, str) or not ledger_value:
        return [], [_finding("paper_config_invalid", "claim_ledger_path is required")]
    ledger_path = root / ledger_value
    if not ledger_path.is_file():
        return [], [_finding("claim_ledger_missing", f"claim ledger not found: {ledger_value}")]
    try:
        payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [], [_finding("claim_ledger_invalid", f"cannot parse claim ledger: {exc}")]
    claims = payload.get("claims") if isinstance(payload, dict) else None
    if not isinstance(claims, list):
        return [], [_finding("claim_ledger_invalid", "claims must be an array")]
    active = [claim for claim in claims if isinstance(claim, dict) and claim.get("status") == "active"]
    if len(active) != sum(isinstance(claim, dict) and claim.get("status") == "active" for claim in claims):
        return [], [_finding("claim_ledger_invalid", "active claims must be objects")]
    return active, []


def _collect_numbers(value: Any) -> set[Decimal]:
    if isinstance(value, bool):
        return set()
    if isinstance(value, (int, float)):
        return {Decimal(str(value))}
    if isinstance(value, list):
        return set().union(*(_collect_numbers(item) for item in value)) if value else set()
    return set()


def _numeric_tokens(text: str) -> list[tuple[str, Decimal]]:
    without_markers = _CLAIM_MARKER.sub("", text)
    without_headings = _HEADING_LINE.sub("", without_markers)
    without_versions = _VERSION_NUMBER.sub("", without_headings)
    tokens: list[tuple[str, Decimal]] = []
    for match in _NUMBER.finditer(without_versions):
        literal = match.group(0)
        try:
            tokens.append((literal, Decimal(literal.replace(",", ""))))
        except InvalidOperation:
            continue
    return tokens


def _check_claim_values(text: str, root: Path, config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    claims, findings = _active_claims(root, config)
    if findings:
        return findings, [], []
    configured_ids = config.get("active_claim_ids")
    if not isinstance(configured_ids, list) or not all(
        isinstance(claim_id, str) and claim_id for claim_id in configured_ids
    ):
        return [
            _finding("paper_config_invalid", "active_claim_ids must be a string array")
        ], [], []

    ledger_ids = [str(claim.get("claim_id", "")) for claim in claims]
    if set(configured_ids) != set(ledger_ids) or len(configured_ids) != len(set(configured_ids)):
        findings.append(
            _finding(
                "active_claim_configuration_mismatch",
                "active_claim_ids must exactly match the ledger's active claim IDs",
            )
        )

    active_id_set = set(ledger_ids)
    allowed_numbers: set[Decimal] = set()
    allowed_numbers_by_claim: dict[str, set[Decimal]] = {}
    boundaries_by_claim: dict[str, str] = {}
    for claim in claims:
        claim_id = str(claim.get("claim_id", ""))
        boundary = claim.get("claim_boundary")
        if not isinstance(boundary, str) or boundary not in config.get(
            "required_claim_boundaries", []
        ):
            findings.append(
                _finding(
                    "claim_boundary_configuration_mismatch",
                    "active claim boundary must be configured for manuscript use",
                    claim_id=claim_id,
                )
            )
            continue
        boundaries_by_claim[claim_id] = boundary
        claim_numbers: set[Decimal] = set()
        for assertion in claim.get("assertions", []):
            if isinstance(assertion, dict):
                claim_numbers.update(_collect_numbers(assertion.get("expected")))
        allowed_numbers.update(claim_numbers)
        allowed_numbers_by_claim[claim_id] = claim_numbers

    for paragraph in re.split(r"\n\s*\n", text):
        marker_ids = _CLAIM_MARKER.findall(paragraph)
        active_markers = [claim_id for claim_id in marker_ids if claim_id in active_id_set]
        for claim_id in marker_ids:
            if claim_id not in active_id_set:
                findings.append(
                    _finding(
                        "inactive_claim_id",
                        f"manuscript references a claim that is not active: {claim_id}",
                        claim_id=claim_id,
                    )
                )
                continue
            boundary = boundaries_by_claim.get(claim_id)
            if boundary is None or boundary not in paragraph:
                findings.append(
                    _finding(
                        "missing_claim_binding",
                        "claim marker must share a paragraph with its configured boundary",
                        claim_id=claim_id,
                    )
                )
        paragraph_without_boundaries = paragraph
        for claim_id in active_markers:
            boundary = boundaries_by_claim.get(claim_id)
            if boundary:
                paragraph_without_boundaries = paragraph_without_boundaries.replace(
                    boundary, ""
                )
        for literal, value in _numeric_tokens(paragraph_without_boundaries):
            if not active_markers:
                findings.append(
                    _finding(
                        "unmarked_claim_value",
                        "formal manuscript number lacks an active claim marker and boundary",
                        value=literal,
                    )
                )
            if not any(
                value in allowed_numbers_by_claim.get(claim_id, set())
                for claim_id in active_markers
            ):
                findings.append(
                    _finding(
                        "uncovered_claim_value",
                        f"manuscript number is not covered by its active claim marker: {literal}",
                        value=literal,
                    )
                )

    return findings, ledger_ids, [str(value) for value in sorted(allowed_numbers)]


def audit_text(text: str, root: Path, config: dict[str, Any]) -> dict[str, Any]:
    """Return every manuscript-contract violation without silently widening scope."""

    root = Path(root).resolve()
    findings: list[dict[str, Any]] = []
    findings.extend(_check_required_sections(text, config))
    findings.extend(_check_claim_audit(root, config))
    findings.extend(_check_figure_manifest(root, config))
    findings.extend(_check_forbidden_artifacts(text, root, config))
    findings.extend(_check_required_boundaries(text, config.get("required_claim_boundaries")))
    claim_findings, allowed_ids, allowed_numbers = _check_claim_values(text, root, config)
    findings.extend(claim_findings)
    return {
        "schema_version": 1,
        "evidence_id": "txnmem_manuscript_audit",
        "finding_count": len(findings),
        "findings": findings,
        "allowed_claim_ids": allowed_ids,
        "allowed_numeric_values": allowed_numbers,
        "required_claim_boundaries": config.get("required_claim_boundaries", []),
        "status": "passed" if not findings else "failed",
    }


def audit_manuscript(source: Path, root: Path, config: dict[str, Any]) -> dict[str, Any]:
    """Audit a UTF-8 Markdown source file using the frozen manuscript contract."""

    return audit_text(source.read_text(encoding="utf-8"), root, config)


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit TxnMem manuscript evidence")
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    root = args.root.resolve()
    config_path = args.config if args.config.is_absolute() else root / args.config
    source_path = args.source if args.source.is_absolute() else root / args.source
    out_path = args.out if args.out.is_absolute() else root / args.out
    report = audit_manuscript(source_path, root, load_paper_config(config_path))
    _write_json(report, out_path)
    print(
        f"manuscript audit {report['status']}: {report['finding_count']} findings -> {out_path}"
    )
    return 0 if report["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
