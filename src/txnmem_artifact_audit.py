"""Fail closed when raw traces or secret-bearing fields enter result artifacts."""

from __future__ import annotations

import argparse
import ipaddress
import json
from pathlib import Path
import re
import subprocess
from typing import Iterable


_RAW_FILENAMES = {
    "locomo_paired_predictions.json",
    "model_load_traces.jsonl",
    "native_model_traces.jsonl",
}
_CONTROLLED_PUBLIC_ALLOWLIST = {
    ("final_controlled", "data", "generated_instances.jsonl"),
    ("final_controlled", "data", "reference_oracles.jsonl"),
    ("final_controlled_200", "data", "generated_instances.jsonl"),
    ("final_controlled_200", "data", "reference_oracles.jsonl"),
}
_SENSITIVE_KEY = re.compile(
    r'"(?:password|api_key|access_token|secret|messages|arguments|tool_args|prompt|content)"\s*:',
    flags=re.IGNORECASE,
)
_TOKEN_VALUE = re.compile(
    r"(?:\bsk-[A-Za-z0-9_-]{8,}\b|\bBearer\s+[A-Za-z0-9._-]{8,}|-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    flags=re.IGNORECASE,
)
_IPV4_VALUE = re.compile(r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])")


def _contains_sensitive_value(text: str) -> bool:
    if _TOKEN_VALUE.search(text):
        return True
    for match in _IPV4_VALUE.finditer(text):
        try:
            address = ipaddress.ip_address(match.group(0))
        except ValueError:
            continue
        if address.is_global:
            return True
    return False


def audit_result_paths(paths: Iterable[str | Path]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for source in paths:
        path = Path(source)
        parts = path.parts
        if "results" not in parts:
            continue
        result_index = parts.index("results")
        result_parts = parts[result_index + 1 :]
        relative_result_path = tuple(result_parts)
        if ".." in parts:
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        audit_root = path.parents[len(result_parts)]
        resolved_path = path.resolve(strict=False)
        try:
            resolved_relative = resolved_path.relative_to(audit_root.resolve(strict=False))
        except ValueError:
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        if resolved_relative != path.relative_to(audit_root):
            findings.append({"code": "result_path_escape", "path": str(path)})
            continue
        if (
            ("data" in result_parts and relative_result_path not in _CONTROLLED_PUBLIC_ALLOWLIST)
            or path.name in _RAW_FILENAMES
        ):
            findings.append({"code": "raw_result_path", "path": str(path)})
        if not path.is_file() or path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if _SENSITIVE_KEY.search(text):
            findings.append({"code": "sensitive_result_key", "path": str(path)})
        if _contains_sensitive_value(text):
            findings.append({"code": "sensitive_result_value", "path": str(path)})
    return findings


def _git_result_paths(root: Path) -> list[Path]:
    commands = (
        ["git", "ls-files", "results"],
        ["git", "ls-files", "--others", "--exclude-standard", "results"],
    )
    paths: set[Path] = set()
    for command in commands:
        completed = subprocess.run(
            command,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        for line in completed.stdout.splitlines():
            if line.strip():
                paths.add(root / line.strip())
    return sorted(paths)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    findings = audit_result_paths(_git_result_paths(root))
    print(json.dumps({"finding_count": len(findings), "findings": findings}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
