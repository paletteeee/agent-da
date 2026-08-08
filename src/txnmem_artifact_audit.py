"""Fail closed when raw traces or secret-bearing fields enter result artifacts."""

from __future__ import annotations

import argparse
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
_SENSITIVE_KEY = re.compile(
    r'"(?:password|api_key|access_token|secret|messages|arguments|prompt|content)"\s*:',
    flags=re.IGNORECASE,
)


def audit_result_paths(paths: Iterable[str | Path]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for source in paths:
        path = Path(source)
        parts = path.parts
        if "results" not in parts:
            continue
        result_index = parts.index("results")
        result_parts = parts[result_index + 1 :]
        if "data" in result_parts or path.name in _RAW_FILENAMES:
            findings.append({"code": "raw_result_path", "path": str(path)})
        if not path.is_file() or path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if _SENSITIVE_KEY.search(text):
            findings.append({"code": "sensitive_result_key", "path": str(path)})
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
