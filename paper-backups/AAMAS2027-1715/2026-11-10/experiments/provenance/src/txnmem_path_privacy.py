"""Scan publication-facing added diff lines for private absolute paths."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any


_PATTERNS = (
    (
        "personal_absolute_path",
        re.compile(r"/(?:Users|home)/[^\s`\"']+"),
    ),
    (
        "remote_run_path",
        re.compile(r"/data/" r"txnmem(?:[_/-][^\s`\"']*)?"),
    ),
    (
        "versioned_codex_cache",
        re.compile(
            r"(?:\.codex/plugins/cache|\.cache/codex-runtimes)/"
            r"[^\s`\"']*?\d+\.\d+\.\d+[^\s`\"']*"
        ),
    ),
)


def scan_added_diff(diff_text: str) -> list[dict[str, Any]]:
    """Return forbidden path occurrences from added content lines only."""

    findings: list[dict[str, Any]] = []
    current_path = "<unknown>"
    for diff_line_number, line in enumerate(diff_text.splitlines(), start=1):
        if line.startswith("+++ b/"):
            current_path = line[6:]
            continue
        if not line.startswith("+") or line.startswith("+++"):
            continue
        added = line[1:]
        for kind, pattern in _PATTERNS:
            for match in pattern.finditer(added):
                findings.append(
                    {
                        "kind": kind,
                        "path": current_path,
                        "diff_line": diff_line_number,
                        "match": match.group(0),
                    }
                )
    return findings


def scan_git_range(root: Path, base: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["git", "diff", "--unified=0", base],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return scan_added_diff(completed.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--base", required=True)
    args = parser.parse_args(argv)
    findings = scan_git_range(args.root.resolve(), args.base)
    print(
        json.dumps(
            {"finding_count": len(findings), "findings": findings},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())
