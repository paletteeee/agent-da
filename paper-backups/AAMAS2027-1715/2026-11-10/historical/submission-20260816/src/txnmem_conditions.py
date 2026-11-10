"""Canonical hashes for paired experimental conditions and source artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from pathlib import PurePosixPath
import re
import subprocess
from typing import Any, Mapping


def canonical_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_identity(paths: Mapping[str, str | Path]) -> dict[str, Any]:
    """Hash named source files without retaining host-specific absolute paths."""

    hashes = {
        str(name): file_sha256(path)
        for name, path in sorted(paths.items(), key=lambda item: str(item[0]))
    }
    return {
        "component_sha256": hashes,
        "fingerprint": canonical_fingerprint(hashes),
    }


def _canonical_source_path(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("source component path must be a nonempty relative path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or "\\" in value
        or any(ord(character) < 32 for character in value)
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
    ):
        raise ValueError(f"source component path is not canonical: {value!r}")
    return value


def verify_git_source_containment(
    repository_root: str | Path,
    commit: str,
    components: Mapping[str, str],
) -> dict[str, Any]:
    """Verify declared current hashes against tracked blobs at a Git commit."""

    root = Path(repository_root)
    normalized: dict[str, str] = {}
    for path, digest in sorted(components.items()):
        canonical = _canonical_source_path(path)
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(f"source component hash is not a lowercase SHA-256: {canonical}")
        normalized[canonical] = digest
    commit_exists = False
    if isinstance(commit, str) and re.fullmatch(r"[0-9a-f]{40}", commit):
        completed = subprocess.run(
            ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
            cwd=root,
            capture_output=True,
        )
        commit_exists = completed.returncode == 0
    results: dict[str, dict[str, str]] = {}
    for path, declared_hash in normalized.items():
        if not commit_exists:
            results[path] = {"status": "commit_missing"}
            continue
        object_type = subprocess.run(
            ["git", "cat-file", "-t", f"{commit}:{path}"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        if object_type.returncode != 0 or object_type.stdout.strip() != "blob":
            results[path] = {"status": "untracked"}
            continue
        blob = subprocess.run(
            ["git", "show", f"{commit}:{path}"],
            cwd=root,
            capture_output=True,
        )
        if blob.returncode != 0:
            results[path] = {"status": "untracked"}
            continue
        blob_hash = hashlib.sha256(blob.stdout).hexdigest()
        current_path = root / path
        current_hash = file_sha256(current_path) if current_path.is_file() else None
        matched = blob_hash == declared_hash == current_hash
        results[path] = {
            "status": "matched" if matched else "hash_mismatch",
            "declared_sha256": declared_hash,
            "blob_sha256": blob_hash,
            "current_sha256": current_hash or "missing",
        }
    return {
        "commit": commit,
        "commit_exists": commit_exists,
        "component_count": len(normalized),
        "component_results": results,
        "contained_in_commit": commit_exists and all(
            result["status"] == "matched" for result in results.values()
        ),
    }
