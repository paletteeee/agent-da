"""Canonical hashes for paired experimental conditions and source artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
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
