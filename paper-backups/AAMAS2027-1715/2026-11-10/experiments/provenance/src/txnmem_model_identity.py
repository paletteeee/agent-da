"""Content-derived model revision identity for local model artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from txnmem_conditions import canonical_fingerprint, file_sha256


def fingerprint_model_artifacts(model_root: str | Path) -> dict[str, Any]:
    root = Path(model_root).resolve()
    if not root.is_dir():
        raise ValueError("model root must be a directory")
    artifacts = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if any(part.startswith(".") for part in Path(relative).parts) or path.suffix == ".lock":
            continue
        artifacts.append(
            {
                "relative_path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    if not artifacts:
        raise ValueError("model root contains no fingerprintable artifacts")
    return {
        "identity_method": "sha256_of_relative_path_size_and_full_file_sha256",
        "artifact_count": len(artifacts),
        "artifact_bytes": sum(item["size_bytes"] for item in artifacts),
        "artifacts": artifacts,
        "model_revision_sha256": canonical_fingerprint(artifacts),
        "absolute_paths_retained": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    identity = fingerprint_model_artifacts(args.model_root)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(identity, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(identity["model_revision_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
