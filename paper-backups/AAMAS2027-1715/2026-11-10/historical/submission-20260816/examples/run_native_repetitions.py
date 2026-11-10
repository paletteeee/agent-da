"""Run sanitized native-agent repetitions against an OpenAI-compatible endpoint."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from txnmem_model_protocol import OpenAICompatibleClient  # noqa: E402
from txnmem_real_experiment import load_task_manifest  # noqa: E402
from txnmem_statistics import run_repetitions  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--api-key-env", default="OPENAI_API_KEY")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()
    manifest, manifest_sha256 = load_task_manifest(args.manifest)
    model = OpenAICompatibleClient(
        args.endpoint,
        args.model,
        api_key=os.environ.get(args.api_key_env),
        timeout_s=args.timeout,
    )
    report = run_repetitions(manifest, model, args.out_dir, repetitions=args.repetitions)
    report["manifest_sha256"] = manifest_sha256
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "repetition_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote native repetition report -> {args.out_dir / 'repetition_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
