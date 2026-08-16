#!/usr/bin/env python3
"""Create strict, compact submission artifacts from sanitized remote summaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from txnmem_evidence_aggregates import (
    aggregate_e2e_submission_evidence,
    aggregate_tau_submission_evidence,
    aggregate_toxiproxy_submission_evidence,
)


def _write(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    tau = subparsers.add_parser("tau")
    tau.add_argument("--source", type=Path, required=True)
    tau.add_argument("--out", type=Path, required=True)
    tau.add_argument("--expected-task-count", type=int, default=50)
    tau.add_argument("--model-revision", required=True)
    tau.add_argument("--model-server-build", required=True)
    tau.add_argument("--source-commit", required=True)
    tau.add_argument("--run-command", required=True)
    tau.add_argument("--runtime-attestation", type=Path, required=True)

    e2e = subparsers.add_parser("e2e")
    e2e.add_argument("--source", type=Path, required=True)
    e2e.add_argument("--out", type=Path, required=True)
    e2e.add_argument("--expected-task-count", type=int, default=5)
    e2e.add_argument("--source-commit", required=True)
    e2e.add_argument("--run-command", required=True)

    toxiproxy = subparsers.add_parser("toxiproxy")
    toxiproxy.add_argument("--source", type=Path, required=True)
    toxiproxy.add_argument("--out", type=Path, required=True)
    toxiproxy.add_argument("--expected-repetitions", type=int, default=30)
    toxiproxy.add_argument("--toxiproxy-version", required=True)
    toxiproxy.add_argument("--source-commit", required=True)
    toxiproxy.add_argument("--run-command", required=True)
    toxiproxy.add_argument("--runtime-attestation", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "tau":
        attestation = json.loads(args.runtime_attestation.read_text(encoding="utf-8"))
        result = aggregate_tau_submission_evidence(
            args.source,
            expected_task_count=args.expected_task_count,
            model_revision=args.model_revision,
            model_server_build=args.model_server_build,
            source_commit=args.source_commit,
            run_command=args.run_command,
            runtime_attestation=attestation,
        )
    elif args.command == "e2e":
        result = aggregate_e2e_submission_evidence(
            args.source,
            expected_task_count=args.expected_task_count,
            source_commit=args.source_commit,
            run_command=args.run_command,
        )
    else:
        attestation = json.loads(args.runtime_attestation.read_text(encoding="utf-8"))
        result = aggregate_toxiproxy_submission_evidence(
            args.source,
            expected_repetitions=args.expected_repetitions,
            toxiproxy_version=args.toxiproxy_version,
            source_commit=args.source_commit,
            run_command=args.run_command,
            runtime_attestation=attestation,
        )
    _write(result, args.out)
    print(f"wrote {result['evidence_id']} -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
