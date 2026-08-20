"""Gate-controlled provenance candidate entry point for immutable exports."""

from __future__ import annotations

import hashlib
import json
import os
import sys


def _argument_value(arguments: list[str], name: str) -> str:
    if arguments.count(name) != 1:
        raise ValueError("formal runner argument is missing or duplicated")
    index = arguments.index(name)
    if index + 1 >= len(arguments) or not arguments[index + 1]:
        raise ValueError("formal runner argument has no value")
    return arguments[index + 1]


def _candidate_completion_material(arguments: list[str]) -> dict:
    from txnmem_provenance_performance import (
        candidate_attestation_material,
        formal_matrix_config_sha256,
        provenance_bundle_id,
    )

    run_id = _argument_value(arguments, "--run-id")
    candidate_root = _argument_value(arguments, "--out-dir")
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    bundle_id = provenance_bundle_id(
        config_sha256=formal_matrix_config_sha256(),
        run_id_sha256=run_hash,
        formal=False,
        backend="vector-graph",
    )
    return candidate_attestation_material(candidate_root, bundle_id)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("formal completion receipt write failed")
        view = view[written:]


def main(argv: list[str] | None = None) -> int:
    gate_value = os.environ.pop("TXNMEM_PROVENANCE_START_GATE_FD", None)
    ready_value = os.environ.pop("TXNMEM_PROVENANCE_READY_FD", None)
    completion_value = os.environ.pop("TXNMEM_PROVENANCE_COMPLETION_FD", None)
    runtime_site = os.environ.pop("TXNMEM_PROVENANCE_RUNTIME_SITE", None)
    if (
        gate_value is None
        or not gate_value.isdigit()
        or ready_value is None
        or not ready_value.isdigit()
        or completion_value is None
        or not completion_value.isdigit()
        or gate_value == ready_value
        or completion_value in {gate_value, ready_value}
        or runtime_site is None
        or not os.path.isabs(runtime_site)
        or not os.path.isdir(runtime_site)
    ):
        return 70
    gate_fd = int(gate_value)
    ready_fd = int(ready_value)
    completion_fd = int(completion_value)
    try:
        if os.write(ready_fd, b"R") != 1:
            return 70
    finally:
        os.close(ready_fd)
    try:
        token = os.read(gate_fd, 1)
    finally:
        os.close(gate_fd)
    if token != b"G":
        return 71

    try:
        arguments = list(sys.argv[1:] if argv is None else argv)
        if (
            not arguments
            or arguments[0] != "provenance-performance"
            or "--formal" in arguments
            or "--backend" not in arguments
            or arguments[arguments.index("--backend") + 1] != "vector-graph"
        ):
            return 72
        sys.path.insert(0, runtime_site)
        sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))
        from txnmem_experiment import main as experiment_main

        result = experiment_main(arguments)
        if type(result) is not int:
            return 73
        if result == 0:
            material = _candidate_completion_material(arguments)
            payload = json.dumps(
                material,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            if not payload or len(payload) > 65536:
                return 74
            _write_all(completion_fd, payload)
        return result
    except (OSError, TypeError, ValueError, RuntimeError):
        return 74
    finally:
        os.close(completion_fd)


if __name__ == "__main__":
    raise SystemExit(main())
