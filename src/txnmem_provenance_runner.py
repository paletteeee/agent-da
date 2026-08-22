"""Gate-controlled provenance candidate entry point for immutable exports."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import urllib.request


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


def _completion_payload(material: dict) -> bytes:
    return json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _load_smoke_graph_database(runtime_site: Path):
    root = runtime_site.expanduser().absolute().resolve(strict=True)
    module = importlib.import_module("neo4j")
    module_file = Path(str(getattr(module, "__file__", ""))).resolve(strict=True)
    try:
        module_file.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("smoke Neo4j driver escaped locked runtime") from exc
    graph_database = getattr(module, "GraphDatabase", None)
    if graph_database is None:
        raise RuntimeError("smoke Neo4j driver is unavailable")
    return graph_database


def _probe_smoke_qdrant(url: str) -> bool:
    if url != "http://127.0.0.1:19000/readyz":
        raise ValueError("smoke Qdrant endpoint is not exact loopback")
    with urllib.request.urlopen(url, timeout=10.0) as response:
        status = int(response.status)
        body = response.read(4097)
    return 200 <= status < 300 and len(body) <= 4096


def _probe_smoke_neo4j(
    *,
    runtime_site: Path,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
) -> bool:
    if (
        neo4j_uri != "bolt://127.0.0.1:19001"
        or neo4j_user != "neo4j"
        or not isinstance(neo4j_password, str)
        or not neo4j_password
    ):
        raise ValueError("smoke Neo4j endpoint or credential is invalid")
    GraphDatabase = _load_smoke_graph_database(runtime_site)
    with GraphDatabase.driver(
        neo4j_uri, auth=(neo4j_user, neo4j_password)
    ) as driver:
        with driver.session() as session:
            with session.begin_transaction() as transaction:
                record = transaction.run("RETURN 1 AS value").single(strict=True)
                if record is None or record["value"] != 1:
                    return False
                transaction.commit()
    return True


def _provenance_smoke_receipt(
    runtime_site: Path, neo4j_password: str
) -> dict:
    qdrant_ok = _probe_smoke_qdrant("http://127.0.0.1:19000/readyz")
    neo4j_ok = _probe_smoke_neo4j(
        runtime_site=runtime_site,
        neo4j_uri="bolt://127.0.0.1:19001",
        neo4j_user="neo4j",
        neo4j_password=neo4j_password,
    )
    if qdrant_ok is not True or neo4j_ok is not True:
        raise RuntimeError("formal smoke backend probe failed")
    return {
        "schema": "txnmem-provenance-smoke-child-receipt-v1",
        "qdrant_proxy_ok": True,
        "neo4j_proxy_ok": True,
    }


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
        if not arguments or arguments[0] not in {
            "provenance-performance",
            "provenance-smoke",
        }:
            return 72
        runtime_path = Path(runtime_site).expanduser().absolute().resolve(strict=True)
        sys.path.insert(0, runtime_site)
        sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))
        if arguments[0] == "provenance-smoke":
            if arguments != ["provenance-smoke"]:
                return 72
            neo4j_password = os.environ.pop("TXNMEM_NEO4J_PASSWORD", None)
            if not neo4j_password:
                return 72
            material = _provenance_smoke_receipt(
                runtime_path, neo4j_password
            )
            payload = _completion_payload(material)
            if not payload or len(payload) > 65536:
                return 74
            _write_all(completion_fd, payload)
            return 0
        if (
            "--formal" in arguments
            or arguments.count("--backend") != 1
            or arguments.index("--backend") + 1 >= len(arguments)
            or arguments[arguments.index("--backend") + 1] != "vector-graph"
        ):
            return 72
        from txnmem_experiment import main as experiment_main

        result = experiment_main(arguments)
        if type(result) is not int:
            return 73
        if result == 0:
            material = _candidate_completion_material(arguments)
            payload = _completion_payload(material)
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
