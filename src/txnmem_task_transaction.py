"""Task-scoped memory transaction coordination and deterministic staging backends."""

from __future__ import annotations

import copy
import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from txnmem_event_contract import validate_events
from txnmem_transaction_journal import TransactionJournal


PolicySnapshotProvider = Callable[[], Mapping[str, Any]]
PhaseHook = Callable[[str, Mapping[str, Any]], None]
_RECORD_TOOLS = frozenset(
    {"memory_write", "memory_derive", "memory_propagate", "memory_supersede"}
)


class TransactionBackend(Protocol):
    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None: ...

    def search_committed(self, query: str | None = None) -> list[Mapping[str, Any]]: ...

    def current_version(self, memory_id: str) -> int | None: ...

    def invalidate_committed(self, memory_id: str) -> Mapping[str, Any]: ...

    def stage_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        phase_hook: PhaseHook | None = None,
    ) -> Mapping[str, Any]: ...

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]: ...

    def finalize_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]: ...

    def cleanup_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]: ...

    def raw_transaction_state(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]: ...


class TaskTransactionError(RuntimeError):
    """A task transaction failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def _normalize_policy(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise TaskTransactionError("invalid_policy_snapshot")
    try:
        normalized = json.loads(json.dumps(dict(snapshot), ensure_ascii=False))
        version = normalized["version"]
        denied_actions = normalized.get("denied_actions", [])
        scope_overrides = normalized.get("scope_overrides", {})
    except (KeyError, TypeError, ValueError) as exc:
        raise TaskTransactionError("invalid_policy_snapshot") from exc
    if isinstance(version, bool) or not isinstance(version, int):
        raise TaskTransactionError("invalid_policy_snapshot")
    if not isinstance(denied_actions, list) or any(
        not isinstance(action, str) for action in denied_actions
    ):
        raise TaskTransactionError("invalid_policy_snapshot")
    if not isinstance(scope_overrides, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in scope_overrides.items()
    ):
        raise TaskTransactionError("invalid_policy_snapshot")
    return {
        "version": version,
        "denied_actions": denied_actions,
        "scope_overrides": scope_overrides,
    }


def _intent_memory_id(intent: Mapping[str, Any]) -> str | None:
    arguments = intent["arguments"]
    if intent["tool_name"] == "memory_supersede":
        return str(arguments["new_memory_id"])
    memory_id = arguments.get("memory_id")
    return str(memory_id) if memory_id is not None else None


def _source_ids(intent: Mapping[str, Any]) -> list[str]:
    arguments = intent["arguments"]
    if intent["tool_name"] in {"memory_derive", "memory_propagate"}:
        return [str(item) for item in arguments.get("source_ids", [])]
    if intent["tool_name"] == "memory_supersede":
        return [str(arguments["old_memory_id"])]
    return []


def _new_record(intent: Mapping[str, Any], previous_version: int | None = None) -> dict[str, Any] | None:
    tool_name = intent["tool_name"]
    if tool_name not in _RECORD_TOOLS:
        return None
    arguments = intent["arguments"]
    memory_id = _intent_memory_id(intent)
    assert memory_id is not None
    return {
        "memory_id": memory_id,
        "value": copy.deepcopy(arguments.get("value", memory_id)),
        "status": "pending",
        "target_status": "active",
        "agent_id": arguments.get("agent_id", "agent_model"),
        "scope": arguments.get("scope", "tenant:user_001"),
        "derived_from": _source_ids(intent)
        if tool_name in {"memory_derive", "memory_propagate"}
        else [],
        "supersedes_id": arguments.get("old_memory_id")
        if tool_name == "memory_supersede"
        else arguments.get("supersedes_id"),
        "version": (previous_version or 0) + 1,
    }


def _expected_ids(intents: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(_latest_record_intents(intents))


def _latest_record_intents(
    intents: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for intent in sorted(intents, key=lambda item: int(item["sequence"])):
        memory_id = _intent_memory_id(intent)
        if memory_id is not None and intent["tool_name"] in _RECORD_TOOLS:
            latest[memory_id] = intent
    return latest


def _expected_edges(intents: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    edges: list[dict[str, str]] = []
    for target_id, intent in _latest_record_intents(intents).items():
        if intent["tool_name"] in {"memory_derive", "memory_propagate"}:
            edges.extend(
                {"kind": "DERIVED_FROM", "source_id": source_id, "target_id": target_id}
                for source_id in _source_ids(intent)
            )
        elif intent["tool_name"] == "memory_supersede":
            edges.append(
                {
                    "kind": "SUPERSEDES",
                    "source_id": target_id,
                    "target_id": str(intent["arguments"]["old_memory_id"]),
                }
            )
    return sorted(edges, key=lambda edge: (edge["kind"], edge["source_id"], edge["target_id"]))


def _expected_overlays(intents: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "sequence": int(intent["sequence"]),
            "memory_id": str(intent["arguments"]["memory_id"]),
            "target_status": str(intent["arguments"]["target_status"]),
        }
        for intent in sorted(intents, key=lambda item: int(item["sequence"]))
        if intent["tool_name"] == "status_overlay"
    ]


def _replay_staged_records(
    intents: Sequence[Mapping[str, Any]],
    current_version: Callable[[str], int | None],
) -> dict[str, dict[str, Any]]:
    versions: dict[str, int] = {}
    records: dict[str, dict[str, Any]] = {}
    for intent in sorted(intents, key=lambda item: int(item["sequence"])):
        if intent["tool_name"] in _RECORD_TOOLS:
            memory_id = _intent_memory_id(intent)
            assert memory_id is not None
            versions.setdefault(memory_id, current_version(memory_id) or 0)
            record = _new_record(intent, versions[memory_id])
            assert record is not None
            versions[memory_id] = int(record["version"])
            records[memory_id] = record
        elif intent["tool_name"] == "status_overlay":
            memory_id = str(intent["arguments"]["memory_id"])
            versions.setdefault(memory_id, current_version(memory_id) or 0)
            versions[memory_id] += 1
            if memory_id in records:
                records[memory_id]["version"] = versions[memory_id]
                records[memory_id]["target_status"] = str(
                    intent["arguments"]["target_status"]
                )
    return records


class InMemoryTransactionBackend:
    """Deterministic adapter with separate committed and per-transaction state."""

    def __init__(
        self,
        memories: Mapping[str, Mapping[str, Any]] | None = None,
        decision_resolver: Callable[[str], str | None] | None = None,
    ):
        self.committed: dict[str, dict[str, Any]] = {}
        for memory_id, memory in (memories or {}).items():
            normalized = copy.deepcopy(dict(memory))
            normalized.setdefault("memory_id", memory_id)
            normalized.setdefault("value", memory_id)
            normalized.setdefault("status", "active")
            normalized.setdefault("scope", "tenant:user_001")
            normalized.setdefault("agent_id", "agent_model")
            normalized.setdefault("derived_from", [])
            normalized.setdefault("version", 1)
            self.committed[str(memory_id)] = normalized
        self.pending: dict[str, dict[str, Any]] = {}
        self._decision_resolver = decision_resolver or (lambda txn_id: None)
        self._finalized: set[str] = set()

    def bind_decision_resolver(self, resolver: Callable[[str], str | None]) -> None:
        self._decision_resolver = resolver

    def _effective_overlay_status(self, memory_id: str) -> str | None:
        overlays: list[tuple[int, str]] = []
        for txn_id, staged in self.pending.items():
            if txn_id in self._finalized or self._decision_resolver(txn_id) != "COMMITTED":
                continue
            overlays.extend(
                (int(overlay["sequence"]), str(overlay["target_status"]))
                for overlay in staged["overlays"]
                if overlay["memory_id"] == memory_id
            )
        return max(overlays, default=(0, ""))[1] or None

    def _committed_pending(self, memory_id: str) -> dict[str, Any] | None:
        for txn_id, staged in self.pending.items():
            if txn_id in self._finalized or self._decision_resolver(txn_id) != "COMMITTED":
                continue
            record = staged["qdrant"].get(memory_id)
            if record is not None:
                effective = copy.deepcopy(record)
                effective["status"] = effective["target_status"]
                if effective["status"] == "active":
                    effective.pop("target_status", None)
                    effective.pop("txn_id", None)
                    return effective
        return None

    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None:
        pending = self._committed_pending(memory_id)
        if pending is not None:
            return pending
        record = self.committed.get(memory_id)
        effective_status = self._effective_overlay_status(memory_id)
        if record is None or (effective_status or record.get("status")) != "active":
            return None
        return copy.deepcopy(record)

    def search_committed(self, query: str | None = None) -> list[Mapping[str, Any]]:
        ids = set(self.committed)
        for staged in self.pending.values():
            ids.update(staged["qdrant"])
        matches = []
        for memory_id in sorted(ids):
            record = self.read_committed(memory_id)
            if record is not None and (
                query is None
                or query
                in {
                    record.get("memory_id"),
                    record.get("value"),
                    record.get("attribute"),
                }
            ):
                matches.append(record)
        return matches

    def current_version(self, memory_id: str) -> int | None:
        record = self.committed.get(memory_id)
        return int(record.get("version", 1)) if record is not None else None

    def invalidate_committed(self, memory_id: str) -> Mapping[str, Any]:
        record = self.committed.get(memory_id)
        if record is None:
            raise KeyError(memory_id)
        record["status"] = "invalid"
        record["version"] = int(record.get("version", 1)) + 1
        return copy.deepcopy(record)

    def _staged(self, txn_id: str) -> dict[str, Any]:
        return self.pending.setdefault(
            txn_id, {"qdrant": {}, "nodes": {}, "edges": [], "overlays": []}
        )

    def stage_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        phase_hook: PhaseHook | None = None,
    ) -> Mapping[str, Any]:
        staged = self._staged(txn_id)
        staged["qdrant"] = _replay_staged_records(intents, self.current_version)
        for record in staged["qdrant"].values():
            record["txn_id"] = txn_id
        qdrant_evidence = {
            "txn_id": txn_id,
            "memory_ids": sorted(staged["qdrant"]),
        }
        if phase_hook:
            phase_hook("after_qdrant_stage", qdrant_evidence)

        staged["nodes"] = {
            memory_id: copy.deepcopy(record)
            for memory_id, record in staged["qdrant"].items()
        }
        staged["edges"] = _expected_edges(intents)
        staged["overlays"] = _expected_overlays(intents)
        neo4j_evidence = {
            "txn_id": txn_id,
            "memory_ids": sorted(staged["nodes"]),
            "edge_count": len(staged["edges"]),
        }
        if phase_hook:
            phase_hook("after_neo4j_stage", neo4j_evidence)
        return {"qdrant": qdrant_evidence, "neo4j": neo4j_evidence}

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        staged = self.pending.get(txn_id)
        expected_ids = _expected_ids(intents)
        if staged is None:
            return {"status": "absent", "txn_id": txn_id}
        qdrant_ids = sorted(staged["qdrant"])
        node_ids = sorted(staged["nodes"])
        edges = sorted(
            staged["edges"],
            key=lambda edge: (edge["kind"], edge["source_id"], edge["target_id"]),
        )
        status = (
            "complete"
            if qdrant_ids == expected_ids
            and node_ids == expected_ids
            and edges == _expected_edges(intents)
            and staged["overlays"] == _expected_overlays(intents)
            else "partial"
        )
        return {"status": status, "txn_id": txn_id}

    def finalize_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        if txn_id in self._finalized:
            return {"status": "complete", "txn_id": txn_id}
        staged = self.pending.get(txn_id)
        if staged is None:
            return {"status": "absent", "txn_id": txn_id}
        overlays_by_memory: dict[str, list[dict[str, Any]]] = {}
        for overlay in staged["overlays"]:
            overlays_by_memory.setdefault(overlay["memory_id"], []).append(overlay)
        for memory_id, record in staged["qdrant"].items():
            committed = copy.deepcopy(record)
            committed["status"] = committed.pop("target_status")
            committed.pop("txn_id", None)
            self.committed[memory_id] = committed
        for memory_id, overlays in overlays_by_memory.items():
            if memory_id in staged["qdrant"]:
                continue
            record = self.committed.get(memory_id)
            if record is None:
                continue
            for overlay in sorted(overlays, key=lambda item: item["sequence"]):
                record["status"] = overlay["target_status"]
                record["version"] = int(record.get("version", 1)) + 1
        self._finalized.add(txn_id)
        return {"status": "complete", "txn_id": txn_id}

    def cleanup_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        self.pending.pop(txn_id, None)
        return {"status": "clean", "txn_id": txn_id}

    def raw_transaction_state(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        staged = self.pending.get(
            txn_id, {"qdrant": {}, "nodes": {}, "edges": [], "overlays": []}
        )
        visible = [
            memory_id
            for memory_id in sorted(staged["qdrant"])
            if self.read_committed(memory_id) is not None
        ]
        return {
            "qdrant": {
                "read_ok": True,
                "objects": [
                    {"memory_id": memory_id} for memory_id in sorted(staged["qdrant"])
                ],
            },
            "neo4j": {
                "read_ok": True,
                "nodes": [
                    {"memory_id": memory_id} for memory_id in sorted(staged["nodes"])
                ],
                "edges": copy.deepcopy(staged["edges"]),
            },
            "gateway_visible": visible,
        }


class SQLiteStagingTransactionBackend:
    """File-persistent deterministic vector/graph staging adapter."""

    def __init__(
        self,
        path: str | Path,
        memories: Mapping[str, Mapping[str, Any]] | None = None,
        decision_resolver: Callable[[str], str | None] | None = None,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._decision_resolver = decision_resolver or (lambda txn_id: None)
        self._create_schema()
        for memory_id, memory in (memories or {}).items():
            normalized = copy.deepcopy(dict(memory))
            normalized.setdefault("memory_id", memory_id)
            normalized.setdefault("value", memory_id)
            normalized.setdefault("status", "active")
            normalized.setdefault("scope", "tenant:user_001")
            normalized.setdefault("agent_id", "agent_model")
            normalized.setdefault("derived_from", [])
            normalized.setdefault("version", 1)
            self._put_committed(normalized)

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS committed_records (
                memory_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vector_objects (
                txn_id TEXT NOT NULL, memory_id TEXT NOT NULL, payload_json TEXT NOT NULL,
                PRIMARY KEY (txn_id, memory_id)
            );
            CREATE TABLE IF NOT EXISTS graph_nodes (
                txn_id TEXT NOT NULL, memory_id TEXT NOT NULL, payload_json TEXT NOT NULL,
                PRIMARY KEY (txn_id, memory_id)
            );
            CREATE TABLE IF NOT EXISTS graph_edges (
                txn_id TEXT NOT NULL, kind TEXT NOT NULL, source_id TEXT NOT NULL, target_id TEXT NOT NULL,
                PRIMARY KEY (txn_id, kind, source_id, target_id)
            );
            CREATE TABLE IF NOT EXISTS status_overlays (
                txn_id TEXT NOT NULL, sequence INTEGER NOT NULL, memory_id TEXT NOT NULL,
                target_status TEXT NOT NULL, PRIMARY KEY (txn_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS finalized_transactions (
                txn_id TEXT PRIMARY KEY
            );
            """
        )

    def bind_decision_resolver(self, resolver: Callable[[str], str | None]) -> None:
        self._decision_resolver = resolver

    @staticmethod
    def _payload(record: Mapping[str, Any]) -> str:
        return json.dumps(dict(record), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _put_committed(self, record: Mapping[str, Any]) -> None:
        self._connection.execute(
            "INSERT INTO committed_records(memory_id, payload_json) VALUES (?, ?) "
            "ON CONFLICT(memory_id) DO UPDATE SET payload_json=excluded.payload_json",
            (str(record["memory_id"]), self._payload(record)),
        )

    def _overlay_status(self, memory_id: str) -> str | None:
        rows = self._connection.execute(
            "SELECT overlays.txn_id, overlays.target_status FROM status_overlays AS overlays "
            "WHERE overlays.memory_id = ? AND NOT EXISTS ("
            "SELECT 1 FROM finalized_transactions AS finalized "
            "WHERE finalized.txn_id = overlays.txn_id) "
            "ORDER BY overlays.sequence",
            (memory_id,),
        ).fetchall()
        status = None
        for row in rows:
            if self._decision_resolver(str(row["txn_id"])) == "COMMITTED":
                status = str(row["target_status"])
        return status

    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None:
        rows = self._connection.execute(
            "SELECT objects.txn_id, objects.payload_json FROM vector_objects AS objects "
            "WHERE objects.memory_id = ? AND NOT EXISTS ("
            "SELECT 1 FROM finalized_transactions AS finalized "
            "WHERE finalized.txn_id = objects.txn_id) ORDER BY objects.txn_id",
            (memory_id,),
        ).fetchall()
        for row in rows:
            if self._decision_resolver(str(row["txn_id"])) == "COMMITTED":
                record = json.loads(row["payload_json"])
                record["status"] = record.pop("target_status")
                record.pop("txn_id", None)
                if record["status"] == "active":
                    return record
        row = self._connection.execute(
            "SELECT payload_json FROM committed_records WHERE memory_id = ?", (memory_id,)
        ).fetchone()
        if row is None:
            return None
        record = json.loads(row["payload_json"])
        if (self._overlay_status(memory_id) or record.get("status")) != "active":
            return None
        return record

    def search_committed(self, query: str | None = None) -> list[Mapping[str, Any]]:
        ids = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT memory_id FROM committed_records UNION SELECT memory_id FROM vector_objects"
            ).fetchall()
        }
        return [
            record
            for memory_id in sorted(ids)
            for record in [self.read_committed(memory_id)]
            if record is not None
            and (
                query is None
                or query
                in {record.get("memory_id"), record.get("value"), record.get("attribute")}
            )
        ]

    def current_version(self, memory_id: str) -> int | None:
        row = self._connection.execute(
            "SELECT payload_json FROM committed_records WHERE memory_id = ?", (memory_id,)
        ).fetchone()
        return int(json.loads(row[0]).get("version", 1)) if row is not None else None

    def invalidate_committed(self, memory_id: str) -> Mapping[str, Any]:
        row = self._connection.execute(
            "SELECT payload_json FROM committed_records WHERE memory_id = ?", (memory_id,)
        ).fetchone()
        if row is None:
            raise KeyError(memory_id)
        record = json.loads(row[0])
        record["status"] = "invalid"
        record["version"] = int(record.get("version", 1)) + 1
        self._put_committed(record)
        return copy.deepcopy(record)

    def stage_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        phase_hook: PhaseHook | None = None,
    ) -> Mapping[str, Any]:
        records = _replay_staged_records(intents, self.current_version)
        self._connection.execute("DELETE FROM vector_objects WHERE txn_id = ?", (txn_id,))
        for record in records.values():
            record["txn_id"] = txn_id
            self._connection.execute(
                "INSERT INTO vector_objects(txn_id, memory_id, payload_json) VALUES (?, ?, ?) "
                "ON CONFLICT(txn_id, memory_id) DO UPDATE SET payload_json=excluded.payload_json",
                (txn_id, record["memory_id"], self._payload(record)),
            )
        qdrant_evidence = {"txn_id": txn_id, "memory_ids": _expected_ids(intents)}
        if phase_hook:
            phase_hook("after_qdrant_stage", qdrant_evidence)

        self._connection.execute("DELETE FROM graph_nodes WHERE txn_id = ?", (txn_id,))
        self._connection.execute("DELETE FROM graph_edges WHERE txn_id = ?", (txn_id,))
        self._connection.execute("DELETE FROM status_overlays WHERE txn_id = ?", (txn_id,))
        for memory_id, record in records.items():
            self._connection.execute(
                "INSERT INTO graph_nodes(txn_id, memory_id, payload_json) VALUES (?, ?, ?) "
                "ON CONFLICT(txn_id, memory_id) DO UPDATE SET payload_json=excluded.payload_json",
                (txn_id, memory_id, self._payload(record)),
            )
        for edge in _expected_edges(intents):
            self._connection.execute(
                "INSERT OR IGNORE INTO graph_edges(txn_id, kind, source_id, target_id) VALUES (?, ?, ?, ?)",
                (txn_id, edge["kind"], edge["source_id"], edge["target_id"]),
            )
        for overlay in _expected_overlays(intents):
            self._connection.execute(
                "INSERT INTO status_overlays(txn_id, sequence, memory_id, target_status) "
                "VALUES (?, ?, ?, ?)",
                (
                    txn_id,
                    overlay["sequence"],
                    overlay["memory_id"],
                    overlay["target_status"],
                ),
            )
        neo4j_evidence = {
            "txn_id": txn_id,
            "memory_ids": _expected_ids(intents),
            "edge_count": len(_expected_edges(intents)),
        }
        if phase_hook:
            phase_hook("after_neo4j_stage", neo4j_evidence)
        return {"qdrant": qdrant_evidence, "neo4j": neo4j_evidence}

    def verify_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        raw = self.raw_transaction_state(txn_id, intents)
        qdrant_ids = [item["memory_id"] for item in raw["qdrant"]["objects"]]
        node_ids = [item["memory_id"] for item in raw["neo4j"]["nodes"]]
        overlays = [
            {
                "sequence": int(row[0]),
                "memory_id": str(row[1]),
                "target_status": str(row[2]),
            }
            for row in self._connection.execute(
                "SELECT sequence, memory_id, target_status FROM status_overlays "
                "WHERE txn_id = ? ORDER BY sequence",
                (txn_id,),
            ).fetchall()
        ]
        if (
            qdrant_ids == _expected_ids(intents)
            and node_ids == _expected_ids(intents)
            and raw["neo4j"]["edges"] == _expected_edges(intents)
            and overlays == _expected_overlays(intents)
        ):
            status = "complete"
        elif not qdrant_ids and not node_ids and not raw["neo4j"]["edges"] and not overlays:
            status = "absent"
        else:
            status = "partial"
        return {"status": status, "txn_id": txn_id}

    def finalize_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        if self._connection.execute(
            "SELECT 1 FROM finalized_transactions WHERE txn_id = ?", (txn_id,)
        ).fetchone():
            return {"status": "complete", "txn_id": txn_id}
        rows = self._connection.execute(
            "SELECT memory_id, payload_json FROM vector_objects WHERE txn_id = ? ORDER BY memory_id",
            (txn_id,),
        ).fetchall()
        overlays = self._connection.execute(
            "SELECT sequence, memory_id, target_status FROM status_overlays "
            "WHERE txn_id = ? ORDER BY sequence",
            (txn_id,),
        ).fetchall()
        by_memory: dict[str, list[sqlite3.Row]] = {}
        for overlay in overlays:
            by_memory.setdefault(str(overlay["memory_id"]), []).append(overlay)
        for row in rows:
            record = json.loads(row["payload_json"])
            record["status"] = record.pop("target_status")
            record.pop("txn_id", None)
            self._put_committed(record)
        staged_ids = {str(row["memory_id"]) for row in rows}
        for memory_id, memory_overlays in by_memory.items():
            if memory_id in staged_ids:
                continue
            committed_row = self._connection.execute(
                "SELECT payload_json FROM committed_records WHERE memory_id = ?", (memory_id,)
            ).fetchone()
            if committed_row is None:
                continue
            record = json.loads(committed_row[0])
            for overlay in memory_overlays:
                record["status"] = str(overlay["target_status"])
                record["version"] = int(record.get("version", 1)) + 1
            self._put_committed(record)
        self._connection.execute(
            "INSERT OR IGNORE INTO finalized_transactions(txn_id) VALUES (?)", (txn_id,)
        )
        return {"status": "complete", "txn_id": txn_id}

    def cleanup_transaction(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        for table in ("vector_objects", "graph_nodes", "graph_edges", "status_overlays"):
            self._connection.execute(f"DELETE FROM {table} WHERE txn_id = ?", (txn_id,))
        return {"status": "clean", "txn_id": txn_id}

    def raw_transaction_state(
        self, txn_id: str, intents: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        object_ids = [
            str(row[0])
            for row in self._connection.execute(
                "SELECT memory_id FROM vector_objects WHERE txn_id = ? ORDER BY memory_id", (txn_id,)
            ).fetchall()
        ]
        node_ids = [
            str(row[0])
            for row in self._connection.execute(
                "SELECT memory_id FROM graph_nodes WHERE txn_id = ? ORDER BY memory_id", (txn_id,)
            ).fetchall()
        ]
        edges = [
            {"kind": str(row[0]), "source_id": str(row[1]), "target_id": str(row[2])}
            for row in self._connection.execute(
                "SELECT kind, source_id, target_id FROM graph_edges WHERE txn_id = ? "
                "ORDER BY kind, source_id, target_id",
                (txn_id,),
            ).fetchall()
        ]
        return {
            "qdrant": {
                "read_ok": True,
                "objects": [{"memory_id": memory_id} for memory_id in object_ids],
            },
            "neo4j": {
                "read_ok": True,
                "nodes": [{"memory_id": memory_id} for memory_id in node_ids],
                "edges": edges,
            },
            "gateway_visible": [
                memory_id for memory_id in object_ids if self.read_committed(memory_id) is not None
            ],
        }

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class TaskTransactionCoordinator:
    """Coordinate one task transaction using the journal as decision authority."""

    _MUTATING_TOOLS = frozenset(
        {
            "memory_write",
            "memory_derive",
            "memory_propagate",
            "memory_supersede",
            "memory_invalidate",
        }
    )

    def __init__(
        self,
        *,
        journal: TransactionJournal,
        backend: TransactionBackend,
        task_id: str,
        agent_id: str,
        txn_id: str,
        policy_snapshot_provider: PolicySnapshotProvider,
        phase_hook: PhaseHook | None = None,
    ):
        self.journal = journal
        self.backend = backend
        self.task_id = task_id
        self.agent_id = agent_id
        self.txn_id = txn_id
        self.policy_snapshot_provider = policy_snapshot_provider
        self.phase_hook = phase_hook
        self.begin_policy = _normalize_policy(policy_snapshot_provider())
        self.journal.begin(
            txn_id=txn_id,
            task_id=task_id,
            agent_id=agent_id,
            begin_policy_version=self.begin_policy["version"],
        )
        binder = getattr(backend, "bind_decision_resolver", None)
        if binder is not None:
            binder(self._resolve_decision)
        self._events: list[dict[str, Any]] = []
        self._pending_records: dict[str, dict[str, Any]] = {}
        self._pending_status: dict[str, str] = {}
        self._event("begin_txn")

    def _resolve_decision(self, txn_id: str) -> str | None:
        try:
            return self.journal.load(txn_id).decision
        except Exception:
            return None

    def _event(self, kind: str, **fields: Any) -> dict[str, Any]:
        sequence = len(self._events) + 1
        event = {
            "event_id": f"{self.txn_id}:event:{sequence:04d}",
            "kind": kind,
            "step": sequence,
            "agent_id": self.agent_id,
            "txn_id": self.txn_id,
        }
        event.update({key: copy.deepcopy(value) for key, value in fields.items() if value is not None})
        self._events.append(event)
        return event

    def validated_events(self) -> list[dict[str, Any]]:
        return validate_events(self._events)

    def _next_intent_sequence(self) -> int:
        intents = self.journal.intents(self.txn_id)
        return int(intents[-1]["sequence"]) + 1 if intents else 1

    def _append_intent(self, tool_name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        return self.journal.append_intent(
            self.txn_id,
            sequence=self._next_intent_sequence(),
            tool_name=tool_name,
            arguments=arguments,
        )

    def _visible_record(self, memory_id: str, *, record_read: bool = True) -> dict[str, Any] | None:
        if self._pending_status.get(memory_id) not in {None, "active"}:
            return None
        pending = self._pending_records.get(memory_id)
        if pending is not None:
            if self._pending_status.get(memory_id, pending.get("status", "active")) != "active":
                return None
            return copy.deepcopy(pending)
        record = self.backend.read_committed(memory_id)
        if record is None:
            return None
        normalized = copy.deepcopy(dict(record))
        normalized.setdefault("version", 1)
        normalized.setdefault("scope", "tenant:user_001")
        if record_read:
            self.journal.record_read(
                self.txn_id,
                memory_id=memory_id,
                observed_version=int(normalized["version"]),
                scope=str(normalized["scope"]),
            )
        return normalized

    def read(self, memory_id: str) -> dict[str, Any] | None:
        result = self._visible_record(memory_id)
        self._event("memory_read", memory_id=memory_id)
        return result

    def search(self, query: str | None = None) -> list[dict[str, Any]]:
        merged = {str(item["memory_id"]): copy.deepcopy(dict(item)) for item in self.backend.search_committed(query)}
        for memory_id, status in self._pending_status.items():
            if status != "active":
                merged.pop(memory_id, None)
        for memory_id, record in self._pending_records.items():
            if self._pending_status.get(memory_id, record.get("status", "active")) != "active":
                continue
            if query is None or query in {memory_id, record.get("value"), record.get("attribute")}:
                merged[memory_id] = copy.deepcopy(record)
        for memory_id in sorted(merged):
            if memory_id not in self._pending_records:
                self._visible_record(memory_id)
        self._event("memory_search", query=query)
        return [merged[memory_id] for memory_id in sorted(merged)]

    def _require_source(self, memory_id: str) -> dict[str, Any]:
        source = self._visible_record(memory_id)
        if source is None:
            raise TaskTransactionError("source_invalidated", f"source unavailable: {memory_id}")
        return source

    def _require_existing(self, memory_id: str) -> dict[str, Any]:
        pending = self._pending_records.get(memory_id)
        if pending is not None:
            return copy.deepcopy(pending)
        record = self.backend.read_committed(memory_id)
        if record is None:
            raise TaskTransactionError("source_invalidated", f"memory unavailable: {memory_id}")
        return copy.deepcopy(dict(record))

    @staticmethod
    def _policy_action(tool_name: str) -> str:
        return tool_name.removeprefix("memory_")

    @staticmethod
    def _target_id(tool_name: str, arguments: Mapping[str, Any]) -> str | None:
        if tool_name == "memory_supersede":
            return str(arguments["new_memory_id"])
        memory_id = arguments.get("memory_id")
        return str(memory_id) if memory_id is not None else None

    def _apply_accept_policy(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        current_policy: Mapping[str, Any],
    ) -> None:
        policies = (self.begin_policy, current_policy)
        action = self._policy_action(tool_name)
        for policy in policies:
            denied = set(policy["denied_actions"])
            if "write" in denied or action in denied:
                raise TaskTransactionError("policy_revalidation_failed")

        target_id = self._target_id(tool_name, arguments)
        overrides = [
            str(policy["scope_overrides"][target_id])
            for policy in policies
            if target_id is not None and target_id in policy["scope_overrides"]
        ]
        if len(set(overrides)) > 1:
            raise TaskTransactionError("policy_revalidation_failed")
        if overrides:
            arguments["scope"] = overrides[-1]

    def _enforce_source_scope_policy(
        self,
        memory_id: str,
        source: Mapping[str, Any],
        current_policy: Mapping[str, Any],
    ) -> None:
        for policy in (self.begin_policy, current_policy):
            required = policy["scope_overrides"].get(memory_id)
            if required is not None and str(source.get("scope")) != str(required):
                raise TaskTransactionError("policy_revalidation_failed")

    def mutate(self, tool_name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        if self.journal.load(self.txn_id).state != "ACTIVE":
            raise TaskTransactionError("transaction_not_active")
        normalized = copy.deepcopy(dict(arguments))
        normalized.setdefault("agent_id", self.agent_id)
        normalized.setdefault("scope", "tenant:user_001")
        current_policy = _normalize_policy(self.policy_snapshot_provider())
        self._apply_accept_policy(tool_name, normalized, current_policy)
        if tool_name == "memory_propagate":
            normalized["source_ids"] = [str(normalized.pop("source_id"))]
        if tool_name in {"memory_derive", "memory_propagate"}:
            for source_id in normalized.get("source_ids", []):
                source = self._require_source(str(source_id))
                self._enforce_source_scope_policy(
                    str(source_id), source, current_policy
                )
        elif tool_name == "memory_supersede":
            source_id = str(normalized["old_memory_id"])
            source = self._require_source(source_id)
            self._enforce_source_scope_policy(source_id, source, current_policy)
        elif tool_name == "memory_invalidate":
            source_id = str(normalized["memory_id"])
            source = self._require_existing(source_id)
            self._enforce_source_scope_policy(source_id, source, current_policy)

        intent = self._append_intent(tool_name, normalized)
        record = _new_record(intent, self.backend.current_version(_intent_memory_id(intent) or ""))
        if record is not None:
            record["status"] = "active"
            record.pop("target_status", None)
            self._pending_records[str(record["memory_id"])] = record
            self._pending_status[str(record["memory_id"])] = "active"

        if tool_name == "memory_supersede":
            self._append_status_overlay(str(normalized["old_memory_id"]), "superseded")
        elif tool_name == "memory_invalidate":
            for memory_id in self._descendants_including(str(normalized["memory_id"])):
                self._append_status_overlay(memory_id, "invalid")

        event_fields = copy.deepcopy(normalized)
        if tool_name == "memory_propagate":
            event_fields["source_id"] = event_fields["source_ids"][0]
        self._event(tool_name, **event_fields)
        return {"pending": True, "txn_id": self.txn_id}

    def _append_status_overlay(self, memory_id: str, target_status: str) -> None:
        self._append_intent(
            "status_overlay",
            {"memory_id": memory_id, "target_status": target_status},
        )
        self._pending_status[memory_id] = target_status

    def _descendants_including(self, source_id: str) -> list[str]:
        children: dict[str, set[str]] = {}
        for record in self.backend.search_committed():
            for parent in record.get("derived_from", []):
                children.setdefault(str(parent), set()).add(str(record["memory_id"]))
        for record in self._pending_records.values():
            for parent in record.get("derived_from", []):
                children.setdefault(str(parent), set()).add(str(record["memory_id"]))
        expanded: list[str] = []
        queue = [source_id]
        seen: set[str] = set()
        while queue:
            memory_id = queue.pop(0)
            if memory_id in seen:
                continue
            seen.add(memory_id)
            expanded.append(memory_id)
            queue.extend(sorted(children.get(memory_id, set())))
        return expanded

    def _revalidate_policy(
        self,
        intents: Sequence[Mapping[str, Any]],
        read_set: Sequence[Mapping[str, Any]],
    ) -> None:
        source_scopes = {
            str(item["memory_id"]): str(item["scope"]) for item in read_set
        }
        source_scopes.update(
            {
                memory_id: str(intent["arguments"].get("scope", "tenant:user_001"))
                for memory_id, intent in _latest_record_intents(intents).items()
            }
        )
        for intent in intents:
            if intent["tool_name"] == "status_overlay":
                continue
            action = self._policy_action(str(intent["tool_name"]))
            target_id = self._target_id(str(intent["tool_name"]), intent["arguments"])
            current_policy = _normalize_policy(self.policy_snapshot_provider())
            for policy in (self.begin_policy, current_policy):
                denied = set(policy["denied_actions"])
                if "write" in denied or action in denied:
                    raise TaskTransactionError("policy_revalidation_failed")
                required_scope = policy["scope_overrides"].get(target_id)
                if (
                    required_scope is not None
                    and str(intent["arguments"].get("scope")) != str(required_scope)
                ):
                    raise TaskTransactionError("policy_revalidation_failed")
                for source_id in _source_ids(intent):
                    required_source_scope = policy["scope_overrides"].get(source_id)
                    if (
                        required_source_scope is not None
                        and source_scopes.get(source_id) != str(required_source_scope)
                    ):
                        raise TaskTransactionError("policy_revalidation_failed")

    def _revalidate_reads_and_sources(
        self,
        intents: Sequence[Mapping[str, Any]],
        read_set: Sequence[Mapping[str, Any]],
    ) -> None:
        source_ids = {source_id for intent in intents for source_id in _source_ids(intent)}
        reads = {str(item["memory_id"]): item for item in read_set}
        for memory_id, observed in reads.items():
            current_raw = getattr(self.backend, "committed", {}).get(memory_id)
            current = current_raw or self.backend.read_committed(memory_id)
            if memory_id in source_ids and (
                current is None or current.get("status", "active") != "active"
            ):
                raise TaskTransactionError("source_invalidated")
            if current is None:
                raise TaskTransactionError("read_version_changed")
            if memory_id in source_ids and str(current.get("scope", "tenant:user_001")) != str(observed["scope"]):
                raise TaskTransactionError("source_scope_changed")
            if int(current.get("version", 1)) != int(observed["observed_version"]):
                raise TaskTransactionError("read_version_changed")

    def _revalidate_graph(self, intents: Sequence[Mapping[str, Any]]) -> None:
        graph: dict[str, set[str]] = {}
        for record in self.backend.search_committed():
            graph[str(record["memory_id"])] = {str(source) for source in record.get("derived_from", [])}
        for memory_id, record in getattr(self.backend, "committed", {}).items():
            graph[str(memory_id)] = {str(source) for source in record.get("derived_from", [])}
        for memory_id, intent in _latest_record_intents(intents).items():
            graph[memory_id] = (
                set(_source_ids(intent))
                if intent["tool_name"] in {"memory_derive", "memory_propagate"}
                else set()
            )

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(memory_id: str) -> None:
            if memory_id in visiting:
                raise TaskTransactionError("provenance_cycle")
            if memory_id in visited:
                return
            visiting.add(memory_id)
            for source_id in graph.get(memory_id, set()):
                visit(source_id)
            visiting.remove(memory_id)
            visited.add(memory_id)

        for memory_id in sorted(graph):
            visit(memory_id)

    def _record_backend_phase(self, phase: str, evidence: Mapping[str, Any]) -> None:
        logical = {
            "after_qdrant_stage": "qdrant_staged",
            "after_neo4j_stage": "neo4j_staged",
        }[phase]
        self.journal.record_backend_receipt(
            self.txn_id,
            backend="qdrant" if phase == "after_qdrant_stage" else "neo4j",
            operation_key=self.txn_id,
            phase=logical,
            evidence=evidence,
        )
        self.journal.record_phase(self.txn_id, logical, evidence)
        if self.phase_hook:
            self.phase_hook(phase, evidence)

    def _phase(self, logical: str, hook: str, evidence: Mapping[str, Any]) -> None:
        self.journal.record_phase(self.txn_id, logical, evidence)
        if self.phase_hook:
            self.phase_hook(hook, evidence)

    def _abort_before_decision(self, code: str, intents: Sequence[Mapping[str, Any]]) -> None:
        record = self.journal.load(self.txn_id)
        if record.decision == "COMMITTED":
            return
        if record.decision != "ABORTED":
            self.journal.decide(self.txn_id, "ABORTED")
            self.journal.record_phase(self.txn_id, "abort_decided", {"code": code})
            self._event("abort", reason=code)
        self.backend.cleanup_transaction(self.txn_id, intents)
        self.journal.record_phase(self.txn_id, "cleanup_complete", {"status": "clean"})

    @staticmethod
    def _ordered_phases() -> list[str]:
        return [
            "prepare_recorded",
            "qdrant_staged",
            "neo4j_staged",
            "stage_verified",
            "commit_decided",
            "finalize_complete",
        ]

    def commit(self) -> dict[str, Any]:
        record = self.journal.load(self.txn_id)
        if record.state == "ABORTED":
            raise TaskTransactionError("transaction_aborted")
        if record.state == "COMMITTED":
            frozen = self.journal.frozen_snapshot(self.txn_id)
            intents = frozen["intents"] if frozen is not None else self.journal.intents(self.txn_id)
            try:
                self.backend.finalize_transaction(self.txn_id, intents)
                self._phase("finalize_complete", "after_finalize", {"status": "complete"})
            except Exception as exc:
                raise TaskTransactionError("commit_decided_response_lost") from exc
            return {
                "txn_id": self.txn_id,
                "decision": "COMMITTED",
                "phases": self._ordered_phases(),
            }

        intents = self.journal.intents(self.txn_id)
        try:
            frozen = self.journal.frozen_snapshot(self.txn_id)
            if frozen is None:
                frozen = self.journal.freeze(self.txn_id)
            intents = frozen["intents"]
            read_set = frozen["read_set"]
            self._revalidate_policy(intents, read_set)
            self._revalidate_reads_and_sources(intents, read_set)
            self._revalidate_graph(intents)
            self.journal.prepare(self.txn_id)
            self._phase(
                "prepare_recorded",
                "after_prepare",
                {"intent_count": len(intents)},
            )
            self.backend.stage_transaction(
                self.txn_id, intents, phase_hook=self._record_backend_phase
            )
            verification = self.backend.verify_transaction(self.txn_id, intents)
            status = verification.get("status")
            if status == "partial" or status == "absent":
                raise TaskTransactionError("backend_stage_incomplete")
            if status != "complete":
                raise TaskTransactionError("backend_state_unknown")
            self._phase("stage_verified", "after_stage_verify", verification)
        except TaskTransactionError as exc:
            self._abort_before_decision(exc.code, intents)
            raise
        except Exception as exc:
            self._abort_before_decision("backend_state_unknown", intents)
            raise TaskTransactionError("backend_state_unknown") from exc

        try:
            self.journal.decide(self.txn_id, "COMMITTED")
        except Exception as exc:
            decision_after = self.journal.load(self.txn_id)
            if decision_after.decision != "COMMITTED":
                self._abort_before_decision("commit_decision_failed", intents)
                raise TaskTransactionError("commit_decision_failed") from exc
        self.journal.record_phase(self.txn_id, "commit_decided", {"decision": "COMMITTED"})
        self._event("commit")
        try:
            if self.phase_hook:
                self.phase_hook("after_commit_decision", {"decision": "COMMITTED"})
            self.backend.finalize_transaction(self.txn_id, intents)
            self._phase("finalize_complete", "after_finalize", {"status": "complete"})
        except Exception as exc:
            raise TaskTransactionError("commit_decided_response_lost") from exc
        return {
            "txn_id": self.txn_id,
            "decision": "COMMITTED",
            "phases": self._ordered_phases(),
        }

    def abort(self, code: str = "task_aborted") -> dict[str, Any]:
        record = self.journal.load(self.txn_id)
        if record.decision == "COMMITTED":
            return {"txn_id": self.txn_id, "decision": "COMMITTED"}
        self._abort_before_decision(code, self.journal.intents(self.txn_id))
        return {"txn_id": self.txn_id, "decision": "ABORTED", "code": code}


class TaskTransactionGateway:
    """Model-facing memory gateway for one automatically-begun task transaction."""

    def __init__(
        self,
        *,
        journal: TransactionJournal,
        backend: TransactionBackend,
        task_id: str,
        agent_id: str,
        txn_id: str,
        policy_snapshot_provider: PolicySnapshotProvider,
        phase_hook: PhaseHook | None = None,
    ):
        self.revoked_actions: set[str] = set()
        self._policy_snapshot_provider = policy_snapshot_provider
        self.coordinator = TaskTransactionCoordinator(
            journal=journal,
            backend=backend,
            task_id=task_id,
            agent_id=agent_id,
            txn_id=txn_id,
            policy_snapshot_provider=self._policy_snapshot,
            phase_hook=phase_hook,
        )

    def _policy_snapshot(self) -> Mapping[str, Any]:
        provided = self._policy_snapshot_provider()
        if not isinstance(provided, Mapping):
            return provided
        snapshot = copy.deepcopy(dict(provided))
        if not self.revoked_actions:
            return snapshot
        denied_actions = snapshot.get("denied_actions", [])
        if isinstance(denied_actions, list):
            snapshot["denied_actions"] = sorted(
                set(str(action) for action in denied_actions) | self.revoked_actions
            )
        version = snapshot.get("version")
        if isinstance(version, int) and not isinstance(version, bool):
            snapshot["version"] = version + len(self.revoked_actions)
        return snapshot

    def revoke_policy(self, action: str, *, trigger_event: Mapping[str, Any]) -> None:
        self.revoked_actions.add(action)

    def call(self, name: str, arguments: Mapping[str, Any]) -> Any:
        if not isinstance(arguments, Mapping):
            raise TaskTransactionError("invalid_tool_arguments")
        try:
            if name == "memory_read":
                return self.coordinator.read(str(arguments["memory_id"]))
            if name == "memory_search":
                query = arguments.get("query")
                return self.coordinator.search(str(query) if query is not None else None)
            if name in self.coordinator._MUTATING_TOOLS:
                return self.coordinator.mutate(name, arguments)
        except (KeyError, TypeError, ValueError) as exc:
            raise TaskTransactionError("invalid_tool_arguments") from exc
        raise TaskTransactionError("unknown_tool")

    def commit(self) -> dict[str, Any]:
        return self.coordinator.commit()

    def abort(self, code: str = "task_aborted") -> dict[str, Any]:
        return self.coordinator.abort(code)

    def validated_events(self) -> list[dict[str, Any]]:
        return self.coordinator.validated_events()
