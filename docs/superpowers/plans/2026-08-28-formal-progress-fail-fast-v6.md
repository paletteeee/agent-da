# Formal Progress and Fail-Fast v6 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add exact sanitized repetition progress, first-failure termination, bounded Neo4j operations, and orphan-safe lifecycle handling, then restart the full TxnMem provenance matrix under a fresh v6 identity.

**Architecture:** A dedicated `txnmem_provenance_progress` module owns the canonical event schema, monotonic state machine, atomic snapshot store, and pipe drain loop. The attested runner emits one event after each completed repetition through a collector-owned one-way FD; the collector validates and writes a root-owned snapshot outside the candidate tree. Protected execution separately forces formal eligibility, applies the frozen request timeout to both backends, and uses Linux parent-death plus validated process-group cleanup.

**Tech Stack:** Python 3.10/3.12 standard library, Neo4j Python driver 5.28.1, Qdrant HTTP, Toxiproxy, nftables, Docker Compose, `unittest`, Git.

**Spec:** `docs/superpowers/specs/2026-08-28-formal-progress-fail-fast-v6-design.md`

## Global Constraints

- Keep the registered matrix exactly 15 cells, 450 repetitions, and 14,400 operation samples: graph sizes `100/1000/10000`, concurrencies `1/2/4/8/16`, 30 repetitions, and 32 samples per repetition.
- A progress event may contain only schema, safe binding/config hashes, matrix position/counts, sequence, phase, and status; never include credentials, address, username, nonce, run ID, namespace, endpoint, private path, raw log, payload, database value, PID, or raw latency.
- Store progress outside the candidate tree as root-owned mode `0600`; progress is operational telemetry and never part of candidate sealing, promotion, topology evidence, or paper claims.
- Keep direct diagnostic CLI behavior permissive; only the protected collector path sets `require_formal_eligibility=True`.
- Use the same finite positive `request_timeout_seconds` for Qdrant HTTP, Neo4j connection, connection acquisition, and transaction/query timeout. The frozen formal value remains 30 seconds.
- A collector/pipe/progress protocol failure is fail-closed and prevents promotion.
- The collector must keep the nft guard active until the measured child is proven stopped, then deactivate it and prove zero residue.
- Preserve all v5 private material, candidate roots, and service volumes; do not resume, promote, overwrite, or delete them.
- v6 requires a new source commit, registration commit, run identity, nonce, compose project, and empty backend volumes.
- Every code change follows RED → GREEN and ends in a focused commit before the next task.
- Never print or commit credentials, server coordinates, private paths, raw logs, raw payloads, database contents, or private run identities.

---

## File Structure

- Create `src/txnmem_provenance_progress.py`: progress schemas, canonical decoding, monotonic transition validation, atomic snapshot storage, safe reader, and pipe drainer.
- Create `tests/test_txnmem_provenance_progress.py`: exhaustive protocol, storage, and drainer contract tests.
- Modify `src/txnmem_provenance_performance.py`: separate `require_formal_eligibility` from publication mode.
- Modify `src/txnmem_experiment.py`: expose internal-only progress and eligibility hooks and translate cell-local updates into global matrix progress.
- Modify `src/txnmem_provenance_runner.py`: parse reserved progress descriptors/binding, emit canonical progress lines, and close safely.
- Modify `src/txnmem_vector_graph_backend.py`: enforce driver-supported Neo4j connection/acquisition/transaction/query timeouts.
- Modify `src/txnmem_provenance_execution_collector.py`: create/drain the progress pipe, own the snapshot, derive the safe binding, handle interruption, and terminate a validated process group.
- Modify `src/txnmem_topology_attestation.py`: validate command-manifest v3 progress and timeout policy fields.
- Modify `src/txnmem_formal_controller.py`: require the progress module and expose a sanitized read-only progress command.
- Modify `src/txnmem_formal_smoke.py`: exercise the protected progress channel and bounded real-backend execution.
- Modify `scripts/install_formal_provenance_runtime.sh`: require the new source module in the approved closure.
- Create `scripts/read_formal_provenance_progress.sh`: invoke only the protected progress reader and output sanitized JSON.
- Modify focused tests in `tests/test_txnmem_provenance_performance.py`, `tests/test_cli_outputs.py`, `tests/test_txnmem_vector_graph_backend.py`, `tests/test_txnmem_provenance_execution_collector.py`, `tests/test_txnmem_topology_attestation.py`, `tests/test_txnmem_formal_controller.py`, and `tests/test_txnmem_formal_smoke.py`.

---

### Task 1: Canonical Progress Event and Monotonic State Machine

**Files:**
- Create: `src/txnmem_provenance_progress.py`
- Create: `tests/test_txnmem_provenance_progress.py`

**Interfaces:**
- Produces: `ProgressProtocolError(RuntimeError)`.
- Produces: `FORMAL_MATRIX_CELLS: tuple[tuple[int, int], ...]` ordered by graph size then concurrency.
- Produces: `decode_progress_line(payload: bytes) -> dict[str, Any]`.
- Produces: `build_progress_event(*, run_binding_sha256: str, config_sha256: str, cell_index: int, graph_size: int, concurrency: int, repetition_index: int, completed_repetitions: int, completed_samples: int, update_sequence: int) -> dict[str, Any]`.
- Produces: `canonical_progress_line(event: Mapping[str, Any]) -> bytes`.
- Produces: `FormalProgressState(run_binding_sha256: str, config_sha256: str).consume(event: Mapping[str, Any]) -> dict[str, Any]`.
- Consumes: no project module other than standard-library types; this keeps runner and collector on the same small protocol implementation.

- [ ] **Step 1: Write failing event-schema and transition tests**

Add tests that construct the first two valid events and assert exact canonical bytes and exact normalized fields:

```python
def event(*, cell=1, repetition=1, completed=1, sequence=1):
    graph_size, concurrency = FORMAL_MATRIX_CELLS[cell - 1]
    return build_progress_event(
        run_binding_sha256="a" * 64,
        config_sha256="b" * 64,
        cell_index=cell,
        graph_size=graph_size,
        concurrency=concurrency,
        repetition_index=repetition,
        completed_repetitions=completed,
        completed_samples=completed * 32,
        update_sequence=sequence,
    )

def test_valid_progress_is_canonical_and_monotonic(self):
    state = FormalProgressState("a" * 64, "b" * 64)
    first = state.consume(decode_progress_line(canonical_progress_line(event())))
    second_event = event(repetition=2, completed=2, sequence=2)
    second = state.consume(decode_progress_line(canonical_progress_line(second_event)))
    self.assertEqual(first["completed_repetitions"], 1)
    self.assertEqual(second["completed_samples"], 64)
```

Add table-driven rejection tests for unknown/missing fields, duplicate JSON keys, bool-as-int, non-finite numbers, uppercase/short hashes, non-canonical spacing/order, NUL, empty line, no trailing newline, UTF-8 failure, truncated JSON, 4097-byte records, sequence/repetition/sample duplicate, rollback, jump, wrong matrix cell, wrong graph/concurrency, early cell switch, and count overflow.

Add a full-loop test that feeds all 450 events and asserts the last state is cell 15, repetition 30, completed repetitions 450, completed samples 14,400, and sequence 450.

- [ ] **Step 2: Run the new test module and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress -v`

Expected: import failure for `txnmem_provenance_progress`.

- [ ] **Step 3: Implement the exact closed schema and state transitions**

Define these constants and dataclass shape:

```python
PROGRESS_EVENT_SCHEMA = "txnmem-provenance-progress-event-v1"
PROGRESS_SNAPSHOT_SCHEMA = "txnmem-provenance-progress-snapshot-v1"
MAX_PROGRESS_LINE_BYTES = 4096
FORMAL_MATRIX_CELLS = tuple(
    (graph_size, concurrency)
    for graph_size in (100, 1000, 10000)
    for concurrency in (1, 2, 4, 8, 16)
)
EVENT_FIELDS = frozenset({
    "schema", "run_binding_sha256", "config_sha256", "phase",
    "cell_index", "cell_count", "graph_size", "concurrency",
    "repetition_index", "repetition_count", "completed_repetitions",
    "total_repetitions", "completed_samples", "total_samples",
    "update_sequence", "status",
})
```

`decode_progress_line` must require one final newline, decode strict UTF-8, reject duplicate keys with `object_pairs_hook`, reject constants through `parse_constant`, compare `json.dumps(..., sort_keys=True, separators=(",", ":"), allow_nan=False).encode()` to the bytes before the newline, and then require an exact mapping.

`build_progress_event` must fill fixed values `phase="measurement"`, `cell_count=15`, `repetition_count=30`, `total_repetitions=450`, `total_samples=14400`, and `status="running"`, then run the same field/type validator used by `consume`.

`FormalProgressState.consume` must calculate the one legal successor from its current sequence. For successor `n`:

```python
expected_cell_index = (n - 1) // 30 + 1
expected_repetition_index = (n - 1) % 30 + 1
expected_graph_size, expected_concurrency = FORMAL_MATRIX_CELLS[expected_cell_index - 1]
expected_samples = n * 32
```

Reject every event that differs from those values or either constructor hash. Return a deep-copied normalized mapping so a caller cannot mutate internal state.

- [ ] **Step 4: Run protocol tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress -v`

Expected: all progress protocol tests pass.

- [ ] **Step 5: Commit the protocol unit**

```bash
git add src/txnmem_provenance_progress.py tests/test_txnmem_provenance_progress.py
git commit -m "feat: define formal progress protocol"
```

---

### Task 2: Root-Owned Atomic Snapshot and Pipe Drainer

**Files:**
- Modify: `src/txnmem_provenance_progress.py`
- Modify: `tests/test_txnmem_provenance_progress.py`

**Interfaces:**
- Consumes: `FormalProgressState.consume` and `decode_progress_line` from Task 1.
- Produces: `ProgressSnapshotStore(path: Path, *, expected_uid: int, expected_gid: int)` with `write_starting(run_binding_sha256, config_sha256)`, `write_running(event)`, `write_terminal(status, reason_class)`, and `read_view()`.
- Produces: `canonical_snapshot_line(snapshot: Mapping[str, Any]) -> bytes` for the already validated snapshot/view closure.
- Produces: `ProgressPipeDrainer(descriptor: int, state: FormalProgressState, store: ProgressSnapshotStore)` with `start()`, `finish(timeout_seconds: float)`, `abort()`, and `failure`.
- Produces: on-disk snapshots ending in one newline; `read_view()` derives `last_update_age_seconds` from validated file metadata and never returns the raw timestamp.

- [ ] **Step 1: Write failing atomic-storage tests**

Use a temporary directory and current UID/GID. Assert that `write_running` creates a regular mode-`0600` file with one canonical JSON line, `read_view()` returns only the snapshot field closure, and a second write atomically replaces the first complete document.

Patch `os.replace` to fail and assert the previous snapshot remains byte-for-byte readable. Add rejection cases for symlink target, symlink parent, non-regular target, hard-link count greater than one, wrong owner, wrong mode, malformed persisted JSON, and terminal transitions outside `{completed, blocked, interrupted}`.

The persisted snapshot must include `last_update_age_seconds=0`; `read_view()` replaces it with `max(0, floor(time.time() - st_mtime))` after revalidating the same inode/owner/mode and returns no timestamp/path. `write_starting` writes sequence/repetition/sample counts of zero while identifying the first scheduled cell `(100, 1)`; running snapshots require the event's positive counts.

- [ ] **Step 2: Write failing pipe-drainer tests**

Create a real `os.pipe()`, start `ProgressPipeDrainer`, write two canonical lines, close the writer, and assert `finish(2.0)` returns the second snapshot. Add cases for malformed line, writer closing mid-record, reader abort, oversized line, and EOF with no events. Assert every protocol error is retained in `drainer.failure` and no background thread remains alive.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress -v`

Expected: failures because `ProgressSnapshotStore` and `ProgressPipeDrainer` do not exist.

- [ ] **Step 4: Implement symlink-safe atomic storage and bounded draining**

Use parent-directory FDs and these operations:

```python
temporary_fd = os.open(
    temporary_name,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
    0o600,
    dir_fd=parent_fd,
)
os.write(temporary_fd, canonical_snapshot_bytes)
os.fsync(temporary_fd)
os.replace(temporary_name, target_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
os.fsync(parent_fd)
```

Loop on short writes, cap every pipe record at 4096 bytes, never hold more than one record plus one byte, and use a daemon thread only so a broken test process can exit. `finish` must join within the supplied finite positive timeout and raise `ProgressProtocolError("progress drainer did not stop")` if still alive. `abort` closes the read descriptor once and is idempotent.

Snapshot exact keys are the event keys with `schema` replaced by the snapshot schema, plus `last_update_age_seconds` and optional terminal `terminal_reason_class`; terminal reasons are restricted to `completed`, `formal_eligibility_failed`, `backend_timeout`, `progress_protocol_failed`, `collector_interrupted`, and `resource_cleanup_failed`. `canonical_snapshot_line` validates that closure before encoding and is never used to accept an unvalidated event.

- [ ] **Step 5: Run storage/drainer tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress -v`

Expected: all tests pass and no drainer thread is left alive.

- [ ] **Step 6: Commit the storage unit**

```bash
git add src/txnmem_provenance_progress.py tests/test_txnmem_provenance_progress.py
git commit -m "feat: persist sanitized formal progress"
```

---

### Task 3: Bound Every Neo4j Formal Operation

**Files:**
- Modify: `src/txnmem_vector_graph_backend.py:304-357, 546-1319, 1335-1366`
- Modify: `src/txnmem_provenance_performance.py:1956-2047`
- Modify: `tests/test_txnmem_vector_graph_backend.py:784-914`
- Modify: `tests/test_txnmem_provenance_performance.py:430-470`

**Interfaces:**
- Produces: `_Neo4jBoltClient(..., request_timeout_seconds: float = 15.0)`.
- Produces: `_Neo4jBoltClient.timeout_policy() -> dict[str, float]` with exact keys `connection_seconds`, `connection_acquisition_seconds`, and `transaction_query_seconds`.
- Consumes: the validated `request_timeout_seconds` already accepted by `VectorGraphMemoryBackend` and `_ReusableVectorGraphBackendFactory`.

- [ ] **Step 1: Extend the fake Neo4j module and write timeout propagation tests**

Provide both `GraphDatabase` and `Query` in the fake module. Capture driver kwargs, `Query(text, timeout=...)`, and `session.begin_transaction(timeout=...)`. Instantiate with `request_timeout_seconds=30.0` and assert:

```python
self.assertEqual(observed_driver["connection_timeout"], 30.0)
self.assertEqual(observed_driver["connection_acquisition_timeout"], 30.0)
self.assertEqual(observed_transaction_timeout, 30.0)
self.assertTrue(all(query.timeout == 30.0 for query in observed_queries))
self.assertEqual(client.timeout_policy(), {
    "connection_seconds": 30.0,
    "connection_acquisition_seconds": 30.0,
    "transaction_query_seconds": 30.0,
})
```

Add bool, zero, negative, NaN, and infinity constructor rejection tests. Add factory tests asserting `_ReusableVectorGraphBackendFactory` passes the exact value to `_Neo4jBoltClient` and `VectorGraphMemoryBackend` does the same when it owns its clients.

- [ ] **Step 2: Run focused timeout tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_vector_graph_backend.VectorGraphMemoryBackendTests.test_neo4j_driver_and_writes_disable_implicit_retry tests.test_txnmem_provenance_performance -v`

Expected: assertions fail because the Neo4j client lacks the timeout parameter and driver bindings.

- [ ] **Step 3: Implement finite validation and driver-supported timeouts**

Import `GraphDatabase, Query` from the locked Neo4j package. Validate with exact type checks and store `self.request_timeout_seconds`.

Build the driver with:

```python
driver_config = {
    "auth": tuple(auth),
    "max_transaction_retry_time": 0.0,
    "connection_timeout": timeout,
    "connection_acquisition_timeout": timeout,
}
```

Add:

```python
def _bounded_query(self, text: str):
    return self._Query(text, timeout=self.request_timeout_seconds)
```

Use `session.begin_transaction(timeout=self.request_timeout_seconds)` for explicit transactions. Wrap every direct `session.run(text, ...)` in `_bounded_query(text)`; explicit `tx.run` calls remain covered by the transaction timeout. Keep max transaction retry time at zero.

Pass `request_timeout_seconds` at both existing `_Neo4jBoltClient` construction sites. Do not add retries around timeout exceptions.

- [ ] **Step 4: Run focused backend and performance tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_vector_graph_backend tests.test_txnmem_provenance_performance -v`

Expected: all tests pass; fake driver observes all three 30-second bindings.

- [ ] **Step 5: Commit bounded backend I/O**

```bash
git add src/txnmem_vector_graph_backend.py src/txnmem_provenance_performance.py tests/test_txnmem_vector_graph_backend.py tests/test_txnmem_provenance_performance.py
git commit -m "fix: bound formal Neo4j operations"
```

---

### Task 4: Wire the Attested Progress Pipe Through Collector, Runner, and Experiment

**Files:**
- Modify: `src/txnmem_experiment.py:860, 1318-1533`
- Modify: `src/txnmem_provenance_runner.py:1-180`
- Modify: `src/txnmem_provenance_execution_collector.py:107-205, 1180-1451, 4583-4985`
- Modify: `src/txnmem_topology_attestation.py:129-187, 669-779`
- Modify: `src/txnmem_formal_controller.py:37-54`
- Modify: `scripts/install_formal_provenance_runtime.sh:120-170`
- Modify: `tests/test_cli_outputs.py:299-394`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:1001-1102, 2636-2785, 3465-3565`
- Modify: `tests/test_txnmem_topology_attestation.py`
- Modify: `tests/test_txnmem_formal_controller.py`

**Interfaces:**
- Consumes: progress protocol/store/drainer from Tasks 1-2 and backend timeout policy from Task 3.
- Produces: `txnmem_experiment.main(argv=None, *, _progress_callback=None, _require_formal_eligibility=False) -> int`.
- Produces: runner-reserved `TXNMEM_PROVENANCE_PROGRESS_FD` and `TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256`.
- Produces: command manifest `txnmem-provenance-command-manifest-v3` with `progress_environment_variable`, `progress_binding_environment_variable`, `progress_binding_sha256`, `progress_channel_required`, and `backend_timeout_policy`.
- Produces: `_GatedCandidate` owns a `ProgressPipeDrainer` and exposes `finish_progress(timeout: float)`.

- [ ] **Step 1: Write failing experiment-global-progress tests**

Patch `run_matrix_cell` to invoke its callback twice in each of two cells. Call `txnmem_experiment.main(..., _progress_callback=events.append)` and assert the callback receives global fields, not run IDs or namespaces:

```python
self.assertEqual(events[0], {
    "cell_index": 1, "cell_count": 2, "graph_size": 2,
    "concurrency": 1, "repetition_index": 1, "repetition_count": 2,
    "completed_repetitions": 1, "total_repetitions": 4,
    "completed_samples": 4, "total_samples": 16,
    "update_sequence": 1,
})
self.assertEqual(events[-1]["completed_repetitions"], 4)
self.assertEqual(events[-1]["update_sequence"], 4)
```

Assert a normal CLI caller cannot set either internal hook through argv or config.

- [ ] **Step 2: Write failing runner FD tests**

Provide gate, ready, completion, and progress pipes. Patch experiment main to call its supplied callback once and return zero. Assert the progress reader gets one canonical v1 line with the supplied safe binding/config hash and no forbidden keys. Assert missing/duplicate/equal descriptors, malformed binding hash, `EPIPE`, and short write return a nonzero runner status and do not write a completion receipt.

- [ ] **Step 3: Write failing collector/channel and manifest tests**

Extend the gated-child fixture to write one progress line. Assert `_start_gated_candidate` rejects caller-provided reserved progress env, passes only the write FD, closes the parent copy, and `finish_progress(2.0)` returns the validated snapshot.

Assert the progress path resolves to `workspace.root / "progress.json"`, is outside `workspace.candidate`, and is absent from `_seal_candidate_tree` file counts.

Upgrade command-manifest fixtures to v3 and assert exact timeout policy:

```python
"backend_timeout_policy": {
    "qdrant_request_seconds": 30.0,
    "neo4j_connection_seconds": 30.0,
    "neo4j_connection_acquisition_seconds": 30.0,
    "neo4j_transaction_query_seconds": 30.0,
}
```

Add mutation tests for one changed timeout, absent/extra progress field, progress binding mismatch, old v2 schema, and source closure missing `src/txnmem_provenance_progress.py`.

- [ ] **Step 4: Run all new integration tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_provenance_execution_collector tests.test_txnmem_topology_attestation tests.test_txnmem_formal_controller -v`

Expected: failures for missing internal hooks, progress descriptors, and manifest v3 fields.

- [ ] **Step 5: Implement experiment progress translation**

Change only the Python-call signature; do not add argparse flags. Validate `_require_formal_eligibility` as an exact bool and `_progress_callback` as callable-or-None.

Enumerate cells starting at 1. In `record_progress`, calculate global counts from completed-cell bases and emit the exact mapping tested in Step 1 after updating the existing blocked-report counters. Set `total_repetitions=sum(cell["repetitions"] for cell in cells)` and `total_samples=sum(cell["repetitions"] * cell["operations_per_type"] * 4 for cell in cells)`.

- [ ] **Step 6: Implement runner canonical emission**

For `provenance-performance`, require and pop both progress environment values before readiness. Obtain `config_sha256` from `formal_matrix_config_sha256()` in the immutable export, build each event with `build_progress_event`, write `canonical_progress_line(event)` through `_write_all`, and pass the emitter plus `_require_formal_eligibility=True` to experiment main. Close progress FD in `finally` before completion FD. Keep `provenance-smoke` compatible by making the progress pair optional only in smoke mode.

- [ ] **Step 7: Implement collector ownership and binding**

Derive the progress binding from canonical bytes of:

```python
{
    "schema": "txnmem-provenance-progress-binding-v1",
    "source_manifest_sha256": source_hash,
    "argv_sha256": argv_hash,
    "config_file_sha256": expected_config_hash,
    "run_id_sha256": hashlib.sha256(run_id.encode()).hexdigest(),
    "candidate_root_sha256": hashlib.sha256(str(candidate).encode()).hexdigest(),
}
```

Create the pipe only when `require_progress=True`; add the write FD to `pass_fds`, write the `starting` snapshot and start the drainer before gate release, and pass `workspace.root / "progress.json"` with expected root UID/GID. After candidate exit, require drainer EOF and terminal `completed` before sealing. If child exits nonzero or progress fails, write a safe `blocked` terminal and prevent sealing.

- [ ] **Step 8: Implement command-manifest v3 and source closure**

Add the progress module to collector/controller/install required source sets. Validate v3 with exact-key equality, recompute the safe binding from execution-bound fields, require all four timeout values to be the same finite positive number, and require the two progress env variable names exactly. Do not accept v2 as v3.

- [ ] **Step 9: Run focused integration tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_provenance_execution_collector tests.test_txnmem_topology_attestation tests.test_txnmem_formal_controller -v`

Expected: all focused tests pass, the runner emits exactly one line per completed repetition, and manifest mutations fail closed.

- [ ] **Step 10: Commit the attested channel**

```bash
git add src/txnmem_experiment.py src/txnmem_provenance_runner.py src/txnmem_provenance_execution_collector.py src/txnmem_topology_attestation.py src/txnmem_formal_controller.py scripts/install_formal_provenance_runtime.sh tests/test_cli_outputs.py tests/test_txnmem_provenance_execution_collector.py tests/test_txnmem_topology_attestation.py tests/test_txnmem_formal_controller.py
git commit -m "feat: stream attested formal progress"
```

---

### Task 5: Fail Fast on the First Formally Ineligible Repetition

**Files:**
- Modify: `src/txnmem_provenance_performance.py:809-1132`
- Modify: `src/txnmem_experiment.py:1423-1457`
- Modify: `tests/test_txnmem_provenance_performance.py:344-385, 561-705`
- Modify: `tests/test_cli_outputs.py:299-394`

**Interfaces:**
- Produces: `run_matrix_cell(..., require_formal_eligibility: bool = False)`.
- Consumes: `_require_formal_eligibility=True` from the protected runner path added in Task 4.
- Guarantees: an ineligible repetition is neither appended nor emitted as completed; direct diagnostic mode still records it.

- [ ] **Step 1: Write failing protected-diagnostic fail-fast tests**

Use a backend whose first repetition has `isolation_verified=False` but otherwise closes state. Call with `formal=False, require_formal_eligibility=True` and assert `ProvenancePerformanceError` before the second backend is created and before any progress callback fires.

Add a third-repetition failure fixture and assert exactly two progress events. Call the same fixture with `require_formal_eligibility=False` and assert diagnostic rows are retained. Reject non-bool values for the new argument.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_performance -v`

Expected: signature or behavior failure because protected diagnostic mode is not enforced.

- [ ] **Step 3: Implement one eligibility gate variable**

After exact bool validation, define:

```python
enforce_formal_eligibility = formal or require_formal_eligibility
```

Use it for service availability, isolation, empty namespace, preload closure, zero retry policy, exact retry metric, and final `eligible` checks. Keep report field `formal_requested=bool(formal)` unchanged. Append samples/repetition and call progress only after the final eligibility gate passes.

Pass `_require_formal_eligibility` from `txnmem_experiment.main` into every `run_matrix_cell` call.

- [ ] **Step 4: Run performance and CLI tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_performance tests.test_cli_outputs -v`

Expected: protected diagnostic mode stops on the first invalid repetition; ordinary diagnostics retain current behavior.

- [ ] **Step 5: Commit fail-fast semantics**

```bash
git add src/txnmem_provenance_performance.py src/txnmem_experiment.py tests/test_txnmem_provenance_performance.py tests/test_cli_outputs.py
git commit -m "fix: fail fast on formal ineligibility"
```

---

### Task 6: Parent-Death Signal, Validated Process-Group Termination, and Guard-Last Cleanup

**Files:**
- Modify: `src/txnmem_provenance_execution_collector.py:170-238, 1342-1451, 2109-2163, 4532-4580, 4663-4983`
- Modify: `src/txnmem_provenance_runner.py`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:1001-1102, 1156-1225, 3069-3100`
- Modify: `tests/test_txnmem_formal_smoke.py:1297-1371`

**Interfaces:**
- Produces: `_set_parent_death_signal(parent_pid: int, *, prctl=None, getppid=os.getppid)`.
- Produces: `_prepare_formal_child_process(parent_pid: int, uid: int, gid: int)` used as the one `preexec_fn`.
- Produces: `_CollectorInterruption` and a self-pipe-backed `_SignalLatch` whose read FD is accepted by `wait_with_receipt`.
- Produces: `_GatedCandidate.bind_process_identity(start_identity: str)` and `terminate_validated_group(term_seconds=5.0, kill_seconds=5.0)`.

- [ ] **Step 1: Write failing parent-death tests**

Inject a fake `prctl` and assert calls `(1, SIGTERM, 0, 0, 0)` for `PR_SET_PDEATHSIG`, followed by a parent PID equality check before privilege drop. Assert non-Linux/unavailable prctl, changed parent PID, zero/negative parent PID, and failed syscall all block launch.

- [ ] **Step 2: Write failing process-group cleanup tests**

Create fake process/identity readers. Assert cleanup order is monitor abort → validated process-group SIGTERM/wait → optional SIGKILL/wait → progress close → guard deactivate. Assert SIGKILL is not sent when SIGTERM succeeds. Assert PID/start-identity/PGID/session mismatch sends no signal and records a hard cleanup failure.

Update the prior cleanup test expected event order from `monitor, guard, child` to `monitor, child, guard`.

- [ ] **Step 3: Write failing signal-latch integration test**

Start a child blocked after gate release, trigger the latch, and assert `wait_with_receipt(interrupt_fd=...)` raises `_CollectorInterruption`; the surrounding `finally` stops the process group, closes progress, and removes the fake guard. Assert repeated latch/cleanup calls are idempotent.

- [ ] **Step 4: Run lifecycle tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector tests.test_txnmem_formal_smoke -v`

Expected: missing parent-death, latch, identity binding, and group cleanup APIs.

- [ ] **Step 5: Implement Linux parent-death before privilege drop**

Use `ctypes.CDLL(None, use_errno=True).prctl` with operation 1. Call it before `_set_no_new_privileges`, `setgroups`, `setgid`, and `setuid`. Immediately reject `os.getppid() != parent_pid` to close the parent-death setup race.

- [ ] **Step 6: Implement interruption wakeup and validated group termination**

The signal handler may only set an event and write one byte to a nonblocking self-pipe. Add the latch read FD to the existing `select.select` in `wait_with_receipt`; when readable, drain one byte and raise `_CollectorInterruption`.

Before `os.killpg`, re-read `/proc/<pid>/stat` and `/proc/<pid>/cmdline`, require the stored start identity, require `pgid == pid` and `sid == pid`, then signal the exact group. Revalidate before SIGKILL. Keep the guard active until this method proves the child has exited.

Runner SIGTERM handling sets a stop flag, closes clients through existing `finally`, closes the progress FD, and returns nonzero without publishing partial candidate bytes.

- [ ] **Step 7: Run lifecycle tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector tests.test_txnmem_formal_smoke -v`

Expected: all lifecycle tests pass with guard-last cleanup and no child/thread residue.

- [ ] **Step 8: Commit lifecycle safety**

```bash
git add src/txnmem_provenance_execution_collector.py src/txnmem_provenance_runner.py tests/test_txnmem_provenance_execution_collector.py tests/test_txnmem_formal_smoke.py
git commit -m "fix: prevent orphaned formal runners"
```

---

### Task 7: Sanitized Progress Reader and Protected Same-Path Smoke

**Files:**
- Modify: `src/txnmem_provenance_progress.py`
- Modify: `src/txnmem_provenance_execution_collector.py`
- Modify: `src/txnmem_formal_controller.py:390-470`
- Modify: `src/txnmem_formal_smoke.py:400-1150`
- Modify: `src/txnmem_provenance_runner.py`
- Create: `scripts/read_formal_provenance_progress.sh`
- Modify: `tests/test_txnmem_provenance_progress.py`
- Modify: `tests/test_txnmem_formal_controller.py`
- Modify: `tests/test_txnmem_formal_smoke.py`
- Modify: `tests/test_real_backend_script.py`

**Interfaces:**
- Produces: protected controller action `progress` taking `--run-id` and `--authorization-nonce`, deriving the registered workspace internally, and writing one sanitized canonical JSON object to stdout.
- Produces: `scripts/read_formal_provenance_progress.sh RUN_ID AUTHORIZATION_NONCE` with an isolated environment.
- Produces: formal smoke receipt v2 proving `progress_monotonic`, `formal_fail_fast`, `backend_timeout_bounded`, `interruption_cleanup`, and `candidate_unpublished` booleans.

- [ ] **Step 1: Write failing safe-reader tests**

Create a valid private snapshot and assert the controller output keys equal the snapshot closure, `last_update_age_seconds` is nonnegative, and output contains none of a seeded credential/address/path/run-ID/nonce set. Assert an unregistered run/nonce, candidate path injection, symlink snapshot, wrong owner/mode, malformed file, or unexpected key returns blocked without raw exception text.

Test the shell script text for `/usr/bin/env -i`, the protected controller path, exact argument count, and absence of database/log commands.

- [ ] **Step 2: Write failing same-path smoke contract tests**

Mock the real-service boundaries but keep the actual progress protocol. Require the smoke orchestration to run four isolated child scenarios:

1. normal: a two-repetition prefix of the first formal cell produces sequences 1 and 2;
2. ineligible: first repetition blocks before a completed event;
3. timeout: a bounded backend operation returns blocked within configured smoke timeout plus cleanup grace;
4. interrupt: termination leaves child/controller/guard counts zero and no candidate files.

Assert the v2 receipt contains only booleans, fixed schema, and safe counts. Each scenario must use a separate smoke identity and never be accepted by the formal promotion validator.

- [ ] **Step 3: Run progress-reader and smoke tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_real_backend_script -v`

Expected: missing protected progress action/script and smoke v2 fields.

- [ ] **Step 4: Implement read-only protected progress action**

Validate controller context and registered nonce exactly as measurement does, derive the workspace with `_require_derived_candidate_root`, call `ProgressSnapshotStore.read_view`, and print only `canonical_snapshot_line(view)`. The action must not import a database driver, open a candidate file, or read logs.

The shell wrapper must isolate environment to `LANG`, `LC_ALL`, and `PYTHONDONTWRITEBYTECODE`, then exec the protected controller. It must not echo either argument.

- [ ] **Step 5: Implement protected real-service smoke scenarios**

Reuse the existing root health, topology, route, guard, and immutable-runner setup. Run the normal two-repetition scenario first and validate sequences. For timeout, use a smoke-only finite timeout smaller than the injected delay; do not alter formal config. For interruption, signal the validated process group through collector cleanup rather than directly killing a PID. Always stop the child before guard deactivation.

Write receipt `txnmem-provenance-formal-smoke-v2` only if all booleans are true. The smoke output remains diagnostic and outside candidate/promotion directories.

- [ ] **Step 6: Run safe-reader and smoke tests and verify GREEN**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_real_backend_script -v`

Expected: all tests pass; reader output is closed and smoke contract rejects every mutation.

- [ ] **Step 7: Commit monitoring and smoke**

```bash
git add src/txnmem_provenance_progress.py src/txnmem_provenance_execution_collector.py src/txnmem_formal_controller.py src/txnmem_formal_smoke.py src/txnmem_provenance_runner.py scripts/read_formal_provenance_progress.sh tests/test_txnmem_provenance_progress.py tests/test_txnmem_formal_controller.py tests/test_txnmem_formal_smoke.py tests/test_real_backend_script.py
git commit -m "feat: verify formal progress before restart"
```

---

### Task 8: Local Verification, Independent Review, Remote Smoke, and Fresh v6 Restart

**Files:**
- Modify after private registration: `src/txnmem_topology_attestation.py:457-470`
- Modify after private registration: `tests/test_txnmem_topology_attestation.py:77-116`
- Create after successful smoke: sanitized smoke artifact under the repository's existing approved evidence directory only if it passes artifact audit.
- Do not modify: v5 private run material, v5 candidate, or v5 Docker volumes.

**Interfaces:**
- Consumes: all code and tests from Tasks 1-7.
- Produces: reviewed implementation commit, fresh v6 launch-registration commit, protected-controller install from that exact commit, successful same-path smoke, and one newly launched full v6 matrix.
- Produces after the matrix succeeds: the existing material → sanitized topology v6 → independent registration → promotion pipeline; measurement is not rerun during promotion.

- [ ] **Step 1: Run static and focused verification**

Run:

```bash
PYTHONPATH=src python3 -m py_compile src/txnmem_provenance_progress.py src/txnmem_provenance_runner.py src/txnmem_provenance_execution_collector.py src/txnmem_provenance_performance.py src/txnmem_vector_graph_backend.py src/txnmem_experiment.py
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_provenance_execution_collector tests.test_txnmem_provenance_performance tests.test_txnmem_vector_graph_backend tests.test_txnmem_topology_attestation tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_cli_outputs tests.test_real_backend_script -v
git diff --check
```

Expected: zero failures, zero unexpected skips, and no diff-check output.

- [ ] **Step 2: Run the complete local suite and audits**

Run: `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py' -v`

Run the repository's existing claim and artifact audit tests as part of that suite; confirm both report zero findings. Record only aggregate test counts and safe commit hashes.

- [ ] **Step 3: Request two-stage independent review**

First review spec compliance against `docs/superpowers/specs/2026-08-28-formal-progress-fail-fast-v6-design.md`; second review code quality, race safety, FD closure, process identity validation, schema exactness, and sensitive-data closure. Resolve every Critical or Important finding with a new failing test and focused commit, then rerun Step 1.

- [ ] **Step 4: Push the reviewed implementation branch**

```bash
git status --short
git push -u origin codex/provenance-progress-v6
```

Expected: clean worktree and remote branch updated to the reviewed implementation commit.

- [ ] **Step 5: Create and register a fresh private v6 identity**

On the configured server, use the established private registrar to create a new run identity and mode-`0600` nonce outside the repository. Copy only the resulting run-hash → nonce-hash pair into `FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN`; add a test asserting that exact pair. Never display or commit the underlying identity, nonce, private path, or server coordinates.

Commit and push:

```bash
git add src/txnmem_topology_attestation.py tests/test_txnmem_topology_attestation.py
git commit -m "evidence: authorize fresh formal performance run v6"
git push
```

- [ ] **Step 6: Rebuild exact registration commit and prepare clean services**

Use the remote-server-development workflow to fetch the pushed branch, verify a clean checkout at the registration commit, and reinstall the protected controller from that commit. Stop only the validated prior containers; do not run `down -v`. Start the same digest-pinned images under a new compose project with new named volumes, then verify all three services healthy and the old v5 volumes still present.

- [ ] **Step 7: Run protected same-path smoke**

Invoke `scripts/run_cross_host_provenance_performance.sh smoke` with a fresh private smoke output. Verify the v2 smoke receipt is complete, the safe progress reader reports monotonic counts, timeout/fail-fast scenarios terminate in bounds, and runner/controller/nft residue counts are zero. A failed smoke consumes its identity and requires a new identity before retry.

- [ ] **Step 8: Launch exactly one full v6 matrix**

Invoke the established `measure` action once with the registered v6 private inputs and new candidate location. Immediately verify launcher/controller/runner and nft guard counts are each one, completion is absent, and the protected progress reader is either `starting` or a valid monotonic `running` snapshot. Do not query databases or read raw logs/payloads while it runs.

- [ ] **Step 9: Monitor without duplicate launch**

At each heartbeat, read only launcher/controller state, completion existence, runner count, nft guard count, and sanitized progress view. If it is still healthy, leave it untouched. If completion appears, follow the existing strict 15/450/14,400 material validation, topology v6 review/registration, promotion without rerun, sanitized synchronization, local byte-for-byte statistics recomputation, paper/DOCX/PDF update, visual QA, full tests, independent review, main merge, and push.

- [ ] **Step 10: Preserve failure evidence if v6 blocks**

If v6 fails, retain its one-time raw private evidence and terminal sanitized classification, prove runner/controller/nft counts are zero, and diagnose from the bounded failure class plus progress position. Never reuse that run identity or nonce. Create v7 only after a new failing test and reviewed corrective commit establish a specific fix.
