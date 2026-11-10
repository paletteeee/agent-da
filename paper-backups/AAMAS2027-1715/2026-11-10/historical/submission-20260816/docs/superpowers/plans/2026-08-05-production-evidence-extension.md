# TxnMem Production Evidence Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce auditable large-scale native workflow/memory evidence for τ-bench, AppWorld, and LoCoMo, add official evaluator boundaries, run real Qdrant/Neo4j backend and network/performance experiments, and update the paper without overstating the results.

**Architecture:** Extend the existing merged benchmark+memory runner with a batch command that fixes task manifests, episode-level backend isolation, official evaluator output, native event validation, independent oracle replay, and task-level confidence intervals. Add a separate real-service adapter using Qdrant HTTP, Neo4j Bolt, and Toxiproxy; keep SQLite as the persistence baseline and record service/fault/timing evidence in a separate result namespace. Run code and long experiments on `/data/txnmem` of the configured remote GPU server; commit only sanitized aggregates and reproducibility metadata locally.

**Tech Stack:** Python 3.11+, existing standard-library TxnMem modules, Qwen2.5-7B OpenAI-compatible endpoint, official τ-bench/AppWorld/LoCoMo runtimes, SQLite, Qdrant HTTP, Neo4j Bolt, Toxiproxy, Docker Compose, `unittest`, JSON/CSV reports, bundled DOCX renderer.

## Global Constraints

- Keep `reference_outcome()` independent from TxnMem variants and model responses.
- Treat task or conversation as the statistical unit; never use API/event row count as sample count.
- Keep raw prompts, model responses, tool arguments, conversation content, memory payloads, database files, and raw traces on the remote server only.
- Use `trace_ground_truth_native: true` only for actual memory-tool events emitted by the backend connector; projection replay remains separately labeled.
- Official evaluator output, TxnMem oracle output, event-contract output, and backend consistency output must remain separate fields.
- Missing runtime, evaluator, credentials, Docker, or service health must produce a machine-readable `blocked` report, never a synthetic success.
- Every performance report must set `production_latency_claim: false`.
- Preserve existing local/remote user changes and never use `git reset --hard`, `git checkout --`, or `rsync --delete`.
- Git push is conditional on a remote URL supplied by the user; do not invent or infer a repository target.

---

### Task 1: Add task-level official evaluator and batch-report schemas

**Files:**
- Modify: `src/txnmem_benchmark_bridge.py`
- Modify: `src/txnmem_real_experiment.py`
- Modify: `src/txnmem_statistics.py`
- Modify: `src/txnmem_experiment.py`
- Create: `tests/test_public_batch_reporting.py`
- Modify: `tests/test_benchmark_bridge.py`
- Modify: `tests/test_txnmem_statistics.py`

**Interfaces:**
- `BenchmarkEnvAdapter.evaluate(run_report) -> dict[str, Any]` continues to return the official result, but every adapter must also expose `official_evaluator_status` as `available`, `blocked`, or `error`.
- Add `run_benchmark_batch(manifest, model, out_dir, backend_factory, adapter_factory, repetitions=1) -> dict[str, Any]` in `src/txnmem_real_experiment.py`; it returns sanitized task summaries, official aggregate metrics, native event counts, oracle match, and 95% intervals.
- Add `aggregate_official_results(task_summaries, dataset) -> dict[str, Any]` in `src/txnmem_statistics.py`; it uses task/conversation-level denominators and `binomial_interval()`.
- Add CLI command `benchmark-native-batch` with `--benchmark`, `--manifest`, `--memory-backend`, `--out-dir`, `--repetitions`, `--endpoint`, `--model`, `--appworld-root`, `--appworld-apps`, and `--locomo-evaluator-command`.

- [ ] **Step 1: Write failing tests for official status and task-level denominators**

```python
def test_official_aggregate_uses_tasks_not_event_rows():
    rows = [
        {"task_id": "a", "official": {"success": True}, "native_event_count": 100},
        {"task_id": "b", "official": {"success": False}, "native_event_count": 1},
    ]
    aggregate = aggregate_official_results(rows, "appworld")
    assert aggregate["successes"] == 1
    assert aggregate["trials"] == 2
    assert aggregate["event_count"] == 101
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `PYTHONPATH=src python3 -m unittest tests.test_public_batch_reporting tests.test_txnmem_statistics -v`

Expected: FAIL because `aggregate_official_results` and the batch command are not yet defined.

- [ ] **Step 3: Implement the batch runner and normalized official status**

Use the existing `run_benchmark_agent()`, `evaluate_native_trace()`, `SQLiteInstrumentedMemoryBackend`, and `sanitize_run_report()`. Add a `task_evaluator` object to every task summary with `status`, `success` when available, `score` when numeric, and `error` when not available. Count `official_successes` only from official evaluator fields, not from `status == completed`.

- [ ] **Step 4: Add AppWorld tuned configuration boundary**

Make `AppWorldAdapter` accept an explicit `app_names` tuple and preserve the official `environment.evaluate()` result as `success`, `pass_count`, and `total_count`. Add a `--appworld-apps` value to the batch manifest metadata so baseline and tuned runs are auditable and comparable.

- [ ] **Step 5: Add LoCoMo evaluator command boundary**

Add `--locomo-evaluator-command` as a JSON argv array. The runner checks every executable path before the first task; when absent, it returns `blocked_official_qa_evaluator` and still records native events/oracle results. When present, it writes a sanitized prediction/annotation manifest to a remote-only temporary directory, invokes the command, validates a JSON result containing `question_count`, `correct_count`, and `score`, then removes the temporary prediction file.

- [ ] **Step 6: Run focused tests and the CLI help check**

Run: `PYTHONPATH=src python3 -m unittest tests.test_public_batch_reporting tests.test_benchmark_bridge tests.test_txnmem_statistics -v`

Run: `PYTHONPATH=src python3 src/txnmem_experiment.py benchmark-native-batch --help`

Expected: all focused tests pass and help lists the batch/evaluator arguments.

- [ ] **Step 7: Commit the batch/evaluator layer**

```bash
git add src/txnmem_benchmark_bridge.py src/txnmem_real_experiment.py src/txnmem_statistics.py src/txnmem_experiment.py tests/test_public_batch_reporting.py tests/test_benchmark_bridge.py tests/test_txnmem_statistics.py
git commit -m "feat: add task-level native benchmark aggregation"
```

### Task 2: Build fixed large-scale manifests and remote batch runner

**Files:**
- Modify: `src/txnmem_benchmark_manifests.py`
- Create: `configs/native_scale.json`
- Create: `scripts/run_native_scale.sh`
- Create: `tests/test_native_scale_manifest.py`
- Modify: `README.md`

**Interfaces:**
- `build_native_scale_manifest(benchmark, source, limit, seed=17, split="test") -> dict[str, Any]` returns a stable ordered task manifest with `manifest_hash`, `task_count`, and `task_level_split`.
- `scripts/run_native_scale.sh` accepts `--endpoint`, `--model`, `--tau-tasks`, `--appworld-tasks`, `--locomo-tasks`, `--out-dir`, and `--repetitions`; it never swallows a nonzero batch status.

- [ ] **Step 1: Write failing manifest tests**

Test deterministic ordering, fixed limits of 50/20/10, manifest hash stability, no train/holdout task overlap, and rejection of duplicate task IDs.

- [ ] **Step 2: Run the manifest tests to verify failure**

Run: `PYTHONPATH=src python3 -m unittest tests.test_native_scale_manifest -v`

Expected: FAIL because the scale manifest function does not yet exist.

- [ ] **Step 3: Implement manifest hashing and split metadata**

Hash canonical JSON after removing raw prompt/context fields. Store `source_sha256`, runtime version fields, task IDs, seeds, and split grouping key; do not store user content.

- [ ] **Step 4: Implement the remote batch shell entry point**

Use `$ROOT/.venv/bin/python`, create separate output directories `tau_bench`, `appworld`, and `locomo`, select SQLite per episode, pass task-specific app schemas, and write a top-level `native_scale_summary.json`. Do not use `|| true`; a blocked evaluator is represented in JSON while infrastructure failures exit nonzero.

- [ ] **Step 5: Run local manifest tests and dry-run help**

Run: `PYTHONPATH=src python3 -m unittest tests.test_native_scale_manifest -v`

Run: `bash scripts/run_native_scale.sh --help`

- [ ] **Step 6: Commit the scale runner**

```bash
git add src/txnmem_benchmark_manifests.py configs/native_scale.json scripts/run_native_scale.sh tests/test_native_scale_manifest.py README.md
git commit -m "feat: add reproducible native benchmark scale runner"
```

### Task 3: Implement real vector/graph backend with idempotent rollback

**Files:**
- Create: `src/txnmem_vector_graph_backend.py`
- Create: `tests/test_txnmem_vector_graph_backend.py`
- Create: `infra/real_backend/docker-compose.yml`
- Create: `infra/real_backend/README.md`
- Modify: `scripts/setup_remote_deps.sh`
- Modify: `requirements-remote.txt`

**Interfaces:**
- `VectorGraphMemoryBackend(db_namespace, qdrant_url, neo4j_uri, neo4j_auth, proxy_requester=None)` implements the existing `write`, `read`, `search`, `derive`, `propagate`, `supersede`, `invalidate`, `snapshot`, `validated_events`, and `close` methods.
- `VectorGraphMemoryBackend.healthcheck() -> dict[str, Any]` returns Qdrant/Neo4j availability and version metadata.
- `VectorGraphMemoryBackend.metrics() -> dict[str, Any]` returns request counts, retries, rollback count, error count, and per-operation timing samples.
- `infra/real_backend/docker-compose.yml` starts Qdrant, Neo4j Community, and Toxiproxy on fixed service-network names; service versions and image digests are captured by the remote runner.

- [ ] **Step 1: Write failing fake-client tests**

Cover write/search/read, derive with source IDs, supersede, invalidation, duplicate request id, graph failure after vector write, compensation delete, and absence of a committed event after rollback.

- [ ] **Step 2: Run focused backend tests to verify failure**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_vector_graph_backend -v`

Expected: FAIL because the backend class and fake-client seam do not yet exist.

- [ ] **Step 3: Implement deterministic embeddings and request idempotency**

Use a dependency-free fixed-dimension hashing embedding for backend experiments; document that it is a storage/retrieval fixture, not a semantic embedding model. Every mutation receives a stable idempotency key derived from namespace, operation, memory ID, and source IDs.

- [ ] **Step 4: Implement Qdrant and Neo4j adapters**

Use HTTP requests for Qdrant collections/upsert/search/delete and the Neo4j Python driver for nodes/edges. Write vector first, graph second, and compensate the vector write if graph commit fails. Emit one canonical committed event only after both stores succeed; failed operations emit sanitized error/rollback metadata.

- [ ] **Step 5: Add service health and dependency setup**

Add `qdrant-client`, `neo4j`, and the remote service commands to `requirements-remote.txt` and `scripts/setup_remote_deps.sh`. Keep local unit tests on fake clients; do not require Docker for the default local test suite.

- [ ] **Step 6: Run focused tests and static checks**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_vector_graph_backend -v`

Run: `python3 -m py_compile src/txnmem_vector_graph_backend.py`

- [ ] **Step 7: Commit the real backend layer**

```bash
git add src/txnmem_vector_graph_backend.py tests/test_txnmem_vector_graph_backend.py infra/real_backend/docker-compose.yml infra/real_backend/README.md scripts/setup_remote_deps.sh requirements-remote.txt
git commit -m "feat: add vector graph memory backend adapter"
```

### Task 4: Add deterministic network fault controller and backend performance runner

**Files:**
- Create: `src/txnmem_backend_performance.py`
- Create: `src/txnmem_service_faults.py`
- Create: `tests/test_txnmem_backend_performance.py`
- Create: `tests/test_txnmem_service_faults.py`
- Modify: `src/txnmem_performance.py`
- Modify: `src/txnmem_experiment.py`

**Interfaces:**
- `FaultScenario(name, service, trigger_operation, action, seed)` is an immutable fault specification.
- `run_fault_matrix(backend_factory, scenarios, workload, repetitions) -> dict[str, Any]` returns consistency and failure metrics by scenario.
- `benchmark_backend(backend_factory, workload_sizes=(50, 200, 1000), repetitions=30) -> dict[str, Any]` returns p50/p95/p99, throughput, request/retry/error counts, and `production_latency_claim: false`.
- CLI command `backend-performance` accepts `--backend`, `--service-url`, `--fault-matrix`, `--events`, `--repetitions`, and `--out-dir`.

- [ ] **Step 1: Write failing fault and percentile tests**

Test normal, delay, timeout, connection drop, retry-success, p50/p95/p99 ordering, no partial commit, and explicit abort classification.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_backend_performance tests.test_txnmem_service_faults -v`

- [ ] **Step 3: Implement request-count trigger and Toxiproxy controller**

The controller records service, operation, request ordinal, fault action, seed, recovery action, and whether retry succeeded. It does not use wall-clock randomness to decide whether a fault fires.

- [ ] **Step 4: Implement backend-only workload and timing aggregation**

Run fixed event workloads of 50, 200, and 1000 events; warm up once; collect at least 30 timed repetitions per condition; compute percentile intervals from raw timing samples on the remote host and commit only aggregate rows.

- [ ] **Step 5: Implement model-in-the-loop timing separation**

Wrap the existing `run_benchmark_agent()` so each report contains `model_seconds`, `backend_seconds`, `official_evaluator_seconds`, and `total_seconds`. The model timing and backend timing are never added together and then reported as backend latency.

- [ ] **Step 6: Run local fake-service tests and CLI help**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_backend_performance tests.test_txnmem_service_faults -v`

Run: `PYTHONPATH=src python3 src/txnmem_experiment.py backend-performance --help`

- [ ] **Step 7: Commit fault/performance code**

```bash
git add src/txnmem_backend_performance.py src/txnmem_service_faults.py tests/test_txnmem_backend_performance.py tests/test_txnmem_service_faults.py src/txnmem_performance.py src/txnmem_experiment.py
git commit -m "feat: add backend fault and performance experiments"
```

### Task 5: Add remote service smoke and synchronize the implementation

**Files:**
- Create: `scripts/run_real_backend_smoke.sh`
- Create: `scripts/run_remote_evidence.sh`
- Modify: `scripts/setup_remote_deps.sh`
- Modify: `README.md`
- Test: remote `/data/txnmem` working tree and service namespace

**Interfaces:**
- `scripts/run_real_backend_smoke.sh` checks Docker, starts the pinned service stack, runs health checks, performs one write/read/derive/provenance/reopen cycle, then writes a sanitized `real_backend_smoke.json`.
- `scripts/run_remote_evidence.sh` checks the remote model endpoint, runtime imports, evaluator command availability, service health, and manifest hashes before launching long jobs through `tmux` or `nohup` with a log path.

- [ ] **Step 1: Inspect remote identity and working tree**

Run remotely: `hostname`, `pwd`, `whoami`, `nvidia-smi`, `git -C /data/txnmem status --short`, and `git -C /data/txnmem log -1 --oneline`. Preserve any unrelated remote changes.

- [ ] **Step 2: Dry-run a selected-file sync**

Use `rsync -azn --itemize-changes` for the committed project tree to `/data/txnmem/`; exclude `.env`, credentials, raw traces, model files, SQLite files, and external benchmark data. Review that only code/config/docs changes are selected.

- [ ] **Step 3: Transfer and run dependency setup**

Run the non-destructive sync, then `bash /data/txnmem/scripts/setup_remote_deps.sh --root /data/txnmem`. Record package versions and official runtime import checks.

- [ ] **Step 4: Start Qdrant/Neo4j/Toxiproxy and run service smoke**

Run `bash /data/txnmem/scripts/run_real_backend_smoke.sh`; require successful health checks, idempotent reopen, graph edge visibility, and zero partial-commit violations.

- [ ] **Step 5: Commit only sanitized remote status back locally**

Use `rsync -azn` to verify code parity, then copy only aggregate JSON/CSV/environment metadata into the local `results/remaining_tasks/` tree; do not copy raw traces or databases.

### Task 6: Run large native benchmark sampling and official evaluators

**Files:**
- Use: `scripts/run_native_scale.sh`
- Use: `configs/native_scale.json`
- Create: `results/remaining_tasks/native_scale/`
- Create: `results/remaining_tasks/native_scale/scale_summary.json`
- Create: `docs/remaining_tasks/native_scale_report_zh.md`

- [ ] **Step 1: Generate and hash the fixed manifests remotely**

Generate τ-bench 50-task, AppWorld 20-task, and LoCoMo 10-conversation manifests with seed 17; record task-level train/holdout grouping and source/runtime hashes.

- [ ] **Step 2: Run a one-task preflight for each benchmark**

Require the official runtime import, SQLite reopen, canonical event validation, independent oracle replay, and official evaluator status before starting the batch. Stop that benchmark with a blocked report if any prerequisite fails.

- [ ] **Step 3: Run the τ-bench batch**

Run 50 fixed airline tasks with the scripted user boundary, Qwen2.5-7B endpoint, SQLite backend, and official reward evaluator. Preserve the official reward separately from TxnMem oracle match.

- [ ] **Step 4: Run the AppWorld baseline/tuned batches**

Run the same 20 task IDs with baseline and task-specific app-schema configurations; reset the official environment for every task; call the official evaluator; classify failures as model/tool schema/task reset/evaluator/runtime.

- [ ] **Step 5: Run the LoCoMo batch and QA evaluator**

Run all 10 conversations in chronological session order; write remote-only predictions/annotations; invoke the configured official QA evaluator; report question-level and conversation-level denominators separately.

- [ ] **Step 6: Optional secondary repetitions**

If the preflight and primary batches complete, repeat the fixed manifests with seed offsets 100 and 200. Report these as secondary repetitions, not additional unique tasks.

- [ ] **Step 7: Validate the aggregate before copying it locally**

Require `trace_ground_truth_native`, official evaluator status, oracle match, task count, event count, 95% intervals, model ID, runtime hashes, and `raw_reports_committed: false`. Reject any aggregate containing prompt, response, arguments, content, value, or memory payload fields.

### Task 7: Run real backend fault and performance matrix

**Files:**
- Use: `scripts/run_real_backend_smoke.sh`
- Use: `src/txnmem_backend_performance.py`
- Create: `results/remaining_tasks/real_backend_performance/`
- Create: `docs/remaining_tasks/real_backend_performance_zh.md`

- [ ] **Step 1: Run normal-service backend-only matrix**

Run SQLite baseline and Qdrant/Neo4j backend at 50/200/1000 events, 30 repetitions per workload/operation condition, with one warm-up. Save sanitized timing aggregates and environment metadata.

- [ ] **Step 2: Run delay/timeout/drop/retry matrix**

Activate one deterministic Toxiproxy scenario at a time, record request ordinal and recovery action, and verify no partial commit or provenance closure violation.

- [ ] **Step 3: Run fixed model-in-the-loop comparison**

Use the native benchmark manifest subset that completed in Task 6; record model and backend timing separately and include official task result, TxnMem oracle, retry/error, and backend health status.

- [ ] **Step 4: Generate the performance report**

Write p50/p95/p99, throughput, error/retry/abort, payload bytes, service versions, and `production_latency_claim: false`. Do not include raw service logs or database files.

### Task 8: Update paper, status, and visual QA

**Files:**
- Modify: `build_txnmem_paper_draft.py`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `docs/official_trace_replay_zh.md`
- Modify: `results/remaining_tasks/final_status.json`
- Modify: `docs/remaining_tasks_implementation_zh.md`
- Render: `outputs/TxnMem_论文初稿.docx`

- [ ] **Step 1: Add result tables with explicit boundaries**

Add separate tables for official benchmark score, native event/contract/oracle evidence, real backend consistency, and remote timing. Label projection replay and native workflow separately.

- [ ] **Step 2: Regenerate the DOCX**

Run `python3 build_txnmem_paper_draft.py` from the workspace using the bundled workspace runtime.

- [ ] **Step 3: Run structural and accessibility audits**

Run the heading/section audit and `a11y_audit.py`; require high/medium/low findings equal to zero.

- [ ] **Step 4: Render and inspect every PNG**

Run `scripts/render_docx_with_bundled_libs.sh outputs/TxnMem_论文初稿.docx --output_dir outputs/TxnMem_论文初稿_render --emit_pdf`; inspect every generated page at high resolution for clipping, overflow, broken tables, missing glyphs, and footer/header errors.

- [ ] **Step 5: Commit paper and evidence updates**

```bash
git add build_txnmem_paper_draft.py docs/formal_paper_task_status_zh.md docs/official_trace_replay_zh.md results/remaining_tasks/final_status.json docs/remaining_tasks_implementation_zh.md outputs/TxnMem_论文初稿.docx
git commit -m "docs: update paper with native and backend evidence"
```

### Task 9: Final verification and Git remote handoff

**Files:**
- Modify: `.gitignore` only if a newly introduced raw artifact path is not already excluded
- Inspect: `git remote -v`
- Inspect: `git status --short`

- [ ] **Step 1: Run the full local suite**

Run: `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py'`

Expected: zero failures; dependency-only skips must be listed in the final report.

- [ ] **Step 2: Validate all committed JSON aggregates**

Run: `python3 -m json.tool` on every committed JSON result and a script that rejects raw-content keys and requires `production_latency_claim: false` for performance outputs.

- [ ] **Step 3: Check repository cleanliness and commit history**

Run: `git diff --check`, `git status --short`, and `git log --oneline -8`. Confirm no raw traces, databases, credentials, or model files are staged.

- [ ] **Step 4: Configure and push only after receiving a URL**

After the user supplies an exact remote URL and confirms the target branch, run:

```bash
git remote add origin <user-supplied-url>
git push -u origin main
```

Verify `git remote -v` and the push result. If no URL is supplied, mark this task blocked and leave the local commits intact.
