# TxnMem Evidence Extension Design

**Date:** 2026-08-02
**Goal:** Extend the current deterministic benchmark with auditable
cross-process concurrency evidence, canonical event-contract validation, and
native-backend replay examples without making production-system claims.

## Scope and boundary

This phase targets evidence that can be reproduced inside the dependency-free
repository. It adds three local capabilities:

1. A process-based concurrency harness that records operation events through a
   single serialized backend and reports the observed linearization order.
2. A strict validator for the canonical memory event contract, plus a small
   example showing how an Agent/backend connector emits and validates native
   events before replay.
3. A trace evidence report that separates source operation counts, projected
   transaction-envelope operations, holdout grouping, provenance shape, and
   oracle comparison by variant.

The phase does not claim to implement a production transaction coordinator,
distributed 2PC, a networked vector/graph backend, or an independent LLM
Agent run. Those remain external-environment tasks requiring authorized
credentials and deployment infrastructure. The DOCX renderer is also outside
this code phase; its missing native dependency will remain documented.

## Design alternatives

### Option A: dependency-free process harness (recommended)

Use Python `multiprocessing` with a shared command queue and one serialized
backend owner process. Each worker submits a local sequence; the owner applies
each backend action under one lock and appends an explicit linearization index.
The report includes worker count, submitted operation count, backend event
count, unique event IDs, final memory snapshot, and whether every submitted
action was acknowledged.

This is reproducible and reviewable in the existing environment. It is a
distributed-style smoke test, not a production distributed transaction
protocol.

### Option B: external Redis/PostgreSQL backend

Implement the same adapter against a real service and measure network and
storage behavior. This would provide stronger systems evidence but introduces
service lifecycle, dependency, credential, and reproducibility requirements
that are not available in the current workspace.

### Option C: only expand deterministic simulator coverage

This is simplest but would not materially reduce the current gap around real
concurrency or native event capture.

Option A is selected for this phase; Option B is the next production-oriented
phase once a backend and deployment environment are authorized.

## Architecture

The extension keeps the existing event contract and adds focused modules:

- `src/txnmem_distributed.py`: process worker/owner harness and report schema.
- `src/txnmem_event_contract.py`: validation and normalization of native
  memory events; rejects missing IDs, unsupported kinds, malformed provenance,
  and non-serializable payloads.
- `src/txnmem_backend.py`: reuse the existing instrumented backend as the
  reference connector and expose a contract-validation boundary before
  `trace_to_instance`.
- `src/txnmem_realism.py`: expose per-variant replay and trace-shape summary
  helpers without changing the oracle or synthetic generator.
- `src/txnmem_experiment.py`: add explicit CLI commands for the process smoke
  test and event-contract validation.
- `tests/`: red-green tests for worker completion, event ordering, validation
  failures, and summary consistency.

No raw public benchmark input, transformed instance containing user content,
credentials, or backend response body is committed. Reports store counts,
labels, hashes, and aggregate metrics only.

## Canonical event contract

Every native event must be a mapping with:

- `event_id`: non-empty stable string unique within one recording;
- `kind`: one of `memory_read`, `memory_search`, `memory_write`,
  `memory_derive`, `memory_propagate`, `memory_supersede`, `invalidate`,
  `policy_change`, or `policy_revoke`;
- `agent_id`: non-empty principal identifier;
- `step`: positive integer recording source order;
- for write/derive/propagate/supersede events, a non-empty output memory ID;
- for derive/propagate events, source IDs that are non-empty strings;
- optional `txn_id`, `scope`, `projection`, `task_id`, `sample_id`,
  `session_id`, and redacted metadata.

The validator returns a deterministic normalized copy and a list of warnings
for optional metadata. It never invents provenance edges: derive and
propagate source IDs are retained from the connector event itself.

## Process concurrency flow

```text
worker_1 actions ─┐
worker_2 actions ─┼─> command queue ─> backend owner ─> event log + index
worker_N actions ─┘                         │
                                           └─> final snapshot + summary
```

Workers preserve their own local action order. The owner process is the sole
writer of backend state, so the resulting index is a real observed
linearization order rather than a post-hoc permutation. If a worker fails or
the queue closes early, the report marks the run incomplete and includes the
unacknowledged operation IDs.

## Trace evidence report

For each source and variant, report:

- source episode/instance count and canonical source-operation count;
- replay-envelope operation count, when an envelope is projected;
- train/holdout record counts and grouping key;
- operation count, transaction size, policy-change rate, provenance depth,
  branch factor, and agent count;
- `oracle_match`, invariant violations, and minimum mismatch fields;
- explicit flags `trace_ground_truth_native` and
  `production_latency_claim`, both false for the current public replay.

The report distinguishes full TxnMem oracle agreement from variant failures.
Agreement on a projected replay is a semantic sanity check, not a benchmark
task-success score.

## Testing and acceptance

Tests follow red-green-refactor:

1. malformed events fail contract validation with stable error codes;
2. valid derive/propagate events preserve source IDs and provenance metadata;
3. process workers complete, preserve per-worker order, and produce unique
   linearization indexes;
4. a forced worker failure is represented as incomplete rather than silently
   accepted;
5. source counts and replay-envelope counts remain distinct;
6. the existing full suite remains green.

Acceptance requires the new commands to run with Python standard-library
dependencies, the process report to be JSON serializable, no raw trace content
to enter committed outputs, and the final documentation to list the remaining
production gaps explicitly.
