# TxnMemBench Core Extension Design

**Date:** 2026-07-30  
**Target:** `/data/txnmem` on the remote server  
**Source plan:** `TxnMem_两类数据集构造步骤.docx`

## Goal

Extend the existing dependency-free TxnMem pilot into a reproducible
TxnMemBench core that covers W1-W8 controlled workloads, produces explicit
ground truth and invariant violations, and emits experiment tables, summaries,
and simple figures. Real-agent dataset adapters remain a later phase.

## Current context

The remote project currently contains:

- `src/txnmem_experiment.py`, a single-file simulator for W1, W3, and W6;
- `tests/test_txnmem_experiment.py`, standard-library unit tests;
- `data/pilot_instances.jsonl` and `results/pilot_results.csv`;
- a `README.md` describing the pilot CLI.

The directory is not a Git repository, so the design and implementation will
be preserved under `/data/txnmem` without initializing or creating a Git
history.

## Scope

### In scope

- Preserve the current JSONL instance format and pilot CLI behavior.
- Add workload families W4 Scope Bypass, W5 Supersession Consistency, W7
  Provenance Branch Repair, and W8 Mixed Stress.
- Keep and harden W1 Atomic Multi-Write, W2 Crash During Commit, W3 Revoke
  Before Commit, and W6 Provenance Chain Repair.
- Split responsibilities into schema validation, workload generation,
  deterministic replay, invariant checking, metrics, and CLI/output helpers.
- Add `configs/workload_families.yaml`, schema documentation, generated JSONL,
  experiment CSV, summary JSON, and standard-library SVG figures.
- Keep the runtime usable with Python 3.12 and no required third-party
  packages. The config loader may use PyYAML when present and must fall back to
  a JSON-compatible YAML representation parsed by the standard library.

### Out of scope for this phase

- Connecting to a real memory backend or LLM agent.
- Downloading or executing τ-bench, AppWorld, LoCoMo, LongMemEval, or
  SWE-bench.
- Building a production concurrent storage engine.
- Initializing a Git repository in the existing remote directory.

## Architecture

The implementation uses small standard-library modules while retaining a
compatibility wrapper at `src/txnmem_experiment.py`.

- `src/txnmem_schema.py`: canonical field names, defaults, validation, and
  JSON/YAML configuration loading.
- `src/txnmem_workloads.py`: deterministic W1-W8 instance generators and
  workload-family configuration.
- `src/txnmem_simulator.py`: event replay for Naive, ablations, and TxnMem.
- `src/txnmem_invariants.py`: stable violation names and checks for atomicity,
  commit authorization, read scope, supersession, and provenance closure.
- `src/txnmem_metrics.py`: per-result rows, group mean/std summaries, and SVG
  chart generation.
- `src/txnmem_experiment.py`: CLI compatibility layer delegating to the
  modules above.

The public boundary is a plain Python dictionary so existing JSONL files and
tests continue to work. Internal helpers may use dataclasses, but serialization
must remain deterministic: sorted JSON keys, stable operation ordering, and
seed-controlled pseudo-randomness.

## Canonical data flow

```text
workload_families.yaml + seed
        -> deterministic instance generator
        -> generated_instances.jsonl
        -> variant replay engine
        -> invariant checkers
        -> per-instance metrics
        -> CSV + summary JSON + SVG figures
```

Each instance contains `config`, `initial_memories`, `operations`, `policies`,
`failure_schedule`, `provenance_edges`, and `expected_outcome`.

Memory objects contain at least `memory_id`, `agent_id`, `scope`, `status`,
`policy_version`, `supersedes_id`, and `derived_from`. Operations include
`begin_txn`, `read`, `search`, `get_by_id`, `write`, `supersede`, `propagate`,
`invalidate`, and `commit`. Failure events include `crash`, `revoke`,
`delay`, `invalidate`, and `repair` and are applied at explicit operation
steps.

## Workload semantics

- **W1 Atomic Multi-Write:** crash after the first buffered write; TxnMem
  aborts all writes, while no-transaction variants may partially commit.
- **W2 Crash During Commit:** crash at the commit boundary; recovery must leave
  either the complete transaction or no transaction, never a half-committed
  set.
- **W3 Revoke Before Commit:** revoke write permission after write and before
  commit; TxnMem revalidates policy version and aborts.
- **W4 Scope Bypass:** a permitted search is paired with an unauthorized
  direct-id read; the replay must record the denied read and never expose the
  memory to the caller.
- **W5 Supersession Consistency:** a new memory supersedes an old memory; the
  old record and new record must agree on the relation and active status.
- **W6 Provenance Chain Repair:** invalidating a root invalidates every active
  descendant in the chain.
- **W7 Provenance Branch Repair:** invalidating a root invalidates every branch
  descendant, including branches deeper than one edge.
- **W8 Mixed Stress:** combines multi-write, policy churn, concurrent-looking
  operation ordering, crash, and repair events while checking all applicable
  invariants.

Baseline variants remain `Naive`, `TxnMem-NoTxn`,
`TxnMem-NoPolicyCommit`, `TxnMem-NoRepair`, and `TxnMem`. Each ablation disables
only its named mechanism so failures are attributable to one missing feature.

## Metrics and outputs

Every instance/variant pair emits a CSV row with:

`instance_id`, `workload`, `seed`, `variant`, `transaction_state`,
`partial_update_rate`, `invalid_commit_rate`, `stale_write_rate`,
`repair_recall`, `leak_rate`, `supersession_consistency`,
`scope_bypass_rate`, `latency`, `any_violation`, `violations`,
`committed_count`, `operation_count`, and `repair_count`.

`summary.json` groups by workload and variant and stores count, mean, and
population standard deviation for numeric metrics. `figures/` contains SVG
bar charts for violation rate and repair recall, generated without a plotting
dependency.

## Testing and acceptance

Tests use `unittest` and follow red-green-refactor. New tests must first fail
for the missing workload, invariant, or metric and then pass with the minimal
implementation.

Required test coverage:

1. deterministic generation and schema validation;
2. one positive/negative replay test for each W1-W8;
3. ablation tests showing the intended failure mode;
4. transitive and branching provenance repair;
5. scope-safe search and direct-id reads;
6. supersession bidirectional consistency;
7. metric aggregation, mean/std calculation, and SVG output;
8. CLI generation, replay, and full-suite output;
9. all existing pilot tests.

Acceptance requires the full test suite to pass, W1-W8 to generate and replay,
the TxnMem variant to have no targeted violations on canonical positive cases,
and the CLI to produce valid JSONL, CSV, summary JSON, and SVG artifacts under
`/data/txnmem`.

## Future extension point

The later real-dataset phase will convert τ-bench, AppWorld, and LoCoMo events
into the same `memory object` and `operation` dictionaries, adding transaction
boundaries, policy labels, provenance edges, failure schedules, and expected
outcomes without changing the replay/checker/metrics interfaces.
