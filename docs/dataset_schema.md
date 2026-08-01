# TxnMemBench Dataset Schema

## Instance object

Each JSONL line is one deterministic controlled experiment instance.

| Field | Type | Meaning |
|---|---|---|
| instance_id | string | Stable workload/seed identifier |
| workload | string | One of the W1-W8 workload family names |
| seed | integer | Reproduction seed |
| config | object | Agent count, transaction size, provenance depth, branch factor, concurrency, and policy churn |
| initial_memories | array | Memory records present before replay |
| operations | array | Ordered memory operations |
| policies | array | Versioned read/write/search permissions |
| failure_schedule | array | Step-indexed crash, revoke, delay, invalidate, or repair events |
| provenance_edges | array | source_id -> derived_id relationships |
| expected_outcome | object | Ground truth transaction state, committed IDs, repaired IDs, and target invariants |

## Memory object

A memory record contains:

- memory_id: unique identifier;
- agent_id: creating or owning agent;
- scope: access scope such as tenant:user_001;
- entity_id and attribute: logical entity/field;
- value: stored fact;
- status: active, pending, superseded, or invalid;
- policy_version: policy version used for the write;
- supersedes_id: replaced memory, when applicable;
- derived_from: direct source memory IDs.

## Operation types

- begin_txn: start a transaction and capture its policy version;
- write: buffer or apply a memory write;
- read: read a specific memory with scope enforcement;
- search: search memories visible to the caller scope;
- get_by_id: direct ID lookup that must still enforce scope;
- supersede: make a new memory replace an old memory;
- propagate: record a derived/provenance update;
- invalidate: invalidate a source memory;
- commit: commit buffered writes after policy revalidation.

Every operation has a unique op_id, a non-decreasing integer step, an agent_id, and a type.

## Workload families

| Name | Purpose |
|---|---|
| atomic_multi_write | Crash after the first write must not leave a partial transaction |
| crash_during_commit | Commit-boundary crash must leave a complete commit or no commit |
| revoke_before_commit | Commit must revalidate a permission revoked after write |
| scope_bypass | Search and direct-ID paths must enforce the same scope |
| supersession_consistency | Old/new memory replacement metadata and status must agree |
| provenance_chain_repair | Invalidating a root repairs all descendants in a chain |
| provenance_branch_repair | Invalidating a root repairs every branch descendant |
| mixed_stress | Combines writes, policy churn, crash, and recovery pressure |

## Variants

- Naive: immediate writes and no policy/scope/repair safeguards;
- TxnMem-NoTxn: no transaction buffering;
- TxnMem-NoPolicyCommit: no commit-time policy revalidation;
- TxnMem-NoRepair: no provenance repair;
- TxnMem: complete reference semantics.

## Invariant names

- atomicity_violation
- unexpected_commit
- recovery_consistency_violation
- invalid_commit_violation
- stale_write_violation
- scope_leak_violation
- supersession_consistency_violation
- provenance_closure_violation

## Result CSV fields

Each instance/variant row contains the workload identity, transaction state, violation names, committed count, operation count, repair count, and:

- partial_update_rate
- invalid_commit_rate
- stale_write_rate
- repair_recall
- leak_rate
- supersession_consistency
- scope_bypass_rate
- latency
- any_violation

## Output artifacts

The full experiment command writes:

- data/generated_instances.jsonl
- results/experiment_results.csv
- results/summary.json
- results/figures/violation_rate.svg
- results/figures/repair_recall.svg

