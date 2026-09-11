# Multi-Agent Shared-Memory Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run an auditable multi-agent TxnMem experiment in which two agents execute interleaved transactions with distinct permissions and cross-agent provenance, then compare five variants against independent reference semantics on exhaustive schedules, 300 unique seeded cases, and 240 real Qdrant/Neo4j observations.

**Architecture:** Extend the existing deterministic replay engine from one global write buffer to per-transaction state while preserving legacy single-agent outputs. Generate explicit two-agent programs, enumerate all legal micro-interleavings, and generate fifty structurally distinct stress cases per workload. Keep reference execution independent, add multi-agent invariants and evidence gates, then reuse the transaction semantics through a backend coordinator for paired real-storage runs.

**Tech Stack:** Python 3 standard library, `unittest`, JSON/JSONL/CSV, existing TxnMem simulator/reference/metrics modules, `ThreadPoolExecutor`, Qdrant, Neo4j, Toxiproxy, Docker Compose, Git.

**Spec:** `docs/superpowers/specs/2026-09-11-multi-agent-shared-memory-correctness-design.md`

## Global Constraints

- Before implementation, use `superpowers:using-git-worktrees` and create the isolated branch `codex/multi-agent-correctness` from the current `main` commit.
- Preserve all existing untracked `results/cross_host_model_load_*` directories and `results/real_backend_faults_state_verified_30_v2/exit_code`; never stage, rewrite, or delete them.
- Run all five variants on the exact same frozen instance bytes.
- Keep `src/txnmem_reference.py` independent: it must not import `txnmem_simulator`, `txnmem_invariants`, `txnmem_multiagent_invariants`, or `txnmem_multiagent_storage`.
- Keep legacy `experiment`, `pilot`, and `formal` CLI behavior and legacy single-agent normalized results unchanged apart from an explicitly versioned oracle field when the reference schema changes.
- A controlled stress run contains exactly six workload families × fifty structurally unique instances × five variants = 1500 observations.
- Exhaustive interleavings are a separate denominator and must never be merged with seeded observations.
- A real-storage run contains exactly four scenarios × thirty repetitions × two conditions = 240 observations.
- Every timeout, service error, partial readback, or ambiguous readback remains in the formal denominator as `unknown` or the corresponding failure classification.
- Do not activate the paper claim unless every gate in the approved design passes; failed gates must produce an inactive `claim_status.json` with the observed reason.
- All Python behavior changes follow RED–GREEN–REFACTOR. Run the focused failing test before changing production code, then run focused and regression tests after the change.

---

### Task 1: Define two-agent workloads and structural uniqueness

**Files:**

- Create: `src/txnmem_multiagent_workloads.py`
- Create: `tests/test_txnmem_multiagent_workloads.py`
- Modify: `src/txnmem_schema.py`
- Test: `tests/test_txnmem_schema.py`

**Interfaces:**

- Produces: `MULTI_AGENT_WORKLOADS: tuple[str, ...]`
- Produces: `generate_multi_agent_instance(workload: str, seed: int) -> dict[str, Any]`
- Produces: `generate_multi_agent_suite(seeds: Iterable[int] = range(50)) -> list[dict[str, Any]]`
- Produces: `structural_signature(instance: Mapping[str, Any]) -> str`
- Produces: `exhaustive_multi_agent_instances() -> list[dict[str, Any]]`
- Consumes later: `validate_instance`, `enumerate_interleavings`

- [ ] **Step 1: Write generator contract tests**

Create `tests/test_txnmem_multiagent_workloads.py` with these assertions:

```python
import unittest
from collections import Counter, defaultdict

from txnmem_multiagent_workloads import (
    MULTI_AGENT_WORKLOADS,
    exhaustive_multi_agent_instances,
    generate_multi_agent_instance,
    generate_multi_agent_suite,
    structural_signature,
)
from txnmem_schema import validate_instance


class MultiAgentWorkloadTests(unittest.TestCase):
    def test_suite_has_fifty_unique_two_agent_instances_per_family(self):
        suite = generate_multi_agent_suite(range(50))
        self.assertEqual(len(MULTI_AGENT_WORKLOADS), 6)
        self.assertEqual(len(suite), 300)
        counts = Counter(item["workload"] for item in suite)
        self.assertEqual(set(counts.values()), {50})
        signatures = defaultdict(set)
        for item in suite:
            validate_instance(item)
            agents = {op["agent_id"] for op in item["operations"]}
            self.assertEqual(agents, {"agent_1", "agent_2"})
            self.assertGreaterEqual(len({op.get("txn_id") for op in item["operations"] if op.get("txn_id")}), 2)
            self.assertEqual(item["structure_signature"], structural_signature(item))
            signatures[item["workload"]].add(item["structure_signature"])
        self.assertTrue(all(len(values) == 50 for values in signatures.values()))

    def test_same_seed_is_byte_deterministic(self):
        for workload in MULTI_AGENT_WORKLOADS:
            self.assertEqual(
                generate_multi_agent_instance(workload, 17),
                generate_multi_agent_instance(workload, 17),
            )

    def test_exhaustive_instances_preserve_each_agent_program_order(self):
        instances = exhaustive_multi_agent_instances()
        self.assertGreater(len(instances), len(MULTI_AGENT_WORKLOADS))
        for item in instances:
            self.assertEqual(item["evidence_tier"], "exhaustive")
            for agent_id in ("agent_1", "agent_2"):
                ranks = [op["program_index"] for op in item["operations"] if op["agent_id"] == agent_id]
                self.assertEqual(ranks, sorted(ranks))
```

- [ ] **Step 2: Run the new test and observe RED**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_workloads -v
```

Expected: import failure for missing `txnmem_multiagent_workloads`.

- [ ] **Step 3: Add the six schema workload names**

Extend `KNOWN_WORKLOADS` in `src/txnmem_schema.py` with these exact values without changing the legacy `CONFIG_WORKLOADS` membership:

```python
LEGACY_WORKLOADS = (
    "atomic_multi_write",
    "crash_during_commit",
    "revoke_before_commit",
    "scope_bypass",
    "supersession_consistency",
    "provenance_chain_repair",
    "provenance_branch_repair",
    "mixed_stress",
)
MULTI_AGENT_WORKLOADS = (
    "multi_agent_visibility",
    "interleaved_atomicity",
    "concurrent_supersession",
    "actor_policy_scope",
    "cross_agent_provenance",
    "multi_agent_mixed",
)
KNOWN_WORKLOADS = LEGACY_WORKLOADS + MULTI_AGENT_WORKLOADS + ("trace_grounded_replay",)
CONFIG_WORKLOADS = LEGACY_WORKLOADS
```

Add schema validation for optional multi-agent metadata:

```python
if "structure_signature" in instance:
    signature = instance["structure_signature"]
    if not isinstance(signature, str) or len(signature) != 64:
        raise ValueError("structure_signature must be a SHA-256 hex digest")
if instance.get("evidence_tier") not in {None, "seeded", "exhaustive"}:
    raise ValueError("evidence_tier must be seeded or exhaustive")
```

- [ ] **Step 4: Implement deterministic candidate generation**

Create `src/txnmem_multiagent_workloads.py`. Use the following exact family list and meaningful candidate dimensions:

```python
MULTI_AGENT_WORKLOADS = (
    "multi_agent_visibility",
    "interleaved_atomicity",
    "concurrent_supersession",
    "actor_policy_scope",
    "cross_agent_provenance",
    "multi_agent_mixed",
)

PARAMETER_GRID = {
    "multi_agent_visibility": {
        "writer": ("agent_1", "agent_2"),
        "txn_size": (1, 2, 3, 4),
        "reader_probe_count": (1, 2),
    },
    "interleaved_atomicity": {
        "txn_size_a": (1, 2, 3, 4),
        "txn_size_b": (1, 2, 3, 4),
        "crash_victim": ("agent_1", "agent_2"),
        "crash_phase": ("before_linearize", "after_linearize"),
    },
    "concurrent_supersession": {
        "first_committer": ("agent_1", "agent_2"),
        "precommit_reads": (1, 2),
        "new_value_count": (1, 2, 3, 4),
    },
    "actor_policy_scope": {
        "revoked_agent": ("agent_1", "agent_2"),
        "revoked_action": ("write", "derive"),
        "probe_type": ("search", "get_by_id"),
        "trigger_slot": (1, 2, 3, 4),
    },
    "cross_agent_provenance": {
        "source_agent": ("agent_1", "agent_2"),
        "depth": (1, 2, 3, 4),
        "branch_factor": (1, 2, 3),
        "invalidation_phase": ("before_derived_commit", "after_derived_commit"),
    },
    "multi_agent_mixed": {
        "txn_size_a": (1, 2, 3, 4),
        "txn_size_b": (1, 2, 3, 4),
        "revoked_agent": ("agent_1", "agent_2"),
        "depth": (1, 2, 3, 4),
        "crash_phase": ("before_linearize", "after_linearize"),
    },
}
```

For each candidate index, select meaningful parameter values with mixed-radix arithmetic, build two per-agent operation sequences, and use a seeded topological merge that preserves each agent’s program order. Enforce family predicates such as placing a visibility probe between another transaction’s write and commit. Canonicalize a `signature_view` containing workload, sampled parameters, ordered `(agent_id, type, txn role, scope role)` operations, and normalized fault triggers. Hash with `json.dumps(signature_view, sort_keys=True, separators=(",", ":"))` and SHA-256. Walk candidate indexes deterministically, retain only unseen signatures, require fifty unique candidates, and select the seed-indexed retained candidate for seeds `0..49`. Cache each family’s retained candidate tuple with `functools.lru_cache` so repeated generation does not rebuild schedules. Reserve complete `enumerate_interleavings` use for the short exhaustive programs only; do not enumerate the stress-program Cartesian product.

The generated instance must store:

```python
{
    "instance_id": f"{workload}_seed_{seed}",
    "workload": workload,
    "seed": seed,
    "evidence_tier": "seeded",
    "config": {
        "agent_count": 2,
        "concurrency": 2,
        "sampled_parameters": sampled_parameters,
    },
    "operations": operations,
    "policies": policies_for_both_agents,
    "failure_schedule": failure_schedule,
    "initial_memories": initial_memories,
    "provenance_edges": initial_edges,
    "structure_signature": signature,
}
```

`exhaustive_multi_agent_instances()` must use one minimal program for visibility, independent atomicity, concurrent supersession, selective revocation, and cross-agent provenance; enumerate every legal interleaving; assign `evidence_tier="exhaustive"`; and keep exhaustive IDs separate from seeded IDs.

- [ ] **Step 5: Verify generator and legacy schema tests**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_workloads tests.test_txnmem_schema tests.test_txnmem_workloads -v
```

Expected: all tests pass, 300 seeded instances validate, and every family has fifty distinct signatures.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/txnmem_multiagent_workloads.py src/txnmem_schema.py tests/test_txnmem_multiagent_workloads.py tests/test_txnmem_schema.py
git commit -m "feat: generate structurally unique multi-agent workloads"
```

---

### Task 2: Refactor the simulator to per-transaction state

**Files:**

- Modify: `src/txnmem_simulator.py`
- Modify: `tests/test_txnmem_simulator.py`
- Create: `tests/test_txnmem_multiagent_simulator.py`

**Interfaces:**

- Preserves: `run_instance(instance: dict[str, Any], variant: str) -> dict[str, Any]`
- Adds result field: `read_observations: list[dict[str, Any]]`
- Adds result field: `transaction_write_sets: dict[str, list[str]]`
- Adds metric: `conflict_abort_count: int`
- Consumes: multi-agent instances from Task 1

- [ ] **Step 1: Write isolation and independent-commit tests**

Add `tests/test_txnmem_multiagent_simulator.py`:

```python
import unittest

from txnmem_multiagent_workloads import generate_multi_agent_instance
from txnmem_simulator import run_instance


class MultiAgentSimulatorTests(unittest.TestCase):
    def test_other_transaction_pending_write_is_not_visible(self):
        instance = generate_multi_agent_instance("multi_agent_visibility", 0)
        result = run_instance(instance, "TxnMem")
        probes = [row for row in result["read_observations"] if row["probe_role"] == "before_writer_commit"]
        self.assertTrue(probes)
        self.assertTrue(all(not row["visible_memory_ids"] for row in probes))

    def test_commit_flushes_only_its_own_write_set(self):
        instance = generate_multi_agent_instance("interleaved_atomicity", 0)
        result = run_instance(instance, "TxnMem")
        for txn_id, write_ids in result["transaction_write_sets"].items():
            committed = set(write_ids) & set(result["committed_memory_ids"])
            state = result["transaction_states"][txn_id]
            self.assertEqual(committed, set(write_ids) if state == "committed" else set())

    def test_operation_actor_must_match_transaction_actor(self):
        instance = generate_multi_agent_instance("multi_agent_visibility", 1)
        transaction_operation = next(op for op in instance["operations"] if op.get("txn_id") == "txn_agent_1" and op["type"] == "write")
        transaction_operation["agent_id"] = "agent_2"
        with self.assertRaisesRegex(ValueError, "transaction actor mismatch"):
            run_instance(instance, "TxnMem")
```

- [ ] **Step 2: Run focused tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_simulator -v
```

Expected: missing `read_observations` or cross-transaction write leakage from the global buffer.

- [ ] **Step 3: Replace global buffers with transaction contexts**

Inside `run_instance`, replace `buffered_writes`, `buffered_edges`, `begin_policy_version`, and the single write permission flag with these state containers:

```python
transactions: dict[str, dict[str, Any]] = {}
policy_versions: dict[str, int] = defaultdict(lambda: 1)
revoked_permissions: set[tuple[str, str, str]] = set()
read_observations: list[dict[str, Any]] = []

def transaction(operation: dict[str, Any]) -> dict[str, Any]:
    txn_id = str(operation.get("txn_id") or "implicit")
    actor = str(operation.get("agent_id") or "agent_1")
    if txn_id not in transactions:
        transactions[txn_id] = {
            "agent_id": actor,
            "status": "active",
            "begin_policy_version": policy_versions[actor],
            "writes": {},
            "edges": [],
            "supersessions": [],
            "read_versions": {},
        }
    if transactions[txn_id]["agent_id"] != actor:
        raise ValueError(f"transaction actor mismatch: {txn_id}")
    return transactions[txn_id]
```

Use a transaction’s own `writes` for read-your-writes and never inspect another transaction’s writes. A transactional commit publishes only that transaction’s `writes`, `edges`, and `supersessions`; abort clears only that transaction. A process-level crash aborts all active transactions, preserves previously committed transactions, records each terminal state, and stops execution.

For non-transaction variants, writes remain immediately visible but still record the logical `txn_id` in `transaction_write_sets`, allowing the checker to attribute partial and cross-transaction exposure.

- [ ] **Step 4: Add actor policy, version-conflict, and provenance tests**

Extend the test file:

```python
    def test_revocation_targets_only_one_agent(self):
        instance = generate_multi_agent_instance("actor_policy_scope", 0)
        result = run_instance(instance, "TxnMem")
        revoked = instance["config"]["sampled_parameters"]["revoked_agent"]
        survivor = "agent_2" if revoked == "agent_1" else "agent_1"
        self.assertEqual(result["transaction_states"][f"txn_{revoked}"], "aborted")
        self.assertEqual(result["transaction_states"][f"txn_{survivor}"], "committed")

    def test_second_conflicting_supersession_aborts(self):
        result = run_instance(
            generate_multi_agent_instance("concurrent_supersession", 0),
            "TxnMem",
        )
        states = list(result["transaction_states"].values())
        self.assertEqual(states.count("committed"), 1)
        self.assertEqual(states.count("aborted"), 1)
        self.assertEqual(result["metrics"]["conflict_abort_count"], 1)

    def test_cross_agent_descendants_are_invalidated(self):
        result = run_instance(
            generate_multi_agent_instance("cross_agent_provenance", 0),
            "TxnMem",
        )
        self.assertTrue(all(
            memory["status"] == "invalid"
            for memory in result["final_memories"].values()
            if memory.get("derived_from")
        ))
```

Run the three new tests and observe the expected failures before implementing actor-specific permissions, memory versions, staged supersession, and cross-agent repair.

- [ ] **Step 5: Implement the remaining multi-agent semantics**

Implement policy keys as `(agent_id, action, scope)`. A revoke event without `agent_id` remains a legacy global revoke; a revoke with `agent_id` affects only that actor. At commit, revalidate every staged mutating operation for the transaction actor when the variant is in `POLICY_REVALIDATION_VARIANTS`.

Give every committed memory `version`, defaulting initial memories to `1`. Record `(version, status, scope)` when a transaction reads a committed memory or uses it as a derive source. Before commit, compare the stored tuple with current committed state; mismatch aborts the transaction and increments `conflict_abort_count`.

Stage supersession status changes in `txn["supersessions"]`; do not mutate the committed old record before commit. On commit, apply new records, mark the old record superseded, and increment both affected versions in the same linearization step.

For invalidation, use committed provenance edges regardless of author. Full and repair-enabled variants invalidate every transitive descendant. A no-repair variant invalidates only the root. Record each read result as:

```python
{
    "operation_id": operation["op_id"],
    "reader_agent_id": operation["agent_id"],
    "txn_id": operation.get("txn_id"),
    "probe_role": operation.get("probe_role"),
    "visible_memory_ids": sorted(visible_ids),
    "denied_reason": denied_reason,
}
```

- [ ] **Step 6: Run focused and legacy regression tests**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_simulator tests.test_txnmem_simulator tests.test_txnmem_invariants tests.test_txnmem_differential -v
```

Expected: all tests pass; old single-agent workloads keep their prior transaction outcomes and visible-memory behavior.

- [ ] **Step 7: Commit Task 2**

```bash
git add src/txnmem_simulator.py tests/test_txnmem_simulator.py tests/test_txnmem_multiagent_simulator.py
git commit -m "feat: isolate concurrent transaction state by transaction id"
```

---

### Task 3: Extend independent reference semantics and differential comparison

**Files:**

- Modify: `src/txnmem_reference.py`
- Modify: `src/txnmem_differential.py`
- Modify: `tests/test_txnmem_reference.py`
- Modify: `tests/test_txnmem_differential.py`
- Create: `tests/test_txnmem_multiagent_reference.py`

**Interfaces:**

- Preserves: `reference_outcome(instance: dict[str, Any]) -> dict[str, Any]`
- Extends oracle outcomes with: `read_observations`, `active_versions`, and complete `txn_states`
- Preserves: `compare_result_to_oracle(instance, result, oracle=None) -> dict[str, Any]`

- [ ] **Step 1: Write independent multi-agent oracle tests**

Create `tests/test_txnmem_multiagent_reference.py`:

```python
import inspect
import unittest

import txnmem_reference
from txnmem_multiagent_workloads import generate_multi_agent_instance
from txnmem_reference import reference_outcome


class MultiAgentReferenceTests(unittest.TestCase):
    def test_reference_tracks_each_transaction_separately(self):
        oracle = reference_outcome(generate_multi_agent_instance("interleaved_atomicity", 0))
        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(set(outcome["txn_states"]), {"txn_agent_1", "txn_agent_2"})

    def test_reference_denies_other_transaction_pending_read(self):
        oracle = reference_outcome(generate_multi_agent_instance("multi_agent_visibility", 0))
        before = [row for row in oracle["allowed_outcomes"][0]["read_observations"] if row["probe_role"] == "before_writer_commit"]
        self.assertTrue(before)
        self.assertTrue(all(not row["visible_memory_ids"] for row in before))

    def test_reference_has_no_implementation_import(self):
        source = inspect.getsource(txnmem_reference)
        for forbidden in ("txnmem_simulator", "txnmem_invariants", "txnmem_multiagent_invariants", "txnmem_multiagent_storage"):
            self.assertNotIn(forbidden, source)
```

- [ ] **Step 2: Run oracle tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_reference -v
```

Expected: missing read observations, actor-specific revoke semantics, or conflict revalidation.

- [ ] **Step 3: Implement reference-only multi-agent state**

Change the reference state from global revoked actions to actor-scoped keys:

```python
"policy_versions": defaultdict(lambda: 1),
"revoked_permissions": set(),
```

Each reference transaction stores `agent_id`, `begin_policy_version`, `read_versions`, `write_set`, `pending_edges`, and `supersessions`. `_policy_allows` must evaluate the operation actor and scope. Revoke events with an actor update only that actor’s permission version; legacy events without an actor update the wildcard actor.

Committed memory starts at version `1`. Reads and derive sources record `(version, status, scope)`. `_commit` revalidates that tuple and current actor permissions before changing `state["memories"]`; any mismatch produces `aborted` with reason `READ_VERSION_CHANGED`, `SOURCE_INVALID`, or `POLICY_REVOKED`. Reference supersession stays staged until commit.

Record reference reads with the same public field names as Task 2, but implement the visibility calculation entirely inside the reference module. Increment `ORACLE_VERSION` to `0.2` and update the explicit legacy version assertion in `tests/test_txnmem_differential.py`.

- [ ] **Step 4: Make differential comparison include observations and versions**

Extend `_implementation_snapshot` and `_matches_outcome` with:

```python
"read_observations": sorted(
    result.get("read_observations", []),
    key=lambda row: (str(row.get("operation_id")), str(row.get("reader_agent_id"))),
),
"active_versions": {
    memory_id: int(memory.get("version", 1))
    for memory_id, memory in sorted(final_memories.items())
    if memory.get("status") == "active"
},
```

Compare these fields only when the oracle outcome contains them so legacy external oracle fixtures remain readable.

- [ ] **Step 5: Verify reference, differential, and legacy oracle behavior**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_reference tests.test_txnmem_reference tests.test_txnmem_differential -v
```

Expected: full TxnMem matches the independent oracle for representative multi-agent and all existing legacy tests.

- [ ] **Step 6: Commit Task 3**

```bash
git add src/txnmem_reference.py src/txnmem_differential.py tests/test_txnmem_reference.py tests/test_txnmem_differential.py tests/test_txnmem_multiagent_reference.py
git commit -m "feat: add independent multi-agent reference outcomes"
```

---

### Task 4: Add multi-agent invariants, rows, and mutation witnesses

**Files:**

- Create: `src/txnmem_multiagent_invariants.py`
- Create: `tests/test_txnmem_multiagent_invariants.py`
- Modify: `src/txnmem_invariants.py`
- Modify: `src/txnmem_metrics.py`
- Modify: `src/txnmem_mutation.py`
- Modify: `tests/test_txnmem_metrics.py`
- Modify: `tests/test_txnmem_mutation.py`

**Interfaces:**

- Produces: `MULTI_AGENT_VIOLATION_ORDER`
- Produces: `check_multi_agent_invariants(instance, result) -> list[str]`
- Routes from: `check_invariants(instance, result)`
- Extends: `result_row(instance, result)` with multi-agent evidence columns

- [ ] **Step 1: Write one direct test per invariant**

Create `tests/test_txnmem_multiagent_invariants.py` and construct minimal result mutations that trigger exactly these names:

```python
EXPECTED = (
    "dirty_read_violation",
    "atomicity_violation",
    "cross_txn_flush_violation",
    "actor_authorization_violation",
    "actor_scope_violation",
    "conflict_consistency_violation",
    "cross_agent_provenance_violation",
    "recovery_consistency_violation",
)
```

Also add:

```python
def test_full_txnmem_has_no_violation_for_all_seed_zero_families(self):
    for workload in MULTI_AGENT_WORKLOADS:
        instance = generate_multi_agent_instance(workload, 0)
        self.assertEqual(
            check_multi_agent_invariants(instance, run_instance(instance, "TxnMem")),
            [],
        )
```

- [ ] **Step 2: Run focused tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_invariants -v
```

Expected: import failure for missing multi-agent checker.

- [ ] **Step 3: Implement checker without calling the oracle**

`check_multi_agent_invariants` must derive each result directly from the frozen program and implementation trace:

- `dirty_read_violation`: a read observation contains a memory whose producer transaction was not committed at that observation’s trace index.
- `atomicity_violation`: a logical transaction write set intersects committed IDs in a nonzero proper subset.
- `cross_txn_flush_violation`: a transaction marked non-committed has any of its write IDs committed.
- `actor_authorization_violation`: a committed write was produced by an actor/action/scope revoked before that transaction’s commit.
- `actor_scope_violation`: a read observation exposes memory whose scope differs from the operation scope or actor policy.
- `conflict_consistency_violation`: more than one active memory supersedes the same old ID or represents the same `(entity_id, attribute, base_version)` conflict group.
- `cross_agent_provenance_violation`: an invalid root has any active transitive descendant in the result edge graph.
- `recovery_consistency_violation`: explicit before/after-linearize crash classification disagrees with the target transaction’s final write visibility.

The checker may use shared pure graph traversal helpers but must not call `reference_outcome` or `compare_result_to_oracle` to manufacture a named invariant result.

Route multi-agent workloads from `check_invariants`; keep legacy workload branches unchanged.

- [ ] **Step 4: Extend result rows and Wilson intervals**

For multi-agent observations, add these columns while preserving the existing fields:

```python
"agent_count": 2,
"transaction_count": len(result.get("transaction_states", {})),
"structure_signature": instance.get("structure_signature", ""),
"dirty_read_rate": float("dirty_read_violation" in violations),
"cross_txn_flush_rate": float("cross_txn_flush_violation" in violations),
"actor_authorization_rate": float("actor_authorization_violation" in violations),
"actor_scope_rate": float("actor_scope_violation" in violations),
"conflict_violation_rate": float("conflict_consistency_violation" in violations),
"cross_agent_provenance_rate": float("cross_agent_provenance_violation" in violations),
```

Use `txnmem_statistics.binomial_interval` in the multi-agent summary builder added in Task 5; do not label per-row binary indicators as population rates in prose.

- [ ] **Step 5: Add multi-agent mutant specifications**

Extend the mutation campaign with these exact targets:

```python
MULTI_AGENT_MUTANTS = {
    "expose_other_txn_pending": "dirty_read_violation",
    "flush_all_txn_buffers": "cross_txn_flush_violation",
    "disable_actor_commit_revalidation": "actor_authorization_violation",
    "disable_read_version_check": "conflict_consistency_violation",
    "disable_cross_agent_repair": "cross_agent_provenance_violation",
}
```

Map `expose_other_txn_pending`, `flush_all_txn_buffers`, and `disable_read_version_check` to `TxnMem-NoTxn`; map actor revalidation to `TxnMem-NoPolicyCommit`; map cross-agent repair to `TxnMem-NoRepair`. Find the shortest operation prefix that reproduces the target invariant, and verify that removing the final operation no longer reproduces it. Do not add a sixth experimental variant or a test-only production switch.

- [ ] **Step 6: Verify metrics and mutation regressions**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_invariants tests.test_txnmem_metrics tests.test_txnmem_mutation tests.test_txnmem_invariants -v
```

- [ ] **Step 7: Commit Task 4**

```bash
git add src/txnmem_multiagent_invariants.py src/txnmem_invariants.py src/txnmem_metrics.py src/txnmem_mutation.py tests/test_txnmem_multiagent_invariants.py tests/test_txnmem_metrics.py tests/test_txnmem_mutation.py
git commit -m "feat: audit multi-agent transaction policy and provenance invariants"
```

---

### Task 5: Build exhaustive and seeded controlled experiment reporting

**Files:**

- Create: `src/txnmem_multiagent_experiment.py`
- Create: `tests/test_txnmem_multiagent_experiment.py`
- Modify: `src/txnmem_interleavings.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**

- Produces: `run_multi_agent_controlled(out_dir: Path, seeds: int = 50, replications: int = 3, variants: Iterable[str] = VARIANTS) -> dict[str, Any]`
- Produces CLI: `txnmem_experiment.py multi-agent-experiment`
- Produces all controlled artifacts defined by the design

- [ ] **Step 1: Write an end-to-end temporary-directory test**

Create `tests/test_txnmem_multiagent_experiment.py`:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
import csv
import json
import unittest

from txnmem_multiagent_experiment import run_multi_agent_controlled


class MultiAgentExperimentTests(unittest.TestCase):
    def test_small_run_writes_separate_seeded_and_exhaustive_denominators(self):
        with TemporaryDirectory() as tmp:
            report = run_multi_agent_controlled(Path(tmp), seeds=2, replications=2)
            seeded = list(csv.DictReader(open(Path(tmp) / "results/experiment_results.csv", encoding="utf-8")))
            exhaustive = list(csv.DictReader(open(Path(tmp) / "results/exhaustive_results.csv", encoding="utf-8")))
            self.assertEqual(len(seeded), 6 * 2 * 5)
            self.assertGreater(len(exhaustive), 0)
            self.assertEqual(report["seeded_observation_count"], 60)
            self.assertEqual(report["replication_count"], 2)
            self.assertTrue((Path(tmp) / "results/claim_status.json").exists())
            self.assertTrue((Path(tmp) / "results/minimal_witnesses.json").exists())
```

- [ ] **Step 2: Run the focused test and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_experiment -v
```

Expected: missing experiment module.

- [ ] **Step 3: Implement controlled runner and artifacts**

`run_multi_agent_controlled` must:

1. generate and validate the seeded suite;
2. reject any repeated structural signature within a family;
3. materialize independent oracle records before running variants;
4. run all variants on deep copies of the same instance;
5. run every exhaustive instance separately;
6. write CSV/JSONL using existing `write_csv`, `write_jsonl`, and `write_summary`;
7. calculate per-family and per-variant exact counts and 95% Wilson intervals;
8. write parameter, interleaving, invariant, and mutation coverage;
9. derive prefix-minimal witnesses for every multi-agent mutant;
10. rerun generation and execution in memory `replications` times and compare canonical SHA-256 hashes;
11. evaluate each design success gate and write an inactive or active controlled-only claim status.

Use this return schema:

```python
{
    "schema_version": 1,
    "seeded_instance_count": len(instances),
    "seeded_observation_count": len(rows),
    "exhaustive_instance_count": len(exhaustive_instances),
    "exhaustive_observation_count": len(exhaustive_rows),
    "replication_count": replications,
    "full_txnmem_seeded_violations": int,
    "full_txnmem_seeded_oracle_mismatches": int,
    "full_txnmem_exhaustive_violations": int,
    "full_txnmem_exhaustive_oracle_mismatches": int,
    "controlled_gates_passed": bool,
}
```

- [ ] **Step 4: Add the CLI command**

Add parser arguments:

```python
multi_agent = subparsers.add_parser(
    "multi-agent-experiment",
    help="run exhaustive and structurally unique multi-agent correctness experiments",
)
multi_agent.add_argument("--out-dir", type=Path, required=True)
multi_agent.add_argument("--seeds", type=int, default=50)
multi_agent.add_argument("--replications", type=int, default=3)
multi_agent.add_argument("--variants", nargs="+", choices=VARIANTS, default=list(VARIANTS))
```

Dispatch to `run_multi_agent_controlled` and return exit code `0` when the run completes, even if claim gates fail; claim failure is scientific output, not a CLI crash. Invalid schema, duplicate signatures, missing artifact, or hash inconsistency returns nonzero.

- [ ] **Step 5: Test the CLI output contract**

Add a subprocess test using `--seeds 2 --replications 2`. Assert all required files exist, seeded count is 60, all rows have two agents and at least two transactions, and exhaustive rows are not in the seeded CSV.

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_experiment tests.test_cli_outputs -v
```

- [ ] **Step 6: Commit Task 5**

```bash
git add src/txnmem_multiagent_experiment.py src/txnmem_interleavings.py src/txnmem_experiment.py tests/test_txnmem_multiagent_experiment.py tests/test_cli_outputs.py
git commit -m "feat: run exhaustive and seeded multi-agent experiments"
```

---

### Task 6: Add strict claim and artifact auditing

**Files:**

- Create: `src/txnmem_multiagent_audit.py`
- Create: `tests/test_txnmem_multiagent_audit.py`
- Modify: `src/txnmem_multiagent_experiment.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**

- Produces: `audit_controlled_artifacts(root: Path, expected_seeds: int) -> dict[str, Any]`
- Produces: `evaluate_claim_status(controlled: Mapping[str, Any], storage: Mapping[str, Any] | None) -> dict[str, Any]`
- Produces: `write_paper_summary(claim_status: Mapping[str, Any], controlled: Mapping[str, Any], storage: Mapping[str, Any] | None, path: Path) -> None`
- Produces CLI: `txnmem_experiment.py multi-agent-audit`

- [ ] **Step 1: Write fail-closed audit tests**

Create tests proving that the audit rejects:

- 299 or 301 seeded instances;
- any workload with fewer than fifty unique signatures;
- any variant missing from an instance;
- an oracle file not aligned by instance ID;
- an inactive/missing mutation witness;
- inconsistent replication hashes;
- a real-storage report containing `unknown`;
- a report containing passwords, API keys, absolute home paths, or routable IP addresses.

Add one passing fixture with exact controlled and storage counts.

- [ ] **Step 2: Run audit tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_audit -v
```

Expected: missing audit module.

- [ ] **Step 3: Implement exact-count and hash audit**

`audit_controlled_artifacts` must load the generated files, recompute file SHA-256 values, validate every row’s instance/workload/seed/variant key, enforce `6 * expected_seeds` instances and `6 * expected_seeds * 5` observations, and recompute summary totals from CSV rather than trusting `summary.json`. Claim evaluation requires `expected_seeds == 50`; smaller test runs can pass artifact integrity while their claim remains inactive.

`evaluate_claim_status` returns:

```python
{
    "schema_version": 1,
    "status": "active" if all(gates.values()) else "inactive",
    "claim": "In the defined operation model and covered fault schedules, TxnMem preserves transactional, actor-policy, and cross-agent provenance consistency for shared memory.",
    "gates": gates,
    "failed_gates": sorted(name for name, passed in gates.items() if not passed),
    "scope_boundary": "finite exhaustive micro-programs, 300 seeded controlled cases, and 240 state-verified real-storage observations",
}
```

Before real-storage results exist, status remains `inactive` with `real_storage_evidence=False`; this is expected after the controlled run.

Add `multi-agent-audit --controlled-root PATH --storage-report PATH --expected-seeds 50`. The command must audit both sources, rewrite only `results/claim_status.json` and `results/paper_summary.md` under the controlled root, and return nonzero for malformed artifacts. `write_paper_summary` must interpolate audited counts and failed gates; it must not contain hard-coded observed results.

- [ ] **Step 4: Integrate the audit into the controlled runner**

After writing controlled artifacts, call `audit_controlled_artifacts`, save `results/artifact_audit.json`, and write its hashes into `claim_status.json`. Do not catch `ValueError` and continue; malformed evidence must make the CLI fail.

- [ ] **Step 5: Verify and commit Task 6**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_audit tests.test_txnmem_multiagent_experiment tests.test_cli_outputs -v
git add src/txnmem_multiagent_audit.py src/txnmem_multiagent_experiment.py src/txnmem_experiment.py tests/test_txnmem_multiagent_audit.py tests/test_cli_outputs.py
git commit -m "feat: gate multi-agent claims on audited evidence"
```

---

### Task 7: Implement a transaction-aware concurrent storage coordinator

**Files:**

- Create: `src/txnmem_multiagent_storage.py`
- Create: `tests/test_txnmem_multiagent_storage.py`
- Modify: `src/txnmem_backend.py`
- Modify: `src/txnmem_vector_graph_backend.py`
- Create: `tests/test_txnmem_backend.py`
- Modify: `tests/test_txnmem_vector_graph_backend.py`

**Interfaces:**

- Produces: `MultiAgentStorageCoordinator`
- Produces: `run_concurrent_storage_scenario(scenario, coordinator) -> dict[str, Any]`
- Adds backend method: `delete(memory_id: str, **fields: Any) -> None`
- Consumes: `InstrumentedMemoryBackend` or `VectorGraphMemoryBackend`

- [ ] **Step 1: Write fake-backend transaction tests**

Create tests with a real `InstrumentedMemoryBackend` and a subclass that raises on a selected write ordinal:

```python
class FailingBackend(InstrumentedMemoryBackend):
    def __init__(self, fail_on_write):
        super().__init__()
        self.fail_on_write = fail_on_write
        self.write_count = 0

    def write(self, memory_id, value=None, **fields):
        self.write_count += 1
        if self.write_count == self.fail_on_write:
            raise ConnectionError("injected storage failure")
        return super().write(memory_id, value=value, **fields)
```

Tests must prove:

- another transaction cannot read pending writes;
- committing one transaction does not flush another;
- selective revoke aborts only the matching actor;
- a second-write failure compensates the first write and classifies the transaction `aborted_clean`;
- failed compensation classifies the result `unknown`, not success;
- cross-agent invalidation reaches descendants written by another actor;
- two threads produce a recorded commit linearization order with unique indexes.

- [ ] **Step 2: Run focused tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_storage -v
```

Expected: missing storage coordinator and backend delete interface.

- [ ] **Step 3: Add recoverable delete support**

Add `delete` to `InstrumentedMemoryBackend`, `SQLiteInstrumentedMemoryBackend`, and `VectorGraphMemoryBackend`. The vector/graph implementation must call Qdrant delete and Neo4j delete with the existing idempotency key scheme, then use persistent readback to require absence. A lost response followed by verified absence counts as deleted; unreadable state raises `VectorGraphBackendError`.

- [ ] **Step 4: Implement the coordinator state machine**

`MultiAgentStorageCoordinator` stores per-transaction actor, begin policy version, staged action list, read versions, and state. It exposes these exact signatures: `__init__(self, backend, *, variant: str = "TxnMem", policies: Iterable[Mapping[str, Any]] | None = None)`, `begin(self, txn_id: str, agent_id: str) -> None`, `stage(self, txn_id: str, action: Mapping[str, Any]) -> None`, `read(self, txn_id: str, memory_id: str, scope: str) -> dict[str, Any] | None`, `revoke(self, agent_id: str, action: str, scope: str) -> None`, `commit(self, txn_id: str) -> dict[str, Any]`, `invalidate(self, agent_id: str, memory_id: str) -> dict[str, Any]`, and `report(self) -> dict[str, Any]`.

Use an `RLock` for commit linearization and a `Barrier` only in the concurrent scenario runner. In `TxnMem`, validate actor permissions and read/source versions under the lock, capture prior state for every existing object, apply staged actions in order, and track successfully applied mutations. On failure, compensate in reverse order: delete newly created objects and restore the complete prior payload/status for invalidated or updated objects. In `Naive`, `stage` dispatches immediately and compensation is disabled.

After commit or compensation, verify every action via backend readback. Extend `VectorGraphMemoryBackend.invalidate` to persist `status="invalid"` to Qdrant and Neo4j, and extend `verify_persistent_state` to validate both write and invalidate actions. Classify only complete committed batches as `committed_complete`, fully restored/absent aborted batches as `aborted_clean`, mixed state as `partial_visible`, and unreadable state as `unknown`.

- [ ] **Step 5: Verify fake and vector/graph adapter behavior**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_storage tests.test_txnmem_vector_graph_backend tests.test_txnmem_backend -v
```

- [ ] **Step 6: Commit Task 7**

```bash
git add src/txnmem_multiagent_storage.py src/txnmem_backend.py src/txnmem_vector_graph_backend.py tests/test_txnmem_multiagent_storage.py tests/test_txnmem_backend.py tests/test_txnmem_vector_graph_backend.py
git commit -m "feat: coordinate concurrent transactions over storage backends"
```

---

### Task 8: Add the real Qdrant/Neo4j experiment command and launcher

**Files:**

- Create: `src/txnmem_multiagent_storage_experiment.py`
- Create: `tests/test_txnmem_multiagent_storage_experiment.py`
- Create: `scripts/run_multi_agent_real_storage.sh`
- Modify: `src/txnmem_experiment.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**

- Produces: `run_real_storage_matrix(backend_factory: Callable[[str, Callable[..., Any] | None], Any], repetitions: int = 30) -> dict[str, Any]`
- Produces CLI: `txnmem_experiment.py multi-agent-storage-experiment`
- Produces launcher: `scripts/run_multi_agent_real_storage.sh`

- [ ] **Step 1: Write fixture-mode matrix tests**

Create an offline fixture mode that uses an instrumented backend but follows the same scenario builder and report schema. Test exact counts for two repetitions:

```python
report = run_real_storage_matrix(
    backend_factory=fixture_backend_factory,
    repetitions=2,
)
self.assertEqual(report["scenario_count"], 4)
self.assertEqual(report["condition_count"], 2)
self.assertEqual(report["observation_count"], 16)
self.assertEqual(len(report["observations"]), 16)
```

Assert each observation contains `scenario`, `repetition`, `condition`, `agent_count=2`, `actual_commit_order`, `classification`, `state_readback`, and sanitized fault evidence.

- [ ] **Step 2: Run focused tests and observe RED**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_storage_experiment -v
```

Expected: missing experiment module.

- [ ] **Step 3: Implement the four paired scenarios**

Implement exact scenario names:

```python
REAL_STORAGE_SCENARIOS = (
    "concurrent_atomic_backend_failure",
    "selective_actor_revocation",
    "cross_agent_provenance_invalidation",
    "concurrent_mixed_failure",
)
```

For every repetition and condition, construct a unique namespace, two transactions, identical actions, and the same deterministic failure route. `TxnMem` and `Naive` differ only in coordinator variant. Record service versions once and Toxiproxy evidence per observation. Recompute classification from Qdrant and Neo4j operation-after readback; never infer success from a returned write response.

- [ ] **Step 4: Add CLI and shell launcher**

Add CLI arguments for backend type, Qdrant URL, Neo4j URI, Neo4j user/password from environment, Toxiproxy URL, repetitions, and output directory. The default formal repetitions value is `30`; an explicit `--fixture` flag is required for offline tests and must set `real_service_evidence=False`.

The launcher must follow the existing `run_real_backend_smoke.sh` pattern: start `infra/real_backend/docker-compose.yml`, wait for Qdrant, Neo4j, and Toxiproxy health, replace proxy routes, run the new command, and fail unless the output has exactly 240 observations and `all_state_readbacks_complete=True`. Credentials remain environment variables and never enter JSON artifacts.

- [ ] **Step 5: Test fixture CLI and shell syntax**

```bash
bash -n scripts/run_multi_agent_real_storage.sh
PYTHONPATH=src python3 -m unittest tests.test_txnmem_multiagent_storage_experiment tests.test_cli_outputs -v
```

- [ ] **Step 6: Commit Task 8**

```bash
git add src/txnmem_multiagent_storage_experiment.py src/txnmem_experiment.py scripts/run_multi_agent_real_storage.sh tests/test_txnmem_multiagent_storage_experiment.py tests/test_cli_outputs.py
git commit -m "feat: add paired real-storage multi-agent experiment"
```

---

### Task 9: Run formal experiments, audit evidence, and generate the paper report

**Files:**

- Create via experiment: `results/multi_agent_controlled_v1/data/generated_instances.jsonl`
- Create via experiment: `results/multi_agent_controlled_v1/data/exhaustive_interleavings.jsonl`
- Create via experiment: `results/multi_agent_controlled_v1/data/reference_oracles.jsonl`
- Create via experiment: `results/multi_agent_controlled_v1/results/experiment_results.csv`
- Create via experiment: `results/multi_agent_controlled_v1/results/exhaustive_results.csv`
- Create via experiment: `results/multi_agent_controlled_v1/results/summary.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/coverage.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/minimal_witnesses.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/reproducibility.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/artifact_audit.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/claim_status.json`
- Create via experiment: `results/multi_agent_controlled_v1/results/paper_summary.md`
- Create via experiment: `results/multi_agent_real_storage_v1/results/multi_agent_storage_results.json`
- Create via experiment: `results/multi_agent_real_storage_v1/results/artifact_audit.json`
- Modify after both tiers: `results/multi_agent_controlled_v1/results/claim_status.json`

**Interfaces:**

- Consumes all tasks and the approved design gates
- Produces final evidence and a Chinese paper-ready interpretation generated from observed counts

- [ ] **Step 1: Run focused multi-agent tests**

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_multiagent_workloads \
  tests.test_txnmem_multiagent_simulator \
  tests.test_txnmem_multiagent_reference \
  tests.test_txnmem_multiagent_invariants \
  tests.test_txnmem_multiagent_experiment \
  tests.test_txnmem_multiagent_audit \
  tests.test_txnmem_multiagent_storage \
  tests.test_txnmem_multiagent_storage_experiment -v
```

Expected: zero failures and zero errors.

- [ ] **Step 2: Run the complete regression suite**

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Expected: zero failures and zero errors. Any unrelated environment-dependent integration test must remain reported separately; it may not be silently skipped unless it already carries an explicit repository skip condition.

- [ ] **Step 3: Run the formal controlled experiment**

```bash
PYTHONPATH=src python3 src/txnmem_experiment.py multi-agent-experiment \
  --out-dir results/multi_agent_controlled_v1 \
  --seeds 50 \
  --replications 3
```

Verify exact file counts and audit status from `artifact_audit.json`. The controlled claim remains inactive until real-storage evidence is supplied, even when all controlled gates pass.

- [ ] **Step 4: Run the formal real-storage matrix**

```bash
TXNMEM_REPETITIONS=30 \
TXNMEM_OUT_DIR=results/multi_agent_real_storage_v1 \
bash scripts/run_multi_agent_real_storage.sh
```

This step requires local Docker access. If Docker or service health is unavailable, preserve the generated blocked report and stop the claim activation path; do not substitute fixture data for real service evidence.

- [ ] **Step 5: Re-evaluate the combined claim and generate prose**

Run the multi-agent audit command against both formal roots:

```bash
PYTHONPATH=src python3 src/txnmem_experiment.py multi-agent-audit \
  --controlled-root results/multi_agent_controlled_v1 \
  --storage-report results/multi_agent_real_storage_v1/results/multi_agent_storage_results.json \
  --expected-seeds 50
```

It must rewrite only the controlled `claim_status.json` and `paper_summary.md`, deriving all numbers from audited artifacts. The Markdown report must state:

- exact seeded and exhaustive denominators;
- per-variant violation and oracle-match counts;
- one minimal witness for each disabled mechanism;
- exact real-storage classifications;
- 95% Wilson intervals;
- the approved finite-model claim boundary;
- any failed gate without euphemism.

- [ ] **Step 6: Re-run final verification after artifact generation**

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src
git diff --check
git status --short
```

Read the full outputs. Confirm that only intended source, test, documentation, and new formal evidence files are staged or modified; historical untracked result directories remain untouched.

- [ ] **Step 7: Commit formal evidence only after successful audits**

If and only if both artifact audits pass, stage the two new result directories and commit:

```bash
git add results/multi_agent_controlled_v1 results/multi_agent_real_storage_v1
git commit -m "exp: add audited multi-agent correctness evidence"
```

If a gate fails, commit the complete failed evidence with an inactive claim using:

```bash
git add results/multi_agent_controlled_v1 results/multi_agent_real_storage_v1
git commit -m "exp: record multi-agent correctness findings"
```

Never change observed rows to make the first commit message applicable.
