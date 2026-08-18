### Task 3: Add controlled saturation, diversity, manifest and strict evidence audit

**Files:**
- Modify: `src/txnmem_statistics.py`
- Modify: `src/txnmem_metrics.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `src/txnmem_claim_audit.py`
- Modify: `src/txnmem_artifact_audit.py`
- Modify: `tests/test_txnmem_statistics.py`
- Modify: `tests/test_txnmem_claim_audit.py`
- Modify: `tests/test_txnmem_artifact_audit.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**
- Produces: `controlled_violation_saturation(rows, checkpoints, confidence=0.95)` and `controlled_diversity(instances)`.
- Experiment writes `run_manifest.json`, `saturation.json`, `diversity.json`, and `saturation.svg`.

- [ ] **Step 1: Write failing tests for balanced nested prefixes**

Create rows for two families, two variants and seeds 0–3. Assert checkpoints select equal seed prefixes per family, Wilson intervals are deterministic, and duplicate/missing family×seed×variant rows raise `ValueError`.

- [ ] **Step 2: Verify RED**

Run statistics/claim/CLI tests; expect missing interfaces/artifacts.

- [ ] **Step 3: Implement the fail-closed aggregators and SVG writer**

The saturation report includes checkpoint seed count, instance count, variant, violations, rate, interval, oracle matches and interval. Diversity includes family, instance count, unique fingerprints, parameter value counts and coverage ratios.

- [ ] **Step 4: Add precise public allowlist for only controlled synthetic/oracle files**

Keep sensitive-key scanning active and reject lookalike paths, nested raw files and any public benchmark raw trace.

- [ ] **Step 5: Verify GREEN and deterministic two-run hashes**

Run 200 seeds twice into two `/tmp` paths; compare all canonical JSONL/CSV/JSON bytes except a manifest field explicitly designed to vary. No wall-clock time or absolute path may enter canonical files.

- [ ] **Step 6: Commit**

Commit message: `feat: add controlled evidence saturation and diversity`

#### Review fix round 1 — fail-closed domains, source containment, and claim bundle

Bind saturation to the exact approved family and variant domains; observed rows
may not define their own expected cube. Bind every family to its exact approved
semantic-parameter set and verify metadata equals executable config. A scaled
controlled claim must use the exact validation profile and a complete six-
artifact bundle with strict schemas, arithmetic, canonical relative paths,
hash closure, oracle/config/domain identity, and component containment in an
existing declared Git commit. Formal generation records whether all relevant
source/config bytes are contained in that commit and supports a fail-closed
formal-source gate. Artifact audit rejects public raw-capable paths globally
and schema-validates every controlled raw exception.

#### Review fix round 2 — unbypassable scale signal and closed artifact truth

Do not infer scaled evidence only from an artifact filename or optional profile.
Any active claim declaring the registered controlled domains, formal counts, or
controlled-scale identity must require the exact profile and six-artifact
bundle. Recompute diversity/coverage from `generated_instances.jsonl`, compare
it to `diversity.json`, retain all existing result/oracle cross-checks, and
parse `saturation.svg` as a genuine SVG document. Reject public raw-capable
path components such as `payloads`, `conversations`, and `transcripts` anywhere
in the repository. Controlled raw exceptions must recursively enforce their
closed synthetic schema, including nested `config` objects, so a benchmark
customer/conversation payload cannot be hidden below an allowed top-level key.

#### Review fix round 3 — recursive scale identity, regenerated oracle truth, and closed records

Recognize formal scale signatures recursively in the active artifact, independent
of field names and nesting. The claim's primary `artifact_path` and assertions
must be bound to the declared six-artifact closure; an unrelated seventh
artifact cannot carry a scaled assertion. Regenerate each reference oracle from
its generated instance with the independent reference semantics, require exact
canonical equality and a non-empty allowed-outcome set, and reconcile result
oracle fields instead of trusting CSV flags. Raw-capable ancestor directories
always fail even when the basename is a nominally safe aggregate. Replace
generic nested scalar/list acceptance for controlled instance and oracle JSONL
with explicit recursive schemas for operations, policies, schedules,
provenance, allowed outcomes, and event traces; reject dialogue/payload fields
hidden in any executable or trace container.
