# TxnMemBench Core Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `/data/txnmem` from the three-workload pilot to a deterministic W1-W8 TxnMemBench core with schema validation, modular replay, invariant checks, metrics, summaries, and SVG figures.

**Architecture:** Keep plain Python dictionaries as the JSONL boundary and split the current simulator into focused standard-library modules. Preserve the existing `txnmem_experiment.py` CLI as a compatibility entry point while delegating generation, replay, checking, and reporting to the new modules.

**Tech Stack:** Python 3.12, `unittest`, `argparse`, `json`, `csv`, `statistics`, `xml.etree.ElementTree`/string SVG generation, and optional PyYAML with a JSON-compatible YAML fallback.

## Global Constraints

- Work only in the persistent remote project directory `/data/txnmem`.
- Do not initialize Git or discard existing remote files; the directory is not a Git repository.
- Keep the runtime dependency-free; PyYAML may be used only when already installed.
- Preserve the existing public functions `generate_instance`, `run_instance`, `check_invariants`, `run_suite`, `write_jsonl`, and `write_csv`.
- Preserve existing pilot workload names, variant names, JSONL fields, CSV fields, and CLI commands.
- Every production behavior change must have a failing `unittest` before implementation.
- Generated files must use deterministic ordering and seed-controlled randomness.
- All tests and validation commands run on the remote server from `/data/txnmem`.

---

### Task 1: Capture a clean baseline and add the modular package boundary

**Files:**
- Create: `tests/test_baseline_contract.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: existing `src/txnmem_experiment.py` functions and CLI.
- Produces: executable baseline checks that prevent accidental changes while modules are extracted.

- [ ] **Step 1: Write the failing contract test**

```python
def test_pilot_contract_stays_available():
    instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})
    assert instance["instance_id"] == "atomic_multi_write_seed_0"
    assert run_instance(instance, "TxnMem")["transaction_state"] == "aborted"
    assert check_invariants(instance, run_instance(instance, "TxnMem")) == []
```

- [ ] **Step 2: Run the test and confirm it exercises the existing implementation**

Run: `python3 -m unittest discover -s tests -v`

Expected: the existing six pilot tests and the new contract test pass before any extraction.

- [ ] **Step 3: Record the baseline CLI outputs**

Run: `python3 src/txnmem_experiment.py pilot --out-dir /tmp/txnmem-baseline --seeds 2`

Expected: 6 JSONL instances and 30 CSV rows are created under `/tmp/txnmem-baseline`.

- [ ] **Step 4: Document the compatibility boundary**

Update `README.md` with the existing public functions, the preserved CLI commands, and the new planned output locations without changing the current pilot command examples.

- [ ] **Step 5: Re-run the baseline test suite**

Run: `python3 -m unittest discover -s tests -v`

Expected: all tests pass with no new warnings.

---

### Task 2: Implement schema validation and workload configuration loading

**Files:**
- Create: `src/txnmem_schema.py`
- Create: `tests/test_txnmem_schema.py`
- Create: `configs/workload_families.yaml`

**Interfaces:**
- `validate_instance(instance: dict[str, Any]) -> None`
- `load_workload_config(path: Path) -> dict[str, Any]`
- `DEFAULT_CONFIG: dict[str, Any]`
- `REQUIRED_INSTANCE_KEYS: tuple[str, ...]`

- [ ] **Step 1: Write failing validation tests**

```python
def test_validate_instance_rejects_missing_top_level_key():
    instance = {"workload": "atomic_multi_write"}
    with self.assertRaises(ValueError):
        validate_instance(instance)

def test_validate_instance_rejects_unknown_provenance_reference():
    instance = minimal_instance()
    instance["provenance_edges"] = [{"source_id": "missing", "derived_id": "m1", "relation": "derived_from"}]
    with self.assertRaises(ValueError):
        validate_instance(instance)

def test_load_workload_config_returns_w1_to_w8_definitions():
    config = load_workload_config(ROOT / "configs" / "workload_families.yaml")
    self.assertEqual(set(config["workloads"]), set(WORKLOADS))
```

- [ ] **Step 2: Run the schema tests to verify the expected failures**

Run: `python3 -m unittest tests.test_txnmem_schema -v`

Expected: import or missing-function failures for the new module.

- [ ] **Step 3: Implement the schema contract**

Implement top-level key checks, memory ID uniqueness, operation step monotonicity, transaction references, policy fields, and provenance source/derived references. Use `json.loads` when the configuration file is valid JSON-compatible YAML; use `yaml.safe_load` only if the optional module is available.

- [ ] **Step 4: Add the explicit workload configuration**

Create `configs/workload_families.yaml` with eight entries. Each entry must include `workload`, `target_invariant`, `failure_types`, and numeric parameter ranges for `txn_size`, `provenance_depth`, `branch_factor`, `policy_churn`, and `concurrency`. Keep the file JSON-compatible so the standard-library fallback can load it.

- [ ] **Step 5: Run the schema tests and full existing tests**

Run: `python3 -m unittest tests.test_txnmem_schema tests.test_txnmem_experiment -v`

Expected: all schema and pilot tests pass.

---

### Task 3: Extract and extend deterministic W1-W8 workload generation

**Files:**
- Create: `src/txnmem_workloads.py`
- Create: `tests/test_txnmem_workloads.py`
- Modify: `src/txnmem_experiment.py`

**Interfaces:**
- `WORKLOADS: tuple[str, ...]` containing W1-W8 names.
- `generate_instance(workload: str, seed: int, config: dict[str, Any] | None = None) -> dict[str, Any]`
- `generate_suite(workloads: Iterable[str], seeds: Iterable[int], config: dict[str, Any] | None = None) -> list[dict[str, Any]]`

- [ ] **Step 1: Write failing W4-W8 generation tests**

```python
def test_scope_bypass_contains_search_and_direct_id_read():
    instance = generate_instance("scope_bypass", seed=10)
    types = [operation["type"] for operation in instance["operations"]]
    self.assertIn("search", types)
    self.assertIn("get_by_id", types)

def test_branch_repair_has_all_branch_edges():
    instance = generate_instance("provenance_branch_repair", seed=11, config={"branch_factor": 3, "provenance_depth": 2})
    self.assertEqual(len(instance["provenance_edges"]), 6)

def test_generation_is_deterministic_for_all_workloads():
    for workload in WORKLOADS:
        self.assertEqual(generate_instance(workload, 13), generate_instance(workload, 13))
```

- [ ] **Step 2: Run the workload tests and verify they fail for missing workload support**

Run: `python3 -m unittest tests.test_txnmem_workloads -v`

Expected: failures because W4-W8 are not present in the current pilot.

- [ ] **Step 3: Move shared helpers and add W2**

Move `_memory`, `_merged_config`, and deterministic agent selection into `txnmem_workloads.py`. Add `crash_during_commit` with a commit-boundary crash event and an expected atomic outcome.

- [ ] **Step 4: Implement W4 and W5 instance generation**

Generate scoped memories and policy labels for `scope_bypass`, including one allowed search and one unauthorized direct-id read. Generate old/new memory pairs for `supersession_consistency`, with reciprocal metadata needed by the checker.

- [ ] **Step 5: Implement W7 and W8 instance generation**

Generate a root with `branch_factor` descendants at each requested depth for `provenance_branch_repair`. Generate `mixed_stress` by combining a multi-write transaction, a policy revoke, a crash event, and a root invalidation with deterministic step ordering.

- [ ] **Step 6: Preserve the old generator import path**

Make `src/txnmem_experiment.py` import and re-export `WORKLOADS`, `generate_instance`, and the default configuration from `txnmem_workloads.py`.

- [ ] **Step 7: Run workload and pilot tests**

Run: `python3 -m unittest tests.test_txnmem_workloads tests.test_txnmem_experiment -v`

Expected: all W1-W8 generation tests and all existing pilot tests pass.

---

### Task 4: Extract the replay engine and implement W2/W4/W5/W7/W8 semantics

**Files:**
- Create: `src/txnmem_simulator.py`
- Create: `tests/test_txnmem_simulator.py`
- Modify: `src/txnmem_experiment.py`

**Interfaces:**
- `VARIANTS: tuple[str, ...]`
- `run_instance(instance: dict[str, Any], variant: str) -> dict[str, Any]`
- Result keys: `variant`, `transaction_state`, `final_memories`, `committed_memory_ids`, `trace`, and `metrics`.

- [ ] **Step 1: Write failing replay tests**

```python
def test_scope_bypass_denies_direct_id_read_in_txnmem():
    result = run_instance(generate_instance("scope_bypass", 20), "TxnMem")
    self.assertEqual(result["metrics"]["exposed_memory_ids"], [])

def test_supersession_updates_old_and_new_records():
    result = run_instance(generate_instance("supersession_consistency", 21), "TxnMem")
    old = result["final_memories"]["m_old"]
    new = result["final_memories"]["m_new"]
    self.assertEqual(new["supersedes_id"], "m_old")
    self.assertEqual(old["status"], "superseded")

def test_branch_repair_invalidates_every_descendant():
    instance = generate_instance("provenance_branch_repair", 22, {"branch_factor": 2, "provenance_depth": 3})
    result = run_instance(instance, "TxnMem")
    self.assertTrue(all(memory["status"] == "invalid" for memory in result["final_memories"].values()))
```

- [ ] **Step 2: Run simulator tests and confirm missing behavior failures**

Run: `python3 -m unittest tests.test_txnmem_simulator -v`

Expected: failures for the unimplemented workload types or missing result fields.

- [ ] **Step 3: Move the existing W1/W3/W6 replay logic**

Move transaction buffering, policy-version revalidation, crash handling, and chain repair into `txnmem_simulator.py` without changing the existing result dictionary.

- [ ] **Step 4: Implement W2 commit-boundary recovery**

When a crash event targets commit, clear or atomically apply the transaction buffer according to the variant. TxnMem must return either `aborted` with zero committed IDs or `committed` with the full transaction size.

- [ ] **Step 5: Implement scope enforcement and read traces**

For `search`, filter by policy scope. For `get_by_id`, require the same scope check and record `denied_read` without adding the memory ID to `exposed_memory_ids`.

- [ ] **Step 6: Implement supersession and mixed replay**

For `supersede`, mark the old memory `superseded`, set the new memory's `supersedes_id`, and make the ablation that lacks transaction semantics expose an inconsistent intermediate state when the scheduled failure requires it. Replay W8 through the same event loop, preserving trace order.

- [ ] **Step 7: Preserve the old replay import path and run tests**

Run: `python3 -m unittest tests.test_txnmem_simulator tests.test_txnmem_experiment -v`

Expected: simulator and all pilot tests pass.

---

### Task 5: Implement invariant checkers for all W1-W8

**Files:**
- Create: `src/txnmem_invariants.py`
- Create: `tests/test_txnmem_invariants.py`
- Modify: `src/txnmem_experiment.py`

**Interfaces:**
- `check_invariants(instance: dict[str, Any], result: dict[str, Any]) -> list[str]`
- Stable violation names: `atomicity_violation`, `unexpected_commit`,
  `invalid_commit_violation`, `stale_write_violation`, `scope_leak_violation`,
  `supersession_consistency_violation`, `provenance_closure_violation`, and
  `recovery_consistency_violation`.

- [ ] **Step 1: Write one failing checker test per new invariant**

```python
def test_scope_leak_is_reported():
    instance = generate_instance("scope_bypass", 30)
    result = run_instance(instance, "Naive")
    self.assertIn("scope_leak_violation", check_invariants(instance, result))

def test_txnmem_has_no_scope_violation():
    instance = generate_instance("scope_bypass", 31)
    self.assertEqual(check_invariants(instance, run_instance(instance, "TxnMem")), [])

def test_supersession_violation_is_reported_when_old_memory_remains_active():
    instance = generate_instance("supersession_consistency", 32)
    result = run_instance(instance, "TxnMem-NoTxn")
    self.assertIn("supersession_consistency_violation", check_invariants(instance, result))
```

- [ ] **Step 2: Run checker tests to verify the expected failures**

Run: `python3 -m unittest tests.test_txnmem_invariants -v`

Expected: new invariant names are absent or the new workload paths fail.

- [ ] **Step 3: Implement stable invariant checks**

Check committed counts against expected transaction sizes, commit authorization against the expected abort state, exposed IDs against policy scope, reciprocal supersession metadata, and active descendants of invalid roots. Deduplicate violations while preserving a stable order.

- [ ] **Step 4: Re-run all unit tests**

Run: `python3 -m unittest discover -s tests -v`

Expected: every test passes.

---

### Task 6: Add metrics, summaries, and SVG figures

**Files:**
- Create: `src/txnmem_metrics.py`
- Create: `tests/test_txnmem_metrics.py`

**Interfaces:**
- `result_row(instance: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]`
- `summarize(rows: Iterable[dict[str, Any]], group_keys: tuple[str, ...]) -> dict[str, Any]`
- `write_summary(summary: dict[str, Any], path: Path) -> None`
- `write_violation_figure(summary: dict[str, Any], path: Path) -> None`
- `write_repair_figure(summary: dict[str, Any], path: Path) -> None`

- [ ] **Step 1: Write failing metric tests**

```python
def test_summary_contains_mean_and_population_std():
    summary = summarize([{"workload": "w", "variant": "v", "leak_rate": 0.0}, {"workload": "w", "variant": "v", "leak_rate": 1.0}], ("workload", "variant"))
    stats = summary["groups"]["w/v"]["leak_rate"]
    self.assertEqual(stats["mean"], 0.5)
    self.assertEqual(stats["std"], 0.5)

def test_svg_figure_has_svg_root():
    path = ROOT / "results" / "figures" / "test.svg"
    write_violation_figure({"groups": {}}, path)
    self.assertTrue(path.read_text().startswith("<svg"))
```

- [ ] **Step 2: Run metric tests and confirm they fail**

Run: `python3 -m unittest tests.test_txnmem_metrics -v`

Expected: missing-module or missing-function failures.

- [ ] **Step 3: Implement per-result metrics**

Map invariant names to rates, calculate `repair_recall`, `leak_rate`, `scope_bypass_rate`, `supersession_consistency`, and deterministic operation-count latency. Keep existing CSV field names and append new fields.

- [ ] **Step 4: Implement group summaries**

Group rows by workload and variant, calculate count/mean/population standard deviation for every numeric metric, and serialize sorted keys with `json.dump(..., sort_keys=True)`.

- [ ] **Step 5: Implement dependency-free SVG output**

Write valid SVG bar charts with labels for violation rate and repair recall. Escape labels and create parent directories before writing.

- [ ] **Step 6: Run metrics and full unit tests**

Run: `python3 -m unittest discover -s tests -v`

Expected: all tests pass.

---

### Task 7: Wire the CLI, documentation, and full artifact generation

**Files:**
- Modify: `src/txnmem_experiment.py`
- Modify: `README.md`
- Create: `docs/dataset_schema.md`
- Create: `tests/test_cli_outputs.py`

**Interfaces:**
- Preserve commands `generate`, `run`, and `pilot`.
- Add command `experiment --config configs/workload_families.yaml --out-dir . --seeds N`.
- `experiment` writes `data/generated_instances.jsonl`, `results/experiment_results.csv`, `results/summary.json`, and `results/figures/*.svg`.

- [ ] **Step 1: Write failing CLI output tests**

```python
def test_experiment_command_writes_all_artifacts():
    with TemporaryDirectory() as tmp:
        completed = subprocess.run([sys.executable, "src/txnmem_experiment.py", "experiment", "--out-dir", tmp, "--seeds", "1"], check=True, capture_output=True, text=True)
        self.assertIn("wrote", completed.stdout)
        self.assertTrue((Path(tmp) / "data/generated_instances.jsonl").exists())
        self.assertTrue((Path(tmp) / "results/summary.json").exists())
        self.assertTrue(list((Path(tmp) / "results/figures").glob("*.svg")))
```

- [ ] **Step 2: Run the CLI test and confirm the command is missing**

Run: `python3 -m unittest tests.test_cli_outputs -v`

Expected: argparse rejects the new command or expected artifacts are absent.

- [ ] **Step 3: Wire generation, replay, metrics, and output paths**

Add the `experiment` subcommand, load the workload config, generate W1-W8 instances, replay all variants, write JSONL/CSV, summarize rows, and write both SVG figures.

- [ ] **Step 4: Write the schema documentation**

Document every top-level instance field, memory field, operation type, failure event, invariant name, variant name, and CSV/summary field in `docs/dataset_schema.md`.

- [ ] **Step 5: Update README run instructions**

Add exact commands for `generate`, `run`, `pilot`, and `experiment`, including the persistent remote path `/data/txnmem` and output artifact list.

- [ ] **Step 6: Run CLI and full tests**

Run:

```bash
python3 -m unittest discover -s tests -v
python3 src/txnmem_experiment.py experiment --out-dir /data/txnmem --seeds 2
```

Expected: all tests pass; 16 instances, 80 result rows, one summary JSON, and two SVG figures are written.

---

### Task 8: Final acceptance and remote handoff

**Files:**
- Verify: `src/*.py`, `tests/*.py`, `configs/workload_families.yaml`, `docs/dataset_schema.md`
- Verify: `data/generated_instances.jsonl`, `results/experiment_results.csv`, `results/summary.json`, `results/figures/*.svg`

- [ ] **Step 1: Run the complete verification suite**

Run:

```bash
cd /data/txnmem
python3 -m unittest discover -s tests -v
python3 src/txnmem_experiment.py experiment --out-dir /data/txnmem --seeds 10
python3 - <<'PY'
import csv, json
from pathlib import Path
root = Path('/data/txnmem')
instances = [line for line in (root / 'data/generated_instances.jsonl').read_text().splitlines() if line]
rows = list(csv.DictReader((root / 'results/experiment_results.csv').open()))
summary = json.loads((root / 'results/summary.json').read_text())
assert len(instances) == 80
assert len(rows) == 400
assert summary['groups']
assert list((root / 'results/figures').glob('*.svg'))
print(f'verified instances={len(instances)} rows={len(rows)} groups={len(summary["groups"])}')
PY
```

Expected: exit code 0, all tests pass, 80 instances, 400 rows, non-empty summary groups, and SVG files present.

- [ ] **Step 2: Inspect generated schema and sample outputs**

Run: `sed -n '1,2p' data/generated_instances.jsonl; sed -n '1,3p' results/experiment_results.csv; python3 -m json.tool results/summary.json >/dev/null`

Expected: valid JSONL, CSV headers include the new metrics, and summary JSON parses.

- [ ] **Step 3: Report the remote handoff**

Report the remote project path, test command and result, generated artifact paths, and that the SSH session remains open or must be re-established. Do not claim completion without the fresh command output from Step 1.

---

## Review checkpoints

- After Task 2: schema and config can validate a generated pilot instance.
- After Task 4: every W1-W8 can generate and replay through one simulator interface.
- After Task 6: metrics can be computed without the CLI.
- After Task 7: the full experiment command creates all requested artifacts.
- After Task 8: only verified outputs are reported.

