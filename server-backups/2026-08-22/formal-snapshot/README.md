# TxnMem Pilot Experiment

This is a dependency-free pilot for the first three controlled workloads:

- `atomic_multi_write` (W1): a crash after the first write;
- `revoke_before_commit` (W3): a write permission revoked before commit;
- `provenance_chain_repair` (W6): an invalid source memory and its derived chain.

The reference variants are `Naive`, `TxnMem-NoTxn`, `TxnMem-NoPolicyCommit`,
`TxnMem-NoRepair`, and `TxnMem`. The simulator is intended to
validate the dataset format and invariant checker before connecting a real
memory backend or LLM workflow.

## Run locally or remotely

From the project root:

```bash
python3 -m unittest discover -s tests -v
python3 src/txnmem_experiment.py pilot --out-dir . --seeds 10
```

Outputs:

```text
data/pilot_instances.jsonl
results/pilot_results.csv
```

Generate instances without replaying them:

```bash
python3 src/txnmem_experiment.py generate \
  --out data/pilot_instances.jsonl \
  --seeds 10
```

Replay a saved instance file:

```bash
python3 src/txnmem_experiment.py run \
  --instances data/pilot_instances.jsonl \
  --results results/pilot_results.csv
```

The CSV records `partial_update_rate`, `invalid_commit_rate`,
`stale_write_rate`, `repair_recall`, `any_violation`, and the exact violation names for every
instance/variant pair.

## Remote placement

Place the files under the persistent directory `/data/txnmem` and run the
commands above from that directory. Keep generated data, results, and logs
under `/data/txnmem` so they survive container replacement.

## TxnMemBench Core Experiment

The pilot commands remain available for the original three workloads. The core
experiment command generates the W1-W8 controlled dataset, replays every
variant, and writes the experiment artifacts.

From /data/txnmem:

~~~bash
python3 -m unittest discover -s tests -v
python3 src/txnmem_experiment.py experiment --out-dir /data/txnmem --seeds 10
~~~

Optional arguments:

~~~bash
python3 src/txnmem_experiment.py experiment --config configs/workload_families.yaml --out-dir /data/txnmem --seeds 10 --variants Naive TxnMem-NoTxn TxnMem-NoPolicyCommit TxnMem-NoRepair TxnMem
~~~

The command writes:

- data/generated_instances.jsonl
- results/experiment_results.csv
- results/summary.json
- results/figures/violation_rate.svg
- results/figures/repair_recall.svg

The persistent directories for code, generated data, results, and logs are
/root and /data. Keep experiment artifacts under /data/txnmem.

## External-memory baselines

The optional Mem0 and persistent LangGraph Store baselines use the exact
versions pinned in [`requirements-baselines.txt`](requirements-baselines.txt).
On the isolated remote worktree, create the environment and verify both the
adapter imports and existing regression suite:

```bash
cd /data/agent-da-baselines-worktree
python3 -m venv .venv-baselines
.venv-baselines/bin/python -m pip install --upgrade pip
.venv-baselines/bin/pip install -r requirements-baselines.txt
.venv-baselines/bin/python -m unittest tests.test_external_dependencies -v
.venv-baselines/bin/python -m unittest discover -s tests -v
.venv-baselines/bin/pip freeze
```

The native-operation mapping, including explicit capability limits, is in
[`docs/external_baseline_protocol.md`](docs/external_baseline_protocol.md).

### Reproducible external-baseline replay

Replay a validated JSONL instance set against a fixed-order subset of the
controlled and optional native baselines:

```bash
python3 src/txnmem_external_experiment.py run \
  --instances data/generated_instances.jsonl \
  --out-dir results/external_baselines \
  --adapters AppendOnly LastWriteWins MetadataFiltered \
  --run-id controlled-20260821 \
  --mem0-state-root /data/txnmem/mem0-state
```

The selected adapters always run in this registry order: `AppendOnly`,
`LastWriteWins`, `MetadataFiltered`, `Mem0`, `LangGraphStore`. The runner
publishes `results.csv`, `summary.json`, capability tables, environment and
error logs, and a hash-bound `run_manifest.json`. Invalid JSON/schema input or
duplicate instance IDs is rejected before those artifacts are published.

`--mem0-state-root` is optional; when supplied, the runner creates exactly one
new `<run-id>` directory below that base and rejects an unsafe ID or an
existing directory. `Mem0` uses this fresh per-run embedded state root.
`LangGraphStore` reads a
PostgreSQL DSN only from `TXNMEM_LANGGRAPH_POSTGRES_DSN`; it is never accepted
as a command-line argument or recorded in artifacts. If PostgreSQL preflight
does not succeed, normal workloads use official `InMemoryStore` fallback and
crash-scheduled workloads are emitted as excluded `unsupported_mapping` rows,
not recovery scores. If LangGraph is not installed at all, it remains selected
and produces one redacted excluded `runtime_error` row per attempted instance.

The capability artifacts use one ordered eight-column semantic matrix for all
five formal adapters: `single_record_read_write`,
`atomic_multi_record_commit`, `commit_policy_revalidation`,
`shared_scope_isolation`, `version_supersession`,
`provenance_propagation`, `recursive_provenance_invalidation`, and
`crash_recovery`. Backend-specific API details remain in the capability
`detail` field; a positive capability does not replace oracle-based correctness
judgment. In particular, Mem0's formal deterministic factory can be closed and
reopened to observe persistent state, but this does not claim atomic recovery.
