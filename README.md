# TxnMem Pilot Experiment

This repository now contains two research prototypes:

- **TxnMem** explores transactional shared memory for multi-agent systems.
- **InferTxn** applies MVCC and two-phase commit to atomic metadata migration
  in prefill-decode disaggregated LLM inference.

## InferTxn: transactional inference metadata

InferTxn models route metadata, KV-cache metadata, and request ownership as
three independent database shards. A migration uses snapshot-isolated reads,
first-committer-wins conflict detection, and 2PC so the three records either
move to the target decode node together or remain at the source.

Run all InferTxn tests:

```bash
python3 -m unittest discover -s tests -p 'test_infertxn*.py' -v
```

Run the three-process HTTP demonstration:

```bash
python3 -m src.infertxn.demo
```

The JSON result should report `"state": "committed"` and
`"consistent": true`. See [docs/infertxn.md](docs/infertxn.md) for the
protocol, failure model, limitations, and code map.

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
