from pathlib import Path

path = Path('/data/txnmem/README.md')
text = path.read_text(encoding='utf-8')
marker = '## TxnMemBench Core Experiment'
section = '''
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
'''
if marker not in text:
    path.write_text(text.rstrip() + '\n\n' + section.strip() + '\n', encoding='utf-8')

