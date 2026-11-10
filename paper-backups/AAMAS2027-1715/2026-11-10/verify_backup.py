"""Read-only integrity and paper-result checks. Python 3.10+, standard library."""
from pathlib import Path
from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def check(condition, description):
    if not condition:
        raise ValueError(description)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    files = read(ROOT / 'metadata/file_manifest.json')['files']
    for item in files:
        p = ROOT / item['path']
        check(p.is_file(), 'Missing file: ' + item['path'])
        check(p.stat().st_size == item['size'] and sha(p) == item['sha256'],
              'Changed file: ' + item['path'])

    native = ROOT / 'experiments/native-comparison'
    out = native / 'results/external-formal-v1'
    sys.path.insert(0, str(native / 'src'))
    from txnmem_external_compare import digest, judge

    manifest = read(out / 'run_manifest.json')
    for name, expected in manifest['artifacts'].items():
        check(sha(out / name) == expected, 'Native artifact checksum: ' + name)
    cases = {x['case_id']: x for x in map(json.loads, (out / 'inputs.jsonl').read_text().splitlines())}
    for c in cases.values():
        check(digest({k: v for k, v in c.items() if k != 'input_sha256'}) == c['input_sha256'], 'Input hash')
    observations = {}
    for p in out.glob('*-observations.jsonl'):
        for row in map(json.loads, p.read_text().splitlines()):
            observations[row['system'], row['case_id']] = row
    rows = []
    for p in out.glob('*-rows.json'):
        for row in read(p):
            raw = observations[row['system'], row['case_id']]
            case = cases[row['case_id']]
            check(raw['input_sha256'] == case['input_sha256'] == row['input_sha256'], 'Paired input hash')
            check(row['observation_sha256'] == digest(raw['observation']), 'Observation hash')
            for k, value in judge(case, raw['observation']).items():
                check(row[k] == value, 'Raw judgment mismatch: ' + k)
            rows.append(row)
    check(len(rows) == len(observations) == manifest['execution_count'] == 3300, 'Native record count')
    check(len(cases) == 900, 'Distinct native inputs')
    replays = []
    for p in out.glob('*-replays.jsonl'):
        for row in map(json.loads, p.read_text().splitlines()):
            decision = judge(cases[row['case_id']], row['observation'])
            check(row['reproduced'] == bool(decision['correctness_included'] and row['target_violation'] in decision['violations']), 'Replay judgment')
            replays.append(row)
    check(len(replays) == 60 and all(x['reproduced'] for x in replays), 'Witness replay counts')

    discovery_expected = {
        'Mem0': [(200, 0), (150, 95), (150, 150)],
        'G-Memory': [(100, 0), (100, 45), (100, 100)],
        'A-MEM': [(200, 0), (200, 145), (200, 200)],
    }
    for system, values in discovery_expected.items():
        for strategy, expected in zip(['ordinary', 'random', 'directed'], values):
            selected = [r for r in rows if r['system'] == system and r['strategy'] == strategy and 'discovery' in r['roles']]
            good = [r for r in selected if r['correctness_included']]
            check(len(selected) == 300, 'Discovery budget')
            check((len(good), sum(r['any_violation'] for r in good)) == expected, 'Paper table 2 mismatch')
    descendants = {}
    for system in ['TxnMem', 'Mem0', 'A-MEM']:
        selected = [r for r in rows if r['system'] == system and r['family'] == 'cross_agent_provenance' and 'comparison_perturbed' in r['roles'] and r['correctness_included']]
        value = [sum(r['stale_descendant_count'] for r in selected), sum(r['affected_descendant_count'] for r in selected)]
        check(value == ([0, 242] if system == 'TxnMem' else [242, 242]), 'Paper table 3 descendants')
        descendants[system] = value

    controlled = ROOT / 'experiments/controlled/results/multi_agent_controlled_v1/results'
    summary = read(controlled / 'summary.json')
    totals = {}
    for section in ['seeded', 'exhaustive']:
        grouped = defaultdict(lambda: [0, 0, 0])
        for row in summary[section].values():
            g = grouped[row['variant']]
            g[0] += row['observation_count']
            g[1] += row['violations']['count']
            g[2] += row['oracle_matches']['count']
        totals[section] = dict(grouped)
    check(totals['seeded'] == {'Naive': [300, 282, 7], 'TxnMem': [300, 0, 300], 'TxnMem-NoPolicyCommit': [300, 50, 250], 'TxnMem-NoRepair': [300, 24, 276], 'TxnMem-NoTxn': [300, 232, 31]}, 'Paper table 1 seeded')
    for name, count in {'Naive': 207, 'TxnMem': 0, 'TxnMem-NoPolicyCommit': 70, 'TxnMem-NoRepair': 0, 'TxnMem-NoTxn': 203}.items():
        check(totals['exhaustive'][name][0:2] == [207, count], 'Paper table 1 exhaustive')
    check(read(controlled / 'mutation_report.json')['kill_rate'] == 0.868, 'Mutation rate')

    storage = read(ROOT / 'experiments/real-storage/results/multi_agent_real_storage_v1/results/multi_agent_storage_results.json')
    counts = Counter(r['condition'] + '/' + r['classification'] for r in storage['observations'])
    check(len(storage['observations']) == 240, 'Storage observation count')
    check(counts['TxnMem/committed_complete'] == 90 and counts['TxnMem/aborted_clean'] == 30, 'Storage outcomes')

    perf = read(ROOT / 'experiments/provenance/results/provenance_performance_v10_measurements/aggregate.json')
    check(perf['measurement_matrix']['repetition_count'] == 450, 'Performance repetitions')
    check(perf['measurement_matrix']['sample_count'] == 14400 and perf['measurement_matrix']['failed_sample_count'] == 0, 'Performance samples')
    cells = {x['cell_id']: x for x in perf['cells']}
    for key, throughput, latency in [('g100_c1', 16.825, 172.244), ('g100_c16', 19.759, 1895.605), ('g10000_c1', 0.123, 35404.604), ('g10000_c16', 0.063, 545677.946)]:
        check(abs(cells[key]['successful_throughput_ops_per_second'] - throughput) < 0.00051, 'Performance throughput')
        check(abs(cells[key]['latency_ns']['p99'] / 1e6 - latency) < 0.00051, 'Performance p99')

    public = ROOT / 'experiments/controlled/results'
    qwen = read(public / 'remaining_tasks/native_repetitions5/repetition_report.json')
    check((qwen['total_tasks'], qwen['native_event_count'], qwen['contract_success_count']) == (50, 110, 50), 'Qwen result')
    tau = read(public / 'submission_evidence/tau_bench_50/aggregate.json')
    check((tau['task_count'], tau['native_event_count'], tau['reward_sum']) == (50, 497, 15), 'Tau result')
    app = read(public / 'prompt_profile_formal_v4/appworld_tuned/native_batch_summary.json')['official']
    check((app['task_count'], app['successes'], app['pass_count'], app['total_count']) == (20, 1, 53, 112), 'AppWorld result')
    loco = read(public / 'prompt_profile_formal_v4/locomo_prompt_comparison.json')
    check(loco['paired_repetition_count'] == 3 and abs(loco['mean_f1_delta'] - 0.0016169580043333333) < 1e-12, 'LoCoMo result')

    print(json.dumps({'status': 'passed', 'source_files_verified': len(files),
        'source_bytes_verified': sum(x['size'] for x in files),
        'native_inputs': len(cases), 'native_execution_records': len(rows),
        'native_raw_judgments_recomputed': True, 'witness_replays_verified': len(replays),
        'paper_table_2_matches': True, 'paper_table_3_descendants': descendants,
        'controlled_totals': totals, 'mutation_kill_rate': 0.868,
        'real_storage_counts': dict(counts), 'performance_repetitions': 450,
        'performance_operations': 14400, 'public_integration_summary_checks': 'passed',
        'validation_scope': 'File integrity and existing experiment evidence; experiments were not rerun.'}, indent=2))


if __name__ == '__main__':
    main()
