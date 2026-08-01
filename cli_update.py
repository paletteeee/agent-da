from pathlib import Path

path = Path('/data/txnmem/src/txnmem_experiment.py')
text = path.read_text(encoding='utf-8')

imports = """
from txnmem_invariants import check_invariants as core_check_invariants
from txnmem_metrics import (
    result_row,
    summarize,
    write_repair_figure,
    write_summary,
    write_violation_figure,
)
from txnmem_schema import load_workload_config
from txnmem_simulator import VARIANTS as CORE_VARIANTS
from txnmem_simulator import run_instance as core_run_instance
from txnmem_workloads import WORKLOADS as CORE_WORKLOADS
from txnmem_workloads import generate_suite as generate_core_suite
"""
anchor = "from typing import Any, Iterable\n"
if "from txnmem_workloads import WORKLOADS as CORE_WORKLOADS" not in text:
    text = text.replace(anchor, anchor + imports, 1)

helper = """

def _run_core_instances(instances: list[dict[str, Any]], variants: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instance in instances:
        for variant in variants:
            result = core_run_instance(instance, variant)
            rows.append(result_row(instance, result))
    return rows
"""
anchor = "\n\ndef _build_parser() -> argparse.ArgumentParser:\n"
if "def _run_core_instances" not in text:
    text = text.replace(anchor, helper + anchor, 1)

parser_block = """
    experiment = subparsers.add_parser("experiment", help="generate, replay, and summarize W1-W8")
    experiment.add_argument("--config", type=Path, default=Path("configs/workload_families.yaml"))
    experiment.add_argument("--out-dir", type=Path, default=Path("."))
    experiment.add_argument("--seeds", type=int, default=10)
    experiment.add_argument("--variants", nargs="+", choices=CORE_VARIANTS, default=list(CORE_VARIANTS))
"""
anchor = """
    return parser
"""
if 'add_parser("experiment"' not in text:
    text = text.replace(anchor, parser_block + anchor, 1)

handler = """
    if args.command == "experiment":
        load_workload_config(args.config)
        instances = generate_core_suite(workloads=CORE_WORKLOADS, seeds=range(args.seeds))
        rows = _run_core_instances(instances, args.variants)
        data_path = args.out_dir / "data" / "generated_instances.jsonl"
        result_path = args.out_dir / "results" / "experiment_results.csv"
        summary_path = args.out_dir / "results" / "summary.json"
        figures_dir = args.out_dir / "results" / "figures"
        write_jsonl(instances, data_path)
        write_csv(rows, result_path)
        summary = summarize(rows, ("workload", "variant"))
        write_summary(summary, summary_path)
        write_violation_figure(summary, figures_dir / "violation_rate.svg")
        write_repair_figure(summary, figures_dir / "repair_recall.svg")
        print(f"generated {len(instances)} instances -> {data_path}")
        print(f"wrote {len(rows)} result rows -> {result_path}")
        print(f"wrote summary and figures -> {args.out_dir / 'results'}")
        return 0
"""
anchor = """
    if args.command == "generate":
"""
if 'if args.command == "experiment"' not in text:
    text = text.replace(anchor, handler + anchor, 1)

path.write_text(text, encoding='utf-8')

