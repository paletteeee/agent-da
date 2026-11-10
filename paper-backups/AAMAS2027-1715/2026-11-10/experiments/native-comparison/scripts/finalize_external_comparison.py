"""Complete per-type censoring tables and record postprocessing provenance."""
import argparse,csv,hashlib,json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"src"))
from txnmem_external_report import build,write_csv
from txnmem_external_compare import SYSTEMS,STRATEGIES
def main(out):
 audit=build(out)
 rows=[]
 for f in out.glob("*-rows.json"):rows.extend(json.loads(f.read_text()))
 rows=[r for r in rows if "discovery" in r["roles"]]
 result=[]
 for system in SYSTEMS[1:]:
  universe=sorted({v for r in rows if r["system"]==system and r["correctness_included"] for v in r["violations"]})
  for strategy in STRATEGIES:
   rs=[r for r in rows if r["system"]==system and r["strategy"]==strategy]
   valid=[r for r in rs if r["correctness_included"]]
   for violation in universe:
    hits=[r["strategy_trial_index"] for r in valid if violation in r["violations"]]
    result.append(dict(system=system,strategy=strategy,violation=violation,first_execution=min(hits) if hits else None,
       right_censored=not hits,attempted_budget=len(rs),evaluable=len(valid)))
 write_csv(out/"experiment1_first_type.csv",result)
 audit["checks"].append("per-system observed type union includes explicit censored rows for other strategies")
 audit["postprocessing_sources"]={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT/"src/txnmem_external_report.py"]}
 (out/"audit.json").write_text(json.dumps(audit,indent=2)+"\n")
 print(json.dumps(audit))
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("out",type=Path);a=p.parse_args();main(a.out)
