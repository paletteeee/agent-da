"""Durable campaigns sharing executions between detection and comparison."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import select
import subprocess
import sys
import time
from txnmem_external_compare import FAMILIES, SYSTEMS, STRATEGIES, build_case, judge, summarize_rows, digest

ROOT=Path(__file__).resolve().parents[1]

def make_jobs(seeds):
    if not 1<=seeds<=50:raise ValueError("seeds must be in [1,50]")
    jobs=[]
    for system in SYSTEMS:
        for seed in range(seeds):
            for fi,family in enumerate(FAMILIES):
                for strategy in STRATEGIES:
                    if system=="TxnMem" and strategy=="random":continue
                    roles=[] if system=="TxnMem" else ["discovery"]
                    if strategy=="ordinary":roles.append("comparison_normal")
                    if strategy=="directed":roles.append("comparison_perturbed")
                    jobs.append({"system":system,"case":build_case(family,seed,strategy),
                                 "roles":roles,"trial_index":seed+1,
                                 "strategy_trial_index":seed*len(FAMILIES)+fi+1})
    return jobs

def experiment_summaries(rows):
    return {role:summarize_rows([r for r in rows if role in r["roles"]])
            for role in ("discovery","comparison_perturbed","comparison_normal")}

def write_json(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,sort_keys=True)+"\n")

class Worker:
    def __init__(self,system,out):
        self.system=system;self.out=out;self.p=None;self.calls=0
        self.log=open(out/(system.replace("-","").lower()+"-worker.log"),"a")
    def start(self):
        env=dict(os.environ)
        env.update(PYTHONPATH=str(ROOT/"src"),ANONYMIZED_TELEMETRY="False",
                   MEM0_TELEMETRY="false",TOKENIZERS_PARALLELISM="false",
                   OPENAI_API_BASE="http://127.0.0.1:9",OPENAI_API_KEY="not-a-secret-controlled-provider",
                   OMP_NUM_THREADS="1",NUMBA_NUM_THREADS="1")
        python=ROOT/(".venv-mem0" if self.system=="Mem0" else ".venv")/"bin/python"
        self.p=subprocess.Popen([str(python),str(ROOT/"src/txnmem_external_native.py"),self.system],
                 cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1,env=env)
        self.calls=0
    def close_process(self):
        if self.p:
            self.p.stdin.close()
            try:self.p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.p.terminate()
                try:self.p.wait(timeout=5)
                except subprocess.TimeoutExpired:self.p.kill();self.p.wait()
            self.p.stdout.close();self.p=None
    def run(self,case):
        if self.p is None or self.p.poll() is not None or self.calls>=50:
            self.close_process();self.start()
        try:
            self.p.stdin.write(json.dumps(case)+"\n");self.p.stdin.flush()
            ready,_,_=select.select([self.p.stdout],[],[],120)
            if not ready:
                self.close_process()
                return {"status":"runtime_error","reason":"native case timeout after 120s"}
            line=self.p.stdout.readline()
            if not line:return {"status":"runtime_error","reason":"native worker exited without observation"}
            result=json.loads(line);self.calls+=1
            return result
        except Exception as exc:
            self.close_process()
            return {"status":"runtime_error","reason":f"worker protocol: {type(exc).__name__}: {exc}"}
    def close(self):self.close_process();self.log.close()

def run_system(system,jobs,out,replays):
    worker=Worker(system,out)
    rows=[];witnesses={}
    name=system.replace("-","").lower()
    try:
        with open(out/(name+"-observations.jsonl"),"w") as raw:
            for j in jobs:
                case=j["case"];obs=worker.run(case)
                record={"system":system,"case_id":case["case_id"],"input_sha256":case["input_sha256"],"observation":obs}
                raw.write(json.dumps(record,ensure_ascii=False)+"\n");raw.flush()
                row={**judge(case,obs),"system":system,"roles":j["roles"],
                     "trial_index":j["trial_index"],"strategy_trial_index":j["strategy_trial_index"],
                     "observation_sha256":digest(obs),"input_sha256":case["input_sha256"]}
                rows.append(row)
                if "discovery" in j["roles"] and row["correctness_included"]:
                    for v in row["violations"]:
                        witnesses.setdefault(v,{"case":case,"violation":v,"observation_sha256":digest(obs)})
                if len(rows)%30==0:
                    print(json.dumps({"system":system,"completed":len(rows),"planned":len(jobs)},ensure_ascii=False),flush=True)
        replay_rows=[]
        with open(out/(name+"-replays.jsonl"),"w") as raw:
            for violation,w in sorted(witnesses.items()):
                w["minimality"]="not claimed; full recorded execution"
                w["replay_successes"]=0;w["replay_attempts"]=replays
                for repetition in range(replays):
                    obs=worker.run(w["case"]);row=judge(w["case"],obs)
                    reproduced=bool(row["correctness_included"] and violation in row["violations"])
                    w["replay_successes"]+=int(reproduced)
                    rr={"system":system,"target_violation":violation,"case_id":w["case"]["case_id"],
                        "repetition":repetition+1,"reproduced":reproduced,"observation":obs}
                    raw.write(json.dumps(rr,ensure_ascii=False)+"\n");raw.flush()
                    replay_rows.append({k:v for k,v in rr.items() if k!="observation"})
        write_json(out/(name+"-rows.json"),rows)
        write_json(out/(name+"-witnesses.json"),witnesses)
        return rows,replay_rows
    finally:worker.close()

def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--seeds",type=int,default=50)
    parser.add_argument("--replays",type=int,default=5)
    parser.add_argument("--systems",nargs="+",choices=SYSTEMS,default=list(SYSTEMS))
    args=parser.parse_args(argv)
    out=args.out.resolve()
    if out.exists():raise ValueError("output directory already exists; do not overwrite a run")
    out.mkdir(parents=True)
    jobs=[j for j in make_jobs(args.seeds) if j["system"] in args.systems]
    cases={j["case"]["case_id"]:j["case"] for j in jobs}
    with open(out/"inputs.jsonl","w") as f:
        for c in cases.values():f.write(json.dumps(c,ensure_ascii=False)+"\n")
    write_json(out/"jobs.json",[{k:v for k,v in j.items() if k!="case"}|{"case_id":j["case"]["case_id"]} for j in jobs])
    from txnmem_external_native import CAPABILITIES, PROVIDERS
    write_json(out/"capabilities.json",CAPABILITIES)
    manifest={"started_utc":datetime.now(timezone.utc).isoformat(),"status":"running",
       "seeds":args.seeds,"systems":args.systems,"replay_count":args.replays,
       "execution_count":len(jobs),"distinct_inputs":len(cases),"provider_profile":PROVIDERS,
       "inputs_sha256":hashlib.sha256((out/"inputs.jsonl").read_bytes()).hexdigest(),
       "source_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
       "source_files":{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
          for p in sorted((ROOT/"src").glob("txnmem_external_*.py"))},
       "shared_evidence":"E1 directed/ordinary native executions are reused in E2, not independent replications",
       "fault_model":"operation-boundary controller interruption; not process kill or storage recovery",
       "upstream_sources":json.loads((ROOT/"configs/external_source_manifest.json").read_text()),
       "txnmem_verification":json.loads((ROOT/"configs/txnmem_source_verification.json").read_text()),
       "pilot_overlap":"Technical pilot seeds 0,1 reused; no held-out inference",
       "sampling_scope":"Reduced normalized programs; source parameters not all exercised"}
    write_json(out/"run_manifest.json",manifest)
    env={"platform":platform.platform(),"runtimes":{}}
    for name in [".venv",".venv-mem0"]:
        py=ROOT/name/"bin/python"
        env["runtimes"][name]={"python":subprocess.check_output([str(py),"-V"],text=True).strip(),
          "packages":subprocess.check_output([str(py),"-m","pip","freeze"],text=True).splitlines()}
    write_json(out/"environment.json",env)
    rows=[];replays=[]
    with ThreadPoolExecutor(max_workers=len(args.systems)) as executor:
        futures=[executor.submit(run_system,s,[j for j in jobs if j["system"]==s],out,args.replays) for s in args.systems]
        for future in as_completed(futures):
            rs,rr=future.result();rows.extend(rs);replays.extend(rr)
    rows.sort(key=lambda r:(r["system"],r["family"],r["strategy"],r["trial_index"]))
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with open(out/"results.csv","w",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        for r in rows:writer.writerow({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict)) else v for k,v in r.items()})
    write_json(out/"summary.json",experiment_summaries(rows))
    write_json(out/"replay_summary.json",replays)
    manifest.update(status="completed",ended_utc=datetime.now(timezone.utc).isoformat(),
                    results_count=len(rows),replay_attempts=len(replays),
                    runtime_errors=sum(r["status"]=="runtime_error" for r in rows))
    manifest["artifacts"]={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir())
                           if p.is_file() and p.name!="run_manifest.json"}
    write_json(out/"run_manifest.json",manifest)
    print(json.dumps({k:manifest[k] for k in ("status","results_count","replay_attempts","runtime_errors")}),flush=True)
if __name__=="__main__":main()
