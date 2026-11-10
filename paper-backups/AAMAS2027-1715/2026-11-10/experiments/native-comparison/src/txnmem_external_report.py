"""Audit raw native evidence and build the two experiment tables."""
import argparse,csv,hashlib,json
from collections import Counter
from pathlib import Path
from txnmem_external_compare import FAMILIES,SYSTEMS,STRATEGIES,digest,judge
LABELS=dict(zip(FAMILIES,["共享可见性","交错原子性","并发版本替换","执行者策略与作用域","跨智能体来源传播","混合工作负载"]))
def load(p):return json.loads(p.read_text())
def pct(n,d):return f"{n}/{d} ({100*n/d:.1f}%)" if d else "NA"
def table(headers,rows):
 return "\n".join(["| "+" | ".join(headers)+" |","|"+"|".join(["---"]*len(headers))+"|"]+["| "+" | ".join(str(x) for x in r)+" |" for r in rows])
def write_csv(p,rows):
 if rows:
  with p.open("w",newline="") as f:
   w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
def build(out):
 m=load(out/"run_manifest.json");assert m["status"]=="completed"
 for name,h in m["artifacts"].items():assert hashlib.sha256((out/name).read_bytes()).hexdigest()==h,name
 inputs={r["case_id"]:r for r in map(json.loads,(out/"inputs.jsonl").read_text().splitlines())}
 for c in inputs.values():assert digest({k:v for k,v in c.items() if k!="input_sha256"})==c["input_sha256"]
 raw={}
 for f in out.glob("*-observations.jsonl"):
  for r in map(json.loads,f.read_text().splitlines()):raw[r["system"],r["case_id"]]=r
 rows=[]
 for f in out.glob("*-rows.json"):
  for r in load(f):
   record=raw[r["system"],r["case_id"]];case=inputs[r["case_id"]]
   assert record["input_sha256"]==case["input_sha256"]==r["input_sha256"]
   assert r["observation_sha256"]==digest(record["observation"])
   for k,v in judge(case,record["observation"]).items():assert r[k]==v,(r["case_id"],k)
   rows.append(r)
 assert len(rows)==len(raw)==m["execution_count"]
 replay=[]
 for f in out.glob("*-replays.jsonl"):
  for r in map(json.loads,f.read_text().splitlines()):
   j=judge(inputs[r["case_id"]],r["observation"])
   assert r["reproduced"]==bool(j["correctness_included"] and r["target_violation"] in j["violations"])
   replay.append(r)
 assert len(replay)==m["replay_attempts"]
 disc=[r for r in rows if "discovery" in r["roles"]]
 detection=[];first=[];rep=[];comparison=[];paired=[];shapes=[]
 for s in SYSTEMS[1:]:
  for strategy in STRATEGIES:
   rs=[r for r in disc if r["system"]==s and r["strategy"]==strategy]
   good=[r for r in rs if r["correctness_included"]]
   types=sorted({v for r in good for v in r["violations"]})
   hits=[r["strategy_trial_index"] for r in good if r["any_violation"]]
   detection.append(dict(system=s,strategy=strategy,attempted=len(rs),evaluated=len(good),violations=sum(r["any_violation"] for r in good),
     first_execution=min(hits) if hits else None,types=";".join(types),
     effective_faults=sum(bool(raw[s,r["case_id"]]["observation"].get("fault_effective")) for r in rs),
     excluded=json.dumps(dict(Counter(r["status"] for r in rs if not r["correctness_included"])),sort_keys=True)))
   for v in types:first.append(dict(system=s,strategy=strategy,violation=v,first_execution=min(r["strategy_trial_index"] for r in good if v in r["violations"])))
  for v in sorted({r["target_violation"] for r in replay if r["system"]==s}):
   rr=[r for r in replay if r["system"]==s and r["target_violation"]==v]
   rep.append(dict(system=s,violation=v,successes=sum(r["reproduced"] for r in rr),attempts=len(rr)))
 for family in FAMILIES:
  for strategy in STRATEGIES:
   cs=[c for c in inputs.values() if c["family"]==family and c["strategy"]==strategy]
   shapes.append(dict(family=family,strategy=strategy,executions=len(cs),unique_executed_shapes=len({c.get("executed_structure_sha256",c["input_sha256"]) for c in cs})))
  for s in SYSTEMS:
   rs=[r for r in rows if r["system"]==s and r["family"]==family and "comparison_perturbed" in r["roles"]]
   good=[r for r in rs if r["correctness_included"]]
   normal=[r for r in rows if r["system"]==s and r["family"]==family and "comparison_normal" in r["roles"] and r["correctness_included"]]
   comparison.append(dict(system=s,family=family,attempted=len(rs),evaluated=len(good),violations=sum(r["any_violation"] for r in good),
     normal_completed=sum(r["normal_completed"] for r in normal),normal_cases=len(normal),
     false_aborts=sum(r["false_abort_count"] for r in normal),normal_tasks=sum(r["normal_task_count"] for r in normal),
     stale=sum(r["stale_descendant_count"] for r in good),affected=sum(r["affected_descendant_count"] for r in good),
     excluded=json.dumps(dict(Counter(r["status"] for r in rs if not r["correctness_included"])),sort_keys=True)))
  tx={r["input_sha256"]:r for r in rows if r["system"]=="TxnMem" and r["family"]==family and "comparison_perturbed" in r["roles"] and r["correctness_included"]}
  for s in SYSTEMS[1:]:
   bs={r["input_sha256"]:r for r in rows if r["system"]==s and r["family"]==family and "comparison_perturbed" in r["roles"] and r["correctness_included"]}
   shared=sorted(set(tx)&set(bs))
   paired.append(dict(system=s,family=family,paired_cases=len(shared),txnmem_violations=sum(tx[k]["any_violation"] for k in shared),
     baseline_violations=sum(bs[k]["any_violation"] for k in shared),
     baseline_only=sum(bs[k]["any_violation"]==1 and tx[k]["any_violation"]==0 for k in shared),
     txnmem_only=sum(tx[k]["any_violation"]==1 and bs[k]["any_violation"]==0 for k in shared)))
 for name,data in [("experiment1_detection",detection),("experiment1_first_type",first),("experiment1_replay",rep),("experiment2_comparison",comparison),("experiment2_paired",paired),("effective_workload_shapes",shapes)]:
  write_csv(out/(name+".csv"),data)
 text=["# TxnMemBench 原生记忆系统对比实验报告","",
  "本报告由实际原生接口调用、事件与独立判定器生成。结论限于受控操作配置下的应用级一致性契约，不代表第三方系统承诺事务语义，也不代表默认 LLM 工作流质量。","",
  f"每类 {m['seeds']} 次执行；共 {len(rows)} 条执行记录、{len(inputs)} 个不同输入、{len(replay)} 次额外见证重放。E1 普通/定向执行复用于 E2，不能算作两份独立证据。","",
  "## 配置与可比边界","",
  "- TxnMem：仓库 MultiAgentStorageCoordinator 与真实 SQLite。Mem0 2.0.18：原生 Memory/Qdrant。A-MEM：原生 note/Chroma。G-Memory：原生任务记忆写入和检索。",
  "- 固定 64 维确定性嵌入。Mem0 infer=False；A-MEM 模型固定返回不演化；G-Memory 模型固定评分，检索 top-k=50、threshold=-1。未覆盖自动抽取、自动演化或默认语义检索质量。",
  "- 扰动在 API 边界打断逻辑任务，或删除/失效来源；没有杀进程或提交内部并发。TxnMem commit 是同步整体调用。",
  "- 六类名称和采样参数来自既有生成器，但使用了公开操作可映射的简化程序。不是原有内部轨迹的逐条复用。实际结构哈希、有效参数及去重数量均留档；版本竞争和策略类可能只有一种结构，不得声称每类 50 种结构。",
  "- 普通测试是无故障串行控制；随机测试随机交错及扰动边界；定向测试命中契约关键边界。两者存在故障覆盖差异，不能据此证明同等故障预算下的搜索算法优势。随机选择在 commit 之后的中断不生效，effective_faults 单列。",
  "- 三个基线不支持策略/混合映射；G-Memory 另无版本删除与来源失效映射。记 NA，不当通过或失败。四系统共同范围只有可见性与交错原子性。",
  "- 技术试跑种子 0、1 包含在正式 0–49 中，没有留出集；没有根据结果筛选正式种子。","",
  "## 实验一：第三方系统问题发现","",
  table(["系统","方式","可判定/尝试","违规执行","首次执行序号","发现类型"],
    [[r["system"],r["strategy"],f'{r["evaluated"]}/{r["attempted"]}',r["violations"],r["first_execution"] if r["first_execution"] is not None else "预算内未发现",r["types"] or "—"] for r in detection]),"",
  "首次序号按 seed→六类的固定顺序累计，包括不支持和运行错误的预算尝试。未发现记右删失，不填 0。各违规类型首次序号见 experiment1_first_type.csv；各类工作负载见 summary.json。","",
  table(["系统","违规类型","重放成功/尝试"],[[r["system"],r["violation"],pct(r["successes"],r["attempts"])] for r in rep]),"",
  "每系统每种违规保存第一份完整见证，跨策略选择一次，再创建全新存储重放；不声称见证最小化。","",
  "## 实验二：共享记忆一致性","",
  table(["工作负载","系统","违规/可判定","正常完成/案例","误中止/正常任务","残留/受影响派生","排除"],
    [[LABELS[r["family"]],r["system"],pct(r["violations"],r["evaluated"]),pct(r["normal_completed"],r["normal_cases"]),pct(r["false_aborts"],r["normal_tasks"]),pct(r["stale"],r["affected"]),r["excluded"]] for r in comparison]),"",
  "违规率分母为完整可判定扰动案例。正常完成率要求两智能体任务完成、预期内容正确且替换内容消失；误中止率分母为正常控制逻辑任务。失效残留分母来自外部来源图，包括中止后应不可见的待发布派生；没有失效事件时为 NA。","",
  "experiment2_paired.csv 仅对输入哈希相同且双方均可判定的案例配对。不同能力覆盖范围不汇总总分。样本是结构化场景与重复执行，不计算会掩盖结构重复的独立同分布置信区间。","",
  "## 异常与结论限制","",
  f"原生运行错误 {m['runtime_errors']} 条。Mem0 竞争删除同一旧记忆时拒绝第二次删除；保留原生异常，不补幂等删除，不计作一致性通过或违规。详情在 observations.jsonl。",
  "发现应用契约缺口，不等于发现第三方系统违反其自身 API 保证。策略/混合只用于 TxnMem 自检，不能支撑跨系统优越性。",
  "0 次违规只能写成“本次配置与样本中未观察到违规”。正常完成率和误中止率仅覆盖无故障串行控制，不能推断无冲突并发活性或真实任务质量。","",
  "## 复现","",
  "运行：PYTHONPATH=src .venv/bin/python src/txnmem_external_campaign.py --seeds 50 --replays 5 --out results/external-formal-new",
  "汇总：PYTHONPATH=src .venv/bin/python src/txnmem_external_report.py results/external-formal-new","",
  "inputs.jsonl / jobs.json 固定输入和预算；capabilities.json 标注能力排除；observations.jsonl 保留事件/快照/原生结果；witnesses.json 与 replays.jsonl 保留见证；environment.json 固定依赖；run_manifest.json 固定源码与结果哈希；audit.json 记录重算验证。",""]
 (out/"report_zh.md").write_text("\n".join(text))
 audit=dict(status="passed",rows=len(rows),replays=len(replay),checks=["all campaign artifact hashes","all input and observation hashes","all judgments recomputed from raw evidence","all replay outcomes recomputed"])
 (out/"audit.json").write_text(json.dumps(audit,indent=2)+"\n")
 return audit
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("out",type=Path);a=p.parse_args();print(json.dumps(build(a.out)))
