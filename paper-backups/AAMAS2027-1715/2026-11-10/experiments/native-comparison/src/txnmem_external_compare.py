"""Controlled external-memory workloads and an implementation-independent observer.

The generator reuses parameterized instances from the existing six-family suite.
Native API mapping, model providers and capability exclusions are separate.
"""
from __future__ import annotations
import copy
import hashlib
import json
import random
from collections import defaultdict, deque
from txnmem_multiagent_workloads import generate_multi_agent_instance, MULTI_AGENT_WORKLOADS

FAMILIES = tuple(MULTI_AGENT_WORKLOADS)
STRATEGIES = ("ordinary", "random", "directed")
SYSTEMS = ("TxnMem", "Mem0", "G-Memory", "A-MEM")

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()

def build_case(family, seed, strategy, control=False):
    if family not in FAMILIES or strategy not in STRATEGIES:
        raise ValueError("unknown workload or strategy")
    original = generate_multi_agent_instance(family, seed)
    p = original["config"]["sampled_parameters"]
    normal = bool(control or strategy == "ordinary")
    n = max(2, int(p.get("txn_size",p.get("txn_size_a",2))))
    programs={"A":[], "B":[]}
    initial=[{"key":"unrelated","value":"Independent fact remains valid","scope":"shared"}]
    edges=[]; producers={}; write_sets={"A":[],"B":[]}
    def op(actor,kind,**kw):
        row={"id":f"{actor}{len(programs[actor])+1}","actor":actor,
             "txn":actor,"actor_step":len(programs[actor])+1,"kind":kind,**kw}
        programs[actor].append(row)
        if kind=="put":
            producers[row["key"]]=actor; write_sets[actor].append(row["key"])
            edges.extend([[s,row["key"]] for s in row.get("sources",[])])
        return row
    def put(actor,key,**kw):
        return op(actor,"put",key=key,value=f"Fact {key} instance {seed}",scope="shared",**kw)
    fault=None; expected_removed=[]; precedence=[]
    for a in programs: op(a,"begin")
    if family in ("multi_agent_visibility","interleaved_atomicity"):
        for i in range(n): put("A",f"w{i}")
        op("A","commit")
        if family=="interleaved_atomicity":
            for i in range(max(2,int(p.get("txn_size_b",2)))): put("B",f"b{i}")
        for i in range(n if family=="interleaved_atomicity" else 1): op("B","read",key=f"w{i}",scope="shared")
        op("B","commit")
        directed=programs["A"][:-1]+programs["B"]+programs["A"][-1:]
        if family=="interleaved_atomicity" and not normal:
            fault={"kind":"interrupt","after":programs["A"][1]["id"],"actor":"A"}
    elif family=="concurrent_supersession":
        initial.append({"key":"old","value":"Version one","scope":"shared"})
        for a in programs:
            old="newA" if normal and a=="B" else "old"
            op(a,"read",key=old,scope="shared")
            put(a,"new"+a,supersedes=old)
            op(a,"delete",key=old)
            op(a,"commit")
        directed=programs["A"][:2]+programs["B"][:2]+programs["A"][2:]+programs["B"][2:]
        expected_removed=["old"]+(["newA"] if normal else [])
    elif family=="actor_policy_scope":
        put("A","protected")
        op("A","commit")
        put("B","independent")
        op("B","commit")
        directed=programs["A"][:-1]+programs["B"]+programs["A"][-1:]
        if not normal: fault={"kind":"revoke","after":programs["A"][-2]["id"],"actor":"A","scope":"shared"}
    elif family in ("cross_agent_provenance","multi_agent_mixed"):
        initial.append({"key":"root","value":"Source fact","scope":"shared"})
        op("A","read",key="root",scope="shared")
        if family=="multi_agent_mixed":
            for i in range(n): put("A",f"w{i}")
        op("A","commit")
        depth=int(p.get("depth",2))
        for branch in range(int(p.get("branch_factor",1))):
            source="root"
            for level in range(depth):
                key=f"d{branch}_{level}"
                put("B",key,sources=[source]); source=key
        op("B","commit")
        directed=programs["A"]+programs["B"]
        if not normal:
            after=programs["B"][-1]["id"] if p.get("invalidation_phase","after_derived_commit")=="after_derived_commit" else programs["B"][-2]["id"]
            fault={"kind":"invalidate","after":after,"key":"root"}
            if family=="multi_agent_mixed":
                fault={"kind":"mixed","after":programs["B"][-2]["id"],"key":"root","actor":"B","scope":"shared"}
    if normal:
        ordered=programs["A"]+programs["B"]
    elif strategy=="directed":
        ordered=directed
    else:
        rng=random.Random(seed+90421)
        remaining=copy.deepcopy(programs); ordered=[]
        while any(remaining.values()):
            actor=rng.choice([a for a,seq in remaining.items() if seq])
            ordered.append(remaining[actor].pop(0))
        if fault:
            actor=fault.get("actor","B")
            candidates=programs[actor]
            fault={**fault,"after":rng.choice(candidates)["id"]}
    expected_ids=sorted(set(x["key"] for x in initial)|set(producers))
    expected_ids=[x for x in expected_ids if x not in expected_removed]
    case={"case_id":f"{family}-{seed}-{strategy}-{'normal' if normal else 'perturbed'}",
          "family":family,"seed":seed,"strategy":strategy,"control":normal,
          "source_instance_id":original["instance_id"],
          "source_structure_signature":original["structure_signature"],
          "sampled_parameters":p,"initial":initial,"operations":ordered,
          "write_sets":write_sets,"producers":producers,"edges":edges,
          "expected_ids":expected_ids,"expected_removed":expected_removed,
          "fault":fault,"fault_model":"operation_boundary_interruption_not_process_crash",
          "application_contract":"multi_record_task_visibility",
          "mapping_version":"external-compare-v1"}
    values={x["key"]:x["value"] for x in initial}
    values.update({o["key"]:o["value"] for o in ordered if o["kind"]=="put"})
    case["expected_values"]={k:values[k] for k in expected_ids}
    shape={"operations":[{k:v for k,v in o.items() if k!="value"} for o in ordered],"fault":fault}
    case["executed_structure_sha256"]=digest(shape)
    case["effective_parameters"]={"operation_count":len(ordered),"write_counts":{a:len(w) for a,w in write_sets.items()},"source_edges":len(edges),"fault":fault}
    case["sampling_note"]="Source parameters are provenance only; executed structure is reduced and may repeat across seeds."
    case["input_sha256"]=digest(case)
    return case

def descendants(edges,roots):
    children=defaultdict(list)
    for parent,child in edges: children[parent].append(child)
    result=set(); queue=deque(roots)
    while queue:
        for child in children[queue.popleft()]:
            if child not in result: result.add(child); queue.append(child)
    return result

def judge(case,obs):
    row={"case_id":case["case_id"],"family":case["family"],
         "strategy":case["strategy"],"control":case.get("control",False),
         "status":obs.get("status","inconclusive"),"reason":obs.get("reason",""),
         "correctness_included":False,"any_violation":None,"violations":[],
         "normal_completed":None,"false_abort_count":None,"normal_task_count":None,
         "stale_descendant_count":None,"affected_descendant_count":None}
    if row["status"]!="ok": return row
    required=("final_ids","transactions","reads","invalidated","events")
    if any(x not in obs for x in required):
        row.update(status="inconclusive",reason="missing required observation"); return row
    if case.get("fault") and not obs.get("fault_triggered"):
        row.update(status="inconclusive",reason="scheduled fault did not fire"); return row
    if any(v=="unknown" for v in obs["transactions"].values()):
        row.update(status="inconclusive",reason="unknown transaction outcome");return row
    if case.get("control") and case.get("expected_values") and "final_snapshot" not in obs:
        row.update(status="inconclusive",reason="missing content observations");return row
    final=set(obs["final_ids"]); violations=set()
    for read in obs["reads"]:
        for key in read["visible"]:
            producer=case.get("producers",{}).get(key)
            if producer and producer!=read["txn"] and producer not in read["completed"]:
                violations.add("dirty_read")
    for txn,keys in case.get("write_sets",{}).items():
        keys=set(keys)
        if obs["transactions"].get(txn) in ("interrupted","aborted"):
            if final & keys: violations.add("aborted_task_residue")
        if obs["transactions"].get(txn)=="interrupted" and 0<len(final&keys)<len(keys):
            violations.add("partial_update")
    if case["family"]=="concurrent_supersession" and not case.get("control"):
        versions=set(case.get("producers",{})) & final
        if len(versions)>1: violations.add("conflicting_versions")
    revoked=set(obs.get("revoked",[]))
    for actor in revoked:
        if obs["transactions"].get(actor)=="completed" and final&set(case.get("write_sets",{}).get(actor,[])):
            violations.add("revoked_publication")
    affected=descendants(case.get("edges",[]),obs["invalidated"])
    if set(obs["invalidated"]) & final: violations.add("invalidated_source_visible")
    residual=affected & final
    if residual: violations.add("stale_descendant")
    initial={x["key"] for x in case.get("initial",[])}
    unrelated=initial - affected - set(obs["invalidated"]) - set(case.get("expected_removed",[]))
    if unrelated-final: violations.add("unrelated_loss")
    row.update(correctness_included=True,any_violation=int(bool(violations)),
               violations=sorted(violations),stale_descendant_count=len(residual),
               affected_descendant_count=len(affected))
    if case.get("control"):
        tasks=obs["transactions"]
        aborts=sum(v=="aborted" for v in tasks.values())
        required=set(case["expected_ids"])
        removed=set(case.get("expected_removed",[]))
        complete=bool(tasks) and all(v=="completed" for v in tasks.values()) and required<=final and not removed&final
        if case.get("expected_values"):
            complete = complete and all(obs["final_snapshot"].get(k,{}).get("value")==v for k,v in case["expected_values"].items())
        row.update(normal_completed=int(complete),false_abort_count=aborts,normal_task_count=len(tasks))
        if not complete and not violations:
            row["violations"]=["normal_effect_missing"]; row["any_violation"]=1
    return row

def summarize_rows(rows):
    grouped=defaultdict(list)
    for row in rows: grouped[f"{row['system']}/{row['strategy']}/{row['family']}"].append(row)
    result={"groups":{}}
    for key,rs in sorted(grouped.items()):
        valid=[r for r in rs if r["correctness_included"]]
        hits=[r.get("trial_index",i+1) for i,r in enumerate(rs) if r.get("any_violation")==1]
        normal=[r for r in valid if r.get("control")]
        affected=sum(r.get("affected_descendant_count") or 0 for r in valid)
        residual=sum(r.get("stale_descendant_count") or 0 for r in valid)
        task_count=sum(r.get("normal_task_count") or 0 for r in normal)
        result["groups"][key]={"attempted":len(rs),"evaluated":len(valid),
          "excluded_by_status":{s:sum(r["status"]==s for r in rs) for s in sorted({r["status"] for r in rs if not r["correctness_included"]})},
          "violating_cases":sum(r["any_violation"] for r in valid),
          "violation_rate":sum(r["any_violation"] for r in valid)/len(valid) if valid else None,
          "types":sorted({v for r in valid for v in r["violations"]}),
          "first_detection_trial":min(hits) if hits else None,"right_censored":not hits,
          "normal_cases":len(normal),"normal_completed":sum(r.get("normal_completed") or 0 for r in normal),
          "normal_completion_rate":sum(r.get("normal_completed") or 0 for r in normal)/len(normal) if normal else None,
          "false_abort_rate":sum(r.get("false_abort_count") or 0 for r in normal)/task_count if task_count else None,
          "stale_descendant_count":residual,"affected_descendant_count":affected,
          "stale_descendant_rate":residual/affected if affected else None}
    return result
