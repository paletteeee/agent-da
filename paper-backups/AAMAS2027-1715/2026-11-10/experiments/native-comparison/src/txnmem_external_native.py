"""Native adapters for a controlled-operation experiment, not production QA.

All library memory methods are upstream. Deterministic embedding and generation
providers are explicit interventions used to hold content fixed.
"""
from __future__ import annotations
import contextlib
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import time
import traceback

ROOT=Path(__file__).resolve().parents[1]
PROVIDERS={"embedding":"sha256-counter-v1-64d",
           "generation":"fixed-response-v1",
           "scope":"controlled operations; no semantic quality or process-crash claim"}
CAPABILITIES={
 "TxnMem":{"unsupported":[],"implementation":"MultiAgentStorageCoordinator/SQLiteInstrumentedMemoryBackend"},
 "Mem0":{"unsupported":["actor_policy_scope","multi_agent_mixed"],"implementation":"mem0.Memory 2.0.18; infer=False; embedded Qdrant"},
 "A-MEM":{"unsupported":["actor_policy_scope","multi_agent_mixed"],"implementation":"AgenticMemorySystem; native note/Chroma methods"},
 "G-Memory":{"unsupported":["actor_policy_scope","multi_agent_mixed","cross_agent_provenance","concurrent_supersession"],
             "implementation":"GMemory.add_memory/retrieve_memory; task-record mapping"},
}

class FixedEmbedding:
    dimension=64
    def embed(self,text,memory_action=None):
        data=str(text).encode()
        return [(v/127.5)-1 for k in range(2) for v in hashlib.sha256(bytes([k])+data).digest()]
    def embed_documents(self,texts): return [self.embed(t) for t in texts]
    def embed_query(self,input):
        return [self.embed(t) for t in input] if isinstance(input,list) else self.embed(input)
    def __call__(self,input): return self.embed_documents(input)
    @staticmethod
    def name(): return "txnmembench-fixed64"
    def get_config(self): return {}
    @staticmethod
    def build_from_config(config): return FixedEmbedding()

class Mem0Native:
    def __init__(self,path):
        os.environ["MEM0_TELEMETRY"]="false"
        os.environ["MEM0_DIR"]=str(path/"sdk")
        from mem0 import Memory
        config={"history_db_path":str(path/"history.db"),
          "vector_store":{"provider":"qdrant","config":{"collection_name":"benchmark","path":str(path/"qdrant"),"embedding_model_dims":64}},
          "llm":{"provider":"vllm","config":{"model":"unused-controlled","vllm_base_url":"http://127.0.0.1:9"}},
          "embedder":{"provider":"openai","config":{"model":"text-embedding-3-small","api_key":"not-a-secret-controlled-provider"}}}
        self.memory=Memory.from_config(config)
        self.memory.embedding_model=FixedEmbedding()
        self.ids={}
    def put(self,o):
        key=o["key"]
        if key in self.ids:
            self.memory.update(self.ids[key],o["value"])
        else:
            r=self.memory.add(o["value"],user_id="controlled-case",metadata={"benchmark_id":key,"source_ids":o.get("sources",[])},infer=False)
            self.ids[key]=r["results"][0]["id"]
    def read(self,key):
        if key not in self.ids:return []
        r=self.memory.get(self.ids[key])
        return [key] if r else []
    def delete(self,key):
        if key in self.ids:self.memory.delete(self.ids[key])
    def snapshot(self):
        r=self.memory.get_all(filters={"user_id":"controlled-case"},top_k=1000)
        return {x["metadata"]["benchmark_id"]:{"value":x["memory"],"native_id":x["id"]} for x in r["results"]}
    def close(self):
        self.memory.vector_store.client.close()

class AMemNative:
    def __init__(self,path):
        sys.path.insert(0,str(ROOT/"external"/"A-mem"))
        import agentic_memory.retrievers as retrievers
        # Provider substitution only; no memory/retrieval implementation replaced.
        retrievers.SentenceTransformerEmbeddingFunction=lambda **kwargs:FixedEmbedding()
        from agentic_memory.memory_system import AgenticMemorySystem
        self.memory=AgenticMemorySystem(api_key="not-a-secret-controlled-provider")
        self.model_calls=0
        def completion(prompt,**kwargs):
            self.model_calls+=1
            return json.dumps({"should_evolve":False,"actions":[],"suggested_connections":[],
                 "tags_to_update":[],"new_context_neighborhood":[],"new_tags_neighborhood":[]})
        self.memory.llm_controller.llm.get_completion=completion
    def put(self,o):
        if self.memory.read(o["key"]):
            self.memory.update(o["key"],content=o["value"])
        else:
            self.memory.add_note(o["value"],id=o["key"],links=o.get("sources",[]),context="controlled shared-memory fact")
    def read(self,key):
        # Native ID read plus a search probe is retained separately by runner.
        r=self.memory.read(key)
        return [key] if r else []
    def delete(self,key):self.memory.delete(key)
    def snapshot(self):
        # Cross-check native object view against the native vector collection.
        objects={k:{"value":v.content} for k,v in self.memory.memories.items()}
        vector=self.memory.retriever.collection.get()
        if set(vector["ids"])!=set(objects):raise RuntimeError("native object/vector identity disagreement")
        return objects
    def close(self):
        self.memory.retriever.client.reset()

class GMemoryNative:
    def __init__(self,path):
        sys.path.insert(0,str(ROOT/"external"/"GMemory"))
        os.environ.setdefault("OPENAI_API_BASE","http://127.0.0.1:9")
        os.environ.setdefault("OPENAI_API_KEY","not-a-secret-controlled-provider")
        previous=Path.cwd()
        try:
            os.chdir(ROOT/"external"/"GMemory")
            from mas.memory.mas_memory.GMemory import GMemory
        finally:
            os.chdir(previous)
        self.model_calls=0
        def llm(*args,**kwargs):
            self.model_calls+=1
            return "10"
        self.memory=GMemory(namespace="case",global_config={"working_dir":str(path)},
                            llm_model=llm,embedding_func=FixedEmbedding())
        self.keys={}
    def put(self,o):
        from mas.memory.common import MASMessage
        if o["key"] in self.keys:raise ValueError("G-Memory has no native overwrite mapping")
        self.keys[o["key"]]=o["value"]
        message=MASMessage(task_main=o["value"],task_description=o["value"],label=True)
        message.move_state("record",o["value"])
        self.memory.add_memory(message)
    def read(self,key):
        if key not in self.keys:return []
        successful,failed,insights=self.memory.retrieve_memory(self.keys[key],successful_topk=50,failed_topk=0,insight_topk=0,threshold=-1.0)
        texts={x.task_main for x in successful+failed}
        return [key] if self.keys[key] in texts else []
    def snapshot(self):
        r=self.memory.main_memory.get()
        reverse={v:k for k,v in self.keys.items()}
        return {reverse[t]:{"value":t,"native_id":i} for i,t in zip(r["ids"],r["documents"])}
    def delete(self,key):raise NotImplementedError("no native delete")
    def close(self):pass

class TxnMemNative:
    def __init__(self,path):
        from txnmem_backend import SQLiteInstrumentedMemoryBackend
        from txnmem_multiagent_storage import MultiAgentStorageCoordinator
        self.backend=SQLiteInstrumentedMemoryBackend(path/"memory.db")
        self.coordinator=MultiAgentStorageCoordinator(self.backend)
        self.writes={}
    def initialize(self,o):self.backend.write(o["key"],o["value"],scope=o.get("scope","shared"))
    def begin(self,actor):
        self.coordinator.begin(actor,actor);self.writes[actor]=0
    def put(self,o):
        args={"type":"derive" if o.get("sources") else "write","memory_id":o["key"],"value":o["value"],
              "agent_id":o["actor"],"scope":o.get("scope","shared")}
        if o.get("sources"):args["source_ids"]=o["sources"]
        self.coordinator.stage(o["txn"],args);self.writes[o["txn"]]+=1
    def read(self,key,txn):
        r=self.coordinator.read(txn,key,"shared")
        return [key] if r else []
    def delete(self,key,txn):
        self.coordinator.stage(txn,{"type":"delete","memory_id":key,"scope":"shared"})
        self.writes[txn]+=1
    def commit(self,txn):
        if not self.writes[txn]:return "completed"
        r=self.coordinator.commit(txn)
        return {"committed_complete":"completed","aborted_clean":"aborted"}.get(r["classification"],"unknown")
    def invalidate(self,key):self.coordinator.invalidate("controller",key)
    def revoke(self,actor):self.coordinator.revoke(actor,"derive" if actor=="B" else "write","shared")
    def snapshot(self):
        return {k:v for k,v in self.backend.snapshot().items() if v.get("status")=="active"}
    def close(self):self.backend._connection.close()

def execute_case(system,case):
    caps=CAPABILITIES[system]
    if case["family"] in caps["unsupported"]:
        return {"status":"unsupported_mapping","reason":"no native policy/invalidation/version operation with required semantics",
                "implementation":caps["implementation"],"events":[]}
    state_root=ROOT/"results"/"native-state"; state_root.mkdir(parents=True,exist_ok=True)
    events=[]; reads=[]; invalidated=[]; revoked=[]; states={}; fault_triggered=False; fault_effective=False
    started=time.perf_counter_ns()
    adapter=None
    try:
        with tempfile.TemporaryDirectory(prefix=system.replace("-","")+"_",
                                         dir=state_root) as directory:
            constructors={"TxnMem":TxnMemNative,"Mem0":Mem0Native,"A-MEM":AMemNative,"G-Memory":GMemoryNative}
            adapter=constructors[system](Path(directory))
            is_txn=system=="TxnMem"
            for initial in case["initial"]:
                (adapter.initialize if is_txn else adapter.put)(initial)
            initial_snapshot=adapter.snapshot()
            fault=case.get("fault")
            for o in case["operations"]:
                actor=o["actor"]
                if states.get(actor)=="interrupted":continue
                kind=o["kind"]; response=None
                t=time.perf_counter_ns()
                if kind=="begin":
                    states[actor]="active"
                    if is_txn:adapter.begin(actor)
                elif kind=="put":adapter.put(o)
                elif kind=="read":
                    visible=adapter.read(o["key"],actor) if is_txn else adapter.read(o["key"])
                    reads.append({"operation":o["id"],"txn":actor,"key":o["key"],"visible":visible,
                                  "completed":[a for a,s in states.items() if s=="completed"]})
                    response=visible
                elif kind=="delete":
                    adapter.delete(o["key"],actor) if is_txn else adapter.delete(o["key"])
                elif kind=="commit":
                    states[actor]=adapter.commit(actor) if is_txn else "completed"
                    response=states[actor]
                else:raise ValueError("unknown operation")
                events.append({"operation":o["id"],"kind":kind,"actor":actor,"response":response,
                               "elapsed_ns":time.perf_counter_ns()-t})
                if fault and o["id"]==fault["after"]:
                    fault_triggered=True
                    kind=fault["kind"]
                    if kind=="interrupt":
                        victim=fault["actor"]
                        if states[victim]!="completed":
                            states[victim]="interrupted";fault_effective=True
                    if kind in ("invalidate","mixed"):
                        fault_effective=True
                        adapter.invalidate(fault["key"]) if is_txn else adapter.delete(fault["key"])
                        invalidated.append(fault["key"])
                    if kind in ("revoke","mixed"):
                        fault_effective=True
                        if not is_txn:raise NotImplementedError("no policy mapping")
                        adapter.revoke(fault["actor"]);revoked.append(fault["actor"])
                    events.append({"fault":dict(fault),"state_after_fault":adapter.snapshot()})
            final=adapter.snapshot()
            # Actual public reads verify visible descendants; snapshot alone is diagnostic evidence.
            final_reads={}
            if not is_txn:
                for key in final:
                    final_reads[key]=adapter.read(key)
                    if key not in final_reads[key]:raise RuntimeError("stored record not retrievable through mapped native interface")
            result={"status":"ok","implementation":caps["implementation"],"provider_profile":PROVIDERS,
                    "initial_snapshot":initial_snapshot,"final_snapshot":final,"final_ids":sorted(final),
                    "events":events,"reads":reads,"final_native_reads":final_reads,"transactions":states,
                    "invalidated":invalidated,"revoked":revoked,"fault_triggered":fault_triggered,"fault_effective":fault_effective,
                    "model_provider_calls":getattr(adapter,"model_calls",0),
                    "elapsed_ns":time.perf_counter_ns()-started}
            adapter.close();adapter=None
            return result
    except Exception as exc:
        return {"status":"runtime_error","reason":f"{type(exc).__name__}: {exc}",
                "traceback":traceback.format_exc(),"events":events,"implementation":caps["implementation"],
                "fault_triggered":fault_triggered}
    finally:
        if adapter:
            try:adapter.close()
            except Exception:pass

def main():
    system=sys.argv[1]
    for line in sys.stdin:
        try:
            case=json.loads(line)
            with contextlib.redirect_stdout(sys.stderr):
                result=execute_case(system,case)
            print(json.dumps(result,ensure_ascii=False),flush=True)
        except Exception as exc:
            print(json.dumps({"status":"runtime_error","reason":str(exc)}),flush=True)
if __name__=="__main__":main()
