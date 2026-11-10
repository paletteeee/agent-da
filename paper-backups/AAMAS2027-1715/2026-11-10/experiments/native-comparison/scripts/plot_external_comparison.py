"""Publication-exportable figures from audited comparison tables."""
import argparse,csv,json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
SYSTEMS=["TxnMem","Mem0","G-Memory","A-MEM"]
FAMILIES=["multi_agent_visibility","interleaved_atomicity","concurrent_supersession","actor_policy_scope","cross_agent_provenance","multi_agent_mixed"]
LABELS=["Visibility","Interrupted\natomicity","Competing\nversions","Actor\npolicy","Provenance","Mixed"]
def build(out):
 assert json.loads((out/"audit.json").read_text())["status"]=="passed"
 rows=list(csv.DictReader((out/"experiment2_comparison.csv").open()))
 data=np.full((4,6),np.nan);labels={}
 for r in rows:
  i=SYSTEMS.index(r["system"]);j=FAMILIES.index(r["family"]);n=int(r["evaluated"]);v=int(r["violations"])
  if n:data[i,j]=v/n
  labels[i,j]=f"{v}/{n}\n{100*v/n:.0f}%" if n else ("ERROR\nexcluded" if "runtime_error" in r["excluded"] else "NA")
 plt.rcParams.update({"font.size":10,"svg.fonttype":"none","pdf.fonttype":42})
 fig,ax=plt.subplots(figsize=(10,4.5))
 cmap=plt.get_cmap("YlOrRd").copy();cmap.set_bad("#e5e7eb")
 im=ax.imshow(data,vmin=0,vmax=1,cmap=cmap,aspect="auto")
 for (i,j),txt in labels.items():ax.text(j,i,txt,ha="center",va="center",color="white" if data[i,j]>.7 else "#172033")
 ax.set_xticks(range(6),LABELS);ax.set_yticks(range(4),SYSTEMS)
 ax.set_title("E2: Application-contract violations / evaluable directed cases",pad=15)
 fig.colorbar(im,ax=ax,label="Violation fraction",shrink=.8)
 fig.text(.06,.035,"Controlled providers; reduced workload mappings. NA/error excluded, never counted as pass.\nOnly visibility and interrupted atomicity are supported by all four systems.",fontsize=9)
 fig.subplots_adjust(bottom=.22,top=.86,left=.12,right=.94)
 for ext in ("svg","pdf","png"):fig.savefig(out/("experiment2_violations."+ext),dpi=180)
 plt.close(fig)
 rows=list(csv.DictReader((out/"experiment1_detection.csv").open()))
 fig,axes=plt.subplots(1,3,figsize=(11,4.3),sharey=True)
 for ax,s in zip(axes,SYSTEMS[1:]):
  rr=[r for r in rows if r["system"]==s];x=np.arange(len(rr));vals=[int(r["violations"])/int(r["evaluated"]) if int(r["evaluated"]) else 0 for r in rr]
  ax.bar(x,vals,color=["#8a98aa","#d7a642","#c85949"],width=.65)
  ax.set_xticks(x,[r["strategy"] for r in rr],rotation=15);ax.set_ylim(0,1.15);ax.set_title(s)
  for i,r in enumerate(rr):ax.text(i,vals[i]+.035,f'{r["violations"]}/{r["evaluated"]}',ha="center",fontsize=9)
 axes[0].set_ylabel("Cases with observed contract violation")
 fig.suptitle("E1: Detection under three test conditions",y=.97)
 fig.text(.05,.025,"Ordinary = serial/no fault; random/directed include perturbations. Different fault coverage;\nthis is not an equal-fault-budget search-efficiency comparison. Denominators exclude NA/errors.",fontsize=9)
 fig.subplots_adjust(bottom=.25,top=.83,wspace=.12)
 for ext in ("svg","pdf","png"):fig.savefig(out/("experiment1_detection."+ext),dpi=180)
 plt.close(fig)
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("out",type=Path);a=p.parse_args();build(a.out)
