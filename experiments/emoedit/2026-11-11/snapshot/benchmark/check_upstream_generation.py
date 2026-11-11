import os,sys,pathlib,types,json
os.environ['HF_HUB_OFFLINE']='1';os.environ['TRANSFORMERS_OFFLINE']='1'
from protocol import ROOT,RUN,sha256
sys.path[:0]=[str(ROOT/'benchmark/upstream/scripts'),str(ROOT/'EmoEdit/scripts')]
source=(ROOT/'benchmark/upstream/scripts/validation_args.py').read_text()
source=source.replace('/mnt/d/model/instruct-pix2pix',str(ROOT/'models/instruct-pix2pix')).replace('/mnt/d/model/clip-vit-large-patch14',str(ROOT/'models/clip-vit-large-patch14'))
source=source.replace('torch_dtype=torch.float16).to("cuda")','torch_dtype=torch.float16,variant="fp16",local_files_only=True).to("cuda")')
m=types.ModuleType('upstream_reference');exec(compile(source,'upstream_validation_adapted_paths_only','exec'),m.__dict__)
single=RUN/'reference_input';single.mkdir(exist_ok=True)
p=single/'1.png'
if not p.exists():p.symlink_to(ROOT/'benchmark/data/Original_405/1.png')
m.train_datadir=str(RUN/'reference');import torch
with torch.no_grad():m.main(str(single),[str(ROOT/'models/Q-Former_4000.pth')],str(RUN/'reference'),48000)
ref=RUN/'reference/4000';files=list(ref.glob('*.jpg'));assert len(files)==8
checks={p.name:sha256(p)==sha256(RUN/'smoke'/p.name) for p in files}
print(json.dumps(checks,indent=2));assert all(checks.values()),'Runner differs from official validation outputs'
(RUN/'upstream_equivalence.json').write_text(json.dumps(checks,indent=2))
print('OFFICIAL_OUTPUT_EQUIVALENCE_PASS',flush=True)
