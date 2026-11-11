import pathlib,json,hashlib,os
from PIL import Image
EMOTIONS=['amusement','awe','contentment','excitement','anger','disgust','fear','sadness']
EXCLUDED_IDS={'21','18','111','137','218','228','237','266','277','333'}
ROOT=pathlib.Path('/data/emoedit-baseline')
RUN=ROOT/'benchmark/official30_seed48000_qformer4000'
def sha256(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
 return h.hexdigest()
def atomic_json(path,data):
 path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(data,indent=2,allow_nan=False));os.replace(tmp,path)
def ensure_protocol(path,data):
 path=pathlib.Path(path)
 if path.exists():
  if json.loads(path.read_text())!=data:raise ValueError('protocol mismatch: refuse mixing runs')
 else:atomic_json(path,data)
def build_manifest(directory):
 files=sorted([p for p in pathlib.Path(directory).iterdir() if p.suffix.lower() in {'.png','.jpg','.jpeg'}],key=lambda p:int(p.stem))
 ids=[p.stem for p in files]
 if len(set(ids))!=len(ids):raise ValueError('duplicate input IDs')
 selected=[p for p in files if p.stem not in EXCLUDED_IDS]
 if len(selected)!=405:raise ValueError(f'expected 405 inputs, found {len(selected)}')
 inputs=[];pairs=[]
 for p in selected:
  with Image.open(p) as im:im.load();size=list(im.size)
  inputs.append({'image_id':p.stem,'path':str(p.resolve()),'sha256':sha256(p),'size':size})
  for emotion in EMOTIONS:pairs.append({'image_id':p.stem,'source':str(p.resolve()),'emotion':emotion,'target_index':EMOTIONS.index(emotion),'output':f'{p.stem}_{emotion}_step-4000_seed-48000.jpg'})
 return {'raw_count':len(files),'excluded_ids':sorted(EXCLUDED_IDS,key=int),'inputs':inputs,'pairs':pairs}
def audit_outputs(manifest,directory):
 directory=pathlib.Path(directory);expected={p['output'] for p in manifest['pairs']};actual={p.name for p in directory.glob('*.jpg')}
 if missing:=expected-actual:raise ValueError(f'missing outputs: {len(missing)}')
 if extra:=actual-expected:raise ValueError(f'unexpected outputs: {len(extra)}')
 for name in sorted(expected):
  try:
   with Image.open(directory/name) as im:
    im.load()
    if im.size!=(512,512):raise ValueError(f'non-benchmark output size {im.size}')
  except Exception as e:raise ValueError(f'corrupt output {name}') from e
 return {'count':len(actual),'expected':len(expected),'verified':True}
def audit_provenance(manifest,directory,complete=True):
 directory=pathlib.Path(directory);expected={p['output']:p for p in manifest['pairs']};records={}
 log=directory/'generation.jsonl'
 for line in log.read_text().splitlines() if log.exists() else []:
  r=json.loads(line);name=r['output']
  if name in records:raise ValueError(f'duplicate generation record: {name}')
  if name not in expected or any(r.get(k)!=v for k,v in expected[name].items()):raise ValueError(f'generation pair mismatch: {name}')
  records[name]=r
 actual={p.name for p in directory.glob('*.jpg')}
 if actual!=set(records):raise ValueError('output/record coverage mismatch')
 if actual-set(expected) or (complete and actual!=set(expected)):raise ValueError('incomplete or unexpected generation records')
 for name,r in records.items():
  if sha256(directory/name)!=r['sha256']:raise ValueError(f'output hash mismatch: {name}')
 return {'verified_records':len(records),'complete':complete}
def prepare():
 manifest=build_manifest(ROOT/'benchmark/data/Original_405')
 RUN.mkdir(parents=True,exist_ok=True)
 ensure_protocol(RUN/'manifest.json',manifest)
 import torch,transformers,diffusers
 config={'source_commit':'88f622cdb5c7afdc8954eef899064296fe4a4273','adapter_sha256':sha256(ROOT/'models/Q-Former_4000.pth'),'seed':48000,'steps':30,'guidance_scale':7.5,'image_guidance_scale':1.5,'batch_size':1,'manifest_sha256':sha256(RUN/'manifest.json'),'dtype':'float16 base pipeline / float32 adapter and CLIP','generation_preprocess':'official shorter side 512 Lanczos; preserve aspect ratio','output_format':'JPEG Pillow default','torch':torch.__version__,'transformers':transformers.__version__,'diffusers':diffusers.__version__,'models':{}}
 for name in ['instruct-pix2pix','clip-vit-large-patch14']:
  m=json.loads((ROOT/'models'/f'{name}_metadata.json').read_text());config['models'][name]={'id':m['id'],'revision':m['sha']}
 ensure_protocol(RUN/'protocol.json',config)
 return manifest,config
if __name__=='__main__':
 m,c=prepare();print(json.dumps({'inputs':len(m['inputs']),'pairs':len(m['pairs']),'run':str(RUN)}))
