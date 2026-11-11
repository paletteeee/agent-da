import pathlib,json,shutil,hashlib
from PIL import Image
import numpy as np
root=pathlib.Path('/data/emoedit-baseline');out=root/'artifacts'
records=json.loads((root/'logs/eight_emotions.json').read_text());assert len(records)==8,len(records)
checks=[]
for r in records:
 p=pathlib.Path(r['output']);im=Image.open(p);im.load();a=np.asarray(im);assert im.size==(512,512);assert float(a.std())>1
 checks.append({'emotion':r['emotion'],'size':list(im.size),'std':float(a.std()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()});shutil.copy2(p,out/p.name)
source=Image.open(root/'EmoEdit/examples/test.jpg').convert('RGB').resize((512,512),Image.Resampling.LANCZOS);source.save(out/'input.png')
for name in ['demo_result.json','eight_emotions.json','provenance.json','environment.freeze.txt','demo.log','eight_emotions.log']:
 shutil.copy2(root/'logs'/name,out/name)
for name in ['run_demo.py','run_other_emotions.py']:
 shutil.copy2(root/name,out/name)
shutil.copy2(root/'EmoEdit/test.py',out/'test.py')
(out/'validation.json').write_text(json.dumps(checks,indent=2))
(out/'RUN_INFO.txt').write_text('EmoEdit official example smoke test, 2026-11-10\nServer: root+vm-jQO80X6g9pvcTndz@39.145.28.59:32222\nRemote directory: /data/emoedit-baseline\nRepeat default demo: cd /data/emoedit-baseline && .venv/bin/python run_demo.py\nRepeat other 7 emotions: cd /data/emoedit-baseline && .venv/bin/python run_other_emotions.py\nSeed: 42; steps: 100; text guidance: 7.5; image guidance: 1.5\nInput: official examples/test.jpg resized to 512 x 512\nThis is a smoke test of eight emotion conditions on one image, not the paper benchmark.\nWeights and full Python environment remain on the server.\n')
print(json.dumps(checks,indent=2))
