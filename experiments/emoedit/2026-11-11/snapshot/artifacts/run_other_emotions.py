import os,sys,time,json,pathlib
os.environ['HF_HUB_OFFLINE']='1';os.environ['TRANSFORMERS_OFFLINE']='1'
import torch
root=pathlib.Path('/data/emoedit-baseline');os.chdir(root/'EmoEdit');sys.path.insert(0,str(root/'EmoEdit'))
from test import main
records=[json.loads((root/'logs/demo_result.json').read_text())]
for emotion in ['awe','contentment','excitement','anger','disgust','fear','sadness']:
 torch.manual_seed(42);torch.cuda.manual_seed_all(42);torch.cuda.reset_peak_memory_stats();t=time.perf_counter()
 main(str(root/'models/Q-Former_4000.pth'),'examples/test.jpg',emotion,'examples/result')
 torch.cuda.synchronize();r={'emotion':emotion,'seed':42,'steps':100,'elapsed_seconds':time.perf_counter()-t,'peak_allocated_GiB':torch.cuda.max_memory_allocated()/1024**3,'output':str(root/'EmoEdit/examples/result'/f'test-{emotion}.jpg')};records.append(r);print(json.dumps(r),flush=True)
 (root/'logs/eight_emotions.json').write_text(json.dumps(records,indent=2))
print('ALL_EIGHT_COMPLETE',flush=True)
