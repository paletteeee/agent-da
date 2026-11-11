import os,time,runpy,json,pathlib
os.environ['HF_HUB_OFFLINE']='1'
os.environ['TRANSFORMERS_OFFLINE']='1'
import torch
root=pathlib.Path('/data/emoedit-baseline')
os.chdir(root/'EmoEdit')
import sys
sys.path.insert(0,str(root/'EmoEdit'))
torch.manual_seed(42)
torch.cuda.manual_seed_all(42)
torch.cuda.reset_peak_memory_stats()
t=time.perf_counter()
runpy.run_path('test.py',run_name='__main__')
torch.cuda.synchronize()
r={'seed':42,'emotion':'amusement','steps':100,'guidance_scale':7.5,'image_guidance_scale':1.5,'elapsed_seconds':time.perf_counter()-t,'peak_allocated_GiB':torch.cuda.max_memory_allocated()/1024**3,'peak_reserved_GiB':torch.cuda.max_memory_reserved()/1024**3,'torch':torch.__version__,'gpu':torch.cuda.get_device_name(0),'output':str(root/'EmoEdit/examples/result/test-amusement.jpg')}
(root/'logs/demo_result.json').write_text(json.dumps(r,indent=2));print(json.dumps(r,indent=2),flush=True)
