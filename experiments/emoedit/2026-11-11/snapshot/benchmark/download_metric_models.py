import os,pathlib,json,requests
os.environ['HF_ENDPOINT']='https://hf-mirror.com'
os.environ['HF_HUB_DOWNLOAD_TIMEOUT']='120'
os.environ['TORCH_HOME']='/data/emoedit-baseline/models/torch'
from huggingface_hub import snapshot_download
from torchvision.models import AlexNet_Weights
import torch
root=pathlib.Path('/data/emoedit-baseline')
repo='openai/clip-vit-base-patch32'
r=requests.get('https://hf-mirror.com/api/models/'+repo,timeout=30);r.raise_for_status();meta=r.json()
(root/'models/clip-vit-base-patch32_metadata.json').write_text(r.text)
print('CLIP',meta['sha'],flush=True)
snapshot_download(repo,revision=meta['sha'],local_dir=str(root/'models/clip-vit-base-patch32'),allow_patterns=['*.json','merges.txt','vocab.json','pytorch_model.bin'],max_workers=4)
print('CLIP_COMPLETE',flush=True)
url=AlexNet_Weights.IMAGENET1K_V1.url
print('ALEXNET',url,flush=True)
torch.hub.load_state_dict_from_url(url,progress=False,check_hash=True,map_location='cpu')
print('ALL_METRIC_MODELS_COMPLETE',flush=True)
