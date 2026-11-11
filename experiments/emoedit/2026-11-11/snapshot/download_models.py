import os
os.environ['HF_ENDPOINT']='https://hf-mirror.com'
os.environ['HF_HUB_DOWNLOAD_TIMEOUT']='120'
from huggingface_hub import snapshot_download
jobs=[('timbrooks/instruct-pix2pix','models/instruct-pix2pix',['model_index.json','scheduler/*','tokenizer/*','feature_extractor/*','text_encoder/config.json','text_encoder/model.fp16.safetensors','unet/config.json','unet/diffusion_pytorch_model.fp16.safetensors','vae/config.json','vae/diffusion_pytorch_model.fp16.safetensors']),('openai/clip-vit-large-patch14','models/clip-vit-large-patch14',['*.json','merges.txt','vocab.json','model.safetensors'])]
for repo,dest,patterns in jobs:
 print('START',repo,flush=True)
 snapshot_download(repo,local_dir=dest,allow_patterns=patterns,max_workers=4)
 print('DONE',repo,flush=True)
