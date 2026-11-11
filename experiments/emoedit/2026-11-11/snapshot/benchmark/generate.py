import os
os.environ['HF_HUB_OFFLINE']='1';os.environ['TRANSFORMERS_OFFLINE']='1'
import argparse,pathlib,sys,time,json
import torch
from PIL import Image
from diffusers import StableDiffusionInstructPix2PixPipeline
from transformers import CLIPModel,CLIPProcessor
from protocol import ROOT,RUN,prepare,sha256,ensure_protocol,atomic_json,audit_outputs,audit_provenance
sys.path.insert(0,str(ROOT/'EmoEdit/scripts'))
from model import CombinedModel

def resize(img):
 img=img.convert('RGB');w,h=img.size
 size=(512,int(h/w*512)) if w<h else (int(w/h*512),512)
 return img.resize(size,Image.Resampling.LANCZOS)

@torch.no_grad()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int,default=0);parser.add_argument('--smoke',action='store_true');args=parser.parse_args()
 manifest,config=prepare();pairs=manifest['pairs'][:args.limit] if args.limit else manifest['pairs']
 out=RUN/('smoke' if args.smoke else 'outputs');out.mkdir(exist_ok=True)
 ensure_protocol(out/'generation_protocol.json',{'config':config,'code_sha256':sha256(__file__),'count':len(pairs)})
 audit_provenance({'pairs':pairs},out,complete=False)
 adapter=CombinedModel(76);adapter.load_state_dict(torch.load(ROOT/'models/Q-Former_4000.pth',map_location='cpu',weights_only=True));adapter.eval().cuda()
 pipe=StableDiffusionInstructPix2PixPipeline.from_pretrained(ROOT/'models/instruct-pix2pix',torch_dtype=torch.float16,variant='fp16',local_files_only=True,safety_checker=None,requires_safety_checker=False).to('cuda')
 pipe.set_progress_bar_config(disable=True)
 processor=CLIPProcessor.from_pretrained(ROOT/'models/clip-vit-large-patch14',local_files_only=True)
 clip=CLIPModel.from_pretrained(ROOT/'models/clip-vit-large-patch14',local_files_only=True).eval().cuda()
 torch.cuda.reset_peak_memory_stats();start=time.perf_counter();done=0;generated=0
 log=out/'generation.jsonl'
 for pair in pairs:
  target=out/pair['output']
  if target.exists():
   try:
    with Image.open(target) as old:old.load();assert old.size==(512,512)
    done+=1;continue
   except Exception:raise RuntimeError(f'Invalid existing output: {target}; refusing silent overwrite')
  with Image.open(pair['source']) as original:image=resize(original)
  data=processor(images=image,text=pair['emotion'],return_tensors='pt',padding='max_length').to('cuda')
  embeds=clip(pixel_values=data['pixel_values'],input_ids=data['input_ids'])
  conditioned=adapter(text_embeds=embeds['text_embeds'].unsqueeze(0),img_embeds=embeds['image_embeds'].unsqueeze(0))
  generator=torch.Generator(device='cuda').manual_seed(config['seed']);t=time.perf_counter()
  with torch.autocast('cuda'):
   result=pipe(prompt_embeds=conditioned,image=image,guidance_scale=config['guidance_scale'],image_guidance_scale=config['image_guidance_scale'],num_inference_steps=config['steps'],generator=generator).images[0]
  temp=target.with_suffix('.tmp');result.save(temp,format='JPEG');os.replace(temp,target)
  record={**pair,'sha256':sha256(target),'seconds':time.perf_counter()-t,'size':list(result.size)}
  with log.open('a') as f:f.write(json.dumps(record)+'\n');f.flush()
  done+=1;generated+=1
  progress={'status':'generating','completed':done,'total':len(pairs),'generated_this_run':generated,'elapsed_seconds':time.perf_counter()-start,'last_output':target.name,'peak_allocated_GiB':torch.cuda.max_memory_allocated()/1024**3}
  atomic_json(out/'progress.json',progress)
  if done%40==0 or done==len(pairs):print(json.dumps(progress),flush=True)
 audit=audit_outputs({'pairs':pairs},out);audit.update(audit_provenance({'pairs':pairs},out));atomic_json(out/'generation_complete.json',{**audit,'elapsed_seconds':time.perf_counter()-start,'peak_allocated_GiB':torch.cuda.max_memory_allocated()/1024**3})
 atomic_json(out/'progress.json',{'status':'generation_complete','completed':len(pairs),'total':len(pairs)})
 print('GENERATION_COMPLETE',json.dumps(audit),flush=True)
if __name__=='__main__':main()
