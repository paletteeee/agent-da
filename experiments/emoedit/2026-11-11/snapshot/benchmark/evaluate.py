import os
os.environ['HF_HUB_OFFLINE']='1';os.environ['TRANSFORMERS_OFFLINE']='1';os.environ['TORCH_HOME']='/data/emoedit-baseline/models/torch'
import argparse,pathlib,json,sys,time,math
import numpy as np
from PIL import Image
from skimage.metrics import structural_similarity,peak_signal_noise_ratio
from protocol import ROOT,RUN,EMOTIONS,prepare,sha256,atomic_json,ensure_protocol,audit_outputs,audit_provenance

def crop_image(img):
 img=img.convert('RGB');w,h=img.size;s=min(w,h)
 return img.crop(((w-s)//2,(h-s)//2,(w+s)//2,(h+s)//2)).resize((512,512))
def pixel_metrics(original,edited):
 r=int(original.max())-int(original.min())
 return {'SSIM':float(structural_similarity(edited,original,channel_axis=2)),'PSNR':float(peak_signal_noise_ratio(edited,original,data_range=r)),'PSNR_fixed255':float(peak_signal_noise_ratio(edited,original,data_range=255))}
def emotion_metrics(original_probs,edited_probs,target):
 pred=int(np.argmax(edited_probs))
 return {'Emo-A':int(pred==target),'Emo-S':float(edited_probs[target]-original_probs[target]),'prediction':pred}
def summarize(rows):
 keys=['PSNR','SSIM','LPIPS','CLIP-I','Emo-A','Emo-S','LPIPS_standard','PSNR_fixed255']
 return {k:float(np.mean([r[k] for r in rows]))*(100 if k=='Emo-A' else 1) for k in keys}
class Evaluator:
 def __init__(self):
  import torch,lpips
  torch.backends.cudnn.allow_tf32=False;torch.backends.cuda.matmul.allow_tf32=False
  from transformers import CLIPModel,CLIPProcessor
  sys.path.insert(0,str(ROOT/'benchmark/upstream/metrics'))
  from classifier_model import clip_classifier
  self.torch=torch;self.clip=CLIPModel.from_pretrained(ROOT/'models/clip-vit-base-patch32',local_files_only=True).eval().cuda()
  self.processor=CLIPProcessor.from_pretrained(ROOT/'models/clip-vit-base-patch32',local_files_only=True)
  self.classifier=clip_classifier(8).cuda().eval();self.classifier.load_state_dict(torch.load(ROOT/'benchmark/upstream/metrics/weight/2024-02-23-best.pth',map_location='cpu',weights_only=True))
  self.lpips=lpips.LPIPS(net='alex',version='0.1').eval().cuda()
 def batch(self,originals,edited,targets):
  import torch.nn.functional as F
  torch=self.torch
  with torch.no_grad():
   features=self.clip.get_image_features(**self.processor(images=originals+edited,return_tensors='pt').to('cuda'))
   a,b=features.chunk(2,dim=0);probs=self.classifier(features).cpu().numpy();pa,pb=np.split(probs,2)
   clip_i=F.cosine_similarity(a,b,dim=-1).cpu().numpy()
   ta=torch.from_numpy(np.stack([np.array(x) for x in originals])).permute(0,3,1,2).float().cuda()/255
   tb=torch.from_numpy(np.stack([np.array(x) for x in edited])).permute(0,3,1,2).float().cuda()/255
   official=self.lpips(tb,ta).flatten().cpu().numpy()
   standard=self.lpips(tb*2-1,ta*2-1).flatten().cpu().numpy()
  rows=[]
  for i,(src,dst,target) in enumerate(zip(originals,edited,targets)):
   rows.append({**pixel_metrics(np.array(src),np.array(dst)),**emotion_metrics(pa[i],pb[i],target),'LPIPS':float(official[i]),'LPIPS_standard':float(standard[i]),'CLIP-I':float(clip_i[i]),'original_target_probability':float(pa[i,target]),'edited_target_probability':float(pb[i,target])})
  return rows

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--smoke',action='store_true');parser.add_argument('--batch-size',type=int,default=16);args=parser.parse_args()
 manifest,config=prepare();out=RUN/('smoke' if args.smoke else 'outputs');pairs=manifest['pairs'][:8] if args.smoke else manifest['pairs']
 audit_outputs({'pairs':pairs},out);audit_provenance({'pairs':pairs},out)
 target=RUN/('smoke_metrics' if args.smoke else 'metrics');target.mkdir(exist_ok=True)
 meta=json.loads((ROOT/'models/clip-vit-base-patch32_metadata.json').read_text())
 protocol={'generation_protocol_sha256':sha256(out/'generation_protocol.json'),'metric_code_sha256':sha256(__file__),'clip':meta['id'],'clip_revision':meta['sha'],'classifier_sha256':sha256(ROOT/'benchmark/upstream/metrics/weight/2024-02-23-best.pth'),'LPIPS':'alex v0.1; upstream [0,1], standard additional [-1,1]','PSNR':'upstream original max-min; fixed255 additional','class_order':EMOTIONS,'expected_count':len(pairs),'batch_size':args.batch_size,'TF32':False}
 ensure_protocol(target/'protocol.json',protocol)
 evaluator=Evaluator();rows=[];t=time.perf_counter();tmp=target/'per_pair.jsonl.tmp'
 with tmp.open('w') as f:
  for offset in range(0,len(pairs),args.batch_size):
   chunk=pairs[offset:offset+args.batch_size];originals=[];edited=[]
   for pair in chunk:
    with Image.open(pair['source']) as im:originals.append(crop_image(im))
    with Image.open(out/pair['output']) as im:edited.append(im.convert('RGB').resize((512,512)))
   values=evaluator.batch(originals,edited,[x['target_index'] for x in chunk])
   for pair,value in zip(chunk,values):
    if not all(math.isfinite(float(value[k])) for k in ['PSNR','SSIM','LPIPS','CLIP-I','Emo-A','Emo-S','LPIPS_standard','PSNR_fixed255']):raise ValueError(f'Nonfinite metric: {pair}')
    row={**pair,'output_sha256':sha256(out/pair['output']),**value};rows.append(row);f.write(json.dumps(row,allow_nan=False)+'\n')
   f.flush();atomic_json(target/'progress.json',{'completed':len(rows),'total':len(pairs),'elapsed_seconds':time.perf_counter()-t})
   if len(rows)%160==0 or len(rows)==len(pairs):print('METRICS_PROGRESS',len(rows),len(pairs),flush=True)
 os.replace(tmp,target/'per_pair.jsonl')
 confusion=np.zeros((8,8),dtype=int)
 for row in rows:confusion[row['target_index'],row['prediction']]+=1
 counts={emotion:sum(r['emotion']==emotion for r in rows) for emotion in EMOTIONS}
 if not args.smoke and any(v!=405 for v in counts.values()):raise ValueError('Incomplete emotion coverage')
 summary={'scope':'smoke' if args.smoke else 'full_official_test_set','pairs':len(rows),'inputs':len({r['image_id'] for r in rows}),'overall':summarize(rows),'per_emotion':{emotion:summarize([r for r in rows if r['emotion']==emotion]) for emotion in EMOTIONS},'count_per_emotion':counts,'confusion_target_rows_prediction_columns':confusion.tolist(),'class_order':EMOTIONS,'elapsed_seconds':time.perf_counter()-t,'paper_table1_reference':{'PSNR':16.62,'SSIM':.571,'LPIPS':.289,'CLIP-I':.828,'Emo-A':50.09,'Emo-S':.335},'notes':['Emo-A is percentage; Emo-S is probability increment.','LPIPS follows published repository [0,1] input convention; LPIPS_standard uses recommended [-1,1].','Evaluation uses released Q-Former_4000 checkpoint; original paper training checkpoint identity is not established.','Original paper table numbers are reference values, not measurements from this run.']}
 atomic_json(target/'summary.json',summary);atomic_json(target/'progress.json',{'status':'complete','completed':len(rows),'total':len(pairs)})
 print('METRICS_COMPLETE',json.dumps(summary['overall']),flush=True)
if __name__=='__main__':main()
