import pathlib,sys,json,numpy as np,torch
from PIL import Image
from protocol import ROOT,RUN,atomic_json
from evaluate import Evaluator,crop_image
sys.path.insert(0,str(ROOT/'benchmark/upstream/metrics'))
from Metric_args import count_ssim,count_psnr,count_lpips,count_CLIP_I
m=json.loads((RUN/'manifest.json').read_text());pairs=m['pairs'][:8];originals=[];edited=[]
for p in pairs:
 with Image.open(p['source']) as im:originals.append(crop_image(im))
 with Image.open(RUN/'smoke'/p['output']) as im:edited.append(im.convert('RGB').resize((512,512)))
e=Evaluator();batch=e.batch(originals,edited,[x['target_index'] for x in pairs]);checks=[]
with torch.no_grad():
 for p,src,dst,row in zip(pairs,originals,edited,batch):
  a,b=np.array(src),np.array(dst)
  scalar={'SSIM':count_ssim(b,a),'PSNR':count_psnr(b,a),'LPIPS':count_lpips(b,a,e.lpips),'CLIP-I':count_CLIP_I(dst,src,e.clip,e.processor)}
  def prob(im):return e.classifier(e.clip.get_image_features(**e.processor(images=im,return_tensors='pt',padding=True).to('cuda'))).cpu().numpy()[0]
  sp,dp=prob(src),prob(dst);scalar['Emo-A']=int(int(dp.argmax())==p['target_index']);scalar['Emo-S']=float(dp[p['target_index']]-sp[p['target_index']])
  errors={k:abs(float(row[k])-float(v)) for k,v in scalar.items()};assert all(v<1e-5 for v in errors.values()),errors;checks.append({'output':p['output'],'absolute_errors':errors})
 identity=e.batch([originals[0]],[originals[0]],[0])[0]
 assert identity['LPIPS']<1e-7 and identity['LPIPS_standard']<1e-7 and abs(identity['CLIP-I']-1)<1e-6 and identity['Emo-S']==0 and identity['SSIM']==1
atomic_json(RUN/'metric_equivalence.json',{'scalar_vs_batch':checks,'identity_pair':{'LPIPS':identity['LPIPS'],'LPIPS_standard':identity['LPIPS_standard'],'CLIP-I':identity['CLIP-I'],'Emo-S':identity['Emo-S'],'SSIM':identity['SSIM']},'passed':True})
print('OFFICIAL_METRIC_EQUIVALENCE_PASS',flush=True)
