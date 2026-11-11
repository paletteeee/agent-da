import json,math,datetime
from protocol import ROOT,RUN,EMOTIONS,prepare,sha256,atomic_json,audit_outputs,audit_provenance
from evaluate import summarize
m,c=prepare();out=RUN/'outputs';metrics=RUN/'metrics'
audit=audit_outputs(m,out);audit.update(audit_provenance(m,out))
rows=[json.loads(x) for x in (metrics/'per_pair.jsonl').read_text().splitlines()];expected={p['output']:p for p in m['pairs']}
assert len(rows)==3240 and {r['output'] for r in rows}==set(expected)
for r in rows:
 assert all(r[k]==v for k,v in expected[r['output']].items())
 assert r['output_sha256']==sha256(out/r['output'])
s=json.loads((metrics/'summary.json').read_text());assert s['pairs']==3240 and s['inputs']==405
for k,v in summarize(rows).items():assert math.isclose(s['overall'][k],v,rel_tol=1e-12,abs_tol=1e-12)
for name in EMOTIONS:
 selected=[r for r in rows if r['emotion']==name];assert len(selected)==405
 for k,v in summarize(selected).items():assert math.isclose(s['per_emotion'][name][k],v,rel_tol=1e-12,abs_tol=1e-12)
audit.update({'metric_rows':len(rows),'summary_recomputed':True,'summary_sha256':sha256(metrics/'summary.json'),'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});atomic_json(RUN/'final_audit.json',audit)
keys=['PSNR','SSIM','LPIPS','CLIP-I','Emo-A','Emo-S']
lines=['# EmoEdit 全量 benchmark 结果','','405 张输入 × 8 种目标情感 = 3,240 个样本。每种情感 405 个样本。','', '公开 Q-Former_4000 权重；30 步；seed=48000；guidance=7.5；image guidance=1.5。','','| 范围 | '+' | '.join(keys)+' |','|---|'+'---:|'*len(keys)]
for name,values in [('总体',s['overall'])]+list(s['per_emotion'].items()):lines.append('| '+name+' | '+' | '.join(f'{values[k]:.4f}' for k in keys)+' |')
lines+=['','Emo-A 的单位是百分比；Emo-S 是目标情感概率的增量。','PSNR、SSIM、CLIP-I、Emo-A、Emo-S 越高越好；LPIPS 越低越好。','','LPIPS 主列遵循官方代码的 [0,1] 输入；标准 [-1,1] 版本另存为 LPIPS_standard。',f"总体 LPIPS_standard：{s['overall']['LPIPS_standard']:.6f}。",'','本结果使用公开 checkpoint，不保证与论文原始训练权重完全一致。论文表格数值仅供参考，不作为本次实测结果。','', '所有输出已核验尺寸、唯一配对、生成记录和 SHA256；总体与分类汇总已重新计算核对。','']
(RUN/'REPORT.md').write_text('\n'.join(lines));print('FINAL_AUDIT_PASS',json.dumps(audit),flush=True)
