# TxnMem 正式论文任务状态

更新时间：2026-08-06。以下状态区分“代码/实验接口已完成”“已完成正式 batch”“仍不足以支撑生产级论文结论”，避免把环境验证误写成 benchmark accuracy。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成 | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON、minimal counterexample 报告 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成（含真实服务 smoke） | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix；SQLite 与 `VectorGraphMemoryBackend`；真实 Qdrant 1.11.5/Neo4j 5.22.0 direct service smoke | 尚非生产级跨主机部署；需要扩大真实多 Agent 事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair 等 mutant/ablation 及 kill/evaluation 报告 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（含 native batch） | τ-bench、LoCoMo、AppWorld projection replay、trace realism、episode-level holdout；固定 50/20/10 manifest、task-level split/hash 与 `benchmark-native-batch`；native batch 已产生 τ 497、AppWorld 49、LoCoMo 44 events | 需要把 native trace 与 synthetic 联合分布做正式显著性/置信区间分析 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层 + 真实 backend E2E smoke） | GPU endpoint；5×10 repetition；官方 runtime native batch；5 个 τ-bench + Qdrant/Neo4j E2E，5/5 completed，mean 17,879.4 ms、P50 15,351.6 ms | 扩大真实 backend 样本并报告成本、跨主机并发与更高任务质量 |
| 7 | 并发、跨进程、协议故障 | 已完成（真实服务 fault/performance smoke） | 4 类确定性 protocol schedule、5/5 invariant coverage、0 minimal counterexample；真实 Toxiproxy normal/delay/timeout/connection-drop/retry-success；backend p50/p95/p99 | 尚非生产级 2PC；需要多主机、多 Agent 并发和更长故障序列 |
| 8 | τ-bench native runtime | 已完成（50-task batch + retry 合并） | official runtime；50 task manifest；最终合并 497 events、0 replay evaluation error、50/50 evaluator available、reward sum 15、mean 0.3000；2 max-step、1 no-events、1 retry 后 no-events | 官方 reward 非 accuracy，需避免将 0 success 当成 memory 结论 |
| 9 | AppWorld native runtime | 已完成（20-task batch） | official runtime/data/evaluator；按 DB snapshot + task API allowlist；20/20 evaluator available、0/20 task success、17/112 official assertions、49 native events、0 evaluation error | success rate 为 0%，需后续改进 agent/tool prompting；不能把 oracle match 当 task success |
| 10 | LoCoMo executable Agent | 已完成（10-conversation native + official QA） | Qwen2.5-7B native contextual batch：10/10 completed、44 events、0 evaluation error、TxnMem oracle 9/10；官方 QA evaluator：1,986 questions，mean F1=0.3222 | native memory trace 与 QA 运行目前分开；需做严格 task-level paired evaluation |
| 11 | 论文初稿与结果同步 | 已完成（含 render/视觉 QA） | `outputs/TxnMem_论文初稿.docx` 已写入三 benchmark、真实服务与 E2E 结果；已生成 20 页 PNG/PDF，完成逐页视觉检查；DOCX accessibility audit 为 high=0、medium=0、low=0 | 正式投稿前仍需按目标会议模板排版 |
| 12 | Git 备份与远端推送 | 本地备份已完成；远端推送阻塞 | 当前新增代码已有 Git commit；`git remote -v` 为空 | 用户提供远端 URL 后才能 `git remote add origin` / push |

## 论文目前可以正式声称的内容

1. TxnMemBench 的 controlled correctness suite、独立 oracle、failure coverage、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行，并已在 Qwen2.5-7B 上接入 per-task SQLite memory backend 做小规模 replay；当前只能写成 native workflow/runtime smoke 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。新增结果见 `results/remaining_tasks/native_memory_replay/`。

## 仍未完成且不能用措辞掩盖的任务

- τ-bench 50-task 主 batch + 2 个网络错误 task retry 已完成最终合并：50/50 evaluator available，497 native events；2 max-step、1 no-events、1 retry 后 no-events，不能把这些 episode 写成任务成功。
- 真实 backend 当前是单机 Qdrant/Neo4j/Toxiproxy 和 5-task E2E smoke，不是生产级跨主机多 Agent 部署；尚需扩大并发、长流程、成本和跨主机网络实验。
- LoCoMo native memory batch 与官方 QA evaluator 已完成，但二者尚未做逐问题 paired native-memory ablation；当前 0.3222 F1 不能直接归因于 TxnMem。
- Git 远端推送仍阻塞：仓库没有配置 remote URL，无法在没有用户提供 URL 的情况下安全 push。
- DOCX 视觉 render 已通过；使用工作区已有的 Poppler `liblcms2.2.dylib` 由 wrapper 注入 LibreOffice，生成 20 页 PNG/PDF 并完成逐页检查；accessibility audit 无 high/medium/low findings。
