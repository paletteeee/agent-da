# TxnMem 正式论文任务状态

更新时间：2026-08-05。以下状态区分“代码/实验接口已完成”“已完成小规模 smoke”“仍不足以支撑正式论文结论”，避免把环境验证误写成 benchmark accuracy。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成 | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON、minimal counterexample 报告 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成 | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix | 接入真实 memory backend 的生产事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair 等 mutant/ablation 及 kill/evaluation 报告 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（projection 层） | τ-bench、LoCoMo、AppWorld projection replay、trace realism、episode-level holdout | 完成公开 runtime 的大规模 native memory trace，并比较联合分布 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层） | GPU endpoint；5×10 repetition，50 task、110 native events、0 evaluation error、50/50 contract、50/50 oracle match，Wilson 95% 下界 0.929 | 换成真实 memory backend，并报告端到端任务质量/成本 |
| 7 | 并发、跨进程、协议故障 | 已完成（smoke 层） | 4 类确定性 protocol schedule、5/5 invariant coverage、0 minimal counterexample；线程/owner-linearization harness | 生产级多进程/网络/存储 interleaving 与性能 |
| 8 | τ-bench native runtime | 已完成（扩样 smoke 层） | official runtime 安装；3/3 tasks、20 events、0 evaluator error、official reward 均 0.0、oracle 3/3 | 接入真实 memory read/write/derive instrumentation；报告官方 accuracy |
| 9 | AppWorld native runtime | 已完成（扩样 smoke 层） | official runtime/data/evaluator 安装；2/2 tasks、4 events、official evaluator 0/14、oracle 2/2 | 降低 schema/context 限制；报告官方 success rate |
| 10 | LoCoMo executable Agent | 已完成（扩样 contextual smoke 层） | 5 conversations、40 events、4/5 complete、1 network error、0 evaluator error、TxnMem oracle 4/5 | 稳定 endpoint，接入真实 memory backend，并报告 LoCoMo QA/long-memory 指标 |
| 11 | 论文初稿与结果同步 | 已完成（内容/结构层） | `outputs/TxnMem_论文初稿.docx`、结构审计、a11y 审计 | 本机 LibreOffice 缺少 `liblcms2`，视觉 PNG render 尚未通过 |
| 12 | Git 备份与远端推送 | 本地备份已完成；远端推送阻塞 | 当前新增代码已有 Git commit；`git remote -v` 为空 | 用户提供远端 URL 后才能 `git remote add origin` / push |

## 论文目前可以正式声称的内容

1. TxnMemBench 的 controlled correctness suite、独立 oracle、failure coverage、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行并完成 smoke，但当前只能写成 native workflow/runtime smoke 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。

## 尚未完成且不能用措辞掩盖的任务

- 公开 benchmark 的大规模 native Agent memory trace：当前只有小规模 smoke，尚未形成可支撑统计结论的样本量。
- 真实向量/图 memory backend、真实网络故障、端到端吞吐/尾延迟/成本与生产级多 Agent 长流程。
- LoCoMo 的稳定大规模 QA 评测，以及 τ-bench/AppWorld 的正式官方 success/accuracy 统计。
- DOCX 的视觉 render：结构与可访问性已通过，但本机 LibreOffice 动态库缺失导致 PNG QA 阻塞。
- 远程 Git 推送：仓库没有配置 remote，无法在没有 URL 的情况下安全推送。
