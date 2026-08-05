# TxnMem 正式论文任务状态

更新时间：2026-08-05。以下状态区分“代码/实验接口已完成”“已完成小规模 smoke”“仍不足以支撑正式论文结论”，避免把环境验证误写成 benchmark accuracy。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成 | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON、minimal counterexample 报告 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成（含持久化 smoke） | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix；新增 `SQLiteInstrumentedMemoryBackend`，真实 tool loop 的事件与状态可跨 reopen 恢复 | 接入真实向量/图 backend 的生产事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair 等 mutant/ablation 及 kill/evaluation 报告 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（projection + 小规模 native backend 层） | τ-bench、LoCoMo、AppWorld projection replay、trace realism、episode-level holdout；新增 SQLite-backed native replay aggregate | 扩大 native memory 样本，并比较 synthetic 与原生 memory trace 的联合分布 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层；backend smoke） | GPU endpoint；5×10 repetition，50 task、110 native events、0 evaluation error、50/50 contract、50/50 oracle match；另完成三类官方 runtime 的 SQLite backend smoke | 扩大真实 backend 样本，并报告端到端任务质量/成本 |
| 7 | 并发、跨进程、协议故障 | 已完成（smoke 层） | 4 类确定性 protocol schedule、5/5 invariant coverage、0 minimal counterexample；线程/owner-linearization harness | 生产级多进程/网络/存储 interleaving 与性能 |
| 8 | τ-bench native runtime | 已完成（runtime + SQLite backend smoke） | official runtime；历史扩样 3/3 tasks、20 events；新增 Qwen2.5-7B/SQLite 1 task、4 events、0 evaluator error、official reward 0.0、oracle 1/1 | 扩大任务数并报告官方 accuracy |
| 9 | AppWorld native runtime | 已完成（runtime + SQLite backend smoke） | official runtime/data/evaluator；Venmo-only schema 1 task、1 event、0 evaluator error、official evaluator 0/7、oracle 1/1；默认全 schema 仅作为失败诊断记录 | 扩大 schema/task 样本并提升官方 success rate；当前 smoke 不足以支持准确率结论 |
| 10 | LoCoMo executable Agent | 已完成（memory-only runtime + SQLite smoke） | 5 conversations contextual smoke 之外，新增 Qwen2.5-7B/SQLite ordered contract：1 conversation、2 events、0 evaluation error、oracle 1/1；LoCoMo QA evaluator 当前边界不可用 | 扩大 conversation 样本并接入可审计 QA evaluator |
| 11 | 论文初稿与结果同步 | 已完成（内容/结构层） | `outputs/TxnMem_论文初稿.docx`、结构审计、a11y 审计 | 本机 LibreOffice 缺少 `liblcms2`，视觉 PNG render 尚未通过 |
| 12 | Git 备份与远端推送 | 本地备份已完成；远端推送阻塞 | 当前新增代码已有 Git commit；`git remote -v` 为空 | 用户提供远端 URL 后才能 `git remote add origin` / push |

## 论文目前可以正式声称的内容

1. TxnMemBench 的 controlled correctness suite、独立 oracle、failure coverage、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行，并已在 Qwen2.5-7B 上接入 per-task SQLite memory backend 做小规模 replay；当前只能写成 native workflow/runtime smoke 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。新增结果见 `results/remaining_tasks/native_memory_replay/`。

## 尚未完成且不能用措辞掩盖的任务

- 公开 benchmark 的大规模 native Agent memory trace：已完成小规模 SQLite-backed smoke，但尚未形成可支撑统计结论的样本量；AppWorld 当前 official 0/7，LoCoMo QA evaluator 不可用。
- 真实向量/图 memory backend、真实网络故障、端到端吞吐/尾延迟/成本与生产级多 Agent 长流程。
- LoCoMo 的稳定大规模 QA 评测，以及 τ-bench/AppWorld 的正式官方 success/accuracy 统计。
- DOCX 的视觉 render：结构与可访问性已通过，但本机 LibreOffice 动态库缺失导致 PNG QA 阻塞。
- 远程 Git 推送：仓库没有配置 remote，无法在没有 URL 的情况下安全推送。
