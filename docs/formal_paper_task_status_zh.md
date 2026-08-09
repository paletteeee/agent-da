# TxnMem 正式论文任务状态

更新时间：2026-08-10。以下状态区分“代码/实验接口已完成”“已完成正式 batch”“仍不足以支撑生产级论文结论”，避免把环境验证误写成 benchmark accuracy。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成 | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON、minimal counterexample 报告 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成（含真实服务 smoke） | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix；SQLite 与 `VectorGraphMemoryBackend`；真实 Qdrant 1.11.5/Neo4j 5.22.0 direct service smoke | 尚非生产级跨主机部署；需要扩大真实多 Agent 事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair 等 mutant/ablation 及 kill/evaluation 报告 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（含高维 joint test） | 400 synthetic instances；按 episode 拆分 calibration/holdout；六维 standardized RBF random-feature MMD permutation test；τ-bench 141/34、LoCoMo 8/2；AppWorld 从 5 个官方 task 重新生成 380 条 method/URL-only 脱敏 projection events 并按 3/2 拆分 | LoCoMo/AppWorld holdout 均只有 2 个 episode，joint-test 低功效；结果显示分布差异，不支持等价性 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层、真实 backend E2E 与跨主机 client-to-server 负载） | GPU endpoint；5×10 repetition；官方 runtime native batch；5 个 τ-bench + Qdrant/Neo4j E2E，5/5 completed；v7 3 次独立 attested cross-host 运行合计 204 cycles、1,632/1,632 contract success、3,251,534 endpoint-reported tokens，模型 revision `7b44…26b4`、vLLM `0.8.5.post1` | 非生产 latency 结论；无显式 pricing rate，货币成本未计算；不是多主机 Agent workers |
| 7 | 并发、跨进程、协议故障 | 已完成（含 attested cross-host client-to-server） | 4 类确定性 protocol schedule、5/5 invariant coverage、0 minimal counterexample；真实 Toxiproxy normal/delay/timeout/connection-drop/retry-success；v7 configured/observed concurrency=4，3 个 non-overlapping UTC interval、3 个 distinct tunnel process、endpoint/transport failure=0 | future work：生产级多主机 Agent workers、连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j 与更长故障序列；不伪装为已完成 |
| 8 | τ-bench native runtime | 已完成（50-task batch + retry 合并） | official runtime；50 task manifest；最终合并 497 events、0 replay evaluation error、50/50 evaluator available、reward sum 15、mean 0.3000；2 max-step、1 no-events、1 retry 后 no-events | 官方 reward 非 accuracy，需避免将 0 success 当成 memory 结论 |
| 9 | AppWorld native runtime | 已完成（20-task baseline/tuned 配对） | 同 manifest/condition/tool attestation；baseline 0/20 success、17/112 assertions、517,564 exact tokens；tuned 1/20、53/112，13 task 改善、7 不变；20/20 evaluator available | tuned 有 4 个 unauthorized-tool 和 2 个 model-HTTP execution failure；2,171,632 observed tokens 为下界；n=20 不支持总体显著性声称 |
| 10 | LoCoMo executable Agent | 已完成（10-conversation native + paired official QA + 3 repetitions） | Qwen2.5-7B native contextual batch；paired QA baseline/tuned 各 3 次、每次 1,986 问题、相同 seeds/condition；baseline mean F1=0.13836，tuned=0.13998，平均差值 +0.00162；token 精确增加 40,539 | 仅 3 次描述性 paired repetition，增益很小且一次回退，不能声称总体显著提升 |
| 11 | 论文初稿与结果同步 | 已完成（含 render/视觉 QA） | `outputs/TxnMem_论文初稿.docx` 已写入三 benchmark、真实服务与 E2E 结果；已生成 20 页 PNG/PDF，完成逐页视觉检查；DOCX accessibility audit 为 high=0、medium=0、low=0 | 正式投稿前仍需按目标会议模板排版 |
| 12 | Git 备份与远端推送 | 本地备份已完成；远端推送阻塞 | 代码修复提交 `15b8e69` 与脱敏 v7 结果提交 `c86b46f` 已在本地保存；`git remote -v` 为空 | 用户提供远端 URL 后才能 `git remote add origin` / push |

## 论文目前可以正式声称的内容

1. TxnMemBench 的 controlled correctness suite、独立 oracle、failure coverage、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行，并已在 Qwen2.5-7B 上接入 per-task SQLite memory backend；LoCoMo 已完成同条件 paired repetitions，AppWorld 已完成工具集合逐任务证明一致的 baseline/tuned 配对，joint realism 已完成并显示分布差异。当前只能写成 native workflow/runtime evidence 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。
4. Qwen2.5-7B v7 完成 3 次 independently attested cross-host client-to-model-server repetition；合计 1,632/1,632 contract success，408 个 runner-level failures 均为预期 `injected_crash`/`policy_denied` workload 机制，3 份 endpoint/transport analysis 均为 0 相关失败。

完整汇总报告见 `docs/current_experiment_report_zh.md`。

## 已完成结果的限制、future work 与外部阻塞

- τ-bench 50-task 主 batch + 2 个网络错误 task retry 已完成最终合并：50/50 evaluator available，497 native events；2 max-step、1 no-events、1 retry 后 no-events，不能把这些 episode 写成任务成功。
- 真实 backend 当前是单机 Qdrant/Neo4j/Toxiproxy 和 5-task E2E smoke；50/200/1000 events 的真实服务性能扩展已完成各 30 次。v7 已补足跨主机 client-to-model-server 负载，不改变其不是生产级跨主机多 Agent 部署的边界。
- LoCoMo paired baseline/tuned 各 3 次 repetition 已完成，但平均 F1 只增加 0.00162 且一次回退；这是描述性证据，不是总体显著性结论。直接全文上下文 QA 的 0.3222 F1 仍不能与不同条件的旧 paired run 直接归因比较。
- AppWorld tuned 将官方 success 从 0/20 提至 1/20、断言从 17/112 提至 53/112，但有 6 个 execution failure，token usage 也有 2 次响应缺失；必须按全 20 task 分母报告，并将 tuned token 总量标为观测下界。
- joint realism 已完成，但 τ/LoCoMo/AppWorld 的结果显示分布差异；LoCoMo/AppWorld holdout 均为 2，不支持分布等价性或强推断。
- v7 的跨主机范围为 1 Agent-worker host + 1 model-server host、3 条独立 tunnel；生产级多主机 Agent workers、单一连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j 和有明确定价率的成本核算均为 future work/claim boundary，不能伪装成已完成。
- Git 远端推送仍阻塞：仓库没有配置 remote URL，无法在没有用户提供 URL 的情况下安全 push。
- DOCX 视觉 render 已通过；使用工作区已有的 Poppler `liblcms2.2.dylib` 由 wrapper 注入 LibreOffice，生成 20 页 PNG/PDF 并完成逐页检查；accessibility audit 无 high/medium/low findings。
