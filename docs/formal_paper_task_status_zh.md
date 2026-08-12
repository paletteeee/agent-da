# TxnMem 正式论文任务状态

更新时间：2026-08-12。以下状态区分“代码/实验接口已完成”“已完成正式 batch”“仍不足以支撑生产级论文结论”，避免把环境验证误写成 benchmark accuracy。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成；正式口径为 400 instances、2,000 variant rows，TxnMem 0/400 违规且 400/400 oracle match | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON；四类 mutant 均有可重放的 operation-prefix-minimal witness，去掉最后一步后不再复现目标违规 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成（含真实服务 smoke） | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix；SQLite 与 `VectorGraphMemoryBackend`；真实 Qdrant 1.11.5/Neo4j 5.22.0 direct service smoke | 尚非生产级跨主机部署；需要扩大真实多 Agent 事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair、scope bypass 等 mutant/ablation 及 kill/evaluation 报告；`partial_commit`、`remove_commit_revalidation`、`disable_provenance_traversal`、`bypass_scope_check` 四个最小 witness 已固化 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（含高维 joint test） | 400 synthetic instances；按 episode 拆分 calibration/holdout；六维 standardized RBF random-feature MMD permutation test；τ-bench 141/34、LoCoMo 8/2；AppWorld 从 5 个官方 task 重新生成 380 条 method/URL-only 脱敏 projection events 并按 3/2 拆分 | LoCoMo/AppWorld holdout 均只有 2 个 episode，joint-test 低功效；结果显示分布差异，不支持等价性 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层、真实 backend E2E 与跨主机 client-to-server 负载） | GPU endpoint；5×10 repetition；官方 runtime native batch；5 个 τ-bench + Qdrant/Neo4j E2E 为 5/5 completed、30 events、mean 18,851.6 ms、P50 15,497.0 ms；v8 完成 3 次独立 attested cross-host 运行，合计 204 cycles、1,632/1,632 contract success、3,251,506 endpoint-reported tokens，模型 revision `7b44…26b4`、vLLM `0.8.5.post1` | 非生产 latency 结论；无显式 pricing rate，货币成本未计算；不是多主机 Agent workers |
| 7 | 并发、跨进程、协议故障 | 已完成（含真实 Toxiproxy 与 attested cross-host） | 4 类确定性 protocol schedule、5/5 invariant coverage；真实 Toxiproxy 5 场景各 30 次，四个非 normal 场景 trigger/toxic/proxy-path 均 30/30，150 次合计 0 partial commit；v8 configured/observed concurrency=4，3 个 non-overlapping UTC interval、3 个 distinct tunnel process、preflight/final PID-owned listener 与 ControlMaster 连续性均通过 | future work：生产级多主机 Agent workers、连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j 与更长故障序列；不伪装为已完成 |
| 8 | τ-bench native runtime | 已完成（50-task batch + retry 合并） | official runtime；50 个唯一 task ID；最终合并 497 events、50/50 evaluator available、reward sum 15、mean 0.3000；manifest/hash、模型/runtime identity、retry 归并、source artifact hash 与运行命令均已固化 | 官方 reward 非 accuracy，需避免将失败 episode 写成任务成功或 memory 结论 |
| 9 | AppWorld native runtime | 已完成（20-task baseline/tuned 配对） | 同 manifest/condition/tool attestation；baseline 0/20 success、17/112 assertions、517,564 exact tokens；tuned 1/20、53/112，13 task 改善、7 不变；20/20 evaluator available | tuned 有 4 个 unauthorized-tool 和 2 个 model-HTTP execution failure；2,171,632 observed tokens 为下界；n=20 不支持总体显著性声称 |
| 10 | LoCoMo executable Agent | 已完成（10-conversation native + paired official QA + 3 repetitions） | Qwen2.5-7B native contextual batch；paired QA baseline/tuned 各 3 次、每次 1,986 问题、相同 seeds/condition；baseline mean F1=0.13836，tuned=0.13998，平均差值 +0.00162；token 精确增加 40,539 | 仅 3 次描述性 paired repetition，增益很小且一次回退，不能声称总体显著提升 |
| 11 | 论文初稿与结果同步 | 已完成（最终复验关闭） | `outputs/TxnMem_CCF-A中文论文初稿.docx`，SHA-256 `870feaf210bf3a7b9507795988aaf242ae6d759f69a65b2c0fef54b40fe04e6b`；精确 `render_final_v4` 为 27 页 PNG/PDF，6 图、8 表、32 条已核验参考文献；a11y 为 0/0/0，OOXML 无 rsid/批注/追踪/custom props/个人元数据或绝对工作站路径 | 仅待选定 venue 后按其匿名、版式与投稿系统字段进行模板适配 |
| 12 | Claim ledger 与历史结果作废 | 已完成 | 15 条 active claim、132 个字段断言、0 findings；ledger digest `56e985fa4947b54fdc01c3ab4044dd16358d3b8c9078c39b931bae504792a198`，manuscript/artifact audit 均为 0 findings；artifact/hash、命令、manifest/hash、source commit、claim boundary 全部关联；3 个历史 artifact 已进入 supersession index | 新增论文数字时必须同步更新 ledger 并重跑 fail-closed audit |
| 13 | Git 备份与远端推送 | 本地备份已完成；远端推送阻塞 | 投稿前证据代码与脱敏 aggregate 已在隔离分支逐项提交；`git remote -v` 为空 | 用户提供远端 URL 后才能 `git remote add origin` / push |

## 论文目前可以正式声称的内容

1. TxnMemBench 的 400-instance/2,000-row controlled correctness suite、独立 oracle、failure coverage、四类最小 mutant witness、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行，并已在 Qwen2.5-7B 上接入 per-task SQLite memory backend；LoCoMo 已完成同条件 paired repetitions，AppWorld 已完成工具集合逐任务证明一致的 baseline/tuned 配对，joint realism 已完成并显示分布差异。当前只能写成 native workflow/runtime evidence 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。
4. Qwen2.5-7B v8 在修复监听器归属与 ControlMaster 连续性校验后完成 3 次 independently attested cross-host client-to-model-server repetition；合计 1,632/1,632 contract success，408 个 runner-level failures 均为预期 `injected_crash`/`policy_denied` workload 机制，3 份 endpoint/transport analysis 均为 0 相关失败。v7 仅作为修复前审计历史保留，不承担最终结论。
5. 单机 Qdrant/Neo4j 故障矩阵已证明请求真实穿过 Toxiproxy：5 个场景各 30 次，四个非 normal 场景的 trigger/toxic/proxy-path 证据完整，150 次均无 partial commit；5-task E2E 的正式重跑为 5/5 completed、mean 18,851.6 ms、P50 15,497.0 ms，且不作生产 latency 声称。

完整汇总报告见 `docs/current_experiment_report_zh.md`。

## 已完成结果的限制、future work 与外部阻塞

- τ-bench 50-task 主 batch + 2 个网络错误 task retry 已完成最终合并：50/50 evaluator available，497 native events；2 max-step、1 no-events、1 retry 后 no-events，不能把这些 episode 写成任务成功。
- 真实 backend 当前是单机 Qdrant/Neo4j/Toxiproxy 5×30 故障矩阵和 5-task E2E smoke。旧 50/200/1000-events artifact 因未证明 toxic 位于真实请求路径而被 supersede，不再承担正式故障或性能结论。v8 的跨主机 client-to-model-server 负载不改变其不是生产级跨主机多 Agent 部署的边界。
- LoCoMo paired baseline/tuned 各 3 次 repetition 已完成，但平均 F1 只增加 0.00162 且一次回退；这是描述性证据，不是总体显著性结论。直接全文上下文 QA 的 0.3222 F1 仍不能与不同条件的旧 paired run 直接归因比较。
- AppWorld tuned 将官方 success 从 0/20 提至 1/20、断言从 17/112 提至 53/112，但有 6 个 execution failure，token usage 也有 2 次响应缺失；必须按全 20 task 分母报告，并将 tuned token 总量标为观测下界。
- joint realism 已完成，但 τ/LoCoMo/AppWorld 的结果显示分布差异；LoCoMo/AppWorld holdout 均为 2，不支持分布等价性或强推断。
- v8 的跨主机范围为 1 Agent-worker host + 1 model-server host、3 条独立 tunnel；生产级多主机 Agent workers、单一连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j 和有明确定价率的成本核算均为 future work/claim boundary，不能伪装成已完成。
- Claim audit 当前覆盖 15 条正式主张和 132 个字段断言，ledger digest 为 `56e985fa4947b54fdc01c3ab4044dd16358d3b8c9078c39b931bae504792a198`，0 findings；manuscript/artifact audit 同为 0 findings。`results/remaining_tasks/final_status.json`、`results/remaining_tasks/production_evidence_status.json` 和旧 backend fault artifact 仅保留作历史审计，不得再被正文引用为当前状态。
- Git 远端推送仍阻塞：仓库没有配置 remote URL，无法在没有用户提供 URL 的情况下安全 push。
- 最终 DOCX 已关闭复验：`outputs/TxnMem_CCF-A中文论文初稿.docx` 的 SHA-256 为 `870feaf210bf3a7b9507795988aaf242ae6d759f69a65b2c0fef54b40fe04e6b`，对应 `render_final_v4` 的 27 页 PNG/PDF；6 图、8 表、32 条已核验参考文献，a11y=0/0/0。全量测试后 hash/size/mtime 不变；OOXML 和新增 diff 的通用凭据扫描均为 0，未发现 SSH 密码、私有 token、个人 author/company 或绝对工作站路径。论文交付仅剩选定 venue 后的模板适配。
