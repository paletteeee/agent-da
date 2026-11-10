# TxnMem 剩余实验任务设计规格

## 目标

在不削弱独立 ground truth 约束的前提下，补齐当前原型剩余的三类证据：

1. 公开 workflow 的原生 Agent memory event 采集；
2. 最小但可验证的分布式事务故障实验；
3. 统计重复、文档视觉 QA 和可复现交付收尾。

本规格不把公开 benchmark 原生提供的内容误称为 TxnMem memory ground truth，也不把 simulator smoke 结果误称为生产 backend 性能。

## 范围与边界

### 纳入范围

- τ-bench、AppWorld 和 LoCoMo 的统一 native workflow runner 接口；
- 在可用的真实环境中运行少量 Qwen2.5-7B episode，并记录实际 tool-generated memory event；
- 公开数据缺少可执行环境或凭据时，生成机器可读的 blocked report，并保留已有 projection replay 作为单独证据层；
- 一个依赖标准库的 coordinator/participant 状态机，覆盖 prepare、commit、abort、crash-after-prepare 和 deterministic network drop；
- 3 次以上 native repetition 的汇总与简单 Wilson/正态近似置信区间；
- DOCX 渲染、逐页 PNG 检查、结构化和无障碍审计；
- 只提交脱敏 aggregate、代码和配置，不提交 prompt、tool arguments、memory value、API response body 或凭据。

### 明确不纳入范围

- 不实现声称兼容任意生产数据库的完整 2PC 服务；
- 不把 LoCoMo 的自然对话直接当作 memory backend event；
- 不在没有 benchmark 环境、凭据或许可证确认时执行真实外部 API；
- 不报告生产吞吐、尾延迟、真实业务成功率或真实用户分布拟合结论；
- 不自动推送到未配置的 Git remote。

## 方案选择

### 方案 A：统一 native runner + 最小分布式状态机（采用）

新增 benchmark-neutral runner，将公开 workflow task 转成带有 `episode_id`、`context`、`allowed_tools` 和 `memory_policy` 的任务；真实 Agent 只通过 canonical memory tools 写入 instrumented backend。分布式部分采用显式状态机和故障 schedule，在每个状态转换记录 aggregate-only outcome。该方案复用现有 model client、tool loop、event contract 和 reference executor，新增面最小，且可以准确区分“真实 native event”和“projection replay”。

### 方案 B：直接修改各 benchmark 的原始 Agent 环境

为每个 benchmark 写深度定制的 patch。它可能更接近 benchmark 原生执行，但依赖安装、版本、凭据和环境状态多，且难以在本仓库内复现。

### 方案 C：继续扩大 projection replay

实施成本最低，但不能解决“没有真实 memory ground truth”的核心可信度问题，因此不采用作为剩余任务的主要完成方式。

## 架构

```text
public task/context
        |
        v
PublicWorkflowAdapter -- unavailable --> blocked_report.json
        |
        v
NativeWorkflowTask
        |
        v
Qwen2.5-7B / compatible model
        |
        v
Canonical memory tool gateway -> InstrumentedMemoryBackend
        |
        +--> native event contract validator
        +--> independent reference executor
        +--> sanitized aggregate report

coordinator -> participant states -> deterministic fault controller
                          |
                          +--> independent protocol invariant checker
```

### Native event 约束

每个实际 backend event 至少包含 `event_id`、`episode_id`、`agent_id`、`txn_id`、`event_type`、`step` 和必要的 source/reference IDs。memory value 和 prompt 只进入远端 raw run 目录，不进入汇总。derive/propagate 的 provenance 必须来自实际 tool arguments 或 backend call，而不是事后补边。

### Ground truth 约束

reference executor 只读取 canonical event projection、policy timeline、failure schedule 和初始状态；它不读取 model final text，也不采用 TxnMem variant 的 expected outcome。native run 的 acceptance contract 与 oracle match 分开报告。

### 外部依赖处理

- τ-bench/AppWorld 需要检查安装、可执行入口和凭据状态；缺失时只输出 `blocked_external_dependency`，不得静默回退成 projection。
- LoCoMo 不提供原生可执行 Agent 环境；仅在明确构造 Agent memory episode 后运行，结果命名为 `native_contextual_agent_run`。
- DOCX 渲染优先复用 bundled LibreOffice；若 `liblcms2` 缺失，则尝试可逆的临时运行时修复或备用渲染环境，并记录精确失败原因。

## 组件与验收标准

### 1. Public workflow native runner

新增 adapter/runner 和测试，至少覆盖：任务转换、episode 隔离、canonical event 校验、不可用环境的 blocked report、aggregate 脱敏。每个可运行数据源至少完成 smoke episode；每个不可运行数据源必须有非零退出原因和可读报告。

### 2. Distributed protocol smoke

新增 coordinator、participant 和 deterministic fault schedule。对每个 schedule，独立 checker 验证：

- 未完成 prepare 的 participant 不得产生 committed write；
- crash-after-prepare 不得出现半提交事务；
- abort 后不得有可见 committed memory；
- 重试 commit 必须幂等；
- network drop 只能导致 retry/abort，不能导致违反 atomicity。

报告 schedule coverage、invariant coverage、最小反例和每个故障点的最终状态。

### 3. Repetition and confidence intervals

至少使用当前固定 task manifest 的 10 个 task、5 次 repetition；报告 contract success、evaluation error、TxnMem oracle match 及 95% confidence interval。预期失败（例如 crash/policy denied）单独计数，不与正常完成率混淆。

### 4. Document QA and delivery

重新生成初稿，执行 unzip integrity、heading/section audit、a11y audit、PDF/PNG render。逐页检查 PNG 是否存在截断、重叠、表格溢出或字体缺失。视觉渲染仍失败时，交付中明确说明失败命令和动态库原因。

## 错误处理与可复现性

- 外部依赖失败必须记录 `status=blocked`、依赖名称、检查命令、错误类别和下一步，不得返回空成功结果。
- 单 episode replay error 不得中断整批任务；汇总中同时记录 `evaluation_error_count` 和成功 episode 数。
- 所有结果写入带有 manifest hash、source hash、model id、seed、environment metadata 的 aggregate JSON。
- 原始模型输出、prompt、tool arguments、memory payload 和公开数据原文不进入 Git。

## 交付结果

- 新增/修改的 runner、状态机、checker、tests 和配置；
- `results/remaining_tasks/` 下的脱敏 aggregate、blocked reports、coverage 和 confidence interval；
- 更新论文初稿与剩余任务文档；
- 本地 Git commit。远端 Git push 只有在用户提供 remote URL 和权限后执行。
