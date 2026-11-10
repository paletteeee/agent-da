# AppWorld 公平 Agent/Tool 策略对照设计

## 目标

在 AppWorld 官方任务与 evaluator 上比较 baseline 和 tuned Agent，同时避免把“可见工具不同”混入 prompt/preflight 的处理效应。正式结果必须能够回答：在任务、模型、工具、采样参数和评测器完全相同的条件下，强化提示与可信预检是否提高官方成功率。

## 对照口径

采用“共享推断工具集”方案。每个任务先只根据公开任务指令推断所需 app，再从 AppWorld `prepare_api_docs(..., include_private_apis=False)` 中加载这些 app 的公开 API。baseline 和 tuned 使用同一个推断函数与同一份排序后的模型可见 schema。

- baseline：原始任务提示；不执行可信预检。
- tuned：强化的规划、工具调用和验证提示；允许框架内部执行只读的可信预检。
- Supervisor 的 profile/password 预检 API 仅供框架内部使用，不进入模型可见 schema，也不能由模型调用。
- 两组使用相同任务 manifest、Qwen2.5-7B 模型修订、vLLM build、temperature、token 上限、超时、memory backend 和 AppWorld runtime/evaluator。

不采用以下两种替代方案：仅依赖 manifest app 列表会因当前 manifest 常只声明 Supervisor 而人为限制任务；暴露全部公开 app 会造成不现实的工具选择与上下文开销。

## 组件与数据流

1. CLI 增加显式 `appworld_tool_strategy`，正式实验固定为 `instruction_inferred`。
2. 每个任务在重置 AppWorld 环境后，依据 instruction 计算 app 集合并加载公开 schema。
3. 运行器把同一模型可见 schema 交给 baseline 或 tuned；tuned 的预检通过单独的 trusted gateway 执行。
4. runtime allowlist 使用模型实际收到的 schema 精确校验；越权和私有 API 调用返回结构化错误且不产生 memory projection。
5. 无论模型成功、失败、超时或达到步数上限，都执行统一的 save → official evaluate → close。
6. 每个任务保存脱敏后的工具集合摘要、工具策略、处理变量、官方分数、失败类型和 token usage；不保存密码、参数值或完整对话。

## 条件指纹与可比性

条件指纹包含 manifest hash、任务 ID 集、模型 ID/修订、模型服务 build、AppWorld/runtime/evaluator 版本、memory backend、生成参数、模型可见工具策略与关键源码身份。`prompt_profile` 和 `trusted_preflight_enabled` 是处理标签，不进入共享条件指纹。比较器拒绝以下输入：指纹为空或不一致、任务集合不一致、官方 evaluator 分母不一致、任一组不完整。

报告同时给出两组官方成功数/总数、任务级配对差异、错误分类、未经补齐的精确 token 总量，以及只有部分响应返回 usage 时的下界标记。

## 错误处理与安全

- schema 加载失败、未授权工具、工具执行失败、模型协议错误和 evaluator 失败分别计数。
- 工具失败不伪装为正常 observation，不产生成功 provenance 事件。
- 终止路径必须幂等地 finalise；close 失败不得覆盖原始失败，但需要写入诊断字段。
- 正式同步与 Git 提交仅包含汇总、脱敏任务记录和条件身份；raw conversation、参数值、password 和 AppWorld 状态目录均排除。

## 验证

先以测试驱动补充：

1. baseline/tuned 在同一任务上得到相同模型可见工具名。
2. baseline 不执行预检，tuned 执行可信预检；预检工具对模型不可见且模型调用会被拒绝。
3. 两组条件指纹相同，处理标签不同。
4. 每种失败路径都调用官方 finalizer。
5. 比较器拒绝不完整或不匹配的运行。

通过本地单元测试后，在远端先运行一个任务的 tuned 冒烟测试，再顺序执行 20-task baseline 和 tuned，最后只拉取脱敏汇总并运行配对比较。

## 范围边界

本轮只回答 AppWorld prompt/tool strategy 的公平比较，不把 trusted preflight 的收益解释为 TxnMem 核心事务机制收益。若两组仍为 0/20，结果按失败证据报告，不通过更改任务集、模型或 evaluator 追求非零分数。
