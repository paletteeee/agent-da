# TxnMem 真实模型实验设计

## 目标

把真实 LLM Agent 接入 TxnMem 的 memory event contract，采集可审计的
native memory trace，并用独立 reference executor、任务 evaluator 和
failure controller 形成可复现的真实模型实验闭环。

## 设计决策

1. 模型通过 OpenAI-compatible HTTP 协议调用。仓库不绑定某个推理框架；远程
   GPU 上可以使用 vLLM、TGI 或其他兼容服务。HTTP 客户端使用 Python 标准库，
   本地不需要安装模型依赖。
2. Agent 的 memory 操作必须通过结构化 tool call。`memory_derive` 和
   `memory_propagate` 的 `source_ids` 必须来自 tool arguments 或 backend 返回
   的实际关系，不能由 replay 阶段事后补边。
3. 模型行为、memory event、fault/policy event 和 evaluator 结果分层保存。
   原始 prompt/content 只保存在本地运行目录；仓库提交 aggregate-only 结果。
4. 真实模型 trace 首先作为 `trace-grounded replay` 输入，由现有
   `trace_to_instance` 和独立 reference executor 进行 differential evaluation。
   这一步不把模型输出当作 ground truth。
5. 每个 task 使用固定 model id、prompt hash、seed、temperature、tool schema
   和 failure schedule。模型随机性通过重复 seed 报告，而不是通过单次结果
   下结论。

## 数据流

```text
Task + policy timeline
        |
        v
OpenAI-compatible model --> Agent runner --> Memory tool gateway
                                      |              |
                                      |              +--> canonical event log
                                      +--> fault/policy controller
                                                     |
                                                     v
                       trace adapter --> reference executor --> metrics
```

## 组件边界

- `txnmem_model_protocol.py`: 标准库 HTTP 模型客户端、响应/tool-call 解析和
  stable request metadata。
- `txnmem_real_agent.py`: system/task prompt、tool loop、最大步数、错误记录和
  gateway 调用；不判断 expected outcome。
- `txnmem_real_experiment.py`: task manifest、variant/seed 矩阵、trace 采集、
  differential evaluation 和脱敏 aggregate report。
- `examples/real_model_smoke.py`: 对远程模型 endpoint 的最小可运行入口。
- 现有 `txnmem_event_contract.py`、`txnmem_trace.py`、`txnmem_reference.py`:
  分别负责 native event 验证、trace 转换和独立 oracle。

## 失败与边界

- HTTP timeout、非 JSON 响应、缺少 tool call、未知工具和超出 max steps 都要
  记录为明确 failure reason；不能静默丢弃 trace。
- 缺少 endpoint、model id 或授权时，CLI 应停止并给出配置错误，不生成
  `trace_ground_truth_native=true` 或虚假的模型结果。
- 真实模型 smoke run 通过后，才允许在本地运行目录生成 raw trace；提交仓库
  的文件仅包含计数、oracle match、violation metrics 和配置摘要。

## 验收标准

1. 无第三方模型依赖时，协议解析、tool loop、脱敏报告和失败处理测试通过。
2. 使用 fake OpenAI-compatible server 时，模型可完成 read/write/derive 工具
   循环，derive event 保留真实 `source_ids`。
3. 使用真实 endpoint 时，CLI 能保存 raw trace、sanitized aggregate report，
   并对 trace 运行 independent differential evaluation。
4. 文档明确区分真实模型 native trace、公开数据 projection 和 synthetic suite。
