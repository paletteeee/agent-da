# 八项补强的实现状态

本轮把八项补强拆成了可以在当前仓库内独立验证的代码接口，并将外部数据/运行环境依赖显式保留。

| 项目 | 当前实现 | 证据/边界 |
|---|---|---|
| 1. τ-bench/AppWorld/LoCoMo trace-grounded replay | `txnmem_trace_pipeline.py` 保留 projection replay；新增 `txnmem_public_native.py` 与 `public-native-smoke`，明确区分 native boundary 和 projection | 三类公开输入均已生成 `blocked_external_dependency` report：远端未安装 τ-bench/AppWorld runtime，LoCoMo 无可执行 Agent 环境；已有 projection replay 仍为 τ-bench 175/920、LoCoMo 10/272、AppWorld 5/380，不是原生 memory ground truth |
| 2. benchmark-specific adapter | `txnmem_bench_adapters.py`：`tau-bench`、`appworld`、`locomo`、`normalized` | 只转换显式 tool/API/memory event；普通对话会被跳过 |
| 3. 真实 Agent memory trace 采集 | `txnmem_model_protocol.py` 的 OpenAI-compatible client、`txnmem_real_agent.py` 的 structured tool loop、`txnmem_public_native.py` 的 public boundary、`txnmem_event_contract.py` validator | 已在远程 RTX 4090 上运行 Qwen2.5-7B-Instruct；完成 1 smoke、8 train、2 holdout，并完成 5×10 native repetition；raw trace 仅保存在远端结果目录，仓库只保留脱敏 aggregate |
| 4. trace 校准与 holdout | `txnmem_realism.py` 的 `calibrate_config`、`calibrated_suite`、`split_holdout` | 按完整 episode 划分，校准只影响 synthetic config，不改变 oracle |
| 5. 并发/micro-witness | `txnmem_interleavings.py` 穷举保持各 agent 局部顺序的所有 linearization；`txnmem_concurrency.py` 提供线程锁 harness；`txnmem_distributed.py` 提供跨进程 owner-linearization；新增 `txnmem_distributed_protocol.py` 和 `process-protocol-smoke` | 已完成 4 类确定性 fault schedule、5/5 invariant coverage、无最小反例；仍不等同于生产级跨进程/分布式事务 |
| 6. incremental repair failure | `txnmem_repair.py` 的 `incremental_repair` 和 `repair_failure_matrix` | 明确定义 crash-after-k repair steps，并报告 unsafe active descendants |
| 7. backend/Agent workflow | `AgentReplayRunner`、`run_real_agent`、`run_repetitions` 接受可替换 model/backend；固定 task、seed、temperature、failure schedule 和 acceptance contract | Qwen2.5-7B 真实 endpoint 已完成 5×10 repetition：50/50 contract、50/50 TxnMem oracle match，0 evaluation error；仍不是生产 backend 或真实多 Agent 长流程 |
| 8. 论文同步 | 根目录脚本/初稿同步 controlled suite、三类官方 replay、protocol smoke、native repetition 和边界 | 初稿已重新生成；结构化/a11y 审计通过；视觉 QA report 已记录，但渲染仍受本机 LibreOffice `liblcms2` 动态库路径和权限阻塞 |

## 当前状态与下一步

1. 已完成三类官方输入的 workflow/API projection replay，并新增 canonical public-native boundary；三类 native run 当前均以机器可读 blocked report 结束，没有静默 fallback；Qwen2.5-7B GPU native trace 采集、train/holdout differential evaluation 已完成。
2. 已保留非敏感的 `trace_replay.csv`、`trace_realism.json` 和 calibration/performance JSON；原始输入及含内容的 instance 文件不提交仓库。
3. 已按 task/trial/sample episode 做 holdout；native Qwen task 已按 seed=17、holdout=0.2 划分 8/2，并报告 task contract、replay error 和 oracle match。仍需将同样 instrumentation 接入真实 τ-bench/AppWorld/LoCoMo Agent 执行。
4. 已完成小规模 interleaving、线程锁、跨进程 owner-linearization smoke harness 和 trigger-based controller；下一步是从真实 trace 生成每个 agent 的局部 operation sequence，并运行完整 linearization 检查。
5. 远程 GPU endpoint、smoke/train/holdout、native event log 与 reference differential comparison 已完成；Qwen2.5-7B 五次重复（50 个 task）保持 50/50 contract 与 50/50 oracle match；最小 distributed protocol smoke 也已完成。仍需真实公开 workflow 原生 Agent trace、生产级 backend/网络故障和端到端性能。

## 当前真实模型实验入口

本地可先运行无模型依赖的协议 fixture：

```bash
python3 examples/real_model_smoke.py \
  --manifest configs/real_model_smoke.json \
  --offline-fixture \
  --out-dir results/real_model_fixture
```

真实 GPU endpoint 使用：

```bash
python3 examples/real_model_smoke.py \
  --manifest configs/real_model_tasks.json \
  --endpoint http://GPU_HOST:8000/v1 \
  --model MODEL_ID \
  --out-dir results/real_model_run
```

当前仓库只提交脱敏 aggregate summary；原始 prompt、tool arguments 和 native
event trace 保存在远端运行目录，不作为公开数据集提交。Qwen2.5-7B 最终证据路径为：
`/data/txnmem/results/real_model_qwen2.5_7b_smoke_final/`、
`/data/txnmem/results/real_model_qwen2.5_7b_splits_rerun/train/` 和
`/data/txnmem/results/real_model_qwen2.5_7b_splits_rerun/holdout/`。
