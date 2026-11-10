# TxnMemBench 原生记忆系统对比实验报告

本报告由实际原生接口调用、事件与独立判定器生成。结论限于受控操作配置下的应用级一致性契约，不代表第三方系统承诺事务语义，也不代表默认 LLM 工作流质量。

每类 1 次执行；共 66 条执行记录、18 个不同输入、12 次额外见证重放。E1 普通/定向执行复用于 E2，不能算作两份独立证据。

## 配置与可比边界

- TxnMem：仓库 MultiAgentStorageCoordinator 与真实 SQLite。Mem0 2.0.18：原生 Memory/Qdrant。A-MEM：原生 note/Chroma。G-Memory：原生任务记忆写入和检索。
- 固定 64 维确定性嵌入。Mem0 infer=False；A-MEM 模型固定返回不演化；G-Memory 模型固定评分，检索 top-k=50、threshold=-1。未覆盖自动抽取、自动演化或默认语义检索质量。
- 扰动在 API 边界打断逻辑任务，或删除/失效来源；没有杀进程或提交内部并发。TxnMem commit 是同步整体调用。
- 六类名称和采样参数来自既有生成器，但使用了公开操作可映射的简化程序。不是原有内部轨迹的逐条复用。实际结构哈希、有效参数及去重数量均留档；版本竞争和策略类可能只有一种结构，不得声称每类 50 种结构。
- 普通测试是无故障串行控制；随机测试随机交错及扰动边界；定向测试命中契约关键边界。两者存在故障覆盖差异，不能据此证明同等故障预算下的搜索算法优势。随机选择在 commit 之后的中断不生效，effective_faults 单列。
- 三个基线不支持策略/混合映射；G-Memory 另无版本删除与来源失效映射。记 NA，不当通过或失败。四系统共同范围只有可见性与交错原子性。
- 技术试跑种子 0、1 包含在正式 0–49 中，没有留出集；没有根据结果筛选正式种子。

## 实验一：第三方系统问题发现

| 系统 | 方式 | 可判定/尝试 | 违规执行 | 首次执行序号 | 发现类型 |
|---|---|---|---|---|---|
| Mem0 | ordinary | 4/6 | 0 | 预算内未发现 | — |
| Mem0 | random | 3/6 | 2 | 2 | aborted_task_residue;partial_update;stale_descendant |
| Mem0 | directed | 3/6 | 3 | 1 | aborted_task_residue;dirty_read;partial_update;stale_descendant |
| G-Memory | ordinary | 2/6 | 0 | 预算内未发现 | — |
| G-Memory | random | 2/6 | 1 | 2 | aborted_task_residue;partial_update |
| G-Memory | directed | 2/6 | 2 | 1 | aborted_task_residue;dirty_read;partial_update |
| A-MEM | ordinary | 4/6 | 0 | 预算内未发现 | — |
| A-MEM | random | 4/6 | 3 | 2 | aborted_task_residue;conflicting_versions;partial_update;stale_descendant |
| A-MEM | directed | 4/6 | 4 | 1 | aborted_task_residue;conflicting_versions;dirty_read;partial_update;stale_descendant |

首次序号按 seed→六类的固定顺序累计，包括不支持和运行错误的预算尝试。未发现记右删失，不填 0。各违规类型首次序号见 experiment1_first_type.csv；各类工作负载见 summary.json。

| 系统 | 违规类型 | 重放成功/尝试 |
|---|---|---|
| Mem0 | aborted_task_residue | 1/1 (100.0%) |
| Mem0 | dirty_read | 1/1 (100.0%) |
| Mem0 | partial_update | 1/1 (100.0%) |
| Mem0 | stale_descendant | 1/1 (100.0%) |
| G-Memory | aborted_task_residue | 1/1 (100.0%) |
| G-Memory | dirty_read | 1/1 (100.0%) |
| G-Memory | partial_update | 1/1 (100.0%) |
| A-MEM | aborted_task_residue | 1/1 (100.0%) |
| A-MEM | conflicting_versions | 1/1 (100.0%) |
| A-MEM | dirty_read | 1/1 (100.0%) |
| A-MEM | partial_update | 1/1 (100.0%) |
| A-MEM | stale_descendant | 1/1 (100.0%) |

每系统每种违规保存第一份完整见证，跨策略选择一次，再创建全新存储重放；不声称见证最小化。

## 实验二：共享记忆一致性

| 工作负载 | 系统 | 违规/可判定 | 正常完成/案例 | 误中止/正常任务 | 残留/受影响派生 | 排除 |
|---|---|---|---|---|---|---|
| 共享可见性 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 共享可见性 | Mem0 | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 共享可见性 | G-Memory | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 共享可见性 | A-MEM | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 交错原子性 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 交错原子性 | Mem0 | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 交错原子性 | G-Memory | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 交错原子性 | A-MEM | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 并发版本替换 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 并发版本替换 | Mem0 | NA | 1/1 (100.0%) | 0/2 (0.0%) | NA | {"runtime_error": 1} |
| 并发版本替换 | G-Memory | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 并发版本替换 | A-MEM | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 执行者策略与作用域 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | NA | {} |
| 执行者策略与作用域 | Mem0 | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 执行者策略与作用域 | G-Memory | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 执行者策略与作用域 | A-MEM | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 跨智能体来源传播 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | 0/1 (0.0%) | {} |
| 跨智能体来源传播 | Mem0 | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | 1/1 (100.0%) | {} |
| 跨智能体来源传播 | G-Memory | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 跨智能体来源传播 | A-MEM | 1/1 (100.0%) | 1/1 (100.0%) | 0/2 (0.0%) | 1/1 (100.0%) | {} |
| 混合工作负载 | TxnMem | 0/1 (0.0%) | 1/1 (100.0%) | 0/2 (0.0%) | 0/1 (0.0%) | {} |
| 混合工作负载 | Mem0 | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 混合工作负载 | G-Memory | NA | NA | NA | NA | {"unsupported_mapping": 1} |
| 混合工作负载 | A-MEM | NA | NA | NA | NA | {"unsupported_mapping": 1} |

违规率分母为完整可判定扰动案例。正常完成率要求两智能体任务完成、预期内容正确且替换内容消失；误中止率分母为正常控制逻辑任务。失效残留分母来自外部来源图，包括中止后应不可见的待发布派生；没有失效事件时为 NA。

experiment2_paired.csv 仅对输入哈希相同且双方均可判定的案例配对。不同能力覆盖范围不汇总总分。样本是结构化场景与重复执行，不计算会掩盖结构重复的独立同分布置信区间。

## 异常与结论限制

原生运行错误 2 条。Mem0 竞争删除同一旧记忆时拒绝第二次删除；保留原生异常，不补幂等删除，不计作一致性通过或违规。详情在 observations.jsonl。
发现应用契约缺口，不等于发现第三方系统违反其自身 API 保证。策略/混合只用于 TxnMem 自检，不能支撑跨系统优越性。
0 次违规只能写成“本次配置与样本中未观察到违规”。正常完成率和误中止率仅覆盖无故障串行控制，不能推断无冲突并发活性或真实任务质量。

## 复现

运行：PYTHONPATH=src .venv/bin/python src/txnmem_external_campaign.py --seeds 50 --replays 5 --out results/external-formal-new
汇总：PYTHONPATH=src .venv/bin/python src/txnmem_external_report.py results/external-formal-new

inputs.jsonl / jobs.json 固定输入和预算；capabilities.json 标注能力排除；observations.jsonl 保留事件/快照/原生结果；witnesses.json 与 replays.jsonl 保留见证；environment.json 固定依赖；run_manifest.json 固定源码与结果哈希；audit.json 记录重算验证。
