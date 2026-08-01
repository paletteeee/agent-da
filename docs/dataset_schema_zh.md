# TxnMemBench 数据集 Schema（中文版）

## 实例对象

JSONL 中的每一行都是一个可复现的、受控的实验实例。

| 字段 | 类型 | 含义 |
|---|---|---|
| instance_id | 字符串 | 稳定的 workload/seed 标识 |
| workload | 字符串 | W1–W8 之一 |
| seed | 整数 | 用于复现实例的随机种子 |
| config | 对象 | agent 数量、事务大小、provenance 深度、分支因子、并发度和策略变化次数 |
| initial_memories | 数组 | 回放开始前已经存在的 memory 记录 |
| operations | 数组 | 按顺序排列的 memory 操作 |
| policies | 数组 | 带版本的读、写和搜索权限 |
| failure_schedule | 数组 | 按步骤注入的 crash、revoke、delay、invalidate 或 repair 事件 |
| provenance_edges | 数组 | source_id 到 derived_id 的来源关系 |
| expected_outcome | 对象 | 事务状态、应提交的 memory、应修复的 memory 和目标不变量 |

## Memory 对象

每条 memory 记录至少包含：

- memory_id：唯一标识；
- agent_id：创建者或拥有者 agent；
- scope：访问范围，例如 tenant:user_001；
- entity_id 和 attribute：逻辑实体及其字段；
- value：保存的事实内容；
- status：active、pending、superseded 或 invalid；
- policy_version：写入时使用的策略版本；
- supersedes_id：被当前 memory 替代的旧 memory；
- derived_from：直接来源 memory 的 ID 列表。

## Operation 操作类型

- begin_txn：开启事务并记录事务开始时的策略版本；
- write：写入或缓存一条 memory；
- read：在 scope 检查下读取指定 memory；
- search：在调用者允许的 scope 内搜索 memory；
- get_by_id：按 ID 直接读取，但仍必须执行 scope 检查；
- supersede：让新 memory 替代旧 memory；
- propagate：记录派生 memory 或 provenance 更新；
- invalidate：使源 memory 失效；
- commit：完成策略重新校验后，提交缓存中的写入。

每个 operation 都必须包含唯一的 op_id、非递减的整数 step、agent_id 和 type。

## Workload 家族

| 名称 | 测试目标 |
|---|---|
| atomic_multi_write | 第一条写入后崩溃时，不能产生半提交事务 |
| crash_during_commit | commit 边界崩溃后，只能得到完整提交或完全不提交 |
| revoke_before_commit | 写入后被撤权时，commit 必须重新检查权限 |
| scope_bypass | search 和按 ID 读取路径必须执行一致的 scope 检查 |
| supersession_consistency | 新旧 memory 的替代关系和状态必须一致 |
| provenance_chain_repair | 根 memory 失效后，链上的所有派生 memory 都应修复 |
| provenance_branch_repair | 根 memory 失效后，所有分支上的派生 memory 都应修复 |
| mixed_stress | 组合写入、策略变化、崩溃和恢复压力 |

## 系统变体

- Naive：立即写入，不提供事务、策略、scope 和 repair 保护；
- TxnMem-NoTxn：关闭事务缓存；
- TxnMem-NoPolicyCommit：关闭 commit 时的策略重新校验；
- TxnMem-NoRepair：关闭 provenance repair；
- TxnMem：完整的参考实现。

## 不变量违规名称

- atomicity_violation：原子性违规；
- unexpected_commit：不应发生的提交；
- recovery_consistency_violation：崩溃恢复一致性违规；
- invalid_commit_violation：撤权后的非法提交；
- stale_write_violation：旧权限或旧状态导致的残留写入；
- scope_leak_violation：scope 越权泄露；
- supersession_consistency_violation：新旧 memory 替代关系不一致；
- provenance_closure_violation：provenance 闭包未修复。

## 结果 CSV 字段

每个实例/变体组合输出一行结果，包含 workload 标识、事务状态、违规名称、提交数量、操作数量、repair 数量，以及以下指标：

- partial_update_rate：半更新率；
- invalid_commit_rate：非法提交率；
- stale_write_rate：残留旧写入率；
- repair_recall：provenance 修复召回率；
- leak_rate：信息泄露率；
- supersession_consistency：supersession 一致性；
- scope_bypass_rate：scope 绕过率；
- latency：回放延迟代理指标；
- any_violation：是否存在任意违规。

## 输出文件

完整的 experiment 命令会生成：

- data/generated_instances.jsonl：生成的测试实例；
- results/experiment_results.csv：每个实例/变体的详细结果；
- results/summary.json：按 workload 和 variant 聚合的均值、标准差；
- results/figures/violation_rate.svg：违规率图；
- results/figures/repair_recall.svg：provenance 修复召回率图。

## 运行方式

在服务器的 /data/txnmem 目录下执行：

~~~bash
python3 src/txnmem_experiment.py experiment --out-dir /data/txnmem --seeds 10
~~~

运行全部测试：

~~~bash
python3 -m unittest discover -s tests -v
~~~

