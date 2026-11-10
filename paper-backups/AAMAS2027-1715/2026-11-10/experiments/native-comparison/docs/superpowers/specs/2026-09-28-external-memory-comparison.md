# 第三方记忆系统发现与一致性比较实验
状态：用户已授权实施两项实验；协议在正式结果生成前冻结。不得预设 TxnMem 胜出。
## 范围
E1 比较 ordinary、random、directed 三种测试策略在 Mem0、G-Memory、A-MEM 上的违规类型、首次发现执行序号与见证重放成功率。
E2 在同一六类双智能体工作负载下比较 TxnMem、Mem0、G-Memory、A-MEM 的违规率、正常任务完成率、误中止率与失效派生残留率。
六类：multi_agent_visibility、interleaved_atomicity、concurrent_supersession、actor_policy_scope、cross_agent_provenance、multi_agent_mixed。
## 运行与版本
仅在用户指定的远程主机执行；独立目录，不修改既有实验。账户认证不写入代码或清单。
TxnMem 以既有 multi-agent-correctness 快照为基础；与上游 45b2d529066b470f809b54313ae52ae90a1666aa 比对相关源文件摘要并记录差异。
Mem0 固定 mem0ai 2.0.18，与旧实验一致；G-Memory 固定 7b581c51d993bd600df14691d101d7e601040cc6；A-MEM 固定 ceffb860f0712bbae97b184d440df62bc910ca8d。
各系统独立进程和依赖环境，每个实例独立状态。禁止读取旧实验结果冒充本轮结果。
## 执行边界
先完成真实库、真实存储操作的受控操作层实验；冻结内容和生成响应以隔离状态管理。
固定生成响应与确定性嵌入必须显式报告，不能声称测试完整生产语义检索、真实模型质量或默认配置。
不得替换被测系统的写入、更新、删除、检索或一致性实现。模型/嵌入替身只允许作为明确标注的受控依赖提供者，不参与不变量判定。
原生能力无合理映射的场景标记 unsupported_mapping，运行失败标记 runtime_error，证据不足标记 inconclusive。
G-Memory 公共 add_memory/retrieve_memory 可用于任务记忆；未发现有效原生单条撤销接口，来源撤销场景不伪造。
actor_policy_scope 在无原生策略入口时不可伪称原生权限漏洞。统一网关扩展如实施，必须单列 deployment_profile，不能与原生结果混合。
应用多操作原子性缺口与被测系统原生承诺缺陷分别分类。
## 判定
以实际返回、读回对象和受控工作负载依赖真值判定；未知状态不默认通过。
删除对象和失效标记均可满足不可见要求；修复真值不得从被测系统返回的边推导。
每次合法完成率必须确认预定效果；全部拒绝和空返回不能通过正常对照。
公开中间状态需要实际读取见证。逻辑 commit 标记不是第三方原生提交。
本轮默认故障为操作边界中断/可捕获后端错误；不得写成 SIGKILL 或跨服务持久化恢复实验。
## 策略
ordinary 按串行正常条件运行，是正常功能对照，不与含故障测试做同分布优越性检验。
random 和 directed 使用相同结构集合及执行预算；random 保留单智能体顺序与来源依赖等必要先后关系。
directed 复用现有触发点；random 在预先声明的合法边界选择扰动，不依据结果重选。
协议修订：既有生成器限制种子 0–49，技术试跑使用 0、1；正式采用完整 0–49，存在试跑复用，不宣称留出验证。E2 主条件为每族50个实例，各带正常对照，共2400个计划项。unsupported保留全分母覆盖表；违规率按明确适用分母给出。
E1 对每个系统每种策略使用固定预注册顺序，报告首次发现序号；未发现作为右删失。不同类别分别计算，重复见证不算不同缺陷。
每个系统/违规类型选择一条可重放见证，至少重放5次；严格保留触发证据。仅做前缀缩减时不称全局最小。
## 输出
inputs.jsonl、observations.jsonl、results.csv、capabilities.json、witnesses.json、replay_results.jsonl、summary.json、environment.json、source_manifest.json、run_manifest.json、report_zh.md。
所有原始执行结果保留，报告不排除不利结果；审计成功与科学假设成立分离。
## 验收
契约/调度/判定单元测试通过；原生适配集成检查通过；运行两项实验并汇总实际覆盖与限制；提交可复现代码与结果，不将试跑当正式结果。

## 正式运行前审查修订
使用简化的原生操作映射；源结构签名仅用于追溯，不作为实际结构多样性证明。记录 executed_structure_sha256 与 effective_parameters，报告每类实际去重结构数。部分工作负载重复同一结构，50 次执行不等于50种结构。
所有发现（包括普通控制）都可选为重放见证。首次发现按每系统/策略/违规类型的全局执行预算序号报告。区分 fault_triggered 和 fault_effective；中断发生在提交之后时不声称实际中断。显式检测失效来源仍可见。Mem0 重复删除异常保留为 runtime_error，不添加补偿改变原生行为。
