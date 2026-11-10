# 两项原生对比实验：复现说明

本目录包含受控配置下实际运行的两个实验：第三方问题发现和共享记忆一致性比较。正式数据不是模拟器生成的预设结论。原始调用、快照、失败、见证、重放和审计均在 results/external-formal-v1。

## 适用范围

这是面向应用契约的原生操作实验。固定模型/嵌入提供器，保留 Mem0、G-Memory、A-MEM 的原生记忆 API 与存储实现。Mem0 infer=False；A-MEM 模型固定不演化；G-Memory 检索 top-k=50、threshold=-1。不能据此推断默认完整智能体流程、语义质量、并发提交内部交错或真实进程崩溃行为。

程序复用既有六类生成器的采样参数，再映射成可通过原生接口执行的简化程序。部分采样维度未生效，实际结构可能重复；去重数量见 effective_workload_shapes.csv。不把不支持的能力或运行错误记成成功，也不补上基线不存在的事务保障。

## 环境和源码

使用 Python 3.12。两个环境分离以解决 Mem0 与 Chroma 对 posthog 的版本约束冲突。CPU Torch 下载源与一般 PyPI 包不同：

    python3.12 -m venv .venv
    python3.12 -m venv .venv-mem0
    .venv/bin/python -m pip install torch==2.6.0+cpu --index-url https://download.pytorch.org/whl/cpu
    .venv/bin/python -m pip install -r requirements-external-native.txt --extra-index-url https://download.pytorch.org/whl/cpu
    .venv-mem0/bin/python -m pip install -r requirements-external-mem0.txt
    python3.12 scripts/fetch_external_comparison_sources.py

依赖清单来自实际环境。源码获取脚本按固定提交下载所需的 upstream 文件并校验 SHA256；不会覆盖不同内容的已有文件。数据包未重新分发第三方源代码；运行时将从各原始仓库获取。G-Memory：bingreeky/GMemory，A-MEM：agiresearch/A-mem；版本与文件摘要在 configs/external_source_manifest.json，Mem0 2.0.18。

不需要真实模型服务或 API 密钥。代码中的占位字符串仅为构造原生客户端提供受控测试参数，模型请求不会发往外部服务。

## 执行、审计和绘图

    PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_txnmem_external_*.py tests/test_txnmem_multiagent*.py
    PYTHONPATH=src .venv/bin/python src/txnmem_external_campaign.py --seeds 50 --replays 5 --out results/external-formal-new
    PYTHONPATH=src .venv/bin/python scripts/finalize_external_comparison.py results/external-formal-new
    python3.12 -m venv .venv-report
    .venv-report/bin/python -m pip install -r requirements-external-report.txt
    .venv-report/bin/python scripts/plot_external_comparison.py results/external-formal-new

输出目录必须是新目录，避免覆盖既有结果。每个原生任务实例独立存储；各系统在独立工作进程中执行。50次后轮换进程以释放原生库资源。

65项相关测试、18个子测试通过。新实验自身23项测试通过。既有全套测试尝试412项，仍有2失败/21错误/4跳过：原论文结果与制图夹具缺失、docx依赖及原Git历史基点缺失等；这不表示原有整套论文流水线已通过。

## 结果解释

E1：普通=串行无故障控制，随机=合法随机交错与扰动边界，定向=契约关键边界；普通与定向不具有相同故障覆盖，不能宣称同等故障预算下的算法效率优越。首次发现按固定 seed→六类顺序累计，保留不支持/错误的尝试预算，未发现按右删失。每系统每类违规选择一份完整见证，重放5次，不宣称全局最小。

E2：相同输入哈希配对；四系统共同可比只有可见性和交错原子性。其他工作负载按可支持系统成对比较。正常完成率以完整双智能体控制案例为单位，误中止率以正常逻辑任务为单位；误中止率仅覆盖串行控制。失效残留率以外部真值来源闭包为分母。

Mem0 同一旧记忆的第二次删除会抛异常；保留原始错误并排除于一致性分母，不改成幂等删除。源记录删除和派生闭包清理是不同保证。第三方缺少应用所要求的保证，不等于违反自身API承诺。

E1普通/定向原生执行复用于E2，不是独立重复。试跑种子0、1被包含在正式0–49中，没有留出验证。结果不支持生产环境失败概率或永久零违规保证。
