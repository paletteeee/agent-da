# AAMAS 2027 Submission 1715：代码、数据与结果备份

论文：**Transactional Shared Memory for Multiagent Systems via Policy-Consistent Commit and Provenance-Driven Repair**。

备份日期：2026-11-10。主要来源为 2026-11-09 从 suma 服务器导出的三个压缩包；三个包的 SHA256 均已与随附校验文件核对。最终受控实验快照补充自本仓库已有提交 `45b2d529066b470f809b54313ae52ae90a1666aa`。来源、筛选范围和论文 PDF 的 SHA256 见 [source_archives.json](metadata/source_archives.json)。

这是按论文内容整理的备份，不是整台服务器或所有压缩包的镜像。各实验保留独立代码版本，避免把不同阶段的实现混合成未经验证的新版本。原始文件按字节保存；新增的索引和核验工具单独放置。

## 从论文找到文件

| 论文位置 | 代码与配置 | 数据与结果 | 本次核对 |
| --- | --- | --- | --- |
| 表 1、§3.1：受控正确性、消融、定向变异 | [controlled/src](experiments/controlled/src)、[配置](experiments/controlled/configs) | [multi_agent_controlled_v1](experiments/controlled/results/multi_agent_controlled_v1/results)；包含逐例 CSV、汇总、最小见证、变异与复现报告 | 300 个种子实例、207 个穷举交错；各实现违规数与论文一致；变异暴露率 0.868 |
| 表 2、表 3、§3.2–3.3：Mem0 / G-Memory / A-MEM 原生比较 | [native-comparison/src](experiments/native-comparison/src)、[复现说明](experiments/native-comparison/EXTERNAL_COMPARISON_README.md) | [external-formal-v1](experiments/native-comparison/results/external-formal-v1) | 900 个不同输入、3,300 条执行记录、60 次见证重放；从原始观察重算判定通过；后代残留数 0/242、242/242 对应论文 |
| §3.4、表 4：真实 Qdrant / Neo4j 存储 | [real-storage/src](experiments/real-storage/src)、[运行脚本](experiments/real-storage/scripts/run_multi_agent_real_storage.sh) | [multi_agent_real_storage_v1](experiments/real-storage/results/multi_agent_real_storage_v1/results) | 240 条观察；TxnMem 90 次完整提交、30 次干净中止 |
| 图 5、§3.5、表 4：图规模与并发性能 | [provenance/src](experiments/provenance/src)、[配置](experiments/provenance/configs/provenance_performance_matrix.json) | [provenance_performance_v10_measurements](experiments/provenance/results/provenance_performance_v10_measurements) | 15 个配置、450 次重复、14,400 次成功操作；抽核吞吐量和 p99 与论文一致 |
| 表 4：Qwen 事件集成 | [controlled/src](experiments/controlled/src)、[模型配置](experiments/controlled/configs/real_model_tasks.json) | [repetition_report.json](experiments/controlled/results/remaining_tasks/native_repetitions5/repetition_report.json) | 50 个 episode、110 个事件、50 次契约检查通过 |
| 表 4：τ-bench | [controlled/src](experiments/controlled/src) | [tau_bench_50](experiments/controlled/results/submission_evidence/tau_bench_50) | 50 个任务、497 个事件、reward sum 15、mean 0.3 |
| 表 4：AppWorld | [controlled/src](experiments/controlled/src) | [appworld_tuned](experiments/controlled/results/prompt_profile_formal_v4/appworld_tuned) | 1/20 成功、53/112 官方断言通过 |
| 表 4：LoCoMo | [LoCoMo 实现](experiments/controlled/src/locomo_paired_eval.py) | [locomo_prompt_comparison.json](experiments/controlled/results/prompt_profile_formal_v4/locomo_prompt_comparison.json) | 3 次配对重复，平均 F1 差值 +0.001616958 |

## 原生对比实验的原始数据

[external-formal-v1](experiments/native-comparison/results/external-formal-v1) 是表 2、表 3 的正式结果目录：

- `inputs.jsonl`、`jobs.json`：输入、调度和预算。
- `*-observations.jsonl`：四个系统的原始调用、事件、快照和观察。
- `*-rows.json`、`results.csv`：逐案例判定结果。
- `*-witnesses.json`、`*-replays.jsonl`：见证与重复执行的观察。
- `experiment1_detection.csv`、`experiment2_comparison.csv`、`experiment2_paired.csv`：论文对照表。
- `run_manifest.json`、`environment.json`、`audit.json`：源码身份、环境、文件哈希及原始审计。

`external-pilot-v1`、`external-pilot-v2` 保留作历史记录，不能替代正式结果。E1 与 E2 复用了部分执行记录，不能把两项实验当作独立重复。

原生比较使用的第三方源码沿用原始复现说明的获取方式：运行 `scripts/fetch_external_comparison_sources.py`，按固定提交下载并校验。获取脚本、依赖清单、`configs/external_source_manifest.json` 和 [上游文件清单](metadata/upstream_source_inventory.json)均已保存；第三方安装环境和下载源码副本不在此备份中。

## 历史快照和补充数据

- [historical/server-main](historical/server-main)：原 `/home/suma/txnmem`，包含早期代码、输入、模型运行记录及已有 LoCoMo / τ-bench 数据文件。
- [historical/submission-20260816](historical/submission-20260816)：原提交整理目录。
- [historical/nonroot-6022abd](historical/nonroot-6022abd)：非 root 环境中的后续代码与结果快照。
- [support/scale-runtime](support/scale-runtime)：LoCoMo 评估器、来源身份、相关运行配置和辅助记录。
- [metadata/server-export](metadata/server-export)：服务器导出的 Python/Conda 依赖及环境信息。

历史目录中的失败、blocked、superseded 或较早结果保留原状，不作为正式论文结论。

## 核验与复现

在此目录中使用 Python 3.10 或更新版本执行：

```bash
python3 verify_backup.py
```

该命令只读核验所有来源文件的 SHA256，核对原生实验的原始输入与观察哈希，重新运行独立判定函数核对已保存的 3,300 条记录及 60 次重放记录，并检查论文中的关键计数。它不重新运行模型、数据库服务或第三方实验。

本次执行结果保存在 [backup_validation.json](validation/backup_validation.json)。各来源文件的原服务器路径、来源压缩包/提交、大小和哈希见 [file_manifest.json](metadata/file_manifest.json)。`SHA256SUMS` 额外覆盖备份目录中的文档、索引与验证工具自身（不包含校验文件自身）。

如需重新执行原生实验，从 [原生实验复现说明](experiments/native-comparison/EXTERNAL_COMPARISON_README.md)开始；输出到一个新目录，保留已有证据不变。其他实验也应在各自快照目录和相应依赖环境下执行。

## 已知边界

- 本次核验的是已有文件和结果，不是重新运行整套论文实验，也不代表所有历史测试套件通过。
- 性能 v10 包保存的是汇总测量：`cells.csv` 为 15 个配置、`operations.csv` 为配置与操作类型汇总；不含全部 14,400 次操作的原始计时样本。
- Qwen、AppWorld、LoCoMo 等部分报告注明 `raw_reports_committed=false`。没有把未找到的完整原始模型轨迹或预测文件宣称为已恢复。
- 300 个多智能体受控实例的最终逐例结果和生成器已保存；当前三个服务器压缩包中未找到对应 `multi_agent_controlled_v1/data` 原始输入目录。其他 `data/` 与 `final_controlled/data/` 属于各自历史实验，不能混用。
- 未上传虚拟环境、模型权重、Git 内部对象库、未选中的重复开发目录、控制器私有记录及与论文无关的 CQ 电力市场项目。完整原压缩包仍保留在用户本地。
- 论文 PDF 本身未上传，其 SHA256 已记录用于识别本次核对版本。第三方代码和数据仍适用其原有许可。
