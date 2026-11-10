# 投稿前六项证据闭环实施计划

> **执行说明：** 本计划按测试先行执行；每一项先看到针对缺口的测试失败，再写最小实现并复跑相关测试。远程实验只同步脱敏聚合。

**Goal:** 补齐六项投稿硬证据，使论文中的每个正式实验数字都能从 tracked artifact 自动复算，并通过 claim audit。

**Architecture:** 以 `results/final_controlled` 为受控实验唯一来源；为真实 vector/graph backend 增加 scenario-aware Toxiproxy 请求包装；用严格聚合器验证 τ-bench 50-task 与 5-task E2E 原始结果；复用 coverage shrinker 生成 mutant witnesses；最后用机器可读 claim ledger 串联 artifact、命令、manifest/hash 和 commit。

**Tech stack:** Python 3.11+、`unittest`、Qdrant HTTP、Neo4j Bolt、Toxiproxy HTTP API、τ-bench official runtime、Qwen2.5-7B/vLLM、JSON/CSV、DOCX renderer、Git。

---

## Task 1：固化设计与干净基线

**Files**

- Create: `docs/superpowers/specs/2026-08-11-pre-submission-evidence-closure-design.md`
- Create: `docs/superpowers/plans/2026-08-11-pre-submission-evidence-closure.md`

**Steps**

1. 记录六项范围、claim boundary 和逐项验收标准。
2. 检查仓库状态，保留所有既有未跟踪历史结果目录。
3. 运行全量测试，记录基线用例数、skip 数和失败数。
4. 提交设计与计划。

## Task 2：统一 controlled suite 为 400/2,000

**Files**

- Create: `tests/test_txnmem_claim_audit.py`
- Create: `src/txnmem_claim_audit.py`
- Create: `configs/paper_claims.json`
- Create: `results/paper_evidence/controlled_suite.json`
- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `build_txnmem_paper_draft.py`

**RED**

1. 测试从 `generated_instances.jsonl` 和 `experiment_results.csv` 复算实例、variant row、variant violations 和 oracle match。
2. 测试拒绝手工声明的 160/800 和与实际 CSV 不一致的值。
3. 运行：`python -m unittest tests.test_txnmem_claim_audit -v`，确认因模块/聚合缺失失败。

**GREEN**

1. 实现 deterministic controlled-suite aggregator。
2. 生成 `controlled_suite.json`，记录源路径、SHA-256、派生规则、400/2,000 及各变体计数。
3. 将 Markdown 与论文构建脚本中的 160/800、140/80/20/40 全部改为 400/2,000、350/200/50/100。
4. 复跑测试和 `rg` 检查，旧口径只允许出现在明确标为历史的文件中。

## Task 3：真实接入 Toxiproxy 并重跑故障矩阵

**Files**

- Modify: `tests/test_txnmem_service_faults.py`
- Modify: `tests/test_txnmem_backend_performance.py`
- Modify: `tests/test_txnmem_vector_graph_backend.py`
- Modify: `src/txnmem_service_faults.py`
- Modify: `src/txnmem_backend_performance.py`
- Modify: `src/txnmem_vector_graph_backend.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `scripts/run_real_backend_smoke.sh`
- Modify: `infra/real_backend/docker-compose.yml`
- Create: `results/real_backend_faults_formal/`

**RED**

1. 测试证明 backend factory 必须收到完整 scenario，而不是把它误当 `size`。
2. 测试 scenario wrapper 在指定 service/operation/ordinal 前安装 toxic，并记录 `trigger_fired`、`toxic_installed`、`proxy_path_verified` 和 clear 状态。
3. 测试非 normal 场景没有真实触发证据时 `evidence_valid=false`，正式矩阵整体失败。
4. 测试 `retry_success` 只在清理 toxic 后重试一次，且 canonical event 不重复。
5. 运行三个相关测试模块，确认新增断言先失败。

**GREEN**

1. 修正 backend factory 调用协议，显式传 `scenario=` 和 `size=`。
2. 扩展 Toxiproxy controller：ensure proxy、replace/clear toxic、事件日志、sanitized evidence。
3. 用 `proxy_requester` 将 controller 接入 `VectorGraphMemoryBackend._call`；操作映射统一为 Qdrant `write` 和 Neo4j `commit`。
4. 为 fault matrix 增加每 repetition 证据、有效性判定和 fail-closed aggregate。
5. 脚本创建两个真实代理，backend 使用代理 listen ports，保存服务版本、proxy config 和运行命令。
6. 在远程服务上执行 normal/delay/timeout/connection_drop/retry_success，验证实际异常/延迟、retry/abort 和 0 partial commit。
7. 同步仅含聚合与哈希的 `results/real_backend_faults_formal`。

## Task 4：补齐 τ-bench 50-task 和 5-task E2E 聚合

**Files**

- Create: `tests/test_txnmem_evidence_aggregates.py`
- Create: `src/txnmem_evidence_aggregates.py`
- Create: `scripts/aggregate_submission_evidence.py`
- Create: `results/submission_evidence/tau_bench_50/`
- Create: `results/submission_evidence/qwen_vector_graph_e2e_5/`

**RED**

1. τ 聚合器测试拒绝 49 tasks、重复 task、缺 evaluator 状态和未归并 retry。
2. E2E 聚合器测试拒绝少于 5 个唯一 task、模型/服务身份缺失、healthcheck 失败和非有限 latency。
3. 两类聚合都必须携带 manifest/source artifact SHA-256、run command 和 claim boundary。
4. 运行新测试，确认模块缺失或验收不满足。

**GREEN**

1. 实现两个 strict aggregators 和 CLI。
2. 先通过 SSH 只读检查远端 `/data/txnmem*` 是否已有对应原始结果；校验 task IDs、模型 revision、服务健康与源哈希。
3. 缺失时用固定 manifest 重跑：τ-bench 50 tasks；5 tasks 通过 Qwen2.5-7B + Qdrant + Neo4j。
4. 生成并同步脱敏正式聚合；严禁把原始 prompt/messages/events 内容写入 tracked results。
5. 对聚合执行 strict replay 并记录远端命令、UTC 时间、运行时版本和 Git source commit。

## Task 5：发布四类最小 mutant witnesses

**Files**

- Modify: `tests/test_txnmem_mutation.py`
- Modify: `tests/test_txnmem_coverage.py`
- Modify: `src/txnmem_mutation.py`
- Modify: `src/txnmem_experiment.py`
- Create: `results/final_controlled/results/minimal_mutant_witnesses.json`

**RED**

1. 测试要求四个稳定 mutant ID 均有 witness。
2. 测试要求每个 witness 的最小操作前缀仍触发目标失败，去掉最后一个操作后不再触发同一失败。
3. 测试要求保存 source hash、shrink trace、expected/observed 摘要且不含原始敏感文本。
4. 运行相关测试，确认当前空 `minimal_counterexamples` 不能通过。

**GREEN**

1. 建立 mutant ID 到执行 variant/故障谓词的稳定映射。
2. 从 400 instances 中确定性选择最短可杀死实例，调用 `find_minimal_counterexample` 收缩。
3. 写出四个 witness，并提供 replay 校验函数/CLI。
4. 将 mutation report 引用到新 witness 文件，保留原始 campaign 总计。

## Task 6：Claim ledger、supersession 与 fail-closed 审计

**Files**

- Modify: `tests/test_txnmem_claim_audit.py`
- Modify: `src/txnmem_claim_audit.py`
- Modify: `configs/paper_claims.json`
- Create: `results/paper_evidence/claim_audit.json`
- Create: `results/paper_evidence/supersession_index.json`
- Modify: `results/remaining_tasks/final_status.json` or add sidecar marker only
- Modify: `results/remaining_tasks/production_evidence_status.json` or add sidecar marker only

**RED**

1. 测试覆盖缺文件、JSON pointer 缺失、值不符、artifact/manifest hash 不符、指向 superseded artifact、远程 task 数不完整、Toxiproxy 未触发。
2. 测试要求 ledger 每条正式 claim 都有 run command、manifest/hash、artifact hash、source commit 和 boundary。

**GREEN**

1. 实现 ledger loader、JSON pointer/CSV 派生校验、SHA-256 和 supersession 检查。
2. 为 controlled、schedule/mutation、native repetition、AppWorld、LoCoMo、τ50、backend、E2E5、cross-host、realism 建立正式 claim 条目。
3. 生成 supersession index，不删除历史结果；当前文档只引用 active artifact。
4. 运行 claim audit 并写出 `finding_count=0` 的脱敏报告。

## Task 7：论文同步、DOCX 视觉 QA 与最终 Git 备份

**Files**

- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `build_txnmem_paper_draft.py`
- Regenerate: `outputs/TxnMem_论文初稿.docx`
- Regenerate: `outputs/TxnMem_论文初稿.pdf`
- Create/Modify: visual-QA evidence under `outputs/` or the existing QA result namespace

**Steps**

1. 只把通过 claim audit 的数字写入论文；若实际远程结果与旧文字不同，以新 artifact 为准。
2. 明确 Toxiproxy、τ reward、E2E latency、single-host backend 和 production claim boundary。
3. 运行全量 `unittest`、artifact audit、claim audit、strict aggregate replay。
4. 构建 DOCX，渲染全部页面为 PNG/PDF，逐页检查标题、表格、图、分页、页眉页脚和字体；运行 accessibility audit。
5. 检查 `git diff --check`、凭据扫描和结果脱敏。
6. 按逻辑提交代码、实验聚合和论文；记录最终 commit。没有 remote URL 时不执行 push，并在交付中明确说明。

## 最终验收命令

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
PYTHONPATH=src python src/txnmem_artifact_audit.py --root .
PYTHONPATH=src python src/txnmem_claim_audit.py --root . --ledger configs/paper_claims.json
git diff --check
git status --short --branch
```

文档验收使用 bundled document runtime 的 DOCX build、render、PDF 与 accessibility 命令；最终报告记录页数和逐页检查结论。
