# Real-Agent Task Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在真实 Qwen2.5-7B 工具循环中实现系统管理的 task-scoped durable memory transaction，并用独立 reference semantics、真实 `SIGKILL`、Qdrant/Neo4j 分阶段故障、配对重放和 live Agent 运行证明事务结果在恢复后是完整提交或完整不可见。

**Architecture:** `run_real_agent` 保留现有 `direct` 默认路径，并新增显式 `task` 路径。`TaskTransactionGateway` 将一个 Agent task 的多次 memory 操作写入 SQLite journal，提交时重验证 policy、read set 和 provenance，再把带 `txn_id` 的 pending 对象分阶段写入 Qdrant/Neo4j；journal 的唯一终态决议是 gateway 可见性的权威来源。独立 recovery process 根据 journal 对已提交事务幂等 finalize、对未决/已中止事务幂等 cleanup。实验层把真实 Qwen trace 冻结后在 DirectDispatch 与 TaskTransaction 上配对重放，并由不导入事务实现的 reference executor 复算结果。

**Tech Stack:** Python 3 标准库、SQLite WAL、现有 OpenAI-compatible Qwen client、Qdrant 1.11.5、Neo4j 5.22、Toxiproxy 2.5.0、`unittest`、JSON/JSONL、Docker Compose、Git、现有 claim/artifact/manuscript audit 与 DOCX 构建链。

## Global Constraints

- 开始实现前使用 `superpowers:using-git-worktrees` 创建隔离分支 `codex/real-agent-task-transactions`；不得直接在 `main` 上开发。
- 严格遵守 `docs/superpowers/specs/2026-08-17-real-agent-task-transactions-design.md`。若实现需要改变 claim boundary、case 数量、事务线性化点或 failure phase，先更新设计并重新取得用户确认。
- `run_real_agent(transaction_mode="direct")` 必须保持当前默认行为、事件格式和历史实验结果；新增事务功能只能通过显式参数启用。
- 模型看不到 `begin`、`commit` 或 `abort` 工具。系统在 task 开始时 begin，在无工具调用的最终回答前 commit，在模型/工具/步数失败时 abort。
- SQLite journal 是唯一持久化决议源。`COMMITTED` 与 `ABORTED` 是不可逆终态，同一 `txn_id` 不得出现冲突决议。
- commit 决议只能发生在 Qdrant 和 Neo4j 全部 pending 写入及 operation-after readback 完成之后。决议写入是 gateway 可见性的线性化点。
- supersession 与 invalidation 在 commit 决议前不得直接修改既有 committed 对象；先把 status overlay 持久化为 journal intent，commit 后由 gateway overlay 和 recovery/finalize 生效。
- `src/txnmem_reference.py` 与 oracle 转换代码不得导入 `txnmem_task_transaction`、`txnmem_transaction_journal`、`txnmem_transaction_recovery` 或 vector/graph staging 实现。
- 每个真实故障 case 必须由父进程观察 durable phase 后发送操作系统 `SIGKILL`，确认 worker 以 `-SIGKILL` 退出，再启动新的 recovery process。Python 异常不算进程故障证据。
- ambiguous backend response 必须通过 raw Qdrant/Neo4j readback 判定；无法回读时 fail-closed 为 `unknown`，不得按客户端异常推断 absent 或 committed。
- 每个 live episode 和 formal repetition 都进入固定分母。不得删除 `model_contract_failure`、timeout、tool failure、unknown 或失败重试后重算成功率。
- 不把 τ-bench/AppWorld 外部 API 副作用纳入 memory transaction；公开 runtime 只做兼容性 smoke。
- 所有正式 artifact 必须脱敏：不得保存密码、API key、SSH 命令、可路由 IP、用户名、完整主机名或原始业务秘密。endpoint 只保留 loopback/抽象 transport，host identity 只保留 SHA-256。
- Python 行为变更遵循 RED–GREEN–REFACTOR；每项任务先运行聚焦测试，再小步提交。不得把本计划中未跟踪的历史 `results/cross_host_model_load_*` 目录加入提交。
- 只有当 480 个 deterministic case、360 次 real-service observation、120 个 live Qwen episode、全量测试和全部审计都通过时，才能把新论文 claim 标记为 active。

---

## Task 1: Extend the canonical event contract and independent reference semantics

**Files:**

- Create: `tests/test_txnmem_event_contract.py`
- Modify: `tests/test_txnmem_trace.py`
- Modify: `tests/test_txnmem_reference.py`
- Modify: `src/txnmem_event_contract.py`
- Modify: `src/txnmem_trace.py`
- Modify: `src/txnmem_reference.py`

### Step 1: Write lifecycle event contract tests and observe RED

- [ ] 在 `tests/test_txnmem_event_contract.py` 添加 begin/commit/abort 正例，并断言生命周期事件必须有非空 `txn_id`：

```python
def test_transaction_lifecycle_events_require_txn_id(self):
    base = {
        "event_id": "e1",
        "kind": "begin_txn",
        "agent_id": "agent_model",
        "step": 1,
    }
    with self.assertRaisesRegex(EventContractError, "txn_id"):
        validate_event(base)
    self.assertEqual(
        validate_event({**base, "txn_id": "txn_task_1"})["txn_id"],
        "txn_task_1",
    )
```

- [ ] 添加回归断言：旧的 direct memory event 不带 `txn_id` 仍然合法；事务型 memory event 如果带 `txn_id`，值必须为非空字符串。
- [ ] 添加 `memory_invalidate` 逻辑操作事件正例。它与外部 failure event `invalidate` 分开：前者进入事务 write set，后者继续表示 source invalidation schedule。
- [ ] 添加严格递增 step 和唯一 event ID 的生命周期混合序列测试。
- [ ] 运行并确认因 `begin_txn`/`commit`/`abort` 尚未全部受支持而失败：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_event_contract -v
```

### Step 2: Write trace normalization tests and observe RED

- [ ] 在 `tests/test_txnmem_trace.py` 添加 begin → write → abort 序列，断言 `normalize_trace` 保留同一 `txn_id`，并把 `abort` 映射为 reference operation `abort`。
- [ ] 添加 `memory_invalidate` → reference operation `invalidate` 测试，同时断言外部 `invalidate` 仍转换为 failure schedule 而不是模型操作。
- [ ] 添加 begin → derive → commit 序列，断言 `source_ids`、`scope`、`task_id` 与 `txn_id` 不丢失。
- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_trace -v
```

- [ ] 确认新增 abort 映射断言失败，而现有 trace 测试仍通过。

### Step 3: Write explicit-abort reference tests and observe RED

- [ ] 在 `tests/test_txnmem_reference.py` 构造 begin → write → abort，断言写集不进入 snapshot，trace 的终态 decision 为 aborted，reason code 为传入的 `abort_reason` 或稳定默认值 `EXPLICIT_ABORT`。
- [ ] 构造 begin → write → abort → commit，断言后续 commit 不能复活事务。
- [ ] 构造 begin → transactional invalidate → abort 与 begin → transactional invalidate → commit：前者保持 root/descendants active，后者在 commit 时一起 invalid，并且决议前不改 committed snapshot。
- [ ] 构造 derive 后的 `after_operation` source-invalidation schedule，再 commit；oracle 必须整体 abort，不能留下 active derived object。构造 write 后的 `after_operation` policy revoke，再 commit；oracle 必须因 commit-time policy revalidation abort。
- [ ] 添加 independence guard：读取 `src/txnmem_reference.py` 文本并断言不出现四个被禁止的事务实现模块名。
- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_reference -v
```

### Step 4: Implement the minimal lifecycle support

- [ ] 在 `SUPPORTED_KINDS` 中加入 `begin_txn`、`commit`、`abort`、`memory_invalidate`，并增加以下验证规则：

```python
TRANSACTION_LIFECYCLE_KINDS = frozenset({"begin_txn", "commit", "abort"})

if kind in TRANSACTION_LIFECYCLE_KINDS:
    normalized["txn_id"] = _require_non_empty_string(
        normalized, "txn_id", "missing_txn_id"
    )
elif "txn_id" in normalized:
    normalized["txn_id"] = _require_non_empty_string(
        normalized, "txn_id", "invalid_txn_id"
    )
```

- [ ] 在 `KIND_TO_TYPE` 加入 `"abort": "abort"`；保留当前 `begin_txn` 与 `commit` 映射。
- [ ] 在 `KIND_TO_TYPE` 加入 `"memory_invalidate": "invalidate"`，并让 event contract 对该 kind 要求 `memory_id`；不改变外部 `invalidate` 的 failure-schedule 解释。
- [ ] 在 `txnmem_reference._run` 添加显式 abort 分支，只调用现有 `_abort_transaction`，且 terminal transaction 后的操作不能改变 snapshot：

```python
elif op_type == "abort":
    _abort_transaction(
        state,
        txn_id,
        str(operation.get("abort_reason") or "EXPLICIT_ABORT"),
    )
```

- [ ] 在 dispatch 前检查已存在 txn 的 `status in {"committed", "aborted"}`；除重复同终态 lifecycle event 外，后续 write/read/derive/propagate/supersede/invalidate/commit 一律只追加 `TERMINAL_TRANSACTION` denied trace，不再调用会修改 write set 的 helper。
- [ ] transaction state 增加 `read_versions` 与 `invalidations`。读取 committed source 时记录 `memory_id -> (version, scope, status)`；abort 清空 invalidations；commit 在写入任何对象前重验证 read/source version、scope 和 active status。
- [ ] initial memory 缺省 `version=1`；commit 更新既有对象、supersede 或 invalidate 时递增 version。事务内 invalidate 只把 root 及其当前 descendants 写入 `invalidations`，到 commit 才原子应用。
- [ ] 把 `_apply_policy_event` 扩为只作用于外部 schedule 的 `_apply_schedule_event`，并在 pre-events 与 post-events 两处调用。`revoke` 更新 policy；外部 `invalidate` 立即失效目标 committed source、递增 version 并执行 provenance repair；`crash` 仍由 crash 分支处理。
- [ ] 把 `abort_reason` 加入 trace normalization 的保留字段。

### Step 5: Verify compatibility and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_event_contract \
  tests.test_txnmem_trace \
  tests.test_txnmem_reference \
  tests.test_txnmem_real_model -v
```

- [ ] 确认 direct event 和现有 reference tests 无回归。
- [ ] 提交：

```bash
git add src/txnmem_event_contract.py src/txnmem_trace.py src/txnmem_reference.py \
  tests/test_txnmem_event_contract.py tests/test_txnmem_trace.py tests/test_txnmem_reference.py
git commit -m "feat: define transaction lifecycle event semantics"
```

---

## Task 2: Implement the durable SQLite transaction journal

**Files:**

- Create: `src/txnmem_transaction_journal.py`
- Create: `tests/test_txnmem_transaction_journal.py`

### Step 1: Specify the journal state machine in tests and observe RED

- [ ] 添加 `TransactionRecord`、begin、prepare、decide 的导入测试；初次运行应因模块不存在而失败。
- [ ] 使用 `tempfile.TemporaryDirectory()` 创建真实 SQLite 文件，覆盖以下转换：

```text
missing -> ACTIVE
ACTIVE -> PREPARED
ACTIVE -> ABORTED
PREPARED -> COMMITTED
PREPARED -> ABORTED
COMMITTED -> COMMITTED  (same-decision idempotence)
ABORTED -> ABORTED      (same-decision idempotence)
COMMITTED -> ABORTED    (reject)
ABORTED -> COMMITTED    (reject)
```

- [ ] 断言 `prepare` 不能把 terminal state 重新打开，`COMMITTED` 只能从 `PREPARED` 产生。
- [ ] 断言冲突决议抛出 `TransactionDecisionError`，稳定 code 为 `terminal_decision_conflict`。
- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_transaction_journal -v
```

### Step 2: Add idempotent intent, read-set, phase and receipt tests

- [ ] 用相同 `(txn_id, sequence, canonical payload)` 两次调用 `append_intent`，断言只保留一行。
- [ ] 用相同 sequence 和不同 payload 调用，断言 code 为 `intent_sequence_conflict`。
- [ ] 对同一 memory/version/scope 重复 `record_read`，断言幂等；不同 observed version 必须报 `read_set_conflict`。
- [ ] 重复写入相同 phase evidence 与 backend receipt，断言不增加记录数；相同 receipt key 的不同 evidence 必须报冲突。
- [ ] 关闭 journal 后重新打开，断言 state、intents、read set、phases 和 receipts 均持久存在。

### Step 3: Implement schema and public interfaces

- [ ] 实现以下稳定接口：

```python
@dataclass(frozen=True)
class TransactionRecord:
    txn_id: str
    task_id: str
    agent_id: str
    begin_policy_version: int
    state: str
    decision: str | None


class TransactionDecisionError(RuntimeError):
    def __init__(self, code: str, message: str): pass


class TransactionJournal:
    def __init__(self, path: str | Path): pass
    def begin(self, *, txn_id: str, task_id: str, agent_id: str,
              begin_policy_version: int) -> TransactionRecord: pass
    def append_intent(self, txn_id: str, *, sequence: int,
                      tool_name: str,
                      arguments: Mapping[str, Any]) -> dict[str, Any]: pass
    def record_read(self, txn_id: str, *, memory_id: str,
                    observed_version: int, scope: str) -> None: pass
    def record_phase(self, txn_id: str, phase: str,
                     evidence: Mapping[str, Any] | None = None) -> None: pass
    def record_backend_receipt(self, txn_id: str, *, backend: str,
                               operation_key: str, phase: str,
                               evidence: Mapping[str, Any]) -> None: pass
    def prepare(self, txn_id: str) -> TransactionRecord: pass
    def decide(self, txn_id: str, decision: str) -> TransactionRecord: pass
    def load(self, txn_id: str) -> TransactionRecord: pass
    def intents(self, txn_id: str) -> list[dict[str, Any]]: pass
    def read_set(self, txn_id: str) -> list[dict[str, Any]]: pass
    def phases(self, txn_id: str) -> list[dict[str, Any]]: pass
    def backend_receipts(self, txn_id: str) -> list[dict[str, Any]]: pass
    def recoverable_transaction_ids(self) -> list[str]: pass
    def state_digest(self, txn_id: str) -> str: pass
    def close(self) -> None: pass
```

- [ ] 建表 `transactions`、`intents`、`read_set`、`transaction_events`、`backend_receipts`；对状态、decision 和唯一键使用 SQLite `CHECK`/`PRIMARY KEY`/`UNIQUE` 约束，而不是只依赖 Python 条件。
- [ ] 每个连接执行：

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
```

- [ ] 所有 JSON 先用 `sort_keys=True, separators=(",", ":"), ensure_ascii=False` 规范化，再计算 SHA-256；digest 排除 wall-clock timestamp 和自增 row id。
- [ ] `recoverable_transaction_ids` 返回 ACTIVE/PREPARED，以及缺少 `finalize_complete` 的 COMMITTED、缺少 `cleanup_complete` 的 ABORTED，按 `txn_id` 排序。

### Step 4: Prove process durability and decision uniqueness

- [ ] 添加两个独立 `multiprocessing.Process` 竞争相反决议的测试；最终必须恰有一个 terminal decision，另一进程得到 conflict。
- [ ] 添加 abrupt worker exit 后新进程可读取 PREPARED 和 intents 的测试。
- [ ] 添加第二次相同 recovery metadata 不改变 `state_digest` 的测试。

### Step 5: Verify and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_transaction_journal -v
```

- [ ] 运行 `python3 -m compileall -q src/txnmem_transaction_journal.py`。
- [ ] 提交：

```bash
git add src/txnmem_transaction_journal.py tests/test_txnmem_transaction_journal.py
git commit -m "feat: add durable transaction decision journal"
```

---

## Task 3: Implement the task transaction coordinator and deterministic backends

**Files:**

- Create: `src/txnmem_task_transaction.py`
- Create: `tests/test_txnmem_task_transaction.py`
- Modify: `src/txnmem_backend.py`
- Create: `tests/test_txnmem_backend.py`

### Step 1: Define the backend protocol and gateway behavior in tests

- [ ] 在 `tests/test_txnmem_task_transaction.py` 定义最小 initial state，测试 task begin 自动生成首个 `begin_txn` canonical event。
- [ ] 测试连续 `memory_write`、`memory_derive`、`memory_propagate` 返回 `{"pending": True, "txn_id": "txn_task_1"}`，但另一个事务和普通 committed read 看不到 pending 对象。
- [ ] 测试本事务 read-your-writes：write 后 read/derive 能解析 pending source，read set 记录已提交 source 的 observed version，不把本事务新对象误记为外部 read dependency。
- [ ] 测试 `memory_supersede` 与 `memory_invalidate` 在 commit 前不改变 committed snapshot。
- [ ] 初次运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_task_transaction -v
```

- [ ] 确认因模块不存在而 RED。

### Step 2: Define commit revalidation failures

- [ ] 添加以下表驱动测试，每个失败都必须得到 ABORTED、零 gateway-visible 新对象和稳定 error code：

```text
policy version changed and write is now denied -> policy_revalidation_failed
read object version changed                  -> read_version_changed
source became invalid                        -> source_invalidated
source scope no longer matches               -> source_scope_changed
derived graph would contain a cycle           -> provenance_cycle
backend verify reports partial                -> backend_stage_incomplete
backend verify reports unknown                -> backend_state_unknown
```

- [ ] 添加 valid commit 测试，断言顺序为 `prepare_recorded` → `qdrant_staged` → `neo4j_staged` → `stage_verified` → `commit_decided` → `finalize_complete`。
- [ ] 添加 fault after decision 测试：`commit_decided` 后 hook 抛出的响应故障不得把事务改为 ABORTED；调用方得到 `commit_decided_response_lost`，recovery 后仍 complete。

### Step 3: Implement the protocol and transaction gateway

- [ ] 实现 backend contract：

```python
class TransactionBackend(Protocol):
    def read_committed(self, memory_id: str) -> Mapping[str, Any] | None: pass
    def search_committed(self, query: str | None = None) -> list[Mapping[str, Any]]: pass
    def current_version(self, memory_id: str) -> int | None: pass
    def stage_transaction(
        self,
        txn_id: str,
        intents: Sequence[Mapping[str, Any]],
        phase_hook: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> Mapping[str, Any]: pass
    def verify_transaction(self, txn_id: str,
                           intents: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: pass
    def finalize_transaction(self, txn_id: str,
                             intents: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: pass
    def cleanup_transaction(self, txn_id: str,
                            intents: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: pass
    def raw_transaction_state(self, txn_id: str,
                              intents: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: pass
```

- [ ] 实现 `TaskTransactionCoordinator` 与 `TaskTransactionGateway`。构造器明确接收 `journal`、`backend`、`task_id`、`agent_id`、`txn_id`、`policy_snapshot_provider` 和可选 `phase_hook`。
- [ ] policy snapshot 统一为以下 JSON-safe 结构，commit 对每个 intent 重新调用 provider 后检查：

```python
{
    "version": 3,
    "denied_actions": ["write"],
    "scope_overrides": {},
}
```

- [ ] canonical event 与 physical receipt 分离。`validated_events()` 只返回 begin/memory/commit/abort 逻辑事件；journal phase 和 backend receipt 不重复伪装为模型 memory event。
- [ ] 模型工具 `memory_invalidate` 的 canonical kind 固定为 `memory_invalidate`；failure controller 产生的外部来源失效事件固定为 `invalidate`，避免 oracle 把两者混淆。
- [ ] event ID 使用 `f"{txn_id}:event:{sequence:04d}"`，intent sequence 与 tool call 顺序一致；commit/abort 追加在最后且 step 严格递增。
- [ ] pending graph 由真实 intent 产生；derive/propagate 必须保留直接 `source_ids`，supersede 保留 old/new，invalidate 展开 descendants 后把每个 status overlay 写入 journal intent。

### Step 4: Add deterministic in-memory and SQLite staging adapters

- [ ] 在 `src/txnmem_backend.py` 为 committed records 增加向后兼容的整数 `version`：旧记录缺省为 1，write/supersede/invalidate 更新时递增；现有 snapshot 字段不删除。
- [ ] 在 `txnmem_task_transaction.py` 实现 `InMemoryTransactionBackend`，用于快速 unit/replay；pending 与 committed state 分开存放。
- [ ] 实现 `SQLiteStagingTransactionBackend`，使用单独 SQLite 文件持久化 vector-like objects、graph-like nodes/edges 和 status overlays，供真实 subprocess kill 测试使用。其 stage 顺序必须产生与 real backend 相同的 `after_qdrant_stage`、`after_neo4j_stage` hooks。
- [ ] 两个 adapter 的 `raw_transaction_state` 都返回统一字段：

```python
{
    "qdrant": {"read_ok": True, "objects": [{"memory_id": "memory_a"}]},
    "neo4j": {"read_ok": True, "nodes": [{"memory_id": "memory_a"}], "edges": []},
    "gateway_visible": ["memory_a"],
}
```

### Step 5: Verify coordinator invariants and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_backend \
  tests.test_txnmem_task_transaction -v
```

- [ ] 重复执行相同 txn commit/finalize/cleanup，确认 snapshot 和 digest 不变化。
- [ ] 提交：

```bash
git add src/txnmem_backend.py src/txnmem_task_transaction.py \
  tests/test_txnmem_backend.py tests/test_txnmem_task_transaction.py
git commit -m "feat: coordinate task-scoped memory transactions"
```

---

## Task 4: Integrate task transactions into the real Agent tool loop

**Files:**

- Modify: `src/txnmem_real_agent.py`
- Modify: `src/txnmem_failure_controller.py`
- Modify: `src/txnmem_real_experiment.py`
- Modify: `tests/test_txnmem_real_model.py`
- Create: `tests/test_txnmem_real_experiment.py`
- Create: `tests/test_txnmem_failure_controller.py`

### Step 1: Lock direct-mode compatibility with tests

- [ ] 扩展现有 scripted-model tests，显式调用 `transaction_mode="direct"`，断言结果与省略参数逐字段相同：status、failure code、events、steps 和 memory snapshot。
- [ ] 断言 direct mode 不创建 journal 文件、不返回 transaction summary，现有 failure schedule 仍在第一条 backend event 后触发。
- [ ] 运行现有 real-model tests 记录 GREEN 基线：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_real_model tests.test_txnmem_real_experiment -v
```

### Step 2: Write task-mode success and abort tests and observe RED

- [ ] 用 scripted model 产生 write → derive → final answer；以 `transaction_mode="task"` 运行并断言：

```python
self.assertEqual(report["status"], "completed")
self.assertEqual(report["transaction"]["decision"], "committed")
self.assertEqual([e["kind"] for e in report["events"]], [
    "begin_txn", "memory_write", "memory_derive", "commit"
])
```

- [ ] 分别注入 unknown tool、model protocol error、non-JSON tool result 和 max steps，断言 transaction 为 aborted，canonical events 以 `abort` 结束，pending state 不对新事务可见。
- [ ] 提交重验证失败时，保留模型最终文本供诊断，但 run status 必须为 failed，failure code 使用 coordinator 的稳定 code，不得返回 completed。
- [ ] 运行新增 tests 并确认因 `run_real_agent` 尚无事务参数而 RED。

### Step 3: Extend `run_real_agent` without changing the default

- [ ] 将函数签名扩展为：

```python
def run_real_agent(
    task: Mapping[str, Any],
    model: Any,
    backend: InstrumentedMemoryBackend,
    *,
    max_steps: int = 12,
    seed: int = 0,
    temperature: float = 0.0,
    transaction_mode: str = "direct",
    transaction_journal_path: str | Path | None = None,
    transaction_id: str | None = None,
    policy_snapshot_provider: Callable[[], Mapping[str, Any]] | None = None,
    transaction_phase_hook: Callable[[str, Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
```

- [ ] 只接受 `direct`/`task`。task mode 若缺少 journal path，抛出调用方配置错误 `ValueError`，不得静默退回 direct。
- [ ] task 开始创建 gateway；每个失败出口调用统一 `_abort_task_transaction(code)`，但若 journal 已 `COMMITTED`，不得反向 abort。
- [ ] 模型返回无 tool calls 时，先 commit；commit 成功才返回 completed。报告增加脱敏 transaction summary：`txn_id`、state、decision、phase names、intent count、read-set count、state digest。
- [ ] `events` 在 direct mode 继续来自 `backend.validated_events()`，在 task mode 来自 `gateway.validated_events()`。

### Step 4: Add semantic failure hooks

- [ ] 在 `FailureController` 增加 `observe_phase(phase, evidence, *, backend, gateway)`，复用 schedule validation，但触发器使用 `{"phase": "after_prepare"}` 等语义名。
- [ ] `run_real_agent` 在 direct/task 两种模式下每完成一条 mutating tool call 都调用 phase hook `after_mutation`，evidence 含从 1 开始的 `mutation_count`；这为 `kill_after_first_mutation` 提供共同触发边界。
- [ ] 支持且只支持以下 commit phase：

```text
after_prepare
after_qdrant_stage
after_neo4j_stage
after_stage_verify
after_commit_decision
after_finalize
after_mutation
```

- [ ] 普通 unit test 的 `crash` action 继续转换为 coded failure；formal process harness 不使用该异常，而是由父进程在 phase 持久化后发送 `SIGKILL`。

### Step 5: Thread transaction options through manifest execution

- [ ] 在 `run_experiment_manifest` 增加 opt-in transaction settings，并把每个 task 的 journal 路径固定为 `<out_dir>/journals/<case_id>.sqlite3`。
- [ ] `sanitize_run_report` 保留 transaction decision/digest/counts，删除 raw values、prompt secret 和 backend endpoint。
- [ ] 添加旧 manifest 不含 transaction 配置时输出不变的回归测试。

### Step 6: Verify and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_real_model \
  tests.test_txnmem_real_experiment \
  tests.test_txnmem_failure_controller -v
```

- [ ] 提交：

```bash
git add src/txnmem_real_agent.py src/txnmem_failure_controller.py \
  src/txnmem_real_experiment.py tests/test_txnmem_real_model.py \
  tests/test_txnmem_real_experiment.py tests/test_txnmem_failure_controller.py
git commit -m "feat: run real agents inside task transactions"
```

---

## Task 5: Add transactional staging and visibility to Qdrant/Neo4j

**Files:**

- Modify: `src/txnmem_vector_graph_backend.py`
- Modify: `tests/test_txnmem_vector_graph_backend.py`
- Modify: `scripts/run_e2e_real_backend.py`
- Modify: `tests/test_real_backend_script.py`

### Step 1: Extend fake clients for raw transactional readback

- [ ] 扩展 `_FakeQdrant` 和 `_FakeNeo4j`，保存 `txn_id`、`record_kind`、`target_status`、edge status，并支持按 txn 列举/删除。
- [ ] 为每个后端分别注入四类结果：operation-before failure、operation-after response loss、readback failure、cleanup failure。
- [ ] 保留现有 direct write/idempotency/compensation tests，不改其期望。

### Step 2: Write staging and visibility tests and observe RED

- [ ] `stage_transaction` 后 raw Qdrant/Neo4j 都存在 pending object/edge，但普通 `read`/`search` 不可见。
- [ ] journal decision resolver 返回 committed 后，即使 physical status 仍为 pending，gateway-visible read 按 `target_status` 解释为完整 committed state。
- [ ] resolver 返回 aborted/None 时，所有该 txn pending rows 不可见。
- [ ] 另一事务不能读取 pending；所属事务的 read-your-writes 由 coordinator buffer 提供，不依赖后端 search 泄露。
- [ ] committed status overlay 在 finalize 前就从 gateway 隐藏旧 superseded/invalid memory；raw 旧对象在决议前保持 active。

### Step 3: Write operation-after verification tests

- [ ] 对 write、derive、propagate、supersede 各自断言 Qdrant payload、Neo4j node、直接 provenance edge 和 supersession edge 均完整。
- [ ] Qdrant 成功但 Neo4j absent/partial 时 `verify_transaction` 返回 `partial`；任一 readback 失败返回 `unknown`。
- [ ] operation-after response loss 但 raw 两端完整时返回 complete；不得因为客户端抛异常而 cleanup 一个已经完整 staged 的事务。
- [ ] cleanup 后 raw state absent；cleanup response loss 后 readback absent 仍视为 clean；readback 失败视为 unknown。

### Step 4: Implement low-level client methods

- [ ] 给 `_QdrantHTTPClient` 增加 `retrieve_many_by_txn` 和 `delete_many_by_txn`，使用 namespace 与 `txn_id` filter，返回明确的 `read_ok`/rows，而不是把 transport error 转成空列表。
- [ ] 给 `_Neo4jBoltClient` 增加按 txn 的 node/edge raw readback 与 idempotent cleanup。`DERIVED_FROM`/`SUPERSEDES` relationship 写入 `txn_id` 和 pending status。
- [ ] 所有 staged idempotency key 使用 `sha256(namespace, txn_id, sequence, operation)`，不得用随机 UUID。

### Step 5: Implement `TransactionBackend` on `VectorGraphMemoryBackend`

- [ ] 增加可选 decision resolver，但默认 `None` 时 direct records 维持现有可见性；带未知 `txn_id` 的 pending records fail-closed 不可见。
- [ ] 实现 `stage_transaction`：先 Qdrant 后 Neo4j，每阶段成功后写 receipt 并调用 phase hook。status overlay 只作为 durable journal intent；决议前不更新旧对象。
- [ ] 实现 `verify_transaction`：按 intent 计算 expected object/node/edge set，对两端逐项回读并只返回 `complete`、`partial`、`absent`、`unknown` 四类。
- [ ] 实现 `finalize_transaction`：把 committed pending objects 的 physical status 更新为 target status，并应用 supersession/invalidation overlay；每步可重复。
- [ ] 实现 `cleanup_transaction`：删除 txn pending object/node/edge，不触碰其他 txn 或历史 committed data。
- [ ] 实现 `raw_transaction_state`，保留 ID/hash/status/source/supersedes 和 read flags，但不返回 memory value。

### Step 6: Verify direct compatibility and real-backend smoke seam

- [ ] 在 `scripts/run_e2e_real_backend.py` 增加 `--transaction-mode direct|task`、`--journal-path`；默认 direct。
- [ ] `tests/test_real_backend_script.py` 断言 task mode 必须使用 journal，且脚本没有把 data ports 绕过 Toxiproxy。
- [ ] 运行：

```bash
PYTHONPATH=src:scripts python3 -m unittest \
  tests.test_txnmem_vector_graph_backend \
  tests.test_real_backend_script -v
```

- [ ] 提交：

```bash
git add src/txnmem_vector_graph_backend.py tests/test_txnmem_vector_graph_backend.py \
  scripts/run_e2e_real_backend.py tests/test_real_backend_script.py
git commit -m "feat: stage task transactions across vector and graph stores"
```

---

## Task 6: Implement independent recovery and a real SIGKILL harness

**Files:**

- Create: `src/txnmem_transaction_recovery.py`
- Create: `src/txnmem_transaction_worker.py`
- Create: `tests/test_txnmem_transaction_recovery.py`
- Create: `tests/test_txnmem_transaction_process.py`

### Step 1: Write recovery state tests and observe RED

- [ ] 对 ACTIVE/PREPARED/COMMITTED/ABORTED 四种 journal state 建 fixture，断言：

```text
ACTIVE    -> decide ABORTED -> cleanup -> verify invisible
PREPARED  -> decide ABORTED -> cleanup -> verify invisible
COMMITTED -> finalize -> verify complete
ABORTED   -> cleanup -> verify invisible
```

- [ ] 对 cleanup/finalize 的 operation-after response loss 使用 raw readback 确认结果；readback 失败时返回 `unknown`。
- [ ] 连续运行 recovery 两次，断言第二次 normalized state digest、decision、visible set 与 raw classification 均不变。
- [ ] 运行并确认模块不存在导致 RED：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_transaction_recovery -v
```

### Step 2: Implement recovery APIs

- [ ] 实现：

```python
def recover_transaction(
    *,
    journal: TransactionJournal,
    backend: TransactionBackend,
    txn_id: str,
) -> dict[str, Any]: pass


def recover_pending_transactions(
    *,
    journal: TransactionJournal,
    backend: TransactionBackend,
) -> list[dict[str, Any]]: pass
```

- [ ] recovery 只能依据 journal state/decision/intents 与 raw readback，不读取 runner summary。
- [ ] 每个结果包含 `decision_before`、`decision_after`、`action`、`raw_state`、`gateway_visible_ids`、`classification_hint`、`digest_before`、`digest_after`。
- [ ] committed finalize 不完整时不得改成 aborted；分类提示为 committed incomplete/unknown，保留原唯一决议。

### Step 3: Build a subprocess worker with a durable phase handshake

- [ ] `txnmem_transaction_worker` 接收以下 CLI：

```text
--case PATH
--journal PATH
--backend-config PATH
--result PATH
--pause-at-phase PHASE
--phase-ready PATH
--mode execute|recover
```

- [ ] execute mode 的 phase hook 先确认 journal 已 durable 记录 phase，再以原子 rename 写 `phase-ready` JSON，随后阻塞等待父进程 kill；不自行捕获或伪造 `SIGKILL`。
- [ ] recover mode 在全新进程打开相同 journal/backend，运行 recovery 并写脱敏 result JSON。
- [ ] worker 所有错误写到独立 stderr/exit code；没有 terminal decision 时不得写假 completed result。

### Step 4: Prove actual SIGKILL in automated tests

- [ ] 父进程以 `subprocess.Popen` 启动 execute worker，轮询 ready file（固定 10 秒 deadline），验证 phase 与 journal 一致后执行 `os.kill(pid, signal.SIGKILL)`。
- [ ] `wait()` 后断言 return code 等于 `-signal.SIGKILL`，再用第二个 `subprocess.run` 启动 recover worker。
- [ ] 对以下四个 commit phase 分别测试，不能参数化后只执行一个 phase：

```text
after_prepare
after_qdrant_stage
after_neo4j_stage
after_commit_decision
```

- [ ] 前三者必须 aborted + invisible；最后一个必须 committed + complete。每个 phase 再运行第二次 recovery 验证 digest 稳定。
- [ ] 另加 `after_mutation`、`mutation_count=1` 的双条件进程测试：DirectDispatch 被 kill 后允许观察到首条已持久化 mutation，TaskTransaction recovery 必须 aborted + invisible；两者都必须由父进程发送真实 `SIGKILL`。
- [ ] test 使用 `SQLiteStagingTransactionBackend`，所以不依赖 Docker 也能真实跨进程验证。

### Step 5: Verify and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_transaction_recovery \
  tests.test_txnmem_transaction_process -v
```

- [ ] 通过 `ps`/测试清理断言确认没有遗留 worker。
- [ ] 提交：

```bash
git add src/txnmem_transaction_recovery.py src/txnmem_transaction_worker.py \
  tests/test_txnmem_transaction_recovery.py tests/test_txnmem_transaction_process.py
git commit -m "feat: recover task transactions after process kill"
```

---

## Task 7: Add independent classification, oracle comparison and paired statistics

**Files:**

- Create: `src/txnmem_transaction_experiment.py`
- Create: `tests/test_txnmem_transaction_experiment.py`
- Modify: `tests/test_txnmem_reference.py`

### Step 1: Define mutually exclusive outcome classification tests

- [ ] 为九个结果各建一个最小 fixture，并断言恰好一个 classification：

```text
committed_complete
aborted_clean
aborted_invisible_orphan
partial_visible
committed_incomplete
policy_violation
provenance_violation
model_contract_failure
unknown
```

- [ ] 明确优先级：missing/readback failure → unknown；model contract failure → model_contract_failure；policy/provenance violation；terminal-state consistency；最后才区分 clean/orphan。
- [ ] unknown 不能计入 oracle match 或成功；orphan 可以保持 atomic visibility 正确，但单独进入 cleanup failure 指标。
- [ ] 运行并确认 RED：

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_transaction_experiment -v
```

### Step 2: Define the independent oracle boundary

- [ ] 实现 `transaction_case_to_reference_instance(trace, schedule, initial_state)`，输出只包含 reference executor 已定义的 operations/policies/failure schedule。
- [ ] 调用 `reference_outcome` 生成允许结果，不调用 coordinator/journal/recovery。
- [ ] 候选 snapshot 只从 gateway-visible readback 重建；raw pending orphan 不进入可见 snapshot。
- [ ] 增加 import guard：AST 检查 `txnmem_transaction_experiment.py` 的 oracle conversion helper 只能导入 `txnmem_reference`/`txnmem_trace` 数据接口，reference 模块本身仍不导入 transaction 实现。

### Step 3: Implement classifier and case record schema

- [ ] 实现稳定接口：

```python
def classify_transaction_case(
    *, case: Mapping[str, Any], journal: Mapping[str, Any],
    raw_state: Mapping[str, Any], visible_state: Mapping[str, Any],
    oracle: Mapping[str, Any], model_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]: pass

def run_paired_replay(
    *, traces: Sequence[Mapping[str, Any]],
    tasks_by_id: Mapping[str, Mapping[str, Any]],
    scenarios: Sequence[Mapping[str, Any]],
    out_path: str | Path,
) -> list[dict[str, Any]]: pass

def run_recovery_case(
    *, case: Mapping[str, Any], journal_path: str | Path,
    backend_config_path: str | Path, out_dir: str | Path,
) -> dict[str, Any]: pass

def aggregate_transaction_results(
    *, paired_rows: Sequence[Mapping[str, Any]],
    recovery_rows: Sequence[Mapping[str, Any]],
    live_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]: pass

def exact_mcnemar(direct_violations: Sequence[bool],
                   task_violations: Sequence[bool]) -> dict[str, Any]: pass
```

- [ ] 每个 case row 包含 stable `case_id`、trace hash、condition、scenario、family、seed/repetition、process exit evidence、journal decision/digest、raw/gateway readback digest、classification、oracle match 和 artifact schema version。
- [ ] 不保存 raw prompt/value；对 tool arguments 只保存 schema-safe IDs、operation kind 和 SHA-256。

### Step 4: Verify statistical calculations

- [ ] 对已知 discordant pair 计算 two-sided exact McNemar：`p = min(1, 2 * P[Binomial(b+c, 0.5) <= min(b,c)])`。
- [ ] 报告 pair count、`b`、`c`、paired risk difference、固定 seed 的 10,000 次 paired bootstrap 95% percentile CI；不把 deterministic、service repetition 和 live episode 混在同一分母。
- [ ] 对 0 violations 报告明确的 `0/N` 和 exact binomial upper bound，不写成一般性零风险。
- [ ] 聚合按 scenario 和 workload family 分层，并检查每层期望分母；任一缺行/重复 `case_id` 直接失败。

### Step 5: Verify and commit

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_transaction_experiment \
  tests.test_txnmem_reference -v
```

- [ ] 提交：

```bash
git add src/txnmem_transaction_experiment.py \
  tests/test_txnmem_transaction_experiment.py tests/test_txnmem_reference.py
git commit -m "feat: classify transaction outcomes with independent oracle"
```

---

## Task 8: Freeze the 40-task workload and failure manifests

**Files:**

- Create: `configs/real_agent_transaction_tasks.json`
- Create: `configs/real_agent_transaction_schedules.json`
- Create: `scripts/build_real_agent_transaction_manifests.py`
- Create: `tests/test_real_agent_transaction_manifests.py`

### Step 1: Write manifest contract tests and observe RED

- [ ] 断言 task manifest 恰好 40 个唯一 task，四类各 10 个：

```text
atomic_multi_write
policy_revoke_before_commit
provenance_chain_branch
supersession_mixed
```

- [ ] 每个 task 固定 `task_id`、family、agent_id、initial_memories、prompt template ID、required tool kinds、`minimum_mutations >= 2` 和 public-safe values。
- [ ] 断言 schedule manifest 有四个 paired scenario 和四个 task-only recovery phase，名称与设计逐字一致。
- [ ] 断言所有 source-invalidation schedules 只引用 provenance family 中真实 initial/source IDs；policy schedule 在最后一个 mutation 后、commit 前触发。
- [ ] 运行并确认文件缺失导致 RED：

```bash
PYTHONPATH=src python3 -m unittest tests.test_real_agent_transaction_manifests -v
```

### Step 2: Implement a deterministic manifest builder

- [ ] builder 不调用模型、不联网；由四个固定模板和 index 1–10 生成任务。
- [ ] 每个任务使用 namespace-safe ID，例如 `txn_atomic_01_source`，避免 repetitions 之间碰撞。
- [ ] schedule trigger 使用语义条件，不使用 wall-clock：

```json
{"scenario":"kill_after_first_mutation","trigger":{"mutation_count":1},"action":"parent_sigkill"}
```

- [ ] recovery phases 精确映射为 `after_prepare`、`after_qdrant_stage`、`after_neo4j_stage`、`after_commit_decision`。
- [ ] JSON 使用 UTF-8、2-space indent、排序稳定；schema version 和 generator source hash 写入 manifest metadata。

### Step 3: Generate, validate and freeze hashes

- [ ] 运行 builder 两次并确认输出 SHA-256 相同：

```bash
PYTHONPATH=src python3 scripts/build_real_agent_transaction_manifests.py \
  --tasks configs/real_agent_transaction_tasks.json \
  --schedules configs/real_agent_transaction_schedules.json
```

- [ ] test 复算 family counts、scenario names、task IDs 和 source references，不在 test 中只比较一个整体 hash。
- [ ] 运行 privacy scan，确保无姓名、凭据、主机和真实业务值。

### Step 4: Commit frozen manifests

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest tests.test_real_agent_transaction_manifests -v
```

- [ ] 提交：

```bash
git add configs/real_agent_transaction_tasks.json \
  configs/real_agent_transaction_schedules.json \
  scripts/build_real_agent_transaction_manifests.py \
  tests/test_real_agent_transaction_manifests.py
git commit -m "data: freeze real-agent transaction workloads"
```

---

## Task 9: Add CLI commands, trace capture and the 480-case deterministic batch

**Files:**

- Modify: `src/txnmem_experiment.py`
- Modify: `src/txnmem_transaction_experiment.py`
- Create: `tests/test_txnmem_transaction_cli.py`
- Create after run: `results/real_agent_transactions/deterministic/captured_trace_manifest.json`
- Create after run: `results/real_agent_transactions/deterministic/paired_results.jsonl`
- Create after run: `results/real_agent_transactions/deterministic/recovery_results.jsonl`
- Create after run: `results/real_agent_transactions/deterministic/aggregate.json`

### Step 1: Define CLI parser tests and observe RED

- [ ] 添加四个 subcommand 的 parser/dispatch tests：

```text
transaction-trace-capture
transaction-paired-replay
transaction-recovery-batch
transaction-aggregate
```

- [ ] 每个命令必须要求 task/schedule manifest、out-dir 和 source attestation；capture 还要求 endpoint/model 或显式 `--offline-fixture`。
- [ ] formal replay 不允许 `--drop-failures`、自动补跑替换失败行或未固定 seed。

### Step 2: Implement trace capture

- [ ] `transaction-trace-capture` 对 40 个 task 各运行一次 Qwen tool loop，保存 sanitized canonical events、model contract evaluation、model revision/server build、endpoint token usage、task/source hash 和原始 attempt index。
- [ ] model 未产生至少两次 mutation 时保留该 task row，标记 `model_contract_failure`；不循环调用直到成功。
- [ ] capture 使用无故障 `transaction_mode=task`，只冻结 begin/逻辑 memory operations/commit 或 abort；captured trace manifest 恰好 40 rows，按 task ID 排序，whole-file SHA-256 写入 sidecar attestation。
- [ ] offline fixture 只用于 unit/smoke，不得作为 formal captured trace artifact。

### Step 3: Implement paired and recovery deterministic batches

- [ ] paired replay 对 40 traces × 4 scenarios × 2 conditions 产生恰好 320 rows；同一 `(trace, scenario)` 共享 initial state、schedule 和 seed。oracle converter 无论 replay condition 为何都在逻辑 operations 外包裹系统 task begin/terminal decision，避免把 DirectDispatch 的物理事件格式当作 ground truth。
- [ ] deterministic `kill_after_first_mutation` 在完成第一条 operation 后截断执行并复算结果；真实 `SIGKILL` 证据由 Task 6 subprocess test 与 Task 10 real-service repetitions 提供，aggregate 不把 deterministic 截断伪称为 process kill。
- [ ] recovery batch 对 40 traces × 4 kill phases 产生恰好 160 rows，并使用 TaskTransaction condition。
- [ ] 每个 batch 在写 aggregate 前检查 case ID 唯一、期望矩阵完整、oracle evidence 存在、unknown 不被过滤。
- [ ] 写 JSONL 使用临时文件 + atomic rename；已有 out-dir 非空时要求显式 `--resume`，resume 只补缺失 case，不覆盖已有 row。

### Step 4: Add count and fail-closed integration tests

- [ ] 用 2-task miniature manifest 跑全路径，断言 16 paired rows、8 recovery rows，重复执行 aggregate hash 稳定。
- [ ] 删除一行、复制一行、移除 raw readback 或篡改 trace hash，aggregate 必须失败。
- [ ] 模型 contract failure 仍在所有适用 case row 中，aggregate 总分母不减少。

### Step 5: Run the formal deterministic batch

- [ ] 在可用 Qwen2.5-7B endpoint 上运行一次正式 capture，并记录精确 model revision/server build；若 endpoint 不可用，本任务保持未完成，不得用 fixture 替代。
- [ ] 对冻结 trace 运行：

```bash
PYTHONPATH=src python3 src/txnmem_experiment.py transaction-paired-replay \
  --tasks configs/real_agent_transaction_tasks.json \
  --schedules configs/real_agent_transaction_schedules.json \
  --traces results/real_agent_transactions/deterministic/captured_trace_manifest.json \
  --out-dir results/real_agent_transactions/deterministic

PYTHONPATH=src python3 src/txnmem_experiment.py transaction-recovery-batch \
  --tasks configs/real_agent_transaction_tasks.json \
  --schedules configs/real_agent_transaction_schedules.json \
  --traces results/real_agent_transactions/deterministic/captured_trace_manifest.json \
  --out-dir results/real_agent_transactions/deterministic

PYTHONPATH=src python3 src/txnmem_experiment.py transaction-aggregate \
  --paired results/real_agent_transactions/deterministic/paired_results.jsonl \
  --recovery results/real_agent_transactions/deterministic/recovery_results.jsonl \
  --out results/real_agent_transactions/deterministic/aggregate.json
```

- [ ] 独立读取 aggregate，确认 `paired_case_count=320`、`recovery_case_count=160`、`deterministic_case_count=480`，并检查每个 classification 总和等于分母。

### Step 6: Verify and commit implementation plus deterministic evidence

- [ ] 运行：

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_transaction_cli \
  tests.test_txnmem_transaction_experiment -v
```

- [ ] 对 formal artifact 运行 privacy scan 和 source-hash validation。
- [ ] 提交 implementation；只有正式 capture/batch 完整时才在同一或后续 data commit 中加入四个 formal artifacts：

```bash
git add src/txnmem_experiment.py src/txnmem_transaction_experiment.py \
  tests/test_txnmem_transaction_cli.py
git commit -m "feat: run deterministic task-transaction batches"

git add results/real_agent_transactions/deterministic
git commit -m "data: add deterministic task-transaction evidence"
```

---

## Task 10: Run 360 real-service observations and 120 live Qwen episodes

**Files:**

- Create: `scripts/run_real_agent_transaction_experiment.sh`
- Create: `configs/submission_evidence/real_agent_task_transactions.json`
- Modify: `infra/real_backend/docker-compose.yml` only if a deterministic health/readback seam is missing
- Modify: `tests/test_real_backend_script.py`
- Create after run: `results/real_agent_transactions/real_service/paired_results.jsonl`
- Create after run: `results/real_agent_transactions/real_service/recovery_results.jsonl`
- Create after run: `results/real_agent_transactions/live_qwen/results.jsonl`
- Create after run: `results/real_agent_transactions/environment_attestation.json`
- Create after run: `results/real_agent_transactions/aggregate.json`

### Step 1: Define the formal runner contract

- [ ] shell runner 使用 `set -euo pipefail`，接受 `TXNMEM_ENDPOINT`、`TXNMEM_MODEL`、`TXNMEM_MODEL_REVISION`、`TXNMEM_MODEL_SERVER_BUILD`、`TXNMEM_OUT_DIR`，但不得把 API key 或 endpoint host 写入 artifact。
- [ ] 启动 Qdrant/Neo4j/Toxiproxy 后逐服务 healthcheck，创建代理，并验证 client 只通过代理 data ports 访问后端。
- [ ] 每个 scenario 先运行 1 次 smoke；smoke 不进入 formal 分母。任一 smoke 缺 journal/raw/gateway/oracle evidence 时停止 formal batch。
- [ ] trap 只清理由 runner 创建的容器/worker；不能删除历史 result 目录。

### Step 2: Add runner and attestation tests

- [ ] 测试脚本包含固定 task/schedule manifest、30 repetitions、唯一 namespace、phase handshake、exit-code capture、second recovery 和 privacy scan。
- [ ] 测试脚本不直接发布 Qdrant/Neo4j data port给 experiment client，不包含密码、真实 IP、用户名或 `sshpass`。
- [ ] attestation schema 要求 Python/Docker/Compose/kernel、service tags+digests、model revision/server build、source commit、manifest hashes、sanitized command、host hash 和 exit code 0。

### Step 3: Run the 360-observation real-service matrix

- [ ] paired service batch：4 scenarios × 30 repetitions × 2 conditions = 240 observations。
- [ ] recovery service batch：4 kill phases × 30 repetitions = 120 observations。
- [ ] 每次 repetition 使用唯一 namespace/memory IDs，四个 workload family 按稳定 round-robin 轮换；不重用失败 repetition 的 ID。
- [ ] 父进程保存 worker PID、ready phase、`SIGKILL` exit code 和 recovery process exit code；正式 artifact 只保留 PID 是否匹配/退出码，不保留长期可识别主机信息。
- [ ] `kill_after_first_mutation` 的 60 个 paired observations（30 repetitions × 2 conditions）都在 durable backend mutation/intent 后由父进程发送真实 `SIGKILL`；DirectDispatch recovery 只审计已有状态，TaskTransaction recovery 依据 journal abort/cleanup。
- [ ] aggregate 检查 `real_service_observation_count=360`，并分别报告 240/120 分母、raw state、gateway visibility、orphan 与 unknown。

### Step 4: Run the 120 live Qwen episodes

- [ ] 从每类选择 task index 1–5，共 20 个 task；DirectDispatch/TaskTransaction 两条件；固定 seeds `11, 29, 47`：20 × 2 × 3 = 120。
- [ ] 两条件使用同一 model revision、prompt、tool schema、temperature、max tokens、max steps 和初始 state。
- [ ] 保留 endpoint-reported prompt/completion/total token、request count、tool call count、latency、task status、contract status 和 transaction classification。
- [ ] 缺 token usage 时标记 `token_usage_missing=true`，不得按字符数伪造 token cost。
- [ ] aggregate 同时报告全 120 episode 与满足多事件 contract 的条件分母。

### Step 5: Add public-runtime compatibility smoke

- [ ] 在已安装的 τ-bench 与 AppWorld runtime 各选择一个固定 task，通过 merged benchmark+memory gateway 启用 `transaction_mode=task`。
- [ ] 仅断言 runtime task 可启动、memory transaction 有唯一 terminal decision、benchmark tool 仍可调用；报告中明确外部 API side effects 不受 rollback 保护。
- [ ] 这些 smoke rows 存入补充 compatibility artifact，不计入 480/360/120 主结果。

### Step 6: Aggregate and verify formal evidence

- [ ] 聚合器从 raw rows 重新计算所有分母和分类；不得信任 runner 写入的 top-level totals。
- [ ] 检查成功标准：TaskTransaction 的 `partial_visible`、`committed_incomplete`、`policy_violation`、`provenance_violation`、`unknown` 是否均为 0；若任一非零，保留结果并把 claim 标为 failed/observed，不修改数据迎合标准。
- [ ] 运行真实服务 focused test 和全量 transaction tests：

```bash
PYTHONPATH=src:scripts python3 -m unittest \
  tests.test_real_backend_script \
  tests.test_txnmem_vector_graph_backend \
  tests.test_txnmem_transaction_process \
  tests.test_txnmem_transaction_experiment -v
```

- [ ] 将脚本/config 与数据分开提交：

```bash
git add scripts/run_real_agent_transaction_experiment.sh \
  configs/submission_evidence/real_agent_task_transactions.json \
  infra/real_backend/docker-compose.yml tests/test_real_backend_script.py
git commit -m "feat: automate real-service transaction experiment"

git add results/real_agent_transactions
git commit -m "data: add real-agent transaction evidence"
```

---

## Task 11: Promote verified evidence into the claim ledger and paper

**Files:**

- Modify: `configs/paper_claims.json`
- Modify: `configs/txnmem_ccfa_paper.json`
- Modify: `results/paper_evidence/supersession_index.json` only if an older artifact makes the same claim
- Regenerate: `results/paper_evidence/claim_audit.json`
- Regenerate: `results/paper_evidence/manuscript_audit.json`
- Modify: `docs/paper/txnmem_ccfa_draft_zh.md`
- Modify: `docs/paper/evidence_map_zh.md`
- Create: `docs/real_agent_transaction_experiment_zh.md`
- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify if required: `scripts/build_txnmem_ccfa_docx.py`
- Modify if required: `tests/test_txnmem_ccfa_docx.py`
- Regenerate: `outputs/TxnMem_CCF-A中文论文初稿.docx`
- Modify: `docs/paper/txnmem_ccfa_docx_qa_zh.md`

### Step 1: Add fail-closed claim-audit tests

- [ ] 新 claim 必须同时绑定 deterministic aggregate、real-service aggregate、live aggregate、environment attestation、task/schedule/trace hashes 和 source commit。
- [ ] claim audit 在任一以下条件下失败：480/360/120 分母不足、unknown 被删除、source hash 不匹配、formal exit code 非 0、claim boundary 缺失、classification totals 不守恒、论文数字不等于 artifact。
- [ ] 新 claim 默认先用 `status: provisional`；只有 Task 10 成功标准和所有审计通过后才改为 `active`。

### Step 2: Add the exact bounded claim

- [ ] active claim 的中文边界必须包含：

```text
在固定 Qwen2.5-7B、多事件任务清单、TxnMem gateway 和被测故障调度下，task-scoped durable memory transactions 在真实进程终止及独立恢复后得到 oracle-confirmed complete-or-invisible 状态；该结论不覆盖任意外部工具副作用、绕过 gateway 的查询、Qdrant/Neo4j 原生统一事务或一般分布式事务保证。
```

- [ ] ledger 分开记录 deterministic 480、real-service 360、live Qwen 120，不合并为一个成功率。
- [ ] 0 violations 必须写成 `0/N observed` 并附 exact upper bound，不写“永不发生”。

### Step 3: Update manuscript and evidence map

- [ ] 系统章节增加 task boundary、journal state machine、pending visibility、commit linearization 和 recovery 算法；明确模型不调用 begin/commit。
- [ ] 实验章节增加 workload family、40-task trace 来源、4 paired scenarios、4 kill phases、三个独立分母和统计方法。
- [ ] 结果章节只引用 active artifact 实际值；如果成功标准未满足，诚实报告 violation/unknown 并缩窄结论。
- [ ] 威胁章节明确 gateway bypass、外部 API rollback、单协调者 journal、固定模型/任务/faults 和 real-service repetition 范围。
- [ ] evidence map 为每个正文数字记录 JSON Pointer、统计单位、manifest/hash 和 claim boundary。
- [ ] `docs/real_agent_transaction_experiment_zh.md` 独立记录系统实现、40-task 数据来源、480/360/120 三组实验、每组目的、故障触发、oracle、实际分类/统计、token usage、复现命令、artifact hash 和不支持的保证，供作者与审稿证据核对。

### Step 4: Run claim, artifact and manuscript audits

- [ ] 运行：

```bash
PYTHONPATH=src python3 src/txnmem_claim_audit.py audit \
  --root . --ledger configs/paper_claims.json \
  --out results/paper_evidence/claim_audit.json

PYTHONPATH=src python3 src/txnmem_artifact_audit.py --root .

PYTHONPATH=src python3 src/txnmem_manuscript_audit.py \
  --root . --config configs/txnmem_ccfa_paper.json \
  --source docs/paper/txnmem_ccfa_draft_zh.md \
  --out results/paper_evidence/manuscript_audit.json
```

- [ ] 三项 findings 均为 0 后才把 claim status 改为 active，并再次运行三项审计。

### Step 5: Rebuild and visually verify DOCX

- [ ] 执行时先读取并使用 `documents:documents` skill 的完整 render/verify/privacy instructions。
- [ ] 使用 bundled workspace Python 重建 DOCX，运行 `tests.test_txnmem_ccfa_docx`、ZIP integrity、heading/section/image/table/style/privacy/accessibility audits。
- [ ] 用 `scripts/render_docx_with_bundled_libs.sh` 输出全页 PNG/PDF，逐页检查中文字体、表格溢出、图注、公式、参考文献、页眉页脚、空白页和裁切。
- [ ] 在 `txnmem_ccfa_docx_qa_zh.md` 记录最终 SHA-256、页数、图表/参考文献数量、审计命令、0 findings 和逐页检查结论。

### Step 6: Commit paper promotion

- [ ] 提交 claim/audits/manuscript，再单独提交最终 DOCX/QA；不要加入 render 临时 PNG：

```bash
git add configs/paper_claims.json configs/txnmem_ccfa_paper.json \
  results/paper_evidence docs/paper/txnmem_ccfa_draft_zh.md \
  docs/real_agent_transaction_experiment_zh.md \
  docs/paper/evidence_map_zh.md docs/current_experiment_report_zh.md \
  docs/formal_paper_task_status_zh.md
git commit -m "docs: add real-agent transaction evidence to paper"

git add scripts/build_txnmem_ccfa_docx.py tests/test_txnmem_ccfa_docx.py \
  outputs/TxnMem_CCF-A中文论文初稿.docx docs/paper/txnmem_ccfa_docx_qa_zh.md
git commit -m "docs: rebuild paper with task-transaction results"
```

---

## Task 12: Run final verification and integrate the feature branch

**Files:**

- Verify: all tracked source, config, evidence, paper and DOCX files from Tasks 1–11
- Modify only if verification exposes a real defect; each fix requires a focused regression test and separate commit

### Step 1: Run the full automated suite

- [ ] 运行：

```bash
PYTHONPATH=src:scripts python3 -m unittest discover -s tests -q
git diff --check
```

- [ ] 记录 test count、skip count、exit code；environment-dependent skips 必须说明原因，transaction core/process/oracle tests 不得 skip。

### Step 2: Recompute formal artifacts from raw rows

- [ ] 在新临时目录重新聚合 deterministic、real-service 和 live artifacts，逐文件比较 canonical SHA-256；差异必须可解释为明确排除的 timestamp，否则失败。
- [ ] 断言矩阵：

```text
deterministic paired rows       320
deterministic recovery rows     160
deterministic total             480
real-service paired rows        240
real-service recovery rows      120
real-service total              360
live Qwen episodes              120
```

- [ ] 复算每组 classification、oracle match、recovery convergence、orphan、token usage completeness 与 McNemar contingency table。

### Step 3: Run security, privacy and clean-archive verification

- [ ] 对 tracked files 扫描密码样式、API key、SSH user@host、可路由 IPv4/IPv6、绝对 home path 和原始 prompt/value。
- [ ] 用 `git archive HEAD` 解压到临时目录，从 archive 运行 unit tests、claim audit、artifact audit、manuscript audit 和 aggregate verification，证明没有依赖 untracked local files。
- [ ] 确认 `git status --short` 只显示用户原有的未跟踪历史 results；不得把它们删除、移动或提交。

### Step 4: Review claim boundary and evidence completeness

- [ ] 对照设计 spec 逐项检查完成定义 1–6，并在最终 handoff 报告中列出 artifact 路径、hash、分母、实际结果和仍然不支持的保证。
- [ ] 若任一 formal run、readback、audit 或 visual QA 未完成，把论文 claim 保持 provisional，并明确列为剩余任务；不得宣称所有实验完成。

### Step 5: Record final verification and commit

- [ ] 在 `docs/formal_paper_task_status_zh.md` 写入 final source commit、test/skip count、480/360/120 分母、三个 aggregate SHA-256、claim/artifact/manuscript audit digest、DOCX SHA-256 与当前 claim status；所有值从已验证 artifact 读取。
- [ ] 提交最终验证记录：

```bash
git add docs/formal_paper_task_status_zh.md results/paper_evidence \
  docs/paper/txnmem_ccfa_docx_qa_zh.md
git commit -m "chore: record final task-transaction verification"
```

### Step 6: Integrate only after verification

- [ ] 使用 `superpowers:requesting-code-review` 做实现复核；处理反馈时使用 `superpowers:receiving-code-review` 并以测试证据判断。
- [ ] 使用 `superpowers:verification-before-completion` 重跑关键命令，再使用 `superpowers:finishing-a-development-branch` 将 `codex/real-agent-task-transactions` 合并到本地 `main`。
- [ ] 合并后在 `main` 再运行 full suite 和 audits。没有 Git remote URL 时只报告“本地已合并”，不得声称已推送；只有用户提供/确认 remote 后才 push。
