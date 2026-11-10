# AppWorld Fair Prompt Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a paired AppWorld baseline/tuned experiment in which both arms expose the same instruction-inferred public tools and differ only in prompt and trusted-preflight treatment.

**Architecture:** Add one explicit tool-strategy resolver and pass the strategy—not a profile-dependent result—into the existing `AppWorldAdapter`. The adapter resolves apps only after reset has obtained the official instruction. Keep model-visible public tools separate from fixed internal trusted-preflight calls, place the shared tool strategy in the condition fingerprint, and place prompt/preflight only in treatment metadata. Run a one-task remote smoke before two sequential 20-task official batches.

**Tech Stack:** Python 3.11, `argparse`, AppWorld public function-calling schemas, `unittest`, Qwen2.5-7B-Instruct through vLLM's OpenAI-compatible endpoint, SQLite memory backend.

## Global Constraints

- Formal baseline and tuned runs use `appworld_tool_strategy=instruction_inferred`.
- Both arms use the same task manifest, ordered model-visible schema names, model revision, vLLM build, generation parameters, memory backend, AppWorld runtime, and official evaluator.
- Supervisor profile/password APIs are trusted-preflight-only: never model-visible and never executable through the model tool gateway.
- `prompt_profile` and `trusted_preflight_enabled` are treatment labels and are excluded from the shared condition fingerprint.
- Every terminal path performs save, official evaluation, and close.
- Only sanitized summaries may be synchronized or committed; raw conversations, argument values, passwords, SQLite state, and AppWorld state directories remain excluded.

---

### Task 1: Resolve a Shared AppWorld Tool Strategy

**Files:**
- Modify: `src/txnmem_benchmark_bridge.py:94-113`
- Modify: `tests/test_txnmem_real_model.py:238-263`

**Interfaces:**
- Consumes: `infer_appworld_app_names(instruction: str, supplied_app_names: Sequence[str] | None) -> list[str]`.
- Produces: `resolve_appworld_app_names(strategy: str, instruction: str, supplied_app_names: Sequence[str] | None) -> list[str] | None` and `APPWORLD_TOOL_STRATEGIES`.

- [ ] **Step 1: Write failing resolver tests**

```python
def test_instruction_inferred_tool_strategy_is_prompt_profile_independent(self):
    expected = ["phone", "venmo", "supervisor"]
    self.assertEqual(
        resolve_appworld_app_names(
            "instruction_inferred",
            "Request money on Venmo from my friend Stacy.",
            ["supervisor"],
        ),
        expected,
    )

def test_manifest_scoped_tool_strategy_preserves_manifest_apps(self):
    self.assertEqual(
        resolve_appworld_app_names("manifest_scoped", "Use Venmo.", ["supervisor"]),
        ["supervisor"],
    )
```

- [ ] **Step 2: Run the tests and confirm the missing import/failing behavior**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_real_model.TxnMemRealAgentTests.test_instruction_inferred_tool_strategy_is_prompt_profile_independent tests.test_txnmem_real_model.TxnMemRealAgentTests.test_manifest_scoped_tool_strategy_preserves_manifest_apps`

Expected: FAIL because `resolve_appworld_app_names` is not defined.

- [ ] **Step 3: Implement the strategy resolver**

```python
APPWORLD_TOOL_STRATEGIES = ("instruction_inferred", "manifest_scoped", "all_public")

def resolve_appworld_app_names(strategy, instruction, supplied_app_names=None):
    if strategy == "instruction_inferred":
        return infer_appworld_app_names(instruction, supplied_app_names)
    if strategy == "manifest_scoped":
        return list(supplied_app_names) if supplied_app_names is not None else None
    if strategy == "all_public":
        return None
    raise ValueError(f"unsupported AppWorld tool strategy: {strategy}")
```

- [ ] **Step 4: Run focused tests**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_real_model`

Expected: all tests pass.

- [ ] **Step 5: Commit the resolver**

```bash
git add src/txnmem_benchmark_bridge.py tests/test_txnmem_real_model.py
git commit -m "feat: resolve shared AppWorld tool strategy"
```

### Task 2: Expose the Strategy in Both Native CLI Paths

**Files:**
- Modify: `src/txnmem_benchmark_bridge.py:430-510`
- Modify: `src/txnmem_benchmark_bridge.py:799-825`
- Modify: `src/txnmem_experiment.py:332-391`
- Modify: `src/txnmem_experiment.py:813-875`
- Modify: `src/txnmem_experiment.py:929-980`
- Modify: `tests/test_cli_outputs.py`
- Modify: `tests/test_txnmem_real_model.py`

**Interfaces:**
- Consumes: `APPWORLD_TOOL_STRATEGIES` and `resolve_appworld_app_names(...)` from Task 1.
- Produces: `--appworld-tool-strategy {instruction_inferred,manifest_scoped,all_public}` on `benchmark-native-smoke` and `benchmark-native-batch`; `AppWorldAdapter(tool_strategy=..., supplied_app_names=...)` resolves apps after reset without reading `prompt_profile`.

- [ ] **Step 1: Add failing parser and adapter-factory tests**

```python
def test_appworld_batch_accepts_instruction_inferred_tool_strategy(self):
    parser = _build_parser()
    args = parser.parse_args([
        "benchmark-native-batch", "--benchmark", "appworld",
        "--manifest", "manifest.json", "--appworld-tool-strategy",
        "instruction_inferred",
    ])
    self.assertEqual(args.appworld_tool_strategy, "instruction_inferred")

def test_appworld_strategy_does_not_depend_on_prompt_profile(self):
    baseline = resolve_appworld_app_names("instruction_inferred", "Use Venmo.", ["supervisor"])
    tuned = resolve_appworld_app_names("instruction_inferred", "Use Venmo.", ["supervisor"])
    self.assertEqual(baseline, tuned)

def test_appworld_baseline_and_tuned_reset_before_schema_loading(self):
    for profile in ("baseline", "tuned"):
        order = []

        class _OrderingAdapter(_NoopBenchmarkAdapter):
            dataset = "appworld"

            def reset(self, task):
                order.append("reset")
                return str(task["instruction"])

            def tool_schemas(self):
                order.append("schemas")
                return []

        run_benchmark_agent(
            {"task_id": profile, "instruction": "Use Venmo.", "prompt_profile": profile},
            _ScriptedModel([ModelResponse("done", [])]),
            InstrumentedMemoryBackend(),
            _OrderingAdapter(),
        )
        self.assertEqual(order[:2], ["reset", "schemas"])
```

- [ ] **Step 2: Run tests and confirm parser failure**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_real_model`

Expected: FAIL because the parser does not expose `--appworld-tool-strategy` and baseline currently loads schemas before reset.

- [ ] **Step 3: Add the parser option and use the resolver in both factories**

```python
parser.add_argument(
    "--appworld-tool-strategy",
    choices=APPWORLD_TOOL_STRATEGIES,
    default="manifest_scoped",
)

return AppWorldAdapter(
    appworld_root=args.appworld_root,
    tool_strategy=args.appworld_tool_strategy,
    supplied_app_names=task_app_names or default_app_names,
)
```

In `AppWorldAdapter.reset`, retain the official instruction. In `tool_schemas`, call `resolve_appworld_app_names(self.tool_strategy, self._instruction, self.supplied_app_names)`. In `run_benchmark_agent`, use reset → schemas for AppWorld in both profiles. Public Supervisor tools are subject to the same model allowlist in both arms; the fixed trusted preflight names bypass model schemas only through `call_trusted_preflight`.

- [ ] **Step 4: Run focused tests**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_real_model tests.test_benchmark_bridge`

Expected: all tests pass; AppWorld runtime-security skips remain skips when the package is absent locally.

- [ ] **Step 5: Commit the CLI integration**

```bash
git add src/txnmem_benchmark_bridge.py src/txnmem_experiment.py tests/test_cli_outputs.py tests/test_txnmem_real_model.py
git commit -m "feat: expose AppWorld tool strategy"
```

### Task 3: Make the Condition Fingerprint Enforce Tool Equality

**Files:**
- Modify: `src/txnmem_experiment.py:78-149`
- Modify: `src/txnmem_experiment.py:1019-1041`
- Modify: `tests/test_cli_outputs.py`
- Modify: `tests/test_txnmem_prompt_comparison.py`

**Interfaces:**
- Consumes: `_paired_benchmark_condition(...)` and `canonical_fingerprint(...)`.
- Produces: condition field `appworld_model_tool_strategy`; treatment fields `prompt_profile` and `trusted_preflight_enabled`.

- [ ] **Step 1: Write a failing fingerprint test**

```python
def test_appworld_prompt_profiles_share_condition_fingerprint(self):
    arguments = {
        "benchmark": "appworld",
        "manifest_sha256": "a" * 64,
        "model_id": "qwen2.5-7b-instruct",
        "model_execution_mode": "remote_endpoint",
        "memory_backend": "sqlite",
        "repetitions": 1,
        "max_tokens": 1024,
        "timeout_seconds": 300.0,
        "model_revision": "b" * 64,
        "model_server_build": "vllm:0.8.5.post1",
        "appworld_tool_strategy": "instruction_inferred",
    }
    baseline = _paired_benchmark_condition(**arguments)
    tuned = _paired_benchmark_condition(**arguments)
    self.assertEqual(canonical_fingerprint(baseline), canonical_fingerprint(tuned))
    self.assertNotIn("prompt_profile", baseline)
    self.assertNotIn("trusted_preflight_enabled", baseline)
```

- [ ] **Step 2: Run the test and confirm the missing condition field**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_prompt_comparison`

Expected: FAIL because the helper does not accept or record the strategy.

- [ ] **Step 3: Extend condition and treatment metadata**

```python
condition["appworld_model_tool_strategy"] = (
    appworld_tool_strategy if benchmark == "appworld" else "not_applicable"
)
report["treatment"] = {
    "prompt_profile": args.prompt_profile,
    "trusted_preflight_enabled": (
        args.benchmark == "appworld" and args.prompt_profile == "tuned"
    ),
}
```

- [ ] **Step 4: Run focused comparison tests**

Run: `PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_prompt_comparison`

Expected: all tests pass and mismatched strategies produce mismatched fingerprints.

- [ ] **Step 5: Commit condition enforcement**

```bash
git add src/txnmem_experiment.py tests/test_cli_outputs.py tests/test_txnmem_prompt_comparison.py
git commit -m "test: enforce AppWorld paired conditions"
```

### Task 4: Verify Model-Visible and Trusted Tool Separation

**Files:**
- Modify: `src/txnmem_benchmark_bridge.py:799-865`
- Modify: `tests/test_txnmem_real_model.py:288-380`
- Modify: `tests/test_benchmark_bridge.py`

**Interfaces:**
- Consumes: `AppWorldAdapter.set_model_authorized_tools(...)`, `execute_trusted_preflight(...)`, and `BenchmarkToolGateway.call_trusted_preflight(...)`.
- Produces: task report fields `model_visible_benchmark_tool_names_sha256`, `model_visible_benchmark_tool_count`, `trusted_preflight_enabled`, and `unauthorized_tool_attempt_count`, without retaining raw schema bodies or preflight values.

- [ ] **Step 1: Add failing separation and equality tests**

```python
class _PreflightAdapter(_NoopBenchmarkAdapter):
    dataset = "appworld"

    def tool_schemas(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": name,
                    "parameters": {"type": "object", "properties": {}},
                },
            }
            for name in (
                "venmo__get_profile",
                "supervisor__show_profile",
                "supervisor__show_account_passwords",
            )
        ]

    def execute_trusted_preflight(self, name, arguments):
        if name == "supervisor__show_profile":
            return '{"first_name":"Alex"}', {"ok": True}
        return '{"venmo":"redacted"}', {"ok": True}

def test_supervisor_preflight_tools_are_never_model_visible(self):
    adapter = _PreflightAdapter()
    model = _ScriptedModel([ModelResponse("done", [])])
    report = run_benchmark_agent(
        {"task_id": "tuned", "instruction": "Use Venmo.", "prompt_profile": "tuned"},
        model,
        InstrumentedMemoryBackend(),
        adapter,
    )
    names = {tool["function"]["name"] for tool in model.calls[0]["tools"]}
    self.assertNotIn("supervisor__show_profile", names)
    self.assertNotIn("supervisor__show_account_passwords", names)
    self.assertEqual(report["trusted_preflight_enabled"], True)

def test_tool_set_digest_is_stable_across_prompt_profiles(self):
    reports = {}
    for profile in ("baseline", "tuned"):
        reports[profile] = run_benchmark_agent(
            {"task_id": profile, "instruction": "Use Venmo.", "prompt_profile": profile},
            _ScriptedModel([ModelResponse("done", [])]),
            InstrumentedMemoryBackend(),
            _PreflightAdapter(),
        )
    self.assertEqual(
        reports["baseline"]["model_visible_benchmark_tool_names_sha256"],
        reports["tuned"]["model_visible_benchmark_tool_names_sha256"],
    )
```

- [ ] **Step 2: Run the tests and confirm missing report fields**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_real_model tests.test_benchmark_bridge`

Expected: FAIL on the missing digest/treatment fields.

- [ ] **Step 3: Record a names-only digest and treatment flag**

```python
visible_names = sorted(
    str(schema["function"]["name"]) for schema in benchmark_schemas
)
report["model_visible_benchmark_tool_names_sha256"] = hashlib.sha256(
    json.dumps(visible_names, separators=(",", ":")).encode("utf-8")
).hexdigest()
report["model_visible_benchmark_tool_count"] = len(visible_names)
report["trusted_preflight_enabled"] = (
    prompt_profile == "tuned" and adapter.dataset == "appworld"
)
```

- [ ] **Step 4: Run security and lifecycle tests**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_real_model tests.test_benchmark_bridge`

Expected: all tests pass, including unauthorized private-tool rejection and finalizer coverage.

- [ ] **Step 5: Commit audit metadata**

```bash
git add src/txnmem_benchmark_bridge.py tests/test_txnmem_real_model.py tests/test_benchmark_bridge.py
git commit -m "feat: attest AppWorld model-visible tools"
```

### Task 5: Run Remote Smoke and Paired Official Batches

**Files:**
- Read: `results/native_scale/manifests/appworld_task_scoped_v3.json`
- Create remotely: `/data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_smoke`
- Create remotely: `/data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_baseline`
- Create remotely: `/data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_tuned`
- Create locally: `results/prompt_profile_formal_v4/appworld_baseline/native_batch_summary.json`
- Create locally: `results/prompt_profile_formal_v4/appworld_tuned/native_batch_summary.json`
- Create locally: `results/prompt_profile_formal_v4/appworld_prompt_comparison.json`

**Interfaces:**
- Consumes: the CLI and condition contract from Tasks 1-4.
- Produces: two complete 20-task official summaries and one paired comparison.

- [ ] **Step 1: Run the full local unit suite before synchronization**

Run: `PYTHONPYCACHEPREFIX=/private/tmp/txnmem_pycache PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py'`

Expected: zero failures; AppWorld integration-only tests may skip when the runtime is absent locally.

- [ ] **Step 2: Synchronize only the changed source files to the remote overlay**

Run from the repository root:

```bash
rsync -az -e 'ssh -p 32222' \
  src/txnmem_benchmark_bridge.py \
  src/txnmem_experiment.py \
  src/txnmem_real_experiment.py \
  src/txnmem_conditions.py \
  'root+vm-6l7y1ZA0clrezfHs@223.72.199.245:/data/txnmem_overlay_20260807/src/'
```

Then compare `sha256sum` output for those four local and remote files. Do not synchronize local `results/**/data`.

Expected: remote SHA-256 values match local source files.

- [ ] **Step 3: Run one tuned smoke task**

Create the one-task manifest without changing task content:

```bash
jq '.tasks = [.tasks[0]]' \
  /data/txnmem_run_20260806/results/native_scale/manifests/appworld_task_scoped_v3.json \
  > /data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_smoke_manifest.json
```

Then run:

```bash
PYTHONPATH=/data/txnmem_overlay_20260807/src \
/data/venvs/appworld/bin/python /data/txnmem_overlay_20260807/src/txnmem_experiment.py \
  benchmark-native-batch \
  --benchmark appworld \
  --manifest /data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_smoke_manifest.json \
  --appworld-root /data/txnmem_run_20260806/external_data/deps/appworld-data \
  --appworld-tool-strategy instruction_inferred \
  --out-dir /data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_smoke \
  --memory-backend sqlite \
  --repetitions 1 \
  --endpoint http://127.0.0.1:8000/v1 \
  --model qwen2.5-7b-instruct \
  --model-revision 7b44fc9c62f21ea990dc4290f251f127c53a06eb1b9f170f75a20f1cd29f26b4 \
  --model-server-build vllm:0.8.5.post1 \
  --timeout 300 \
  --max-tokens 1024 \
  --prompt-profile tuned
```

Expected: summary status is terminal, official evaluator is available, Supervisor preflight names are absent from model-visible tools, no password value exists in the sanitized summary, and the adapter is closed.

- [ ] **Step 4: Run baseline 20-task batch**

Run the Step 3 command with the full manifest `/data/txnmem_run_20260806/results/native_scale/manifests/appworld_task_scoped_v3.json`, output `/data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_baseline`, and `--prompt-profile baseline`; retain every other argument exactly.

Expected: 20 task attempts, 20 official evaluator outcomes, and no configuration-level abort.

- [ ] **Step 5: Run tuned 20-task batch**

Run the Step 4 full-manifest command with output `/data/txnmem_run_20260806/results/prompt_profile_formal_v4/appworld_tuned` and `--prompt-profile tuned`; retain every other argument exactly.

Expected: 20 task attempts, 20 official evaluator outcomes, and no configuration-level abort.

- [ ] **Step 6: Pull summaries only and compare**

Run: `PYTHONPATH=src python3 src/txnmem_prompt_comparison.py --kind appworld --baseline results/prompt_profile_formal_v4/appworld_baseline/native_batch_summary.json --tuned results/prompt_profile_formal_v4/appworld_tuned/native_batch_summary.json --out results/prompt_profile_formal_v4/appworld_prompt_comparison.json`

Expected: matching nonempty condition fingerprints, identical task IDs and evaluator denominators, paired official success delta, error categories, and exact-or-lower-bound token comparison.

- [ ] **Step 7: Commit sanitized formal evidence**

```bash
git add results/prompt_profile_formal_v4 docs/current_experiment_report_zh.md docs/formal_paper_task_status_zh.md
git commit -m "exp: add paired AppWorld prompt comparison"
```

### Task 6: Final Artifact and Regression Gate

**Files:**
- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Read: `.gitignore`
- Read: `src/txnmem_artifact_audit.py`

**Interfaces:**
- Consumes: Task 5 comparison and summaries.
- Produces: paper-facing claims tied to exact denominators and an auditable clean result set.

- [ ] **Step 1: Update the reports with exact outcomes**

Record official success counts as `successes/20`, paired delta, failure taxonomy, total requests/tokens with completeness status, model revision, strategy name, condition fingerprint, and the distinction between prompt/preflight benefit and TxnMem mechanism benefit.

- [ ] **Step 2: Run the artifact audit**

Run: `PYTHONPATH=src python3 src/txnmem_artifact_audit.py --root .`

Expected: zero findings.

- [ ] **Step 3: Run final regression and formatting gates**

Run: `PYTHONPYCACHEPREFIX=/private/tmp/txnmem_pycache PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py'`

Run: `git diff --check`

Expected: zero test failures and no whitespace errors.

- [ ] **Step 4: Verify Git state and commit final report changes**

```bash
git status --short
git add docs/current_experiment_report_zh.md docs/formal_paper_task_status_zh.md
git commit -m "docs: report fair AppWorld evaluation"
```

Expected: only explicitly deferred or externally blocked files remain uncommitted.
