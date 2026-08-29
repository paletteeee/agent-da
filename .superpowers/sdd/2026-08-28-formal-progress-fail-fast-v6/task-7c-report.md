# Task 7C implementation report

Date: 2026-08-29

Branch/base: `codex/provenance-progress-v6` from `8c05f12`

Implementation commit: `28f4f91` (`feat: add integrated protected Linux lifecycle gate`)

Status: implemented and locally verified; the protected-root Linux zero-skip
gate remains outstanding because this local host does not provide the required
Linux/root/kernel/filesystem primitives.

## Scope and implementation

Exactly one test selector was added:

`tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue`

A diff scan found one added `def test_` and no second Task 7C selector. The
selector does not call another test method. It performs cross-platform contract
checks first and then explicitly skips when the protected Linux primitives are
unavailable.

The protected path is one process/resource lineage:

1. The independently installed controller verifies its protected installation,
   approval manifest, repository HEAD, committed controller bytes, and installed
   progress-reader bytes.
2. That controller creates its inode-bound committed export and imports the
   collector only from the exported `src` directory.
3. The collector validates the private exact-enum fault and controller context,
   re-attests the approved commit, and creates a private temporary workspace.
   Fixed private labels are used only below that temporary root; no registered
   formal run identity, authorization nonce, or formal matrix registration is
   created or reused.
4. The collector creates and publishes the real immutable committed-source
   export, verifies the immutable runner hash, and forks one collector worker.
5. The worker starts the runner with `_start_gated_candidate`. The production
   pre-exec path clears supplementary groups, drops GID and UID to the dedicated
   identity, and sets and queries `PDEATHSIG=SIGKILL` after the credential drop.
6. The runner starts under `-I -S -B`, binds imports to the immutable exported
   `src` directory, repeats the post-exec/post-drop kernel and credential checks,
   and forks one distinct descendant.
7. The descendant re-establishes and queries its own fork-cleared
   `PDEATHSIG=SIGKILL`, verifies its exact credentials/groups, ignores SIGTERM,
   and enters a `PyDLL` sleep that holds the GIL.
8. After the real runner/descendant readiness boundary, the worker activates
   and verifies the production nftables guard and releases the runner gate.
9. The runner executes a tiny diagnostic matrix through the real performance
   functions and calls `publish_provenance_bundle`. The invocation-scoped
   private publication enum disables named compatibility fallback, forcing the
   real fd-bound anonymous-inode publication path. The real precommit callback
   proves that neither the final pointer nor a named temporary pointer is
   visible before the exclusive link; the worker then loads the real pointer.
10. The worker separately proves that the bundle object's internal
    `COMPLETED.json` exists, the runner-to-collector completion receipt FD is
    byte-empty, and the external collector completion file is absent.
11. The worker sends SIGTERM to the exact two-process inventory through
    revalidated pidfds and proves both the blocked runner and TERM-ignoring/GIL
    descendant remain alive. It then injects start-identity drift into the same
    real pidfd signaling routine, proves rejection before any signal, proves the
    live inventory is unchanged, and proves every temporary pidfd closed.
12. The controller-side collector process opens and revalidates a pidfd for the
    collector worker and kills that actual parent. Linux then kills the runner
    through its parent-death signal and kills the descendant through the
    runner's parent death. As a subreaper, the controller-side collector reaps
    all three and requires exact SIGKILL status plus an empty dedicated-UID
    inventory while the nft guard is still active.
13. With the real pointer still intact and both completion-receipt forms still
    absent, the path calls the real `_seal_candidate_tree`. It rejects with the
    exact missing-receipt reason before mutating candidate bytes. The path then
    calls the real `promote_provenance_candidate`; its real seal validator
    rejects the absent candidate seal with the exact reason and creates no
    promotion output.
14. Cleanup removes the exact inode-bound private lifecycle tree, restores the
    subreaper state, and removes the nft guard last. The installed controller
    removes the exact committed-export inode. The test waits for that controller
    process to exit and independently compares dedicated-UID processes, owned
    pidfds, nft tables, and bootstrap exports with their pre-run inventories.

The returned sanitized receipt contains only its fixed schema/selector,
booleans, and zero counts. Child stdout and stderr are captured in temporary
files; failure reporting exposes only the selector and aggregate status.

## Internal-only support

Three closed one-member enums exist:

- collector and runner: `POINTER_WITHOUT_RECEIPT = "pointer_without_receipt"`;
- publisher: `INTEGRATED_POINTER_WITHOUT_RECEIPT = "integrated_pointer_without_receipt"`.

Each validator uses exact type identity, so a bool, raw string, string subclass,
or unrelated enum is rejected. All hook parameters default to `None`. The
collector and runner hooks are private Python functions and are absent from
their public `main` paths. The controller public dispatch map still rejects
`integrated-lifecycle`. A diff scan found no added argparse option, enabling
environment variable, authorization-nonce route, config field, or manifest
field. The runner consumes only the already reserved gate, ready, and completion
FD environment variables created by `_start_gated_candidate`; none selects the
fault.

Ordinary diagnostic publication remains unchanged: with the private mode at its
default `None`, diagnostic publication still passes `_allow_named_fallback=True`.
Only the exact private enum on this one invocation makes it false. Formal
publication remains false as before.

The real candidate seal now validates the full runner material receipt before
changing ownership or modes. The real promotion validator checks for a matching
candidate seal before loading or aggregating candidate evidence, making the
missing-receipt/missing-seal branch fail fast without creating output.

## Strict TDD evidence

No production file was edited before the exact selector and its pre-skip
contract were added.

RED command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue -v
```

Sanitized RED outcome: status 1; one test run, one failure, zero errors, zero
skips. The failure was the expected missing private installed-controller entry:
`integrated lifecycle controller entry is unavailable`.

GREEN command: the same exact command after the minimum internal support.

Sanitized GREEN outcome on this host: status 0; one test run, zero failures,
one explicit skip for unavailable protected Linux lifecycle primitives. This is
a contract-check GREEN and is not counted as a protected-lifecycle pass.

An initial shell wrapper around RED used the reserved zsh variable name
`status`; it was immediately rerun with `test_rc`. Only the successful second
wrapper is the recorded RED observation.

## Changed files and justification

- `src/txnmem_formal_controller.py`: adds the sole private installed-controller
  bridge needed to verify installed bytes, create/import from the committed
  export, preserve a primary failure, and remove the exact export. It is not in
  `_dispatch` or `main`.
- `src/txnmem_provenance_execution_collector.py`: owns the single lifecycle
  orchestration because this is where the production gated child, pidfd group
  validation, nft guard, immutable source export, seal, process inventory, and
  cleanup primitives already live. Small private helpers only normalize the
  fixed diagnostic config, current-process pidfd inventory, `/proc` credential
  and start identity checks, and inode-bound private-tree cleanup. It also adds
  exact receipt validation before the real candidate seal.
- `src/txnmem_provenance_runner.py`: adds the post-drop parent-death query used
  by ordinary runner hardening and the private exact-enum probe that creates the
  real resistant descendant and invokes the real diagnostic workload/publisher
  from immutable source. The probe is not reachable from `main`.
- `src/txnmem_provenance_performance.py`: adds the exact private publication
  mode needed to override diagnostic named fallback for only this invocation,
  while preserving default behavior, and moves the existing real candidate-seal
  validator to the promotion fail-fast boundary.
- `tests/test_txnmem_provenance_execution_collector.py`: adds the one exact
  selector and extends the existing candidate-seal test to preserve the valid
  receipt contract while proving an absent receipt causes no mode mutation.
- `tests/test_txnmem_formal_smoke.py`: changes no selector count; it extends the
  existing runner-hardening tests to model and verify the new exact
  `PR_GET_PDEATHSIG` call and mismatch rejection.
- `.superpowers/sdd/2026-08-28-formal-progress-fail-fast-v6/task-7c-report.md`:
  this implementation, verification, proof, and concern record.

The production lifecycle logic is intentionally not duplicated in test
fixtures or independent selectors. The larger collector/runner additions are
the two sides of the one real process boundary: the collector must own root,
pidfd, nft, receipt, seal, promotion, and cleanup observations, while the
immutable unprivileged runner must own post-drop state, descendant behavior,
workload execution, and anonymous publication.

## Proof mapping

| Requirement | Executable proof |
| --- | --- |
| Exactly one exact selector | Diff scan reports one added `def test_`; its fully qualified name exactly matches the brief. |
| One genuine lifecycle | One installed-controller subprocess, one committed export/import, one collector worker, one immutable runner, and one real descendant are linked through one gated run; no test method is called. |
| Installed controller | `_verify_installed_controller` verifies protected installed/approved/committed bytes before export. |
| Committed export | `_create_committed_export` creates the real export; `_remove_export` removes its exact bound inode; outer before/after inventory must match. |
| Collector and immutable runner | The exported collector starts the runner from the immutable source export under `-I -S -B`, with an explicit immutable `src` import root and runner hash check. |
| Groups/GID/UID drop | Production pre-exec clears groups then sets GID/UID; runner and collector independently require four exact UID/GID values and an empty group list. |
| Post-drop `PDEATHSIG=SIGKILL` | Pre-exec sets/queries after dropping credentials; post-exec runner hardening queries it again; the descendant re-establishes and queries its fork-cleared value. |
| Actual parent death | A revalidated pidfd sends SIGKILL to the real collector worker; subreaper wait statuses require SIGKILL for worker, runner, and descendant. |
| Resistant descendant | A distinct descendant ignores SIGTERM and holds the GIL in `PyDLL.sleep`; a real pidfd SIGTERM delivery leaves the exact two-process inventory alive. |
| pidfd identity drift/no wrong target | All exact members are pidfd-opened before signaling and inventory is re-read. Injected start drift rejects before delivery; process and pidfd inventories remain unchanged; no killpg or UID-wide fallback occurs. |
| Guard-last ordering | Guard is activated/verified before release, remains present after all three processes are reaped and the UID is empty, and is removed after tree cleanup and subreaper restoration. |
| Real anonymous pointer | The real publisher receives the private enum, sets named fallback false, uses the fd-bound publication path, proves no pre-link name/temp, and exposes the real canonical pointer after link. |
| Receipt distinctions | Internal object `COMPLETED.json` must exist while the runner-to-collector receipt FD is empty and the external collector completion file is absent. |
| Real seal rejection | The real `_seal_candidate_tree` is invoked with the absent receipt and must raise exactly `candidate completion receipt is invalid` before candidate mutation. |
| Real promotion rejection | The real `promote_provenance_candidate` is invoked on the still-intact real pointer and must raise exactly `formal topology omits candidate seal`; the formal output directory must not exist. |
| Zero residue | The private root's exact device/inode is removed; UID and current-process pidfd inventories are empty/baseline; nft and bootstrap inventories match baseline; controller subprocess is absent after wait; all receipt count fields are zero. |
| Exact private hooks | One-member enums, exact-type validators, `None` defaults, no public dispatch/CLI/config/environment/manifest/schema route, and no ordinary-run call site. |
| Smoke schema preserved | Pre-skip contract asserts authoritative `txnmem-formal-provenance-smoke-v2`. |
| Formal v6 matrix/identity frozen | The gate uses a tiny diagnostic config under a temporary root and no formal identity map, registration, nonce file, service, or database. |
| Sanitized output | Child streams go to temporary files; success emits one bounded canonical JSON line containing only fixed labels, booleans, and counts. |

## User-raised hazard review

1. Diagnostic named fallback: confirmed. Ordinary diagnostics retain
   `_allow_named_fallback=True`; only the exact private publication enum forces
   false, so the protected proof cannot take the compatibility path.
2. Completion concepts: confirmed separately. The internal bundle-object
   `COMPLETED.json` is required present. The runner-to-collector pipe receipt is
   required open and byte-empty. The external collector completion file is
   required absent before seal/promotion and through cleanup.
3. Formal identity reuse: none. No registered run hash or authorization nonce is
   read, created, or changed. Private authorization is the exact collector enum
   selected by the already verified installed controller for one call; the
   runner and publisher each require their own exact enum within that call.
   Ordinary formal registration checks are unchanged.

## Verification

All verbose output was redirected to private temporary files. Only aggregate
counts and selector names are recorded here.

### Focused and adjacent passes

- Exact selector RED: 1 run; 1 expected failure; 0 skips.
- Exact selector local GREEN: 1 run; 0 failures; 1 protected-primitives skip.
- Candidate-seal/runner-hardening focused repair: 2/2 pass and then 5/5 pass
  in the focused contract set (the protected promotion member remained an
  explicit platform skip where selected).
- Collector + smoke + performance regression:
  `Ran 284 tests in 8.535s`; 270 passed, 14 explicit skips, 0 failures.
- Task 7 Step 6 reader/controller/smoke/script regression:
  `Ran 142 tests in 1.908s`; 140 passed, 2 explicit environment skips,
  0 failures.

### Exact protected-gate list on this local host

Command: one `python3 -m unittest` invocation containing the 14 fully qualified
selectors from `task-7c-protected-gates.md`.

Sanitized outcome: `Ran 14 tests in 0.003s`; 0 passes, 14 explicit protected
Linux/root/filesystem skips, 0 failures. These skips are not passes:

- `test_parent_death_sigkill_terminates_child_after_actual_parent_exit`
- `test_parent_death_signal_real_kernel_set_and_query`
- `test_protected_linux_collector_kills_gil_holder_within_external_bound`
- `test_protected_linux_component_probes_root_drop_parent_death_pidfd_guard_pointer`
- `test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue`
- `test_protected_linux_pidfd_preflight_uses_real_kernel_syscalls`
- `test_protected_linux_post_popen_pidfd_failure_fail_stops_and_clears_uid`
- `test_protected_linux_preexec_mask_and_dedicated_uid_are_exact`
- `test_protected_linux_same_uid_peer_cannot_reopen_commit_fd_for_writing`
- `test_validated_group_kills_term_ignoring_descendant_fixture`
- `test_protected_linux_publication_gate_never_mismatches_pointer_and_receipt`
- `test_formal_pointer_writer_real_anonymous_inode_linkat_has_no_residue`
- `test_publisher_revalidates_and_exclusively_points_to_valid_formal_object`
- `test_two_stage_candidate_attestation_and_promotion_reuses_exact_bytes`

The protected host must rerun this exact list from the reviewed commit with
zero skips and repeat the independent post-process residue inventory before
Task 7C acceptance.

### Full suite and static checks

- Full repository discovery:
  `PYTHONPATH=src python3 -m unittest discover -s tests -v`;
  `Ran 1185 tests in 123.883s`; 1,166 passed, 19 explicit
  platform/root/environment skips, 0 failures.
- A preceding bare `unittest discover` found zero tests because this repository
  requires `-s tests`; it was rejected as invalid evidence and is not counted.
- `py_compile` passed for all four changed production modules and both changed
  test modules using an isolated bytecode cache.
- `git diff --check` passed.
- Scope scan: one added selector; exact six code/test files plus this report.
- Public-hook scan: zero added integrated argparse/environment/nonce routes.
- Credential-pattern scan: zero hits in added lines.
- Branch/base check: `codex/provenance-progress-v6` at `8c05f12` before the
  focused Task 7C commit.

## Self-review and concerns

The mutation-pressure checklist was reviewed directly:

- Bool, raw string, string subclass, and unrelated enum values cannot cross any
  hook validator; defaults are `None` and ordinary paths never call the hooks.
- A pre-drop-only parent-death implementation would fail the post-drop runner
  query, and a fork-cleared descendant setting would fail its own set/query
  readiness byte.
- Early parent death cannot produce the pointer: the runner must finish
  post-drop hardening and descendant readiness before the worker activates the
  guard and releases publication.
- A cooperative/non-distinct descendant would fail the exact two-member
  inventory and the delivered-SIGTERM survival check.
- Identity drift is checked after opening every pidfd and before any signal; the
  rejection path proves no process changed and no pidfd leaked.
- `_signal_formal_inventory` closes every opened pidfd even on failure; existing
  close-once and close-failure regressions remain in the passing collector suite.
- The guard cannot deactivate before exact process/UID and pidfd quiescence;
  fallback cleanup also attempts process, tree, subreaper, then guard in that
  order and preserves an active primary exception.
- A named diagnostic fallback would fail the precommit name inventory and the
  forced publication mode; ordinary diagnostic fallback remains unchanged.
- Internal `COMPLETED.json` alone cannot satisfy either the byte-empty receipt
  pipe or the absent external completion check.
- Seal and promotion booleans are accepted only alongside exact real exception
  reasons, an intact pointer, and absent promotion output.
- Lifecycle-tree deletion first verifies the original device/inode; committed
  export deletion uses the existing descriptor/device/inode implementation.
- macOS/rootless execution reaches only contract checks and an explicit skip;
  it cannot emit a passing protected receipt.

Concern/blocker: this machine cannot execute the protected-root Linux
lifecycle. Consequently, the integrated selector and all 14 protected gates
remain unaccepted until the protected host records zero skips and zero residue.
No remote access was attempted, no database/log/payload was read, no formal
identity or nonce was created/reused, no credentials or coordinates were
exposed, and no formal v6 matrix was launched. Per the task instruction, no
subagent, independent reviewer, push, or merge was used.

## Fix Round 1

Date: 2026-08-29

Rejected implementation under repair: `28f4f91`

Fix commit subject: `fix: bind integrated lifecycle cleanup`

### Accepted findings and corrected lifecycle

Every accepted Critical and Important finding in `task-7c-review.md` was
addressed. The deferred Minor concerning duplicated collector/runner lifecycle
material was intentionally left unchanged.

The sole integrated selector and its installed-controller/committed-export/
collector/immutable-runner lifecycle remain the same. The corrected boundaries
inside that one lifecycle are now:

1. Before the worker may start the runner, the surviving collector parent
   records the worker PID and `/proc` start ticks. The worker then records the
   exact runner and resistant-descendant PID/start inventory from the existing
   validated process-group boundary.
2. The worker transfers that exact two-process inventory and the runner's
   actual completion-receipt read FD to the surviving parent over one private
   Unix socket using `SCM_RIGHTS`. It relinquishes its copy; it does not create
   a replacement pipe or completion mapping.
3. Every lifecycle signal is issued only through a pidfd bound to a recorded
   PID. All pidfds are opened before any signal, every live PID's start ticks
   are revalidated after binding, and drift rejects before delivery. The
   dedicated-UID inventory is used only to assert expected membership or
   quiescence and is never converted into signal targets.
4. The worker never removes the nft guard. A parent-PID-bound owner object,
   created before `fork`, is the only route used by this lifecycle to call the
   real guard deactivation method. A non-owner call, failed quiescence check,
   start-identity drift, tree-cleanup failure, subreaper-restore failure, or any
   other cleanup failure leaves the guard active and fails closed.
5. After the parent kills the recorded worker through its revalidated pidfd,
   the kernel kills the runner and descendant through their post-drop
   `PDEATHSIG`. The parent reaps all three exact identities, confirms the
   dedicated UID is empty and owned pidfds are at baseline, and only then reads
   the transferred receipt FD through byte-empty EOF. That observed absence is
   represented by `None`; no `{}` is fabricated.
6. The external completion output is preflighted through the same
   `FormalStore` boundary used by ordinary collection. The ordinary production
   completion write was factored through `_write_collector_completion_record`;
   the integrated path calls that exact writer with the actual absent receipt
   and requires its real rejection plus an absent external completion file.
   The bundle object's internal `bundle_objects/<object>/COMPLETED.json` is
   independently required present and is never confused with either receipt.
7. The real `_seal_candidate_tree` receives the same actual `None` and must
   reject before mutation. The still-intact real anonymous-inode pointer then
   reaches the real promotion validator, which must reject the missing seal
   without output.
8. Lifecycle-tree cleanup first binds the exact parent and lifecycle-root
   device/inode. It performs a complete owner/type/device/inode inspection,
   rejecting links and other special files, then recursively reopens and
   revalidates every member with `O_NOFOLLOW` and deletes only through
   descriptor-relative `dir_fd` operations. Controller ownership is required
   outside the candidate subtree and dedicated-runner ownership within it. A
   dangling root replacement cannot be treated as absence.
9. The parent removes the tree, restores the subreaper, rechecks exact
   quiescence, and removes the nft guard last. Failure cleanup follows the same
   ownership and ordering rules; it never falls back to UID-wide signaling or
   `shutil`/path-recursive deletion.

The accepted contracts remain intact: the three hooks are still closed
one-member exact enums with `None` defaults; no public CLI, config, environment,
manifest, schema, registration, identity, or nonce route was added; ordinary
diagnostic named fallback remains unchanged; this one invocation still forces
the real anonymous-inode publication mode; post-drop parent-death behavior,
the real resistant descendant, exact fixed counts, real seal/promotion
rejections, the smoke-v2 spelling, and bounded sanitized output are unchanged.

### Binding TDD evidence

The same exact selector was extended with four pre-skip correction subtests;
no second Task 7C selector or independent test-method concatenation was added.
The first correction RED was run before any Fix Round 1 production edit.

Exact RED command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue -v
```

Sanitized RED outcome: status 1; one selector run; four failed subtests; zero
errors; zero skips. The expected failed correction labels were:

- `recorded-pidfd-identities-only`
- `surviving-parent-owns-guard-removal`
- `actual-receipt-and-completion-boundary`
- `descriptor-relative-tree-removal`

After implementing the corresponding production boundaries, the same exact
command returned status 0; one selector run; zero failures; one explicit local
skip for unavailable protected Linux primitives. The four pre-skip behavioral
contracts passed and the skipped kernel body is not counted as a protected
pass.

Aggressive source review then found a dangling-root replacement bypass in the
new cleanup helper. Before editing that helper, the same selector was tightened
to bind the edge and rerun with the same exact command.

Sanitized supplemental RED outcome: status 1; one selector run; one failed
`descriptor-relative-tree-removal` subtest; zero errors. After removing the
path-level absence shortcut and keeping the exact root FD open through removal,
the same command returned status 0; one selector run; zero failures; one
explicit protected-primitives skip.

The correction subtests now prove:

- exact recorded PID/start maps are pidfd-opened and revalidated before any
  signal, never query UID inventory for targeting, close every pidfd, and send
  no signal on drift;
- non-owner and failed-quiescence guard-removal attempts cannot deactivate the
  guard, while the lifecycle source has no worker removal call and exactly two
  parent-owned success/failure call sites;
- a real pipe read FD crosses a real Unix descriptor-transfer boundary, yields
  byte-empty EOF only after its writer closes, feeds the same absent object to
  the real completion writer and real seal, and ordinary collection calls that
  same completion writer;
- descriptor-relative cleanup rejects a nested link and a dangling root
  replacement without deleting either, contains no `shutil`, `rglob`, or
  path-level `exists` bypass, and uses `O_NOFOLLOW` plus `dir_fd` operations.

### Changed files and production-helper justification

Fix Round 1 changes exactly two code/test files plus this report:

- `src/txnmem_provenance_execution_collector.py`: corrects the one integrated
  orchestration and the real ordinary completion-write boundary. No public
  entry point or ordinary authorization behavior changes.
- `tests/test_txnmem_provenance_execution_collector.py`: extends only the
  already accepted exact Task 7C selector with four pre-skip behavioral
  subtests and source-closure assertions. It adds no selector.
- `.superpowers/sdd/2026-08-28-formal-progress-fail-fast-v6/task-7c-report.md`:
  records Fix Round 1's implementation, TDD evidence, verification, proof map,
  file/helper justification, skips, and concerns.

Every added or materially changed production helper is justified below:

- `_write_collector_completion_record`: one small extraction of the existing
  ordinary `FormalStore.write_json_exclusive` completion boundary. It rejects
  absent/non-mapping evidence and lets the integrated lifecycle exercise the
  actual writer rather than a decorative path; the ordinary successful call
  retains the same payload and mode.
- `_integrated_lifecycle_observed_start_ticks` and the adjusted strict
  `_integrated_lifecycle_start_ticks`: distinguish a dead PID from malformed or
  drifted identity without weakening the strict caller. Cleanup needs this
  distinction to wait exact recorded identities without targeting replacements.
- `_signal_integrated_lifecycle_identities`: centralizes the required
  open-all/revalidate-all/signal-exactly-recorded/close-all sequence. It has no
  UID enumeration and sends nothing if any bound live identity drifts.
- `_IntegratedLifecycleGuardOwner`: binds guard removal to the surviving
  parent's PID and requires a caller-supplied exact-quiescence check before the
  real deactivation. Both normal and exceptional removal use it; the worker
  cannot.
- `_normalize_integrated_lifecycle_worker_state`: closes the socket protocol to
  exactly one schema with one runner and one descendant PID/start identity.
- `_send_integrated_lifecycle_worker_state` and
  `_receive_integrated_lifecycle_worker_state`: transfer that closed state and
  exactly one actual receipt FD over the existing fork boundary with
  `SCM_RIGHTS`, canonical bytes, bounded reads, close-on-exec/non-inheritable FD
  handling, and exact ancillary-count validation.
- `_read_integrated_lifecycle_absent_receipt`: reads the transferred descriptor
  to EOF under a bound, rejects any byte or oversize/timeout, closes the FD, and
  returns the actual absent result used downstream.
- `_integrated_lifecycle_expected_owner`: defines the closed ownership rule:
  runner UID only at/below the candidate subtree, controller UID everywhere
  else.
- `_inspect_integrated_lifecycle_tree_fd`: creates the deletion manifest only
  after recursively binding every directory/regular-file type, owner,
  device, and inode through `O_NOFOLLOW` descriptors; links/special files fail.
- `_delete_integrated_lifecycle_tree_fd`: reopens and revalidates every manifest
  member before descriptor-relative deletion, retaining each bound FD through
  its unlink/rmdir operation.
- `_remove_integrated_lifecycle_tree`: now requires the pre-recorded parent and
  root device/inode plus both ownership domains, binds parent/root with
  `O_NOFOLLOW`, invokes the two-phase descriptor walk, and has no path-recursive
  or missing-path success fallback.
- `_run_protected_linux_integrated_lifecycle`: wires the worker gate, exact
  recorded lineage, parent-owned guard, actual FD transfer/EOF, actual external
  completion writer, same absent receipt at seal, exact cleanup metadata, and
  fail-closed final ordering into the existing single lifecycle. It does not
  duplicate the runner workload or publication implementation.

No other test module was changed in this round. The previously accepted six
code/test files in `28f4f91` remain justified by the original report; this fix
does not expand that scope.

### Proof mapping for the review corrections

| Accepted correction | Binding implementation and proof |
| --- | --- |
| No dedicated-UID-wide signaling fallback | The lifecycle has no direct pidfd-send call. `_signal_integrated_lifecycle_identities` accepts only a non-empty exact PID/start map, opens all pidfds, revalidates all live identities, sends only afterward, and closes all descriptors. The sole selector patches UID enumeration to raise while exact signaling still passes. Cleanup uses UID inventory only in `_require_formal_uid_processes` assertions. |
| Drift leaves guard active/fails closed | Drift causes the exact signal helper to return no delivery and raise. Exceptional cleanup records that failure, cannot satisfy its no-failure guard-removal predicate, and raises while retaining the table. The guard-owner subtest proves a failed drift/quiescence callback performs no deactivation. |
| Surviving parent is sole guard-removal owner | `_IntegratedLifecycleGuardOwner.owner_pid` is captured before fork. It rejects any other PID. Source closure proves `worker_main` has no direct or wrapped deactivation and that only the parent normal/finally paths invoke the owner. |
| Actual runner receipt FD retained by parent | The worker transfers `child._receipt_fd` with `SCM_RIGHTS`, clears its child ownership, and closes its local copy. Parent receipt EOF is read only after exact worker/runner/descendant SIGKILL reaping, empty UID, and pidfd baseline. The unit boundary uses a real pipe/socket pair. |
| Internal object completion is distinct | Worker independently requires the real bundle object's `COMPLETED.json`; this does not satisfy the still-open byte-empty receipt pipe or absent external file. |
| External completion uses real writer | `_collect_execution_evidence` now calls `_write_collector_completion_record`; the integrated lifecycle uses the exact preflighted completion store/name and passes actual `None`, requiring the real writer rejection and a missing output entry. |
| Actual absence reaches real seal/promotion | The value returned by the actual EOF reader is passed unchanged as `completion_receipt=absent_receipt` to `_seal_candidate_tree`. Exact seal rejection leaves the real pointer intact for the unchanged real promotion validator, which rejects the absent seal and creates no output. |
| Owner/type/inode-bound descriptor cleanup | Pre-recorded parent/root identities are required; every member is inspected and revalidated by owner/type/device/inode through `O_NOFOLLOW`; recursive deletion uses only `dir_fd` unlink/rmdir. Nested/dangling links reject and remain. No `shutil`, `rglob`, or absence shortcut exists in these helpers. |
| Binding TDD and one selector | Four correction labels fail together before production support, then pass before the local protected skip. A supplemental edge follows its own RED/GREEN. Diff closure from `8c05f12` reports exactly one added `def test_`, the brief's exact selector. |

### Verification commands and sanitized outcomes

Focused correction command:

```text
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_collector_writes_launch_before_run_and_completion_after_exact_candidate \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_candidate_seal_is_tree_complete_and_receipt_bound \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfds_open_all_then_revalidate_start_identity_before_signal \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfd_partial_open_failure_closes_once_without_signal \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfd_close_failure_never_broadens_target \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_cleanup_identity_failure_preserves_guard_and_is_hard_failure \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue \
  -v
```

Sanitized outcome: status 0; 7 tests run in 0.034s; 6 local passes;
1 explicit protected-primitives skip; 0 failures/errors.

Adjacent collector/smoke/performance command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector tests.test_txnmem_formal_smoke tests.test_txnmem_provenance_performance -v
```

Sanitized outcome: status 0; 284 tests run in 8.680s; 270 passes;
14 explicit environment/protected skips; 0 failures/errors.

Task 7 Step 6 command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_real_backend_script -v
```

Sanitized outcome: status 0; 142 tests run in 1.977s; 140 passes;
2 explicit environment skips; 0 failures/errors.

Local protected-gate command:

```text
PYTHONPATH=src python3 -m unittest \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_parent_death_sigkill_terminates_child_after_actual_parent_exit \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_parent_death_signal_real_kernel_set_and_query \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_collector_kills_gil_holder_within_external_bound \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_component_probes_root_drop_parent_death_pidfd_guard_pointer \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_pidfd_preflight_uses_real_kernel_syscalls \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_post_popen_pidfd_failure_fail_stops_and_clears_uid \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_preexec_mask_and_dedicated_uid_are_exact \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_same_uid_peer_cannot_reopen_commit_fd_for_writing \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_validated_group_kills_term_ignoring_descendant_fixture \
  tests.test_txnmem_formal_smoke.ProvenanceSmokeRunnerTests.test_protected_linux_publication_gate_never_mismatches_pointer_and_receipt \
  tests.test_txnmem_provenance_performance.ProvenanceAggregationTests.test_formal_pointer_writer_real_anonymous_inode_linkat_has_no_residue \
  tests.test_txnmem_provenance_performance.ProvenanceAggregationTests.test_publisher_revalidates_and_exclusively_points_to_valid_formal_object \
  tests.test_txnmem_provenance_performance.ProvenanceAggregationTests.test_two_stage_candidate_attestation_and_promotion_reuses_exact_bytes \
  -v
```

Sanitized outcome: status 0; 14 tests run in 0.022s; 0 protected passes;
14 explicit local platform/root/kernel/filesystem skips; 0 failures/errors.
These skips are separated from passes and continue to block protected-host
acceptance.

The one Fix Round 1 full-suite command, run after all production edits:

```text
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Sanitized outcome: status 0; 1,185 tests run in 124.296s; 1,166 passes;
19 explicit platform/root/environment skips; 0 failures/errors.

Syntax/diff/security commands:

```text
PYTHONPYCACHEPREFIX=<private-temporary-directory> python3 -m py_compile src/txnmem_formal_controller.py src/txnmem_provenance_execution_collector.py src/txnmem_provenance_runner.py src/txnmem_provenance_performance.py tests/test_txnmem_provenance_execution_collector.py tests/test_txnmem_formal_smoke.py
git diff --check
git diff 8c05f12 -- tests | rg -c '^\+    def test_'
git diff 8c05f12 -- src | rg '^\+' | rg -ci '(add_argument|os\.environ|getenv|authorization_nonce|authorization-nonce).*(integrated|pointer_without_receipt)|integrated.*(add_argument|os\.environ|getenv|authorization_nonce|authorization-nonce)'
git diff 8c05f12 -- src | rg '^\+' | rg -ci '"(integrated_lifecycle|pointer_without_receipt|private_publication_mode)"[[:space:]]*:'
git diff HEAD -- src tests | rg '^\+' | rg -ci '(password|passwd|api[_-]?key|access[_-]?token|private[_-]?key)[[:space:]]*='
```

Sanitized outcomes: `py_compile` status 0; `git diff --check` status 0;
exactly 1 added selector from the accepted base; 0 public enable-route hits;
0 config/schema enable-field hits; 0 credential-literal assignment hits. An
initial broader word scan returned two reviewed false positives for the private
cleanup manifest variable; neither is a public manifest field or enable route.

All verbose test output was redirected to private temporary files. The report
contains only commands, selector names, aggregate counts, fixed reasons, and
relative repository paths.

### Fix Round 1 self-review and concerns

- The accepted review text was reread verbatim and each Critical/Important
  correction was traced to both a production call site and a pre-skip binding
  contract in the sole selector.
- No map derived from `_formal_uid_processes` reaches any signal routine. The
  worker, runner, and descendant are represented only by recorded PID/start
  identities; exact pidfds are bound and revalidated before delivery.
- The worker source contains no guard deactivation. Normal and failure removal
  are parent-owned and occur only after exact lineage/UID/pidfd quiescence,
  descriptor-relative tree removal, subreaper restoration, and zero cleanup
  failures.
- The actual runner receipt read FD is the descriptor transferred to and read
  by the surviving parent. EOF is read after kernel death/reaping. Its exact
  `None` reaches both the actual external completion writer rejection and real
  seal rejection. The internal object completion marker remains separately
  required present.
- Cleanup has no path-recursive fallback and cannot treat a missing/dangling
  replacement as successful removal. Every deletion is preceded by exact
  owner/type/device/inode validation under no-follow descriptors.
- Ordinary diagnostic publication still uses named compatibility fallback by
  default; the integrated exact enum alone forces the real anonymous-inode
  path. Formal ordinary registration checks and the frozen matrix remain
  unchanged; no identity/nonce was used.
- Exactly one Task 7C selector exists. No Task 7C selector was added to another
  module, and no existing test method is invoked from it.
- No remote system, database, raw log/payload, credentials, server coordinates,
  usernames, nonces, or private remote paths were accessed or exposed. No
  subagent/reviewer was dispatched and no push/merge was performed.

Concern/blocker: this local machine still lacks the protected Linux/root/kernel/
filesystem environment. The corrected integrated kernel body and all 14 exact
protected gates therefore have zero local protected passes and remain blocked
on the required later protected-host zero-skip run and independent residue
inventory. The deferred Minor about duplicated collector/runner lifecycle
material remains for final whole-branch review, as directed; Fix Round 1 did not
expand scope to address it.

## Fix Round 2

Date: 2026-08-29

Starting commit: `317cdf2`

### Implementation and exact lifecycle

Fix Round 2 keeps the one accepted lifecycle and exact selector. It does not
add a second selector, invoke another test method, duplicate a runner workload,
or add a public activation route.

The corrected lifecycle is:

1. The surviving controller parent creates and PID-binds the nft guard owner
   before `fork`. The worker starts the existing gated immutable runner, binds
   the runner and resistant descendant PID/start identities, proves the exact
   credential drop, and transfers the actual runner receipt read FD plus the
   two exact identities to the parent over `SCM_RIGHTS`. It then blocks on the
   parent's guard-active byte.
2. The surviving parent validates the transferred lineage and invokes the real
   `_NftNetworkGuard` activation. Integrated activation deliberately retains a
   possibly created table when the post-apply snapshot fails; no rollback
   delete is available to the worker. Only after activation and verification
   does the parent release the worker, which releases the real runner.
3. The existing immutable runner publishes one real anonymous-inode pointer,
   preserves its internal bundle-object `COMPLETED.json`, deliberately omits
   the distinct runner/collector receipt, and leaves the distinct external
   collector completion entry absent. The worker proves the real pointer and
   resistant descendant behavior, using only already recorded PID/start
   identities and pidfds.
4. The parent kills the collector worker through its exact revalidated pidfd,
   proves kernel death and reaping of worker/runner/descendant, then reads the
   transferred receipt FD to byte-empty EOF. That actual absent result is fed
   into the ordinary collector completion writer, the real seal function, and
   the real promotion validator; all three required rejection boundaries are
   observed while the pointer remains real.
5. Tree cleanup binds parent, root, every directory, and every regular file by
   no-follow descriptor and owner/type/device/inode. Each object is atomically
   detached into a same-filesystem root-owned `0700` unique quarantine before
   its detached name and held FD are revalidated. Only the detached,
   revalidated object can be destroyed. Cleanup accumulates the first failure
   and original traceback while continuing safe siblings/descendants and every
   owned FD close.
6. The surviving parent alone can remove the nft table. Both normal and
   exceptional removal require exact lineage death/reaping, assertion-only UID
   emptiness, pidfd baseline, successful bound-tree cleanup, subreaper restore,
   and zero cleanup failures. Identity drift, quarantine mismatch, close
   failure, or any other cleanup failure keeps the guard active and fails
   closed.

Ordinary diagnostic publication behavior is unchanged: its named compatibility
fallback remains the default. The integrated private exact enum remains the
only path that forces the real anonymous-inode proof. Ordinary registration,
the frozen formal matrix, and the authoritative
`txnmem-formal-provenance-smoke-v2` spelling are unchanged.

### Binding TDD RED and GREEN

Only the existing exact selector was extended:

```text
tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue
```

Before any Fix Round 2 production edit, the selector added four pre-skip
behavior groups. The exact RED command was:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue -v
```

Sanitized RED outcome: status 1; 10 assertion failures; 0 errors; 1 explicit
protected-body skip. The failures map to one real post-apply nft snapshot
rollback/delete, three same-name replacement deletions (regular file, nested
directory, and root), five SCM ownership/protocol cases (timeout restoration,
unexpected ancillary with and without restore failure, non-integral control
data, and message truncation), and one first-failure cleanup stop/replacement.
The multiple-FD SCM case was present in the same RED group and already rejected
by the narrow legacy count check; the failing sibling cases bound the missing
outer ownership and closed-protocol behavior while the multiple-FD case
retained its no-leak contract.

After only the guard-ownership production correction, the same command gave
status 1; 9 failures; 0 errors; 1 protected-body skip. The nft correction was
GREEN while all remaining correction failures stayed RED.

After atomic quarantine and failure accumulation, the same command gave status
1; 5 SCM failures; 1 existing dangling-root normalization error; 1
protected-body skip. All three swap cases and the failure-accumulation case
were GREEN. The dangling-root `ENOTDIR` was normalized to the existing
fail-closed identity `CollectorError` contract.

Final GREEN used the same exact command. Sanitized outcome: status 0; the four
pre-skip correction groups completed with no failure/error; unittest then
reported the protected lifecycle body as exactly 1 explicit skip. That skip is
not counted as a pass.

The behavior groups prove:

- `round2-parent-owned-guard-activation`: a real `_NftNetworkGuard.activate`
  with a post-apply snapshot failure demonstrates the old delete, while the
  PID-bound integrated owner rejects a non-owner before any nft call and the
  parent activation retains the real table/active state. Failed quiescence
  cannot deactivate it.
- `round2-atomic-quarantine-deletion`: a swap is injected at the real atomic
  rename boundary after descriptor validation for a regular file, nested
  directory, and root. In every case both original and replacement inode
  survive, cleanup fails, and the guard-removal callback is not reached.
- `round2-exact-scm-rights-receive`: real socket pairs and real SCM_RIGHTS FDs
  cover timeout-restore failure, unexpected control records, protocol-primary
  preservation when restore also fails, non-integral control bytes, multiple
  FDs, and `MSG_TRUNC`. Every actually received FD is attempted exactly once by
  the receiver and is closed on every rejection (`fstat` then fails).
- `round2-failure-accumulating-cleanup`: the sorted first sibling's quarantine
  unlink raises a specific `BaseException`; its FD is really closed and then a
  secondary close exception is injected. The later sibling is still attempted,
  the exact primary object and originating traceback survive, and guard removal
  is blocked.

### Changed files and helper justification

Fix Round 2 changes exactly these files:

- `src/txnmem_provenance_execution_collector.py`: corrects guard activation
  ownership, exact SCM receipt ownership, atomic lifecycle-tree removal,
  failure accumulation, and the existing one-lifecycle parent/worker
  handshake. It does not change public collection parameters or ordinary
  authorization/publication behavior.
- `tests/test_txnmem_provenance_execution_collector.py`: extends only the sole
  exact Task 7C selector with the four required pre-skip behavior groups. No
  other test method or test module is changed and no test method is called from
  the selector.
- `.superpowers/sdd/2026-08-28-formal-progress-fail-fast-v6/task-7c-report.md`:
  records this round's exact implementation, TDD evidence, proof map,
  verification, skips, self-review, and remaining protected-host blocker.

Every new or materially changed production helper is justified here:

- `_NftNetworkGuard._activate_retaining_table`, plus its integrated owner PID
  checks in `activate`/`deactivate`: uses the real nft check/apply/snapshot but
  prevents the ordinary rollback delete after table creation in this private
  lifecycle. Ordinary guards have no integrated owner PID and retain their
  accepted rollback behavior.
- `_IntegratedLifecycleGuardOwner.activate_retaining_table`: makes the
  surviving parent PID the sole integrated activation caller, complementing
  its existing sole-removal/quiescence boundary.
- `_receive_integrated_lifecycle_worker_state`: places every descriptor
  received from `recvmsg` under one exception-safe ownership scope through
  ancillary parsing, canonical-state validation, non-inheritable setup,
  timeout restoration, close, or transfer. It closes all complete SCM_RIGHTS
  integers and preserves a protocol primary over restore/close failures.
- `_integrated_lifecycle_manifest_identity_matches`: one closed comparison for
  the only accepted regular-file/directory type plus exact owner/device/inode;
  it prevents divergent checks across detach and recursive cleanup.
- `_quarantine_integrated_lifecycle_entry`: atomically renames one bound name
  to a unique protected quarantine entry and revalidates both detached name and
  held FD before returning a destroyable name. Mismatch leaves the unbound
  object intact and raises.
- `_delete_integrated_lifecycle_tree_fd`: consumes the existing bound manifest,
  quarantines each child before destruction, captures the first exception and
  traceback, continues every still-safe child/descendant, attempts every child
  FD close, and raises the original primary last.
- `_remove_integrated_lifecycle_tree`: creates and binds the same-filesystem
  root-owned quarantine, applies the atomic rule to the lifecycle root itself,
  closes all root/quarantine/parent FDs even after failure, preserves the
  operation primary, and normalizes a dangling/no-follow root to the existing
  identity failure.
- `_run_protected_linux_integrated_lifecycle`: moves real guard activation to
  the surviving parent and adds one parent/worker byte gate around the existing
  runner release. It transfers the actual receipt FD before activation so the
  parent can retain it even when the worker later dies; it adds no independent
  lifecycle implementation.

No other production helper or test module changed in Fix Round 2.

### Proof map for every Fix Round 2 finding

| Finding | Production proof | Behavioral proof |
| --- | --- | --- |
| Critical: all post-create nft removal is parent-owned | Integrated owner PID is bound before fork. Worker sends state/receipt then waits. Parent alone calls real activation and release; integrated snapshot failure leaves `active=True` and never invokes delete. Both `activate` and `deactivate` reject inherited non-owner PIDs. Finally removal is gated by complete quiescence and zero cleanup failures. | Real post-apply snapshot fault shows the ordinary old rollback delete, non-owner calls make zero nft calls, parent-owned integrated activation retains the table, and failed quiescence performs no delete. Source closure has no worker guard call. |
| Important: atomic deletion binding | Every file, nested directory, and root is atomically renamed by `dir_fd` into unique mode-0700 quarantine, then compared against its manifest and held `O_NOFOLLOW` FD before unlink/rmdir. Mismatch is retained and raised. | Three rename-boundary swaps prove both original and replacement inode survive and guard deactivation remains unreachable. |
| Important: exact/exception-safe SCM_RIGHTS receive | One outer ownership scope captures all complete rights integers immediately after `recvmsg`; protocol requires one exact SOL_SOCKET/SCM_RIGHTS record of one-int size and rejects extra/non-rights/non-integral/multiple/truncated control or payload. Timeout restore and every close are attempted without replacing the first error; success transfers exactly one non-inheritable FD. | Six real descriptor-transfer cases prove invalid/restore primary selection, exact-once receiver close, and no live received FD after rejection. Both `MSG_CTRUNC` and `MSG_TRUNC` are closed source checks; `MSG_TRUNC` is fault-injected. |
| Important: failure-accumulating cleanup | Recursive cleanup records the first exception plus traceback, continues safe sorted siblings/descendants, closes every owned child FD, and raises the exact first exception last. The lifecycle finally treats any cleanup failure as a guard-removal veto. | First sibling unlink failure plus secondary close failure still reaches the later sibling; the exact primary and traceback are asserted; deactivation events remain empty. |
| Important: all behavior bindings precede protected skip | The production path is not needed for contract checks on non-protected systems. | All four correction labels execute before the one platform/root/kernel/filesystem skip in the sole exact selector. |

Accepted contracts remain intact: exact recorded PID/start pidfd identities only;
UID inventory is assertion-only; actual receipt FD/EOF reaches the real
completion writer, seal, and promotion validator; the private fault enums each
have one exact member and default `None`; no public route enables them; the
integrated diagnostic path forces the real anonymous pointer without changing
ordinary named diagnostic fallback; fixed result counts and bounded sanitized
output remain unchanged.

### Verification commands and sanitized outcomes

Reviewer-focused seven selectors:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_collector_writes_launch_before_run_and_completion_after_exact_candidate tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_candidate_seal_is_tree_complete_and_receipt_bound tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfds_open_all_then_revalidate_start_identity_before_signal tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfd_partial_open_failure_closes_once_without_signal tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_formal_pidfd_close_failure_never_broadens_target tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_cleanup_identity_failure_preserves_guard_and_is_hard_failure tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue
```

Sanitized outcome: status 0; 7 tests in 0.047s; 6 passes; 1 explicit
protected-body skip; 0 failures/errors.

Adjacent collector/smoke/performance:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector tests.test_txnmem_formal_smoke tests.test_txnmem_provenance_performance
```

Sanitized outcome: status 0; 284 tests in 8.502s; 270 passes; 14 explicit
environment/protected skips; 0 failures/errors.

Task 7 Step 6:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_real_backend_script
```

Sanitized outcome: status 0; 142 tests in 1.903s; 140 passes; 2 explicit
environment skips; 0 failures/errors.

Explicit protected gates used the same 14-selector command listed in Fix Round
1. Sanitized outcome: status 0; 14 tests in 0.031s; 0 protected passes; 14
explicit platform/root/kernel/filesystem skips; 0 failures/errors. These skips
are not included in any pass count.

The one Fix Round 2 full-suite run, after all production/test edits:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
```

Sanitized outcome: status 0; 1,185 tests in 124.546s; 1,166 passes; 19 explicit
platform/root/environment skips; 0 failures/errors.

Static/diff/security checks:

```text
env PYTHONPYCACHEPREFIX=/private/tmp/txnmem-task7c-r2-pycache python3 -m py_compile src/txnmem_provenance_execution_collector.py tests/test_txnmem_provenance_execution_collector.py
git diff --check
git diff 8c05f12 -- tests/test_txnmem_provenance_execution_collector.py | rg -c '^\+\s+def test_'
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(add_argument|ArgumentParser|TXNMEM_.*INTEGRATED|INTEGRATED.*TXNMEM_|os\.environ.*INTEGRATED|getenv.*INTEGRATED)'
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(password\s*=|passwd\s*=|token\s*=|secret\s*=|api[_-]?key\s*=|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY)'
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(_formal_uid_processes.*(kill|signal)|killall|pkill|kill\s+--?uid|kill\s+-u)'
rg -n 'self\.test_[A-Za-z0-9_]+' tests/test_txnmem_provenance_execution_collector.py
rg -n --pcre2 'txnmem-formal-provenance-smoke-v(?!2)' src tests configs scripts
```

Sanitized outcomes: `py_compile` status 0 using the private temporary cache;
`git diff --check` status 0; exactly 1 selector added from accepted Task 7B
commit `8c05f12`; 0 public integrated enable-route hits; 0 credential-literal
assignment hits; 0 UID-wide signal hits; 0 cross-test-method calls; 0 smoke
schema spellings other than v2. The first `py_compile` attempt used macOS's
default cache location and failed with a cache-directory permission error
before compilation; rerunning the exact files with the private temporary cache
gave status 0. No code failure was hidden.

All verbose outputs were redirected to private temporary files. Only aggregate
counts and selector names are recorded here.

### Fix Round 2 self-review and concerns

- The four accepted findings were reread verbatim and each is mapped above to
  both a production boundary and behavior in the only selector before skip.
- Worker source has no guard activation or removal call. The inherited real
  guard additionally rejects direct activation/deactivation from any PID other
  than the bound surviving parent, before an nft command. Snapshot failure
  after apply retains the table and makes exceptional cleanup prove complete
  quiescence before parent removal.
- There is no UID-derived signaling path. Every signal target comes from the
  worker/runner/descendant PID/start map, all pidfds open before all identities
  revalidate, and any drift stops delivery and keeps the guard.
- Quarantine is unique, root-owned, mode `0700`, on the expected device, and
  held open. Destruction uses only its detached descriptor-relative names after
  held-FD revalidation. A same-name replacement can be quarantined but cannot
  be destroyed because its inode mismatches; both it and the originally bound
  object remain recoverable and guard removal is vetoed.
- SCM ownership starts immediately after the real `recvmsg`; all complete
  SCM_RIGHTS integers are collected before validation. Restore, parse,
  canonical validation, inheritable setup, close, and transfer have one owner.
  A primary protocol error survives secondary timeout/close errors.
- Cleanup catches `BaseException` intentionally at ownership boundaries so
  even injected non-`Exception` failures cannot skip later safe children or FD
  closes. It re-raises the exact primary with its original traceback and lets
  the lifecycle's zero-cleanup-failure predicate veto guard removal.
- No formal identity or nonce was created or reused. No remote service,
  database, raw log/payload, credential, server coordinate, username, nonce, or
  private remote path was accessed or exposed. No subagent/reviewer was
  dispatched; no push or merge was performed.
- The large pre-existing Task 7C delta was not expanded into another lifecycle.
  This round changes only one production module, the one existing selector,
  and this report. The deferred Minor concerning duplicated lifecycle material
  remains deliberately unaddressed, as instructed.

Remaining concern/blocker: this machine is not the protected Linux/root/kernel/
filesystem host. The exact integrated kernel body and all 14 protected gates
therefore have zero protected passes locally and must be rerun from the final
commit with zero skips, followed by the independent residue inventory. No
other known Critical or Important concern remains after local verification.

## Fix Round 3

Date: 2026-08-29

Round baseline: `85dcf1f`

This round addresses only I-N4 and I-N5 from `task-7c-r2-review.md` and adds
the missing behavioral bindings requested by that review. It preserves the
reviewed C-N1 parent-only guard ownership, I-N1 atomic quarantine deletion,
I-N2 exact SCM_RIGHTS ownership, exact recorded pidfd identities, the actual
runner receipt FD/EOF boundary, and the private exact-enum contracts. It does
not add a second selector or call another test method.

### Implementation and exact lifecycle

The real integrated lifecycle now has one resource record and one finalization
owner:

1. `_prepare_integrated_lifecycle_setup` computes immutable names first, creates
   the private lifecycle root, and immediately transfers it into an unbound,
   fail-closed `_IntegratedLifecycleResources` record. Its resource-owning
   `try` then binds and validates the exact parent/root device and inode
   identities before any root mode/ownership, workspace/input/export,
   external-output preflight, guard-owner, subreaper, socket, or fork work. A
   binding failure is permanently recorded, makes the one cleanup transaction
   non-retryable, leaves the unbound root untouched, and vetoes guard removal.
2. A successful subreaper change is marked owned immediately. Both socketpair
   endpoints are put in their ownership slots before either inheritable-state
   operation. A setup exception is captured with its original traceback and
   sent to the same finalizer used by the real lifecycle body.
3. The surviving parent records the worker PID/start identity, receives the
   real runner/descendant PID/start inventory and actual receipt read FD, and
   remains the sole owner of guard activation/removal. The worker never owns or
   invokes guard removal.
4. The existing lifecycle still proves the real anonymous pointer, bundle
   object's internal `COMPLETED.json`, absent runner-to-collector receipt EOF,
   rejected real external completion writer, rejected real seal, and rejected
   real promotion validator in that order.
5. The lifecycle body performs no formal tree removal. Its `finally` calls
   `_finalize_integrated_lifecycle_resources` exactly once, passing the exact
   body/setup primary object and traceback.
6. The finalizer closes every still-owned channel/receipt FD, signals only
   recorded PID/start identities through the existing pidfd boundary, reaps and
   verifies quiescence, makes one formal tree-removal decision, restores an
   owned subreaper, and only then evaluates guard removal.
7. `_IntegratedLifecycleTreeCleanup` is a persistent, one-attempt transaction.
   It records the unique quarantine name, bound quarantine identity, root
   removal, and quarantine resolution. A cleanup transaction cannot be retried.
8. Guard removal requires all of: no body/setup primary, no persistent cleanup
   primary, an empty persistent failure list, exact lineage quiescence, real
   root removal, no unresolved quarantine, restored subreaper state, unchanged
   pidfd inventory, empty assertion-only dedicated-UID inventory, and no live
   recorded identity. Any failure permanently vetoes removal and the exact
   first primary is re-raised with its original traceback.

There is no fallback to installed-controller export cleanup for the independent
lifecycle root and no path-based or `shutil` retry. The accepted descriptor-
relative, owner/type/device/inode-bound recursive deletion remains the only
tree destruction implementation.

### Resource ownership table

| Resource | Ownership acquisition | Persistent record | Exact release / veto |
| --- | --- | --- | --- |
| Lifecycle parent/root | immediate unbound resource record after `mkdtemp`, then no-follow metadata/resolve binding in its `try` | bound flag, parent and root device/inode, controller UID | finalizer's one tree transaction; unbound/mismatch/failure leaves residue and vetoes guard |
| Candidate/input/export/external subtree | construction below the bound root | exact runner-owned relative candidate plus root transaction | descriptor-relative inspection, detach, revalidation, recursive deletion |
| Quarantine | unique directory created by the remover | name, device, inode, created/removed flags | removed only after detached entries; any residue is permanently visible and vetoes guard |
| Subreaper state | successful `PR_SET_CHILD_SUBREAPER` | `prctl`, prior value, owned/restored flags | finalizer restores prior value; restore failure preserves ownership and primary precedence |
| Parent socket endpoint | immediately after `socketpair` | `parent_channel` slot | exact-once slot transfer-to-null before close |
| Worker socket endpoint | immediately after `socketpair` | `worker_channel` slot | exact-once slot transfer-to-null before close |
| Runner receipt read FD | successful exact SCM_RIGHTS receive | `parent_receipt_fd` slot | transferred once to the EOF reader or closed once by finalizer |
| Worker/runner/descendant | worker fork or exact received PID/start inventory | `worker_pid`, `recorded_identities`, `reaped_statuses` | pidfd bind/revalidate/signal only; UID inventory is assertion-only |
| nft guard | parent constructs owner and activates | `guard`, `guard_owner` | only the surviving-parent finalizer may remove it after the complete predicate |
| Failure state | first body/setup/cleanup failure | exact exception/traceback in `cleanup_primary` plus persistent list | never cleared; later safe cleanup continues, then first primary is re-raised |

### TDD RED and GREEN

The sole existing selector was edited first; no production file had changed
when the binding RED was recorded.

Exact RED command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue -v
```

Sanitized RED outcome: status 1; 1 selector; 6 failing pre-skip subtests;
0 errors; 1 explicit protected-body skip. Failure labels were:

- `round3-persistent-final-cleanup-veto`
- `round3-setup-resource-ownership (post-root-chown)`
- `round3-setup-resource-ownership (workspace-construction)`
- `round3-setup-resource-ownership (export-construction)`
- `round3-setup-resource-ownership (post-subreaper-pre-socket)`
- `round3-setup-resource-ownership (socket-endpoint-setup)`

The I-N4 subtest requires the same production finalizer called by the real
lifecycle and wraps the real remover. It injects the first quarantine unlink
failure and binds one removal attempt, the exact primary/traceback, a visible
root and quarantine residue, `tree_removed == False`, and zero guard-deactivate
calls. Its source-closure assertions require exactly one real-lifecycle
finalizer call, no body remover call, and one finalizer-owned guard removal
call. The five I-N5 labels invoke the actual production setup boundary and
inject failures at the required acquisition points.

One test-harness calibration run, still before production edits, initially
counted each regular child FD once rather than once during manifest binding and
once during deletion binding. The expectation was corrected to two; the
binding RED above was then rerun against unchanged production and is the RED of
record.

After the minimum production implementation, the same exact command was run.
Sanitized GREEN outcome: status 0; 1 selector in 0.059s; every pre-skip subtest
passed; 1 explicit protected-body skip; 0 failures/errors. After final
self-review moved identity bootstrap under immediate fail-closed resource
ownership, the exact selector was rerun in 0.062s with the same GREEN result.
The skip is not a protected pass.

The same selector also now behavior-binds the already accepted SCM receiver:

- `MSG_CTRUNC` is rejected and every received descriptor is closed;
- a descriptor is really closed before an injected secondary close exception,
  while the protocol primary remains the raised exception;
- a successful transferred descriptor is non-inheritable, is not closed by the
  receiver after ownership transfer, and remains usable by the caller;
- the cleanup accumulator attempts both binding and close for every later
  sibling and nested descendant FD after the first sorted unlink failure, while
  retaining the first primary traceback despite a secondary close failure.

These supplemental bindings passed the already accepted Round 2 production
implementation; no SCM implementation rewrite was made in this round.

### Proof map for the accepted findings

I-N4, permanent cleanup veto:

- `_run_protected_linux_integrated_lifecycle` contains one finalizer call and no
  direct `_remove_integrated_lifecycle_tree` call.
- `_finalize_integrated_lifecycle_resources` is the sole formal tree-removal
  decision and the sole integrated guard-removal call site.
- `_IntegratedLifecycleTreeCleanup.attempted` rejects a second formal cleanup
  attempt. The transaction retains unresolved quarantine state even if other
  later cleanup operations succeed.
- `cleanup_primary` and `lifecycle_failures` are never replaced or reset.
  `tree_removed` requires both actual root removal and zero unresolved
  quarantine. Every failure therefore permanently vetoes guard removal.
- The production-helper subtest uses the real remover and verifies one attempt,
  exact primary/traceback, visible residue, no guard deactivation, and false
  `tree_removed`.

I-N5, setup ownership from first mutable resource:

- `mkdtemp` success is immediately registered in a fail-closed resource record;
  exact root and parent identities are then bound and stored in its owning
  `try` before mode, ownership, workspace, export, preflight, subreaper, socket,
  or fork work. An identity-bootstrap failure retains the original primary,
  marks the cleanup transaction attempted, and cannot delete an unbound name.
- All those operations are inside the setup owner's exception boundary and use
  the same production finalizer as body failures.
- Subreaper ownership is recorded immediately after the successful state
  change. Both socket endpoint slots are populated immediately after
  `socketpair`, before inheritable setup.
- Setup cleanup records the original exception and traceback first, continues
  safe closes/tree cleanup/subreaper restoration, and does not let a secondary
  failure replace the primary or permit guard removal.
- Five production-boundary fault cases prove one root cleanup attempt, removed
  root and zero quarantine on successful cleanup, exact primary/traceback,
  subreaper set/restore where owned, and exact-once close of both socket
  endpoints.

Preserved reviewed contracts:

- C-N1: guard activation and every removal path remain in the surviving parent;
  activation post-apply snapshot failure retains the table and worker code has
  no guard owner/removal access.
- I-N1: files, nested directories, and root are atomically detached to unique
  quarantine names and revalidated against held owner/type/device/inode
  descriptors before destruction; replacements survive swaps.
- I-N2: the SCM receiver maintains one outer FD-ownership `try/finally`, rejects
  non-exact ancillary protocols and truncation, preserves the primary, and
  closes/transfers every received FD exactly once.
- Signals use only recorded PID/start identities after pidfd binding and
  revalidation; there is no dedicated-UID-wide signaling fallback.
- The actual receipt FD reaches byte-empty EOF only after kernel death/reaping;
  that actual absence reaches the real completion writer, seal, and promotion
  validator.
- `_IntegratedLifecycleFault` remains an exact private one-member enum with a
  `None` default and no CLI/config/environment/manifest/schema route. Ordinary
  registration checks and diagnostic named-fallback behavior are unchanged.
- The authoritative smoke schema remains
  `txnmem-formal-provenance-smoke-v2`, fixed formal counts remain unchanged,
  and there is one lifecycle and one Task 7C selector.

### Regression and local-skip results

All verbose outputs were redirected to private temporary files. Only aggregate
counts and selector labels are reported here.

Reviewer-focused seven selectors (candidate seal, three pidfd failure modes,
cleanup identity, and the integrated selector): status 0; 7 tests in 0.075s;
6 passes; 1 protected skip; 0 failures/errors.

Adjacent collector/smoke/performance command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_execution_collector tests.test_txnmem_formal_smoke tests.test_txnmem_provenance_performance
```

Sanitized outcome: status 0; 284 tests in 8.528s; 270 passes; 14 explicit
environment/protected skips; 0 failures/errors.

Task 7 Step 6 command:

```text
PYTHONPATH=src python3 -m unittest tests.test_txnmem_provenance_progress tests.test_txnmem_formal_controller tests.test_txnmem_formal_smoke tests.test_real_backend_script
```

Sanitized outcome: status 0; 142 tests in 2.195s; 140 passes; 2 explicit
environment skips; 0 failures/errors.

The exact 14-selector protected-gate command remains the one listed in Fix
Round 1. Sanitized outcome: status 0; 14 tests in 0.051s; 0 protected passes;
14 explicit local platform/root/kernel/filesystem skips; 0 failures/errors.
These skips are not included in any pass count.

The final Fix Round 3 full-suite command, after all production/test edits and
the identity-bootstrap self-review correction:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
```

Sanitized outcome: status 0; 1,185 tests in 124.028s; 1,166 passes; 19 explicit
platform/root/environment skips; 0 failures/errors.

A pre-final full discover had also passed 1,185 tests in 124.688s with the same
1,166/19/0 pass/skip/fail split. It was rerun because the subsequent self-review
correction moved the identity bootstrap itself under immediate resource
ownership; only the post-correction run above is used as final evidence.

### Static, diff, and security checks

Commands:

```text
env PYTHONPYCACHEPREFIX=/private/tmp/txnmem-task7c-r3-pycache-final python3 -m py_compile src/txnmem_provenance_execution_collector.py tests/test_txnmem_provenance_execution_collector.py
git diff --check
git diff 8c05f12 -- tests/test_txnmem_provenance_execution_collector.py | rg -c '^\+\s+def test_'
rg -n '^\s+def test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue\(' tests
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(add_argument|ArgumentParser|TXNMEM_.*INTEGRATED|INTEGRATED.*TXNMEM_|os\.environ.*INTEGRATED|getenv.*INTEGRATED)'
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(password\s*=|passwd\s*=|token\s*=|secret\s*=|api[_-]?key\s*=|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY)'
git diff 8c05f12 -- src tests configs scripts | rg '^\+.*(_formal_uid_processes.*(kill|signal)|killall|pkill|kill\s+--?uid|kill\s+-u)'
rg -n 'self\.test_[A-Za-z0-9_]+' tests/test_txnmem_provenance_execution_collector.py
rg -n --pcre2 'txnmem-formal-provenance-smoke-v(?!2)' src tests configs scripts
```

Sanitized outcomes: `py_compile` status 0 with isolated cache; `git diff
--check` status 0; exactly 1 added selector from accepted Task 7B commit
`8c05f12`; exactly 1 definition of the required selector; 0 public integrated
enable-route hits; 0 credential-literal assignment hits; 0 UID-wide signal
hits; 0 cross-test-method calls; 0 smoke schema spellings other than v2.

### Changed files and helper justification

- `src/txnmem_provenance_execution_collector.py`: adds the persistent resource,
  cleanup-transaction, captured-failure, setup-result, setup-owner, and finalizer
  boundaries. These helpers are necessary to make one owner and one cleanup
  decision explicit and behavior-testable; they factor the existing lifecycle
  rather than implement a second lifecycle. The integrated body now delegates
  setup and final cleanup to those boundaries.
- `tests/test_txnmem_provenance_execution_collector.py`: extends only the one
  existing Task 7C selector with I-N4/I-N5 production-boundary fault injection
  and the missing SCM/failure-accumulator behavior bindings. No new test method
  or cross-test call is added.
- `.superpowers/sdd/2026-08-28-formal-progress-fail-fast-v6/task-7c-report.md`:
  records this review round's design, TDD evidence, proof map, aggregate
  verification, and remaining blocker.

### Self-review and remaining concern

- The real lifecycle has one finalizer invocation, no body tree-remover call,
  and no worker guard-removal route. The finalizer has one formal tree-remover
  call and one integrated guard-removal call.
- The cleanup transaction rejects blind retry and retains unresolved quarantine
  inventory. No later successful cleanup can clear the first failure or turn
  residue into `tree_removed`.
- Setup-owned sockets are nulled before close for exact-once semantics; an owned
  subreaper remains marked owned if restoration fails. Primary exception object
  and traceback precedence are preserved across all secondary cleanup failures.
- Final self-review found and removed a smaller pre-ownership window between
  `mkdtemp` and identity binding. The root now enters resource state immediately
  in unbound fail-closed mode; no unbound path is ever deleted, and a binding
  failure permanently vetoes the guard. All focused and full verification was
  rerun after this correction.
- The fixed selector schema/counts, anonymous pointer path, external completion
  boundary, real seal/promotion rejection, private enum, and ordinary
  collector/diagnostic behavior remain unchanged.
- No deferred Minor was deliberately addressed. No second lifecycle or selector
  was introduced despite the necessary extraction of setup/finalization state.
- No remote system, database, raw log/payload, formal identity, nonce,
  credential, server coordinate, username, or private remote path was accessed
  or exposed. No subagent/reviewer was dispatched; no push or merge occurred.

Remaining concern/blocker: this machine cannot execute the protected-root Linux
kernel/filesystem lifecycle. The exact integrated body and all 14 protected
gates therefore have zero protected passes locally. The final commit must be
rerun on the protected host with zero skips, followed by the independent residue
inventory. No other known Critical or Important concern remains after the local
behavioral and full-suite verification above.
