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
