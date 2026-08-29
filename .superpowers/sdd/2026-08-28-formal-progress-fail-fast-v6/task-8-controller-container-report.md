# Task 8A controller-container report

## RED evidence

- `PYTHONPATH=src python3 -m unittest tests/test_txnmem_formal_controller_container.py -v`
  failed before implementation with `ModuleNotFoundError: No module named
  'txnmem_formal_controller_container'`.
- After the closure assertions were added and before the closure changes,
  `PYTHONPATH=src python3 -m unittest tests/test_txnmem_formal_controller.py
  tests/test_txnmem_provenance_execution_collector.py -v` failed with two
  expected missing approved-source entries.
- The inspection-error regression test failed before the exact absence check,
  reaching `Docker create failed` rather than failing closed at inspect.

## GREEN verification

- `PYTHONPATH=src python3 -m unittest tests/test_txnmem_formal_controller_container.py tests/test_txnmem_formal_controller.py tests/test_txnmem_provenance_execution_collector.py -v`
  — 189 passed, 10 platform-specific skips, 0 failures.
- `git diff --check` — passed.
- `PYTHONPYCACHEPREFIX=/private/tmp/txnmem-task-8a-pycache python3 -m py_compile src/txnmem_formal_controller_container.py src/txnmem_formal_controller.py src/txnmem_provenance_execution_collector.py`
  — passed.
- `bash -n scripts/manage_formal_controller_container.sh scripts/install_formal_provenance_runtime.sh`
  — passed.

## Files changed

- `infra/formal_controller/Dockerfile`
- `src/txnmem_formal_controller_container.py`
- `scripts/manage_formal_controller_container.sh`
- approved-source closures in the formal controller, collector, and installer
- focused lifecycle and closure tests

## Commit

- Implementation: `777138f646cfd6182a80fd520680bb3b758e8416`

## Remaining remote-only validation

- Build the pinned image and exercise the protected lifecycle on the approved
  target host. No Docker build, container lifecycle, smoke run, or formal
  matrix was launched locally.

## Fix 1 review closure

### RED/GREEN evidence

1. Docker CLI extraction: RED reported one pinned `FROM` instead of two.
   GREEN proves the same digest in both stages, extractor-only `docker.io`,
   CLI-only copy, final `nftables`, Python 3.10.12, and daemon absence guards.
2. Wrapper portability: RED returned 1 because external `/usr/bin/cd` did not
   establish the copied fixture root. GREEN behaviorally executes the real
   wrapper against a harmless temporary Python boundary and proves inherited
   environment removal.
3. Mount-source closure: RED showed Docker inspect calls for broad, nested,
   dirty, extra, missing, altered, symlinked, and directory inputs. GREEN
   rejects all before Docker mutation and accepts only an exact clean Git
   top-level plus the runtime-lock-bound wheel closure.
4. Lifecycle races: RED lacked the caller-supplied token API; a follow-up RED
   observed two container create calls. GREEN uses token/role labels, explicit
   state-volume creation, inspect-bound ownership, one container create, and
   removes no unproven concurrent claimant.
5. Cleanup failures: RED returned the original start failure for nonzero
   cleanup and skipped volume cleanup after an invocation error. GREEN always
   attempts both owned targets and raises a bounded cleanup-specific error
   chained to the original lifecycle failure.
6. Install identity: RED lacked lifecycle-token validation and accepted direct
   exec. GREEN verifies running state, image/UID/namespaces, capability and
   security closure, exact mounts, container labels, and state-volume labels
   before exec by immutable container ID.
7. Image immutability: RED failed ownership when the desired image-ID label
   appeared. GREEN resolves the requested tag through one unambiguous local
   image inspect before mutation, creates from the exact `sha256` ID, labels
   that ID, and verifies it at install. Malformed/ambiguous IDs fail closed.
- Additional RED/GREEN checks bounded malformed capability input and allowed
  unrelated inherited OCI labels while retaining the exact formal-label
  closure.

### Fix 1 aggregate verification

- `PYTHONPATH=src python3 -m unittest tests/test_txnmem_formal_controller_container.py tests/test_txnmem_formal_controller.py tests/test_txnmem_provenance_execution_collector.py -v`
  — 203 passed, 10 platform-specific skips, 0 failures.
- `PYTHONPYCACHEPREFIX=/private/tmp/txnmem-task-8a-fix1-pycache python3 -m py_compile src/txnmem_formal_controller_container.py src/txnmem_formal_controller.py src/txnmem_provenance_execution_collector.py`
  — passed.
- `bash -n scripts/manage_formal_controller_container.sh scripts/install_formal_provenance_runtime.sh`
  — passed.
- `git diff --check` — passed.

### Fix 1 files and commit

- Updated `infra/formal_controller/Dockerfile`.
- Updated `scripts/manage_formal_controller_container.sh`.
- Updated `src/txnmem_formal_controller_container.py`.
- Expanded `tests/test_txnmem_formal_controller_container.py`.
- Implementation: `81028f9c9cc2cfea2a6209a083630e000ca3593d`.

### Fix 1 remaining remote-only validation

- Build the pinned multi-stage image and exercise build/create/install on the
  approved target host. No remote access, network access, Docker lifecycle,
  smoke run, or formal matrix was performed in this fix.

## Fix 2 no-sudo boundary closure

### Security-boundary decision

- The approved no-sudo controller profile uses an exact allow-list after
  `--cap-drop ALL`: `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `KILL`, `NET_ADMIN`,
  `SETGID`, `SETUID`, and `SYS_PTRACE`.
- This replaces the earlier two-capability draft. The additional capabilities
  are not optional convenience grants: they cover the committed controller's
  ownership changes, post-chown mode enforcement, protected-tree traversal,
  privilege drop, cross-UID child signaling, nft guard, and `/proc` process
  attribution respectively.
- `--privileged`, host-root binds, a host daemon, and sudo remain forbidden.
  The remote smoke must behaviorally prove the exact allow-list is sufficient;
  no formal matrix may start before that smoke succeeds.

### Review findings and closure

1. Named-volume deletion had an inspect/delete TOCTOU because Docker volumes
   have no immutable deletion identifier. The controller now requests one
   anonymous local-driver state volume by omitting its source from the create
   contract. Install verifies both the Engine request (`Source` is empty) and
   the realized 64-hex mount with `Driver=local`; any requested or realized
   custom driver fails closed. Cleanup uses `docker rm -f -v` only
   against the full immutable container ID; no volume name is ever a deletion
   target. This retains block-device-identifiable storage for formal medium
   attestation without reintroducing the named-volume race.
2. Cleanup now converts identity-inspection, removal, and post-removal
   inventory failures to one bounded cleanup error. It never falls back from
   an immutable ID to a reusable name.
3. Docker inspect normalization accepts only the three semantically equivalent
   no-new-privileges forms and exact capability sets with an optional `CAP_`
   prefix. Duplicate, malformed, missing, or extra capabilities still fail
   closed.
4. Direct module execution now invokes Docker with one fixed environment: the
   local Unix socket, an empty protected config location, a nonexistent home,
   and fixed locale. Caller Docker contexts, remote hosts, TLS variables, and
   credential/config paths cannot redirect the boundary.
5. The POSIX wrapper remains environment-empty and uses fixed executable
   paths; hostile `PATH`, `BASH_ENV`, `ENV`, and the retired sentinel cannot
   alter startup.

### Fix 2 RED/GREEN evidence

- The anonymous-state contract tests failed before the create argv included an
  anonymous volume, before install could prove the empty Engine source, and
  before immutable-ID cleanup included anonymous-volume removal.
- The direct-module environment test failed because the old `_run` supplied no
  controlled `env`.
- The real-inspect fixture failed before `CAP_`, `CAP_ALL`, and
  `no-new-privileges=true` normalization was added.
- The Engine-schema fixture failed before omitted JSON zero values were
  normalized: absent anonymous `Source` and writable `ReadOnly` now mean the
  documented empty/false values, while explicit `null` still fails closed.
- The post-delete test failed before cleanup independently proved name absence.
- Focused controller-container tests: 30 passed, 0 failed.
- Combined controller/container/collector/backend tests: 227 passed,
  11 platform-specific skips, 0 failures.
- Full repository suite: 1,215 passed, 19 environment/platform skips,
  0 failures.
- `py_compile`, POSIX `sh -n`, and `git diff --check` passed.

### Fix 2 remaining remote-only validation

- Build the exact reviewed commit on the approved host and inspect the real
  Engine schema.
- Run a disposable protected Linux smoke that exercises install, chown/chmod,
  UID/GID drop, pidfd/signaling, nft, `/proc` attribution, Docker access, and
  zero container residue.
- Only after this gate passes may a fresh formal lifecycle identity be created
  and the 15-cell matrix started exactly once.

## Fix 3 anonymous-volume residue and privilege-channel review iteration

Status at the end of this iteration: reopened by independent review. The
verification counts below remain valid historical evidence, but this iteration
is not by itself eligible for commit, remote smoke, or formal launch.

### Independent-review findings and closure

1. The earlier post-delete check proved only that the reusable controller name
   was absent. It did not independently prove removal of the original
   container or its anonymous state volume. The controller now captures the
   realized 64-hex local-volume identity before deletion, invokes only
   `docker rm -f -v` against the full immutable container ID, then separately
   inventories full non-truncated container IDs and volume names. Cleanup
   succeeds only when both captured identities are absent. It never invokes
   `docker volume rm` by name. Ownership proof also requires the state mount to
   be the container's only volume, so `rm -v` cannot delete an unproven second
   anonymous volume.
2. The Engine's Linux local-volume implementation may report mode `z` for the
   anonymous mount. The realized-mount proof accepts only the equivalent empty,
   `rw`, or `z` modes while retaining exact local-driver, writable, nonempty
   backing-source, and 64-hex anonymous-name requirements.
3. Install verification previously ignored several independent privilege
   channels. The reviewed contract now requires empty legacy binds, device
   mappings, device requests, and supplementary groups; rejects host IPC, UTS,
   user, and cgroup namespaces; and freezes create-time IPC and cgroup
   namespaces to `private`.
4. Runtime mount propagation and Engine `BindOptions` are now closed. Bind
   propagation may only be empty or `rprivate`; recursive/create-mountpoint
   extensions must be false; option families for a different mount type are
   rejected; and the anonymous volume cannot carry bind options or nonempty
   propagation.

### Fix 3 RED/GREEN evidence

- Before implementation, the immutable-ID residue test and surviving-volume
  test both failed because cleanup returned success after checking only the
  reusable container name.
- Before privilege closure, 12 table-driven security/mount mutations were
  accepted, including device requests, host namespaces, legacy binds, and
  shared propagation; the exact create-contract test also lacked private IPC
  and cgroup namespace flags.
- Focused controller-container tests: 32 passed, 0 failed.
- Combined controller/container/collector/backend regression: 219 tests run,
  10 platform-specific skips, 0 failures.
- Full repository suite: 1,216 tests run, 19 environment/platform skips,
  0 failures.
- `py_compile` with an isolated bytecode cache, POSIX `sh -n`, `bash -n`, and
  `git diff --check` passed.

### Fix 3 remaining remote-only validation

- Build the exact reviewed commit on the approved Docker 29 host and inspect
  the real create/inspect schema, including zero-value option normalization.
- Run the disposable no-sudo protected smoke and prove the expected Linux disk
  medium, required controller behaviors, one owned anonymous volume, and zero
  container/volume residue after cleanup.
- A formal v6 identity still does not exist, and no formal matrix has started.
  The smoke remains a hard gate before the 15-cell, 450-repetition,
  14,400-sample run is launched exactly once.

## Fix 4 independent-review closure pending re-review

Status: all four blocking findings from the first independent review have
focused regression coverage and pass locally. This is not an approval claim:
a new read-only reviewer must re-evaluate the complete diff before commit,
push, remote smoke, or formal launch.

### Blocking findings closed in code

1. An unsuccessful or malformed Docker create response can no longer trigger
   ownership lookup or deletion through the reusable container name. Cleanup
   eligibility begins only after create returns a full 64-hex immutable
   container ID and inspection of that exact ID proves all lifecycle labels,
   image identity, name, and anonymous-volume request and realization. If the
   immutable ID is unavailable, the controller returns one bounded cleanup
   error and deliberately leaves any ambiguous resource for manual inspection.
2. The create contract and install-time inspection now require private IPC and
   cgroup namespaces. The runtime is exactly `runc`; host IPC, host UTS, host
   user namespaces, host/empty cgroup namespaces, or a missing/custom runtime
   fail closed.
3. Anonymous-volume ownership now requires both sides of the Engine schema:
   the requested mount has type `volume`, the exact state target, and an
   omitted/empty source with no custom driver or unsafe option family; the
   realized mount is the sole volume, local-driver, writable, and carries the
   Engine-generated 64-hex identity. A caller-supplied 64-hex named volume is
   rejected even if its realized form otherwise resembles an anonymous volume.
4. Independent HostConfig privilege channels are closed: device cgroup rules,
   volumes-from, tmpfs, sysctls, custom runtime, weakened masked/read-only
   system paths, and non-volume mount-option families all fail closed. The
   accepted masked/read-only path sets match the Moby defaults, allowing only
   the Engine's per-CPU thermal-throttle masked paths as dynamic additions.

### Identity scopes and non-reuse proof

- `com.txnmem.formal.lifecycle` is an ephemeral ownership capability for the
  Docker create/start/install/cleanup transaction. It is not cited as formal
  experiment identity and cannot authorize a benchmark run.
- Formal-run authorization is independently bound to the committed
  `run_id` digest and the pre-registered digest of an out-of-tree random nonce.
  The raw nonce is not committed or exposed to the benchmark child.
- Before any formal work begins, `_prepare_formal_run_workspace` derives the
  protected run directory from both digests and creates it atomically. An
  existing identity-derived directory is rejected rather than resumed or
  overwritten. The candidate directory is inode-bound and ownership/mode
  checked before use. Therefore reusing a container lifecycle token cannot
  reuse, replace, or authorize a formal experiment identity.
- No v6 formal run/nonce pair has been registered or generated, and no v6
  matrix process exists. The future identity may be created exactly once only
  after the reviewed remote no-sudo smoke succeeds.

### Focused verification after Fix 4

- The new regressions were observed RED before implementation for malformed
  create-ID fallback, caller-named volume acceptance, privilege-channel
  mutations, omitted namespace/runtime flags, and symlinked wrapper execution.
- Focused controller-container tests: 34 passed, 0 failed.
- Combined controller/container/collector/backend regression: 222 tests run,
  10 platform-specific skips, 0 failures.
- Full repository suite: 1,219 tests run, 19 environment/platform skips,
  0 failures.
- `py_compile`, POSIX `sh -n`, `bash -n`, and `git diff --check` passed.

## Fix 5 mutable wrapper-path review finding

The replacement independent reviewer returned `NOT APPROVE` with one P1 and
one P3. The P1 showed that the shell could already be executing the trusted
wrapper while the invocation symlink in `$0` was retargeted before Python
resolved it. The P3 noted that this report had not recorded the latest related
and full-suite counts; those counts are now included above.

### P1 RED/GREEN closure

- A deterministic regression invokes a wrapper through an attacker-adjacent
  redirect while retaining the trusted repository as the physical working
  directory. Before the fix, the wrapper followed the invocation path and ran
  the attacker-adjacent Python source. The static contract also failed because
  the wrapper contained `$0` and `script_path`. Both tests were observed RED.
- The wrapper no longer reads, resolves, or trusts `$0`. It takes the physical
  working directory from the shell's `pwd -P` builtin and invokes the fixed
  `src/txnmem_formal_controller_container.py` child beneath that root directly
  with isolated Python and an empty environment. Retargeting the invocation
  symlink can no longer select a sibling source tree after the shell has begun
  executing the wrapper.
- The operational contract now requires invoking the wrapper with the exact
  repository top-level as the current directory. Remote build/create/install
  commands must satisfy that contract; the manager's repository checks remain
  fail-closed before Docker mutation.
- The two formerly failing regressions and the environment-sanitization wrapper
  test pass after the change. The complete focused controller-container suite
  passes 34 tests with 0 failures. `py_compile`, POSIX `sh -n`, `bash -n`, and
  `git diff --check` also pass.

### Fix 5 fresh verification

- Focused controller-container tests: 34 passed, 0 failed.
- Combined controller/container/collector/backend regression: 222 tests run,
  10 platform-specific skips, 0 failures.
- Full repository suite: 1,219 tests run, 19 environment/platform skips,
  0 failures.
- `py_compile`, POSIX `sh -n`, `bash -n`, and `git diff --check` passed.

Status at this checkpoint: the second review's findings were implemented and
all local verification had been rerun. A fresh read-only reviewer was still
required to return `APPROVE` before commit or remote smoke; its result follows.

### Final independent re-review

The fresh read-only reviewer returned `APPROVE` with no P0, P1, P2, or P3
findings. It independently confirmed the physical-working-directory wrapper
anchor, immutable-ID-only cleanup, anonymous sole local state volume,
namespace/runtime and HostConfig closure, and the separation between ephemeral
container lifecycle ownership and the run-hash/nonce-hash exclusive formal
workspace. It also confirmed the test counts recorded above.

The reviewer left only remote-smoke assumptions: invoke from the exact physical
repository top-level with protected ancestry; verify Docker 29 inspect
normalization, capability sufficiency, anonymous-volume removal, and zero
residue on the approved host. These are the explicit next gate and do not
authorize a formal v6 launch before the smoke passes.
