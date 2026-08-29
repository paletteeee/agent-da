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
