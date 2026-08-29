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
