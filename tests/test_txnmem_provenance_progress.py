import json
import math
import os
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import txnmem_provenance_progress as progress
from txnmem_provenance_progress import (
    FORMAL_MATRIX_CELLS,
    FormalProgressState,
    ProgressProtocolError,
    build_progress_event,
    canonical_progress_line,
    decode_progress_line,
)


class TxnMemProvenanceProgressTests(unittest.TestCase):
    def event(self, *, cell=1, repetition=1, completed=1, sequence=1):
        graph_size, concurrency = FORMAL_MATRIX_CELLS[cell - 1]
        return build_progress_event(
            run_binding_sha256="a" * 64,
            config_sha256="b" * 64,
            cell_index=cell,
            graph_size=graph_size,
            concurrency=concurrency,
            repetition_index=repetition,
            completed_repetitions=completed,
            completed_samples=completed * 32,
            update_sequence=sequence,
        )

    def test_valid_progress_is_canonical_and_monotonic(self):
        state = FormalProgressState("a" * 64, "b" * 64)
        first_event = self.event()
        expected_first_line = (
            '{"cell_count":15,"cell_index":1,"completed_repetitions":1,'
            '"completed_samples":32,"concurrency":1,"config_sha256":"'
            + "b" * 64
            + '","graph_size":100,"phase":"measurement","repetition_count":30,'
            '"repetition_index":1,"run_binding_sha256":"'
            + "a" * 64
            + '","schema":"txnmem-provenance-progress-event-v1","status":"running",'
            '"total_repetitions":450,"total_samples":14400,"update_sequence":1}\n'
        ).encode()
        self.assertEqual(canonical_progress_line(first_event), expected_first_line)

        first = state.consume(decode_progress_line(expected_first_line))
        second_event = self.event(repetition=2, completed=2, sequence=2)
        second = state.consume(decode_progress_line(canonical_progress_line(second_event)))
        self.assertEqual(first, first_event)
        self.assertEqual(first["completed_repetitions"], 1)
        self.assertEqual(first["completed_samples"], 32)
        self.assertEqual(second["completed_repetitions"], 2)
        self.assertEqual(second["completed_samples"], 64)

    def test_formal_matrix_has_fifteen_cells_in_required_order(self):
        self.assertEqual(
            FORMAL_MATRIX_CELLS,
            (
                (100, 1),
                (100, 2),
                (100, 4),
                (100, 8),
                (100, 16),
                (1000, 1),
                (1000, 2),
                (1000, 4),
                (1000, 8),
                (1000, 16),
                (10000, 1),
                (10000, 2),
                (10000, 4),
                (10000, 8),
                (10000, 16),
            ),
        )

    def test_full_formal_loop_reaches_the_fixed_terminal_state(self):
        state = FormalProgressState("a" * 64, "b" * 64)
        last = None
        for sequence in range(1, 451):
            cell = (sequence - 1) // 30 + 1
            repetition = (sequence - 1) % 30 + 1
            last = state.consume(
                decode_progress_line(
                    canonical_progress_line(
                        self.event(
                            cell=cell,
                            repetition=repetition,
                            completed=sequence,
                            sequence=sequence,
                        )
                    )
                )
            )
        self.assertEqual(last["cell_index"], 15)
        self.assertEqual(last["repetition_index"], 30)
        self.assertEqual(last["completed_repetitions"], 450)
        self.assertEqual(last["completed_samples"], 14400)
        self.assertEqual(last["update_sequence"], 450)

    def test_unknown_or_missing_fields_are_rejected(self):
        valid = self.event()
        cases = [
            {**valid, "unexpected": 1},
            {key: value for key, value in valid.items() if key != "status"},
        ]
        for candidate in cases:
            with self.subTest(candidate=candidate):
                with self.assertRaises(ProgressProtocolError):
                    canonical_progress_line(candidate)

    def test_malformed_progress_lines_are_rejected(self):
        valid_line = canonical_progress_line(self.event())
        duplicate_key_line = valid_line.replace(
            b'"status":"running"', b'"status":"running","status":"running"'
        )
        nonfinite_line = valid_line.replace(b'"completed_samples":32', b'"completed_samples":NaN')
        noncanonical_spacing = valid_line.replace(b'{"cell_count"', b'{ "cell_count"', 1)
        cases = [
            (duplicate_key_line, "duplicate JSON keys"),
            (nonfinite_line, "non-finite number"),
            (noncanonical_spacing, "non-canonical spacing"),
            (b'{"cell_count":1}\x00\n', "NUL"),
            (b"\n", "empty line"),
            (valid_line[:-1], "missing trailing newline"),
            (b"\xff\n", "invalid UTF-8"),
            (valid_line[:-12] + b"\n", "truncated JSON"),
            (b"{" + b"a" * 4094 + b"}\n", "4097-byte record"),
        ]
        for payload, label in cases:
            with self.subTest(case=label):
                with self.assertRaises(ProgressProtocolError):
                    decode_progress_line(payload)

    def test_non_mapping_json_is_rejected(self):
        for payload in (b"[]\n", b"1\n", b'"text"\n'):
            with self.subTest(payload=payload):
                with self.assertRaises(ProgressProtocolError):
                    decode_progress_line(payload)

    def test_bool_as_int_and_invalid_hashes_are_rejected(self):
        valid = self.event()
        cases = [
            {**valid, "update_sequence": True},
            {**valid, "run_binding_sha256": "A" * 64},
            {**valid, "config_sha256": "b" * 63},
        ]
        for candidate in cases:
            with self.subTest(candidate=candidate):
                with self.assertRaises(ProgressProtocolError):
                    FormalProgressState("a" * 64, "b" * 64).consume(candidate)

    def test_sequence_repetition_and_sample_duplicates_rollbacks_and_jumps_are_rejected(self):
        valid_second = self.event(repetition=2, completed=2, sequence=2)
        cases = [
            {**valid_second, "update_sequence": 1},
            {**valid_second, "repetition_index": 1},
            {**valid_second, "completed_samples": 32},
            {**valid_second, "update_sequence": 0},
            {**valid_second, "update_sequence": 3},
        ]
        for candidate in cases:
            with self.subTest(candidate=candidate):
                state = FormalProgressState("a" * 64, "b" * 64)
                state.consume(self.event())
                with self.assertRaises(ProgressProtocolError):
                    state.consume(candidate)

    def test_wrong_cell_graph_concurrency_early_switch_and_count_overflow_are_rejected(self):
        valid_second = self.event(repetition=2, completed=2, sequence=2)
        cases = [
            {**valid_second, "cell_index": 2},
            {**valid_second, "graph_size": 1000},
            {**valid_second, "concurrency": 2},
            {
                **self.event(cell=2, repetition=1, completed=2, sequence=2),
            },
            {**valid_second, "completed_repetitions": 451},
        ]
        for candidate in cases:
            with self.subTest(candidate=candidate):
                state = FormalProgressState("a" * 64, "b" * 64)
                state.consume(self.event())
                with self.assertRaises(ProgressProtocolError):
                    state.consume(candidate)

    def test_wrong_binding_hashes_and_fixed_counts_are_rejected(self):
        valid = self.event()
        cases = [
            {**valid, "run_binding_sha256": "c" * 64},
            {**valid, "config_sha256": "d" * 64},
            {**valid, "cell_count": 14},
            {**valid, "repetition_count": 29},
            {**valid, "total_repetitions": 449},
            {**valid, "total_samples": 14368},
        ]
        for candidate in cases:
            with self.subTest(candidate=candidate):
                with self.assertRaises(ProgressProtocolError):
                    FormalProgressState("a" * 64, "b" * 64).consume(candidate)

    def test_canonical_progress_line_rejects_noncanonical_key_order(self):
        valid = self.event()
        reordered = dict(reversed(list(valid.items())))
        self.assertEqual(canonical_progress_line(reordered), canonical_progress_line(valid))
        raw = json.dumps(reordered, separators=(",", ":")).encode() + b"\n"
        with self.assertRaises(ProgressProtocolError):
            decode_progress_line(raw)

    def test_consumed_result_is_not_an_internal_alias(self):
        state = FormalProgressState("a" * 64, "b" * 64)
        first = state.consume(self.event())
        first["completed_samples"] = 999
        second = state.consume(self.event(repetition=2, completed=2, sequence=2))
        self.assertEqual(second["completed_samples"], 64)


class TxnMemProgressSnapshotStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "formal-progress.json"
        self.store = progress.ProgressSnapshotStore(
            self.path, expected_uid=os.getuid(), expected_gid=os.getgid()
        )

    def event(self, *, sequence=1):
        graph_size, concurrency = FORMAL_MATRIX_CELLS[(sequence - 1) // 30]
        return build_progress_event(
            run_binding_sha256="a" * 64,
            config_sha256="b" * 64,
            cell_index=(sequence - 1) // 30 + 1,
            graph_size=graph_size,
            concurrency=concurrency,
            repetition_index=(sequence - 1) % 30 + 1,
            completed_repetitions=sequence,
            completed_samples=sequence * 32,
            update_sequence=sequence,
        )

    def test_running_snapshot_is_canonical_private_and_atomically_replaced(self):
        self.store.write_running(self.event())
        first_document = self.path.read_bytes()
        first_stat = self.path.stat()
        self.assertTrue(stat.S_ISREG(first_stat.st_mode))
        self.assertEqual(stat.S_IMODE(first_stat.st_mode), 0o600)
        self.assertEqual(first_document, progress.canonical_snapshot_line(self.store.read_view()))
        self.assertEqual(first_document.count(b"\n"), 1)

        self.store.write_running(self.event(sequence=2))
        second_document = self.path.read_bytes()
        self.assertNotEqual(first_document, second_document)
        self.assertEqual(second_document, progress.canonical_snapshot_line(self.store.read_view()))
        self.assertEqual(self.store.read_view()["update_sequence"], 2)
        self.assertEqual(set(self.store.read_view()), progress.SNAPSHOT_FIELDS)

    def test_replace_failure_keeps_previous_complete_document(self):
        self.store.write_running(self.event())
        previous_document = self.path.read_bytes()
        with mock.patch("txnmem_provenance_progress.os.replace", side_effect=OSError("replace")):
            with self.assertRaises(ProgressProtocolError):
                self.store.write_running(self.event(sequence=2))
        self.assertEqual(self.path.read_bytes(), previous_document)
        self.assertEqual(self.store.read_view()["update_sequence"], 1)

    def test_replace_failure_never_unlinks_a_replaced_temporary_file(self):
        self.store.write_running(self.event())
        temporary_paths: list[Path] = []

        def replace_after_temporary_substitution(source, *_args, **_kwargs):
            temporary_path = self.path.parent / source
            temporary_path.unlink()
            temporary_path.write_bytes(b"unrelated")
            temporary_path.chmod(0o600)
            temporary_paths.append(temporary_path)
            raise OSError("replace")

        with mock.patch("txnmem_provenance_progress.os.replace", replace_after_temporary_substitution):
            with self.assertRaises(ProgressProtocolError):
                self.store.write_running(self.event(sequence=2))
        self.assertEqual(temporary_paths[0].read_bytes(), b"unrelated")

    def test_starting_snapshot_and_read_view_have_only_sanitized_fields(self):
        self.store.write_starting("a" * 64, "b" * 64)
        persisted = json.loads(self.path.read_text())
        view = self.store.read_view()
        self.assertEqual(persisted["last_update_age_seconds"], 0)
        self.assertEqual(view["cell_index"], 1)
        self.assertEqual(view["graph_size"], 100)
        self.assertEqual(view["concurrency"], 1)
        self.assertEqual(view["repetition_index"], 0)
        self.assertEqual(view["completed_repetitions"], 0)
        self.assertEqual(view["completed_samples"], 0)
        self.assertEqual(view["update_sequence"], 0)
        self.assertEqual(set(view), progress.SNAPSHOT_FIELDS)
        self.assertNotIn("timestamp", view)
        self.assertNotIn("path", view)

    def test_read_view_derives_age_from_same_validated_file_metadata(self):
        self.store.write_running(self.event())
        old_mtime = time.time() - 2.2
        os.utime(self.path, (old_mtime, old_mtime))
        view = self.store.read_view()
        self.assertGreaterEqual(view["last_update_age_seconds"], 2)
        self.assertEqual(view["last_update_age_seconds"], math.floor(time.time() - self.path.stat().st_mtime))

    def test_unsafe_snapshot_targets_are_rejected_without_replacement(self):
        cases = []
        symlink_target = Path(self.temporary_directory.name) / "symlink-target.json"
        symlink_target.symlink_to("missing-target.json")
        cases.append((symlink_target, None, "symlink target"))

        nonregular_target = Path(self.temporary_directory.name) / "directory-target.json"
        nonregular_target.mkdir()
        cases.append((nonregular_target, None, "non-regular target"))

        linked_target = Path(self.temporary_directory.name) / "linked-target.json"
        linked_target.write_bytes(b"{}\n")
        linked_target.chmod(0o600)
        os.link(linked_target, Path(self.temporary_directory.name) / "linked-target-copy.json")
        cases.append((linked_target, None, "hard-linked target"))

        wrong_mode_target = Path(self.temporary_directory.name) / "wrong-mode-target.json"
        wrong_mode_target.write_bytes(b"{}\n")
        wrong_mode_target.chmod(0o644)
        cases.append((wrong_mode_target, None, "wrong mode"))

        wrong_owner_target = Path(self.temporary_directory.name) / "wrong-owner-target.json"
        wrong_owner_target.write_bytes(b"{}\n")
        wrong_owner_target.chmod(0o600)
        cases.append((wrong_owner_target, os.getuid() + 1, "wrong owner"))

        for target, expected_uid, label in cases:
            with self.subTest(target=label):
                store = progress.ProgressSnapshotStore(
                    target,
                    expected_uid=os.getuid() if expected_uid is None else expected_uid,
                    expected_gid=os.getgid(),
                )
                with self.assertRaises(ProgressProtocolError):
                    store.write_starting("a" * 64, "b" * 64)

    def test_symlink_parent_and_malformed_persisted_json_are_rejected(self):
        actual_parent = Path(self.temporary_directory.name) / "actual-parent"
        actual_parent.mkdir()
        linked_parent = Path(self.temporary_directory.name) / "linked-parent"
        linked_parent.symlink_to(actual_parent, target_is_directory=True)
        linked_store = progress.ProgressSnapshotStore(
            linked_parent / "snapshot.json", expected_uid=os.getuid(), expected_gid=os.getgid()
        )
        with self.assertRaises(ProgressProtocolError):
            linked_store.write_starting("a" * 64, "b" * 64)

        self.path.write_bytes(b'{"not":"a snapshot"}\n')
        self.path.chmod(0o600)
        with self.assertRaises(ProgressProtocolError):
            self.store.read_view()

    def test_starting_snapshot_rejects_boolean_zero_counts(self):
        self.store.write_starting("a" * 64, "b" * 64)
        malformed = json.loads(self.path.read_text())
        malformed["repetition_index"] = False
        self.path.write_bytes(
            json.dumps(malformed, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        self.path.chmod(0o600)
        with self.assertRaises(ProgressProtocolError):
            self.store.read_view()

    def test_canonical_snapshot_rejects_positive_starting_counts(self):
        snapshot = dict(self.event())
        snapshot["schema"] = progress.PROGRESS_SNAPSHOT_SCHEMA
        snapshot["status"] = "starting"
        snapshot["last_update_age_seconds"] = 0
        with self.assertRaises(ProgressProtocolError):
            progress.canonical_snapshot_line(snapshot)

    def test_persisted_snapshot_rejects_positive_starting_counts(self):
        snapshot = dict(self.event())
        snapshot["schema"] = progress.PROGRESS_SNAPSHOT_SCHEMA
        snapshot["status"] = "starting"
        snapshot["last_update_age_seconds"] = 0
        self.path.write_bytes(
            json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        self.path.chmod(0o600)
        with self.assertRaises(ProgressProtocolError):
            self.store.read_view()

    def test_failed_replace_never_deletes_a_substitution_after_cleanup_inspection(self):
        self.store.write_running(self.event())
        real_stat = os.stat
        temporary_name: list[str] = []
        inspected = threading.Event()

        def substitute_temporary() -> None:
            temporary_path = self.path.parent / temporary_name[0]
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
            temporary_path.write_bytes(b"unrelated")
            temporary_path.chmod(0o600)

        def replace_failure(source, *_args, **_kwargs):
            temporary_name.append(source)
            raise OSError("replace")

        def stat_then_substitute(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if temporary_name and path == temporary_name[0] and not inspected.is_set():
                inspected.set()
                substitute_temporary()
            return result

        with mock.patch("txnmem_provenance_progress.os.replace", replace_failure), mock.patch(
            "txnmem_provenance_progress.os.stat", stat_then_substitute
        ):
            with self.assertRaises(ProgressProtocolError):
                self.store.write_running(self.event(sequence=2))
        if not inspected.is_set():
            substitute_temporary()
        self.assertEqual((self.path.parent / temporary_name[0]).read_bytes(), b"unrelated")

    def test_terminal_transition_serializes_a_concurrent_running_write(self):
        self.store.write_running(self.event())
        entered_terminal_read = threading.Event()
        release_terminal_read = threading.Event()
        terminal_errors: list[BaseException] = []
        running_errors: list[BaseException] = []
        original_read = self.store._read_persisted

        def pause_terminal_read(parent_fd):
            result = original_read(parent_fd)
            if threading.current_thread().name == "terminal-writer" and not entered_terminal_read.is_set():
                entered_terminal_read.set()
                release_terminal_read.wait(2.0)
            return result

        def terminal_writer():
            try:
                self.store.write_terminal("blocked", "backend_timeout")
            except BaseException as exc:
                terminal_errors.append(exc)

        def running_writer():
            try:
                self.store.write_running(self.event(sequence=2))
            except BaseException as exc:
                running_errors.append(exc)

        with mock.patch.object(self.store, "_read_persisted", side_effect=pause_terminal_read):
            terminal_thread = threading.Thread(target=terminal_writer, name="terminal-writer")
            terminal_thread.start()
            self.assertTrue(entered_terminal_read.wait(2.0))
            running_thread = threading.Thread(target=running_writer, name="running-writer")
            running_thread.start()
            time.sleep(0.05)
            release_terminal_read.set()
            terminal_thread.join(2.0)
            running_thread.join(2.0)

        self.assertFalse(terminal_thread.is_alive())
        self.assertFalse(running_thread.is_alive())
        self.assertEqual(terminal_errors, [])
        self.assertEqual(len(running_errors), 1)
        self.assertIsInstance(running_errors[0], ProgressProtocolError)
        view = self.store.read_view()
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["update_sequence"], 1)

    def test_terminal_status_and_reason_are_restricted(self):
        self.store.write_starting("a" * 64, "b" * 64)
        for status, reason in (
            ("failed", "formal_eligibility_failed"),
            ("completed", "raw_exception"),
        ):
            with self.subTest(status=status, reason=reason):
                with self.assertRaises(ProgressProtocolError):
                    self.store.write_terminal(status, reason)
        self.store.write_terminal("blocked", "formal_eligibility_failed")
        view = self.store.read_view()
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["terminal_reason_class"], "formal_eligibility_failed")


class TxnMemProgressPipeDrainerTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.reader, self.writer = os.pipe()
        self.addCleanup(self._close_writer)
        self.addCleanup(self._close_reader)
        self.store = progress.ProgressSnapshotStore(
            Path(self.temporary_directory.name) / "formal-progress.json",
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )
        self.state = FormalProgressState("a" * 64, "b" * 64)

    def _close_reader(self):
        try:
            os.close(self.reader)
        except OSError:
            pass

    def _close_writer(self):
        try:
            os.close(self.writer)
        except OSError:
            pass

    def event(self, sequence):
        graph_size, concurrency = FORMAL_MATRIX_CELLS[(sequence - 1) // 30]
        return build_progress_event(
            run_binding_sha256="a" * 64,
            config_sha256="b" * 64,
            cell_index=(sequence - 1) // 30 + 1,
            graph_size=graph_size,
            concurrency=concurrency,
            repetition_index=(sequence - 1) % 30 + 1,
            completed_repetitions=sequence,
            completed_samples=sequence * 32,
            update_sequence=sequence,
        )

    def _drainer(self):
        drainer = progress.ProgressPipeDrainer(self.reader, self.state, self.store)
        drainer.start()
        return drainer

    def _write_and_close(self, payload):
        os.write(self.writer, payload)
        self._close_writer()

    def test_drains_two_real_pipe_records_and_returns_the_second_snapshot(self):
        drainer = self._drainer()
        self._write_and_close(
            canonical_progress_line(self.event(1)) + canonical_progress_line(self.event(2))
        )
        snapshot = drainer.finish(2.0)
        self.assertEqual(snapshot["update_sequence"], 2)
        self.assertIsNone(drainer.failure)
        self.assertFalse(drainer.thread.is_alive())

    def test_malformed_partial_oversized_and_empty_pipe_failures_are_retained(self):
        cases = (
            (b"{malformed}\n", "malformed"),
            (canonical_progress_line(self.event(1))[:-1], "partial"),
            (b"x" * 4096 + b"\n", "oversized"),
            (b"", "no events"),
        )
        for payload, label in cases:
            with self.subTest(case=label):
                reader, writer = os.pipe()
                store = progress.ProgressSnapshotStore(
                    Path(self.temporary_directory.name) / (label + ".json"),
                    expected_uid=os.getuid(),
                    expected_gid=os.getgid(),
                )
                drainer = progress.ProgressPipeDrainer(
                    reader, FormalProgressState("a" * 64, "b" * 64), store
                )
                drainer.start()
                if payload:
                    os.write(writer, payload)
                os.close(writer)
                with self.assertRaises(ProgressProtocolError):
                    drainer.finish(2.0)
                self.assertIsInstance(drainer.failure, ProgressProtocolError)
                self.assertFalse(drainer.thread.is_alive())

    def test_abort_closes_the_reader_once_and_stops_the_daemon_thread(self):
        drainer = self._drainer()
        drainer.abort()
        drainer.abort()
        self._close_writer()
        self.assertIsNone(drainer.finish(2.0))
        self.assertIsNone(drainer.failure)
        self.assertFalse(drainer.thread.is_alive())

    def test_unexpected_store_exception_is_sanitized_and_fails_closed(self):
        drainer = progress.ProgressPipeDrainer(self.reader, self.state, self.store)
        with mock.patch.object(self.store, "write_running", side_effect=ValueError()):
            drainer.start()
            self._write_and_close(canonical_progress_line(self.event(1)))
            with self.assertRaises(ProgressProtocolError) as raised:
                drainer.finish(2.0)
        self.assertIs(drainer.failure, raised.exception)
        self.assertEqual(str(raised.exception), "progress drainer worker failed")
        self.assertFalse(drainer.thread.is_alive())

    def test_start_failure_closes_descriptor_and_retains_sanitized_failure(self):
        drainer = progress.ProgressPipeDrainer(self.reader, self.state, self.store)
        with mock.patch("txnmem_provenance_progress.threading.Thread.start", side_effect=RuntimeError()):
            with self.assertRaises(ProgressProtocolError) as raised:
                drainer.start()
        self.assertIs(drainer.failure, raised.exception)
        self.assertEqual(str(raised.exception), "progress drainer could not start")
        with self.assertRaises(ProgressProtocolError):
            _ = drainer.thread
        with self.assertRaises(OSError):
            os.fstat(self.reader)

    def test_finish_timeout_is_bounded_then_daemon_exits_after_blocked_store_releases(self):
        entered_store = threading.Event()
        release_store = threading.Event()
        original_write_running = self.store.write_running

        def blocked_write_running(event):
            entered_store.set()
            self.assertTrue(release_store.wait(2.0))
            return original_write_running(event)

        drainer = progress.ProgressPipeDrainer(self.reader, self.state, self.store)
        with mock.patch.object(self.store, "write_running", side_effect=blocked_write_running):
            drainer.start()
            os.write(self.writer, canonical_progress_line(self.event(1)))
            self.assertTrue(entered_store.wait(2.0))
            started = time.monotonic()
            with self.assertRaisesRegex(ProgressProtocolError, "progress drainer did not stop"):
                drainer.finish(0.1)
            self.assertLess(time.monotonic() - started, 0.5)
            with self.assertRaises(OSError):
                os.fstat(self.reader)
            self.assertTrue(drainer.thread.is_alive())
            release_store.set()
            self._close_writer()
            snapshot = drainer.finish(2.0)

        self.assertEqual(snapshot["update_sequence"], 1)
        self.assertFalse(drainer.thread.is_alive())


if __name__ == "__main__":
    unittest.main()
