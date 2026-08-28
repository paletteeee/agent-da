import json
import unittest

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


if __name__ == "__main__":
    unittest.main()
