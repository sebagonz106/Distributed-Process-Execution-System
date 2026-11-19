"""The log store of plan/15, D15.2 and D15.11. No Ray: a temporary folder."""

import hashlib
import io
import os
import struct
import tempfile
import time
import unittest
from unittest import mock

from dpes.logstore import LogDir, tail

EID = "job-1/compile/0/1"
OTHER = "job-1/link/0/1"


class _Short:
    """A file whose `write` leaves out the last byte, as a full disk can."""

    def __init__(self, real):
        self._real = real

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._real.close()

    def write(self, data):
        return self._real.write(data[:-1])

    def truncate(self, size):
        return self._real.truncate(size)


async def aclose(stream):
    """Let an unfinished tail go, as a failed test would otherwise leave it."""
    await stream.aclose()


class LogDirTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = directory.name
        self.logs = LogDir(self.root)

    def reopen(self):
        return LogDir(self.root)

    def path(self, execution_id):
        name = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
        return os.path.join(self.root, name + ".log")

    def test_a_written_fragment_comes_back(self):
        self.assertTrue(self.logs.append(EID, 1, b"hola\n"))

        self.assertEqual([(1, b"hola\n")], self.logs.read(EID, 0))

    def test_fragments_keep_the_order_they_were_written_in(self):
        for seq, data in ((1, b"one\n"), (2, b"two\n"), (3, b"three\n")):
            self.logs.append(EID, seq, data)

        self.assertEqual(
            [(1, b"one\n"), (2, b"two\n"), (3, b"three\n")],
            self.logs.read(EID, 0),
        )

    def test_a_duplicate_seq_is_ignored(self):
        self.logs.append(EID, 1, b"first\n")

        self.assertFalse(self.logs.append(EID, 1, b"again\n"))

        self.assertEqual([(1, b"first\n")], self.logs.read(EID, 0))

    def test_a_seq_that_was_already_passed_is_ignored(self):
        self.logs.append(EID, 3, b"three\n")

        self.assertFalse(self.logs.append(EID, 2, b"two\n"))

        self.assertEqual([(3, b"three\n")], self.logs.read(EID, 0))

    def test_attempts_do_not_share_fragments(self):
        self.logs.append(EID, 1, b"first\n")
        self.logs.append(OTHER, 1, b"second\n")

        self.assertEqual([(1, b"first\n")], self.logs.read(EID, 0))
        self.assertEqual([(1, b"second\n")], self.logs.read(OTHER, 0))

    def test_an_attempt_that_never_wrote_reads_nothing(self):
        self.assertEqual([], self.logs.read(EID, 0))
        self.assertFalse(os.path.exists(self.path(EID)))

    def test_a_byte_offset_slices_the_fragment_it_falls_in(self):
        self.logs.append(EID, 1, b"hello\n")
        self.logs.append(EID, 2, b"world\n")

        self.assertEqual([(1, b"lo\n"), (2, b"world\n")],
                         self.logs.read(EID, 3))

    def test_a_limit_stops_mid_fragment(self):
        self.logs.append(EID, 1, b"hello\n")
        self.logs.append(EID, 2, b"world\n")

        self.assertEqual([(1, b"lo\n"), (2, b"wor")],
                         self.logs.read(EID, 3, 6))

    def test_an_offset_at_the_end_or_past_it_reads_nothing(self):
        self.logs.append(EID, 1, b"hola\n")

        self.assertEqual([], self.logs.read(EID, 5))
        self.assertEqual([], self.logs.read(EID, 99))

    def test_a_limit_of_zero_reads_nothing(self):
        self.logs.append(EID, 1, b"hola\n")

        self.assertEqual([], self.logs.read(EID, 0, 0))

    def test_negative_arguments_are_refused(self):
        with self.assertRaises(ValueError):
            self.logs.read(EID, -1)
        with self.assertRaises(ValueError):
            self.logs.read(EID, 0, -1)

    def test_an_empty_fragment_is_refused(self):
        with self.assertRaises(ValueError):
            self.logs.append(EID, 1, b"")

        self.assertEqual([], self.logs.read(EID, 0))

    def test_a_seq_below_one_is_refused(self):
        with self.assertRaises(ValueError):
            self.logs.append(EID, 0, b"hola\n")

        self.assertEqual([], self.logs.read(EID, 0))

    def test_fragments_survive_reopening(self):
        self.logs.append(EID, 1, b"one\n")
        self.logs.append(EID, 2, b"two\n")

        reopened = self.reopen()

        self.assertEqual([(1, b"one\n"), (2, b"two\n")],
                         reopened.read(EID, 0))

    def test_a_reopened_store_keeps_numbering_where_it_stopped(self):
        self.logs.append(EID, 1, b"one\n")

        reopened = self.reopen()

        self.assertFalse(reopened.append(EID, 1, b"again\n"))
        self.assertTrue(reopened.append(EID, 2, b"two\n"))
        self.assertEqual([(1, b"one\n"), (2, b"two\n")],
                         self.reopen().read(EID, 0))

    def test_a_half_written_fragment_is_dropped_on_reopening(self):
        self.logs.append(EID, 1, b"one\n")
        with open(self.path(EID), "ab") as f:  # seq 2 announces 5 bytes, has 2
            f.write(b"\x02" + b"\x00" * 7 + b"\x05\x00\x00\x00" + b"ab")

        reopened = self.reopen()

        self.assertEqual([(1, b"one\n")], reopened.read(EID, 0))
        self.assertTrue(reopened.append(EID, 2, b"two\n"))
        self.assertEqual([(1, b"one\n"), (2, b"two\n")],
                         self.reopen().read(EID, 0))

    def test_a_file_headed_by_another_attempt_is_refused(self):
        self.logs.append(EID, 1, b"hola\n")
        os.replace(self.path(EID), self.path(OTHER))

        with self.assertRaises(ValueError):
            self.logs.read(OTHER, 0)

    def test_a_creation_cut_short_starts_over(self):
        with open(self.path(EID), "wb") as f:
            f.write(b"DPES\x01\x00")  # half a header, as a full disk leaves

        logs = self.reopen()

        self.assertEqual([], logs.read(EID, 0))
        self.assertTrue(logs.append(EID, 1, b"hola\n"))
        self.assertEqual([(1, b"hola\n")], self.reopen().read(EID, 0))

    def test_a_header_cut_inside_the_execution_id_starts_over(self):
        head = struct.pack("<4sBH", b"DPES", 1, len(EID))
        with open(self.path(EID), "wb") as f:
            f.write(head + EID.encode("utf-8")[:4])

        logs = self.reopen()

        self.assertEqual([], logs.read(EID, 0))
        self.assertTrue(logs.append(EID, 1, b"hola\n"))
        self.assertEqual([(1, b"hola\n")], self.reopen().read(EID, 0))

    def test_a_foreign_file_is_refused(self):
        with open(self.path(EID), "wb") as f:
            f.write(struct.pack("<4sBH", b"NOPE", 1, 0))

        with self.assertRaises(ValueError):
            self.reopen().read(EID, 0)

    def test_a_short_write_is_an_error_and_leaves_no_trace(self):
        self.logs.append(EID, 1, b"one\n")

        with mock.patch("builtins.open", self.short_open):
            with self.assertRaises(OSError):
                self.logs.append(EID, 2, b"two\n")

        # The index still matches the file, so the fragment can be resent.
        self.assertEqual([(1, b"one\n")], self.logs.read(EID, 0))
        self.assertTrue(self.logs.append(EID, 2, b"two\n"))
        self.assertEqual([(1, b"one\n"), (2, b"two\n")],
                         self.reopen().read(EID, 0))

    def test_a_log_ends_when_it_is_closed(self):
        self.logs.append(EID, 1, b"one\n")

        self.assertFalse(self.logs.finished(EID))
        self.assertTrue(self.logs.close(EID))
        self.assertTrue(self.logs.finished(EID))

    def test_closing_twice_says_it_was_already_closed(self):
        self.assertTrue(self.logs.close(EID))

        self.assertFalse(self.logs.close(EID))

    def test_a_closed_log_still_serves_its_output(self):
        self.logs.append(EID, 1, b"one\n")
        self.logs.close(EID)

        self.assertEqual([(1, b"one\n")], self.reopen().read(EID, 0))

    def test_no_fragment_is_taken_after_the_end(self):
        self.logs.close(EID)

        with self.assertRaises(ValueError):
            self.logs.append(EID, 1, b"late\n")

    def test_the_end_survives_reopening(self):
        self.logs.append(EID, 1, b"one\n")
        self.logs.close(EID)

        self.assertTrue(self.reopen().finished(EID))

    def test_a_log_closed_without_output_keeps_its_header(self):
        self.assertTrue(self.logs.close(EID))

        self.assertEqual([], self.reopen().read(EID, 0))
        self.assertTrue(self.reopen().finished(EID))

    def test_an_end_marker_cut_short_does_not_count(self):
        self.logs.append(EID, 1, b"one\n")
        with open(self.path(EID), "ab") as f:
            f.write(struct.pack("<QI", 0, 0)[:5])  # as a crash leaves it

        logs = self.reopen()

        self.assertFalse(logs.finished(EID))
        self.assertEqual([(1, b"one\n")], logs.read(EID, 0))

    def short_open(self, path, mode="r", *args, **kwargs):
        """`open`, but writes into the file stop one byte short."""
        real = io.open(path, mode, *args, **kwargs)  # the unpatched one
        return _Short(real) if "a" in mode or "w" in mode else real


class TailTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.logs = LogDir(directory.name)

    async def test_tail_yields_the_fragments_already_there(self):
        self.logs.append(EID, 1, b"one\n")
        self.logs.append(EID, 2, b"two\n")

        self.assertEqual(
            [(1, b"one\n"), (2, b"two\n")],
            [piece async for piece in tail(self.logs, EID, 0,
                                           poll_s=0.01, idle_s=0.05)],
        )

    async def test_tail_yields_what_arrives_while_it_waits(self):
        self.logs.append(EID, 1, b"one\n")
        stream = tail(self.logs, EID, 0, poll_s=0.01, idle_s=5)
        self.addAsyncCleanup(aclose, stream)

        self.assertEqual((1, b"one\n"), await anext(stream))
        self.logs.append(EID, 2, b"two\n")

        self.assertEqual((2, b"two\n"), await anext(stream))

    async def test_tail_resumes_a_byte_offset(self):
        self.logs.append(EID, 1, b"hello\n")
        self.logs.append(EID, 2, b"world\n")
        stream = tail(self.logs, EID, 3, poll_s=0.01, idle_s=5)
        self.addAsyncCleanup(aclose, stream)

        self.assertEqual((1, b"lo\n"), await anext(stream))
        self.assertEqual((2, b"world\n"), await anext(stream))

    async def test_tail_waits_for_an_attempt_with_no_fragments_yet(self):
        stream = tail(self.logs, EID, 0, poll_s=0.01, idle_s=5)
        self.addAsyncCleanup(aclose, stream)

        self.logs.append(EID, 1, b"late\n")

        self.assertEqual((1, b"late\n"), await anext(stream))

    async def test_tail_ends_after_an_idle_stretch(self):
        started = time.monotonic()

        pieces = [piece async for piece in tail(self.logs, EID, 0,
                                                poll_s=0.01, idle_s=0.05)]

        self.assertEqual([], pieces)
        self.assertGreaterEqual(time.monotonic() - started, 0.05)

    async def test_tail_ends_when_the_log_is_closed(self):
        self.logs.append(EID, 1, b"one\n")
        self.logs.close(EID)
        started = time.monotonic()

        pieces = [piece async for piece in tail(self.logs, EID, 0,
                                                poll_s=0.01, idle_s=60)]

        self.assertEqual([(1, b"one\n")], pieces)
        self.assertLess(time.monotonic() - started, 5)  # not the 60 s limit

    async def test_tail_stops_waiting_once_the_attempt_ends(self):
        stream = tail(self.logs, EID, 0, poll_s=0.01, idle_s=60)
        self.addAsyncCleanup(aclose, stream)

        self.logs.close(EID)

        self.assertEqual([], [piece async for piece in stream])


if __name__ == "__main__":
    unittest.main()
