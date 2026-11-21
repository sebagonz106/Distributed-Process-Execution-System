"""The `logs` subcommand as a user runs it: flags, output and exit codes.

The gateway is the real one (`make_server`), in front of a stand-in for the
coordinator that serves one task's log (plan/15, D15.7).
"""

import contextlib
import io
import threading
import time
import unittest
from unittest import mock

from dpes import cli
from dpes.gateway import make_server

JOB = "job-1"
TASK = "build"
EID = "job-1/build/0/1"


class FakeCoordinator:
    """Only what the log endpoint needs: a view and an attempt's output."""

    def __init__(self):
        self.views = {}  # job id -> the status view
        self.logs = {}  # execution id -> the output it has stored
        self.reads = []  # (execution id, offset, limit) asked for
        self.follows = []  # (execution id, offset) followed
        self.streams = {}  # execution id -> what its tail yields
        self.broken = False  # the coordinator cannot answer at all

    def status(self, job_id):
        if self.broken:  # Ray raises a plain exception when it cannot reach
            raise RuntimeError("coordinator unreachable")
        return self.views.get(job_id)

    def log_size(self, execution_id):
        return len(self.logs.get(execution_id, b""))

    def read_fragments(self, execution_id, offset, limit=None):
        self.reads.append((execution_id, offset, limit))
        data = self.logs.get(execution_id, b"")
        end = len(data) if limit is None else min(len(data), offset + limit)
        return [(1, data[offset:end])] if offset < end else []

    def tail_fragments(self, execution_id, offset):
        self.follows.append((execution_id, offset))
        yield from self.streams.get(execution_id, lambda start: ())(offset)


class LogsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # One server per class: stopping one waits for its poll interval.
        cls.server = make_server(None, "127.0.0.1", 0)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.coordinator = self.server.coordinator = FakeCoordinator()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.output = b"one\ntwo\nthree\n"
        self.coordinator.logs[EID] = self.output
        self.coordinator.views[JOB] = {
            "job_id": JOB, "state": "RUNNING",
            "tasks": {TASK: {"state": "RUNNING", "execution_id": EID}},
        }

    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["--url", self.url, "logs", *args])
        return code, out.getvalue(), err.getvalue()

    def follow_cli(self, *args):
        """`run_cli` for a follow, waiting until the gateway logged its end.

        The gateway writes the last chunk of the body before it records that
        the follow ended (D15.15), so a test that returns before that record
        exists would leave it to land in the next test's capture."""
        with self.assertLogs("dpes.gateway", "INFO") as caught:
            answer = self.run_cli(*args)
            deadline = time.monotonic() + 5
            while not [r for r in caught.records
                       if r.getMessage() == "log followed"]:
                self.assertLess(time.monotonic(), deadline,
                                "the gateway never logged the end")
                time.sleep(0.02)
        return answer

    def test_the_whole_log_is_printed_and_it_ends_well(self):
        code, out, _ = self.run_cli(JOB, TASK)

        self.assertEqual((0, "one\ntwo\nthree\n"), (code, out))
        self.assertEqual([(EID, 0, None)], self.coordinator.reads)

    def test_the_offset_asks_for_the_log_from_that_byte(self):
        code, out, _ = self.run_cli(JOB, TASK, "-o", "4")

        self.assertEqual((0, "two\nthree\n"), (code, out))
        self.assertEqual([(EID, 4, None)], self.coordinator.reads)

    def test_lines_keeps_the_end_of_the_log(self):
        cases = (("1", "three\n"), ("2", "two\nthree\n"),
                 ("99", "one\ntwo\nthree\n"),  # fewer lines than asked
                 ("0", "one\ntwo\nthree\n"))  # every line
        for lines, expected in cases:
            with self.subTest(lines=lines):
                code, out, _ = self.run_cli(JOB, TASK, "-n", lines)

                self.assertEqual((0, expected), (code, out))

    def test_lines_counts_from_the_offset(self):
        code, out, _ = self.run_cli(JOB, TASK, "-o", "4", "-n", "1")

        self.assertEqual((0, "three\n"), (code, out))

    def test_following_prints_the_log_as_it_comes(self):
        self.coordinator.streams[EID] = lambda offset: [
            (1, b"one\n"), (2, b"two\n")]

        code, out, _ = self.follow_cli(JOB, TASK, "-f")

        self.assertEqual((0, "one\ntwo\n"), (code, out))
        self.assertEqual([(EID, 0)], self.coordinator.follows)
        self.assertEqual([], self.coordinator.reads)

    def test_following_from_an_offset_asks_from_there(self):
        self.coordinator.streams[EID] = lambda offset: [(1, b"two\n")]

        code, out, _ = self.follow_cli(JOB, TASK, "-f", "-o", "4")

        self.assertEqual((0, "two\n"), (code, out))
        self.assertEqual([(EID, 4)], self.coordinator.follows)

    def test_a_character_split_between_fragments_is_printed_whole(self):
        # A fragment boundary may fall inside a UTF-8 character (D15.4).
        self.coordinator.streams[EID] = lambda offset: [
            (1, b"hol"), (2, b"a\n")]

        code, out, _ = self.follow_cli(JOB, TASK, "-f")

        self.assertEqual((0, "hola\n"), (code, out))

    def test_ctrl_c_stops_a_follow_without_an_error(self):
        response = mock.MagicMock()
        response.read1.side_effect = [b"one\n", KeyboardInterrupt]
        opened = mock.MagicMock()
        opened.__enter__.return_value = response

        with mock.patch.object(cli, "_open", return_value=opened):
            code, out, _ = self.run_cli(JOB, TASK, "-f")

        self.assertEqual((0, "one\n"), (code, out))

    def test_an_unknown_job_is_not_found(self):
        code, _, err = self.run_cli("no-such-job", TASK)

        self.assertEqual(cli.EXIT_NOT_FOUND, code)
        self.assertIn("404", err)

    def test_an_unknown_task_is_not_found(self):
        code, _, err = self.run_cli(JOB, "no-such-task")

        self.assertEqual(cli.EXIT_NOT_FOUND, code)
        self.assertIn("unknown task", err)

    def test_a_task_that_never_ran_is_not_found(self):
        self.coordinator.views[JOB]["tasks"][TASK]["execution_id"] = None

        code, _, err = self.run_cli(JOB, TASK)

        self.assertEqual(cli.EXIT_NOT_FOUND, code)
        self.assertIn("has not run", err)

    def test_any_other_refusal_of_the_gateway_is_one(self):
        # A negative offset is the gateway's 400, not a local guess (D15.15).
        code, _, err = self.run_cli(JOB, TASK, "-o", "-1")

        self.assertEqual(cli.EXIT_HTTP_ERROR, code)
        self.assertIn("HTTP 400", err)

    def test_a_gateway_that_cannot_answer_at_all_is_three(self):
        self.url = "http://127.0.0.1:9"  # nothing listens there

        code, _, err = self.run_cli(JOB, TASK)

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("cannot reach", err)

    def test_a_coordinator_that_breaks_is_one(self):
        self.coordinator.broken = True

        code, _, err = self.run_cli(JOB, TASK)

        self.assertEqual(cli.EXIT_HTTP_ERROR, code)
        self.assertIn("HTTP 503", err)

    def test_lines_and_follow_together_are_a_usage_error(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                cli.main(["--url", self.url, "logs", JOB, TASK,
                          "-f", "-n", "3"])

        self.assertEqual(2, caught.exception.code)
        self.assertEqual([], self.coordinator.follows)

    def test_a_negative_line_count_is_a_usage_error(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                cli.main(["--url", self.url, "logs", JOB, TASK, "-n", "-1"])

        self.assertEqual(2, caught.exception.code)
        self.assertEqual([], self.coordinator.reads)


if __name__ == "__main__":
    unittest.main()
