"""Cut C4: a task's log over HTTP, whole, by offset and as it grows.

Runs inside the control node, where the gateway listens (plan/15, D15.7):

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import os
import subprocess
import sys
import time
import unittest

from gateway_client import (URL, cli, events, log_size, submit, wait,
                            wait_for_gateway)

from dpes.executor import MAX_CAPTURE_BYTES

CLI = [sys.executable, "-m", "dpes.cli"]


def read_until(process, marker, timeout_s):
    """What the process printed until `marker`, and whether it still ran."""
    os.set_blocking(process.stdout.fileno(), False)  # read what is there
    deadline = time.monotonic() + timeout_s
    seen = b""
    while marker not in seen:
        chunk = process.stdout.read()  # None: nothing yet, b"": the end
        if chunk:
            seen += chunk
        elif chunk == b"":
            raise AssertionError(f"the stream ended early: {seen!r}")
        elif time.monotonic() > deadline:
            raise AssertionError(f"{marker!r} never came: {seen!r}")
        else:
            time.sleep(0.05)
    return seen, process.poll() is None


def read_rest(process, timeout_s):
    """Everything else the process prints, until it closes its output."""
    deadline = time.monotonic() + timeout_s
    seen = b""
    while True:
        chunk = process.stdout.read()
        if chunk:
            seen += chunk
        elif chunk == b"":
            return seen
        elif time.monotonic() > deadline:
            raise AssertionError(f"the log never ended: {seen!r}")
        else:
            time.sleep(0.05)


class LogsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def run_job(self, tasks, expected_code=0):
        # These tests watch the log of an attempt, so no task may be served
        # from the content cache instead of running (plan/16, F9.5).
        tasks = {name: {"cache": False, **task}
                 for name, task in tasks.items()}
        code, job_id, err = submit(tasks)
        self.assertEqual(0, code, err)
        code, view = wait(job_id)
        self.assertEqual(expected_code, code)
        return view

    def test_the_log_of_a_finished_attempt_is_served_whole(self):
        view = self.run_job({"t": {"command": ["sh", "-c",
                                              "echo one; echo two"]}})

        code, out, err = cli("logs", view["job_id"], "t")

        self.assertEqual((0, "one\ntwo\n"), (code, out), err)

    def test_the_offset_resumes_at_that_byte(self):
        view = self.run_job({"t": {"command": ["sh", "-c",
                                              "echo one; echo two"]}})

        code, out, err = cli("logs", view["job_id"], "t", "-o", "4")

        self.assertEqual((0, "two\n"), (code, out), err)

    def test_lines_keeps_the_end_of_the_log(self):
        view = self.run_job({"t": {"command": ["sh", "-c",
                                              "echo one; echo two"]}})

        code, out, err = cli("logs", view["job_id"], "t", "-n", "1")

        self.assertEqual((0, "two\n"), (code, out), err)

    def test_the_log_of_a_failed_task_is_served_too(self):
        view = self.run_job({"t": {"command": ["sh", "-c",
                                              "echo boom; exit 3"]}},
                            expected_code=1)

        code, out, err = cli("logs", view["job_id"], "t")

        self.assertEqual((0, "boom\n"), (code, out), err)

    def test_a_runaway_command_stops_at_the_log_limit(self):
        # plan/15, D15.13: the worker kills it at its capture ceiling, so the
        # attempt fails there instead of growing the disk without bound.
        began = time.monotonic()

        view = self.run_job(
            {"t": {"command": ["sh", "-c", "while :; do echo spam; done"]}},
            expected_code=1,
        )

        task = view["tasks"]["t"]
        self.assertTrue(task["truncated"])
        self.assertEqual("TASK", task["failure_class"])
        self.assertIn("output over the log limit", task["error"])
        self.assertLess(time.monotonic() - began, 120)  # not the deadline
        size = log_size(view["job_id"], "t")
        self.assertGreater(size, 0)  # what it wrote before being killed
        self.assertLessEqual(size, MAX_CAPTURE_BYTES)  # is all, and it is held
        code, out, err = cli("logs", view["job_id"], "t")
        self.assertEqual(0, code, err)
        self.assertEqual("spam\n", out.splitlines(True)[-1])

    def test_following_prints_while_the_task_still_runs(self):
        code, job_id, err = submit({"t": {"command": [
            "sh", "-c", "echo first; sleep 6; echo second"], "cache": False}})
        self.assertEqual(0, code, err)
        process = subprocess.Popen(
            CLI + ["--url", URL, "logs", job_id.strip(), "t", "-f"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.kill)

        first, alive = read_until(process, b"first\n", timeout_s=30)
        rest = read_rest(process, timeout_s=60)

        self.assertEqual(0, process.wait(timeout=30),
                         process.stderr.read())
        # The proof that it is live: the first line came out before the end.
        self.assertTrue(alive, f"printed only after the job ended: {first}")
        self.assertIn(b"second\n", first + rest)

    def test_a_job_or_task_that_does_not_exist_is_not_found(self):
        view = self.run_job({"t": {"command": ["true"]}})

        for args in (("logs", "no-such-job", "t"),
                     ("logs", view["job_id"], "no-such-task")):
            with self.subTest(args=args):
                code, _, err = cli(*args)

                self.assertEqual(2, code, err)
                self.assertIn("404", err)

    def test_a_refused_request_is_one(self):
        view = self.run_job({"t": {"command": ["true"]}})

        code, _, err = cli("logs", view["job_id"], "t", "-o", "-1")

        self.assertEqual(1, code, err)
        self.assertIn("HTTP 400", err)

    def test_a_gateway_that_is_not_there_is_three(self):
        # Nothing listens on that port: no answer at all, so no HTTP code.
        code, _, err = cli("logs", "any", "t", url="http://127.0.0.1:9")

        self.assertEqual(3, code, err)
        self.assertIn("cannot reach", err)

    def test_the_log_outlives_the_executor_that_wrote_it(self):
        # The process prints, saves its result and dies before answering (R7):
        # what it wrote sits in the control node, so the log is still served.
        view = self.run_job({"t": {
            "command": ["sh", "-c", "echo before"],
            "env": {"DPES_FAULT": "lose-ack:1"},
        }})

        code, out, err = cli("logs", view["job_id"], "t", "-o", "0")

        self.assertEqual((0, "before\n"), (code, out), err)
        self.assertEqual(1, len(view["tasks"]["t"]["attempts"]))

    def test_the_feed_tells_the_history_of_a_retried_task(self):
        view = self.run_job({"t": {
            "command": ["sh", "-c", "echo ran"],
            "env": {"DPES_FAULT": "network:1"},
        }})

        feed = events(view["job_id"])

        self.assertEqual(
            ["TASK_LAUNCHED", "ATTEMPT_FAILED", "RETRY_SCHEDULED",
             "ATTEMPT_SUCCEEDED", "JOB_FINISHED"],
            [event["type"] for event in feed],
        )
        failed, retry, again = feed[1], feed[2], feed[3]
        self.assertEqual("NETWORK", failed["payload"]["failure_class"])
        self.assertEqual("RETRY_WAIT", failed["payload"]["state"])
        self.assertGreater(retry["payload"]["retry_in_s"], 0)
        self.assertEqual(retry["payload"]["next_execution_id"],
                         again["execution_id"])
        end = feed[-1]
        self.assertEqual((None, ""), (end["task"], end["execution_id"]))
        self.assertEqual({"state": "SUCCEEDED", "counts": {"succeeded": 1}},
                         end["payload"])
        # Ordered as it happened, in ISO 8601 UTC with milliseconds (D15.5).
        stamps = [event["timestamp"] for event in feed]
        self.assertEqual(sorted(stamps), stamps)
        self.assertRegex(stamps[0],
                         r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
        self.assertEqual({view["job_id"]}, {event["job_id"] for event in feed})


if __name__ == "__main__":
    unittest.main()
