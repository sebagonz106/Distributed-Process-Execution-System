"""The executor of plan/07, D07.17. Linux only, no Ray needed."""

import os
import sys
import unittest
from unittest import mock

if sys.platform != "linux":
    raise unittest.SkipTest("the executor needs Linux process groups")

from dpes.executor import execute  # imported after the skip: it needs Linux
from dpes.log import Formatter
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult


def attempt(script, **changes):
    fields = {
        "job_id": "job-1",
        "task": "t",
        "round": 0,
        "execution_id": "job-1/t/0/1",
        "command": ("sh", "-c", script),
        "env": {},
        "inputs": {},
        "outputs": (),
        "timeout_s": 10.0,
    }
    return Attempt(**{**fields, **changes})


class ExecuteTest(unittest.TestCase):
    def test_a_finished_command_is_a_result_of_this_attempt(self):
        result = execute(attempt("echo hi; exit 4"))

        self.assertIsInstance(result, TaskResult)
        self.assertEqual("job-1/t/0/1", result.execution_id)
        self.assertEqual((4, "hi\n"), (result.exit_code, result.output_tail))

    def test_the_declared_environment_reaches_the_command(self):
        result = execute(attempt("echo $NAME", env={"NAME": "value"}))

        self.assertEqual("value\n", result.output_tail)

    def test_every_attempt_starts_in_an_empty_workspace(self):
        execute(attempt("touch leftover"))

        self.assertEqual("", execute(attempt("ls -A")).output_tail)

    def test_the_workspace_is_destroyed_afterwards(self):
        workspace = execute(attempt("pwd")).output_tail.strip()

        self.assertFalse(os.path.exists(workspace))

    def test_no_workspace_is_reported_as_executor_failure(self):
        with mock.patch("tempfile.tempdir", "/nonexistent/dpes"):
            outcome = execute(attempt("true"))

        self.assertIsInstance(outcome, ExecutorFailure)
        self.assertEqual(Evidence.WORKSPACE_FAILED, outcome.evidence)


class ExecutorLogTest(unittest.TestCase):
    def logged(self, level, run):
        with self.assertLogs("dpes.executor", level) as caught:
            run()
        return caught.records

    def test_an_attempt_logs_where_it_ran_and_how_it_ended(self):
        records = self.logged("INFO", lambda: execute(attempt("exit 4")))

        running, finished = records
        self.assertEqual("attempt running", running.getMessage())
        self.assertEqual("job-1/t/0/1", running.fields["execution"])
        self.assertIn("host", running.fields)
        self.assertEqual("command finished", finished.getMessage())
        self.assertEqual(4, finished.fields["exit_code"])
        self.assertEqual("job-1/t/0/1", finished.fields["execution"])

    def test_neither_the_environment_nor_the_output_reaches_the_log(self):
        secret = attempt("echo printed-output", env={"TOKEN": "hunter2"})

        records = self.logged("DEBUG", lambda: execute(secret))

        text = "\n".join(Formatter().format(r) for r in records)
        self.assertNotIn("hunter2", text)
        self.assertNotIn("printed-output", text)

    def test_no_workspace_is_an_error(self):
        with mock.patch("tempfile.tempdir", "/nonexistent/dpes"):
            records = self.logged("ERROR", lambda: execute(attempt("true")))

        self.assertEqual("no workspace", records[0].getMessage())

    @mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "1"})
    def test_an_injected_fault_is_a_warning(self):
        faulty = attempt("true", env={"DPES_FAULT": "network:1"})

        records = self.logged("WARNING", lambda: execute(faulty))

        self.assertEqual(("injected fault", "network"),
                         (records[0].getMessage(), records[0].fields["kind"]))


class Stopped(Exception):
    """Stands in for exiting or hanging, which would stop the test run."""


@mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "1"})
class InjectedFaultTest(unittest.TestCase):
    def numbered(self, number, fault):
        return attempt("echo ran", env={"DPES_FAULT": fault},
                       execution_id=f"job-1/t/0/{number}")

    def test_network_fails_the_first_attempts_then_lets_them_run(self):
        outcomes = [execute(self.numbered(n, "network:2")) for n in (1, 2, 3)]

        self.assertEqual(
            [Evidence.INPUT_UNAVAILABLE, Evidence.INPUT_UNAVAILABLE],
            [outcome.evidence for outcome in outcomes[:2]],
        )
        self.assertEqual("ran\n", outcomes[2].output_tail)

    def test_crash_ends_the_executor_process(self):
        with mock.patch("os._exit", side_effect=Stopped) as exit_:
            with self.assertRaises(Stopped):
                execute(self.numbered(1, "crash:1"))
        exit_.assert_called_once()

    def test_hang_blocks_before_running_the_command(self):
        # Only the executor's own wait: subprocess also sleeps internally.
        with mock.patch("dpes.executor._hang", side_effect=Stopped) as hang:
            with mock.patch("dpes.sandbox.run") as run:
                with self.assertRaises(Stopped):
                    execute(self.numbered(1, "hang:1"))
        hang.assert_called_once()
        run.assert_not_called()

    def test_a_malformed_fault_is_ignored(self):
        for fault in ("crash", "crash:x", "explode:1", ""):
            with self.subTest(fault=fault):
                outcome = execute(self.numbered(1, fault))
                self.assertEqual("ran\n", outcome.output_tail)


class FaultsDisabledTest(unittest.TestCase):
    def test_without_the_cluster_switch_a_fault_is_ignored(self):
        with mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "0"}):
            outcome = execute(attempt("echo ran",
                                      env={"DPES_FAULT": "network:9"}))

        self.assertEqual("ran\n", outcome.output_tail)


if __name__ == "__main__":
    unittest.main()
