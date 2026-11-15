"""The executor of plan/07, D07.17. Linux only, no Ray needed."""

import os
import sys
import unittest
from unittest import mock

if sys.platform != "linux":
    raise unittest.SkipTest("the executor needs Linux process groups")

from dpes.executor import execute  # imported after the skip: it needs Linux
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


if __name__ == "__main__":
    unittest.main()
