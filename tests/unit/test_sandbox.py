"""The sandbox of plan/07, D07.15 and D07.16. Linux only."""

import os
import shutil
import sys
import tempfile
import time
import unittest

if sys.platform != "linux":
    raise unittest.SkipTest("the sandbox needs Linux process groups")

from dpes import sandbox  # imported after the skip: it needs Linux


def gone(pid, wait_s=2.0):
    """True once `pid` no longer exists or is a zombie nobody reaped yet."""
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        try:
            with open(f"/proc/{pid}/stat") as handle:
                state = handle.read().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            return True  # gone before open, or between open and read
        if state == "Z":
            return True
        time.sleep(0.05)
    return False


class WorkspaceTestCase(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.workspace, True)

    def sh(self, script, timeout_s=10.0, env=None):
        command = ("sh", "-c", script)
        return sandbox.run(command, env or {}, timeout_s, self.workspace)


class SandboxTest(WorkspaceTestCase):
    def test_the_exit_code_passes_through(self):
        outcome = self.sh("exit 3")

        self.assertEqual((3, False), (outcome.exit_code, outcome.timed_out))

    def test_stdout_and_stderr_are_captured_in_order(self):
        output = self.sh("echo out; echo err >&2").output

        self.assertEqual("out\nerr\n", output)

    def test_only_the_tail_of_a_long_output_is_kept(self):
        size = sandbox.OUTPUT_LIMIT + 1000
        script = f"head -c {size} /dev/zero | tr '\\0' x; echo END"

        output = self.sh(script).output

        self.assertTrue(output.endswith("xEND\n"))
        self.assertIn("omitted", output.splitlines()[0])
        self.assertLess(len(output), sandbox.OUTPUT_LIMIT + 100)

    def test_the_command_runs_in_the_workspace(self):
        self.assertEqual(self.workspace + "\n", self.sh("pwd").output)

    def test_the_environment_is_the_minimum_plus_what_is_declared(self):
        os.environ["DPES_HOST_ONLY"] = "must not leak"
        self.addCleanup(os.environ.pop, "DPES_HOST_ONLY")

        output = self.sh("env", env={"NAME": "value"}).output

        self.assertIn("NAME=value", output)
        self.assertIn(f"HOME={self.workspace}", output)
        self.assertNotIn("DPES_HOST_ONLY", output)

    def test_the_command_reads_an_empty_input(self):
        outcome = self.sh("cat", timeout_s=5)

        self.assertEqual((0, False), (outcome.exit_code, outcome.timed_out))

    def test_a_timeout_kills_the_whole_process_tree(self):
        started = time.monotonic()

        outcome = self.sh("sleep 300 & echo $!; sleep 300", timeout_s=1)

        self.assertTrue(outcome.timed_out)
        self.assertLess(time.monotonic() - started, 5)
        self.assertTrue(gone(int(outcome.output.split()[0])))

    def test_no_background_process_outlives_a_finished_command(self):
        started = time.monotonic()

        outcome = self.sh("sleep 300 & echo $!")

        self.assertEqual(0, outcome.exit_code)
        self.assertLess(time.monotonic() - started, 5)
        self.assertTrue(gone(int(outcome.output.split()[0])))

    def test_a_program_that_does_not_exist_is_a_task_failure(self):
        outcome = sandbox.run(("no-such-program",), {}, 5, self.workspace)

        self.assertEqual(127, outcome.exit_code)
        self.assertIn("no-such-program", outcome.output)


@unittest.skipUnless(os.geteuid() == 0, "dropping privileges needs root")
class UnprivilegedTest(WorkspaceTestCase):
    def test_the_command_does_not_run_as_root(self):
        self.assertNotEqual("0\n", self.sh("id -u").output)

    def test_a_task_cannot_read_another_workspace(self):
        other = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, other, True)
        with open(os.path.join(other, "secret"), "w") as handle:
            handle.write("not yours")

        outcome = self.sh(f"cat {other}/secret")

        self.assertNotEqual(0, outcome.exit_code)
        self.assertNotIn("not yours", outcome.output)


if __name__ == "__main__":
    unittest.main()
