"""Cut C1: one task end to end, client to gateway to worker and back.

Run inside the control node, where the gateway listens (plan/07, D07.18):

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import contextlib
import io
import json
import os
import tempfile
import time
import unittest
import urllib.error
import urllib.request

from dpes import cli

URL = "http://localhost:8000"


def wait_for_gateway(timeout_s=60):
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            urllib.request.urlopen(f"{URL}/jobs/none", timeout=2)
        except urllib.error.HTTPError:
            return  # it answered, so it is up
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


class OneTaskTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def cli(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(["--url", URL, *args])
        return code, out.getvalue()

    def run_task(self, **task):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "job.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"tasks": {"t": task}}, handle)
            code, out = self.cli("submit", path)
        self.assertEqual(0, code)
        code, out = self.cli("wait", out.strip(), "--interval", "0.2")
        return code, json.loads(out)["tasks"]["t"]

    def test_a_successful_task_shows_its_output(self):
        code, task = self.run_task(
            command=["sh", "-c", "echo hola $NAME"], env={"NAME": "mundo"}
        )

        self.assertEqual((0, "SUCCEEDED"), (code, task["state"]))
        self.assertEqual("hola mundo\n", task["output"])

    def test_a_failing_command_shows_its_exit_code(self):
        code, task = self.run_task(command=["sh", "-c", "exit 3"])

        outcome = (code, task["state"], task["exit_code"])
        self.assertEqual((1, "FAILED", 3), outcome)

    def test_a_command_over_its_time_limit_is_stopped(self):
        started = time.monotonic()

        code, task = self.run_task(
            command=["sh", "-c", "sleep 300 & sleep 300"], timeout_s=2
        )

        self.assertEqual((1, "FAILED"), (code, task["state"]))
        self.assertTrue(task["timed_out"])
        self.assertLess(time.monotonic() - started, 30)

    def test_the_task_runs_on_a_worker_isolated_from_its_environment(self):
        script = "hostname; id -u; env"

        code, task = self.run_task(command=["sh", "-c", script])

        hostname, uid, *environment = task["output"].splitlines()
        self.assertEqual(0, code)
        self.assertTrue(hostname.startswith("worker-"), hostname)
        self.assertNotEqual("0", uid)
        leaked = [v for v in environment if v.startswith(("RAY_", "PYTHON"))]
        self.assertEqual([], leaked)


if __name__ == "__main__":
    unittest.main()
