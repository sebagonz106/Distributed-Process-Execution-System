"""Cut C5 from outside: a worker container dies in the middle of a task.

Runs on the host, not in the cluster: it needs the Docker CLI to kill the
container, and reaches the gateway on its published port (plan/09, D09.7).
With the cluster up, from the repository root:

    python -m unittest discover -s tests/host
"""

import contextlib
import io
import json
import os
import subprocess
import tempfile
import time
import unittest
import uuid

from dpes import cli

URL = "http://127.0.0.1:8000"
WORKERS = ("dpes-worker-1", "dpes-worker-2")


def run_cli(*args):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(["--url", URL, *args])
    return code, out.getvalue()


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


MARKER = f"marker-{uuid.uuid4().hex}"


def running_the_task():
    """The worker whose workspace holds this run's marker, or None.

    Not just any workspace: attempts killed by earlier tests leave theirs
    behind (plan/11, D11.11).
    """
    for worker in WORKERS:
        found = docker("exec", worker, "sh", "-c",
                       f"ls /tmp/dpes-*/{MARKER}")
        if found.returncode == 0:
            return worker
    return None


def live_nodes():
    count = ("import ray; ray.init(address='auto', logging_level='ERROR');"
             "print(sum(n['Alive'] for n in ray.nodes()))")
    out = docker("exec", "dpes-head", "python", "-c", count).stdout
    return int(out.split()[-1]) if out.split() else 0


class WorkerLossTest(unittest.TestCase):
    def submit(self, tasks):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "job.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"tasks": tasks}, handle)
            code, out = run_cli("submit", path)
        self.assertEqual(0, code, "is the cluster up?")
        return out.strip()

    def wait_until(self, condition, timeout_s, what):
        deadline = time.monotonic() + timeout_s
        while True:
            value = condition()
            if value:
                return value
            self.assertLess(time.monotonic(), deadline, f"no {what}")
            time.sleep(0.5)

    def test_a_killed_worker_is_a_worker_failure_retried_elsewhere(self):
        job_id = self.submit({
            "slow": {"command": ["sh", "-c",
                                 f"touch {MARKER}; sleep 8; hostname"]},
        })
        victim = self.wait_until(running_the_task, 30, "running task")

        self.assertEqual(0, docker("kill", victim).returncode)
        self.addCleanup(self.restore, victim)
        code, out = run_cli("wait", job_id, "--interval", "0.5")

        task = json.loads(out)["tasks"]["slow"]
        attempts = task["attempts"]
        self.assertEqual(0, code)
        self.assertEqual(["WORKER", "SUCCEEDED"],
                         [attempt["outcome"] for attempt in attempts])
        self.assertNotEqual(victim.removeprefix("dpes-"),
                            task["output"].strip())

    def restore(self, worker):
        docker("start", worker)
        self.wait_until(lambda: live_nodes() == 3, 120, "rejoined worker")


if __name__ == "__main__":
    unittest.main()
