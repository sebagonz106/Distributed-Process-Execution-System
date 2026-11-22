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


def new_marker():
    """A name only this test's workspace will hold."""
    return f"marker-{uuid.uuid4().hex}"


def running_the_task(marker):
    """The worker whose workspace holds `marker`, or None.

    One marker per test, not one for the whole module: a killed attempt leaves
    its workspace behind (plan/11, D11.11), so a shared marker would point a
    test at the previous test's leftover.
    """
    for worker in WORKERS:
        found = docker("exec", worker, "sh", "-c",
                       f"ls /tmp/dpes-*/{marker}")
        if found.returncode == 0:
            return worker
    return None


def live_nodes():
    count = ("import ray; ray.init(address='auto', logging_level='ERROR');"
             "print(sum(n['Alive'] for n in ray.nodes()))")
    out = docker("exec", "dpes-head", "python", "-c", count).stdout
    # Ray puede traer líneas de aviso; me quedo con el último entero.
    numbers = [part for part in out.split() if part.isdigit()]
    return int(numbers[-1]) if numbers else 0


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
        marker = new_marker()
        job_id = self.submit({
            "slow": {"command": ["sh", "-c",
                                 f"touch {marker}; sleep 8; hostname"]},
        })
        victim = self.wait_until(lambda: running_the_task(marker), 30,
                                 "running task")

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

    def test_the_log_of_an_attempt_survives_the_death_of_its_worker(self):
        # Cut C4 (plan/15): the output is stored in the control node, so it is
        # still served after the worker that produced it is gone.
        marker = new_marker()
        job_id = self.submit({"slow": {
            # The pause before the marker lets the emitter flush the line: the
            # log is not lost with the process (D15.3, FLUSH_S).
            "command": ["sh", "-c",
                        f"echo before; sleep 2; touch {marker}; sleep 6"],
            # No budget: the job ends with the dead attempt, so the view keeps
            # pointing at it and `logs` can be asked for that very log.
            "retries": {"task": 0, "worker": 0, "network": 0},
        }})
        victim = self.wait_until(lambda: running_the_task(marker), 30,
                                 "running task")

        self.assertEqual(0, docker("kill", victim).returncode)
        self.addCleanup(self.restore, victim)
        code, _ = run_cli("wait", job_id, "--interval", "0.5")
        self.assertEqual(2, code)  # a worker failure out of budget (plan/09)

        code, out = run_cli("logs", job_id, "slow", "-o", "0")

        self.assertEqual(0, code, "the log of the dead attempt was not served")
        self.assertEqual("before\n", out)

    def restore(self, worker):
        docker("start", worker)
        self.wait_until(lambda: live_nodes() == 3, 120, "rejoined worker")


if __name__ == "__main__":
    unittest.main()
