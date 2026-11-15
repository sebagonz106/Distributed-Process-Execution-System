"""The descriptive logs trace one job through every process (plan/10).

Runs on the host: it reads the control node's container log, where Ray
gathers the gateway, the coordinator and the executors (D10.7). With the
cluster up, from the repository root:

    python -m unittest discover -s tests/host
"""

import contextlib
import io
import json
import os
import re
import subprocess
import tempfile
import time
import unittest

from dpes import cli

URL = "http://127.0.0.1:8000"
SECRET = "do-not-log-7f3a"
LINE = re.compile(
    r"(?P<time>\d{4}-\d\d-\d\dT[\d:.]+Z) (?P<level>\w+) +(?P<component>\w+)"
    r" +(?P<message>[a-z ]+?)(?: (?P<fields>\w+=.*))?$"
)


def run_cli(*args):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(["--url", URL, *args])
    return code, out.getvalue().strip()


def control_node_log():
    result = subprocess.run(["docker", "logs", "dpes-head"],
                            capture_output=True, text=True)
    return result.stdout + result.stderr


def lines_about(job_id, log):
    """(time, component, message, fields) for each line naming `job_id`."""
    found = []
    for raw in log.splitlines():
        start = raw.find("20")  # skip Ray's "(Coordinator pid=…)" prefix
        match = LINE.match(raw[start:]) if job_id in raw else None
        if match:
            fields = dict(pair.split("=", 1)
                          for pair in (match["fields"] or "").split())
            found.append((match["time"], match["component"],
                          match["message"], fields))
    return found


class LogTraceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "job.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"tasks": {
                    "build": {"command": ["sh", "-c", "echo built"],
                              "env": {"TOKEN": SECRET}},
                    "test": {"command": ["sh", "-c", "exit 1"],
                             "needs": ["build"]},
                }}, handle)
            code, cls.job_id = run_cli("submit", path)
        assert code == 0, "is the cluster up?"
        run_cli("wait", cls.job_id, "--interval", "0.3")
        # Ray forwards worker lines asynchronously: wait for the last one.
        deadline = time.monotonic() + 20
        while True:
            cls.log = control_node_log()
            cls.lines = lines_about(cls.job_id, cls.log)
            finished = any(m == "job finished" for _, _, m, _ in cls.lines)
            if finished or time.monotonic() > deadline:
                break
            time.sleep(0.5)

    def test_every_process_describes_its_part_of_the_job(self):
        build = f"{self.job_id}/build/0/1"
        test = f"{self.job_id}/test/0/1"

        expected = [
            ("gateway", "job accepted", "job", self.job_id),
            ("coordinator", "job started", "job", self.job_id),
            ("coordinator", "attempt launched", "execution", build),
            ("executor", "attempt running", "execution", build),
            ("executor", "command finished", "execution", build),
            ("coordinator", "attempt succeeded", "execution", build),
            ("executor", "attempt running", "execution", test),
            ("coordinator", "attempt failed", "execution", test),
            ("coordinator", "job finished", "job", self.job_id),
        ]
        seen = [(c, m, f.get(key)) for _, c, m, f in self.lines
                for key in ("job", "execution")]
        for component, message, _, value in expected:
            with self.subTest(message=message, about=value):
                self.assertIn((component, message, value), seen)

    def first_time(self, message):
        return min(s for s, _, m, _ in self.lines if m == message)

    def test_the_lines_follow_the_order_things_happened_in(self):
        order = ["job started", "attempt launched", "attempt running",
                 "command finished", "job finished"]

        stamps = [self.first_time(message) for message in order]

        self.assertEqual(sorted(stamps), stamps)

    def test_the_gateway_reports_acceptance_only_once_confirmed(self):
        # It logs after the coordinator answers, so after "job started".
        accepted = self.first_time("job accepted")

        self.assertGreaterEqual(accepted, self.first_time("job started"))
        self.assertLessEqual(accepted, self.first_time("job finished"))

    def test_the_executor_names_the_worker_it_ran_on(self):
        hosts = {f["host"] for _, _, m, f in self.lines
                 if m == "attempt running"}

        self.assertTrue(hosts)
        self.assertTrue(all(h.startswith("worker-") for h in hosts), hosts)

    def test_the_failure_is_described_with_its_class(self):
        (fields,) = [f for _, _, m, f in self.lines if m == "attempt failed"]

        self.assertEqual("TASK", fields["failure_class"])

    def test_no_environment_value_reaches_the_log(self):
        self.assertNotIn(SECRET, self.log)


if __name__ == "__main__":
    unittest.main()
