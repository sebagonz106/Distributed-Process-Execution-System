"""The coordinator actor on the cluster, with a stand-in executor.

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import time
import unittest

import ray

from dpes.coordinator import Coordinator
from dpes.spec import parse_job


def one_task(*command):
    return parse_job({"tasks": {"t": {"command": list(command)}}})


class CoordinatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(address="auto", logging_level="ERROR")

        # Defined here so Ray ships it by value: tests/ is not on the workers.
        @ray.remote
        def stand_in_executor(attempt):
            import os

            from dpes.messages import TaskResult

            program = attempt.command[0]
            if program == "die":
                os._exit(1)
            if program == "sleep":
                time.sleep(15)
            if program == "garbage":
                return "not a result"
            return TaskResult(attempt.execution_id, int(program), False, {})

        cls.coordinator = Coordinator.remote(stand_in_executor)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def status(self, job_id):
        return ray.get(self.coordinator.status.remote(job_id), timeout=10)

    def run_job(self, *command):
        job_id = ray.get(self.coordinator.submit.remote(one_task(*command)))
        deadline = time.monotonic() + 60
        view = self.status(job_id)
        while view["state"] in ("PENDING", "RUNNING"):
            self.assertLess(time.monotonic(), deadline, "the job never finished")
            time.sleep(0.2)
            view = self.status(job_id)
        return view

    def test_a_successful_attempt_succeeds_the_job(self):
        view = self.run_job("0")

        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(0, view["tasks"]["t"]["exit_code"])

    def test_a_failing_command_fails_the_job_with_its_exit_code(self):
        view = self.run_job("3")

        self.assertEqual("FAILED", view["state"])
        self.assertEqual(3, view["tasks"]["t"]["exit_code"])

    def test_a_dead_executor_is_an_infrastructure_error(self):
        view = self.run_job("die")

        self.assertEqual("ERROR", view["state"])
        self.assertEqual("WorkerCrashedError", view["tasks"]["t"]["error"])

    def test_an_executor_breaking_its_contract_does_not_hang_the_job(self):
        view = self.run_job("garbage")

        self.assertEqual("ERROR", view["state"])
        self.assertIn("str", view["tasks"]["t"]["error"])

    def test_status_is_served_while_an_attempt_runs(self):
        job_id = ray.get(self.coordinator.submit.remote(one_task("sleep")))

        started = time.monotonic()
        view = self.status(job_id)

        self.assertEqual("RUNNING", view["state"])
        self.assertLess(time.monotonic() - started, 2)

    def test_an_unknown_job_has_no_status(self):
        self.assertIsNone(self.status("no-such-job"))


if __name__ == "__main__":
    unittest.main()
