"""The coordinator actor on the cluster, with a stand-in executor.

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import time
import unittest

import ray

from dpes.coordinator import Coordinator, _Client
from dpes.spec import SpecError, parse_job


def stand_in_executor(attempt):
    """Obeys the command's name instead of running it. Defined at module
    level but shipped by value: tests/ is not on the workers' path."""
    import os
    import time

    from dpes.messages import TaskResult

    program = attempt.command[0]
    number = int(attempt.execution_id.rsplit("/", 1)[1])
    if program == "die" or (program == "die-once" and number == 1):
        os._exit(1)
    if program == "sleep" or (program == "hang-once" and number == 1):
        time.sleep(15)
    if program == "garbage":
        return "not a result"
    exit_code = int(program) if program.isdigit() else 0
    return TaskResult(attempt.execution_id, exit_code, False, {})


ray.cloudpickle.register_pickle_by_value(__import__(__name__))


def job(**tasks):
    return parse_job({"tasks": tasks})


class CoordinatorTestCase(unittest.TestCase):
    queue_timeout_s = 3600

    @classmethod
    def setUpClass(cls):
        # Tolerant: tearDownClass does not run if a setUpClass fails.
        ray.init(address="auto", logging_level="ERROR",
                 ignore_reinit_error=True)
        cls.coordinator = Coordinator.remote(
            stand_in_executor, queue_timeout_s=cls.queue_timeout_s
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def status(self, job_id):
        return ray.get(self.coordinator.status.remote(job_id), timeout=10)

    def submit(self, spec):
        return ray.get(self.coordinator.submit.remote(spec))

    def run_job(self, spec, timeout_s=60):
        job_id = self.submit(spec)
        deadline = time.monotonic() + timeout_s
        view = self.status(job_id)
        while view["state"] in ("PENDING", "RUNNING"):
            self.assertLess(time.monotonic(), deadline, "the job never finished")
            time.sleep(0.2)
            view = self.status(job_id)
        return view

    def run_one(self, *command, **fields):
        return self.run_job(job(t={"command": list(command), **fields}))


class CoordinatorTest(CoordinatorTestCase):
    def test_a_successful_attempt_succeeds_the_job(self):
        view = self.run_one("0")

        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(0, view["tasks"]["t"]["exit_code"])

    def test_a_failing_command_fails_the_job_with_its_exit_code(self):
        view = self.run_one("3")

        self.assertEqual("FAILED", view["state"])
        self.assertEqual(3, view["tasks"]["t"]["exit_code"])
        self.assertEqual("TASK", view["tasks"]["t"]["failure_class"])

    def test_a_dead_executor_out_of_retries_is_an_infrastructure_error(self):
        view = self.run_one("die", retries={"worker": 1})

        task = view["tasks"]["t"]
        self.assertEqual("ERROR", view["state"])
        self.assertEqual("WORKER", task["failure_class"])
        self.assertEqual("WorkerCrashedError", task["error"])
        self.assertEqual(2, len(task["attempts"]))

    def test_an_executor_breaking_its_contract_does_not_hang_the_job(self):
        view = self.run_one("garbage", retries={"worker": 0})

        self.assertEqual("ERROR", view["state"])
        self.assertIn("str", view["tasks"]["t"]["error"])

    def test_status_is_served_while_an_attempt_runs(self):
        job_id = self.submit(job(t={"command": ["sleep"]}))

        started = time.monotonic()
        view = self.status(job_id)

        self.assertEqual("RUNNING", view["state"])
        self.assertLess(time.monotonic() - started, 2)

    def test_an_unknown_job_has_no_status(self):
        self.assertIsNone(self.status("no-such-job"))

    def test_a_dag_runs_every_task_once_its_dependencies_succeed(self):
        view = self.run_job(job(
            a={"command": ["0"]},
            b={"command": ["0"], "needs": ["a"]},
            c={"command": ["0"], "needs": ["a"]},
            d={"command": ["0"], "needs": ["b", "c"]},
        ))

        self.assertEqual("SUCCEEDED", view["state"])

    def test_a_failure_skips_its_dependents_only(self):
        view = self.run_job(job(
            a={"command": ["0"]},
            b={"command": ["3"], "needs": ["a"]},
            c={"command": ["0"], "needs": ["a"]},
            d={"command": ["0"], "needs": ["b", "c"]},
        ))

        states = {name: t["state"] for name, t in view["tasks"].items()}
        self.assertEqual("FAILED", view["state"])
        self.assertEqual(
            {"a": "SUCCEEDED", "b": "FAILED", "c": "SUCCEEDED",
             "d": "SKIPPED"},
            states,
        )

    def test_a_job_no_live_node_can_run_is_refused_before_starting(self):
        spec = job(t={"command": ["0"], "resources": {"gpu": 1}})

        with self.assertRaises(SpecError) as caught:
            _Client(self.coordinator).submit(spec)

        self.assertIn("gpu", caught.exception.errors[0])


class RetryTest(CoordinatorTestCase):
    def test_a_worker_that_dies_once_is_retried_and_the_job_succeeds(self):
        view = self.run_one("die-once")

        attempts = view["tasks"]["t"]["attempts"]
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(["WORKER", "SUCCEEDED"],
                         [a["outcome"] for a in attempts])
        self.assertEqual(0.0, attempts[0]["retry_in_s"])

    def test_an_attempt_silent_past_its_deadline_is_a_network_failure(self):
        started = time.monotonic()

        view = self.run_one("hang-once", timeout_s=1)

        attempts = view["tasks"]["t"]["attempts"]
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(("NETWORK", "deadline"),
                         (attempts[0]["outcome"], attempts[0]["reason"]))
        self.assertGreater(attempts[0]["retry_in_s"], 0)
        self.assertLess(time.monotonic() - started, 15, "deadline too late")


class QueueDeadlineTest(CoordinatorTestCase):
    queue_timeout_s = 2

    def test_an_attempt_stuck_in_the_queue_hits_the_queue_deadline(self):
        # The first job holds worker-1's only unit; the second cannot start.
        self.submit(job(t={"command": ["sleep"],
                           "resources": {"worker-1": 1}}))

        view = self.run_one("0", resources={"worker-1": 1},
                            retries={"network": 0})

        task = view["tasks"]["t"]
        self.assertEqual("ERROR", view["state"])
        self.assertEqual(("NETWORK", "deadline"),
                         (task["failure_class"], task["error"]))


if __name__ == "__main__":
    unittest.main()
