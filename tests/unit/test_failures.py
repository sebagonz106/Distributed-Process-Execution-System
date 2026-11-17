import unittest

from dpes.failures import (
    DEADLINE,
    RESTARTED,
    FailureClass,
    classify,
    retry_delay,
)
from dpes.messages import Evidence, ExecutorFailure, TaskResult

TASK, WORKER, NETWORK = FailureClass


class ClassifyTest(unittest.TestCase):
    def test_a_result_that_is_not_a_success_is_the_task_failing(self):
        self.assertEqual(TASK, classify(TaskResult("e", 1, False, {})))
        self.assertEqual(TASK, classify(TaskResult("e", -9, True, {})))

    def test_no_workspace_is_the_worker_failing(self):
        failure = ExecutorFailure("e", Evidence.WORKSPACE_FAILED)

        self.assertEqual(WORKER, classify(failure))

    def test_every_transfer_problem_is_the_network_failing(self):
        for evidence in set(Evidence) - {Evidence.WORKSPACE_FAILED}:
            with self.subTest(evidence=evidence):
                self.assertEqual(
                    NETWORK, classify(ExecutorFailure("e", evidence))
                )

    def test_silence_until_the_deadline_is_the_network_failing(self):
        self.assertEqual(NETWORK, classify(DEADLINE))

    def test_a_ray_exception_is_the_worker_failing(self):
        for name in ("WorkerCrashedError", "NodeDiedError", "RayTaskError"):
            with self.subTest(name=name):
                self.assertEqual(WORKER, classify(name))

    def test_a_coordinator_restart_is_the_worker_failing(self):
        # Ray stops the attempts of an owner that died (plan/14, D14.4).
        self.assertEqual(WORKER, classify(RESTARTED))


class RetryDelayTest(unittest.TestCase):
    def test_task_and_worker_failures_retry_at_once(self):
        for failure_class in (TASK, WORKER):
            with self.subTest(failure_class=failure_class):
                self.assertEqual(0.0, retry_delay(failure_class, 3, 0.7))

    def test_a_network_wait_is_between_half_and_all_of_its_cap(self):
        self.assertEqual((0.5, 1.0), (retry_delay(NETWORK, 0, 0.0),
                                      retry_delay(NETWORK, 0, 1.0)))
        self.assertEqual((1.0, 2.0), (retry_delay(NETWORK, 1, 0.0),
                                      retry_delay(NETWORK, 1, 1.0)))

    def test_the_cap_stops_growing_at_thirty_seconds(self):
        self.assertEqual(30.0, retry_delay(NETWORK, 10, 1.0))

    def test_waits_never_shrink_while_the_cap_doubles(self):
        # Caps 1, 2, 4, 8, 16 s: covers the default four network retries.
        for n in range(4):
            with self.subTest(retry=n):
                longest_now = retry_delay(NETWORK, n, 1.0)
                shortest_next = retry_delay(NETWORK, n + 1, 0.0)
                self.assertLessEqual(longest_now, shortest_next)

    def test_at_the_cap_waits_stay_between_fifteen_and_thirty_seconds(self):
        for n in range(5, 11):
            with self.subTest(retry=n):
                self.assertEqual((15.0, 30.0), (retry_delay(NETWORK, n, 0.0),
                                                retry_delay(NETWORK, n, 1.0)))


if __name__ == "__main__":
    unittest.main()
