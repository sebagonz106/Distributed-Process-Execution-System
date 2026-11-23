"""Cut C5: every failure class, its evidence and its retry (plan/09).

Faults are injected from inside the executor (D09.7). The real loss of a
worker container is in tests/host, which needs Docker on the host.

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import unittest

from gateway_client import run_job, wait_for_gateway


def task(fault=None, **fields):
    env = {"DPES_FAULT": fault} if fault else {}
    # Each of these tests watches an attempt: a cached result would show
    # nothing of what it checks (plan/16, F9.5).
    return {"command": ["sh", "-c", "echo ran"], "env": env, "cache": False,
            **fields}


class FailureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def run_task(self, spec):
        code, view = run_job({"t": spec})
        state = view["tasks"]["t"]
        return code, state, state["attempts"]

    def outcomes(self, attempts):
        return [attempt["outcome"] for attempt in attempts]

    def test_a_failing_command_is_its_own_fault_and_is_not_retried(self):
        code, _, attempts = self.run_task(
            {"command": ["sh", "-c", "exit 1"]}
        )

        self.assertEqual(1, code)
        self.assertEqual(["TASK"], self.outcomes(attempts))
        self.assertEqual("exit code 1", attempts[0]["reason"])

    def test_a_dead_worker_process_is_retried_at_once(self):
        code, state, attempts = self.run_task(task("crash:1"))

        self.assertEqual(0, code)
        self.assertEqual(["WORKER", "SUCCEEDED"], self.outcomes(attempts))
        self.assertEqual("WorkerCrashedError", attempts[0]["reason"])
        self.assertEqual(0.0, attempts[0]["retry_in_s"])
        self.assertEqual("ran\n", state["output"])

    def test_network_failures_are_retried_after_growing_waits(self):
        code, _, attempts = self.run_task(task("network:2"))

        waits = [attempt["retry_in_s"] for attempt in attempts[:2]]
        self.assertEqual(0, code)
        self.assertEqual(["NETWORK", "NETWORK", "SUCCEEDED"],
                         self.outcomes(attempts))
        self.assertTrue(0 < waits[0] <= waits[1], waits)

    def test_an_attempt_silent_past_its_deadline_is_a_network_failure(self):
        code, _, attempts = self.run_task(task("hang:1", timeout_s=1))

        self.assertEqual(0, code)
        self.assertEqual(("NETWORK", "deadline"),
                         (attempts[0]["outcome"], attempts[0]["reason"]))

    def test_an_exhausted_budget_ends_in_an_infrastructure_error(self):
        code, state, attempts = self.run_task(
            task("network:9", retries={"network": 1})
        )

        self.assertEqual(2, code)
        self.assertEqual("ERROR", state["state"])
        self.assertEqual("NETWORK", state["failure_class"])
        self.assertEqual(["NETWORK", "NETWORK"], self.outcomes(attempts))


if __name__ == "__main__":
    unittest.main()
