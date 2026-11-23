"""Cut C8, the part of F7.7: the coordinator restarts mid-job (plan/14).

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import json
import time
import unittest

import ray

from gateway_client import cli, submit, wait, wait_for_gateway


class CoordinatorRestartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()
        ray.init(address="auto", namespace="dpes", logging_level="ERROR",
                 ignore_reinit_error=True, log_to_driver=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def until_running(self, job_id, task, timeout_s=60):
        deadline = time.monotonic() + timeout_s
        while True:
            code, out, _ = cli("status", job_id)
            view = json.loads(out) if code == 0 else None
            if view and view["tasks"][task]["state"] == "RUNNING":
                return
            self.assertLess(time.monotonic(), deadline, f"{task} never ran")
            time.sleep(0.2)

    def test_a_job_finishes_although_the_coordinator_restarts_mid_task(self):
        code, job_id, err = submit({
            # A task that must be seen running twice, so not from the cache.
            "build": {"command": ["sh", "-c", "sleep 8; echo built"],
                      "cache": False},
            "test": {"command": ["echo", "tested"], "needs": ["build"],
                     "cache": False},
        })
        self.assertEqual(0, code, err)
        self.until_running(job_id, "build")
        time.sleep(2)  # the attempt is out on a worker, its launch journaled
        ray.kill(ray.get_actor("coordinator"), no_restart=False)

        code, view = wait(job_id)  # the real client, through the restart

        build = view["tasks"]["build"]
        lost, *_, last = build["attempts"]
        self.assertEqual(0, code)
        self.assertEqual(("WORKER", "coordinator restart"),
                         (lost["outcome"], lost["reason"]))
        self.assertEqual(("SUCCEEDED", "built\n"),
                         (last["outcome"], build["output"]))
        self.assertEqual("tested\n", view["tasks"]["test"]["output"])


if __name__ == "__main__":
    unittest.main()
