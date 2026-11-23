"""Cut C6: the task ended, but its confirmation was lost (plan/12).

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import json
import unittest
import urllib.error
import urllib.request

from gateway_client import URL, run_job, wait_for_gateway


def post_job(tasks, key):
    request = urllib.request.Request(
        f"{URL}/jobs", json.dumps({"tasks": tasks}).encode(),
        headers={"Idempotency-Key": key}, method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        with error:
            return error.code, json.load(error)


class LostConfirmationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def test_a_saved_result_whose_answer_is_lost_is_adopted(self):
        code, view = run_job({"t": {
            "command": ["sh", "-c", "echo ran"],
            "env": {"DPES_FAULT": "lose-ack:1"},
            "cache": False,  # the command must run to be saved, then lost
        }})

        task = view["tasks"]["t"]
        self.assertEqual(0, code)
        # One attempt only: the command was not run a second time.
        (attempt,) = task["attempts"]
        self.assertEqual("SUCCEEDED", attempt["outcome"])
        self.assertIn("adopted by reconciliation", attempt["reason"])
        self.assertIn("WorkerCrashedError", attempt["reason"])
        self.assertEqual("ran\n", task["output"])

    def test_resending_a_job_with_its_key_gives_back_the_same_job(self):
        tasks = {"t": {"command": ["true"]}}

        first = post_job(tasks, "c6-resend-1")
        again = post_job(tasks, "c6-resend-1")

        self.assertEqual(201, first[0])
        self.assertEqual(first, again)

    def test_a_key_reused_for_another_job_is_refused(self):
        post_job({"t": {"command": ["true"]}}, "c6-reused-1")

        status, body = post_job({"t": {"command": ["false"]}}, "c6-reused-1")

        self.assertEqual(400, status)
        self.assertIn("c6-reused-1", body["errors"][0])


if __name__ == "__main__":
    unittest.main()
