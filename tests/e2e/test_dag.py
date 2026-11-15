"""Cut C2: dependencies and resources, through the gateway (plan/08).

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import unittest

from gateway_client import run_job, submit, wait, wait_for_gateway

# Prints "<worker> <start>", works for two seconds, then prints "<end>".
TIMED = 'echo "$(hostname) $(date +%s.%N)"; sleep 2; date +%s.%N'


def timed(**fields):
    return {"command": ["sh", "-c", TIMED], **fields}


def interval(task):
    first, end = task["output"].split("\n")[:2]
    host, start = first.split()
    return host, float(start), float(end)


class DagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def test_a_rhombus_runs_its_middle_tasks_in_parallel_on_two_workers(self):
        # Two CPUs each is a whole worker: B and C cannot share one (D08.8).
        code, view = run_job({
            "a": {"command": ["true"]},
            "b": timed(needs=["a"], resources={"cpu": 2}),
            "c": timed(needs=["a"], resources={"cpu": 2}),
            "d": timed(needs=["b", "c"]),
        })

        tasks = view["tasks"]
        b_host, b_start, b_end = interval(tasks["b"])
        c_host, c_start, c_end = interval(tasks["c"])
        _, d_start, _ = interval(tasks["d"])
        self.assertEqual(0, code)
        self.assertNotEqual(b_host, c_host)
        self.assertTrue(b_start < c_end and c_start < b_end, "not parallel")
        self.assertGreaterEqual(d_start, max(b_end, c_end))

    def test_a_cycle_is_rejected_naming_it(self):
        code, _, err = submit({
            "a": {"command": ["true"], "needs": ["b"]},
            "b": {"command": ["true"], "needs": ["a"]},
        })

        self.assertEqual(3, code)
        self.assertIn("dependency cycle a -> b -> a", err)

    def test_a_failure_skips_what_depends_on_it_and_the_rest_finishes(self):
        code, view = run_job({
            "a": {"command": ["true"]},
            "b": {"command": ["false"], "needs": ["a"]},
            "c": {"command": ["true"], "needs": ["a"]},
            "d": {"command": ["true"], "needs": ["b", "c"]},
        })

        states = {name: t["state"] for name, t in view["tasks"].items()}
        self.assertEqual(1, code)
        self.assertEqual(
            {"a": "SUCCEEDED", "b": "FAILED", "c": "SUCCEEDED",
             "d": "SKIPPED"},
            states,
        )

    def test_a_resource_no_node_has_is_rejected_on_submission(self):
        code, _, err = submit({
            "t": {"command": ["true"], "resources": {"gpu": 1}},
        })

        self.assertEqual(3, code)
        self.assertIn("gpu", err)

    def test_two_jobs_submitted_together_both_finish(self):
        job = {"t": {"command": ["sleep", "1"]}}
        submitted = [submit(job), submit(job)]

        finals = [wait(job_id) for _, job_id, _ in submitted]

        self.assertEqual([0, 0], [code for code, _ in finals])
        self.assertNotEqual(submitted[0][1], submitted[1][1])


if __name__ == "__main__":
    unittest.main()
