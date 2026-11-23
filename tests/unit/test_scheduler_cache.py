"""The cache hit as a transition of the state machine (plan/16, D16.3, D16.8).

A hit is not an attempt: nothing ran, so the task has no execution of its own
and the attempt launched for its round has to be taken back. What the job
keeps is the original execution, whose blobs and log are still there.
"""

import unittest

from dpes.messages import TaskResult
from dpes.scheduler import Job, State
from dpes.spec import JobSpec, TaskSpec

ORIGINAL = "job-0/compile/0/1"
OUTPUTS = {"app": "sha256:" + "a" * 64}


def task_spec(needs=()):
    return TaskSpec(("cc", "-o", "app"), {}, 30.0, tuple(needs),
                    outputs=("app",))


def result(execution_id):
    return TaskResult(execution_id, 0, False, {})


def only(launches):
    (launch,) = launches
    return launch


def names(launches):
    return [launch.attempt.task for launch in launches]


class CacheHitTest(unittest.TestCase):
    def setUp(self):
        spec = JobSpec({"compile": task_spec(), "test": task_spec(["compile"])})
        self.job = Job("job-1", spec)
        self.attempt = only(self.job.start()).attempt

    def hit(self, **kwargs):
        return self.job.on_cache_hit("compile", OUTPUTS, ORIGINAL, **kwargs)

    def view(self):
        return self.job.view()

    def compile_view(self):
        return self.view()["tasks"]["compile"]

    def test_a_hit_succeeds_the_task_with_the_stored_digests(self):
        self.hit()

        task = self.compile_view()
        self.assertEqual("SUCCEEDED", task["state"])
        self.assertEqual(OUTPUTS, task["outputs"])
        self.assertEqual(0, task["exit_code"])

    def test_a_hit_has_no_attempt_of_its_own(self):
        self.hit()

        task = self.compile_view()
        self.assertIsNone(task["execution_id"])
        self.assertTrue(task["cached"])
        self.assertEqual(ORIGINAL, task["cached_from"])

    def test_a_task_that_never_used_the_cache_says_so(self):
        task = self.compile_view()

        self.assertFalse(task["cached"])
        self.assertIsNone(task["cached_from"])

    def test_a_hit_launches_what_it_unblocks(self):
        self.assertEqual(["test"], names(self.hit()))
        self.assertEqual(State.RUNNING, self.job.state)

    def test_a_hit_leaves_nothing_of_its_attempt_in_flight(self):
        self.hit()

        # Only the dependent it launched is out; the hit's own attempt is not.
        self.assertEqual(["test"], [a.task for a in self.job.unsettled()])
        # Its answer is not a second result for a task already settled.
        self.assertEqual([], self.job.on_result(result(self.attempt.execution_id)))
        self.assertEqual("SUCCEEDED", self.compile_view()["state"])

    def test_the_history_says_the_cache_answered(self):
        self.hit()

        self.assertEqual([{
            "execution_id": None,
            "outcome": "CACHE_HIT",
            "reason": f"cache hit: {ORIGINAL}",
            "retry_in_s": None,
        }], self.compile_view()["attempts"])

    def test_the_hit_is_an_event_naming_the_original_execution(self):
        self.hit()

        (event,) = [e for e in self.job.events() if e.type == "CACHE_HIT"]
        self.assertEqual(("", "compile"), (event.execution_id, event.task))
        self.assertEqual({"original_execution_id": ORIGINAL}, event.payload)

    def test_a_hit_counts_and_adds_the_bytes_it_reused(self):
        self.hit(bytes_saved=40)

        self.assertEqual({"hits": 1, "misses": 0, "bytes_saved": 40},
                         self.view()["cache_stats"])

    def test_a_lookup_that_had_to_run_is_a_miss(self):
        self.job.on_cache_miss()

        self.assertEqual({"hits": 0, "misses": 1, "bytes_saved": 0},
                         self.view()["cache_stats"])
        task = self.compile_view()
        self.assertEqual("RUNNING", task["state"])
        self.assertEqual(self.attempt.execution_id, task["execution_id"])

    def test_the_same_calls_rebuild_the_same_view(self):
        def play():
            spec = JobSpec({"compile": task_spec(),
                            "test": task_spec(["compile"])})
            job = Job("job-1", spec)
            job.start()
            job.on_cache_miss()
            job.on_cache_hit("compile", OUTPUTS, ORIGINAL, 40)
            return job.view()

        self.assertEqual(play(), play())


if __name__ == "__main__":
    unittest.main()
