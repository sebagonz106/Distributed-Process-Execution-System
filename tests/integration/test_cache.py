"""The content cache on the cluster: work that already succeeded is not done
twice (plan/16, R9).

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import os
import tempfile
import time
import unittest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.coordinator import BlobStore, Coordinator, LogStore, _Client
from dpes.hashing import digest
from dpes.spec import parse_job


def cache_executor(attempt, store):
    """Stands in for the executor, its confirmation included.

    It leaves a log fragment, so a test tells an execution from a cache hit:
    on a hit nothing runs, so there is no fragment (plan/16, D16.5). It saves
    the result with the attempt's cache key, as the real executor does
    (D16.6), and derives each artifact from the attempt's inputs, so equal
    inputs yield equal digests -- that is what chains the keys of the
    dependents (F9.4). `broken` stands in for a command that fails.
    Defined at module level but shipped by value: tests/ is not on the
    workers' path.
    """
    from dpes.messages import TaskResult

    store.append_fragment(attempt.execution_id, 1, b"ran\n")
    if attempt.command[0] == "broken":
        failed = TaskResult(attempt.execution_id, 1, False, {})
        return store.save_result(attempt.result_key, failed, attempt.cache_key)
    material = "|".join(sorted(attempt.inputs.values())) or attempt.command[0]
    result = TaskResult(attempt.execution_id, 0, False,
                        {path: store.put(material.encode())
                         for path in attempt.outputs})
    return store.save_result(attempt.result_key, result, attempt.cache_key)


ray.cloudpickle.register_pickle_by_value(__import__(__name__))


def job(**tasks):
    return parse_job({"tasks": tasks})


class CacheTestCase(unittest.TestCase):
    """One store, one log store and one coordinator per test class.

    Nothing here touches the volume of the demonstration cluster: a cache
    filled by these tests must not answer a job of somebody else.
    """

    @classmethod
    def setUpClass(cls):
        # Tolerant: tearDownClass does not run if a setUpClass fails.
        ray.init(address="auto", logging_level="ERROR",
                 ignore_reinit_error=True, log_to_driver=False)
        folder = tempfile.mkdtemp(prefix="dpes-cache-")
        # On this node, as on the control node: the folder is only here.
        here = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
        cls.here = here
        cls.journal = os.path.join(folder, "coordinator.sqlite")
        cls.store = BlobStore.options(scheduling_strategy=here).remote(
            os.path.join(folder, "blobs"),
            os.path.join(folder, "results.sqlite"),
        )
        cls.logs = LogStore.options(scheduling_strategy=here).remote(
            os.path.join(folder, "logs"),
        )
        cls.coordinator = Coordinator.options(scheduling_strategy=here).remote(
            cache_executor, cls.store, cls.logs, cls.journal,
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def client(self):
        return _Client(self.coordinator, self.store, self.logs)

    def upload(self, data):
        """Store `data` and return its digest, to use as a task's input."""
        return self.client().put_blob(digest(data), data)

    def run_job(self, spec, timeout_s=60):
        """`spec` as a new job, waited for."""
        job_id = self.client().submit(spec)
        deadline = time.monotonic() + timeout_s
        view = self.client().status(job_id)
        while view["state"] in ("PENDING", "RUNNING"):
            self.assertLess(time.monotonic(), deadline, "the job never ended")
            time.sleep(0.2)
            view = self.client().status(job_id)
        return view

    def ran(self, view, task="t"):
        """Whether the attempt that settled `task` did any work.

        Its log is the proof: a task served from the cache runs nothing and
        writes none (plan/16, D16.5).
        """
        execution_id = view["tasks"][task]["execution_id"]
        return self.client().log_size(execution_id) > 0

    def reason(self, view, task="t"):
        """Why the attempt that settled `task` did so."""
        return view["tasks"][task]["attempts"][0]["reason"]


class CacheHitTest(CacheTestCase):
    def test_the_same_work_is_not_done_twice(self):
        spec = job(t={"command": ["build-twice"], "outputs": ["out.txt"]})

        first = self.run_job(spec)
        second = self.run_job(spec)

        self.assertEqual("SUCCEEDED", first["state"])
        self.assertTrue(self.ran(first), "the first job did not run")
        self.assertFalse(self.ran(second), "the second job ran the command")
        self.assertIn("cache hit", self.reason(second))

    def test_a_hit_serves_the_artifact_the_earlier_run_stored(self):
        spec = job(t={"command": ["build-stored"], "outputs": ["out.txt"]})
        first = self.run_job(spec)

        hit = self.run_job(spec)  # no worker is involved in this one

        produced = first["tasks"]["t"]["outputs"]["out.txt"]
        self.assertEqual(produced, hit["tasks"]["t"]["outputs"]["out.txt"])
        self.assertEqual(b"build-stored", self.client().get_blob(produced))

    def test_the_same_task_on_another_worker_reuses_the_result(self):
        # The cache belongs to the cluster, not to a worker (D16.7): `b` is
        # placed on worker-2 and is still served from worker-1's result. It
        # declares no outputs, so its work is exactly `a`'s.
        spec = job(
            a={"command": ["build-anywhere"], "resources": {"worker-1": 1}},
            b={"command": ["build-anywhere"], "resources": {"worker-2": 1},
               "needs": ["a"]},
        )

        view = self.run_job(spec)

        self.assertEqual("SUCCEEDED", view["state"])
        self.assertTrue(self.ran(view, "a"), "the first task did not run")
        self.assertFalse(self.ran(view, "b"), "the second worker ran it again")


class CacheMissTest(CacheTestCase):
    def test_a_changed_input_reruns_only_the_task_it_changes(self):
        one, two = self.upload(b"one"), self.upload(b"two")
        spec = {
            "p": {"command": ["compile-p"], "inputs": {"p.in": one},
                  "outputs": ["p.o"]},
            "q": {"command": ["compile-q"], "inputs": {"q.in": one},
                  "outputs": ["q.o"]},
        }
        self.run_job(job(**spec))

        again = self.run_job(job(**spec))
        self.assertFalse(self.ran(again, "p") or self.ran(again, "q"),
                         "an unchanged job ran something")

        changed = dict(spec, p={**spec["p"], "inputs": {"p.in": two}})
        rerun = self.run_job(job(**changed))

        self.assertTrue(self.ran(rerun, "p"), "the changed task did not run")
        self.assertFalse(self.ran(rerun, "q"), "an untouched task ran again")

    def test_a_dependent_reruns_when_the_artifact_it_consumes_changes(self):
        one, two = self.upload(b"one"), self.upload(b"two")
        spec = {
            "compile": {"command": ["compile"], "inputs": {"src": one},
                        "outputs": ["obj"]},
            "link": {"command": ["link"], "needs": ["compile"],
                     "outputs": ["app"]},
        }
        self.run_job(job(**spec))

        again = self.run_job(job(**spec))
        self.assertFalse(self.ran(again, "compile") or self.ran(again, "link"))

        changed = dict(spec, compile={**spec["compile"],
                                      "inputs": {"src": two}})
        rerun = self.run_job(job(**changed))

        self.assertTrue(self.ran(rerun, "compile"))
        self.assertTrue(self.ran(rerun, "link"),
                        "the dependent was served a different artifact")

    def test_a_dependent_is_served_when_its_source_reruns_the_same_artifact(
        self,
    ):
        # The other half of F9.4: a new command reruns the source, which
        # produces the same artifact, so the dependent's inputs -- and its
        # key -- are unchanged and its work is reused.
        one = self.upload(b"one")
        spec = {
            "compile": {"command": ["compile", "v1"], "inputs": {"src": one},
                        "outputs": ["obj"]},
            "link": {"command": ["link"], "needs": ["compile"],
                     "outputs": ["app"]},
        }
        self.run_job(job(**spec))

        rerun = self.run_job(job(**dict(
            spec, compile={**spec["compile"], "command": ["compile", "v2"]}
        )))

        self.assertTrue(self.ran(rerun, "compile"), "the source was reused")
        self.assertFalse(self.ran(rerun, "link"),
                         "the dependent reran on an unchanged artifact")

    def test_a_failed_task_is_never_served(self):
        spec = job(t={"command": ["broken"]})

        first = self.run_job(spec)
        second = self.run_job(spec)

        self.assertEqual("FAILED", first["state"])
        self.assertEqual("FAILED", second["state"])
        self.assertTrue(self.ran(second), "a failure was taken from the cache")


class CacheOptOutTest(CacheTestCase):
    def test_a_task_that_opted_out_runs_every_time(self):
        spec = job(t={"command": ["build-each-time"], "outputs": ["out.txt"],
                      "cache": False})

        first = self.run_job(spec)
        second = self.run_job(spec)

        self.assertTrue(self.ran(first))
        self.assertTrue(self.ran(second), "an opted-out task was served")
        self.assertNotIn("cache hit", self.reason(second))

    def test_an_opted_out_task_leaves_nothing_for_others_to_reuse(self):
        self.run_job(job(t={"command": ["build-private"],
                           "outputs": ["out.txt"], "cache": False}))

        cached = self.run_job(job(t={"command": ["build-private"],
                                     "outputs": ["out.txt"]}))

        self.assertTrue(self.ran(cached), "it was served by an opted-out task")


class CacheJournalTest(CacheTestCase):
    def test_a_cached_job_is_still_successful_after_a_restart(self):
        spec = job(t={"command": ["build-journaled"], "outputs": ["out.txt"]})
        self.run_job(spec)
        hit = self.run_job(spec)
        self.assertFalse(self.ran(hit))

        # A hit is journaled as a result, never as a running attempt: nothing
        # was launched, so replaying the journal rebuilds the job as finished
        # (plan/16, D16.3).
        restarted = Coordinator.options(scheduling_strategy=self.here).remote(
            cache_executor, self.store, self.logs, self.journal,
        )
        self.addCleanup(ray.kill, restarted)

        view = ray.get(restarted.status.remote(hit["job_id"]), timeout=10)

        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(hit["tasks"]["t"]["outputs"],
                         view["tasks"]["t"]["outputs"])


if __name__ == "__main__":
    unittest.main()
