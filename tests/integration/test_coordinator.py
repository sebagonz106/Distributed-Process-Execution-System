"""The coordinator actor on the cluster, with a stand-in executor.

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
from dpes.messages import ResultKey, TaskResult
from dpes.spec import SpecError, parse_job
from dpes.store import Journal


def stand_in_executor(attempt, store):
    """Obeys the command's name instead of running it. Defined at module
    level but shipped by value: tests/ is not on the workers' path."""
    import os
    import time

    from dpes.messages import TaskResult

    program = attempt.command[0]
    number = int(attempt.execution_id.rsplit("/", 1)[1])
    if program == "die" or (program == "die-once" and number == 1):
        os._exit(1)
    if program == "sleep":
        time.sleep(15)
    if program == "hang-once" and number == 1:
        time.sleep(60)  # far past any deadline a test waits for
    if program == "garbage":
        return "not a result"
    if program == "save-then-hang" and number == 1:
        # The task ends and is saved; then its coordinator dies (plan/14).
        saved = TaskResult(attempt.execution_id, 0, False, {},
                           output_tail="saved\n")
        store.save_result(attempt.result_key, saved)
        time.sleep(15)
    if program == "lose-ack" and number == 1:
        # The task ends and is saved, but its answer never arrives.
        saved = TaskResult(attempt.execution_id, 0, False, {},
                           output_tail="saved\n")
        store.save_result(attempt.result_key, saved)
        os._exit(1)
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
        # Quiet: these check states; tests/host checks the logs.
        ray.init(address="auto", logging_level="ERROR",
                 ignore_reinit_error=True, log_to_driver=False)
        folder = tempfile.mkdtemp(prefix="dpes-store-")
        # On this node, as on the control node: the folder is only here, and
        # a restarted coordinator must find its journal again (plan/14).
        here = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
        cls.here = here
        cls.store = BlobStore.options(scheduling_strategy=here).remote(
            os.path.join(folder, "blobs"),
            os.path.join(folder, "results.sqlite"),
        )
        cls.logs = LogStore.options(scheduling_strategy=here).remote(
            os.path.join(folder, "logs"),
        )
        cls.coordinator = Coordinator.options(scheduling_strategy=here).remote(
            stand_in_executor, cls.store, cls.logs,
            os.path.join(folder, "coordinator.sqlite"),
            queue_timeout_s=cls.queue_timeout_s,
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def status(self, job_id):
        return ray.get(self.coordinator.status.remote(job_id), timeout=10)

    def events(self, job_id):
        return ray.get(self.coordinator.events.remote(job_id), timeout=10)

    def submit(self, spec):
        return ray.get(self.coordinator.submit.remote(spec))

    def run_job(self, spec, timeout_s=60):
        job_id = self.submit(spec)
        deadline = time.monotonic() + timeout_s
        view = self.status(job_id)
        while view["state"] in ("PENDING", "RUNNING"):
            self.assertLess(time.monotonic(), deadline,
                            "the job never finished")
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

    def test_an_unknown_job_has_no_events(self):
        self.assertIsNone(self.events("no-such-job"))

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
            _Client(self.coordinator, self.store, self.logs).submit(spec)

        self.assertIn("gpu", caught.exception.errors[0])

    def test_a_blob_goes_into_the_store_and_comes_back(self):
        client = _Client(self.coordinator, self.store, self.logs)

        client.put_blob(digest(b"hola"), b"hola")

        self.assertEqual(b"hola", client.get_blob(digest(b"hola")))
        self.assertIsNone(client.get_blob(digest(b"never")))

    def test_a_job_whose_inputs_were_never_uploaded_is_refused(self):
        spec = job(t={"command": ["0"], "inputs": {"a.c": digest(b"no")}})

        with self.assertRaises(SpecError) as caught:
            _Client(self.coordinator, self.store, self.logs).submit(spec)

        self.assertIn("never uploaded", caught.exception.errors[0])


class RetryTest(CoordinatorTestCase):
    def test_a_worker_that_dies_once_is_retried_and_the_job_succeeds(self):
        view = self.run_one("die-once")

        attempts = view["tasks"]["t"]["attempts"]
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(["WORKER", "SUCCEEDED"],
                         [a["outcome"] for a in attempts])
        self.assertEqual(0.0, attempts[0]["retry_in_s"])

    def test_the_feed_shows_the_failure_the_retry_and_the_end(self):
        view = self.run_one("die-once")

        events = self.events(view["job_id"])

        self.assertEqual(
            ["TASK_LAUNCHED", "ATTEMPT_FAILED", "RETRY_SCHEDULED",
             "ATTEMPT_SUCCEEDED", "JOB_FINISHED"],
            [e["type"] for e in events],
        )
        self.assertEqual("WORKER", events[1]["payload"]["failure_class"])
        self.assertEqual(events[3]["execution_id"],
                         events[2]["payload"]["next_execution_id"])
        self.assertEqual(f"{view['job_id']}/t/0/2",
                         events[3]["execution_id"])

    def test_an_attempt_silent_past_its_deadline_is_a_network_failure(self):
        started = time.monotonic()

        view = self.run_one("hang-once", timeout_s=1)

        attempts = view["tasks"]["t"]["attempts"]
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(("NETWORK", "deadline"),
                         (attempts[0]["outcome"], attempts[0]["reason"]))
        self.assertGreater(attempts[0]["retry_in_s"], 0)
        # Well short of the 60 s hang, with room for the slow worker
        # start-ups of a cluster that ran the whole suite (plan/14).
        self.assertLess(time.monotonic() - started, 30, "deadline too late")


class LostConfirmationTest(CoordinatorTestCase):
    """plan/12: the task ended and was saved, but its answer was lost."""

    def test_a_saved_result_is_adopted_instead_of_running_again(self):
        view = self.run_one("lose-ack")

        task = view["tasks"]["t"]
        (attempt,) = task["attempts"]
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual("SUCCEEDED", attempt["outcome"])
        self.assertIn("adopted by reconciliation", attempt["reason"])
        self.assertEqual("saved\n", task["output"])

    def test_the_same_key_and_job_give_the_same_job_id(self):
        spec = job(t={"command": ["0"]})

        first = ray.get(self.coordinator.submit.remote(spec, "key-1"))
        again = ray.get(self.coordinator.submit.remote(spec, "key-1"))

        self.assertEqual(first, again)

    def test_the_same_key_for_another_job_is_refused(self):
        _Client(self.coordinator, self.store, self.logs).submit(
            job(t={"command": ["0"]}), "key-2")

        with self.assertRaises(SpecError) as caught:
            _Client(self.coordinator, self.store, self.logs).submit(
                job(t={"command": ["3"]}), "key-2")

        self.assertIn("key-2", caught.exception.errors[0])


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


class RecoveryTest(CoordinatorTestCase):
    """plan/14: the coordinator's process is killed and Ray starts it again.

    Each test proves the restart happened: a job that was in flight shows it
    in its history.
    """

    def poll(self, job_id, done, timeout_s=60):
        """The first view of the job for which `done` holds."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                view = self.status(job_id)
                if done(view):
                    return view
            except ray.exceptions.RayActorError:
                pass  # this call reached the coordinator as it died
            self.assertLess(time.monotonic(), deadline, "it never happened")
            time.sleep(0.2)

    def finished(self, job_id):
        return self.poll(job_id, lambda v: v["state"] not in ("PENDING",
                                                              "RUNNING"))

    def kill(self):
        ray.kill(self.coordinator, no_restart=False)

    def kill_while_running(self, spec):
        job_id = self.submit(spec)
        self.poll(job_id, lambda v: v["tasks"]["t"]["state"] == "RUNNING")
        time.sleep(1)  # its launch is journaled and the attempt is out
        self.kill()
        return job_id

    def test_an_attempt_lost_in_a_restart_is_retried_as_a_worker_failure(self):
        job_id = self.kill_while_running(job(t={"command": ["hang-once"]}))

        task = self.finished(job_id)["tasks"]["t"]

        lost, retried = task["attempts"]
        self.assertEqual(("WORKER", "coordinator restart"),
                         (lost["outcome"], lost["reason"]))
        self.assertEqual(("SUCCEEDED", "SUCCEEDED"),
                         (retried["outcome"], task["state"]))

    def test_without_a_budget_a_lost_attempt_is_not_run_again(self):
        spec = job(t={"command": ["hang-once"], "retries": {"worker": 0}})

        view = self.finished(self.kill_while_running(spec))

        (lost,) = view["tasks"]["t"]["attempts"]
        self.assertEqual(("ERROR", "coordinator restart"),
                         (view["state"], lost["reason"]))

    def test_a_result_saved_before_the_restart_is_adopted(self):
        job_id = self.submit(job(t={"command": ["save-then-hang"]}))
        key = ResultKey(job_id, "t", 0)
        self.poll(job_id, lambda _: ray.get(self.store.result.remote(key)))
        self.kill()

        task = self.finished(job_id)["tasks"]["t"]

        (attempt,) = task["attempts"]  # the command did not run again
        self.assertEqual(
            ("SUCCEEDED", "adopted by reconciliation after coordinator "
                          "restart", "saved\n"),
            (attempt["outcome"], attempt["reason"], task["output"]),
        )

    def test_finished_jobs_and_their_keys_survive_a_restart(self):
        spec = job(t={"command": ["0"]})
        done = ray.get(self.coordinator.submit.remote(spec, "kept-key"))
        before = self.finished(done)

        proof = self.finished(
            self.kill_while_running(job(t={"command": ["hang-once"]})))

        self.assertEqual("coordinator restart",
                         proof["tasks"]["t"]["attempts"][0]["reason"])
        self.assertEqual(before, self.status(done))
        self.assertEqual(
            done, ray.get(self.coordinator.submit.remote(spec, "kept-key")))

    def test_the_feed_of_a_finished_job_survives_unchanged(self):
        job_id = self.submit(job(t={"command": ["0"]}))
        self.finished(job_id)
        before = self.events(job_id)
        self.assertEqual(["TASK_LAUNCHED", "ATTEMPT_SUCCEEDED",
                          "JOB_FINISHED"],
                         [e["type"] for e in before])

        self.finished(
            self.kill_while_running(job(t={"command": ["hang-once"]})))

        self.assertEqual(before, self.events(job_id))


class ReplayedJournalTest(CoordinatorTestCase):
    """A restart that finds a journal whose last call had no successor."""

    def actor(self, path):
        actor = Coordinator.options(scheduling_strategy=self.here).remote(
            stand_in_executor, self.store, self.logs, path
        )
        self.addCleanup(ray.kill, actor)
        return actor

    def test_a_job_that_finished_just_before_a_crash_records_its_end(self):
        folder = tempfile.mkdtemp(prefix="dpes-journal-")
        path = os.path.join(folder, "coordinator.sqlite")
        journal = Journal(path)
        journal.append("j1", "accepted", job(t={"command": ["0"]}), None)
        journal.append("j1", "running", "j1/t/0/1")
        journal.append("j1", "result", TaskResult("j1/t/0/1", 0, False, {}),
                       "j1/t/0/1", "")
        journal.close()  # the crash lands before the `finished` call

        feed = ray.get(self.actor(path).events.remote("j1"), timeout=10)

        # The job is over, so its feed must end where it did (D15.17), even
        # though the previous instance never got to journal the end.
        self.assertEqual(["TASK_LAUNCHED", "ATTEMPT_SUCCEEDED",
                          "JOB_FINISHED"],
                         [e["type"] for e in feed])


if __name__ == "__main__":
    unittest.main()
