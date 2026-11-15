import json
import unittest

from dpes.failures import FailureClass, retry_delay
from dpes.messages import Attempt, TaskResult
from dpes.scheduler import Job, Launch, State
from dpes.spec import JobSpec, TaskSpec

TASK, WORKER, NETWORK = FailureClass
NO_RETRIES = {"task": 0, "worker": 0, "network": 0}


def task_spec(needs=(), retries=None):
    return TaskSpec(("echo", "hi"), {"A": "1"}, 30.0, tuple(needs),
                    retries=retries or dict(NO_RETRIES))


def make_job(*names, retries=None, rand=0.5):
    tasks = {name: task_spec(retries=retries) for name in names or ("hello",)}
    return Job("job-1", JobSpec(tasks), rand=lambda: rand)


def result(execution_id, exit_code=0, timed_out=False, output_tail=""):
    return TaskResult(execution_id, exit_code, timed_out, {},
                      output_tail=output_tail)


def only(launches):
    (launch,) = launches
    return launch


class StartTest(unittest.TestCase):
    def test_a_new_job_is_pending(self):
        self.assertEqual(State.PENDING, make_job().state)

    def test_start_launches_the_task_with_its_spec_and_no_wait(self):
        job = make_job()

        launches = job.start()

        expected = Attempt(
            job_id="job-1",
            task="hello",
            round=0,
            execution_id="job-1/hello/0/1",
            command=("echo", "hi"),
            env={"A": "1"},
            inputs={},
            outputs=(),
            timeout_s=30.0,
        )
        self.assertEqual([Launch(expected, 0.0)], launches)
        self.assertEqual(State.RUNNING, job.state)

    def test_starting_twice_launches_nothing_new(self):
        job = make_job()
        job.start()

        self.assertEqual([], job.start())


class OutcomeTest(unittest.TestCase):
    def setUp(self):
        self.job = make_job()
        self.eid = only(self.job.start()).attempt.execution_id

    def task_view(self):
        return self.job.view()["tasks"]["hello"]

    def test_a_successful_result_succeeds(self):
        self.job.on_result(result(self.eid))

        self.assertEqual(State.SUCCEEDED, self.job.state)
        self.assertEqual(0, self.task_view()["exit_code"])

    def test_the_output_of_the_command_is_shown(self):
        self.job.on_result(result(self.eid, output_tail="hi\n"))

        self.assertEqual("hi\n", self.task_view()["output"])

    def test_a_task_without_result_shows_no_output(self):
        self.assertEqual("", self.task_view()["output"])

    def test_a_non_zero_exit_code_fails_the_task(self):
        self.job.on_result(result(self.eid, exit_code=3))

        self.assertEqual(State.FAILED, self.job.state)
        self.assertEqual(3, self.task_view()["exit_code"])
        self.assertEqual("TASK", self.task_view()["failure_class"])

    def test_a_timed_out_command_fails_the_task(self):
        self.job.on_result(result(self.eid, exit_code=-9, timed_out=True))

        self.assertEqual(State.FAILED, self.job.state)
        self.assertTrue(self.task_view()["timed_out"])

    def test_no_result_is_an_infrastructure_error(self):
        self.job.on_failure(self.eid, WORKER, "WorkerCrashedError")

        self.assertEqual(State.ERROR, self.job.state)
        self.assertEqual("WorkerCrashedError", self.task_view()["error"])
        self.assertEqual("WORKER", self.task_view()["failure_class"])

    def test_an_event_for_an_unknown_attempt_changes_nothing(self):
        self.assertEqual([], self.job.on_result(result("job-1/hello/0/99")))
        self.assertEqual([], self.job.on_failure("job-2/x/0/1", WORKER, "x"))

        self.assertEqual(State.RUNNING, self.job.state)

    def test_an_event_after_the_task_finished_changes_nothing(self):
        self.job.on_result(result(self.eid))
        self.job.on_failure(self.eid, WORKER, "NodeDiedError arriving late")
        self.job.on_result(result(self.eid, exit_code=1))

        self.assertEqual(State.SUCCEEDED, self.job.state)
        self.assertIsNone(self.task_view()["error"])

    def test_the_view_is_plain_json(self):
        self.job.on_result(result(self.eid))

        view = json.loads(json.dumps(self.job.view()))

        self.assertEqual("job-1", view["job_id"])
        self.assertEqual("SUCCEEDED", view["state"])
        self.assertEqual(self.eid, view["tasks"]["hello"]["execution_id"])


class JobStateTest(unittest.TestCase):
    def finish(self, outcomes):
        job = make_job(*outcomes)
        for launch in job.start():
            eid = launch.attempt.execution_id
            outcome = outcomes[launch.attempt.task]
            if outcome == "ok":
                job.on_result(result(eid))
            elif outcome == "fail":
                job.on_result(result(eid, exit_code=1))
            elif outcome == "error":
                job.on_failure(eid, WORKER, "lost")
        return job.state

    def test_a_task_failure_outranks_an_infrastructure_error(self):
        self.assertEqual(State.FAILED, self.finish({"a": "fail", "b": "error"}))

    def test_an_infrastructure_error_outranks_success(self):
        self.assertEqual(State.ERROR, self.finish({"a": "ok", "b": "error"}))

    def test_the_job_runs_while_any_task_runs(self):
        self.assertEqual(State.RUNNING, self.finish({"a": "ok", "b": "running"}))


def dag_job(**needs):
    """A job whose tasks are `name=[dependencies]`, in that order."""
    tasks = {name: task_spec(needs=deps) for name, deps in needs.items()}
    return Job("job-1", JobSpec(tasks))


def names(launches):
    return [launch.attempt.task for launch in launches]


class DagTest(unittest.TestCase):
    def setUp(self):
        self.job = dag_job(a=[], b=["a"], c=["a"], d=["b", "c"])
        self.eid = {}
        self.track(self.job.start())

    def track(self, launches):
        self.eid.update({l.attempt.task: l.attempt.execution_id
                         for l in launches})
        return names(launches)

    def succeed(self, task):
        return self.track(self.job.on_result(result(self.eid[task])))

    def fail(self, task):
        return names(self.job.on_result(result(self.eid[task], exit_code=1)))

    def state(self, task):
        return self.job.view()["tasks"][task]["state"]

    def test_only_tasks_without_dependencies_start(self):
        self.assertEqual(["a"], list(self.eid))

    def test_independent_tasks_are_launched_together(self):
        self.assertEqual(["b", "c"], self.succeed("a"))

    def test_a_task_waits_for_all_its_dependencies(self):
        self.succeed("a")

        self.assertEqual([], self.succeed("b"))
        self.assertEqual("PENDING", self.state("d"))
        self.assertEqual(["d"], self.succeed("c"))

    def test_a_failure_skips_its_dependents_and_the_rest_goes_on(self):
        self.succeed("a")

        self.assertEqual([], self.fail("b"))
        self.assertEqual("SKIPPED", self.state("d"))
        self.assertIn("'b'", self.job.view()["tasks"]["d"]["error"])
        self.assertEqual("RUNNING", self.state("c"))

        self.assertEqual([], self.succeed("c"))
        self.assertEqual(State.FAILED, self.job.state)

    def test_skipping_reaches_every_transitive_dependent(self):
        job = dag_job(a=[], b=["a"], c=["b"], x=[])
        eid = {l.attempt.task: l.attempt.execution_id for l in job.start()}

        job.on_failure(eid["a"], WORKER, "WorkerCrashedError")

        tasks = job.view()["tasks"]
        self.assertEqual("SKIPPED", tasks["b"]["state"])
        self.assertEqual("SKIPPED", tasks["c"]["state"])
        self.assertEqual("RUNNING", tasks["x"]["state"])

    def test_a_job_with_a_skipped_task_never_succeeds(self):
        job = dag_job(a=[], b=["a"])
        launch = only(job.start())

        job.on_failure(launch.attempt.execution_id, WORKER, "lost")

        self.assertEqual(State.ERROR, job.state)


class RetryTest(unittest.TestCase):
    def setUp(self):
        self.budget = {"task": 0, "worker": 0, "network": 0}

    def start(self, rand=0.5, **budget):
        self.job = make_job(retries={**self.budget, **budget}, rand=rand)
        return only(self.job.start()).attempt.execution_id

    def task_view(self):
        return self.job.view()["tasks"]["hello"]

    def test_a_task_failure_is_not_retried_by_default(self):
        eid = self.start()

        self.assertEqual([], self.job.on_result(result(eid, exit_code=1)))
        self.assertEqual(State.FAILED, self.job.state)

    def test_a_declared_task_retry_opens_a_new_round(self):
        eid = self.start(task=1)

        retry = only(self.job.on_result(result(eid, exit_code=1)))

        self.assertEqual(1, retry.attempt.round)
        self.assertEqual("job-1/hello/1/2", retry.attempt.execution_id)
        self.assertEqual(0.0, retry.delay_s)

    def test_a_worker_failure_is_retried_at_once_in_the_same_round(self):
        eid = self.start(worker=1)

        retry = only(self.job.on_failure(eid, WORKER, "NodeDiedError"))

        self.assertEqual("job-1/hello/0/2", retry.attempt.execution_id)
        self.assertEqual(0.0, retry.delay_s)

    def test_a_retry_waits_in_retry_wait_until_it_is_launched(self):
        eid = self.start(network=1)
        retry = only(self.job.on_failure(eid, NETWORK, "deadline"))

        self.assertEqual("RETRY_WAIT", self.task_view()["state"])
        self.assertEqual(State.RUNNING, self.job.state)

        self.job.running(retry.attempt.execution_id)

        self.assertEqual("RUNNING", self.task_view()["state"])

    def test_network_retries_wait_longer_each_time(self):
        eid = self.start(rand=0.5, network=3)
        delays = []
        for _ in range(3):
            retry = only(self.job.on_failure(eid, NETWORK, "deadline"))
            delays.append(retry.delay_s)
            eid = retry.attempt.execution_id

        expected = [retry_delay(NETWORK, n, 0.5) for n in range(3)]
        self.assertEqual(expected, delays)
        self.assertEqual(sorted(delays), delays)

    def test_each_class_spends_only_its_own_budget(self):
        eid = self.start(worker=1, network=1)

        eid = only(self.job.on_failure(eid, NETWORK, "x")).attempt.execution_id
        eid = only(self.job.on_failure(eid, WORKER, "y")).attempt.execution_id
        self.assertEqual([], self.job.on_failure(eid, NETWORK, "z"))

        self.assertEqual(State.ERROR, self.job.state)
        self.assertEqual("NETWORK", self.task_view()["failure_class"])

    def test_the_history_keeps_the_evidence_of_every_attempt(self):
        eid = self.start(network=1)
        retry = only(self.job.on_failure(eid, NETWORK, "deadline"))
        self.job.on_result(result(retry.attempt.execution_id))

        history = self.task_view()["attempts"]

        self.assertEqual(
            [
                {"execution_id": eid, "outcome": "NETWORK",
                 "reason": "deadline", "retry_in_s": retry.delay_s},
                {"execution_id": retry.attempt.execution_id,
                 "outcome": "SUCCEEDED", "reason": "", "retry_in_s": None},
            ],
            history,
        )
        self.assertIsNone(self.task_view()["error"])

    def test_a_late_result_of_a_replaced_attempt_is_ignored(self):
        eid = self.start(worker=1)
        self.job.on_failure(eid, WORKER, "NodeDiedError")

        self.assertEqual([], self.job.on_result(result(eid)))
        self.assertEqual("RETRY_WAIT", self.task_view()["state"])

    def test_dependents_wait_for_the_retry_to_succeed(self):
        tasks = {"a": task_spec(retries={**self.budget, "worker": 1}),
                 "b": task_spec(needs=["a"])}
        job = Job("job-1", JobSpec(tasks))
        eid = only(job.start()).attempt.execution_id

        retry = only(job.on_failure(eid, WORKER, "NodeDiedError"))
        self.assertEqual(["a"], names([retry]))

        unblocked = job.on_result(result(retry.attempt.execution_id))
        self.assertEqual(["b"], names(unblocked))


if __name__ == "__main__":
    unittest.main()
