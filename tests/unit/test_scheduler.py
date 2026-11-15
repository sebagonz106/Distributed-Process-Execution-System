import json
import unittest

from dpes.messages import Attempt, TaskResult
from dpes.scheduler import Job, State
from dpes.spec import JobSpec, TaskSpec

HELLO = TaskSpec(("echo", "hi"), {"A": "1"}, 30.0)


def make_job(*names):
    return Job("job-1", JobSpec({name: HELLO for name in names or ("hello",)}))


def result(execution_id, exit_code=0, timed_out=False):
    return TaskResult(execution_id, exit_code, timed_out, {})


class StartTest(unittest.TestCase):
    def test_a_new_job_is_pending(self):
        self.assertEqual(State.PENDING, make_job().state)

    def test_start_launches_the_task_with_its_spec(self):
        job = make_job()

        attempts = job.start()

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
        self.assertEqual([expected], attempts)
        self.assertEqual(State.RUNNING, job.state)

    def test_starting_twice_launches_nothing_new(self):
        job = make_job()
        job.start()

        self.assertEqual([], job.start())


class OutcomeTest(unittest.TestCase):
    def setUp(self):
        self.job = make_job()
        (attempt,) = self.job.start()
        self.eid = attempt.execution_id

    def task_view(self):
        return self.job.view()["tasks"]["hello"]

    def test_a_successful_result_succeeds(self):
        self.job.on_result(result(self.eid))

        self.assertEqual(State.SUCCEEDED, self.job.state)
        self.assertEqual(0, self.task_view()["exit_code"])

    def test_the_output_of_the_command_is_shown(self):
        finished = TaskResult(self.eid, 0, False, {}, output_tail="hi\n")

        self.job.on_result(finished)

        self.assertEqual("hi\n", self.task_view()["output"])

    def test_a_task_without_result_shows_no_output(self):
        self.assertEqual("", self.task_view()["output"])

    def test_a_non_zero_exit_code_fails_the_task(self):
        self.job.on_result(result(self.eid, exit_code=3))

        self.assertEqual(State.FAILED, self.job.state)
        self.assertEqual(3, self.task_view()["exit_code"])

    def test_a_timed_out_command_fails_the_task(self):
        self.job.on_result(result(self.eid, exit_code=-9, timed_out=True))

        self.assertEqual(State.FAILED, self.job.state)
        self.assertTrue(self.task_view()["timed_out"])

    def test_no_result_is_an_infrastructure_error(self):
        self.job.on_failure(self.eid, "WorkerCrashedError")

        self.assertEqual(State.ERROR, self.job.state)
        self.assertEqual("WorkerCrashedError", self.task_view()["error"])

    def test_an_event_for_an_unknown_attempt_changes_nothing(self):
        self.job.on_result(result("job-1/hello/0/99"))
        self.job.on_failure("job-2/hello/0/1", "late")

        self.assertEqual(State.RUNNING, self.job.state)

    def test_an_event_after_the_task_finished_changes_nothing(self):
        self.job.on_result(result(self.eid))
        self.job.on_failure(self.eid, "NodeDiedError arriving late")
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
        for attempt in job.start():
            outcome = outcomes[attempt.task]
            if outcome == "ok":
                job.on_result(result(attempt.execution_id))
            elif outcome == "fail":
                job.on_result(result(attempt.execution_id, exit_code=1))
            elif outcome == "error":
                job.on_failure(attempt.execution_id, "lost")
        return job.state

    def test_a_task_failure_outranks_an_infrastructure_error(self):
        self.assertEqual(State.FAILED, self.finish({"a": "fail", "b": "error"}))

    def test_an_infrastructure_error_outranks_success(self):
        self.assertEqual(State.ERROR, self.finish({"a": "ok", "b": "error"}))

    def test_the_job_runs_while_any_task_runs(self):
        self.assertEqual(State.RUNNING, self.finish({"a": "ok", "b": "running"}))


if __name__ == "__main__":
    unittest.main()
