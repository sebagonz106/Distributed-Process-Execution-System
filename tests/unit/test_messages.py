import dataclasses
import pickle
import unittest

from dpes.messages import (
    Attempt,
    Evidence,
    ExecutorFailure,
    LogChunk,
    ResultKey,
    TaskResult,
)


def make_attempt(**changes):
    fields = {
        "job_id": "job-1",
        "task": "compile",
        "round": 0,
        "execution_id": "exec-1",
        "command": ("gcc", "-c", "main.c"),
        "env": {"CC": "gcc"},
        "inputs": {"main.c": "sha256:aaa"},
        "outputs": ("main.o",),
        "timeout_s": 60.0,
    }
    return Attempt(**{**fields, **changes})


def make_result(**changes):
    fields = {
        "execution_id": "exec-1",
        "exit_code": 0,
        "timed_out": False,
        "outputs": {"main.o": "sha256:bbb"},
    }
    return TaskResult(**{**fields, **changes})


ONE_OF_EACH = (
    make_attempt(),
    make_result(),
    ExecutorFailure("exec-1", Evidence.DIGEST_MISMATCH, "main.c"),
    LogChunk("exec-1", 0, b"hello\n"),
    ResultKey("job-1", "compile", 0),
)


class ResultKeyTest(unittest.TestCase):
    def test_infrastructure_retries_of_a_round_share_the_result_key(self):
        first = make_attempt(execution_id="exec-1")
        retry = make_attempt(execution_id="exec-2")

        self.assertEqual(first.result_key, retry.result_key)

    def test_a_new_round_has_its_own_result_key(self):
        self.assertNotEqual(
            make_attempt(round=0).result_key, make_attempt(round=1).result_key
        )

    def test_the_same_task_name_in_another_job_has_its_own_result_key(self):
        self.assertNotEqual(
            make_attempt(job_id="job-1").result_key,
            make_attempt(job_id="job-2").result_key,
        )

    def test_another_task_of_the_job_has_its_own_result_key(self):
        self.assertNotEqual(
            make_attempt(task="compile").result_key,
            make_attempt(task="link").result_key,
        )


class TaskResultTest(unittest.TestCase):
    def test_exit_code_zero_with_every_declared_output_succeeded(self):
        self.assertTrue(make_result().succeeded)

    def test_non_zero_exit_code_did_not_succeed(self):
        self.assertFalse(make_result(exit_code=1).succeeded)

    def test_timed_out_did_not_succeed_even_with_exit_code_zero(self):
        self.assertFalse(make_result(timed_out=True).succeeded)

    def test_missing_declared_output_did_not_succeed(self):
        self.assertFalse(
            make_result(outputs={}, missing_outputs=("main.o",)).succeeded
        )


class WireTest(unittest.TestCase):
    def test_every_message_survives_the_trip_between_processes(self):
        for message in ONE_OF_EACH:
            with self.subTest(type(message).__name__):
                self.assertEqual(message, pickle.loads(pickle.dumps(message)))

    def test_a_sent_message_cannot_have_a_field_reassigned(self):
        for message in ONE_OF_EACH:
            field = dataclasses.fields(message)[0].name
            with self.subTest(type(message).__name__):
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(message, field, "changed")


if __name__ == "__main__":
    unittest.main()
