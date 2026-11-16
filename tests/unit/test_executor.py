"""The executor of plan/07, D07.17. Linux only, no Ray needed."""

import os
import sys
import unittest
from unittest import mock

if sys.platform != "linux":
    raise unittest.SkipTest("the executor needs Linux process groups")

from dpes import executor  # imported after the skip: it needs Linux
from dpes.hashing import digest
from dpes.log import Formatter
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult


class Blobs(dict):
    """In-memory store with the executor's interface (plan/11, D11.5;
    plan/12, D12.1): blobs, and results where the first writer wins."""

    def __init__(self, *args):
        super().__init__(*args)
        self.results = {}
        self.saves = []

    def get(self, name):
        return super().get(name)

    def put(self, data):
        self[digest(data)] = data
        return digest(data)

    def save_result(self, key, result):
        self.saves.append((key, result))
        return self.results.setdefault(key, result)


def execute(attempt, store=None):
    return executor.execute(attempt, Blobs() if store is None else store)


def attempt(script, **changes):
    fields = {
        "job_id": "job-1",
        "task": "t",
        "round": 0,
        "execution_id": "job-1/t/0/1",
        "command": ("sh", "-c", script),
        "env": {},
        "inputs": {},
        "outputs": (),
        "timeout_s": 10.0,
    }
    return Attempt(**{**fields, **changes})


class ExecuteTest(unittest.TestCase):
    def test_a_finished_command_is_a_result_of_this_attempt(self):
        result = execute(attempt("echo hi; exit 4"))

        self.assertIsInstance(result, TaskResult)
        self.assertEqual("job-1/t/0/1", result.execution_id)
        self.assertEqual((4, "hi\n"), (result.exit_code, result.output_tail))

    def test_the_declared_environment_reaches_the_command(self):
        result = execute(attempt("echo $NAME", env={"NAME": "value"}))

        self.assertEqual("value\n", result.output_tail)

    def test_every_attempt_starts_in_an_empty_workspace(self):
        execute(attempt("touch leftover"))

        self.assertEqual("", execute(attempt("ls -A")).output_tail)

    def test_the_workspace_is_destroyed_afterwards(self):
        workspace = execute(attempt("pwd")).output_tail.strip()

        self.assertFalse(os.path.exists(workspace))

    def test_no_workspace_is_reported_as_executor_failure(self):
        with mock.patch("tempfile.tempdir", "/nonexistent/dpes"):
            outcome = execute(attempt("true"))

        self.assertIsInstance(outcome, ExecutorFailure)
        self.assertEqual(Evidence.WORKSPACE_FAILED, outcome.evidence)


class SavesFirst(Blobs):
    """Fails the first `failures` saves, as a lost store reply would."""

    def __init__(self, failures):
        super().__init__()
        self.failures = failures

    def save_result(self, key, result):
        self.saves.append((key, result))
        if len(self.saves) <= self.failures:
            raise ConnectionError("store unreachable")
        return self.results.setdefault(key, result)


@mock.patch("dpes.executor._SAVE_WAITS", (0, 0, 0))
class ConfirmationTest(unittest.TestCase):
    def test_a_result_is_confirmed_before_it_is_returned(self):
        store = Blobs()

        result = execute(attempt("echo hi"), store)

        self.assertEqual({attempt("x").result_key: result}, store.results)

    def test_a_failing_command_is_confirmed_too(self):
        store = Blobs()

        result = execute(attempt("exit 3"), store)

        self.assertEqual(3, store.results[attempt("x").result_key].exit_code)
        self.assertEqual(3, result.exit_code)

    def test_the_winner_is_returned_when_another_attempt_saved_first(self):
        store = Blobs()
        earlier = TaskResult("job-1/t/0/1", 0, False, {}, output_tail="old\n")
        store.results[attempt("x").result_key] = earlier

        result = execute(attempt("echo new", execution_id="job-1/t/0/2"),
                         store)

        self.assertEqual(earlier, result)

    def test_a_failed_save_is_retried_with_the_same_key_and_result(self):
        store = SavesFirst(failures=2)

        result = execute(attempt("echo hi"), store)

        self.assertIsInstance(result, TaskResult)
        self.assertEqual(3, len(store.saves))
        self.assertTrue(all(save == store.saves[0] for save in store.saves))

    def test_a_save_that_never_succeeds_is_a_result_save_failure(self):
        failure = execute(attempt("echo hi"), SavesFirst(failures=99))

        self.assertIsInstance(failure, ExecutorFailure)
        self.assertEqual(Evidence.RESULT_SAVE_FAILED, failure.evidence)

    def test_no_result_is_saved_when_the_executor_itself_failed(self):
        store = Blobs()

        execute(attempt("true", inputs={"a": digest(b"never uploaded")}),
                store)

        self.assertEqual([], store.saves)


class Broken:
    def get(self, name):
        raise ConnectionError("store unreachable")

    def put(self, data):
        raise ConnectionError("store unreachable")


class InputsOutputsTest(unittest.TestCase):
    def test_inputs_are_placed_at_their_paths_before_the_command(self):
        store = Blobs()
        name = store.put(b"hola\n")

        result = execute(attempt("cat src/a.txt", inputs={"src/a.txt": name}),
                         store)

        self.assertEqual("hola\n", result.output_tail)

    def test_an_input_whose_content_does_not_match_is_refused(self):
        store = Blobs({digest(b"expected"): b"tampered"})

        failure = execute(attempt("true",
                                  inputs={"a": digest(b"expected")}), store)

        self.assertEqual(Evidence.DIGEST_MISMATCH, failure.evidence)

    def test_an_input_that_cannot_be_fetched_is_unavailable(self):
        absent = attempt("true", inputs={"a": digest(b"x")})
        for store in (Blobs(), Broken()):
            with self.subTest(store=type(store).__name__):
                failure = execute(absent, store)
                self.assertEqual(Evidence.INPUT_UNAVAILABLE, failure.evidence)

    def test_declared_outputs_are_stored_and_listed_by_digest(self):
        store = Blobs()

        result = execute(attempt("echo hi > out.txt; echo no > extra.txt",
                                 outputs=("out.txt",)), store)

        self.assertEqual({"out.txt": digest(b"hi\n")}, result.outputs)
        self.assertEqual(b"hi\n", store[digest(b"hi\n")])
        self.assertNotIn(digest(b"no\n"), store)

    def test_a_declared_output_that_is_missing_fails_the_task(self):
        result = execute(attempt("true", outputs=("out.txt",)))

        self.assertEqual(("out.txt",), result.missing_outputs)
        self.assertFalse(result.succeeded)

    def test_an_output_is_never_read_through_a_link(self):
        for script, path in (("ln -s /etc/hostname out", "out"),
                             ("ln -s /etc d", "d/hostname")):
            with self.subTest(path=path):
                result = execute(attempt(script, outputs=(path,)))
                self.assertEqual((path,), result.missing_outputs)

    def test_an_output_that_cannot_be_uploaded_is_a_failed_transfer(self):
        failure = execute(attempt("echo hi > out", outputs=("out",)),
                          Broken())

        self.assertEqual(Evidence.OUTPUT_UPLOAD_FAILED, failure.evidence)

    def test_the_command_can_write_inside_a_folder_that_came_as_input(self):
        store = Blobs()
        name = store.put(b"int main(){}\n")

        result = execute(attempt("touch build/new && echo ok",
                                 inputs={"build/a.c": name}), store)

        self.assertEqual((0, "ok\n"), (result.exit_code, result.output_tail))


class ExecutorLogTest(unittest.TestCase):
    def logged(self, level, run):
        with self.assertLogs("dpes.executor", level) as caught:
            run()
        return caught.records

    def test_an_attempt_logs_where_it_ran_and_how_it_ended(self):
        records = self.logged("INFO", lambda: execute(attempt("exit 4")))

        running, finished, confirmed = records
        self.assertEqual("attempt running", running.getMessage())
        self.assertEqual("job-1/t/0/1", running.fields["execution"])
        self.assertIn("host", running.fields)
        self.assertEqual("command finished", finished.getMessage())
        self.assertEqual(4, finished.fields["exit_code"])
        self.assertEqual("job-1/t/0/1", finished.fields["execution"])
        self.assertEqual("result confirmed", confirmed.getMessage())
        self.assertEqual("job-1/t/0/1", confirmed.fields["winner"])

    def fields_of(self, records, message):
        (record,) = [r for r in records if r.getMessage() == message]
        return record.fields

    def test_placed_inputs_and_stored_outputs_are_counted(self):
        store = Blobs()
        name = store.put(b"hola\n")
        work = attempt("cp a.txt b.txt", inputs={"a.txt": name},
                       outputs=("b.txt", "never.txt"))

        records = self.logged("INFO", lambda: execute(work, store))

        placed = self.fields_of(records, "inputs placed")
        stored = self.fields_of(records, "outputs stored")
        self.assertEqual((1, 5), (placed["files"], placed["bytes"]))
        self.assertEqual((1, 1), (stored["files"], stored["missing"]))
        self.assertEqual("job-1/t/0/1", stored["execution"])

    def test_a_failed_result_save_is_a_warning_before_its_retry(self):
        records = self.logged("WARNING",
                              lambda: execute(attempt("true"), SavesFirst(1)))

        fields = self.fields_of(records, "result save failed")
        self.assertEqual("job-1/t/0/1", fields["execution"])
        self.assertEqual(executor._SAVE_WAITS[0], fields["retry_in_s"])

    def test_neither_the_environment_nor_the_output_reaches_the_log(self):
        secret = attempt("echo printed-output", env={"TOKEN": "hunter2"})

        records = self.logged("DEBUG", lambda: execute(secret))

        text = "\n".join(Formatter().format(r) for r in records)
        self.assertNotIn("hunter2", text)
        self.assertNotIn("printed-output", text)

    def test_no_workspace_is_an_error(self):
        with mock.patch("tempfile.tempdir", "/nonexistent/dpes"):
            records = self.logged("ERROR", lambda: execute(attempt("true")))

        self.assertEqual("no workspace", records[0].getMessage())

    @mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "1"})
    def test_an_injected_fault_is_a_warning(self):
        faulty = attempt("true", env={"DPES_FAULT": "network:1"})

        records = self.logged("WARNING", lambda: execute(faulty))

        self.assertEqual(("injected fault", "network"),
                         (records[0].getMessage(), records[0].fields["kind"]))


class Stopped(Exception):
    """Stands in for exiting or hanging, which would stop the test run."""


@mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "1"})
class InjectedFaultTest(unittest.TestCase):
    def numbered(self, number, fault):
        return attempt("echo ran", env={"DPES_FAULT": fault},
                       execution_id=f"job-1/t/0/{number}")

    def test_network_fails_the_first_attempts_then_lets_them_run(self):
        outcomes = [execute(self.numbered(n, "network:2")) for n in (1, 2, 3)]

        self.assertEqual(
            [Evidence.INPUT_UNAVAILABLE, Evidence.INPUT_UNAVAILABLE],
            [outcome.evidence for outcome in outcomes[:2]],
        )
        self.assertEqual("ran\n", outcomes[2].output_tail)

    def test_lose_ack_ends_the_process_after_the_result_is_saved(self):
        store = Blobs()

        with mock.patch("os._exit", side_effect=Stopped):
            with self.assertRaises(Stopped):
                execute(self.numbered(1, "lose-ack:1"), store)

        self.assertEqual(1, len(store.results))

    def test_crash_ends_the_executor_process(self):
        with mock.patch("os._exit", side_effect=Stopped) as exit_:
            with self.assertRaises(Stopped):
                execute(self.numbered(1, "crash:1"))
        exit_.assert_called_once()

    def test_hang_blocks_before_running_the_command(self):
        # Only the executor's own wait: subprocess also sleeps internally.
        with mock.patch("dpes.executor._hang", side_effect=Stopped) as hang:
            with mock.patch("dpes.sandbox.run") as run:
                with self.assertRaises(Stopped):
                    execute(self.numbered(1, "hang:1"))
        hang.assert_called_once()
        run.assert_not_called()

    def test_a_malformed_fault_is_ignored(self):
        for fault in ("crash", "crash:x", "explode:1", ""):
            with self.subTest(fault=fault):
                outcome = execute(self.numbered(1, fault))
                self.assertEqual("ran\n", outcome.output_tail)


class FaultsDisabledTest(unittest.TestCase):
    def test_without_the_cluster_switch_a_fault_is_ignored(self):
        with mock.patch.dict(os.environ, {"DPES_FAULT_INJECTION": "0"}):
            outcome = execute(attempt("echo ran",
                                      env={"DPES_FAULT": "network:9"}))

        self.assertEqual("ran\n", outcome.output_tail)


if __name__ == "__main__":
    unittest.main()
