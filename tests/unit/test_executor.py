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
from dpes.logstore import LogTruncated
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult


class Blobs(dict):
    """In-memory store with the executor's interface (plan/11, D11.5;
    plan/12, D12.1; plan/15, D15.8): blobs, results where the first writer
    wins, and the log as the fragments that arrived."""

    def __init__(self, *args):
        super().__init__(*args)
        self.results = {}
        self.saves = []
        self.fragments = []  # (seq, data) as the log store took them
        self.closed = []  # execution ids whose log was ended

    def get(self, name):
        return super().get(name)

    def put(self, data):
        self[digest(data)] = data
        return digest(data)

    def save_result(self, key, result):
        self.saves.append((key, result))
        return self.results.setdefault(key, result)

    def append_fragment(self, execution_id, seq, data):
        self.fragments.append((seq, data))
        return True

    def close_fragments(self, execution_id):
        self.closed.append(execution_id)
        return True


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
    """A store that cannot be reached at all: nothing gets in or out."""

    def get(self, name):
        raise ConnectionError("store unreachable")

    def put(self, data):
        raise ConnectionError("store unreachable")

    def append_fragment(self, execution_id, seq, data):
        raise ConnectionError("store unreachable")

    def close_fragments(self, execution_id):
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
        with mock.patch("dpes.executor._SEND_WAITS", (0, 0, 0)):
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

        running = self.fields_of(records, "attempt running")
        self.assertEqual("job-1/t/0/1", running["execution"])
        self.assertIn("host", running)
        finished = self.fields_of(records, "command finished")
        self.assertEqual(4, finished["exit_code"])
        self.assertEqual("job-1/t/0/1", finished["execution"])
        confirmed = self.fields_of(records, "result confirmed")
        self.assertEqual("job-1/t/0/1", confirmed["winner"])

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


class NoLogs(Blobs):
    """A store whose log half is unreachable, as a partition leaves it."""

    def append_fragment(self, execution_id, seq, data):
        raise ConnectionError("store unreachable")

    def close_fragments(self, execution_id):
        raise ConnectionError("store unreachable")


class SlowFirst(Blobs):
    """Refuses the first fragment once, as a timed-out call would."""

    def __init__(self):
        super().__init__()
        self.refusals = 1

    def append_fragment(self, execution_id, seq, data):
        if self.refusals:
            self.refusals -= 1
            raise ConnectionError("store unreachable")
        return super().append_fragment(execution_id, seq, data)


class DiesHalfway(Blobs):
    """Takes one fragment and then stops answering, as a dying store would."""

    def __init__(self):
        super().__init__()
        self.taken = 0

    def append_fragment(self, execution_id, seq, data):
        if self.taken:
            raise ConnectionError("store unreachable")
        self.taken += 1
        return super().append_fragment(execution_id, seq, data)


class LogEmissionTest(unittest.TestCase):
    """The emitter of plan/15, D15.3: the output leaves the worker as it is
    written, in numbered fragments, and the log is ended with the attempt."""

    def run_with_store(self, script, store=None):
        store = Blobs() if store is None else store
        return execute(attempt(script), store), store

    def test_the_output_arrives_in_fragments_with_consecutive_seq(self):
        result, store = self.run_with_store(r"printf 'one\ntwo\n'")

        seqs = [seq for seq, _ in store.fragments]
        self.assertEqual(list(range(1, len(seqs) + 1)), seqs)
        self.assertEqual(b"one\ntwo\n",
                         b"".join(data for _, data in store.fragments))
        self.assertEqual("one\ntwo\n", result.output_tail)

    def test_a_long_output_comes_in_fragments_of_whole_lines(self):
        expected = "".join(f"{n}\n" for n in range(1, 4001)).encode()

        _, store = self.run_with_store("seq 1 4000")

        self.assertGreater(len(store.fragments), 1)
        self.assertEqual(expected, b"".join(d for _, d in store.fragments))
        for _, data in store.fragments:
            self.assertLessEqual(len(data), executor.FRAGMENT_BYTES)
            self.assertTrue(data.endswith(b"\n"))

    @mock.patch("dpes.executor.FLUSH_S", 0.05)
    def test_output_that_trickles_out_is_sent_without_a_full_fragment(self):
        _, store = self.run_with_store("echo first; sleep 0.5; echo second")

        self.assertEqual([b"first\n", b"second\n"],
                         [data for _, data in store.fragments])

    def test_the_log_is_ended_with_the_attempt_even_when_it_said_nothing(self):
        result, store = self.run_with_store("true")

        self.assertEqual([], store.fragments)  # no fragment for no output
        self.assertEqual([result.execution_id], store.closed)

    @mock.patch("dpes.executor._SEND_WAITS", (0, 0, 0))
    def test_a_fragment_is_retried_under_the_same_seq(self):
        store = SlowFirst()

        self.run_with_store("echo hi", store)

        self.assertEqual([(1, b"hi\n")], store.fragments)

    @mock.patch("dpes.executor._SEND_WAITS", (0, 0, 0))
    def test_a_store_that_refuses_fragments_does_not_fail_the_task(self):
        store = NoLogs()
        ran = []

        with self.assertLogs("dpes.executor", "WARNING") as caught:
            ran.append(self.run_with_store("echo hi", store))

        task, _ = ran[0]
        self.assertIsInstance(task, TaskResult)
        self.assertEqual((0, "hi\n"), (task.exit_code, task.output_tail))
        self.assertEqual([], store.fragments)
        self.assertEqual([], store.closed)
        fields = [r.fields for r in caught.records
                  if r.getMessage() == "log fragments lost"]
        self.assertEqual(1, len(fields))
        self.assertEqual(1, fields[0]["lost"])
        self.assertEqual("job-1/t/0/1", fields[0]["execution"])

    @mock.patch("dpes.executor._SEND_WAITS", (0, 0, 0))
    def test_a_store_that_dies_midway_abandons_the_rest_of_the_log(self):
        store = DiesHalfway()
        first = "".join(f"{n}\n" for n in range(1, 101)).encode()

        with self.assertLogs("dpes.executor", "WARNING") as caught:
            task = self.run_with_store("seq 1 200", store)[0]

        self.assertEqual(0, task.exit_code)
        self.assertEqual([(1, first)], store.fragments)  # no fragment after it
        self.assertEqual([], store.closed)  # and no end marker either
        fields = [r.fields for r in caught.records
                  if r.getMessage() == "log fragments lost"][0]
        self.assertEqual((1, 1), (fields["sent"], fields["lost"]))


class Capped(Blobs):
    """A store with a ceiling, as the control node's has (D15.13)."""

    def __init__(self, limit):
        super().__init__()
        self.limit = limit
        self.taken = 0

    def append_fragment(self, execution_id, seq, data):
        if self.taken + len(data) > self.limit:
            raise LogTruncated(execution_id, self.taken, self.limit)
        self.taken += len(data)
        return super().append_fragment(execution_id, seq, data)


class LogCeilingTest(unittest.TestCase):
    """What stops the log at its ceiling (plan/15, D15.13)."""

    def test_the_store_ceiling_ends_the_log_without_failing_the_task(self):
        store = Capped(300)

        with self.assertLogs("dpes.executor", "WARNING") as caught:
            task = execute(attempt("i=0; while [ $i -lt 200 ]; do echo $i; "
                                   "i=$((i+1)); done"), store)

        self.assertTrue(task.log_truncated)
        self.assertEqual(0, task.exit_code)  # the command itself ended well
        stored = b"".join(d for _, d in store.fragments)
        self.assertEqual(290, len(stored))  # the fragment that still fitted
        self.assertTrue(stored.endswith(b"99\n"))
        # The log is ended as usual, so a follower stops instead of waiting.
        self.assertEqual([task.execution_id], store.closed)
        (truncated,) = [r.fields for r in caught.records
                        if r.getMessage() == "log truncated"]
        self.assertEqual(("control", 290, 300),
                         (truncated["side"], truncated["bytes"],
                          truncated["limit"]))

    @mock.patch("dpes.executor.MAX_CAPTURE_BYTES", 10)
    def test_the_worker_ceiling_cuts_the_last_fragment_to_what_fits(self):
        store = Blobs()

        with self.assertLogs("dpes.executor", "WARNING") as caught:
            task = execute(
                attempt("printf 'abcdefghij\\nklmnopqrst\\n'"), store
            )

        self.assertTrue(task.log_truncated)
        self.assertEqual(b"abcdefghij", b"".join(d for _, d in store.fragments))
        self.assertEqual([task.execution_id], store.closed)
        (truncated,) = [r.fields for r in caught.records
                        if r.getMessage() == "log truncated"]
        self.assertEqual(("worker", 10), (truncated["side"],
                                          truncated["limit"]))

    @mock.patch("dpes.executor.MAX_CAPTURE_BYTES", 1000)
    def test_a_flooding_command_is_killed_and_the_log_says_it_was_cut(self):
        store = Blobs()

        with self.assertLogs("dpes.executor", "WARNING") as caught:
            task = execute(attempt("while :; do echo spam; done"), store)

        self.assertTrue(task.log_truncated)
        self.assertLess(task.exit_code, 0)  # killed by a signal
        self.assertEqual([task.execution_id], store.closed)
        self.assertLessEqual(
            len(b"".join(d for _, d in store.fragments)), 1000
        )
        self.assertEqual(1, len([r for r in caught.records
                                 if r.getMessage() == "log truncated"]))


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
