import os
import sqlite3
import tempfile
import time
import unittest

from dpes.failures import FailureClass
from dpes.messages import ResultKey, TaskResult
from dpes.spec import parse_job
from dpes.store import Journal, ResultLog

KEY = ResultKey("job-1", "compile", 0)
SPEC = parse_job({"tasks": {
    "compile": {
        "command": ["cc", "-c", "main.c"], "env": {"CC": "gcc"},
        "timeout_s": 90, "resources": {"cpu": 2, "memory_mb": 256, "gpu": 1},
        "retries": {"worker": 1}, "inputs": {"main.c": "sha256:" + "b" * 64},
        "outputs": ["main.o"],
    },
    "link": {"command": ["cc", "main.o"], "needs": ["compile"],
             "outputs": ["app"]},
}})


def result(execution_id, exit_code=0):
    return TaskResult(
        execution_id=execution_id,
        exit_code=exit_code,
        timed_out=False,
        outputs={"out/main.o": "sha256:" + "a" * 64},
        missing_outputs=("report.txt",),
        output_tail="compilé ✓\n",
    )


class ResultLogTraceTest(unittest.TestCase):
    """Who won each confirmation is in the log (plan/10, D10.11)."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.log = ResultLog(os.path.join(directory.name, "r.sqlite"))
        self.addCleanup(self.log.close)

    def save(self, level, execution_id):
        with self.assertLogs("dpes.store", level) as caught:
            self.log.save(KEY, result(execution_id))
        (record,) = caught.records
        return record.getMessage(), record.levelname, record.fields

    def test_the_first_confirmation_is_logged_as_saved(self):
        message, level, fields = self.save("INFO", "job-1/compile/0/1")

        self.assertEqual(("result saved", "INFO"), (message, level))
        self.assertEqual("job-1/compile/0/1", fields["execution"])

    def test_the_same_attempt_saving_again_is_a_repeat(self):
        self.log.save(KEY, result("job-1/compile/0/1"))

        message, level, _ = self.save("INFO", "job-1/compile/0/1")

        self.assertEqual(("result save repeated", "INFO"), (message, level))

    def test_a_later_attempt_learns_another_one_won(self):
        self.log.save(KEY, result("job-1/compile/0/1"))

        message, level, fields = self.save("WARNING", "job-1/compile/0/2")

        self.assertEqual(("result already confirmed", "WARNING"),
                         (message, level))
        self.assertEqual(("job-1/compile/0/2", "job-1/compile/0/1"),
                         (fields["execution"], fields["winner"]))


class ResultLogTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "results.sqlite")
        self.log = self.open()

    def open(self):
        log = ResultLog(self.path)
        self.addCleanup(log.close)
        return log

    def test_nothing_is_confirmed_until_saved(self):
        self.assertIsNone(self.log.get(KEY))

    def test_a_saved_result_comes_back_unchanged(self):
        saved = self.log.save(KEY, result("job-1/compile/0/1"))

        self.assertEqual(result("job-1/compile/0/1"), saved)
        self.assertEqual(saved, self.log.get(KEY))

    def test_the_first_writer_wins_and_every_writer_learns_it(self):
        first = self.log.save(KEY, result("job-1/compile/0/1"))

        second = self.log.save(KEY, result("job-1/compile/0/2", exit_code=1))

        self.assertEqual(first, second)
        self.assertEqual(first, self.log.get(KEY))

    def test_saving_the_same_result_again_is_harmless(self):
        once = self.log.save(KEY, result("job-1/compile/0/1"))

        self.assertEqual(once, self.log.save(KEY, result("job-1/compile/0/1")))

    def test_each_round_has_its_own_result(self):
        self.log.save(KEY, result("job-1/compile/0/1", exit_code=1))
        next_round = ResultKey("job-1", "compile", 1)

        saved = self.log.save(next_round, result("job-1/compile/1/2"))

        self.assertEqual("job-1/compile/1/2", saved.execution_id)

    def test_a_confirmed_result_survives_reopening(self):
        self.log.save(KEY, result("job-1/compile/0/1"))
        self.log.close()

        self.assertEqual(result("job-1/compile/0/1"), self.open().get(KEY))

    def test_two_writers_on_the_same_file_agree_on_one_winner(self):
        # Uniqueness comes from SQLite's primary key, not from one process.
        other = self.open()

        a = self.log.save(KEY, result("job-1/compile/0/1"))
        b = other.save(KEY, result("job-1/compile/0/2", exit_code=1))

        self.assertEqual(a, b)


class JournalTest(unittest.TestCase):
    """The coordinator's calls come back as they went in (plan/14, D14.1)."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "coordinator.sqlite")
        self.journal = self.open()

    def open(self):
        journal = Journal(self.path)
        self.addCleanup(journal.close)
        return journal

    def calls(self, journal=None):
        """What each row says, without its time."""
        source = journal or self.journal
        return [(job_id, kind, args)
                for job_id, kind, args, _ in source.events()]

    def test_each_kind_of_call_comes_back_as_it_went_in(self):
        calls = [
            ("job-1", "accepted", (SPEC, "key-1")),
            ("job-1", "running", ("job-1/compile/0/1",)),
            ("job-1", "result",
             (result("job-1/compile/0/1"), "job-1/compile/0/1", "a note")),
            ("job-1", "failure",
             ("job-1/link/0/1", FailureClass.WORKER, "coordinator restart")),
            ("job-1", "finished", ()),
        ]
        for job_id, kind, args in calls:
            self.journal.append(job_id, kind, *args)

        self.assertEqual(calls, self.calls())

    def test_a_job_sent_without_a_key_keeps_none(self):
        self.journal.append("job-1", "accepted", SPEC, None)

        ((_, _, (_, key), _),) = self.journal.events()

        self.assertIsNone(key)

    def test_calls_keep_their_order_across_jobs_and_reopening(self):
        for job_id, eid in (("job-1", "a"), ("job-2", "b"), ("job-1", "c")):
            self.journal.append(job_id, "running", eid)
        self.journal.close()

        self.assertEqual(
            [("job-1", "running", ("a",)), ("job-2", "running", ("b",)),
             ("job-1", "running", ("c",))],
            self.calls(self.open()),
        )

    def test_each_call_keeps_the_wall_clock_time_it_was_made(self):
        before = time.time()

        at = self.journal.append("job-1", "running", "a")

        after = time.time()
        ((_, _, _, recorded),) = self.journal.events()
        self.assertEqual(at, recorded)
        self.assertLessEqual(before, at)
        self.assertLessEqual(at, after)

    def test_that_time_survives_reopening(self):
        at = self.journal.append("job-1", "running", "a")
        self.journal.close()

        ((_, _, _, recorded),) = self.open().events()

        self.assertEqual(at, recorded)


class JournalMigrationTest(unittest.TestCase):
    """A journal written before D15.16 opens and keeps its calls."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "coordinator.sqlite")

    def test_rows_without_a_time_gain_the_column_and_keep_their_data(self):
        # The schema as it was before D15.16, with one row already in it.
        old = sqlite3.connect(self.path)
        old.execute(
            "CREATE TABLE events (seq INTEGER PRIMARY KEY, job TEXT NOT NULL, "
            "kind TEXT NOT NULL, data TEXT NOT NULL)"
        )
        old.execute("INSERT INTO events (job, kind, data) VALUES (?, ?, ?)",
                    ("job-1", "running", '{"execution_id": "a"}'))
        old.commit()
        old.close()

        journal = Journal(self.path)
        self.addCleanup(journal.close)

        ((job_id, kind, args, at),) = journal.events()
        self.assertEqual(("job-1", "running", ("a",)), (job_id, kind, args))
        # Their time was never recorded: the epoch says so, it does not lie.
        self.assertEqual(0.0, at)

    def test_new_rows_of_a_migrated_journal_keep_their_time(self):
        old = sqlite3.connect(self.path)
        old.execute(
            "CREATE TABLE events (seq INTEGER PRIMARY KEY, job TEXT NOT NULL, "
            "kind TEXT NOT NULL, data TEXT NOT NULL)"
        )
        old.commit()
        old.close()
        journal = Journal(self.path)
        self.addCleanup(journal.close)

        at = journal.append("job-1", "running", "a")

        ((_, _, _, recorded),) = journal.events()
        self.assertEqual(at, recorded)


if __name__ == "__main__":
    unittest.main()
