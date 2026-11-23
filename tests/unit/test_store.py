import dataclasses
import json
import os
import sqlite3
import tempfile
import time
import unittest

from dpes.failures import FailureClass
from dpes.hashing import action_key
from dpes.messages import ResultKey, TaskResult
from dpes.spec import parse_job
from dpes.store import Journal, ResultLog

KEY = ResultKey("job-1", "compile", 0)
NEXT_ROUND = ResultKey("job-1", "compile", 1)
OTHER_JOB = ResultKey("job-2", "compile", 0)
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
# The key of the compile task above (plan/16, D16.1).
CACHE_KEY = action_key(
    ("cc", "-c", "main.c"), {"CC": "gcc"}, {"main.c": "sha256:" + "b" * 64},
    ("main.o",), "linux/amd64:gcc-13",
)
OTHER_KEY = action_key(
    ("cc", "-c", "main.c"), {"CC": "gcc"}, {"main.c": "sha256:" + "b" * 64},
    ("main.o",), "linux/arm64:gcc-13",
)


def result(execution_id, exit_code=0):
    return TaskResult(
        execution_id=execution_id,
        exit_code=exit_code,
        timed_out=False,
        outputs={"out/main.o": "sha256:" + "a" * 64},
        missing_outputs=("report.txt",),
        output_tail="compilé ✓\n",
    )


def success(execution_id):
    """A result that counts as a success (D06.2): exit code 0, nothing missing."""
    return TaskResult(
        execution_id=execution_id,
        exit_code=0,
        timed_out=False,
        outputs={"out/main.o": "sha256:" + "a" * 64},
    )


def text(stored):
    """A result as it sits in the database."""
    return json.dumps(dataclasses.asdict(stored), ensure_ascii=False)


def stored_row(path, key):
    """(succeeded, cache_key) as written for `key`."""
    db = sqlite3.connect(path)
    try:
        return db.execute(
            "SELECT succeeded, cache_key FROM results "
            "WHERE job = ? AND task = ? AND round = ?",
            (key.job_id, key.task, key.round),
        ).fetchone()
    finally:
        db.close()


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


class ResultLogCacheTest(unittest.TestCase):
    """The cache is a lookup over confirmed successes (plan/16, D16.2)."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "results.sqlite")
        self.log = self.open()

    def open(self):
        log = ResultLog(self.path)
        self.addCleanup(log.close)
        return log

    def row(self, key):
        return stored_row(self.path, key)

    def test_no_key_is_found_before_anything_is_saved(self):
        self.assertIsNone(self.log.get_by_cache_key(CACHE_KEY))

    def test_a_succeeded_result_is_found_by_its_key(self):
        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)

        self.assertEqual(success("job-1/compile/0/1"),
                         self.log.get_by_cache_key(CACHE_KEY))

    def test_a_result_that_did_not_succeed_is_never_cached(self):
        # Exit code 0 but a declared output is missing: not a success (D06.2).
        self.log.save(KEY, result("job-1/compile/0/1"), CACHE_KEY)

        self.assertIsNone(self.log.get_by_cache_key(CACHE_KEY))
        self.assertEqual((0, None), self.row(KEY))

    def test_a_success_without_a_key_is_not_cached(self):
        self.log.save(KEY, success("job-1/compile/0/1"))

        self.assertIsNone(self.log.get_by_cache_key(CACHE_KEY))
        self.assertEqual(success("job-1/compile/0/1"), self.log.get(KEY))

    def test_an_empty_key_matches_nothing(self):
        self.log.save(KEY, success("job-1/compile/0/1"), "")

        self.assertIsNone(self.log.get_by_cache_key(""))

    def test_the_confirmed_round_keeps_the_key_of_the_attempt_that_won(self):
        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)

        self.log.save(KEY, success("job-1/compile/0/2"), OTHER_KEY)

        self.assertEqual(success("job-1/compile/0/1"),
                         self.log.get_by_cache_key(CACHE_KEY))
        self.assertIsNone(self.log.get_by_cache_key(OTHER_KEY))

    def test_two_tasks_that_did_the_same_work_both_keep_their_result(self):
        # "One result per task" does not depend on which task owns the entry.
        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)
        self.log.save(OTHER_JOB, success("job-2/compile/0/1"), CACHE_KEY)

        self.assertEqual(success("job-1/compile/0/1"), self.log.get(KEY))
        self.assertEqual(success("job-2/compile/0/1"), self.log.get(OTHER_JOB))
        self.assertIn(self.log.get_by_cache_key(CACHE_KEY),
                      (success("job-1/compile/0/1"),
                       success("job-2/compile/0/1")))

    def test_the_cache_survives_reopening(self):
        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)
        self.log.close()

        self.assertEqual(success("job-1/compile/0/1"),
                         self.open().get_by_cache_key(CACHE_KEY))

    def test_a_row_is_never_visible_without_its_key(self):
        # Result and key are one write: another reader never sees half of it.
        other = self.open()
        self.assertIsNone(other.get_by_cache_key(CACHE_KEY))

        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)

        self.assertEqual((1, CACHE_KEY), stored_row(self.path, KEY))
        self.assertEqual(success("job-1/compile/0/1"),
                         other.get_by_cache_key(CACHE_KEY))

    def test_a_save_that_fails_leaves_no_row_behind(self):
        # Encoding happens before the insert, so nothing is half-written.
        broken = TaskResult(
            execution_id="job-1/compile/0/1", exit_code=0, timed_out=False,
            outputs={"out/main.o": object()},
        )

        with self.assertRaises(TypeError):
            self.log.save(KEY, broken, CACHE_KEY)

        self.assertIsNone(self.log.get(KEY))
        self.assertIsNone(self.log.get_by_cache_key(CACHE_KEY))

    def test_the_table_says_which_results_succeeded(self):
        self.log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)
        self.log.save(NEXT_ROUND, result("job-1/compile/1/1"))

        self.assertEqual((1, CACHE_KEY), self.row(KEY))
        self.assertEqual((0, None), self.row(NEXT_ROUND))


class ResultLogMigrationTest(unittest.TestCase):
    """A results table written before the cache opens and keeps its rows."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "results.sqlite")

    def old_table(self, *rows):
        # The schema as it was before the cache (plan/12), with rows in it.
        old = sqlite3.connect(self.path)
        old.execute(
            "CREATE TABLE results (job TEXT NOT NULL, task TEXT NOT NULL, "
            "round INTEGER NOT NULL, result TEXT NOT NULL, "
            "PRIMARY KEY (job, task, round))"
        )
        old.executemany("INSERT INTO results VALUES (?, ?, ?, ?)", rows)
        old.commit()
        old.close()

    def open(self):
        log = ResultLog(self.path)
        self.addCleanup(log.close)
        return log

    def test_old_rows_keep_their_result_and_gain_a_truthful_flag(self):
        self.old_table(
            ("job-1", "compile", 0, text(success("job-1/compile/0/1"))),
            ("job-1", "compile", 1, text(result("job-1/compile/1/1"))),
        )

        log = self.open()

        self.assertEqual(success("job-1/compile/0/1"), log.get(KEY))
        self.assertEqual(result("job-1/compile/1/1"), log.get(NEXT_ROUND))
        self.assertEqual((1, None), stored_row(self.path, KEY))
        self.assertEqual((0, None), stored_row(self.path, NEXT_ROUND))

    def test_nothing_was_cached_before_the_cache_existed(self):
        self.old_table(("job-1", "compile", 0, text(success("e1"))))

        log = self.open()

        self.assertEqual(success("e1"), log.get(KEY))
        self.assertIsNone(log.get_by_cache_key(CACHE_KEY))

    def test_new_rows_of_a_migrated_table_can_be_cached(self):
        self.old_table()
        log = self.open()

        log.save(KEY, success("job-1/compile/0/1"), CACHE_KEY)

        self.assertEqual(success("job-1/compile/0/1"),
                         log.get_by_cache_key(CACHE_KEY))

    def test_a_table_half_migrated_still_gains_its_flag(self):
        # A crash between the two columns must not stop the second one.
        self.old_table(("job-1", "compile", 0, text(success("e1"))))
        db = sqlite3.connect(self.path)
        db.execute("ALTER TABLE results ADD COLUMN cache_key TEXT")
        db.commit()
        db.close()

        log = self.open()

        self.assertEqual(success("e1"), log.get(KEY))
        self.assertEqual((1, None), stored_row(self.path, KEY))


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
            ("job-1", "cache_hit",
             ("compile", {"app": "sha256:" + "a" * 64},
              "job-0/compile/0/1", 12)),
            ("job-1", "cache_miss", ()),
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
