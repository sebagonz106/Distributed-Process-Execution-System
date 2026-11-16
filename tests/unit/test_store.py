import os
import tempfile
import unittest

from dpes.messages import ResultKey, TaskResult
from dpes.store import ResultLog

KEY = ResultKey("job-1", "compile", 0)


def result(execution_id, exit_code=0):
    return TaskResult(
        execution_id=execution_id,
        exit_code=exit_code,
        timed_out=False,
        outputs={"out/main.o": "sha256:" + "a" * 64},
        missing_outputs=("report.txt",),
        output_tail="compilé ✓\n",
    )


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


if __name__ == "__main__":
    unittest.main()
