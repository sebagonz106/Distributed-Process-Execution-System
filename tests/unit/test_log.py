import logging
import os
import unittest
from unittest import mock

from dpes import log


def render(message, level=logging.INFO, name="dpes.coordinator", exc=None,
           **fields):
    record = logging.LogRecord(name, level, __file__, 1, message, None, exc)
    record.created = 1_700_000_000.0  # 2023-11-14T22:13:20 UTC
    record.msecs = 123
    record.fields = fields
    return log.Formatter().format(record)


class FormatTest(unittest.TestCase):
    def test_a_line_has_time_level_component_message_and_fields(self):
        line = render("attempt launched", execution="j/t/0/1", cpu=1.0)

        self.assertEqual(
            "2023-11-14T22:13:20.123Z INFO    coordinator attempt launched"
            " execution=j/t/0/1 cpu=1.0",
            line,
        )

    def test_a_value_with_spaces_quotes_or_nothing_is_quoted(self):
        line = render("x", reason="exit code 1", said='say "hi"', empty="")

        self.assertIn('reason="exit code 1"', line)
        self.assertIn(r'said="say \"hi\""', line)
        self.assertIn('empty=""', line)

    def test_a_value_cannot_break_the_line(self):
        self.assertEqual(1, len(render("x", reason="a\nb").splitlines()))

    def test_an_exception_follows_its_line(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            line = render("failed", level=logging.ERROR, exc=sys.exc_info())

        first, *rest = line.splitlines()
        self.assertTrue(first.endswith("failed"))
        self.assertIn("ValueError: boom", rest[-1])

    def test_event_attaches_the_fields_to_the_record(self):
        logger = logging.getLogger("dpes.test")
        with self.assertLogs(logger, logging.WARNING) as caught:
            log.event(logger, logging.WARNING, "retry scheduled", delay_s=1.5)

        self.assertEqual({"delay_s": 1.5}, caught.records[0].fields)


class SetupTest(unittest.TestCase):
    def setUp(self):
        self.root = logging.getLogger("dpes")
        saved = (list(self.root.handlers), self.root.level,
                 self.root.propagate)
        self.addCleanup(self.restore, *saved)

    def restore(self, handlers, level, propagate):
        self.root.handlers[:] = handlers
        self.root.setLevel(level)
        self.root.propagate = propagate

    def stderr_handlers(self):
        return [h for h in self.root.handlers
                if isinstance(h.formatter, log.Formatter)]

    def test_setting_up_twice_adds_one_handler(self):
        log.setup()
        log.setup()

        self.assertEqual(1, len(self.stderr_handlers()))

    def test_the_level_comes_from_the_environment(self):
        with mock.patch.dict(os.environ, {"DPES_LOG_LEVEL": "debug"}):
            log.setup()

        self.assertEqual(logging.DEBUG, self.root.level)

    def test_an_unknown_level_falls_back_to_info(self):
        with mock.patch.dict(os.environ, {"DPES_LOG_LEVEL": "LOUD"}):
            log.setup()

        self.assertEqual(logging.INFO, self.root.level)


class QuietByDefaultTest(unittest.TestCase):
    def test_without_setup_a_warning_prints_nothing(self):
        # A library must not print through logging's last-resort handler.
        self.assertTrue(logging.getLogger("dpes").handlers)


if __name__ == "__main__":
    unittest.main()
