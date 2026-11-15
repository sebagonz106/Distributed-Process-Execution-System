"""Descriptive operational logs, the same format in every process (plan/10).

Modules log with `event(logger, level, message, **fields)`; each process
calls `setup()` once to send them to stderr (D10.7).
"""

import json
import logging
import os
import sys
import time

ENV_LEVEL = "DPES_LOG_LEVEL"


class Formatter(logging.Formatter):
    """`<UTC time> <LEVEL> <component> <message> key=value ...` (D10.2)."""

    def format(self, record):
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
        component = record.name.removeprefix("dpes.")
        line = (
            f"{stamp}.{int(record.msecs):03d}Z {record.levelname:<7} "
            f"{component:<11} {record.getMessage()}"
        )
        for key, value in getattr(record, "fields", {}).items():
            line += f" {key}={_quoted(value)}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def event(logger, level, message, **fields):
    """Log `message` with `fields` as key=value pairs."""
    logger.log(level, message, extra={"fields": fields})


def setup():
    """Send dpes logs to this process's stderr. Safe to call again."""
    root = logging.getLogger("dpes")
    if any(isinstance(h.formatter, Formatter) for h in root.handlers):
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(Formatter())
    root.addHandler(handler)
    root.propagate = False
    level = os.environ.get(ENV_LEVEL, "INFO").upper()
    known = logging.getLevelNamesMapping()
    root.setLevel(level if level in known else logging.INFO)


def _quoted(value):
    text = str(value)
    if text and not any(c.isspace() or c in '"=' for c in text):
        return text
    return json.dumps(text, ensure_ascii=False)  # escapes quotes and newlines
