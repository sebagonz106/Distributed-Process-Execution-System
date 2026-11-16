"""Confirmed results: the first writer wins (plan/02, D02.10; plan/12).

One SQLite row per result key (job, task, round). Saving inserts only if the
key is new and then reads the row back, so every writer learns the winner.
"""

import dataclasses
import json
import logging
import sqlite3

from dpes.log import event
from dpes.messages import ResultKey, TaskResult

_log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    job TEXT NOT NULL,
    task TEXT NOT NULL,
    round INTEGER NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY (job, task, round)
)
"""


class ResultLog:
    def __init__(self, path):
        # Autocommit: each statement is its own transaction, on disk when it
        # returns. One caller at a time per connection (an actor serialises).
        self._db = sqlite3.connect(path, isolation_level=None,
                                   check_same_thread=False)
        self._db.execute(_SCHEMA)

    def save(self, key: ResultKey, result: TaskResult) -> TaskResult:
        """Confirm `result` for `key` unless one already is; the winner."""
        inserted = self._db.execute(
            "INSERT OR IGNORE INTO results VALUES (?, ?, ?, ?)",
            (key.job_id, key.task, key.round, _encode(result)),
        ).rowcount == 1
        winner = self.get(key)
        eid = result.execution_id
        if inserted:
            event(_log, logging.INFO, "result saved", execution=eid)
        elif winner.execution_id == eid:  # its earlier reply was lost
            event(_log, logging.INFO, "result save repeated", execution=eid)
        else:
            event(_log, logging.WARNING, "result already confirmed",
                  execution=eid, winner=winner.execution_id)
        return winner

    def get(self, key: ResultKey):
        """The confirmed result for `key`, or None."""
        row = self._db.execute(
            "SELECT result FROM results WHERE job = ? AND task = ? "
            "AND round = ?",
            (key.job_id, key.task, key.round),
        ).fetchone()
        return _decode(row[0]) if row else None

    def close(self):
        self._db.close()


def _encode(result: TaskResult) -> str:
    return json.dumps(dataclasses.asdict(result), ensure_ascii=False)


def _decode(text: str) -> TaskResult:
    fields = json.loads(text)
    fields["missing_outputs"] = tuple(fields["missing_outputs"])
    return TaskResult(**fields)
