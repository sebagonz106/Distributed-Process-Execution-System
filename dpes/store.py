"""What the control node keeps in SQLite.

Confirmed results, where the first writer wins (plan/02, D02.10; plan/12): one
row per result key (job, task, round). Saving inserts only if the key is new
and then reads the row back, so every writer learns the winner.

The coordinator's journal (plan/14): its calls to the state machine, in order.
"""

import dataclasses
import json
import logging
import sqlite3

from dpes.failures import FailureClass
from dpes.log import event
from dpes.messages import ResultKey, TaskResult
from dpes.spec import parse_job

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
_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY,
    job TEXT NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL
)
"""


def _connect(path, schema):
    # Autocommit: each statement is its own transaction, on disk when it
    # returns. One caller at a time per connection (an actor serialises).
    db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    db.execute(schema)
    return db


class ResultLog:
    def __init__(self, path):
        self._db = _connect(path, _SCHEMA)

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


class Journal:
    """The coordinator's calls to its state machine, in the order it made
    them. Replaying them rebuilds its state (plan/14, D14.1)."""

    def __init__(self, path):
        self._db = _connect(path, _JOURNAL_SCHEMA)

    def append(self, job_id, kind, *args):
        """Record a call of `kind` with `args`; on disk when it returns."""
        encode, _ = _CALLS[kind]
        self._db.execute(
            "INSERT INTO events (job, kind, data) VALUES (?, ?, ?)",
            (job_id, kind, json.dumps(encode(*args), ensure_ascii=False)),
        )

    def events(self):
        """(job id, kind, args) of every call, oldest first."""
        rows = self._db.execute(
            "SELECT job, kind, data FROM events ORDER BY seq"
        ).fetchall()
        for job_id, kind, data in rows:
            _, decode = _CALLS[kind]
            yield job_id, kind, decode(json.loads(data))

    def close(self):
        self._db.close()


# Each kind of call: its arguments to JSON, and back.
_CALLS = {
    "accepted": (
        lambda spec, key: {"spec": dataclasses.asdict(spec), "key": key},
        lambda data: (parse_job(data["spec"]), data["key"]),
    ),
    "running": (
        lambda execution_id: {"execution_id": execution_id},
        lambda data: (data["execution_id"],),
    ),
    "result": (
        lambda result, execution_id, note: {
            "result": dataclasses.asdict(result),
            "execution_id": execution_id, "note": note,
        },
        lambda data: (_result(data["result"]), data["execution_id"],
                      data["note"]),
    ),
    "failure": (
        lambda execution_id, failure_class, reason: {
            "execution_id": execution_id,
            "failure_class": failure_class.value, "reason": reason,
        },
        lambda data: (data["execution_id"],
                      FailureClass(data["failure_class"]), data["reason"]),
    ),
}


def _encode(result: TaskResult) -> str:
    return json.dumps(dataclasses.asdict(result), ensure_ascii=False)


def _decode(text: str) -> TaskResult:
    return _result(json.loads(text))


def _result(fields: dict) -> TaskResult:
    fields["missing_outputs"] = tuple(fields["missing_outputs"])
    return TaskResult(**fields)
