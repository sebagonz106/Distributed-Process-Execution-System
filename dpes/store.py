"""What the control node keeps in SQLite.

Confirmed results, where the first writer wins (plan/02, D02.10; plan/12): one
row per result key (job, task, round). Saving inserts only if the key is new
and then reads the row back, so every writer learns the winner. A row of a
successful result also carries its action key, which makes the content cache
(plan/16, D16.2) a lookup over confirmed results instead of a second store.

The coordinator's journal (plan/14): its calls to the state machine, in order.
"""

import dataclasses
import json
import logging
import sqlite3
import time

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
    cache_key TEXT,
    succeeded INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job, task, round)
)
"""
# The content cache is a lookup by action key over confirmed results
# (plan/16, D16.2). The index is partial, so attempts that failed stay
# out of it, and it is not unique: two tasks may do the same work and each
# keeps its own row, which is what "one result per task" needs.
_CACHE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_results_cache_key
    ON results(cache_key) WHERE succeeded = 1
"""
_RESULTS_ADD_CACHE = "ALTER TABLE results ADD COLUMN cache_key TEXT"
_RESULTS_ADD_SUCCEEDED = (
    "ALTER TABLE results ADD COLUMN succeeded INTEGER NOT NULL DEFAULT 0"
)
_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY,
    job TEXT NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL,
    at REAL NOT NULL DEFAULT 0
)
"""
# Journals written before D15.16 have no `at`. SQLite only adds a column with a
# constant default, so those rows come back at the epoch: their time was never
# recorded and inventing one would be a lie.
_JOURNAL_ADD_AT = "ALTER TABLE events ADD COLUMN at REAL NOT NULL DEFAULT 0"


def _connect(path, schema):
    # Autocommit: each statement is its own transaction, on disk when it
    # returns. One caller at a time per connection (an actor serialises).
    db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    db.execute(schema)
    return db


class ResultLog:
    def __init__(self, path):
        self._db = _connect(path, _SCHEMA)
        self._add_cache_columns()
        self._db.execute(_CACHE_INDEX)

    def _add_cache_columns(self):
        """Give a table written before the cache (D16.2) its columns.

        `succeeded` is filled from each stored result instead of being left at
        its default, so the column never claims that a success failed. The
        column and its values arrive in one transaction: a crash cannot leave
        half the table flagged. The key of old rows stays empty: they were
        confirmed before any cache existed and no key can be rebuilt from a
        result, which holds no command.
        """
        columns = {row[1] for row in
                   self._db.execute("PRAGMA table_info(results)")}
        if "cache_key" not in columns:  # one statement, so atomic on its own
            self._db.execute(_RESULTS_ADD_CACHE)
        if "succeeded" in columns:
            return
        self._db.execute("BEGIN")
        try:
            self._db.execute(_RESULTS_ADD_SUCCEEDED)
            rows = self._db.execute(
                "SELECT job, task, round, result FROM results"
            ).fetchall()
            for job, task, round_, text in rows:
                self._db.execute(
                    "UPDATE results SET succeeded = ? "
                    "WHERE job = ? AND task = ? AND round = ?",
                    (int(_decode(text).succeeded), job, task, round_),
                )
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise

    def save(self, key: ResultKey, result: TaskResult,
             cache_key: str | None = None) -> TaskResult:
        """Confirm `result` for `key` unless one already is; the winner.

        A successful result is registered in the cache under `cache_key`
        (plan/16, D16.2): the result row is the cache entry, so both land in
        the same statement. Failures are never given a key, and a round that
        was already confirmed keeps the key of the attempt that won it.
        """
        registered = cache_key if cache_key and result.succeeded else None
        inserted = self._db.execute(
            "INSERT OR IGNORE INTO results "
            "(job, task, round, result, cache_key, succeeded) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (key.job_id, key.task, key.round, _encode(result), registered,
             int(result.succeeded)),
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

    def get_by_cache_key(self, cache_key: str):
        """The confirmed success registered under `cache_key`, or None.

        Only rows flagged as successes are candidates (D16.4), and an empty key
        matches nothing: a caller with no key is asking for no cache.
        """
        if not cache_key:
            return None
        row = self._db.execute(
            "SELECT result FROM results WHERE cache_key = ? "
            "AND succeeded = 1 LIMIT 1",
            (cache_key,),
        ).fetchone()
        return _decode(row[0]) if row else None

    def close(self):
        self._db.close()


class Journal:
    """The coordinator's calls to its state machine, in the order it made
    them. Replaying them rebuilds its state (plan/14, D14.1). Each call keeps
    the wall-clock time it was made, so the events it produces keep theirs
    across a restart (plan/15, D15.16)."""

    def __init__(self, path):
        self._db = _connect(path, _JOURNAL_SCHEMA)
        columns = {row[1] for row in
                   self._db.execute("PRAGMA table_info(events)")}
        if "at" not in columns:  # a journal from before D15.16
            self._db.execute(_JOURNAL_ADD_AT)

    def append(self, job_id, kind, *args) -> float:
        """Record a call of `kind` with `args`; on disk when it returns.
        Returns the time stored with it, so the events of that very call are
        stamped with it (live and replayed alike)."""
        encode, _ = _CALLS[kind]
        at = time.time()
        self._db.execute(
            "INSERT INTO events (job, kind, data, at) VALUES (?, ?, ?, ?)",
            (job_id, kind, json.dumps(encode(*args), ensure_ascii=False), at),
        )
        return at

    def events(self):
        """(job id, kind, args, time) of every call, oldest first."""
        rows = self._db.execute(
            "SELECT job, kind, data, at FROM events ORDER BY seq"
        ).fetchall()
        for job_id, kind, data, at in rows:
            _, decode = _CALLS[kind]
            yield job_id, kind, decode(json.loads(data)), at

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
    "cache_hit": (  # the task was served without anything running (D16.3)
        lambda name, outputs, original_execution, bytes_saved: {
            "task": name, "outputs": dict(outputs),
            "original_execution": original_execution,
            "bytes_saved": bytes_saved,
        },
        lambda data: (data["task"], data["outputs"],
                      data["original_execution"], data["bytes_saved"]),
    ),
    "cache_miss": (  # a count of lookups that had to run (D16.8)
        lambda: {},
        lambda data: (),
    ),
    "finished": (  # the job is over; its feed gets JOB_FINISHED (D15.17)
        lambda: {},
        lambda data: (),
    ),
}


def _encode(result: TaskResult) -> str:
    return json.dumps(dataclasses.asdict(result), ensure_ascii=False)


def _decode(text: str) -> TaskResult:
    return _result(json.loads(text))


def _result(fields: dict) -> TaskResult:
    fields["missing_outputs"] = tuple(fields["missing_outputs"])
    return TaskResult(**fields)
