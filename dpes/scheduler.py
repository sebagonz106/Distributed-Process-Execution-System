"""Coordinator state machine: events in, attempts to launch out.

Pure module: no Ray, no disk, no clock of its own (plan/02, D02.3). Retry waits
draw from a generator seeded with the job id, and the time an event is stamped
with comes from a clock the caller injects, so the same calls rebuild the same
job (plan/14, D14.1; plan/15, D15.16). Rules: plan/07 (D07.4 to D07.6),
plan/08 (D08.4, D08.5) and plan/09 (D09.2 to D09.6).
"""

import random
from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from dpes.failures import FailureClass, retry_delay
from dpes.messages import Attempt, TaskResult
from dpes.spec import JobSpec, TaskSpec


class State(str, Enum):
    PENDING = "PENDING"  # waiting for its dependencies
    RUNNING = "RUNNING"  # an attempt is in flight, possibly queued in Ray
    RETRY_WAIT = "RETRY_WAIT"  # failed with budget left; waiting to retry
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"  # the command's own failure, out of retries
    ERROR = "ERROR"  # an infrastructure failure, out of retries
    SKIPPED = "SKIPPED"  # a dependency did not succeed


_FINISHED = {State.SUCCEEDED, State.FAILED, State.ERROR, State.SKIPPED}


@dataclass(frozen=True)
class Launch:
    """Launch `attempt` after waiting `delay_s` seconds.

    `retry` marks an attempt that retries a failure of the round it belongs to.
    Its work was already looked up in the cache when the round began, and a
    round with no confirmed result cannot be served from it (plan/16, D16.4).
    """

    attempt: Attempt
    delay_s: float = 0.0
    retry: bool = False


@dataclass(frozen=True)
class Event:
    """One step of a job's history, for clients (plan/15, D15.5).

    Data, not an operational log (D10.4, D15.10): the coordinator journals the
    calls that produce it and the gateway serves it. `timestamp` is wall-clock
    seconds, taken from the injected clock (D15.16).
    """

    timestamp: float
    type: str  # TASK_LAUNCHED, ATTEMPT_SUCCEEDED, ATTEMPT_FAILED,
    #            RETRY_SCHEDULED, TASK_SKIPPED, CACHE_HIT or JOB_FINISHED
    job_id: str
    task: str | None  # None for JOB_FINISHED: no single task is to blame
    execution_id: str  # "" when no attempt is involved
    payload: dict


def _epoch() -> float:
    """Time for a job built without a clock: unit tests only (D15.16)."""
    return 0.0


class _Task:
    def __init__(self, spec: TaskSpec):
        self.spec = spec
        self.state = State.PENDING
        self.round = 0
        self.attempts = 0
        self.execution_id = None  # attempt in flight or that settled the task
        self.cached_from = None  # the execution a cache hit reuses (D16.5)
        self.result = None
        self.error = None
        self.failure_class = None
        self.truncated = False  # the settling attempt's log hit its ceiling
        self.retries_used = {key: 0 for key in spec.retries}
        self.history = []


class Job:
    def __init__(self, job_id: str, spec: JobSpec, rand=None, clock=None):
        self.job_id = job_id
        self.spec = spec
        self._rand = rand or random.Random(job_id).random
        self._clock = clock or _epoch
        self._tasks = {name: _Task(task) for name, task in spec.tasks.items()}
        self._dependents = {name: [] for name in spec.tasks}
        for name, task in spec.tasks.items():
            for need in task.needs:
                self._dependents[need].append(name)
        self._running = {}  # execution id -> task name
        self._events = []  # append-only, oldest first (D15.5)
        self._done = False  # JOB_FINISHED recorded once (D15.17)
        # Lookups this job made of the content cache (plan/16, D16.8).
        self._cache_hits = 0
        self._cache_misses = 0
        self._bytes_saved = 0

    def start(self) -> list[Launch]:
        """Launches for the tasks that depend on nothing."""
        return [
            self._launch(name)
            for name, task in self._tasks.items()
            if task.state is State.PENDING and not task.spec.needs
        ]

    def running(self, execution_id: str) -> None:
        """A retry that was waiting has just been launched.

        No event: `RETRY_SCHEDULED` already said when it was due (D15.18).
        """
        name = self._running.get(execution_id)
        if name and self._tasks[name].state is State.RETRY_WAIT:
            self._tasks[name].state = State.RUNNING

    def unsettled(self) -> list[Attempt]:
        """Attempts handed out and not settled yet (plan/14, D14.4)."""
        return [self._attempt(name) for name in self._running.values()]

    def events(self) -> list[Event]:
        """The job's history, oldest first, for clients (D15.5)."""
        return list(self._events)

    @property
    def done(self) -> bool:
        """Whether the end of the job is already in `events` (D15.17).

        The coordinator asks before journaling: replaying the journal rebuilds
        the event, so a finished job must not record it twice.
        """
        return self._done

    def finished(self) -> None:
        """Record that the job reached its terminal state (D15.17)."""
        if self._done:  # replay or a repeated call: not a second end
            return
        self._done = True
        counts = {}
        for task in self._tasks.values():
            key = task.state.value.lower()
            counts[key] = counts.get(key, 0) + 1
        self._record_event("JOB_FINISHED", "", None,
                           state=self.state.value, counts=counts)

    def on_result(self, result: TaskResult, execution_id: str = "",
                  note: str = "") -> list[Launch]:
        """Settle attempt `execution_id` (by default, the one that produced
        `result`). The result may come from another attempt of the same
        round: adopted by reconciliation, or the winner of the save
        (plan/12, D12.5). `note` says so in the history."""
        execution_id = execution_id or result.execution_id
        name = self._running.pop(execution_id, None)
        if name is None:
            return []  # duplicate, late or unknown: ignored (D07.6)
        if result.execution_id != execution_id and not note:
            note = f"confirmed result of {result.execution_id}"
        task = self._tasks[name]
        task.result = result
        task.truncated = result.log_truncated
        if not result.succeeded:
            reason = _reason(result) + (f"; {note}" if note else "")
            return self._failed(name, FailureClass.TASK, reason)
        task.state = State.SUCCEEDED
        task.error = task.failure_class = None
        self._record(task, "SUCCEEDED", note, None)
        payload = {"state": State.SUCCEEDED.value,
                   "exit_code": result.exit_code}
        if note:
            payload["note"] = note  # an adopted result is worth explaining
        self._record_event("ATTEMPT_SUCCEEDED", execution_id, name, **payload)
        return self._settled(name)

    def on_cache_hit(self, name: str, outputs: Mapping[str, str],
                     original_execution: str | None,
                     bytes_saved: int = 0) -> list[Launch]:
        """Settle `name` with the result the cache already has (D16.3).

        Nothing ran: the attempt launched for this round is taken back before
        it loses its id, so a late answer to it is ignored instead of settling
        a finished task, and a restart finds nothing in flight. The task keeps
        the digests of the original execution -- the blobs are already in the
        store (D16.5) -- and its id, so that execution's log can still be
        served. The dependents are launched: the DAG goes on.
        """
        task = self._tasks[name]
        self._running.pop(task.execution_id, None)
        task.execution_id = None
        task.state = State.SUCCEEDED
        task.result = TaskResult("", 0, False, dict(outputs))
        task.error = task.failure_class = None
        task.cached_from = original_execution
        self._cache_hits += 1
        self._bytes_saved += bytes_saved
        self._record(task, "CACHE_HIT", f"cache hit: {original_execution}", None)
        self._record_event("CACHE_HIT", "", name,
                           original_execution_id=original_execution)
        return self._settled(name)

    def on_cache_miss(self) -> None:
        """A round asked the cache and found nothing to serve (D16.8).

        No event: a miss is a metric, not a step of the job's history.
        """
        self._cache_misses += 1

    def on_failure(self, execution_id: str, failure_class: FailureClass,
                   reason: str) -> list[Launch]:
        name = self._running.pop(execution_id, None)
        if name is None:
            return []
        self._tasks[name].result = None
        self._tasks[name].truncated = False  # that attempt left no log to trust
        return self._failed(name, failure_class, reason)

    @property
    def state(self) -> State:
        states = {task.state for task in self._tasks.values()}
        if states == {State.PENDING}:
            return State.PENDING
        if not states <= _FINISHED:
            return State.RUNNING
        for outcome in (State.FAILED, State.ERROR):
            if outcome in states:
                return outcome
        return State.SUCCEEDED

    def view(self) -> dict:
        """JSON-ready snapshot for clients."""
        return {
            "job_id": self.job_id,
            "state": self.state.value,
            "cache_stats": {
                "hits": self._cache_hits,
                "misses": self._cache_misses,
                "bytes_saved": self._bytes_saved,
            },
            "tasks": {
                name: {
                    "state": task.state.value,
                    "needs": list(task.spec.needs),
                    "execution_id": task.execution_id,
                    "cached": task.cached_from is not None,
                    "cached_from": task.cached_from,
                    "exit_code": task.result and task.result.exit_code,
                    "timed_out": bool(task.result and task.result.timed_out),
                    "output": task.result.output_tail if task.result else "",
                    "outputs": dict(task.result.outputs) if task.result
                    else {},
                    "failure_class": (
                        task.failure_class.value if task.failure_class
                        else None
                    ),
                    "error": task.error,
                    "truncated": task.truncated,
                    "attempts": list(task.history),
                }
                for name, task in self._tasks.items()
            },
        }

    def _failed(self, name, failure_class, reason) -> list[Launch]:
        """Retry within the class's budget (D09.2), or settle the task."""
        task = self._tasks[name]
        failed = task.execution_id  # the attempt settled, before any retry
        task.failure_class = failure_class
        task.error = reason
        budget = failure_class.value.lower()
        used = task.retries_used[budget]
        if used < task.spec.retries[budget]:
            task.retries_used[budget] += 1
            delay = retry_delay(failure_class, used, self._rand())
            self._record(task, failure_class.value, reason, delay)
            # A task failure competes for a new result, so its round starts
            # over and is worth looking up; an infrastructure retry is the same
            # round, whose work is known not to have a confirmed success.
            new_round = failure_class is FailureClass.TASK
            if new_round:
                task.round += 1
            attempt = self._new_attempt(name)
            task.state = State.RETRY_WAIT
            self._record_event(
                "ATTEMPT_FAILED", failed, name,
                state=State.RETRY_WAIT.value,
                failure_class=failure_class.value,
                reason=reason, retry_in_s=delay,
            )
            self._record_event(
                "RETRY_SCHEDULED", failed, name,
                failure_class=failure_class.value, retry_in_s=delay,
                next_execution_id=attempt.execution_id,
            )
            return [Launch(attempt, delay, retry=not new_round)]
        self._record(task, failure_class.value, reason, None)
        if failure_class is FailureClass.TASK:
            task.state = State.FAILED
        else:
            task.state = State.ERROR
        self._record_event(
            "ATTEMPT_FAILED", failed, name,
            state=task.state.value, failure_class=failure_class.value,
            reason=reason, retry_in_s=None,
        )
        return self._settled(name)

    def _settled(self, name: str) -> list[Launch]:
        """`name` just finished: launch what it unblocked or skip what it
        can no longer feed."""
        if self._tasks[name].state is not State.SUCCEEDED:
            self._skip_dependents_of(name)
            return []
        return [
            self._launch(dependent)
            for dependent in self._dependents[name]
            if self._tasks[dependent].state is State.PENDING
            and all(
                self._tasks[need].state is State.SUCCEEDED
                for need in self._tasks[dependent].spec.needs
            )
        ]

    def _skip_dependents_of(self, name: str) -> None:
        pending = [name]
        while pending:  # iterative: a long chain cannot exhaust the stack
            current = pending.pop()
            for dependent in self._dependents[current]:
                task = self._tasks[dependent]
                if task.state is State.PENDING:
                    task.state = State.SKIPPED
                    task.error = f"dependency {current!r} did not succeed"
                    self._record_event(
                        "TASK_SKIPPED", "", dependent,
                        state=State.SKIPPED.value, reason=task.error,
                    )
                    pending.append(dependent)

    def _launch(self, name: str) -> Launch:
        attempt = self._new_attempt(name)
        self._tasks[name].state = State.RUNNING
        self._record_event("TASK_LAUNCHED", attempt.execution_id, name,
                           state=State.RUNNING.value)
        return Launch(attempt)

    def _new_attempt(self, name: str) -> Attempt:
        task = self._tasks[name]
        task.attempts += 1
        task.execution_id = (
            f"{self.job_id}/{name}/{task.round}/{task.attempts}"
        )
        self._running[task.execution_id] = name
        return self._attempt(name)

    def _attempt(self, name: str) -> Attempt:
        """The attempt `name` has out now."""
        task = self._tasks[name]
        inputs = dict(task.spec.inputs)
        for need in task.spec.needs:  # artifacts flow along needs (D11.3)
            inputs.update(self._tasks[need].result.outputs)
        return Attempt(
            job_id=self.job_id,
            task=name,
            round=task.round,
            execution_id=task.execution_id,
            command=task.spec.command,
            env=task.spec.env,
            inputs=inputs,
            outputs=task.spec.outputs,
            timeout_s=task.spec.timeout_s,
        )

    def _record_event(self, type: str, execution_id: str, task: str | None,
                      **payload) -> None:
        """Append one history event, stamped by the injected clock (D15.6).

        Only `scheduler.py` writes here: every transition records once, so
        replaying the same calls rebuilds the same feed.
        """
        self._events.append(Event(self._clock(), type, self.job_id, task,
                                  execution_id, payload))

    @staticmethod
    def _record(task, outcome, reason, retry_in_s):
        task.history.append({
            "execution_id": task.execution_id,
            "outcome": outcome,
            "reason": reason,
            "retry_in_s": retry_in_s,
        })


def _reason(result: TaskResult) -> str:
    """Why a command's result is not a success, for people to read."""
    if result.timed_out:
        return "timed out"
    if result.missing_outputs:
        return f"declared outputs missing: {', '.join(result.missing_outputs)}"
    if result.log_truncated:
        return "output over the log limit"  # it was killed for it (D15.13)
    return f"exit code {result.exit_code}"
