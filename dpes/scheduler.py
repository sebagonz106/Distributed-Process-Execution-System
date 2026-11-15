"""Coordinator state machine: events in, attempts to launch out.

Pure module: no Ray, no disk, no clock (plan/02, D02.3); randomness for retry
waits is injected. Rules: plan/07 (D07.4 to D07.6), plan/08 (D08.4, D08.5) and
plan/09 (D09.2 to D09.6).
"""

import random
from dataclasses import dataclass
from enum import Enum

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
    """Launch `attempt` after waiting `delay_s` seconds."""

    attempt: Attempt
    delay_s: float = 0.0


class _Task:
    def __init__(self, spec: TaskSpec):
        self.spec = spec
        self.state = State.PENDING
        self.round = 0
        self.attempts = 0
        self.execution_id = None  # attempt in flight or that settled the task
        self.result = None
        self.error = None
        self.failure_class = None
        self.retries_used = {key: 0 for key in spec.retries}
        self.history = []


class Job:
    def __init__(self, job_id: str, spec: JobSpec, rand=random.random):
        self.job_id = job_id
        self.spec = spec
        self._rand = rand
        self._tasks = {name: _Task(task) for name, task in spec.tasks.items()}
        self._dependents = {name: [] for name in spec.tasks}
        for name, task in spec.tasks.items():
            for need in task.needs:
                self._dependents[need].append(name)
        self._running = {}  # execution id -> task name

    def start(self) -> list[Launch]:
        """Launches for the tasks that depend on nothing."""
        return [
            self._launch(name)
            for name, task in self._tasks.items()
            if task.state is State.PENDING and not task.spec.needs
        ]

    def running(self, execution_id: str) -> None:
        """A retry that was waiting has just been launched."""
        name = self._running.get(execution_id)
        if name and self._tasks[name].state is State.RETRY_WAIT:
            self._tasks[name].state = State.RUNNING

    def on_result(self, result: TaskResult) -> list[Launch]:
        name = self._running.pop(result.execution_id, None)
        if name is None:
            return []  # duplicate, late or unknown: ignored (D07.6)
        task = self._tasks[name]
        task.result = result
        if not result.succeeded:
            return self._failed(name, FailureClass.TASK, _reason(result))
        task.state = State.SUCCEEDED
        task.error = task.failure_class = None
        self._record(task, "SUCCEEDED", "", None)
        return self._settled(name)

    def on_failure(self, execution_id: str, failure_class: FailureClass,
                   reason: str) -> list[Launch]:
        name = self._running.pop(execution_id, None)
        if name is None:
            return []
        self._tasks[name].result = None
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
            "tasks": {
                name: {
                    "state": task.state.value,
                    "needs": list(task.spec.needs),
                    "execution_id": task.execution_id,
                    "exit_code": task.result and task.result.exit_code,
                    "timed_out": bool(task.result and task.result.timed_out),
                    "output": task.result.output_tail if task.result else "",
                    "failure_class": (
                        task.failure_class.value if task.failure_class
                        else None
                    ),
                    "error": task.error,
                    "attempts": list(task.history),
                }
                for name, task in self._tasks.items()
            },
        }

    def _failed(self, name, failure_class, reason) -> list[Launch]:
        """Retry within the class's budget (D09.2), or settle the task."""
        task = self._tasks[name]
        task.failure_class = failure_class
        task.error = reason
        budget = failure_class.value.lower()
        used = task.retries_used[budget]
        if used < task.spec.retries[budget]:
            task.retries_used[budget] += 1
            delay = retry_delay(failure_class, used, self._rand())
            self._record(task, failure_class.value, reason, delay)
            if failure_class is FailureClass.TASK:
                task.round += 1  # a new round competes for a new result
            attempt = self._new_attempt(name)
            task.state = State.RETRY_WAIT
            return [Launch(attempt, delay)]
        self._record(task, failure_class.value, reason, None)
        if failure_class is FailureClass.TASK:
            task.state = State.FAILED
        else:
            task.state = State.ERROR
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
                    pending.append(dependent)

    def _launch(self, name: str) -> Launch:
        attempt = self._new_attempt(name)
        self._tasks[name].state = State.RUNNING
        return Launch(attempt)

    def _new_attempt(self, name: str) -> Attempt:
        task = self._tasks[name]
        task.attempts += 1
        task.execution_id = (
            f"{self.job_id}/{name}/{task.round}/{task.attempts}"
        )
        self._running[task.execution_id] = name
        return Attempt(
            job_id=self.job_id,
            task=name,
            round=task.round,
            execution_id=task.execution_id,
            command=task.spec.command,
            env=task.spec.env,
            inputs={},
            outputs=(),
            timeout_s=task.spec.timeout_s,
        )

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
    return f"exit code {result.exit_code}"
