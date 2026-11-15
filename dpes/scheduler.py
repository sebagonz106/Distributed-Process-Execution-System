"""Coordinator state machine: events in, attempts to launch out.

Pure module: no Ray, no disk, no clock (plan/02, D02.3).
Phase 1 rules: plan/07, D07.4 to D07.6.
"""

from enum import Enum

from dpes.messages import Attempt, TaskResult
from dpes.spec import JobSpec, TaskSpec


class State(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"  # the command ran and its result is not a success
    ERROR = "ERROR"  # no result: the infrastructure failed


_FINISHED = {State.SUCCEEDED, State.FAILED, State.ERROR}


class _Task:
    def __init__(self, spec: TaskSpec):
        self.spec = spec
        self.state = State.PENDING
        self.round = 0
        self.attempts = 0
        self.execution_id = None  # attempt in flight or that settled the task
        self.result = None
        self.error = None


class Job:
    def __init__(self, job_id: str, spec: JobSpec):
        self.job_id = job_id
        self._tasks = {name: _Task(task) for name, task in spec.tasks.items()}

    def start(self) -> list[Attempt]:
        """Attempts for every task that has not started yet."""
        return [
            self._launch(name, task)
            for name, task in self._tasks.items()
            if task.state is State.PENDING
        ]

    def on_result(self, result: TaskResult) -> None:
        task = self._running(result.execution_id)
        if task:
            task.result = result
            task.state = State.SUCCEEDED if result.succeeded else State.FAILED

    def on_failure(self, execution_id: str, reason: str) -> None:
        task = self._running(execution_id)
        if task:
            task.error = reason
            task.state = State.ERROR

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
                    "execution_id": task.execution_id,
                    "exit_code": task.result and task.result.exit_code,
                    "timed_out": bool(task.result and task.result.timed_out),
                    "output": task.result.output_tail if task.result else "",
                    "error": task.error,
                }
                for name, task in self._tasks.items()
            },
        }

    def _launch(self, name: str, task: _Task) -> Attempt:
        task.attempts += 1
        task.state = State.RUNNING
        task.execution_id = (
            f"{self.job_id}/{name}/{task.round}/{task.attempts}"
        )
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

    def _running(self, execution_id: str):
        """The task whose current attempt is `execution_id`, if still running.

        Anything else is a duplicate, late or unknown event and is ignored.
        """
        for task in self._tasks.values():
            running = task.state is State.RUNNING
            if running and task.execution_id == execution_id:
                return task
        return None
