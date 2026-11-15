"""Coordinator state machine: events in, attempts to launch out.

Pure module: no Ray, no disk, no clock (plan/02, D02.3).
Rules: plan/07 (D07.4 to D07.6) and plan/08 (D08.4, D08.5).
"""

from enum import Enum

from dpes.messages import Attempt, TaskResult
from dpes.spec import JobSpec, TaskSpec


class State(str, Enum):
    PENDING = "PENDING"  # waiting for its dependencies
    RUNNING = "RUNNING"  # an attempt is in flight, possibly queued in Ray
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"  # the command ran and its result is not a success
    ERROR = "ERROR"  # no result: the infrastructure failed
    SKIPPED = "SKIPPED"  # a dependency did not succeed


_FINISHED = {State.SUCCEEDED, State.FAILED, State.ERROR, State.SKIPPED}


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
        self.spec = spec
        self._tasks = {name: _Task(task) for name, task in spec.tasks.items()}
        self._dependents = {name: [] for name in spec.tasks}
        for name, task in spec.tasks.items():
            for need in task.needs:
                self._dependents[need].append(name)
        self._running = {}  # execution id -> task name

    def start(self) -> list[Attempt]:
        """Attempts for the tasks that depend on nothing."""
        return [
            self._launch(name)
            for name, task in self._tasks.items()
            if task.state is State.PENDING and not task.spec.needs
        ]

    def on_result(self, result: TaskResult) -> list[Attempt]:
        name = self._running.pop(result.execution_id, None)
        if name is None:
            return []  # duplicate, late or unknown: ignored (D07.6)
        task = self._tasks[name]
        task.result = result
        task.state = State.SUCCEEDED if result.succeeded else State.FAILED
        return self._settled(name)

    def on_failure(self, execution_id: str, reason: str) -> list[Attempt]:
        name = self._running.pop(execution_id, None)
        if name is None:
            return []
        task = self._tasks[name]
        task.error = reason
        task.state = State.ERROR
        return self._settled(name)

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
                    "error": task.error,
                }
                for name, task in self._tasks.items()
            },
        }

    def _settled(self, name: str) -> list[Attempt]:
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

    def _launch(self, name: str) -> Attempt:
        task = self._tasks[name]
        task.attempts += 1
        task.state = State.RUNNING
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
