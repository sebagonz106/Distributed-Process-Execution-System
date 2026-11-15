"""Job specification and its validation at the trust boundary.

Pure module. Format and rules: plan/07, D07.1 to D07.3.
"""

import re
from dataclasses import dataclass
from typing import Mapping

DEFAULT_TIMEOUT_S = 600.0
MAX_TIMEOUT_S = 86_400.0

_TASK_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_JOB_FIELDS = {"tasks"}
_TASK_FIELDS = {"command", "env", "timeout_s"}


class SpecError(ValueError):
    """The job was rejected; `errors` lists every problem found."""

    def __init__(self, errors):
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass(frozen=True)
class TaskSpec:
    command: tuple[str, ...]
    env: Mapping[str, str]
    timeout_s: float


@dataclass(frozen=True)
class JobSpec:
    tasks: Mapping[str, TaskSpec]


def parse_job(document) -> JobSpec:
    """Validate a decoded JSON document; raise SpecError listing all errors."""
    if not isinstance(document, dict):
        raise SpecError(["job: must be a JSON object"])
    errors = [f"{key}: unknown field" for key in document.keys() - _JOB_FIELDS]
    raw_tasks = document.get("tasks")
    if not isinstance(raw_tasks, dict):
        errors.append("tasks: required, an object of named tasks")
        raise SpecError(errors)
    # shortcut: one task per job until dependencies exist; phase 2 lifts it.
    if len(raw_tasks) != 1:
        errors.append("tasks: phase 1 accepts exactly one task per job")

    tasks = {}
    for name, raw in raw_tasks.items():
        if not _TASK_NAME.fullmatch(name):
            errors.append(f"tasks.{name}: invalid name, 1-64 [A-Za-z0-9_.-]")
        task = _parse_task(f"tasks.{name}", raw, errors)
        if task:
            tasks[name] = task
    if errors:
        raise SpecError(errors)
    return JobSpec(tasks)


def _parse_task(path, raw, errors):
    if not isinstance(raw, dict):
        errors.append(f"{path}: must be an object")
        return None
    found = len(errors)
    errors.extend(
        f"{path}.{key}: unknown field" for key in raw.keys() - _TASK_FIELDS
    )

    command = raw.get("command")
    if not (
        isinstance(command, list)
        and command
        and all(isinstance(arg, str) for arg in command)
        and command[0]
    ):
        errors.append(f"{path}.command: required, a non-empty list of strings")

    env = raw.get("env", {})
    if not (
        isinstance(env, dict)
        and all(
            _ENV_NAME.fullmatch(key) and isinstance(value, str)
            for key, value in env.items()
        )
    ):
        errors.append(f"{path}.env: must map variable names to strings")

    timeout = raw.get("timeout_s", DEFAULT_TIMEOUT_S)
    # bool is an int in Python; "timeout_s": true must not mean 1 second.
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 0 < timeout <= MAX_TIMEOUT_S
    ):
        errors.append(f"{path}.timeout_s: a number in (0, {MAX_TIMEOUT_S:g}]")

    if len(errors) > found:
        return None
    return TaskSpec(tuple(command), dict(env), float(timeout))
