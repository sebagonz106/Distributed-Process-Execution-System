"""Job specification and its validation at the trust boundary.

Pure module. Format and rules: plan/07 (D07.1, D07.2) and plan/08 (D08.1 to
D08.3).
"""

import math
import re
from dataclasses import dataclass, field
from typing import Mapping

from dpes.dag import find_cycle

DEFAULT_TIMEOUT_S = 600.0
MAX_TIMEOUT_S = 86_400.0

_TASK_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CAPABILITY = re.compile(r"[a-z][a-z0-9_-]{0,63}")
_RAY_OWNED = {"memory", "object_store_memory"}  # Ray's own lower-case names
_JOB_FIELDS = {"tasks"}
_TASK_FIELDS = {"command", "env", "timeout_s", "needs", "resources"}


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
    needs: tuple[str, ...] = ()
    resources: Mapping[str, float] = field(
        default_factory=lambda: {"cpu": 1.0}
    )


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
    if not raw_tasks:
        errors.append("tasks: a job needs at least one task")

    tasks = {}
    for name, raw in raw_tasks.items():
        if not _TASK_NAME.fullmatch(name):
            errors.append(f"tasks.{name}: invalid name, 1-64 [A-Za-z0-9_.-]")
        task = _parse_task(f"tasks.{name}", raw, errors)
        if task:
            tasks[name] = task

    for name, task in tasks.items():
        for unknown in sorted(set(task.needs) - raw_tasks.keys()):
            errors.append(f"tasks.{name}.needs: unknown task {unknown!r}")
    cycle = find_cycle({name: task.needs for name, task in tasks.items()})
    if cycle:
        errors.append(f"tasks: dependency cycle {' -> '.join(cycle)}")
    if errors:
        raise SpecError(errors)
    return JobSpec(tasks)


def demand(task: TaskSpec) -> dict[str, float]:
    """What `task` asks of one node, in Ray's resource names (D08.3)."""
    wanted = {}
    for key, amount in task.resources.items():
        if key == "cpu":
            wanted["CPU"] = amount
        elif key == "memory_mb":
            wanted["memory"] = amount * 2**20
        else:
            wanted[key] = amount
    return wanted


def unschedulable(spec: JobSpec, capacities) -> list[str]:
    """Errors for tasks that no single node of `capacities` could ever run.

    `capacities` holds the total resources of each live node, in Ray's names.
    """
    errors = []
    for name, task in spec.tasks.items():
        wanted = demand(task)
        if not any(
            all(node.get(key, 0) >= amount for key, amount in wanted.items())
            for node in capacities
        ):
            errors.append(
                f"tasks.{name}.resources: no live node has "
                f"{dict(task.resources)}"
            )
    return errors


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
    if not (_is_amount(timeout) and timeout <= MAX_TIMEOUT_S):
        errors.append(f"{path}.timeout_s: a number in (0, {MAX_TIMEOUT_S:g}]")

    needs = raw.get("needs", [])
    names = isinstance(needs, list) and all(isinstance(n, str) for n in needs)
    if not names:
        errors.append(f"{path}.needs: must be a list of task names")
    elif len(set(needs)) != len(needs):
        errors.append(f"{path}.needs: a task is listed twice")

    resources = _parse_resources(
        f"{path}.resources", raw.get("resources", {}), errors
    )

    if len(errors) > found:
        return None
    return TaskSpec(
        tuple(command), dict(env), float(timeout), tuple(needs), resources
    )


def _parse_resources(path, raw, errors):
    if not isinstance(raw, dict):
        errors.append(f"{path}: must be an object")
        return None
    resources = {"cpu": 1.0}
    for key, amount in raw.items():
        if key == "memory_mb":
            valid = _is_amount(amount) and isinstance(amount, int)
            rule = "a whole number of megabytes above 0"
        elif key == "cpu" or (
            _CAPABILITY.fullmatch(key) and key not in _RAY_OWNED
        ):
            valid = _is_amount(amount)
            rule = "a number above 0"
        else:
            errors.append(
                f"{path}.{key}: invalid capability name, use [a-z][a-z0-9_-]"
                " and not a name Ray uses"
            )
            continue
        if valid:
            resources[key] = amount
        else:
            errors.append(f"{path}.{key}: {rule}")
    return resources


def _is_amount(value):
    # bool is an int in Python: "cpu": true must not mean 1 CPU.
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )
