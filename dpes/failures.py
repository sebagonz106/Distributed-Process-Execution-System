"""Failure taxonomy, classifier and retry policy. Pure (plan/09).

The classifier turns what the coordinator observed into one of three classes
(D09.1); the policy says how long to wait before a retry (D09.2, D09.3).
"""

from enum import Enum

from dpes.messages import Evidence, ExecutorFailure, TaskResult


class FailureClass(str, Enum):
    TASK = "TASK"  # the command ran and its result is its own
    WORKER = "WORKER"  # the process or the node running it died
    NETWORK = "NETWORK"  # communication failed; the worker may still be alive


DEADLINE = "deadline"  # the coordinator's own per-attempt deadline expired
# The coordinator died with the attempt in flight, and Ray stops the tasks of
# an owner that dies (plan/14, D14.4).
RESTARTED = "coordinator restart"

# Retries allowed per class unless a task declares its own (D09.2).
DEFAULT_RETRIES = {"task": 0, "worker": 3, "network": 4}

_BASE_S = 1.0
_CAP_S = 30.0


def classify(observed) -> FailureClass:
    """`observed` is a TaskResult that is not a success, an ExecutorFailure,
    DEADLINE, RESTARTED, or the name of the exception Ray raised."""
    if isinstance(observed, TaskResult):
        return FailureClass.TASK
    if isinstance(observed, ExecutorFailure):
        if observed.evidence is Evidence.WORKSPACE_FAILED:
            return FailureClass.WORKER
        return FailureClass.NETWORK
    if observed == DEADLINE:
        return FailureClass.NETWORK
    # A Ray exception or RESTARTED: the process running the attempt died.
    return FailureClass.WORKER


def retry_delay(failure_class: FailureClass, retry_number: int,
                rand: float) -> float:
    """Seconds to wait before retry `retry_number` (from 0); `rand` in [0, 1].

    Network waits use "equal jitter": half the cap plus a random half, so
    retries that failed together spread out, and while the cap doubles each
    wait is at least as long as the one before. At the 30 s cap they stay
    between 15 and 30 s.
    """
    if failure_class is not FailureClass.NETWORK:
        return 0.0
    cap = min(_CAP_S, _BASE_S * 2**retry_number)
    return cap / 2 + rand * cap / 2
