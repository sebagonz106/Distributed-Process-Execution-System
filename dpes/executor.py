"""Run one attempt on a worker. Knows nothing about Ray (plan/07, D07.17).

The coordinator turns `execute` into a Ray task. It returns, never raises,
a TaskResult or an ExecutorFailure (plan/06, D06.3).
"""

import os
import shutil
import tempfile
import time

from dpes import sandbox
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult

_HANG_S = 3600  # longer than any deadline: the coordinator cancels first


def execute(attempt: Attempt):
    fault = _injected_fault(attempt)
    if fault == "crash":
        os._exit(1)
    if fault == "hang":
        _hang()
    if fault == "network":
        return ExecutorFailure(
            attempt.execution_id, Evidence.INPUT_UNAVAILABLE, "injected fault"
        )

    try:
        workspace = tempfile.mkdtemp(prefix="dpes-")
    except OSError as error:
        return ExecutorFailure(
            attempt.execution_id, Evidence.WORKSPACE_FAILED, str(error)
        )
    try:
        outcome = sandbox.run(
            attempt.command, attempt.env, attempt.timeout_s, workspace
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    return TaskResult(
        execution_id=attempt.execution_id,
        exit_code=outcome.exit_code,
        timed_out=outcome.timed_out,
        outputs={},
        output_tail=outcome.output,
    )


def _injected_fault(attempt):
    """Test hook (plan/09, D09.7): the fault this attempt must suffer, if any.

    Only when the worker allows it. The task asks with DPES_FAULT=<kind>:<n>,
    which fails its first n attempts; anything malformed is ignored.
    """
    if os.environ.get("DPES_FAULT_INJECTION") != "1":
        return None
    kind, _, count = attempt.env.get("DPES_FAULT", "").partition(":")
    number = int(attempt.execution_id.rsplit("/", 1)[1])
    if kind not in ("crash", "hang", "network") or not count.isdigit():
        return None
    return kind if number <= int(count) else None


def _hang():
    time.sleep(_HANG_S)
