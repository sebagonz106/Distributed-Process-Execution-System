"""Run one attempt on a worker. Knows nothing about Ray (plan/07, D07.17).

The coordinator turns `execute` into a Ray task. It returns, never raises,
a TaskResult or an ExecutorFailure (plan/06, D06.3).
"""

import logging
import os
import shutil
import socket
import tempfile
import time

from dpes import sandbox
from dpes.log import event
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult

_HANG_S = 3600  # longer than any deadline: the coordinator cancels first
_log = logging.getLogger(__name__)


def execute(attempt: Attempt):
    eid = attempt.execution_id
    # Program and argument count only: arguments and env may hold secrets.
    event(_log, logging.INFO, "attempt running", execution=eid,
          host=socket.gethostname(), program=attempt.command[0],
          args=len(attempt.command) - 1)
    fault = _injected_fault(attempt)
    if fault:
        event(_log, logging.WARNING, "injected fault", execution=eid,
              kind=fault)
    if fault == "crash":
        os._exit(1)
    if fault == "hang":
        _hang()
    if fault == "network":
        return ExecutorFailure(
            eid, Evidence.INPUT_UNAVAILABLE, "injected fault"
        )

    try:
        workspace = tempfile.mkdtemp(prefix="dpes-")
    except OSError as error:
        event(_log, logging.ERROR, "no workspace", execution=eid,
              error=str(error))
        return ExecutorFailure(eid, Evidence.WORKSPACE_FAILED, str(error))
    started = time.monotonic()
    try:
        outcome = sandbox.run(
            attempt.command, attempt.env, attempt.timeout_s, workspace
        )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    event(_log, logging.INFO, "command finished", execution=eid,
          exit_code=outcome.exit_code, timed_out=outcome.timed_out,
          duration_s=round(time.monotonic() - started, 3))
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
