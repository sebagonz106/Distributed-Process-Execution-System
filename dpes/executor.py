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
from dpes.hashing import digest
from dpes.log import event
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult

_HANG_S = 3600  # longer than any deadline: the coordinator cancels first
_log = logging.getLogger(__name__)


def execute(attempt: Attempt, store):
    """`store` has get(digest) -> bytes or None, and put(bytes) -> digest."""
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
    try:
        failure = _place_inputs(attempt, store, workspace)
        if failure:
            return failure
        started = time.monotonic()
        outcome = sandbox.run(
            attempt.command, attempt.env, attempt.timeout_s, workspace
        )
        event(_log, logging.INFO, "command finished", execution=eid,
              exit_code=outcome.exit_code, timed_out=outcome.timed_out,
              duration_s=round(time.monotonic() - started, 3))
        outputs, missing = {}, []
        if outcome.exit_code == 0 and not outcome.timed_out:
            failure = _collect_outputs(attempt, store, workspace, outputs,
                                       missing)
            if failure:
                return failure
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    return TaskResult(
        execution_id=eid,
        exit_code=outcome.exit_code,
        timed_out=outcome.timed_out,
        outputs=outputs,
        missing_outputs=tuple(missing),
        output_tail=outcome.output,
    )


def _place_inputs(attempt, store, workspace):
    """Put every input in place, checked against its digest (D11.4)."""
    eid = attempt.execution_id
    for path, name in attempt.inputs.items():
        try:
            data = store.get(name)
        except Exception as error:  # the store or the way to it failed
            return _transfer_failed(eid, Evidence.INPUT_UNAVAILABLE,
                                    f"{path}: {error}")
        if data is None:
            return _transfer_failed(eid, Evidence.INPUT_UNAVAILABLE,
                                    f"{path}: {name} is not stored")
        if digest(data) != name:
            return _transfer_failed(eid, Evidence.DIGEST_MISMATCH, path)
        target = os.path.join(workspace, path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as handle:
            handle.write(data)
    return None


def _collect_outputs(attempt, store, workspace, outputs, missing):
    """Store each declared output; only regular files, never through a link
    (D11.6): the executor is root and must not leak the worker's files."""
    root = os.path.realpath(workspace) + os.sep
    for path in attempt.outputs:
        full = os.path.join(workspace, path)
        real = os.path.realpath(full)
        if (os.path.islink(full) or not real.startswith(root)
                or not os.path.isfile(real)):
            missing.append(path)
            continue
        with open(real, "rb") as handle:
            data = handle.read()
        try:
            outputs[path] = store.put(data)
        except Exception as error:  # the store or the way to it failed
            return _transfer_failed(attempt.execution_id,
                                    Evidence.OUTPUT_UPLOAD_FAILED,
                                    f"{path}: {error}")
    return None


def _transfer_failed(eid, evidence, detail):
    event(_log, logging.WARNING, "transfer failed", execution=eid,
          evidence=evidence.value, detail=detail)
    return ExecutorFailure(eid, evidence, detail)


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
