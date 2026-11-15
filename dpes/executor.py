"""Run one attempt on a worker. Knows nothing about Ray (plan/07, D07.17).

The coordinator turns `execute` into a Ray task. It returns, never raises,
a TaskResult or an ExecutorFailure (plan/06, D06.3).
"""

import shutil
import tempfile

from dpes import sandbox
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult


def execute(attempt: Attempt):
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
