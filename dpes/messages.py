"""Messages exchanged between coordinator, executor, store and log sink.

Pure module: no Ray, no disk, no clock. See plan/06, D06.1 to D06.4.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


@dataclass(frozen=True)
class ResultKey:
    """Names the single confirmed result of a task (plan/02, D02.10)."""

    job_id: str
    task: str
    round: int


@dataclass(frozen=True)
class Attempt:
    """One execution of a task, sent by the coordinator to an executor."""

    job_id: str
    task: str
    round: int  # grows only when a task failure is retried on purpose
    execution_id: str
    command: tuple[str, ...]
    env: Mapping[str, str]
    inputs: Mapping[str, str]  # workspace path -> blob digest
    outputs: tuple[str, ...]  # declared output paths
    timeout_s: float

    @property
    def result_key(self) -> ResultKey:
        # execution_id is left out so infrastructure retries of a round
        # compete for one result: that is what makes "at most one" hold.
        return ResultKey(self.job_id, self.task, self.round)


@dataclass(frozen=True)
class TaskResult:
    """What the command did. The executor saves it before returning it."""

    execution_id: str
    exit_code: int
    timed_out: bool
    outputs: Mapping[str, str]  # declared path -> digest, only those produced
    missing_outputs: tuple[str, ...] = ()
    output_tail: str = ""  # last bytes of stdout and stderr (plan/07, D07.14)

    @property
    def succeeded(self) -> bool:
        return (
            self.exit_code == 0
            and not self.timed_out
            and not self.missing_outputs
        )


class Evidence(Enum):
    """What an executor saw when it could not produce a result.

    It reports the observation; the coordinator decides the failure class.
    """

    WORKSPACE_FAILED = "workspace_failed"
    INPUT_UNAVAILABLE = "input_unavailable"
    DIGEST_MISMATCH = "digest_mismatch"
    OUTPUT_UPLOAD_FAILED = "output_upload_failed"
    RESULT_SAVE_FAILED = "result_save_failed"


@dataclass(frozen=True)
class ExecutorFailure:
    """Returned, not raised, so Ray's own exceptions stay a separate signal."""

    execution_id: str
    evidence: Evidence
    detail: str = ""


@dataclass(frozen=True)
class LogChunk:
    execution_id: str
    seq: int  # starts at 0; a repeated seq is a resend and is ignored
    data: bytes
