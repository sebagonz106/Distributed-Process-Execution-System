"""Run one command isolated in its workspace. Linux only.

What is isolated and how: plan/07, D07.15 and D07.16.
"""

import contextlib
import os
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass

OUTPUT_LIMIT = 64 * 1024  # bytes of output kept (plan/07, D07.14)
_POLL_S = 0.1  # how often the wait looks at the deadline and the capture
_BASE_ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}
_FIRST_UID = 100_000


@dataclass(frozen=True)
class Outcome:
    exit_code: int  # negative: killed by that signal
    timed_out: bool
    output: str  # tail of stdout and stderr, interleaved as written
    overflow: bool = False  # killed for writing past its capture limit


def run(command, env, timeout_s, workspace, sink=None,
        capture_limit=None) -> Outcome:
    """Run `command` in `workspace`; its output goes to `sink`, or to a file of
    our own, and the tail of it comes back. `sink` is left open: the caller
    follows it while the command runs (plan/15, D15.3). A command that writes
    past `capture_limit` bytes is killed: truncating the file would not free
    the disk, because the next write puts it back at the same offset
    (D15.13)."""
    identity = {}
    if os.geteuid() == 0:
        # One Ray worker process runs one attempt at a time and live pids are
        # unique, so concurrent attempts always get different users.
        uid = _FIRST_UID + os.getpid()
        # Everything in it, inputs included (plan/11, D11.7).
        for folder, subfolders, files in os.walk(workspace):
            os.chown(folder, uid, uid)
            for name in files:
                os.lchown(os.path.join(folder, name), uid, uid)
        identity = {"user": uid, "group": uid, "extra_groups": []}

    # A file, not a pipe: a background child would keep a pipe open and
    # reading it to the end would wait for that child.
    with contextlib.ExitStack() as files:
        if sink is None:
            output = files.enter_context(tempfile.TemporaryFile())
        else:
            output = sink  # the caller's: closed by the caller
        try:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env={**_BASE_ENV, "HOME": workspace, **env},
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # its own process group, killed below
                **identity,
            )
        except (FileNotFoundError, PermissionError) as error:
            # The job named a program that cannot run: its fault, like a shell.
            code = 127 if isinstance(error, FileNotFoundError) else 126
            reason = f"cannot run {command[0]!r}: {error.strerror}\n"
            return Outcome(code, False, reason)
        try:
            stop = _wait(process, timeout_s, output, capture_limit)
        finally:
            _kill_group(process.pid)  # also after a clean exit: no leftovers
        return Outcome(process.wait(), stop == "timeout", _tail(output),
                       overflow=stop == "overflow")


def _wait(process, timeout_s, output, capture_limit):
    """Wait for the command; return "timeout", "overflow" or None if it ended.

    Checked in short steps: a command that floods its output must be stopped
    as soon as it passes the limit, not when the deadline arrives."""
    deadline = time.monotonic() + timeout_s
    while True:
        step = max(0.0, min(_POLL_S, deadline - time.monotonic()))
        try:
            process.wait(timeout=step)
            return None
        except subprocess.TimeoutExpired:
            pass
        if capture_limit is not None and _captured(output) > capture_limit:
            return "overflow"
        if time.monotonic() >= deadline:
            return "timeout"


def _captured(handle):
    """Bytes the command has written so far."""
    try:
        return os.fstat(handle.fileno()).st_size
    except (OSError, ValueError):
        return 0  # no file behind it: nothing to cap


def _kill_group(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # the group is already empty


def _tail(handle):
    size = handle.seek(0, os.SEEK_END)
    handle.seek(max(0, size - OUTPUT_LIMIT))
    text = handle.read().decode("utf-8", errors="replace")
    if size > OUTPUT_LIMIT:
        omitted = size - OUTPUT_LIMIT
        text = f"[{omitted} bytes of earlier output omitted]\n{text}"
    return text
