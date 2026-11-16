"""Run one command isolated in its workspace. Linux only.

What is isolated and how: plan/07, D07.15 and D07.16.
"""

import os
import signal
import subprocess
import tempfile
from dataclasses import dataclass

OUTPUT_LIMIT = 64 * 1024  # bytes of output kept (plan/07, D07.14)
_BASE_ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}
_FIRST_UID = 100_000


@dataclass(frozen=True)
class Outcome:
    exit_code: int  # negative: killed by that signal
    timed_out: bool
    output: str  # tail of stdout and stderr, interleaved as written


def run(command, env, timeout_s, workspace) -> Outcome:
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
    with tempfile.TemporaryFile() as output:
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
            process.wait(timeout=timeout_s)
            timed_out = False
        except subprocess.TimeoutExpired:
            timed_out = True
        _kill_group(process.pid)  # also after a clean exit: no leftovers
        return Outcome(process.wait(), timed_out, _tail(output))


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
