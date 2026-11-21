"""Drive the real gateway with the real client, as a user would.

Shared by the end-to-end tests; they run inside the control node, where the
gateway listens (plan/07, D07.18).
"""

import contextlib
import io
import json
import os
import tempfile
import time
import urllib.error
import urllib.request

from dpes import cli as dpes_cli

URL = "http://localhost:8000"


def wait_for_gateway(timeout_s=60):
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            urllib.request.urlopen(f"{URL}/jobs/none", timeout=2)
        except urllib.error.HTTPError:
            return  # it answered, so it is up
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def cli(*args, url=URL):
    """(exit code, stdout, stderr) of `python -m dpes.cli --url URL *args`."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = dpes_cli.main(["--url", url, *args])
    return code, out.getvalue(), err.getvalue()


def submit(tasks):
    """Submit a job made of `tasks`; (exit code, job id or "", stderr)."""
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "job.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"tasks": tasks}, handle)
        code, out, err = cli("submit", path)
    return code, out.strip(), err


def wait(job_id):
    """(exit code, final view) of `wait job_id`."""
    code, out, err = cli("wait", job_id, "--interval", "0.2")
    if not out:
        raise AssertionError(f"wait {job_id} failed: {err}")
    return code, json.loads(out)


def run_job(tasks):
    code, job_id, err = submit(tasks)
    if code != 0:
        raise AssertionError(f"the job was rejected: {err}")
    return wait(job_id)
