"""Command line client of the gateway (plan/07 D07.10; plan/11; plan/15 D15.7).

    python -m dpes.cli submit job.json
    python -m dpes.cli status <job id>
    python -m dpes.cli wait <job id>
    python -m dpes.cli artifacts <job id> <task> <folder>
    python -m dpes.cli logs <job id> <task> [-f] [-o N] [-n N]
    python -m dpes.cli cache-stats <job id>
"""

import argparse
import codecs
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from dpes.hashing import digest
from dpes.spec import is_workspace_path

EXIT_BY_STATE = {"SUCCEEDED": 0, "FAILED": 1, "ERROR": 2}
EXIT_HTTP_ERROR = 1  # the gateway refused or broke, other than 404 (D15.7)
EXIT_NOT_FOUND = 2  # 404: no such job, task or attempt (D15.7)
# No answer at all (D15.7); and, for now, every failure of the other commands.
EXIT_REQUEST_FAILED = 3

READ_CHUNK = 8192  # bytes asked of a followed log per read


class RequestFailed(Exception):
    """The gateway answered, and the answer was a failure."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status  # the HTTP code, when there was one


class NoAnswer(RequestFailed):
    """The request may or may not have taken effect: no answer at all, or a
    503 because the coordinator could not give one (plan/14, D14.5)."""


class GatewayUnreachable(NoAnswer):
    """No answer at all."""


SUBMIT_TRIES = 3
UNANSWERED_GRACE_S = 60  # how long `wait` rides out a coordinator's silence
DEFAULT_LINES = 1000  # `logs` prints this many lines from the end (D15.7)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="dpes")
    parser.add_argument(
        "--url", default=os.environ.get("DPES_URL", "http://localhost:8000")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("submit").add_argument("job_file")
    commands.add_parser("status").add_argument("job_id")
    commands.add_parser("cache-stats").add_argument("job_id")
    wait = commands.add_parser("wait")
    wait.add_argument("job_id")
    wait.add_argument("--interval", type=float, default=1.0)
    artifacts = commands.add_parser("artifacts")
    artifacts.add_argument("job_id")
    artifacts.add_argument("task")
    artifacts.add_argument("folder")
    logs = commands.add_parser("logs")
    logs.add_argument("job_id")
    logs.add_argument("task")
    logs.add_argument("-f", "--follow", action="store_true")
    logs.add_argument("-o", "--offset", type=int, default=0)
    logs.add_argument("-n", "--lines", type=int, default=None,
                      help="lines printed from the end (default 1000; "
                           "0: every one)")
    args = parser.parse_args(argv)
    base = args.url.rstrip("/")
    if args.command == "logs":
        if args.lines is not None and args.follow:
            parser.error("-n: not with -f, which prints every line")
        if args.lines is not None and args.lines < 0:
            parser.error("-n: a whole number from zero")

    try:
        if args.command == "submit":
            print(_submit(base, args.job_file))
            return 0
        if args.command == "logs":
            return _logs(base, args.job_id, args.task, args.follow,
                         args.offset, args.lines)
        job_url = f"{base}/jobs/{urllib.parse.quote(args.job_id, safe='')}"
        # Only `wait` rides out a coordinator that cannot answer (D14.5).
        read = _poll if args.command == "wait" else _call
        view = read(job_url)
        if args.command == "status":
            print(json.dumps(view, indent=2))
            return 0
        if args.command == "cache-stats":
            # What the content cache did for this job (plan/16, D16.8).
            print(json.dumps(view.get("cache_stats", {}), indent=2))
            return 0
        if args.command == "artifacts":
            _download(base, view, args.task, args.folder)
            return 0
        while view["state"] not in EXIT_BY_STATE:
            time.sleep(args.interval)
            view = read(job_url)
        print(json.dumps(view, indent=2))
        return EXIT_BY_STATE[view["state"]]
    except (RequestFailed, OSError) as error:
        print(f"dpes: {error}", file=sys.stderr)
        return _failure_code(error, args.command)


def _submit(base, job_file):
    """Upload each task's listed inputs, then send the job with digests."""
    with open(job_file, "rb") as handle:
        document = json.loads(handle.read())
    folder = os.path.dirname(os.path.abspath(job_file))
    for task in (document.get("tasks") or {}).values():
        if isinstance(task, dict) and isinstance(task.get("inputs"), list):
            task["inputs"] = {
                path: _upload(base, os.path.join(folder, path))
                for path in task["inputs"]
            }
    body = json.dumps(document).encode()
    # The same key on every try: if a reply was lost, the gateway answers
    # with the job it already created instead of a second one (D12.7).
    headers = {"Idempotency-Key": uuid.uuid4().hex}
    for tries_left in range(SUBMIT_TRIES - 1, -1, -1):
        try:
            reply = _request(f"{base}/jobs", body, headers=headers)
            return json.loads(reply)["job_id"]
        except NoAnswer:
            if not tries_left:
                raise
            time.sleep(1)


def _upload(base, path):
    with open(path, "rb") as handle:
        data = handle.read()
    name = digest(data)
    _request(f"{base}/blobs/{name}", data, method="PUT")
    return name


def _download(base, view, task, folder):
    """Fetch every output of `task`, each checked against its digest."""
    outputs = view["tasks"][task]["outputs"]
    for path, name in outputs.items():
        if not is_workspace_path(path):  # never write outside `folder`
            raise RequestFailed(f"refusing artifact path {path!r}")
        data = _request(f"{base}/blobs/{name}")
        if digest(data) != name:
            raise RequestFailed(f"{path}: content does not match {name}")
        target = os.path.join(folder, path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as handle:
            handle.write(data)


def _logs(base, job_id, task, follow, offset, lines):
    """Print the task's current attempt from `offset`, whole or as it grows."""
    url = (f"{base}/jobs/{urllib.parse.quote(job_id, safe='')}"
           f"/tasks/{urllib.parse.quote(task, safe='')}/logs")
    if follow:
        _follow(f"{url}?follow=1&offset={offset}")
        return 0
    data = _request(f"{url}?offset={offset}")
    sys.stdout.write(_tail(data, DEFAULT_LINES if lines is None else lines))
    return 0


def _follow(url):
    """Print a log as it comes, until the attempt ends or the user stops it.

    No deadline of our own: a task may be silent for longer than any timeout
    worth guessing, and the gateway ends the stream when the attempt closes
    its log (D15.4). Ctrl+C is the way out, and it is not an error; a gateway
    that does not even accept the connection is left to the system's own
    timeout."""
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    try:
        with _open(url, timeout=None) as response:
            while True:  # read1: answer as soon as some bytes are there
                piece = response.read1(READ_CHUNK)
                if not piece:
                    break
                sys.stdout.write(decoder.decode(piece))
                sys.stdout.flush()
            sys.stdout.write(decoder.decode(b"", True))  # a cut last character
    except KeyboardInterrupt:
        return


def _tail(data, lines):
    """The last `lines` lines of `data`, or all of it when `lines` is 0.

    Exact, not estimated from a byte count: a guessed offset can start in the
    middle of a line. The whole log is read to do it (plan/15, D15.7)."""
    text = data.decode("utf-8", "replace")
    if lines <= 0:
        return text
    return "".join(text.splitlines(keepends=True)[-lines:])


def _failure_code(error, command):
    """The exit code of a failed request: `logs` names the cause (D15.7).

    The other commands still answer 3 whatever went wrong; giving them the
    same distinctions is pending work (plan/15, D15.7)."""
    if command != "logs":
        return EXIT_REQUEST_FAILED
    if isinstance(error, GatewayUnreachable):
        return EXIT_REQUEST_FAILED  # D15.7's 3: nothing answered
    if getattr(error, "status", None) == 404:
        return EXIT_NOT_FOUND
    return EXIT_HTTP_ERROR


def _call(url, body=None):
    return json.loads(_request(url, body))


def _poll(url):
    """`_call`, asking again while there is no answer, for a while."""
    deadline = time.monotonic() + UNANSWERED_GRACE_S
    while True:
        try:
            return _call(url)
        except NoAnswer:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def _request(url, body=None, method=None, headers=None):
    with _open(url, body, method, headers) as response:
        return response.read()


def _open(url, body=None, method=None, headers=None, timeout=30):
    """The open response of a request, with the refusals `_request` raises."""
    request = urllib.request.Request(url, body, headers or {}, method=method)
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        with error:
            detail = _detail(error)
        # 503: the coordinator could not answer; the request may have landed.
        failure = NoAnswer if error.code == 503 else RequestFailed
        raise failure(f"HTTP {error.code}: {detail}", error.code) from None
    except urllib.error.URLError as error:
        raise GatewayUnreachable(
            f"cannot reach the gateway at {url}: {error.reason}"
        ) from None


def _detail(response):
    try:
        body = json.load(response)
    except ValueError:
        return "no details"
    return "\n".join(body.get("errors", [])) or body.get("error", "no details")


if __name__ == "__main__":
    sys.exit(main())
