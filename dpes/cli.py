"""Command line client of the gateway (plan/07, D07.10; plan/11).

    python -m dpes.cli submit job.json
    python -m dpes.cli status <job id>
    python -m dpes.cli wait <job id>
    python -m dpes.cli artifacts <job id> <task> <folder>
"""

import argparse
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
EXIT_REQUEST_FAILED = 3  # rejected, unknown job or gateway unreachable


class RequestFailed(Exception):
    pass


class GatewayUnreachable(RequestFailed):
    """No answer at all: the request may or may not have arrived."""


SUBMIT_TRIES = 3


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="dpes")
    parser.add_argument(
        "--url", default=os.environ.get("DPES_URL", "http://localhost:8000")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("submit").add_argument("job_file")
    commands.add_parser("status").add_argument("job_id")
    wait = commands.add_parser("wait")
    wait.add_argument("job_id")
    wait.add_argument("--interval", type=float, default=1.0)
    artifacts = commands.add_parser("artifacts")
    artifacts.add_argument("job_id")
    artifacts.add_argument("task")
    artifacts.add_argument("folder")
    args = parser.parse_args(argv)
    base = args.url.rstrip("/")

    try:
        if args.command == "submit":
            print(_submit(base, args.job_file))
            return 0
        job_url = f"{base}/jobs/{urllib.parse.quote(args.job_id, safe='')}"
        view = _call(job_url)
        if args.command == "status":
            print(json.dumps(view, indent=2))
            return 0
        if args.command == "artifacts":
            _download(base, view, args.task, args.folder)
            return 0
        while view["state"] not in EXIT_BY_STATE:
            time.sleep(args.interval)
            view = _call(job_url)
        print(json.dumps(view, indent=2))
        return EXIT_BY_STATE[view["state"]]
    except (RequestFailed, OSError) as error:
        print(f"dpes: {error}", file=sys.stderr)
        return EXIT_REQUEST_FAILED


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
        except GatewayUnreachable:
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


def _call(url, body=None):
    return json.loads(_request(url, body))


def _request(url, body=None, method=None, headers=None):
    request = urllib.request.Request(url, body, headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        with error:
            detail = _detail(error)
        raise RequestFailed(f"HTTP {error.code}: {detail}") from None
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
