"""Command line client of the gateway (plan/07, D07.10).

    python -m dpes.cli submit job.json
    python -m dpes.cli status <job id>
    python -m dpes.cli wait <job id>
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

EXIT_BY_STATE = {"SUCCEEDED": 0, "FAILED": 1, "ERROR": 2}
EXIT_REQUEST_FAILED = 3  # rejected, unknown job or gateway unreachable


class RequestFailed(Exception):
    pass


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
    args = parser.parse_args(argv)
    base = args.url.rstrip("/")

    try:
        if args.command == "submit":
            with open(args.job_file, "rb") as handle:
                print(_call(f"{base}/jobs", handle.read())["job_id"])
            return 0
        job_url = f"{base}/jobs/{urllib.parse.quote(args.job_id, safe='')}"
        view = _call(job_url)
        if args.command == "status":
            print(json.dumps(view, indent=2))
            return 0
        while view["state"] not in EXIT_BY_STATE:
            time.sleep(args.interval)
            view = _call(job_url)
        print(json.dumps(view, indent=2))
        return EXIT_BY_STATE[view["state"]]
    except (RequestFailed, OSError) as error:
        print(f"dpes: {error}", file=sys.stderr)
        return EXIT_REQUEST_FAILED


def _call(url, body=None):
    request = urllib.request.Request(url, body)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        with error:
            detail = _detail(error)
        raise RequestFailed(f"HTTP {error.code}: {detail}") from None
    except urllib.error.URLError as error:
        raise RequestFailed(
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
