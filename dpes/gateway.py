"""HTTP gateway: validates jobs at the trust boundary, forwards them.

It knows nothing about Ray: it talks to any object with `submit(spec)` and
`status(job_id)` (plan/07, D07.9). Run on the control node with
`python -m dpes.gateway`.
"""

import json
import logging
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dpes.log import event, setup
from dpes.spec import SpecError, parse_job

MAX_BODY = 1 << 20  # checked before reading, so a client cannot exhaust memory
PORT = 8000
_JOB_PATH = re.compile(r"/jobs/([^/]+)")
# Not __name__: run with `python -m`, this module is "__main__", outside the
# "dpes" logger tree that setup() configures, and every line would be lost.
_log = logging.getLogger("dpes.gateway")


def make_server(coordinator, host: str, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.coordinator = coordinator
    return server


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/jobs":
            return self._send(404, {"error": "not found"})
        length = self.headers.get("Content-Length", "")
        if not length.isdigit():
            return self._refuse(411, "Content-Length required")
        if int(length) > MAX_BODY:
            return self._refuse(413, f"body over {MAX_BODY} bytes")
        try:
            document = json.loads(self.rfile.read(int(length)))
        except ValueError:
            return self._reject(["body: not valid JSON"])
        try:
            spec = parse_job(document)
        except SpecError as error:
            return self._reject(error.errors)
        try:
            job_id = self.server.coordinator.submit(spec)
        except SpecError as error:  # valid, but no node could run it (D08.3)
            return self._reject(error.errors)
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        event(_log, logging.INFO, "job accepted", job=job_id,
              tasks=len(spec.tasks))
        self._send(201, {"job_id": job_id})

    def do_GET(self):
        match = _JOB_PATH.fullmatch(self.path)
        if not match:
            return self._send(404, {"error": "not found"})
        try:
            view = self.server.coordinator.status(match[1])
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        # Clients poll this many times a second: below INFO (D10.5).
        event(_log, logging.DEBUG, "status served", job=match[1],
              found=view is not None)
        if view is None:
            return self._send(404, {"error": "unknown job"})
        self._send(200, view)

    def _reject(self, errors):
        event(_log, logging.WARNING, "job rejected", errors=len(errors),
              first=errors[0])
        self._send(400, {"errors": errors})

    def _refuse(self, code, reason):
        event(_log, logging.WARNING, "request refused", status=code,
              reason=reason)
        self._send(code, {"error": reason})

    def _unavailable(self, error):
        event(_log, logging.ERROR, "coordinator unavailable",
              error=f"{type(error).__name__}: {error}")
        self._send(503, {"error": f"coordinator unavailable: {error}"})

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass  # replaced by the descriptive lines above (plan/10)


def main():
    from dpes.coordinator import connect  # Ray is only needed in the cluster

    setup()
    server = make_server(connect(), "0.0.0.0", PORT)
    event(_log, logging.INFO, "gateway listening", port=PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
