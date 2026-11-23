"""HTTP gateway: validates jobs at the trust boundary, forwards them.

Talks to any object with `submit(spec)`, `status(job_id)`, `events(job_id)` and
the log calls (`read_fragments`, `tail_fragments`, `log_size`) (plan/07, D07.9;
plan/15, D15.5 and D15.15). Run on the control node with
    `python -m dpes.gateway`.
"""

import json
import logging
import queue
import re
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from dpes.hashing import digest
from dpes.log import event, setup
from dpes.spec import SpecError, parse_job

MAX_BODY = 1 << 20  # checked before reading, so a client cannot exhaust memory
MAX_BLOB = 16 << 20  # blobs travel whole in memory (plan/11, D11.5)
PORT = 8000
FOLLOW_POLL_S = 0.5  # how often a follower checks its client is still there
_JOB_PATH = re.compile(r"/jobs/([^/]+)")
_LOG_PATH = re.compile(r"/jobs/([^/]+)/tasks/([^/]+)/logs")
_EVENTS_PATH = re.compile(r"/jobs/([^/]+)/events")
_BLOB_PATH = re.compile(r"/blobs/(sha256:[0-9a-f]{64})")
_RANGE = re.compile(r"bytes=(\d*)-(\d*)")
_KEY = re.compile(r"[A-Za-z0-9_-]{1,64}")  # idempotency key (plan/12, D12.7)
# Not __name__: run with `python -m`, this module is "__main__", outside the
# "dpes" logger tree that setup() configures, and every line would be lost.
_log = logging.getLogger("dpes.gateway")


def make_server(coordinator, host: str, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.coordinator = coordinator
    return server


class _Handler(BaseHTTPRequestHandler):
    # Chunked is defined for 1.1; every answer closes, so no kept-alive
    # connection can carry the body of a request this handler refused unread.
    protocol_version = "HTTP/1.1"

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
        key = self.headers.get("Idempotency-Key")
        if key is not None and not _KEY.fullmatch(key):
            return self._reject(["Idempotency-Key: 1-64 of [A-Za-z0-9_-]"])
        try:
            job_id = self.server.coordinator.submit(spec, key)
        except SpecError as error:  # cannot run, or a reused key (D12.7)
            return self._reject(error.errors)
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        # The key is the client's handle on its resends, not a secret (D10.12).
        event(_log, logging.INFO, "job accepted", job=job_id,
              tasks=len(spec.tasks),
              **({"idempotency_key": key} if key else {}))
        self._send(201, {"job_id": job_id})

    def do_PUT(self):
        match = _BLOB_PATH.fullmatch(self.path)
        if not match:
            return self._send(404, {"error": "not found"})
        length = self.headers.get("Content-Length", "")
        if not length.isdigit():
            return self._refuse(411, "Content-Length required")
        if int(length) > MAX_BLOB:
            return self._refuse(413, f"blob over {MAX_BLOB} bytes")
        data = self.rfile.read(int(length))
        if digest(data) != match[1]:  # checked on every hop (D11.4)
            return self._refuse(400, "content does not match its digest")
        try:
            self.server.coordinator.put_blob(match[1], data)
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        event(_log, logging.INFO, "blob stored", digest=match[1],
              bytes=len(data))
        self._send(201, {"digest": match[1]})

    def do_GET(self):
        path = urlsplit(self.path).path  # the query string carries the offsets
        blob = _BLOB_PATH.fullmatch(path)
        if blob:
            return self._serve_blob(blob[1])
        logs = _LOG_PATH.fullmatch(path)
        if logs:
            return self._serve_logs(unquote(logs[1]), unquote(logs[2]))
        feed = _EVENTS_PATH.fullmatch(path)
        if feed:
            return self._serve_events(unquote(feed[1]))
        match = _JOB_PATH.fullmatch(path)
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

    def _serve_blob(self, name):
        try:
            data = self.server.coordinator.get_blob(name)
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        if data is None:
            return self._send(404, {"error": "unknown blob"})
        event(_log, logging.INFO, "blob served", digest=name, bytes=len(data))
        self._start(200, "application/octet-stream", len(data))
        self.wfile.write(data)

    def _serve_events(self, job_id):
        """The job's event feed, oldest first, each time as ISO 8601 (D15.5).

        The actor keeps the events with the float the journal recorded, so a
        restart rebuilds them exactly; the wire format is this layer's job."""
        try:
            feed = self.server.coordinator.events(job_id)
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)
        # Clients poll this as they poll the status: below INFO (D10.5).
        event(_log, logging.DEBUG, "events served", job=job_id,
              found=feed is not None,
              **({"events": len(feed)} if feed is not None else {}))
        if feed is None:
            return self._send(404, {"error": "unknown job"})
        self._send(200, [_wire(e) for e in feed])

    def _serve_logs(self, job_id, task):
        """The log of the task's current attempt: by offset, or as it grows."""
        try:
            return self._answer_logs(job_id, task)
        except (BrokenPipeError, ConnectionResetError) as error:
            # Only these two mean the client left before its body was out:
            # anything else is the store's or the coordinator's, and has to
            # reach the client as a 503, which `logs` turns into its own code
            # instead of a silent cut (D15.7).
            event(_log, logging.WARNING, "log cut short", job=job_id,
                  error=f"{type(error).__name__}: {error}")
        except Exception as error:  # whatever broke, the gateway stays up
            return self._unavailable(error)

    def _answer_logs(self, job_id, task):
        view = self.server.coordinator.status(job_id)
        if view is None:
            return self._send(404, {"error": "unknown job"})
        task_view = view["tasks"].get(task)
        if task_view is None:
            return self._send(404, {"error": "unknown task"})
        execution_id = task_view.get("execution_id")
        if execution_id is None:
            # A hit has no attempt of its own, but its work did run once: the
            # log of that execution is the one to serve (D16.5).
            execution_id = task_view.get("cached_from")
        if execution_id is None:
            return self._send(404, {"error": "the task has not run"})
        try:
            offset, limit, follow, ranged = _log_request(
                parse_qs(urlsplit(self.path).query), self.headers
            )
        except ValueError as error:
            return self._refuse(400, str(error))
        if follow:
            return self._follow(execution_id, offset)
        size = None
        if ranged:
            size = self.server.coordinator.log_size(execution_id)
            if offset >= size:
                return self._refuse(
                    416, f"range beyond the {size}-byte log",
                    (("Content-Range", f"bytes */{size}"),),
                )
        pieces = self.server.coordinator.read_fragments(
            execution_id, offset, limit)
        data = b"".join(piece for _, piece in pieces)
        extra = ()
        if ranged:
            last = offset + len(data) - 1
            extra = (("Content-Range", f"bytes {offset}-{last}/{size}"),)
        self._start(206 if ranged else 200, "text/plain; charset=utf-8",
                    len(data), extra)
        # The record goes out before the last byte of the body: a client that
        # has read the whole log can already see why it was served (D15.15),
        # and a test that reads it cannot race the record.
        event(_log, logging.INFO, "logs served", execution=execution_id,
              offset=offset, bytes=len(data),
              **({"limit": limit} if limit is not None else {}))
        self.wfile.write(data)

    def _follow(self, execution_id, offset):
        """Stream the log as it grows, one chunk per fragment (D15.4).

        Ends when the attempt closes its log, when the idle limit passes in the
        store, or when the client leaves: the tail runs on its own thread so a
        quiet attempt cannot hold this handler for minutes (D15.15)."""
        reader = _Reader(self.server.coordinator.tail_fragments(
            execution_id, offset))
        sent, noted = 0, False
        try:
            self._start(200, "text/plain; charset=utf-8", None,
                        (("Transfer-Encoding", "chunked"),))
            while True:
                try:
                    data = reader.get(timeout=FOLLOW_POLL_S)
                except queue.Empty:
                    if self._client_gone():
                        return self._note_follow(execution_id, offset, sent,
                                                 "client")
                    continue
                if data is None:  # the log ended: nothing more will come
                    break
                if data:  # never an empty chunk: it would end the body
                    self._chunk(data)
                    sent += len(data)
            # Say it ended, then finish the body: the record is in place before
            # the end can be seen, and the follow is recorded exactly once.
            self._note_follow(execution_id, offset, sent, "end")
            noted = True
            try:
                self.wfile.write(b"0\r\n\r\n")  # the last chunk ends the body
            except (BrokenPipeError, ConnectionResetError):
                pass  # the log itself went out; the client left at the trailer
        except (BrokenPipeError, ConnectionResetError):
            if not noted:  # it left while a fragment was being written
                self._note_follow(execution_id, offset, sent, "client")
        except Exception as error:  # the store, or the socket, failed under us
            if not noted:
                self._note_follow(execution_id, offset, sent, "error",
                                  f"{type(error).__name__}: {error}")

    def _note_follow(self, execution_id, offset, sent, stopped, fault=None):
        """Record how a followed log ended (D15.15)."""
        fields = {"execution": execution_id, "offset": offset,
                  "bytes": sent, "stopped": stopped}
        if fault:
            event(_log, logging.WARNING, "log cut off", error=fault, **fields)
        else:
            event(_log, logging.INFO, "log followed", **fields)

    def _client_gone(self):
        """Whether the client closed a stream that has nothing to write."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and not self.connection.recv(
                1, socket.MSG_PEEK)
        except OSError:  # reset by the peer: nothing left to write to
            return True

    def _chunk(self, data):
        """One chunk of a chunked body."""
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")

    def _reject(self, errors):
        event(_log, logging.WARNING, "job rejected", errors=len(errors),
              first=errors[0])
        self._send(400, {"errors": errors})

    def _refuse(self, code, reason, extra=()):
        event(_log, logging.WARNING, "request refused", status=code,
              reason=reason)
        self._send(code, {"error": reason}, extra)

    def _unavailable(self, error):
        event(_log, logging.ERROR, "coordinator unavailable",
              error=f"{type(error).__name__}: {error}")
        self._send(503, {"error": f"coordinator unavailable: {error}"})

    def _send(self, code, body, extra=()):
        data = json.dumps(body).encode()
        self._start(code, "application/json", len(data), extra)
        self.wfile.write(data)

    def _start(self, code, content_type, length=None, extra=()):
        """Begin a response. `keep-alive` is never used (D15.15), and a
        missing length means chunked, which the caller writes itself."""
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        for name, value in extra:
            self.send_header(name, value)
        if length is not None:
            self.send_header("Content-Length", str(length))
        self.send_header("Connection", "close")
        self.end_headers()

    def log_message(self, *args):
        pass  # replaced by the descriptive lines above (plan/10)


class _Reader:
    """A log tail read on its own thread, so this handler can notice that its
    client left while the attempt says nothing (D15.15). Ray cannot cancel a
    remote generator, so a tail already running ends on its own (D15.4); what
    this frees is the handler's thread, which would otherwise wait for the next
    fragment."""

    def __init__(self, stream):
        self._stream = stream
        self._pieces = queue.Queue()
        threading.Thread(target=self._read, daemon=True,
                         name="dpes-tail").start()

    def _read(self):
        try:
            for _, data in self._stream:
                self._pieces.put(data)
        except Exception as error:  # the store or the way to it failed
            self._pieces.put(error)
        finally:
            self._pieces.put(None)  # the tail ended, well or badly

    def get(self, timeout):
        """The next fragment's bytes, None once the log ended, or timeout."""
        item = self._pieces.get(timeout=timeout)
        if isinstance(item, Exception):
            raise item
        return item


def _wire(event_dict):
    """One event as the client sees it: the same keys, the time in ISO 8601.

    The actor keeps the wall-clock float because that is what the journal
    replays; only the transport decides how a time is written (D15.5)."""
    return dict(event_dict, timestamp=_iso(event_dict["timestamp"]))


def _iso(timestamp):
    """A wall-clock time as ISO 8601 UTC with milliseconds (D15.5)."""
    millis = round(timestamp * 1000)  # rounding can roll into the next second
    return (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(millis // 1000))
            + f".{millis % 1000:03d}Z")


def _log_request(query, headers):
    """(offset, limit, follow, ranged): what a log request asks for.

    `Range` and the query parameters are two ways to say the same bytes, so a
    request that mixes them is a mistake worth reporting (D15.15)."""
    offset = _whole(query, "offset", 0)
    limit = _whole(query, "limit", None)
    follow = query.get("follow", ["0"])[-1]
    if follow not in ("0", "1"):
        raise ValueError("follow: 0 or 1")
    range_header = headers.get("Range")
    if range_header is None:
        return offset, limit, follow == "1", False
    if follow == "1":
        raise ValueError("Range: not with follow")
    if "offset" in query or "limit" in query:
        raise ValueError("Range: not with offset or limit")
    start, last = _range(range_header)
    if start is None:  # not a byte range this server serves: the whole log
        return offset, limit, False, False
    return start, None if last is None else last - start + 1, False, True


def _whole(query, name, default):
    """A whole number from the query string, or `default` when absent."""
    values = query.get(name)
    if not values:
        return default
    text = values[-1]
    # ASCII only: `int` does not read every character `isdigit` accepts.
    if not text.isascii() or not text.isdigit():
        raise ValueError(f"{name}: a whole number")
    return int(text)


def _range(header):
    """(first, last) of a `Range: bytes=N-M` header, None to be ignored.

    A suffix range (`bytes=-500`) and anything malformed are ignored, which is
    what HTTP allows a server to do with a range it does not serve."""
    match = _RANGE.fullmatch(header.strip())
    if not match or not match[1]:
        return None, None
    first = int(match[1])
    last = int(match[2]) if match[2] else None
    if last is not None and last < first:
        return None, None
    return first, last


def main():
    from dpes.coordinator import connect  # Ray is only needed in the cluster

    setup()
    server = make_server(connect(), "0.0.0.0", PORT)
    event(_log, logging.INFO, "gateway listening", port=PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
