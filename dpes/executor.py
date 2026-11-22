"""Run one attempt on a worker. Knows nothing about Ray (plan/07, D07.17).

The coordinator turns `execute` into a Ray task. It returns, never raises,
a TaskResult or an ExecutorFailure (plan/06, D06.3).
"""

import logging
import os
import shutil
import socket
import tempfile
import threading
import time

from dpes import sandbox
from dpes.hashing import digest
from dpes.log import event
from dpes.logstore import LogTruncated
from dpes.messages import Attempt, Evidence, ExecutorFailure, TaskResult

_HANG_S = 3600  # longer than any deadline: the coordinator cancels first
_SAVE_WAITS = (0.5, 1, 2)  # seconds before each retry of the save (D12.3)
_SEND_WAITS = (0.5, 1, 2)  # seconds before each retry of a fragment (D15.8)
_FAULTS = ("crash", "hang", "network", "lose-ack")
FRAGMENT_BYTES = 4096  # what one fragment holds at most (D15.3)
FRAGMENT_LINES = 100  # ... or the lines it holds, whichever comes first
FLUSH_S = 0.5  # ... or how long it waits with nothing more to add
FOLLOW_S = 0.05  # how often the emitter looks for new output
JOIN_S = 3.0  # how long the attempt waits for its log to end (RUN_MARGIN_S)
# Output one attempt may capture and ship: past it the command is killed, since
# truncating the file it writes to would not free the disk (D15.13).
MAX_CAPTURE_BYTES = int(os.environ.get("DPES_LOG_CAPTURE_MIB", "50")) << 20
_log = logging.getLogger(__name__)


def execute(attempt: Attempt, store):
    """`store` has get(digest) -> bytes or None, put(bytes) -> digest,
    save_result(key, result) -> the confirmed result for that key, and
    append_fragment / close_fragments for the log (plan/15, D15.8)."""
    eid = attempt.execution_id
    # Program and argument count only: arguments and env may hold secrets.
    event(_log, logging.INFO, "attempt running", execution=eid,
          host=socket.gethostname(), program=attempt.command[0],
          args=len(attempt.command) - 1)
    fault = _injected_fault(attempt)
    if fault:
        event(_log, logging.WARNING, "injected fault", execution=eid,
              kind=fault)
    if fault == "crash":
        os._exit(1)
    if fault == "hang":
        _hang()
    if fault == "network":
        return ExecutorFailure(
            eid, Evidence.INPUT_UNAVAILABLE, "injected fault"
        )

    try:
        workspace = tempfile.mkdtemp(prefix="dpes-")
    except OSError as error:
        event(_log, logging.ERROR, "no workspace", execution=eid,
              error=str(error))
        return ExecutorFailure(eid, Evidence.WORKSPACE_FAILED, str(error))
    try:
        failure = _place_inputs(attempt, store, workspace)
        if failure:
            return failure
        try:
            emission = _Emitter(store, eid).start()
        except OSError as error:
            event(_log, logging.ERROR, "no file for the output",
                  execution=eid, error=str(error))
            return ExecutorFailure(eid, Evidence.WORKSPACE_FAILED,
                                   str(error))
        started = time.monotonic()
        try:
            outcome = sandbox.run(
                attempt.command, attempt.env, attempt.timeout_s, workspace,
                sink=emission.sink, capture_limit=MAX_CAPTURE_BYTES,
            )
        finally:
            emission.stop()  # however the command ended, its log ends here
        event(_log, logging.INFO, "command finished", execution=eid,
              exit_code=outcome.exit_code, timed_out=outcome.timed_out,
              duration_s=round(time.monotonic() - started, 3))
        outputs, missing = {}, []
        if outcome.exit_code == 0 and not outcome.timed_out:
            failure = _collect_outputs(attempt, store, workspace, outputs,
                                       missing)
            if failure:
                return failure
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    return _confirm(attempt, store, fault, TaskResult(
        execution_id=eid,
        exit_code=outcome.exit_code,
        timed_out=outcome.timed_out,
        outputs=outputs,
        missing_outputs=tuple(missing),
        output_tail=outcome.output,
        log_truncated=emission.truncated or outcome.overflow,
    ))


def _confirm(attempt, store, fault, result):
    """Save the result before answering: the confirmation point (plan/12,
    D12.2). Returns the winner, which another attempt may have saved."""
    eid = attempt.execution_id
    last = None
    for wait in (0, *_SAVE_WAITS):
        if last:
            event(_log, logging.WARNING, "result save failed", execution=eid,
                  error=f"{type(last).__name__}: {last}", retry_in_s=wait)
        time.sleep(wait)
        try:
            winner = store.save_result(attempt.result_key, result)
            break
        except Exception as error:  # the store or the way to it failed
            last = error
    else:
        return _transfer_failed(eid, Evidence.RESULT_SAVE_FAILED, str(last))
    event(_log, logging.INFO, "result confirmed", execution=eid,
          winner=winner.execution_id)
    if fault == "lose-ack":
        os._exit(1)  # saved, but the answer never leaves (D12.6)
    return winner


def _place_inputs(attempt, store, workspace):
    """Put every input in place, checked against its digest (D11.4)."""
    eid = attempt.execution_id
    placed = 0
    for path, name in attempt.inputs.items():
        try:
            data = store.get(name)
        except Exception as error:  # the store or the way to it failed
            return _transfer_failed(eid, Evidence.INPUT_UNAVAILABLE,
                                    f"{path}: {error}")
        if data is None:
            return _transfer_failed(eid, Evidence.INPUT_UNAVAILABLE,
                                    f"{path}: {name} is not stored")
        if digest(data) != name:
            return _transfer_failed(eid, Evidence.DIGEST_MISMATCH, path)
        target = os.path.join(workspace, path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as handle:
            handle.write(data)
        placed += len(data)
    if attempt.inputs:
        event(_log, logging.INFO, "inputs placed", execution=eid,
              files=len(attempt.inputs), bytes=placed)
    return None


def _collect_outputs(attempt, store, workspace, outputs, missing):
    """Store each declared output; only regular files, never through a link
    (D11.6): the executor is root and must not leak the worker's files."""
    root = os.path.realpath(workspace) + os.sep
    for path in attempt.outputs:
        full = os.path.join(workspace, path)
        real = os.path.realpath(full)
        if (os.path.islink(full) or not real.startswith(root)
                or not os.path.isfile(real)):
            missing.append(path)
            continue
        with open(real, "rb") as handle:
            data = handle.read()
        try:
            outputs[path] = store.put(data)
        except Exception as error:  # the store or the way to it failed
            return _transfer_failed(attempt.execution_id,
                                    Evidence.OUTPUT_UPLOAD_FAILED,
                                    f"{path}: {error}")
    if attempt.outputs:
        event(_log, logging.INFO, "outputs stored",
              execution=attempt.execution_id, files=len(outputs),
              missing=len(missing))
    return None


class _Emitter:
    """Ships an attempt's output to the log store while the command runs.

    The command writes to `sink`; a second handle follows the same file and
    sends whole fragments from it (D15.3), so the log grows with the task and
    survives the worker (F5.4). Best effort by design: a fragment the store
    never takes is counted, never raised, because a log cannot fail a task.
    Fragments are bytes: a reader joins them before decoding, as a cut can
    fall inside a character. The log stops once it reaches its ceiling
    (D15.13), and is ended as usual, so a follower sees it finish.
    """

    def __init__(self, store, execution_id):
        self.sink, self._follower = _capture()
        self._store = store
        self._execution_id = execution_id
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="dpes-log")
        self._pending = b""  # read and not sent yet
        self._seq = self._sent = self._lost = 0
        self._bytes = 0  # of output that reached the store
        self._limit = MAX_CAPTURE_BYTES  # of output shipped for this attempt
        self._last_send = time.monotonic()
        self._down = False  # the store is refusing: stop trying
        self._capped = False  # the log reached its ceiling (D15.13)
        self._fault = None

    def start(self):
        self._thread.start()
        return self

    @property
    def truncated(self):
        """Whether the log stopped at its ceiling (D15.13)."""
        return self._capped

    def stop(self):
        """No more output: send the rest, end the log, close both files.

        Bounded: the coordinator allows RUN_MARGIN_S (5 s) past the command's
        own deadline, so a store that does not answer must not hold the attempt
        longer than JOIN_S. The thread is a daemon and finishes on its own."""
        self._stop.set()
        self._thread.join(JOIN_S)

    def _run(self):
        try:
            self._follow()
        except Exception as error:  # the followed file failed under us
            self._fault = f"{type(error).__name__}: {error}"
        finally:
            try:
                self._finish()
            except Exception as error:  # a log never kills the attempt
                self._fault = self._fault or f"{type(error).__name__}: {error}"
            self.sink.close()
            self._follower.close()
            self._report()

    def _follow(self):
        """Read what the command writes, in fragments, until it is over."""
        while True:
            if self._down or self._capped:
                # Nowhere to send it, or nowhere left: the file may grow in
                # peace until the command is over (the sandbox kills a flood).
                if self._stop.wait(FLUSH_S):
                    return
                continue
            data = self._follower.read(FRAGMENT_BYTES)
            if data:
                self._pending += data
            self._send_ready(final=False)  # by size or by line
            quiet = time.monotonic() - self._last_send
            if self._pending and quiet >= FLUSH_S:
                self._send_ready(final=True)  # by time
            if self._stop.is_set() and not data:
                return  # drained: the command will not write again
            if not data:
                time.sleep(FOLLOW_S)

    def _send_ready(self, final):
        while self._pending and not self._down and not self._capped:
            cut = self._cut(final)
            if cut is None:
                return
            self._ship(self._pending[:cut])
            self._pending = self._pending[cut:]

    def _cut(self, final):
        """How many bytes the next fragment holds, or None if too few yet."""
        size = len(self._pending)
        if size >= FRAGMENT_BYTES:
            limit = FRAGMENT_BYTES
        elif self._pending.count(b"\n") >= FRAGMENT_LINES:
            limit = self._after_lines()
        elif final:
            limit = size
        else:
            return None
        # End on a line when one lies inside the limit, so a reader gets whole
        # lines; otherwise the limit itself.
        newline = self._pending.rfind(b"\n", 0, limit) + 1
        return newline or limit

    def _after_lines(self):
        """Where the FRAGMENT_LINES-th line ends."""
        at = -1
        for _ in range(FRAGMENT_LINES):
            at = self._pending.find(b"\n", at + 1)
        return at + 1

    def _ship(self, piece):
        """Send one fragment, keeping the attempt under its capture ceiling.

        The last fragment is cut down to what still fits, so the log holds
        exactly the ceiling and not a byte more (D15.13)."""
        room = self._limit - self._bytes
        if len(piece) > room:
            if room:
                self._send(piece[:room])
            self._truncate("worker", self._limit)
            return
        self._send(piece)

    def _send(self, piece):
        """Send one fragment under its own seq and retry it as it is: the store
        drops a repeat, so a lost reply costs nothing. Once the retries are
        spent the store is taken for dead and the rest of the log is lost."""
        self._seq += 1
        self._last_send = time.monotonic()
        for wait in (0, *_SEND_WAITS):
            time.sleep(wait)
            try:
                # False means an earlier try of this same seq landed.
                self._store.append_fragment(
                    self._execution_id, self._seq, piece
                )
                self._sent += 1
                self._bytes += len(piece)
                return
            except LogTruncated as full:  # the attempt in the store is full
                self._truncate("control", full.limit)
                return
            except Exception:  # the store or the way to it failed
                continue
        self._lost += 1
        self._down = True  # retrying every fragment of a dead store is waste

    def _truncate(self, side, limit):
        """Stop the log here and say so once: the rest of the output is lost
        on purpose, so it cannot pass for the end of what the command wrote."""
        if self._capped:
            return
        self._capped = True
        self._pending = b""  # it would never be sent
        event(_log, logging.WARNING, "log truncated",
              execution=self._execution_id, side=side, bytes=self._bytes,
              limit=limit)

    def _finish(self):
        self._send_ready(final=True)
        if self._down:
            return
        for wait in (0, *_SEND_WAITS):
            time.sleep(wait)
            try:
                self._store.close_fragments(self._execution_id)
                return
            except Exception:  # the store or the way to it failed
                continue
        self._fault = self._fault or "the log was not closed"

    def _report(self):
        if self._lost or self._fault:
            event(_log, logging.WARNING, "log fragments lost",
                  execution=self._execution_id, sent=self._sent,
                  lost=self._lost,
                  **({"error": self._fault} if self._fault else {}))
        else:
            event(_log, logging.INFO, "log closed",
                  execution=self._execution_id, fragments=self._sent,
                  bytes=self._bytes)


def _capture():
    """A write side for the command's output and a read side that follows it.

    The file is unlinked at once, so a crash leaves nothing behind, and both
    sides keep their own offset into the same inode: the command writes through
    the first, the emitter reads through the second. The first is opened for
    reading too, because `sandbox.run` takes the tail of the output from it."""
    fd, path = tempfile.mkstemp(prefix="dpes-output-")
    try:
        sink = os.fdopen(fd, "r+b", buffering=0)
        try:
            followed = open(path, "rb", buffering=0)
        except OSError:
            sink.close()
            raise
    finally:
        os.unlink(path)  # the handles keep the inode alive
    return sink, followed


def _transfer_failed(eid, evidence, detail):
    event(_log, logging.WARNING, "transfer failed", execution=eid,
          evidence=evidence.value, detail=detail)
    return ExecutorFailure(eid, evidence, detail)


def _injected_fault(attempt):
    """Test hook (plan/09, D09.7): the fault this attempt must suffer, if any.

    Only when the worker allows it. The task asks with DPES_FAULT=<kind>:<n>,
    which fails its first n attempts; anything malformed is ignored.
    """
    if os.environ.get("DPES_FAULT_INJECTION") != "1":
        return None
    kind, _, count = attempt.env.get("DPES_FAULT", "").partition(":")
    number = int(attempt.execution_id.rsplit("/", 1)[1])
    if kind not in _FAULTS or not count.isdigit():
        return None
    return kind if number <= int(count) else None


def _hang():
    time.sleep(_HANG_S)
