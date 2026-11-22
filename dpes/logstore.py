"""The log store: each attempt's output in numbered fragments, on disk.

One append-only file per execution id (plan/15, D15.2). The file is named by
the digest of the id, so no id can choose a path outside the root, and its
header repeats the id so a mismatched file is caught (D15.11). After the header
come `(seq, length, data)` records, little-endian; an index built by scanning
the file turns a byte offset of the concatenated output into a read. The last
record of a finished attempt is `(0, 0)`: seq 0 cannot be a fragment, so it can
only mean `close` was called and no more output will come (D15.14).

Ray-free: the actor that wraps this module lives in `coordinator.py` (D15.10),
as `BlobStore` wraps `cas.BlobDir`. No locks: `append` and `read` never await,
so the actor's event loop runs each one whole (D15.11). An attempt has a ceiling
(`MAX_ATTEMPT_BYTES`) and a finished job's superseded attempts are dropped, so
the volume cannot grow without bound (D15.13).
"""

import asyncio
import hashlib
import os
import struct
import time

MAGIC = b"DPES"
VERSION = 1
POLL_S = 0.2  # how often a tail looks for new fragments
IDLE_S = 300.0  # a tail that sees nothing new for this long ends (D15.4)
# Bytes one attempt may store: the volume is shared with the blobs, the results
# and the coordinator's journal, so a runaway task cannot be allowed to fill it
# (D15.13).
MAX_ATTEMPT_BYTES = int(os.environ.get("DPES_LOG_MAX_MIB", "100")) << 20

_HEADER = struct.Struct("<4sBH")  # magic, version, execution id length
_ENTRY = struct.Struct("<QI")  # seq, data length
_END = _ENTRY.pack(0, 0)  # seq 0 cannot be a fragment: it ends the log
_MAX_ID = (1 << 16) - 1
_MAX_DATA = (1 << 32) - 1


class LogTruncated(Exception):
    """The attempt has reached its ceiling: this fragment does not fit (D15.13).

    Separate from `ValueError` because the executor must tell it apart from a
    store that is refusing calls: the first means «the log stops here», the
    second «the store is gone»."""

    def __init__(self, execution_id, size, limit):
        super().__init__(
            f"{execution_id} already holds {size} bytes (limit {limit})"
        )
        self.execution_id = execution_id
        self.size = size
        self.limit = limit

    def __reduce__(self):
        """Rebuild it as it was: Ray ships a worker's exception by pickling it,
        and the default way calls the class with the message alone, which this
        one cannot take (D15.13). Unpicklable, it arrives as a generic error
        and the executor cannot tell «full» from «store dead»."""
        return (type(self), (self.execution_id, self.size, self.limit))


class LogDir:
    def __init__(self, root, max_bytes=None):
        self._root = root
        os.makedirs(root, exist_ok=True)
        self._max_bytes = MAX_ATTEMPT_BYTES if max_bytes is None else max_bytes
        self._attempts = {}  # execution id -> _Attempt, loaded on first use
        self._dropped = set()  # kept so a late fragment or reader finds none

    def append(self, execution_id: str, seq: int, data: bytes) -> bool:
        """Store fragment `seq` of `execution_id`; False if one that old is
        already there (D15.1: only the first write of a seq counts) or if the
        attempt was dropped. Raises `LogTruncated` if it does not fit under
        the attempt's ceiling (D15.13)."""
        if seq < 1:
            raise ValueError(f"fragment seq must be positive: {seq}")
        if not data:
            raise ValueError("an empty fragment cannot be stored")
        if len(data) > _MAX_DATA:
            raise ValueError(f"fragment over {_MAX_DATA} bytes")
        attempt = self._attempt(execution_id)
        if attempt is None:  # dropped: a fragment that arrived too late
            return False
        if seq <= attempt.max_seq:
            return False  # a duplicate or a straggler; never a truncation
        if attempt.size + len(data) > self._max_bytes:
            raise LogTruncated(execution_id, attempt.size, self._max_bytes)
        return attempt.append(seq, data)

    def read(self, execution_id: str, offset: int, limit=None):
        """(seq, data) pieces covering bytes [offset, offset+limit) of the
        concatenated output, or to its end when `limit` is None. The first and
        last pieces are sliced at the requested bytes."""
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError("offset and limit cannot be negative")
        attempt = self._attempt(execution_id)
        return [] if attempt is None else attempt.read(offset, limit)

    def close(self, execution_id: str) -> bool:
        """Say the attempt will write no more, so a follower can stop; False
        if there was nothing to end (D15.14)."""
        attempt = self._attempt(execution_id)
        return False if attempt is None else attempt.close()

    def finished(self, execution_id: str) -> bool:
        """Whether `close` was called for this attempt, or it was dropped."""
        attempt = self._attempt(execution_id)
        return True if attempt is None else attempt.finished

    def size(self, execution_id: str) -> int:
        """Bytes of output stored for this attempt (plan/15, D15.15)."""
        attempt = self._attempt(execution_id)
        return 0 if attempt is None else attempt.size

    @property
    def limit(self):
        """Bytes one attempt may store before it is truncated (D15.13)."""
        return self._max_bytes

    def drop(self, execution_ids) -> int:
        """Forget those attempts and delete their files; returns the bytes
        freed. For the logs a finished job no longer serves (D15.13).

        Idempotent and quiet: an attempt that was never written, or already
        dropped, frees nothing. The id stays remembered so a fragment that
        arrives late cannot recreate the file, and a follower of it ends at
        once instead of waiting for `IDLE_S`."""
        freed = 0
        for execution_id in execution_ids:
            self._attempts.pop(execution_id, None)
            self._dropped.add(execution_id)
            try:
                freed += os.path.getsize(self._path(execution_id))
                os.unlink(self._path(execution_id))
            except FileNotFoundError:
                pass
        return freed

    def _path(self, execution_id):
        name = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
        return os.path.join(self._root, name + ".log")

    def _attempt(self, execution_id):
        """The attempt's index, or None if it was dropped (D15.13)."""
        if execution_id in self._dropped:
            return None
        attempt = self._attempts.get(execution_id)
        if attempt is None:
            attempt = _Attempt(self._path(execution_id), execution_id)
            self._attempts[execution_id] = attempt
        return attempt


async def tail(logs: LogDir, execution_id: str, offset: int,
               poll_s: float = POLL_S, idle_s: float = IDLE_S):
    """Yield (seq, data) from byte `offset` as `execution_id` writes them.

    Ends once the attempt says it is done (D15.14), or after `idle_s` seconds
    with nothing new: a finished attempt says nothing more, and the caller that
    wants to keep following can ask again from the last byte it received
    (D15.4). Async because the actor serves other calls while this waits; each
    `logs.read` runs whole, without an await inside (D15.11).
    """
    deadline = time.monotonic() + idle_s
    while True:
        pieces = logs.read(execution_id, offset, None)
        if pieces:
            for seq, data in pieces:
                offset += len(data)
                yield seq, data
            deadline = time.monotonic() + idle_s
            continue
        if logs.finished(execution_id):
            return  # nothing more will come: no need to wait for the deadline
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(poll_s, remaining))


class _Attempt:
    """One attempt's file: its index and size, rebuilt by scanning it."""

    def __init__(self, path, execution_id):
        self._path = path
        self._execution_id = execution_id
        self._entries = []  # (seq, file offset of its data, length), in order
        self._end = 0  # file offset just past the last complete fragment
        self._size = 0  # bytes of output stored
        self._max_seq = 0
        self._eof = False  # `close` was called: no fragment may follow
        self._load()

    def append(self, seq, data):
        if self._eof:
            raise ValueError(
                f"the log of {self._execution_id} is already finished"
            )
        if seq <= self._max_seq:
            return False  # already written, or a seq that arrived too late
        at = self._put(_ENTRY.pack(seq, len(data)) + data)
        self._entries.append((seq, at + _ENTRY.size, len(data)))
        self._end += _ENTRY.size + len(data)
        self._size += len(data)
        self._max_seq = seq
        return True

    def close(self):
        """Write the end marker; False if the log was already finished."""
        if self._eof:
            return False
        self._put(_END)
        self._end += _ENTRY.size
        self._eof = True
        return True

    @property
    def finished(self):
        return self._eof

    @property
    def size(self):
        return self._size

    @property
    def max_seq(self):
        """Highest fragment stored: anything not past it is a repeat."""
        return self._max_seq

    def _put(self, record):
        """Append one record at the end of the file, holding its offset.

        The file is created, or started over, when it has no header: "wb" when
        `_end` is 0, which a crash mid-creation also leaves. Unbuffered, so a
        reader sees the record as soon as write returns, and a short write (a
        full disk) is rolled back and raised instead of indexed."""
        with open(self._path, "ab" if self._end else "wb", buffering=0) as f:
            if not self._end:
                header = _header(self._execution_id)
                _write(f, header)  # a failure leaves _end at 0: "wb" next time
                self._end = len(header)
            at = self._end
            try:
                _write(f, record)  # it counts once it is on disk
            except OSError:
                f.truncate(at)  # never index bytes off the file
                raise
        return at

    def read(self, offset, limit):
        end = self._size if limit is None else min(self._size, offset + limit)
        if end <= offset:
            return []
        pieces = []
        position = 0  # where this fragment starts in the output
        with open(self._path, "rb") as f:
            for seq, start, length in self._entries:
                stop = position + length
                if stop <= offset:
                    position = stop
                    continue
                if position >= end:
                    break
                begin, finish = max(offset, position), min(end, stop)
                f.seek(start + begin - position)  # records sit between data
                pieces.append((seq, f.read(finish - begin)))
                position = stop
        return pieces

    def _load(self):
        try:
            with open(self._path, "rb") as f:
                self._scan(f)
        except FileNotFoundError:
            pass  # an attempt that has not written yet

    def _scan(self, f):
        head = f.read(_HEADER.size)
        if len(head) < _HEADER.size:
            return  # a creation that never finished: no header, no data
        magic, version, length = _HEADER.unpack(head)
        if magic != MAGIC or version != VERSION:
            raise ValueError(f"not a log file: {self._path}")
        name = f.read(length)
        if len(name) < length:
            return  # a header cut short, as a full disk could leave it
        if name != self._execution_id.encode("utf-8"):
            raise ValueError(
                f"not the log of {self._execution_id}: {self._path}"
            )
        self._end = _HEADER.size + length
        while True:
            head = f.read(_ENTRY.size)
            if len(head) < _ENTRY.size:
                break
            seq, size = _ENTRY.unpack(head)
            if seq == 0:  # the end marker (D15.14): nothing may follow it
                if size == 0:
                    self._eof = True
                    self._end += _ENTRY.size
                break
            data = f.read(size)
            if len(data) < size:
                break  # a crash left the last fragment half written
            self._entries.append((seq, self._end + _ENTRY.size, size))
            self._size += size
            self._max_seq = max(self._max_seq, seq)
            self._end += _ENTRY.size + size
        if os.path.getsize(self._path) > self._end:
            with open(self._path, "r+b") as whole:
                whole.truncate(self._end)  # drop the half-written fragment


def _header(execution_id):
    name = execution_id.encode("utf-8")
    if len(name) > _MAX_ID:
        raise ValueError(f"execution id over {_MAX_ID} bytes")
    return _HEADER.pack(MAGIC, VERSION, len(name)) + name


def _write(f, data):
    """Write all of it or fail. A full disk can take fewer bytes than asked
    without raising, and the index must not claim data that never landed."""
    if f.write(data) != len(data):
        raise OSError(f"short write of a {len(data)}-byte record")
