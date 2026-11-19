"""The log store: each attempt's output in numbered fragments, on disk.

One append-only file per execution id (plan/15, D15.2). The file is named by
the digest of the id, so no id can choose a path outside the root, and its
header repeats the id so a mismatched file is caught (D15.11). After the header
come `(seq, length, data)` records, little-endian; an index built by scanning
the file turns a byte offset of the concatenated output into a read.

Ray-free: the actor that wraps this module lives in `coordinator.py` (D15.10),
as `BlobStore` wraps `cas.BlobDir`. No locks: `append` and `read` never await,
so the actor's event loop runs each one whole (D15.11).
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

_HEADER = struct.Struct("<4sBH")  # magic, version, execution id length
_ENTRY = struct.Struct("<QI")  # seq, data length
_MAX_ID = (1 << 16) - 1
_MAX_DATA = (1 << 32) - 1


class LogDir:
    def __init__(self, root):
        self._root = root
        os.makedirs(root, exist_ok=True)
        self._attempts = {}  # execution id -> _Attempt, loaded on first use

    def append(self, execution_id: str, seq: int, data: bytes) -> bool:
        """Store fragment `seq` of `execution_id`; False if one that old is
        already there (D15.1: only the first write of a seq counts)."""
        if seq < 1:
            raise ValueError(f"fragment seq must be positive: {seq}")
        if not data:
            raise ValueError("an empty fragment cannot be stored")
        if len(data) > _MAX_DATA:
            raise ValueError(f"fragment over {_MAX_DATA} bytes")
        return self._attempt(execution_id).append(seq, data)

    def read(self, execution_id: str, offset: int, limit=None):
        """(seq, data) pieces covering bytes [offset, offset+limit) of the
        concatenated output, or to its end when `limit` is None. The first and
        last pieces are sliced at the requested bytes."""
        if offset < 0 or (limit is not None and limit < 0):
            raise ValueError("offset and limit cannot be negative")
        return self._attempt(execution_id).read(offset, limit)

    def _path(self, execution_id):
        name = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
        return os.path.join(self._root, name + ".log")

    def _attempt(self, execution_id):
        attempt = self._attempts.get(execution_id)
        if attempt is None:
            attempt = _Attempt(self._path(execution_id), execution_id)
            self._attempts[execution_id] = attempt
        return attempt


async def tail(logs: LogDir, execution_id: str, offset: int,
               poll_s: float = POLL_S, idle_s: float = IDLE_S):
    """Yield (seq, data) from byte `offset` as `execution_id` writes them.

    Ends once `idle_s` seconds pass with nothing new: a finished attempt says
    nothing more, and the caller that wants to keep following can ask again
    from the last byte it received (D15.4). Async because the actor serves
    other calls while this waits; each `logs.read` runs whole, without an
    await inside (D15.11).
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
        self._load()

    def append(self, seq, data):
        if seq <= self._max_seq:
            return False  # already written, or a seq that arrived too late
        record = _ENTRY.pack(seq, len(data)) + data
        # Unbuffered, so a reader sees the fragment as soon as write returns.
        # "wb" when no header was found: a new file, or one a crash left
        # unfinished.
        with open(self._path, "ab" if self._end else "wb", buffering=0) as f:
            if not self._end:
                header = _header(self._execution_id)
                _write(f, header)  # a failure leaves _end at 0: "wb" next time
                self._end = len(header)
            data_at = self._end + _ENTRY.size
            try:
                _write(f, record)  # it joins the index once it is on disk
            except OSError:
                f.truncate(self._end)  # never index bytes off the file
                raise
        self._entries.append((seq, data_at, len(data)))
        self._end += _ENTRY.size + len(data)
        self._size += len(data)
        self._max_seq = seq
        return True

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
