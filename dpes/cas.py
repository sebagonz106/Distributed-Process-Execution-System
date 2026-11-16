"""Content-addressed blobs on disk (plan/11, D11.5).

Each blob is a file named by its digest's hex part (":" is not allowed in file
names on every system). Writes go to a temporary file that is then renamed,
so a file named by a digest never holds partial content.
"""

import os
import tempfile

from dpes.hashing import digest, is_digest


class BlobDir:
    def __init__(self, root):
        self._root = root
        os.makedirs(root, exist_ok=True)

    def put(self, name: str, data: bytes) -> str:
        """Store `data` under `name`, which must be its digest."""
        path = self._path(name)
        if digest(data) != name:
            raise ValueError(f"content does not match {name}")
        if not os.path.exists(path):  # same name, same content: nothing to do
            fd, temporary = tempfile.mkstemp(dir=self._root, prefix=".part-")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                os.replace(temporary, path)
            except BaseException:
                os.unlink(temporary)
                raise
        return name

    def get(self, name: str):
        """The blob's content, or None if it is not stored."""
        try:
            with open(self._path(name), "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def missing(self, names) -> list:
        """The names in `names` that are not stored, in order."""
        paths = [(name, self._path(name)) for name in names]
        return [name for name, path in paths if not os.path.exists(path)]

    def _path(self, name):
        # Checking the form also keeps a name from leaving the store's folder.
        if not is_digest(name):
            raise ValueError(f"not a digest: {name!r}")
        return os.path.join(self._root, name.removeprefix("sha256:"))
