"""Content digests (plan/11, D11.4). Pure."""

import hashlib
import re

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def is_digest(text) -> bool:
    """True only for the canonical `sha256:<64 lower-case hex>` form."""
    return isinstance(text, str) and bool(_DIGEST.fullmatch(text))
