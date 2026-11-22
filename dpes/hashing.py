"""Content digests (plan/11, D11.4) and action keys (plan/16, D16.1). Pure."""

import hashlib
import json
import re
from typing import Mapping, Sequence

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")

# The shape of the canonical payload below. Bumping it makes every old key stop
# matching, which is exactly the point: a changed definition must never reuse a
# result computed under the previous one (plan/02, D02.12).
ACTION_KEY_VERSION = 1


def digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def is_digest(text) -> bool:
    """True only for the canonical `sha256:<64 lower-case hex>` form."""
    return isinstance(text, str) and bool(_DIGEST.fullmatch(text))


def action_key(
    command: Sequence[str],
    env: Mapping[str, str],
    inputs: Mapping[str, str],
    outputs: Sequence[str],
    platform_id: str,
    key_format_version: int = ACTION_KEY_VERSION,
) -> str:
    """SHA-256 hex of the canonical description of one unit of work.

    Two tasks get the same key exactly when they would do the same work, so it
    is the cache lookup key (plan/16, D16.1). The description holds only what
    decides the result (plan/02, D02.12): the command with its argument order,
    the declared environment, the inputs as declared path -> digest (dependency
    outputs included, which is what chains keys), the declared output paths, and
    the platform the work runs on. Task and job names, resources, the time limit
    and the retry policy stay out: none of them changes what the command
    produces, and including the platform stops a binary built elsewhere from
    being served.

    The JSON is canonical (keys sorted, no whitespace, UTF-8) and the output
    paths are sorted too, so the order in which equivalent work is written down
    cannot yield two different keys. `key_format_version` lets the payload shape
    change later without colliding with keys of the old one.

    Returns bare lower-case hex, not the `sha256:` form of `digest`: this names
    a piece of work, not a blob.
    """
    if not platform_id:
        raise ValueError("action_key: platform_id is required")
    payload = {
        "cmd": list(command),
        "env": dict(env),
        "inputs": dict(inputs),
        "outputs": sorted(outputs),
        "platform": platform_id,
        "v": key_format_version,
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
