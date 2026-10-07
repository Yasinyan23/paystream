import hashlib
import json
from collections.abc import Mapping
from typing import Any


def request_fingerprint(payload: Mapping[str, Any]) -> str:
    """sha256 of the canonical JSON form: same body in any key order → same fingerprint."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()
