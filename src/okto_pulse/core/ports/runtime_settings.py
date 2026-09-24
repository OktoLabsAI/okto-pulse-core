"""Pure compatibility contract for the sanitized KG scheduler failure signal.

F4 retires settings writers and their DTOs. The existing signal name, payload
fields and credential redaction remain stable for runtime diagnostics.
"""

from __future__ import annotations

import re
from typing import Any

from .scheduler import KG_DAILY_TICK_JOB_ID

#: Canonical failure signal name (pre-existing; preserved, not recreated).
KG_TICK_RESCHEDULE_FAILED_SIGNAL = "kg.tick.reschedule_failed"
#: Fields the signal MUST carry (api_e9a378a5 failure_signal_contract).
RESCHEDULE_FAILED_REQUIRED_FIELDS: tuple[str, ...] = (
    "job_id",
    "error_class",
    "sanitized_message",
    "actor_id",
    "source",
)
#: Fields the signal MUST NEVER carry.
RESCHEDULE_FAILED_FORBIDDEN_FIELDS: tuple[str, ...] = (
    "secret",
    "token",
    "password",
    "raw_exception_with_credentials",
)


_SECRET_PATTERN = re.compile(
    r"(?i)(password|passwd|secret|token|api[_-]?key|authorization|bearer)\s*[=:]\s*\S+"
)
_URL_CREDENTIALS = re.compile(r"://[^/\s:@]+:[^/\s@]+@")










def sanitize_message(message: str) -> str:
    """Redact credential-like substrings from a message (never leak secrets)."""
    redacted = _SECRET_PATTERN.sub(r"\1=[REDACTED]", message)
    redacted = _URL_CREDENTIALS.sub("://[REDACTED]@", redacted)
    return redacted


def build_reschedule_failed_signal(
    *,
    error: BaseException,
    actor_id: str,
    source: str,
    job_id: str = KG_DAILY_TICK_JOB_ID,
) -> dict[str, Any]:
    """Build a complete, secret-free ``kg.tick.reschedule_failed`` payload.

    Carries every required field, never a forbidden one. ``sanitized_message`` is
    the redacted exception text.
    """
    payload: dict[str, Any] = {
        "signal": KG_TICK_RESCHEDULE_FAILED_SIGNAL,
        "job_id": job_id,
        "error_class": type(error).__name__,
        "sanitized_message": sanitize_message(str(error)),
        "actor_id": actor_id,
        "source": source,
    }
    for forbidden in RESCHEDULE_FAILED_FORBIDDEN_FIELDS:
        payload.pop(forbidden, None)
    return payload
