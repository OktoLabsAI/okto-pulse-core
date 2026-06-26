"""RuntimeSettingsPort runtime port (spec #15, tr_5a02106c / api_e9a378a5).

Separates settings PERSISTENCE from runtime EFFECTS — replacing the monolithic
``settings_service._maybe_reschedule_tick``. The KG tick failure signal
``kg.tick.reschedule_failed`` is preserved/enriched (never recreated) with
``job_id`` (canonical ``kg_daily_tick``), ``error_class``, a sanitized message,
``actor_id`` and ``source``, and NEVER a secret/token/raw credential.

Pure: Protocols + frozen DTOs + a stdlib sanitiser. No concrete persistence or
scheduler import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

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

EffectStatus = Literal["applied", "skipped", "failed"]
EffectAuditStatus = Literal["rescheduled", "skipped", "failed"]

_SECRET_PATTERN = re.compile(
    r"(?i)(password|passwd|secret|token|api[_-]?key|authorization|bearer)\s*[=:]\s*\S+"
)
_URL_CREDENTIALS = re.compile(r"://[^/\s:@]+:[^/\s@]+@")


@runtime_checkable
class ActorContextLike(Protocol):
    """Structural actor: identity + inbound surface as plain data."""

    actor_id: str
    source: str


@dataclass(frozen=True)
class RuntimeSettingsSnapshot:
    """An immutable settings snapshot for a scope."""

    scope: str
    version: int
    values: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeEffectResult:
    """Result of applying one runtime effect of a settings change."""

    effect: str
    status: EffectStatus
    job_id: str | None = None
    signal: str | None = None
    audit_status: EffectAuditStatus | None = None
    error_class: str | None = None
    sanitized_message: str | None = None


@runtime_checkable
class RuntimeSettingsPort(Protocol):
    """Loads/persists runtime settings and applies their runtime effects."""

    async def load(self, scope: str) -> RuntimeSettingsSnapshot:
        ...

    async def persist(
        self, changes: Mapping[str, Any], *, actor: ActorContextLike
    ) -> RuntimeSettingsSnapshot:
        """Persist settings only — no implicit scheduler/runtime side effect."""
        ...

    async def apply_runtime_effects(
        self, before: RuntimeSettingsSnapshot, after: RuntimeSettingsSnapshot
    ) -> list[RuntimeEffectResult]:
        """Apply runtime effects (e.g. KG tick reschedule) for a settings delta."""
        ...


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
