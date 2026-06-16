"""Publish-health DTO for the agent/UI health surface (spec R5C, card R5C-A).

This is the R5C CONSUMER of the R5A producer boundary
(`docs/architecture/telemetry_r5a_r5c_boundary.md`). The DTO is built strictly as
an allowlist projection of the R5A PUBLIC failure-state
(`failure_state.public_status_projection`) — R5C does NOT recompute trust /
failure-state, does NOT redefine the sensitive fields, and does NOT re-derive
redaction. The only R5C logic here is CLASSIFYING the health `status` vocabulary,
deriving `freshness`, and labelling the `source` — over the already-safe,
already-redacted R5A projection.

ts_4c7fd83a: given a local failure-state with last success/failure + a scheduled
retry, the response carries status, reason_code, last_success_at, last_failure_at,
next_retry_at, retry_count, freshness and the REDACTED install id.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from okto_pulse.core.telemetry import failure_state as fs

# --- R5C health status vocabulary (classification over the R5A status) ---------
HEALTH_DISABLED = "disabled"
HEALTHY = "healthy"
DEGRADED = "degraded"
RECOVERING = "recovering"
FAILING = "failing"
STALE = "stale"
UNAVAILABLE = "unavailable"
HEALTH_STATUSES = frozenset(
    {HEALTH_DISABLED, HEALTHY, DEGRADED, RECOVERING, FAILING, STALE, UNAVAILABLE}
)

# --- health source vocabulary --------------------------------------------------
SOURCE_LOCAL = "local"
SOURCE_INSTALL_LIFECYCLE = "install_lifecycle"
SOURCE_AWS_INGEST = "aws_ingest"
SOURCE_REPORT_ATHENA = "report_athena"
SOURCE_COMBINED = "combined"
HEALTH_SOURCES = frozenset(
    {SOURCE_LOCAL, SOURCE_INSTALL_LIFECYCLE, SOURCE_AWS_INGEST, SOURCE_REPORT_ATHENA, SOURCE_COMBINED}
)

# Structured error when NO health source can be read at all.
HEALTH_SOURCE_UNAVAILABLE = "HEALTH_SOURCE_UNAVAILABLE"

# A successful publish older than this is presented as ``stale`` (presentation
# threshold for the health surface, NOT a backend SLA). 6h >> the hourly cadence.
DEFAULT_STALE_THRESHOLD_SECONDS = 6 * 3600

# The DTO fields R5C exposes. The underlying failure-state fields are copied
# VERBATIM from the R5A public projection; the rest is R5C classification.
PUBLISH_HEALTH_FIELDS: tuple[str, ...] = (
    "status",
    "source",
    "reason_code",
    "http_status",
    "last_success_at",
    "last_failure_at",
    "next_retry_at",
    "retry_count",
    "freshness",
    "install_id_redacted",
    "message",
    "redaction_applied",
)


@dataclass(frozen=True)
class PublishHealth:
    status: str
    source: str
    reason_code: str | None
    http_status: int | None
    last_success_at: str | None
    last_failure_at: str | None
    next_retry_at: str | None
    retry_count: int
    freshness: dict[str, Any]
    install_id_redacted: str | None
    message: str
    redaction_applied: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in PUBLISH_HEALTH_FIELDS}


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _freshness(last_success_at: Any, *, now: datetime, threshold_seconds: int) -> dict[str, Any]:
    parsed = _parse_iso(last_success_at)
    age = None if parsed is None else max(0, int((now - parsed).total_seconds()))
    return {
        "last_success_at": last_success_at if isinstance(last_success_at, str) else None,
        "age_seconds": age,
        "is_stale": age is not None and age > threshold_seconds,
        "stale_threshold_seconds": threshold_seconds,
    }


def _classify(projection: dict[str, Any], *, is_stale: bool) -> str:
    """Map the R5A failure-state to the R5C health vocabulary (classification only —
    the underlying fields are not recomputed)."""
    status = projection.get("status")
    if not projection.get("publish_enabled") or status == fs.STATUS_BLOCKED or projection.get(
        "consent_state"
    ) == fs.CONSENT_BLOCKED:
        return HEALTH_DISABLED
    if status == fs.STATUS_FATAL:
        return FAILING
    if status == fs.STATUS_DEGRADED:
        return DEGRADED
    if status == fs.STATUS_OK:
        if is_stale:
            return STALE
        # recovered_at stamped on THIS success (cleared a prior failure).
        if projection.get("recovered_at") and projection.get("recovered_at") == projection.get("last_success_at"):
            return RECOVERING
        return HEALTHY
    # STATUS_UNKNOWN / no recorded outcome with publishing enabled -> no health yet.
    return UNAVAILABLE


def _message(status: str, projection: dict[str, Any]) -> str:
    reason = projection.get("reason_code")
    if status == HEALTH_DISABLED:
        return "Publishing is disabled (telemetry off or consent not granted)."
    if status == FAILING:
        return f"Publishing is failing ({reason}); manual action may be required."
    if status == DEGRADED:
        return f"Publishing is degraded ({reason}); a retry is scheduled."
    if status == STALE:
        return "Last successful publish is stale; no recent success within the freshness window."
    if status == RECOVERING:
        return "Publishing recovered after a prior failure."
    if status == HEALTHY:
        return "Publishing is healthy; the last publish succeeded."
    return "No publish outcome has been recorded yet."


def resolve_publish_health(
    public_projection: dict[str, Any],
    *,
    now: datetime,
    source: str = SOURCE_LOCAL,
    stale_threshold_seconds: int = DEFAULT_STALE_THRESHOLD_SECONDS,
) -> PublishHealth:
    """Build the publish-health DTO from the R5A PUBLIC failure-state projection.

    ``public_projection`` MUST be the allowlisted, already-redacted dict from
    :func:`failure_state.public_status_projection` (or ``FailureState.to_public_dict``)
    — never raw state. The underlying fields are copied verbatim; only the health
    ``status``, ``freshness``, ``source`` and ``message`` are R5C classification."""
    freshness = _freshness(
        public_projection.get("last_success_at"), now=now, threshold_seconds=stale_threshold_seconds
    )
    status = _classify(public_projection, is_stale=freshness["is_stale"])
    return PublishHealth(
        status=status,
        source=source,
        reason_code=public_projection.get("reason_code"),
        http_status=public_projection.get("http_status"),
        last_success_at=public_projection.get("last_success_at"),
        last_failure_at=public_projection.get("last_failure_at"),
        next_retry_at=public_projection.get("next_retry_at"),
        retry_count=int(public_projection.get("retry_count") or 0),
        freshness=freshness,
        install_id_redacted=public_projection.get("install_id_redacted"),
        message=_message(status, public_projection),
        redaction_applied=True,
    )
