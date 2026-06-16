"""Publish-health classifier for the agent/UI health surface (spec R5C).

R5C-A introduced the DTO as an allowlist projection of the R5A PUBLIC
failure-state (`failure_state.public_status_projection`). R5C-B EVOLVES the
classifier: it maps each `reason`/`source` to a distinct, actionable category
(UNKNOWN_INSTALL, TOKEN_EXPIRED, INVALID_SIGNATURE, transport/5xx, disabled,
stale_ingest, stale_report), assigns a severity, and composes multiple health
sources so that the absence of a source — or a mandatory source that is
unavailable/expired/stale — can NEVER be reported as ``healthy``.

Boundary (`docs/architecture/telemetry_r5a_r5c_boundary.md`): R5C does NOT
recompute trust / failure-state, does NOT redefine the sensitive fields, does
NOT re-derive redaction, and adds NO real AWS/report integration (the synthetic
source classification here is pure; real adapters are R5C-C). The only R5C logic
is CLASSIFYING status/severity/source and producing the actionable message.

SECURITY (R5C-B complement): the actionable messages are keyed SOLELY by the
bounded reason/source category — they NEVER interpolate a value read from the
state/projection (install id, token, signature, nonce, payload, raw id). So a
message is secret-free by construction even when the source state carries a
secret.

ts_4c7fd83a: a degraded local failure-state yields the actionable DTO.
ts_a81de6bc: UNKNOWN_INSTALL / TOKEN_EXPIRED / INVALID_SIGNATURE / transport-5xx /
disabled produce distinct, actionable status+severity+message.
ts_60131b20: AWS/R5B unavailable or expired yields degraded/unavailable/stale —
never healthy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
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

# Ordering used to pick the WORST contributing source (higher = more severe).
# ``disabled`` is handled by a short-circuit (publishing is deliberately off).
_STATUS_RANK: dict[str, int] = {
    HEALTHY: 0,
    RECOVERING: 1,
    STALE: 2,
    DEGRADED: 3,
    UNAVAILABLE: 4,
    FAILING: 5,
}

# --- severity (coarse UI/alert axis) -------------------------------------------
SEVERITY_NONE = "none"
SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"
SEVERITIES = frozenset({SEVERITY_NONE, SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_CRITICAL})
_SEVERITY_RANK: dict[str, int] = {
    SEVERITY_NONE: 0,
    SEVERITY_INFO: 1,
    SEVERITY_WARNING: 2,
    SEVERITY_CRITICAL: 3,
}

# --- reason / source categories ------------------------------------------------
REASON_NONE = "none"
REASON_UNKNOWN_INSTALL = "unknown_install"
REASON_TOKEN_EXPIRED = "token_expired"
REASON_INVALID_SIGNATURE = "invalid_signature"
REASON_TRANSPORT_5XX = "transport_5xx"
REASON_DISABLED = "disabled"
REASON_STALE_INGEST = "stale_ingest"
REASON_STALE_REPORT = "stale_report"
REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_SOURCE_EXPIRED = "source_expired"
# R5C-C: a promised source whose ADAPTER is absent in this build (the real AWS
# ingest / Athena report integration lives downstream). It is an observability
# GAP — never healthy, but classified as ``degraded`` (not full ``unavailable``)
# so it floors the overall at degraded when the local client is otherwise OK.
REASON_SOURCE_GAP = "source_gap"
REASON_CATEGORIES = frozenset(
    {
        REASON_NONE,
        REASON_UNKNOWN_INSTALL,
        REASON_TOKEN_EXPIRED,
        REASON_INVALID_SIGNATURE,
        REASON_TRANSPORT_5XX,
        REASON_DISABLED,
        REASON_STALE_INGEST,
        REASON_STALE_REPORT,
        REASON_SOURCE_UNAVAILABLE,
        REASON_SOURCE_EXPIRED,
        REASON_SOURCE_GAP,
    }
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

# Availability of an external source.
SRC_AVAILABLE = "available"
SRC_UNAVAILABLE = "unavailable"
SRC_EXPIRED = "expired"
SRC_STALE = "stale"
# R5C-C: the source's real adapter is absent in this build (downstream-only) —
# an explicit observability gap, never inferred healthy from a local send.
SRC_GAP = "gap"

# Structured error when NO health source can be read at all.
HEALTH_SOURCE_UNAVAILABLE = "HEALTH_SOURCE_UNAVAILABLE"

# A successful publish older than this is presented as ``stale`` (presentation
# threshold for the health surface, NOT a backend SLA). 6h >> the hourly cadence.
DEFAULT_STALE_THRESHOLD_SECONDS = 6 * 3600

# Bounded, STATIC actionable messages keyed ONLY by the reason/source category.
# They never interpolate a value from the state/projection -> secret-free by
# construction (R5C-B complement). Keep these short and operator-actionable.
_REASON_MESSAGES: dict[str, str] = {
    REASON_UNKNOWN_INSTALL: (
        "The server does not recognize this install; the next cycle re-handshakes "
        "to obtain a new install token. No action needed unless it persists."
    ),
    REASON_TOKEN_EXPIRED: (
        "The install token expired and was dropped; the next cycle re-handshakes "
        "automatically. No action needed."
    ),
    REASON_INVALID_SIGNATURE: (
        "The request signature was rejected (integrity/auth failure). Publishing is "
        "halted and will not auto-recover; investigate the signing key and clock skew."
    ),
    REASON_TRANSPORT_5XX: (
        "The ingest endpoint returned a transport/server error; a retry is scheduled "
        "with backoff. No action needed unless it persists."
    ),
    REASON_DISABLED: (
        "Publishing is disabled (telemetry off or consent not granted). Enable "
        "anonymous telemetry to resume."
    ),
    REASON_STALE_INGEST: (
        "The AWS ingest source has not advanced within the freshness window; recent "
        "usage may be missing downstream. Check the ingest pipeline."
    ),
    REASON_STALE_REPORT: (
        "The report (Athena) source is stale; the latest report may not reflect "
        "recent data. Re-run the report once ingest is fresh."
    ),
    REASON_SOURCE_UNAVAILABLE: (
        "A required health source is unavailable; status cannot be confirmed healthy "
        "until it returns."
    ),
    REASON_SOURCE_EXPIRED: (
        "A required health source's window/credentials expired; refresh it to restore "
        "reporting."
    ),
    REASON_SOURCE_GAP: (
        "This health source has no adapter in this build (the real AWS ingest / report "
        "freshness is provided by the downstream pipeline); freshness cannot be "
        "confirmed, so it is reported as an observability gap, not healthy."
    ),
}
_STATUS_MESSAGES: dict[str, str] = {
    HEALTHY: "Publishing is healthy; the last publish succeeded.",
    RECOVERING: "Publishing recovered after a prior failure; monitoring the next cycles.",
    STALE: "Last successful publish is stale; no recent success within the freshness window.",
    UNAVAILABLE: "No publish outcome has been recorded yet.",
    DEGRADED: "Publishing is degraded; a retry is scheduled.",
    FAILING: "Publishing is failing; manual action may be required.",
    HEALTH_DISABLED: "Publishing is disabled.",
}

# The DTO fields R5C exposes. The underlying failure-state fields are copied
# VERBATIM from the R5A public projection; the rest is R5C classification.
PUBLISH_HEALTH_FIELDS: tuple[str, ...] = (
    "status",
    "source",
    "severity",
    "reason_code",
    "reason_category",
    "http_status",
    "last_success_at",
    "last_failure_at",
    "next_retry_at",
    "retry_count",
    "freshness",
    "install_id_redacted",
    "message",
    "sources",
    "redaction_applied",
)


@dataclass(frozen=True)
class SourceHealth:
    """Per-source health classification (local or an external/synthetic source)."""

    name: str
    status: str
    severity: str
    reason_category: str
    message: str
    available: bool
    last_success_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "severity": self.severity,
            "reason_category": self.reason_category,
            "message": self.message,
            "available": self.available,
            "last_success_at": self.last_success_at,
        }


@dataclass(frozen=True)
class PublishHealth:
    status: str
    source: str
    severity: str
    reason_code: str | None
    reason_category: str
    http_status: int | None
    last_success_at: str | None
    last_failure_at: str | None
    next_retry_at: str | None
    retry_count: int
    freshness: dict[str, Any]
    install_id_redacted: str | None
    message: str
    sources: list[dict[str, Any]] = field(default_factory=list)
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


def classify_reason_category(health_status: str, reason_code: Any, http_status: Any) -> str:
    """Map an R5A ``reason_code`` (+ http status) to a distinct R5C reason category.

    Pure over bounded inputs; never reads a secret. ``reason_code`` is itself a
    bounded backend code (e.g. ``USAGE_503``, ``INVALID_SIGNATURE``), not a secret.
    """
    if health_status == HEALTH_DISABLED:
        return REASON_DISABLED
    code = str(reason_code or "").upper()
    if code == "UNKNOWN_INSTALL":
        return REASON_UNKNOWN_INSTALL
    if code == "TOKEN_EXPIRED":
        return REASON_TOKEN_EXPIRED
    if code == "INVALID_SIGNATURE":
        return REASON_INVALID_SIGNATURE
    if code.startswith("USAGE_") or (isinstance(http_status, int) and 500 <= http_status <= 599):
        return REASON_TRANSPORT_5XX
    return REASON_NONE


def severity_for(health_status: str) -> str:
    """Coarse severity for a health status (keeps status<->severity coherent)."""
    if health_status == FAILING:
        return SEVERITY_CRITICAL
    if health_status in (DEGRADED, STALE, UNAVAILABLE):
        return SEVERITY_WARNING
    if health_status in (RECOVERING, HEALTH_DISABLED):
        return SEVERITY_INFO
    return SEVERITY_NONE  # healthy


def actionable_message(health_status: str, reason_category: str) -> str:
    """Bounded, STATIC actionable message keyed by reason/source category.

    Never interpolates a state/projection value -> secret-free by construction.
    """
    if reason_category in _REASON_MESSAGES:
        return _REASON_MESSAGES[reason_category]
    return _STATUS_MESSAGES.get(health_status, _STATUS_MESSAGES[UNAVAILABLE])


def _classify_local_source(
    projection: dict[str, Any], *, now: datetime, stale_threshold_seconds: int
) -> tuple[SourceHealth, dict[str, Any]]:
    freshness = _freshness(
        projection.get("last_success_at"), now=now, threshold_seconds=stale_threshold_seconds
    )
    status = _classify(projection, is_stale=freshness["is_stale"])
    reason_category = classify_reason_category(
        status, projection.get("reason_code"), projection.get("http_status")
    )
    src = SourceHealth(
        name=SOURCE_LOCAL,
        status=status,
        severity=severity_for(status),
        reason_category=reason_category,
        message=actionable_message(status, reason_category),
        available=True,  # the local source was readable (we have the projection)
        last_success_at=projection.get("last_success_at"),
    )
    return src, freshness


def _external_descriptor(descriptor: Any) -> tuple[str, str | None]:
    """Normalize an external source descriptor to (availability, last_success_at)."""
    if isinstance(descriptor, dict):
        availability = str(descriptor.get("availability") or SRC_UNAVAILABLE)
        last = descriptor.get("last_success_at")
        return availability, last if isinstance(last, str) else None
    return str(descriptor or SRC_UNAVAILABLE), None


def _classify_external_source(name: str, descriptor: Any, *, now: datetime) -> SourceHealth | None:
    """Classify a synthetic external source (aws_ingest / report_athena).

    ``descriptor`` is None when the source does not contribute. Otherwise it is an
    availability string or ``{"availability": ..., "last_success_at": ...}``. No
    real adapter / network call here (R5C-C owns the real integration)."""
    if descriptor is None:
        return None
    availability, last_success = _external_descriptor(descriptor)
    if name == SOURCE_AWS_INGEST:
        stale_reason = REASON_STALE_INGEST
    elif name == SOURCE_REPORT_ATHENA:
        stale_reason = REASON_STALE_REPORT
    else:
        stale_reason = REASON_SOURCE_GAP
    if availability == SRC_AVAILABLE:
        status, reason_category, available = HEALTHY, REASON_NONE, True
    elif availability == SRC_STALE:
        status, reason_category, available = STALE, stale_reason, True
    elif availability == SRC_EXPIRED:
        status, reason_category, available = DEGRADED, REASON_SOURCE_EXPIRED, False
    elif availability == SRC_GAP:
        # adapter absent: never healthy, but a DEGRADED observability gap (not a
        # full unavailable) so a healthy local client floors the overall at degraded.
        status, reason_category, available = DEGRADED, REASON_SOURCE_GAP, False
    else:  # unavailable / unknown -> treat as unavailable, never healthy
        status, reason_category, available = UNAVAILABLE, REASON_SOURCE_UNAVAILABLE, False
    return SourceHealth(
        name=name,
        status=status,
        severity=severity_for(status),
        reason_category=reason_category,
        message=actionable_message(status, reason_category),
        available=available,
        last_success_at=last_success,
    )


def _missing_required_source(name: str) -> SourceHealth:
    """A required source that was not provided/readable -> unavailable (never healthy)."""
    return SourceHealth(
        name=name,
        status=UNAVAILABLE,
        severity=severity_for(UNAVAILABLE),
        reason_category=REASON_SOURCE_UNAVAILABLE,
        message=actionable_message(UNAVAILABLE, REASON_SOURCE_UNAVAILABLE),
        available=False,
        last_success_at=None,
    )


def _combine(sources: list[SourceHealth]) -> tuple[str, str, str, str]:
    """Combine per-source health into (status, reason_category, severity, message).

    The local source (always first) being ``disabled`` short-circuits to disabled
    — publishing is deliberately off, so external staleness is moot. Otherwise the
    WORST source by status rank wins, and the overall is healthy ONLY when every
    contributing source is healthy."""
    local = sources[0]
    if local.status == HEALTH_DISABLED:
        return (HEALTH_DISABLED, REASON_DISABLED, severity_for(HEALTH_DISABLED), local.message)
    worst = max(sources, key=lambda s: _STATUS_RANK.get(s.status, 0))
    severity = severity_for(worst.status)
    return (worst.status, worst.reason_category, severity, worst.message)


def resolve_publish_health(
    public_projection: dict[str, Any],
    *,
    now: datetime,
    stale_threshold_seconds: int = DEFAULT_STALE_THRESHOLD_SECONDS,
    install_lifecycle: Any = None,
    aws_ingest: Any = None,
    report_athena: Any = None,
    required_sources: tuple[str, ...] = (),
) -> PublishHealth:
    """Build the publish-health DTO from the R5A PUBLIC failure-state projection.

    ``public_projection`` MUST be the allowlisted, already-redacted dict from
    :func:`failure_state.public_status_projection` (or ``FailureState.to_public_dict``)
    — never raw state. The local source is always classified from it; optional
    ``install_lifecycle`` / ``aws_ingest`` / ``report_athena`` descriptors (and any
    ``required_sources`` that are missing) contribute to the COMBINED status so a
    missing/unavailable/expired/stale/gap source can never read as ``healthy``.

    R5C-C: ``aws_ingest`` / ``report_athena`` are classified ONLY from their own
    descriptor — never inferred healthy from the local send (the local failure-state
    proves the client published, NOT that AWS ingested or the report is fresh)."""
    local, freshness = _classify_local_source(
        public_projection, now=now, stale_threshold_seconds=stale_threshold_seconds
    )
    sources: list[SourceHealth] = [local]
    lifecycle = _classify_external_source(SOURCE_INSTALL_LIFECYCLE, install_lifecycle, now=now)
    if lifecycle is not None:
        sources.append(lifecycle)
    aws = _classify_external_source(SOURCE_AWS_INGEST, aws_ingest, now=now)
    if aws is not None:
        sources.append(aws)
    report = _classify_external_source(SOURCE_REPORT_ATHENA, report_athena, now=now)
    if report is not None:
        sources.append(report)
    present = {s.name for s in sources}
    for required in required_sources:
        if required not in present:
            sources.append(_missing_required_source(required))
            present.add(required)

    status, reason_category, severity, message = _combine(sources)
    source_label = SOURCE_LOCAL if len(sources) == 1 else SOURCE_COMBINED
    return PublishHealth(
        status=status,
        source=source_label,
        severity=severity,
        reason_code=public_projection.get("reason_code"),
        reason_category=reason_category,
        http_status=public_projection.get("http_status"),
        last_success_at=public_projection.get("last_success_at"),
        last_failure_at=public_projection.get("last_failure_at"),
        next_retry_at=public_projection.get("next_retry_at"),
        retry_count=int(public_projection.get("retry_count") or 0),
        freshness=freshness,
        install_id_redacted=public_projection.get("install_id_redacted"),
        message=message,
        sources=[s.to_dict() for s in sources],
        redaction_applied=True,
    )


# --- R5C-C source discovery (real local/lifecycle signals + declared gaps) -----

def derive_install_lifecycle(state: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    """Derive the install-lifecycle source descriptor from REAL local state signals.

    Uses only non-secret signals: the PRESENCE of an install token (a boolean — the
    value is never read/surfaced), the non-secret ``install_token_expires_at`` and
    ``last_handshake_at`` timestamps, and the consent mode. Distinguishes the install
    handshake/token lifecycle from the local publish outcome (the ``local`` source)."""
    mode = state.get("mode")
    if mode in (None, "", "disabled"):
        # publishing off — lifecycle is moot (the local source short-circuits to
        # disabled); report available so it does not raise a spurious alarm.
        return {"availability": SRC_AVAILABLE}
    has_token = bool(state.get("install_token"))  # PRESENCE only, never the value
    handshaked_at = state.get("last_handshake_at")
    expires_at = state.get("install_token_expires_at")
    expiry = _parse_iso(expires_at)
    if has_token and expiry is not None and expiry <= now:
        return {"availability": SRC_EXPIRED, "last_success_at": handshaked_at if isinstance(handshaked_at, str) else None}
    if has_token or isinstance(handshaked_at, str):
        return {"availability": SRC_AVAILABLE, "last_success_at": handshaked_at if isinstance(handshaked_at, str) else None}
    # beacon enabled but the install never handshaked -> lifecycle not established.
    return {"availability": SRC_UNAVAILABLE}


def discover_external_sources(settings: Any) -> tuple[Any, Any]:
    """Discover the real aws_ingest / report_athena source descriptors.

    There is NO real AWS ingest / Athena report adapter in the core client today —
    those live in the downstream pipeline (R5B / R4 in ``okto_labs_community_metrics``).
    So by default both are an explicit observability GAP (``SRC_GAP`` -> degraded,
    never healthy). A deployment that wires real adapters can supply descriptors via
    a ``metrics_health_external_sources`` mapping on settings (forward-compatible),
    but the default can NEVER mask the missing AWS/report visibility as healthy."""
    configured = getattr(settings, "metrics_health_external_sources", None)
    if isinstance(configured, dict):
        aws = configured.get(SOURCE_AWS_INGEST, {"availability": SRC_GAP})
        report = configured.get(SOURCE_REPORT_ATHENA, {"availability": SRC_GAP})
        return aws, report
    return {"availability": SRC_GAP}, {"availability": SRC_GAP}


# --- R5C-E redaction guardrail (API / MCP / UI / logs) -------------------------
# Defense-in-depth: every health payload that leaves the process (DTO, API/MCP
# response, anything logged) passes through this recursive guard, so a secret can
# never surface even if a future code path puts one into the DTO/sources/debug
# metadata. It is NOT a substitute for the allowlist construction — it backstops it.

REDACTED = "[REDACTED]"

# VALUE-level secret patterns: catch a secret VALUE even under an innocent key,
# precise enough NOT to redact descriptive words like "token"/"install" that appear
# in the bounded catalog messages (validator complement #2 vs #4).
_SECRET_VALUE_PATTERNS = (
    re.compile(r"oat_[A-Za-z0-9._-]{3,}"),  # opaque (Clerk-style) tokens
    re.compile(r"(?:sk|pk|tok|key|secret)_[A-Za-z0-9]{6,}", re.IGNORECASE),
    re.compile(r"eyJ[A-Za-z0-9._-]{10,}"),  # JWT
    re.compile(r"\b[0-9a-fA-F]{32,}\b"),  # full nonce / hash / signature hex
)


def _is_forbidden_health_key(key: Any) -> bool:
    """Keys whose value must never surface: the failure_state secret keys plus the
    RAW install_id, a full nonce, and any sensitive payload."""
    k = str(key).lower()
    if fs.is_secret_key(k):
        return True
    if k == "install_id":  # the RAW install id (NOT the redacted iid_ form)
        return True
    if "nonce" in k or "payload" in k:
        return True
    return False


def collect_health_secret_values(state: Any) -> set[str]:
    """Gather the VALUES stored under forbidden keys anywhere in a state dict, so the
    guardrail can scrub them even if they later appear under an innocent key."""
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if _is_forbidden_health_key(key) and isinstance(value, str) and len(value) >= 6:
                    found.add(value)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(state)
    return found


def _scrub_string(value: str, secret_values: tuple[str, ...]) -> str:
    out = value
    for secret in secret_values:
        if secret and secret in out:
            out = out.replace(secret, REDACTED)
    for pattern in _SECRET_VALUE_PATTERNS:
        out = pattern.sub(REDACTED, out)
    return out


def redact_health_payload(payload: Any, *, secret_values: Any = ()) -> Any:
    """Recursively redact a health payload before it reaches API/MCP/UI/logs.

    Redacts forbidden KEYS (install_token/token_hash/signature/nonce/payload and the
    raw install_id) AND scrubs secret VALUES (known state secrets + opaque-token /
    JWT / full-nonce-hash patterns) under ANY key, recursing through dicts and lists
    (sources[], freshness, future fields). The raw install_id is converted to the
    safe ``iid_`` form. Idempotent and non-destructive on a legitimate DTO: static
    messages, ``iid_`` ids, timestamps and enums are preserved (the value patterns do
    not match descriptive words like ``token``/``install``)."""
    secrets = tuple(s for s in secret_values if isinstance(s, str) and len(s) >= 6)

    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            out: dict[Any, Any] = {}
            for key, value in node.items():
                if str(key).lower() == "install_id" and isinstance(value, str):
                    out[key] = fs.redact_install_id(value) or REDACTED
                elif _is_forbidden_health_key(key):
                    out[key] = REDACTED
                else:
                    out[key] = walk(value)
            return out
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, str):
            return _scrub_string(node, secrets)
        return node

    return walk(payload)
