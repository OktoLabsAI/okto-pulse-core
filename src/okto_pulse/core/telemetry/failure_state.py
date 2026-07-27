"""Base failure-state schema for the telemetry publish lifecycle (spec R1).

R1 owns the base failure-state schema; R5A extends it with instrumentation
metadata and R5C consumes the composed DTO (spec decision "R1 é dono do schema
base de failure-state local"). Keeping a single producer here avoids two
divergent writers in ``state.json``.

Security invariant (BR ``br_89e39ee6`` / FR ``fr_8ead6f5e``): the failure-state
and every public/status projection NEVER carry ``install_token``,
``token_hash``, ``signature`` or derived secrets. The public projection is
built from an *allowlist* of the schema fields, so a future field added to the
stored block (or a secret accidentally written next to it) cannot leak.

Scope boundary: this module owns the schema, its (de)serialisation, legacy
migration and the redaction/projection primitives. Computing ``next_retry_at``
via backoff is R1-B; classifying reason codes (UNKNOWN_INSTALL / INVALID_
SIGNATURE / DUPLICATE) is R1-C; event watermark/delta is R3. Callers in those
cards mutate the state through :func:`merge`.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Any

# --- Operational status vocabulary ----------------------------------------
# ``status`` is the coarse operational state; ``reason_code`` carries the
# specific backend/transport code (e.g. UNKNOWN_INSTALL, USAGE_500).
STATUS_UNKNOWN = "unknown"  # no recorded publish outcome yet (e.g. legacy migration)
STATUS_OK = "ok"  # last publish succeeded
STATUS_DEGRADED = "degraded"  # transient failure, a retry is scheduled (next_retry_at)
STATUS_BLOCKED = "blocked"  # publishing blocked by consent_state
STATUS_FATAL = "fatal"  # actionable integrity/auth failure (e.g. INVALID_SIGNATURE)
VALID_STATUSES = frozenset(
    {STATUS_UNKNOWN, STATUS_OK, STATUS_DEGRADED, STATUS_BLOCKED, STATUS_FATAL}
)

# --- Consent vocabulary (aligned with telemetry modes) ---------------------
CONSENT_GRANTED = "granted"  # anonymous_beacon opt-in active
CONSENT_BLOCKED = "blocked"  # disabled / local_only / consent not for beacon
CONSENT_UNKNOWN = "unknown"  # no mode recorded yet
VALID_CONSENT = frozenset({CONSENT_GRANTED, CONSENT_BLOCKED, CONSENT_UNKNOWN})

# Key under which the failure-state block lives inside ``state.json``.
FAILURE_STATE_KEY = "failure_state"

# Allowlist of fields exposed publicly — EXACTLY the schema fields, no secrets.
# Public projections are built strictly from this tuple.
PUBLIC_FAILURE_STATE_FIELDS: tuple[str, ...] = (
    "status",
    "reason_code",
    "http_status",
    "last_success_at",
    "last_failure_at",
    "next_retry_at",
    "retry_count",
    "recovered_at",
    "publish_enabled",
    "consent_state",
    # R5A instrumentation extension (br_14606103: extends the base schema, does
    # not redefine it). A NON-reversible redacted install token for correlating a
    # failing publish to an install without storing the raw id (never a secret).
    "install_id_redacted",
)

# Keys that must NEVER appear in any public projection or structured log of
# telemetry state. ``is_secret_key`` also covers derived patterns.
SECRET_STATE_KEYS = frozenset({"install_token", "token_hash", "signature", "token"})


def is_secret_key(key: str) -> bool:
    """Return ``True`` if a state key carries a credential/secret to redact.

    Matches the explicit secret keys plus conservative patterns (``*_token``,
    ``*_hash``, ``signature``, ``*secret*``). It deliberately does NOT match
    ``install_token_expires_at`` — a non-secret expiry timestamp that is safe
    (and useful) to surface in status.
    """
    k = str(key).lower()
    if k in SECRET_STATE_KEYS:
        return True
    if k.endswith("_token") or k.endswith("_hash") or k == "signature":
        return True
    if "secret" in k:
        return True
    return False


def redact_install_id(install_id: str | None) -> str | None:
    """Non-reversible, stable short token correlating failure-state to an install
    WITHOUT storing the raw id (R5A instrumentation). ``None`` when no install id is
    known. A sha256 prefix — not the install_id, not a secret, not reversible."""
    if not install_id:
        return None
    return "iid_" + hashlib.sha256(install_id.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class FailureState:
    """Base failure-state of the telemetry publish lifecycle.

    All fields are non-secret by construction. Frozen so callers must go
    through :func:`merge` (or :func:`dataclasses.replace`) to evolve it.
    """

    status: str = STATUS_UNKNOWN
    reason_code: str | None = None
    http_status: int | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    next_retry_at: str | None = None
    retry_count: int = 0
    recovered_at: str | None = None
    publish_enabled: bool = False
    consent_state: str = CONSENT_UNKNOWN
    # R5A instrumentation extension: a non-reversible redacted install token (never
    # the raw install_id / a secret). None until a publish outcome records it.
    install_id_redacted: str | None = None

    def to_public_dict(self) -> dict[str, Any]:
        """Allowlisted, secret-free dict of the schema fields.

        Built from :data:`PUBLIC_FAILURE_STATE_FIELDS` (not ``asdict``) so a
        future non-public field on the dataclass would still never leak.
        """
        return {name: getattr(self, name) for name in PUBLIC_FAILURE_STATE_FIELDS}


def consent_state_from_mode(mode: str | None) -> str:
    """Derive ``consent_state`` from a persisted telemetry ``mode``."""
    if mode == "anonymous_beacon":
        return CONSENT_GRANTED
    if mode in (None, ""):
        return CONSENT_UNKNOWN
    return CONSENT_BLOCKED


def _coerce_status(value: Any) -> str:
    return value if value in VALID_STATUSES else STATUS_UNKNOWN


def _coerce_consent(value: Any, *, default: str) -> str:
    return value if value in VALID_CONSENT else default


def _coerce_opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _coerce_opt_int(value: Any) -> int | None:
    if isinstance(value, bool):  # bool is an int subclass — reject it explicitly
        return None
    return value if isinstance(value, int) else None


def _coerce_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def read_failure_state(state: dict[str, Any]) -> FailureState:
    """Read the base failure-state from a telemetry ``state`` dict.

    When the state has no ``failure_state`` block (legacy ``state.json``),
    build safe, actionable defaults derived from the recorded ``mode`` and any
    legacy success timestamp (``last_send_at``). Unknown/extra keys in a stored
    block are ignored so future extensions (R5A) never break the base reader,
    and a secret accidentally written into the block is dropped on read.
    """
    mode = state.get("mode")
    default_consent = consent_state_from_mode(mode if isinstance(mode, str) else None)

    raw = state.get(FAILURE_STATE_KEY)
    if not isinstance(raw, dict):
        # Legacy migration: safe defaults; seed last_success_at from the legacy
        # ``last_send_at`` field when present so status stays actionable.
        legacy_success = _coerce_opt_str(state.get("last_send_at"))
        return FailureState(
            status=STATUS_UNKNOWN,
            last_success_at=legacy_success,
            publish_enabled=(default_consent == CONSENT_GRANTED),
            consent_state=default_consent,
        )

    consent = _coerce_consent(raw.get("consent_state"), default=default_consent)
    publish_enabled = raw.get("publish_enabled")
    if not isinstance(publish_enabled, bool):
        publish_enabled = consent == CONSENT_GRANTED
    return FailureState(
        status=_coerce_status(raw.get("status")),
        reason_code=_coerce_opt_str(raw.get("reason_code")),
        http_status=_coerce_opt_int(raw.get("http_status")),
        last_success_at=_coerce_opt_str(raw.get("last_success_at")),
        last_failure_at=_coerce_opt_str(raw.get("last_failure_at")),
        next_retry_at=_coerce_opt_str(raw.get("next_retry_at")),
        retry_count=_coerce_int(raw.get("retry_count")),
        recovered_at=_coerce_opt_str(raw.get("recovered_at")),
        publish_enabled=publish_enabled,
        consent_state=consent,
        install_id_redacted=_coerce_opt_str(raw.get("install_id_redacted")),
    )


def merge(failure_state: FailureState, **changes: Any) -> FailureState:
    """Return a new :class:`FailureState` with ``changes`` applied and validated.

    The single validated mutation path for R1-B/R1-C: rejects unknown
    ``status``/``consent_state`` values so the schema invariants hold no matter
    which card writes the state.
    """
    if "status" in changes and changes["status"] not in VALID_STATUSES:
        raise ValueError(f"invalid failure-state status: {changes['status']!r}")
    if "consent_state" in changes and changes["consent_state"] not in VALID_CONSENT:
        raise ValueError(f"invalid consent_state: {changes['consent_state']!r}")
    return replace(failure_state, **changes)


def write_failure_state(state: dict[str, Any], failure_state: FailureState) -> dict[str, Any]:
    """Return a new state dict with the ``failure_state`` block (re)written.

    Pure (no disk I/O). The block is built solely from the allowlisted schema,
    so it can never carry a secret. Other keys in ``state`` are preserved.
    """
    new_state = dict(state)
    new_state[FAILURE_STATE_KEY] = failure_state.to_public_dict()
    return new_state


def redact_secret_keys(state: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of a telemetry ``state`` dict with secret keys removed.

    Defense-in-depth for code paths that must emit a broad state view (e.g.
    structured logs). The status surface should instead use
    :func:`public_status_projection`, which is allowlist-based.
    """
    return {key: value for key, value in state.items() if not is_secret_key(key)}


def public_status_projection(state: dict[str, Any]) -> dict[str, Any]:
    """Allowlisted, secret-free public view of the failure-state from a full state.

    Structurally cannot leak secrets: only the schema fields are emitted. Used
    by R1-D when exposing publish status to UI/MCP/CLI.
    """
    return read_failure_state(state).to_public_dict()


# R-P2-08: the local ``state.json`` persistence helpers (``load_failure_state`` /
# ``persist_failure_state``) were extracted to the Community edition
# (``okto_pulse.community.adapters.telemetry_state``). The core keeps ONLY the
# PURE projections above (``read_failure_state`` / ``write_failure_state`` /
# ``public_status_projection``); it no longer persists telemetry state locally.
