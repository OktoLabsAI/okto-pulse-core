"""MCPProjectionEnvelopeHelper — shared response-only projection envelope (spec
R5, card R5.1, FR0–4 / TR0–3 / api_d6609dec).

A *pure response-shaping* helper that wraps already-computed high-volume MCP
payloads with a canonical envelope so R1–R4 token optimizations stop being
one-off per-tool shapes. It NEVER touches domain objects, persistence,
authorization, or KG/source-of-truth data (BR br_bf73930f, TR tr_8b7faf58) —
it only reshapes the *presentation* of a response the caller already built.

Shared contract SC1 — envelope metadata keys:
    ``profile``, ``outcome``, ``payload_bytes``, ``truncated``,
    ``omitted_count``, ``deduped_count``, ``follow_up[{rel, target_ref}]``.

Canonical success key is ``outcome`` ∈ {``ok``, ``error``} (decision
dec_dacdb04c); ``status`` is optional compatibility metadata only.

Profiles (deterministic, BR br_94b4e897):
    summary  — IDs / counts / minimal state (default/slim profile)
    detail   — adds selected bodies
    full     — complete current modern body within hard safety caps
    legacy   — preserves prior compatibility fields where required

Success migration (TR tr_7e1f918c / tr_2509ce27, BR br_801f3e2a):
    * ``full`` and ``legacy`` preserve both ``success: true`` and
      ``success: false`` exactly where the legacy shape exposes them.
    * ``summary``/``detail`` omit positive ``success`` and rely on ``outcome``;
      failures still expose ``outcome=error`` plus ``error``/``error_code`` so
      callers never need ``result.get("success")`` truthiness.
    * ``outcome`` defaults to ``None`` = INFER: a payload that signals failure
      (``success is False`` / ``error`` / ``error_code`` / ``legacy_success=False``)
      yields ``outcome=error`` even when the caller passes no explicit outcome —
      a legacy failure can never silently become a canonical success. An explicit
      ``outcome=ok`` over a failing payload returns a structured
      ``projection_contract_violation`` instead of an ambiguous envelope. Any
      ``outcome=error`` response is guaranteed to carry ``error`` + ``error_code``
      (synthesized safely if absent).

Observability (or_13c4906b / or_6afaf91c) is *structurally prepared* here — the
envelope carries ``profile`` + counts + ``payload_bytes`` so R5.2 can wire the
telemetry emission without reshaping responses again.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any


class ProjectionProfile(str, Enum):
    """The four deterministic projection profiles."""

    SUMMARY = "summary"
    DETAIL = "detail"
    FULL = "full"
    LEGACY = "legacy"


SUPPORTED_PROFILES: tuple[str, ...] = tuple(p.value for p in ProjectionProfile)
DEFAULT_PROFILE: str = ProjectionProfile.SUMMARY.value

# Canonical success values for the ``outcome`` key.
OUTCOME_OK = "ok"
OUTCOME_ERROR = "error"

# SC1 metadata keys the envelope always attaches.
ENVELOPE_METADATA_KEYS: tuple[str, ...] = (
    "profile",
    "outcome",
    "payload_bytes",
    "truncated",
    "omitted_count",
    "deduped_count",
    "follow_up",
)

# Body selection fallback chain per profile (deterministic) when a producer
# supplies only a subset of the per-profile bodies.
_BODY_FALLBACK: dict[str, tuple[str, ...]] = {
    "summary": ("summary",),
    "detail": ("detail", "summary"),
    "full": ("full", "detail", "summary"),
    "legacy": ("legacy", "full", "detail", "summary"),
}

# Profiles that preserve a legacy ``success`` boolean.
_SUCCESS_PRESERVING = frozenset({"full", "legacy"})

# Unsupported-profile structured error code.
UNSUPPORTED_PROFILE_CODE = "unsupported_projection"

# Raised (as a structured response) when the caller's explicit outcome conflicts
# with the failure signals in the payload it asked to project.
PROJECTION_CONTRACT_VIOLATION_CODE = "projection_contract_violation"

# Synthesized failure fields when outcome=error but no error detail is available
# anywhere — keeps the slim/default failure contract always satisfiable.
_SYNTHESIZED_ERROR = "Unspecified error"
_SYNTHESIZED_ERROR_CODE = "unspecified_error"


def resolve_profile(profile: str | ProjectionProfile | None) -> str | None:
    """Return the canonical profile string, ``DEFAULT_PROFILE`` for empty input,
    or ``None`` if the requested profile is unsupported."""
    if profile is None or profile == "":
        return DEFAULT_PROFILE
    value = profile.value if isinstance(profile, ProjectionProfile) else str(profile)
    return value if value in SUPPORTED_PROFILES else None


def is_supported_profile(profile: str | ProjectionProfile | None) -> bool:
    return resolve_profile(profile) is not None


def unsupported_projection_error(
    profile: str | ProjectionProfile | None,
) -> dict[str, Any]:
    """Structured error for an unsupported profile (BR br_94b4e897)."""
    if isinstance(profile, ProjectionProfile):
        requested: Any = profile.value
    else:
        requested = profile
    return {
        "outcome": OUTCOME_ERROR,
        "error": f"Unsupported projection profile: {requested!r}",
        "error_code": UNSUPPORTED_PROFILE_CODE,
        "supported_profiles": list(SUPPORTED_PROFILES),
    }


def projection_contract_violation_error(
    message: str, **extra: Any
) -> dict[str, Any]:
    """Structured error when a requested outcome cannot satisfy the failure
    contract for the projected payload (e.g. outcome=ok over a failure body)."""
    return {
        "outcome": OUTCOME_ERROR,
        "error": message,
        "error_code": PROJECTION_CONTRACT_VIOLATION_CODE,
        **extra,
    }


def _body_has_failure_signal(body: Any) -> bool:
    """True when a payload carries a legacy failure signal: ``success is False``,
    a non-empty ``error``, or a non-empty ``error_code``."""
    if not isinstance(body, Mapping):
        return False
    if body.get("success") is False:
        return True
    return bool(body.get("error")) or bool(body.get("error_code"))


def _stable_payload_bytes(body: Any) -> int:
    """Deterministic byte size of a JSON-serializable body using stable
    serialization (sorted keys, compact separators)."""
    try:
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        encoded = json.dumps(str(body))
    return len(encoded.encode("utf-8"))


def _normalize_follow_up(
    follow_up: Sequence[Any] | None,
) -> list[dict[str, str]]:
    """Normalize follow-up affordances to compact ``{rel, target_ref}`` dicts."""
    if not follow_up:
        return []
    out: list[dict[str, str]] = []
    for item in follow_up:
        if isinstance(item, Mapping):
            rel = item.get("rel")
            target = item.get("target_ref")
        else:
            rel = getattr(item, "rel", None)
            target = getattr(item, "target_ref", None)
        if rel is None or target is None:
            continue
        out.append({"rel": str(rel), "target_ref": str(target)})
    return out


class MCPProjectionEnvelopeHelper:
    """Wrap an already-computed payload with the canonical projection envelope.

    The helper is response-only: it copies the selected body into a NEW dict and
    never mutates the input payload object.
    """

    def project(
        self,
        *,
        profile: str | ProjectionProfile | None,
        bodies: Mapping[str, Any] | None = None,
        body: Any = None,
        outcome: str | None = None,
        error: str | None = None,
        error_code: str | None = None,
        omitted_count: int = 0,
        deduped_count: int = 0,
        truncated: bool = False,
        follow_up: Sequence[Any] | None = None,
        legacy_success: bool | None = None,
    ) -> dict[str, Any]:
        resolved = resolve_profile(profile)
        if resolved is None:
            return unsupported_projection_error(profile)

        selected = self._select_body(resolved, bodies, body)

        # --- Resolve the canonical outcome deterministically -----------------
        # A failure is signalled by the payload (success is False / error /
        # error_code), by explicit error args, or by legacy_success=False. The
        # default (outcome=None) INFERS the outcome from those signals so a
        # legacy failure can never silently become a canonical success
        # (fr_5c7bccbd / tr_7e1f918c / br_801f3e2a / ac_38572be7).
        failure_signal = (
            _body_has_failure_signal(selected)
            or bool(error)
            or bool(error_code)
            or legacy_success is False
        )
        if outcome is None:
            outcome_value = OUTCOME_ERROR if failure_signal else OUTCOME_OK
        else:
            explicit = OUTCOME_ERROR if str(outcome) == OUTCOME_ERROR else OUTCOME_OK
            if explicit == OUTCOME_OK and failure_signal:
                # Caller claims success over a payload that signals failure —
                # refuse rather than emit an ambiguous envelope.
                return projection_contract_violation_error(
                    f"Requested outcome=ok for profile {resolved!r} but the "
                    "payload signals a failure (success=false / error / "
                    "error_code).",
                    profile=resolved,
                )
            outcome_value = explicit

        # Build a NEW dict — never mutate the caller's payload (TR-0).
        if isinstance(selected, Mapping):
            result: dict[str, Any] = dict(selected)
        else:
            result = {"data": selected}

        if resolved in _SUCCESS_PRESERVING:
            # Preserve both success true and success false (TR-3, AC-2): explicit
            # arg wins, else keep the body's own success, else derive from outcome.
            if legacy_success is not None:
                result["success"] = bool(legacy_success)
            elif "success" in result:
                result["success"] = bool(result["success"])
            else:
                result["success"] = outcome_value == OUTCOME_OK
        else:
            # summary/detail omit positive success and rely on outcome (AC-3).
            result.pop("success", None)

        # --- Failure responses MUST carry error + error_code (AC-3) ----------
        if outcome_value == OUTCOME_ERROR:
            eff_error = error if error is not None else result.get("error")
            eff_code = error_code if error_code is not None else result.get("error_code")
            if not eff_error and not eff_code:
                eff_error, eff_code = _SYNTHESIZED_ERROR, _SYNTHESIZED_ERROR_CODE
            elif not eff_error:
                eff_error = str(eff_code)
            elif not eff_code:
                eff_code = _SYNTHESIZED_ERROR_CODE
            result["error"] = eff_error
            result["error_code"] = eff_code

        result["profile"] = resolved
        result["outcome"] = outcome_value
        result["payload_bytes"] = _stable_payload_bytes(selected)
        result["truncated"] = bool(truncated)
        result["omitted_count"] = int(omitted_count)
        result["deduped_count"] = int(deduped_count)
        result["follow_up"] = _normalize_follow_up(follow_up)
        return result

    def _select_body(
        self,
        profile: str,
        bodies: Mapping[str, Any] | None,
        body: Any,
    ) -> Any:
        if bodies:
            for key in _BODY_FALLBACK[profile]:
                candidate = bodies.get(key)
                if candidate is not None:
                    return candidate
        if body is not None:
            return body
        return {}


_ENVELOPE = MCPProjectionEnvelopeHelper()


def project_response(
    *,
    profile: str | ProjectionProfile | None,
    bodies: Mapping[str, Any] | None = None,
    body: Any = None,
    outcome: str | None = None,
    error: str | None = None,
    error_code: str | None = None,
    omitted_count: int = 0,
    deduped_count: int = 0,
    truncated: bool = False,
    follow_up: Sequence[Any] | None = None,
    legacy_success: bool | None = None,
) -> dict[str, Any]:
    """Module-level convenience over a shared envelope-helper instance."""
    return _ENVELOPE.project(
        profile=profile,
        bodies=bodies,
        body=body,
        outcome=outcome,
        error=error,
        error_code=error_code,
        omitted_count=omitted_count,
        deduped_count=deduped_count,
        truncated=truncated,
        follow_up=follow_up,
        legacy_success=legacy_success,
    )
