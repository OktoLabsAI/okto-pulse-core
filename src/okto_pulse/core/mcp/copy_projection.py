"""Copy-tool response projection (spec R2, card R2.3, FR9 / api_c163fb7d).

Compacts the default response of ``okto_pulse_copy_architecture_to_card`` so the
frequently-called card-execution pre-flight stops echoing full Architecture
Design bodies (HTML/Excalidraw payloads, adapter payloads, diagram bodies). It
reuses the R5.1 envelope primitives + canonical R5 projection metadata and aligns
with the sibling copy tools (``copy_mockups_to_card`` / ``copy_knowledge_to_card``
already return ``{success, copied, total_on_card}``).

Profiles for the copy tools are a 3-value set — there is no ``detail`` middle
ground: either copy metadata (``summary``) or the full copied bodies
(``full``/``legacy``):

- ``summary`` (DEFAULT) — ``success`` + ``copied`` + ``design_ids`` +
  ``total_on_card`` + the canonical ``projection`` metadata. NO
  ``architecture_designs`` body.
- ``full`` / ``legacy`` — the prior payload exactly: ``{success, copied,
  architecture_designs:[full bodies]}`` (FR-2 back-compat, no envelope injected).
- unsupported profile (including ``detail``) → structured ``unsupported_projection``
  error with ``supported_profiles=[summary, full, legacy]``; never a silent
  fallback.

It is PURE response shaping — it never touches the copy persistence (that already
ran) or authorization; ``total_on_card`` is computed by the caller after the copy.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from okto_pulse.core.mcp.payload_budget import (
    METRIC_COPY_ARCHITECTURE_BYTES,
    METRIC_COPY_ARCHITECTURE_USAGE,
)
from okto_pulse.core.mcp.projection_envelope import (
    OUTCOME_ERROR,
    OUTCOME_OK,
    UNSUPPORTED_PROFILE_CODE,
    _stable_payload_bytes,
)

_LOG = logging.getLogger("okto_pulse.mcp.copy_projection")

TOOL_NAME = "okto_pulse_copy_architecture_to_card"

# The copy tools support a 3-value profile set (no ``detail``).
COPY_SUPPORTED_PROFILES: tuple[str, ...] = ("summary", "full", "legacy")
COPY_DEFAULT_PROFILE = "summary"
_COPY_PASSTHROUGH = frozenset({"full", "legacy"})


def resolve_copy_profile(profile: str | None) -> str | None:
    """Canonical copy profile, ``summary`` for empty input, or ``None`` if the
    requested profile is unsupported for the copy tools (``detail`` is unsupported
    here, unlike the context tools)."""
    if profile is None or profile == "":
        return COPY_DEFAULT_PROFILE
    value = str(profile)
    return value if value in COPY_SUPPORTED_PROFILES else None


def unsupported_copy_profile_error(profile: str | None) -> dict[str, Any]:
    """Structured error for an unsupported copy profile (no silent fallback)."""
    return {
        "outcome": OUTCOME_ERROR,
        "error": f"Unsupported projection profile: {profile!r}",
        "error_code": UNSUPPORTED_PROFILE_CODE,
        "supported_profiles": list(COPY_SUPPORTED_PROFILES),
    }


def _emit_copy_architecture_metric(
    *, profile: str, payload_bytes: int, copied: int
) -> dict[str, Any]:
    """Emit the SAFE copy-architecture diagnostics (R2.3, or_a51c51b4) under
    ``mcp_copy_architecture_response_total`` + ``mcp_copy_architecture_response_bytes``.
    Counts + identifiers only — never a body."""
    labels = {
        "tool_name": TOOL_NAME,
        "profile": profile,
        "payload_bytes": int(payload_bytes),
        "copied": int(copied),
    }
    _LOG.info(METRIC_COPY_ARCHITECTURE_USAGE, extra={"copy_architecture": labels})
    _LOG.info(METRIC_COPY_ARCHITECTURE_BYTES, extra={"copy_architecture": labels})
    return labels


def _set_final_payload_bytes(response: dict[str, Any], projection: dict[str, Any]) -> int:
    """Set ``projection['payload_bytes']`` to the byte size of the FINAL projected
    ``response`` — measured AFTER projection, including the envelope (card R2.3).

    The counter lives inside the payload it measures, so the size is the fixpoint of
    ``n -> len(serialize(response | payload_bytes=n))``. It converges in a couple of
    steps because only the decimal width of the counter changes between iterations.
    After this returns, ``projection['payload_bytes'] == _stable_payload_bytes(response)``.
    """
    size = 0
    for _ in range(8):
        projection["payload_bytes"] = size
        new_size = _stable_payload_bytes(response)
        if new_size == size:
            return size
        size = new_size
    projection["payload_bytes"] = size
    return size


def project_copy_architecture_response(
    designs: Sequence[Mapping[str, Any]],
    *,
    total_on_card: int,
    profile: str | None,
) -> dict[str, Any]:
    """Shape the ``copy_architecture_to_card`` response for ``profile``.

    ``designs`` are the FULL serialized designs copied in this call (used for the
    full/legacy body and to derive ``copied``/``design_ids``). ``total_on_card`` is
    the count of Architecture Designs on the card AFTER the copy.
    """
    resolved = resolve_copy_profile(profile)
    if resolved is None:
        return unsupported_copy_profile_error(profile)

    copied = len(designs)
    design_ids = [d.get("id") for d in designs if isinstance(d, Mapping)]

    if resolved in _COPY_PASSTHROUGH:
        # FR-2 back-compat: the prior payload shape exactly (full bodies, no
        # envelope injected) — byte-identical to the pre-R2.3 response.
        out: dict[str, Any] = {
            "success": True,
            "copied": copied,
            "architecture_designs": list(designs),
        }
        _emit_copy_architecture_metric(
            profile=resolved, payload_bytes=_stable_payload_bytes(out), copied=copied
        )
        return out

    # summary/default: copy metadata + ids only — the full bodies are persisted on
    # the card now, so echoing them duplicates persisted data. They stay reachable
    # via ``profile=full`` (the read_full_architecture follow_up).
    projection: dict[str, Any] = {
        "profile": resolved,
        "outcome": OUTCOME_OK,
        "payload_bytes": 0,  # set below to the FINAL projected size
        "truncated": False,
        "omitted_count": 0,
        # the N copied bodies are duplicated persisted data dropped from the echo.
        "deduped_count": copied,
        "follow_up": [{"rel": "read_full_architecture", "target_ref": TOOL_NAME}],
    }
    response: dict[str, Any] = {
        # Keep the sibling resource-copy contract uniform. ``projection.outcome``
        # remains the canonical projection envelope, while this positive flag is
        # the stable workflow-level acknowledgement used by all three copy tools.
        "success": True,
        "copied": copied,
        "design_ids": design_ids,
        "total_on_card": int(total_on_card),
        "projection": projection,
    }
    # payload_bytes measures the serialized payload AFTER projection (incl. envelope).
    payload_bytes = _set_final_payload_bytes(response, projection)
    _emit_copy_architecture_metric(
        profile=resolved, payload_bytes=payload_bytes, copied=copied
    )
    return response
