"""Impact-evidence enforcement policy (SK-B2-S1, FR-5/FR-6, TR-4).

The board setting is resolved in one place, in the same
``invalid_value_fail_compat`` pattern as ``reviewer_separation``: reading a
persisted legacy/tampered value NEVER fail-closes a move — an absent or
out-of-enum value resolves to ``off`` with an explicit, auditable source.
Write-time validation (BoardCreate/BoardUpdate via ``BoardSettings``) owns
rejecting invalid values.
"""

from __future__ import annotations

from typing import Mapping

IMPACT_EVIDENCE_MODES = frozenset({"off", "advisory", "require"})


def resolve_impact_evidence_mode(board: object | None) -> tuple[str, str]:
    settings = getattr(board, "settings", None) if board is not None else None
    if (
        not isinstance(settings, Mapping)
        or "impact_evidence_mode" not in settings
    ):
        return "off", "legacy_absent_compat"
    mode = str(settings.get("impact_evidence_mode") or "off").strip().lower()
    if mode not in IMPACT_EVIDENCE_MODES:
        return "off", "invalid_value_fail_compat"
    return mode, "board_settings"


__all__ = ["IMPACT_EVIDENCE_MODES", "resolve_impact_evidence_mode"]
