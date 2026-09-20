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


async def require_current_report_impact(session: object, card: object, spec: object) -> None:
    """Called only by the impact `require` completion posture, not Delivery policy."""
    from okto_pulse.core.domain.delivery_selection import current_delivery_report, report_reuses_impact
    from okto_pulse.core.domain.delivery_evidence import CardDeliveryScope
    from okto_pulse.core.services.delivery_evidence import card_delivery_store

    report = current_delivery_report(card)
    if report is None or not report_reuses_impact(report):
        return
    status = await card_delivery_store(session).report_impact_status(
        CardDeliveryScope(card.board_id, card.id, spec.id, int(spec.edition)), for_update=True
    )
    if status.get("current") is not True:
        raise ValueError("impact_evidence_stale: " + str(status.get("reason") or "basis_unavailable")[:128])


__all__ = ["IMPACT_EVIDENCE_MODES", "resolve_impact_evidence_mode", "require_current_report_impact"]
