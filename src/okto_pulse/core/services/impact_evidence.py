"""Current impact-evidence enforcement policy; invalid values are refused."""

from __future__ import annotations

from typing import Mapping

IMPACT_EVIDENCE_MODES = frozenset({"off", "advisory", "require"})


def resolve_impact_evidence_mode(board: object | None) -> tuple[str, str]:
    settings = getattr(board, "settings", None) if board is not None else None
    if settings is None:
        settings = {}
    if not isinstance(settings, Mapping):
        raise ValueError("impact_evidence_policy_invalid")
    if "impact_evidence_mode" not in settings:
        return "off", "board_default"
    mode = settings["impact_evidence_mode"]
    if not isinstance(mode, str) or mode not in IMPACT_EVIDENCE_MODES:
        raise ValueError("impact_evidence_policy_invalid")
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
