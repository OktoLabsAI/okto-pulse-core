"""R5-IMP2 — read-only skip-override read-model for context/readiness payloads.

Exposes the gates an operator explicitly SKIPPED, READ-ONLY, so MCP/UI (and the
R4 enforcement contract) can show what is being bypassed and why — WITHOUT a new
store. Two sources, reused as-is:

- ambiguity skip: ``Ideation.skip_ambiguity_gate`` (bool) + the most recent
  ``ActivityLog`` ``ideation.ambiguity_gate_skip_updated`` (applied_by/at/source).
  The ambiguity skip carries no reason_code/justification/evidence (justification
  is therefore null).
- cognitive skip: the EXISTING ``CognitiveConsolidationItem`` ledger
  (``status=skipped``) — reason_code / justification / evidence_refs / revisit_at /
  actor.

DLQ and CanonicalDebt are SEPARATE domains and are never masked here (a skip never
clears a technical/canonical blocker). ``mutation_allowed`` is always ``False`` in
the agent-facing payload — this is a read-model; mutations go through the dedicated
skip-write tools (R5-IMP1 territory).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

GATE_AMBIGUITY = "ambiguity_gate"
GATE_COGNITIVE = "cognitive_consolidation"

SOURCE_AMBIGUITY = "ideation.skip_ambiguity_gate"
SOURCE_COGNITIVE = "cognitive_consolidation_item_ledger"

STATE_ACTIVE = "active"
STATE_EXPIRED = "expired"

_AMBIGUITY_SKIP_ACTION = "ideation.ambiguity_gate_skip_updated"


@dataclass
class SkipOverride:
    gate: str
    effective_state: str
    source: str
    applied_by: str | None = None
    applied_at: str | None = None
    reason_code: str | None = None
    justification: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    revisit_at: str | None = None
    mutation_allowed: bool = False
    # Identifies which artifact the skip applies to (cognitive skips are
    # per-artifact; ambiguity is the ideation itself).
    target_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "effective_state": self.effective_state,
            "source": self.source,
            "applied_by": self.applied_by,
            "applied_at": self.applied_at,
            "reason_code": self.reason_code,
            "justification": self.justification,
            "evidence_refs": list(self.evidence_refs),
            "revisit_at": self.revisit_at,
            "mutation_allowed": self.mutation_allowed,
            "target_ref": self.target_ref,
        }


def _revisit_expired(revisit_at: str | None) -> bool:
    """True when a cognitive skip's revisit_at is in the past (skip lapsed)."""
    if not revisit_at:
        return False
    try:
        dt = datetime.fromisoformat(str(revisit_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt < datetime.now(timezone.utc)


async def _ambiguity_skip_override(
    db: AsyncSession, ideation, board_id: str,
) -> SkipOverride | None:
    """The ideation's own ambiguity skip, with applied_by/at from the latest
    audit log entry that turned it ON. Returns None when not skipped."""
    if not getattr(ideation, "skip_ambiguity_gate", False):
        return None

    from okto_pulse.core.models.db import ActivityLog

    applied_by: str | None = None
    applied_at: str | None = None
    source_channel = SOURCE_AMBIGUITY
    rows = (await db.execute(
        select(ActivityLog)
        .where(
            ActivityLog.board_id == board_id,
            ActivityLog.action == _AMBIGUITY_SKIP_ACTION,
        )
        .order_by(ActivityLog.created_at.desc())
    )).scalars().all()
    for log in rows:
        details = log.details or {}
        if str(details.get("ideation_id") or "") != str(ideation.id):
            continue
        if details.get("new_value") is True:
            applied_by = log.actor_id
            applied_at = log.created_at.isoformat() if log.created_at else None
            if details.get("source"):
                source_channel = f"{SOURCE_AMBIGUITY}:{details['source']}"
            break

    return SkipOverride(
        gate=GATE_AMBIGUITY,
        effective_state=STATE_ACTIVE,  # ambiguity skip has no revisit -> active while on
        source=source_channel,
        applied_by=applied_by,
        applied_at=applied_at,
        reason_code=None,
        justification=None,
        evidence_refs=[],
        revisit_at=None,
        target_ref=f"ideation:{ideation.id}",
    )


def _candidate_ids(source_ref: str, artifact_id: str) -> set[str]:
    ids: set[str] = set()
    if artifact_id:
        ids.add(str(artifact_id))
    parts = str(source_ref or "").split(":")
    if len(parts) >= 2 and parts[1]:
        ids.add(parts[1])
    return ids


def _cognitive_skip_overrides(board_id: str, relevant_ids: set[str]) -> list[SkipOverride]:
    """Skipped items from the EXISTING cognitive consolidation ledger whose
    artifact belongs to ``relevant_ids`` (spec id + its card ids). Degrades to an
    empty list if the ledger is unavailable (never a false 'no skips' that hides a
    blocker — DLQ/CanonicalDebt are surfaced by their own separate diagnostics)."""
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        CognitiveItemStatus,
        default_rebuild_base_dir,
    )

    try:
        store = CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
        gen = store.latest_generation(board_id)
        if gen is None:
            return []
        items = store.list_items(
            board_id, gen, status_filter=CognitiveItemStatus.SKIPPED.value,
        )
    except Exception:
        return []

    out: list[SkipOverride] = []
    for item in items:
        if relevant_ids and not (_candidate_ids(item.source_ref, item.artifact_id) & relevant_ids):
            continue
        out.append(SkipOverride(
            gate=GATE_COGNITIVE,
            effective_state=(
                STATE_EXPIRED if _revisit_expired(item.revisit_at) else STATE_ACTIVE
            ),
            source=SOURCE_COGNITIVE,
            applied_by=item.actor or item.updated_by_agent_id,
            applied_at=item.updated_at or item.recorded_at,
            reason_code=item.reason_code,
            justification=item.justification,
            evidence_refs=list(item.evidence_refs or ()),
            revisit_at=item.revisit_at,
            target_ref=item.source_ref_original or item.source_ref,
        ))
    return out


async def ideation_skip_overrides(db: AsyncSession, ideation, board_id: str) -> list[dict[str, Any]]:
    """R5-IMP2 read-model for get_ideation_context: the ideation's own ambiguity
    skip (effective override). Cognitive skips are per-artifact and surface in the
    spec/readiness contexts, not here."""
    override = await _ambiguity_skip_override(db, ideation, board_id)
    return [override.to_dict()] if override else []


async def spec_skip_overrides(db: AsyncSession, spec, board_id: str) -> list[dict[str, Any]]:
    """R5-IMP2 read-model for get_spec_context: cognitive skips for the spec and its
    cards (the effective overrides on this spec's artifacts). The parent ideation's
    ambiguity skip is lineage/history, NOT an effective override of the spec, so it
    is intentionally excluded (codex Q1)."""
    relevant_ids: set[str] = {str(spec.id)}
    for card in getattr(spec, "cards", None) or []:
        if getattr(card, "id", None):
            relevant_ids.add(str(card.id))
    return [o.to_dict() for o in _cognitive_skip_overrides(board_id, relevant_ids)]


__all__ = [
    "GATE_AMBIGUITY",
    "GATE_COGNITIVE",
    "SkipOverride",
    "ideation_skip_overrides",
    "spec_skip_overrides",
]
