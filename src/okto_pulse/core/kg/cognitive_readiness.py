"""S1 Cognitive Closure — CognitiveReadinessService (spec 2012f38d, card 3326e3e4).

Single owner of cognitive readiness composition. Reads the
:class:`CognitiveConsolidationItemStore` DIRECTLY (never via the API/MCP
projections), reconciles aliases via the S1.1 normalized ``artifact_id``
(``normalize_cognitive_artifact_id``), and applies ONE deterministic 6-tier
precedence (fr_d9cea4e0 / dec_2cc65238):

    1. technical DLQ                  -> BLOCK (readiness_signal=technical_dlq)
    2. canonical_debt in OPEN_STATES  -> BLOCK (readiness_signal=canonical_debt_open)
    3. cognitive item active/failed   -> BLOCK (cognitive debt)
    4. expired revisit-required skip  -> BLOCK (skip lapsed)
    5. valid skip / no_action         -> UNBLOCK (per policy)
    6. terminal committed history     -> READY (informational; last_error never blocks)

An artifact with no reusable cognition (a task/test that carries no decision,
learning or reusable root cause) stays advisory / non-blocking even under a
global blocking policy (fr_6c423f6d).

``record_cognitive_skip`` is LEDGER-ONLY (br_658945e6 / dec_2cc65238): it wraps
:meth:`CognitiveConsolidationItemStore.update_item` (status=skipped,
outcome_type=no_action_required) and NEVER calls the KG consolidation worker,
creates a node/edge, or runs the connectivity guard. It rejects BEFORE any
write (fr_07954805 / api_159e9cd0 / api_08c667c7):
  * 400 ``invalid_reason_code``            — unknown reason, or a technical signal used as reason_code;
  * 400 ``revisit_at_required``            — revisit-required reason without a valid/future revisit_at;
  * 409 ``technical_debt_cannot_be_skipped`` — DLQ or canonical_debt OPEN for the same normalized artifact_id.

``reason_code`` is a CLOSED, cognitive-only, selectable registry. The technical
signals ``technical_dlq`` / ``canonical_debt_open`` / ``connectivity_guard`` are
readiness signals / error causes only — NEVER selectable reason_codes
(br_246210a3).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import CanonicalDebt, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import OPEN_STATES
from okto_pulse.core.kg.rebuild_audit import (
    ACTIVE_ITEM_STATUSES,
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    compute_cognitive_item_id,
    normalize_cognitive_artifact_id,
)


# ---------------------------------------------------------------------------
# Closed reason_code registry (br_246210a3) — cognitive + selectable only.
# ---------------------------------------------------------------------------


class CognitiveReasonCode(str, Enum):
    """The ONLY reason_codes a caller may select on a cognitive skip/no_action.

    Terminal codes close the item; revisit-required codes demand a future
    ``revisit_at`` and re-block once that instant passes.
    """

    # terminal — may unblock readiness per policy
    NO_REUSABLE_LEARNING = "no_reusable_learning"
    DUPLICATE_BUG = "duplicate_bug"
    TRIVIAL_FIX = "trivial_fix"
    # revisit-required — need a valid/future revisit_at
    ROOT_CAUSE_UNCONFIRMED = "root_cause_unconfirmed"
    EVIDENCE_INSUFFICIENT = "evidence_insufficient"
    PATH_B_PENDING = "path_b_pending"
    EXTERNAL_CONTEXT_MISSING = "external_context_missing"


TERMINAL_REASON_CODES: frozenset[str] = frozenset({
    CognitiveReasonCode.NO_REUSABLE_LEARNING.value,
    CognitiveReasonCode.DUPLICATE_BUG.value,
    CognitiveReasonCode.TRIVIAL_FIX.value,
})

REVISIT_REQUIRED_REASON_CODES: frozenset[str] = frozenset({
    CognitiveReasonCode.ROOT_CAUSE_UNCONFIRMED.value,
    CognitiveReasonCode.EVIDENCE_INSUFFICIENT.value,
    CognitiveReasonCode.PATH_B_PENDING.value,
    CognitiveReasonCode.EXTERNAL_CONTEXT_MISSING.value,
})

SELECTABLE_REASON_CODES: frozenset[str] = TERMINAL_REASON_CODES | REVISIT_REQUIRED_REASON_CODES

# Technical readiness signals / error causes — NEVER selectable as reason_code
# (br_246210a3). Surfaced as ``readiness_signal`` on a blocking verdict.
TECHNICAL_DLQ_SIGNAL = "technical_dlq"
CANONICAL_DEBT_OPEN_SIGNAL = "canonical_debt_open"
CONNECTIVITY_GUARD_SIGNAL = "connectivity_guard"
TECHNICAL_READINESS_SIGNALS: frozenset[str] = frozenset({
    TECHNICAL_DLQ_SIGNAL,
    CANONICAL_DEBT_OPEN_SIGNAL,
    CONNECTIVITY_GUARD_SIGNAL,
})


class ReadinessTier(str, Enum):
    """Deterministic precedence tier that decided a verdict (highest first)."""

    TECHNICAL_DLQ = "technical_dlq"            # 1
    CANONICAL_DEBT_OPEN = "canonical_debt_open"  # 2
    COGNITIVE_ACTIVE = "cognitive_active"      # 3
    SKIP_EXPIRED = "skip_expired"              # 4
    SKIP_VALID = "skip_valid"                  # 5
    TERMINAL_HISTORY = "terminal_history"      # 6
    ADVISORY_NO_COGNITION = "advisory_no_cognition"  # non-blocking by nature
    READY = "ready"                            # no pending cognitive work


# The deterministic subset of tiers the DONE-GATE actually ENFORCES when a
# board's blocking policy is active (services/main.py). A COGNITIVE_ACTIVE
# verdict is ``blocking=True`` at the verdict level but ADVISORY at the gate —
# which is exactly why a task/test cognitive-pending never blocks done. Kept as
# a single shared source so the gate AND the MCP report enforcement WITHOUT
# recomputing it (S3.2 carry-forward: never recompute enforcement in the MCP).
GATE_BLOCKING_TIERS: frozenset[str] = frozenset({
    ReadinessTier.TECHNICAL_DLQ.value,
    ReadinessTier.CANONICAL_DEBT_OPEN.value,
    ReadinessTier.SKIP_EXPIRED.value,
})


class CognitiveReadinessError(Exception):
    """Typed error carrying the bounded ``code`` + HTTP status the API/MCP
    surfaces echo verbatim (api_159e9cd0 / api_08c667c7)."""

    def __init__(self, code: str, message: str, *, http_status: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message, "status_code": self.http_status}


# Bounded readiness_effect per precedence tier — more specific than a bare
# ready|blocking so consumers (bug evaluate REST/MCP, Action Center) can render
# WHY without recomputing precedence. This service is the ONLY source of
# readiness_effect / precedence_explanation (tr_28465cc7).
_READINESS_EFFECT_BY_TIER: dict[str, str] = {
    ReadinessTier.TECHNICAL_DLQ.value: "blocking_technical",
    ReadinessTier.CANONICAL_DEBT_OPEN.value: "blocking_technical",
    ReadinessTier.COGNITIVE_ACTIVE.value: "blocking_cognitive",
    ReadinessTier.SKIP_EXPIRED.value: "blocking_revisit_lapsed",
    ReadinessTier.SKIP_VALID.value: "ready_skip",
    ReadinessTier.TERMINAL_HISTORY.value: "ready_committed",
    ReadinessTier.ADVISORY_NO_COGNITION.value: "advisory",
    ReadinessTier.READY.value: "ready",
}


@dataclass(frozen=True, slots=True)
class CognitiveReadinessVerdict:
    """Composed readiness for ONE normalized artifact_id."""

    artifact_id: str
    ready: bool
    blocking: bool
    tier: str
    readiness_signal: str | None = None
    reason_code: str | None = None
    revisit_at: str | None = None
    detail: str = ""
    # Derived from the deciding tier (single source for downstream surfaces —
    # the bug evaluate must mirror these, never recompute precedence).
    readiness_effect: str = ""
    precedence_explanation: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.readiness_effect:
            object.__setattr__(
                self,
                "readiness_effect",
                _READINESS_EFFECT_BY_TIER.get(self.tier, "ready"),
            )
        if not self.precedence_explanation:
            object.__setattr__(self, "precedence_explanation", {
                "tier": self.tier,
                "readiness_signal": self.readiness_signal,
                "reason_code": self.reason_code,
                "detail": self.detail,
            })

    def to_api(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "ready": self.ready,
            "blocking": self.blocking,
            "tier": self.tier,
            "readiness_signal": self.readiness_signal,
            "reason_code": self.reason_code,
            "revisit_at": self.revisit_at,
            "detail": self.detail,
            "readiness_effect": self.readiness_effect,
            "precedence_explanation": dict(self.precedence_explanation),
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def validate_skip_reason(reason_code: Any, revisit_at: Any, *, now: datetime | None = None) -> None:
    """Reject a skip request BEFORE any write (fr_07954805).

    Raises :class:`CognitiveReadinessError` with the bounded code:
      * 400 ``invalid_reason_code`` — reason not in the closed cognitive registry
        (this also rejects a technical signal smuggled in as a reason_code);
      * 400 ``revisit_at_required`` — revisit-required reason without a
        valid, strictly-future ``revisit_at``.
    """

    code = str(reason_code or "")
    if code not in SELECTABLE_REASON_CODES:
        raise CognitiveReadinessError(
            "invalid_reason_code",
            (
                f"reason_code {code!r} is not a selectable cognitive reason. "
                f"Allowed: {sorted(SELECTABLE_REASON_CODES)}. Technical signals "
                f"({sorted(TECHNICAL_READINESS_SIGNALS)}) are never selectable."
            ),
            http_status=400,
        )
    if code in REVISIT_REQUIRED_REASON_CODES:
        when = _parse_dt(revisit_at)
        if when is None or when <= (now or _now()):
            raise CognitiveReadinessError(
                "revisit_at_required",
                (
                    f"reason_code {code!r} is revisit-required and needs a valid, "
                    "future revisit_at (ISO-8601)."
                ),
                http_status=400,
            )


def compose_readiness(
    *,
    artifact_id: str,
    technical_dlq: bool,
    canonical_debt_open: bool,
    cognitive_items: Sequence[CognitiveConsolidationItem],
    has_reusable_cognition: bool = True,
    now: datetime | None = None,
) -> CognitiveReadinessVerdict:
    """Pure 6-tier readiness composition for a single artifact_id.

    The technical tiers (1, 2) win even when a valid skip exists — a skip can
    NEVER mask DLQ / canonical debt (br_658945e6). All inputs are already
    resolved; this function does no I/O so the precedence is fully testable.
    """

    now = now or _now()

    # Tier 1 — technical DLQ.
    if technical_dlq:
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=False, blocking=True,
            tier=ReadinessTier.TECHNICAL_DLQ.value,
            readiness_signal=TECHNICAL_DLQ_SIGNAL,
            detail="Technical dead-letter present; resolve the DLQ before closure.",
        )

    # Tier 2 — canonical_debt OPEN.
    if canonical_debt_open:
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=False, blocking=True,
            tier=ReadinessTier.CANONICAL_DEBT_OPEN.value,
            readiness_signal=CANONICAL_DEBT_OPEN_SIGNAL,
            detail="Canonical debt is OPEN; resolve/retry before closure.",
        )

    active = [i for i in cognitive_items if i.status in ACTIVE_ITEM_STATUSES]
    skips = [
        i for i in cognitive_items
        if i.status == CognitiveItemStatus.SKIPPED.value
    ]
    consolidated = [
        i for i in cognitive_items
        if i.status == CognitiveItemStatus.CONSOLIDATED.value
    ]

    # Tier 3 — active/failed cognitive item.
    if active:
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=False, blocking=True,
            tier=ReadinessTier.COGNITIVE_ACTIVE.value,
            detail=f"{len(active)} active/failed cognitive item(s) await consolidation.",
        )

    # Tiers 4/5 — skip lifecycle. An expired revisit-required skip re-blocks;
    # a valid (terminal, or not-yet-due revisit) skip unblocks.
    if skips:
        expired = [
            s for s in skips
            if s.reason_code in REVISIT_REQUIRED_REASON_CODES
            and (_parse_dt(s.revisit_at) is None or _parse_dt(s.revisit_at) <= now)
        ]
        if expired:
            chosen = expired[0]
            return CognitiveReadinessVerdict(
                artifact_id=artifact_id, ready=False, blocking=True,
                tier=ReadinessTier.SKIP_EXPIRED.value,
                reason_code=chosen.reason_code,
                revisit_at=chosen.revisit_at,
                detail="Revisit-required skip has lapsed; re-evaluate before closure.",
            )
        chosen = skips[0]
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=True, blocking=False,
            tier=ReadinessTier.SKIP_VALID.value,
            reason_code=chosen.reason_code,
            revisit_at=chosen.revisit_at,
            detail="Valid cognitive skip / no_action recorded.",
        )

    # Tier 6 — terminal committed history (informational; last_error never blocks).
    if consolidated:
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=True, blocking=False,
            tier=ReadinessTier.TERMINAL_HISTORY.value,
            detail="Cognitive consolidation already committed.",
        )

    # No cognitive items for this artifact.
    if not has_reusable_cognition:
        return CognitiveReadinessVerdict(
            artifact_id=artifact_id, ready=True, blocking=False,
            tier=ReadinessTier.ADVISORY_NO_COGNITION.value,
            detail="No reusable cognition (task/test); advisory, non-blocking.",
        )
    return CognitiveReadinessVerdict(
        artifact_id=artifact_id, ready=True, blocking=False,
        tier=ReadinessTier.READY.value,
        detail="No pending cognitive work for this artifact.",
    )


class CognitiveReadinessService:
    """Single owner of readiness composition + the ledger-only skip writer."""

    def __init__(
        self,
        store: CognitiveConsolidationItemStore,
        *,
        now=_now,
    ) -> None:
        self._store = store
        self._now = now

    # -- signal resolution ------------------------------------------------

    async def _open_canonical_debt(
        self, db: AsyncSession, board_id: str, artifact_id: str
    ) -> bool:
        rows = (await db.execute(
            select(CanonicalDebt).where(CanonicalDebt.board_id == board_id)
        )).scalars().all()
        for row in rows:
            if str(row.canonical_state or "") not in OPEN_STATES:
                continue
            ref = row.source_ref or f"{row.artifact_type}:{row.artifact_id}"
            if normalize_cognitive_artifact_id(ref) == artifact_id:
                return True
        return False

    async def _technical_dlq(
        self, db: AsyncSession, board_id: str, artifact_id: str
    ) -> bool:
        rows = (await db.execute(
            select(ConsolidationDeadLetter).where(
                ConsolidationDeadLetter.board_id == board_id
            )
        )).scalars().all()
        for row in rows:
            ref = f"{row.artifact_type}:{row.artifact_id}"
            if normalize_cognitive_artifact_id(ref) == artifact_id:
                return True
        return False

    def _items_for_artifact(
        self, board_id: str, artifact_id: str, kg_generation_id: str | None
    ) -> list[CognitiveConsolidationItem]:
        gen = kg_generation_id or self._store.latest_generation(board_id)
        if not gen:
            return []
        return [
            item for item in self._store.list_items(board_id, gen)
            if item.artifact_id == artifact_id
        ]

    # -- public API -------------------------------------------------------

    async def evaluate_artifact(
        self,
        db: AsyncSession,
        *,
        board_id: str,
        source_ref: str,
        kg_generation_id: str | None = None,
        has_reusable_cognition: bool = True,
    ) -> CognitiveReadinessVerdict:
        """Compose the 6-tier readiness verdict for one artifact (api_159e9cd0)."""

        artifact_id = normalize_cognitive_artifact_id(source_ref)
        technical_dlq = await self._technical_dlq(db, board_id, artifact_id)
        debt_open = await self._open_canonical_debt(db, board_id, artifact_id)
        items = self._items_for_artifact(board_id, artifact_id, kg_generation_id)
        return compose_readiness(
            artifact_id=artifact_id,
            technical_dlq=technical_dlq,
            canonical_debt_open=debt_open,
            cognitive_items=items,
            has_reusable_cognition=has_reusable_cognition,
            now=self._now(),
        )

    def cognitive_items_for(
        self,
        board_id: str,
        source_ref: str,
        kg_generation_id: str | None = None,
    ) -> list[CognitiveConsolidationItem]:
        """Public read of the cognitive items for one artifact (by normalized
        artifact_id). Lets the bug evaluate detect a MISSING bug node without
        reimplementing store access or precedence."""
        return self._items_for_artifact(
            board_id,
            normalize_cognitive_artifact_id(source_ref),
            kg_generation_id,
        )

    async def record_cognitive_skip(
        self,
        db: AsyncSession,
        *,
        board_id: str,
        source_ref: str,
        reason_code: str,
        actor: str,
        justification: str | None = None,
        evidence_refs: Sequence[str] | None = None,
        revisit_at: str | None = None,
        kg_generation_id: str | None = None,
    ) -> CognitiveConsolidationItem:
        """Ledger-only cognitive skip / no_action (api_08c667c7 / br_658945e6).

        Validates + refuses BEFORE any write, then wraps
        ``store.update_item`` (status=skipped, outcome_type=no_action_required).
        Never calls the KG worker, creates a node/edge, or runs the
        connectivity guard.
        """

        now = self._now()
        # 1. 400s — closed registry + revisit_at (before any write).
        validate_skip_reason(reason_code, revisit_at, now=now)

        artifact_id = normalize_cognitive_artifact_id(source_ref)
        # 2. 409 — a skip must never mask technical DLQ / open canonical debt.
        if await self._technical_dlq(db, board_id, artifact_id):
            raise CognitiveReadinessError(
                "technical_debt_cannot_be_skipped",
                "A technical dead-letter exists for this artifact; resolve it "
                "instead of recording a cognitive skip.",
                http_status=409,
            )
        if await self._open_canonical_debt(db, board_id, artifact_id):
            raise CognitiveReadinessError(
                "technical_debt_cannot_be_skipped",
                "Canonical debt is OPEN for this artifact; resolve/retry it "
                "instead of recording a cognitive skip.",
                http_status=409,
            )

        gen = kg_generation_id or self._store.latest_generation(board_id)
        if not gen:
            raise CognitiveReadinessError(
                "generation_not_found",
                "No cognitive generation exists for this board; nothing to skip.",
                http_status=404,
            )
        item_id = compute_cognitive_item_id(board_id, gen, source_ref)
        # 3. Ledger-only write — store.update_item ONLY. No KG worker / node /
        # edge / connectivity guard (dec_2cc65238).
        updated = self._store.update_item(
            board_id=board_id,
            kg_generation_id=gen,
            item_id=item_id,
            new_status=CognitiveItemStatus.SKIPPED.value,
            updated_by_agent_id=actor,
            outcome_type=CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value,
            reason_code=reason_code,
            justification=justification,
            evidence_refs=evidence_refs,
            actor=actor,
            revisit_at=revisit_at,
        )
        if updated is None:
            raise CognitiveReadinessError(
                "cognitive_item_not_found",
                f"No cognitive item {item_id!r} for source_ref {source_ref!r}.",
                http_status=404,
            )
        return updated

    async def clear_cognitive_skip(
        self,
        db: AsyncSession,
        *,
        board_id: str,
        source_ref: str,
        actor: str,
        kg_generation_id: str | None = None,
    ) -> CognitiveConsolidationItem:
        """Clear a cognitive skip / no_action, reopening the item to PENDING via
        the SAME central store path (fr_2112ce8c). Ledger-only — never touches
        the KG worker / node / edge / connectivity guard.

        Reopening only INCREASES readiness pressure (an active item blocks), so —
        unlike record — it carries NO technical DLQ/debt 409 guard. The clear is
        an explicit mutation: ``clear_readiness_metadata`` drops the stale
        reason_code / justification / revisit_at and stamps the clearing actor so
        the audit trail (updated_by_agent_id / updated_at) is preserved.

        Errors (before any write):
          * 404 ``generation_not_found``      — no cognitive generation for the board;
          * 404 ``cognitive_item_not_found``  — no item for the source_ref;
          * 409 ``cognitive_item_not_skipped`` — the item is not in a skipped state.
        """

        gen = kg_generation_id or self._store.latest_generation(board_id)
        if not gen:
            raise CognitiveReadinessError(
                "generation_not_found",
                "No cognitive generation exists for this board; nothing to clear.",
                http_status=404,
            )
        item_id = compute_cognitive_item_id(board_id, gen, source_ref)
        existing = next(
            (i for i in self._store.list_items(board_id, gen) if i.item_id == item_id),
            None,
        )
        if existing is None:
            raise CognitiveReadinessError(
                "cognitive_item_not_found",
                f"No cognitive item {item_id!r} for source_ref {source_ref!r}.",
                http_status=404,
            )
        if existing.status != CognitiveItemStatus.SKIPPED.value:
            raise CognitiveReadinessError(
                "cognitive_item_not_skipped",
                (
                    f"cognitive item {item_id!r} is {existing.status!r}, not skipped; "
                    "only a skipped item can be cleared/reopened."
                ),
                http_status=409,
            )
        updated = self._store.update_item(
            board_id=board_id,
            kg_generation_id=gen,
            item_id=item_id,
            new_status=CognitiveItemStatus.PENDING.value,
            updated_by_agent_id=actor,
            outcome_type=None,
            actor=actor,
            clear_readiness_metadata=True,
        )
        if updated is None:
            raise CognitiveReadinessError(
                "cognitive_item_not_found",
                f"No cognitive item {item_id!r} for source_ref {source_ref!r}.",
                http_status=404,
            )
        return updated


__all__ = [
    "CANONICAL_DEBT_OPEN_SIGNAL",
    "CONNECTIVITY_GUARD_SIGNAL",
    "CognitiveReadinessError",
    "CognitiveReadinessService",
    "CognitiveReadinessVerdict",
    "CognitiveReasonCode",
    "GATE_BLOCKING_TIERS",
    "ReadinessTier",
    "REVISIT_REQUIRED_REASON_CODES",
    "SELECTABLE_REASON_CODES",
    "TECHNICAL_DLQ_SIGNAL",
    "TECHNICAL_READINESS_SIGNALS",
    "TERMINAL_REASON_CODES",
    "compose_readiness",
    "validate_skip_reason",
]
