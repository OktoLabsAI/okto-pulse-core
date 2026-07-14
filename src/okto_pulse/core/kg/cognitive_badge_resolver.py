"""Cognitive consolidation entity badge resolver (KG-03.6 + KG-03A.2).

Implements ``CognitivePendingEntityBadgeResolver`` (api_49fce0e1) and the
bounded counter for ``kg_cognitive_badge_resolve_total``
(or_41797e76 + or_90fa2709).

The resolver returns, per ``source_ref`` in the request:

    {
        "show_badge": <bool>,
        "status": <"pending"|"in_progress"|"failed"|"consolidated"|"skipped"|None>,
        "reason": <CognitiveBadgeReason enum>,
        "label": "Pending cognitive consolidation" | "",
        "item_id": <opt>,
        "updated_at": <opt>,
    }

Eligibility rule (KG-03A.2 — extends KG-03.6):
    spec, refinement, task, test, bug, decision -> ELIGIBLE for the local badge
    ideation, other                              -> INELIGIBLE

Rationale: decision is a first-class semantic source (KG-03A spec —
br_6c9efd64 + dec_fee09d11). Active formal decisions emit
``decision:<spec_id>:<decision_id>`` source rows from BoardSourceReader and
participate in the cognitive pending workflow; ideations remain outside
this surface (dec_0d0f2d9c).

Severity order for active items per source_ref (most severe wins):

    failed > in_progress > pending

Terminal statuses (br_50cc5700): ``consolidated`` and ``skipped`` HIDE
the badge — show_badge=false with reason=terminal_status.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
)
from okto_pulse.core.observability.sample_buffer import runtime_counter_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock


# ---------------------------------------------------------------------------
# Bounded enums
# ---------------------------------------------------------------------------


class EntityCardType(str, Enum):
    """Bounded card entity types. The resolver rejects anything outside
    this set as ``other`` (which is then ineligible)."""

    SPEC = "spec"
    REFINEMENT = "refinement"
    TASK = "task"
    TEST = "test"
    BUG = "bug"
    IDEATION = "ideation"
    DECISION = "decision"
    OTHER = "other"


ELIGIBLE_ENTITY_TYPES: frozenset[str] = frozenset(
    {
        EntityCardType.SPEC.value,
        EntityCardType.DECISION.value,
        EntityCardType.REFINEMENT.value,
        EntityCardType.TASK.value,
        EntityCardType.TEST.value,
        EntityCardType.BUG.value,
    }
)


INELIGIBLE_ENTITY_TYPES: frozenset[str] = frozenset(
    {
        EntityCardType.IDEATION.value,
        EntityCardType.OTHER.value,
    }
)


class CognitiveBadgeReason(str, Enum):
    """Bounded reason for the badge resolution. Reason names the WHY,
    never the raw source data."""

    ACTIVE_COGNITIVE_ITEM = "active_cognitive_item"
    TERMINAL_STATUS = "terminal_status"
    INELIGIBLE_ENTITY_TYPE = "ineligible_entity_type"
    NOT_FOUND = "not_found"


# Severity order for active statuses (most severe first). When a source
# has multiple active items, the most severe one drives the badge.
_ACTIVE_SEVERITY_ORDER: tuple[str, ...] = (
    CognitiveItemStatus.FAILED.value,
    CognitiveItemStatus.IN_PROGRESS.value,
    CognitiveItemStatus.PENDING.value,
)

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        CognitiveItemStatus.CONSOLIDATED.value,
        CognitiveItemStatus.SKIPPED.value,
    }
)


BADGE_LABEL_ACTIVE = "Pending cognitive consolidation"


# ---------------------------------------------------------------------------
# Resolver result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CognitiveBadgeView:
    """Frozen badge state returned per source_ref."""

    show_badge: bool
    status: str | None
    reason: str  # CognitiveBadgeReason value
    item_id: str | None = None
    updated_at: str | None = None
    label: str = ""

    def to_api(self) -> dict[str, Any]:
        return {
            "show_badge": self.show_badge,
            "label": self.label,
            "status": self.status,
            "item_id": self.item_id,
            "updated_at": self.updated_at,
            "reason": self.reason,
        }


# ---------------------------------------------------------------------------
# Counter — kg_cognitive_badge_resolve_total (or_41797e76 + or_90fa2709)
# ---------------------------------------------------------------------------
#
# Per Codex precedent, labels are exactly bounded:
#   - board_id_hash:        sha256(board_id)[:16] hex — no PII leak.
#   - outcome:              success | invalid_source_refs | unavailable.
#   - requested_count_bucket: 0 | 1-10 | 11-50 | 51-100 | 101-200.
#   - eligible_entity_type: one of EntityCardType values OR "n/a".
#   - reason:               one of CognitiveBadgeReason values OR "n/a".
#
# FORBIDDEN as labels: source_ref, item_id, raw artifact_type outside the
# bounded enum, board_id raw, agent.


class CognitiveBadgeResolveOutcome(str, Enum):
    SUCCESS = "success"
    INVALID_SOURCE_REFS = "invalid_source_refs"
    UNAVAILABLE = "unavailable"


_BADGE_RESOLVE_LABELS = (
    "board_id_hash",
    "outcome",
    "requested_count_bucket",
    "eligible_entity_type",
    "reason",
)

_RESOLVE_SAMPLES = runtime_counter_sample_buffer(
    "kg.cognitive_badge_resolver",
    _BADGE_RESOLVE_LABELS,
)
_RESOLVE_SAMPLES_LOCK = runtime_lock("kg.cognitive_badge_resolver.samples")


def _board_id_hash(board_id: str) -> str:
    """Bounded board_id projection — sha256[:16] hex."""

    return hashlib.sha256(board_id.encode("utf-8")).hexdigest()[:16]


def _count_bucket(count: int) -> str:
    """Map ``requested_count`` to a bounded bucket label."""

    if count <= 0:
        return "0"
    if count <= 10:
        return "1-10"
    if count <= 50:
        return "11-50"
    if count <= 100:
        return "51-100"
    if count <= 200:
        return "101-200"
    return "200+"  # defensive — request validation caps at 200.


def _emit_resolve_sample(
    *,
    board_id_hash: str,
    outcome: str,
    requested_count_bucket: str,
    eligible_entity_type: str,
    reason: str,
) -> None:
    with _RESOLVE_SAMPLES_LOCK:
        _RESOLVE_SAMPLES.append(
            {
                "board_id_hash": board_id_hash,
                "outcome": outcome,
                "requested_count_bucket": requested_count_bucket,
                "eligible_entity_type": eligible_entity_type,
                "reason": reason,
            }
        )


def get_badge_resolve_counter_labels() -> tuple[str, ...]:
    return _BADGE_RESOLVE_LABELS


def get_badge_resolve_samples() -> list[dict[str, Any]]:
    with _RESOLVE_SAMPLES_LOCK:
        return _RESOLVE_SAMPLES.snapshot()


def get_badge_resolve_event_count(
    *,
    board_id_hash: str | None = None,
    outcome: str | None = None,
    requested_count_bucket: str | None = None,
    eligible_entity_type: str | None = None,
    reason: str | None = None,
) -> int:
    with _RESOLVE_SAMPLES_LOCK:
        return _RESOLVE_SAMPLES.count(
            board_id_hash=board_id_hash,
            outcome=outcome,
            requested_count_bucket=requested_count_bucket,
            eligible_entity_type=eligible_entity_type,
            reason=reason,
        )


def reset_badge_resolve_counter() -> None:
    with _RESOLVE_SAMPLES_LOCK:
        _RESOLVE_SAMPLES.clear()


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


def _normalize_entity_type(value: Any) -> str:
    """Map any input to a bounded entity-type string. Unknown values
    collapse to ``other`` so we never produce a metric label outside the
    bounded enum."""

    if isinstance(value, str) and value in {
        e.value for e in EntityCardType
    }:
        return value
    return EntityCardType.OTHER.value


def _pick_active_severity(items: Sequence[CognitiveConsolidationItem]) -> CognitiveConsolidationItem | None:
    """Return the most-severe ACTIVE item per the severity order, or
    None if no active item exists."""

    by_status: dict[str, CognitiveConsolidationItem] = {}
    for item in items:
        if item.status in _ACTIVE_SEVERITY_ORDER and item.status not in by_status:
            by_status[item.status] = item
    for status in _ACTIVE_SEVERITY_ORDER:
        if status in by_status:
            return by_status[status]
    return None


def _pick_terminal_latest(items: Sequence[CognitiveConsolidationItem]) -> CognitiveConsolidationItem | None:
    """Return the most-recently-updated TERMINAL item, or None."""

    terminal = [item for item in items if item.status in _TERMINAL_STATUSES]
    if not terminal:
        return None
    return max(
        terminal,
        key=lambda i: (
            i.updated_at or "",
            i.recorded_at or "",
        ),
    )


def resolve_entity_badges(
    *,
    board_id: str,
    source_refs: Sequence[str],
    card_entity_types: Mapping[str, str],
    kg_generation_id: str | None,
    store: CognitiveConsolidationItemStore,
) -> tuple[dict[str, CognitiveBadgeView], frozenset[str]]:
    """Pure resolver. Returns ``(badges_by_source_ref, eligible_entity_types)``.

    Per Codex audit precedent: this function emits exactly one counter
    sample per ``(eligible_entity_type, reason)`` bucket observed across
    the batch — so a request mixing eligible+ineligible types still
    fires bounded samples but does not flood the time-series with a
    sample per source_ref.

    The caller (REST router) is responsible for the per-batch outcome
    sample (success/invalid/unavailable + requested_count_bucket).
    """

    resolved_generation = (
        kg_generation_id
        if kg_generation_id
        else store.latest_generation(board_id)
    )
    all_items: list[CognitiveConsolidationItem] = (
        store.list_items(board_id, resolved_generation)
        if resolved_generation
        else []
    )
    items_by_source: dict[str, list[CognitiveConsolidationItem]] = {}
    for item in all_items:
        items_by_source.setdefault(item.source_ref, []).append(item)

    board_hash = _board_id_hash(board_id)
    requested_count_bucket = _count_bucket(len(source_refs))
    badges: dict[str, CognitiveBadgeView] = {}
    bucket_keys: set[tuple[str, str]] = set()

    for source_ref in source_refs:
        entity_type = _normalize_entity_type(card_entity_types.get(source_ref))
        if entity_type not in ELIGIBLE_ENTITY_TYPES:
            badge = CognitiveBadgeView(
                show_badge=False,
                status=None,
                reason=CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value,
                label="",
            )
            badges[source_ref] = badge
            bucket_keys.add(
                (
                    entity_type,
                    CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value,
                )
            )
            continue

        candidate_items = items_by_source.get(source_ref, [])
        active_item = _pick_active_severity(candidate_items)
        if active_item is not None:
            badge = CognitiveBadgeView(
                show_badge=True,
                status=active_item.status,
                reason=CognitiveBadgeReason.ACTIVE_COGNITIVE_ITEM.value,
                item_id=active_item.item_id,
                updated_at=active_item.updated_at,
                label=BADGE_LABEL_ACTIVE,
            )
            badges[source_ref] = badge
            bucket_keys.add(
                (
                    entity_type,
                    CognitiveBadgeReason.ACTIVE_COGNITIVE_ITEM.value,
                )
            )
            continue

        terminal_item = _pick_terminal_latest(candidate_items)
        if terminal_item is not None:
            badge = CognitiveBadgeView(
                show_badge=False,
                status=terminal_item.status,
                reason=CognitiveBadgeReason.TERMINAL_STATUS.value,
                item_id=terminal_item.item_id,
                updated_at=terminal_item.updated_at,
                label="",
            )
            badges[source_ref] = badge
            bucket_keys.add(
                (
                    entity_type,
                    CognitiveBadgeReason.TERMINAL_STATUS.value,
                )
            )
            continue

        # No items at all — neutral.
        badges[source_ref] = CognitiveBadgeView(
            show_badge=False,
            status=None,
            reason=CognitiveBadgeReason.NOT_FOUND.value,
            label="",
        )
        bucket_keys.add(
            (entity_type, CognitiveBadgeReason.NOT_FOUND.value)
        )

    # Emit one sample per (eligible_entity_type, reason) bucket observed.
    # This keeps cardinality bounded (at most |EntityCardType| × |reason|
    # entries per request) and still satisfies or_90fa2709's expectation
    # that ineligible_entity_type for refinement never appears.
    for entity_type, reason in bucket_keys:
        _emit_resolve_sample(
            board_id_hash=board_hash,
            outcome=CognitiveBadgeResolveOutcome.SUCCESS.value,
            requested_count_bucket=requested_count_bucket,
            eligible_entity_type=entity_type,
            reason=reason,
        )

    return badges, ELIGIBLE_ENTITY_TYPES


def emit_invalid_batch_sample(
    *, board_id: str, requested_count: int
) -> None:
    """REST adapter helper — emit the failure sample when the batch is
    rejected pre-resolution (e.g. ``invalid_source_refs``)."""

    _emit_resolve_sample(
        board_id_hash=_board_id_hash(board_id),
        outcome=CognitiveBadgeResolveOutcome.INVALID_SOURCE_REFS.value,
        requested_count_bucket=_count_bucket(requested_count),
        eligible_entity_type="n/a",
        reason="n/a",
    )


def emit_unavailable_batch_sample(
    *, board_id: str, requested_count: int
) -> None:
    _emit_resolve_sample(
        board_id_hash=_board_id_hash(board_id),
        outcome=CognitiveBadgeResolveOutcome.UNAVAILABLE.value,
        requested_count_bucket=_count_bucket(requested_count),
        eligible_entity_type="n/a",
        reason="n/a",
    )


__all__ = [
    "BADGE_LABEL_ACTIVE",
    "CognitiveBadgeReason",
    "CognitiveBadgeResolveOutcome",
    "CognitiveBadgeView",
    "ELIGIBLE_ENTITY_TYPES",
    "EntityCardType",
    "INELIGIBLE_ENTITY_TYPES",
    "emit_invalid_batch_sample",
    "emit_unavailable_batch_sample",
    "get_badge_resolve_counter_labels",
    "get_badge_resolve_event_count",
    "get_badge_resolve_samples",
    "reset_badge_resolve_counter",
    "resolve_entity_badges",
]
