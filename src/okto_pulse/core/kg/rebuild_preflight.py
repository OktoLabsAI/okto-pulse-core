"""KG rebuild preflight (KG-02, FR2, contract api_5216837e).

The preflight is the READ-ONLY entry point for the recovery/rebuild
flow. Operator-driven UI calls this first, sees the impact summary,
then explicitly confirms via the confirm/run path. Until that
confirmation, NOTHING mutates: no lock is acquired, no generation is
created, no graph files are moved, no cognitive pending is enqueued.

This module is intentionally narrow:

* ``RebuildPreflightService.run(board_id) -> RebuildPreflightResult``
* Reads the spec source set (count + skipped-cancelled count + non-
  deterministic flag) without persisting any manifest — manifest
  construction is KG-02.2.
* Reads the current ``kg_generation_id`` from the generation repo
  (when available) — also read-only.
* Consults ``KGHealthStateClassifier`` (KG-01.1) for the base health
  state. If KG-01 reports ``recovery_needed`` or ``quarantined``,
  preflight surfaces ``required`` action; ``at_risk`` surfaces
  ``recommended``; ``healthy`` surfaces ``not_required``.
* Computes a deterministic ``preflight_hash`` over (board_id, source
  count, skipped count, current generation id, base health state).
  KG-02.2 binds this hash into the immutable manifest so the confirm
  step can detect drift.

TR13 is the load-bearing invariant: the service refuses to call any
mutation API. Counter ``kg_rebuild_preflight_total`` (OR or_4494881d)
records the outcome bucket — ``ready`` / ``confirmation_required`` /
``blocked`` — with safe labels only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger("okto_pulse.kg.rebuild_preflight")


class PreflightOutcome(str, Enum):
    """Bounded outcome enum for OR or_4494881d counter labels."""

    READY = "ready"
    CONFIRMATION_REQUIRED = "confirmation_required"
    BLOCKED = "blocked"


class PreflightBlockReason(str, Enum):
    """Bounded reason enum — never expose raw values."""

    HEALTH_RECOVERY_NEEDED = "health_recovery_needed"
    HEALTH_QUARANTINED = "health_quarantined"
    HEALTH_BACKPRESSURE_WITHOUT_ADMIN = "health_backpressure_without_admin"
    NON_DETERMINISTIC_SOURCES = "non_deterministic_sources"
    SOURCE_STORE_UNAVAILABLE = "source_store_unavailable"


@dataclass(frozen=True, slots=True)
class RebuildSourceSummary:
    """Read-only summary of the spec source set for the board.

    Produced by ``SourceEnumeratorProbe.summary`` — the production
    implementation lives in KG-02.2. KG-02.1 only consumes the summary
    surface so preflight stays read-only.
    """

    eligible_count: int
    skipped_cancelled_count: int
    has_non_deterministic_inputs: bool
    canonical_source_count: int = 0
    working_source_count: int = 0
    skipped_by_maturity_count: int = 0
    skipped_expired_working_count: int = 0
    legacy_unknown_count: int = 0
    layer_counts: dict[str, int] = field(default_factory=dict)
    source_partition_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "eligible_count": self.eligible_count,
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "canonical_source_count": (
                self.canonical_source_count or self.eligible_count
            ),
            "working_source_count": self.working_source_count,
            "skipped_by_maturity_count": self.skipped_by_maturity_count,
            "skipped_expired_working_count": self.skipped_expired_working_count,
            "legacy_unknown_count": self.legacy_unknown_count,
            "layer_counts": dict(self.layer_counts or {}),
            "source_partition_counts": dict(self.source_partition_counts or {}),
        }


@dataclass(frozen=True, slots=True)
class RebuildHealthSummary:
    """Read-only KG-01 health snapshot used by preflight.

    Mirrors the relevant subset of KG-01.1 ``HealthClassification`` so
    preflight does not need to re-classify. ``base_state`` is one of
    the KG-01 canonical 5 states; ``metric_status`` is the REST-shaped
    available|unavailable (collapsed from KG-01.1 partial).
    """

    base_state: str
    metric_status: str
    current_kg_generation_id: str | None


@dataclass(frozen=True, slots=True)
class RebuildPreflightResult:
    """Frozen result returned to the UI/API layer.

    Contract-shaped enough to feed the REST response without further
    transformation, but with two additive operational fields
    (``rebuild_status`` and ``operational_substatus``) that the
    composed KG Health surface reads to render TR17 sub-status.
    """

    board_id: str
    outcome: str  # PreflightOutcome value
    action_required: str  # "not_required" | "recommended" | "required"
    reason: str | None
    base_state: str
    metric_status: str
    current_kg_generation_id: str | None
    eligible_source_count: int
    skipped_cancelled_count: int
    has_non_deterministic_inputs: bool
    canonical_source_count: int
    working_source_count: int
    skipped_by_maturity_count: int
    skipped_expired_working_count: int
    legacy_unknown_count: int
    layer_counts: dict[str, int]
    source_partition_counts: dict[str, int]
    preflight_hash: str
    generated_at: str
    rebuild_status: str = "idle"  # idle|in_progress|completed|rebuild_failed
    operational_substatus: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "outcome": self.outcome,
            "action_required": self.action_required,
            "reason": self.reason,
            "base_state": self.base_state,
            "metric_status": self.metric_status,
            "current_kg_generation_id": self.current_kg_generation_id,
            "eligible_source_count": self.eligible_source_count,
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "canonical_source_count": self.canonical_source_count,
            "working_source_count": self.working_source_count,
            "skipped_by_maturity_count": self.skipped_by_maturity_count,
            "skipped_expired_working_count": self.skipped_expired_working_count,
            "legacy_unknown_count": self.legacy_unknown_count,
            "layer_counts": self.layer_counts,
            "source_partition_counts": self.source_partition_counts,
            "preflight_hash": self.preflight_hash,
            "generated_at": self.generated_at,
            "rebuild_status": self.rebuild_status,
            "operational_substatus": self.operational_substatus,
        }


# --- Counter OR or_4494881d ---------------------------------------------------

_PREFLIGHT_LABELS = ("board_id", "outcome", "reason")

_PreflightCounterKey = tuple[str, str, str]
_preflight_counter: dict[_PreflightCounterKey, int] = {}
_preflight_counter_lock = threading.Lock()


def _bump_preflight(*, board_id: str, outcome: str, reason: str) -> None:
    key: _PreflightCounterKey = (board_id, outcome, reason)
    with _preflight_counter_lock:
        _preflight_counter[key] = _preflight_counter.get(key, 0) + 1


def get_preflight_count(
    board_id: str, outcome: str, *, reason: str | None = None
) -> int:
    with _preflight_counter_lock:
        total = 0
        for (b, out, rsn), value in _preflight_counter.items():
            if b != board_id or out != outcome:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_preflight_samples() -> list[dict[str, Any]]:
    with _preflight_counter_lock:
        return [
            {"board_id": b, "outcome": out, "reason": rsn, "count": value}
            for (b, out, rsn), value in _preflight_counter.items()
        ]


def get_preflight_counter_labels() -> tuple[str, ...]:
    return _PREFLIGHT_LABELS


def reset_preflight_counter() -> None:
    with _preflight_counter_lock:
        _preflight_counter.clear()


# --- Adapter callables --------------------------------------------------------

# SourceEnumeratorProbe.summary(board_id) -> RebuildSourceSummary
SourceProbe = Callable[[str], RebuildSourceSummary]
# HealthProbe(board_id) -> RebuildHealthSummary
HealthProbe = Callable[[str], RebuildHealthSummary]
# RebuildStatusProbe(board_id) -> tuple[str, str] = (rebuild_status, operational_substatus)
RebuildStatusProbe = Callable[[str], tuple[str, str]]


def _default_rebuild_status(board_id: str) -> tuple[str, str]:
    """KG-02.4 wires the real KGGenerationRepository. Until then,
    preflight reports an idle status — runs that never started, never
    failed."""
    return ("idle", "")


def _compose_preflight_hash(
    board_id: str,
    source: RebuildSourceSummary,
    health: RebuildHealthSummary,
) -> str:
    """Deterministic hash over the preflight inputs.

    TR15-aligned: excludes timestamps and any random ids. The same
    (board_id, source counts, generation, base_state) MUST produce
    the same hash so the confirmation step can detect that the
    operator's view drifted between preflight and confirm.
    """
    payload = json.dumps(
        {
            "board_id": board_id,
            "eligible_source_count": source.eligible_count,
            "skipped_cancelled_count": source.skipped_cancelled_count,
            "has_non_deterministic_inputs": source.has_non_deterministic_inputs,
            "canonical_source_count": source.to_dict()["canonical_source_count"],
            "working_source_count": source.working_source_count,
            "skipped_by_maturity_count": source.skipped_by_maturity_count,
            "skipped_expired_working_count": source.skipped_expired_working_count,
            "legacy_unknown_count": source.legacy_unknown_count,
            "layer_counts": source.layer_counts,
            "source_partition_counts": source.source_partition_counts,
            "current_kg_generation_id": health.current_kg_generation_id,
            "base_state": health.base_state,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --- Service ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RebuildPreflightService:
    """Read-only preflight (TR13).

    Constructed with adapter callables so unit tests pass fakes and
    production wires the real KG-01 health + KG-02.2 source enumerator.
    """

    source_probe: SourceProbe
    health_probe: HealthProbe
    rebuild_status_probe: RebuildStatusProbe = _default_rebuild_status

    def run(self, *, board_id: str) -> RebuildPreflightResult:
        """Read-only preflight. Never mutates."""
        if not board_id:
            raise ValueError("board_id is required")

        # 1. Read base health from KG-01.
        health = self.health_probe(board_id)

        # 2. Read source summary from KG-02.2.
        try:
            source = self.source_probe(board_id)
        except Exception as exc:
            reason = PreflightBlockReason.SOURCE_STORE_UNAVAILABLE.value
            _bump_preflight(
                board_id=board_id,
                outcome=PreflightOutcome.BLOCKED.value,
                reason=reason,
            )
            logger.warning(
                "kg.rebuild_preflight.source_store_unavailable board=%s err=%s",
                board_id, exc,
                extra={
                    "event": "kg.rebuild_preflight.source_store_unavailable",
                    "board_id": board_id,
                },
            )
            empty_source = RebuildSourceSummary(0, 0, False)
            return RebuildPreflightResult(
                board_id=board_id,
                outcome=PreflightOutcome.BLOCKED.value,
                action_required="required",
                reason=reason,
                base_state=health.base_state,
                metric_status=health.metric_status,
                current_kg_generation_id=health.current_kg_generation_id,
                eligible_source_count=0,
                skipped_cancelled_count=0,
                has_non_deterministic_inputs=False,
                canonical_source_count=0,
                working_source_count=0,
                skipped_by_maturity_count=0,
                skipped_expired_working_count=0,
                legacy_unknown_count=0,
                layer_counts={},
                source_partition_counts={},
                preflight_hash=_compose_preflight_hash(
                    board_id, empty_source, health
                ),
                generated_at=datetime.now(timezone.utc).isoformat(),
                rebuild_status=self.rebuild_status_probe(board_id)[0],
                operational_substatus=self.rebuild_status_probe(board_id)[1],
            )

        rebuild_status, operational_substatus = self.rebuild_status_probe(board_id)
        preflight_hash = _compose_preflight_hash(board_id, source, health)

        # 3. Apply decision rules.
        outcome, action_required, reason = self._classify(health, source)
        _bump_preflight(
            board_id=board_id,
            outcome=outcome,
            reason=reason or "n/a",
        )
        logger.info(
            "kg.rebuild_preflight.evaluated board=%s outcome=%s action=%s "
            "reason=%s preflight_hash=%s",
            board_id, outcome, action_required, reason, preflight_hash[:12],
            extra={
                "event": "kg.rebuild_preflight.evaluated",
                "board_id": board_id,
                "outcome": outcome,
                "action_required": action_required,
                "reason": reason,
            },
        )
        return RebuildPreflightResult(
            board_id=board_id,
            outcome=outcome,
            action_required=action_required,
            reason=reason,
            base_state=health.base_state,
            metric_status=health.metric_status,
            current_kg_generation_id=health.current_kg_generation_id,
            eligible_source_count=source.eligible_count,
            skipped_cancelled_count=source.skipped_cancelled_count,
            has_non_deterministic_inputs=source.has_non_deterministic_inputs,
            canonical_source_count=source.to_dict()["canonical_source_count"],
            working_source_count=source.working_source_count,
            skipped_by_maturity_count=source.skipped_by_maturity_count,
            skipped_expired_working_count=source.skipped_expired_working_count,
            legacy_unknown_count=source.legacy_unknown_count,
            layer_counts=dict(source.layer_counts or {}),
            source_partition_counts=dict(source.source_partition_counts or {}),
            preflight_hash=preflight_hash,
            generated_at=datetime.now(timezone.utc).isoformat(),
            rebuild_status=rebuild_status,
            operational_substatus=operational_substatus,
        )

    # --- internals ---------------------------------------------------------

    def _classify(
        self,
        health: RebuildHealthSummary,
        source: RebuildSourceSummary,
    ) -> tuple[str, str, str | None]:
        # quarantined → required + blocked (operator must run quarantine
        # recovery before rebuild — TR2).
        if health.base_state == "quarantined":
            return (
                PreflightOutcome.BLOCKED.value,
                "required",
                PreflightBlockReason.HEALTH_QUARANTINED.value,
            )
        if health.base_state == "recovery_needed":
            return (
                PreflightOutcome.CONFIRMATION_REQUIRED.value,
                "required",
                PreflightBlockReason.HEALTH_RECOVERY_NEEDED.value,
            )
        # TR6: non-deterministic inputs require operator confirmation.
        if source.has_non_deterministic_inputs:
            return (
                PreflightOutcome.CONFIRMATION_REQUIRED.value,
                "recommended",
                PreflightBlockReason.NON_DETERMINISTIC_SOURCES.value,
            )
        # backpressure without an admin lane = blocked. Admin lane
        # determination is the rebuild service's job (KG-02.3); from
        # preflight's read-only viewpoint the safer default is to
        # require the operator to confirm.
        if health.base_state == "backpressure":
            return (
                PreflightOutcome.CONFIRMATION_REQUIRED.value,
                "recommended",
                PreflightBlockReason.HEALTH_BACKPRESSURE_WITHOUT_ADMIN.value,
            )
        if health.base_state == "at_risk":
            return (
                PreflightOutcome.READY.value,
                "recommended",
                None,
            )
        # healthy and metric_status==available: rebuild not needed.
        return (
            PreflightOutcome.READY.value,
            "not_required",
            None,
        )


__all__ = [
    "HealthProbe",
    "PreflightBlockReason",
    "PreflightOutcome",
    "RebuildHealthSummary",
    "RebuildPreflightResult",
    "RebuildPreflightService",
    "RebuildSourceSummary",
    "RebuildStatusProbe",
    "SourceProbe",
    "get_preflight_count",
    "get_preflight_counter_labels",
    "get_preflight_samples",
    "reset_preflight_counter",
]
