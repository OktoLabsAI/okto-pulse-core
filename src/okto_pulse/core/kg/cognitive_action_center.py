"""S3 / card 38a014f7 — Cognitive Action Center read-model (spec 2731a346).

A READ-ONLY projection (dec_272906c3 / fr_7695a7a7): it never persists state,
creates a store/table/queue/ledger, or invents a taxonomy. It AGGREGATES the
existing sources for a board —

  * ``CognitiveConsolidationItemStore`` items (cognitive signals),
  * ``CanonicalDebt`` rows (OPEN_STATES = blocking; terminal/committed =
    informational ``terminal_history`` — dec_7a96b05e / br_670b4d10),
  * ``ConsolidationDeadLetter`` rows (technical DLQ),

reconciles them by the S1.1 normalized ``artifact_id`` (so ``card:<uuid>`` and
``bug:<uuid>`` collapse), and for each artifact obtains the readiness verdict
from ``CognitiveReadinessService`` — the SINGLE precedence
(tr_6bfe98e7 / tr_0aab9bea). The Action Center NEVER decides readiness locally:
``readiness_effect`` / ``blocking`` / ``precedence_explanation`` are surfaced
verbatim from the service (br_ee939fc7).

A cognitive ``reason_code`` is kept distinct from a technical
``error_cause`` / readiness signal: ``technical_dlq`` / ``canonical_debt_open`` /
``connectivity_guard`` are surfaced as ``error_cause``, NEVER as a selectable
``reason_code``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from okto_pulse.core.kg.cognitive_readiness import (
    CANONICAL_DEBT_OPEN_SIGNAL,
    REVISIT_REQUIRED_REASON_CODES,
    SELECTABLE_REASON_CODES,
    TECHNICAL_DLQ_SIGNAL,
    CognitiveReadinessError,
    CognitiveReadinessService,
    CognitiveReadinessVerdict,
    ReadinessTier,
)
from okto_pulse.core.kg.rebuild_audit import (
    ACTIVE_ITEM_STATUSES,
    CognitiveItemStatus,
    normalize_cognitive_artifact_id,
)
from okto_pulse.core.ports.kg_operational import get_kg_operational_read_model_port
from okto_pulse.core.services.canonical_debt_service import OPEN_STATES


# Per-row signal classification (the filter vocabulary, fr_cc1c2e86).
SIGNAL_ALL = "all"
SIGNAL_COGNITIVE_PENDING = "cognitive_pending"
SIGNAL_SKIPPED = "skipped"
SIGNAL_REVISIT_REQUIRED = "revisit_required"
SIGNAL_OPEN_CANONICAL_DEBT = "open_canonical_debt"
SIGNAL_TERMINAL_HISTORY = "terminal_history"
SIGNAL_DLQ = "dlq"

VALID_SIGNAL_FILTERS: frozenset[str] = frozenset({
    SIGNAL_ALL,
    SIGNAL_COGNITIVE_PENDING,
    SIGNAL_SKIPPED,
    SIGNAL_REVISIT_REQUIRED,
    SIGNAL_OPEN_CANONICAL_DEBT,
    SIGNAL_TERMINAL_HISTORY,
    SIGNAL_DLQ,
})

# signal_source — WHERE the row came from (distinct from the per-row signal type).
SOURCE_COGNITIVE_ITEM = "cognitive_item"
SOURCE_CANONICAL_DEBT = "canonical_debt"
SOURCE_DLQ = "dlq"

# The single precedence the backend applies (highest first) — surfaced so the
# UI/agent renders WHY without recomputing it (br_ee939fc7).
PRECEDENCE_ORDER: tuple[str, ...] = (
    ReadinessTier.TECHNICAL_DLQ.value,
    ReadinessTier.CANONICAL_DEBT_OPEN.value,
    ReadinessTier.COGNITIVE_ACTIVE.value,
    ReadinessTier.SKIP_EXPIRED.value,
    ReadinessTier.SKIP_VALID.value,
    ReadinessTier.TERMINAL_HISTORY.value,
)

_MAX_LIMIT = 200


@dataclass(frozen=True, slots=True)
class ActionCenterSignal:
    """One readiness signal row (read-only projection)."""

    artifact_id: str
    source_ref_original: str
    aliases: tuple[str, ...]
    artifact_type: str
    signal: str
    signal_source: str
    status: str | None
    outcome_type: str | None
    reason_code: str | None        # cognitive only
    error_cause: str | None        # technical only (dlq / canonical_debt_open)
    revisit_at: str | None
    readiness_effect: str
    blocking: bool
    precedence_explanation: dict[str, Any] = field(default_factory=dict)

    def to_api(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "source_ref_original": self.source_ref_original,
            "aliases": list(self.aliases),
            "artifact_type": self.artifact_type,
            "signal": self.signal,
            "signal_source": self.signal_source,
            "status": self.status,
            "outcome_type": self.outcome_type,
            "reason_code": self.reason_code,
            "error_cause": self.error_cause,
            "revisit_at": self.revisit_at,
            "readiness_effect": self.readiness_effect,
            "blocking": self.blocking,
            "precedence_explanation": dict(self.precedence_explanation),
        }


@dataclass(frozen=True, slots=True)
class _RawSignal:
    """Pre-verdict signal gathered from a source (verdict filled in later)."""

    artifact_id: str
    source_ref_original: str
    artifact_type: str
    signal: str
    signal_source: str
    status: str | None
    outcome_type: str | None
    reason_code: str | None
    error_cause: str | None
    revisit_at: str | None
    recorded_at: str | None = None


def _cognitive_item_signal(item: Any) -> str:
    status = str(item.status or "")
    if status in ACTIVE_ITEM_STATUSES:
        return SIGNAL_COGNITIVE_PENDING
    if status == CognitiveItemStatus.SKIPPED.value:
        if str(item.reason_code or "") in REVISIT_REQUIRED_REASON_CODES:
            return SIGNAL_REVISIT_REQUIRED
        return SIGNAL_SKIPPED
    return SIGNAL_TERMINAL_HISTORY  # consolidated / other terminal


class CognitiveActionCenterReadModel:
    """Aggregates board readiness signals; defers readiness to the service."""

    def __init__(self, service: CognitiveReadinessService) -> None:
        self._service = service

    async def list_signals(
        self,
        context: Any,
        *,
        board_id: str,
        signal: str = SIGNAL_ALL,
        artifact_id: str | None = None,
        source_ref: str | None = None,
        reason_code: str | None = None,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
        kg_generation_id: str | None = None,
    ) -> dict[str, Any]:
        """Return ``{items, summary, precedence}`` (api_8ee8f41b)."""

        signal = (signal or SIGNAL_ALL).lower()
        if signal not in VALID_SIGNAL_FILTERS:
            raise CognitiveReadinessError(
                "invalid_filter",
                f"signal must be one of {sorted(VALID_SIGNAL_FILTERS)}",
                http_status=400,
            )
        bounded_limit = max(1, min(int(limit or 50), _MAX_LIMIT))
        bounded_offset = max(0, int(offset or 0))

        try:
            raw = await self._gather(context, board_id, kg_generation_id)
        except Exception as exc:  # source read failure → 503
            raise CognitiveReadinessError(
                "readiness_source_unavailable",
                f"a readiness source could not be read ({type(exc).__name__})",
                http_status=503,
            ) from exc

        # Aliases per artifact_id — every distinct source_ref_original seen.
        aliases_by_artifact: dict[str, list[str]] = {}
        for r in raw:
            bucket = aliases_by_artifact.setdefault(r.artifact_id, [])
            if r.source_ref_original not in bucket:
                bucket.append(r.source_ref_original)

        # Summary over the FULL (unfiltered) set; filters only narrow ``items``.
        summary = self._summarize(raw)

        filtered = self._apply_filters(
            raw, signal=signal, artifact_id=artifact_id, source_ref=source_ref,
            reason_code=reason_code, status=status, search=search,
        )
        total = len(filtered)
        page = filtered[bounded_offset:bounded_offset + bounded_limit]

        # Readiness verdict per artifact — from the SERVICE, cached per request.
        verdict_cache: dict[str, Any] = {}
        items: list[dict[str, Any]] = []
        for r in page:
            verdict = verdict_cache.get(r.artifact_id)
            if verdict is None:
                # Mirror the gate convention (services/main.py): task/test carry
                # no reusable cognition → advisory; the verdict is still composed
                # by the service, never recomputed here.
                primary_type = (
                    r.artifact_type or r.source_ref_original.split(":", 1)[0]
                ).lower()
                verdict = await self._service.evaluate_artifact(
                    context, board_id=board_id, source_ref=r.source_ref_original,
                    kg_generation_id=kg_generation_id,
                    has_reusable_cognition=primary_type not in ("task", "test"),
                )
                verdict_cache[r.artifact_id] = verdict
            items.append(ActionCenterSignal(
                artifact_id=r.artifact_id,
                source_ref_original=r.source_ref_original,
                aliases=tuple(aliases_by_artifact.get(r.artifact_id, [])),
                artifact_type=r.artifact_type,
                signal=r.signal,
                signal_source=r.signal_source,
                status=r.status,
                outcome_type=r.outcome_type,
                reason_code=r.reason_code,
                error_cause=r.error_cause,
                revisit_at=r.revisit_at,
                readiness_effect=verdict.readiness_effect,
                blocking=verdict.blocking,
                precedence_explanation=verdict.precedence_explanation,
            ).to_api())

        summary["total"] = total
        summary["limit"] = bounded_limit
        summary["offset"] = bounded_offset
        return {"items": items, "summary": summary, "precedence": list(PRECEDENCE_ORDER)}

    async def _gather(
        self, context: Any, board_id: str, kg_generation_id: str | None
    ) -> list[_RawSignal]:
        raw: list[_RawSignal] = []

        gen = kg_generation_id or self._service._store.latest_generation(board_id)
        if gen:
            for item in self._service._store.list_items(board_id, gen):
                raw.append(_RawSignal(
                    artifact_id=item.artifact_id,
                    source_ref_original=item.source_ref_original or item.source_ref,
                    artifact_type=item.artifact_type,
                    signal=_cognitive_item_signal(item),
                    signal_source=SOURCE_COGNITIVE_ITEM,
                    status=item.status,
                    outcome_type=item.outcome_type,
                    reason_code=item.reason_code,
                    error_cause=None,
                    revisit_at=item.revisit_at,
                    recorded_at=getattr(item, "recorded_at", None),
                ))

        port = get_kg_operational_read_model_port()
        debt_rows = await port.list_canonical_debt_signals(
            context,
            board_id=board_id,
        )
        for row in debt_rows:
            ref = row.source_ref or f"{row.artifact_type}:{row.artifact_id}"
            is_open = str(row.canonical_state or "") in OPEN_STATES
            raw.append(_RawSignal(
                artifact_id=normalize_cognitive_artifact_id(ref),
                source_ref_original=ref,
                artifact_type=str(row.artifact_type or ""),
                signal=SIGNAL_OPEN_CANONICAL_DEBT if is_open else SIGNAL_TERMINAL_HISTORY,
                signal_source=SOURCE_CANONICAL_DEBT,
                status=str(row.canonical_state or ""),
                outcome_type=None,
                reason_code=None,
                error_cause=CANONICAL_DEBT_OPEN_SIGNAL if is_open else None,
                revisit_at=None,
            ))

        dlq_rows = await port.list_dead_letter_signals(
            context,
            board_id=board_id,
        )
        for row in dlq_rows:
            ref = f"{row.artifact_type}:{row.artifact_id}"
            raw.append(_RawSignal(
                artifact_id=normalize_cognitive_artifact_id(ref),
                source_ref_original=ref,
                artifact_type=str(row.artifact_type or ""),
                signal=SIGNAL_DLQ,
                signal_source=SOURCE_DLQ,
                status="dead_lettered",
                outcome_type=None,
                reason_code=None,
                error_cause=TECHNICAL_DLQ_SIGNAL,
                revisit_at=None,
            ))
        return raw

    @staticmethod
    def _apply_filters(
        raw: Sequence[_RawSignal],
        *,
        signal: str,
        artifact_id: str | None,
        source_ref: str | None,
        reason_code: str | None,
        status: str | None,
        search: str | None,
    ) -> list[_RawSignal]:
        out = list(raw)
        if signal != SIGNAL_ALL:
            out = [r for r in out if r.signal == signal]
        if artifact_id:
            target = normalize_cognitive_artifact_id(artifact_id)
            out = [r for r in out if r.artifact_id == target or r.artifact_id == artifact_id]
        if source_ref:
            out = [r for r in out if r.source_ref_original == source_ref]
        if reason_code:
            out = [r for r in out if (r.reason_code or "") == reason_code]
        if status:
            out = [r for r in out if (r.status or "") == status]
        if search:
            needle = search.lower()
            out = [
                r for r in out
                if needle in r.artifact_id.lower()
                or needle in r.source_ref_original.lower()
                or needle in (r.reason_code or "").lower()
            ]
        return out

    @staticmethod
    def _summarize(raw: Sequence[_RawSignal]) -> dict[str, Any]:
        by_signal: dict[str, int] = {}
        for r in raw:
            by_signal[r.signal] = by_signal.get(r.signal, 0) + 1
        # board-level technical-vs-cognitive distinction (tr_153531c1): a count of
        # technical blocking signals separate from the cognitive-pending count, so
        # a healthy KG with pending cognitive work is visible without coupling each
        # row to get_kg_health.
        technical_blocking = (
            by_signal.get(SIGNAL_DLQ, 0) + by_signal.get(SIGNAL_OPEN_CANONICAL_DEBT, 0)
        )
        cognitive_pending = (
            by_signal.get(SIGNAL_COGNITIVE_PENDING, 0)
            + by_signal.get(SIGNAL_REVISIT_REQUIRED, 0)
        )
        return {
            "by_signal": by_signal,
            "technical_blocking_signals": technical_blocking,
            "cognitive_pending_signals": cognitive_pending,
        }

    async def metrics(
        self,
        context: Any,
        *,
        board_id: str,
        kg_generation_id: str | None = None,
    ) -> dict[str, Any]:
        """Bounded readiness metrics DERIVED from the read-model (fr_9ae21310 /
        or_25486844). Every label is bounded — status / outcome_type / a bounded
        reason_code class / artifact_type / readiness_effect / signal /
        signal_source / age bucket. Free-text (justification, evidence_refs, raw
        long source_ref, error messages) is NEVER a label (br_3a55b609).
        Precedence is read from the central service, never recomputed here."""

        try:
            raw = await self._gather(context, board_id, kg_generation_id)
        except Exception as exc:
            raise CognitiveReadinessError(
                "readiness_source_unavailable",
                f"a readiness source could not be read ({type(exc).__name__})",
                http_status=503,
            ) from exc

        now = self._service._now()
        verdict_cache: dict[str, CognitiveReadinessVerdict] = {}
        by_status: dict[str, int] = {}
        by_outcome: dict[str, int] = {}
        by_reason: dict[str, int] = {}
        by_type: dict[str, int] = {}
        by_effect: dict[str, int] = {}
        by_signal: dict[str, int] = {}
        by_source: dict[str, int] = {}
        by_age: dict[str, int] = {}

        def _bump(bucket: dict[str, int], key: str) -> None:
            bucket[key] = bucket.get(key, 0) + 1

        for r in raw:
            verdict = verdict_cache.get(r.artifact_id)
            if verdict is None:
                primary_type = (
                    r.artifact_type or r.source_ref_original.split(":", 1)[0]
                ).lower()
                verdict = await self._service.evaluate_artifact(
                    context, board_id=board_id, source_ref=r.source_ref_original,
                    kg_generation_id=kg_generation_id,
                    has_reusable_cognition=primary_type not in ("task", "test"),
                )
                verdict_cache[r.artifact_id] = verdict
            _bump(by_status, r.status or "unknown")
            _bump(by_outcome, r.outcome_type or "none")
            _bump(by_reason, _bounded_reason(r.reason_code))
            _bump(by_type, r.artifact_type or "unknown")
            _bump(by_effect, verdict.readiness_effect)
            _bump(by_signal, r.signal)
            _bump(by_source, r.signal_source)
            _bump(by_age, _age_bucket(r.recorded_at, now))

        summary = self._summarize(raw)
        return {
            "board_id": board_id,
            "total": len(raw),
            "by_status": by_status,
            "by_outcome_type": by_outcome,
            "by_reason_code": by_reason,
            "by_artifact_type": by_type,
            "by_readiness_effect": by_effect,
            "by_signal": by_signal,
            "by_signal_source": by_source,
            "by_age_bucket": by_age,
            "technical_blocking_signals": summary["technical_blocking_signals"],
            "cognitive_pending_signals": summary["cognitive_pending_signals"],
            "expired_revisit_skips": by_effect.get("blocking_revisit_lapsed", 0),
            "open_canonical_debt": by_signal.get(SIGNAL_OPEN_CANONICAL_DEBT, 0),
            "technical_dlq": by_signal.get(SIGNAL_DLQ, 0),
            "terminal_history": by_signal.get(SIGNAL_TERMINAL_HISTORY, 0),
        }


def _bounded_reason(reason_code: str | None) -> str:
    """Clamp a reason_code to a BOUNDED metric label: a member of the closed
    cognitive registry, else ``none`` (absent) / ``other`` (unexpected)."""
    if not reason_code:
        return "none"
    return reason_code if reason_code in SELECTABLE_REASON_CODES else "other"


def _age_bucket(recorded_at: str | None, now: datetime) -> str:
    """Coarse, bounded age bucket from an ISO timestamp."""
    if not recorded_at:
        return "unknown"
    try:
        dt = datetime.fromisoformat(str(recorded_at).replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = now - dt
    if age <= timedelta(days=1):
        return "lt_1d"
    if age <= timedelta(days=7):
        return "1d_7d"
    return "gt_7d"


def _skip_classification(reason_code: str | None) -> str:
    return (
        "revisit_required" if reason_code in REVISIT_REQUIRED_REASON_CODES
        else "terminal"
    )


def build_skip_response(
    item: Any,
    verdict: CognitiveReadinessVerdict,
    *,
    actor: str,
    would_block_done: bool,
) -> dict[str, Any]:
    """Single source for the record-skip response shape so REST and the MCP twin
    never diverge (tr_d9f9f65e). justification / evidence_refs are echoed back to
    the actor who submitted them (audit), never used as a metric label."""
    return {
        "item_id": item.item_id,
        "status": item.status,
        "outcome_type": item.outcome_type,
        "reason_code": item.reason_code,
        "justification": getattr(item, "justification", None),
        "evidence_refs": list(getattr(item, "evidence_refs", []) or []),
        "actor": getattr(item, "actor", None) or actor,
        "revisit_at": getattr(item, "revisit_at", None),
        "updated_at": item.updated_at,
        "classification": _skip_classification(item.reason_code),
        "readiness_effect": verdict.readiness_effect,
        "blocking": verdict.blocking,
        "would_block_done": would_block_done,
        "precedence_explanation": dict(verdict.precedence_explanation),
    }


def build_clear_response(
    item: Any,
    verdict: CognitiveReadinessVerdict,
    *,
    actor: str,
    would_block_done: bool,
) -> dict[str, Any]:
    """Single source for the clear/reopen response shape (REST + MCP parity)."""
    return {
        "item_id": item.item_id,
        "status": item.status,
        "reason_code": item.reason_code,
        "revisit_at": getattr(item, "revisit_at", None),
        "actor": getattr(item, "actor", None) or actor,
        "updated_at": item.updated_at,
        "readiness_effect": verdict.readiness_effect,
        "blocking": verdict.blocking,
        "would_block_done": would_block_done,
        "precedence_explanation": dict(verdict.precedence_explanation),
    }


__all__ = [
    "ActionCenterSignal",
    "CognitiveActionCenterReadModel",
    "build_clear_response",
    "build_skip_response",
    "PRECEDENCE_ORDER",
    "SIGNAL_ALL",
    "SIGNAL_COGNITIVE_PENDING",
    "SIGNAL_DLQ",
    "SIGNAL_OPEN_CANONICAL_DEBT",
    "SIGNAL_REVISIT_REQUIRED",
    "SIGNAL_SKIPPED",
    "SIGNAL_TERMINAL_HISTORY",
    "SOURCE_CANONICAL_DEBT",
    "SOURCE_COGNITIVE_ITEM",
    "SOURCE_DLQ",
    "VALID_SIGNAL_FILTERS",
]
