"""Shared cognitive closeout gate.

This module is the service-layer policy boundary for lifecycle transitions
to ``done``. It is intentionally read-only: it inspects the existing
``CognitiveConsolidationItemStore`` and returns a decision, but never mutates
the cognitive ledger.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.kg.rebuild_audit import (
    ACTIVE_ITEM_STATUSES,
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
)
from okto_pulse.core.observability.sample_buffer import BoundedCounterSampleBuffer


ELIGIBLE_CLOSEOUT_ENTITY_TYPES: frozenset[str] = frozenset(
    {"spec", "refinement", "task", "test", "bug"}
)


class CognitiveCloseoutReason(str, Enum):
    NO_ACTIVE_COGNITIVE_ITEMS = "no_active_cognitive_items"
    COGNITIVE_CONSOLIDATION_PENDING = "cognitive_consolidation_pending"
    COGNITIVE_STATUS_UNAVAILABLE = "cognitive_status_unavailable"
    BOARD_SKIP_ENABLED = "board_skip_enabled"
    DEGRADED_KG_AUTO_SKIP = "degraded_kg_auto_skip"


class CognitiveCloseoutOutcome(str, Enum):
    ALLOWED = "allowed"
    BLOCKED = "blocked"
    UNAVAILABLE = "unavailable"
    SKIPPED = "skipped"
    INVALID_TARGET_STATUS = "invalid_target_status"


class CognitiveCloseoutGateError(Exception):
    """Raised when callers violate the gate contract."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class CognitiveItemStoreProtocol(Protocol):
    def latest_generation(self, board_id: str) -> str | None: ...

    def list_items(
        self,
        board_id: str,
        kg_generation_id: str,
        *,
        status_filter: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[CognitiveConsolidationItem]: ...


@dataclass(frozen=True, slots=True)
class CognitiveCloseoutBlockingItem:
    item_id: str
    status: str
    source_ref: str

    @property
    def is_decision_child(self) -> bool:
        return self.source_ref.startswith("decision:")

    def to_api(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "status": self.status,
            "source_ref": self.source_ref,
        }


@dataclass(frozen=True, slots=True)
class CognitiveSourceRefs:
    source_refs: tuple[str, ...]
    ignored_sources: tuple[str, ...] = ()

    def to_api(self) -> dict[str, Any]:
        return {
            "source_refs": list(self.source_refs),
            "ignored_sources": list(self.ignored_sources),
        }


@dataclass(frozen=True, slots=True)
class CognitiveCloseoutResult:
    allowed: bool
    reason: str
    outcome: str
    skip_enabled: bool
    blocking_items: tuple[CognitiveCloseoutBlockingItem, ...] = field(
        default_factory=tuple
    )
    source_refs: tuple[str, ...] = field(default_factory=tuple)
    kg_generation_id: str | None = None

    @property
    def blocking_count(self) -> int:
        return len(self.blocking_items)

    @property
    def has_decision_child_block(self) -> bool:
        return any(item.is_decision_child for item in self.blocking_items)

    def to_api(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "outcome": self.outcome,
            "skip_enabled": self.skip_enabled,
            "blocking_count": self.blocking_count,
            "blocking_items": [item.to_api() for item in self.blocking_items],
            "source_refs": list(self.source_refs),
            "kg_generation_id": self.kg_generation_id,
        }


_CLOSEOUT_LABELS = (
    "board_id_hash",
    "entity_id_hash",
    "entity_type",
    "outcome",
    "reason",
    "skip_enabled",
    "blocking_count_bucket",
)
_closeout_samples = BoundedCounterSampleBuffer(_CLOSEOUT_LABELS)
_closeout_samples_lock = threading.Lock()


def _board_id_hash(board_id: str) -> str:
    return hashlib.sha256(board_id.encode("utf-8")).hexdigest()[:16]


def _entity_id_hash(entity_id: str | None) -> str:
    safe_id = "" if entity_id is None else str(entity_id)
    return hashlib.sha256(safe_id.encode("utf-8")).hexdigest()[:16]


def _blocking_count_bucket(count: int) -> str:
    if count <= 0:
        return "0"
    if count <= 1:
        return "1"
    if count <= 5:
        return "2-5"
    if count <= 20:
        return "6-20"
    return "20+"


def _emit_closeout_sample(
    *,
    board_id: str,
    entity_id: str | None,
    entity_type: str,
    outcome: str,
    reason: str,
    skip_enabled: bool,
    blocking_count: int,
) -> None:
    try:
        with _closeout_samples_lock:
            _closeout_samples.append(
                {
                    "board_id_hash": _board_id_hash(board_id),
                    "entity_id_hash": _entity_id_hash(entity_id),
                    "entity_type": entity_type,
                    "outcome": outcome,
                    "reason": reason,
                    "skip_enabled": str(bool(skip_enabled)).lower(),
                    "blocking_count_bucket": _blocking_count_bucket(blocking_count),
                }
            )
    except Exception:
        # Metrics are diagnostic. The closeout decision itself remains
        # authoritative even if an in-process sink or future exporter fails.
        return


def get_closeout_gate_counter_labels() -> tuple[str, ...]:
    return _CLOSEOUT_LABELS


def get_closeout_gate_samples() -> list[dict[str, Any]]:
    with _closeout_samples_lock:
        return _closeout_samples.snapshot()


def get_closeout_gate_event_count(
    *,
    entity_type: str | None = None,
    outcome: str | None = None,
    reason: str | None = None,
    skip_enabled: str | None = None,
) -> int:
    with _closeout_samples_lock:
        return _closeout_samples.count(
            entity_type=entity_type,
            outcome=outcome,
            reason=reason,
            skip_enabled=skip_enabled,
        )


def reset_closeout_gate_samples() -> None:
    with _closeout_samples_lock:
        _closeout_samples.clear()


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _get_attr_or_key(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _enum_or_str(value: Any) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _canonical_payload_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _decision_id(decision: Mapping[str, Any], index: int) -> str:
    explicit = decision.get("id")
    if explicit:
        return str(explicit)
    return f"idx-{index}-{_canonical_payload_hash(decision)[:12]}"


def resolve_cognitive_source_refs(
    *,
    entity_type: str,
    entity: Any,
    entity_id: str | None = None,
) -> CognitiveSourceRefs:
    """Resolve lifecycle entity → cognitive ledger source_refs.

    The spec path includes formal active child decisions. Candidate,
    superseded and revoked decisions are intentionally ignored.
    """

    normalized_type = _normalize_closeout_entity_type(entity_type, entity)
    if normalized_type not in ELIGIBLE_CLOSEOUT_ENTITY_TYPES:
        raise CognitiveCloseoutGateError(
            "unsupported_entity_type",
            f"Entity type {entity_type!r} is not eligible for cognitive closeout",
        )

    resolved_id = entity_id or _get_attr_or_key(entity, "id")
    if not resolved_id:
        raise CognitiveCloseoutGateError(
            "missing_entity_id",
            "Entity is missing an identifier for cognitive source_ref resolution",
        )
    resolved_id = str(resolved_id)

    source_refs: list[str] = [f"{normalized_type}:{resolved_id}"]
    ignored_sources: list[str] = []

    if normalized_type == "spec":
        for index, raw_decision in enumerate(_get_attr_or_key(entity, "decisions", []) or []):
            decision = _as_mapping(raw_decision)
            if not decision:
                ignored_sources.append(f"decision:{resolved_id}:<non-object-{index}>")
                continue
            status = str(decision.get("status", "active") or "active").lower()
            local_id = _decision_id(decision, index)
            source_ref = f"decision:{resolved_id}:{local_id}"
            if status == "active":
                source_refs.append(source_ref)
            else:
                ignored_sources.append(source_ref)

    return CognitiveSourceRefs(
        source_refs=tuple(dict.fromkeys(source_refs)),
        ignored_sources=tuple(ignored_sources),
    )


def _normalize_closeout_entity_type(entity_type: str, entity: Any | None = None) -> str:
    raw = str(entity_type or "").lower()
    if raw == "card":
        card_type = _enum_or_str(_get_attr_or_key(entity, "card_type", "normal")).lower()
        if card_type == "test":
            return "test"
        if card_type == "bug":
            return "bug"
        return "task"
    if raw == "normal":
        return "task"
    return raw


@dataclass(frozen=True, slots=True)
class CognitiveCloseoutGate:
    store: CognitiveItemStoreProtocol

    def evaluate(
        self,
        *,
        board_id: str,
        entity_type: str,
        entity_id: str | None = None,
        entity: Any | None = None,
        target_status: str,
        board_skip_enabled: bool = False,
        source_refs: Sequence[str] | None = None,
        kg_generation_id: str | None = None,
        graph_state: str | None = None,
    ) -> CognitiveCloseoutResult:
        if target_status != "done":
            normalized_type = _normalize_closeout_entity_type(entity_type, entity)
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=CognitiveCloseoutOutcome.INVALID_TARGET_STATUS.value,
                reason=CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
                skip_enabled=board_skip_enabled,
                blocking_count=0,
            )
            raise CognitiveCloseoutGateError(
                "invalid_target_status",
                "Cognitive closeout gate only applies to target_status done",
            )

        normalized_type = _normalize_closeout_entity_type(entity_type, entity)
        if source_refs is None:
            refs = resolve_cognitive_source_refs(
                entity_type=normalized_type,
                entity=entity or {"id": entity_id},
                entity_id=entity_id,
            ).source_refs
        else:
            refs = tuple(dict.fromkeys(str(ref) for ref in source_refs if ref))

        try:
            resolved_generation = kg_generation_id or self.store.latest_generation(board_id)
            items = (
                self.store.list_items(board_id, resolved_generation)
                if resolved_generation
                else []
            )
        except Exception:
            if board_skip_enabled:
                result = CognitiveCloseoutResult(
                    allowed=True,
                    reason=CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value,
                    outcome=CognitiveCloseoutOutcome.SKIPPED.value,
                    skip_enabled=True,
                    source_refs=refs,
                    kg_generation_id=kg_generation_id,
                )
                _emit_closeout_sample(
                    board_id=board_id,
                    entity_id=entity_id or _get_attr_or_key(entity, "id"),
                    entity_type=normalized_type,
                    outcome=result.outcome,
                    reason=CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value,
                    skip_enabled=True,
                    blocking_count=0,
                )
                return result

            result = CognitiveCloseoutResult(
                allowed=False,
                reason=CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value,
                outcome=CognitiveCloseoutOutcome.UNAVAILABLE.value,
                skip_enabled=False,
                source_refs=refs,
                kg_generation_id=kg_generation_id,
            )
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=result.outcome,
                reason=result.reason,
                skip_enabled=False,
                blocking_count=0,
            )
            return result

        # F16 positive liveness check: surface the EXISTING unavailable outcome
        # (instead of silently falling through to ALLOWED) when cognitive status
        # cannot be confirmed for a ``done`` transition. graph_state is a plain
        # input threaded by the async caller (no I/O here — the gate stays
        # sync/pure). Two distinct conditions:
        #   - degraded: graph_state is a hard-reject member (recovery_needed /
        #     quarantined) — the KG is known-broken, status is unreadable.
        #     NC-1 Degraded-KG Auto-Skip: when NOT board_skip_enabled, the gate
        #     auto-skips with allowed=True + outcome=UNAVAILABLE + reason=
        #     DEGRADED_KG_AUTO_SKIP (auditable, not silent). board_skip_enabled
        #     still wins as SKIPPED (short-circuit before this branch).
        #   - unconfirmed: health could NOT be resolved (graph_state is None)
        #     AND there is no generation — the genuine can't-read shape (the
        #     3cf5dede live failure). KEEP fail-closed (allowed=False) per DEC-B.
        #     A CONFIRMED-healthy graph with no generation is NOT unavailable —
        #     it simply has no pending cognitive work, so it stays ALLOWED
        #     (a missing generation alone never blocks).
        degraded = graph_state is not None and graph_state in _RISK_STATE_HARD_REJECT
        unconfirmed = graph_state is None and resolved_generation is None

        if degraded:
            if board_skip_enabled:
                result = CognitiveCloseoutResult(
                    allowed=True,
                    reason=CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value,
                    outcome=CognitiveCloseoutOutcome.SKIPPED.value,
                    skip_enabled=True,
                    source_refs=refs,
                    kg_generation_id=kg_generation_id,
                )
                _emit_closeout_sample(
                    board_id=board_id,
                    entity_id=entity_id or _get_attr_or_key(entity, "id"),
                    entity_type=normalized_type,
                    outcome=result.outcome,
                    reason=CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value,
                    skip_enabled=True,
                    blocking_count=0,
                )
                return result

            # NC-1: degraded KG auto-skip — allowed=True, auditable via telemetry.
            # outcome reuses UNAVAILABLE.value (no new enum member on outcome per F16
            # no-new-enum constraint); reason is the new type-safe DEGRADED_KG_AUTO_SKIP.
            result = CognitiveCloseoutResult(
                allowed=True,
                reason=CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value,
                outcome=CognitiveCloseoutOutcome.UNAVAILABLE.value,
                skip_enabled=False,
                source_refs=refs,
                kg_generation_id=kg_generation_id,
            )
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=CognitiveCloseoutOutcome.UNAVAILABLE.value,
                reason=CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value,
                skip_enabled=False,
                blocking_count=0,
            )
            return result

        if unconfirmed:
            # DEC-B / 3cf5dede: fail-closed — cannot confirm cognitive status,
            # KEEP allowed=False.
            if board_skip_enabled:
                result = CognitiveCloseoutResult(
                    allowed=True,
                    reason=CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value,
                    outcome=CognitiveCloseoutOutcome.SKIPPED.value,
                    skip_enabled=True,
                    source_refs=refs,
                    kg_generation_id=kg_generation_id,
                )
                _emit_closeout_sample(
                    board_id=board_id,
                    entity_id=entity_id or _get_attr_or_key(entity, "id"),
                    entity_type=normalized_type,
                    outcome=result.outcome,
                    reason=CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value,
                    skip_enabled=True,
                    blocking_count=0,
                )
                return result

            result = CognitiveCloseoutResult(
                allowed=False,
                reason=CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value,
                outcome=CognitiveCloseoutOutcome.UNAVAILABLE.value,
                skip_enabled=False,
                source_refs=refs,
                kg_generation_id=kg_generation_id,
            )
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=result.outcome,
                reason=result.reason,
                skip_enabled=False,
                blocking_count=0,
            )
            return result

        refs_set = frozenset(refs)
        active = tuple(
            CognitiveCloseoutBlockingItem(
                item_id=item.item_id,
                status=item.status,
                source_ref=item.source_ref,
            )
            for item in items
            if item.source_ref in refs_set and item.status in ACTIVE_ITEM_STATUSES
        )

        if active and board_skip_enabled:
            result = CognitiveCloseoutResult(
                allowed=True,
                reason=CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value,
                outcome=CognitiveCloseoutOutcome.SKIPPED.value,
                skip_enabled=True,
                blocking_items=active,
                source_refs=refs,
                kg_generation_id=resolved_generation,
            )
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=result.outcome,
                reason=result.reason,
                skip_enabled=True,
                blocking_count=result.blocking_count,
            )
            return result

        if active:
            metric_reason = (
                "active_decision_child_pending"
                if any(item.is_decision_child for item in active)
                else CognitiveCloseoutReason.COGNITIVE_CONSOLIDATION_PENDING.value
            )
            result = CognitiveCloseoutResult(
                allowed=False,
                reason=CognitiveCloseoutReason.COGNITIVE_CONSOLIDATION_PENDING.value,
                outcome=CognitiveCloseoutOutcome.BLOCKED.value,
                skip_enabled=False,
                blocking_items=active,
                source_refs=refs,
                kg_generation_id=resolved_generation,
            )
            _emit_closeout_sample(
                board_id=board_id,
                entity_id=entity_id or _get_attr_or_key(entity, "id"),
                entity_type=normalized_type,
                outcome=result.outcome,
                reason=metric_reason,
                skip_enabled=False,
                blocking_count=result.blocking_count,
            )
            return result

        result = CognitiveCloseoutResult(
            allowed=True,
            reason=CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
            outcome=CognitiveCloseoutOutcome.ALLOWED.value,
            skip_enabled=board_skip_enabled,
            source_refs=refs,
            kg_generation_id=resolved_generation,
        )
        _emit_closeout_sample(
            board_id=board_id,
            entity_id=entity_id or _get_attr_or_key(entity, "id"),
            entity_type=normalized_type,
            outcome=result.outcome,
            reason=result.reason,
            skip_enabled=board_skip_enabled,
            blocking_count=0,
        )
        return result


def build_default_cognitive_closeout_gate() -> CognitiveCloseoutGate:
    from okto_pulse.core.kg.rebuild_audit import default_rebuild_base_dir

    return CognitiveCloseoutGate(
        store=CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
    )


__all__ = [
    "CognitiveCloseoutBlockingItem",
    "CognitiveCloseoutGate",
    "CognitiveCloseoutGateError",
    "CognitiveCloseoutOutcome",
    "CognitiveCloseoutReason",
    "CognitiveCloseoutResult",
    "CognitiveItemStoreProtocol",
    "CognitiveSourceRefs",
    "ELIGIBLE_CLOSEOUT_ENTITY_TYPES",
    "build_default_cognitive_closeout_gate",
    "get_closeout_gate_counter_labels",
    "get_closeout_gate_event_count",
    "get_closeout_gate_samples",
    "reset_closeout_gate_samples",
    "resolve_cognitive_source_refs",
]
