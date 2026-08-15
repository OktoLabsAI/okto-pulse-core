"""KG-02.7 — kg.rebuilt audit event, cognitive pending marker and
confirmation consumption audit recorder.

This module wires three independent primitives that close the KG-02
loop:

* ``KGRebuiltEventPublisher`` (api_6fcc64aa) — validates the kg.rebuilt
  payload against the api contract and publishes it via an injected
  adapter. Counter ``kg_rebuilt_event_total`` (OR or_9da6b2d7) records
  ``published`` and ``publish_failed`` outcomes.

* ``CognitivePendingMarker`` (api_3e9d65ce, IR ir_47da2d34, TR9
  tr_a825cdae, BR br_0d710a8f) — marks eligible artifacts as
  ``pending`` cognitive consolidation for the new ``kg_generation_id``.
  NEVER marks ``completed`` — that's an agent decision the rebuild
  cannot make on its own. Counter ``kg_cognitive_pending_marked_total``
  (OR or_85dd2e90).

* ``ConfirmationConsumptionAuditRecorder`` (api_c9bc9a8c, BR br_d379c40d
  + br_48da2f8a) — records a durable audit row for every
  ``ConfirmationOutcome``: ``consumed``, ``expired``, ``replayed``,
  ``scope_mismatch``, ``missing``. Enforces safe payload semantics —
  the audit MUST NOT carry raw confirmation tokens or sensitive
  artifact payloads (api_c9bc9a8c response_errors ``unsafe_audit_payload``).

Layout:

    <base>/rebuild/audit/confirmation/<board>/<audit_id>.json
    <base>/rebuild/audit/events/<board>/<event_id>.json
    <base>/rebuild/audit/cognitive_pending/<board>/<gen_id>.json
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.query_contract import CognitiveOutcomeType
from okto_pulse.core.observability.sample_buffer import runtime_counter_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock, runtime_state


def confirmation_fingerprint(confirmation_id: str) -> str:
    """val_302bdec8 — produce a safe reference for a confirmation token.

    Returns ``conf_fp_<sha256-hex>``. The fingerprint correlates the
    audit row with the confirmation that produced the consumption
    outcome WITHOUT persisting the raw token (br_d379c40d). Different
    raw tokens always yield different fingerprints; the same raw token
    always yields the same fingerprint, so an operator can join audit
    rows by fingerprint when investigating an incident.
    """

    if not isinstance(confirmation_id, str) or not confirmation_id:
        raise ValueError("confirmation_id must be a non-empty string")
    digest = hashlib.sha256(confirmation_id.encode("utf-8")).hexdigest()
    return f"conf_fp_{digest}"


logger = logging.getLogger("okto_pulse.kg.rebuild_audit")


REBUILD_DIRNAME = "rebuild"
AUDIT_DIRNAME = "audit"
CONFIRMATION_AUDIT_DIRNAME = "confirmation"
EVENT_AUDIT_DIRNAME = "events"
COGNITIVE_PENDING_DIRNAME = "cognitive_pending"


def require_rebuild_audit_artifact_store() -> RebuildAuditArtifactStore:
    """Return the edition-composed rebuild audit artifact store."""

    from okto_pulse.core.kg.interfaces import get_kg_registry

    return get_kg_registry().require_rebuild_audit_artifact_store()


def resolve_rebuild_audit_artifact_store(
    *,
    base_dir: object | None,
    artifact_store: RebuildAuditArtifactStore | None,
) -> RebuildAuditArtifactStore:
    """Resolve productive artifact IO through the edition registry.

    ``base_dir`` is an opaque compatibility token. Only an edition resolver may
    interpret it; Core never performs path operations or chooses a backend.
    """

    if artifact_store is not None:
        return artifact_store
    if base_dir is not None:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        resolver = get_kg_registry().require_rebuild_audit_artifact_store_resolver()
        return resolver.resolve(base_dir)
    return require_rebuild_audit_artifact_store()


# ---------------------------------------------------------------------------
# kg.rebuilt event publisher
# ---------------------------------------------------------------------------


KG_REBUILT_REQUIRED_FIELDS: tuple[str, ...] = (
    "board_id",
    "previous_kg_generation_id",
    "kg_generation_id",
    "triggered_by",
    "started_at",
    "finished_at",
    "status",
    "counts",
    "report_ref",
)


class EventPublishOutcome(str, Enum):
    """Bounded outcomes for OR or_9da6b2d7 ``outcome`` label."""

    PUBLISHED = "published"
    PUBLISH_FAILED = "publish_failed"
    INVALID_PAYLOAD = "invalid_payload"


class EventPublishErrorCode(str, Enum):
    """Bounded error codes for api_6fcc64aa response_errors."""

    EVENT_PUBLISH_FAILED = "event_publish_failed"
    INVALID_PAYLOAD = "invalid_payload"


@dataclass(frozen=True, slots=True)
class EventPublishResult:
    """Frozen response for ``KGRebuiltEventPublisher.publish``."""

    accepted: bool
    outcome: str  # EventPublishOutcome value
    event_ref: str | None = None
    audit_ref: str | None = None
    error_code: str | None = None
    detail: str | None = None


_EVENT_LABELS = ("board_id", "status", "outcome")
_event_counter = runtime_state("kg.rebuild_audit.event_counter", dict)
_event_counter_lock = runtime_lock("kg.rebuild_audit.event_counter")


def _bump_event(*, board_id: str, status: str, outcome: str) -> None:
    key = (board_id, status, outcome)
    with _event_counter_lock:
        _event_counter[key] = _event_counter.get(key, 0) + 1


def get_event_count(
    board_id: str,
    *,
    status: str | None = None,
    outcome: str | None = None,
) -> int:
    with _event_counter_lock:
        total = 0
        for (b, st, oc), value in _event_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            if outcome is not None and oc != outcome:
                continue
            total += value
        return total


def get_event_counter_labels() -> tuple[str, ...]:
    return _EVENT_LABELS


def get_event_samples() -> list[dict[str, Any]]:
    with _event_counter_lock:
        return [
            {"board_id": b, "status": st, "outcome": oc, "count": value}
            for (b, st, oc), value in _event_counter.items()
        ]


def reset_event_counter() -> None:
    with _event_counter_lock:
        _event_counter.clear()


def validate_kg_rebuilt_event(payload: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validate payload against api_6fcc64aa request_body.

    Returns ``(True, None)`` on success, ``(False, reason)`` otherwise.
    The validator is strict about required fields and counts. A completed
    event must identify the promoted UUID v4. A report-backed terminal failure
    may carry ``kg_generation_id=None`` only when it identifies the valid
    candidate generation that was deliberately not promoted.
    """

    from okto_pulse.core.kg.rebuild_generation import (
        is_valid_kg_generation_id,
    )

    for field_name in KG_REBUILT_REQUIRED_FIELDS:
        if field_name not in payload:
            return False, f"missing_required_field:{field_name}"

    if not isinstance(payload.get("board_id"), str) or not payload["board_id"]:
        return False, "board_id_must_be_non_empty_string"
    kg_gen = payload.get("kg_generation_id")
    status = payload.get("status")
    if kg_gen is None:
        if status == "completed":
            return False, "completed_kg_generation_id_must_be_uuid_v4"
        candidate = payload.get("candidate_kg_generation_id")
        if not isinstance(candidate, str) or not is_valid_kg_generation_id(candidate):
            return False, "non_promoted_candidate_generation_id_must_be_uuid_v4"
    elif not isinstance(kg_gen, str) or not is_valid_kg_generation_id(kg_gen):
        return False, "kg_generation_id_must_be_uuid_v4"
    prev_gen = payload.get("previous_kg_generation_id")
    if prev_gen is not None and (
        not isinstance(prev_gen, str) or not is_valid_kg_generation_id(prev_gen)
    ):
        return False, "previous_kg_generation_id_must_be_uuid_v4_or_null"
    if not isinstance(payload.get("counts"), dict):
        return False, "counts_must_be_dict"
    for field_name in (
        "triggered_by",
        "started_at",
        "finished_at",
        "status",
        "report_ref",
    ):
        if not isinstance(payload.get(field_name), str) or not payload[field_name]:
            return False, f"{field_name}_must_be_non_empty_string"
    return True, None


# Adapters MUST deduplicate by the deterministic ``event_id`` carried in the
# payload. A process can crash after the external adapter accepts delivery but
# before the local success marker is durable; retrying that same logical event
# is therefore intentional and must not create a second downstream event.
KGRebuiltPublishAdapter = Callable[[Mapping[str, Any]], bool]


def _default_publish_adapter(_payload: Mapping[str, Any]) -> bool:
    """Default publish adapter — accepts every well-formed payload.

    Production wires the real event bus (Kafka/SQS/etc); the default
    keeps the rebuild loop runnable in CLI/test environments without
    external infrastructure. Returns True so the counter records
    ``published`` and the audit row marks accepted.
    """

    try:
        from okto_pulse.core.services.discovery_selector_catalog import (
            SELECTOR_EVENT_KG_REBUILT,
            get_default_discovery_selector_cache,
        )

        get_default_discovery_selector_cache().invalidate_event(
            {
                "event_type": SELECTOR_EVENT_KG_REBUILT,
                "board_id": _payload.get("board_id"),
                "kg_generation_id": _payload.get("kg_generation_id"),
            }
        )
    except Exception:  # noqa: BLE001
        logger.exception("kg.rebuilt.selector_cache_invalidation_failed")
    return True


@dataclass(frozen=True, slots=True)
class KGRebuiltEventPublisher:
    """Publishes kg.rebuilt events with retryable failure semantics.

    The publisher always persists a durable audit row first (TR12-style
    invariant), THEN invokes the publish adapter. If the adapter raises
    or returns False, the audit row remains and the outcome surfaces
    ``publish_failed`` with ``event_publish_failed`` so the operator can
    re-drive the event from the audit without losing the report.
    """

    base_dir: object | None = None
    publish_adapter: KGRebuiltPublishAdapter = _default_publish_adapter
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    @staticmethod
    def _audit_key(board_id: str, event_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="event_audit",
            board_id=board_id,
            artifact_id=event_id,
        )

    def publish(self, *, event_payload: Mapping[str, Any]) -> EventPublishResult:
        valid, reason = validate_kg_rebuilt_event(event_payload)
        board_id = str(event_payload.get("board_id", "unknown"))
        status = str(event_payload.get("status", "unknown"))

        if not valid:
            _bump_event(
                board_id=board_id,
                status=status,
                outcome=EventPublishOutcome.INVALID_PAYLOAD.value,
            )
            return EventPublishResult(
                accepted=False,
                outcome=EventPublishOutcome.INVALID_PAYLOAD.value,
                error_code=EventPublishErrorCode.INVALID_PAYLOAD.value,
                detail=reason,
            )

        try:
            run_id = str(event_payload.get("run_id") or "")
            event_id = (
                "evt_"
                + hashlib.sha256(f"{board_id}\x1f{run_id}".encode("utf-8")).hexdigest()[
                    :32
                ]
                if run_id
                else f"evt_{uuid.uuid4().hex}"
            )
            audit_key = self._audit_key(board_id, event_id)
            existing = self.artifact_store.read_json(audit_key)
            expected_event = {
                "event_id": event_id,
                "event": "kg.rebuilt",
                **dict(event_payload),
            }
            if existing is not None:
                observed_event = {
                    key: value
                    for key, value in existing.items()
                    if key
                    not in {
                        "persisted_at",
                        "delivered_at",
                        "delivery_outcome",
                        "delivery_detail",
                    }
                }
                replay_event = dict(expected_event)
                for timestamp_field in ("started_at", "finished_at"):
                    observed_event.pop(timestamp_field, None)
                    replay_event.pop(timestamp_field, None)
                if observed_event != replay_event:
                    raise RuntimeError("kg_rebuilt_event_run_binding_conflict")
                if (
                    existing.get("delivery_outcome")
                    == EventPublishOutcome.PUBLISHED.value
                ):
                    return EventPublishResult(
                        accepted=True,
                        outcome=EventPublishOutcome.PUBLISHED.value,
                        event_ref=event_id,
                        audit_ref=self.artifact_store.reference(audit_key),
                        detail="already_published",
                    )
                audit_payload = dict(existing)
            else:
                audit_payload = {
                    **expected_event,
                    "persisted_at": datetime.now(timezone.utc).isoformat(),
                    "delivery_outcome": "pending",
                }
                self.artifact_store.write_json_atomic(audit_key, audit_payload)
            audit_ref = self.artifact_store.reference(audit_key)
        except Exception as exc:
            logger.error(
                "kg.rebuilt.audit_persist_failed board=%s err=%s",
                board_id,
                exc,
            )
            _bump_event(
                board_id=board_id,
                status=status,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
            )
            return EventPublishResult(
                accepted=False,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
                error_code=EventPublishErrorCode.EVENT_PUBLISH_FAILED.value,
                detail=f"audit_persist_exception={type(exc).__name__}",
            )

        try:
            ok = self.publish_adapter(audit_payload)
        except Exception as exc:
            try:
                self.artifact_store.write_json_atomic(
                    audit_key,
                    {
                        **audit_payload,
                        "delivery_outcome": EventPublishOutcome.PUBLISH_FAILED.value,
                        "delivery_detail": f"publish_adapter_exception={type(exc).__name__}",
                    },
                )
            except Exception:
                logger.exception(
                    "kg.rebuilt.publish_failure_marker_failed board=%s event=%s",
                    board_id,
                    event_id,
                )
            logger.error(
                "kg.rebuilt.publish_adapter_failed board=%s err=%s",
                board_id,
                exc,
            )
            _bump_event(
                board_id=board_id,
                status=status,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
            )
            return EventPublishResult(
                accepted=False,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
                event_ref=event_id,
                audit_ref=audit_ref,
                error_code=EventPublishErrorCode.EVENT_PUBLISH_FAILED.value,
                detail=f"publish_adapter_exception={type(exc).__name__}",
            )

        if not ok:
            try:
                self.artifact_store.write_json_atomic(
                    audit_key,
                    {
                        **audit_payload,
                        "delivery_outcome": EventPublishOutcome.PUBLISH_FAILED.value,
                        "delivery_detail": "publish_adapter_returned_false",
                    },
                )
            except Exception:
                logger.exception(
                    "kg.rebuilt.publish_failure_marker_failed board=%s event=%s",
                    board_id,
                    event_id,
                )
            _bump_event(
                board_id=board_id,
                status=status,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
            )
            return EventPublishResult(
                accepted=False,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
                event_ref=event_id,
                audit_ref=audit_ref,
                error_code=EventPublishErrorCode.EVENT_PUBLISH_FAILED.value,
                detail="publish_adapter_returned_false",
            )

        delivered = {
            **audit_payload,
            "delivery_outcome": EventPublishOutcome.PUBLISHED.value,
            "delivery_detail": None,
            "delivered_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.artifact_store.write_json_atomic(audit_key, delivered)
        except Exception as exc:
            logger.exception(
                "kg.rebuilt.publish_success_marker_failed board=%s event=%s",
                board_id,
                event_id,
            )
            _bump_event(
                board_id=board_id,
                status=status,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
            )
            return EventPublishResult(
                accepted=False,
                outcome=EventPublishOutcome.PUBLISH_FAILED.value,
                event_ref=event_id,
                audit_ref=audit_ref,
                error_code=EventPublishErrorCode.EVENT_PUBLISH_FAILED.value,
                detail=f"success_marker_exception={type(exc).__name__}",
            )

        _bump_event(
            board_id=board_id,
            status=status,
            outcome=EventPublishOutcome.PUBLISHED.value,
        )
        return EventPublishResult(
            accepted=True,
            outcome=EventPublishOutcome.PUBLISHED.value,
            event_ref=event_id,
            audit_ref=audit_ref,
        )


# ---------------------------------------------------------------------------
# KG-03.1 — Per-item cognitive consolidation ledger
# ---------------------------------------------------------------------------


CognitivePendingOutcomeType = CognitiveOutcomeType
"""Backward-compatible domain name for the canonical KG outcome enum.

The values now originate in :mod:`okto_pulse.core.kg.query_contract`, which is
also consumed by MCP schema introspection and contract-parity tests.
"""


class CognitiveItemStatus(str, Enum):
    """Bounded status enum per tr_a7b391e0.

    Active: pending, in_progress, failed (cognitive debt visible).
    Terminal: consolidated, skipped (closed by agent via MCP item tools).
    Rebuild + Marker can ONLY write pending (br_84194faf + br_f79887a3 +
    tr_9521d8fd). Mutation to other statuses is owned by KG-03.3 MCP
    update tool.
    """

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    CONSOLIDATED = "consolidated"
    SKIPPED = "skipped"
    FAILED = "failed"


ACTIVE_ITEM_STATUSES: frozenset[str] = frozenset(
    {
        CognitiveItemStatus.PENDING.value,
        CognitiveItemStatus.IN_PROGRESS.value,
        CognitiveItemStatus.FAILED.value,
    }
)


def compute_cognitive_item_id(
    board_id: str, kg_generation_id: str, source_ref: str
) -> str:
    """Deterministic item_id per tr_39e76c26.

    SHA256 of ``board_id|kg_generation_id|source_ref`` so the same triple
    always yields the same id across process restarts and record
    rewrites. Used by KG-03.1 ledger + KG-03.3 MCP update tool to refer
    to a single item without exposing the raw source_ref in metric
    labels."""

    digest = hashlib.sha256(
        f"{board_id}|{kg_generation_id}|{source_ref}".encode("utf-8")
    ).hexdigest()
    return f"cogn_{digest[:32]}"


# Artifact-type prefixes that denote the SAME underlying Card row. A bug IS a
# Card (card_type=bug), so a canonical-debt ref ``card:<uuid>`` and a cognitive
# ref ``bug:<uuid>`` for the same uuid must collapse to ONE artifact_id so the
# readiness precedence (later cards) can be applied without a silent no-op
# (fr_43ea6e97 / ac_50e4d48e).
_CARD_ALIAS_PREFIXES: frozenset[str] = frozenset({"card", "bug"})


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def normalize_cognitive_artifact_id(source_ref: str) -> str:
    """Canonical per-ARTIFACT identity for cognitive readiness grouping
    (fr_43ea6e97).

    Collapses equivalent ``source_ref`` aliases so a canonical-debt
    ``card:<uuid>`` and a cognitive ``bug:<uuid>`` for the SAME uuid resolve to
    one ``artifact_id``. The caller preserves the untouched ref in
    ``source_ref_original``; this returns only the canonical grouping key.

    Rules (pure + deterministic — same input always yields the same output,
    never raises):
    - ``card:<uuid>`` / ``bug:<uuid>`` (second token a UUID) -> ``card:<uuid-lower>``.
    - any other ``<type>:<rest>`` -> ``<type-lower>:<rest>`` (uuid tail lowercased
      for stability; non-uuid rest preserved verbatim, e.g. ``decision:<spec>:<id>``).
    - no colon / empty -> the input trimmed, verbatim.

    This is NOT a replacement for :func:`compute_cognitive_item_id` (the
    per-ITEM identity); it is the per-ARTIFACT key used to reconcile aliases.
    """

    ref = str(source_ref or "").strip()
    if ":" not in ref:
        return ref
    prefix, rest = ref.split(":", 1)
    prefix_norm = prefix.strip().lower()
    rest = rest.strip()
    if prefix_norm in _CARD_ALIAS_PREFIXES and _is_uuid(rest):
        return f"card:{rest.lower()}"
    if _is_uuid(rest):
        return f"{prefix_norm}:{rest.lower()}"
    return f"{prefix_norm}:{rest}"


# Counter for OR or_3b71e3c1 — kg_cognitive_pending_items_materialized_total.
#
# Contract (Codex audit val_036cb81e + or_3b71e3c1):
#   * Emit exactly ONE sample per successful marker write.
#   * Labels: (board_id, outcome) ONLY.
#   * `item_count` is the sample VALUE, NOT a label.
#   * `artifact_type`, `source_ref`, `item_id`, `actor` MUST NOT appear as labels.
#   * `update_item` does NOT emit on this counter (that's KG-03.3 territory —
#     `kg_cognitive_item_update_total`).
_MATERIALIZED_LABELS = ("board_id", "outcome")


class CognitiveMaterializeOutcome(str, Enum):
    """Bounded outcomes for the materialization counter label."""

    MATERIALIZED = "materialized"
    EMPTY = "empty"  # marker call with zero consolidable sources
    FAILED = "failed"


_materialized_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.materialized",
    _MATERIALIZED_LABELS,
    sum_fields=("item_count",),
)

_COGNITIVE_OVERLAY_REVISION_VERSION = 1
_COGNITIVE_OVERLAY_REVISION_ARTIFACT_ID = "cognitive_pending_overlay_revision"
_COGNITIVE_OVERLAY_MAX_BOARDS = 2_000
_COGNITIVE_OVERLAY_MAX_GENERATIONS_PER_BOARD = 256
_COGNITIVE_OVERLAY_MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
_COGNITIVE_OVERLAY_MAX_ITEMS_PER_BOARD = 10_000
_COGNITIVE_OVERLAY_MAX_CAPTURE_SECONDS = 300.0


class CognitivePendingOverlaySnapshotError(RuntimeError):
    """A bounded cognitive publication-overlay snapshot could not be proven."""

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


def _cognitive_overlay_revision_key() -> RebuildAuditKey:
    return RebuildAuditKey(
        namespace="global_discovery_recovery",
        board_id="_global",
        artifact_id=_COGNITIVE_OVERLAY_REVISION_ARTIFACT_ID,
    )


def _valid_stable_cognitive_overlay_revision(
    payload: Mapping[str, Any] | None,
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    revision = payload.get("revision")
    nonce = payload.get("nonce")
    return (
        payload.get("version") == _COGNITIVE_OVERLAY_REVISION_VERSION
        and payload.get("state") == "stable"
        and isinstance(revision, int)
        and not isinstance(revision, bool)
        and revision >= 0
        and isinstance(nonce, str)
        and 16 <= len(nonce) <= 128
    )


def _cognitive_overlay_revision_fingerprint(payload: Mapping[str, Any]) -> str:
    binding = {
        "version": payload["version"],
        "revision": payload["revision"],
        "nonce": payload["nonce"],
    }
    encoded = json.dumps(
        binding,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _next_cognitive_overlay_revision(
    current: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_revision = current.get("revision") if isinstance(current, dict) else None
    base_revision = (
        raw_revision
        if isinstance(raw_revision, int)
        and not isinstance(raw_revision, bool)
        and raw_revision >= 0
        else 0
    )
    revision = base_revision + 1
    nonce = secrets.token_urlsafe(24)
    common = {
        "version": _COGNITIVE_OVERLAY_REVISION_VERSION,
        "revision": revision,
        "nonce": nonce,
    }
    return ({**common, "state": "mutating"}, {**common, "state": "stable"})


@dataclass(frozen=True, slots=True)
class CognitivePendingOverlaySnapshot:
    """Immutable active-hold projection bound to one durable revision."""

    revision_fingerprint: str
    exclusions: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]

    def exclusions_for_board(self, board_id: str) -> dict[str, str]:
        requested = str(board_id)
        for captured_board_id, rows in self.exclusions:
            if captured_board_id == requested:
                return dict(rows)
        return {}


@dataclass(frozen=True, slots=True)
class CognitivePendingOverlaySnapshotService:
    """Cheap revision reads plus bounded, drift-checked overlay capture."""

    artifact_store: RebuildAuditArtifactStore

    def current_fingerprint(self) -> str:
        key = _cognitive_overlay_revision_key()
        current = self.artifact_store.read_json(key)
        if _valid_stable_cognitive_overlay_revision(current):
            return _cognitive_overlay_revision_fingerprint(current)
        if isinstance(current, Mapping) and current.get("state") == "unfenced":
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_revision_unfenced",
                "artifact adapter cannot serialize ledger and revision writes",
            )

        def repair(
            observed: dict[str, Any] | None,
        ) -> dict[str, Any]:
            if _valid_stable_cognitive_overlay_revision(observed):
                return dict(observed)
            _pending, stable = _next_cognitive_overlay_revision(observed)
            return stable

        repaired = self.artifact_store.replace_json(key, repair)
        if not _valid_stable_cognitive_overlay_revision(repaired):
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_revision_invalid",
                "overlay revision could not be initialized or repaired",
            )
        return _cognitive_overlay_revision_fingerprint(repaired)

    def capture(
        self,
        *,
        board_ids: Sequence[str],
        deadline_seconds: float,
    ) -> CognitivePendingOverlaySnapshot:
        normalized = tuple(sorted({str(board_id).strip() for board_id in board_ids}))
        if not normalized or any(not board_id for board_id in normalized):
            raise ValueError("board_ids must contain non-empty identifiers")
        if len(normalized) > _COGNITIVE_OVERLAY_MAX_BOARDS:
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_board_limit_exceeded",
                f"board count exceeds {_COGNITIVE_OVERLAY_MAX_BOARDS}",
            )
        if any(len(board_id) > 255 for board_id in normalized):
            raise ValueError("board_id length must not exceed 255 characters")
        budget = float(deadline_seconds)
        if not 0 < budget <= _COGNITIVE_OVERLAY_MAX_CAPTURE_SECONDS:
            raise ValueError(
                "deadline_seconds must be positive and no greater than 300"
            )
        deadline = time.monotonic() + budget
        before = self.current_fingerprint()
        captured: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        from okto_pulse.core.kg.connectivity_guard import (
            CANONICAL_LEARNING_WORKING_ONLY_REASON,
        )

        bounded_list = getattr(self.artifact_store, "list_json_bounded", None)
        bounded_implementation = getattr(
            type(self.artifact_store), "list_json_bounded", None
        )
        if (
            not callable(bounded_list)
            or bounded_implementation is RebuildAuditArtifactStore.list_json_bounded
        ):
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_bounded_reader_unavailable",
                "artifact adapter does not provide bounded overlay reads",
            )
        for board_id in normalized:
            if time.monotonic() >= deadline:
                raise CognitivePendingOverlaySnapshotError(
                    "cognitive_overlay_capture_timeout",
                    "overlay capture exceeded its explicit deadline",
                )
            try:
                records = bounded_list(
                    RebuildAuditKey(
                        namespace="cognitive_pending",
                        board_id=board_id,
                    ),
                    max_results=_COGNITIVE_OVERLAY_MAX_GENERATIONS_PER_BOARD,
                    max_document_bytes=_COGNITIVE_OVERLAY_MAX_DOCUMENT_BYTES,
                )
            except Exception as exc:
                raise CognitivePendingOverlaySnapshotError(
                    "cognitive_overlay_capture_failed",
                    f"bounded overlay read failed for board {board_id!r}",
                ) from exc
            latest = max(
                records,
                key=lambda row: (
                    str(row.get("recorded_at", "")),
                    str(row.get("kg_generation_id", "")),
                ),
                default=None,
            )
            exclusions: dict[str, str] = {}
            if latest is not None:
                items = latest.get("items")
                if items is not None and not isinstance(items, list):
                    raise CognitivePendingOverlaySnapshotError(
                        "cognitive_overlay_record_invalid",
                        f"overlay items are invalid for board {board_id!r}",
                    )
                if isinstance(items, list):
                    if len(items) > _COGNITIVE_OVERLAY_MAX_ITEMS_PER_BOARD:
                        raise CognitivePendingOverlaySnapshotError(
                            "cognitive_overlay_item_limit_exceeded",
                            f"overlay item limit exceeded for board {board_id!r}",
                        )
                    for raw in items:
                        if not isinstance(raw, Mapping):
                            raise CognitivePendingOverlaySnapshotError(
                                "cognitive_overlay_record_invalid",
                                f"overlay item is invalid for board {board_id!r}",
                            )
                        if (
                            str(raw.get("status") or "") in ACTIVE_ITEM_STATUSES
                            and str(raw.get("reason_code") or "")
                            == CANONICAL_LEARNING_WORKING_ONLY_REASON
                        ):
                            artifact_id = normalize_cognitive_artifact_id(
                                str(raw.get("source_ref") or "")
                            )
                            if artifact_id:
                                exclusions[artifact_id] = (
                                    CANONICAL_LEARNING_WORKING_ONLY_REASON
                                )
            captured.append((board_id, tuple(sorted(exclusions.items()))))

        after = self.current_fingerprint()
        if before != after:
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_snapshot_drift",
                "cognitive overlay changed while it was captured",
            )
        if time.monotonic() >= deadline:
            raise CognitivePendingOverlaySnapshotError(
                "cognitive_overlay_capture_timeout",
                "overlay capture exceeded its explicit deadline",
            )
        return CognitivePendingOverlaySnapshot(
            revision_fingerprint=before,
            exclusions=tuple(captured),
        )


_materialized_samples_lock = runtime_lock("kg.rebuild_audit.materialized.samples")


def _emit_materialized_sample(*, board_id: str, outcome: str, item_count: int) -> None:
    """Emit one materialization sample. ``item_count`` is the value of the
    sample — labels are restricted to (board_id, outcome)."""

    with _materialized_samples_lock:
        _materialized_samples.append(
            {
                "board_id": board_id,
                "outcome": outcome,
                "item_count": int(item_count),
            }
        )


def get_materialized_count(
    board_id: str,
    *,
    outcome: str | None = None,
) -> int:
    """Sum of ``item_count`` values across matching samples — i.e. total
    items materialized for ``board_id`` (optionally filtered by outcome).
    This is the metric value an exporter would publish."""

    with _materialized_samples_lock:
        return _materialized_samples.sum(
            "item_count",
            board_id=board_id,
            outcome=outcome,
        )


def get_materialized_event_count(
    board_id: str,
    *,
    outcome: str | None = None,
) -> int:
    """Number of marker events (samples) — useful to assert the one-event
    -per-marker invariant required by or_3b71e3c1."""

    with _materialized_samples_lock:
        return _materialized_samples.count(board_id=board_id, outcome=outcome)


def get_materialized_counter_labels() -> tuple[str, ...]:
    return _MATERIALIZED_LABELS


def get_materialized_samples() -> list[dict[str, Any]]:
    """Return a snapshot list of emitted samples. Each sample contains
    only the contract-mandated keys: board_id, outcome, item_count."""

    with _materialized_samples_lock:
        return _materialized_samples.snapshot()


def reset_materialized_counter() -> None:
    with _materialized_samples_lock:
        _materialized_samples.clear()


# ---------------------------------------------------------------------------
# Counter for OR or_c8fff4f5 — kg_cognitive_item_list_total
# ---------------------------------------------------------------------------
#
# Contract (or_c8fff4f5 + Codex KG-03.1 audit precedent):
#   * Emit exactly one sample per MCP/REST list call.
#   * Labels: (surface, board_id, outcome, status_filter_present, reason_code).
#   * NO source_ref, item_id, status_filter VALUE or actor labels —
#     keeping cardinality bounded.
#   * surface ∈ {"mcp", "rest"}.
#   * outcome ∈ {"success", "validation_error"}.
#   * status_filter_present is a stringified bool ("true"/"false") so the
#     label space stays in {"true", "false"} regardless of input.
#   * reason_code is a bounded enum value (no free-text).


class CognitiveItemListSurface(str, Enum):
    """Bounded label values for the list counter ``surface`` dimension."""

    MCP = "mcp"
    REST = "rest"


class CognitiveItemListOutcome(str, Enum):
    """Bounded outcomes for the list counter."""

    SUCCESS = "success"
    VALIDATION_ERROR = "validation_error"
    NOT_FOUND = "not_found"


class CognitiveItemListReasonCode(str, Enum):
    """Bounded reason_code values surfaced by the MCP/REST list response
    and the list counter. ``NONE`` is the canonical no-reason value."""

    NONE = "none"
    INVALID_STATUS = "invalid_status"
    MISSING_BOARD_ID = "missing_board_id"
    NO_GENERATION_FOUND = "no_generation_found"


_LIST_LABELS = (
    "surface",
    "board_id",
    "outcome",
    "status_filter_present",
    "reason_code",
)
_list_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.list",
    _LIST_LABELS,
)
_list_samples_lock = runtime_lock("kg.rebuild_audit.list.samples")


def _emit_list_sample(
    *,
    surface: str,
    board_id: str,
    outcome: str,
    status_filter_present: bool,
    reason_code: str,
    item_count: int,
) -> None:
    """Emit one sample on ``kg_cognitive_item_list_total``.

    ``item_count`` is carried as the sample value for parity with the
    materialization counter and for monitoring "how many items did the
    caller see"; it is NOT a label."""

    with _list_samples_lock:
        _list_samples.append(
            {
                "surface": surface,
                "board_id": board_id,
                "outcome": outcome,
                "status_filter_present": ("true" if status_filter_present else "false"),
                "reason_code": reason_code,
                "item_count": int(item_count),
            }
        )


def get_list_event_count(
    *,
    board_id: str | None = None,
    surface: str | None = None,
    outcome: str | None = None,
    reason_code: str | None = None,
) -> int:
    with _list_samples_lock:
        return _list_samples.count(
            board_id=board_id,
            surface=surface,
            outcome=outcome,
            reason_code=reason_code,
        )


def get_list_counter_labels() -> tuple[str, ...]:
    return _LIST_LABELS


def get_list_samples() -> list[dict[str, Any]]:
    with _list_samples_lock:
        return _list_samples.snapshot()


def reset_list_counter() -> None:
    with _list_samples_lock:
        _list_samples.clear()


# ---------------------------------------------------------------------------
# Counter for OR or_b8ff0cc2 — kg_operational_inspection_list_total
# ---------------------------------------------------------------------------
#
# Contract (or_b8ff0cc2): count operational-inspection listing calls so the
# ABSENCE of drill-down usage is diagnosable. One bounded sample per call to
# the three operational-signal listings KG Health points at — cognitive
# pending, dead-letter queue, canonical debt (kept as separate `signal`
# values per dec_68fd26a2). Labels only; no free-form text.
OPERATIONAL_INSPECTION_SIGNALS: frozenset[str] = frozenset(
    {
        "cognitive_pending",
        "dead_letter",
        "canonical_debt",
    }
)
_OPERATIONAL_INSPECTION_LABELS = ("signal", "surface", "outcome", "board_id")
_operational_inspection_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.operational_inspection",
    _OPERATIONAL_INSPECTION_LABELS,
)
_operational_inspection_lock = runtime_lock(
    "kg.rebuild_audit.operational_inspection.samples"
)


def emit_operational_inspection_sample(
    *,
    signal: str,
    surface: str,
    outcome: str,
    board_id: str,
    item_count: int,
) -> None:
    """Emit one sample on ``kg_operational_inspection_list_total`` (or_b8ff0cc2).

    ``signal`` is one of the three separate operational domains
    (cognitive_pending / dead_letter / canonical_debt); ``surface`` is
    mcp/rest; ``outcome`` is success/error. ``item_count`` is the sample
    value (how many rows the caller saw), NOT a label.
    """

    with _operational_inspection_lock:
        _operational_inspection_samples.append(
            {
                "signal": signal,
                "surface": surface,
                "outcome": outcome,
                "board_id": board_id,
                "item_count": int(item_count),
            }
        )


def get_operational_inspection_event_count(
    *,
    signal: str | None = None,
    surface: str | None = None,
    outcome: str | None = None,
    board_id: str | None = None,
) -> int:
    with _operational_inspection_lock:
        return _operational_inspection_samples.count(
            signal=signal,
            surface=surface,
            outcome=outcome,
            board_id=board_id,
        )


def get_operational_inspection_counter_labels() -> tuple[str, ...]:
    return _OPERATIONAL_INSPECTION_LABELS


def get_operational_inspection_samples() -> list[dict[str, Any]]:
    with _operational_inspection_lock:
        return _operational_inspection_samples.snapshot()


def reset_operational_inspection_counter() -> None:
    with _operational_inspection_lock:
        _operational_inspection_samples.clear()


# ---------------------------------------------------------------------------
# Counter for OR or_36e0cd85 — okto_pulse_kg_cognitive_technical_signal_total (RKG-05)
# ---------------------------------------------------------------------------
#
# Contract (or_36e0cd85): one bounded sample whenever a NON-MASKABLE technical KG
# signal (technical_dlq / dead_letter_backlog / canonical_debt_open /
# persistence_error) is surfaced through the health/readiness projection — so the
# PRESENCE of a technical blocker, and whether it is gate-blocking vs advisory, is
# observable independently of any cognitive skip/no_action. Labels only.
COGNITIVE_TECHNICAL_SIGNALS: frozenset[str] = frozenset(
    {
        "technical_dlq",
        "dead_letter_backlog",
        "canonical_debt_open",
        "persistence_error",
    }
)
_COGNITIVE_TECHNICAL_SIGNAL_LABELS = (
    "signal",
    "surface",
    "blocking",
    "would_block_done",
    "board_id",
)
_cognitive_technical_signal_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.cognitive_technical_signal",
    _COGNITIVE_TECHNICAL_SIGNAL_LABELS,
)
_cognitive_technical_signal_lock = runtime_lock(
    "kg.rebuild_audit.cognitive_technical_signal.samples"
)


def emit_cognitive_technical_signal_sample(
    *,
    signal: str,
    surface: str,
    blocking: bool,
    would_block_done: bool,
    board_id: str,
) -> None:
    """Emit one sample on ``okto_pulse_kg_cognitive_technical_signal_total`` (or_36e0cd85).

    ``signal`` is one of COGNITIVE_TECHNICAL_SIGNALS; ``surface`` is rest/mcp;
    ``blocking`` is whether a technical problem is visible; ``would_block_done``
    is whether the gate would actually block (enforcement-aware, advisory→False).
    """
    with _cognitive_technical_signal_lock:
        _cognitive_technical_signal_samples.append(
            {
                "signal": signal,
                "surface": surface,
                "blocking": bool(blocking),
                "would_block_done": bool(would_block_done),
                "board_id": board_id,
            }
        )


def get_cognitive_technical_signal_event_count(
    *,
    signal: str | None = None,
    surface: str | None = None,
    blocking: bool | None = None,
    would_block_done: bool | None = None,
    board_id: str | None = None,
) -> int:
    with _cognitive_technical_signal_lock:
        return _cognitive_technical_signal_samples.count(
            signal=signal,
            surface=surface,
            blocking=blocking,
            would_block_done=would_block_done,
            board_id=board_id,
        )


def get_cognitive_technical_signal_counter_labels() -> tuple[str, ...]:
    return _COGNITIVE_TECHNICAL_SIGNAL_LABELS


def get_cognitive_technical_signal_samples() -> list[dict[str, Any]]:
    with _cognitive_technical_signal_lock:
        return _cognitive_technical_signal_samples.snapshot()


def reset_cognitive_technical_signal_counter() -> None:
    with _cognitive_technical_signal_lock:
        _cognitive_technical_signal_samples.clear()


# ---------------------------------------------------------------------------
# Counter for OR or_174f18d5 — kg_cognitive_item_update_total
# ---------------------------------------------------------------------------
#
# Contract (or_174f18d5):
#   * Emit exactly one sample per accepted OR rejected update call.
#   * Labels: (board_id, target_status, outcome, reason_code).
#   * Rejections for missing consolidation_session_id and missing reason
#     are distinguishable via reason_code.
#   * NO source_ref, item_id, raw reason text, actor labels.


class CognitiveItemUpdateOutcome(str, Enum):
    """Bounded outcomes for the update counter."""

    UPDATED = "updated"
    VALIDATION_ERROR = "validation_error"
    ITEM_NOT_FOUND = "item_not_found"
    STORE_ERROR = "store_error"


class CognitiveItemUpdateReasonCode(str, Enum):
    """Bounded reason_code values for update results.

    The MCP/REST update tool MUST surface one of these; never free-text.
    """

    NONE = "none"
    INVALID_STATUS = "invalid_status"
    CONSOLIDATION_SESSION_REQUIRED = "consolidation_session_required"
    REASON_REQUIRED = "reason_required"
    ITEM_NOT_FOUND = "item_not_found"
    UNSAFE_PAYLOAD = "unsafe_payload"
    MISSING_BOARD_ID = "missing_board_id"
    MISSING_GENERATION_ID = "missing_generation_id"
    MISSING_ITEM_ID = "missing_item_id"
    # KG-03A.3 — bounded outcome enforcement for terminal consolidated
    OUTCOME_REQUIRED = "outcome_required"
    INVALID_OUTCOME_TYPE = "invalid_outcome_type"


# Bounded label value used when caller submits a status outside the enum;
# keeps the target_status label cardinality finite even on bad input.
_INVALID_TARGET_STATUS = "invalid"


_UPDATE_LABELS = ("board_id", "target_status", "outcome", "reason_code")
_update_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.update",
    _UPDATE_LABELS,
)
_update_samples_lock = runtime_lock("kg.rebuild_audit.update.samples")


def _emit_update_sample(
    *,
    board_id: str,
    target_status: str,
    outcome: str,
    reason_code: str,
) -> None:
    """Emit one sample on ``kg_cognitive_item_update_total``."""

    with _update_samples_lock:
        _update_samples.append(
            {
                "board_id": board_id,
                "target_status": target_status,
                "outcome": outcome,
                "reason_code": reason_code,
            }
        )


def get_update_event_count(
    *,
    board_id: str | None = None,
    target_status: str | None = None,
    outcome: str | None = None,
    reason_code: str | None = None,
) -> int:
    with _update_samples_lock:
        return _update_samples.count(
            board_id=board_id,
            target_status=target_status,
            outcome=outcome,
            reason_code=reason_code,
        )


def get_update_counter_labels() -> tuple[str, ...]:
    return _UPDATE_LABELS


def get_update_samples() -> list[dict[str, Any]]:
    with _update_samples_lock:
        return _update_samples.snapshot()


def reset_update_counter() -> None:
    with _update_samples_lock:
        _update_samples.clear()


# ---------------------------------------------------------------------------
# Counter for KG-03A.7 — kg_cognitive_pending_reopen_total
# ---------------------------------------------------------------------------


class CognitivePendingReopenOutcome(str, Enum):
    """Bounded outcome label for the reopen counter."""

    SUCCESS = "success"
    NOOP = "noop"


class CognitivePendingReopenReasonCode(str, Enum):
    """Bounded reason label for why a reopen sample was emitted."""

    NONE = "none"
    CONTENT_CHANGED = "content_changed"
    FIRST_OBSERVATION = "first_observation"


_REOPEN_LABELS = ("entity_type", "outcome", "reason_code")
_reopen_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.reopen",
    _REOPEN_LABELS,
)
_reopen_samples_lock = runtime_lock("kg.rebuild_audit.reopen.samples")


def _emit_reopen_sample(
    *,
    entity_type: str,
    outcome: str,
    reason_code: str,
) -> None:
    """Emit one bounded sample on ``kg_cognitive_pending_reopen_total``.

    Per ``or_029bd920`` the allowed labels are ``entity_type``,
    ``outcome`` and ``reason_code``. The board id, source_ref and
    content_hash MUST NOT leak into the metric stream — they are
    high-cardinality and would defeat the bounded-label contract.
    ``entity_type`` is the artifact_type of the source row that
    triggered the reopen (``spec``, ``decision``, ``refinement``,
    ``task``, ``test``, ``bug``).
    """

    with _reopen_samples_lock:
        _reopen_samples.append(
            {
                "entity_type": entity_type,
                "outcome": outcome,
                "reason_code": reason_code,
            }
        )


def get_reopen_event_count(
    *,
    entity_type: str | None = None,
    outcome: str | None = None,
    reason_code: str | None = None,
) -> int:
    with _reopen_samples_lock:
        return _reopen_samples.count(
            entity_type=entity_type,
            outcome=outcome,
            reason_code=reason_code,
        )


def get_reopen_counter_labels() -> tuple[str, ...]:
    return _REOPEN_LABELS


def get_reopen_samples() -> list[dict[str, Any]]:
    with _reopen_samples_lock:
        return _reopen_samples.snapshot()


def reset_reopen_counter() -> None:
    with _reopen_samples_lock:
        _reopen_samples.clear()


# ---------------------------------------------------------------------------
# Counter for OR or_03222a4f — kg_cognitive_item_unsafe_payload_total
# ---------------------------------------------------------------------------
#
# Alert metric (severity: high). The threshold says any non-zero value
# in production must be investigated, because the cognitive feature must
# store safe metadata only (br_858a0859). Labels are bounded; reason
# codes are controlled enum values, never raw payload excerpts.


class CognitiveUnsafePayloadSurface(str, Enum):
    """Bounded surfaces where unsafe payload was caught."""

    MCP_UPDATE = "mcp_update"
    REST_LIST = "rest_list"
    STORE_WRITE = "store_write"


class CognitiveUnsafePayloadReason(str, Enum):
    """Bounded reasons. Each value names the FIELD that tripped the
    check — never the raw value itself."""

    CONSOLIDATION_SESSION_ID = "consolidation_session_id"
    REASON = "reason"
    SUMMARY_TEXT = "summary_text"
    ITEM_FIELD_LEAK = "item_field_leak"
    OTHER = "other"


_UNSAFE_LABELS = ("surface", "board_id", "reason")
_unsafe_samples = runtime_counter_sample_buffer(
    "kg.rebuild_audit.unsafe",
    _UNSAFE_LABELS,
)
_unsafe_samples_lock = runtime_lock("kg.rebuild_audit.unsafe.samples")


def _emit_unsafe_payload_sample(*, surface: str, board_id: str, reason: str) -> None:
    """Emit one sample on ``kg_cognitive_item_unsafe_payload_total``."""

    with _unsafe_samples_lock:
        _unsafe_samples.append(
            {
                "surface": surface,
                "board_id": board_id,
                "reason": reason,
            }
        )


def get_unsafe_payload_event_count(
    *,
    board_id: str | None = None,
    surface: str | None = None,
    reason: str | None = None,
) -> int:
    with _unsafe_samples_lock:
        return _unsafe_samples.count(
            board_id=board_id,
            surface=surface,
            reason=reason,
        )


def get_unsafe_payload_counter_labels() -> tuple[str, ...]:
    return _UNSAFE_LABELS


def get_unsafe_payload_samples() -> list[dict[str, Any]]:
    with _unsafe_samples_lock:
        return _unsafe_samples.snapshot()


def reset_unsafe_payload_counter() -> None:
    with _unsafe_samples_lock:
        _unsafe_samples.clear()


def project_item_for_update_api(
    item: "CognitiveConsolidationItem",
) -> dict[str, Any]:
    """Safe API projection for the update response (api_525a25f1 +
    KG-03A.3 outcome metadata).

    Differs from ``project_item_for_api`` only in semantic emphasis —
    ``updated_at`` and ``updated_by_agent_id`` are populated for an
    updated item, but the projection itself returns the same bounded
    shape. Free-text ``reason`` is never echoed; the bounded
    ``item.reason_code`` — persisted on the item since S1 card 9aeeaebd —
    is echoed as the only narrative field so a caller can confirm what was
    stored without an extra round-trip.

    KG-03A.3 rework (Codex audit val_44b86726): the bounded outcome
    metadata persisted by the update path is echoed back so callers can
    confirm what was stored without an extra round-trip. All four
    fields go through ``detect_unsafe_update_payload`` before write, so
    echoing them is safe by construction.
    """

    return {
        "item_id": item.item_id,
        "source_ref": item.source_ref,
        "artifact_type": item.artifact_type,
        "status": item.status,
        "updated_at": item.updated_at,
        "updated_by_agent_id": item.updated_by_agent_id,
        "consolidation_session_id": item.consolidation_session_id,
        "reason_code": item.reason_code,
        "outcome_type": item.outcome_type,
        "evidence_refs": list(item.evidence_refs),
        "generated_candidate_decision_ids": list(item.generated_candidate_decision_ids),
        "promoted_formal_decision_ids": list(item.promoted_formal_decision_ids),
    }


# Maximum bytes accepted on update-request narrative fields. Beyond this
# we assume the caller is trying to smuggle artifact body into the
# audit row and reject as ``unsafe_payload`` (br_858a0859 + br_a76d1b13).
_MAX_UPDATE_NARRATIVE_BYTES = 2000

# KG-03A.3 rework — bounded outcome metadata list shape.
# Each entry MUST be a non-empty string; per-entry length is capped at
# ``_MAX_UPDATE_NARRATIVE_BYTES`` (raw artifact body / oversize smuggle
# guard, br_858a0859). The list itself is capped at
# ``_MAX_OUTCOME_LIST_LEN`` so a caller cannot DoS the ledger by
# sending thousands of references per update.
_MAX_OUTCOME_LIST_LEN = 50
_MAX_OUTCOME_ENTRY_BYTES = 200


def detect_unsafe_update_payload(
    *,
    consolidation_session_id: str | None,
    reason: str | None,
    summary_text: str | None,
    evidence_refs: Any = None,
    generated_candidate_decision_ids: Any = None,
    promoted_formal_decision_ids: Any = None,
) -> tuple[bool, str | None]:
    """Return ``(unsafe, field_name)``. Used by the MCP update tool to
    reject raw token shapes and oversized narrative fields BEFORE they
    land on disk.

    KG-03A.3 rework: the outcome metadata fields
    (``evidence_refs``, ``generated_candidate_decision_ids``,
    ``promoted_formal_decision_ids``) are also validated. Each must be
    ``None`` or a list/tuple of bounded, non-empty, non-token-shape
    strings. Non-string entries, dicts, nested lists, empty strings and
    oversized strings all trip the guard (Codex audit val_44b86726).
    """

    for field_name, value in (
        ("consolidation_session_id", consolidation_session_id),
        ("reason", reason),
        ("summary_text", summary_text),
    ):
        if value is None:
            continue
        if not isinstance(value, str):
            return True, field_name
        if len(value.encode("utf-8")) > _MAX_UPDATE_NARRATIVE_BYTES:
            return True, field_name
        if _is_raw_token_shape(value):
            return True, field_name

    for field_name, value in (
        ("evidence_refs", evidence_refs),
        ("generated_candidate_decision_ids", generated_candidate_decision_ids),
        ("promoted_formal_decision_ids", promoted_formal_decision_ids),
    ):
        if value is None:
            continue
        # list/tuple only — strings (even if iterable) are NOT accepted.
        if not isinstance(value, (list, tuple)) or isinstance(value, (str, bytes)):
            return True, field_name
        if len(value) > _MAX_OUTCOME_LIST_LEN:
            return True, field_name
        for entry in value:
            if not isinstance(entry, str):
                return True, field_name
            stripped = entry.strip()
            if not stripped:
                return True, field_name
            if len(stripped.encode("utf-8")) > _MAX_OUTCOME_ENTRY_BYTES:
                return True, field_name
            if _is_raw_token_shape(stripped):
                return True, field_name
    return False, None


@dataclass(frozen=True, slots=True)
class MaterializeFromMarkerResult:
    """Frozen response for ``CognitiveConsolidationItemStore.materialize_from_marker``
    matching api_1ae54fa0.

    ``record_ref`` is the durable storage path of the per-generation file.
    ``item_count`` is the total number of items written/stored.
    ``counts`` is a per-``artifact_type`` breakdown used by reports + UI —
    it is response payload, NOT metric labels (or_3b71e3c1).
    ``outcome`` is the materialization outcome label that was emitted on
    ``kg_cognitive_pending_items_materialized_total``.
    """

    record_ref: str
    item_count: int
    counts: dict[str, int]
    outcome: str = CognitiveMaterializeOutcome.MATERIALIZED.value
    items: tuple["CognitiveConsolidationItem", ...] = field(default_factory=tuple)


# Fields exposed by the MCP / REST list contracts (api_ae3a932a +
# api_cce40fa6). The storage CognitiveConsolidationItem holds additional
# diagnostic fields (board_id, kg_generation_id, event_ref and free-text
# ``reason``) that the API surface intentionally hides.
_API_ITEM_FIELDS: tuple[str, ...] = (
    "item_id",
    "source_ref",
    "artifact_type",
    "status",
    "recorded_at",
    "updated_at",
    "updated_by_agent_id",
    "consolidation_session_id",
    "reason_code",
)


def project_item_for_api(
    item: "CognitiveConsolidationItem",
) -> dict[str, Any]:
    """Safe API projection of a storage item per api_ae3a932a + api_cce40fa6.

    Codex audit val_ead80fbd: the MCP/REST list response MUST NOT echo
    the storage free-text ``reason`` field (potentially free-text agent
    input). We expose the bounded ``reason_code`` only. Since S1 card
    9aeeaebd the storage item carries ``reason_code``, so this projection
    echoes ``item.reason_code`` (``None`` until the readiness service /
    update path sets a bounded code on a terminal transition).

    No board_id, kg_generation_id, event_ref or raw artifact metadata
    leaks through this projection (br_858a0859).
    """

    return {
        "item_id": item.item_id,
        "source_ref": item.source_ref,
        "artifact_type": item.artifact_type,
        "status": item.status,
        "recorded_at": item.recorded_at,
        "updated_at": item.updated_at,
        "updated_by_agent_id": item.updated_by_agent_id,
        "consolidation_session_id": item.consolidation_session_id,
        "reason_code": item.reason_code,
    }


def empty_status_counts() -> dict[str, int]:
    """Zero-filled bucket counts matching the contract counts shape.

    All five ``CognitiveItemStatus`` values plus ``total`` are present so
    the response is contract-aligned even when no items exist.
    """

    return {
        CognitiveItemStatus.PENDING.value: 0,
        CognitiveItemStatus.IN_PROGRESS.value: 0,
        CognitiveItemStatus.CONSOLIDATED.value: 0,
        CognitiveItemStatus.SKIPPED.value: 0,
        CognitiveItemStatus.FAILED.value: 0,
        "total": 0,
    }


def compute_status_counts(
    items: Sequence["CognitiveConsolidationItem"],
) -> dict[str, int]:
    """Tally items by status. Unknown statuses are still counted in
    ``total`` so the invariant ``total == len(items)`` always holds."""

    buckets = empty_status_counts()
    for item in items:
        if item.status in buckets:
            buckets[item.status] += 1
        buckets["total"] += 1
    return buckets


@dataclass(frozen=True, slots=True)
class CognitiveConsolidationItem:
    """Frozen per-item row in the cognitive consolidation ledger.

    Fields exposed via KG-03.2 MCP list tool + KG-03.4 REST read-only
    endpoint. Raw artifact body never lives here (br_858a0859 + FR9 +
    AC10) — only safe identifiers + lifecycle metadata.
    """

    item_id: str
    board_id: str
    kg_generation_id: str
    source_ref: str
    artifact_type: str
    status: str  # CognitiveItemStatus value
    recorded_at: str
    updated_at: str | None = None
    updated_by_agent_id: str | None = None
    consolidation_session_id: str | None = None
    reason: str | None = None
    event_ref: str | None = None
    # KG-03A.3 — outcome metadata for terminal consolidated transitions
    outcome_type: str | None = None  # CognitivePendingOutcomeType value
    evidence_refs: tuple[str, ...] = ()
    generated_candidate_decision_ids: tuple[str, ...] = ()
    promoted_formal_decision_ids: tuple[str, ...] = ()
    # KG-03A.7 — content hash propagated from BoardSourceStore so the
    # marker can dedupe across generations and detect ``reopen`` when the
    # underlying artifact's content changes between rebuilds.
    content_hash: str | None = None
    # S1 Cognitive Closure (fr_a74b3bc5) — readiness metadata persisted on the
    # EXISTING item (dec_effa4634: no parallel closure ledger). reason_code is
    # cognitive + closed-validated by the readiness service / later cards
    # (dec_25557d9a) — here the field merely exists. revisit_at is an ISO-8601
    # string so the frozen row stays JSON-serializable.
    reason_code: str | None = None
    justification: str | None = None
    actor: str | None = None
    revisit_at: str | None = None
    # source_ref_original preserves the untouched ref for audit; artifact_id is
    # the canonical per-artifact grouping key (fr_43ea6e97 / ac_50e4d48e). Both
    # default safely from source_ref in __post_init__ so legacy rows stay
    # readable WITHOUT a silent disk mutation (tr_3db366b6).
    source_ref_original: str | None = None
    artifact_id: str = ""

    def __post_init__(self) -> None:
        # In-memory safe defaults only — never written back by construction;
        # only mark/update persist (tr_3db366b6). frozen dataclass → setattr via
        # object to seed the derived identity fields when absent.
        if not self.source_ref_original:
            object.__setattr__(self, "source_ref_original", self.source_ref)
        if not self.artifact_id:
            object.__setattr__(
                self,
                "artifact_id",
                normalize_cognitive_artifact_id(self.source_ref),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "board_id": self.board_id,
            "kg_generation_id": self.kg_generation_id,
            "source_ref": self.source_ref,
            "artifact_type": self.artifact_type,
            "status": self.status,
            "recorded_at": self.recorded_at,
            "updated_at": self.updated_at,
            "updated_by_agent_id": self.updated_by_agent_id,
            "consolidation_session_id": self.consolidation_session_id,
            "reason": self.reason,
            "event_ref": self.event_ref,
            "outcome_type": self.outcome_type,
            "evidence_refs": list(self.evidence_refs),
            "generated_candidate_decision_ids": list(
                self.generated_candidate_decision_ids
            ),
            "promoted_formal_decision_ids": list(self.promoted_formal_decision_ids),
            "content_hash": self.content_hash,
            "reason_code": self.reason_code,
            "justification": self.justification,
            "actor": self.actor,
            "revisit_at": self.revisit_at,
            "source_ref_original": self.source_ref_original,
            "artifact_id": self.artifact_id,
        }

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "CognitiveConsolidationItem":
        def _tuple_or_empty(value: Any) -> tuple[str, ...]:
            if isinstance(value, (list, tuple)):
                return tuple(str(item) for item in value if item)
            return ()

        return CognitiveConsolidationItem(
            item_id=str(payload.get("item_id", "")),
            board_id=str(payload.get("board_id", "")),
            kg_generation_id=str(payload.get("kg_generation_id", "")),
            source_ref=str(payload.get("source_ref", "")),
            artifact_type=str(payload.get("artifact_type", "")),
            status=str(payload.get("status", CognitiveItemStatus.PENDING.value)),
            recorded_at=str(payload.get("recorded_at", "")),
            updated_at=payload.get("updated_at"),
            updated_by_agent_id=payload.get("updated_by_agent_id"),
            consolidation_session_id=payload.get("consolidation_session_id"),
            reason=payload.get("reason"),
            event_ref=payload.get("event_ref"),
            outcome_type=payload.get("outcome_type"),
            evidence_refs=_tuple_or_empty(payload.get("evidence_refs")),
            generated_candidate_decision_ids=_tuple_or_empty(
                payload.get("generated_candidate_decision_ids")
            ),
            promoted_formal_decision_ids=_tuple_or_empty(
                payload.get("promoted_formal_decision_ids")
            ),
            content_hash=payload.get("content_hash"),
            # S1: new readiness fields. Absent on legacy rows → safe defaults
            # (artifact_id/source_ref_original derived in __post_init__) without
            # rewriting the stored payload (tr_3db366b6).
            reason_code=payload.get("reason_code"),
            justification=payload.get("justification"),
            actor=payload.get("actor"),
            revisit_at=payload.get("revisit_at"),
            source_ref_original=payload.get("source_ref_original"),
            artifact_id=str(payload.get("artifact_id") or ""),
        )


@dataclass(frozen=True, slots=True)
class CognitiveConsolidationItemStore:
    """File-backed per-item ledger colocated with the KG-02.7 aggregate
    records (dec_580b6933 + tr_53caab33).

    Layout (extends the existing aggregate file, keeping one JSON per
    board+generation so backward compat with KG-02 readers is preserved):

        <base>/rebuild/audit/cognitive_pending/<board>/<gen>.json
        ├── pending_count  (aggregate, KG-02.7)
        ├── pending_refs   (aggregate, KG-02.7)
        ├── status         (aggregate, KG-02.7)
        ├── recorded_at    (aggregate, KG-02.7)
        └── items[]        (per-item, NEW KG-03.1)

    Writes are atomic via temp+replace (tr_746090f6). The `items` array
    is the ONLY mutable surface for cognitive consolidation state
    transitions; the aggregate fields above remain immutable after the
    marker writes them.
    """

    base_dir: object | None = None
    artifact_store: RebuildAuditArtifactStore | None = None
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    def _replace_record_with_overlay_revision(
        self,
        *,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        revisioned_replace = getattr(
            self.artifact_store, "replace_json_with_revision", None
        )
        implementation = getattr(
            type(self.artifact_store), "replace_json_with_revision", None
        )
        if (
            not callable(revisioned_replace)
            or implementation is RebuildAuditArtifactStore.replace_json_with_revision
        ):
            # Backward-compatible normal ledger operation for an older edition
            # adapter.  Mark the overlay explicitly *unfenced* so Global
            # Discovery recovery remains fail-closed until the adapter is
            # upgraded; never misrepresent three separate writes as atomic.
            revision_key = _cognitive_overlay_revision_key()
            pending, _stable = _next_cognitive_overlay_revision(
                self.artifact_store.read_json(revision_key)
            )
            unfenced = {
                **pending,
                "state": "unfenced",
                "reason": "revisioned_replace_unavailable",
            }
            self.artifact_store.write_json_atomic(revision_key, unfenced)
            target = self.artifact_store.replace_json(key, transform)
            self.artifact_store.write_json_atomic(revision_key, unfenced)
            return dict(target)
        target, _committed_revision = revisioned_replace(
            key=key,
            transform=transform,
            revision_key=_cognitive_overlay_revision_key(),
            revision_transition=_next_cognitive_overlay_revision,
        )
        return dict(target)

    @staticmethod
    def _record_key(board_id: str, kg_generation_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="cognitive_pending",
            board_id=board_id,
            kg_generation_id=kg_generation_id,
        )

    def load_record(
        self, board_id: str, kg_generation_id: str
    ) -> dict[str, Any] | None:
        """Read the aggregate + items record. Returns None if not found
        or unparseable."""

        try:
            return self.artifact_store.read_json(
                self._record_key(board_id, kg_generation_id)
            )
        except Exception as exc:
            logger.error(
                "kg.cognitive_item_store.read_failed board=%s gen=%s err=%s",
                board_id,
                kg_generation_id,
                exc,
            )
            return None

    def record_exists(self, board_id: str, kg_generation_id: str) -> bool:
        """True iff a ledger file is on disk for this generation. Used
        by MCP/REST adapters to distinguish ``generation_not_found`` from
        an empty-but-extant record (api_ae3a932a + api_cce40fa6)."""

        return self.artifact_store.exists(self._record_key(board_id, kg_generation_id))

    def is_legacy_record(self, board_id: str, kg_generation_id: str) -> bool:
        """True iff the persisted record predates KG-03.1 — i.e. it has
        the KG-02 aggregate ``pending_refs`` but lacks the ``items``
        array. Adapters surface this as ``legacy_mode=true`` so the UI
        can hint that statuses are synthesized rather than agent-tracked.
        """

        record = self.load_record(board_id, kg_generation_id)
        if record is None:
            return False
        return not isinstance(record.get("items"), list)

    def list_items(
        self,
        board_id: str,
        kg_generation_id: str,
        *,
        status_filter: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[CognitiveConsolidationItem]:
        """List items for a generation, optionally filtered by status.

        Backward compat (br_3d985533 + FR10 + AC7): if the record lacks
        an explicit ``items`` array (legacy KG-02 aggregate-only file),
        synthesize a list from ``pending_refs`` so MCP/REST callers
        receive a coherent view without crashing.

        Pagination is contract-aligned with api_ae3a932a / api_cce40fa6.
        ``offset`` is applied AFTER the status filter so the caller's
        page numbers refer to the filtered set. ``limit=None`` returns
        every remaining item past ``offset``.
        """

        record = self.load_record(board_id, kg_generation_id)
        if record is None:
            return []
        items_raw = record.get("items")
        if items_raw is None:
            # Legacy aggregate-only — synthesize from pending_refs.
            recorded_at = str(record.get("recorded_at", ""))
            event_ref = record.get("event_ref")
            synth: list[CognitiveConsolidationItem] = []
            for source_ref in record.get("pending_refs") or []:
                # Best-effort artifact_type extraction from source_ref
                # ("spec:1" → "spec"). Falls back to "unknown".
                artifact_type = "unknown"
                if isinstance(source_ref, str) and ":" in source_ref:
                    artifact_type = source_ref.split(":", 1)[0]
                synth.append(
                    CognitiveConsolidationItem(
                        item_id=compute_cognitive_item_id(
                            board_id, kg_generation_id, str(source_ref)
                        ),
                        board_id=board_id,
                        kg_generation_id=kg_generation_id,
                        source_ref=str(source_ref),
                        artifact_type=artifact_type,
                        status=CognitiveItemStatus.PENDING.value,
                        recorded_at=recorded_at,
                        event_ref=event_ref,
                    )
                )
            items = synth
        else:
            items = [
                CognitiveConsolidationItem.from_dict(it)
                for it in items_raw
                if isinstance(it, Mapping)
            ]
        if status_filter is not None:
            items = [i for i in items if i.status == status_filter]
        if offset:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        return items

    def _previous_terminal_state_by_source_ref(
        self,
        board_id: str,
        exclude_generation_id: str,
    ) -> dict[str, dict[str, Any]]:
        """KG-03A.7 — walk PRIOR generations newest→oldest and return
        the most recent terminal row per ``source_ref``.

        Excludes ``exclude_generation_id`` (the generation currently
        being materialized). Returns a mapping
        ``source_ref → terminal_row_dict`` carrying status +
        content_hash + the full item payload (so the caller can decide
        between dedupe-carry-forward and reopen).
        """

        terminal_statuses = {
            CognitiveItemStatus.CONSOLIDATED.value,
            CognitiveItemStatus.SKIPPED.value,
            CognitiveItemStatus.FAILED.value,
        }

        entries: list[tuple[str, str, dict[str, Any]]] = []
        records = self.artifact_store.list_json(
            RebuildAuditKey(
                namespace="cognitive_pending",
                board_id=board_id,
            )
        )
        for record in records:
            gen_id = str(record.get("kg_generation_id", ""))
            if not gen_id or gen_id == exclude_generation_id:
                continue
            recorded_at = str(record.get("recorded_at", ""))
            entries.append((recorded_at, gen_id, dict(record)))

        if not entries:
            return {}

        # Newest first; ties broken by lexically greater generation_id
        # (deterministic for UUID v4).
        entries.sort(key=lambda t: (t[0], t[1]), reverse=True)

        terminal_by_source: dict[str, dict[str, Any]] = {}
        for _, _, record in entries:
            raw_items = record.get("items")
            if not isinstance(raw_items, list):
                continue
            for raw in raw_items:
                if not isinstance(raw, Mapping):
                    continue
                status = raw.get("status")
                if status not in terminal_statuses:
                    continue
                src = str(raw.get("source_ref", ""))
                if not src or src in terminal_by_source:
                    continue
                terminal_by_source[src] = dict(raw)
        return terminal_by_source

    def latest_generation(self, board_id: str) -> str | None:
        """Per br_d510e1e3: deterministic latest-generation fallback.

        Reads recorded_at across all generation records and returns the
        generation_id with the most recent ISO8601 timestamp. Ties (same
        timestamp) are broken by lexically greater generation_id.
        """

        best: tuple[str, str] | None = None
        for record in self.artifact_store.list_json(
            RebuildAuditKey(
                namespace="cognitive_pending",
                board_id=board_id,
            )
        ):
            gen_id = str(record.get("kg_generation_id", ""))
            if not gen_id:
                continue
            recorded_at = str(record.get("recorded_at", ""))
            if best is None or (recorded_at, gen_id) > best:
                best = (recorded_at, gen_id)
        return best[1] if best else None

    def materialize_from_marker(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        event_ref: str,
        source_set: Sequence[Mapping[str, Any]],
        aggregate_status: str | None = None,
        recorded_at: str | None = None,
    ) -> MaterializeFromMarkerResult:
        """Public contract operation per api_1ae54fa0.

        Materializes one pending item per consolidable source from the
        rebuild marker into the per-generation ledger file. Returns the
        contract response shape: ``record_ref`` (durable file path),
        ``item_count`` (total items stored after the write), and
        ``counts`` (artifact_type breakdown for reports — NOT metric
        labels).

        Side-effect: emits EXACTLY ONE sample on
        ``kg_cognitive_pending_items_materialized_total`` per call, with
        labels ``(board_id, outcome)`` and ``item_count`` as the sample
        value (or_3b71e3c1).

        Replay safety: if a same-generation file already contains items
        in terminal MCP-owned status (``consolidated`` / ``skipped``),
        those statuses are PRESERVED across replays. Deterministic
        marker replay never erases an agent-owned terminal state by
        rebuilding a row back to ``pending`` (br_84194faf + br_f79887a3
        + tr_9521d8fd + AC18 + authoritative model).
        """

        resolved_recorded_at = (
            recorded_at
            if recorded_at is not None
            else datetime.now(timezone.utc).isoformat()
        )
        resolved_status = (
            aggregate_status
            if aggregate_status is not None
            else CognitivePendingStatus.PENDING_MARKED.value
        )

        try:
            item_count, items, counts, record_path = self._write_initial(
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                event_ref=event_ref,
                source_set=source_set,
                aggregate_status=resolved_status,
                recorded_at=resolved_recorded_at,
            )
        except Exception:
            # Counter contract: still emit one sample on failure so the
            # operator sees a failed materialization in the metric.
            _emit_materialized_sample(
                board_id=board_id,
                outcome=CognitiveMaterializeOutcome.FAILED.value,
                item_count=0,
            )
            raise

        outcome = (
            CognitiveMaterializeOutcome.MATERIALIZED.value
            if item_count > 0
            else CognitiveMaterializeOutcome.EMPTY.value
        )
        _emit_materialized_sample(
            board_id=board_id,
            outcome=outcome,
            item_count=item_count,
        )

        return MaterializeFromMarkerResult(
            record_ref=str(record_path),
            item_count=item_count,
            counts=dict(counts),
            outcome=outcome,
            items=tuple(items),
        )

    def _write_initial(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        event_ref: str,
        source_set: Sequence[Mapping[str, Any]],
        aggregate_status: str,
        recorded_at: str,
    ) -> tuple[
        int,
        list[CognitiveConsolidationItem],
        dict[str, int],
        str,
    ]:
        """Internal helper that performs the atomic write. Returns
        ``(item_count, items, counts_by_type, record_path)``.

        Per-item status is ALWAYS ``pending`` for newly-created rows
        because rebuild can only OPEN cognitive debt (br_84194faf +
        br_f79887a3 + tr_9521d8fd + AC18). Existing terminal rows
        (``consolidated`` / ``skipped``) are preserved verbatim so
        deterministic replay does not erase agent-owned state.
        """

        existing_by_source: dict[str, dict[str, Any]] = {}
        existing_terminal_by_source: dict[str, dict[str, Any]] = {}
        existing_record = self.load_record(board_id, kg_generation_id)
        if existing_record is not None:
            raw_items = existing_record.get("items")
            if isinstance(raw_items, list):
                terminal_statuses = {
                    CognitiveItemStatus.CONSOLIDATED.value,
                    CognitiveItemStatus.SKIPPED.value,
                }
                for raw in raw_items:
                    if not isinstance(raw, Mapping):
                        continue
                    status = raw.get("status")
                    src = str(raw.get("source_ref", ""))
                    if src:
                        existing_by_source[src] = dict(raw)
                        if status in terminal_statuses:
                            existing_terminal_by_source[src] = dict(raw)

        # KG-03A.7 — cross-generation terminal lookup. Used to:
        #   1. carry forward terminal items when content_hash matches
        #      (dedupe rebuild noise);
        #   2. emit ``kg_cognitive_pending_reopen_total`` with
        #      reason_code=content_changed when the content hash differs.
        prior_terminal_by_source = (
            self._previous_terminal_state_by_source_ref(
                board_id,
                exclude_generation_id=kg_generation_id,
            )
            if not existing_terminal_by_source
            else {}
        )

        items: list[CognitiveConsolidationItem] = []
        counts_by_type: dict[str, int] = {}
        incoming_source_refs: set[str] = set()
        for row in source_set:
            artifact_type = str(row.get("artifact_type", ""))
            if artifact_type not in CONSOLIDABLE_ARTIFACT_TYPES:
                continue
            source_ref = str(row.get("source_ref", row.get("id", "")))
            if not source_ref:
                continue
            incoming_source_refs.add(source_ref)

            row_content_hash = row.get("content_hash")
            row_content_hash = str(row_content_hash) if row_content_hash else None

            terminal = existing_terminal_by_source.get(source_ref)
            if terminal is not None:
                items.append(CognitiveConsolidationItem.from_dict(terminal))
            elif source_ref in prior_terminal_by_source:
                prior = prior_terminal_by_source[source_ref]
                prior_hash = prior.get("content_hash")
                prior_hash_str = str(prior_hash) if prior_hash else None
                if (
                    row_content_hash is not None
                    and prior_hash_str is not None
                    and row_content_hash == prior_hash_str
                ):
                    # Dedupe: content unchanged since the prior terminal
                    # transition. Carry the terminal row forward verbatim
                    # so the new generation does NOT re-open pending debt.
                    items.append(CognitiveConsolidationItem.from_dict(prior))
                else:
                    # Reopen: source_ref has a terminal prior, but the
                    # content has changed (or one of the hashes is
                    # absent → conservative reopen). Emit the bounded
                    # reopen counter so operators can see the cycle.
                    item_id = compute_cognitive_item_id(
                        board_id, kg_generation_id, source_ref
                    )
                    reopened = CognitiveConsolidationItem(
                        item_id=item_id,
                        board_id=board_id,
                        kg_generation_id=kg_generation_id,
                        source_ref=source_ref,
                        artifact_type=artifact_type,
                        status=CognitiveItemStatus.PENDING.value,
                        recorded_at=recorded_at,
                        event_ref=event_ref,
                        content_hash=row_content_hash,
                    )
                    from okto_pulse.core.kg.refinement_cognitive_guard import (
                        assert_deterministic_only_pending,
                    )

                    assert_deterministic_only_pending(
                        board_id=board_id, items=[reopened]
                    )
                    items.append(reopened)
                    _emit_reopen_sample(
                        entity_type=artifact_type,
                        outcome=CognitivePendingReopenOutcome.SUCCESS.value,
                        reason_code=(
                            CognitivePendingReopenReasonCode.CONTENT_CHANGED.value
                        ),
                    )
            else:
                item_id = compute_cognitive_item_id(
                    board_id, kg_generation_id, source_ref
                )
                new_item = CognitiveConsolidationItem(
                    item_id=item_id,
                    board_id=board_id,
                    kg_generation_id=kg_generation_id,
                    source_ref=source_ref,
                    artifact_type=artifact_type,
                    status=CognitiveItemStatus.PENDING.value,
                    recorded_at=recorded_at,
                    event_ref=event_ref,
                    content_hash=row_content_hash,
                )
                # KG-03.7 + tr_9521d8fd + ir_dbf036d8 — defensive guard:
                # deterministic write paths MUST only set ``pending``.
                # The check is INTENTIONALLY against the NEWLY-CREATED
                # item (not the replay-preserved terminal ones, which
                # are MCP-owned). A future bug that flips this status
                # value fires the alert + raises so the corruption is
                # caught at the storage boundary.
                from okto_pulse.core.kg.refinement_cognitive_guard import (
                    assert_deterministic_only_pending,
                )

                assert_deterministic_only_pending(board_id=board_id, items=[new_item])
                items.append(new_item)
            counts_by_type[artifact_type] = counts_by_type.get(artifact_type, 0) + 1

        # Live closeout handlers append one source at a time to the current KG
        # generation. Keep unrelated rows already present in that generation;
        # otherwise opening bug B silently erases pending/in-progress work for
        # bug A. Rows replayed in the incoming set retain the existing terminal
        # ownership rules above.
        for source_ref, raw in existing_by_source.items():
            if source_ref not in incoming_source_refs:
                items.append(CognitiveConsolidationItem.from_dict(raw))

        with self._lock:
            active_refs = sorted(
                {i.source_ref for i in items if i.status in ACTIVE_ITEM_STATUSES}
            )
            payload = {
                "board_id": board_id,
                "kg_generation_id": kg_generation_id,
                "event_ref": event_ref,
                "pending_count": len(active_refs),
                "pending_refs": active_refs,
                "status": aggregate_status,
                "recorded_at": recorded_at,
                "items": [i.to_dict() for i in items],
            }
            key = self._record_key(board_id, kg_generation_id)
            self._replace_record_with_overlay_revision(
                key=key,
                transform=lambda _current: payload,
            )
            record_ref = self.artifact_store.reference(key)

        return len(items), items, counts_by_type, record_ref

    def update_item(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        item_id: str,
        new_status: str,
        updated_by_agent_id: str,
        consolidation_session_id: str | None = None,
        reason: str | None = None,
        outcome_type: str | None = None,
        evidence_refs: Sequence[str] | None = None,
        generated_candidate_decision_ids: Sequence[str] | None = None,
        promoted_formal_decision_ids: Sequence[str] | None = None,
        reason_code: str | None = None,
        justification: str | None = None,
        actor: str | None = None,
        revisit_at: str | None = None,
        clear_readiness_metadata: bool = False,
    ) -> CognitiveConsolidationItem | None:
        """Single-item atomic update per br_d544da65 + FR3 + AC6.

        Returns the updated item, or None if the item_id was not found
        in the ledger. Status validation + reason/session requirements
        are enforced by the KG-03.3 MCP update tool — this method is
        the storage primitive that the tool drives.

        KG-03A.3 extension: ``outcome_type``, ``evidence_refs``,
        ``generated_candidate_decision_ids`` and
        ``promoted_formal_decision_ids`` carry the auditable terminal
        outcome metadata when ``new_status == "consolidated"``. The
        MCP tool enforces presence/shape (br_7500e5f9 + tr_16ec917c);
        this primitive only persists what it receives.
        """

        with self._lock:
            record = self.load_record(board_id, kg_generation_id)
            if record is None:
                return None
            items_raw = record.get("items")
            if items_raw is None:
                # Legacy aggregate-only — synthesize items first so the
                # update can land somewhere (br_3d985533 — we MUST stay
                # readable AND mutable on legacy records).
                items_raw = []
                for source_ref in record.get("pending_refs") or []:
                    artifact_type = "unknown"
                    if isinstance(source_ref, str) and ":" in source_ref:
                        artifact_type = source_ref.split(":", 1)[0]
                    items_raw.append(
                        {
                            "item_id": compute_cognitive_item_id(
                                board_id, kg_generation_id, str(source_ref)
                            ),
                            "board_id": board_id,
                            "kg_generation_id": kg_generation_id,
                            "source_ref": str(source_ref),
                            "artifact_type": artifact_type,
                            "status": CognitiveItemStatus.PENDING.value,
                            "recorded_at": str(record.get("recorded_at", "")),
                            "event_ref": record.get("event_ref"),
                        }
                    )

            target_idx = None
            for idx, it in enumerate(items_raw):
                if it.get("item_id") == item_id:
                    target_idx = idx
                    break
            if target_idx is None:
                return None

            now = datetime.now(timezone.utc).isoformat()
            evidence_refs_list = list(evidence_refs) if evidence_refs else []
            generated_candidates_list = (
                list(generated_candidate_decision_ids)
                if generated_candidate_decision_ids
                else []
            )
            promoted_decisions_list = (
                list(promoted_formal_decision_ids)
                if promoted_formal_decision_ids
                else []
            )
            existing = items_raw[target_idx]
            src_ref = str(existing.get("source_ref", ""))
            items_raw[target_idx] = {
                **existing,
                "status": new_status,
                "updated_at": now,
                "updated_by_agent_id": updated_by_agent_id,
                "consolidation_session_id": consolidation_session_id,
                "reason": reason,
                "outcome_type": outcome_type,
                "evidence_refs": evidence_refs_list,
                "generated_candidate_decision_ids": generated_candidates_list,
                "promoted_formal_decision_ids": promoted_decisions_list,
                # S1: canonical artifact identity persisted on this EXPLICIT
                # mutation (not a silent read-time write) + readiness metadata
                # preserved when the caller omits it, so a concurrent update
                # never drops reason_code/justification/actor/revisit_at
                # (tr_3d6b29fe). The lock + temp-replace below keep it atomic.
                "source_ref_original": existing.get("source_ref_original") or src_ref,
                "artifact_id": existing.get("artifact_id")
                or normalize_cognitive_artifact_id(src_ref),
                # S3.2: an explicit clear/reopen (``clear_readiness_metadata``)
                # DROPS the stale skip metadata; otherwise the preserve-when-None
                # default holds (tr_3d6b29fe) so a concurrent update never loses it.
                "reason_code": (
                    None
                    if clear_readiness_metadata
                    else reason_code
                    if reason_code is not None
                    else existing.get("reason_code")
                ),
                "justification": (
                    None
                    if clear_readiness_metadata
                    else justification
                    if justification is not None
                    else existing.get("justification")
                ),
                "actor": actor if actor is not None else existing.get("actor"),
                "revisit_at": (
                    None
                    if clear_readiness_metadata
                    else revisit_at
                    if revisit_at is not None
                    else existing.get("revisit_at")
                ),
            }

            # Recompute aggregate counts from items (br_d544da65 keeps
            # aggregate in sync without the caller needing to remember).
            active_refs = sorted(
                {
                    it["source_ref"]
                    for it in items_raw
                    if it.get("status") in ACTIVE_ITEM_STATUSES
                }
            )
            record["items"] = items_raw
            record["pending_count"] = len(active_refs)
            record["pending_refs"] = active_refs

            key = self._record_key(board_id, kg_generation_id)
            self._replace_record_with_overlay_revision(
                key=key,
                transform=lambda _current: record,
            )

            # NOTE (Codex audit val_036cb81e):
            # update_item MUST NOT emit on
            # ``kg_cognitive_pending_items_materialized_total`` — that is
            # the materialization metric. Update-side metrics belong to
            # KG-03.3 (``kg_cognitive_item_update_total``), implemented
            # by the MCP update tool that wraps this primitive.
            return CognitiveConsolidationItem.from_dict(items_raw[target_idx])


def _cognitive_hold_artifact_type(
    hold_payload: Mapping[str, Any], source_ref: str
) -> str:
    """Resolve a store-acceptable artifact_type for an R7 hold row.

    Prefers the payload's inferred type; falls back to the bug:* heuristic
    (a working-only canonical Learning hold is always bug-derived)."""
    declared = str(hold_payload.get("artifact_type") or "")
    if declared in CONSOLIDABLE_ARTIFACT_TYPES:
        return declared
    if (
        source_ref.startswith("bug:")
        or source_ref.startswith("card:bug:")
        or ":bug:" in source_ref
    ):
        return "bug"
    return declared


def record_cognitive_working_only_hold(
    *,
    board_id: str,
    hold_payload: Mapping[str, Any],
    actor_id: str,
    base_dir: object | None = None,
    artifact_store: RebuildAuditArtifactStore | None = None,
) -> dict[str, str] | None:
    """Persist an R7 working-only canonical Learning go-forward HOLD as a
    cognitive pending item (NEVER CanonicalDebt / DLQ / a parallel store).

    Reuses the existing CognitiveConsolidationItemStore: it materializes one
    pending row for the held source and stamps the R7 ``reason_code`` via
    ``update_item``. The store is file-backed (rebuild base dir), so this needs
    no SQL db and is safe to call from any caller that catches the structured
    ``KGPrimitiveError`` (MCP commit tool, live consolidation, adapters).

    Generation resolution follows the read-side fallback chain and never
    promotes the ``current`` pointer because of a live hold:
    ``RebuildAuditKGGenerationRepository.get_current`` ->
    ``store.latest_generation`` -> ``generate_kg_generation_id`` (first live
    ledger). A legacy ``KGGenerationRepository`` fallback remains only when the
    runtime registry is unavailable in tests/legacy callsites.

    Returns ``{generation_id, item_id, artifact_type}`` on success, or None
    when the payload is unusable / the artifact_type is not consolidable (the
    structured error has already surfaced to the caller in that case).
    """
    source_ref = str(hold_payload.get("source_ref") or "")
    reason_code = str(hold_payload.get("reason_code") or "")
    if not source_ref or not reason_code:
        return None
    artifact_type = _cognitive_hold_artifact_type(hold_payload, source_ref)
    if artifact_type not in CONSOLIDABLE_ARTIFACT_TYPES:
        # The store silently skips a non-consolidable row; do not pretend a hold
        # was recorded. The caller still surfaces the structured error.
        logger.warning(
            "kg.r7_hold.non_consolidable_artifact board=%s source_ref=%s type=%s",
            board_id,
            source_ref,
            artifact_type,
        )
        return None

    from okto_pulse.core.kg.rebuild_generation import (
        RebuildAuditKGGenerationRepository,
        generate_kg_generation_id,
    )

    resolved_store = resolve_rebuild_audit_artifact_store(
        base_dir=base_dir,
        artifact_store=artifact_store,
    )

    store = CognitiveConsolidationItemStore(
        base_dir=base_dir,
        artifact_store=resolved_store,
    )
    current_generation_id = RebuildAuditKGGenerationRepository(
        artifact_store=resolved_store
    ).get_current(board_id)
    generation_id = (
        current_generation_id
        or store.latest_generation(board_id)
        or generate_kg_generation_id()
    )
    session_id = str(hold_payload.get("session_id") or "")
    store.materialize_from_marker(
        board_id=board_id,
        kg_generation_id=generation_id,
        event_ref=f"r7_cognitive_hold:{session_id}",
        source_set=[{"source_ref": source_ref, "artifact_type": artifact_type}],
    )
    item_id = compute_cognitive_item_id(board_id, generation_id, source_ref)
    updated = store.update_item(
        board_id=board_id,
        kg_generation_id=generation_id,
        item_id=item_id,
        new_status=CognitiveItemStatus.PENDING.value,
        updated_by_agent_id=actor_id,
        consolidation_session_id=session_id or None,
        reason_code=reason_code,
        reason=(
            "R7: canonical Learning held — bug evidence is working-only "
            "(awaiting canonical Bug)."
        ),
    )
    if updated is None:
        return None
    return {
        "generation_id": generation_id,
        "item_id": item_id,
        "artifact_type": artifact_type,
    }


# ---------------------------------------------------------------------------
# Cognitive pending marker (KG-02.7) — now integrates with item store
# ---------------------------------------------------------------------------


class CognitivePendingStatus(str, Enum):
    """Bounded statuses for api_3e9d65ce response.

    ``COMPLETED`` is INTENTIONALLY ABSENT — br_0d710a8f forbids a
    structural rebuild from claiming cognitive consolidation complete.
    """

    PENDING_MARKED = "pending_marked"
    SKIPPED = "skipped"


class CognitiveMarkerErrorCode(str, Enum):
    COGNITIVE_MARKER_UNAVAILABLE = "cognitive_marker_unavailable"
    INVALID_GENERATION = "invalid_generation"


# Artifact types that need cognitive judgement. Refinements and decisions are
# semantic-only; specs/tasks/tests/bugs are both deterministic rebuild sources
# and semantic cognitive sources. Sources outside this set are skipped silently
# — the count returned to the caller reflects only what would be queued for the
# agent.
CONSOLIDABLE_ARTIFACT_TYPES: frozenset[str] = frozenset(
    {
        "spec",
        "decision",
        "refinement",
        "task",
        "test",
        "bug",
    }
)


@dataclass(frozen=True, slots=True)
class PendingMarkResult:
    """Frozen response for ``CognitivePendingMarker.mark_for_generation``
    matching api_3e9d65ce."""

    pending_count: int
    status: str  # CognitivePendingStatus value
    board_id: str
    kg_generation_id: str
    event_ref: str
    pending_refs: tuple[str, ...] = field(default_factory=tuple)
    record_ref: str | None = None
    error_code: str | None = None
    detail: str | None = None


_PENDING_LABELS = ("board_id", "status")
_pending_counter = runtime_state("kg.rebuild_audit.pending_counter", dict)
_pending_counter_lock = runtime_lock("kg.rebuild_audit.pending_counter")


def _bump_pending(*, board_id: str, status: str, count: int = 1) -> None:
    key = (board_id, status)
    with _pending_counter_lock:
        _pending_counter[key] = _pending_counter.get(key, 0) + count


def get_pending_count(board_id: str, *, status: str | None = None) -> int:
    with _pending_counter_lock:
        total = 0
        for (b, st), value in _pending_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            total += value
        return total


def get_pending_counter_labels() -> tuple[str, ...]:
    return _PENDING_LABELS


def get_pending_samples() -> list[dict[str, Any]]:
    with _pending_counter_lock:
        return [
            {"board_id": b, "status": st, "count": value}
            for (b, st), value in _pending_counter.items()
        ]


def reset_pending_counter() -> None:
    with _pending_counter_lock:
        _pending_counter.clear()


CognitivePendingAdapter = Callable[[str, str, tuple[Mapping[str, Any], ...]], int]


def _default_pending_adapter(
    _board_id: str,
    _kg_generation_id: str,
    sources: tuple[Mapping[str, Any], ...],
) -> int:
    """Default pending adapter — counts consolidable artifacts and
    returns the would-be pending count. The real cognitive module is
    wired in production; this implementation keeps the deterministic
    pending count visible in the report drilldown."""

    return sum(
        1
        for row in sources
        if str(row.get("artifact_type", "")) in CONSOLIDABLE_ARTIFACT_TYPES
    )


@dataclass(frozen=True, slots=True)
class CognitivePendingMarker:
    """Marks eligible artifacts as pending cognitive consolidation for
    the new KG generation. NEVER marks completed (br_0d710a8f + TR9).
    """

    base_dir: object | None = None
    pending_adapter: CognitivePendingAdapter = _default_pending_adapter
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    def mark_for_generation(
        self,
        *,
        board_id: str,
        kg_generation_id: str,
        source_set: Sequence[Mapping[str, Any]],
        event_ref: str,
    ) -> PendingMarkResult:
        from okto_pulse.core.kg.rebuild_generation import (
            is_valid_kg_generation_id,
        )

        if not is_valid_kg_generation_id(kg_generation_id):
            _bump_pending(
                board_id=board_id,
                status=CognitivePendingStatus.SKIPPED.value,
            )
            return PendingMarkResult(
                pending_count=0,
                status=CognitivePendingStatus.SKIPPED.value,
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                event_ref=event_ref,
                error_code=CognitiveMarkerErrorCode.INVALID_GENERATION.value,
                detail="kg_generation_id is not a canonical UUID v4 string",
            )

        rows = tuple(source_set)
        consolidable_refs = tuple(
            sorted(
                str(row.get("source_ref", row.get("id", "")))
                for row in rows
                if str(row.get("artifact_type", "")) in CONSOLIDABLE_ARTIFACT_TYPES
            )
        )

        try:
            adapter_count = int(self.pending_adapter(board_id, kg_generation_id, rows))
        except Exception as exc:
            logger.error(
                "kg.cognitive_pending.adapter_failed board=%s gen=%s err=%s",
                board_id,
                kg_generation_id,
                exc,
            )
            _bump_pending(
                board_id=board_id,
                status=CognitivePendingStatus.SKIPPED.value,
            )
            return PendingMarkResult(
                pending_count=0,
                status=CognitivePendingStatus.SKIPPED.value,
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                event_ref=event_ref,
                error_code=CognitiveMarkerErrorCode.COGNITIVE_MARKER_UNAVAILABLE.value,
                detail=f"adapter_exception={type(exc).__name__}",
            )

        pending_count = max(0, adapter_count)
        status_value = (
            CognitivePendingStatus.PENDING_MARKED.value
            if pending_count > 0
            else CognitivePendingStatus.SKIPPED.value
        )

        # KG-03.1 — drive the per-item ledger through the api_1ae54fa0
        # public contract operation ``materialize_from_marker`` instead
        # of the internal helper (Codex audit val_036cb81e). The store
        # emits the single ``kg_cognitive_pending_items_materialized_total``
        # sample with labels (board_id, outcome) and item_count value.
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            item_store = CognitiveConsolidationItemStore(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            )
            materialization = item_store.materialize_from_marker(
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                event_ref=event_ref,
                source_set=rows,
                aggregate_status=status_value,
                recorded_at=recorded_at,
            )
            # Sanity: materialized count must match adapter_count when the
            # default adapter (counts CONSOLIDABLE_ARTIFACT_TYPES) is wired.
            # If a custom adapter overrides this we trust the adapter and
            # keep pending_count for compatibility with KG-02.7.
            if materialization.item_count != pending_count and pending_count > 0:
                logger.warning(
                    "kg.cognitive_pending.materialized_mismatch board=%s gen=%s "
                    "adapter_count=%d materialized=%d",
                    board_id,
                    kg_generation_id,
                    pending_count,
                    materialization.item_count,
                )
        except Exception as exc:
            logger.error(
                "kg.cognitive_pending.record_failed board=%s gen=%s err=%s",
                board_id,
                kg_generation_id,
                exc,
            )
            _bump_pending(
                board_id=board_id,
                status=CognitivePendingStatus.SKIPPED.value,
            )
            return PendingMarkResult(
                pending_count=pending_count,
                status=CognitivePendingStatus.SKIPPED.value,
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                event_ref=event_ref,
                pending_refs=consolidable_refs,
                error_code=CognitiveMarkerErrorCode.COGNITIVE_MARKER_UNAVAILABLE.value,
                detail=f"record_exception={type(exc).__name__}",
            )

        _bump_pending(
            board_id=board_id,
            status=status_value,
            count=max(1, pending_count)
            if status_value == CognitivePendingStatus.PENDING_MARKED.value
            else 1,
        )
        return PendingMarkResult(
            pending_count=pending_count,
            status=status_value,
            board_id=board_id,
            kg_generation_id=kg_generation_id,
            event_ref=event_ref,
            pending_refs=consolidable_refs,
            record_ref=materialization.record_ref,
        )


# ---------------------------------------------------------------------------
# Confirmation consumption audit recorder
# ---------------------------------------------------------------------------


class ConfirmationAuditOutcome(str, Enum):
    """Bounded outcomes for api_c9bc9a8c request_body ``outcome``."""

    CONSUMED = "consumed"
    EXPIRED = "expired"
    REPLAYED = "replayed"
    SCOPE_MISMATCH = "scope_mismatch"
    MISSING = "missing"


class ConfirmationAuditErrorCode(str, Enum):
    AUDIT_STORE_UNAVAILABLE = "audit_store_unavailable"
    UNSAFE_AUDIT_PAYLOAD = "unsafe_audit_payload"
    INVALID_OPERATION = "invalid_operation"
    INVALID_OUTCOME = "invalid_outcome"


CANONICAL_AUDIT_OPERATIONS: frozenset[str] = frozenset(
    {
        "reset",
        "quarantine",
        "rebuild",
        "promote",
        "rollback",
        "reindex_discovery",
    }
)


@dataclass(frozen=True, slots=True)
class ConfirmationAuditResult:
    """Frozen response for ``ConfirmationConsumptionAuditRecorder.record``
    matching api_c9bc9a8c."""

    audit_ref: str | None
    recorded_at: str | None
    outcome: str
    error_code: str | None = None
    detail: str | None = None


_AUDIT_LABELS = ("board_id", "operation", "outcome")
_audit_counter = runtime_state("kg.rebuild_audit.audit_counter", dict)
_audit_counter_lock = runtime_lock("kg.rebuild_audit.audit_counter")


def _bump_audit(*, board_id: str, operation: str, outcome: str) -> None:
    key = (board_id, operation, outcome)
    with _audit_counter_lock:
        _audit_counter[key] = _audit_counter.get(key, 0) + 1


def get_audit_count(
    board_id: str,
    *,
    operation: str | None = None,
    outcome: str | None = None,
) -> int:
    with _audit_counter_lock:
        total = 0
        for (b, op, oc), value in _audit_counter.items():
            if b != board_id:
                continue
            if operation is not None and op != operation:
                continue
            if outcome is not None and oc != outcome:
                continue
            total += value
        return total


def get_audit_counter_labels() -> tuple[str, ...]:
    return _AUDIT_LABELS


def get_audit_samples() -> list[dict[str, Any]]:
    with _audit_counter_lock:
        return [
            {"board_id": b, "operation": op, "outcome": oc, "count": value}
            for (b, op, oc), value in _audit_counter.items()
        ]


def reset_audit_counter() -> None:
    with _audit_counter_lock:
        _audit_counter.clear()


# Patterns that disqualify a value from landing in the audit row. The
# token must NEVER appear verbatim (api_c9bc9a8c unsafe_audit_payload).
_RAW_TOKEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^conf_[A-Za-z0-9_\-]{16,}$"),  # RebuildConfirmationStore id shape
    re.compile(r"^tok_[A-Za-z0-9_\-]{16,}$"),  # generic token shape
)

# val_302bdec8 — leaf VALUES that begin with this prefix are the safe
# fingerprint produced by ``confirmation_fingerprint()`` and MUST NOT
# trip the raw-token check.
_SAFE_FINGERPRINT_PREFIX = "conf_fp_"

# Allowlist for canonical safe field names whose values are vetted by
# construction (fingerprints, opaque refs). These keys never count as
# "credential-shaped" even if the leaf value contains a long string.
_ALLOWLISTED_AUDIT_KEYS: frozenset[str] = frozenset(
    {
        "confirmation_ref",  # always conf_fp_<sha256>
    }
)


def _is_raw_token_shape(value: str) -> bool:
    if value.startswith(_SAFE_FINGERPRINT_PREFIX):
        return False
    return any(pat.match(value) for pat in _RAW_TOKEN_PATTERNS)


def _audit_payload_is_safe(
    payload: Mapping[str, Any],
) -> tuple[bool, str | None]:
    """Walk the payload and reject if any leaf looks like a raw
    confirmation token shape (``conf_<urlsafe>`` / ``tok_<urlsafe>``)
    or carries a name that suggests credential material. Returns
    ``(True, None)`` if safe, else ``(False, reason)``."""

    suspect_key_pat = re.compile(
        r"(?i)password|secret|api[_-]?key|bearer|raw[_-]?token"
    )

    def _walk(node: Any, path: tuple[str, ...]) -> tuple[bool, str | None]:
        if isinstance(node, Mapping):
            for key, value in node.items():
                key_str = str(key)
                if key_str not in _ALLOWLISTED_AUDIT_KEYS and suspect_key_pat.search(
                    key_str
                ):
                    return False, ".".join((*path, key_str))
                ok, reason = _walk(value, (*path, key_str))
                if not ok:
                    return False, reason
        elif isinstance(node, (list, tuple)):
            for idx, item in enumerate(node):
                ok, reason = _walk(item, (*path, f"[{idx}]"))
                if not ok:
                    return False, reason
        elif isinstance(node, str):
            if _is_raw_token_shape(node):
                return False, ".".join(path) or "<value>"
        return True, None

    return _walk(payload, ())


@dataclass(frozen=True, slots=True)
class ConfirmationConsumptionAuditRecorder:
    """Records a durable safe audit row for every confirmation token
    consumption attempt. Enforces api_c9bc9a8c semantics:

    * operation MUST be in CANONICAL_AUDIT_OPERATIONS.
    * outcome MUST be in ConfirmationAuditOutcome.
    * payload MUST NOT contain raw token shapes or credential-like keys.
    * audit row persisted atomically before the counter bump.
    """

    base_dir: object | None = None
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    @staticmethod
    def _audit_key(board_id: str, audit_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="confirmation_audit",
            board_id=board_id,
            artifact_id=audit_id,
        )

    def record(
        self,
        *,
        board_id: str,
        operation: str,
        outcome: str,
        reason: str,
        actor_ref: str,
        preflight_hash: str | None = None,
        generation_ids: Mapping[str, Any] | None = None,
        affected_files: Sequence[str] | None = None,
        confirmation_ref: str | None = None,
    ) -> ConfirmationAuditResult:
        """Persist a safe audit row.

        ``confirmation_ref`` is the opaque fingerprint produced by
        :func:`confirmation_fingerprint`. It correlates the row with the
        consumed token WITHOUT carrying the raw value (br_d379c40d).
        ``actor_ref`` is similarly expected to be an opaque reference,
        not the raw actor_id when the actor identity is sensitive.
        """

        if operation not in CANONICAL_AUDIT_OPERATIONS:
            return ConfirmationAuditResult(
                audit_ref=None,
                recorded_at=None,
                outcome=outcome,
                error_code=ConfirmationAuditErrorCode.INVALID_OPERATION.value,
                detail=(
                    f"operation={operation!r} not in "
                    f"{sorted(CANONICAL_AUDIT_OPERATIONS)}"
                ),
            )
        try:
            ConfirmationAuditOutcome(outcome)
        except ValueError:
            return ConfirmationAuditResult(
                audit_ref=None,
                recorded_at=None,
                outcome=outcome,
                error_code=ConfirmationAuditErrorCode.INVALID_OUTCOME.value,
                detail=f"outcome={outcome!r} not in ConfirmationAuditOutcome",
            )

        payload = {
            "board_id": board_id,
            "operation": operation,
            "outcome": outcome,
            "reason": reason,
            "actor_ref": actor_ref,
            "preflight_hash": preflight_hash,
            "generation_ids": dict(generation_ids or {}),
            "affected_files": list(affected_files or ()),
            "confirmation_ref": confirmation_ref,
        }
        safe, reason_safe = _audit_payload_is_safe(payload)
        if not safe:
            logger.warning(
                "kg.confirmation_audit.unsafe_payload board=%s op=%s path=%s",
                board_id,
                operation,
                reason_safe,
            )
            return ConfirmationAuditResult(
                audit_ref=None,
                recorded_at=None,
                outcome=outcome,
                error_code=ConfirmationAuditErrorCode.UNSAFE_AUDIT_PAYLOAD.value,
                detail=f"unsafe_field={reason_safe}",
            )

        try:
            audit_id = f"audit_{uuid.uuid4().hex}"
            recorded_at = datetime.now(timezone.utc).isoformat()
            record_payload = {
                "audit_id": audit_id,
                "recorded_at": recorded_at,
                **payload,
            }
            audit_key = self._audit_key(board_id, audit_id)
            self.artifact_store.write_json_atomic(audit_key, record_payload)
            audit_ref = self.artifact_store.reference(audit_key)
        except Exception as exc:
            logger.error(
                "kg.confirmation_audit.persist_failed board=%s op=%s err=%s",
                board_id,
                operation,
                exc,
            )
            return ConfirmationAuditResult(
                audit_ref=None,
                recorded_at=None,
                outcome=outcome,
                error_code=ConfirmationAuditErrorCode.AUDIT_STORE_UNAVAILABLE.value,
                detail=f"persist_exception={type(exc).__name__}",
            )

        _bump_audit(board_id=board_id, operation=operation, outcome=outcome)
        return ConfirmationAuditResult(
            audit_ref=audit_ref,
            recorded_at=recorded_at,
            outcome=outcome,
        )


# ---------------------------------------------------------------------------
# Composer: kg.rebuilt event -> publisher -> cognitive pending marker
# ---------------------------------------------------------------------------


# Resolver signature: receives the rebuild event payload and returns the
# source set the cognitive marker needs to enqueue per artifact. The
# event payload includes ``board_id``, ``kg_generation_id`` and
# ``manifest_ref`` so the resolver can load sources from the manifest
# store deterministically. Returning ``()`` is legitimate (rebuild of an
# empty board); raising propagates as a marker SKIPPED outcome.
SourceSetResolver = Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True, slots=True)
class KGRebuiltHandlerResult:
    """Diagnostic record produced per event_emitter invocation. The
    handler returns this so integration tests can assert wiring
    behaviour without inspecting global counters."""

    publish: EventPublishResult
    mark: PendingMarkResult | None
    skipped_reason: str | None = None

    @property
    def accepted(self) -> bool:
        """Prove the complete terminal delivery, not only event publication.

        A non-promoted terminal outcome intentionally has no generation to
        mark, so accepted publication closes that lane.  A promoted generation
        is accepted only after its cognitive marker record is durable; an empty
        but valid source set still produces that record with ``SKIPPED`` and no
        error.  Resolver/adapter/storage failures therefore remain retryable
        under the same deterministic event id.
        """

        if not self.publish.accepted:
            return False
        if self.skipped_reason == "missing_kg_generation_id":
            return True
        if self.skipped_reason is not None or self.mark is None:
            return False
        return self.mark.error_code is None and bool(self.mark.record_ref)


def build_kg_rebuilt_event_handler(
    *,
    publisher: KGRebuiltEventPublisher,
    cognitive_marker: CognitivePendingMarker,
    source_resolver: SourceSetResolver,
) -> Callable[[Mapping[str, Any]], KGRebuiltHandlerResult]:
    """Compose ``KGRebuiltEventPublisher`` and ``CognitivePendingMarker``
    into a single event_emitter compatible with
    ``KGRebuildService.event_emitter``.

    The returned handler:

    1. Publishes the event via ``publisher.publish(event_payload=...)``.
    2. If publish was NOT accepted, returns early — no pending marking.
       The rebuild report still records the publish failure (the
       publisher persisted an audit row), and the operator can re-drive
       from disk without queueing duplicate cognitive work.
    3. If kg_generation_id is missing (e.g. event for FAILED outcome
       without a promoted generation), returns early — there is no
       generation to mark pending for.
    4. Resolves the manifest-bound source set. Resolver failure is returned as
       a non-accepted composite result; it is never converted to a valid empty
       board.
    5. Otherwise calls ``cognitive_marker.mark_for_generation(...)`` with
       sources resolved via ``source_resolver(event_payload)`` and
       ``event_ref`` from the publish result so the cognitive record
       points back at the durable audit row.

    Returns ``KGRebuiltHandlerResult`` so callers (and integration
    tests) can assert wiring without scraping counters.
    """

    def _handler(event_payload: Mapping[str, Any]) -> KGRebuiltHandlerResult:
        publish_result = publisher.publish(event_payload=event_payload)
        if not publish_result.accepted:
            return KGRebuiltHandlerResult(
                publish=publish_result,
                mark=None,
                skipped_reason=publish_result.error_code or "publish_not_accepted",
            )

        kg_generation_id = event_payload.get("kg_generation_id")
        if not isinstance(kg_generation_id, str) or not kg_generation_id:
            return KGRebuiltHandlerResult(
                publish=publish_result,
                mark=None,
                skipped_reason="missing_kg_generation_id",
            )

        try:
            sources = tuple(source_resolver(event_payload))
        except Exception as exc:
            logger.error(
                "kg.rebuilt_handler.source_resolver_failed board=%s err=%s",
                event_payload.get("board_id"),
                exc,
            )
            return KGRebuiltHandlerResult(
                publish=publish_result,
                mark=None,
                skipped_reason=f"source_resolver_exception={type(exc).__name__}",
            )

        board_id = str(event_payload.get("board_id", ""))
        mark_result = cognitive_marker.mark_for_generation(
            board_id=board_id,
            kg_generation_id=kg_generation_id,
            source_set=sources,
            event_ref=publish_result.event_ref or "missing_event_ref",
        )
        return KGRebuiltHandlerResult(
            publish=publish_result,
            mark=mark_result,
        )

    return _handler


emit_list_sample = _emit_list_sample
is_raw_token_shape = _is_raw_token_shape


__all__ = [
    "AUDIT_DIRNAME",
    "ACTIVE_ITEM_STATUSES",
    "CANONICAL_AUDIT_OPERATIONS",
    "CONFIRMATION_AUDIT_DIRNAME",
    "CONSOLIDABLE_ARTIFACT_TYPES",
    "CognitiveConsolidationItem",
    "CognitiveConsolidationItemStore",
    "CognitiveItemListOutcome",
    "CognitiveItemListReasonCode",
    "CognitiveItemListSurface",
    "CognitiveItemStatus",
    "CognitiveItemUpdateOutcome",
    "CognitiveItemUpdateReasonCode",
    "CognitivePendingOutcomeType",
    "CognitivePendingOverlaySnapshot",
    "CognitivePendingOverlaySnapshotError",
    "CognitivePendingOverlaySnapshotService",
    "CognitivePendingReopenOutcome",
    "CognitivePendingReopenReasonCode",
    "CognitiveMarkerErrorCode",
    "CognitiveMaterializeOutcome",
    "CognitiveUnsafePayloadReason",
    "CognitiveUnsafePayloadSurface",
    "CognitivePendingAdapter",
    "CognitivePendingMarker",
    "CognitivePendingStatus",
    "COGNITIVE_PENDING_DIRNAME",
    "ConfirmationAuditErrorCode",
    "ConfirmationAuditOutcome",
    "ConfirmationAuditResult",
    "ConfirmationConsumptionAuditRecorder",
    "EVENT_AUDIT_DIRNAME",
    "EventPublishErrorCode",
    "EventPublishOutcome",
    "EventPublishResult",
    "KG_REBUILT_REQUIRED_FIELDS",
    "KGRebuiltEventPublisher",
    "KGRebuiltHandlerResult",
    "KGRebuiltPublishAdapter",
    "MaterializeFromMarkerResult",
    "PendingMarkResult",
    "REBUILD_DIRNAME",
    "SourceSetResolver",
    "_API_ITEM_FIELDS",
    "emit_list_sample",
    "_emit_unsafe_payload_sample",
    "_emit_update_sample",
    "build_kg_rebuilt_event_handler",
    "compute_cognitive_item_id",
    "compute_status_counts",
    "confirmation_fingerprint",
    "detect_unsafe_update_payload",
    "empty_status_counts",
    "record_cognitive_working_only_hold",
    "project_item_for_api",
    "project_item_for_update_api",
    "get_audit_count",
    "get_audit_counter_labels",
    "get_audit_samples",
    "get_event_count",
    "get_event_counter_labels",
    "get_event_samples",
    "get_list_counter_labels",
    "get_list_event_count",
    "get_list_samples",
    "get_unsafe_payload_counter_labels",
    "get_unsafe_payload_event_count",
    "get_unsafe_payload_samples",
    "get_update_counter_labels",
    "get_update_event_count",
    "get_update_samples",
    "get_materialized_count",
    "get_materialized_counter_labels",
    "get_materialized_event_count",
    "get_materialized_samples",
    "get_pending_count",
    "get_pending_counter_labels",
    "get_pending_samples",
    "reset_audit_counter",
    "reset_event_counter",
    "reset_list_counter",
    "reset_materialized_counter",
    "reset_pending_counter",
    "reset_unsafe_payload_counter",
    "reset_update_counter",
    "get_reopen_counter_labels",
    "get_reopen_event_count",
    "get_reopen_samples",
    "reset_reopen_counter",
    "require_rebuild_audit_artifact_store",
    "resolve_rebuild_audit_artifact_store",
    "validate_kg_rebuilt_event",
    "is_raw_token_shape",
]
