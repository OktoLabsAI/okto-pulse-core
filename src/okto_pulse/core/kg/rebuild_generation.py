"""KG generation repository + promotion guard (KG-02.4 / IR ir_6d092147).

KG-02.4 ships three primitives that together replace the placeholder
generation IDs that KG-02.3 left as ``None``:

1. ``generate_kg_generation_id()`` — UUID v4 string (TR3). Ordering and
   lifecycle depend on ``created_at`` / ``status``, never on id format.
2. ``KGGenerationRepository`` — file-backed pointer to the current
   ``kg_generation_id`` per board plus a history record. ``promote_current``
   is the ONLY way to advance the pointer and requires a durable
   ``report_ref`` (br_5c7c5dfa + br_82deef11 + IR ir_6d092147).
3. ``KGGenerationPromotionGuard`` — pure decision function exposing
   ``api_bacb7555``. Operators see the same reasons the repository uses,
   so the UI never lies about why a promotion was blocked.

This module persists ONE file per board for the pointer and ONE file per
generation for the history record. Layout:

    <base>/rebuild/generations/<board_id>/current.json
    <base>/rebuild/generations/<board_id>/history/<generation_id>.json

The history record is the durable trail the audit event references via
``previous_kg_generation_id`` / ``current_kg_generation_id`` (TR8).
"""

from __future__ import annotations

import logging
import re
import threading
from okto_pulse.core.runtime_context import runtime_lock, runtime_state
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.rebuild_generation")


REBUILD_DIRNAME = "rebuild"
GENERATIONS_DIRNAME = "generations"
HISTORY_DIRNAME = "history"
CURRENT_FILENAME = "current.json"
GENERATION_CURRENT_NAMESPACE = "generation_current"
GENERATION_HISTORY_NAMESPACE = "generation_history"


# Promotion is restricted to terminal states that produced a real
# generation. ``rolled_back`` is allowed because the new "current"
# generation may be the previous one promoted forward as the safe
# fallback after a failed rebuild.
PROMOTABLE_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        "completed",
        "partially_rebuilt",
        "rolled_back",
    }
)


_UUID_V4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def generate_kg_generation_id() -> str:
    """Return a fresh UUID v4 string per TR3."""

    return str(uuid.uuid4())


def is_valid_kg_generation_id(value: str) -> bool:
    """True if ``value`` is a UUID v4 string in canonical form."""

    if not isinstance(value, str):
        return False
    return bool(_UUID_V4_RE.match(value))


class PromotionOutcome(str, Enum):
    """Bounded outcomes for ``kg_generation_promotion_total`` labels."""

    PROMOTED = "promoted"
    REPORT_REF_REQUIRED = "report_ref_required"
    INVALID_KG_GENERATION_ID = "invalid_kg_generation_id"
    INVALID_PREVIOUS_KG_GENERATION_ID = "invalid_previous_kg_generation_id"
    INVALID_STATUS = "invalid_status"
    GENERATION_CONFLICT = "generation_conflict"
    STRUCTURAL_HASH_MISMATCH = "structural_hash_mismatch"
    PERSIST_FAILED = "persist_failed"


@dataclass(frozen=True, slots=True)
class PromotionResult:
    """Frozen result of ``KGGenerationRepository.promote_current``."""

    outcome: str  # PromotionOutcome value
    board_id: str
    previous_kg_generation_id: str | None
    current_kg_generation_id: str | None
    report_ref: str | None
    promoted_at: str | None
    history_ref: str | None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class PromotionEvaluation:
    """Frozen response for ``KGGenerationPromotionGuard.evaluate``
    matching ``api_bacb7555`` response_success schema."""

    eligible: bool
    reasons: tuple[str, ...]
    required_operator_action: str | None = None


# --- Counter (diagnostic; OR or_56ec0300 surface is in rebuild_report) ------

_PROMOTION_LABELS = ("board_id", "status", "outcome")
_promotion_counter = runtime_state("kg.rebuild_generation.promotion_counter", dict)
_promotion_counter_lock = runtime_lock("kg.rebuild_generation.promotion_counter")


def _bump_promotion(*, board_id: str, status: str, outcome: str) -> None:
    key = (board_id, status, outcome)
    with _promotion_counter_lock:
        _promotion_counter[key] = _promotion_counter.get(key, 0) + 1


def get_promotion_count(
    board_id: str, *, status: str | None = None, outcome: str | None = None
) -> int:
    with _promotion_counter_lock:
        total = 0
        for (b, st, oc), value in _promotion_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            if outcome is not None and oc != outcome:
                continue
            total += value
        return total


def get_promotion_counter_labels() -> tuple[str, ...]:
    return _PROMOTION_LABELS


def get_promotion_samples() -> list[dict[str, Any]]:
    with _promotion_counter_lock:
        return [
            {"board_id": b, "status": st, "outcome": oc, "count": value}
            for (b, st, oc), value in _promotion_counter.items()
        ]


def reset_promotion_counter() -> None:
    with _promotion_counter_lock:
        _promotion_counter.clear()


# --- Repository --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RebuildAuditKGGenerationRepository:
    """Generation pointer repository backed by ``RebuildAuditArtifactStore``.

    This is the storage-agnostic counterpart of ``KGGenerationRepository``.
    It keeps the same public repository contract while moving durable current
    generation state behind the rebuild/audit artifact-store port.
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

    @staticmethod
    def _current_key(board_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace=GENERATION_CURRENT_NAMESPACE,
            board_id=board_id,
            artifact_id="current",
        )

    @staticmethod
    def _history_key(board_id: str, generation_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace=GENERATION_HISTORY_NAMESPACE,
            board_id=board_id,
            kg_generation_id=generation_id,
        )

    def get_current(self, board_id: str) -> str | None:
        """Return the current ``kg_generation_id`` for the board.

        Store exceptions deliberately propagate so callers can distinguish
        "no generation exists" from "generation store unavailable".
        """

        payload = self.artifact_store.read_json(self._current_key(board_id))
        if payload is None:
            return None
        value = payload.get("kg_generation_id")
        if not isinstance(value, str) or not is_valid_kg_generation_id(value):
            return None
        return value

    def get_history_ref(self, board_id: str, generation_id: str) -> str | None:
        key = self._history_key(board_id, generation_id)
        return (
            self.artifact_store.reference(key)
            if self.artifact_store.exists(key)
            else None
        )

    def load_history(self, board_id: str, generation_id: str) -> dict[str, Any] | None:
        return self.artifact_store.read_json(self._history_key(board_id, generation_id))

    def promote_current(
        self,
        *,
        board_id: str,
        previous_kg_generation_id: str | None,
        kg_generation_id: str,
        report_ref: str | None,
        status: str,
        structural_hash: str,
        source_hash: str,
        promoted_by: str,
        run_id: str | None = None,
        operator_override_ref: str | None = None,
    ) -> PromotionResult:
        """Advance the board pointer through the injected artifact store."""

        if status not in PROMOTABLE_TERMINAL_STATUSES:
            _bump_promotion(
                board_id=board_id,
                status=status,
                outcome=PromotionOutcome.INVALID_STATUS.value,
            )
            return PromotionResult(
                outcome=PromotionOutcome.INVALID_STATUS.value,
                board_id=board_id,
                previous_kg_generation_id=previous_kg_generation_id,
                current_kg_generation_id=None,
                report_ref=report_ref,
                promoted_at=None,
                history_ref=None,
                detail=(
                    f"status={status!r} not in {sorted(PROMOTABLE_TERMINAL_STATUSES)}"
                ),
            )

        if not is_valid_kg_generation_id(kg_generation_id):
            _bump_promotion(
                board_id=board_id,
                status=status,
                outcome=PromotionOutcome.INVALID_KG_GENERATION_ID.value,
            )
            return PromotionResult(
                outcome=PromotionOutcome.INVALID_KG_GENERATION_ID.value,
                board_id=board_id,
                previous_kg_generation_id=previous_kg_generation_id,
                current_kg_generation_id=None,
                report_ref=report_ref,
                promoted_at=None,
                history_ref=None,
                detail="kg_generation_id is not a canonical UUID v4 string",
            )

        if previous_kg_generation_id is not None and not is_valid_kg_generation_id(
            previous_kg_generation_id
        ):
            _bump_promotion(
                board_id=board_id,
                status=status,
                outcome=PromotionOutcome.INVALID_PREVIOUS_KG_GENERATION_ID.value,
            )
            return PromotionResult(
                outcome=PromotionOutcome.INVALID_PREVIOUS_KG_GENERATION_ID.value,
                board_id=board_id,
                previous_kg_generation_id=previous_kg_generation_id,
                current_kg_generation_id=None,
                report_ref=report_ref,
                promoted_at=None,
                history_ref=None,
                detail="previous_kg_generation_id is not a canonical UUID v4 string",
            )

        if not isinstance(report_ref, str) or not report_ref.strip():
            _bump_promotion(
                board_id=board_id,
                status=status,
                outcome=PromotionOutcome.REPORT_REF_REQUIRED.value,
            )
            return PromotionResult(
                outcome=PromotionOutcome.REPORT_REF_REQUIRED.value,
                board_id=board_id,
                previous_kg_generation_id=previous_kg_generation_id,
                current_kg_generation_id=None,
                report_ref=None,
                promoted_at=None,
                history_ref=None,
                detail="report_ref required for promotion (ir_6d092147)",
            )

        with self._lock:
            try:
                stored_previous = self.get_current(board_id)
            except Exception as exc:
                logger.error(
                    "kg.generation.artifact_current_read_failed board=%s err=%s",
                    board_id,
                    exc,
                )
                _bump_promotion(
                    board_id=board_id,
                    status=status,
                    outcome=PromotionOutcome.PERSIST_FAILED.value,
                )
                return PromotionResult(
                    outcome=PromotionOutcome.PERSIST_FAILED.value,
                    board_id=board_id,
                    previous_kg_generation_id=previous_kg_generation_id,
                    current_kg_generation_id=None,
                    report_ref=report_ref,
                    promoted_at=None,
                    history_ref=None,
                    detail=f"current_read_exception={type(exc).__name__}",
                )
            if stored_previous == kg_generation_id:
                history = self.load_history(board_id, kg_generation_id)
                replay_matches = bool(
                    history is not None
                    and history.get("board_id") == board_id
                    and history.get("kg_generation_id") == kg_generation_id
                    and history.get("previous_kg_generation_id")
                    == previous_kg_generation_id
                    and history.get("report_ref") == report_ref
                    and history.get("status") == status
                    and history.get("structural_hash") == structural_hash
                    and history.get("source_hash") == source_hash
                    and history.get("promoted_by") == promoted_by
                    and history.get("run_id") == run_id
                    and history.get("operator_override_ref") == operator_override_ref
                )
                if replay_matches:
                    return PromotionResult(
                        outcome=PromotionOutcome.PROMOTED.value,
                        board_id=board_id,
                        previous_kg_generation_id=previous_kg_generation_id,
                        current_kg_generation_id=kg_generation_id,
                        report_ref=report_ref,
                        promoted_at=str(history.get("promoted_at") or "") or None,
                        history_ref=self.get_history_ref(board_id, kg_generation_id),
                        detail="already_promoted",
                    )
            if stored_previous != previous_kg_generation_id:
                _bump_promotion(
                    board_id=board_id,
                    status=status,
                    outcome=PromotionOutcome.GENERATION_CONFLICT.value,
                )
                return PromotionResult(
                    outcome=PromotionOutcome.GENERATION_CONFLICT.value,
                    board_id=board_id,
                    previous_kg_generation_id=previous_kg_generation_id,
                    current_kg_generation_id=None,
                    report_ref=report_ref,
                    promoted_at=None,
                    history_ref=None,
                    detail=(
                        f"expected_previous={previous_kg_generation_id!r} "
                        f"stored_previous={stored_previous!r}"
                    ),
                )

            promoted_at = datetime.now(timezone.utc).isoformat()
            history_payload = {
                "board_id": board_id,
                "kg_generation_id": kg_generation_id,
                "previous_kg_generation_id": previous_kg_generation_id,
                "report_ref": report_ref,
                "status": status,
                "structural_hash": structural_hash,
                "source_hash": source_hash,
                "promoted_by": promoted_by,
                "promoted_at": promoted_at,
                "run_id": run_id,
                "operator_override_ref": operator_override_ref,
            }
            pointer_payload = {
                "board_id": board_id,
                "kg_generation_id": kg_generation_id,
                "previous_kg_generation_id": previous_kg_generation_id,
                "report_ref": report_ref,
                "status": status,
                "promoted_at": promoted_at,
            }
            history_key = self._history_key(board_id, kg_generation_id)
            try:
                self.artifact_store.write_json_atomic(history_key, history_payload)
                self.artifact_store.write_json_atomic(
                    self._current_key(board_id), pointer_payload
                )
            except Exception as exc:
                logger.error(
                    "kg.generation.artifact_promote_persist_failed "
                    "board=%s gen=%s err=%s",
                    board_id,
                    kg_generation_id,
                    exc,
                )
                _bump_promotion(
                    board_id=board_id,
                    status=status,
                    outcome=PromotionOutcome.PERSIST_FAILED.value,
                )
                return PromotionResult(
                    outcome=PromotionOutcome.PERSIST_FAILED.value,
                    board_id=board_id,
                    previous_kg_generation_id=previous_kg_generation_id,
                    current_kg_generation_id=None,
                    report_ref=report_ref,
                    promoted_at=None,
                    history_ref=None,
                    detail=f"persist_exception={type(exc).__name__}",
                )

            _bump_promotion(
                board_id=board_id,
                status=status,
                outcome=PromotionOutcome.PROMOTED.value,
            )
            return PromotionResult(
                outcome=PromotionOutcome.PROMOTED.value,
                board_id=board_id,
                previous_kg_generation_id=previous_kg_generation_id,
                current_kg_generation_id=kg_generation_id,
                report_ref=report_ref,
                promoted_at=promoted_at,
                history_ref=self.artifact_store.reference(history_key),
                detail=None,
            )


KGGenerationRepository = RebuildAuditKGGenerationRepository


# --- Promotion guard ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KGGenerationPromotionGuard:
    """Pure decision function for ``api_bacb7555``.

    The guard does NOT touch storage; it returns a decision so the UI
    and the rebuild service share the same vocabulary. The actual
    promotion is performed by ``KGGenerationRepository.promote_current``
    after the guard returns ``eligible=True``.
    """

    @staticmethod
    def evaluate(
        *,
        board_id: str,
        previous_kg_generation_id: str | None,
        kg_generation_id: str,
        report_ref: str | None,
        status: str,
        structural_hash: str,
        source_hash: str,
        expected_structural_hash: str | None = None,
        observed_previous_kg_generation_id: str | None = None,
        operator_override_ref: str | None = None,
    ) -> PromotionEvaluation:
        """Return whether promotion is eligible per ir_6d092147."""

        reasons: list[str] = []
        required_action: str | None = None

        if status not in PROMOTABLE_TERMINAL_STATUSES:
            reasons.append("invalid_status")
        if not is_valid_kg_generation_id(kg_generation_id):
            reasons.append("invalid_kg_generation_id")
        if previous_kg_generation_id is not None and not is_valid_kg_generation_id(
            previous_kg_generation_id
        ):
            reasons.append("invalid_previous_kg_generation_id")
        if not isinstance(report_ref, str) or not report_ref.strip():
            reasons.append("report_ref_required")
            required_action = "persist_rebuild_report"
        if (
            observed_previous_kg_generation_id is not None
            and observed_previous_kg_generation_id != previous_kg_generation_id
        ):
            reasons.append("generation_conflict")
            required_action = required_action or "resync_generation_pointer"
        if (
            expected_structural_hash is not None
            and expected_structural_hash != structural_hash
            and operator_override_ref is None
        ):
            reasons.append("structural_hash_mismatch")
            required_action = required_action or "operator_override_required"
        if not isinstance(source_hash, str) or not source_hash.strip():
            reasons.append("source_hash_required")

        eligible = not reasons
        return PromotionEvaluation(
            eligible=eligible,
            reasons=tuple(reasons),
            required_operator_action=required_action,
        )


__all__ = [
    "CURRENT_FILENAME",
    "GENERATION_CURRENT_NAMESPACE",
    "GENERATION_HISTORY_NAMESPACE",
    "GENERATIONS_DIRNAME",
    "HISTORY_DIRNAME",
    "KGGenerationPromotionGuard",
    "KGGenerationRepository",
    "PROMOTABLE_TERMINAL_STATUSES",
    "PromotionEvaluation",
    "PromotionOutcome",
    "PromotionResult",
    "REBUILD_DIRNAME",
    "RebuildAuditKGGenerationRepository",
    "generate_kg_generation_id",
    "get_promotion_count",
    "get_promotion_counter_labels",
    "get_promotion_samples",
    "is_valid_kg_generation_id",
    "reset_promotion_counter",
]
