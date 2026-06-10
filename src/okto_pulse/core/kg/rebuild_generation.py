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

import json
import logging
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.rebuild_generation")


REBUILD_DIRNAME = "rebuild"
GENERATIONS_DIRNAME = "generations"
HISTORY_DIRNAME = "history"
CURRENT_FILENAME = "current.json"


# Promotion is restricted to terminal states that produced a real
# generation. ``rolled_back`` is allowed because the new "current"
# generation may be the previous one promoted forward as the safe
# fallback after a failed rebuild.
PROMOTABLE_TERMINAL_STATUSES: frozenset[str] = frozenset({
    "completed",
    "partially_rebuilt",
    "rolled_back",
})


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
_promotion_counter: dict[tuple[str, str, str], int] = {}
_promotion_counter_lock = threading.Lock()


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
class KGGenerationRepository:
    """File-backed repository for the per-board current generation.

    All state lives under ``<base_dir>/rebuild/generations/<board_id>/``.
    A pointer file (``current.json``) names the active generation; one
    history record per generation lives in ``history/``. The pointer is
    rewritten atomically (``.tmp`` + ``replace``) so a crash mid-promote
    cannot leave a half-written pointer.
    """

    base_dir: Path
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def _board_dir(self, board_id: str) -> Path:
        return (
            self.base_dir
            / REBUILD_DIRNAME
            / GENERATIONS_DIRNAME
            / board_id
        )

    def _current_path(self, board_id: str) -> Path:
        return self._board_dir(board_id) / CURRENT_FILENAME

    def _history_path(self, board_id: str, generation_id: str) -> Path:
        return (
            self._board_dir(board_id)
            / HISTORY_DIRNAME
            / f"{generation_id}.json"
        )

    def get_current(self, board_id: str) -> str | None:
        """Return the current ``kg_generation_id`` for the board, or
        ``None`` if no generation has ever been promoted."""

        path = self._current_path(board_id)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception as exc:
            logger.error(
                "kg.generation.current_read_failed board=%s err=%s",
                board_id, exc,
            )
            return None
        value = payload.get("kg_generation_id")
        if not isinstance(value, str) or not is_valid_kg_generation_id(value):
            return None
        return value

    def get_history_ref(
        self, board_id: str, generation_id: str
    ) -> str | None:
        """Return the durable history path for a generation, if any."""

        path = self._history_path(board_id, generation_id)
        return str(path) if path.exists() else None

    def load_history(
        self, board_id: str, generation_id: str
    ) -> dict[str, Any] | None:
        path = self._history_path(board_id, generation_id)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:
            logger.error(
                "kg.generation.history_read_failed board=%s gen=%s err=%s",
                board_id, generation_id, exc,
            )
            return None

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
        """Advance the board pointer to ``kg_generation_id``.

        Returns ``PromotionResult`` with the outcome. The pointer is NOT
        moved unless ``outcome == "promoted"``. ``report_ref`` is the
        durable receipt produced by ``RebuildReportStore.persist`` —
        without it promotion is rejected per IR ir_6d092147.
        """

        # 1. Status MUST be a recognised promotable terminal status.
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
                    f"status={status!r} not in "
                    f"{sorted(PROMOTABLE_TERMINAL_STATUSES)}"
                ),
            )

        # 2. New generation id MUST be UUID v4.
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

        # 3. previous id, when supplied, MUST be UUID v4.
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

        # 4. report_ref MUST be a non-empty string. The repository never
        # judges durability beyond presence — that's the caller's
        # responsibility (RebuildReportStore returned outcome=stored).
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

        # 5. Under lock: previous pointer must match the supplied
        # previous id. This prevents two concurrent promotions racing
        # the same board.
        with self._lock:
            stored_previous = self.get_current(board_id)
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

            board_dir = self._board_dir(board_id)
            history_dir = board_dir / HISTORY_DIRNAME
            try:
                history_dir.mkdir(parents=True, exist_ok=True)
                history_path = self._history_path(board_id, kg_generation_id)
                tmp_history = history_path.with_suffix(".json.tmp")
                with tmp_history.open("w", encoding="utf-8") as fh:
                    json.dump(history_payload, fh, indent=2)
                tmp_history.replace(history_path)

                current_path = self._current_path(board_id)
                tmp_current = current_path.with_suffix(".json.tmp")
                with tmp_current.open("w", encoding="utf-8") as fh:
                    json.dump(pointer_payload, fh, indent=2)
                tmp_current.replace(current_path)
            except Exception as exc:
                logger.error(
                    "kg.generation.promote_persist_failed board=%s gen=%s err=%s",
                    board_id, kg_generation_id, exc,
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
                history_ref=str(history_path),
                detail=None,
            )


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
    "GENERATIONS_DIRNAME",
    "HISTORY_DIRNAME",
    "KGGenerationPromotionGuard",
    "KGGenerationRepository",
    "PROMOTABLE_TERMINAL_STATUSES",
    "PromotionEvaluation",
    "PromotionOutcome",
    "PromotionResult",
    "REBUILD_DIRNAME",
    "generate_kg_generation_id",
    "get_promotion_count",
    "get_promotion_counter_labels",
    "get_promotion_samples",
    "is_valid_kg_generation_id",
    "reset_promotion_counter",
]
