"""CandidateDecisionStore (KG-03A.4 / api_2d0d274d + api_415848a5).

File-backed store for candidate decisions extracted by agents during
cognitive consolidation. Candidate decisions are PROPOSALS — they do
NOT mutate ``spec.decisions`` on the Board DB. Promotion to a formal
decision is an EXPLICIT separate flow (KG-03A.5 command API) that
calls the spec decision service (dec_f8f3d85d + dec_75a74df6).

Layout:

    <base>/candidate_decisions/<board_id>/<candidate_id>.json

Each row carries provenance (source_ref + source_generation_id +
consolidation_session_id) so an auditor can trace the candidate back
to the cognitive item that proposed it.

Counter ``kg_candidate_decision_event_total`` (or_604e1e3e) emits one
bounded sample per lifecycle event with labels
(board_id_hash, action, outcome, reason_code).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    _is_raw_token_shape,
    resolve_rebuild_audit_artifact_store,
)
from okto_pulse.core.observability.sample_buffer import BoundedCounterSampleBuffer


logger = logging.getLogger("okto_pulse.kg.candidate_decision_store")


CANDIDATE_DECISIONS_DIRNAME = "candidate_decisions"


# ---------------------------------------------------------------------------
# Bounded enums
# ---------------------------------------------------------------------------


class CandidateDecisionStatus(str, Enum):
    """Bounded lifecycle states.

    Distinguishes the five outcomes expected by ``dec_75a74df6``:

    * ``proposed`` — initial state after ``record()``.
    * ``promoted`` — operator promoted to a NEW formal decision
      (carries ``formal_decision_ref``).
    * ``linked`` — operator linked to an existing formal decision
      (carries ``formal_decision_ref``).
    * ``dismissed`` — operator rejected the candidate
      (carries ``dismissed_reason_code``).
    * ``no_action_required`` — operator acknowledged but no decision
      action needed (carries ``dismissed_reason_code`` describing why).
    """

    PROPOSED = "proposed"
    PROMOTED = "promoted"
    LINKED = "linked"
    DISMISSED = "dismissed"
    NO_ACTION_REQUIRED = "no_action_required"


class CandidateDecisionAction(str, Enum):
    """Bounded counter label for lifecycle events (or_604e1e3e).

    One ``action`` per lifecycle transition method, plus a single bounded
    sentinel for invalid caller-supplied actions:

    * ``record`` — ``CandidateDecisionStore.record()``
    * ``promote`` — ``CandidateDecisionStore.promote()``
    * ``link_existing`` — ``CandidateDecisionStore.link_existing()``
    * ``dismiss`` — ``CandidateDecisionStore.dismiss()``
    * ``no_action_required`` — ``CandidateDecisionStore.mark_no_action_required()``
    * ``invalid`` — sentinel label used when the caller supplies an
      action that is not in the bounded enum. The raw caller input is
      NEVER emitted as a metric label.
    """

    RECORD = "record"
    PROMOTE = "promote"
    DISMISS = "dismiss"
    LINK_EXISTING = "link_existing"
    NO_ACTION_REQUIRED = "no_action_required"
    INVALID = "invalid"


_STATUS_BY_ACTION: dict[str, str] = {
    CandidateDecisionAction.PROMOTE.value: CandidateDecisionStatus.PROMOTED.value,
    CandidateDecisionAction.LINK_EXISTING.value: CandidateDecisionStatus.LINKED.value,
    CandidateDecisionAction.DISMISS.value: CandidateDecisionStatus.DISMISSED.value,
    CandidateDecisionAction.NO_ACTION_REQUIRED.value: (
        CandidateDecisionStatus.NO_ACTION_REQUIRED.value
    ),
}


_VALID_STATUS_VALUES: frozenset[str] = frozenset(
    s.value for s in CandidateDecisionStatus
)


_VALID_ACTION_VALUES: frozenset[str] = frozenset(
    a.value for a in CandidateDecisionAction
)


# Action values that are legal as INPUT to ``_transition``. ``RECORD`` is
# excluded because record() has its own dedicated entrypoint; ``INVALID``
# is excluded because it is a sentinel emitted by the store itself, not a
# value a caller can supply.
_TRANSITION_INPUT_ACTIONS: frozenset[str] = frozenset(
    a.value for a in CandidateDecisionAction
    if a not in (
        CandidateDecisionAction.RECORD,
        CandidateDecisionAction.INVALID,
    )
)


class CandidateDecisionOutcome(str, Enum):
    SUCCESS = "success"
    VALIDATION_ERROR = "validation_error"
    UNSAFE_PAYLOAD = "unsafe_payload"
    STORE_ERROR = "store_error"
    NOT_FOUND = "not_found"


class CandidateDecisionReasonCode(str, Enum):
    NONE = "none"
    MISSING_BOARD_ID = "missing_board_id"
    MISSING_SOURCE_REF = "missing_source_ref"
    MISSING_TITLE = "missing_title"
    MISSING_RATIONALE = "missing_rationale"
    MISSING_AGENT_ID = "missing_agent_id"
    MISSING_SESSION_ID = "missing_session_id"
    MISSING_GENERATION_ID = "missing_generation_id"
    UNSAFE_PAYLOAD = "unsafe_payload"
    INVALID_STATUS = "invalid_status"
    NOT_FOUND = "not_found"


_MAX_TEXT_BYTES = 4000  # title + rationale upper bound
_MAX_EVIDENCE_REFS = 50
_MAX_EVIDENCE_ENTRY_BYTES = 200


# ---------------------------------------------------------------------------
# Counter
# ---------------------------------------------------------------------------


_CANDIDATE_LABELS = ("board_id_hash", "action", "outcome", "reason_code")
_candidate_samples = BoundedCounterSampleBuffer(_CANDIDATE_LABELS)
_candidate_samples_lock = threading.Lock()


def _board_id_hash(board_id: str) -> str:
    return hashlib.sha256(board_id.encode("utf-8")).hexdigest()[:16]


def _emit_candidate_sample(
    *,
    board_id: str,
    action: str,
    outcome: str,
    reason_code: str,
) -> None:
    with _candidate_samples_lock:
        _candidate_samples.append({
            "board_id_hash": _board_id_hash(board_id),
            "action": action,
            "outcome": outcome,
            "reason_code": reason_code,
        })


def get_candidate_counter_labels() -> tuple[str, ...]:
    return _CANDIDATE_LABELS


def get_candidate_samples() -> list[dict[str, Any]]:
    with _candidate_samples_lock:
        return _candidate_samples.snapshot()


def get_candidate_event_count(
    *,
    board_id: str | None = None,
    action: str | None = None,
    outcome: str | None = None,
    reason_code: str | None = None,
) -> int:
    expected_hash = _board_id_hash(board_id) if board_id else None
    with _candidate_samples_lock:
        return _candidate_samples.count(
            board_id_hash=expected_hash,
            action=action,
            outcome=outcome,
            reason_code=reason_code,
        )


def reset_candidate_counter() -> None:
    with _candidate_samples_lock:
        _candidate_samples.clear()


# ---------------------------------------------------------------------------
# Record + validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CandidateDecisionRecord:
    """Frozen on-disk candidate decision row."""

    candidate_id: str
    board_id: str
    source_ref: str
    source_generation_id: str
    consolidation_session_id: str
    title: str
    rationale: str
    evidence_refs: tuple[str, ...]
    status: str  # CandidateDecisionStatus
    created_by_agent_id: str
    created_at: str
    updated_at: str
    formal_decision_ref: str | None = None
    dismissed_reason_code: str | None = None
    audit_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "board_id": self.board_id,
            "source_ref": self.source_ref,
            "source_generation_id": self.source_generation_id,
            "consolidation_session_id": self.consolidation_session_id,
            "title": self.title,
            "rationale": self.rationale,
            "evidence_refs": list(self.evidence_refs),
            "status": self.status,
            "created_by_agent_id": self.created_by_agent_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "formal_decision_ref": self.formal_decision_ref,
            "dismissed_reason_code": self.dismissed_reason_code,
            "audit_ref": self.audit_ref,
        }

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "CandidateDecisionRecord":
        def _tuple(value: Any) -> tuple[str, ...]:
            if isinstance(value, (list, tuple)):
                return tuple(str(item) for item in value if item)
            return ()

        return CandidateDecisionRecord(
            candidate_id=str(payload.get("candidate_id", "")),
            board_id=str(payload.get("board_id", "")),
            source_ref=str(payload.get("source_ref", "")),
            source_generation_id=str(payload.get("source_generation_id", "")),
            consolidation_session_id=str(
                payload.get("consolidation_session_id", "")
            ),
            title=str(payload.get("title", "")),
            rationale=str(payload.get("rationale", "")),
            evidence_refs=_tuple(payload.get("evidence_refs")),
            status=str(
                payload.get("status", CandidateDecisionStatus.PROPOSED.value)
            ),
            created_by_agent_id=str(payload.get("created_by_agent_id", "")),
            created_at=str(payload.get("created_at", "")),
            updated_at=str(payload.get("updated_at", "")),
            formal_decision_ref=payload.get("formal_decision_ref"),
            dismissed_reason_code=payload.get("dismissed_reason_code"),
            audit_ref=payload.get("audit_ref"),
        )


class CandidateDecisionError(Exception):
    """Bounded error raised when validation fails BEFORE store write."""

    def __init__(
        self,
        *,
        outcome: str,
        reason_code: str,
        message: str,
        unsafe_field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.outcome = outcome
        self.reason_code = reason_code
        self.message = message
        self.unsafe_field = unsafe_field


def _validate_provenance(
    *,
    board_id: str,
    source_ref: str,
    source_generation_id: str,
    consolidation_session_id: str,
    title: str,
    rationale: str,
    created_by_agent_id: str,
) -> tuple[str, str] | None:
    """Provenance check — returns ``(reason_code, message)`` on failure
    or ``None`` if the input is well-formed."""

    if not isinstance(board_id, str) or not board_id.strip():
        return (
            CandidateDecisionReasonCode.MISSING_BOARD_ID.value,
            "board_id is required",
        )
    if not isinstance(source_ref, str) or not source_ref.strip():
        return (
            CandidateDecisionReasonCode.MISSING_SOURCE_REF.value,
            "source_ref is required",
        )
    if not isinstance(source_generation_id, str) or not source_generation_id.strip():
        return (
            CandidateDecisionReasonCode.MISSING_GENERATION_ID.value,
            "source_generation_id is required",
        )
    if not isinstance(consolidation_session_id, str) or not consolidation_session_id.strip():
        return (
            CandidateDecisionReasonCode.MISSING_SESSION_ID.value,
            "consolidation_session_id is required",
        )
    if not isinstance(title, str) or not title.strip():
        return (
            CandidateDecisionReasonCode.MISSING_TITLE.value,
            "title is required",
        )
    if not isinstance(rationale, str) or not rationale.strip():
        return (
            CandidateDecisionReasonCode.MISSING_RATIONALE.value,
            "rationale is required",
        )
    if not isinstance(created_by_agent_id, str) or not created_by_agent_id.strip():
        return (
            CandidateDecisionReasonCode.MISSING_AGENT_ID.value,
            "created_by_agent_id is required",
        )
    return None


def _detect_unsafe_candidate_payload(
    *,
    title: str,
    rationale: str,
    evidence_refs: Any,
) -> tuple[bool, str | None]:
    """Same shape as ``detect_unsafe_update_payload`` from KG-03A.3,
    scoped to candidate-decision narrative fields."""

    for field_name, value in (
        ("title", title),
        ("rationale", rationale),
    ):
        if value is None:
            continue
        if not isinstance(value, str):
            return True, field_name
        if len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
            return True, field_name
        if _is_raw_token_shape(value):
            return True, field_name

    if evidence_refs is None:
        return False, None
    if not isinstance(evidence_refs, (list, tuple)) or isinstance(
        evidence_refs, (str, bytes)
    ):
        return True, "evidence_refs"
    if len(evidence_refs) > _MAX_EVIDENCE_REFS:
        return True, "evidence_refs"
    for entry in evidence_refs:
        if not isinstance(entry, str):
            return True, "evidence_refs"
        stripped = entry.strip()
        if not stripped:
            return True, "evidence_refs"
        if len(stripped.encode("utf-8")) > _MAX_EVIDENCE_ENTRY_BYTES:
            return True, "evidence_refs"
        if _is_raw_token_shape(stripped):
            return True, "evidence_refs"
    return False, None


def generate_candidate_id() -> str:
    """Generate a fresh candidate_id with the ``cand_`` prefix."""

    return f"cand_{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CandidateDecisionStore:
    """File-backed store for candidate decision proposals.

    Writes one JSON file per candidate under
    ``<base>/candidate_decisions/<board_id>/<candidate_id>.json``.
    Writes are atomic via temp+replace.
    """

    base_dir: Path | None = None
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

    def _board_dir(self, board_id: str) -> Path:
        if self.base_dir is None:
            raise RuntimeError(
                "base_dir is required when RebuildAuditArtifactStore is not supplied"
            )
        return self.base_dir / CANDIDATE_DECISIONS_DIRNAME / board_id

    def _record_path(self, board_id: str, candidate_id: str) -> Path:
        return self._board_dir(board_id) / f"{candidate_id}.json"

    @staticmethod
    def _record_key(board_id: str, candidate_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="candidate_decision",
            board_id=board_id,
            artifact_id=candidate_id,
        )

    def _write_record(self, record: CandidateDecisionRecord) -> None:
        payload = record.to_dict()
        if self.artifact_store is not None:
            self.artifact_store.write_json_atomic(
                self._record_key(record.board_id, record.candidate_id),
                payload,
            )
            return
        self._board_dir(record.board_id).mkdir(parents=True, exist_ok=True)
        path = self._record_path(record.board_id, record.candidate_id)
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        tmp.replace(path)

    def record(
        self,
        *,
        board_id: str,
        source_ref: str,
        source_generation_id: str,
        consolidation_session_id: str,
        title: str,
        rationale: str,
        evidence_refs: list[str] | None = None,
        created_by_agent_id: str,
        candidate_id: str | None = None,
    ) -> CandidateDecisionRecord:
        """Persist a new candidate decision in ``proposed`` status.

        Raises ``CandidateDecisionError`` BEFORE writing if provenance
        is incomplete or the payload trips the unsafe guard.

        This method NEVER mutates ``spec.decisions`` on the Board DB —
        promotion is a separate explicit flow (KG-03A.5).
        """

        prov_error = _validate_provenance(
            board_id=board_id,
            source_ref=source_ref,
            source_generation_id=source_generation_id,
            consolidation_session_id=consolidation_session_id,
            title=title,
            rationale=rationale,
            created_by_agent_id=created_by_agent_id,
        )
        if prov_error is not None:
            reason_code, message = prov_error
            _emit_candidate_sample(
                board_id=board_id if isinstance(board_id, str) else "",
                action=CandidateDecisionAction.RECORD.value,
                outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
                reason_code=reason_code,
            )
            raise CandidateDecisionError(
                outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
                reason_code=reason_code,
                message=message,
            )

        unsafe, unsafe_field = _detect_unsafe_candidate_payload(
            title=title, rationale=rationale, evidence_refs=evidence_refs,
        )
        if unsafe:
            _emit_candidate_sample(
                board_id=board_id,
                action=CandidateDecisionAction.RECORD.value,
                outcome=CandidateDecisionOutcome.UNSAFE_PAYLOAD.value,
                reason_code=CandidateDecisionReasonCode.UNSAFE_PAYLOAD.value,
            )
            raise CandidateDecisionError(
                outcome=CandidateDecisionOutcome.UNSAFE_PAYLOAD.value,
                reason_code=CandidateDecisionReasonCode.UNSAFE_PAYLOAD.value,
                message="candidate decision payload contains disallowed content",
                unsafe_field=unsafe_field,
            )

        now = datetime.now(timezone.utc).isoformat()
        record = CandidateDecisionRecord(
            candidate_id=candidate_id or generate_candidate_id(),
            board_id=board_id,
            source_ref=source_ref,
            source_generation_id=source_generation_id,
            consolidation_session_id=consolidation_session_id,
            title=title.strip(),
            rationale=rationale.strip(),
            evidence_refs=tuple(
                entry.strip() for entry in (evidence_refs or []) if entry
            ),
            status=CandidateDecisionStatus.PROPOSED.value,
            created_by_agent_id=created_by_agent_id,
            created_at=now,
            updated_at=now,
        )

        with self._lock:
            try:
                self._write_record(record)
            except Exception as exc:
                logger.error(
                    "kg.candidate_decision_store.write_failed board=%s err=%s",
                    board_id, exc,
                )
                _emit_candidate_sample(
                    board_id=board_id,
                    action=CandidateDecisionAction.RECORD.value,
                    outcome=CandidateDecisionOutcome.STORE_ERROR.value,
                    reason_code=CandidateDecisionReasonCode.NONE.value,
                )
                raise CandidateDecisionError(
                    outcome=CandidateDecisionOutcome.STORE_ERROR.value,
                    reason_code=CandidateDecisionReasonCode.NONE.value,
                    message=f"store write failed: {type(exc).__name__}",
                ) from exc

        _emit_candidate_sample(
            board_id=board_id,
            action=CandidateDecisionAction.RECORD.value,
            outcome=CandidateDecisionOutcome.SUCCESS.value,
            reason_code=CandidateDecisionReasonCode.NONE.value,
        )
        return record

    def get(
        self, board_id: str, candidate_id: str
    ) -> CandidateDecisionRecord | None:
        try:
            if self.artifact_store is not None:
                payload = self.artifact_store.read_json(
                    self._record_key(board_id, candidate_id)
                )
                if payload is None:
                    return None
            else:
                path = self._record_path(board_id, candidate_id)
                if not path.exists():
                    return None
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
        except Exception as exc:
            logger.error(
                "kg.candidate_decision_store.read_failed board=%s cand=%s err=%s",
                board_id, candidate_id, exc,
            )
            return None
        return CandidateDecisionRecord.from_dict(payload)

    def list(
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        source_ref_filter: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[CandidateDecisionRecord]:
        """List candidates for a board. Stable ordering by created_at,
        then candidate_id."""

        records: list[CandidateDecisionRecord] = []
        if self.artifact_store is not None:
            payloads = self.artifact_store.list_json(
                RebuildAuditKey(namespace="candidate_decision", board_id=board_id)
            )
        else:
            board_dir = self._board_dir(board_id)
            if not board_dir.exists():
                return []
            payloads = []
            for path in sorted(board_dir.glob("*.json")):
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        payloads.append(json.load(fh))
                except Exception:
                    continue
        for payload in payloads:
            record = CandidateDecisionRecord.from_dict(payload)
            if status_filter is not None and record.status != status_filter:
                continue
            if (
                source_ref_filter is not None
                and record.source_ref != source_ref_filter
            ):
                continue
            records.append(record)
        records.sort(key=lambda r: (r.created_at, r.candidate_id))
        if offset:
            records = records[offset:]
        if limit is not None:
            records = records[:limit]
        return records

    def _transition(
        self,
        *,
        board_id: str,
        candidate_id: str,
        action: str,
        formal_decision_ref: str | None,
        dismissed_reason_code: str | None,
        audit_ref: str | None = None,
    ) -> CandidateDecisionRecord:
        """Shared lifecycle-transition core. Validates inputs, mutates
        the record, and emits the correct bounded counter sample.

        Raises ``CandidateDecisionError`` for invalid action, missing
        candidate, or store IO failure. NEVER persists if validation
        fails — write happens AFTER all gates pass.
        """

        if action not in _TRANSITION_INPUT_ACTIONS:
            _emit_candidate_sample(
                board_id=board_id if isinstance(board_id, str) else "",
                action=CandidateDecisionAction.INVALID.value,
                outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
                reason_code=CandidateDecisionReasonCode.INVALID_STATUS.value,
            )
            raise CandidateDecisionError(
                outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
                reason_code=CandidateDecisionReasonCode.INVALID_STATUS.value,
                message=(
                    f"action must be one of "
                    f"{sorted(_TRANSITION_INPUT_ACTIONS)}"
                ),
            )

        new_status = _STATUS_BY_ACTION[action]

        with self._lock:
            existing = self.get(board_id, candidate_id)
            if existing is None:
                _emit_candidate_sample(
                    board_id=board_id,
                    action=action,
                    outcome=CandidateDecisionOutcome.NOT_FOUND.value,
                    reason_code=CandidateDecisionReasonCode.NOT_FOUND.value,
                )
                raise CandidateDecisionError(
                    outcome=CandidateDecisionOutcome.NOT_FOUND.value,
                    reason_code=CandidateDecisionReasonCode.NOT_FOUND.value,
                    message=(
                        f"candidate {candidate_id!r} not found on board "
                        f"{board_id!r}"
                    ),
                )

            now = datetime.now(timezone.utc).isoformat()
            updated_payload = existing.to_dict()
            updated_payload["status"] = new_status
            updated_payload["updated_at"] = now
            if formal_decision_ref is not None:
                updated_payload["formal_decision_ref"] = formal_decision_ref
            if dismissed_reason_code is not None:
                updated_payload["dismissed_reason_code"] = dismissed_reason_code
            if audit_ref is not None:
                updated_payload["audit_ref"] = audit_ref

            try:
                self._write_record(CandidateDecisionRecord.from_dict(updated_payload))
            except Exception as exc:
                logger.error(
                    "kg.candidate_decision_store.transition_failed "
                    "board=%s cand=%s action=%s err=%s",
                    board_id, candidate_id, action, exc,
                )
                _emit_candidate_sample(
                    board_id=board_id,
                    action=action,
                    outcome=CandidateDecisionOutcome.STORE_ERROR.value,
                    reason_code=CandidateDecisionReasonCode.NONE.value,
                )
                raise CandidateDecisionError(
                    outcome=CandidateDecisionOutcome.STORE_ERROR.value,
                    reason_code=CandidateDecisionReasonCode.NONE.value,
                    message=f"store write failed: {type(exc).__name__}",
                ) from exc

        _emit_candidate_sample(
            board_id=board_id,
            action=action,
            outcome=CandidateDecisionOutcome.SUCCESS.value,
            reason_code=CandidateDecisionReasonCode.NONE.value,
        )
        return CandidateDecisionRecord.from_dict(updated_payload)

    def promote(
        self,
        *,
        board_id: str,
        candidate_id: str,
        formal_decision_ref: str,
        audit_ref: str | None = None,
    ) -> CandidateDecisionRecord:
        """Promote a candidate to a NEW formal decision. Caller is
        responsible for creating the formal decision on the Board DB
        BEFORE calling promote() and for passing the resulting ref."""

        return self._transition(
            board_id=board_id,
            candidate_id=candidate_id,
            action=CandidateDecisionAction.PROMOTE.value,
            formal_decision_ref=formal_decision_ref,
            dismissed_reason_code=None,
            audit_ref=audit_ref,
        )

    def link_existing(
        self,
        *,
        board_id: str,
        candidate_id: str,
        formal_decision_ref: str,
        audit_ref: str | None = None,
    ) -> CandidateDecisionRecord:
        """Link a candidate to an EXISTING formal decision."""

        return self._transition(
            board_id=board_id,
            candidate_id=candidate_id,
            action=CandidateDecisionAction.LINK_EXISTING.value,
            formal_decision_ref=formal_decision_ref,
            dismissed_reason_code=None,
            audit_ref=audit_ref,
        )

    def dismiss(
        self,
        *,
        board_id: str,
        candidate_id: str,
        reason_code: str,
        audit_ref: str | None = None,
    ) -> CandidateDecisionRecord:
        """Dismiss a candidate (operator rejected the proposal)."""

        return self._transition(
            board_id=board_id,
            candidate_id=candidate_id,
            action=CandidateDecisionAction.DISMISS.value,
            formal_decision_ref=None,
            dismissed_reason_code=reason_code,
            audit_ref=audit_ref,
        )

    def mark_no_action_required(
        self,
        *,
        board_id: str,
        candidate_id: str,
        reason_code: str,
        audit_ref: str | None = None,
    ) -> CandidateDecisionRecord:
        """Mark a candidate as acknowledged but requiring no action."""

        return self._transition(
            board_id=board_id,
            candidate_id=candidate_id,
            action=CandidateDecisionAction.NO_ACTION_REQUIRED.value,
            formal_decision_ref=None,
            dismissed_reason_code=reason_code,
            audit_ref=audit_ref,
        )


__all__ = [
    "CANDIDATE_DECISIONS_DIRNAME",
    "CandidateDecisionAction",
    "CandidateDecisionError",
    "CandidateDecisionOutcome",
    "CandidateDecisionReasonCode",
    "CandidateDecisionRecord",
    "CandidateDecisionStatus",
    "CandidateDecisionStore",
    "generate_candidate_id",
    "get_candidate_counter_labels",
    "get_candidate_event_count",
    "get_candidate_samples",
    "reset_candidate_counter",
]
