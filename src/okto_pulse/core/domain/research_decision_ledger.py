"""Pure contracts for a Refinement research-decision ledger.

The ledger is deliberately separate from ``Refinement.decisions``.  Every
research entry is insert-only; changing an entry means appending a successor
and advancing its head with compare-and-swap semantics.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import ClassVar, Generic, TypeVar

from okto_pulse.core.domain.enums import RefinementStatus

RESEARCH_DECISION_LEDGER_CONTRACT_VERSION = "research-decision-ledger/v1"
RESEARCH_DECISION_PAGE_LIMIT_MAX = 200
RESEARCH_DECISION_REST_PAGE_LIMITS = frozenset({25, 50, 100})

_STABLE_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_MUTABLE_BRACKET_INDEX_RE = re.compile(r"\[\d+\]")
_MUTABLE_SEGMENT_INDEX_RE = re.compile(r"(?:^|[./])\d+(?:$|[./])")


class ResearchDecisionContractError(ValueError):
    """A value violates the frozen research-decision-ledger contract."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise ResearchDecisionContractError(code)
    normalized = value.strip()
    if not normalized:
        raise ResearchDecisionContractError(code)
    return normalized


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _strict_non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ResearchDecisionContractError(code)
    return value


def _strict_positive_int(value: object, code: str) -> int:
    resolved = _strict_non_negative_int(value, code)
    if resolved < 1:
        raise ResearchDecisionContractError(code)
    return resolved


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ResearchDecisionContractError(code)
    return value.astimezone(timezone.utc)


def _enum_instance(value: object, enum_type: type[Enum], code: str) -> None:
    if not isinstance(value, enum_type):
        raise ResearchDecisionContractError(code)


def _instance(value: object, expected_type: type, code: str) -> None:
    if not isinstance(value, expected_type):
        raise ResearchDecisionContractError(code)


def _text_tuple(value: object, code: str) -> tuple[str, ...]:
    if not isinstance(value, tuple | list):
        raise ResearchDecisionContractError(code)
    normalized = tuple(_required_text(item, code) for item in value)
    if len(set(normalized)) != len(normalized):
        raise ResearchDecisionContractError(f"{code}_duplicate")
    return normalized


def _typed_tuple(
    value: object,
    expected_type: type,
    code: str,
) -> tuple:
    if not isinstance(value, tuple | list):
        raise ResearchDecisionContractError(code)
    normalized = tuple(value)
    if any(not isinstance(item, expected_type) for item in normalized):
        raise ResearchDecisionContractError(code)
    return normalized


def _content_digest(value: object, code: str) -> str:
    normalized = _required_text(value, code)
    if (
        len(normalized) != 64
        or normalized != normalized.lower()
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise ResearchDecisionContractError(code)
    return normalized


class ResearchDecisionSubjectType(str, Enum):
    """The ledger intentionally supports no artifact other than Refinement."""

    REFINEMENT = "refinement"


class ResearchDecisionStatus(str, Enum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    DEFERRED = "deferred"


class ResearchDecisionAnchorType(str, Enum):
    FUNCTIONAL_REQUIREMENT = "functional_requirement"
    ACCEPTANCE_CRITERION = "acceptance_criterion"
    TECHNICAL_REQUIREMENT = "technical_requirement"
    QA = "qa"


class ResearchDecisionAction(str, Enum):
    APPEND = "append"
    SUPERSEDE = "supersede"


class ResearchDecisionEventType(str, Enum):
    APPENDED = "research_decision.appended"
    SUPERSEDED = "research_decision.superseded"


@dataclass(frozen=True, slots=True)
class ResearchDecisionAnchor:
    """A typed reference to a stable FR, AC, TR, or Q&A identity."""

    anchor_type: ResearchDecisionAnchorType
    anchor_ref: str

    def __post_init__(self) -> None:
        _enum_instance(
            self.anchor_type,
            ResearchDecisionAnchorType,
            "research_decision_anchor_type_invalid",
        )
        anchor_ref = _required_text(
            self.anchor_ref,
            "research_decision_anchor_ref_required",
        )
        if (
            not _STABLE_REF_RE.fullmatch(anchor_ref)
            or anchor_ref.isdigit()
            or _MUTABLE_BRACKET_INDEX_RE.search(anchor_ref)
            or _MUTABLE_SEGMENT_INDEX_RE.search(anchor_ref)
        ):
            raise ResearchDecisionContractError("research_decision_anchor_ref_unstable")
        object.__setattr__(self, "anchor_ref", anchor_ref)


@dataclass(frozen=True, slots=True)
class ResearchDecisionContent:
    """Versioned content embedded in an immutable ledger entry."""

    unknown: str
    status: ResearchDecisionStatus
    anchor: ResearchDecisionAnchor
    evidence_refs: tuple[str, ...] = ()
    alternatives: tuple[str, ...] = ()
    decision: str | None = None
    rationale: str | None = None
    confidence: float | None = None
    evidence_absence_justification: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "unknown",
            _required_text(
                self.unknown,
                "research_decision_unknown_required",
            ),
        )
        _enum_instance(
            self.status,
            ResearchDecisionStatus,
            "research_decision_status_invalid",
        )
        _instance(
            self.anchor,
            ResearchDecisionAnchor,
            "research_decision_anchor_invalid",
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _text_tuple(
                self.evidence_refs,
                "research_decision_evidence_ref_invalid",
            ),
        )
        object.__setattr__(
            self,
            "alternatives",
            _text_tuple(
                self.alternatives,
                "research_decision_alternative_invalid",
            ),
        )
        decision = _optional_text(
            self.decision,
            "research_decision_decision_invalid",
        )
        rationale = _optional_text(
            self.rationale,
            "research_decision_rationale_invalid",
        )
        absence = _optional_text(
            self.evidence_absence_justification,
            "research_decision_evidence_absence_justification_invalid",
        )
        object.__setattr__(self, "decision", decision)
        object.__setattr__(self, "rationale", rationale)
        object.__setattr__(
            self,
            "evidence_absence_justification",
            absence,
        )

        confidence = self.confidence
        if confidence is not None:
            if (
                isinstance(confidence, bool)
                or not isinstance(confidence, int | float)
                or not math.isfinite(float(confidence))
                or not 0.0 <= float(confidence) <= 1.0
            ):
                raise ResearchDecisionContractError(
                    "research_decision_confidence_invalid"
                )
            object.__setattr__(self, "confidence", float(confidence))

        if self.status is ResearchDecisionStatus.RESOLVED:
            if decision is None:
                raise ResearchDecisionContractError(
                    "resolved_research_decision_required"
                )
            if rationale is None:
                raise ResearchDecisionContractError(
                    "resolved_research_rationale_required"
                )
            if confidence is None:
                raise ResearchDecisionContractError(
                    "resolved_research_confidence_required"
                )
            if not self.evidence_refs and absence is None:
                raise ResearchDecisionContractError(
                    "resolved_research_evidence_required"
                )


def research_decision_content_digest(content: ResearchDecisionContent) -> str:
    """Return the v1 canonical SHA-256 digest of immutable RDL content."""

    _instance(
        content,
        ResearchDecisionContent,
        "research_decision_content_invalid",
    )
    payload = {
        "alternatives": list(content.alternatives),
        "anchor": {
            "anchor_ref": content.anchor.anchor_ref,
            "anchor_type": content.anchor.anchor_type.value,
        },
        "confidence": content.confidence,
        "decision": content.decision,
        "digest_contract": "research-decision-content/v1",
        "evidence_absence_justification": (
            content.evidence_absence_justification
        ),
        "evidence_refs": list(content.evidence_refs),
        "rationale": content.rationale,
        "status": content.status.value,
        "unknown": content.unknown,
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class RefinementLedgerContext:
    """Authoritative Refinement fence resolved before bundle preparation."""

    board_id: str
    refinement_id: str
    version: int
    status: RefinementStatus
    archived: bool
    subject_type: ResearchDecisionSubjectType = ResearchDecisionSubjectType.REFINEMENT

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "research_decision_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "refinement_id",
            _required_text(
                self.refinement_id,
                "research_decision_refinement_id_required",
            ),
        )
        object.__setattr__(
            self,
            "version",
            _strict_positive_int(
                self.version,
                "research_decision_refinement_version_invalid",
            ),
        )
        _enum_instance(
            self.status,
            RefinementStatus,
            "research_decision_refinement_status_invalid",
        )
        if not isinstance(self.archived, bool):
            raise ResearchDecisionContractError(
                "research_decision_refinement_archived_invalid"
            )
        if self.subject_type is not ResearchDecisionSubjectType.REFINEMENT:
            raise ResearchDecisionContractError(
                "research_decision_subject_type_invalid"
            )


@dataclass(frozen=True, slots=True)
class ResearchDecisionEntry:
    """Insert-only research record.  There is intentionally no ``updated_at``."""

    id: str
    ledger_id: str
    board_id: str
    refinement_id: str
    refinement_version: int
    predecessor_entry_id: str | None
    content: ResearchDecisionContent
    created_by: str
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "ledger_id",
            "board_id",
            "refinement_id",
            "created_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "refinement_version",
            _strict_positive_int(
                self.refinement_version,
                "research_decision_refinement_version_invalid",
            ),
        )
        if self.predecessor_entry_id is not None:
            predecessor = _required_text(
                self.predecessor_entry_id,
                "research_decision_predecessor_entry_id_invalid",
            )
            if predecessor == self.id:
                raise ResearchDecisionContractError(
                    "research_decision_predecessor_self_reference"
                )
            object.__setattr__(self, "predecessor_entry_id", predecessor)
        _instance(
            self.content,
            ResearchDecisionContent,
            "research_decision_content_invalid",
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "research_decision_created_at_invalid",
            ),
        )

    @property
    def status(self) -> ResearchDecisionStatus:
        return self.content.status

    @property
    def anchor(self) -> ResearchDecisionAnchor:
        return self.content.anchor

    @property
    def unknown(self) -> str:
        return self.content.unknown


@dataclass(frozen=True, slots=True)
class ResearchDecisionHead:
    """Mutable pointer represented as a CAS replacement value."""

    ledger_id: str
    board_id: str
    refinement_id: str
    current_entry_id: str
    revision: int
    refinement_version: int
    status: ResearchDecisionStatus
    updated_by: str
    updated_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "ledger_id",
            "board_id",
            "refinement_id",
            "current_entry_id",
            "updated_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_head_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "revision",
            _strict_positive_int(
                self.revision,
                "research_decision_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "refinement_version",
            _strict_positive_int(
                self.refinement_version,
                "research_decision_refinement_version_invalid",
            ),
        )
        _enum_instance(
            self.status,
            ResearchDecisionStatus,
            "research_decision_status_invalid",
        )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(
                self.updated_at,
                "research_decision_head_updated_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class RefinementVersionBump:
    board_id: str
    refinement_id: str
    expected_version: int
    resulting_version: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "research_decision_board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "refinement_id",
            _required_text(
                self.refinement_id,
                "research_decision_refinement_id_required",
            ),
        )
        expected = _strict_positive_int(
            self.expected_version,
            "research_decision_refinement_version_invalid",
        )
        resulting = _strict_positive_int(
            self.resulting_version,
            "research_decision_refinement_version_invalid",
        )
        if resulting != expected + 1:
            raise ResearchDecisionContractError(
                "research_decision_requires_single_refinement_version_bump"
            )
        object.__setattr__(self, "expected_version", expected)
        object.__setattr__(self, "resulting_version", resulting)


@dataclass(frozen=True, slots=True)
class ResearchDecisionHistoryIntent:
    id: str
    action: ResearchDecisionAction
    board_id: str
    refinement_id: str
    ledger_id: str
    entry_id: str
    predecessor_entry_id: str | None
    from_refinement_version: int
    to_refinement_version: int
    actor_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "board_id",
            "refinement_id",
            "ledger_id",
            "entry_id",
            "actor_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_history_{field_name}_required",
                ),
            )
        _enum_instance(
            self.action,
            ResearchDecisionAction,
            "research_decision_action_invalid",
        )
        if self.predecessor_entry_id is not None:
            object.__setattr__(
                self,
                "predecessor_entry_id",
                _required_text(
                    self.predecessor_entry_id,
                    "research_decision_predecessor_entry_id_invalid",
                ),
            )
        before = _strict_positive_int(
            self.from_refinement_version,
            "research_decision_refinement_version_invalid",
        )
        after = _strict_positive_int(
            self.to_refinement_version,
            "research_decision_refinement_version_invalid",
        )
        if after != before + 1:
            raise ResearchDecisionContractError(
                "research_decision_requires_single_refinement_version_bump"
            )
        object.__setattr__(self, "from_refinement_version", before)
        object.__setattr__(self, "to_refinement_version", after)
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "research_decision_history_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionEventIntent:
    id: str
    event_type: ResearchDecisionEventType
    board_id: str
    refinement_id: str
    refinement_version: int
    ledger_id: str
    entry_id: str
    actor_id: str
    actor_type: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "board_id",
            "refinement_id",
            "ledger_id",
            "entry_id",
            "actor_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_event_{field_name}_required",
                ),
            )
        _enum_instance(
            self.event_type,
            ResearchDecisionEventType,
            "research_decision_event_type_invalid",
        )
        actor_type = _required_text(
            self.actor_type,
            "research_decision_event_actor_type_required",
        )
        if actor_type not in {"agent", "user"}:
            raise ResearchDecisionContractError(
                "research_decision_event_actor_type_invalid"
            )
        object.__setattr__(self, "actor_type", actor_type)
        object.__setattr__(
            self,
            "refinement_version",
            _strict_positive_int(
                self.refinement_version,
                "research_decision_refinement_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "research_decision_event_occurred_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionOutboxIntent:
    id: str
    event_id: str
    event_type: ResearchDecisionEventType
    partition_key: str
    available_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("id", "event_id", "partition_key"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_outbox_{field_name}_required",
                ),
            )
        _enum_instance(
            self.event_type,
            ResearchDecisionEventType,
            "research_decision_event_type_invalid",
        )
        object.__setattr__(
            self,
            "available_at",
            _aware_utc(
                self.available_at,
                "research_decision_outbox_available_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionWriteBundle:
    """One atomic append/supersede unit handed to a transaction-bound adapter."""

    expected_head_revision: int
    expected_head_entry_id: str | None
    expected_refinement_status: RefinementStatus
    expected_refinement_archived: bool
    version_bump: RefinementVersionBump
    entry: ResearchDecisionEntry
    next_head: ResearchDecisionHead
    history: ResearchDecisionHistoryIntent
    event: ResearchDecisionEventIntent
    outbox: ResearchDecisionOutboxIntent
    idempotency_key: str | None = None
    request_digest: str | None = None

    def __post_init__(self) -> None:
        expected_revision = _strict_non_negative_int(
            self.expected_head_revision,
            "research_decision_head_revision_invalid",
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            expected_revision,
        )
        if self.expected_head_entry_id is not None:
            object.__setattr__(
                self,
                "expected_head_entry_id",
                _required_text(
                    self.expected_head_entry_id,
                    "research_decision_head_entry_id_invalid",
                ),
            )
        if (expected_revision == 0) != (self.expected_head_entry_id is None):
            raise ResearchDecisionContractError(
                "research_decision_expected_head_identity_mismatch"
            )
        if self.expected_refinement_status is not RefinementStatus.DRAFT:
            raise ResearchDecisionContractError(
                "research_decision_write_requires_draft_refinement"
            )
        if self.expected_refinement_archived is not False:
            raise ResearchDecisionContractError(
                "research_decision_write_requires_active_refinement"
            )
        for value, expected_type, code in (
            (
                self.version_bump,
                RefinementVersionBump,
                "research_decision_version_bump_invalid",
            ),
            (
                self.entry,
                ResearchDecisionEntry,
                "research_decision_entry_invalid",
            ),
            (
                self.next_head,
                ResearchDecisionHead,
                "research_decision_head_invalid",
            ),
            (
                self.history,
                ResearchDecisionHistoryIntent,
                "research_decision_history_invalid",
            ),
            (
                self.event,
                ResearchDecisionEventIntent,
                "research_decision_event_invalid",
            ),
            (
                self.outbox,
                ResearchDecisionOutboxIntent,
                "research_decision_outbox_invalid",
            ),
        ):
            _instance(value, expected_type, code)

        bump = self.version_bump
        entry = self.entry
        head = self.next_head
        history = self.history
        event = self.event
        outbox = self.outbox
        identity = (bump.board_id, bump.refinement_id)
        if any(
            candidate != identity
            for candidate in (
                (entry.board_id, entry.refinement_id),
                (head.board_id, head.refinement_id),
                (history.board_id, history.refinement_id),
                (event.board_id, event.refinement_id),
            )
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_scope_mismatch"
            )
        if any(
            candidate != entry.ledger_id
            for candidate in (
                head.ledger_id,
                history.ledger_id,
                event.ledger_id,
            )
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_ledger_mismatch"
            )
        if any(
            candidate != entry.id
            for candidate in (
                head.current_entry_id,
                history.entry_id,
                event.entry_id,
            )
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_entry_mismatch"
            )
        if (
            entry.refinement_version != bump.resulting_version
            or head.refinement_version != bump.resulting_version
            or history.from_refinement_version != bump.expected_version
            or history.to_refinement_version != bump.resulting_version
            or event.refinement_version != bump.resulting_version
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_version_mismatch"
            )
        if head.revision != expected_revision + 1 or head.status is not entry.status:
            raise ResearchDecisionContractError(
                "research_decision_bundle_head_mismatch"
            )
        if (
            entry.predecessor_entry_id != self.expected_head_entry_id
            or history.predecessor_entry_id != self.expected_head_entry_id
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_predecessor_mismatch"
            )
        expected_action = (
            ResearchDecisionAction.APPEND
            if expected_revision == 0
            else ResearchDecisionAction.SUPERSEDE
        )
        expected_event_type = (
            ResearchDecisionEventType.APPENDED
            if expected_revision == 0
            else ResearchDecisionEventType.SUPERSEDED
        )
        if (
            history.action is not expected_action
            or event.event_type is not expected_event_type
            or outbox.event_type is not expected_event_type
            or outbox.event_id != event.id
            or outbox.partition_key != bump.board_id
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_audit_mismatch"
            )
        if (
            entry.created_by != head.updated_by
            or entry.created_by != history.actor_id
            or entry.created_by != event.actor_id
            or entry.created_at != head.updated_at
            or entry.created_at != history.created_at
            or entry.created_at != event.occurred_at
            or entry.created_at != outbox.available_at
        ):
            raise ResearchDecisionContractError(
                "research_decision_bundle_provenance_mismatch"
            )
        if (self.idempotency_key is None) != (self.request_digest is None):
            raise ResearchDecisionContractError(
                "research_decision_idempotency_binding_incomplete"
            )
        if self.idempotency_key is not None:
            object.__setattr__(
                self,
                "idempotency_key",
                _required_text(
                    self.idempotency_key,
                    "research_decision_idempotency_key_required",
                ),
            )
            request_digest = _required_text(
                self.request_digest,
                "research_decision_request_digest_required",
            )
            if (
                len(request_digest) != 64
                or any(character not in "0123456789abcdef" for character in request_digest)
            ):
                raise ResearchDecisionContractError(
                    "research_decision_request_digest_invalid"
                )
            object.__setattr__(self, "request_digest", request_digest)
        durable_ids = {
            entry.ledger_id,
            entry.id,
            history.id,
            event.id,
            outbox.id,
        }
        if len(durable_ids) != 5:
            raise ResearchDecisionContractError(
                "research_decision_bundle_identity_collision"
            )


@dataclass(frozen=True, slots=True)
class ResearchDecisionCommitResult:
    entry: ResearchDecisionEntry
    head: ResearchDecisionHead
    refinement_version: int
    history_id: str
    event_id: str
    outbox_id: str
    replayed: bool = False

    def __post_init__(self) -> None:
        _instance(
            self.entry,
            ResearchDecisionEntry,
            "research_decision_entry_invalid",
        )
        _instance(
            self.head,
            ResearchDecisionHead,
            "research_decision_head_invalid",
        )
        version = _strict_positive_int(
            self.refinement_version,
            "research_decision_refinement_version_invalid",
        )
        object.__setattr__(self, "refinement_version", version)
        for field_name in ("history_id", "event_id", "outbox_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_{field_name}_required",
                ),
            )
        if not isinstance(self.replayed, bool):
            raise ResearchDecisionContractError("research_decision_replayed_invalid")
        if (
            self.head.current_entry_id != self.entry.id
            or self.head.ledger_id != self.entry.ledger_id
            or version != self.entry.refinement_version
            or version != self.head.refinement_version
        ):
            raise ResearchDecisionContractError(
                "research_decision_commit_result_mismatch"
            )


@dataclass(frozen=True, slots=True)
class ResearchDecisionFrozenHead:
    ledger_id: str
    entry_id: str
    head_revision: int
    head_refinement_version: int
    status: ResearchDecisionStatus
    content_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "ledger_id",
            _required_text(
                self.ledger_id,
                "research_decision_ledger_id_required",
            ),
        )
        object.__setattr__(
            self,
            "entry_id",
            _required_text(
                self.entry_id,
                "research_decision_entry_id_required",
            ),
        )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "research_decision_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "head_refinement_version",
            _strict_positive_int(
                self.head_refinement_version,
                "research_decision_refinement_version_invalid",
            ),
        )
        _enum_instance(
            self.status,
            ResearchDecisionStatus,
            "research_decision_status_invalid",
        )
        object.__setattr__(
            self,
            "content_digest",
            _content_digest(
                self.content_digest,
                "research_decision_content_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionLedgerSnapshot:
    """Frozen head identities captured at one Refinement version."""

    id: str
    board_id: str
    refinement_id: str
    refinement_version: int
    heads: tuple[ResearchDecisionFrozenHead, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("id", "board_id", "refinement_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_snapshot_{field_name}_required",
                ),
            )
        version = _strict_positive_int(
            self.refinement_version,
            "research_decision_refinement_version_invalid",
        )
        object.__setattr__(self, "refinement_version", version)
        heads = _typed_tuple(
            self.heads,
            ResearchDecisionFrozenHead,
            "research_decision_snapshot_heads_invalid",
        )
        if len({head.ledger_id for head in heads}) != len(heads):
            raise ResearchDecisionContractError(
                "research_decision_snapshot_duplicate_ledger"
            )
        if len({head.entry_id for head in heads}) != len(heads):
            raise ResearchDecisionContractError(
                "research_decision_snapshot_duplicate_entry"
            )
        if any(head.head_refinement_version > version for head in heads):
            raise ResearchDecisionContractError(
                "research_decision_snapshot_future_head"
            )
        object.__setattr__(
            self,
            "heads",
            tuple(sorted(heads, key=lambda head: head.ledger_id)),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "research_decision_snapshot_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionDerivationRef:
    """Read-only reference; decision content is never copied into a Spec."""

    source_snapshot_id: str
    source_refinement_id: str
    source_refinement_version: int
    ledger_id: str
    entry_id: str
    head_revision: int
    content_digest: str
    status: ResearchDecisionStatus = ResearchDecisionStatus.RESOLVED

    def __post_init__(self) -> None:
        for field_name in (
            "source_snapshot_id",
            "source_refinement_id",
            "ledger_id",
            "entry_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_reference_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "source_refinement_version",
            _strict_positive_int(
                self.source_refinement_version,
                "research_decision_refinement_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "research_decision_head_revision_invalid",
            ),
        )
        if self.status is not ResearchDecisionStatus.RESOLVED:
            raise ResearchDecisionContractError(
                "research_decision_derivation_requires_resolved_head"
            )
        object.__setattr__(
            self,
            "content_digest",
            _content_digest(
                self.content_digest,
                "research_decision_content_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionDerivation:
    """Version-bound resolved ledger references attached to a derived Spec."""

    board_id: str
    spec_id: str
    spec_version: int
    source_refinement_id: str
    source_refinement_version: int
    source_snapshot_id: str
    references: tuple[ResearchDecisionDerivationRef, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "board_id",
            "spec_id",
            "source_refinement_id",
            "source_snapshot_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"research_decision_derivation_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "research_decision_derivation_spec_version_invalid",
            ),
        )
        source_version = _strict_positive_int(
            self.source_refinement_version,
            "research_decision_refinement_version_invalid",
        )
        object.__setattr__(
            self,
            "source_refinement_version",
            source_version,
        )
        references = _typed_tuple(
            self.references,
            ResearchDecisionDerivationRef,
            "research_decision_derivation_references_invalid",
        )
        if len({reference.ledger_id for reference in references}) != len(references):
            raise ResearchDecisionContractError(
                "research_decision_derivation_duplicate_ledger"
            )
        for reference in references:
            if (
                reference.source_snapshot_id != self.source_snapshot_id
                or reference.source_refinement_id != self.source_refinement_id
                or reference.source_refinement_version != source_version
                or reference.status is not ResearchDecisionStatus.RESOLVED
            ):
                raise ResearchDecisionContractError(
                    "research_decision_derivation_reference_mismatch"
                )
        object.__setattr__(
            self,
            "references",
            tuple(sorted(references, key=lambda item: item.ledger_id)),
        )


@dataclass(frozen=True, slots=True)
class ResearchDecisionCursor:
    created_at: datetime
    entry_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "research_decision_cursor_created_at_invalid",
            ),
        )
        object.__setattr__(
            self,
            "entry_id",
            _required_text(
                self.entry_id,
                "research_decision_cursor_entry_id_required",
            ),
        )


PageItemT = TypeVar("PageItemT")


@dataclass(frozen=True, slots=True)
class ResearchDecisionPage(Generic[PageItemT]):
    """Keyset page ordered by ``created_at DESC, id DESC``."""

    items: tuple[PageItemT, ...]
    limit: int
    next_cursor: ResearchDecisionCursor | None
    has_more: bool

    ordering: ClassVar[tuple[str, str]] = ("created_at DESC", "id DESC")

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise ResearchDecisionContractError("research_decision_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        limit = _strict_positive_int(
            self.limit,
            "research_decision_page_limit_invalid",
        )
        if limit > RESEARCH_DECISION_PAGE_LIMIT_MAX:
            raise ResearchDecisionContractError("research_decision_page_limit_invalid")
        object.__setattr__(self, "limit", limit)
        if self.next_cursor is not None:
            _instance(
                self.next_cursor,
                ResearchDecisionCursor,
                "research_decision_page_cursor_invalid",
            )
        if not isinstance(self.has_more, bool):
            raise ResearchDecisionContractError(
                "research_decision_page_has_more_invalid"
            )
        if len(self.items) > limit:
            raise ResearchDecisionContractError("research_decision_page_over_limit")
        if self.has_more != (self.next_cursor is not None):
            raise ResearchDecisionContractError(
                "research_decision_page_cursor_mismatch"
            )


@dataclass(frozen=True, slots=True)
class ResearchDecisionOffsetPage(Generic[PageItemT]):
    """REST-only offset page with window-independent exact totals."""

    items: tuple[PageItemT, ...]
    offset: int
    limit: int
    total_filtered: int
    total_overall: int

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise ResearchDecisionContractError(
                "research_decision_page_items_invalid"
            )
        object.__setattr__(self, "items", tuple(self.items))
        offset = _strict_non_negative_int(
            self.offset,
            "research_decision_page_offset_invalid",
        )
        limit = _strict_positive_int(
            self.limit,
            "research_decision_page_limit_invalid",
        )
        if limit not in RESEARCH_DECISION_REST_PAGE_LIMITS:
            raise ResearchDecisionContractError(
                "research_decision_page_limit_invalid"
            )
        total_filtered = _strict_non_negative_int(
            self.total_filtered,
            "research_decision_page_total_invalid",
        )
        total_overall = _strict_non_negative_int(
            self.total_overall,
            "research_decision_page_total_invalid",
        )
        if total_filtered > total_overall or len(self.items) > limit:
            raise ResearchDecisionContractError(
                "research_decision_page_total_invalid"
            )
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "limit", limit)
        object.__setattr__(self, "total_filtered", total_filtered)
        object.__setattr__(self, "total_overall", total_overall)


__all__ = [
    "RESEARCH_DECISION_LEDGER_CONTRACT_VERSION",
    "RESEARCH_DECISION_PAGE_LIMIT_MAX",
    "RESEARCH_DECISION_REST_PAGE_LIMITS",
    "RefinementLedgerContext",
    "RefinementVersionBump",
    "ResearchDecisionAction",
    "ResearchDecisionAnchor",
    "ResearchDecisionAnchorType",
    "ResearchDecisionCommitResult",
    "ResearchDecisionContent",
    "ResearchDecisionContractError",
    "ResearchDecisionCursor",
    "ResearchDecisionDerivation",
    "ResearchDecisionDerivationRef",
    "ResearchDecisionEntry",
    "ResearchDecisionEventIntent",
    "ResearchDecisionEventType",
    "ResearchDecisionFrozenHead",
    "ResearchDecisionHead",
    "ResearchDecisionHistoryIntent",
    "ResearchDecisionLedgerSnapshot",
    "ResearchDecisionOutboxIntent",
    "ResearchDecisionOffsetPage",
    "ResearchDecisionPage",
    "ResearchDecisionStatus",
    "ResearchDecisionSubjectType",
    "ResearchDecisionWriteBundle",
    "research_decision_content_digest",
]
