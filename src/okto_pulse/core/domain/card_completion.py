"""Typed completion decisions and immutable card rejection causes.

Rejected is a consequence of an admitted completion attempt, never a command
target.  These value objects keep that distinction explicit and prevent
transport or infrastructure failures from being mistaken for human rework.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Iterable, Mapping


REJECTION_ID_MAX_LENGTH = 128
REJECTION_CODE_MAX_LENGTH = 128
REJECTION_SUMMARY_MAX_LENGTH = 1024
REJECTION_REASON_CODE_MAX_LENGTH = 128
REJECTION_REASON_CODE_MAX_COUNT = 32


def _required_token(value: str, *, name: str, max_length: int) -> str:
    normalized = str(value).strip()
    if not normalized or len(normalized) > max_length:
        raise ValueError(f"{name} must contain 1..{max_length} characters")
    return normalized


def _bounded_summary(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("rejection summary is required")
    if len(normalized) <= REJECTION_SUMMARY_MAX_LENGTH:
        return normalized
    return normalized[: REJECTION_SUMMARY_MAX_LENGTH - 1].rstrip() + "…"


def _bounded_reason_codes(values: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = _required_token(
            raw,
            name="rejection reason code",
            max_length=REJECTION_REASON_CODE_MAX_LENGTH,
        )
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
        if len(normalized) == REJECTION_REASON_CODE_MAX_COUNT:
            break
    return tuple(normalized)


class TaskValidationOutcome(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


class CardCompletionOutcome(str, Enum):
    COMPLETED = "completed"
    REJECTED = "rejected"


class CardRejectionKind(str, Enum):
    TASK_VALIDATION = "task_validation"
    COMPLETION_GATE = "completion_gate"


@dataclass(frozen=True, slots=True)
class CompletionGateFailure:
    """A known domain blocker returned by a governed completion gate."""

    code: str
    summary: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "code",
            _required_token(
                self.code,
                name="completion gate code",
                max_length=REJECTION_CODE_MAX_LENGTH,
            ),
        )
        object.__setattr__(self, "summary", _bounded_summary(self.summary))
        object.__setattr__(
            self,
            "reason_codes",
            _bounded_reason_codes(self.reason_codes),
        )


@dataclass(frozen=True, slots=True)
class CardCompletionDecision:
    validation_outcome: TaskValidationOutcome
    completion_outcome: CardCompletionOutcome
    gate_failures: tuple[CompletionGateFailure, ...] = ()

    def __post_init__(self) -> None:
        rejected = self.completion_outcome is CardCompletionOutcome.REJECTED
        if self.validation_outcome is TaskValidationOutcome.FAILED:
            if not rejected:
                raise ValueError("failed validation cannot complete a card")
        elif rejected != bool(self.gate_failures):
            raise ValueError(
                "successful validation is rejected exactly when a completion gate blocks"
            )


@dataclass(frozen=True, slots=True)
class CardRejectionCause:
    """Bounded Current pointer projection; source history remains append-only."""

    kind: CardRejectionKind
    id: str
    code: str
    summary: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            _required_token(
                self.id,
                name="rejection cause id",
                max_length=REJECTION_ID_MAX_LENGTH,
            ),
        )
        object.__setattr__(
            self,
            "code",
            _required_token(
                self.code,
                name="rejection cause code",
                max_length=REJECTION_CODE_MAX_LENGTH,
            ),
        )
        object.__setattr__(self, "summary", _bounded_summary(self.summary))

    def as_dict(self) -> dict[str, str]:
        payload = asdict(self)
        payload["kind"] = self.kind.value
        return payload


@dataclass(frozen=True, slots=True)
class CardRejectionRecord:
    id: str
    card_id: str
    board_id: str
    kind: CardRejectionKind
    code: str
    summary: str
    reason_codes: tuple[str, ...]
    created_by: str
    created_at: str
    subject_version: int
    source_id: str | None = None

    def __post_init__(self) -> None:
        for field_name, value, max_length in (
            ("id", self.id, REJECTION_ID_MAX_LENGTH),
            ("card_id", self.card_id, REJECTION_ID_MAX_LENGTH),
            ("board_id", self.board_id, REJECTION_ID_MAX_LENGTH),
            ("code", self.code, REJECTION_CODE_MAX_LENGTH),
            ("created_by", self.created_by, 255),
            ("created_at", self.created_at, 64),
        ):
            object.__setattr__(
                self,
                field_name,
                _required_token(value, name=field_name, max_length=max_length),
            )
        if self.source_id is not None:
            object.__setattr__(
                self,
                "source_id",
                _required_token(
                    self.source_id,
                    name="rejection source id",
                    max_length=REJECTION_ID_MAX_LENGTH,
                ),
            )
        if self.subject_version < 1:
            raise ValueError("rejection subject_version must be positive")
        object.__setattr__(self, "summary", _bounded_summary(self.summary))
        object.__setattr__(
            self,
            "reason_codes",
            _bounded_reason_codes(self.reason_codes),
        )

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["kind"] = self.kind.value
        payload["reason_codes"] = list(self.reason_codes)
        return payload


def decide_card_completion(
    *,
    validation_outcome: TaskValidationOutcome | str,
    gate_failures: Iterable[CompletionGateFailure] = (),
) -> CardCompletionDecision:
    """Return the sole typed decision consumed by a completion mutation."""

    validation = TaskValidationOutcome(validation_outcome)
    failures = tuple(gate_failures)
    return CardCompletionDecision(
        validation_outcome=validation,
        completion_outcome=(
            CardCompletionOutcome.REJECTED
            if validation is TaskValidationOutcome.FAILED or failures
            else CardCompletionOutcome.COMPLETED
        ),
        gate_failures=failures,
    )


def _field(subject: object, name: str) -> object:
    if isinstance(subject, Mapping):
        return subject.get(name)
    return getattr(subject, name, None)


def _enum_text(value: object) -> str:
    if value is None:
        return ""
    return str(getattr(value, "value", value)).strip()


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 1 else None


def _record_from_payload(payload: object) -> CardRejectionRecord | None:
    if not isinstance(payload, Mapping):
        return None
    raw_reasons = payload.get("reason_codes")
    if not isinstance(raw_reasons, (list, tuple)):
        return None
    subject_version = _positive_int(payload.get("subject_version"))
    if subject_version is None:
        return None
    try:
        return CardRejectionRecord(
            id=str(payload.get("id") or ""),
            card_id=str(payload.get("card_id") or ""),
            board_id=str(payload.get("board_id") or ""),
            kind=CardRejectionKind(_enum_text(payload.get("kind"))),
            code=str(payload.get("code") or ""),
            summary=str(payload.get("summary") or ""),
            reason_codes=tuple(str(item) for item in raw_reasons),
            created_by=str(payload.get("created_by") or ""),
            created_at=str(payload.get("created_at") or ""),
            subject_version=subject_version,
            source_id=(
                str(payload["source_id"])
                if payload.get("source_id") is not None
                else None
            ),
        )
    except (TypeError, ValueError):
        return None


def resolve_current_rejection_record(subject: object) -> CardRejectionRecord | None:
    """Resolve Current to its sealed history record and admitted validation.

    The four scalar ``current_rejection_*`` fields are only a convenient read
    projection.  They are not independently authoritative: the Current id must
    identify exactly one append-only rejection record, that record must agree
    with the projection and its source must identify the admitted validation
    that produced the lifecycle consequence.  Partial legacy rows therefore
    fail closed until the migration repairs or quarantines them.
    """

    current_kind = _enum_text(_field(subject, "current_rejection_kind"))
    current_id = _enum_text(_field(subject, "current_rejection_id"))
    current_code = _enum_text(_field(subject, "current_rejection_code"))
    current_summary = _enum_text(_field(subject, "current_rejection_summary"))
    card_id = _enum_text(_field(subject, "id"))
    board_id = _enum_text(_field(subject, "board_id"))
    if not all(
        (current_kind, current_id, current_code, current_summary, card_id, board_id)
    ):
        return None

    raw_records = _field(subject, "rejection_records")
    if not isinstance(raw_records, (list, tuple)):
        return None
    matching_records = [
        record
        for payload in raw_records
        if (record := _record_from_payload(payload)) is not None
        and record.id == current_id
    ]
    if len(matching_records) != 1:
        return None
    record = matching_records[0]
    if (
        record.card_id != card_id
        or record.board_id != board_id
        or record.kind.value != current_kind
        or record.code != current_code
        or record.summary != current_summary
    ):
        return None

    source_id = record.source_id
    if not source_id:
        return None
    raw_validations = _field(subject, "validations")
    if not isinstance(raw_validations, (list, tuple)):
        return None
    sources = [
        item
        for item in raw_validations
        if isinstance(item, Mapping) and _enum_text(item.get("id")) == source_id
    ]
    if len(sources) != 1:
        return None
    source = sources[0]
    if (
        _enum_text(source.get("card_id")) != card_id
        or _enum_text(source.get("board_id")) != board_id
        or _positive_int(source.get("expected_subject_version"))
        != record.subject_version
        or _enum_text(source.get("completion_outcome"))
        != CardCompletionOutcome.REJECTED.value
    ):
        return None

    validation_outcome = _enum_text(source.get("validation_outcome"))
    if record.kind is CardRejectionKind.TASK_VALIDATION:
        if (
            validation_outcome != TaskValidationOutcome.FAILED.value
            or record.code != "task_validation_failed"
        ):
            return None
    else:
        failures = source.get("completion_gate_failures")
        if (
            validation_outcome != TaskValidationOutcome.SUCCESS.value
            or not isinstance(failures, (list, tuple))
            or record.code
            not in {
                _enum_text(item.get("code"))
                for item in failures
                if isinstance(item, Mapping)
            }
        ):
            return None
    return record


def current_rejection_cause(subject: object) -> CardRejectionCause | None:
    """Read Current only when its sealed causal history resolves completely."""

    record = resolve_current_rejection_record(subject)
    if record is None:
        return None
    return CardRejectionCause(
        kind=record.kind,
        id=record.id,
        code=record.code,
        summary=record.summary,
    )


def card_is_rejected(subject: object) -> bool:
    """Return whether the authoritative card lifecycle state is Rejected."""

    status = (
        subject.get("status")
        if isinstance(subject, Mapping)
        else getattr(subject, "status", None)
    )
    return str(getattr(status, "value", status)).strip().casefold() == "rejected"


__all__ = [
    "CardCompletionDecision",
    "CardCompletionOutcome",
    "CardRejectionCause",
    "CardRejectionKind",
    "CardRejectionRecord",
    "CompletionGateFailure",
    "REJECTION_CODE_MAX_LENGTH",
    "REJECTION_ID_MAX_LENGTH",
    "REJECTION_REASON_CODE_MAX_COUNT",
    "REJECTION_REASON_CODE_MAX_LENGTH",
    "REJECTION_SUMMARY_MAX_LENGTH",
    "TaskValidationOutcome",
    "card_is_rejected",
    "current_rejection_cause",
    "decide_card_completion",
    "resolve_current_rejection_record",
]
