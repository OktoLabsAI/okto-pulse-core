"""Transaction-bound persistence ports for the investigation attestation ledger.

Adapters stage data in the caller-owned unit of work.  They never commit,
rollback, contact an agent, open a repository, invoke Git, or inspect source.
Receipt insertion, request consumption, and head advancement form one atomic
CAS operation exposed explicitly below.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.code_traceability import (
    CodeInvestigationHead,
    CodeInvestigationHeadConflict,
    CodeInvestigationIdempotencyConflict,
    CodeInvestigationOutcome,
    CodeInvestigationReceipt,
    CodeInvestigationReceiptCommitResult,
    CodeInvestigationReceiptRevocation,
    CodeInvestigationRequest,
    CodeTraceabilityContractError,
    CodeTraceabilityPage,
    CodeTraceabilityPageCursor,
    CodeTraceabilityRemediation,
    CodeTraceabilitySubjectType,
)


class CodeInvestigationPersistenceError(RuntimeError):
    """Stable fail-closed adapter error with the public remediation envelope."""

    code = "code_investigation_persistence_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: Mapping[str, object] | None = None,
        remediation: tuple[CodeTraceabilityRemediation, ...] = (),
    ) -> None:
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("code_investigation_persistence_details_invalid")
        if not isinstance(remediation, tuple) or any(
            not isinstance(item, CodeTraceabilityRemediation) for item in remediation
        ):
            raise TypeError("code_investigation_persistence_remediation_invalid")
        self.message = message or self.code
        self.details = MappingProxyType(dict(details or {}))
        self.remediation = remediation
        super().__init__(self.message)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
            "remediation": [item.as_dict() for item in self.remediation],
        }

    def to_error_dict(self) -> dict[str, object]:
        return self.as_dict()


class CodeInvestigationAdapterMissing(CodeInvestigationPersistenceError):
    code = "code_investigation_adapter_missing"


class CodeInvestigationPersistenceConflict(CodeInvestigationPersistenceError):
    code = "code_investigation_persistence_conflict"


class CodeInvestigationImmutableConflict(CodeInvestigationPersistenceError):
    code = "code_investigation_receipt_immutable"


class CodeInvestigationCursorInvalid(CodeInvestigationPersistenceError):
    code = "code_investigation_cursor_invalid"


def _required(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CodeTraceabilityContractError(code)
    return value.strip()


def _aware(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise CodeTraceabilityContractError(code)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CodeInvestigationReceiptQuery:
    board_id: str
    subject_type: CodeTraceabilitySubjectType | None = None
    subject_id: str | None = None
    source_ref: str | None = None
    outcome: CodeInvestigationOutcome | None = None
    cursor: CodeTraceabilityPageCursor | None = None
    limit: int = 50

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required(self.board_id, "code_investigation_query_board_id_invalid"),
        )
        subject_type = self.subject_type
        if subject_type is not None and not isinstance(
            subject_type, CodeTraceabilitySubjectType
        ):
            try:
                subject_type = CodeTraceabilitySubjectType(subject_type)
            except (TypeError, ValueError) as exc:
                raise CodeTraceabilityContractError(
                    "code_investigation_query_subject_type_invalid"
                ) from exc
        object.__setattr__(self, "subject_type", subject_type)
        subject_id = (
            None
            if self.subject_id is None
            else _required(
                self.subject_id,
                "code_investigation_query_subject_id_invalid",
            )
        )
        if (subject_type is None) != (subject_id is None):
            raise CodeTraceabilityContractError(
                "code_investigation_query_subject_incoherent"
            )
        object.__setattr__(self, "subject_id", subject_id)
        object.__setattr__(
            self,
            "source_ref",
            (
                None
                if self.source_ref is None
                else _required(
                    self.source_ref,
                    "code_investigation_query_source_ref_invalid",
                )
            ),
        )
        outcome = self.outcome
        if outcome is not None and not isinstance(outcome, CodeInvestigationOutcome):
            try:
                outcome = CodeInvestigationOutcome(outcome)
            except (TypeError, ValueError) as exc:
                raise CodeTraceabilityContractError(
                    "code_investigation_query_outcome_invalid"
                ) from exc
        object.__setattr__(self, "outcome", outcome)
        if self.cursor is not None and not isinstance(
            self.cursor, CodeTraceabilityPageCursor
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_query_cursor_invalid"
            )
        if type(self.limit) is not int or not 1 <= self.limit <= 200:
            raise CodeTraceabilityContractError(
                "code_investigation_query_limit_invalid"
            )


@dataclass(frozen=True, slots=True)
class CodeInvestigationRequestReplay:
    request: CodeInvestigationRequest
    consumed_receipt_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.request, CodeInvestigationRequest):
            raise CodeTraceabilityContractError(
                "code_investigation_request_replay_invalid"
            )
        if self.consumed_receipt_id is not None:
            object.__setattr__(
                self,
                "consumed_receipt_id",
                _required(
                    self.consumed_receipt_id,
                    "code_investigation_replay_receipt_id_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class CodeInvestigationRequestCreateResult:
    """Outcome of the serialized replay/cap/insert request operation."""

    request: CodeInvestigationRequest
    replayed: bool = False
    consumed_receipt_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.request, CodeInvestigationRequest):
            raise CodeTraceabilityContractError(
                "code_investigation_request_create_result_invalid"
            )
        if not isinstance(self.replayed, bool):
            raise CodeTraceabilityContractError(
                "code_investigation_request_create_result_invalid"
            )
        if self.consumed_receipt_id is not None:
            object.__setattr__(
                self,
                "consumed_receipt_id",
                _required(
                    self.consumed_receipt_id,
                    "code_investigation_replay_receipt_id_invalid",
                ),
            )
        if not self.replayed and self.consumed_receipt_id is not None:
            raise CodeTraceabilityContractError(
                "code_investigation_request_create_result_invalid"
            )


@runtime_checkable
class CodeInvestigationReadPort(Protocol):
    """Board-scoped reads over persisted attestation metadata only."""

    async def get_request(
        self,
        *,
        board_id: str,
        request_id: str,
    ) -> CodeInvestigationRequest | None: ...

    async def resolve_request_replay(
        self,
        *,
        board_id: str,
        issued_to_actor_id: str,
        subject_type: CodeTraceabilitySubjectType,
        subject_id: str,
        subject_version: int,
        idempotency_key: str,
    ) -> CodeInvestigationRequestReplay | None: ...

    async def count_open_requests(
        self,
        *,
        board_id: str,
        issued_to_actor_id: str,
        at: datetime,
    ) -> int: ...

    async def get_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> CodeInvestigationReceipt | None: ...

    async def resolve_receipt_replay(
        self,
        *,
        board_id: str,
        attestor_actor_id: str,
        request_id: str,
        idempotency_key: str,
    ) -> CodeInvestigationReceipt | None: ...

    async def list_receipts(
        self,
        query: CodeInvestigationReceiptQuery,
    ) -> CodeTraceabilityPage[CodeInvestigationReceipt]: ...

    async def get_current_head(
        self,
        *,
        board_id: str,
        source_ref: str,
    ) -> CodeInvestigationHead | None: ...

    async def get_receipt_revocation(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> CodeInvestigationReceiptRevocation | None: ...


@runtime_checkable
class CodeInvestigationStore(CodeInvestigationReadPort, Protocol):
    """Append-only writes staged inside the caller-owned transaction."""

    async def create_request_if_below_open_limit(
        self,
        *,
        request: CodeInvestigationRequest,
        at: datetime,
        max_open_requests: int,
    ) -> CodeInvestigationRequestCreateResult:
        """Serialize replay lookup, active-open count, and insert atomically.

        Implementations must lock the ``(board_id, issued_to_actor_id)``
        admission scope (or provide an equivalent serializable transaction),
        return an existing idempotent request before applying the cap, count
        only non-expired ``open`` requests, and stage at most one insert.
        """
        ...

    async def consume_request_append_receipt_and_advance_head(
        self,
        *,
        request: CodeInvestigationRequest,
        receipt: CodeInvestigationReceipt,
        head: CodeInvestigationHead,
        expected_head_revision: int | None,
    ) -> CodeInvestigationReceiptCommitResult:
        """CAS request/head and append receipt atomically, without committing."""
        ...

    async def append_receipt_revocation(
        self,
        revocation: CodeInvestigationReceiptRevocation,
    ) -> CodeInvestigationReceiptRevocation: ...


__all__ = [
    "CodeInvestigationAdapterMissing",
    "CodeInvestigationCursorInvalid",
    "CodeInvestigationHeadConflict",
    "CodeInvestigationIdempotencyConflict",
    "CodeInvestigationImmutableConflict",
    "CodeInvestigationPersistenceConflict",
    "CodeInvestigationPersistenceError",
    "CodeInvestigationReadPort",
    "CodeInvestigationReceiptQuery",
    "CodeInvestigationRequestCreateResult",
    "CodeInvestigationRequestReplay",
    "CodeInvestigationStore",
]
