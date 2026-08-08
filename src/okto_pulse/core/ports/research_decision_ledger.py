"""Edition-neutral ports for the Refinement research-decision ledger."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.research_decision_ledger import (
    RESEARCH_DECISION_PAGE_LIMIT_MAX,
    RESEARCH_DECISION_REST_PAGE_LIMITS,
    ResearchDecisionCommitResult,
    ResearchDecisionCursor,
    ResearchDecisionDerivation,
    ResearchDecisionEntry,
    ResearchDecisionHead,
    ResearchDecisionLedgerSnapshot,
    ResearchDecisionOffsetPage,
    ResearchDecisionPage,
    ResearchDecisionStatus,
    ResearchDecisionWriteBundle,
)


class ResearchDecisionPersistenceError(RuntimeError):
    """Base transport-neutral adapter failure."""

    code = "research_decision_persistence_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class ResearchDecisionAdapterMissing(ResearchDecisionPersistenceError):
    code = "research_decision_adapter_missing"


class ResearchDecisionHeadRevisionConflict(ResearchDecisionPersistenceError):
    code = "research_decision_head_revision_conflict"


class ResearchDecisionRefinementVersionConflict(ResearchDecisionPersistenceError):
    code = "research_decision_refinement_version_conflict"


class ResearchDecisionRefinementStatusConflict(ResearchDecisionPersistenceError):
    code = "research_decision_refinement_status_conflict"


class ResearchDecisionRefinementLifecycleConflict(ResearchDecisionPersistenceError):
    code = "research_decision_refinement_lifecycle_conflict"


class ResearchDecisionScopeConflict(ResearchDecisionPersistenceError):
    code = "research_decision_scope_conflict"


class ResearchDecisionImmutableEntryConflict(ResearchDecisionPersistenceError):
    code = "research_decision_immutable_entry_conflict"


class ResearchDecisionIdempotencyConflict(ResearchDecisionPersistenceError):
    code = "research_decision_idempotency_conflict"


@dataclass(frozen=True, slots=True)
class ResearchDecisionListQuery:
    """Stable keyset query; adapters must use the frozen ordering."""

    board_id: str
    refinement_id: str
    limit: int = 50
    cursor: ResearchDecisionCursor | None = None
    ledger_id: str | None = None
    status: ResearchDecisionStatus | None = None

    ordering = ("created_at DESC", "id DESC")

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("research_decision_board_id_required")
        if not isinstance(self.refinement_id, str) or not self.refinement_id.strip():
            raise ValueError("research_decision_refinement_id_required")
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= RESEARCH_DECISION_PAGE_LIMIT_MAX
        ):
            raise ValueError(
                "research_decision_page_limit_invalid: limit must be 1..200"
            )
        object.__setattr__(self, "board_id", self.board_id.strip())
        object.__setattr__(self, "refinement_id", self.refinement_id.strip())
        if self.cursor is not None and not isinstance(
            self.cursor,
            ResearchDecisionCursor,
        ):
            raise ValueError("research_decision_cursor_invalid")
        if self.ledger_id is not None:
            if not isinstance(self.ledger_id, str) or not self.ledger_id.strip():
                raise ValueError("research_decision_ledger_id_invalid")
            object.__setattr__(self, "ledger_id", self.ledger_id.strip())
        if self.status is not None and not isinstance(
            self.status,
            ResearchDecisionStatus,
        ):
            raise ValueError("research_decision_status_invalid")


@dataclass(frozen=True, slots=True)
class ResearchDecisionOffsetListQuery:
    """REST-only offset/count query using the product page-size menu."""

    board_id: str
    refinement_id: str
    offset: int = 0
    limit: int = 25
    ledger_id: str | None = None
    status: ResearchDecisionStatus | None = None

    ordering = ("created_at DESC", "id DESC")

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("research_decision_board_id_required")
        if not isinstance(self.refinement_id, str) or not self.refinement_id.strip():
            raise ValueError("research_decision_refinement_id_required")
        if (
            not isinstance(self.offset, int)
            or isinstance(self.offset, bool)
            or self.offset < 0
        ):
            raise ValueError("research_decision_page_offset_invalid")
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or self.limit not in RESEARCH_DECISION_REST_PAGE_LIMITS
        ):
            raise ValueError("research_decision_page_limit_invalid")
        object.__setattr__(self, "board_id", self.board_id.strip())
        object.__setattr__(self, "refinement_id", self.refinement_id.strip())
        if self.ledger_id is not None:
            if not isinstance(self.ledger_id, str) or not self.ledger_id.strip():
                raise ValueError("research_decision_ledger_id_invalid")
            object.__setattr__(self, "ledger_id", self.ledger_id.strip())
        if self.status is not None and not isinstance(
            self.status,
            ResearchDecisionStatus,
        ):
            raise ValueError("research_decision_status_invalid")


@runtime_checkable
class ResearchDecisionLedgerReadPort(Protocol):
    async def get_entry(
        self,
        *,
        board_id: str,
        refinement_id: str,
        entry_id: str,
    ) -> ResearchDecisionEntry | None: ...

    async def get_current(
        self,
        *,
        board_id: str,
        refinement_id: str,
        ledger_id: str,
    ) -> tuple[ResearchDecisionEntry, ResearchDecisionHead] | None: ...

    async def list_current_heads(
        self,
        *,
        board_id: str,
        refinement_id: str,
    ) -> tuple[ResearchDecisionHead, ...]: ...

    async def list_current_entries_with_heads(
        self,
        *,
        board_id: str,
        refinement_id: str,
    ) -> tuple[
        tuple[ResearchDecisionEntry, ResearchDecisionHead],
        ...,
    ]: ...

    async def list_entries(
        self,
        query: ResearchDecisionListQuery,
    ) -> ResearchDecisionPage[ResearchDecisionEntry]: ...

    async def list_entries_offset(
        self,
        query: ResearchDecisionOffsetListQuery,
    ) -> ResearchDecisionOffsetPage[ResearchDecisionEntry]: ...

    async def get_snapshot(
        self,
        *,
        board_id: str,
        refinement_id: str,
        snapshot_id: str,
    ) -> ResearchDecisionLedgerSnapshot | None: ...

    async def get_snapshot_for_version(
        self,
        *,
        board_id: str,
        refinement_id: str,
        refinement_version: int,
    ) -> ResearchDecisionLedgerSnapshot | None: ...

    async def get_derivation(
        self,
        *,
        board_id: str,
        spec_id: str,
        spec_version: int,
    ) -> ResearchDecisionDerivation | None: ...


@runtime_checkable
class ResearchDecisionLedgerPersistencePort(
    ResearchDecisionLedgerReadPort,
    Protocol,
):
    """Transaction-bound append-only source of truth.

    ``apply_bundle_cas`` must atomically:

    * revalidate the board/refinement identity, active lifecycle, ``draft``
      status, and the expected Refinement version;
    * insert the immutable entry (never update an existing entry);
    * insert or replace the head only when its revision and current-entry
      predicates match the bundle;
    * bump ``Refinement.version`` exactly once from ``N`` to ``N + 1``; and
    * stage the history, domain event, and outbox records from the same bundle.

    The adapter operates in the caller's transaction and never commits,
    rolls back, closes the transaction, or opens another unit of work.
    """

    async def apply_bundle_cas(
        self,
        bundle: ResearchDecisionWriteBundle,
    ) -> ResearchDecisionCommitResult: ...

    async def resolve_idempotent_result(
        self,
        *,
        board_id: str,
        refinement_id: str,
        idempotency_key: str,
        request_digest: str,
    ) -> ResearchDecisionCommitResult | None:
        """Return the original receipt for an exact replay.

        A key already bound to another request digest must fail closed with
        :class:`ResearchDecisionIdempotencyConflict`.
        """

        ...

    async def save_snapshot(
        self,
        snapshot: ResearchDecisionLedgerSnapshot,
    ) -> ResearchDecisionLedgerSnapshot:
        """Persist an immutable, version-bound Refinement head snapshot."""

        ...

    async def save_derivation(
        self,
        derivation: ResearchDecisionDerivation,
    ) -> ResearchDecisionDerivation:
        """Persist immutable resolved references for one Spec version."""

        ...


__all__ = [
    "ResearchDecisionAdapterMissing",
    "ResearchDecisionHeadRevisionConflict",
    "ResearchDecisionImmutableEntryConflict",
    "ResearchDecisionIdempotencyConflict",
    "ResearchDecisionLedgerPersistencePort",
    "ResearchDecisionLedgerReadPort",
    "ResearchDecisionListQuery",
    "ResearchDecisionOffsetListQuery",
    "ResearchDecisionPersistenceError",
    "ResearchDecisionRefinementLifecycleConflict",
    "ResearchDecisionRefinementStatusConflict",
    "ResearchDecisionRefinementVersionConflict",
    "ResearchDecisionScopeConflict",
]
