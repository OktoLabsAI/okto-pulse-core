"""Lifecycle-edition aware A3 checklist persistence and preflight ports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.domain.checklist import (
    ChecklistBinding,
    ChecklistCommitResult,
    ChecklistExecution,
    ChecklistExecutionHead,
    ChecklistExecutionStartResult,
    ChecklistPage,
    ChecklistPhase,
    ChecklistPreflight,
    ChecklistReceipt,
    ChecklistReceiptState,
    ChecklistSpecSnapshot,
    ChecklistSubmission,
    ChecklistTargetType,
    ChecklistWriteBundle,
)


class ChecklistPersistenceError(RuntimeError):
    """Base transport-neutral adapter error."""

    code = "checklist_persistence_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.details = dict(details or {})
        super().__init__(message or self.code)


class ChecklistAdapterMissing(ChecklistPersistenceError):
    code = "checklist_adapter_missing"


class ChecklistHeadRevisionConflict(ChecklistPersistenceError):
    code = "checklist_head_revision_conflict"


class ChecklistSpecVersionConflict(ChecklistPersistenceError):
    code = "checklist_spec_version_conflict"


class ChecklistSpecEditionConflict(ChecklistPersistenceError):
    code = "checklist_spec_edition_conflict"


class ChecklistContentDigestConflict(ChecklistPersistenceError):
    code = "checklist_content_digest_conflict"


class ChecklistInputDigestConflict(ChecklistPersistenceError):
    code = "checklist_input_digest_conflict"


class ChecklistTemplateConflict(ChecklistPersistenceError):
    code = "checklist_template_conflict"


class ChecklistBindingConflict(ChecklistPersistenceError):
    code = "checklist_binding_conflict"


class ChecklistIdempotencyConflict(ChecklistPersistenceError):
    code = "checklist_idempotency_conflict"


class ChecklistSpecLifecycleConflict(ChecklistPersistenceError):
    code = "checklist_spec_lifecycle_conflict"


class ChecklistExecutionRevisionConflict(ChecklistPersistenceError):
    code = "checklist_execution_revision_conflict"


class ChecklistExecutionStateConflict(ChecklistPersistenceError):
    code = "checklist_execution_state_conflict"


@dataclass(frozen=True, slots=True)
class ChecklistListQuery:
    board_id: str
    spec_id: str
    offset: int
    limit: int
    current_spec_edition: int | None = None
    state: ChecklistReceiptState | None = None


@runtime_checkable
class ChecklistPreflightReadPort(Protocol):
    """Resolve read-only fences before the caller opens a writable UoW."""

    async def lookup_checklist_replay(
        self,
        *,
        board_id: str,
        idempotency_key: str,
        actor_id: str,
    ) -> ChecklistCommitResult | None:
        """Return only a native replay visible inside the requested board."""
        ...

    async def resolve_checklist_preflight(
        self,
        submission: ChecklistSubmission,
        *,
        actor_id: str,
    ) -> ChecklistPreflight: ...


@runtime_checkable
class ChecklistPersistencePort(Protocol):
    """Transaction-bound persistence with no UoW or commit ownership.

    ``apply_execution_cas`` must repeat native idempotency lookup inside the
    caller's transaction and atomically compare Spec existence/lifecycle,
    version, content and input digests, template version/digest, executable
    binding digest, current binding admission (mode is not ``off``), head
    revision, and predecessor receipt. Binding version/mode remain immutable
    audit facts on executions and receipts, but a governance-only mode/version
    revision is not an executable-identity conflict. A native matching key plus
    request digest returns the original commit result; a different digest raises
    :class:`ChecklistIdempotencyConflict`. Legacy manual references carry no
    idempotency key and must never replay.

    The method stages the append-only receipt and head update but never commits
    or creates a unit of work.  Every conflict is expressed as one of the
    transport-neutral exceptions in this module.

    Binding versions are immutable rows. ``apply_binding_cas`` stages the next
    version and atomically advances its composite board/target/phase head; it
    likewise never commits.
    """

    async def apply_execution_cas(
        self,
        bundle: ChecklistWriteBundle,
    ) -> ChecklistCommitResult: ...

    async def start_execution_cas(
        self,
        execution: ChecklistExecution,
    ) -> ChecklistExecutionStartResult: ...

    async def submit_execution_cas(
        self,
        execution: ChecklistExecution,
        *,
        expected_revision: int,
        bundle: ChecklistWriteBundle,
    ) -> ChecklistCommitResult: ...

    async def apply_binding_cas(
        self,
        binding: ChecklistBinding,
        *,
        expected_version: int,
        expected_digest: str | None,
    ) -> ChecklistBinding: ...

    async def get_binding(
        self,
        *,
        board_id: str,
        target_type: ChecklistTargetType,
        phase: ChecklistPhase,
    ) -> ChecklistBinding | None: ...

    async def get_validation_binding(
        self,
        *,
        board_id: str,
        spec_id: str,
        spec_edition: int,
        target_type: ChecklistTargetType,
        phase: ChecklistPhase,
    ) -> ChecklistBinding:
        """Resolve the immutable governance snapshot for one Spec edition.

        The snapshot includes ``OFF`` and is keyed by board/Spec/edition. The
        adapter pins it atomically when the edition first enters its validation
        lifecycle (with a first-read/write fallback for migrated rows). Later
        board binding/template changes never alter this result; they apply only
        after the Spec returns to Draft and opens a new edition. Legacy subjects
        without an edition continue to use ``get_binding``.
        """

        ...

    async def get_current(
        self,
        *,
        board_id: str,
        spec_id: str,
        phase: ChecklistPhase,
        spec_edition: int | None = None,
    ) -> tuple[ChecklistReceipt, ChecklistExecutionHead] | None: ...

    async def get_execution(
        self,
        *,
        board_id: str,
        spec_id: str,
        execution_id: str,
    ) -> ChecklistExecution | None: ...

    async def get_spec_snapshot(
        self,
        *,
        board_id: str,
        spec_id: str,
    ) -> ChecklistSpecSnapshot | None: ...

    async def get_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
    ) -> ChecklistReceipt | None: ...

    async def list_executions(
        self,
        query: ChecklistListQuery,
    ) -> ChecklistPage[ChecklistReceipt]: ...


__all__ = [
    "ChecklistAdapterMissing",
    "ChecklistBindingConflict",
    "ChecklistContentDigestConflict",
    "ChecklistExecutionRevisionConflict",
    "ChecklistExecutionStateConflict",
    "ChecklistHeadRevisionConflict",
    "ChecklistIdempotencyConflict",
    "ChecklistInputDigestConflict",
    "ChecklistListQuery",
    "ChecklistPersistenceError",
    "ChecklistPersistencePort",
    "ChecklistPreflightReadPort",
    "ChecklistSpecLifecycleConflict",
    "ChecklistSpecEditionConflict",
    "ChecklistSpecVersionConflict",
    "ChecklistTemplateConflict",
]
