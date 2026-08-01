"""Transport-free ``guideline-export/v3`` application orchestration.

The domain codec owns the closed wire contract and the zero-overwrite import
planner.  These use cases add only the application concerns shared by future
REST and MCP adapters:

* authorize every board before obtaining the policy persistence adapter;
* keep owner/realm scoping at the caller-owned unit of work boundary;
* load one consistent snapshot before building or planning;
* apply a conflict-free import in one adapter call and one commit; and
* roll back conflicts and persistence failures without exposing a transport
  framework or a concrete database type.

Legacy ``schema_version=1`` dispatch remains a domain concern.  The existing
v1 routes intentionally continue to use their compatibility use cases until
the inbound surfaces migrate in SK-B B13/B14.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.board_access import (
    load_accessible_board,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    METRICS_AUTHOR,
    REVISIONS_CREATE,
    REVISIONS_READ,
    _require_capability,
)
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineExportEnvelope,
    GuidelineImportPlan,
    GuidelineImportResult,
    GuidelineImportTransactionStatus,
    build_guideline_export_v3,
    parse_guideline_export,
    plan_guideline_import,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    GuidelineScope,
    normalize_policy_bounded_text,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


_BOARD_WRITE_SHARE_PERMISSIONS = frozenset({"editor", "admin"})
Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _bounded_text(value: object, max_length: int, code: str) -> str:
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _bounded_optional_text(
    value: object,
    max_length: int,
    code: str,
) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, max_length, code)


def _clock_value(clock: Clock) -> datetime:
    value = clock()
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError("guideline_import_export_clock_invalid")
    return value.astimezone(timezone.utc)


async def _require_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> None:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=(_BOARD_WRITE_SHARE_PERMISSIONS if write else None),
    )
    if board is None:
        # Missing and unauthorized boards deliberately share the same envelope.
        raise EntityNotFoundError("board", board_id)


def _target_board_ids(
    envelope: GuidelineExportEnvelope,
    *,
    target_board_id: str | None,
) -> tuple[str, ...]:
    """Resolve every board whose import candidate ledger may be affected.

    A target remap collapses all inline identities and source binding histories
    onto that one authorized target.  Without a remap, inline identities and
    imported binding histories retain their source board as inert candidate
    provenance, so each board must be authorized before the adapter is reached.
    """

    if target_board_id is not None:
        return (target_board_id,)
    board_ids: set[str] = set()
    for aggregate in envelope.guidelines:
        if (
            aggregate.identity.scope is GuidelineScope.INLINE
            and aggregate.identity.board_id is not None
        ):
            board_ids.add(aggregate.identity.board_id)
        board_ids.update(
            exported_binding.binding.board_id for exported_binding in aggregate.bindings
        )
    return tuple(sorted(board_ids))


@dataclass(frozen=True, slots=True)
class ExportGuidelinePolicyCommand:
    """Select one actor-owned policy snapshot for canonical v3 export."""

    board_id: str | None = None
    guideline_ids: tuple[str, ...] = ()
    include_binding_history: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_optional_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "guideline_export_board_id_invalid",
            ),
        )
        if not isinstance(self.guideline_ids, tuple | list):
            raise ValueError("guideline_export_guideline_ids_invalid")
        guideline_ids = tuple(
            _bounded_text(
                item,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_export_guideline_id_invalid",
            )
            for item in self.guideline_ids
        )
        if len(set(guideline_ids)) != len(guideline_ids):
            raise ValueError("guideline_export_guideline_ids_duplicate")
        object.__setattr__(self, "guideline_ids", tuple(sorted(guideline_ids)))
        if not isinstance(self.include_binding_history, bool):
            raise ValueError("guideline_export_include_binding_history_invalid")


@dataclass(frozen=True, slots=True)
class ExportGuidelinePolicyResult:
    envelope: GuidelineExportEnvelope


class ExportGuidelinePolicyV3UseCase:
    """Build a canonical v3 envelope from one authorized snapshot."""

    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    async def execute(
        self,
        command: ExportGuidelinePolicyCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ExportGuidelinePolicyResult:
        _require_capability(actor, REVISIONS_READ)
        if command.board_id is not None:
            await _require_board_access(
                uow,
                command.board_id,
                actor,
                write=False,
            )
        port = uow.services.guidelines.policy_persistence()
        snapshot = await port.export_guideline_snapshot(
            owner_id=actor.actor_id,
            board_id=command.board_id,
            guideline_ids=(command.guideline_ids or None),
            include_binding_history=command.include_binding_history,
        )
        envelope = build_guideline_export_v3(
            snapshot,
            exported_at=_clock_value(self._clock),
        )
        return ExportGuidelinePolicyResult(envelope=envelope)


# Temporary import compatibility for callers migrating in parallel.  The old
# name is intentionally excluded from ``__all__`` and always executes v3.
ExportGuidelinePolicyV2UseCase = ExportGuidelinePolicyV3UseCase


@dataclass(frozen=True, slots=True)
class ImportGuidelinePolicyCommand:
    """Fully encoded v1/v2/v3 envelope plus an optional target-board remap."""

    envelope: Mapping[str, Any]
    target_board_id: str | None = None
    dry_run: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, Mapping):
            raise ValueError("guideline_import_envelope_invalid")
        object.__setattr__(
            self,
            "target_board_id",
            _bounded_optional_text(
                self.target_board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "guideline_import_target_board_id_invalid",
            ),
        )
        if not isinstance(self.dry_run, bool):
            raise ValueError("guideline_import_dry_run_invalid")


@dataclass(frozen=True, slots=True)
class ImportGuidelinePolicyResult:
    result: GuidelineImportResult
    plan: GuidelineImportPlan


class ImportGuidelinePolicyUseCase:
    """Dispatch, authorize, plan, and atomically apply a v1/v2/v3 import."""

    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    async def execute(
        self,
        command: ImportGuidelinePolicyCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImportGuidelinePolicyResult:
        _require_capability(actor, REVISIONS_CREATE)
        # The closed codec validates the whole document before any repository or
        # persistence-adapter access.  A malformed late item therefore cannot
        # leave an earlier mutation behind.
        operation_at = _clock_value(self._clock)
        envelope = parse_guideline_export(command.envelope)
        if any(
            exported_revision.revision.metrics
            for aggregate in envelope.guidelines
            for exported_revision in aggregate.revisions
        ):
            _require_capability(actor, METRICS_AUTHOR)
        target_boards = _target_board_ids(
            envelope,
            target_board_id=command.target_board_id,
        )
        for board_id in target_boards:
            await _require_board_access(
                uow,
                board_id,
                actor,
                write=True,
            )

        port = uow.services.guidelines.policy_persistence()
        source_guideline_ids = tuple(
            aggregate.guideline_id for aggregate in envelope.guidelines
        )
        existing = await port.load_guideline_import_snapshot(
            guideline_ids=source_guideline_ids,
        )
        plan = plan_guideline_import(
            envelope,
            existing_aggregates=existing.aggregates,
            dry_run=command.dry_run,
            target_owner_id=actor.actor_id,
            target_board_id=command.target_board_id,
        )

        if plan.transaction_status is GuidelineImportTransactionStatus.ROLLED_BACK:
            await uow.rollback()
            return ImportGuidelinePolicyResult(
                result=GuidelineImportResult.from_plan(plan),
                plan=plan,
            )
        if plan.dry_run:
            return ImportGuidelinePolicyResult(
                result=GuidelineImportResult.from_plan(plan),
                plan=plan,
            )

        try:
            # The adapter receives the whole prevalidated plan once and may
            # write only immutable aggregate rows plus inert candidate-ledger
            # records.  Live bindings are intentionally absent from the plan.
            await port.apply_guideline_import_plan(
                plan,
                imported_by=actor.actor_id,
                imported_at=operation_at,
                import_digest=plan.import_digest,
            )
            await uow.commit()
        except Exception:
            await uow.rollback()
            raise
        return ImportGuidelinePolicyResult(
            result=GuidelineImportResult.from_plan(plan, committed=True),
            plan=plan,
        )


__all__ = [
    "ExportGuidelinePolicyCommand",
    "ExportGuidelinePolicyResult",
    "ExportGuidelinePolicyV3UseCase",
    "ImportGuidelinePolicyCommand",
    "ImportGuidelinePolicyResult",
    "ImportGuidelinePolicyUseCase",
]
