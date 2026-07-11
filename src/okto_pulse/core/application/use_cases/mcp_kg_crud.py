"""KG canonical-debt / partition-integrity / digest-mismatch MCP read use cases.

Transport-free reimplementations of the read-only KG drill-down MCP tools that
still opened ``get_db_for_mcp()`` directly (spec R01A MCP-FU5). Each use case
delegates to the EXISTING reader so the tool payload stays byte-identical; the
public signature never exposes ``AsyncSession`` / ``get_db_for_mcp``. Read-only:
no commit. Mirrors the per-reader pattern of ``list_stale_canonical_parity``.

The MCP tool wires these through ``get_unit_of_work_factory_for_mcp()(actor=...)``
instead of ``get_db_for_mcp()``; ``CognitiveReadinessError`` from the
partition-integrity reader is intentionally NOT caught here so the tool keeps its
legacy ``exc.to_dict()`` envelope.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.ports.scheduler import SchedulerControl


class ListCanonicalDebtCommand:
    """Input for :class:`ListCanonicalDebtUseCase`."""

    __slots__ = ("board_id", "artifact_type", "state", "limit", "offset")

    def __init__(
        self,
        board_id: str,
        *,
        artifact_type: str | None = None,
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> None:
        self.board_id = board_id
        self.artifact_type = artifact_type
        self.state = state
        self.limit = limit
        self.offset = offset


class ListCanonicalDebtResult:
    """Output — carries the reader result object (``.items``/``.counts``/``.total``)."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListCanonicalDebtUseCase:
    """List canonical-debt ledger rows without any transport dependency."""

    async def execute(
        self, command: ListCanonicalDebtCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListCanonicalDebtResult:
        data = await uow.services.kg.list_canonical_debt(
            board_id=command.board_id,
            artifact_type=command.artifact_type,
            state=command.state,
            limit=command.limit,
            offset=command.offset,
        )
        return ListCanonicalDebtResult(data)


class ListCanonicalPartitionIntegrityCommand:
    """Input for :class:`ListCanonicalPartitionIntegrityUseCase`."""

    __slots__ = (
        "board_id", "reason_code", "graph_layer", "source_ref",
        "node_id", "status", "limit", "offset",
    )

    def __init__(
        self,
        board_id: str,
        *,
        reason_code: str | None = None,
        graph_layer: str | None = None,
        source_ref: str | None = None,
        node_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> None:
        self.board_id = board_id
        self.reason_code = reason_code
        self.graph_layer = graph_layer
        self.source_ref = source_ref
        self.node_id = node_id
        self.status = status
        self.limit = limit
        self.offset = offset


class ListCanonicalPartitionIntegrityResult:
    """Output — the drilldown mapping the legacy tool returned directly."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListCanonicalPartitionIntegrityUseCase:
    """List canonical Learning partition-integrity signals (R7), transport-free.

    ``CognitiveReadinessError`` propagates UNCAUGHT so the MCP tool keeps its
    legacy ``exc.to_dict()`` envelope at the adapter boundary.
    """

    async def execute(
        self,
        command: ListCanonicalPartitionIntegrityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListCanonicalPartitionIntegrityResult:
        data = await uow.services.kg.list_canonical_partition_integrity(
            board_id=command.board_id,
            reason_code=command.reason_code,
            graph_layer=command.graph_layer,
            source_ref=command.source_ref,
            node_id=command.node_id,
            status=command.status,
            limit=command.limit,
            offset=command.offset,
        )
        return ListCanonicalPartitionIntegrityResult(data)


class ListDigestLayerMismatchCommand:
    """Input for :class:`ListDigestLayerMismatchUseCase`."""

    __slots__ = ("board_id", "limit", "offset")

    def __init__(self, board_id: str, *, limit: int = 50, offset: int = 0) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset


class ListDigestLayerMismatchResult:
    """Output — the drilldown mapping the legacy tool returned directly."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListDigestLayerMismatchUseCase:
    """List Global Discovery digest/board layer mismatches (R1), transport-free."""

    async def execute(
        self,
        command: ListDigestLayerMismatchCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListDigestLayerMismatchResult:
        data = await uow.services.kg.list_digest_layer_mismatches(
            board_id=command.board_id,
            limit=command.limit,
            offset=command.offset,
        )
        return ListDigestLayerMismatchResult(data)


class AuditOriginatesFromContractCommand:
    """Input for :class:`AuditOriginatesFromContractUseCase`."""

    __slots__ = ("board_id", "limit", "offset", "include_ok")

    def __init__(
        self,
        board_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
        include_ok: bool = False,
    ) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset
        self.include_ok = include_ok


class AuditOriginatesFromContractResult:
    """Output — the advisory audit mapping returned by the reader."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class AuditOriginatesFromContractUseCase:
    """Audit persisted ``originates_from`` endpoint drift without mutation."""

    async def execute(
        self,
        command: AuditOriginatesFromContractCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AuditOriginatesFromContractResult:
        from okto_pulse.core.services.application_kg import (
            audit_originates_from_contract,
        )

        _ = actor
        data = audit_originates_from_contract(
            command.board_id,
            limit=command.limit,
            offset=command.offset,
            include_ok=command.include_ok,
        )
        return AuditOriginatesFromContractResult(data)


class RebuildAdmissionGateCommand:
    """Input for :class:`RebuildAdmissionGateUseCase`.

    ``refuse_fn`` is the admission-gate probe injected by the MCP tool
    (``api.kg_rebuild._refuse_rebuild_if_quarantined``); it is passed as a
    callable so this use case never imports ``okto_pulse.community.api`` (Clean
    Core). ``include_health`` adds the FR9 health probe in the SAME session,
    exactly as the legacy preflight tool did (no extra round-trip).
    """

    __slots__ = ("board_id", "refuse_fn", "include_health", "scheduler_control")

    def __init__(
        self,
        board_id: str,
        *,
        refuse_fn: Any,
        include_health: bool = False,
        scheduler_control: SchedulerControl | None = None,
    ) -> None:
        self.board_id = board_id
        self.refuse_fn = refuse_fn
        self.include_health = include_health
        self.scheduler_control = scheduler_control


class RebuildAdmissionGateResult:
    """Output — ``refusal`` (admission denied) XOR ``raw_health`` (probe data)."""

    __slots__ = ("refusal", "raw_health")

    def __init__(
        self, *, refusal: Any | None = None, raw_health: Any | None = None
    ) -> None:
        self.refusal = refusal
        self.raw_health = raw_health


class RebuildAdmissionGateUseCase:
    """Run the rebuild admission gate (+ optional FR9 health probe) under one
    session, transport-free. Strangles ONLY the session block of the
    preflight/run rebuild tools — the threadpool enumeration, RebuildPreflight/
    KGRebuildService orchestration and manifest/file persistence stay in the tool
    (no transactional-scope change). Read-only: no commit.
    """

    async def execute(
        self,
        command: RebuildAdmissionGateCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RebuildAdmissionGateResult:
        refusal = await uow.services.kg.invoke_rebuild_admission(
            command.refuse_fn,
            command.board_id,
            scheduler_control=command.scheduler_control,
        )
        if refusal is not None:
            return RebuildAdmissionGateResult(refusal=refusal)
        raw_health: Any | None = None
        if command.include_health:
            raw_health = await uow.services.kg.health(
                command.board_id,
                scheduler_control=command.scheduler_control,
            )
        return RebuildAdmissionGateResult(raw_health=raw_health)
