"""KG operational MCP use cases.

Transport-free reimplementations of the read-only KG drill-down MCP tools that
still opened ``get_db_for_mcp()`` directly (spec R01A MCP-FU5). Each use case
delegates to the EXISTING reader so the tool payload stays byte-identical; the
public signature never exposes ``AsyncSession`` / ``get_db_for_mcp``. Read-only:
no commit. Mirrors the per-reader pattern of ``list_stale_canonical_parity``.

The MCP tool wires these through ``get_unit_of_work_factory_for_mcp()(actor=...)``
instead of ``get_db_for_mcp()``; ``CognitiveReadinessError`` from the
partition-integrity reader is intentionally NOT caught here so the tool keeps its
legacy ``exc.to_dict()`` envelope.

Public manual reconciliation is retired; automatic parity events remain internal.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)


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
        from okto_pulse.core.services.canonical_debt_service import (
            validate_canonical_debt_filters,
        )

        validate_canonical_debt_filters(
            artifact_type=command.artifact_type,
            state=command.state,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.integrity.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
            EvaluateCodeTraceabilityKGReadAccessUseCase,
        )

        ct_access = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
            actor=actor,
            board_id=command.board_id,
            uow=uow,
        )
        kwargs = {
            "board_id": command.board_id,
            "artifact_type": command.artifact_type,
            "state": command.state,
            "limit": command.limit,
            "offset": command.offset,
        }
        if not ct_access.allowed:
            kwargs["include_code_traceability"] = False
        data = await uow.services.kg.list_canonical_debt(**kwargs)
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.integrity.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.integrity.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.list_digest_layer_mismatches(
            board_id=command.board_id,
            limit=command.limit,
            offset=command.offset,
        )
        return ListDigestLayerMismatchResult(data)
