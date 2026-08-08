"""Canonical authorization contracts for dedicated KG operational use cases."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases import (
    cognitive_readiness,
    dlq_reprocess,
    kg_health,
    kg_routes_crud,
    list_cognitive_dlq,
    list_dead_letter_rows,
    list_stale_canonical_parity,
    mcp_kg_crud,
    operational_rest,
    queue_health,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.permissions import (
    KG_OPERATIONS_PERMISSION_INTRODUCTION_V1,
    PermissionSet,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


BOARD_ID = "board-kg-operations"


_NAMESPACE_REQUIREMENTS = (
    ("kg.operations.health.read", "kg.admin.settings_read"),
    ("kg.operations.integrity.read", "kg.admin.settings_read"),
    ("kg.operations.integrity.reconcile", "kg.admin.settings_write"),
    ("kg.operations.integrity.backfill", "kg.admin.settings_write"),
    ("kg.operations.cognitive.read", "kg.admin.settings_read"),
    ("kg.operations.cognitive.skip", "kg.admin.settings_write"),
    ("kg.operations.cognitive.clear", "kg.admin.settings_write"),
    ("kg.operations.queue.read", "kg.admin.settings_read"),
    ("kg.operations.queue.reprocess", "kg.admin.settings_write"),
    ("kg.operations.audit.read", "kg.admin.settings_read"),
    ("kg.operations.schema.migrate", "kg.admin.settings_write"),
    ("kg.operations.tick.run", "kg.admin.settings_write"),
    ("kg.operations.global_outbox.read", "kg.admin.settings_read"),
    ("kg.operations.global_outbox.reprocess", "kg.admin.settings_write"),
    ("kg.operations.global_outbox.verify", "kg.admin.settings_read"),
    ("kg.operations.global_recovery.preflight", "kg.admin.settings_read"),
    ("kg.operations.global_recovery.confirm", "kg.admin.settings_write"),
    ("kg.operations.global_recovery.read", "kg.admin.settings_read"),
    ("kg.operations.global_recovery.cancel", "kg.admin.settings_write"),
    ("kg.operations.global_recovery.resume", "kg.admin.settings_write"),
    ("kg.operations.global_recovery.run", "kg.admin.settings_write"),
    (
        "kg.operations.historical.read",
        "kg.admin.historical_consolidation",
    ),
    (
        "kg.operations.historical.start",
        "kg.admin.historical_consolidation",
    ),
    (
        "kg.operations.historical.cancel",
        "kg.admin.historical_consolidation",
    ),
    ("kg.operations.node.boost", "kg.admin.settings_write"),
    ("kg.operations.settings.read", "kg.admin.settings_read"),
    ("kg.operations.settings.write", "kg.admin.settings_write"),
    ("kg.operations.rebuild.preflight", "kg.admin.settings_read"),
    ("kg.operations.rebuild.confirm", "kg.admin.settings_write"),
    ("kg.operations.rebuild.run", "kg.admin.settings_write"),
    ("kg.operations.quarantine.restore", "kg.admin.settings_write"),
    ("kg.operations.board.erase", "kg.admin.wipe_board"),
)


def _permission_set(operation: str, historical_authority: str) -> PermissionSet:
    document: dict[str, Any] = {}
    for path in (operation, historical_authority):
        cursor = document
        parts = path.split(".")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = True
    return PermissionSet(document)


def test_authorization_matrix_covers_the_complete_kg_operations_namespace() -> None:
    assert tuple(operation for operation, _legacy in _NAMESPACE_REQUIREMENTS) == (
        KG_OPERATIONS_PERMISSION_INTRODUCTION_V1.leaves
    )


@pytest.mark.parametrize(("operation", "historical_authority"), _NAMESPACE_REQUIREMENTS)
def test_each_kg_operation_declares_and_accepts_its_two_authorities(
    operation: str,
    historical_authority: str,
) -> None:
    assert (
        KG_OPERATIONS_PERMISSION_INTRODUCTION_V1.historical_authority_for(operation)
        == historical_authority
    )
    requirement = PermissionRequirement(
        operation,
        legacy_operation=historical_authority,
    )

    canonical = decide_authorization(
        ActorContext(
            "operator",
            "mcp",
            permissions=_permission_set(operation, historical_authority),
        ),
        requirement,
    )
    historical = decide_authorization(
        ActorContext("legacy-operator", "mcp", permissions=[historical_authority]),
        requirement,
    )
    canonical_without_historical_ceiling = decide_authorization(
        ActorContext(
            "partial-operator",
            "mcp",
            permissions=_permission_set(operation, "kg.admin.unrelated"),
        ),
        requirement,
    )

    assert canonical.allowed is True
    assert historical.allowed is True
    assert canonical_without_historical_ceiling.allowed is False


class _BoardService:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get_board(
        self,
        board_id: str,
        actor_id: str,
        *,
        query_scope: object,
    ) -> object:
        assert actor_id == "operator"
        assert getattr(query_scope, "target_board_id") == board_id
        self._events.append(f"lookup:{board_id}")
        return SimpleNamespace(id=board_id, owner_id=actor_id)


class _BoardRepository:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get(self, board_id: str) -> object:
        self._events.append(f"lookup:{board_id}")
        return SimpleNamespace(
            id=board_id,
            owner_id="operator",
            realm_id=LOCAL_REALM_ID,
        )


class _KgWriterSpy:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def _write(self, name: str) -> None:
        self._events.append(f"write:{name}")

    async def start_historical_consolidation(self, _board_id: str) -> dict[str, Any]:
        self._write("historical.start")
        return {}

    async def cancel_historical(self, _board_id: str) -> dict[str, Any]:
        self._write("historical.cancel")
        return {}

    async def right_to_erasure(self, _board_id: str) -> dict[str, Any]:
        self._write("board.erase")
        return {}

    async def retry_pending_entry(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        self._write("queue.retry")
        return {}

    async def mutate_boost_node_graph(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("node.boost")
        return {}

    async def enqueue_digest_layer_reconciliation(
        self, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("integrity.reconcile")
        return {}

    async def reprocess_dead_letter_rows(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("queue.dead_letter_reprocess")
        return {}

    async def reprocess_connectivity_guard_dlq(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("queue.connectivity_reprocess")
        return {}

    async def invoke_rebuild_admission(
        self, *_args: Any, **_kwargs: Any
    ) -> None:
        self._write("rebuild.admission")


class _Uow:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.boards = _BoardRepository(self.events)
        self.services = SimpleNamespace(
            boards=_BoardService(self.events),
            kg=_KgWriterSpy(self.events),
        )
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


_WRITE_CASES: tuple[
    tuple[
        Any,
        Callable[[], Any],
        str,
        str,
        bool,
    ],
    ...,
] = (
    (
        kg_routes_crud.StartHistoricalUseCase(),
        lambda: kg_routes_crud.StartHistoricalCommand(BOARD_ID),
        "kg.operations.historical.start",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud.CancelHistoricalUseCase(),
        lambda: kg_routes_crud.CancelHistoricalCommand(BOARD_ID),
        "kg.operations.historical.cancel",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud.DeleteBoardKgUseCase(),
        lambda: kg_routes_crud.DeleteBoardKgCommand(BOARD_ID),
        "kg.operations.board.erase",
        "kg.admin.wipe_board",
        True,
    ),
    (
        kg_routes_crud.RetryPendingEntryUseCase(),
        lambda: kg_routes_crud.RetryPendingEntryCommand(BOARD_ID, "queue-1"),
        "kg.operations.queue.reprocess",
        "kg.admin.settings_write",
        True,
    ),
    (
        kg_routes_crud.BoostNodeUseCase(),
        lambda: kg_routes_crud.BoostNodeCommand(BOARD_ID, "node-1"),
        "kg.operations.node.boost",
        "kg.admin.settings_write",
        True,
    ),
    (
        mcp_kg_crud.ReconcileDigestLayerUseCase(),
        lambda: mcp_kg_crud.ReconcileDigestLayerCommand(
            BOARD_ID,
            reason="incident_42_digest_drift",
        ),
        "kg.operations.integrity.reconcile",
        "kg.admin.settings_write",
        True,
    ),
    (
        dlq_reprocess.ReprocessDeadLetterRowsUseCase(),
        lambda: dlq_reprocess.ReprocessDeadLetterRowsCommand(BOARD_ID),
        "kg.operations.queue.reprocess",
        "kg.admin.settings_write",
        False,
    ),
    (
        dlq_reprocess.ReprocessConnectivityDlqUseCase(),
        lambda: dlq_reprocess.ReprocessConnectivityDlqCommand(
            BOARD_ID,
            ["dead-letter-1"],
        ),
        "kg.operations.queue.reprocess",
        "kg.admin.settings_write",
        False,
    ),
    (
        mcp_kg_crud.RebuildAdmissionGateUseCase(),
        lambda: mcp_kg_crud.RebuildAdmissionGateCommand(
            BOARD_ID,
            refuse_fn=lambda *_args, **_kwargs: None,
            include_health=True,
        ),
        "kg.operations.rebuild.preflight",
        "kg.admin.settings_read",
        False,
    ),
    (
        mcp_kg_crud.RebuildAdmissionGateUseCase(),
        lambda: mcp_kg_crud.RebuildAdmissionGateCommand(
            BOARD_ID,
            refuse_fn=lambda *_args, **_kwargs: None,
            include_health=False,
        ),
        "kg.operations.rebuild.run",
        "kg.admin.settings_write",
        False,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command_factory", "operation", "legacy", "expects_lookup"),
    _WRITE_CASES,
    ids=(
        "historical-start",
        "historical-cancel",
        "board-erase",
        "pending-retry",
        "node-boost",
        "integrity-reconcile",
        "dead-letter-reprocess",
        "connectivity-reprocess",
        "rebuild-preflight",
        "rebuild-run",
    ),
)
async def test_each_dedicated_kg_writer_authorizes_after_lookup_and_before_write(
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    expects_lookup: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = (
        kg_routes_crud
        if use_case.__class__.__module__.endswith("kg_routes_crud")
        else dlq_reprocess
        if use_case.__class__.__module__.endswith("dlq_reprocess")
        else mcp_kg_crud
    )
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs["board_id"] == BOARD_ID
    assert kwargs["uow"] is uow
    assert uow.events == ([f"lookup:{BOARD_ID}"] if expects_lookup else [])
    assert uow.commits == 0
    assert uow.rollbacks == 0


_REST_OPERATION_CASES: tuple[
    tuple[Any, Callable[[], Any], str, str],
    ...,
] = (
    (
        operational_rest.RecordCognitiveSkipUseCase(),
        lambda: operational_rest.CognitiveSkipCommand(
            BOARD_ID,
            "spec:1",
            "operator_decision",
            None,
            None,
            None,
            None,
        ),
        "kg.operations.cognitive.skip",
        "kg.admin.settings_write",
    ),
    (
        operational_rest.ClearCognitiveSkipUseCase(),
        lambda: operational_rest.CognitiveClearCommand(BOARD_ID, "spec:1", None),
        "kg.operations.cognitive.clear",
        "kg.admin.settings_write",
    ),
    (
        operational_rest.GetCognitiveReadinessMetricsUseCase(),
        lambda: operational_rest.CognitiveReadinessMetricsCommand(BOARD_ID, None),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.GetCognitiveEffectivenessInventoryUseCase(),
        lambda: operational_rest.CognitiveEffectivenessInventoryCommand(
            BOARD_ID,
            None,
            False,
            "canonical",
            None,
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.ListCanonicalDebtUseCase(),
        lambda: operational_rest.CanonicalDebtListCommand(
            BOARD_ID,
            None,
            None,
            50,
            0,
        ),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.RetryCanonicalDebtUseCase(),
        lambda: operational_rest.CanonicalDebtRetryCommand(
            BOARD_ID,
            "debt-1",
            None,
        ),
        "kg.operations.queue.reprocess",
        "kg.admin.settings_write",
    ),
    (
        operational_rest.ListCanonicalPartitionIntegrityUseCase(),
        lambda: operational_rest.CanonicalPartitionListCommand(
            BOARD_ID,
            None,
            None,
            None,
            None,
            None,
            50,
            0,
        ),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.GetCanonicalPartitionIntegrityDetailUseCase(),
        lambda: operational_rest.CanonicalPartitionDetailCommand(BOARD_ID, "node-1"),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.ListDigestLayerMismatchUseCase(),
        lambda: operational_rest.DigestLayerMismatchListCommand(BOARD_ID, 50, 0),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.GetOrphanIntegrityReportUseCase(),
        lambda: operational_rest.OrphanIntegrityReportCommand(BOARD_ID, None, 25),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.RunOrphanBackfillUseCase(),
        lambda: operational_rest.OrphanBackfillCommand(
            BOARD_ID,
            None,
            True,
            None,
            25,
            None,
        ),
        "kg.operations.integrity.backfill",
        "kg.admin.settings_write",
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command_factory", "operation", "legacy"),
    _REST_OPERATION_CASES,
    ids=(
        "cognitive-skip",
        "cognitive-clear",
        "cognitive-metrics",
        "cognitive-inventory",
        "canonical-debt-list",
        "canonical-debt-retry",
        "partition-list",
        "partition-detail",
        "digest-list",
        "orphan-report",
        "orphan-backfill",
    ),
)
async def test_operational_rest_authorizes_before_any_kg_service_call(
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(operational_rest, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "rest",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs == {"uow": uow, "board_id": BOARD_ID}
    assert uow.events == [f"lookup:{BOARD_ID}"]
    assert uow.commits == 0
    assert uow.rollbacks == 0


_READ_CASES: tuple[
    tuple[Any, Any, Callable[[], Any], str, str, bool],
    ...,
] = (
    (
        kg_health,
        kg_health.GetKgHealthUseCase(),
        lambda: kg_health.GetKgHealthCommand(BOARD_ID),
        "kg.operations.health.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_health,
        kg_health.GetKgHealthReadinessUseCase(),
        lambda: kg_health.GetKgHealthReadinessCommand(BOARD_ID),
        "kg.operations.health.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.EvaluateBugCognitiveClosureUseCase(),
        lambda: cognitive_readiness.EvaluateBugCognitiveClosureCommand(
            BOARD_ID,
            "bug-1",
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.ListCognitiveReadinessItemsUseCase(),
        lambda: cognitive_readiness.ListCognitiveReadinessItemsCommand(BOARD_ID),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.EvaluateCognitiveReadinessUseCase(),
        lambda: cognitive_readiness.EvaluateCognitiveReadinessCommand(
            BOARD_ID,
            source_ref="spec:1",
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListAuditUseCase(),
        lambda: kg_routes_crud.ListAuditCommand(BOARD_ID, limit=50),
        "kg.operations.audit.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.GetHistoricalProgressUseCase(),
        lambda: kg_routes_crud.GetHistoricalProgressCommand(BOARD_ID),
        "kg.operations.historical.read",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListPendingUseCase(),
        lambda: kg_routes_crud.ListPendingCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListPendingTreeUseCase(),
        lambda: kg_routes_crud.ListPendingTreeCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        mcp_kg_crud,
        mcp_kg_crud.ListCanonicalDebtUseCase(),
        lambda: mcp_kg_crud.ListCanonicalDebtCommand(BOARD_ID),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        mcp_kg_crud,
        mcp_kg_crud.ListCanonicalPartitionIntegrityUseCase(),
        lambda: mcp_kg_crud.ListCanonicalPartitionIntegrityCommand(BOARD_ID),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        mcp_kg_crud,
        mcp_kg_crud.ListDigestLayerMismatchUseCase(),
        lambda: mcp_kg_crud.ListDigestLayerMismatchCommand(BOARD_ID),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        mcp_kg_crud,
        mcp_kg_crud.AuditOriginatesFromContractUseCase(),
        lambda: mcp_kg_crud.AuditOriginatesFromContractCommand(BOARD_ID),
        "kg.operations.audit.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        dlq_reprocess,
        dlq_reprocess.DiagnoseConnectivityDlqUseCase(),
        lambda: dlq_reprocess.DiagnoseConnectivityDlqCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        dlq_reprocess,
        dlq_reprocess.VerifyConnectivityClassUseCase(),
        lambda: dlq_reprocess.VerifyConnectivityClassCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        list_cognitive_dlq,
        list_cognitive_dlq.ListCognitiveDlqUseCase(),
        lambda: list_cognitive_dlq.ListCognitiveDlqCommand(
            BOARD_ID,
            limit=50,
            offset=0,
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        list_dead_letter_rows,
        list_dead_letter_rows.ListDeadLetterRowsUseCase(),
        lambda: list_dead_letter_rows.ListDeadLetterRowsCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        list_stale_canonical_parity,
        list_stale_canonical_parity.ListStaleCanonicalParityUseCase(),
        lambda: list_stale_canonical_parity.ListStaleCanonicalParityCommand(
            BOARD_ID
        ),
        "kg.operations.integrity.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        queue_health,
        queue_health.GetQueueDrilldownUseCase(),
        lambda: queue_health.GetQueueDrilldownCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "module",
        "use_case",
        "command_factory",
        "operation",
        "legacy",
        "expects_lookup",
    ),
    _READ_CASES,
    ids=(
        "health",
        "health-readiness",
        "bug-cognitive",
        "cognitive-list",
        "cognitive-evaluate",
        "audit-list",
        "historical-progress",
        "pending-list",
        "pending-tree",
        "canonical-debt",
        "partition-integrity",
        "digest-mismatch",
        "origin-audit",
        "connectivity-diagnose",
        "connectivity-verify",
        "cognitive-dlq",
        "dead-letter-list",
        "stale-parity",
        "queue-drilldown",
    ),
)
async def test_each_dedicated_kg_reader_checks_the_specific_operation(
    module: Any,
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    expects_lookup: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs["board_id"] == BOARD_ID
    assert kwargs["uow"] is uow
    assert uow.events == ([f"lookup:{BOARD_ID}"] if expects_lookup else [])
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_global_queue_reader_uses_the_same_canonical_requirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[PermissionRequirement, ...]] = []

    async def _deny(
        _actor: ActorContext,
        *requirements: PermissionRequirement,
        **_kwargs: Any,
    ) -> None:
        captured.append(requirements)
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(queue_health, "require_any_authority", _deny)
    uow = _Uow()

    with pytest.raises(PermissionDeniedError, match="denied"):
        await queue_health.GetQueueHealthUseCase().execute(
            queue_health.GetQueueHealthCommand(),
            actor=ActorContext("operator", "rest", permissions=()),
            uow=uow,
        )

    assert captured == [
        (
            PermissionRequirement(
                "kg.operations.queue.read",
                legacy_operation="kg.admin.settings_read",
            ),
        )
    ]
    assert uow.events == []
    assert uow.commits == 0
