"""Focused authorization oracles for board-scoped REST use cases."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.discovery_crud import (
    ExecuteDiscoveryIntentCommand,
    ExecuteDiscoveryIntentUseCase,
)
from okto_pulse.core.application.use_cases.list_dead_letter_rows import (
    DeadLetterBoardNotFoundError,
    ListDeadLetterRowsCommand,
    ListDeadLetterRowsUseCase,
)
from okto_pulse.core.application.use_cases.operational_rest import (
    BoardNotFoundError,
    BugNotFoundError,
    CanonicalDebtListCommand,
    CanonicalDebtRetryCommand,
    CanonicalPartitionDetailCommand,
    CanonicalPartitionListCommand,
    ClearCognitiveSkipUseCase,
    CognitiveClearCommand,
    CognitiveEffectivenessInventoryCommand,
    CognitiveReadinessMetricsCommand,
    CognitiveSkipCommand,
    DigestLayerMismatchListCommand,
    EvaluateBugCognitiveClosureByBugIdCommand,
    EvaluateBugCognitiveClosureByBugIdUseCase,
    GetCanonicalPartitionIntegrityDetailUseCase,
    GetCognitiveEffectivenessInventoryUseCase,
    GetCognitiveReadinessMetricsUseCase,
    GetLineageGraphCommand,
    GetLineageGraphUseCase,
    GetOrphanIntegrityReportUseCase,
    ListCanonicalDebtUseCase,
    ListCanonicalPartitionIntegrityUseCase,
    ListDigestLayerMismatchUseCase,
    OrphanBackfillCommand,
    OrphanIntegrityReportCommand,
    PutRuntimeSettingsCommand,
    PutRuntimeSettingsUseCase,
    RecordCognitiveSkipUseCase,
    RetryCanonicalDebtUseCase,
    RunOrphanBackfillUseCase,
)
from okto_pulse.core.application.use_cases.queue_health import (
    GetQueueDrilldownCommand,
    GetQueueDrilldownUseCase,
    GetQueueHealthCommand,
    GetQueueHealthUseCase,
    QueueBoardNotFoundError,
)


class _Boards:
    def __init__(self, board, events: list[str]) -> None:
        self._board = board
        self._events = events

    async def get(self, board_id: str):
        self._events.append(f"board:{board_id}")
        return self._board


class _Shares:
    def __init__(self, permission, events: list[str]) -> None:
        self._permission = permission
        self._events = events

    async def get_user_permission(self, board_id: str, actor_id: str):
        self._events.append(f"share:{board_id}:{actor_id}")
        return self._permission


class _Cards:
    def __init__(self, card, events: list[str]) -> None:
        self._card = card
        self._events = events

    async def get_card(self, card_id: str):
        self._events.append(f"card:{card_id}")
        return self._card


class _DiscoveryCatalog:
    def __init__(self, intent, events: list[str]) -> None:
        self._intent = intent
        self._events = events

    async def get_intent(self, intent_id: str):
        self._events.append(f"intent:{intent_id}")
        return self._intent


class _Kg:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def evaluate_bug_cognitive_closure(self, *args, **kwargs):
        self._events.append("cognitive")
        return {"ok": True}

    async def record_cognitive_skip(self, *args, **kwargs):
        self._events.append("record-skip")
        return {"id": "skip-1"}

    async def evaluate_cognitive_readiness(self, *args, **kwargs):
        self._events.append("evaluate-readiness")
        return {"ready": True}

    async def cognitive_enforcement_active(self, *args, **kwargs):
        self._events.append("read-enforcement")
        return True

    async def queue_health(self):
        self._events.append("queue-health")
        return {"queue_depth": 0}

    async def queue_drilldown(self, board_id):
        self._events.append(f"queue-drilldown:{board_id}")
        return {"board_id": board_id}

    async def list_dead_letter_rows(self, board_id, **kwargs):
        self._events.append(f"dead-letter:{board_id}")
        return {"rows": [], "total": 0, **kwargs}


class _Services:
    def __init__(self, *, events, permission=None, card=None, intent=None) -> None:
        self.shares = _Shares(permission, events)
        self.cards = _Cards(card, events)
        self.discovery_catalog = _DiscoveryCatalog(intent, events)
        self.kg = _Kg(events)
        self._events = events

    async def build_lineage_graph(self, board_id: str, **kwargs):
        self._events.append("lineage")
        return {"board_id": board_id}

    async def execute_discovery_intent(self, **kwargs):
        self._events.append("dispatch")
        return {"rows": []}

    async def put_runtime_settings(self, values, **kwargs):
        self._events.append("put-runtime")
        return {**values, "actor_id": kwargs["actor_id"]}


class _Uow:
    def __init__(self, *, board, permission=None, card=None, intent=None) -> None:
        self.events: list[str] = []
        self.boards = _Boards(board, self.events)
        self.services = _Services(
            events=self.events,
            permission=permission,
            card=card,
            intent=intent,
        )


ACTOR = ActorContext("user-a", "rest")
KG_QUEUE_READER = ActorContext(
    "user-a",
    "rest",
    board_id="board-b",
    permissions={
        "kg": {
            "operations": {"queue": {"read": True}},
            "admin": {"settings_read": True},
        }
    },
)
KG_COGNITIVE_SKIPPER = ActorContext(
    "user-a",
    "rest",
    board_id="board-b",
    permissions={
        "kg": {
            "operations": {"cognitive": {"skip": True}},
            "admin": {"settings_write": True},
        }
    },
)
FOREIGN_BOARD = SimpleNamespace(id="board-b", owner_id="user-b")


@pytest.mark.asyncio
async def test_lineage_denied_board_is_not_found_before_graph_reader() -> None:
    uow = _Uow(board=FOREIGN_BOARD)

    with pytest.raises(BoardNotFoundError):
        await GetLineageGraphUseCase().execute(
            GetLineageGraphCommand("board-b", "spec", "spec-b", False),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.events == ["board:board-b", "share:board-b:user-a"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("board", "permission"),
    [
        (SimpleNamespace(id="board-b", owner_id="user-a"), None),
        (FOREIGN_BOARD, "read"),
    ],
)
async def test_lineage_owner_or_shared_member_can_reach_graph_reader(
    board,
    permission,
) -> None:
    uow = _Uow(board=board, permission=permission)

    result = await GetLineageGraphUseCase().execute(
        GetLineageGraphCommand("board-b", "spec", "spec-b", False),
        actor=ACTOR,
        uow=uow,
    )

    assert result.data == {"board_id": "board-b"}
    assert uow.events[-1] == "lineage"


@pytest.mark.asyncio
async def test_discovery_denied_board_is_not_found_before_dispatcher() -> None:
    intent = SimpleNamespace(id="intent-1", name="Recent", active=True)
    uow = _Uow(board=FOREIGN_BOARD, intent=intent)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await ExecuteDiscoveryIntentUseCase().execute(
            ExecuteDiscoveryIntentCommand("intent-1", "board-b", {}),
            actor=ACTOR,
            uow=uow,
        )

    assert exc_info.value.entity_type == "board"
    assert "dispatch" not in uow.events


@pytest.mark.asyncio
async def test_discovery_shared_member_can_reach_dispatcher() -> None:
    intent = SimpleNamespace(id="intent-1", name="Recent", active=True)
    uow = _Uow(board=FOREIGN_BOARD, permission="read", intent=intent)

    result = await ExecuteDiscoveryIntentUseCase().execute(
        ExecuteDiscoveryIntentCommand("intent-1", "board-b", {}),
        actor=ACTOR,
        uow=uow,
    )

    assert result.payload["intent_id"] == "intent-1"
    assert uow.events[-1] == "dispatch"


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_action", ["evaluate", "skip", "no_action"])
async def test_bug_denied_board_never_reaches_readiness_or_ledger(
    requested_action: str,
) -> None:
    uow = _Uow(
        board=FOREIGN_BOARD,
        card=SimpleNamespace(id="bug-b", board_id="board-b", card_type="bug"),
    )

    with pytest.raises(BugNotFoundError):
        await EvaluateBugCognitiveClosureByBugIdUseCase().execute(
            EvaluateBugCognitiveClosureByBugIdCommand(
                "bug-b",
                {"root_cause": "known"},
                requested_action,
                "trivial_fix",
                "foreign board must remain untouched",
                None,
                None,
            ),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.events == [
        "card:bug-b",
        "board:board-b",
        "share:board-b:user-a",
    ]


def _operational_case_builders():
    return [
        (
            "record-cognitive-skip",
            lambda events: (
                RecordCognitiveSkipUseCase(
                    readiness_service_factory=lambda: events.append(
                        "readiness-factory"
                    )
                ),
                CognitiveSkipCommand(
                    "board-b",
                    "card-1",
                    "not_actionable",
                    "not actionable yet",
                    [],
                    None,
                    None,
                ),
            ),
        ),
        (
            "clear-cognitive-skip",
            lambda events: (
                ClearCognitiveSkipUseCase(
                    readiness_service_factory=lambda: events.append(
                        "readiness-factory"
                    )
                ),
                CognitiveClearCommand("board-b", "card-1", None),
            ),
        ),
        (
            "cognitive-readiness-metrics",
            lambda events: (
                GetCognitiveReadinessMetricsUseCase(
                    readiness_service_factory=lambda: events.append(
                        "readiness-factory"
                    )
                ),
                CognitiveReadinessMetricsCommand("board-b", None),
            ),
        ),
        (
            "cognitive-effectiveness-inventory",
            lambda _events: (
                GetCognitiveEffectivenessInventoryUseCase(),
                CognitiveEffectivenessInventoryCommand(
                    "board-b", None, False, "canonical", None
                ),
            ),
        ),
        (
            "canonical-debt-list",
            lambda _events: (
                ListCanonicalDebtUseCase(),
                CanonicalDebtListCommand("board-b", None, None, 20, 0),
            ),
        ),
        (
            "canonical-debt-retry",
            lambda _events: (
                RetryCanonicalDebtUseCase(),
                CanonicalDebtRetryCommand("board-b", "debt-1", None),
            ),
        ),
        (
            "canonical-partition-list",
            lambda _events: (
                ListCanonicalPartitionIntegrityUseCase(),
                CanonicalPartitionListCommand(
                    "board-b", None, None, None, None, None, 20, 0
                ),
            ),
        ),
        (
            "canonical-partition-detail",
            lambda _events: (
                GetCanonicalPartitionIntegrityDetailUseCase(),
                CanonicalPartitionDetailCommand("board-b", "node-1"),
            ),
        ),
        (
            "digest-layer-mismatch",
            lambda _events: (
                ListDigestLayerMismatchUseCase(),
                DigestLayerMismatchListCommand("board-b", 20, 0),
            ),
        ),
        (
            "orphan-integrity-report",
            lambda events: (
                GetOrphanIntegrityReportUseCase(
                    scanner_factory=lambda: events.append("scanner-factory")
                ),
                OrphanIntegrityReportCommand("board-b", None, 20),
            ),
        ),
        (
            "orphan-backfill",
            lambda events: (
                RunOrphanBackfillUseCase(
                    health_reader=lambda *args, **kwargs: events.append(
                        "health-reader"
                    ),
                    reconciler_factory=lambda: events.append(
                        "reconciler-factory"
                    ),
                ),
                OrphanBackfillCommand(
                    "board-b", None, True, None, 20, None
                ),
            ),
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case_name", "case_builder"),
    _operational_case_builders(),
    ids=[name for name, _builder in _operational_case_builders()],
)
@pytest.mark.parametrize("board", [None, FOREIGN_BOARD], ids=["missing", "foreign"])
async def test_operational_board_surfaces_fail_before_downstream_access(
    case_name,
    case_builder,
    board,
) -> None:
    del case_name
    uow = _Uow(board=board)
    use_case, command = case_builder(uow.events)

    with pytest.raises(BoardNotFoundError):
        await use_case.execute(command, actor=ACTOR, uow=uow)

    expected = ["board:board-b"]
    if board is FOREIGN_BOARD:
        expected.append("share:board-b:user-a")
    assert uow.events == expected


@pytest.mark.asyncio
async def test_operational_owner_reaches_cognitive_skip_writer() -> None:
    uow = _Uow(board=SimpleNamespace(id="board-b", owner_id="user-a"))

    result = await RecordCognitiveSkipUseCase(
        readiness_service_factory=lambda: uow.events.append("readiness-factory")
    ).execute(
        CognitiveSkipCommand(
            "board-b",
            "card-1",
            "not_actionable",
            "not actionable yet",
            [],
            None,
            None,
        ),
        actor=KG_COGNITIVE_SKIPPER,
        uow=uow,
    )

    assert result.data["item"] == {"id": "skip-1"}
    assert uow.events == [
        "board:board-b",
        "readiness-factory",
        "record-skip",
        "evaluate-readiness",
        "read-enforcement",
    ]


@pytest.mark.asyncio
async def test_runtime_settings_viewer_is_denied_before_writer() -> None:
    uow = _Uow(board=None)
    viewer = ActorContext("user-a", "rest", roles=("viewer",), permissions={})

    with pytest.raises(PermissionDeniedError):
        await PutRuntimeSettingsUseCase().execute(
            PutRuntimeSettingsCommand({"kg_queue_max_attempts": 5}, None, None, None),
            actor=viewer,
            uow=uow,
        )

    assert "put-runtime" not in uow.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    [
        ActorContext("admin-a", "rest", roles=("admin",)),
        ActorContext("operator-a", "rest", roles=("operator",)),
        ActorContext(
            "capability-a",
            "rest",
            permissions={
                "runtime": {"settings": {"write": True}},
                "kg": {"admin": {"settings_write": True}},
            },
        ),
    ],
    ids=["admin", "operator", "capability"],
)
async def test_runtime_settings_authorized_actor_reaches_writer(actor) -> None:
    uow = _Uow(board=None)

    result = await PutRuntimeSettingsUseCase().execute(
        PutRuntimeSettingsCommand({"kg_queue_max_attempts": 5}, None, None, None),
        actor=actor,
        uow=uow,
    )

    assert result.data["actor_id"] == actor.actor_id
    assert uow.events == ["put-runtime"]


@pytest.mark.asyncio
@pytest.mark.parametrize("board", [None, FOREIGN_BOARD], ids=["missing", "foreign"])
async def test_queue_drilldown_board_scope_fails_before_reader(board) -> None:
    uow = _Uow(board=board)

    with pytest.raises(QueueBoardNotFoundError):
        await GetQueueDrilldownUseCase().execute(
            GetQueueDrilldownCommand("board-b"),
            actor=ACTOR,
            uow=uow,
        )

    expected = ["board:board-b"]
    if board is FOREIGN_BOARD:
        expected.append("share:board-b:user-a")
    assert uow.events == expected


@pytest.mark.asyncio
async def test_queue_drilldown_owner_reaches_reader() -> None:
    uow = _Uow(board=SimpleNamespace(id="board-b", owner_id="user-a"))

    result = await GetQueueDrilldownUseCase().execute(
        GetQueueDrilldownCommand("board-b"),
        actor=KG_QUEUE_READER,
        uow=uow,
    )

    assert result.data == {"board_id": "board-b"}
    assert uow.events == ["board:board-b", "queue-drilldown:board-b"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command"),
    [
        (GetQueueHealthUseCase(), GetQueueHealthCommand()),
        (GetQueueDrilldownUseCase(), GetQueueDrilldownCommand()),
    ],
    ids=["health", "drilldown"],
)
async def test_global_queue_viewer_is_denied_before_reader(use_case, command) -> None:
    uow = _Uow(board=None)
    viewer = ActorContext("user-a", "rest", roles=("viewer",), permissions={})

    with pytest.raises(PermissionDeniedError):
        await use_case.execute(command, actor=viewer, uow=uow)

    assert uow.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    [
        ActorContext("admin-a", "rest", roles=("admin",)),
        ActorContext("operator-a", "rest", roles=("operator",)),
        ActorContext(
            "capability-a",
            "rest",
            permissions={
                "kg": {
                    "operations": {"queue": {"read": True}},
                    "admin": {"settings_read": True},
                }
            },
        ),
    ],
    ids=["admin", "operator", "capability"],
)
async def test_global_queue_authorized_actor_reaches_readers(actor) -> None:
    uow = _Uow(board=None)

    health = await GetQueueHealthUseCase().execute(
        GetQueueHealthCommand(),
        actor=actor,
        uow=uow,
    )
    drilldown = await GetQueueDrilldownUseCase().execute(
        GetQueueDrilldownCommand(),
        actor=actor,
        uow=uow,
    )

    assert health.data == {"queue_depth": 0}
    assert drilldown.data == {"board_id": None}
    assert uow.events == ["queue-health", "queue-drilldown:None"]


@pytest.mark.asyncio
@pytest.mark.parametrize("board", [None, FOREIGN_BOARD], ids=["missing", "foreign"])
async def test_dead_letter_board_scope_fails_before_reader(board) -> None:
    uow = _Uow(board=board)

    with pytest.raises(DeadLetterBoardNotFoundError):
        await ListDeadLetterRowsUseCase().execute(
            ListDeadLetterRowsCommand("board-b", limit=20, offset=0),
            actor=ACTOR,
            uow=uow,
        )

    expected = ["board:board-b"]
    if board is FOREIGN_BOARD:
        expected.append("share:board-b:user-a")
    assert uow.events == expected


@pytest.mark.asyncio
async def test_dead_letter_owner_reaches_reader() -> None:
    uow = _Uow(board=SimpleNamespace(id="board-b", owner_id="user-a"))

    result = await ListDeadLetterRowsUseCase().execute(
        ListDeadLetterRowsCommand("board-b", limit=20, offset=0),
        actor=KG_QUEUE_READER,
        uow=uow,
    )

    assert result.data == {"rows": [], "total": 0, "limit": 20, "offset": 0}
    assert uow.events == ["board:board-b", "dead-letter:board-b"]
