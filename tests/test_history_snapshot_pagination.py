from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.history_pagination import (
    HistoryReadValidationError,
    SNAPSHOT_VERSION_MAX,
    validate_history_window,
    validate_snapshot_version,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.ideations_crud import (
    GetIdeationSnapshotCommand,
    ListIdeationHistoryCommand,
    ListIdeationHistoryResult,
    ListIdeationHistoryUseCase,
)
from okto_pulse.core.application.use_cases.mcp_ideation_crud import (
    McpGetIdeationHistoryCommand,
    McpGetIdeationHistoryResult,
    McpGetIdeationHistoryUseCase,
    McpGetIdeationSnapshotCommand,
)
from okto_pulse.core.application.use_cases.mcp_refinement_crud import (
    McpGetRefinementHistoryCommand,
    McpGetRefinementHistoryUseCase,
    McpGetRefinementSnapshotCommand,
)
from okto_pulse.core.application.use_cases.refinements_crud import (
    GetRefinementSnapshotCommand,
    ListRefinementHistoryCommand,
    ListRefinementHistoryResult,
    ListRefinementHistoryUseCase,
)
from okto_pulse.core.application.use_cases.spec_crud import (
    ListSpecHistoryCommand,
    ListSpecHistoryResult,
    ListSpecHistoryUseCase,
)
from okto_pulse.core.services import main as service_module
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecService,
)

BOARD_ID = "board-history"
ACTOR = ActorContext("agent-history", "mcp", board_id=BOARD_ID)


@pytest.mark.parametrize("value", [1, "1", SNAPSHOT_VERSION_MAX, str(SNAPSHOT_VERSION_MAX)])
def test_snapshot_version_accepts_bounded_integer_inputs(value: object) -> None:
    assert validate_snapshot_version(value) == int(value)


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        0,
        -1,
        SNAPSHOT_VERSION_MAX + 1,
        str(SNAPSHOT_VERSION_MAX + 1),
        "1.0",
        "+1",
        " 1",
        "",
        None,
    ],
)
def test_snapshot_version_rejects_non_integer_bool_and_overflow(value: object) -> None:
    with pytest.raises(HistoryReadValidationError) as caught:
        validate_snapshot_version(value)

    assert caught.value.code == "snapshot_version_invalid"
    assert caught.value.field == "version"
    assert caught.value.to_error_dict()["retryable"] is False


@pytest.mark.parametrize(
    ("limit", "offset", "code"),
    [
        (True, 0, "history_limit_invalid"),
        (0, 0, "history_limit_invalid"),
        (201, 0, "history_limit_invalid"),
        ("1.0", 0, "history_limit_invalid"),
        (1, True, "history_offset_invalid"),
        (1, -1, "history_offset_invalid"),
        (1, SNAPSHOT_VERSION_MAX + 1, "history_offset_invalid"),
        (1, str(SNAPSHOT_VERSION_MAX + 1), "history_offset_invalid"),
        (1, " 0", "history_offset_invalid"),
    ],
)
def test_history_window_rejects_invalid_bool_ranges_and_overflow(
    limit: object,
    offset: object,
    code: str,
) -> None:
    with pytest.raises(HistoryReadValidationError) as caught:
        validate_history_window(limit, offset)

    assert caught.value.code == code


def test_history_commands_normalize_mcp_strings_and_rest_integers() -> None:
    commands = [
        ListIdeationHistoryCommand("ideation", limit=25, offset=5),
        ListRefinementHistoryCommand("refinement", limit=25, offset=5),
        ListSpecHistoryCommand("spec", limit=25, offset=5),
        McpGetIdeationHistoryCommand("ideation", BOARD_ID, "25", "5"),
        McpGetRefinementHistoryCommand("refinement", BOARD_ID, "25", "5"),
    ]

    assert [(command.limit, command.offset) for command in commands] == [
        (25, 5)
    ] * len(commands)


def test_snapshot_commands_share_the_same_validation_contract() -> None:
    commands = [
        GetIdeationSnapshotCommand("ideation", "7"),
        GetRefinementSnapshotCommand("refinement", "7"),
        McpGetIdeationSnapshotCommand("ideation", BOARD_ID, "7"),
        McpGetRefinementSnapshotCommand("refinement", BOARD_ID, "7"),
    ]
    assert [command.version for command in commands] == [7, 7, 7, 7]

    for command_type, args in [
        (GetIdeationSnapshotCommand, ("ideation", True)),
        (GetRefinementSnapshotCommand, ("refinement", 0)),
        (McpGetIdeationSnapshotCommand, ("ideation", BOARD_ID, "invalid")),
        (
            McpGetRefinementSnapshotCommand,
            ("refinement", BOARD_ID, SNAPSHOT_VERSION_MAX + 1),
        ),
    ]:
        with pytest.raises(
            HistoryReadValidationError,
            match="snapshot_version_invalid",
        ):
            command_type(*args)


def test_history_results_keep_legacy_collection_and_expose_exact_metadata() -> None:
    for result in [
        ListIdeationHistoryResult([1, 2], total=7, limit=2, offset=2),
        ListRefinementHistoryResult([1, 2], total=7, limit=2, offset=2),
        ListSpecHistoryResult([1, 2], total=7, limit=2, offset=2),
        McpGetIdeationHistoryResult([1, 2], total=7, limit=2, offset=2),
    ]:
        collection = (
            result.entries
            if hasattr(result, "entries")
            else result.history
        )
        assert collection == [1, 2]
        assert result.total == 7
        assert result.has_more is True
        assert result.next_offset == 4
        assert result.truncated is True

    legacy = ListSpecHistoryResult([1, 2])
    assert legacy.history == [1, 2]
    assert legacy.total == 2
    assert legacy.has_more is False
    assert legacy.next_offset is None
    assert legacy.truncated is False

    last_page = ListSpecHistoryResult([7], total=7, limit=2, offset=6)
    assert last_page.has_more is False
    assert last_page.next_offset is None
    assert last_page.truncated is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_type", "entity", "id_field"),
    [
        (IdeationService, "ideation_history", "ideation_id"),
        (RefinementService, "refinement_history", "refinement_id"),
        (SpecService, "spec_history", "spec_id"),
    ],
)
async def test_history_services_forward_exact_window_and_count(
    monkeypatch: pytest.MonkeyPatch,
    service_type: type,
    entity: str,
    id_field: str,
) -> None:
    list_rows = AsyncMock(return_value=["row"])
    count_rows = AsyncMock(return_value=9)
    monkeypatch.setattr(service_module, "_application_list", list_rows)
    monkeypatch.setattr(service_module, "_application_count", count_rows)
    service = service_type(object())

    assert await service.list_history("parent", limit="20", offset="4") == ["row"]
    assert await service.count_history("parent") == 9

    list_rows.assert_awaited_once()
    list_kwargs = list_rows.await_args.kwargs
    assert list_rows.await_args.args[1] == entity
    assert list_kwargs["limit"] == 20
    assert list_kwargs["offset"] == 4
    assert list_kwargs["order_by"] == (("created_at", True), ("id", True))
    assert list_kwargs["filters"][0].field == id_field
    assert list_kwargs["filters"][0].value == "parent"

    count_rows.assert_awaited_once()
    count_kwargs = count_rows.await_args.kwargs
    assert count_rows.await_args.args[1] == entity
    assert count_kwargs["filters"][0].field == id_field
    assert count_kwargs["filters"][0].value == "parent"


@pytest.mark.asyncio
async def test_history_services_validate_before_touching_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    list_rows = AsyncMock()
    monkeypatch.setattr(service_module, "_application_list", list_rows)

    with pytest.raises(
        HistoryReadValidationError,
        match="history_offset_invalid",
    ):
        await IdeationService(object()).list_history(
            "ideation",
            limit=20,
            offset=SNAPSHOT_VERSION_MAX + 1,
        )

    list_rows.assert_not_awaited()


def _uow(service_name: str, service: object) -> SimpleNamespace:
    return SimpleNamespace(services=SimpleNamespace(**{service_name: service}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "service_name", "parent_method", "collection_field"),
    [
        (
            ListIdeationHistoryUseCase(),
            ListIdeationHistoryCommand("parent", limit=2, offset=2),
            "ideations",
            "get_ideation",
            "history",
        ),
        (
            ListRefinementHistoryUseCase(),
            ListRefinementHistoryCommand("parent", limit=2, offset=2),
            "refinements",
            "get_refinement",
            "history",
        ),
        (
            ListSpecHistoryUseCase(),
            ListSpecHistoryCommand("parent", limit=2, offset=2),
            "specs",
            "get_spec",
            "history",
        ),
        (
            McpGetIdeationHistoryUseCase(),
            McpGetIdeationHistoryCommand("parent", BOARD_ID, 2, 2),
            "ideations",
            "get_ideation",
            "entries",
        ),
        (
            McpGetRefinementHistoryUseCase(),
            McpGetRefinementHistoryCommand("parent", BOARD_ID, 2, 2),
            "refinements",
            "get_refinement",
            "entries",
        ),
    ],
)
async def test_history_use_cases_return_exact_page_metadata(
    use_case: object,
    command: object,
    service_name: str,
    parent_method: str,
    collection_field: str,
) -> None:
    service = SimpleNamespace(
        **{
            parent_method: AsyncMock(
                return_value=SimpleNamespace(id="parent", board_id=BOARD_ID)
            ),
            "list_history": AsyncMock(return_value=["h2", "h3"]),
            "count_history": AsyncMock(return_value=5),
        }
    )

    result = await use_case.execute(
        command,
        actor=ACTOR,
        uow=_uow(service_name, service),
    )

    assert getattr(result, collection_field) == ["h2", "h3"]
    assert result.total == 5
    assert result.has_more is True
    assert result.next_offset == 4
    assert result.truncated is True
    service.list_history.assert_awaited_once_with(
        "parent",
        limit=2,
        offset=2,
    )
    service.count_history.assert_awaited_once_with("parent")
