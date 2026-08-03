"""Authorization preflights for the Admin/Catalog application family."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.admin_catalog import (
    AssociateAmendmentRevisionCommand,
    AssociateAmendmentRevisionUseCase,
    CreateAmendmentRevisionCommand,
    CreateAmendmentRevisionUseCase,
    CreateScreenMockupCommand,
    CreateScreenMockupUseCase,
    DefaultBoardConfigCommand,
    GetBoardDefaultConfigDiffUseCase,
    ScreenMockupUseCaseError,
    UpdateScreenMockupCommand,
    UpdateScreenMockupUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.import_export import (
    ImportDesignSystemsCommand,
    ImportDesignSystemsUseCase,
)
from okto_pulse.core.services.amendment_revision_api import (
    AmendmentRevisionApiError,
)
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
)


def _uow(*, boards: dict[str, object], services: object) -> SimpleNamespace:
    return SimpleNamespace(
        boards=SimpleNamespace(get=AsyncMock(side_effect=boards.get)),
        services=services,
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )


def _shares(permission: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(get_user_permission=AsyncMock(return_value=permission))


@pytest.mark.asyncio
async def test_amendment_foreign_and_missing_bug_are_indistinguishable_and_write_nothing():
    board = SimpleNamespace(id="board-b", owner_id="victim", realm_id="local")
    card_service = SimpleNamespace(
        get_card=AsyncMock(
            side_effect=lambda card_id: (
                SimpleNamespace(id=card_id, board_id="board-b")
                if card_id == "foreign-bug"
                else None
            )
        )
    )
    amendments = SimpleNamespace(create=AsyncMock())
    uow = _uow(
        boards={"board-b": board},
        services=SimpleNamespace(
            cards=card_service,
            shares=_shares(),
            amendments=amendments,
        ),
    )
    actor = ActorContext(
        "attacker",
        "rest",
        board_id="board-b",
        realm_id="local",
    )

    errors = []
    for bug_id in ("foreign-bug", "missing-bug"):
        with pytest.raises(AmendmentRevisionApiError) as exc:
            await CreateAmendmentRevisionUseCase().execute(
                CreateAmendmentRevisionCommand("board-b", bug_id, {}),
                actor=actor,
                uow=uow,
            )
        errors.append(exc.value.to_dict())

    assert [error["code"] for error in errors] == ["bug_not_found", "bug_not_found"]
    assert [error["status_code"] for error in errors] == [404, 404]
    amendments.create.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_amendment_missing_revision_is_checked_before_associate_writer():
    board = SimpleNamespace(id="board-a", owner_id="owner", realm_id="local")
    amendments = SimpleNamespace(
        get=AsyncMock(
            side_effect=AmendmentRevisionApiError(
                "amendment_not_found",
                "missing",
                404,
            )
        ),
        associate=AsyncMock(),
    )
    uow = _uow(
        boards={"board-a": board},
        services=SimpleNamespace(
            cards=SimpleNamespace(
                get_card=AsyncMock(
                    return_value=SimpleNamespace(id="bug-a", board_id="board-a")
                )
            ),
            shares=_shares(),
            amendments=amendments,
        ),
    )

    with pytest.raises(AmendmentRevisionApiError) as exc:
        await AssociateAmendmentRevisionUseCase().execute(
            AssociateAmendmentRevisionCommand(
                "board-a",
                "bug-a",
                "missing-amendment",
                {},
            ),
            actor=ActorContext(
                "owner",
                "rest",
                board_id="board-a",
                realm_id="local",
            ),
            uow=uow,
        )
    assert exc.value.code == "amendment_not_found"
    assert exc.value.status_code == 404
    amendments.associate.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_screen_mockup_foreign_parent_stops_before_gate_and_entity_writer():
    entity = SimpleNamespace(
        id="spec-b",
        board_id="board-b",
        screen_mockups=[{"id": "screen-b", "title": "secret"}],
    )
    specs = SimpleNamespace(
        get_spec=AsyncMock(return_value=entity),
        update_spec=AsyncMock(),
    )
    unused = SimpleNamespace()
    gate = SimpleNamespace(
        evaluate_screen=AsyncMock(),
        gate_delta=AsyncMock(),
    )
    services = SimpleNamespace(
        specs=specs,
        ideations=unused,
        refinements=unused,
        cards=unused,
        stories=unused,
        shares=_shares(),
        mockup_design_gate=gate,
    )
    uow = _uow(
        boards={
            "board-b": SimpleNamespace(
                id="board-b",
                owner_id="victim",
                realm_id="local",
            )
        },
        services=services,
    )
    actor = ActorContext("attacker", "rest", realm_id="local")
    create_data = SimpleNamespace(
        title="must not persist",
        description=None,
        screen_type="page",
        html_content="<p>secret</p>",
        design_system_ref=None,
        design_system_version=None,
        design_system_evidence=None,
    )
    update_data = SimpleNamespace(
        title="must not persist",
        description=None,
        screen_type=None,
        html_content=None,
        design_system_ref=None,
        design_system_version=None,
        design_system_evidence=None,
    )

    with pytest.raises(ScreenMockupUseCaseError) as create_exc:
        await CreateScreenMockupUseCase().execute(
            CreateScreenMockupCommand("spec", "spec-b", create_data),
            actor=actor,
            uow=uow,
        )
    with pytest.raises(ScreenMockupUseCaseError) as update_exc:
        await UpdateScreenMockupUseCase().execute(
            UpdateScreenMockupCommand("spec", "spec-b", "screen-b", update_data),
            actor=actor,
            uow=uow,
        )

    assert create_exc.value.status_code == update_exc.value.status_code == 404
    assert create_exc.value.detail["error"] == update_exc.value.detail["error"] == "not_found"
    gate.evaluate_screen.assert_not_awaited()
    gate.gate_delta.assert_not_awaited()
    specs.update_spec.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_design_system_import_normalizes_inline_source_without_board_access():
    catalog = SimpleNamespace(
        list_catalog=AsyncMock(return_value=[]),
        create_design_system=AsyncMock(),
    )
    uow = _uow(
        boards={
            "foreign-board": SimpleNamespace(
                id="foreign-board",
                owner_id="victim",
                realm_id="local",
            )
        },
        services=SimpleNamespace(
            shares=_shares(),
            design_systems=catalog,
        ),
    )

    result = await ImportDesignSystemsUseCase().execute(
        ImportDesignSystemsCommand(
            items=[
                {"title": "valid global", "scope": "global"},
                {
                    "title": "foreign inline",
                    "scope": "inline",
                    "board_id": "foreign-board",
                },
            ]
        ),
        actor=ActorContext("attacker", "rest", realm_id="local"),
        uow=uow,
    )

    assert result.created == 2
    uow.boards.get.assert_not_awaited()
    catalog.list_catalog.assert_not_awaited()
    assert catalog.create_design_system.await_count == 2
    assert all(
        call.kwargs["scope"] == "global" and call.kwargs["board_id"] is None
        for call in catalog.create_design_system.await_args_list
    )
    uow.commit.assert_awaited_once()
    uow.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_config_diff_rejects_foreign_board_before_diff_service():
    default_config = SimpleNamespace(get_board_diff=AsyncMock())
    uow = _uow(
        boards={
            "foreign-board": SimpleNamespace(
                id="foreign-board",
                owner_id="victim",
                realm_id="local",
            )
        },
        services=SimpleNamespace(
            shares=_shares(),
            default_board_config=default_config,
        ),
    )

    with pytest.raises(DefaultBoardConfigurationError) as exc:
        await GetBoardDefaultConfigDiffUseCase().execute(
            DefaultBoardConfigCommand(board_id="foreign-board"),
            actor=ActorContext("attacker", "rest", realm_id="local"),
            uow=uow,
        )
    assert exc.value.code == "board_not_found"
    assert exc.value.status_code == 404
    default_config.get_board_diff.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_config_diff_reuses_authenticated_request_uow():
    board = SimpleNamespace(
        id="owned-board",
        owner_id="owner",
        realm_id="local",
    )
    default_config = SimpleNamespace(
        get_board_diff=AsyncMock(return_value={"fields": []})
    )
    uow = _uow(
        boards={"owned-board": board},
        services=SimpleNamespace(
            shares=_shares(),
            default_board_config=default_config,
        ),
    )

    result = await GetBoardDefaultConfigDiffUseCase().execute(
        DefaultBoardConfigCommand(board_id="owned-board"),
        actor=ActorContext("owner", "rest", realm_id="local"),
        uow=uow,
    )

    assert result.data == {"fields": []}
    default_config.get_board_diff.assert_awaited_once_with(
        board_id="owned-board",
        uow=uow,
    )

