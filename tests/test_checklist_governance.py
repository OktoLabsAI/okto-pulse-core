"""Governance boundary regressions for the curated Spec checklist."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.checklist import (
    UpdateChecklistBindingCommand,
    UpdateChecklistBindingUseCase,
)
from okto_pulse.core.application.use_cases.create_board import CreateBoardUseCase
from okto_pulse.core.domain.checklist import (
    SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    ChecklistContractError,
    ChecklistMode,
)


@pytest.mark.parametrize(
    "snapshot",
    [
        None,
        {},
        {"template_id": "legacy-template"},
    ],
)
def test_board_bootstrap_uses_advisory_for_absent_or_legacy_checklist_snapshot(
    snapshot: object,
) -> None:
    board = SimpleNamespace(default_config_snapshot=snapshot)

    assert (
        CreateBoardUseCase._checklist_mode_from_snapshot(board)
        is ChecklistMode.ADVISORY
    )


def test_board_bootstrap_uses_exact_mode_and_template_from_snapshot() -> None:
    board = SimpleNamespace(
        default_config_snapshot={
            "spec_checklist": {
                "mode": "blocking",
                "template_version_id": SPECIFY_CHECKLIST_TEMPLATE_VERSION,
            }
        }
    )

    assert (
        CreateBoardUseCase._checklist_mode_from_snapshot(board)
        is ChecklistMode.BLOCKING
    )


@pytest.mark.parametrize(
    "snapshot",
    [
        "corrupt",
        {"spec_checklist": "corrupt"},
        {
            "spec_checklist": {
                "mode": "unsupported",
                "template_version_id": SPECIFY_CHECKLIST_TEMPLATE_VERSION,
            }
        },
        {
            "spec_checklist": {
                "mode": "blocking",
                "template_version_id": "/specify/v2",
            }
        },
    ],
)
def test_board_bootstrap_rejects_present_invalid_checklist_snapshot(
    snapshot: object,
) -> None:
    board = SimpleNamespace(default_config_snapshot=snapshot)

    with pytest.raises(ChecklistContractError) as exc:
        CreateBoardUseCase._checklist_mode_from_snapshot(board)

    assert exc.value.code == "invalid_default_checklist_snapshot"


@pytest.mark.asyncio
async def test_human_editor_can_update_binding_without_board_ownership() -> None:
    board = SimpleNamespace(
        id="board-1",
        owner_id="owner-1",
        realm_id=None,
    )
    board_repository = SimpleNamespace(get=AsyncMock(return_value=board))
    shares = SimpleNamespace(
        get_share_permission=AsyncMock(return_value="editor"),
    )
    checklists = SimpleNamespace(
        get_binding=AsyncMock(return_value=None),
        apply_binding_cas=AsyncMock(side_effect=lambda binding, **_: binding),
    )
    board_service = SimpleNamespace(
        record_checklist_binding_change=AsyncMock(),
    )
    uow = SimpleNamespace(
        boards=board_repository,
        services=SimpleNamespace(
            boards=board_service,
            shares=shares,
            checklists=checklists,
        ),
        commit=AsyncMock(),
    )

    binding = await UpdateChecklistBindingUseCase().execute(
        UpdateChecklistBindingCommand(
            board_id="board-1",
            mode=ChecklistMode.ADVISORY,
            expected_revision=0,
        ),
        actor=ActorContext("editor-1", "rest"),
        uow=uow,
    )

    assert binding.mode is ChecklistMode.ADVISORY
    shares.get_share_permission.assert_awaited_once_with(
        "board-1",
        "editor-1",
    )
    board_service.record_checklist_binding_change.assert_awaited_once()
    uow.commit.assert_awaited_once()
