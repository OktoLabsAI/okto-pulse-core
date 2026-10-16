"""Current choice selection, cardinality and typed error behavior.

Native collection validation and retired input rejection are covered in
test_mcp_native_collections.py.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases import mcp_collaboration
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpRespondToChoiceCommand, McpRespondToChoiceUseCase,
)
from okto_pulse.core.services.qa_selection import QASelectionError, validate_choice_selection

def test_single_choice_rejects_multiple_options_without_truncating() -> None:
    choices = [{"id": "opt_0"}, {"id": "opt_1"}]

    with pytest.raises(QASelectionError) as caught:
        validate_choice_selection(
            "single_choice",
            ["opt_0", "opt_1"],
            choices,
        )

    assert caught.value.to_error_dict() == {
        "error": "single_choice_multiple_selection",
        "code": "single_choice_multiple_selection",
        "message": "A single-choice question accepts exactly one selected option.",
        "details": {
            "selected": ["opt_0", "opt_1"],
            "allowed_option_ids": ["opt_0", "opt_1"],
        },
        "mutation_applied": False,
    }


def test_multi_choice_preserves_every_valid_selection() -> None:
    assert validate_choice_selection(
        "multi_choice",
        ["opt_0", "opt_2"],
        [{"id": "opt_0"}, {"id": "opt_1"}, {"id": "opt_2"}],
    ) == ["opt_0", "opt_2"]


def test_choice_membership_failure_reports_all_requested_and_allowed_ids() -> None:
    with pytest.raises(QASelectionError) as caught:
        validate_choice_selection(
            "choice",
            ["opt_0", "opt_missing"],
            [{"id": "opt_0"}, {"id": "opt_1"}],
        )

    assert caught.value.code == "invalid_qa_selection"
    assert caught.value.selected == ("opt_0", "opt_missing")
    assert caught.value.allowed == ("opt_0", "opt_1")


@pytest.mark.asyncio
async def test_choice_comment_membership_error_is_returned_as_typed_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selection_error = QASelectionError(
        "invalid_qa_selection",
        "Unknown choice option id(s): ['opt_missing'].",
        selected=["opt_missing"],
        allowed=["opt_0"],
    )
    comments = SimpleNamespace(
        respond_to_choice=AsyncMock(side_effect=selection_error),
    )
    uow = SimpleNamespace(services=SimpleNamespace(comments=comments))
    monkeypatch.setattr(
        mcp_collaboration,
        "_get_comment_with_card_in_scope",
        AsyncMock(
            return_value=(
                SimpleNamespace(id="comment-1"),
                SimpleNamespace(id="card-1", board_id="board-1"),
            )
        ),
    )

    result = await McpRespondToChoiceUseCase().execute(
        McpRespondToChoiceCommand(
            board_id="board-1",
            comment_id="comment-1",
            selected_ids=["opt_missing"],
            free_text="",
        ),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Agent",
            board_id="board-1",
        ),
        uow=uow,
    )

    assert result.payload == selection_error.to_error_dict()
