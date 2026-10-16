"""C3/TS4: guideline-policy persistence failures never escape the MCP boundary.

``GuidelinePolicySubjectConflict`` (and its sibling conflict classes) used to
propagate out of tools such as ``okto_pulse_answer_ideation_question`` as a raw
exception, which a protocol host reports as a transport fault instead of a
domain outcome.  The single registration choke point in
``mcp/server.py::_xml_safety_log_decorator`` now projects every
``GuidelinePolicyPersistenceError`` through the canonical mapper.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.outcome import McpToolOutcome
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicySubjectConflict,
    GuidelinePolicyVersionConflict,
)

BOARD_ID = "board-conflict-envelope"
USER_ID = "agent-conflict-envelope"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "mcp-conflict-test",
            "permissions": ["*"],
        },
    )()


class _FakeUnitOfWork:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _fake_uow_factory():
    def factory(*_args, **_kwargs):
        return _FakeUnitOfWork()

    return factory


@pytest.fixture(autouse=True)
def _boundary():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(
        mcp_server, "check_permission", return_value=None
    ), patch.object(
        mcp_server, "_mcp_check_permission", return_value=None
    ), patch.object(
        mcp_server, "get_unit_of_work_factory_for_mcp", _fake_uow_factory
    ):
        yield


async def _envelope(tool_name: str, use_case: str, error: Exception, **kwargs) -> dict:
    from okto_pulse.core.application import use_cases as use_cases_module

    tool = await mcp_server.mcp.get_tool(tool_name)
    with patch.object(
        getattr(use_cases_module, use_case),
        "execute",
        AsyncMock(side_effect=error),
    ):
        result = await tool.fn(**kwargs)
    assert isinstance(result, McpToolOutcome), (
        f"{tool_name} must return an outcome, not raise"
    )
    return result.structured_content(tool_name=tool_name)


def _assert_conflict(envelope: dict, reason: str) -> None:
    assert envelope["meta"]["contract"] == "okto-pulse.mcp-tool-outcome"
    assert envelope["outcome"] == "error"
    assert envelope["error_code"] == "conflict"
    assert envelope["retryable"] is True
    assert envelope["next_action"] == "refresh_and_retry"
    assert reason in envelope["message"]
    assert envelope["details"]["reason_code"] == reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,reason",
    [
        (
            GuidelinePolicySubjectConflict("semantic_subject_mutation_conflict"),
            "semantic_subject_mutation_conflict",
        ),
        (
            GuidelinePolicyVersionConflict("subject_version_conflict"),
            "subject_version_conflict",
        ),
    ],
)
async def test_answer_ideation_question_projects_guideline_policy_conflict(
    error, reason
):
    envelope = await _envelope(
        "okto_pulse_answer_ideation_question",
        "McpAnswerIdeationQuestionUseCase",
        error,
        board_id=BOARD_ID,
        ideation_id="ide_1",
        qa_id="qa_1",
        answer="an answer",
    )
    _assert_conflict(envelope, reason)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,reason",
    [
        (
            GuidelinePolicySubjectConflict("semantic_subject_mutation_conflict"),
            "semantic_subject_mutation_conflict",
        ),
        (
            GuidelinePolicyVersionConflict("subject_version_conflict"),
            "subject_version_conflict",
        ),
    ],
)
async def test_answer_refinement_question_projects_guideline_policy_conflict(
    error, reason
):
    envelope = await _envelope(
        "okto_pulse_answer_refinement_question",
        "McpAnswerRefinementQuestionUseCase",
        error,
        board_id=BOARD_ID,
        refinement_id="ref_1",
        qa_id="qa_1",
        answer="an answer",
    )
    _assert_conflict(envelope, reason)
