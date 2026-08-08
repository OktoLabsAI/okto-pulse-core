"""Regression coverage for mutable MCP actions migrated to exact Core policy."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.mcp_ideation_crud import (
    McpAddIdeationKnowledgeCommand,
    McpAddIdeationKnowledgeUseCase,
    McpDeleteIdeationKnowledgeCommand,
    McpDeleteIdeationKnowledgeUseCase,
    McpDeleteIdeationQuestionCommand,
    McpDeleteIdeationQuestionUseCase,
)
from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
    McpAddScreenMockupUseCase,
    McpAnnotateMockupUseCase,
    McpDeleteScreenMockupUseCase,
    McpListScreenMockupsUseCase,
    McpScreenMockupCommand,
    McpUpdateScreenMockupUseCase,
)
from okto_pulse.core.application.use_cases.mcp_refinement_crud import (
    McpDeleteRefinementQuestionCommand,
    McpDeleteRefinementQuestionUseCase,
)
from okto_pulse.core.application.use_cases.mcp_spec_crud import (
    McpDeleteTestScenarioCommand,
    McpDeleteTestScenarioUseCase,
    McpUpdateTestScenarioCommand,
    McpUpdateTestScenarioUseCase,
)
from okto_pulse.core.application.use_cases.mcp_sprint_crud import (
    McpDeleteSprintQuestionCommand,
    McpDeleteSprintQuestionUseCase,
)
from okto_pulse.core.application.use_cases.spec_crud import (
    DeleteSpecQuestionCommand,
    DeleteSpecQuestionUseCase,
    ExecuteTestScenarioEvidenceCommand,
    ExecuteTestScenarioEvidenceUseCase,
)
from okto_pulse.core.domain.permissions import PermissionSet


BOARD_ID = "board-exact-mcp-gap"


def _permission_set(values: dict[str, bool]) -> PermissionSet:
    document: dict[str, Any] = {}
    for path, value in values.items():
        cursor = document
        parts = path.split(".")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = value
    return PermissionSet(document)


def _update_scenario_command() -> McpUpdateTestScenarioCommand:
    return McpUpdateTestScenarioCommand(
        "spec-1",
        "scenario-1",
        title="updated",
        given="",
        when="",
        then="",
        scenario_type=None,
        linked_criteria_tokens=None,
        notes="",
        clear_fields=None,
    )


_CASES = (
    (
        "ideation.knowledge.create",
        "ideation.entity.edit_fields",
        McpAddIdeationKnowledgeUseCase(),
        McpAddIdeationKnowledgeCommand("ideation-1", BOARD_ID, object()),
    ),
    (
        "ideation.knowledge.delete",
        "ideation.entity.edit_fields",
        McpDeleteIdeationKnowledgeUseCase(),
        McpDeleteIdeationKnowledgeCommand("ideation-1", BOARD_ID, "knowledge-1"),
    ),
    (
        "ideation.qa.delete",
        "ideation.qa.answer",
        McpDeleteIdeationQuestionUseCase(),
        McpDeleteIdeationQuestionCommand(BOARD_ID, "ideation-1", "qa-1"),
    ),
    (
        "refinement.qa.delete",
        "refinement.qa.answer",
        McpDeleteRefinementQuestionUseCase(),
        McpDeleteRefinementQuestionCommand(BOARD_ID, "refinement-1", "qa-1"),
    ),
    (
        "sprint.qa.delete",
        "sprint.qa.answer",
        McpDeleteSprintQuestionUseCase(),
        McpDeleteSprintQuestionCommand(BOARD_ID, "sprint-1", "qa-1"),
    ),
    (
        "spec.tests.execute",
        "spec.tests.update_status",
        ExecuteTestScenarioEvidenceUseCase(),
        ExecuteTestScenarioEvidenceCommand("spec-1", "scenario-1", "passed"),
    ),
    (
        "spec.tests.edit",
        "spec.tests.create",
        McpUpdateTestScenarioUseCase(),
        _update_scenario_command(),
    ),
    (
        "spec.tests.delete",
        "spec.tests.create",
        McpDeleteTestScenarioUseCase(),
        McpDeleteTestScenarioCommand("spec-1", "scenario-1"),
    ),
    (
        "spec.qa.delete",
        "spec.qa.answer",
        DeleteSpecQuestionUseCase(),
        DeleteSpecQuestionCommand("qa-1", spec_id="spec-1"),
    ),
    (
        "story.mockups.create",
        "story.entity.edit_fields",
        McpAddScreenMockupUseCase(),
        McpScreenMockupCommand(BOARD_ID, "story-1", "story", title="screen"),
    ),
    (
        "story.mockups.edit",
        "story.entity.edit_fields",
        McpUpdateScreenMockupUseCase(),
        McpScreenMockupCommand(BOARD_ID, "story-1", "story", screen_id="screen-1"),
    ),
    (
        "story.mockups.annotate",
        "story.entity.edit_fields",
        McpAnnotateMockupUseCase(),
        McpScreenMockupCommand(BOARD_ID, "story-1", "story", screen_id="screen-1"),
    ),
    (
        "story.mockups.delete",
        "story.entity.edit_fields",
        McpDeleteScreenMockupUseCase(),
        McpScreenMockupCommand(BOARD_ID, "story-1", "story", screen_id="screen-1"),
    ),
    (
        "story.mockups.read",
        "story.entity.read",
        McpListScreenMockupsUseCase(),
        McpScreenMockupCommand(BOARD_ID, "story-1", "story"),
    ),
)


class _SpecReader:
    async def get_spec(self, spec_id: str) -> Any:
        return SimpleNamespace(id=spec_id, board_id=BOARD_ID)


class _SpecQAReader:
    async def get_question(self, qa_id: str) -> Any:
        return SimpleNamespace(id=qa_id, spec_id="spec-1")


def _authorization_boundary(operation: str) -> Any:
    if operation not in {"spec.tests.execute", "spec.qa.delete"}:
        return SimpleNamespace()
    return SimpleNamespace(
        services=SimpleNamespace(
            specs=_SpecReader(),
            spec_qa=_SpecQAReader(),
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "historical", "use_case", "command"),
    _CASES,
    ids=[case[0] for case in _CASES],
)
async def test_exact_false_denies_before_mutation_boundary(
    operation: str,
    historical: str,
    use_case: Any,
    command: Any,
) -> None:
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=_permission_set({operation: False, historical: True}),
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        await use_case.execute(
            command,
            actor=actor,
            uow=_authorization_boundary(operation),
        )

    denial = json.loads(str(exc_info.value))
    assert denial["required_permission"] == operation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "historical", "use_case", "command"),
    _CASES,
    ids=[case[0] for case in _CASES],
)
async def test_exact_true_reaches_the_business_boundary(
    operation: str,
    historical: str,
    use_case: Any,
    command: Any,
) -> None:
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=_permission_set({operation: True, historical: True}),
    )

    with pytest.raises(AttributeError):
        await use_case.execute(command, actor=actor, uow=SimpleNamespace())


_LEGACY_CASES = (
    (McpAddIdeationKnowledgeUseCase(), _CASES[0][3], "specs:update"),
    (McpDeleteIdeationKnowledgeUseCase(), _CASES[1][3], "specs:update"),
    (McpDeleteIdeationQuestionUseCase(), _CASES[2][3], "qa:delete"),
    (McpDeleteRefinementQuestionUseCase(), _CASES[3][3], "qa:delete"),
    (McpDeleteSprintQuestionUseCase(), _CASES[4][3], "qa:delete"),
    (ExecuteTestScenarioEvidenceUseCase(), _CASES[5][3], "specs:update"),
    (McpUpdateTestScenarioUseCase(), _update_scenario_command(), "specs:update"),
    (McpDeleteTestScenarioUseCase(), _CASES[7][3], "specs:update"),
    (DeleteSpecQuestionUseCase(), _CASES[8][3], "qa:delete"),
    (McpAddScreenMockupUseCase(), _CASES[9][3], "specs:update"),
    (McpUpdateScreenMockupUseCase(), _CASES[10][3], "specs:update"),
    (McpAnnotateMockupUseCase(), _CASES[11][3], "specs:update"),
    (McpDeleteScreenMockupUseCase(), _CASES[12][3], "specs:update"),
    (McpListScreenMockupsUseCase(), _CASES[13][3], "board:read"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "legacy_token"),
    _LEGACY_CASES,
    ids=[f"{type(case[0]).__name__}-{case[2].replace(':', '-')}" for case in _LEGACY_CASES],
)
async def test_flat_legacy_token_still_reaches_the_business_boundary(
    use_case: Any,
    command: Any,
    legacy_token: str,
) -> None:
    actor = ActorContext(
        "legacy-agent",
        "mcp",
        board_id=BOARD_ID,
        permissions=[legacy_token],
    )

    with pytest.raises(AttributeError):
        await use_case.execute(command, actor=actor, uow=SimpleNamespace())


def test_migrated_mcp_handlers_have_no_coarse_adapter_precheck() -> None:
    source_path = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "server.py"
    )
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    target_names = {
        "okto_pulse_add_ideation_knowledge",
        "okto_pulse_delete_ideation_knowledge",
        "okto_pulse_execute_test_scenario_evidence",
        "okto_pulse_update_test_scenario",
        "okto_pulse_delete_test_scenario",
        "okto_pulse_delete_spec_question",
        "okto_pulse_delete_ideation_question",
        "okto_pulse_delete_refinement_question",
        "okto_pulse_delete_sprint_question",
        "okto_pulse_add_screen_mockup",
        "okto_pulse_update_screen_mockup",
        "okto_pulse_annotate_mockup",
        "okto_pulse_delete_screen_mockup",
    }
    functions = {
        node.name: node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in target_names
    }

    assert set(functions) == target_names
    for name, function in functions.items():
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "check_permission"
            for node in ast.walk(function)
        ), name

    spec_delete = functions["okto_pulse_delete_spec_question"]
    assert any(
        isinstance(node, ast.Constant) and node.value == "spec.qa.delete"
        for node in ast.walk(spec_delete)
    )
