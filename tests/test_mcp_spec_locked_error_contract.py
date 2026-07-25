"""Regression coverage for content-lock failures at MCP write boundaries."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
    McpAddScreenMockupUseCase,
    McpAnnotateMockupUseCase,
    McpDeleteScreenMockupUseCase,
    McpUpdateScreenMockupUseCase,
)
from okto_pulse.core.application.use_cases.mcp_spec_crud import (
    McpRemoveSpecEntityUseCase,
    McpAddTestScenarioUseCase,
    McpDeleteTestScenarioUseCase,
    McpUpdateSpecUseCase,
    McpUpdateTestScenarioUseCase,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.outcome import coerce_mcp_tool_outcome
from okto_pulse.core.services.main import (
    SpecLineagePreflightError,
    SpecLockedError,
)


class _UowContext:
    def __init__(self) -> None:
        self.exit_exception: type[BaseException] | None = None

    async def __aenter__(self) -> Any:
        return SimpleNamespace()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        del exc, traceback
        self.exit_exception = exc_type


class _Factory:
    def __init__(self, context: _UowContext) -> None:
        self.context = context

    def __call__(self, **kwargs: Any) -> _UowContext:
        del kwargs
        return self.context


async def _agent_ctx(board_id: str) -> Any:
    return SimpleNamespace(
        agent_id="agent-1",
        agent_name="Agent",
        board_id=board_id,
        permissions=None,
        realm_id=None,
    )


def _assert_locked_error(raw: str, *, tool_name: str) -> None:
    payload = json.loads(raw)
    assert payload == {
        "error": "spec_locked",
        "code": "spec_locked",
        "message": (
            "Spec is locked because validation passed. Move the spec back to "
            "draft or approved to edit (validation will be cleared, history "
            "preserved)."
        ),
        "details": {
            "spec_id": "spec-locked",
            "current_validation_id": "validation-1",
            "mutation_applied": False,
        },
        "retryable": True,
    }
    outcome = coerce_mcp_tool_outcome(raw, tool_name=tool_name)
    assert outcome.is_error is True
    assert outcome.code == "spec_locked"
    assert outcome.retryable is True
    assert "Traceback" not in raw
    assert "SpecLockedError" not in raw


@pytest.mark.asyncio
async def test_add_test_scenario_locked_spec_is_structured_and_rolls_back(
    monkeypatch: Any,
) -> None:
    context = _UowContext()

    async def _raise_locked(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise SpecLockedError("spec-locked", "validation-1")

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(context),
    )
    monkeypatch.setattr(McpAddTestScenarioUseCase, "execute", _raise_locked)

    raw = await server.okto_pulse_add_test_scenario.fn(
        board_id="board-1",
        spec_id="spec-locked",
        title="Blocked scenario",
        given="Given a locked spec",
        when="When content is changed",
        then="Then no scenario is appended",
    )

    _assert_locked_error(raw, tool_name="okto_pulse_add_test_scenario")
    # The exception reaches the UoW boundary before the handler maps it, giving
    # the concrete UoW its normal rollback path.
    assert context.exit_exception is SpecLockedError


@pytest.mark.asyncio
async def test_add_screen_mockup_locked_spec_is_structured_and_rolls_back(
    monkeypatch: Any,
) -> None:
    context = _UowContext()

    async def _raise_locked(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise SpecLockedError("spec-locked", "validation-1")

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(context),
    )
    monkeypatch.setattr(McpAddScreenMockupUseCase, "execute", _raise_locked)

    raw = await server.okto_pulse_add_screen_mockup.fn(
        board_id="board-1",
        entity_id="spec-locked",
        entity_type="spec",
        title="Blocked mockup",
        html_content="<main>must not persist</main>",
    )

    _assert_locked_error(raw, tool_name="okto_pulse_add_screen_mockup")
    assert context.exit_exception is SpecLockedError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "use_case", "kwargs"),
    (
        (
            "okto_pulse_update_spec",
            McpUpdateSpecUseCase,
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "title": "Blocked title",
            },
        ),
        (
            "okto_pulse_update_test_scenario",
            McpUpdateTestScenarioUseCase,
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "scenario_id": "scenario-1",
                "title": "Blocked scenario update",
            },
        ),
        (
            "okto_pulse_delete_test_scenario",
            McpDeleteTestScenarioUseCase,
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "scenario_id": "scenario-1",
            },
        ),
        (
            "okto_pulse_update_screen_mockup",
            McpUpdateScreenMockupUseCase,
            {
                "board_id": "board-1",
                "entity_id": "spec-locked",
                "entity_type": "spec",
                "screen_id": "screen-1",
                "title": "Blocked mockup update",
            },
        ),
        (
            "okto_pulse_annotate_mockup",
            McpAnnotateMockupUseCase,
            {
                "board_id": "board-1",
                "entity_id": "spec-locked",
                "entity_type": "spec",
                "screen_id": "screen-1",
                "text": "Blocked annotation",
            },
        ),
        (
            "okto_pulse_delete_screen_mockup",
            McpDeleteScreenMockupUseCase,
            {
                "board_id": "board-1",
                "entity_id": "spec-locked",
                "entity_type": "spec",
                "screen_id": "screen-1",
            },
        ),
    ),
)
async def test_locked_spec_write_variants_share_one_canonical_envelope(
    monkeypatch: Any,
    tool_name: str,
    use_case: type,
    kwargs: dict[str, Any],
) -> None:
    context = _UowContext()

    async def _raise_locked(*args: Any, **call_kwargs: Any) -> Any:
        del args, call_kwargs
        raise SpecLockedError("spec-locked", "validation-1")

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(context),
    )
    monkeypatch.setattr(use_case, "execute", _raise_locked)

    raw = await getattr(server, tool_name).fn(**kwargs)

    _assert_locked_error(raw, tool_name=tool_name)
    assert context.exit_exception is SpecLockedError


@pytest.mark.asyncio
async def test_safe_spec_update_normalizes_locked_error_unless_fallback_requests_it() -> None:
    class _LockedService:
        async def update_spec(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise SpecLockedError("spec-locked", "validation-1")

    spec, raw = await server._safe_spec_update(
        _LockedService(),
        "spec-locked",
        "agent-1",
        object(),
    )
    assert spec is None
    assert raw is not None
    _assert_locked_error(raw, tool_name="safe_spec_update")

    with pytest.raises(SpecLockedError):
        await server._safe_spec_update(
            _LockedService(),
            "spec-locked",
            "agent-1",
            object(),
            propagate_spec_locked=True,
        )


@pytest.mark.asyncio
async def test_update_spec_lineage_failure_uses_typed_mcp_envelope(
    monkeypatch: Any,
) -> None:
    context = _UowContext()

    async def _raise_lineage(*args: Any, **call_kwargs: Any) -> Any:
        del args, call_kwargs
        raise SpecLineagePreflightError(
            "spec_ideation_not_done",
            "A Spec can only be created from an ideation in status 'done'.",
            facts={"ideation_id": "idea-draft", "ideation_status": "draft"},
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(context),
    )
    monkeypatch.setattr(McpUpdateSpecUseCase, "execute", _raise_lineage)

    raw = await server.okto_pulse_update_spec.fn(
        board_id="board-1",
        spec_id="spec-1",
        title="Relink attempt",
    )

    assert json.loads(raw) == {
        "error": "spec_ideation_not_done",
        "code": "spec_ideation_not_done",
        "message": (
            "A Spec can only be created from an ideation in status 'done'."
        ),
        "facts": {
            "ideation_id": "idea-draft",
            "ideation_status": "draft",
        },
    }
    assert context.exit_exception is SpecLineagePreflightError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "kwargs"),
    (
        (
            "okto_pulse_remove_business_rule",
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "rule_id": "br-1",
            },
        ),
        (
            "okto_pulse_remove_api_contract",
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "contract_id": "api-1",
            },
        ),
        (
            "okto_pulse_remove_decision",
            {
                "board_id": "board-1",
                "spec_id": "spec-locked",
                "decision_id": "decision-1",
            },
        ),
        *(
            (
                "okto_pulse_remove_spec_entity",
                {
                    "board_id": "board-1",
                    "spec_id": "spec-locked",
                    "target_type": target_type,
                    "entity_id": entity_id,
                },
            )
            for target_type, entity_id in (
                ("business_rule", "br-1"),
                ("api_contract", "api-1"),
                ("decision", "decision-1"),
            )
        ),
    ),
)
async def test_locked_spec_remove_aliases_share_canonical_envelope(
    monkeypatch: Any,
    tool_name: str,
    kwargs: dict[str, Any],
) -> None:
    context = _UowContext()

    async def _raise_locked(*args: Any, **call_kwargs: Any) -> Any:
        del args, call_kwargs
        raise SpecLockedError("spec-locked", "validation-1")

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(context),
    )
    monkeypatch.setattr(McpRemoveSpecEntityUseCase, "execute", _raise_locked)

    raw = await getattr(server, tool_name).fn(**kwargs)

    _assert_locked_error(raw, tool_name=tool_name)
    assert context.exit_exception is SpecLockedError
