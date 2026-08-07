from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet, Permissions
from okto_pulse.core.mcp import server


BOARD_ID = "board-mcp-granular-mutations"


def _ctx(permissions) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id="agent-mcp-granular-mutations",
        agent_name="Granular Mutations",
        board_id=BOARD_ID,
        permissions=permissions,
    )


def _required_permission(raw: str) -> str:
    outer = json.loads(raw)
    detail = json.loads(outer["error"])
    assert detail["reason"] == "permission_missing"
    return detail["required_permission"]


@pytest.mark.asyncio
async def test_card_create_validates_input_before_core_lookup_and_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet({"card": {"entity": {"create": False}}}))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_create_card.fn(
        board_id=BOARD_ID,
        title="Denied",
        spec_id="spec-1",
        status="not-a-status",
    )

    payload = json.loads(raw)
    assert "Invalid status" in payload["error"]


@pytest.mark.asyncio
async def test_card_create_keeps_legacy_list_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx([Permissions.CARDS_CREATE])

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_create_card.fn(
        board_id=BOARD_ID,
        title="Legacy",
        spec_id="spec-1",
        status="not-a-status",
    )

    payload = json.loads(raw)
    assert "Invalid status" in payload["error"]
    assert "permission" not in payload["error"].lower()


@pytest.mark.asyncio
async def test_card_create_keeps_internal_mcp_wildcard_compatibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx(["*"])

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_create_card.fn(
        board_id=BOARD_ID,
        title="Trusted internal caller",
        spec_id="spec-1",
        status="not-a-status",
    )

    payload = json.loads(raw)
    assert "Invalid status" in payload["error"]
    assert "permission" not in payload["error"].lower()


@pytest.mark.asyncio
async def test_sprint_qa_ask_defers_permission_until_after_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet({"sprint": {"qa": {"ask": False}}}))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_ask_sprint_question.fn(
        board_id=BOARD_ID,
        sprint_id="sprint-1",
        question="Can this proceed?",
    )

    assert json.loads(raw) == {"error": "Sprint not found"}


@pytest.mark.asyncio
async def test_profile_update_denies_false_canonical_leaf_before_uow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _authenticated_agent():
        return SimpleNamespace(
            id="agent-mcp-granular-mutations",
            name="Granular Mutations",
            permissions=PermissionSet({"profile": {"update": False}}),
        )

    def _forbidden_uow_factory():
        raise AssertionError("denied profile update resolved a UoW")

    monkeypatch.setattr(server, "_get_authenticated_agent", _authenticated_agent)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        _forbidden_uow_factory,
    )

    raw = await server.okto_pulse_update_my_profile.fn(description="Denied")

    assert _required_permission(raw) == "profile.update"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flags", "kwargs"),
    (
        (
            {"card": {"entity": {"edit_fields": False}}},
            {"title": "Changed"},
        ),
        (
            {"card": {"entity": {"assign": False}}},
            {"assignee_id": "agent-2"},
        ),
        (
            {"card": {"entity": {"label": False}}},
            {"labels": ["blocked"]},
        ),
        (
            {"card": {"entity": {"link_tests": False}}},
            {"linked_test_task_ids": ["card-test-1"]},
        ),
        (
            {"card": {"entity": {"edit_bug_fields": False}}},
            {"severity": "major"},
        ),
    ),
)
async def test_update_card_defers_permission_decision_until_after_lookup(
    monkeypatch: pytest.MonkeyPatch,
    flags: dict,
    kwargs: dict,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet(flags))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_update_card.fn(
        board_id=BOARD_ID,
        card_id="card-1",
        **kwargs,
    )

    assert json.loads(raw) == {"error": "Card not found"}


@pytest.mark.asyncio
async def test_structured_resource_denies_false_specific_leaf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet({"spec": {"rules": {"create": False}}}))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_add_business_rule.fn(
        board_id=BOARD_ID,
        spec_id="spec-1",
        title="Rule",
        rule="Must be denied",
        when="When called",
        then="Then deny",
    )

    assert _required_permission(raw) == "spec.rules.create"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool",
    (
        server.okto_pulse_add_card_dependency,
        server.okto_pulse_remove_card_dependency,
    ),
)
async def test_card_dependency_mutations_lookup_before_permission_denial(
    monkeypatch: pytest.MonkeyPatch,
    tool,
) -> None:
    async def _context(_board_id: str):
        return _ctx(
            PermissionSet({"card": {"entity": {"manage_dependencies": False}}})
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await tool.fn(
        board_id=BOARD_ID,
        card_id="card-1",
        depends_on_id="card-2",
    )

    assert json.loads(raw) == {"error": "Card not found"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flags", "kwargs"),
    (
        (
            {"sprint": {"entity": {"edit_fields": False}}},
            {"title": "Changed"},
        ),
        (
            {"sprint": {"entity": {"edit_coverage_flags": False}}},
            {"skip_test_coverage": False},
        ),
        (
            {"sprint": {"entity": {"edit_coverage_flags": False}}},
            {"validation_threshold": 80},
        ),
        (
            {"sprint": {"entity": {"label": False}}},
            {"labels": ["blocked"]},
        ),
    ),
)
async def test_update_sprint_defers_permission_decision_until_after_lookup(
    monkeypatch: pytest.MonkeyPatch,
    flags: dict,
    kwargs: dict,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet(flags))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_update_sprint.fn(
        board_id=BOARD_ID,
        sprint_id="sprint-1",
        **kwargs,
    )

    assert json.loads(raw) == {"error": "Sprint not found"}


@pytest.mark.asyncio
async def test_submit_spec_validation_precheck_accepts_legacy_evaluate_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReachedCore(RuntimeError):
        pass

    async def _context(_board_id: str):
        return _ctx([Permissions.SPECS_EVALUATE])

    def _reached_core_factory():
        raise ReachedCore

    monkeypatch.setattr(server, "_get_agent_ctx", _context)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        _reached_core_factory,
    )

    with pytest.raises(ReachedCore):
        await server.okto_pulse_submit_spec_validation.fn(
            board_id=BOARD_ID,
            spec_id="spec-1",
            completeness=90,
            completeness_justification="Complete enough",
            assertiveness=90,
            assertiveness_justification="Assertive enough",
            ambiguity=10,
            ambiguity_justification="Low ambiguity",
            general_justification="A sufficiently complete validation rationale",
            recommendation="approve",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "flags", "kwargs", "expected_error"),
    (
        (
            server.okto_pulse_submit_sprint_evaluation,
            {"sprint": {"evaluations": {"submit": False}}},
            {
                "breakdown_completeness": 80,
                "breakdown_justification": "Complete",
                "granularity": 80,
                "granularity_justification": "Granular",
                "dependency_coherence": 80,
                "dependency_justification": "Coherent",
                "test_coverage_quality": 80,
                "test_coverage_justification": "Covered",
                "overall_score": 80,
                "overall_justification": "Ready",
                "recommendation": "approve",
            },
            "Sprint not found",
        ),
        (
            server.okto_pulse_delete_sprint_evaluation,
            {"sprint": {"evaluations": {"delete": False}}},
            {"evaluation_id": "evaluation-1"},
            "Sprint not found",
        ),
        (
            server.okto_pulse_answer_sprint_question,
            {"sprint": {"qa": {"answer": False}}},
            {"qa_id": "qa-1", "answer": "No"},
            "Q&A item not found",
        ),
    ),
)
async def test_sprint_mutations_lookup_before_permission_denial(
    monkeypatch: pytest.MonkeyPatch,
    tool,
    flags: dict,
    kwargs: dict,
    expected_error: str,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet(flags))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await tool.fn(
        board_id=BOARD_ID,
        sprint_id="sprint-1",
        **kwargs,
    )

    assert json.loads(raw) == {"error": expected_error}


@pytest.mark.asyncio
async def test_assign_tasks_to_sprint_lookup_precedes_permission_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _context(_board_id: str):
        return _ctx(PermissionSet({"sprint": {"entity": {"assign": False}}}))

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await server.okto_pulse_assign_tasks_to_sprint.fn(
        board_id=BOARD_ID,
        sprint_id="sprint-1",
        card_ids=["card-1", "card-2"],
    )

    assert json.loads(raw) == {"error": "Sprint not found"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "permission_leaf", "kwargs"),
    (
        (
            server.okto_pulse_copy_mockups_to_card,
            "card.copy_from_spec.mockups",
            {"screen_ids": ["screen-1", "screen-2"]},
        ),
        (
            server.okto_pulse_copy_knowledge_to_card,
            "card.copy_from_spec.knowledge",
            {"knowledge_ids": ["knowledge-1", "knowledge-2"]},
        ),
        (
            server.okto_pulse_copy_qa_to_card,
            "card.copy_from_spec.qa",
            {},
        ),
    ),
)
async def test_copy_to_card_looks_up_source_before_permission_denial(
    monkeypatch: pytest.MonkeyPatch,
    tool,
    permission_leaf: str,
    kwargs: dict,
) -> None:
    family, operation, resource = permission_leaf.split(".")

    async def _context(_board_id: str):
        return _ctx(
            PermissionSet(
                {family: {operation: {resource: False}}}
            )
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _context)

    raw = await tool.fn(
        board_id=BOARD_ID,
        spec_id="spec-1",
        card_id="card-1",
        **kwargs,
    )

    assert json.loads(raw) == {"error": "Spec not found"}
