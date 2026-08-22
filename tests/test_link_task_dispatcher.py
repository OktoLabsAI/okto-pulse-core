"""Regression tests for okto_pulse_link_task unified dispatcher (Story 5).

Bug bdadbe0e: Calling per-type tool via `.fn(board_id, spec_id, target_id, card_id)`
positionally fails with `TypeError: takes 0 positional arguments but 4 were given`
because the XML safety wrapper (server.py:881 `_xml_safety_log_decorator`)
exposes only `**kwargs`. The dispatcher MUST forward via keyword arguments.

Each per-type tool has a different name for the "intermediate" id parameter:
  link_card_to_spec(board_id, spec_id, card_id)                       — no intermediate
  link_task_to_scenario(board_id, spec_id, scenario_id, card_id)      — scenario_id
  link_task_to_fr(board_id, spec_id, fr_id, card_id)                  — fr_id
  link_task_to_rule(board_id, spec_id, rule_id, card_id)              — rule_id
  link_task_to_decision(board_id, spec_id, decision_id, card_id)     — decision_id
  link_task_to_tr(board_id, spec_id, tr_id, card_id)                 — tr_id
  link_task_to_contract(board_id, spec_id, contract_id, card_id)     — contract_id
  link_task_to_integration_requirement(..., requirement_id, ...)     — requirement_id
  link_task_to_observability_requirement(..., requirement_id, ...)   — requirement_id
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server
from okto_pulse.core.domain.human_validation_cycle import (
    SubjectEditRequiresDraftError,
)
from sqlalchemy_test_models import Board, Card, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecUpdate
from okto_pulse.core.services.main import SpecService


# Each tuple is (target_type, requires_spec_id)
_TARGET_TYPES = [
    ("scenario", True),
    ("test_scenario", True),
    ("fr", True),
    ("functional_requirement", True),
    ("rule", True),
    ("business_rule", True),
    ("decision", True),
    ("tr", True),
    ("technical_requirement", True),
    ("contract", True),
    ("api_contract", True),
    ("ir", True),
    ("integration_requirement", True),
    ("or", True),
    ("observability_requirement", True),
    ("spec", False),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("target_type,requires_spec_id", _TARGET_TYPES)
async def test_dispatch_reaches_per_type_tool_without_type_error(target_type, requires_spec_id):
    """The dispatcher must forward via kwargs so the per-type tool body is reached.

    We pass fake IDs — the tool body will fail with auth/db error, but NEVER
    with a TypeError about positional arguments.
    """
    result = await server.okto_pulse_link_task.fn(
        board_id="fake_board",
        target_type=target_type,
        target_id="fake_target",
        card_id="fake_card",
        spec_id="fake_spec" if requires_spec_id else "",
    )
    # Bug bdadbe0e symptom: would contain 'takes 0 positional arguments'
    assert "takes 0 positional arguments" not in str(result), (
        f"Dispatch for target_type={target_type!r} hit the XML safety wrapper "
        f"with positional args. Use keyword arguments when forwarding via .fn."
    )


@pytest.mark.asyncio
async def test_unknown_target_type_returns_structured_error():
    """Unknown target_type returns {error: ...} and lists the accepted values."""
    result = await server.okto_pulse_link_task.fn(
        board_id="fake_board",
        target_type="banana",
        target_id="fake",
        card_id="fake",
    )
    assert "Unknown target_type 'banana'" in result
    assert "scenario" in result and "fr" in result and "rule" in result  # whitelist enumerated


@pytest.mark.asyncio
async def test_non_spec_target_requires_spec_id():
    """Every target_type except 'spec' requires spec_id."""
    result = await server.okto_pulse_link_task.fn(
        board_id="fake_board",
        target_type="rule",
        target_id="fake_rule",
        card_id="fake_card",
        spec_id="",
    )
    assert "spec_id is required" in result


_REMOVED_SHIM_NAMES = [
    "okto_pulse_link_task_to_scenario",
    "okto_pulse_link_task_to_rule",
    "okto_pulse_link_task_to_decision",
    "okto_pulse_link_task_to_tr",
    "okto_pulse_link_task_to_contract",
    "okto_pulse_link_task_to_integration_requirement",
    "okto_pulse_link_task_to_observability_requirement",
    "okto_pulse_link_card_to_spec",
]


@pytest.mark.parametrize("shim_name", _REMOVED_SHIM_NAMES)
def test_per_type_shim_no_longer_registered_as_mcp_tool(shim_name):
    """Story 5 closeout: 8 deprecated per-type shims removed in favor of the
    unified okto_pulse_link_task dispatcher. Each shim was downgraded to an
    internal helper (_link_*_internal) without @mcp.tool() registration.

    This regression test prevents the shims from being re-introduced as MCP
    tools by mistake.
    """
    attr = getattr(server, shim_name, None)
    assert attr is None or not hasattr(attr, "fn"), (
        f"Shim {shim_name} was re-introduced as an MCP tool. The unified "
        f"okto_pulse_link_task dispatcher is the only public entry point; "
        f"per-type tools must remain internal helpers (_link_*_internal)."
    )


_INTERNAL_HELPERS = [
    "_link_task_to_scenario_internal",
    "_link_task_to_fr_internal",
    "_link_task_to_rule_internal",
    "_link_task_to_decision_internal",
    "_link_task_to_tr_internal",
    "_link_task_to_contract_internal",
    "_link_task_to_integration_requirement_internal",
    "_link_task_to_observability_requirement_internal",
    "_link_card_to_spec_internal",
]


@pytest.mark.parametrize("helper_name", _INTERNAL_HELPERS)
def test_internal_helper_exists_and_is_async(helper_name):
    """Each dispatch target must exist as an async internal helper
    so okto_pulse_link_task can route correctly.
    """
    import inspect
    helper = getattr(server, helper_name, None)
    assert helper is not None, f"Internal helper {helper_name} missing"
    assert inspect.iscoroutinefunction(helper), (
        f"Internal helper {helper_name} must be async (called via await in dispatcher)"
    )


@pytest.mark.asyncio
async def test_spec_target_does_not_require_spec_id():
    """target_type='spec' uses target_id directly; spec_id can be empty."""
    result = await server.okto_pulse_link_task.fn(
        board_id="fake_board",
        target_type="spec",
        target_id="fake_spec_id",
        card_id="fake_card",
        spec_id="",
    )
    # Must NOT complain about missing spec_id
    assert "spec_id is required" not in result
    # Must NOT raise TypeError
    assert "takes 0 positional arguments" not in str(result)


# Helpers whose success response must include the saturation envelope so
# agents get consistent progress feedback after every link. The decision
# helper was an outlier (missing saturation) — this test pins parity.
_HELPERS_WITH_SATURATION_ENVELOPE = [
    "_link_task_to_scenario_internal",
    "_link_task_to_fr_internal",
    "_link_task_to_rule_internal",
    "_link_task_to_decision_internal",
    "_link_task_to_tr_internal",
    "_link_task_to_contract_internal",
    "_link_task_to_integration_requirement_internal",
    "_link_task_to_observability_requirement_internal",
]


@pytest.mark.parametrize("helper_name", _HELPERS_WITH_SATURATION_ENVELOPE)
def test_link_helper_returns_saturation_envelope(helper_name):
    """Every link_task internal helper must spread _saturation_or_coverage(cov)
    into its success JSON. Without this, agents calling the dispatcher get
    different response shapes per target_type and lose the saturation signal
    that drives 'continue linking vs submit validation' decisions.
    """
    import inspect
    helper = getattr(server, helper_name)
    src = inspect.getsource(helper)
    assert "_saturation_or_coverage" in src, (
        f"{helper_name} success response is missing the saturation envelope. "
        f"Expected `**_saturation_or_coverage(cov)` in the json.dumps payload."
    )


@pytest.mark.asyncio
async def test_fr_target_persists_direct_traceability_without_closing_fr_coverage(db_factory):
    """target_type='fr' must persist FR.linked_task_ids but keep FR coverage
    semantics tied to Business Rules.
    """
    board_id = f"lt-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"lt-spec-{uuid.uuid4().hex[:8]}"
    card_id = f"lt-card-{uuid.uuid4().hex[:8]}"
    fr_id = "fr_dispatch"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Link Task FR", owner_id="link-task-agent"))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Link Task FR Spec",
            status=SpecStatus.APPROVED,
            created_by="link-task-agent",
            functional_requirements=[{"id": fr_id, "text": "Persist FR traceability"}],
            acceptance_criteria=[],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation card",
            created_by="link-task-agent",
        ))
        await db.commit()

    ctx = type(
        "Ctx",
        (),
        {
            "agent_id": "link-task-agent",
            "agent_name": "link-task-agent",
            "board_id": board_id,
            "permissions": ["card.entity.update"],
        },
    )()
    register_mcp_test_runtime(db_factory)
    with patch.object(server, "_get_agent_ctx", AsyncMock(return_value=ctx)), \
         patch.object(server, "check_permission", return_value=None):
        result = json.loads(await server.okto_pulse_link_task.fn(
            board_id=board_id,
            target_type="fr",
            target_id=fr_id,
            card_id=card_id,
            spec_id=spec_id,
        ))

    assert result["success"] is True
    assert result["coverage_note"].startswith("Direct FR task link persisted")

    async with db_factory() as db:
        from okto_pulse.core.services.analytics_service import spec_coverage_summary

        spec = await db.get(Spec, spec_id)
        assert spec.functional_requirements[0]["linked_task_ids"] == [card_id]
        assert spec_coverage_summary(spec)["fr_coverage_pct"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target_type,target_id,field",
    [
        ("fr", "fr_locked", "functional_requirements"),
        ("tr", "tr_locked", "technical_requirements"),
        ("decision", "dec_locked", "decisions"),
    ],
)
async def test_locked_spec_allows_traceability_only_links_for_fr_tr_decision(
    db_factory,
    target_type,
    target_id,
    field,
):
    board_id = f"locked-link-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"locked-link-spec-{uuid.uuid4().hex[:8]}"
    card_id = f"locked-link-card-{uuid.uuid4().hex[:8]}"
    actor_id = "link-task-agent"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Locked Traceability Links", owner_id=actor_id))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Locked Traceability Spec",
            status=SpecStatus.APPROVED,
            created_by=actor_id,
            functional_requirements=[{"id": "fr_locked", "text": "Locked FR"}],
            technical_requirements=[{"id": "tr_locked", "text": "Locked TR"}],
            decisions=[
                {
                    "id": "dec_locked",
                    "title": "Locked decision",
                    "rationale": "Traceability-only coverage",
                    "status": "active",
                }
            ],
            acceptance_criteria=[],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
            skip_rules_coverage=True,
            skip_trs_coverage=True,
            skip_contract_coverage=True,
            skip_decisions_coverage=True,
        ))
        db.add(Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation card",
            created_by=actor_id,
        ))
        await db.commit()
        spec = await db.get(Spec, spec_id)
        validation_id = f"validation-{uuid.uuid4().hex[:8]}"
        spec.current_validation_id = validation_id
        spec.validations = [
            {
                "id": validation_id,
                "outcome": "success",
                "edition": spec.edition,
            }
        ]
        await db.commit()

    ctx = type(
        "Ctx",
        (),
        {
            "agent_id": actor_id,
            "agent_name": actor_id,
            "board_id": board_id,
            "permissions": ["card.entity.update"],
        },
    )()
    register_mcp_test_runtime(db_factory)
    with patch.object(server, "_get_agent_ctx", AsyncMock(return_value=ctx)), \
         patch.object(server, "check_permission", return_value=None):
        result = json.loads(await server.okto_pulse_link_task.fn(
            board_id=board_id,
            target_type=target_type,
            target_id=target_id,
            card_id=card_id,
            spec_id=spec_id,
        ))

    assert result["success"] is True
    assert result["traceability_only"] is True
    assert result["link_changed"] is True

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.version == 1
        assert spec.current_validation_id is not None
        collection = getattr(spec, field)
        target = next(item for item in collection if item["id"] == target_id)
        assert target["linked_task_ids"] == [card_id]
        with pytest.raises(SubjectEditRequiresDraftError):
            await SpecService(db).update_spec(
                spec_id,
                actor_id,
                SpecUpdate(description="semantic edit still blocked"),
            )
