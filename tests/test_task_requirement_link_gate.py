from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import CardOperationError, CardService


USER_ID = "task-req-gate-user"


@pytest_asyncio.fixture(autouse=True)
async def _isolate_default_board_configuration_rows(db_factory):
    """Keep this module's committed template fixtures function-scoped.

    The suite intentionally shares one SQLite database for the session.  The
    default-board store itself is rebound for every test, but that does not (and
    must not) delete persisted templates.  These MCP tests commit template rows,
    so clean only rows owned by this module before and after each case.
    """

    async def clear() -> None:
        async with db_factory() as db:
            await db.execute(
                delete(DefaultBoardConfigurationAudit).where(
                    DefaultBoardConfigurationAudit.actor_id == USER_ID
                )
            )
            await db.execute(
                delete(DefaultBoardConfiguration).where(
                    DefaultBoardConfiguration.created_by == USER_ID
                )
            )
            await db.commit()

    await clear()
    yield
    await clear()


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "task-req-gate-agent",
            "permissions": None,
            "realm_id": "local",
        },
    )()


async def _call_mcp(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_card_case(
    db_factory,
    *,
    board_settings: dict | None = None,
    card_skip: bool = False,
    link_card_to_fr: bool = False,
) -> tuple[str, str, str]:
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Task requirement gate",
                owner_id=USER_ID,
                settings=board_settings
                if board_settings is not None
                else {"skip_task_requirement_link_gate_global": False},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Task requirement gate spec",
                status=SpecStatus.IN_PROGRESS,
                skip_test_coverage=True,
                skip_rules_coverage=True,
                skip_trs_coverage=True,
                skip_contract_coverage=True,
                skip_ir_coverage=True,
                skip_or_coverage=True,
                skip_decisions_coverage=True,
                functional_requirements=[
                    {
                        "id": "fr_gate",
                        "text": "Task must have a direct requirement link",
                        "linked_task_ids": [card_id] if link_card_to_fr else [],
                    }
                ],
                acceptance_criteria=[],
                decisions=[
                    {"id": "dec_gate", "title": "Gate is fail closed", "status": "active"},
                ],
                created_by=USER_ID,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implementation task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                skip_task_requirement_link_gate=card_skip,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


@pytest.mark.asyncio
async def test_card_start_requires_direct_requirement_link(db_factory):
    _, _, card_id = await _seed_card_case(db_factory)

    async with db_factory() as db:
        service = CardService(db)
        with pytest.raises(CardOperationError) as exc:
            await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert exc.value.code == "task_requirement_link_required"
    assert exc.value.facts["required_link_types"] == ["fr", "tr", "rule", "ir", "or"]


@pytest.mark.asyncio
async def test_card_start_allows_structured_fr_link(db_factory):
    _, _, card_id = await _seed_card_case(db_factory, link_card_to_fr=True)

    async with db_factory() as db:
        service = CardService(db)
        moved = await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert moved is not None
    assert moved.status == CardStatus.STARTED


@pytest.mark.asyncio
async def test_card_start_allows_human_card_skip(db_factory):
    _, _, card_id = await _seed_card_case(db_factory, card_skip=True)

    async with db_factory() as db:
        service = CardService(db)
        moved = await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert moved is not None
    assert moved.status == CardStatus.STARTED


@pytest.mark.asyncio
async def test_card_start_allows_board_skip(db_factory):
    _, _, card_id = await _seed_card_case(
        db_factory,
        board_settings={"skip_task_requirement_link_gate_global": True},
    )

    async with db_factory() as db:
        service = CardService(db)
        moved = await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert moved is not None
    assert moved.status == CardStatus.STARTED


@pytest.mark.asyncio
async def test_legacy_board_missing_gate_key_is_grandfathered(db_factory):
    _, _, card_id = await _seed_card_case(db_factory, board_settings={})

    async with db_factory() as db:
        service = CardService(db)
        moved = await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert moved is not None
    assert moved.status == CardStatus.STARTED


def test_settings_patch_preserves_legacy_absent_gate_key():
    merged = BoardGovernanceService.merge_settings_patch(
        {"skip_test_coverage_global": False},
        {"skip_test_coverage_global": True},
    )

    assert merged["skip_test_coverage_global"] is True
    assert "skip_task_requirement_link_gate_global" not in merged


def test_settings_patch_materializes_gate_key_when_explicit_false():
    merged = BoardGovernanceService.merge_settings_patch(
        {"skip_test_coverage_global": False},
        {"skip_task_requirement_link_gate_global": False},
    )

    assert merged["skip_task_requirement_link_gate_global"] is False


@pytest.mark.asyncio
async def test_card_without_spec_id_is_out_of_scope_for_requirement_gate(db_factory):
    board_id = _id("board")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Task requirement gate",
                owner_id=USER_ID,
                settings={"skip_task_requirement_link_gate_global": False},
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=None,
                title="Unscoped task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        moved = await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert moved is not None
    assert moved.status == CardStatus.STARTED


@pytest.mark.asyncio
async def test_cross_spec_requirement_link_does_not_satisfy_card_gate(db_factory):
    board_id, _, card_id = await _seed_card_case(db_factory)
    async with db_factory() as db:
        db.add(
            Spec(
                id=_id("other-spec"),
                board_id=board_id,
                title="Other spec",
                status=SpecStatus.IN_PROGRESS,
                functional_requirements=[
                    {"id": "fr_other", "text": "Other spec FR", "linked_task_ids": [card_id]}
                ],
                decisions=[
                    {"id": "dec_other", "title": "Other", "status": "active"},
                ],
                created_by=USER_ID,
            )
        )
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        with pytest.raises(CardOperationError) as exc:
            await service.move_card(card_id, USER_ID, CardMove(status=CardStatus.STARTED))

    assert exc.value.code == "task_requirement_link_required"


@pytest.mark.asyncio
async def test_spec_validation_requires_active_decision_even_when_decision_coverage_skipped(db_factory):
    board_id, spec_id, card_id = await _seed_card_case(db_factory, link_card_to_fr=True)
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec is not None
        spec.status = SpecStatus.APPROVED
        spec.decisions = []
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        spec = await db.get(Spec, spec_id)
        board = await db.get(Board, board_id)
        assert spec is not None and board is not None
        with pytest.raises(ValueError, match="at least one active Decision"):
            await service.check_decision_presence(spec)


@pytest.mark.asyncio
async def test_spec_gate_lists_normal_cards_without_requirement_links(db_factory):
    board_id, spec_id, card_id = await _seed_card_case(db_factory)

    async with db_factory() as db:
        service = CardService(db)
        spec = await db.get(Spec, spec_id)
        board = await db.get(Board, board_id)
        assert spec is not None and board is not None
        with pytest.raises(ValueError, match="no direct FR/TR/BR/IR/OR link"):
            await service.check_task_requirement_links_for_spec(spec, board)


@pytest.mark.asyncio
async def test_spec_gate_ignores_cancelled_archived_and_card_skip(db_factory):
    board_id, spec_id, card_id = await _seed_card_case(db_factory)
    async with db_factory() as db:
        card = await db.get(Card, card_id)
        assert card is not None
        card.status = CardStatus.CANCELLED
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        spec = await db.get(Spec, spec_id)
        board = await db.get(Board, board_id)
        assert spec is not None and board is not None
        await service.check_task_requirement_links_for_spec(spec, board)

    board_id, spec_id, card_id = await _seed_card_case(db_factory)
    async with db_factory() as db:
        card = await db.get(Card, card_id)
        assert card is not None
        card.archived = True
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        spec = await db.get(Spec, spec_id)
        board = await db.get(Board, board_id)
        assert spec is not None and board is not None
        await service.check_task_requirement_links_for_spec(spec, board)

    board_id, spec_id, card_id = await _seed_card_case(db_factory, card_skip=True)
    async with db_factory() as db:
        service = CardService(db)
        spec = await db.get(Spec, spec_id)
        board = await db.get(Board, board_id)
        assert spec is not None and board is not None
        await service.check_task_requirement_links_for_spec(spec, board)


@pytest.mark.asyncio
async def test_mcp_default_board_config_rejects_explicit_task_requirement_skip(db_factory):
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP default config", owner_id=USER_ID))
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        res = await _call_mcp(
            "okto_pulse_create_default_board_config_version",
            board_id=board_id,
            settings_payload={"skip_task_requirement_link_gate_global": True},
            scope="global",
            activate=False,
        )

    assert res["code"] == "human_control_required"
    assert res["details"]["mutation_allowed"] is False
    async with db_factory() as db:
        rows = (
            await db.execute(
                select(DefaultBoardConfiguration).where(
                    DefaultBoardConfiguration.created_by == USER_ID
                )
            )
        ).scalars().all()
    assert rows == []


@pytest.mark.asyncio
async def test_mcp_default_board_config_preserves_omitted_human_skip(db_factory):
    board_id = _id("board")
    scope = _id("scope")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP default config", owner_id=USER_ID))
        db.add(
            DefaultBoardConfiguration(
                id=_id("template"),
                version=1,
                status="active",
                is_active=True,
                scope=scope,
                settings_payload={"skip_task_requirement_link_gate_global": True},
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        res = await _call_mcp(
            "okto_pulse_create_default_board_config_version",
            board_id=board_id,
            settings_payload={"skip_test_coverage_global": True},
            scope=scope,
            activate=False,
        )

    assert res["settings_payload"]["skip_test_coverage_global"] is True
    assert res["settings_payload"]["skip_task_requirement_link_gate_global"] is True


@pytest.mark.asyncio
async def test_default_board_config_create_preserves_legacy_absent_gate_key(db_factory):
    scope = _id("scope")
    async with db_factory() as db:
        db.add(
            DefaultBoardConfiguration(
                id=_id("legacy-template"),
                version=1,
                status="active",
                is_active=True,
                scope=scope,
                settings_payload={"skip_test_coverage_global": False},
                created_by=USER_ID,
            )
        )
        await db.flush()
        service = DefaultBoardConfigurationService(db)

        next_template = await service.create_version(
            settings_payload={"skip_test_coverage_global": True},
            actor=USER_ID,
            scope=scope,
            activate=False,
        )

    assert next_template.settings_payload["skip_test_coverage_global"] is True
    assert "skip_task_requirement_link_gate_global" not in next_template.settings_payload


@pytest.mark.asyncio
async def test_mcp_default_board_config_activate_rejects_human_skip_change(db_factory):
    board_id = _id("board")
    active_id = _id("active-template")
    target_id = _id("target-template")
    scope = _id("scope")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP default config", owner_id=USER_ID))
        db.add(
            DefaultBoardConfiguration(
                id=active_id,
                version=1,
                status="active",
                is_active=True,
                scope=scope,
                settings_payload={"skip_task_requirement_link_gate_global": True},
                created_by=USER_ID,
            )
        )
        db.add(
            DefaultBoardConfiguration(
                id=target_id,
                version=2,
                status="inactive",
                is_active=False,
                scope=scope,
                settings_payload={},
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        res = await _call_mcp(
            "okto_pulse_activate_default_board_config_version",
            board_id=board_id,
            template_id=target_id,
        )

    assert res["code"] == "human_control_required"
    async with db_factory() as db:
        target = await db.get(DefaultBoardConfiguration, target_id)
        assert target is not None
        assert target.is_active is False


@pytest.mark.asyncio
async def test_mcp_default_board_config_deactivate_rejects_human_skip_change(db_factory):
    board_id = _id("board")
    template_id = _id("active-template")
    scope = _id("scope")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP default config", owner_id=USER_ID))
        db.add(
            DefaultBoardConfiguration(
                id=template_id,
                version=1,
                status="active",
                is_active=True,
                scope=scope,
                settings_payload={"skip_task_requirement_link_gate_global": True},
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        res = await _call_mcp(
            "okto_pulse_deactivate_default_board_config_version",
            board_id=board_id,
            template_id=template_id,
        )

    assert res["code"] == "human_control_required"
    async with db_factory() as db:
        template = await db.get(DefaultBoardConfiguration, template_id)
        assert template is not None
        assert template.is_active is True
