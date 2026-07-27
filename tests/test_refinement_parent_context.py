from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Ideation,
    IdeationComplexity,
    IdeationKnowledgeBase,
    IdeationQAItem,
    IdeationStatus,
    Refinement,
    RefinementKnowledgeBase,
    RefinementStatus,
)
from okto_pulse.core.models.schemas import RefinementCreate
from okto_pulse.core.services.main import RefinementService


USER_ID = "refinement-parent-context-agent"


def _id() -> str:
    return str(uuid.uuid4())


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read", "specs:create"],
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_ideation() -> tuple[str, str, str]:
    board_id = _id()
    ideation_id = _id()
    kb_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Parent Context Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Resource Gate Ideation",
                description="Gate for mandatory Architecture, Mockup and KB resources.",
                problem_statement="Entities can advance without explicit resource completeness.",
                proposed_approach="Track Provided/N/A/Missing and block completion when unresolved.",
                scope_assessment={"domains": 4, "ambiguity": 2, "dependencies": 3},
                complexity=IdeationComplexity.LARGE,
                status=IdeationStatus.DONE,
                version=3,
                created_by=USER_ID,
            )
        )
        db.add(
            IdeationQAItem(
                ideation_id=ideation_id,
                question="Should MCP require N/A justification?",
                question_type="single_choice",
                choices=[{"id": "mcp_only", "label": "Only MCP"}],
                selected=["mcp_only"],
                asked_by=USER_ID,
                answered_by=USER_ID,
            )
        )
        db.add(
            IdeationKnowledgeBase(
                id=kb_id,
                ideation_id=ideation_id,
                title="Resource Gate Decisions",
                description="Closed decisions for resource completeness.",
                content="MCP N/A requires justification; UI justification is optional.",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, ideation_id, kb_id


@pytest.mark.asyncio
async def test_create_refinement_preserves_parent_context_with_explicit_description():
    board_id, ideation_id, kb_id = await _seed_ideation()
    db_factory = get_session_factory()

    async with db_factory() as db:
        refinement = await RefinementService(db).create_refinement(
            ideation_id,
            USER_ID,
            RefinementCreate(
                ideation_id=ideation_id,
                title="Refine Resource Gate",
                description="Focused refinement for Level 1 and Level 2 gates.",
                in_scope=["Resource state", "Task coverage gate"],
            ),
            skip_ownership_check=True,
        )
        await db.commit()
        refinement_id = refinement.id

    async with db_factory() as db:
        refinement = await RefinementService(db).get_refinement(refinement_id)
        assert refinement is not None
        assert refinement.description.startswith("Focused refinement")
        assert "## Parent Ideation Context" in refinement.description
        assert "Entities can advance without explicit resource completeness." in refinement.description
        assert "Track Provided/N/A/Missing" in refinement.description
        assert "mcp_only" in refinement.description

        propagated = (
            await db.execute(
                select(RefinementKnowledgeBase).where(
                    RefinementKnowledgeBase.refinement_id == refinement_id
                )
            )
        ).scalar_one()
        assert propagated.source_type == "ideation"
        assert propagated.source_id == ideation_id
        assert propagated.source_title == "Resource Gate Ideation"
        assert propagated.source_version == 3
        assert propagated.source_kb_id == kb_id

    assert board_id


@pytest.mark.asyncio
async def test_create_refinement_serializes_manual_screen_mockups():
    board_id, ideation_id, _ = await _seed_ideation()
    db_factory = get_session_factory()

    async with db_factory() as db:
        refinement = await RefinementService(db).create_refinement(
            ideation_id,
            USER_ID,
            RefinementCreate(
                ideation_id=ideation_id,
                title="Refine Resource Gate Mockups",
                description="Refinement with a manually supplied mockup.",
                in_scope=["Mockup persistence"],
                screen_mockups=[
                    {
                        "id": "mock-refinement-1",
                        "title": "Board settings modal",
                        "description": "Auto propagation controls.",
                        "screen_type": "modal",
                        "html_content": "<section>Board settings</section>",
                        "annotations": [
                            {
                                "id": "ann-1",
                                "text": "Resource selector appears below the toggle.",
                            }
                        ],
                        "order": 1,
                    }
                ],
            ),
            skip_ownership_check=True,
        )
        await db.commit()
        refinement_id = refinement.id

    async with db_factory() as db:
        refinement = await RefinementService(db).get_refinement(refinement_id)
        assert refinement is not None
        assert refinement.screen_mockups is not None
        assert isinstance(refinement.screen_mockups[0], dict)
        assert refinement.screen_mockups[0]["id"] == "mock-refinement-1"
        assert refinement.screen_mockups[0]["annotations"][0]["id"] == "ann-1"

    assert board_id


@pytest.mark.asyncio
async def test_mcp_refinement_context_exposes_parent_ideation_and_resolved_reference():
    board_id, ideation_id, kb_id = await _seed_ideation()
    db_factory = get_session_factory()

    async with db_factory() as db:
        refinement = await RefinementService(db).create_refinement(
            ideation_id,
            USER_ID,
            RefinementCreate(
                ideation_id=ideation_id,
                title="Refine Resource Gate Context",
                description="Agent supplied a short description.",
                in_scope=["Parent context visibility"],
            ),
            skip_ownership_check=True,
        )
        await db.commit()
        refinement_id = refinement.id

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), patch.object(
        mcp_server, "check_permission", return_value=None
    ):
        context = await _call(
            "okto_pulse_get_refinement_context",
            board_id=board_id,
            refinement_id=refinement_id,
        )

    assert context["parent_ideation"]["id"] == ideation_id
    assert context["parent_ideation"]["problem_statement"] == (
        "Entities can advance without explicit resource completeness."
    )
    assert context["knowledge_bases"][0]["source_kb_id"] == kb_id
    structured_contexts = context["resolved_references"]["structured_contexts"]
    assert structured_contexts[0]["reference_type"] == "parent_ideation"
    assert structured_contexts[0]["source_id"] == ideation_id


@pytest.mark.asyncio
async def test_refinement_to_spec_derivation_includes_parent_context_for_legacy_refinement():
    board_id, ideation_id, _ = await _seed_ideation()
    refinement_id = _id()
    db_factory = get_session_factory()

    async with db_factory() as db:
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Legacy Refinement",
                description="Legacy refinement description without inherited context.",
                in_scope=["Legacy scope"],
                status=RefinementStatus.DONE,
                created_by=USER_ID,
            )
        )
        await db.commit()

    async with db_factory() as db:
        spec = await RefinementService(db).derive_spec(
            refinement_id,
            USER_ID,
            skip_ownership_check=True,
        )
        await db.commit()

        assert spec is not None
        assert "## Refinement Description" in spec.context
        assert "Legacy scope" in spec.context
        assert "## Parent Ideation Context" in spec.context
        assert "Entities can advance without explicit resource completeness." in spec.context
