"""Shared harness + real-pipeline seeding for the R3 scenario test cards.

NOT a test module (no ``test_`` prefix). Provides the MCP tool harness and the
real lineage seeding (board -> ideation -> refinement -> spec/card) the R3
scenario tests drive through the REAL ``okto_pulse_*`` tools and the REAL Resource
Gate / lineage services.
"""

from __future__ import annotations

from datetime import datetime, timezone

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementKnowledgeBase,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
)
from okto_pulse.core.domain.research_decision_ledger import (
    ResearchDecisionLedgerSnapshot,
)
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)

USER_ID = "user-r3-scenarios"


def sid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r3 scenario tester"
        self.permissions = ["*"]


async def call_tool(name: str, **kwargs) -> dict:
    """Invoke a real MCP tool against the test DB with a stubbed auth ctx."""
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_architecture_copy_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def new_board(db_factory) -> str:
    board_id = sid("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r3 scenarios", owner_id=USER_ID))
        await db.commit()
    return board_id


async def freeze_refinement_completion_fixture(db, refinement) -> None:
    """Persist both immutable completion snapshots required by derivation."""

    db.add(
        RefinementSnapshot(
            refinement_id=refinement.id,
            version=refinement.version,
            title=refinement.title,
            description=refinement.description,
            in_scope=refinement.in_scope,
            out_of_scope=refinement.out_of_scope,
            analysis=refinement.analysis,
            decisions=refinement.decisions,
            labels=refinement.labels,
            qa_snapshot=[],
            created_by=USER_ID,
        )
    )
    await require_relational_application_adapter().research_decisions(
        db
    ).save_snapshot(
        ResearchDecisionLedgerSnapshot(
            id=sid("rdl-snapshot"),
            board_id=refinement.board_id,
            refinement_id=refinement.id,
            refinement_version=refinement.version,
            heads=(),
            created_at=datetime.now(timezone.utc),
        )
    )


async def seed_refinement(
    db_factory, board_id, *, status="done", kb=False, mockup=False, architecture=False,
) -> dict:
    """Refinement (with parent ideation) optionally carrying DIRECT KB / mockup /
    architecture. Returns the seeded ids."""
    ideation_id = sid("idea")
    refinement_id = sid("ref")
    kb_id = sid("refkb")
    mockup_id = sid("refmock")
    design_id = sid("refdesign")
    async with db_factory() as db:
        db.add(Ideation(id=ideation_id, board_id=board_id, title="R3 ideation",
                        status=IdeationStatus.DONE, created_by=USER_ID))
        refinement = Refinement(
            id=refinement_id, board_id=board_id, ideation_id=ideation_id,
            title="R3 refinement", created_by=USER_ID,
            status=RefinementStatus(status),
            screen_mockups=(
                [{"id": mockup_id, "title": "Ref mockup", "screen_type": "form",
                  "html_content": "<div/>"}] if mockup else []
            ),
        )
        db.add(refinement)
        if kb:
            db.add(RefinementKnowledgeBase(
                id=kb_id, refinement_id=refinement_id, title="Ref KB",
                description="d", content="ref content", mime_type="text/markdown",
                created_by=USER_ID,
            ))
        if architecture:
            db.add(ArchitectureDesign(
                id=design_id, board_id=board_id, parent_type="refinement",
                refinement_id=refinement_id, title="Ref design", global_description="g",
                entities=[], interfaces=[], diagrams=[], created_by=USER_ID,
            ))
        if refinement.status is RefinementStatus.DONE:
            await db.flush()
            await freeze_refinement_completion_fixture(db, refinement)
        await db.commit()
    return {"ideation_id": ideation_id, "refinement_id": refinement_id,
            "kb_id": kb_id, "mockup_id": mockup_id, "design_id": design_id}


async def seed_legacy_spec_with_card(db_factory, board_id, ref) -> dict:
    """A manual/legacy spec (NO direct resources) linked to the refinement +
    ideation, plus a normal task card. The effective-fallback case."""
    spec_id = sid("spec")
    card_id = sid("card")
    async with db_factory() as db:
        db.add(Spec(id=spec_id, board_id=board_id, refinement_id=ref["refinement_id"],
                    ideation_id=ref["ideation_id"], title="Legacy manual spec",
                    created_by=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="impl card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    return {"spec_id": spec_id, "card_id": card_id}


async def add_card(db_factory, board_id, spec_id) -> str:
    card_id = sid("card")
    async with db_factory() as db:
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="impl card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    return card_id
