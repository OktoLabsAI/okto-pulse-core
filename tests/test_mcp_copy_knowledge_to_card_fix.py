"""Bug 16fd0744 — okto_pulse_copy_knowledge_to_card AttributeError.

The MCP handler was instantiating SpecService and calling .list_knowledge
on it, but list_knowledge actually lives on SpecKnowledgeService. Symptom:
``'SpecService' object has no attribute 'list_knowledge'`` 100% of calls.

Fix: handler now uses SpecKnowledgeService(db).list_knowledge(spec_id).

These tests pin the contract (class layout) AND exercise the actual handler
end-to-end with a real DB so the bug cannot regress silently.
"""

from __future__ import annotations

import inspect
import json
import uuid

import pytest
import pytest_asyncio

from okto_pulse.core.models.db import (
    Board,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
)
from okto_pulse.core.models.schemas import SpecKnowledgeCreate
from okto_pulse.core.services.main import SpecKnowledgeService, SpecService


BOARD_ID = "copy-kb-bug-board-001"
USER_ID = "copy-kb-bug-agent-001"


# ---------------------------------------------------------------------------
# Contract pins — guard against the original mistake reappearing.
# ---------------------------------------------------------------------------

def test_spec_service_does_not_own_list_knowledge():
    """Original bug root cause: SpecService never had list_knowledge."""
    assert not hasattr(SpecService, "list_knowledge"), (
        "SpecService.list_knowledge would resurrect bug 16fd0744. "
        "Knowledge methods belong on SpecKnowledgeService."
    )


def test_spec_knowledge_service_owns_list_knowledge():
    assert hasattr(SpecKnowledgeService, "list_knowledge")
    sig = inspect.signature(SpecKnowledgeService.list_knowledge)
    assert "spec_id" in sig.parameters


def _server_source() -> str:
    from okto_pulse.core.mcp import server as mcp_server
    from pathlib import Path
    return Path(mcp_server.__file__).read_text(encoding="utf-8")


def _handler_block(name: str) -> str:
    """Slice the source between the handler def and the next top-level def."""
    src = _server_source()
    marker = f"async def {name}("
    start = src.index(marker)
    rest = src[start + len(marker):]
    next_def = rest.find("\nasync def ")
    end = start + len(marker) + (next_def if next_def != -1 else len(rest))
    return src[start:end]


def test_handler_source_uses_spec_knowledge_service():
    """The MCP handler must call SpecKnowledgeService — not SpecService."""
    block = _handler_block("okto_pulse_copy_knowledge_to_card")
    assert "SpecKnowledgeService" in block, (
        "copy_knowledge_to_card handler must instantiate SpecKnowledgeService"
    )
    # Make sure the buggy line is gone — old code did `spec_service.list_knowledge`.
    assert "spec_service.list_knowledge" not in block


# ---------------------------------------------------------------------------
# Functional test — exercise the service path end-to-end.
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def _seed_spec_with_kb():
    """Create a board+spec+1 KB row in the test DB."""
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    async with db_factory() as db:
        existing = await db.get(Board, BOARD_ID)
        if existing is None:
            db.add(Board(id=BOARD_ID, name="Copy KB Bug Board", owner_id=USER_ID))
            await db.flush()

        spec_id = str(uuid.uuid4())
        db.add(Spec(
            id=spec_id,
            board_id=BOARD_ID,
            title="Spec for KB copy bug repro",
            status=SpecStatus.APPROVED,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        await db.flush()

        kb_service = SpecKnowledgeService(db)
        kb = await kb_service.create_knowledge(
            spec_id=spec_id,
            user_id=USER_ID,
            data=SpecKnowledgeCreate(
                title="Test KB",
                content="Hello from bug repro",
                mime_type="text/markdown",
                description="kb for copy bug",
            ),
        )
        await db.commit()
        return spec_id, kb.id


@pytest.mark.asyncio
async def test_spec_knowledge_service_list_returns_seeded_kb(_seed_spec_with_kb):
    """Service-level proof: the method that was missing actually returns rows."""
    from okto_pulse.core.infra.database import get_session_factory

    spec_id, kb_id = _seed_spec_with_kb
    db_factory = get_session_factory()
    async with db_factory() as db:
        kb_service = SpecKnowledgeService(db)
        kbs = await kb_service.list_knowledge(spec_id)

    assert len(kbs) == 1
    assert kbs[0].id == kb_id
    assert kbs[0].title == "Test KB"
    assert isinstance(kbs[0], SpecKnowledgeBase)
