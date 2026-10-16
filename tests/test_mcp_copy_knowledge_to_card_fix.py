"""Spec Knowledge service ownership and persisted list contract.

The retired physical Spec-to-Card copy tool has no handler/use case in v0.4.0.
Keep the native service ownership and real persistence regression coverage.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
import pytest_asyncio

from sqlalchemy_test_models import (
    Board,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import SpecKnowledgeCreate
from okto_pulse.core.ports.application_persistence import ApplicationRecord
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
            status=SpecStatus.DRAFT,
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
    assert isinstance(kbs[0], ApplicationRecord)
    assert kbs[0].entity == "spec_knowledge_base"
