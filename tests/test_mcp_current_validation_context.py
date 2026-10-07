"""Current Spec policy remains identical in bounded pre-mutation reads."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.community.adapters.sqlalchemy_models import Base, Board, Card, Spec
from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import CommunitySemanticSession
from okto_pulse.community.adapters.sqlalchemy_unit_of_work import CommunityUnitOfWorkFactory
from okto_pulse.community.adapters.sqlalchemy_application_persistence import CommunitySqlAlchemyApplicationPersistence
from okto_pulse.community.adapters.relational_application import CommunityRelationalApplicationAdapter
from okto_pulse.community.adapters.sqlalchemy_resource_gate_service import CommunitySqlAlchemyResourceGateAdapter
from okto_pulse.core.ports.relational_services import register_resource_gate_adapter_factory
from okto_pulse.core.ports.application_persistence import register_application_persistence_port
from okto_pulse.core.ports.relational_application import register_relational_application_adapter
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope
from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus
from okto_pulse.core.domain.execution_contract import new_execution_contract
from okto_pulse.core.mcp import server


@pytest_asyncio.fixture
async def current_sessions(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'current-policy.db'}")
    sessions = async_sessionmaker(engine, expire_on_commit=False,
        sync_session_class=CommunitySemanticSession, info={"realm_scope": RealmScope.local()})
    register_application_persistence_port(CommunitySqlAlchemyApplicationPersistence())
    register_relational_application_adapter(CommunityRelationalApplicationAdapter())
    register_resource_gate_adapter_factory(CommunitySqlAlchemyResourceGateAdapter)
    factory = CommunityUnitOfWorkFactory(sessions)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        yield sessions
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,scope", (("summary", "all"), ("full", "all"), ("full", "gate")))
@pytest.mark.parametrize("confidence", (90, 60))
async def test_spec_policy_is_identical_in_every_task_context(current_sessions, monkeypatch, profile, scope, confidence):
    suffix = uuid4().hex
    board_id, spec_id, card_id = (f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}")
    async with current_sessions() as db:
        db.add(Board(id=board_id, realm_id=RealmScope.local().realm_id, name="Current task context", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", created_by="owner",
                    status=SpecStatus.IN_PROGRESS, require_task_validation=False,
                    validation_min_confidence=confidence, validation_min_completeness=0, validation_max_drift=0,
                    architecture_adoption=ArchitectureAdoptionScope(board_id=board_id, spec_id=spec_id,
                        adopted_in_edition=1, actor_id="owner", inherited_resource_ids=()).model_dump(mode="json"),
                    execution_contract=new_execution_contract(board_id=board_id, spec_id=spec_id,
                        edition=1, actor_id="owner", origin="new_spec")))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Task",
                    status=CardStatus.NOT_STARTED, card_type=CardType.NORMAL, created_by="owner"))
        await db.commit()
    register_mcp_test_runtime(current_sessions)
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=SimpleNamespace(
        agent_id="owner", agent_name="owner", board_id=board_id, permissions=["*"])))
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(server, "_mcp_code_traceability_projection", AsyncMock(return_value={}))
    tool = await server.mcp.get_tool("okto_pulse_get_task_context")
    result = json.loads(await tool.fn(board_id=board_id, card_id=card_id,
                                     profile=profile, context_scope=scope))
    config = result["validation_config"]
    assert config["min_confidence"] == confidence
    assert config["min_completeness"] == 0
    assert config["max_drift"] == 0
    assert config["required"] is False
    assert set(config["resolved_sources"].values()) == {"spec"}
    assert config["resolved_from"] == "spec"
    async with current_sessions() as db:
        card = await db.get(Card, card_id)
        assert not hasattr(card, "migrated_validation_policy")
