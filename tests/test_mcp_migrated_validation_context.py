"""Migration-only policy must remain effective in bounded pre-mutation reads."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from mcp_runtime_testing import register_mcp_test_runtime
from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus, Sprint
from okto_pulse.core.domain.task_validation_policy import MigratedTaskValidationPolicy
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,scope", (("summary", "all"), ("full", "all"), ("full", "gate")))
@pytest.mark.parametrize("confidence", (90, 60))
@pytest.mark.parametrize("invalid_scope", (False, True, "unmigrated"))
async def test_migrated_policy_is_identical_in_every_task_context(monkeypatch, profile, scope, confidence, invalid_scope):
    suffix = uuid4().hex
    board_id, spec_id, card_id = (f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}")
    policy = MigratedTaskValidationPolicy.model_validate({
        "contract_version": "card-validation-compatibility/v1",
        "board_id": board_id, "card_id": card_id, "source_spec_id": spec_id,
        "source_sprint_id": f"retired-{confidence}", "migration_id": "offline-migration",
        "overrides": {"min_confidence": confidence, "min_completeness": 0,
                      "required": False, "max_drift": 0},
    }).model_dump(mode="json")
    if invalid_scope is True:
        policy["board_id"] = "foreign-board"
    if invalid_scope == "unmigrated":
        policy = None
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="Migrated task context", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", created_by="owner",
                    status=SpecStatus.IN_PROGRESS))
        if invalid_scope == "unmigrated":
            db.add(Sprint(id=f"legacy-{suffix}", board_id=board_id, spec_id=spec_id,
                          title="Historical", created_by="owner", validation_min_confidence=confidence))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Task",
                    status=CardStatus.NOT_STARTED, card_type=CardType.NORMAL,
                    created_by="owner", migrated_validation_policy=policy,
                    sprint_id=f"legacy-{suffix}" if invalid_scope == "unmigrated" else None))
        await db.commit()
    register_mcp_test_runtime(get_session_factory())
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=SimpleNamespace(
        agent_id="owner", agent_name="owner", board_id=board_id, permissions=["*"])))
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(server, "_mcp_code_traceability_projection", AsyncMock(return_value={}))
    tool = await server.mcp.get_tool("okto_pulse_get_task_context")
    if invalid_scope is True:
        with pytest.raises(ValueError, match="card_validation_compatibility_scope_mismatch"):
            await tool.fn(board_id=board_id, card_id=card_id, profile=profile, context_scope=scope)
        return
    result = json.loads(await tool.fn(board_id=board_id, card_id=card_id,
                                     profile=profile, context_scope=scope))
    if invalid_scope == "unmigrated":
        # Summary intentionally omits null optional blocks; both forms provide
        # no usable policy, while full/gate retains the explicit unavailable value.
        assert result.get("validation_config") is None
        if profile == "full":
            assert "validation_config" in result
        return
    config = result["validation_config"]
    assert config["min_confidence"] == confidence
    assert config["min_completeness"] == 0
    assert config["max_drift"] == 0
    assert config["required"] is False
    assert set(config["resolved_sources"].values()) == {"card_compatibility"}
    assert config["resolved_from"] == "card_compatibility"
    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
        assert card.sprint_id is None
        assert card.migrated_validation_policy == policy
