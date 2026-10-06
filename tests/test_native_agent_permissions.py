"""Native Agent DTO, profile projection and permission document contract."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.permissions import (
    PermissionSet, generate_role_summary, get_builtin_presets,
)
from okto_pulse.core.models.schemas import AgentCreate, AgentUpdate, AgentResponse


@pytest.mark.parametrize("schema", [AgentCreate, AgentUpdate])
@pytest.mark.parametrize("old_value", [None, [], ["board:read"], ["*"]])
def test_flat_permission_payload_is_refused(schema, old_value):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        schema.model_validate({"name": "Agent", "permissions": old_value})


def test_native_dto_has_only_flags_and_preset():
    created = AgentCreate(name="Agent", preset_id="reporter", permission_flags={})
    assert created.model_dump()["permission_flags"] == {}
    assert "permissions" not in created.model_dump()
    assert AgentUpdate(permission_flags={"profile": {"update": False}}).permission_flags
    response = AgentResponse(
        id="a", name="Agent", description=None, is_active=True,
        preset_id="reporter", permission_flags={}, created_by="owner",
        created_at=datetime.now(timezone.utc), last_used_at=None,
    )
    assert "permissions" not in response.model_dump()


def test_role_summary_uses_effective_flags_and_owner_review():
    reporter = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Reporter")
    assert generate_role_summary(PermissionSet(reporter)).startswith("Role: Reporter")
    denied = PermissionSet(reporter, owner_review_required=True, review_reason="unknown_preset")
    assert generate_role_summary(denied) == "Role: blocked | Owner review required"
    assert generate_role_summary(["board:read"]) == "Role: unknown"


@pytest.mark.asyncio
async def test_profile_projects_native_flags_without_stringifying_permission_set(monkeypatch):
    import json
    from okto_pulse.core.mcp import server
    flags = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Reporter")
    agent = SimpleNamespace(
        id="a", name="Agent", description=None, objective=None, is_active=True,
        permissions=PermissionSet(flags), created_at=datetime.now(timezone.utc),
        last_used_at=None,
    )
    monkeypatch.setattr(server, "_get_authenticated_agent", AsyncMock(return_value=agent))
    payload = json.loads(await server.okto_pulse_get_my_profile.fn())
    assert payload["permissions"] == flags
    assert payload["role_summary"].startswith("Role: Reporter")
