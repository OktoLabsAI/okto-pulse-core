"""Authenticated template lifecycle operations cannot bypass D17."""
# ruff: noqa: F811 -- pytest injects the imported fixture by argument name.
import pytest

from test_r01a_boards_uow import USER, client  # noqa: F401
from test_default_board_config_api import _isolate_committed_global_templates  # noqa: F401
from okto_pulse.community.api.auth_deps import require_principal
from okto_pulse.community.api.default_board_config import router
from okto_pulse.core.domain.permissions import PERMISSION_REGISTRY
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.authentication import Principal

BASE = "/api/v1/default-board-config"


def _agent(client):
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind="agent",
        claims={"permissions": PERMISSION_REGISTRY},
    )


@pytest.mark.parametrize("operation", ["create", "activate", "deactivate"])
def test_agent_cannot_change_effective_template_cognitive_policy(client, operation):
    client.app.include_router(router, prefix="/api/v1")
    baseline = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": True}, "activate": True,
    })
    assert baseline.status_code == 200, baseline.text
    target = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": False},
    })
    assert target.status_code == 200, target.text
    before = client.get(f"{BASE}/versions").json()
    _agent(client)
    if operation == "create":
        result = client.post(f"{BASE}/versions", json={
            "settings_payload": {"skip_cognitive_consolidation": False}, "activate": True,
        })
    else:
        identity = target.json()["id"] if operation == "activate" else baseline.json()["id"]
        result = client.post(f"{BASE}/versions/{identity}/{operation}")
    assert result.status_code == 403, result.text
    assert client.get(f"{BASE}/versions").json() == before


def test_agent_template_edit_preserves_omitted_human_cognitive_policy(client):
    client.app.include_router(router, prefix="/api/v1")
    baseline = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": True}, "activate": True,
    })
    assert baseline.status_code == 200, baseline.text
    _agent(client)
    result = client.post(f"{BASE}/versions", json={
        "settings_payload": {"max_scenarios_per_card": 4}, "activate": True,
    })
    assert result.status_code == 200, result.text
    assert result.json()["settings_payload"]["skip_cognitive_consolidation"] is True
    assert result.json()["settings_payload"]["max_scenarios_per_card"] == 4
    assert result.json()["id"] != baseline.json()["id"]


def test_authenticated_human_can_change_template_cognitive_policy(client):
    client.app.include_router(router, prefix="/api/v1")
    for skip in (True, False):
        result = client.post(f"{BASE}/versions", json={
            "settings_payload": {"skip_cognitive_consolidation": skip}, "activate": True,
        })
        assert result.status_code == 200, result.text
        assert result.json()["settings_payload"]["skip_cognitive_consolidation"] is skip


def test_agent_import_rejects_later_policy_change_before_any_version_is_created(client):
    client.app.include_router(router, prefix="/api/v1")
    baseline = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": True}, "activate": True,
    })
    assert baseline.status_code == 200, baseline.text
    before = client.get(f"{BASE}/versions").json()
    _agent(client)
    result = client.post(f"{BASE}/import", json={
        "schema_version": "1", "kind": "board_config", "items": [
            {"settings_payload": {"max_scenarios_per_card": 4}},
            {"settings_payload": {"skip_cognitive_consolidation": False}, "is_active": True},
        ],
    })
    assert result.status_code == 400, result.text
    detail = result.json()["detail"]
    assert detail["created"] == 0
    assert detail["errors"][0]["index"] == 1
    assert "human_control_required" in result.text
    assert client.get(f"{BASE}/versions").json() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "activate", "deactivate"])
async def test_mcp_cannot_change_template_cognitive_policy(client, operation):
    from test_default_board_config_api import _call, USER_ID
    from test_r01a_boards_uow import _seed_board

    client.app.include_router(router, prefix="/api/v1")
    baseline = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": True}, "activate": True,
    })
    assert baseline.status_code == 200, baseline.text
    target = client.post(f"{BASE}/versions", json={
        "settings_payload": {"skip_cognitive_consolidation": False},
    })
    assert target.status_code == 200, target.text
    before = client.get(f"{BASE}/versions").json()
    board_id = await _seed_board(owner=USER_ID)
    kwargs = {"board_id": board_id}
    if operation == "create":
        kwargs.update(settings_payload={"skip_cognitive_consolidation": False}, activate=True)
    else:
        kwargs["template_id"] = target.json()["id"] if operation == "activate" else baseline.json()["id"]
    result = await _call(f"okto_pulse_{operation}_default_board_config_version", **kwargs)
    assert result["code"] == "human_control_required", result
    assert client.get(f"{BASE}/versions").json() == before
