"""FCC-01C-A: /me and /presets REST adapters route through UnitOfWork."""

from __future__ import annotations

import inspect
import uuid
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import me as me_api
from okto_pulse.community.api import presets as presets_api
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.me import router as me_router
from okto_pulse.community.api.presets import router as presets_router
from okto_pulse.community.api.auth_deps import (
    get_realm_id,
    require_principal,
    require_user,
)
from okto_pulse.core.domain.permissions import get_builtin_presets
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.ports.authentication import Principal

USER = "fcc01c-user"
OTHER = "fcc01c-other"
PREFIX = "/api/v1"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(me_router, prefix=PREFIX)
    app.include_router(presets_router, prefix=f"{PREFIX}/presets")
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    full_control = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER,
        realm_id=LOCAL_REALM_ID,
        claims={"roles": ["admin"], "permissions": full_control},
        actor_kind="human",
    )
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


async def _seed_preset(
    *,
    owner_id: str | None,
    name: str,
    is_builtin: bool = False,
    flags: dict | None = None,
) -> str:
    from sqlalchemy_test_models import PermissionPreset

    preset_id = f"preset-fcc01c-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            PermissionPreset(
                id=preset_id,
                owner_id=owner_id,
                name=name,
                description=f"{name} description",
                is_builtin=is_builtin,
                flags=flags or {"board": {"read": True, "create": True}},
            )
        )
        await db.commit()
    return preset_id


async def _seed_agent_with_preset(*, preset_id: str, flags: dict | None = None) -> str:
    from sqlalchemy_test_models import Agent

    agent_id = f"agent-fcc01c-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Agent(
                id=agent_id,
                name="fcc01c agent",
                api_key=f"sha256:{uuid.uuid4().hex}",
                api_key_hash=f"hash-{uuid.uuid4().hex}",
                created_by=USER,
                preset_id=preset_id,
                permission_flags=flags,
            )
        )
        await db.commit()
    return agent_id


async def _preset_exists(preset_id: str) -> bool:
    from sqlalchemy_test_models import PermissionPreset

    async with get_session_factory()() as db:
        return await db.get(PermissionPreset, preset_id) is not None


def test_handlers_take_uow_and_do_not_bind_direct_database_session() -> None:
    handlers = {
        "get_my_permissions": me_api.get_my_permissions,
        "list_presets": presets_api.list_presets,
        "create_preset": presets_api.create_preset,
        "clone_preset": presets_api.clone_preset,
        "update_preset": presets_api.update_preset,
        "delete_preset": presets_api.delete_preset,
    }
    for name, handler in handlers.items():
        sig = inspect.signature(handler)
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name

    for module in (me_api, presets_api):
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert "AsyncSession" not in source
        assert "Depends(get_db)" not in source
        assert "okto_pulse.core.models.db" not in source
        assert "from okto_pulse.core.infra.database import get_db" not in source


@pytest.mark.asyncio
async def test_me_permissions_preserves_effective_permission_response(
    client: TestClient,
) -> None:
    from sqlalchemy_test_models import Board

    async with get_session_factory()() as db:
        db.add(Board(id="board-fcc01c", name="FCC01C", owner_id=USER))
        await db.commit()

    preset_id = await _seed_preset(
        owner_id=None,
        name="FCC01C Preset",
        is_builtin=True,
        flags={"board": {"read": True, "create": False}},
    )
    await _seed_agent_with_preset(preset_id=preset_id, flags=None)

    resp = client.get(f"{PREFIX}/me/permissions", params={"board_id": "board-fcc01c"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["board_id"] == "board-fcc01c"
    assert body["preset_name"] == "FCC01C Preset"
    assert body["flags"]["board"]["read"] is True
    assert body["flags"]["board"]["create"] is False


@pytest.mark.asyncio
async def test_presets_crud_clone_and_error_contracts(client: TestClient) -> None:
    builtin_id = await _seed_preset(
        owner_id=None,
        name="FCC01C Builtin",
        is_builtin=True,
        flags={"board": {"read": True, "create": True}},
    )
    foreign_id = await _seed_preset(
        owner_id=OTHER,
        name="FCC01C Foreign",
        flags={"board": {"read": True}},
    )

    created = client.post(
        f"{PREFIX}/presets",
        json={
            "name": "FCC01C Custom",
            "description": "created",
            "flags": {"card": {"entity": {"create": True}}},
        },
    )
    assert created.status_code == 201, created.text
    created_id = created.json()["id"]
    assert created.json()["owner_id"] == USER

    listed = client.get(f"{PREFIX}/presets")
    assert listed.status_code == 200, listed.text
    listed_ids = {item["id"] for item in listed.json()}
    assert {builtin_id, created_id}.issubset(listed_ids)
    assert foreign_id not in listed_ids

    cloned = client.post(
        f"{PREFIX}/presets/{builtin_id}/clone",
        json={
            "name": "FCC01C Clone",
            "description": "",
            "flags": {"board": {"read": False}},
        },
    )
    assert cloned.status_code == 201, cloned.text
    cloned_body = cloned.json()
    assert cloned_body["base_preset_id"] == builtin_id
    assert cloned_body["description"] == "FCC01C Builtin description"
    assert cloned_body["flags"]["board"]["read"] is False
    assert cloned_body["flags"]["board"]["create"] is True

    updated = client.put(
        f"{PREFIX}/presets/{created_id}",
        json={
            "name": "FCC01C Renamed",
            "description": "updated",
            "flags": {"card": {"entity": {"create": False}}},
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["name"] == "FCC01C Renamed"
    assert updated.json()["description"] == "updated"
    assert updated.json()["flags"]["card"]["entity"]["create"] is False

    missing_clone = client.post(
        f"{PREFIX}/presets/missing-{uuid.uuid4().hex[:8]}/clone",
        json={"name": "x", "description": "", "flags": {}},
    )
    assert missing_clone.status_code == 404
    assert missing_clone.json()["detail"] == "Source preset not found"

    builtin_update = client.put(
        f"{PREFIX}/presets/{builtin_id}",
        json={"name": "blocked"},
    )
    assert builtin_update.status_code == 403
    assert (
        builtin_update.json()["detail"]
        == "Built-in presets cannot be modified or deleted"
    )

    foreign_update = client.put(
        f"{PREFIX}/presets/{foreign_id}",
        json={"name": "blocked"},
    )
    assert foreign_update.status_code == 403
    assert foreign_update.json()["detail"] == "You can only modify your own presets"

    foreign_delete = client.delete(f"{PREFIX}/presets/{foreign_id}")
    assert foreign_delete.status_code == 403
    assert foreign_delete.json()["detail"] == "You can only delete your own presets"

    deleted = client.delete(f"{PREFIX}/presets/{created_id}")
    assert deleted.status_code == 204, deleted.text
    assert await _preset_exists(created_id) is False

    missing_delete = client.delete(f"{PREFIX}/presets/{created_id}")
    assert missing_delete.status_code == 404
    assert missing_delete.json()["detail"] == "Preset not found"
