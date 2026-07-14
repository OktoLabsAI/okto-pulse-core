"""S03 authentication-port boundary and REST parity coverage."""

from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
import pytest

from okto_pulse.community.api.auth_deps import get_current_user, get_realm_id, require_user
from okto_pulse.core.application.use_cases.base import actor_context_from_principal
from okto_pulse.core.infra.auth import (
    configure_auth,
    get_auth_provider,
    reset_auth_for_tests,
)
from okto_pulse.core.ports.authentication import (
    AuthorizationDenied,
    Credential,
    Principal,
)
from okto_pulse.core.ports.mcp_auth import AgentAuthSession, principal_from_auth_session


class _AuthPort:
    async def authenticate(self, credential: Credential | None) -> Principal | None:
        if credential is None:
            return Principal("local-user", claims={"roles": ["admin"]})
        if credential.value == "invalid":
            return None
        if credential.value == "denied":
            raise AuthorizationDenied()
        return Principal(
            "saas-user",
            realm_id="tenant-01",
            claims={"roles": ["member"], "email": "saas@example.test"},
        )


def _restore_auth_provider(previous: object | None) -> None:
    if previous is None:
        reset_auth_for_tests()
    else:
        configure_auth(previous)  # type: ignore[arg-type]


def test_pure_auth_contract_imports_without_transport_or_orm() -> None:
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    script = """
import sys
from okto_pulse.core.ports.authentication import Credential, Principal
assert Credential('secret').value == 'secret'
assert Principal('user').subject == 'user'
blocked = ('fastapi', 'starlette', 'sqlalchemy', 'okto_pulse.community')
assert not any(name == item or name.startswith(item + '.') for name in sys.modules for item in blocked)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_auth_registration_seam_has_no_fastapi_ast_import() -> None:
    root = Path(__file__).resolve().parents[1]
    for relative_path in (
        "src/okto_pulse/core/infra/auth.py",
        "src/okto_pulse/core/ports/authentication.py",
    ):
        tree = ast.parse((root / relative_path).read_text(encoding="utf-8"))
        imported_roots = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        assert not {"fastapi", "starlette", "sqlalchemy"}.intersection(imported_roots)


def test_rest_adapter_preserves_local_and_saas_auth_http_contracts() -> None:
    try:
        previous: object | None = get_auth_provider()
    except RuntimeError:
        previous = None

    app = FastAPI()

    @app.get("/identity")
    async def identity(
        user_id: str = Depends(require_user),
        user: dict[str, object] = Depends(get_current_user),
        realm_id: str | None = Depends(get_realm_id),
    ) -> dict[str, object]:
        return {"user_id": user_id, "user": user, "realm_id": realm_id}

    try:
        configure_auth(_AuthPort())
        client = TestClient(app)

        local = client.get("/identity")
        assert local.status_code == 200
        assert local.json()["user_id"] == "local-user"
        assert local.json()["user"]["roles"] == ["admin"]

        saas = client.get("/identity", headers={"Authorization": "Bearer valid"})
        assert saas.status_code == 200
        assert saas.json()["user_id"] == "saas-user"
        assert saas.json()["realm_id"] == "tenant-01"
        assert saas.json()["user"]["email"] == "saas@example.test"

        invalid = client.get("/identity", headers={"Authorization": "Bearer invalid"})
        assert invalid.status_code == 401
        assert invalid.headers["www-authenticate"] == "Bearer"

        denied = client.get("/identity", headers={"Authorization": "Bearer denied"})
        assert denied.status_code == 403
    finally:
        _restore_auth_provider(previous)


def test_fastapi_dependencies_exist_only_on_the_community_inbound_adapter() -> None:
    import okto_pulse.core.infra.auth as core_auth

    assert not hasattr(core_auth, "require_user")
    assert require_user.__module__ == "okto_pulse.community.api.auth_deps"


def test_rest_and_mcp_principals_feed_the_same_actor_policy_contract() -> None:
    rest_actor = actor_context_from_principal(
        Principal(
            "user-01",
            realm_id="tenant-01",
            claims={"name": "User", "roles": ["admin"], "permissions": {"x": True}},
        ),
        source="rest",
        board_id="board-01",
    )
    mcp_principal = principal_from_auth_session(
        AgentAuthSession(
            agent_id="agent-01",
            agent_name="Agent",
            is_active=True,
            metadata={"realm_id": "local"},
        )
    )
    assert mcp_principal is not None
    mcp_actor = actor_context_from_principal(
        mcp_principal,
        source="mcp",
        board_id="board-01",
    )

    assert (rest_actor.actor_id, rest_actor.realm_id, rest_actor.roles) == (
        "user-01",
        "tenant-01",
        ("admin",),
    )
    assert (mcp_actor.actor_id, mcp_actor.actor_name, mcp_actor.source) == (
        "agent-01",
        "Agent",
        "mcp",
    )


@pytest.mark.asyncio
async def test_board_access_denial_is_forbidden_without_a_mutation(monkeypatch) -> None:
    from fastapi import HTTPException

    from okto_pulse.community.api.kg_routes import _ensure_board_access

    class _ReadOnlyBoardService:
        calls = 0

        async def get_board(self, *args, **kwargs):
            self.calls += 1
            return None

    service = _ReadOnlyBoardService()

    class _Services:
        boards = service

    class _UnitOfWork:
        services = _Services()

    actor = actor_context_from_principal(
        Principal("saas-user", realm_id="tenant-01"),
        source="rest",
        board_id="board-01",
    )

    with pytest.raises(HTTPException) as raised:
        await _ensure_board_access(board_id="board-01", actor=actor, uow=_UnitOfWork())

    assert raised.value.status_code == 403
    assert service.calls == 1
