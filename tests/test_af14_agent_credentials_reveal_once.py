from __future__ import annotations

import textwrap

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.agents import router as agents_router
from okto_pulse.core.application.boundary.agent_secret_surface_gate import (
    KIND_PLAINTEXT_AGENT_PERSISTENCE,
    KIND_PERSISTED_SECRET_FIELD,
    KIND_RESPONSE_SECRET_FIELD,
    run_agent_secret_surface_gate,
)
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.schemas import AgentCreate, AgentResponse, AgentRevealResponse
from okto_pulse.core.ports.mcp_auth import McpCredential, mcp_credential_from_sources
from okto_pulse.core.services import AgentService

USER = "af14-user"


class _HarnessMcpAuthenticator:
    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def authenticate(self, credential):
        if credential is None:
            return None
        async with self._session_factory() as db:
            from okto_pulse.core.services.application_agents import (
                authenticate_agent_by_api_key,
            )

            return await authenticate_agent_by_api_key(
                db,
                credential.value,
                credential_source=credential.source,
            )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(agents_router, prefix="/api/v1/agents")
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


def test_af14_agent_response_is_secret_free_and_reveal_response_is_scoped():
    assert "api_key" not in AgentResponse.model_fields
    assert "api_key_hash" not in AgentResponse.model_fields
    assert set(AgentRevealResponse.model_fields) == {"agent", "reveal_once_secret", "message"}


@pytest.mark.asyncio
async def test_af14_service_persists_marker_and_authenticates_by_hash():
    async with get_session_factory()() as db:
        service = AgentService(db)
        agent, first_secret = await service.create_agent(USER, AgentCreate(name="af14-service"))
        await db.commit()

        assert first_secret.startswith("dash_")
        assert agent.api_key != first_secret
        assert agent.api_key.startswith("sha256:")
        assert agent.api_key_hash == AgentService.hash_api_key(first_secret)
        assert (await service.get_agent_by_key(first_secret)).id == agent.id

        _agent, second_secret = await service.regenerate_key(agent.id)
        await db.commit()

        assert second_secret.startswith("dash_")
        assert second_secret != first_secret
        assert await service.get_agent_by_key(first_secret) is None
        assert (await service.get_agent_by_key(second_secret)).id == agent.id
        assert agent.api_key != second_secret


@pytest.mark.asyncio
async def test_af14_mcp_query_header_and_bearer_authenticate_by_hash():
    from okto_pulse.core.mcp import server as mcp_server

    session_factory = get_session_factory()
    mcp_server.register_session_factory(
        session_factory,
        mcp_authenticator=_HarnessMcpAuthenticator(session_factory),
    )

    async with get_session_factory()() as db:
        service = AgentService(db)
        agent, secret = await service.create_agent(USER, AgentCreate(name="af14-mcp"))
        await db.commit()
        assert agent.api_key.startswith("sha256:")
        agent_id = agent.id

    credentials = [
        mcp_credential_from_sources(
            query_param=secret,
            x_api_key_header=None,
            authorization_header=None,
        ),
        mcp_credential_from_sources(
            query_param=None,
            x_api_key_header=secret,
            authorization_header=None,
        ),
        mcp_credential_from_sources(
            query_param=None,
            x_api_key_header=None,
            authorization_header=f"Bearer {secret}",
        ),
    ]
    assert [credential.source for credential in credentials if credential] == [
        "query_param",
        "x_api_key_header",
        "authorization_bearer",
    ]

    for credential in credentials:
        assert isinstance(credential, McpCredential)
        resolved = await mcp_server._authenticate_mcp_credential(credential)
        assert resolved is not None
        assert resolved.id == agent_id
        assert resolved.api_key != secret


def test_af14_rest_create_list_get_and_regenerate_are_secret_free_except_reveal_once():
    client = _client()

    created = client.post("/api/v1/agents", json={"name": "af14-rest"})
    assert created.status_code == 201, created.text
    body = created.json()
    agent_id = body["agent"]["id"]
    assert body["reveal_once_secret"].startswith("dash_")
    assert "api_key" not in body
    assert "api_key" not in body["agent"]

    listed = client.get("/api/v1/agents")
    assert listed.status_code == 200, listed.text
    assert any(a["id"] == agent_id for a in listed.json())
    assert all("api_key" not in a for a in listed.json())

    fetched = client.get(f"/api/v1/agents/{agent_id}")
    assert fetched.status_code == 200, fetched.text
    assert "api_key" not in fetched.json()

    rotated = client.post(f"/api/v1/agents/{agent_id}/regenerate-key")
    assert rotated.status_code == 200, rotated.text
    rotated_body = rotated.json()
    assert rotated_body["reveal_once_secret"].startswith("dash_")
    assert "api_key" not in rotated_body
    assert "api_key" not in rotated_body["agent"]


def test_af14_secret_surface_gate_green_on_real_core():
    report = run_agent_secret_surface_gate()
    assert report.ok, [o.as_dict() for o in report.occurrences]
    assert report.scanned_files == 4


def test_af14_secret_surface_gate_flags_response_and_plaintext_persistence(tmp_path):
    (tmp_path / "models").mkdir()
    (tmp_path / "api").mkdir()
    (tmp_path / "services").mkdir()
    (tmp_path / "models" / "schemas.py").write_text(
        textwrap.dedent(
            """
            class AgentResponse:
                api_key: str
            """
        ),
        encoding="utf-8",
    )
    (tmp_path / "models" / "db.py").write_text(
        textwrap.dedent(
            """
            class Agent:
                secret: str
            """
        ),
        encoding="utf-8",
    )
    (tmp_path / "api" / "agents.py").write_text("", encoding="utf-8")
    (tmp_path / "services" / "main.py").write_text(
        textwrap.dedent(
            """
            def create_agent(secret):
                return Agent(api_key=secret)
            """
        ),
        encoding="utf-8",
    )

    report = run_agent_secret_surface_gate(tmp_path)
    kinds = {o.kind for o in report.occurrences}
    assert KIND_RESPONSE_SECRET_FIELD in kinds
    assert KIND_PERSISTED_SECRET_FIELD in kinds
    assert KIND_PLAINTEXT_AGENT_PERSISTENCE in kinds
