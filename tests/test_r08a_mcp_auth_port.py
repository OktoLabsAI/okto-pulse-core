"""R08-A (CORE target) — MCP Auth port + shims + gates.

Scenarios covered here (core-target):

  ts_5cdbfe80 — the port imports in isolation (subprocess): no FastAPI /
                SQLAlchemy / AgentService / Agent / Request / server.py /
                community pulled into sys.modules.
  ts_c8e09c8d — the MCP server preserves the 3 transports (query param >
                X-API-Key > Authorization Bearer), the legacy auth helpers, and
                the request-scope credential; the conversion shims preserve the
                same precedence + source tagging.
  ts_2411ba5b — the credential-usage AST gate blocks NEW direct uses of the raw
                credential symbols outside the allowlisted shim/definition.
  ts_3ce862a7 — Agent contracts are intact: AgentResponse is secret-free,
                AgentRevealResponse reveals once, the Agent model keeps api_key
                + api_key_hash, hash_api_key is unchanged SHA-256.
  ts_178da21e — the R08-A diff introduces NO SaaS redesign (no CredentialStore /
                JWT / realm / scope / OAuth / password symbols) and removes
                nothing from the Agent credential contract.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import okto_pulse.core.ports.mcp_auth as _port_mod
from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
    run_mcp_credential_usage_gate,
)
from okto_pulse.core.ports import (
    AgentAuthSession,
    AuthSession,
    McpAuthenticator,
    McpCredential,
    mcp_credential_from_sources,
)

CORE_SRC = Path(_port_mod.__file__).parents[4]  # .../okto_labs_pulse_core/src
PORT_PY = Path(_port_mod.__file__)
GATE_PY = (
    Path(_port_mod.__file__).parents[1]
    / "application" / "boundary" / "mcp_credential_usage_gate.py"
)


# ===========================================================================
# ts_5cdbfe80 — the contract imports in isolation.
# ===========================================================================
def test_ts_5cdbfe80_port_imports_in_isolation(tmp_path):
    code = (
        "import sys\n"
        "from okto_pulse.core.ports import (McpAuthenticator, McpCredential, "
        "AgentAuthSession, AuthSession, mcp_credential_from_sources)\n"
        "import okto_pulse.core.ports.mcp_auth as m\n"
        "forbidden_prefixes = (\n"
        "  'fastapi', 'starlette', 'sqlalchemy',\n"
        "  'okto_pulse.core.mcp.server', 'okto_pulse.core.services',\n"
        "  'okto_pulse.core.models', 'okto_pulse.community',\n"
        ")\n"
        "leaked = [n for n in sys.modules if any(\n"
        "  n == p or n.startswith(p + '.') for p in forbidden_prefixes)]\n"
        "assert not leaked, 'port leaked heavy imports: ' + repr(leaked)\n"
        "assert McpCredential is m.McpCredential\n"
        "print('ISOLATION_OK')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(CORE_SRC)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=90,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "ISOLATION_OK" in proc.stdout


# ===========================================================================
# ts_c8e09c8d — server preserves 3 transports + legacy helpers + request scope.
# ===========================================================================
def _scope(query=b"", headers=None):
    return {
        "type": "http", "method": "GET", "path": "/mcp",
        "query_string": query, "headers": headers or [],
    }


def _run_middleware(scope):
    from okto_pulse.core.mcp.server import (
        ApiKeySessionMiddleware,
        request_scope_mcp_credential,
    )

    captured = {}

    async def _app(_scope, _receive, _send):
        captured["credential"] = request_scope_mcp_credential(_scope)

    async def _receive():
        return {"type": "http.request"}

    async def _send(_message):
        return None

    asyncio.run(ApiKeySessionMiddleware(_app)(scope, _receive, _send))
    return captured["credential"]


def test_ts_c8e09c8d_three_transports_and_request_scope():
    # query param
    cred = _run_middleware(_scope(query=b"api_key=QKEY"))
    assert cred.source == "query_param" and cred.value == "QKEY"
    # X-API-Key header
    cred = _run_middleware(_scope(headers=[(b"x-api-key", b"HKEY")]))
    assert cred.source == "x_api_key_header" and cred.value == "HKEY"
    # Authorization: Bearer
    cred = _run_middleware(
        _scope(headers=[(b"authorization", b"Bearer BKEY")])
    )
    assert cred.source == "authorization_bearer" and cred.value == "BKEY"
    # precedence: query > x-api-key > bearer
    cred = _run_middleware(
        _scope(
            query=b"api_key=QWINS",
            headers=[(b"x-api-key", b"H"), (b"authorization", b"Bearer B")],
        )
    )
    assert cred.source == "query_param" and cred.value == "QWINS"


def test_ts_c8e09c8d_conversion_shim_preserves_precedence_and_source():
    # query param wins, tagged source
    cred = mcp_credential_from_sources(
        query_param="Q", x_api_key_header="H", authorization_header="Bearer B"
    )
    assert cred.source == "query_param" and cred.value == "Q"
    # x-api-key next
    cred = mcp_credential_from_sources(
        query_param=None, x_api_key_header="H", authorization_header="Bearer B"
    )
    assert cred.source == "x_api_key_header" and cred.value == "H"
    # bearer last (prefix stripped)
    cred = mcp_credential_from_sources(
        query_param=None, x_api_key_header=None, authorization_header="Bearer B"
    )
    assert cred.source == "authorization_bearer" and cred.value == "B"
    # nothing -> None (fail-closed)
    assert mcp_credential_from_sources(
        query_param=None, x_api_key_header=None, authorization_header=None
    ) is None


def test_ts_c8e09c8d_request_shim_and_legacy_helpers_present():
    from okto_pulse.core.mcp import server as srv

    # New shims exist and preserve precedence from a Request-like object.
    class _Req:
        def __init__(self, query, headers):
            self.query_params = query
            self.headers = headers

    cred = srv.extract_mcp_credential_from_request(
        _Req({"api_key": "Q"}, {"x-api-key": "H", "authorization": "Bearer B"})
    )
    assert cred.source == "query_param" and cred.value == "Q"

    # Facade helpers are preserved; their credential source is request-scoped.
    assert callable(srv._get_authenticated_agent)
    assert callable(srv._get_agent_ctx)
    assert callable(srv.active_api_key_credential)
    assert callable(srv.request_scope_mcp_credential)


# ===========================================================================
# ts_2411ba5b — AST gate blocks NEW direct raw-credential usage.
# ===========================================================================
def test_ts_2411ba5b_real_core_has_no_new_credential_violations():
    report = run_mcp_credential_usage_gate()
    assert report.ok, f"unexpected credential-usage violations: {report.violations}"
    # The only files that reference the sensitive symbols are the allowlisted
    # shim + the canonical service definition.
    files = {f.file for f in report.findings}
    assert files <= set(report.allowlist), f"unexpected referencing files: {files}"
    assert "okto_pulse/core/mcp/server.py" in files  # the shim still uses them


def test_ts_2411ba5b_new_direct_use_is_blocked(tmp_path):
    rogue = tmp_path / "okto_pulse" / "core" / "services" / "rogue.py"
    rogue.parent.mkdir(parents=True, exist_ok=True)
    rogue.write_text(
        "def f(service):\n"
        "    from okto_pulse.core.mcp.server import _active_api_key\n"
        "    key = _active_api_key.get()\n"
        "    return service.get_agent_by_key(key)\n",
        encoding="utf-8",
    )
    report = run_mcp_credential_usage_gate(source_root=tmp_path)
    assert report.ok is False
    rogue_syms = {
        f.symbol for f in report.violations
        if f.file == "okto_pulse/core/services/rogue.py"
    }
    assert rogue_syms == {"_active_api_key", "get_agent_by_key"}


# ===========================================================================
# ts_3ce862a7 — Agent / AgentResponse credential contracts intact.
# ===========================================================================
def test_ts_3ce862a7_agent_contracts_unchanged():
    from okto_pulse.core.models.schemas import AgentResponse, AgentRevealResponse

    fields = set(AgentResponse.model_fields)
    assert "api_key" not in fields  # list/get responses never reveal the key
    assert "is_active" in fields
    assert "last_used_at" in fields
    assert "api_key_hash" not in fields  # response never leaks the stored hash
    assert "reveal_once_secret" in AgentRevealResponse.model_fields

    from okto_pulse.core.models.db import Agent

    cols = {c.name for c in Agent.__table__.columns}
    assert {"api_key", "api_key_hash"} <= cols  # transitional columns stay

    from okto_pulse.core.services.main import AgentService

    # hash_api_key is unchanged SHA-256; get_agent_by_key signature preserved.
    assert AgentService.hash_api_key("abc") == hashlib.sha256(b"abc").hexdigest()
    assert callable(AgentService.get_agent_by_key)


# ===========================================================================
# ts_178da21e — no SaaS redesign in the R08-A diff.
# ===========================================================================
_FORBIDDEN_SAAS_TOKENS = (
    "credentialstore", "jwt", "realm", "scope", "oauth", "password", "bcrypt",
)


def _defined_names(py_path: Path) -> set[str]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    return {
        node.name.lower()
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_ts_178da21e_port_and_gate_introduce_no_saas_symbols():
    for py in (PORT_PY, GATE_PY):
        names = _defined_names(py)
        for token in _FORBIDDEN_SAAS_TOKENS:
            assert not any(token in name for name in names), (
                f"{py.name} defines a forbidden SaaS-redesign symbol "
                f"matching {token!r}: {sorted(names)}"
            )
    # And nothing imports a JWT/OAuth/crypto-password backend.
    port_src = PORT_PY.read_text(encoding="utf-8").lower()
    for mod in ("import jwt", "import bcrypt", "import passlib", "oauthlib"):
        assert mod not in port_src


def test_ts_178da21e_agent_credential_contract_not_removed():
    from okto_pulse.core.models.db import Agent

    cols = {c.name for c in Agent.__table__.columns}
    # register-before-remove: the api_key / api_key_hash credential columns stay.
    assert {"api_key", "api_key_hash"} <= cols


# ===========================================================================
# Port DTO sanity (canonical shapes used by the adapter conformance test).
# ===========================================================================
def test_port_dtos_and_protocol_shapes():
    cred = McpCredential(source="query_param", value="super-secret-key")
    # value is encapsulated: repr must NOT leak the raw secret.
    assert "super-secret-key" not in repr(cred)
    assert "redacted" in repr(cred)

    session = AgentAuthSession(agent_id="a1", agent_name="Agent", is_active=True)
    assert isinstance(session, AuthSession)  # runtime_checkable Protocol shape
    assert "api_key" not in {f for f in session.metadata}

    # A minimal duck-typed authenticator satisfies the Protocol.
    class _Dummy:
        async def authenticate(self, credential):
            return None

    assert isinstance(_Dummy(), McpAuthenticator)
