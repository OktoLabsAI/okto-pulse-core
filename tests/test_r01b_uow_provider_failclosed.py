"""R01B REPLAN-IMP2 (FR3 / AC4) — fail-closed when no relational UnitOfWorkFactory
provider is registered.

Codex constraint (msg_984b302e): the process-level seam
(:mod:`okto_pulse.core.runtime_registry`) is an EXPLICIT, resettable registration
with NO default that constructs a core ``SQLAlchemyUnitOfWork``/``Factory`` in
production. This proves the inbound REST (``get_unit_of_work``) + MCP
(``get_unit_of_work_factory_for_mcp``) paths RAISE — never silently fall back —
when neither ``app.state.runtime_composition`` nor the process seam has a
provider, and that an explicit per-app composition provider still wins over the
seam (``preferred``).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.runtime_registry import (
    is_unit_of_work_factory_registered,
    reset_unit_of_work_factory,
    resolve_unit_of_work_factory,
)


@pytest.mark.asyncio
async def test_rest_get_unit_of_work_fails_closed_without_provider():
    # Empty the seam (the conftest autouse provider) and build a request whose app
    # has NO runtime_composition → both resolution sources are absent.
    reset_unit_of_work_factory()
    assert not is_unit_of_work_factory_registered()
    fake_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    dependency = get_unit_of_work(request=fake_request)
    try:
        with pytest.raises(HTTPException) as exc_info:
            await anext(dependency)
    finally:
        await dependency.aclose()
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["code"] == "persistence_provider_not_configured"
    assert "No relational UnitOfWorkFactory" in exc_info.value.detail["message"]


def test_mcp_factory_fails_closed_without_provider():
    # MCP has no request/app.state — it resolves only the process seam.
    reset_unit_of_work_factory()
    assert not is_unit_of_work_factory_registered()
    with pytest.raises(RuntimeError, match="No relational UnitOfWorkFactory"):
        mcp_server.get_unit_of_work_factory_for_mcp()


def test_resolve_prefers_composition_provider_over_empty_seam():
    # Even with an empty seam, an explicit per-app composition provider resolves
    # (REST app.state path) — proving ``preferred`` wins and the seam is a
    # fallback source, not the only one. Still NO core concrete is constructed.
    reset_unit_of_work_factory()
    assert not is_unit_of_work_factory_registered()
    sentinel = object()
    assert resolve_unit_of_work_factory(preferred=sentinel) is sentinel
