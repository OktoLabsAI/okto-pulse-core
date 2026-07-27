from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from okto_pulse.core.ports.relational_runtime import (
    RelationalRuntime,
    cancel_safe_session,
    configure_database_runtime,
    resolve_database_runtime,
)


class _MinimalRuntime:
    def __init__(self) -> None:
        self.transactional_value = object()
        self.cancel_safe_value = object()

    @asynccontextmanager
    async def transactional_session(self):
        yield self.transactional_value

    @asynccontextmanager
    async def cancel_safe_session_scope(self, session_factory=None):
        if session_factory is not None:
            yield session_factory()
            return
        yield self.cancel_safe_value


class _TransactionalOnlyRuntime:
    @asynccontextmanager
    async def transactional_session(self):
        yield object()


@pytest.mark.asyncio
async def test_minimal_runtime_exposes_required_cancel_safe_capability() -> None:
    previous = resolve_database_runtime()
    runtime = _MinimalRuntime()

    assert isinstance(runtime, RelationalRuntime)
    try:
        configure_database_runtime(runtime=runtime)
        async with cancel_safe_session() as session:
            assert session is runtime.cancel_safe_value
    finally:
        configure_database_runtime(runtime=previous)


def test_composition_rejects_runtime_without_cancel_safe_capability() -> None:
    runtime = _TransactionalOnlyRuntime()

    assert not isinstance(runtime, RelationalRuntime)
    with pytest.raises(TypeError, match="cancel_safe_session_scope"):
        configure_database_runtime(runtime=runtime)  # type: ignore[arg-type]
