"""Public composition scope contains no database mechanism or process-global override."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from okto_pulse.core.ports.relational_runtime import (
    configure_database_runtime, database_runtime_scope, resolve_database_runtime,
)


class Runtime:
    @asynccontextmanager
    async def transactional_session(self):
        yield self

    @asynccontextmanager
    async def cancel_safe_session_scope(self, session_factory=None):
        yield self


@pytest.fixture(autouse=True)
def restore_registered_runtime():
    original = resolve_database_runtime()
    try:
        yield
    finally:
        configure_database_runtime(runtime=original)


def test_nested_scope_and_failure_restore_the_original_runtime():
    original, outer, inner = Runtime(), Runtime(), Runtime()
    configure_database_runtime(runtime=original)
    with pytest.raises(ValueError, match="injected"):
        with database_runtime_scope(runtime=outer):
            assert resolve_database_runtime() is outer
            with database_runtime_scope(runtime=inner):
                assert resolve_database_runtime() is inner
            assert resolve_database_runtime() is outer
            raise ValueError("injected")
    assert resolve_database_runtime() is original


def test_invalid_runtime_does_not_replace_the_enclosing_binding():
    original = Runtime()
    configure_database_runtime(runtime=original)
    with pytest.raises(TypeError):
        with database_runtime_scope(runtime=object()):
            pytest.fail("invalid runtime accepted")
    assert resolve_database_runtime() is original


@pytest.mark.asyncio
async def test_parallel_contexts_keep_independent_runtime_bindings():
    original = Runtime()
    configure_database_runtime(runtime=original)
    first_ready, second_ready = asyncio.Event(), asyncio.Event()

    async def worker(ready, other):
        runtime = Runtime()
        with database_runtime_scope(runtime=runtime):
            ready.set()
            await other.wait()
            assert resolve_database_runtime() is runtime
        assert resolve_database_runtime() is original

    await asyncio.gather(worker(first_ready, second_ready), worker(second_ready, first_ready))
    assert resolve_database_runtime() is original
