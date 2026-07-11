"""F04 application-scoped RuntimeComposition isolation tests."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from okto_pulse.core.composition import (
    RuntimeComposition,
    current_runtime_composition,
    reset_runtime_bridge_usage_for_tests,
    runtime_bridge_usage_snapshot,
    runtime_composition_scope,
)
from okto_pulse.core.infra.auth import get_auth_provider
from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.infra.database import get_engine, get_session_factory
from okto_pulse.core.infra.storage import get_storage_provider
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory


def _composition(name: str) -> RuntimeComposition:
    return RuntimeComposition(
        settings_provider=f"settings:{name}",
        auth_provider=f"auth:{name}",
        storage_provider=f"storage:{name}",
        session_factory=f"session:{name}",
        event_bus=f"events:{name}",
        uow_factory=f"uow:{name}",
        relational_engine=f"engine:{name}",
    )


def test_composition_is_frozen_and_normalizes_hooks_to_tuple() -> None:
    composition = RuntimeComposition(
        settings_provider=object(),
        auth_provider=object(),
        storage_provider=object(),
        session_factory=object(),
        event_bus=object(),
        lifecycle_hooks=[],
    )
    assert composition.lifecycle_hooks == ()
    with pytest.raises(FrozenInstanceError):
        composition.event_bus = object()  # type: ignore[misc]


def test_nested_scope_restores_the_previous_composition() -> None:
    first = _composition("first")
    second = _composition("second")
    assert current_runtime_composition() is None
    with runtime_composition_scope(first):
        assert current_runtime_composition() is first
        with runtime_composition_scope(second):
            assert current_runtime_composition() is second
        assert current_runtime_composition() is first
    assert current_runtime_composition() is None


def test_concurrent_scopes_resolve_independent_providers_without_fallbacks() -> None:
    async def resolve(name: str):
        composition = _composition(name)
        with runtime_composition_scope(composition):
            await asyncio.sleep(0)
            return (
                get_settings(),
                get_auth_provider(),
                get_storage_provider(),
                get_session_factory(),
                get_engine(),
                resolve_unit_of_work_factory(),
                current_runtime_composition(),
            )

    async def drive():
        return await asyncio.gather(resolve("a"), resolve("b"))

    reset_runtime_bridge_usage_for_tests()
    first, second = asyncio.run(drive())

    assert first[:-1] == (
        "settings:a",
        "auth:a",
        "storage:a",
        "session:a",
        "engine:a",
        "uow:a",
    )
    assert second[:-1] == (
        "settings:b",
        "auth:b",
        "storage:b",
        "session:b",
        "engine:b",
        "uow:b",
    )
    assert first[-1] is not second[-1]
    assert runtime_bridge_usage_snapshot() == {}


def test_two_apps_resolve_their_own_composition_after_last_global_write() -> None:
    from okto_pulse.community.app import create_app

    class Hook:
        async def on_startup(self) -> None:
            return None

        async def on_shutdown(self) -> None:
            return None

    def make_app(name: str):
        settings = SimpleNamespace(
            app_name=f"App {name}",
            app_version="test",
            tag=name,
        )
        auth = object()
        storage = object()
        composition = RuntimeComposition(
            settings_provider=settings,
            auth_provider=auth,
            storage_provider=storage,
            session_factory=object(),
            event_bus=object(),
            lifecycle_hooks=(Hook(),),
        )
        app = create_app(
            settings,
            auth,
            storage,
            composition=composition,
            strict_runtime=True,
            runtime_shell_only=True,
        )

        @app.get("/composition-probe")
        def composition_probe():
            return {"tag": get_settings().tag}

        return app

    app_a = make_app("a")
    app_b = make_app("b")
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        assert client_a.get("/composition-probe").json() == {"tag": "a"}
        assert client_b.get("/composition-probe").json() == {"tag": "b"}
        assert client_a.get("/composition-probe").json() == {"tag": "a"}
