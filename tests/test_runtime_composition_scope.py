"""F04 application-scoped RuntimeComposition isolation tests."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from okto_pulse.core.composition import (
    RuntimeComposition,
    current_runtime_composition,
    isolated_runtime_provider_scope,
    reset_runtime_bridge_usage_for_tests,
    runtime_bridge_usage_snapshot,
    runtime_composition_scope,
)
from okto_pulse.core.infra.auth import get_auth_provider
from okto_pulse.core.infra.config import (
    CoreSettings,
    configure_settings,
    get_settings,
)
from okto_pulse.core.infra.storage import get_storage_provider
from okto_pulse.core.runtime_context import (
    RuntimeValueRegistry,
    capture_runtime_values_for_tests,
    register_runtime_value,
    resolve_runtime_value,
    restore_runtime_values_for_tests,
    runtime_lock,
    runtime_value_scope,
)
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory


def _composition(name: str) -> RuntimeComposition:
    return RuntimeComposition(
        settings_provider=f"settings:{name}",
        auth_provider=f"auth:{name}",
        storage_provider=f"storage:{name}",
        event_bus=f"events:{name}",
        uow_factory=f"uow:{name}",
    )


def test_composition_is_frozen_and_normalizes_hooks_to_tuple() -> None:
    composition = RuntimeComposition(
        settings_provider=object(),
        auth_provider=object(),
        storage_provider=object(),
        event_bus=object(),
        uow_factory=object(),
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


def test_process_default_runtime_lock_is_shared_across_raw_threads() -> None:
    """Raw worker threads must not manufacture independent process locks."""

    lock = runtime_lock("tests.raw_thread_exclusion")
    owner_entered = threading.Event()
    release_owner = threading.Event()
    contender_outcomes: list[bool] = []

    def owner() -> None:
        register_runtime_value("tests.raw_thread.owner", object())
        with lock:
            owner_entered.set()
            assert release_owner.wait(timeout=5)

    def contender() -> None:
        register_runtime_value("tests.raw_thread.contender", object())
        assert owner_entered.wait(timeout=5)
        acquired = lock.acquire(blocking=False)
        contender_outcomes.append(acquired)
        if acquired:
            lock.release()

    owner_thread = threading.Thread(target=owner)
    contender_thread = threading.Thread(target=contender)
    owner_thread.start()
    contender_thread.start()
    contender_thread.join(timeout=5)
    release_owner.set()
    owner_thread.join(timeout=5)

    assert not owner_thread.is_alive()
    assert not contender_thread.is_alive()
    assert contender_outcomes == [False]


def test_explicit_runtime_scopes_keep_independent_locks() -> None:
    lock = runtime_lock("tests.explicit_runtime_isolation")
    first = RuntimeValueRegistry()
    second = RuntimeValueRegistry()

    with runtime_value_scope(first):
        first_lock = lock.bind()
    with runtime_value_scope(second):
        second_lock = lock.bind()

    assert first_lock is not second_lock


def test_runtime_registry_copy_detaches_nested_mutable_state() -> None:
    class FirstHandler:
        pass

    class SecondHandler:
        pass

    key = "runtime.state.events.registry"
    original = RuntimeValueRegistry({key: {"event.created": [FirstHandler]}})
    copied = original.copy()

    copied.resolve(key)["event.created"].append(SecondHandler)

    assert original.resolve(key) == {"event.created": [FirstHandler]}
    assert copied.resolve(key) == {
        "event.created": [FirstHandler, SecondHandler],
    }


def test_isolated_provider_scope_clones_and_restores_on_failure() -> None:
    class CloneableProvider:
        def __init__(self, name: str):
            self.name = name

        def clone_for_runtime(self):
            return CloneableProvider(f"{self.name}:clone")

    baseline = capture_runtime_values_for_tests()
    original = CloneableProvider("normal")
    try:
        register_runtime_value("test.provider", original)

        with pytest.raises(RuntimeError, match="bounded operation failed"):
            with isolated_runtime_provider_scope():
                cloned = resolve_runtime_value("test.provider")
                assert cloned is not original
                assert cloned.name == "normal:clone"
                register_runtime_value("test.scope_only", object())
                raise RuntimeError("bounded operation failed")

        assert resolve_runtime_value("test.provider") is original
        assert resolve_runtime_value("test.scope_only") is None
    finally:
        restore_runtime_values_for_tests(baseline)


def test_concurrent_scopes_resolve_independent_providers_without_fallbacks() -> None:
    async def resolve(name: str):
        composition = _composition(name)
        with runtime_composition_scope(composition):
            await asyncio.sleep(0)
            return (
                get_settings(),
                get_auth_provider(),
                get_storage_provider(),
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
        "uow:a",
    )
    assert second[:-1] == (
        "settings:b",
        "auth:b",
        "storage:b",
        "uow:b",
    )
    assert first[-1] is not second[-1]
    assert runtime_bridge_usage_snapshot() == {}


def test_composed_mutable_settings_provider_replaces_only_its_snapshot() -> None:
    class MutableSettingsProvider:
        def __init__(self, settings: CoreSettings) -> None:
            self.settings = settings

        def get_settings_snapshot(self) -> CoreSettings:
            return self.settings

        def replace_settings_snapshot(self, settings: CoreSettings) -> None:
            self.settings = settings

    first_provider = MutableSettingsProvider(CoreSettings(app_name="first-before"))
    second_provider = MutableSettingsProvider(CoreSettings(app_name="second"))
    first = RuntimeComposition(
        settings_provider=first_provider,
        auth_provider=object(),
        storage_provider=object(),
        event_bus=object(),
        uow_factory=object(),
    )
    second = RuntimeComposition(
        settings_provider=second_provider,
        auth_provider=object(),
        storage_provider=object(),
        event_bus=object(),
        uow_factory=object(),
    )

    with runtime_composition_scope(first):
        assert get_settings().app_name == "first-before"
        configure_settings(CoreSettings(app_name="first-after"))
        assert get_settings().app_name == "first-after"

    with runtime_composition_scope(second):
        assert get_settings().app_name == "second"


def test_runtime_compositions_do_not_share_mcp_catalog_mutations() -> None:
    from okto_pulse.core.mcp.server import mcp

    first = _composition("mcp-first")
    second = _composition("mcp-second")

    with runtime_composition_scope(first):
        first_catalog = mcp.resolve()
        first_tools = set(first_catalog._tool_manager._tools)
        assert first_tools
        assert getattr(first_catalog.tool, "_xml_safety_patched", False) is True

        async def runtime_composition_xml_probe(value: str) -> str:
            return value

        registered = first_catalog.tool()(runtime_composition_xml_probe)
        assert getattr(registered.fn, "_xml_safety_wrapped", False) is True
        first_catalog.instructions = "first-only"

    with runtime_composition_scope(second):
        second_catalog = mcp.resolve()
        assert set(second_catalog._tool_manager._tools) == first_tools
        assert getattr(second_catalog.tool, "_xml_safety_patched", False) is True
        assert (
            "runtime_composition_xml_probe" not in second_catalog._tool_manager._tools
        )
        assert second_catalog.instructions != "first-only"

    assert first_catalog is not second_catalog
    assert first_catalog._tool_manager._tools is not second_catalog._tool_manager._tools


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
            event_bus=object(),
            uow_factory=object(),
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
