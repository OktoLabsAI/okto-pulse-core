"""Spec #03 (card 2fb92914) — RuntimeComposition + PulseRuntime + create_app strict.

Locks the composition contract, the strict_runtime fail-fast in create_app
(``runtime_provider_missing``, no silent fallback) and backward compatibility.
The CommunityLifespanReplay + CompositionBoundaryGate are a separate card
(b4f07ad3); the deeper register-before-remove migration of the live lifespan is
out of this card's scope.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from okto_pulse.core.composition import (
    ALL_PROVIDER_KEYS,
    REQUIRED_OWNED_PROVIDERS,
    STRICT_RUNTIME_REQUIRED_PROVIDERS,
    CompositionRuntime,
    InvalidRuntimeComposition,
    PulseRuntime,
    RuntimeComposition,
    RuntimeProviderMissing,
    validate_required_providers,
)


def _full_composition(**overrides) -> RuntimeComposition:
    base = dict(
        settings_provider=object(),
        auth_provider=object(),
        storage_provider=object(),
        session_factory=object(),
        event_bus=object(),
    )
    base.update(overrides)
    return RuntimeComposition(**base)


class _NoopHook:
    async def on_startup(self) -> None:
        return None

    async def on_shutdown(self) -> None:
        return None


def test_required_owned_providers_set() -> None:
    assert set(REQUIRED_OWNED_PROVIDERS) == {
        "settings_provider", "auth_provider", "storage_provider",
        "session_factory", "event_bus",
    }
    # kg_registry is deferred to #05, NOT required here
    assert "kg_registry" not in REQUIRED_OWNED_PROVIDERS
    assert "kg_registry" in ALL_PROVIDER_KEYS
    # R08B: lifecycle_hooks is optional in the historical default path, but
    # explicit strict-runtime shell mode treats an empty tuple as missing.
    assert "lifecycle_hooks" not in REQUIRED_OWNED_PROVIDERS
    assert set(STRICT_RUNTIME_REQUIRED_PROVIDERS) == {
        *REQUIRED_OWNED_PROVIDERS,
        "lifecycle_hooks",
    }


def test_full_composition_has_no_missing_required() -> None:
    assert _full_composition().missing_required() == []
    validate_required_providers(_full_composition())  # no raise


def test_missing_required_provider_is_reported() -> None:
    comp = _full_composition(event_bus=None)
    assert comp.missing_required() == ["event_bus"]
    with pytest.raises(RuntimeProviderMissing) as exc:
        validate_required_providers(comp)
    assert exc.value.code == "runtime_provider_missing"
    assert "event_bus" in exc.value.missing


def test_strict_runtime_empty_lifecycle_hooks_are_missing() -> None:
    comp = _full_composition()
    with pytest.raises(RuntimeProviderMissing) as exc:
        validate_required_providers(comp, require_lifecycle_hooks=True)
    assert exc.value.provider_key == "lifecycle_hooks"
    assert exc.value.missing == ["lifecycle_hooks"]

    validate_required_providers(
        _full_composition(lifecycle_hooks=(_NoopHook(),)),
        require_lifecycle_hooks=True,
    )


def test_require_provider_returns_or_raises() -> None:
    comp = _full_composition(scheduler_control=None)
    assert comp.require_provider("event_bus") is comp.event_bus
    # optional provider that is None -> runtime_provider_missing on explicit require
    with pytest.raises(RuntimeProviderMissing):
        comp.require_provider("scheduler_control")
    # unknown key -> invalid composition
    with pytest.raises(InvalidRuntimeComposition):
        comp.require_provider("nope")


def test_kg_registry_deferred_not_required() -> None:
    # kg_registry absent must NOT make the composition invalid (deferred_to_05)
    comp = _full_composition()
    assert comp.kg_registry is None
    assert comp.missing_required() == []


def test_create_app_strict_runtime_without_composition_fails_fast() -> None:
    from okto_pulse.core.app import create_app

    with pytest.raises(RuntimeProviderMissing) as exc:
        create_app(object(), object(), object(), strict_runtime=True)
    assert exc.value.code == "runtime_provider_missing"
    assert "lifecycle_hooks" in exc.value.missing


def test_create_app_strict_runtime_with_incomplete_composition_fails_fast() -> None:
    from okto_pulse.core.app import create_app

    incomplete = _full_composition(session_factory=None)
    with pytest.raises(RuntimeProviderMissing) as exc:
        create_app(object(), object(), object(), strict_runtime=True, composition=incomplete)
    assert "session_factory" in exc.value.missing


def test_create_app_strict_runtime_empty_lifecycle_hooks_fails_before_database(
    monkeypatch,
) -> None:
    from okto_pulse.core import app as app_mod

    calls: list[str] = []
    monkeypatch.setattr(
        app_mod,
        "create_database",
        lambda *args, **kwargs: calls.append("create_database"),
    )

    with pytest.raises(RuntimeProviderMissing) as exc:
        app_mod.create_app(
            object(),
            object(),
            object(),
            strict_runtime=True,
            composition=_full_composition(),
        )

    assert exc.value.missing == ["lifecycle_hooks"]
    assert calls == []


def test_create_app_runtime_shell_only_skips_database_when_explicit(monkeypatch) -> None:
    from okto_pulse.core import app as app_mod
    from okto_pulse.core.infra.config import configure_settings, get_settings

    calls: list[str] = []
    monkeypatch.setattr(
        app_mod,
        "create_database",
        lambda *args, **kwargs: calls.append("create_database"),
    )
    settings = SimpleNamespace(
        app_name="Shell Smoke",
        app_version="test",
        database_url="sqlite+aiosqlite:///should-not-open.db",
        debug=False,
    )
    composition = _full_composition(lifecycle_hooks=(_NoopHook(),))

    original_settings = get_settings()
    try:
        app = app_mod.create_app(
            settings,
            object(),
            object(),
            strict_runtime=True,
            runtime_shell_only=True,
            composition=composition,
        )
    finally:
        configure_settings(original_settings)

    assert calls == []
    assert app.state.runtime_composition is composition


def test_create_app_runtime_shell_only_lifespan_avoids_default_side_effects(
    monkeypatch,
    tmp_path,
) -> None:
    from okto_pulse.core import app as app_mod
    from okto_pulse.core.infra.config import configure_settings, get_settings

    events: list[str] = []
    side_effects: list[str] = []

    class _RecordingHook:
        async def on_startup(self) -> None:
            events.append("shell_startup")

        async def on_shutdown(self) -> None:
            events.append("shell_shutdown")

    async def _forbidden_init_db() -> None:
        side_effects.append("init_db")
        raise AssertionError("runtime_shell_only must not enter default lifespan")

    monkeypatch.setattr(
        app_mod,
        "create_database",
        lambda *args, **kwargs: side_effects.append("create_database"),
    )
    monkeypatch.setattr(app_mod, "init_db", _forbidden_init_db)

    db_path = tmp_path / "pulse.db"
    settings = SimpleNamespace(
        app_name="Shell Smoke",
        app_version="test",
        database_url=f"sqlite+aiosqlite:///{db_path.as_posix()}",
        debug=False,
    )
    composition = _full_composition(lifecycle_hooks=(_RecordingHook(),))

    original_settings = get_settings()
    try:
        app = app_mod.create_app(
            settings,
            object(),
            object(),
            strict_runtime=True,
            runtime_shell_only=True,
            composition=composition,
        )
        with TestClient(app) as client:
            assert client.get("/health").status_code == 200
    finally:
        configure_settings(original_settings)

    assert events == ["shell_startup", "shell_shutdown"]
    assert side_effects == []
    assert not db_path.exists()


def test_pulse_runtime_protocol_conformance() -> None:
    runtime = CompositionRuntime(_full_composition(lifecycle_hooks=(_NoopHook(),)))
    assert isinstance(runtime, PulseRuntime)
    assert runtime.require_provider("auth_provider") is runtime.composition.auth_provider


def test_composition_runtime_strict_rejects_incomplete() -> None:
    with pytest.raises(RuntimeProviderMissing):
        CompositionRuntime(_full_composition(storage_provider=None), strict_runtime=True)


@pytest.mark.asyncio
async def test_composition_runtime_runs_lifecycle_hooks_in_order() -> None:
    events: list[str] = []

    class _Hook:
        def __init__(self, name: str) -> None:
            self.name = name

        async def on_startup(self) -> None:
            events.append(f"start:{self.name}")

        async def on_shutdown(self) -> None:
            events.append(f"stop:{self.name}")

    comp = _full_composition(lifecycle_hooks=(_Hook("a"), _Hook("b")))
    runtime = CompositionRuntime(comp)
    async with runtime.app_lifespan():
        assert events == ["start:a", "start:b"]
    # shutdown runs hooks in reverse
    assert events == ["start:a", "start:b", "stop:b", "stop:a"]


def test_composition_module_is_composition_layer() -> None:
    from okto_pulse.core.application.boundary import ImportBoundaryGate, ImportBoundaryGateInput
    from okto_pulse.core.application.boundary.layer_resolver import LayerResolver

    assert LayerResolver().resolve("okto_pulse/core/composition.py").layer == "composition"
    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="blocking"))
    assert "okto_pulse/core/composition.py" not in report.evidence["unclassified_layers"]
