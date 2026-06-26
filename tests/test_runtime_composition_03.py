"""Spec #03 (card 2fb92914) — RuntimeComposition + PulseRuntime + create_app strict.

Locks the composition contract, the strict_runtime fail-fast in create_app
(``runtime_provider_missing``, no silent fallback) and backward compatibility.
The CommunityLifespanReplay + CompositionBoundaryGate are a separate card
(b4f07ad3); the deeper register-before-remove migration of the live lifespan is
out of this card's scope.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.composition import (
    ALL_PROVIDER_KEYS,
    REQUIRED_OWNED_PROVIDERS,
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


def test_required_owned_providers_set() -> None:
    assert set(REQUIRED_OWNED_PROVIDERS) == {
        "settings_provider", "auth_provider", "storage_provider",
        "session_factory", "event_bus",
    }
    # kg_registry is deferred to #05, NOT required here
    assert "kg_registry" not in REQUIRED_OWNED_PROVIDERS
    assert "kg_registry" in ALL_PROVIDER_KEYS


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


def test_create_app_strict_runtime_with_incomplete_composition_fails_fast() -> None:
    from okto_pulse.core.app import create_app

    incomplete = _full_composition(session_factory=None)
    with pytest.raises(RuntimeProviderMissing) as exc:
        create_app(object(), object(), object(), strict_runtime=True, composition=incomplete)
    assert "session_factory" in exc.value.missing


def test_pulse_runtime_protocol_conformance() -> None:
    runtime = CompositionRuntime(_full_composition())
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
