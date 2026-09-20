"""Composition seam retained while internal recovery consumers are retired."""

from types import SimpleNamespace

import pytest


def test_public_composition_facade_resolves_only_recovery_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import interfaces as kg_interfaces
    from okto_pulse.core.ports import global_discovery_recovery_control as facade

    recovery = object()
    artifact_store = object()
    registry = SimpleNamespace(
        require_global_discovery_recovery=lambda: recovery,
        require_rebuild_audit_artifact_store=lambda: artifact_store,
    )
    monkeypatch.setattr(kg_interfaces, "get_kg_registry", lambda: registry)

    assert facade.resolve_global_discovery_recovery_runtime_dependencies() == (
        recovery,
        artifact_store,
    )
