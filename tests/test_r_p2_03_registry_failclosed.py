"""R-P2-03A-D — the KG registry is FAIL-CLOSED on the Onda A slots.

cache_backend (03A) / rate_limiter (03B) / session_store (03C) / config (03D) are
NEVER built implicitly for real runtime: ``get_kg_registry`` raises until the
composition configures it, and ``configure_kg_registry`` has no implicit
``_build_defaults`` fallback. A real runtime composes the slots explicitly (the
Community ``base_registry`` adapters); tests use the sanctioned ``defaults_factory``
route (``tests.kg_registry_testing.configure_test_kg_registry``).

This suite proves the fail-closed contract + audits that NO core production module
imports the test-only ``_build_defaults`` fake builder (codex's pre-commit
requirement: no bootstrap/runtime real uses ``defaults_factory=_build_defaults``).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse"
# Onda A slots NOT carried by the composed-path graph defaults — these are the
# ones the composed base must supply (or stay None). ``config`` (03D) is special:
# R-P2-03D made it a REQUIRED composition-supplied slot — it is NO LONGER filled
# by ``_build_graph_defaults`` (the R05-D fallback was retired), and
# ``configure_kg_registry`` FAILS CLOSED when it is absent. So config is covered by
# the dedicated config tests below, not the cache/rate/session "not implicitly
# defaulted" parametrize.
_FULLY_CLOSED_ONDA_A = ("cache_backend", "rate_limiter", "session_store")


# --- AUDIT: no production code uses the test-only fake builder ----------------
def test_no_core_production_imports_build_defaults() -> None:
    """``_build_defaults`` is the TEST-ONLY fake route (reached via
    ``defaults_factory``). No core production module may import it — real runtime
    composes through a ``base_registry``. (The ``interfaces/registry`` module
    DEFINES it; it never imports it.)"""
    offenders: list[str] = []
    for py in _SRC_ROOT.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "_build_defaults" for alias in node.names
            ):
                offenders.append(str(py.relative_to(_SRC_ROOT)))
    assert offenders == [], (
        f"production code imports the test-only _build_defaults fake route: {offenders}"
    )


# --- FAIL-CLOSED: get / configure without explicit composition ---------------
def test_get_kg_registry_fails_closed_without_composition() -> None:
    reset_registry_for_tests()
    try:
        with pytest.raises(RuntimeError, match="not configured"):
            get_kg_registry()
    finally:
        configure_test_kg_registry()


def test_configure_without_base_or_factory_fails_closed() -> None:
    reset_registry_for_tests()
    try:
        with pytest.raises(RuntimeError, match="requires an explicit"):
            configure_kg_registry(session_factory=object())
    finally:
        configure_test_kg_registry()


# --- PER-SLOT (03A/03B/03C): the slot is not implicitly defaulted -------------
@pytest.mark.parametrize("slot", _FULLY_CLOSED_ONDA_A)
def test_onda_a_slot_not_implicitly_defaulted_in_composed_path(slot: str) -> None:
    """A composition supplying a base WITHOUT the slot leaves it ``None`` — the
    core never silently fills cache/rate_limiter/session_store with an embedded
    Onda A default. Consuming the ``None`` slot is then a fail-closed concern of
    the caller, not a silent embedded-default substitution.

    ``config`` IS supplied here because it is a REQUIRED slot (03D) — otherwise
    ``configure_kg_registry`` fails closed before we can observe the slot. R-P2-02
    also requires event_bus/audit_repo, so this test supplies opaque fakes for the
    data ports while proving cache/rate/session are not silently defaulted."""
    from okto_pulse.core.kg.providers.embedded.settings_config import SettingsKGConfig
    from okto_pulse.core.kg.providers.testing.memory_audit_repo import (
        InMemoryAuditRepository,
    )
    from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

    reset_registry_for_tests()
    try:
        configure_kg_registry(
            base_registry=KGProviderRegistry(
                config=SettingsKGConfig(),
                event_bus=InMemoryEventBus(),
                audit_repo=InMemoryAuditRepository(),
            )
        )
        assert getattr(get_kg_registry(), slot) is None, (
            f"{slot} was implicitly defaulted by the core in the composed path"
        )
    finally:
        configure_test_kg_registry()


@pytest.mark.parametrize("missing_slot", ("config", "event_bus", "audit_repo"))
def test_required_data_slots_fail_closed_when_composition_omits_one(
    missing_slot: str,
) -> None:
    """R-P2-03D: the core no longer fills ``config`` (KGConfig) with an embedded
    ``SettingsKGConfig`` (the R05-D ledgered fallback is retired). A composition
    that omits ``config`` fails closed at ``configure_kg_registry`` with an
    actionable error — never a late ``AttributeError`` when a consumer reads
    ``registry.config``. R-P2-02 applies the same fail-closed contract to the
    relational data ports: event_bus and audit_repo."""
    from okto_pulse.core.kg.providers.embedded.settings_config import SettingsKGConfig
    from okto_pulse.core.kg.providers.testing.memory_audit_repo import (
        InMemoryAuditRepository,
    )
    from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

    providers = {
        "config": SettingsKGConfig(),
        "event_bus": InMemoryEventBus(),
        "audit_repo": InMemoryAuditRepository(),
    }
    providers[missing_slot] = None

    reset_registry_for_tests()
    try:
        with pytest.raises(RuntimeError, match=missing_slot):
            configure_kg_registry(base_registry=KGProviderRegistry(**providers))
    finally:
        configure_test_kg_registry()


def test_explicit_fake_composition_supplies_every_onda_a_slot() -> None:
    """The sanctioned test route DOES supply every Onda A slot (so the suite runs
    on real fakes, not on ``None`` holes)."""
    reset_registry_for_tests()
    try:
        configure_test_kg_registry()
        reg = get_kg_registry()
        for slot in (*_FULLY_CLOSED_ONDA_A, "config"):
            assert getattr(reg, slot) is not None, f"fake route left {slot} unset"
    finally:
        configure_test_kg_registry()
