"""R-P2-03 — test-only KG registry configuration helper.

The KG registry no longer lazy-builds implicit Onda A defaults (R-P2-03
fail-closed): ``get_kg_registry`` raises until the composition configures it, and
``configure_kg_registry`` has no implicit ``_build_defaults`` fallback. Tests
configure the embedded fakes EXPLICITLY through this helper — the SANCTIONED
test/fake route (``defaults_factory=_build_defaults``). Real runtime must supply a
Community ``base_registry`` instead; this module lives under ``tests/`` and is
NEVER imported by production code (audited by the R-P2-03 conformance test).
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.kg.interfaces.registry import (
    _build_defaults,
    configure_kg_registry,
)


def configure_test_kg_registry(**overrides: Any) -> None:
    """Configure the KG registry with the embedded fakes (TEST-ONLY).

    A thin, EXPLICIT wrapper over
    ``configure_kg_registry(defaults_factory=_build_defaults, **overrides)`` so the
    test intent ("running on the embedded fakes") is literal and greppable. Pass
    provider overrides / ``session_factory`` exactly as you would to
    ``configure_kg_registry``.
    """
    configure_kg_registry(defaults_factory=_build_defaults, **overrides)
