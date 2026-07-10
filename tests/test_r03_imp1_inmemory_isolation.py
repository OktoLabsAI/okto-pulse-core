"""R03 REPLAN-IMP1 — isolate the in-memory cache/rate/session as ports.

Teeth for:
  - FR2/AC2 (ts_6c5055c9): the duplicate ``tier_power._rate_limiter`` /
    ``_TokenBucket`` module-global is removed from BOTH the code and the
    AntiSingletonGate ledger. Non-vacuous: each "dirty" half fails a DISTINCT
    assertion — left-in-ledger fails ``..._removed_from_baseline_ledger``;
    left-in-code is detected as a NEW (now-un-baselined) singleton and fails
    ``..._removed_from_code``.
  - AC3 (ts_d0afe904 / ts_ac6759d6 / ts_a6e70932): the cache / rate / session
    PORTS fail closed with a structured ``runtime_provider_missing`` when the slot
    was never composed — never a late ``AttributeError`` on ``None`` nor a silent
    concrete fallback. (The configure path is already fail-closed via R-P2-03.)
    The same guarantee holds at the DIRECT runtime call-sites that read the
    registry slots — ``kg_service._cached_call`` and the consolidation primitives
    (``_require_open_session``, ``begin_consolidation`` and the commit/abort/
    invalidation paths) — not only the cache/session facade ports. (This is the
    gap Codex caught at the IMP1 gate: those paths raised ``AttributeError`` on
    ``None`` instead of the structured error.)
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.application.boundary.singleton_gate import (
    AntiSingletonGate,
    AntiSingletonGateInput,
    BASELINE_SINGLETONS,
)
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.interfaces.registry import (
    get_kg_registry,
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry

TIER_POWER_RATE_KEY = "okto_pulse/core/kg/tier_power.py::_rate_limiter"
TIER_POWER_REL = "okto_pulse/core/kg/tier_power.py"


# --------------------------------------------------------------------- FR2/AC2
def test_rate_limiter_singleton_removed_from_baseline_ledger():
    """Dirty variant 'left in the ledger' fails HERE."""
    assert TIER_POWER_RATE_KEY not in BASELINE_SINGLETONS


def test_rate_limiter_singleton_removed_from_code():
    """Dirty variant 'left in the code' fails HERE: a lingering module-global in
    tier_power.py would be detected as a NEW (un-baselined — we removed the
    baseline entry) singleton, so the gate blocks and lists it."""
    report = AntiSingletonGate().run(AntiSingletonGateInput(only_files=(TIER_POWER_REL,)))
    assert report.status == "baseline", report.evidence
    assert report.evidence["new_singletons"] == [], report.evidence


def test_token_bucket_semantics_preserved_in_port_adapter():
    """FR2 'sem alterar a semantica de token bucket': the canonical concrete keeps
    30 tokens / 60s and the same allow()/reset() contract the removed duplicate
    had."""
    from okto_pulse.core.kg.providers.testing.memory import (
        InMemoryTokenBucket,
    )

    tb = InMemoryTokenBucket()
    assert tb._rate == 30 and tb._window == 60.0
    for _ in range(30):
        allowed, retry = tb.allow("agent")
        assert allowed and retry == 0
    blocked, retry = tb.allow("agent")
    assert not blocked and retry >= 1
    tb.reset("agent")
    allowed, retry = tb.allow("agent")
    assert allowed and retry == 0


# ------------------------------------------------------------------------- AC3
def _configure():
    reset_registry_for_tests()
    configure_test_kg_registry(graph_provider="inmemory")


def test_registry_require_accessors_return_composed_providers():
    _configure()
    try:
        reg = get_kg_registry()
        assert reg.require_cache_backend() is reg.cache_backend
        assert reg.require_rate_limiter() is reg.rate_limiter
        assert reg.require_session_store() is reg.session_store
    finally:
        reset_registry_for_tests()


@pytest.mark.parametrize("slot", ["cache_backend", "rate_limiter", "session_store"])
def test_registry_require_accessor_fail_closed_when_slot_absent(slot):
    _configure()
    try:
        reg = get_kg_registry()
        setattr(reg, slot, None)
        require = getattr(reg, f"require_{slot}")
        with pytest.raises(RuntimeProviderMissing) as exc:
            require()
        assert exc.value.provider_key == slot
        assert exc.value.code == "runtime_provider_missing"
    finally:
        reset_registry_for_tests()


def test_cache_porta_fail_closed_when_backend_absent():  # ts_d0afe904
    _configure()
    try:
        get_kg_registry().cache_backend = None
        from okto_pulse.core.kg.cache import cache_get

        with pytest.raises(RuntimeProviderMissing) as exc:
            cache_get("kg_health", "board-1", {})
        assert exc.value.provider_key == "cache_backend"
    finally:
        reset_registry_for_tests()


def test_rate_porta_fail_closed_when_limiter_absent():  # ts_ac6759d6
    _configure()
    try:
        get_kg_registry().rate_limiter = None
        from okto_pulse.core.kg.tier_power import check_rate_limit

        with pytest.raises(RuntimeProviderMissing) as exc:
            check_rate_limit("agent-1")
        assert exc.value.provider_key == "rate_limiter"
    finally:
        reset_registry_for_tests()


def test_session_porta_fail_closed_when_store_absent():  # ts_a6e70932
    _configure()
    try:
        get_kg_registry().session_store = None
        from okto_pulse.core.kg.session_manager import (
            get_session_manager,
            reset_session_manager_for_tests,
        )

        reset_session_manager_for_tests()
        with pytest.raises(RuntimeProviderMissing) as exc:
            get_session_manager()._store()
        assert exc.value.provider_key == "session_store"
    finally:
        reset_registry_for_tests()


# ------------------------------------------- AC3: DIRECT runtime call-sites
# Beyond the facade ports, the KGService cache helper and the consolidation
# primitives read the registry slots DIRECTLY. Those REAL runtime paths must
# also fail closed with the structured error — this is exactly the gap Codex
# rejected at the IMP1 gate (val_c443596e): ``_cached_call`` and
# ``_require_open_session`` raised ``AttributeError: 'NoneType'`` instead.


def test_kg_service_cached_call_fail_closed_when_cache_absent():  # ts_d0afe904
    """``KGService()._cached_call`` is THE cache read-path; with no cache backend
    composed it must raise ``runtime_provider_missing`` before touching it (and
    before calling ``fn``)."""
    _configure()
    try:
        get_kg_registry().cache_backend = None
        from okto_pulse.core.kg.kg_service import KGService

        with pytest.raises(RuntimeProviderMissing) as exc:
            KGService()._cached_call(
                "kg_health", "board-1", {}, lambda: [], use_cache=True
            )
        assert exc.value.provider_key == "cache_backend"
    finally:
        reset_registry_for_tests()


def test_require_open_session_fail_closed_when_store_absent():  # ts_a6e70932
    """``_require_open_session`` is the shared session gate of add_node /
    add_edge / propose / commit / abort; with no session store composed it must
    fail closed at the source instead of dereferencing ``None``."""
    import asyncio

    _configure()
    try:
        get_kg_registry().session_store = None
        from okto_pulse.core.kg.primitives import _require_open_session

        with pytest.raises(RuntimeProviderMissing) as exc:
            asyncio.run(_require_open_session("kgses_x", "agent-1"))
        assert exc.value.provider_key == "session_store"
    finally:
        reset_registry_for_tests()


def test_begin_consolidation_fail_closed_when_store_absent():  # ts_a6e70932
    """``begin_consolidation`` reads the session store DIRECTLY (it does not pass
    through ``_require_open_session``), so it needs its own fail-closed proof."""
    import asyncio

    from okto_pulse.core.kg.schemas import BeginConsolidationRequest

    _configure()
    try:
        get_kg_registry().session_store = None
        from okto_pulse.core.kg.primitives import begin_consolidation

        req = BeginConsolidationRequest(
            board_id="board-1",
            artifact_type="spec",
            artifact_id="artifact-1",
            raw_content="content",
        )
        with pytest.raises(RuntimeProviderMissing) as exc:
            asyncio.run(begin_consolidation(req, agent_id="agent-1"))
        assert exc.value.provider_key == "session_store"
    finally:
        reset_registry_for_tests()
