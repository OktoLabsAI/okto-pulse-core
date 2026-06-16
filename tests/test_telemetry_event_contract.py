"""Contract test for the telemetry EventType registry (spec R5A, card R5A-A).

Enforces that every declared TELEMETRY_EVENT_TYPE is classified (maintained with a
producer + aggregate, or removed/deprecated with justification), so no declared
type becomes a phantom schema. Scenarios ts_d0621cf9 + ts_e8f6c83d; tr_8c6167d8 /
br_62b0f7cc / dec_b74b4d29 / ac_7fb151c8 / ac_66e29ff9.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.telemetry.event_contract import (  # noqa: E402
    COVERAGE_WIRED,
    LIVE_AGGREGATE_MAPS,
    TELEMETRY_EVENT_CONTRACT,
    EventTypeContract,
    _contract_violations,
    assert_contract_complete,
    contract_violations,
    is_pending,
)
from okto_pulse.core.telemetry.schema import TELEMETRY_EVENT_TYPES  # noqa: E402

SRC = ROOT / "src" / "okto_pulse" / "core"


def _src(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


# --- ts_d0621cf9: the live contract covers exactly the declared types ---------

def test_contract_covers_exactly_the_declared_event_types() -> None:
    assert set(TELEMETRY_EVENT_CONTRACT) == set(TELEMETRY_EVENT_TYPES)


def test_live_contract_has_no_violations() -> None:
    assert contract_violations() == []
    assert_contract_complete()  # must not raise


# --- wired types are REAL: emitter present + aggregate live (no ghost) --------

def test_wired_types_have_real_emitter_and_live_aggregate() -> None:
    app_src = _src("app.py")
    metrics_src = _src("api/metrics.py")
    sender_src = _src("telemetry/sender.py")
    for entry in TELEMETRY_EVENT_CONTRACT.values():
        if entry.coverage != COVERAGE_WIRED:
            continue
        # a wired aggregate must really be one the sender materialises
        assert entry.aggregate in LIVE_AGGREGATE_MAPS
        assert entry.aggregate in sender_src, f"{entry.aggregate} not produced by sender.py"
    # the two wired emitters exist in code today
    assert 'record_event("http"' in app_src  # http emitted by the app middleware
    assert "record_event(" in metrics_src     # guided_help via the generic event endpoint
    # every live aggregate name is actually a sender metrics key (no ghost in the set)
    for aggregate in LIVE_AGGREGATE_MAPS:
        assert aggregate in sender_src, f"LIVE_AGGREGATE_MAPS lists {aggregate} but sender.py never emits it"


# --- ts_e8f6c83d: pending wiring is EXPLICIT, never silent --------------------

def test_pending_types_are_explicitly_tracked_not_silent() -> None:
    for event_type in ("cli", "mcp", "kg", "lifecycle", "pipeline_transition"):
        entry = TELEMETRY_EVENT_CONTRACT[event_type]
        assert is_pending(entry.coverage), f"{event_type} pending wiring must be explicit"
        assert entry.producer and entry.aggregate  # the target is declared, not a hole


def test_lifecycle_and_pipeline_have_no_live_aggregate_yet() -> None:
    # the known R5A-B gap, made explicit by the contract: these declared types have
    # NO dedicated aggregate map today, so they are dropped from the delta batch.
    sender_src = _src("telemetry/sender.py")
    for aggregate in ("lifecycle_counts", "pipeline_transition_counts"):
        assert aggregate not in LIVE_AGGREGATE_MAPS
        assert aggregate not in sender_src  # not yet wired anywhere


# --- tr_8c6167d8: the contract test FAILS on each violation mode --------------

def test_fails_when_declared_type_has_no_contract_entry() -> None:
    problems = _contract_violations(
        TELEMETRY_EVENT_TYPES + ("ghost",), TELEMETRY_EVENT_CONTRACT, {}
    )
    assert any("ghost" in p and "phantom schema" in p for p in problems)


def test_fails_when_contract_entry_is_not_a_declared_type() -> None:
    contract = dict(TELEMETRY_EVENT_CONTRACT)
    contract["zombie"] = EventTypeContract("zombie", "maintained", "x", "cli_counts", COVERAGE_WIRED)
    problems = _contract_violations(TELEMETRY_EVENT_TYPES, contract, {})
    assert any("zombie" in p and "not a declared EventType" in p for p in problems)


def test_fails_when_maintained_type_lacks_emitter_or_aggregate() -> None:
    contract = {"cli": EventTypeContract("cli", "maintained", "", "", "pending:R5A-B")}
    problems = _contract_violations(("cli",), contract, {})
    assert any("no producer/emitter" in p for p in problems)
    assert any("no aggregate" in p for p in problems)


def test_fails_when_wired_aggregate_is_a_ghost() -> None:
    contract = {"cli": EventTypeContract("cli", "maintained", "p", "no_such_counts", COVERAGE_WIRED)}
    problems = _contract_violations(("cli",), contract, {})
    assert any("ghost aggregate" in p for p in problems)


def test_fails_when_removed_type_is_still_declared_or_unjustified() -> None:
    # a removed type that is still declared -> ghost schema reappeared
    problems = _contract_violations(("cli",), {"cli": TELEMETRY_EVENT_CONTRACT["cli"]}, {"cli": "x"})
    assert any("ghost schema reappeared" in p for p in problems)
    # a removed type with no justification -> flagged
    problems2 = _contract_violations(("cli",), {"cli": TELEMETRY_EVENT_CONTRACT["cli"]}, {"old_type": ""})
    assert any("no removal justification" in p for p in problems2)
