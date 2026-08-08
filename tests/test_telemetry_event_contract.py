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
from repository_checkout_testing import community_source_for  # noqa: E402

CORE_SRC = ROOT / "src" / "okto_pulse" / "core"
COMMUNITY_SRC = community_source_for(ROOT)


def _core_src(rel: str) -> str:
    return (CORE_SRC / rel).read_text(encoding="utf-8")


def _community_src(rel: str) -> str:
    return (COMMUNITY_SRC / rel).read_text(encoding="utf-8")


# --- ts_d0621cf9: the live contract covers exactly the declared types ---------

def test_contract_covers_exactly_the_declared_event_types() -> None:
    assert set(TELEMETRY_EVENT_CONTRACT) == set(TELEMETRY_EVENT_TYPES)


def test_live_contract_has_no_violations() -> None:
    assert contract_violations() == []
    assert_contract_complete()  # must not raise


# --- wired types are REAL: emitter present + aggregate live (no ghost) --------

def test_wired_types_have_real_emitter_and_live_aggregate() -> None:
    app_src = _community_src("app.py")
    metrics_src = _community_src("api/metrics.py")
    # R10-E Pass 2: TelemetryBeaconSender moved to Community. Read the community
    # sender source (sibling repo) to verify aggregates are still materialised.
    community_sender = COMMUNITY_SRC / "adapters" / "telemetry_sender.py"
    sender_src = community_sender.read_text(encoding="utf-8")
    for entry in TELEMETRY_EVENT_CONTRACT.values():
        if entry.coverage != COVERAGE_WIRED:
            continue
        # a wired aggregate must really be one the sender materialises
        assert entry.aggregate in LIVE_AGGREGATE_MAPS
        assert entry.aggregate in sender_src, f"{entry.aggregate} not produced by community telemetry_sender.py"
    # the wired emitters exist in code: http via the app middleware, guided_help via
    # the generic event endpoint, and cli/mcp/kg/lifecycle/pipeline via the R5A-B
    # runtime emitter helpers.
    assert 'record_event("http"' in app_src
    assert "record_event(" in metrics_src
    emitters_src = _core_src("telemetry/emitters.py")
    for fn in (
        "def emit_cli_event",
        "def emit_mcp_event",
        "def emit_kg_event",
        "def emit_lifecycle_event",
        "def emit_pipeline_transition_event",
    ):
        assert fn in emitters_src, f"missing real emitter {fn}"
    # every live aggregate name is in the community sender (no ghost in the set)
    for aggregate in LIVE_AGGREGATE_MAPS:
        assert aggregate in sender_src, f"LIVE_AGGREGATE_MAPS lists {aggregate} but community telemetry_sender.py never emits it"


# --- ts_e8f6c83d: after R5A-B every maintained type is wired (no pending) ------

def test_no_maintained_type_is_pending_after_r5a_b() -> None:
    # R5A-B closes the pending wiring: every maintained type must be wired with a
    # real emitter + live aggregate, so the contract test FAILS if any maintained
    # type still carries pending:R5A-B.
    for event_type, entry in TELEMETRY_EVENT_CONTRACT.items():
        if entry.status != "maintained":
            continue
        assert not is_pending(entry.coverage), f"{event_type} is still pending after R5A-B"
        assert entry.coverage == COVERAGE_WIRED


def test_lifecycle_and_pipeline_now_have_live_aggregates() -> None:
    # R5A-B added the dedicated maps that used to be missing (the closed gap).
    # R10-E Pass 2: sender moved to Community — verify in the community adapter.
    community_sender = COMMUNITY_SRC / "adapters" / "telemetry_sender.py"
    sender_src = community_sender.read_text(encoding="utf-8")
    for aggregate in ("lifecycle_counts", "pipeline_transition_counts"):
        assert aggregate in LIVE_AGGREGATE_MAPS
        assert aggregate in sender_src  # the community sender materialises them


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
