"""F16 (spec 690416ab) — health-aware closeout gate.

F16: CognitiveCloseoutGate.evaluate() surfaces UNAVAILABLE on a degraded graph
(graph_state in _RISK_STATE_HARD_REJECT OR resolved_generation is None) while
staying sync/pure; the async service caller resolves graph_state via
get_kg_health and threads it in.
F17's manual tick entry points were retired in F4. Their absence is covered
through the registered Community REST and MCP transports.

Scenario -> test-card map:
  ts_abd2cb82, ts_7fce86cf  -> 599d6cea (AC1/AC2: degraded -> UNAVAILABLE)
  ts_b054e4f7, ts_01d256b9  -> 62902967 (AC3/AC6: null-gen UNAVAILABLE; skip wins)
  ts_4663630d, ts_6fa3e068  -> 84d02ee1 (AC4/AC5: healthy no-regression)
  ts_80500309, ts_dedefaf9  -> eb5bf3f3 (AC8/AC13: sync/no-IO + predicate identity)
  ts_327df96b               -> 3bd086d0 (AC14: degraded telemetry sample)
  ts_3eda0dc9               -> c8e10019 (AC7: async plumbing blocks before mutation)
"""

from __future__ import annotations

import inspect
import uuid
from types import SimpleNamespace

import pytest

from coordination_fakes import FakeLeaseProvider, FakeWriteLockPort
import okto_pulse.core.kg.cognitive_closeout_gate as gate_mod
import okto_pulse.core.services.kg_health_service as kg_health_service
import okto_pulse.core.services.main as services_main
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.kg.cognitive_closeout_gate import (
    CognitiveCloseoutGate,
    CognitiveCloseoutOutcome,
    CognitiveCloseoutReason,
    get_closeout_gate_samples,
    reset_closeout_gate_samples,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveItemStatus,
)
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.main import CardService
from okto_pulse.core.ports.coordination import (
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)


@pytest.fixture(autouse=True)
def _coordination_ports():
    reset_coordination_providers_for_tests()
    register_coordination_providers(
        lease_provider=FakeLeaseProvider(),
        write_lock_port=FakeWriteLockPort(),
    )
    yield
    reset_coordination_providers_for_tests()


# --------------------------------------------------------------------------- #
# F16 gate unit scaffolding
# --------------------------------------------------------------------------- #


def _item(source_ref: str, status: str) -> CognitiveConsolidationItem:
    return CognitiveConsolidationItem(
        item_id=f"item-{source_ref.replace(':', '-')}",
        board_id="board-1",
        kg_generation_id="kg-1",
        source_ref=source_ref,
        artifact_type=source_ref.split(":", 1)[0],
        status=status,
        recorded_at="2026-05-29T00:00:00Z",
    )


class _FakeStore:
    """latest_generation returns a non-None generation (healthy shape)."""

    def __init__(self, items: list[CognitiveConsolidationItem] | None = None):
        self.items = list(items or [])

    def latest_generation(self, board_id: str) -> str | None:
        return "kg-1"

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return list(self.items)


class _NoGenStore:
    """latest_generation returns None — the live recovery_needed shape (FR2)."""

    def latest_generation(self, board_id: str) -> str | None:
        return None

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return []


def _gate(store) -> CognitiveCloseoutGate:
    return CognitiveCloseoutGate(store=store)


# --------------------------------------------------------------------------- #
# AC1 / AC2 — 599d6cea (degraded graph_state surfaces UNAVAILABLE)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("degraded_state", ["recovery_needed", "quarantined"])
def test_degraded_graph_state_surfaces_unavailable(degraded_state):
    """ts_abd2cb82 (recovery_needed) + ts_7fce86cf (quarantined): NC-1 Degraded-KG
    Auto-Skip — a non-None graph_state in _RISK_STATE_HARD_REJECT (without
    board_skip) produces allowed=True + outcome=UNAVAILABLE + reason=
    DEGRADED_KG_AUTO_SKIP (auditable, not silent). The outcome value is reused
    from the existing UNAVAILABLE member; no new outcome enum member added (F16
    no-new-enum constraint)."""
    assert degraded_state in _RISK_STATE_HARD_REJECT
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state=degraded_state,
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert result.reason == CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value


# --------------------------------------------------------------------------- #
# AC3 / AC6 — 62902967 (null generation UNAVAILABLE; skip precedence)
# --------------------------------------------------------------------------- #


def test_ts_b054e4f7_null_generation_surfaces_unavailable():
    """No graph_state arg, latest_generation()->None, skip=false: the empty
    ledger is NOT mapped to ALLOWED — this is the exact 3cf5dede failure mode."""
    result = _gate(_NoGenStore()).evaluate(
        board_id="board-1",
        entity_type="spec",
        entity_id="spec-1",
        target_status="done",
    )
    assert result.allowed is False
    assert result.outcome == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value


def test_ts_01d256b9_skip_wins_over_degraded():
    """board_skip_enabled=True short-circuits the degraded liveness branch to a
    SKIPPED result (allowed=True), byte-identical to the exception-path skip."""
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        board_skip_enabled=True,
        graph_state="recovery_needed",
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.SKIPPED.value
    assert result.reason == CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value


# --------------------------------------------------------------------------- #
# AC4 / AC5 — 84d02ee1 (healthy graph no-regression)
# --------------------------------------------------------------------------- #


def test_ts_4663630d_healthy_empty_stays_allowed():
    """Non-None generation, empty active items, healthy graph_state: terminal
    ALLOWED branch preserved (the new trigger must NOT shadow it)."""
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="healthy",
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value


def test_non_hardreject_states_stay_allowed():
    """Boundary guard: at_risk / backpressure are DELIBERATELY not in the
    degraded predicate (_RISK_STATE_HARD_REJECT = {recovery_needed, quarantined},
    the single shared set per dec_e66161eb). A non-hard-reject graph_state with a
    real generation closes out normally — only the hard-reject states block. This
    pins the spec's hard-reject-only contract: a future re-broadening that blocks
    at_risk (which would re-break legitimate mid-SDLC done transitions) fails here.
    Extending the block to the at_risk-with-unreadable-telemetry shape is a
    deliberate non-goal of this spec / a potential follow-up."""
    for state in ("at_risk", "backpressure"):
        assert state not in _RISK_STATE_HARD_REJECT
        result = _gate(_FakeStore()).evaluate(
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            target_status="done",
            graph_state=state,
        )
        assert result.allowed is True
        assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value


def test_ts_6fa3e068_healthy_pending_stays_blocked():
    """Non-None generation + one PENDING item matching the entity, healthy
    graph_state: existing BLOCKED path unaffected by the new trigger."""
    store = _FakeStore([_item("task:card-1", CognitiveItemStatus.PENDING.value)])
    result = _gate(store).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="healthy",
    )
    assert result.allowed is False
    assert result.outcome == CognitiveCloseoutOutcome.BLOCKED.value
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_CONSOLIDATION_PENDING.value
    assert result.blocking_count == 1


# --------------------------------------------------------------------------- #
# AC14 — 3bd086d0 (degraded closeout emits an unavailable telemetry sample, TR6)
# --------------------------------------------------------------------------- #


def test_ts_327df96b_degraded_emits_unavailable_sample():
    """NC-1: degraded closeout now emits outcome=UNAVAILABLE + reason=
    DEGRADED_KG_AUTO_SKIP (the new type-safe reason member).  Outcome value
    reuses the existing UNAVAILABLE member — no new outcome enum (F16 constraint)."""
    reset_closeout_gate_samples()
    _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="recovery_needed",
    )
    sample = get_closeout_gate_samples()[-1]
    assert sample["outcome"] == CognitiveCloseoutOutcome.UNAVAILABLE.value
    assert sample["reason"] == CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value
    assert sample["skip_enabled"] == "false"


# --------------------------------------------------------------------------- #
# AC8 / AC13 — eb5bf3f3 (structural: gate sync/no-IO + single shared predicate)
# --------------------------------------------------------------------------- #


def test_ts_80500309_gate_stays_sync_no_io():
    assert not inspect.iscoroutinefunction(CognitiveCloseoutGate.evaluate)
    src = inspect.getsource(gate_mod)
    for forbidden in ("AsyncSession", "get_kg_health", "get_db", "open_board_connection"):
        assert forbidden not in src, (
            f"the closeout gate must stay sync/pure (no I/O) — found {forbidden!r}"
        )


def test_ts_dedefaf9_single_shared_predicate_identity():
    # The remaining cognitive gate reuses the canonical backpressure predicate.
    assert gate_mod._RISK_STATE_HARD_REJECT is _RISK_STATE_HARD_REJECT
    # It must not redefine a local/parallel predicate set.
    for mod in (gate_mod,):
        src = inspect.getsource(mod)
        assert "_RISK_STATE_HARD_REJECT = " not in src, (
            "the degraded predicate must be IMPORTED, never redefined locally"
        )


# --------------------------------------------------------------------------- #
# AC7 — c8e10019 (async plumbing threads graph_state, blocks before mutation)
# --------------------------------------------------------------------------- #


class _SpyGate:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def evaluate(self, **kwargs):
        self.calls.append(dict(kwargs))
        return SimpleNamespace(
            allowed=False,
            reason="cognitive_status_unavailable",
            blocking_count=0,
            blocking_items=(),
        )


async def _seed_validation_card() -> tuple[str, str]:
    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F16 wiring board", owner_id="f16-agent", settings={}))
        db.add(Spec(id=spec_id, board_id=board_id, title="F16 wiring spec",
                    status=SpecStatus.IN_PROGRESS, created_by="f16-agent",
                    acceptance_criteria=[], test_scenarios=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="F16 card",
                    status=CardStatus.VALIDATION, card_type=CardType.NORMAL,
                    position=0, created_by="f16-agent"))
        await db.commit()
    return board_id, card_id


@pytest.mark.asyncio
async def test_ts_3eda0dc9_async_plumbing_blocks_before_mutation(monkeypatch):
    _, card_id = await _seed_validation_card()
    spy = _SpyGate()

    async def _stub_health(board_id, db, scheduler_control=None):
        return {"graph_state": "recovery_needed"}

    monkeypatch.setattr(kg_health_service, "get_kg_health", _stub_health)

    validation_data = {
        "confidence": 99, "confidence_justification": "strong",
        "estimated_completeness": 100, "completeness_justification": "complete",
        "estimated_drift": 0, "drift_justification": "none",
        "recommendation": "approve", "general_justification": "ready",
    }
    async with get_session_factory()() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: spy
        with pytest.raises(ValueError, match="cognitive_status_unavailable"):
            await service.submit_task_validation(
                card_id=card_id, reviewer_id="f16-agent",
                reviewer_name="f16-agent", data=validation_data,
            )
        await db.rollback()

    # the async caller resolved graph_state via get_kg_health and threaded it in
    assert spy.calls, "the closeout gate was never reached"
    assert spy.calls[0]["graph_state"] == "recovery_needed"
    assert spy.calls[0]["target_status"] == "done"

    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.VALIDATION  # no status mutation
    assert card.validations in (None, [])  # no validation/conclusion append


@pytest.mark.asyncio
async def test_resolve_graph_state_fail_safe_returns_none(monkeypatch):
    """FR6 fail-safe: a get_kg_health failure resolves to None (never swallowed
    into ALLOWED — the gate's null-generation liveness then governs)."""
    async def _boom(board_id, db, scheduler_control=None):
        raise kg_health_service.BoardNotFoundError("nope")

    monkeypatch.setattr(kg_health_service, "get_kg_health", _boom)

    result = await services_main._resolve_closeout_graph_state("b", object())
    assert result is None


# --------------------------------------------------------------------------- #
