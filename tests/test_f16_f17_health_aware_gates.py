"""BASE F6E: authoritative source observation replaces graph-health inference.

Preserve skip policy, substantive item blocks, telemetry and refusal before
lifecycle mutation. Historical F16/F17 liveness expectations are superseded.
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

    def __init__(self, items: list[CognitiveConsolidationItem] | None = None):
        self.items = list(items or [])

    def read_completion_snapshot(self, board_id, kg_generation_id=None):
        return 'kg-1', list(self.items)

    def latest_generation(self, board_id: str) -> str | None:
        return "kg-1"

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return list(self.items)


class _NoGenStore:

    def latest_generation(self, board_id: str) -> str | None:
        return None

    def read_completion_snapshot(self, board_id, kg_generation_id=None):
        return None, []

    def list_items(self, board_id: str, kg_generation_id: str, **_kw):
        return []


def _gate(store) -> CognitiveCloseoutGate:
    return CognitiveCloseoutGate(store=store)




@pytest.mark.parametrize("degraded_state", ["recovery_needed", "quarantined"])
def test_degraded_graph_does_not_override_confirmed_empty_source(degraded_state):
    assert degraded_state in _RISK_STATE_HARD_REJECT
    result = _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state=degraded_state,
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value




def test_confirmed_source_absence_does_not_require_graph_generation():
    result = _gate(_NoGenStore()).evaluate(
        board_id="board-1",
        entity_type="spec",
        entity_id="spec-1",
        target_status="done",
    )
    assert result.allowed is True
    assert result.outcome == CognitiveCloseoutOutcome.ALLOWED.value
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value


def test_explicit_skip_preserves_authority_over_pending_items():
    result = _gate(_FakeStore([_item('task:card-1', CognitiveItemStatus.PENDING.value)])).evaluate(
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




def test_ts_4663630d_healthy_empty_stays_allowed():
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




def test_known_empty_source_emits_allowed_without_automatic_skip():
    reset_closeout_gate_samples()
    _gate(_FakeStore()).evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
        graph_state="recovery_needed",
    )
    sample = get_closeout_gate_samples()[-1]
    assert sample["outcome"] == CognitiveCloseoutOutcome.ALLOWED.value
    assert sample["reason"] == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value
    assert sample["skip_enabled"] == "false"




def test_ts_80500309_gate_stays_sync_no_io():
    assert not inspect.iscoroutinefunction(CognitiveCloseoutGate.evaluate)
    src = inspect.getsource(gate_mod)
    for forbidden in ("AsyncSession", "get_kg_health", "get_db", "open_board_connection"):
        assert forbidden not in src, (
            f"the closeout gate must stay sync/pure (no I/O) — found {forbidden!r}"
        )


def test_completion_gate_does_not_depend_on_backpressure_predicate():
    assert not hasattr(gate_mod, '_RISK_STATE_HARD_REJECT')




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
    from native_subject_testing import record_native_subject_authority
    from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope
    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F16 wiring board", owner_id="f16-agent", settings={}))
        db.add(Spec(id=spec_id, board_id=board_id, title="F16 wiring spec",
                    status=SpecStatus.IN_PROGRESS, created_by="f16-agent",
                    architecture_adoption=ArchitectureAdoptionScope(
                        board_id=board_id, spec_id=spec_id, actor_id="f16-agent",
                        adopted_in_edition=1, inherited_resource_ids=(),
                    ).model_dump(mode="json"),
                    acceptance_criteria=[], test_scenarios=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="F16 card",
                    status=CardStatus.VALIDATION, card_type=CardType.NORMAL,
                    position=0, created_by="f16-agent"))
        await record_native_subject_authority(db)
        await db.commit()
    return board_id, card_id


@pytest.mark.asyncio
async def test_ts_3eda0dc9_async_plumbing_blocks_before_mutation(monkeypatch):
    _, card_id = await _seed_validation_card()
    spy = _SpyGate()

    async def _stub_health(board_id, db, scheduler_control=None):
        pytest.fail("completion must not invoke graph Health")

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
                card_id=card_id, reviewer_id="f16-reviewer",
                reviewer_name="f16-reviewer", data=validation_data,
            )
        await db.rollback()

    assert spy.calls, "the closeout gate was never reached"
    assert spy.calls[0]["graph_state"] is None
    assert spy.calls[0]["target_status"] == "done"

    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.VALIDATION  # no status mutation
    assert card.validations in (None, [])  # no validation/conclusion append


@pytest.mark.asyncio
async def test_resolve_graph_state_fail_safe_returns_none(monkeypatch):
    async def _boom(board_id, db, scheduler_control=None):
        raise kg_health_service.BoardNotFoundError("nope")

    monkeypatch.setattr(kg_health_service, "get_kg_health", _boom)

    result = await services_main._resolve_closeout_graph_state("b", object())
    assert result is None


