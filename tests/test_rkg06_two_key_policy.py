"""RKG-06 (Decision dec_98c9a850) — advisory-default cognitive readiness with a
TWO-KEY blocking opt-in (board ``cognitive_readiness_policy=blocking`` + global
``cognitive_readiness_blocking_enabled``).

Anti-test-theater: the blocking verdict is REAL — an OPEN ``CanonicalDebt`` for the
card's artifact (a technical tier that blocks regardless of the task/test carve-out).
The done transition is exercised through the PRODUCTION gate
``_evaluate_cognitive_readiness_or_raise`` (the same call the card→done path makes),
and the visibility through the RKG-05 ``build_health_readiness`` projection. The
policy never changes technical VISIBILITY — only whether the gate blocks.

Coverage:
  TS1 ts_475f83d3 (integration): advisory default -> no block, blocking visible.
  TS2 ts_ac2fced1 (integration): board policy=blocking + global flag -> blocks done.
  TS3 ts_197ed254 (integration): global flag OFF -> same board reverts to advisory,
     blocker still visible (the flag is the mandatory second key).
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from okto_pulse.core.services.kg_health_readiness_service import build_health_readiness
from okto_pulse.core.services.main import (
    CardService,
    _card_cognitive_entity_type,
    _cognitive_readiness_blocking_active,
    _evaluate_cognitive_readiness_or_raise,
)

USER_ID = "user-rkg06"


def _enable_global_flag(monkeypatch, value=True):
    from okto_pulse.core.infra import config as config_mod
    monkeypatch.setattr(
        config_mod.get_settings(), "cognitive_readiness_blocking_enabled", value,
        raising=False)


async def _seed(db_factory, *, policy=None, with_debt=True):
    board_id = f"rkg06-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    card_id = f"card-{uuid.uuid4().hex[:8]}"
    settings = {"cognitive_readiness_policy": policy} if policy else {}
    async with db_factory() as db:
        db.add(Board(id=board_id, name="rkg06", owner_id=USER_ID, settings=settings))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()
        if with_debt:
            # A normal card resolves to source_ref "task:<id>"; the debt ref must
            # normalize to the SAME artifact_id (mirror R4-TEST3).
            await upsert_canonical_debt(
                db, board_id=board_id, artifact_type="card", artifact_id=card_id,
                source_ref=f"task:{card_id}", content_hash="h",
                target_status="canonical", canonical_state="pending",
                failure_reason="open debt for RKG-06 readiness gate")
            await db.commit()
    return board_id, card_id


async def _run_done_gate(db_factory, board_id, card_id):
    """Exercise the REAL card→done cognitive-readiness gate; raises iff blocked."""
    async with db_factory() as db:
        board = await db.get(Board, board_id)
        card = await db.get(Card, card_id)
        svc = CardService(db)
        await _evaluate_cognitive_readiness_or_raise(
            service_factory=svc._cognitive_readiness_service_factory,
            db=db, board_id=board_id,
            entity_type=_card_cognitive_entity_type(card), entity=card, entity_id=card.id,
            target_label="card",
            policy_blocking=_cognitive_readiness_blocking_active(board),
        )


async def _health(db_factory, board_id):
    async with db_factory() as db:
        return await build_health_readiness(board_id, db, profile="full")


@pytest.mark.asyncio
async def test_ts1_advisory_default_does_not_block_but_visible(db_factory):
    board_id, card_id = await _seed(db_factory, policy=None)  # advisory default

    async with db_factory() as db:
        assert _cognitive_readiness_blocking_active(await db.get(Board, board_id)) is False

    # The real done-gate is a NO-OP under advisory (move would succeed).
    await _run_done_gate(db_factory, board_id, card_id)

    hr = await _health(db_factory, board_id)
    assert hr["readiness"]["blocking"] is True            # blocker IS visible
    assert hr["readiness"]["would_block_done"] is False   # but advisory -> no block
    assert hr["cognitive_enforcement_mode"] == "advisory"
    assert "canonical_debt_open" in hr["readiness"]["reasons"]


@pytest.mark.asyncio
async def test_ts2_two_keys_block_done_with_actionable_error(db_factory, monkeypatch):
    _enable_global_flag(monkeypatch, True)
    board_id, card_id = await _seed(db_factory, policy="blocking")  # key 1 + key 2

    async with db_factory() as db:
        assert _cognitive_readiness_blocking_active(await db.get(Board, board_id)) is True

    # The real done-gate RAISES an actionable error.
    with pytest.raises(ValueError) as exc:
        await _run_done_gate(db_factory, board_id, card_id)
    msg = str(exc.value)
    assert "blocked" in msg and "canonical_debt_open" in msg

    hr = await _health(db_factory, board_id)
    assert hr["enforcement_active"] is True
    assert hr["readiness"]["blocking"] is True
    assert hr["readiness"]["would_block_done"] is True
    assert hr["cognitive_enforcement_mode"] == "blocking"


@pytest.mark.asyncio
async def test_ts3_global_flag_off_reverts_to_advisory_visible(db_factory, monkeypatch):
    # Board OPTS IN (policy=blocking) but the global flag — the mandatory SECOND
    # key — is OFF: the board reverts to advisory; the blocker stays visible.
    _enable_global_flag(monkeypatch, False)
    board_id, card_id = await _seed(db_factory, policy="blocking")

    async with db_factory() as db:
        assert _cognitive_readiness_blocking_active(await db.get(Board, board_id)) is False

    # No block (flag off is the second key) — gate is a no-op.
    await _run_done_gate(db_factory, board_id, card_id)

    hr = await _health(db_factory, board_id)
    assert hr["readiness"]["blocking"] is True            # still visible
    assert hr["readiness"]["would_block_done"] is False   # reverted to advisory
    assert hr["cognitive_enforcement_mode"] == "advisory"


@pytest.mark.asyncio
async def test_invalid_policy_fails_closed_to_advisory(db_factory, monkeypatch):
    # tr_89fc5453: an invalid policy value never blocks silently — it resolves to
    # advisory even with the global flag ON.
    _enable_global_flag(monkeypatch, True)
    board_id, card_id = await _seed(db_factory, policy="bogus-value")

    async with db_factory() as db:
        assert _cognitive_readiness_blocking_active(await db.get(Board, board_id)) is False
    await _run_done_gate(db_factory, board_id, card_id)  # no raise
    hr = await _health(db_factory, board_id)
    assert hr["readiness"]["would_block_done"] is False
    assert hr["readiness"]["blocking"] is True  # technical signal still preserved
