"""Spec 3d907a87 (FR1-FR7) — CognitiveExtractionHandler unit tests.

Cards covered:
    - TC-1 (TS1): bug done com action_plan rico → log learning candidate
    - TC-2 (TS2): bug done com action_plan curto → skip
    - TC-4 (TS4): card spec done → log alternative + assumption candidates
    - TC-5 (TS5): board sem cognitive_llm_config → skip Learning, run others

The handler emits structured logs as the "candidate enqueue" surface
(per spec design — actual Kuzu persistence goes through a downstream
worker registered in the umbrella ideation's out-of-scope list). Tests
assert against ``caplog.records`` since that is the authoritative output
for v1.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.models.db import CardType


def _bug_card(*, card_id: str = "card-1", action_plan: str = "x" * 200) -> SimpleNamespace:
    return SimpleNamespace(
        id=card_id,
        card_type=CardType.BUG,
        spec_id=None,
        action_plan=action_plan,
    )


def _normal_card_with_spec(*, card_id: str = "card-1", spec_id: str = "spec-1") -> SimpleNamespace:
    return SimpleNamespace(
        id=card_id,
        card_type=CardType.NORMAL,
        spec_id=spec_id,
        action_plan=None,
    )


def _spec(spec_id: str = "spec-1", *, context: str = "") -> SimpleNamespace:
    return SimpleNamespace(id=spec_id, context=context)


def _board(*, llm_config: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(settings={"cognitive_llm_config": llm_config} if llm_config else {})


def _moved_event(*, card_id: str = "card-1", to_status: str = "done") -> CardMoved:
    return CardMoved(
        board_id="board-1",
        card_id=card_id,
        from_status="validation",
        to_status=to_status,
    )


def _make_session(*, card, board=None, spec=None) -> AsyncMock:
    """Build an AsyncSession stub whose .get() routes by model class."""
    sess = AsyncMock()

    async def fake_get(model, oid):
        # Route by class name to avoid importing the full model graph.
        name = model.__name__
        if name == "Card":
            return card
        if name == "Board":
            return board
        if name == "Spec":
            return spec
        return None

    sess.get = AsyncMock(side_effect=fake_get)
    return sess


@pytest.mark.asyncio
async def test_to_status_not_done_short_circuits(caplog):
    """BR1: handler ignores non-done transitions."""
    handler = CognitiveExtractionHandler()
    sess = _make_session(card=_bug_card())
    event = _moved_event(to_status="in_progress")
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(event, sess)
    assert sess.get.await_count == 0  # never even loaded the card
    assert not caplog.records


@pytest.mark.asyncio
async def test_card_not_found_logs_skipped(caplog):
    handler = CognitiveExtractionHandler()
    sess = _make_session(card=None)
    event = _moved_event()
    with caplog.at_level(logging.DEBUG, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(event, sess)
    assert any("card_not_found" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_bug_done_short_action_plan_skips_learning(caplog):
    """TC-2 (TS2): action_plan < 50 chars → no Learning candidate."""
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=_bug_card(action_plan="a" * 30),
        board=_board(llm_config={"provider": "openai", "model": "x"}),
    )
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(_moved_event(), sess)
    assert not any("learning" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_bug_done_no_llm_config_skips_learning(caplog):
    """TC-5 (TS5): action_plan rich + no LLM config → skip with log info."""
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=_bug_card(action_plan="x" * 200),
        board=_board(llm_config=None),
    )
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(_moved_event(), sess)
    skipped = [r for r in caplog.records if "learning.skipped" in r.message]
    assert skipped, f"expected a learning.skipped log, got {[r.message for r in caplog.records]}"
    # Make sure the reason field is present in the structured payload.
    assert any(getattr(r, "reason", None) == "no_llm_config" for r in skipped)


@pytest.mark.asyncio
async def test_bug_done_with_llm_config_emits_candidate(caplog):
    """TC-1 (TS1): action_plan rich + LLM config → emits learning.candidate."""
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=_bug_card(action_plan="x" * 200),
        board=_board(llm_config={"provider": "openai", "model": "gpt-4o-mini"}),
    )
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(_moved_event(), sess)
    cands = [r for r in caplog.records if "learning.candidate" in r.message]
    assert cands, f"expected learning.candidate log, got {[r.message for r in caplog.records]}"
    rec = cands[0]
    assert getattr(rec, "card_id", None) == "card-1"
    assert getattr(rec, "llm_provider", None) == "openai"


@pytest.mark.asyncio
async def test_card_with_spec_emits_alternative_and_assumption(caplog):
    """TC-4 (TS4): card with spec_id and rich context → both branches."""
    handler = CognitiveExtractionHandler()
    spec = _spec(
        context=(
            "## Analysis\n\n"
            "Considerei alternativa X mas rejeitei. "
            "Assumindo que o cache hit rate fica acima de 95 percent."
        ),
    )
    sess = _make_session(
        card=_normal_card_with_spec(spec_id="spec-1"),
        board=_board(llm_config=None),
        spec=spec,
    )
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(_moved_event(), sess)
    alts = [r for r in caplog.records if "alternative.candidate" in r.message]
    asss = [r for r in caplog.records if "assumption.candidate" in r.message]
    assert alts, "expected at least one alternative.candidate log"
    assert asss, "expected at least one assumption.candidate log"


@pytest.mark.asyncio
async def test_card_without_spec_skips_alt_assumption(caplog):
    """Card with spec_id=None: no Alternative/Assumption logs."""
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=SimpleNamespace(
            id="c2", card_type=CardType.NORMAL, spec_id=None, action_plan=None
        ),
        board=_board(llm_config=None),
    )
    with caplog.at_level(logging.INFO, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(_moved_event(card_id="c2"), sess)
    assert not any(
        "alternative.candidate" in r.message or "assumption.candidate" in r.message
        for r in caplog.records
    )
