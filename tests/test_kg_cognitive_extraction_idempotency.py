"""Native candidate idempotency and absence of inferred Learning on Done."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
    _node_with_source_ref_exists,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.kg.interfaces import get_kg_registry
from sqlalchemy_test_models import CardType


class _StubCypherExecutor:
    def __init__(self, count: int):
        self._count = count
        self.queries: list[tuple[str, dict]] = []

    def execute_read_only(
        self,
        board_id: str,  # noqa: ARG002
        cypher: str,
        params: dict | None = None,
        *,
        max_rows: int = 1000,  # noqa: ARG002
    ) -> dict:
        self.queries.append((cypher, params or {}))
        return {"rows": [[self._count]], "row_count": 1, "truncated": False}


class _BoomCypherExecutor:
    def __init__(self, message: str):
        self._message = message

    def execute_read_only(self, *a, **kw):  # noqa: ANN002, ANN003
        raise RuntimeError(self._message)








def test_node_with_source_ref_exists_true(monkeypatch):
    monkeypatch.setattr(get_kg_registry(), "cypher_executor", _StubCypherExecutor(2))
    assert _node_with_source_ref_exists("board-1", "Alternative", "spec:abc") is True


def test_node_with_source_ref_exists_false_on_exception(monkeypatch):
    monkeypatch.setattr(
        get_kg_registry(),
        "cypher_executor",
        _BoomCypherExecutor("schema drift"),
    )
    assert _node_with_source_ref_exists("board-1", "Assumption", "spec:abc") is False


@pytest.mark.asyncio
async def test_done_replay_does_not_infer_or_reinterpret_existing_learning(caplog, monkeypatch):
    """KG7.7: graph presence cannot activate the legacy inference path."""
    handler = CognitiveExtractionHandler()
    sess = AsyncMock()

    async def _get(model, oid):
        name = model.__name__
        if name == "Card":
            return SimpleNamespace(
                id="card-1", card_type=CardType.BUG, spec_id=None,
                action_plan="x" * 200,
            )
        if name == "Board":
            return SimpleNamespace(
                settings={"cognitive_llm_config": {"provider": "openai", "model": "x"}},
            )
        return None

    sess.get = AsyncMock(side_effect=_get)
    monkeypatch.setattr(
        get_kg_registry(), "cypher_executor", _StubCypherExecutor(1)
    )  # Learning exists
    event = CardMoved(
        board_id="board-1", card_id="card-1",
        from_status="validation", to_status="done",
    )
    with caplog.at_level(logging.DEBUG, logger="okto_pulse.core.events.cognitive_extraction"):
        await handler.handle(event, sess)
    assert not any("learning" in r.message for r in caplog.records)
