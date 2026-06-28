"""Spec 3d907a87 (FR5 / D3 / TS3) — idempotency probes for cognitive extraction.

The handler short-circuits when Kùzu already holds the equivalent node:
- Learning: a (Learning)-[:validates]->(Bug {id: $bug_node_id}) match
- Alternative / Assumption: a node with the same source_artifact_ref

Both probes are best-effort — they catch any exception (graph not yet
bootstrapped, schema drift, missing column) and return False. These tests
patch the ``cypher_executor`` port to assert the behavior without standing up a
real Kùzu graph (kept hermetic for the unit run).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
    _learning_already_exists,
    _node_with_source_ref_exists,
    _summariser_factory,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.models.db import CardType


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


def test_learning_already_exists_true_when_count_positive(monkeypatch):
    monkeypatch.setattr(get_kg_registry(), "cypher_executor", _StubCypherExecutor(1))
    assert _learning_already_exists("board-1", "bug_xyz") is True


def test_learning_already_exists_false_when_count_zero(monkeypatch):
    monkeypatch.setattr(get_kg_registry(), "cypher_executor", _StubCypherExecutor(0))
    assert _learning_already_exists("board-1", "bug_xyz") is False


def test_learning_already_exists_false_on_exception(monkeypatch):
    monkeypatch.setattr(
        get_kg_registry(),
        "cypher_executor",
        _BoomCypherExecutor("kuzu not bootstrapped"),
    )
    assert _learning_already_exists("board-1", "bug_xyz") is False


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
async def test_handler_skips_learning_when_already_exists(caplog, monkeypatch):
    """TC-3 (TS3): re-mover bug done com Learning existente → skip silencioso."""
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
    skipped = [r for r in caplog.records if "learning.skipped" in r.message
               and getattr(r, "reason", None) == "already_exists"]
    assert skipped, f"expected learning.skipped already_exists, got {[r.message for r in caplog.records]}"


def test_summariser_factory_returns_openai_for_openai_provider():
    s = _summariser_factory({"provider": "openai", "model": "gpt-4o"})
    assert s is not None
    title, body = s.summarise(bug_title="bug X", action_plan="plan Y" * 100)
    assert "bug X" in title
    assert "plan Y" in body


def test_summariser_factory_returns_none_for_unknown_provider():
    assert _summariser_factory({"provider": "anthropic"}) is None
    assert _summariser_factory({}) is None
    assert _summariser_factory({"provider": ""}) is None
