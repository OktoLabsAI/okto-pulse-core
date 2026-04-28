"""Spec 3d907a87 (FR5 / D3 / TS3) — idempotency probes for cognitive extraction.

The handler short-circuits when Kùzu already holds the equivalent node:
- Learning: a (Learning)-[:validates]->(Bug {id: $bug_node_id}) match
- Alternative / Assumption: a node with the same source_artifact_ref

Both probes are best-effort — they catch any exception (graph not yet
bootstrapped, schema drift, missing column) and return False. These tests
patch ``BoardConnection`` to assert the behavior without standing up a
real Kùzu graph (kept hermetic for the unit run).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
    _learning_already_exists,
    _node_with_source_ref_exists,
    _summariser_factory,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.models.db import CardType


class _StubResult:
    def __init__(self, count: int):
        self._rows = [[count]]
        self._idx = 0

    def has_next(self) -> bool:
        return self._idx < len(self._rows)

    def get_next(self):
        v = self._rows[self._idx]
        self._idx += 1
        return v


class _StubConn:
    def __init__(self, count: int):
        self._count = count
        self.queries: list[tuple[str, dict]] = []

    def execute(self, cypher: str, params: dict | None = None):
        self.queries.append((cypher, params or {}))
        return _StubResult(self._count)


def _stub_board_connection_factory(count: int):
    conn = _StubConn(count)

    class _Wrapper:
        def __init__(self, board_id):  # noqa: ARG002
            pass
        def __enter__(self):
            return (None, conn)
        def __exit__(self, *a):
            return False

    return _Wrapper, conn


def test_learning_already_exists_true_when_count_positive():
    Wrapper, _ = _stub_board_connection_factory(1)
    with patch("okto_pulse.core.kg.schema.BoardConnection", Wrapper):
        assert _learning_already_exists("board-1", "bug_xyz") is True


def test_learning_already_exists_false_when_count_zero():
    Wrapper, _ = _stub_board_connection_factory(0)
    with patch("okto_pulse.core.kg.schema.BoardConnection", Wrapper):
        assert _learning_already_exists("board-1", "bug_xyz") is False


def test_learning_already_exists_false_on_exception():
    class _Boom:
        def __init__(self, *a, **kw):
            raise RuntimeError("kuzu not bootstrapped")

    with patch("okto_pulse.core.kg.schema.BoardConnection", _Boom):
        assert _learning_already_exists("board-1", "bug_xyz") is False


def test_node_with_source_ref_exists_true():
    Wrapper, _ = _stub_board_connection_factory(2)
    with patch("okto_pulse.core.kg.schema.BoardConnection", Wrapper):
        assert _node_with_source_ref_exists("board-1", "Alternative", "spec:abc") is True


def test_node_with_source_ref_exists_false_on_exception():
    class _Boom:
        def __init__(self, *a, **kw):
            raise RuntimeError("schema drift")

    with patch("okto_pulse.core.kg.schema.BoardConnection", _Boom):
        assert _node_with_source_ref_exists("board-1", "Assumption", "spec:abc") is False


@pytest.mark.asyncio
async def test_handler_skips_learning_when_already_exists(caplog):
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
    Wrapper, _ = _stub_board_connection_factory(1)  # Learning exists
    event = CardMoved(
        board_id="board-1", card_id="card-1",
        from_status="validation", to_status="done",
    )
    with patch("okto_pulse.core.kg.schema.BoardConnection", Wrapper):
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
