"""Tests for the KG health snapshot service and endpoint.

Spec 20f67c2a (Ideação #5). Covers FR1, FR2, FR3, FR4, FR5, FR8 and
ACs 1-10. Aligns with the Ideação #3 lesson: every scenario marked
``automated`` in the Pulse has a real pytest assertion in this file.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from okto_pulse.core.kg.scoring import (
    CONTRADICT_PENALTY_CAP,
    DECAY_REORDER_POOL_MULTIPLIER,
    _apply_decay_reorder,
    _fetch_node_inputs,
    get_contradict_warn_count,
    reset_contradict_warn_counters,
)
from okto_pulse.core.models.db import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
)
from okto_pulse.core.services.kg_health_service import (
    BoardNotFoundError,
    DEFAULT_SCORE_BAND_HIGH,
    DEFAULT_SCORE_BAND_LOW,
    DEFAULT_SCORE_RATIO_ALARM_THRESHOLD,
    HEALTH_SCHEMA_VERSION,
    get_kg_health,
)


KG_HEALTH_BOARD_ID = "board-kg-health-test"
KG_HEALTH_USER_ID = "user-kg-health-test"


@pytest_asyncio.fixture
async def kg_health_board(db_factory):
    """Idempotent Board row for the health tests; reset queue + counter state."""
    async with db_factory() as session:
        existing = await session.get(Board, KG_HEALTH_BOARD_ID)
        if existing is None:
            session.add(
                Board(
                    id=KG_HEALTH_BOARD_ID,
                    name="kg-health-test",
                    owner_id=KG_HEALTH_USER_ID,
                )
            )
            await session.commit()

        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.execute(
            ConsolidationDeadLetter.__table__.delete().where(
                ConsolidationDeadLetter.board_id == KG_HEALTH_BOARD_ID,
            )
        )
        await session.commit()

    reset_contradict_warn_counters()
    yield KG_HEALTH_BOARD_ID
    reset_contradict_warn_counters()


# --- TS1 / AC1: 10 fields in the health response ---


@pytest.mark.asyncio
async def test_health_response_carries_10_fields(db_factory, kg_health_board):
    """get_kg_health returns the contracted shape (FR1, BR1).

    Spec 28583299 (Ideação #4) extended this additively with
    last_decay_tick_at + nodes_recomputed_in_last_tick — HEALTH_SCHEMA_VERSION
    stays at "1.0" because no existing field changed semantics.
    """
    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    expected_fields = {
        "queue_depth",
        "oldest_pending_age_s",
        "dead_letter_count",
        "total_nodes",
        "default_score_count",
        "default_score_ratio",
        "avg_relevance",
        "top_disconnected_nodes",
        "schema_version",
        "health_schema_version",
        "graph_schema_version",
        "contradict_warn_count",
        "last_decay_tick_at",
        "last_tick_status",
        "last_tick_error",
        "nodes_recomputed_in_last_tick",
        # Bug fix (Run tick now cross-mount race): expõe o estado do
        # advisory lock global ``kg_daily_tick`` para que o frontend
        # consiga desabilitar o botão mesmo se o usuário fechar o modal
        # e voltar enquanto o tick (cron OU manual) está rodando.
        "tick_in_progress",
    }
    assert set(result.keys()) == expected_fields
    assert result["schema_version"] == HEALTH_SCHEMA_VERSION
    assert result["health_schema_version"] == HEALTH_SCHEMA_VERSION
    assert result["schema_version"] == "1.0"
    assert isinstance(result["queue_depth"], int)
    assert isinstance(result["oldest_pending_age_s"], float)
    assert isinstance(result["top_disconnected_nodes"], list)
    assert result["last_decay_tick_at"] is None or isinstance(result["last_decay_tick_at"], str)
    assert result["last_tick_status"] is None or isinstance(result["last_tick_status"], str)
    assert result["last_tick_error"] is None or isinstance(result["last_tick_error"], str)
    assert isinstance(result["nodes_recomputed_in_last_tick"], int)
    assert isinstance(result["tick_in_progress"], bool)


# --- TS2 / AC2: 404 (BoardNotFoundError) for unknown board ---


@pytest.mark.asyncio
async def test_health_raises_for_nonexistent_board(db_factory):
    """Unknown board_id raises BoardNotFoundError so the route maps to 404."""
    async with db_factory() as session:
        with pytest.raises(BoardNotFoundError):
            await get_kg_health("ghost-board-id-does-not-exist", session)


# --- TS3 / AC3: REST + MCP shape parity (proxy via service layer) ---


@pytest.mark.asyncio
async def test_service_layer_response_matches_pydantic_model(
    db_factory, kg_health_board
):
    """The KGHealthResponse Pydantic model accepts the service dict as-is.

    AC3 expects MCP and REST to share a shape; with the MCP wire deferred,
    we prove parity by feeding the service output into the Pydantic model
    that the REST endpoint serializes — any drift would fail validation.
    """
    from okto_pulse.core.api.kg_health import KGHealthResponse

    async with db_factory() as session:
        data = await get_kg_health(kg_health_board, session)

    # Should construct without raising — proves shape parity.
    response = KGHealthResponse(**data)
    # And the dump round-trips into the same set of keys.
    assert set(response.model_dump().keys()) == set(data.keys())


# --- TS4 / AC4: contradict cap preserves floor + structured log ---


def test_contradict_cap_preserves_floor_and_emits_log(caplog):
    """raw_sum=2.5 (5 contradicts × 0.5) is capped at 0.5 with a WARN log.

    Uses a stub Kùzu connection so we don't need a real graph; exercises
    the same code path as production (cap + counter + log + dict shape).
    """
    caplog.set_level(logging.WARNING, logger="okto_pulse.kg.scoring")
    reset_contradict_warn_counters()

    class _StubResult:
        def __init__(self, row):
            self._row = row
            self._consumed = False

        def has_next(self):
            return not self._consumed

        def get_next(self):
            self._consumed = True
            return self._row

        def close(self):
            pass

    class _StubConn:
        def execute(self, _cypher, _params):
            return _StubResult([
                1.0,    # source_confidence
                3,      # out_deg
                2,      # in_deg
                10,     # query_hits
                None,   # last_queried_at
                0.5,    # relevance_score
                2.5,    # SUM(contradict_confidence) — way above cap
                0.0,    # priority_boost
            ])

    inputs = _fetch_node_inputs(
        _StubConn(), "Decision", "decision_x", board_id=KG_HEALTH_BOARD_ID,
    )

    assert inputs is not None
    assert inputs["raw_contradict_penalty"] == 2.5
    assert inputs["contradict_penalty"] == CONTRADICT_PENALTY_CAP == 0.5
    assert any(
        "contradict_penalty_capped" in rec.message
        for rec in caplog.records
    )
    reset_contradict_warn_counters()


# --- TS5 / AC5: contradict_warn_count increments per cap event ---


def test_contradict_warn_count_increments_on_cap_event():
    """Counter is per-board and increments only when raw_sum > cap."""
    reset_contradict_warn_counters()

    class _Stub:
        def __init__(self, penalty):
            self._penalty = penalty
            self._consumed = False

        def execute(self, _c, _p):
            return self

        def has_next(self):
            return not self._consumed

        def get_next(self):
            self._consumed = True
            return [1.0, 2, 1, 5, None, 0.5, self._penalty, 0.0]

        def close(self):
            pass

    # Three nodes that trigger the cap.
    for _ in range(3):
        _fetch_node_inputs(
            _Stub(2.5), "Decision", "x", board_id=KG_HEALTH_BOARD_ID,
        )
    # One node BELOW the cap should not increment.
    _fetch_node_inputs(_Stub(0.2), "Decision", "y", board_id=KG_HEALTH_BOARD_ID)

    assert get_contradict_warn_count(KG_HEALTH_BOARD_ID) == 3
    reset_contradict_warn_counters()


# --- TS6 / AC6: decay reorder reverts stale ranking ---


def test_apply_decay_reorder_reverts_stale_ranking():
    """B (lower raw, fresh) ranks above A (higher raw, 60d stale)."""
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old = (now - timedelta(days=60)).isoformat()
    fresh = (now - timedelta(days=1)).isoformat()
    rows = [
        {
            "node_id": "A",
            "relevance_score": 0.8,
            "query_hits": 10,
            "last_queried_at": old,
        },
        {
            "node_id": "B",
            "relevance_score": 0.7,
            "query_hits": 10,
            "last_queried_at": fresh,
        },
    ]
    result = _apply_decay_reorder(rows, top_k=2, now=now)

    assert [r["node_id"] for r in result] == ["B", "A"]
    assert result[0]["decayed_relevance"] > result[1]["decayed_relevance"]


def test_apply_decay_reorder_handles_edge_cases():
    """Empty input and top_k <= 0 return [] without raising."""
    assert _apply_decay_reorder([], top_k=5) == []
    assert _apply_decay_reorder([{"node_id": "z"}], top_k=0) == []


def test_pool_multiplier_constant_is_three():
    """Refinement decision: limit_pre = top_k * 3."""
    assert DECAY_REORDER_POOL_MULTIPLIER == 3


# --- TS7 / AC7: Cypher ORDER BY clauses unchanged (BR4) ---


def test_cypher_templates_order_by_relevance_score_unchanged():
    """The literal ORDER BY clauses in cypher_templates.py and
    kuzu_graph_store.py are preserved (BR4)."""
    repo_root = Path(__file__).parent.parent

    cypher_templates = (
        repo_root / "src" / "okto_pulse" / "core" / "kg" / "cypher_templates.py"
    ).read_text(encoding="utf-8")
    kuzu_store = (
        repo_root / "src" / "okto_pulse" / "core" / "kg" / "providers"
        / "embedded" / "kuzu_graph_store.py"
    ).read_text(encoding="utf-8")

    assert "ORDER BY d.relevance_score DESC" in cypher_templates
    assert "ORDER BY l.relevance_score DESC" in cypher_templates
    assert "ORDER BY n.relevance_score DESC" in kuzu_store


# --- TS8 / AC8: default-score distribution alarm ---


@pytest.mark.asyncio
async def test_default_score_ratio_skew_emits_alarm_log(
    caplog, db_factory, kg_health_board
):
    """When ratio > threshold, a structured WARN is emitted.

    Without a populated Kùzu graph, total_nodes is 0 so the ratio path
    cannot trigger via aggregations alone. We assert the threshold + log
    machinery by stubbing _aggregate_kuzu_metrics in the service module.
    """
    from okto_pulse.core.services import kg_health_service as svc

    caplog.set_level(logging.WARNING, logger="okto_pulse.services.kg_health")

    original = svc._aggregate_kuzu_metrics

    def _stub(_board_id):
        return {
            "total_nodes": 10,
            "default_score_count": 8,
            "avg_relevance": 0.5,
            "top_disconnected_nodes": [],
        }

    svc._aggregate_kuzu_metrics = _stub
    try:
        async with db_factory() as session:
            data = await get_kg_health(kg_health_board, session)
    finally:
        svc._aggregate_kuzu_metrics = original

    assert data["default_score_count"] == 8
    assert data["default_score_ratio"] == 0.8
    assert 0.8 > DEFAULT_SCORE_RATIO_ALARM_THRESHOLD
    assert any(
        "default_score_skew_high" in rec.message for rec in caplog.records
    )


# --- TS9 / AC9: agent_instructions.md doc subseção complete ---


def test_agent_instructions_documents_kg_health_subsection():
    """The new subsection covers the 4 required topics (FR7, BR7) and
    primarily directs agents to the MCP tool (not the REST endpoint)."""
    repo_root = Path(__file__).parent.parent
    doc = (
        repo_root / "src" / "okto_pulse" / "core" / "mcp"
        / "agent_instructions.md"
    ).read_text(encoding="utf-8")

    assert "KG health and operational signals" in doc
    assert "okto_pulse_kg_health" in doc
    assert "/api/v1/kg/health" in doc
    assert "contradict_penalty" in doc.lower() or "CONTRADICT_PENALTY" in doc
    assert "decay" in doc.lower()
    assert "When to consult" in doc or "when to consult" in doc.lower()


# --- TS10 / AC10: queue_depth + oldest_pending_age_s populated correctly ---


@pytest.mark.asyncio
async def test_queue_metrics_reflect_pending_rows(db_factory, kg_health_board):
    """A pending row bumps queue_depth and exposes oldest_pending_age_s."""
    async with db_factory() as session:
        session.add(
            ConsolidationQueue(
                board_id=kg_health_board,
                artifact_type="spec",
                artifact_id="spec-health-test",
                priority="normal",
                source="test:health",
                status="pending",
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["queue_depth"] >= 1
    assert result["oldest_pending_age_s"] >= 0.0


@pytest.mark.asyncio
async def test_dead_letter_metric_reflects_dlq_rows(db_factory, kg_health_board):
    """A dead-letter row bumps dead_letter_count."""
    async with db_factory() as session:
        session.add(
            ConsolidationDeadLetter(
                board_id=kg_health_board,
                artifact_type="spec",
                artifact_id="spec-dlq-test",
                attempts=3,
                errors=[
                    {
                        "attempt": 1,
                        "occurred_at": "2026-04-27T00:00:00Z",
                        "error_type": "TestError",
                        "message": "seeded for kg-health test",
                    }
                ],
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_health_board, session)

    assert result["dead_letter_count"] >= 1


# --- Bonus: contradict band constants are coherent ---


def test_default_score_band_bounds():
    """The [low, high] band brackets the neutral 0.5."""
    assert DEFAULT_SCORE_BAND_LOW == 0.45
    assert DEFAULT_SCORE_BAND_HIGH == 0.55
    assert DEFAULT_SCORE_BAND_LOW <= 0.5 <= DEFAULT_SCORE_BAND_HIGH
    assert 0.0 < DEFAULT_SCORE_RATIO_ALARM_THRESHOLD < 1.0
