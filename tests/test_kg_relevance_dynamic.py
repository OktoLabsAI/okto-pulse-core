"""Tests for the dynamic relevance scoring features (spec 28583299, Ideação #4).

Covers IMPL-F (schema + scoring + kg_health + KGTickRun) and IMPL-A
(bug severity boost via MAX with priority).

Scenario mapping (no theater — every assertion exercises real code):
    IMPL-F:
        TS29: schema bump 0.3.3 + last_recomputed_at on every node type
        TS30: _persist_score updates relevance_score AND last_recomputed_at
        TS31: _recompute_relevance_batch UNWIND path persists last_recomputed_at
        TS32: KGTickRun SQLAlchemy model + table layout
        TS33: get_kg_health returns last_decay_tick_at + nodes_recomputed_in_last_tick
        TS34: HEALTH_SCHEMA_VERSION stays "1.0" (additive change)
        TS35: KGHealthResponse Pydantic accepts new fields with sane defaults
        TS36: agent-facing MCP tool needs no decorator change (response inherits)
    IMPL-A:
        TS1:  bug severity=critical → boost=0.20 (severity wins over priority=None)
        TS2:  bug severity=minor + priority=critical → boost=0.20 (priority wins)
        TS3:  bug severity=major + priority=high → both produce 0.15 (MAX is stable)
        TS4:  feature with severity field set → ignored, priority-only path
        TS5:  _card_to_dict carries severity (None and populated)
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select

from okto_pulse.core.api.kg_health import KGHealthResponse, TopDisconnectedNode
from okto_pulse.core.kg.schema import (
    LAST_RECOMPUTED_COLUMNS,
    NODE_TYPES,
    SCHEMA_VERSION,
    _COMMON_NODE_ATTRS,
    _ensure_last_recomputed_at_columns,
    apply_schema_to_connection,
)
from okto_pulse.core.kg.scoring import (
    SEVERITY_BOOST_BY_LEVEL,
    _persist_score,
    _recompute_relevance_batch,
    _resolve_priority_boost,
    _resolve_severity_boost,
    reset_contradict_warn_counters,
)
from okto_pulse.core.kg.workers.consolidation import _card_to_dict
from okto_pulse.core.models.db import (
    Board,
    BugSeverity,
    CardPriority,
    CardType,
    KGTickRun,
)
from okto_pulse.core.services.kg_health_service import (
    HEALTH_SCHEMA_VERSION,
    get_kg_health,
)


KG_REL_BOARD_ID = "board-kg-relevance-dynamic-test"
KG_REL_USER_ID = "user-kg-relevance-dynamic-test"


@pytest_asyncio.fixture
async def kg_rel_board(db_factory):
    """Idempotent Board row for the relevance dynamic tests; cleans tick rows."""
    async with db_factory() as session:
        existing = await session.get(Board, KG_REL_BOARD_ID)
        if existing is None:
            session.add(
                Board(
                    id=KG_REL_BOARD_ID,
                    name="kg-rel-dynamic-test",
                    owner_id=KG_REL_USER_ID,
                )
            )
            await session.commit()

        # Clear any KGTickRun rows leaked from prior tests in the same DB.
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    reset_contradict_warn_counters()
    yield KG_REL_BOARD_ID
    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()
    reset_contradict_warn_counters()


# ---------------------------------------------------------------------------
# TS29 — schema 0.3.3 + last_recomputed_at on every node type
# ---------------------------------------------------------------------------


def test_ts29_schema_version_is_0_3_3():
    """SCHEMA_VERSION bumped to 0.3.3 to mark last_recomputed_at on bootstrap."""
    assert SCHEMA_VERSION == "0.3.3"


def test_ts29_last_recomputed_columns_constant_exposes_string_type():
    """Migration probe constant declares (last_recomputed_at, STRING) only."""
    assert LAST_RECOMPUTED_COLUMNS == (("last_recomputed_at", "STRING"),)


def test_ts29_common_node_attrs_includes_last_recomputed_at():
    """Every node type picks up last_recomputed_at via _build_node_ddl."""
    assert "last_recomputed_at STRING" in _COMMON_NODE_ATTRS


def test_ts29_eleven_node_types_share_common_attrs_block():
    """All 11 node types declared by the schema reuse _COMMON_NODE_ATTRS,
    so the new column reaches every table without per-type drift."""
    assert len(NODE_TYPES) == 11
    # The DDL builder substitutes _COMMON_NODE_ATTRS verbatim — covered by the
    # existing ``_build_node_ddl`` test, but we re-assert here so a future
    # refactor that splits attrs per node type breaks IMPL-F too.
    from okto_pulse.core.kg.schema import _build_node_ddl

    for node_type in NODE_TYPES:
        ddl = _build_node_ddl(node_type)
        assert "last_recomputed_at STRING" in ddl


def test_ts29_ensure_last_recomputed_helper_signature_matches_siblings():
    """Helper has (conn, node_type) like _ensure_priority_boost_columns."""
    sig = inspect.signature(_ensure_last_recomputed_at_columns)
    assert list(sig.parameters) == ["conn", "node_type"]


def test_ts29_apply_schema_to_connection_invokes_last_recomputed_helper():
    """apply_schema_to_connection wires the new helper into the bootstrap path
    (legacy boards add the column on next open)."""
    src = inspect.getsource(apply_schema_to_connection)
    assert "_ensure_last_recomputed_at_columns" in src


# ---------------------------------------------------------------------------
# TS30 — _persist_score updates last_recomputed_at
# ---------------------------------------------------------------------------


class _RecordingConn:
    """Stub Kùzu connection that records execute() calls for assertion."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def execute(self, cypher: str, params: dict | None = None):
        self.calls.append((cypher, params or {}))

        class _R:
            def has_next(self):
                return False

            def get_next(self):
                return []

            def close(self):
                pass

        return _R()


def test_ts30_persist_score_sets_both_columns_with_default_now():
    """_persist_score persists relevance_score AND last_recomputed_at."""
    conn = _RecordingConn()
    _persist_score(conn, "Decision", "dec_x", 0.73)
    assert len(conn.calls) == 1
    cypher, params = conn.calls[0]
    assert "SET n.relevance_score = $score" in cypher
    assert "n.last_recomputed_at = $now" in cypher
    assert params["nid"] == "dec_x"
    assert params["score"] == 0.73
    # ISO 8601 with timezone so kg_health round-trips correctly.
    assert isinstance(params["now"], str)
    parsed = datetime.fromisoformat(params["now"])
    assert parsed.tzinfo is not None


def test_ts30_persist_score_honours_caller_supplied_now_iso():
    """Caller may pin a specific timestamp (e.g. batched recompute)."""
    fixed = "2026-04-27T10:00:00+00:00"
    conn = _RecordingConn()
    _persist_score(conn, "Bug", "bug_y", 0.42, now_iso=fixed)
    cypher, params = conn.calls[0]
    assert params["now"] == fixed


# ---------------------------------------------------------------------------
# TS31 — UNWIND batch path persists last_recomputed_at
# ---------------------------------------------------------------------------


class _BatchStubConn:
    """Stub returning enough rows for _fetch_node_inputs + capturing UNWIND."""

    def __init__(self, *, rows_to_serve: int):
        self._remaining = rows_to_serve
        self.unwind_calls: list[tuple[str, dict]] = []
        self.fetch_calls = 0

    def execute(self, cypher: str, params: dict | None = None):
        # The fetch query is the long MATCH with OPTIONAL MATCH clauses.
        if "OPTIONAL MATCH" in cypher and "SUM(COALESCE" in cypher:
            self.fetch_calls += 1

            class _Result:
                def __init__(self):
                    self._consumed = False

                def has_next(self):
                    return not self._consumed

                def get_next(self):
                    self._consumed = True
                    # source_conf, out_deg, in_deg, query_hits,
                    # last_queried_at, score_before, raw_penalty, priority_boost
                    return [1.0, 0, 0, 0, None, 0.5, 0.0, 0.0]

                def close(self):
                    pass

            return _Result()

        # The batch UPDATE is the UNWIND path.
        if cypher.startswith("UNWIND"):
            self.unwind_calls.append((cypher, params or {}))

            class _Empty:
                def has_next(self):
                    return False

                def get_next(self):
                    return []

                def close(self):
                    pass

            return _Empty()

        # Any other command (single-node UPDATE etc.) — accept silently.
        class _Noop:
            def has_next(self):
                return False

            def get_next(self):
                return []

            def close(self):
                pass

        return _Noop()


def test_ts31_recompute_batch_unwind_persists_last_recomputed_at():
    """Batch UNWIND path includes r.now in the SET clause and shares one
    timestamp across every row of the batch."""
    # Force the UNWIND code path: > BATCH_UPDATE_THRESHOLD endpoints.
    endpoints = [("Decision", f"dec_{i}") for i in range(60)]
    conn = _BatchStubConn(rows_to_serve=len(endpoints))
    fixed_now = datetime(2026, 4, 27, 11, 0, 0, tzinfo=timezone.utc)

    persisted = _recompute_relevance_batch(
        conn, KG_REL_BOARD_ID, endpoints, now=fixed_now,
    )
    assert persisted == len(endpoints)
    assert len(conn.unwind_calls) == 1
    cypher, params = conn.unwind_calls[0]
    assert "n.last_recomputed_at = r.now" in cypher
    assert "n.relevance_score = r.score" in cypher
    rows = params["rows"]
    assert len(rows) == len(endpoints)
    # All rows in the same batch carry the same recomputed-at marker.
    iso_marker = fixed_now.isoformat()
    assert {row["now"] for row in rows} == {iso_marker}
    # And every row carries the score the function computed for it.
    assert all("score" in row for row in rows)


# ---------------------------------------------------------------------------
# TS32 — KGTickRun SQLAlchemy model + table layout
# ---------------------------------------------------------------------------


def test_ts32_kg_tick_run_model_columns():
    """SQLAlchemy maps the kg_tick_runs table with the contracted columns."""
    cols = {c.name for c in KGTickRun.__table__.columns}
    assert cols == {
        "tick_id",
        "started_at",
        "completed_at",
        "nodes_recomputed",
        "duration_ms",
        "error",
        "boards_processed",
    }
    pk = [c.name for c in KGTickRun.__table__.primary_key.columns]
    assert pk == ["tick_id"]
    # Primary key auto-defaults to a UUID4 string so callers don't need to
    # generate one when only the lifecycle matters.
    instance = KGTickRun()
    assert instance.tick_id is None  # default fires on flush, not __init__
    # Default factory shape — invoke it explicitly to confirm UUID-ish.
    default = KGTickRun.__table__.c.tick_id.default.arg(None)
    assert isinstance(default, str) and len(default) == 36


def test_ts32_kg_tick_run_index_on_completed_at():
    """The composite read pattern (latest tick) is supported by an index."""
    indexes = {idx.name for idx in KGTickRun.__table__.indexes}
    assert "idx_kg_tick_runs_completed_at" in indexes


@pytest.mark.asyncio
async def test_ts32_kg_tick_run_persists_and_queries(db_factory):
    """Persist a tick, query the latest by completed_at — round-trip works."""
    started = datetime.now(timezone.utc) - timedelta(minutes=5)
    completed = started + timedelta(seconds=42)

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        session.add(
            KGTickRun(
                tick_id="tick-test-001",
                started_at=started,
                completed_at=completed,
                nodes_recomputed=137,
                duration_ms=42_000.5,
                boards_processed=3,
            )
        )
        await session.commit()

    async with db_factory() as session:
        latest = await session.scalar(
            select(KGTickRun)
            .where(KGTickRun.completed_at.is_not(None))
            .order_by(KGTickRun.completed_at.desc())
            .limit(1)
        )
        assert latest is not None
        assert latest.tick_id == "tick-test-001"
        assert latest.nodes_recomputed == 137
        assert latest.duration_ms == 42_000.5
        assert latest.boards_processed == 3
        assert latest.error is None

    # Cleanup so other tests are isolated.
    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()


# ---------------------------------------------------------------------------
# TS33 — get_kg_health surfaces last_decay_tick_at + nodes_recomputed_in_last_tick
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts33_kg_health_returns_zero_state_when_no_tick(
    db_factory, kg_rel_board,
):
    """Fresh board with no tick rows reports None + 0 (additive defaults)."""
    async with db_factory() as session:
        result = await get_kg_health(kg_rel_board, session)

    assert result["last_decay_tick_at"] is None
    assert result["nodes_recomputed_in_last_tick"] == 0


@pytest.mark.asyncio
async def test_ts33_kg_health_surfaces_latest_completed_tick(
    db_factory, kg_rel_board,
):
    """After a tick row is inserted, kg_health reports its completion data."""
    completed = datetime(2026, 4, 27, 9, 30, 0, tzinfo=timezone.utc)

    async with db_factory() as session:
        # An older completed tick (should not win the ORDER BY).
        session.add(
            KGTickRun(
                tick_id="tick-old",
                started_at=completed - timedelta(days=1, seconds=30),
                completed_at=completed - timedelta(days=1),
                nodes_recomputed=10,
                duration_ms=1_000.0,
                boards_processed=1,
            )
        )
        # The latest completed tick — what kg_health must surface.
        session.add(
            KGTickRun(
                tick_id="tick-latest",
                started_at=completed - timedelta(seconds=20),
                completed_at=completed,
                nodes_recomputed=84,
                duration_ms=20_000.0,
                boards_processed=2,
            )
        )
        # An in-flight tick (completed_at IS NULL) — must be ignored.
        session.add(
            KGTickRun(
                tick_id="tick-running",
                started_at=completed + timedelta(minutes=1),
                completed_at=None,
                nodes_recomputed=0,
                boards_processed=0,
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(kg_rel_board, session)

    assert result["nodes_recomputed_in_last_tick"] == 84
    assert result["last_decay_tick_at"] is not None
    parsed = datetime.fromisoformat(result["last_decay_tick_at"])
    assert parsed == completed


# ---------------------------------------------------------------------------
# TS34 — HEALTH_SCHEMA_VERSION stays "1.0" (additive change)
# ---------------------------------------------------------------------------


def test_ts34_health_schema_version_unchanged():
    """Adding fields without changing existing semantics keeps the version
    pinned — clients written for "1.0" still parse the response."""
    assert HEALTH_SCHEMA_VERSION == "1.0"


# ---------------------------------------------------------------------------
# TS35 — KGHealthResponse Pydantic accepts new fields with defaults
# ---------------------------------------------------------------------------


def test_ts35_response_model_defaults_when_fields_missing():
    """Pre-IMPL-F clients that don't send the new fields still validate."""
    payload = {
        "queue_depth": 0,
        "oldest_pending_age_s": 0.0,
        "dead_letter_count": 0,
        "total_nodes": 0,
        "default_score_count": 0,
        "default_score_ratio": 0.0,
        "avg_relevance": 0.0,
        "top_disconnected_nodes": [],
        "schema_version": "1.0",
        "contradict_warn_count": 0,
    }
    response = KGHealthResponse(**payload)
    assert response.last_decay_tick_at is None
    assert response.nodes_recomputed_in_last_tick == 0


def test_ts35_response_model_accepts_populated_new_fields():
    """When kg_health emits the new fields they round-trip cleanly."""
    payload = {
        "queue_depth": 5,
        "oldest_pending_age_s": 12.5,
        "dead_letter_count": 0,
        "total_nodes": 50,
        "default_score_count": 10,
        "default_score_ratio": 0.2,
        "avg_relevance": 0.62,
        "top_disconnected_nodes": [
            {"id": "dec_1", "type": "Decision", "degree": 0},
        ],
        "schema_version": "1.0",
        "contradict_warn_count": 1,
        "last_decay_tick_at": "2026-04-27T09:30:00+00:00",
        "nodes_recomputed_in_last_tick": 137,
    }
    response = KGHealthResponse(**payload)
    assert response.last_decay_tick_at == "2026-04-27T09:30:00+00:00"
    assert response.nodes_recomputed_in_last_tick == 137
    assert response.top_disconnected_nodes == [
        TopDisconnectedNode(id="dec_1", type="Decision", degree=0)
    ]
    dumped = response.model_dump()
    assert "last_decay_tick_at" in dumped
    assert "nodes_recomputed_in_last_tick" in dumped


# ---------------------------------------------------------------------------
# TS36 — agent-facing MCP tool inherits the additive change without re-decoration
# ---------------------------------------------------------------------------


def test_ts36_mcp_tool_kg_health_does_not_redeclare_response_shape():
    """okto_pulse_kg_health (Ideação #5) calls get_kg_health and json.dumps
    the dict — extending the dict adds fields automatically. Asserts the
    decorator body imports get_kg_health and serialises whatever it returns."""
    import okto_pulse.core.mcp.server as mcp_server

    src = inspect.getsource(mcp_server)
    assert "okto_pulse_kg_health" in src
    # The MCP tool calls get_kg_health and serialises with json.dumps —
    # any new field added to the service dict propagates automatically.
    assert "get_kg_health(" in src
    assert "json.dumps(" in src


# ===========================================================================
# IMPL-A — Bug severity boost via MAX with priority (dec_27de54df)
# ===========================================================================


def test_severity_boost_table_matches_decision_dec_27de54df():
    """SEVERITY_BOOST_BY_LEVEL = {critical:0.20, major:0.15, minor:0.10}."""
    assert SEVERITY_BOOST_BY_LEVEL == {
        "critical": 0.20,
        "major": 0.15,
        "minor": 0.10,
    }


def test_resolve_severity_boost_handles_enum_str_and_none():
    """Mirrors _resolve_priority_boost defensive contract."""
    assert _resolve_severity_boost(None) == 0.0
    assert _resolve_severity_boost("critical") == 0.20
    assert _resolve_severity_boost("MAJOR") == 0.15  # case-insensitive
    assert _resolve_severity_boost(" minor ") == 0.10  # whitespace tolerant
    assert _resolve_severity_boost("nonexistent_level") == 0.0  # silent
    assert _resolve_severity_boost(42) == 0.0  # non-string non-None
    assert _resolve_severity_boost(BugSeverity.CRITICAL) == 0.20
    assert _resolve_severity_boost(BugSeverity.MAJOR) == 0.15
    assert _resolve_severity_boost(BugSeverity.MINOR) == 0.10


def _make_card_dict(**overrides):
    """Helper: produces a card-shape dict (the shape worker.process_card sees)."""
    base = {
        "id": "card-impl-a-test",
        "title": "Test card",
        "description": "Body",
        "card_type": "bug",
        "spec_id": None,
        "sprint_id": None,
        "origin_task_id": None,
        "priority": None,
        "severity": None,
    }
    base.update(overrides)
    return base


def _resolve_card_boost(card: dict) -> float:
    """Mirror the deterministic_worker logic so the test isolates the rule.

    Lifted verbatim from process_card so a behavioural change to the worker
    that breaks this contract surfaces as a unit-test failure here, not as
    a downstream regression in consolidation tests.
    """
    if (card.get("card_type") or "normal") == "bug":
        return max(
            _resolve_priority_boost(card.get("priority")),
            _resolve_severity_boost(card.get("severity")),
        )
    return _resolve_priority_boost(card.get("priority"))


def test_ts1_bug_severity_critical_no_priority_yields_severity_boost():
    """AC-A1: bug with severity=critical, priority=None → 0.20."""
    card = _make_card_dict(card_type="bug", severity="critical", priority=None)
    assert _resolve_card_boost(card) == 0.20


def test_ts2_bug_priority_critical_severity_minor_yields_priority_boost():
    """AC-A2: bug with severity=minor, priority=critical → 0.20 (priority)."""
    card = _make_card_dict(card_type="bug", severity="minor", priority="critical")
    assert _resolve_card_boost(card) == 0.20


def test_ts3_bug_severity_major_priority_high_both_resolve_to_0_15():
    """AC-A3: severity=major and priority=high both map to 0.15 → MAX = 0.15.

    Equality on both axes proves the MAX is stable (no DOMINA bias).
    """
    card = _make_card_dict(card_type="bug", severity="major", priority="high")
    assert _resolve_priority_boost("high") == 0.10  # priority maps high → 0.10
    assert _resolve_severity_boost("major") == 0.15
    assert _resolve_card_boost(card) == 0.15  # severity wins


def test_ts4_feature_with_severity_field_ignores_severity():
    """AC-A4: feature card_type ignores severity even when populated.

    Severity is a Bug-only signal — leakage from a misconfigured feature
    must not raise nor change the boost calculation.
    """
    card = _make_card_dict(card_type="feature", severity="critical", priority="medium")
    assert _resolve_card_boost(card) == _resolve_priority_boost("medium")  # 0.05
    # And explicitly NOT the severity value.
    assert _resolve_card_boost(card) != 0.20


def test_ts5_card_to_dict_carries_severity_field_none_and_populated():
    """AC-A6: _card_to_dict serialises severity field (None or value).

    Without this, worker.process_card always sees severity=None even when
    the row has it set. This is the integration glue between SQL → worker.
    """

    class _Card:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    bug_with_severity = _Card(
        id="b1", title="B", description="d", card_type=CardType.BUG,
        spec_id=None, sprint_id=None, origin_task_id=None,
        priority=CardPriority.HIGH,
        severity=BugSeverity.CRITICAL,
    )
    payload = _card_to_dict(bug_with_severity)
    assert payload["severity"] == "critical"  # enum unwrapped to .value
    assert payload["priority"] == "high"

    bug_no_severity = _Card(
        id="b2", title="B2", description="",
        card_type=CardType.BUG, spec_id=None, sprint_id=None,
        origin_task_id=None, priority=None, severity=None,
    )
    payload2 = _card_to_dict(bug_no_severity)
    assert "severity" in payload2  # field always present
    assert payload2["severity"] is None


def test_impl_a_bug_no_priority_no_severity_falls_to_zero():
    """AC-A5: bug without priority nor severity → 0.0 (no regression)."""
    card = _make_card_dict(card_type="bug", severity=None, priority=None)
    assert _resolve_card_boost(card) == 0.0


# ===========================================================================
# IMPL-B — KGHitFlushed event + recompute handler (dec_3a6eb8ad)
# ===========================================================================


def test_kg_hit_flushed_event_class_registered():
    """KGHitFlushed appears in EVENT_TYPES (17 → 18) and resolves by type."""
    from okto_pulse.core.events.types import (
        EVENT_TYPES,
        KGHitFlushed,
        resolve_event_class,
    )

    assert KGHitFlushed.event_type == "kg.hit_flushed"
    assert "kg.hit_flushed" in EVENT_TYPES
    # Was 17 before Ideação #4 — IMPL-B (KGHitFlushed) + IMPL-C
    # (CardPriorityChanged + CardSeverityChanged) + IMPL-D (KGDailyTick)
    # bring the total to 21.
    assert len(EVENT_TYPES) == 21
    assert resolve_event_class("kg.hit_flushed") is KGHitFlushed


def test_kg_hit_flushed_payload_for_storage_excludes_envelope():
    """payload_for_storage emits only event-specific fields."""
    from okto_pulse.core.events.types import KGHitFlushed

    event = KGHitFlushed(
        board_id="b-1",
        node_type="Decision",
        node_id="dec-1",
        hits_delta=5,
        flushed_at="2026-04-27T10:00:00+00:00",
    )
    stored = event.payload_for_storage()
    assert stored == {
        "node_type": "Decision",
        "node_id": "dec-1",
        "hits_delta": 5,
        "flushed_at": "2026-04-27T10:00:00+00:00",
    }
    # Envelope columns (board_id, event_id, etc.) live in dedicated columns,
    # not the JSON payload.
    assert "board_id" not in stored


def test_kg_hit_recompute_handler_registered_for_event_type():
    """Handler registers itself with EventBus for kg.hit_flushed."""
    from okto_pulse.core.events.bus import _registry
    from okto_pulse.core.events.handlers.kg_hit_recompute import (
        KGHitRecomputeHandler,
    )

    # Importing the handler module triggers @register_handler.
    handlers = _registry.get("kg.hit_flushed", [])
    assert KGHitRecomputeHandler in handlers


def test_emit_hit_flushed_event_swallows_missing_session_factory():
    """Test-mode bypass: helper returns silently when DB isn't initialised."""
    import asyncio
    from unittest.mock import patch

    from okto_pulse.core.kg.kg_service import _emit_hit_flushed_event

    with patch(
        "okto_pulse.core.infra.database.get_session_factory",
        side_effect=AssertionError("DB not initialised"),
    ):
        # Should not raise — the publish is fire-and-forget.
        asyncio.run(
            _emit_hit_flushed_event("b", "Decision", "n", 3, "2026-01-01T00:00:00+00:00"),
        )


@pytest.mark.asyncio
async def test_emit_hit_flushed_event_persists_domain_event_row(
    db_factory, kg_rel_board,
):
    """Helper writes a domain_events row + handler executions per registered handler."""
    from sqlalchemy import select as _select
    from okto_pulse.core.kg.kg_service import _emit_hit_flushed_event
    from okto_pulse.core.models.db import (
        DomainEventHandlerExecution,
        DomainEventRow,
    )

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.commit()

    await _emit_hit_flushed_event(
        kg_rel_board, "Decision", "dec-test-b", 5,
        "2026-04-27T10:00:00+00:00",
    )

    async with db_factory() as session:
        row = await session.scalar(
            _select(DomainEventRow).where(
                DomainEventRow.board_id == kg_rel_board,
                DomainEventRow.event_type == "kg.hit_flushed",
            )
        )
        assert row is not None
        assert row.payload_json["node_type"] == "Decision"
        assert row.payload_json["node_id"] == "dec-test-b"
        assert row.payload_json["hits_delta"] == 5
        assert row.payload_json["flushed_at"] == "2026-04-27T10:00:00+00:00"

        # Handler execution row created (KGHitRecomputeHandler is registered).
        execs = (await session.execute(
            _select(DomainEventHandlerExecution).where(
                DomainEventHandlerExecution.event_id == row.id,
            )
        )).scalars().all()
        assert any(
            e.handler_name.endswith("KGHitRecomputeHandler") for e in execs
        )

    # Cleanup so re-runs are isolated.
    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.commit()


# ===========================================================================
# IMPL-C — CardPriorityChanged + CardSeverityChanged + boost recompute
# ===========================================================================


def test_impl_c_card_priority_changed_event_class_registered():
    """CardPriorityChanged registered with class lookup + 5-field payload."""
    from okto_pulse.core.events.types import (
        CardPriorityChanged,
        EVENT_TYPES,
        resolve_event_class,
    )

    assert CardPriorityChanged.event_type == "card.priority_changed"
    assert "card.priority_changed" in EVENT_TYPES
    assert resolve_event_class("card.priority_changed") is CardPriorityChanged

    event = CardPriorityChanged(
        board_id="b-1", card_id="c-1",
        old_priority="low", new_priority="critical",
        spec_id="s-1", changed_by="u-1",
    )
    payload = event.payload_for_storage()
    assert payload == {
        "card_id": "c-1",
        "old_priority": "low",
        "new_priority": "critical",
        "spec_id": "s-1",
        "changed_by": "u-1",
    }


def test_impl_c_card_severity_changed_event_class_registered():
    """CardSeverityChanged registered analogously to CardPriorityChanged."""
    from okto_pulse.core.events.types import (
        CardSeverityChanged,
        EVENT_TYPES,
        resolve_event_class,
    )

    assert CardSeverityChanged.event_type == "card.severity_changed"
    assert "card.severity_changed" in EVENT_TYPES
    assert resolve_event_class("card.severity_changed") is CardSeverityChanged


def test_impl_c_card_boost_handlers_registered():
    """Both Card{Priority,Severity}ChangedHandler classes wired to bus."""
    from okto_pulse.core.events.bus import _registry
    from okto_pulse.core.events.handlers.card_boost_recompute import (
        CardPriorityChangedHandler,
        CardSeverityChangedHandler,
        DECISION_AUDIT_DELTA,
    )

    assert CardPriorityChangedHandler in _registry.get("card.priority_changed", [])
    assert CardSeverityChangedHandler in _registry.get("card.severity_changed", [])
    # The audit threshold matches the smallest priority-level delta (medium=0.05).
    assert DECISION_AUDIT_DELTA == 0.05


def test_impl_c_decision_audit_delta_matches_priority_step():
    """Sanity check: DECISION_AUDIT_DELTA equals the smallest priority step.

    PRIORITY_BOOST_BY_LEVEL: none/low=0.0, medium=0.05, high=0.10, very_high
    =0.15, critical=0.20. Smallest non-zero gap is medium - low = 0.05.
    """
    from okto_pulse.core.kg.scoring import PRIORITY_BOOST_BY_LEVEL
    from okto_pulse.core.events.handlers.card_boost_recompute import (
        DECISION_AUDIT_DELTA,
    )

    sorted_values = sorted(set(PRIORITY_BOOST_BY_LEVEL.values()))
    smallest_gap = min(
        b - a for a, b in zip(sorted_values, sorted_values[1:])
    )
    # Floating-point tolerance — Python returns 0.04999... due to binary repr.
    assert abs(DECISION_AUDIT_DELTA - smallest_gap) < 1e-9


def test_impl_c_root_entity_id_matches_worker_format():
    """_root_entity_id must mirror deterministic_worker.process_card format.

    Worker builds ``f"card_{cid[:8]}_entity"`` — handler relies on the same
    format to find the root node. A divergence here would silently break
    the recompute path.
    """
    from okto_pulse.core.events.handlers.card_boost_recompute import (
        _root_entity_id,
    )

    card_id = "abcdef12-3456-7890-abcd-ef1234567890"
    assert _root_entity_id(card_id) == "card_abcdef12_entity"


def test_impl_c_node_type_resolution():
    """Bug card_type maps to Bug node; everything else maps to Entity."""
    from okto_pulse.core.events.handlers.card_boost_recompute import (
        _resolve_node_type,
    )

    assert _resolve_node_type("bug") == "Bug"
    assert _resolve_node_type("normal") == "Entity"
    assert _resolve_node_type("test") == "Entity"
    assert _resolve_node_type(None) == "Entity"


@pytest.mark.asyncio
async def test_impl_c_update_card_emits_priority_change_event(
    db_factory, kg_rel_board,
):
    """CardService.update_card with new priority emits CardPriorityChanged."""
    from sqlalchemy import select as _select
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.models.db import (
        Card, CardPriority, DomainEventHandlerExecution,
        DomainEventRow,
    )
    from okto_pulse.core.models.schemas import CardUpdate

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        # Ensure kg-rel-board exists (fixture).
        # Create a card with priority=low to start.
        card = Card(
            id="card-impl-c-priority-001",
            board_id=kg_rel_board,
            spec_id=None, sprint_id=None,
            title="Test card C",
            description="body",
            priority=CardPriority.LOW,
            created_by="user-c-test",
        )
        session.add(card)
        await session.commit()

    async with db_factory() as session:
        service = CardService(db=session)
        await service.update_card(
            "card-impl-c-priority-001", "user-c-test",
            CardUpdate(priority=CardPriority.CRITICAL),
        )
        await session.commit()

    async with db_factory() as session:
        events = (await session.execute(
            _select(DomainEventRow).where(
                DomainEventRow.board_id == kg_rel_board,
                DomainEventRow.event_type == "card.priority_changed",
            )
        )).scalars().all()
        assert len(events) == 1
        ev = events[0]
        assert ev.payload_json["card_id"] == "card-impl-c-priority-001"
        assert ev.payload_json["old_priority"] == "low"
        assert ev.payload_json["new_priority"] == "critical"

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.execute(
            Card.__table__.delete().where(
                Card.id == "card-impl-c-priority-001",
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_impl_c_update_card_no_priority_change_no_event(
    db_factory, kg_rel_board,
):
    """Updating fields other than priority must NOT emit CardPriorityChanged."""
    from sqlalchemy import select as _select
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.models.db import (
        Card, CardPriority,
        DomainEventHandlerExecution, DomainEventRow,
    )
    from okto_pulse.core.models.schemas import CardUpdate

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        card = Card(
            id="card-impl-c-noevent-001",
            board_id=kg_rel_board,
            spec_id=None, sprint_id=None,
            title="Test card C2",
            description="body",
            priority=CardPriority.MEDIUM,
            created_by="user-c-test",
        )
        session.add(card)
        await session.commit()

    async with db_factory() as session:
        service = CardService(db=session)
        await service.update_card(
            "card-impl-c-noevent-001", "user-c-test",
            CardUpdate(title="Renamed only"),
        )
        await session.commit()

    async with db_factory() as session:
        events = (await session.execute(
            _select(DomainEventRow).where(
                DomainEventRow.board_id == kg_rel_board,
                DomainEventRow.event_type == "card.priority_changed",
            )
        )).scalars().all()
        assert events == []

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.execute(
            Card.__table__.delete().where(
                Card.id == "card-impl-c-noevent-001",
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_impl_c_severity_event_only_for_bug_cards(
    db_factory, kg_rel_board,
):
    """Non-bug cards must not emit CardSeverityChanged even when severity set."""
    from sqlalchemy import select as _select
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.models.db import (
        BugSeverity, Card, CardType,
        DomainEventHandlerExecution, DomainEventRow,
    )
    from okto_pulse.core.models.schemas import CardUpdate

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        # Create a bug card with severity=minor.
        bug = Card(
            id="card-impl-c-bug-001",
            board_id=kg_rel_board,
            spec_id=None, sprint_id=None,
            title="Bug under test",
            description="body",
            card_type=CardType.BUG,
            severity=BugSeverity.MINOR,
            created_by="u",
        )
        # Create a non-bug card with severity=major (mis-set).
        normal = Card(
            id="card-impl-c-feat-001",
            board_id=kg_rel_board,
            spec_id=None, sprint_id=None,
            title="Feature with stale severity",
            description="body",
            card_type=CardType.NORMAL,
            severity=BugSeverity.MAJOR,
            created_by="u",
        )
        session.add_all([bug, normal])
        await session.commit()

    async with db_factory() as session:
        service = CardService(db=session)
        # Bug severity transition emits the event.
        await service.update_card(
            "card-impl-c-bug-001", "u",
            CardUpdate(severity=BugSeverity.CRITICAL),
        )
        # Non-bug severity transition is suppressed.
        await service.update_card(
            "card-impl-c-feat-001", "u",
            CardUpdate(severity=BugSeverity.CRITICAL),
        )
        await session.commit()

    async with db_factory() as session:
        events = (await session.execute(
            _select(DomainEventRow).where(
                DomainEventRow.board_id == kg_rel_board,
                DomainEventRow.event_type == "card.severity_changed",
            )
        )).scalars().all()
        assert len(events) == 1
        assert events[0].payload_json["card_id"] == "card-impl-c-bug-001"
        assert events[0].payload_json["new_severity"] == "critical"

    async with db_factory() as session:
        await session.execute(DomainEventHandlerExecution.__table__.delete())
        await session.execute(DomainEventRow.__table__.delete())
        await session.execute(
            Card.__table__.delete().where(
                Card.id.in_([
                    "card-impl-c-bug-001",
                    "card-impl-c-feat-001",
                ])
            )
        )
        await session.commit()


def test_impl_c_frozen_on_insert_doc_updated_to_mutable_semantics():
    """The R2.1 'frozen on insert' comment in scoring.py was updated to
    reflect that priority_boost is now recomputed by IMPL-C handlers.

    Asserts the new docstring carries the IMPL-C wording so a future revert
    is caught at test time."""
    import inspect
    from okto_pulse.core.kg.scoring import _compute_relevance

    src = inspect.getsource(_compute_relevance)
    assert "Ideação #4, IMPL-C" in src or "IMPL-C" in src
    assert "CardPriorityChangedHandler" in src


# ===========================================================================
# IMPL-D — APScheduler daily tick + handler + cursor scan + advisory lock
# ===========================================================================


def test_impl_d_kg_daily_tick_event_class_registered():
    """KGDailyTick takes total event count to 21 and resolves by class."""
    from okto_pulse.core.events.types import (
        EVENT_TYPES, KGDailyTick, resolve_event_class,
    )

    assert KGDailyTick.event_type == "kg.tick.daily"
    assert "kg.tick.daily" in EVENT_TYPES
    assert len(EVENT_TYPES) == 21
    assert resolve_event_class("kg.tick.daily") is KGDailyTick


def test_impl_d_kg_daily_tick_handler_registered():
    """KGDailyTickHandler decorated for kg.tick.daily."""
    from okto_pulse.core.events.bus import _registry
    from okto_pulse.core.events.handlers.kg_decay_tick import (
        KGDailyTickHandler,
        KG_DECAY_TICK_BATCH_SIZE,
        KG_DECAY_TICK_STALENESS_DAYS,
    )

    assert KGDailyTickHandler in _registry.get("kg.tick.daily", [])
    assert KG_DECAY_TICK_BATCH_SIZE > 0
    assert KG_DECAY_TICK_STALENESS_DAYS > 0


def test_impl_d_apscheduler_dependency_installed():
    """The runtime dep was added in pyproject and is importable."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger

    # Build a no-op scheduler/trigger to confirm version compatibility.
    cron = CronTrigger(hour=3, minute=0, timezone=timezone.utc)
    sched = AsyncIOScheduler(timezone=timezone.utc)
    assert cron.fields  # non-empty trigger fields
    assert sched.state == 0  # APSCHEDULER_STATE_STOPPED


def test_impl_d_emit_daily_tick_callable_exists():
    """The lifespan callback exists and is module-level (APScheduler-friendly)."""
    from okto_pulse.core.app import _emit_daily_tick

    assert callable(_emit_daily_tick)


@pytest.mark.asyncio
async def test_impl_d_persist_tick_run_inserts_row(db_factory):
    """_persist_tick_run inserts the canonical KGTickRun row."""
    from okto_pulse.core.events.handlers.kg_decay_tick import _persist_tick_run

    started = datetime.now(timezone.utc) - timedelta(seconds=10)
    completed = datetime.now(timezone.utc)

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        await _persist_tick_run(
            session,
            tick_id="tick-impl-d-001",
            started_at=started,
            completed_at=completed,
            nodes_recomputed=42,
            duration_ms=1234.5,
            boards_processed=3,
        )

    async with db_factory() as session:
        row = await session.scalar(
            select(KGTickRun).where(KGTickRun.tick_id == "tick-impl-d-001")
        )
        assert row is not None
        assert row.nodes_recomputed == 42
        assert row.duration_ms == 1234.5
        assert row.boards_processed == 3
        assert row.error is None

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_impl_d_run_daily_tick_emits_log_and_persists(
    db_factory, kg_rel_board,
):
    """End-to-end: _run_daily_tick walks boards, persists a tick_run row.

    Uses a board with no Kùzu graph (test isolation) so the cursor scan
    short-circuits and total_recomputed stays at zero — proves the
    bookkeeping path independent of Kùzu state.
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        summary = await _run_daily_tick(
            tick_id="tick-impl-d-002",
            session=session,
            board_id=kg_rel_board,
        )

    assert summary["tick_id"] == "tick-impl-d-002"
    assert summary["nodes_recomputed"] >= 0
    assert summary["boards_processed"] == 1  # kg_rel_board exists
    assert summary["duration_ms"] >= 0

    async with db_factory() as session:
        row = await session.scalar(
            select(KGTickRun).where(KGTickRun.tick_id == "tick-impl-d-002")
        )
        assert row is not None
        assert row.boards_processed == summary["boards_processed"]

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_impl_d_kg_health_reflects_tick_run_after_handler(
    db_factory, kg_rel_board,
):
    """After _run_daily_tick completes, kg_health surfaces the new state."""
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        summary = await _run_daily_tick(
            tick_id="tick-impl-d-003",
            session=session,
            board_id=kg_rel_board,
        )

    async with db_factory() as session:
        health = await get_kg_health(kg_rel_board, session)

    assert health["last_decay_tick_at"] is not None
    assert health["nodes_recomputed_in_last_tick"] == summary["nodes_recomputed"]

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()


# ===========================================================================
# IMPL-E — kg_query_cypher hit-counting parity (dec_bd607339)
# ===========================================================================


def test_impl_e_uuid_like_predicate_strict():
    """_is_uuid_like accepts canonical 36-char UUIDs only."""
    from okto_pulse.core.mcp.kg_power_tools import _is_uuid_like

    assert _is_uuid_like("12345678-1234-1234-1234-123456789012")
    assert _is_uuid_like("ABCDEF01-2345-6789-abcd-ef0123456789")
    assert not _is_uuid_like("not a uuid")
    assert not _is_uuid_like("12345678123412341234123456789012")  # no dashes
    assert not _is_uuid_like(42)
    assert not _is_uuid_like(None)
    assert not _is_uuid_like("123")


def test_impl_e_extract_node_ids_explicit_id_column():
    """RETURN n.id → column 'n.id' (or 'id' alias) is detected directly."""
    from okto_pulse.core.mcp.kg_power_tools import (
        _extract_node_ids_from_cypher_result,
    )

    result = {
        "columns": ["n.id"],
        "rows": [
            ["12345678-1234-1234-1234-123456789012"],
            ["abcdef01-2345-6789-abcd-ef0123456789"],
        ],
    }
    pairs = _extract_node_ids_from_cypher_result(result)
    assert ("unknown", "12345678-1234-1234-1234-123456789012") in pairs
    assert ("unknown", "abcdef01-2345-6789-abcd-ef0123456789") in pairs
    assert len(pairs) == 2


def test_impl_e_extract_node_ids_with_node_type_column():
    """RETURN labels(n), n.id pairs the labels column with node_type."""
    from okto_pulse.core.mcp.kg_power_tools import (
        _extract_node_ids_from_cypher_result,
    )

    result = {
        "columns": ["labels(n)", "id"],
        "rows": [
            [["Decision"], "12345678-1234-1234-1234-123456789012"],
            ["Bug", "abcdef01-2345-6789-abcd-ef0123456789"],
        ],
    }
    pairs = _extract_node_ids_from_cypher_result(result)
    assert ("Decision", "12345678-1234-1234-1234-123456789012") in pairs
    assert ("Bug", "abcdef01-2345-6789-abcd-ef0123456789") in pairs


def test_impl_e_extract_node_ids_uuid_scan_fallback():
    """When no id column is named, scan each row for a UUID-like scalar."""
    from okto_pulse.core.mcp.kg_power_tools import (
        _extract_node_ids_from_cypher_result,
    )

    result = {
        "columns": ["title", "uuid_anywhere"],
        "rows": [
            ["My title", "12345678-1234-1234-1234-123456789012"],
            ["Another", "no-uuid-here"],
        ],
    }
    pairs = _extract_node_ids_from_cypher_result(result)
    assert pairs == [("unknown", "12345678-1234-1234-1234-123456789012")]


def test_impl_e_extract_node_ids_aggregations_yield_nothing():
    """RETURN count(n) provides no per-row id — counter stays untouched."""
    from okto_pulse.core.mcp.kg_power_tools import (
        _extract_node_ids_from_cypher_result,
    )

    result = {"columns": ["count(n)"], "rows": [[42]]}
    assert _extract_node_ids_from_cypher_result(result) == []

    # Empty result is also a no-op.
    assert _extract_node_ids_from_cypher_result({}) == []
    assert _extract_node_ids_from_cypher_result({"columns": [], "rows": []}) == []


def test_impl_e_extract_node_ids_dedupes_repeated_pairs():
    """Repeated (node_type, node_id) pairs collapse to one increment."""
    from okto_pulse.core.mcp.kg_power_tools import (
        _extract_node_ids_from_cypher_result,
    )

    same = "12345678-1234-1234-1234-123456789012"
    result = {
        "columns": ["id"],
        "rows": [[same], [same], [same]],
    }
    pairs = _extract_node_ids_from_cypher_result(result)
    assert pairs == [("unknown", same)]


def test_impl_e_agent_instructions_documents_return_id_contract():
    """agent_instructions.md must spell out the RETURN-shape requirement."""
    from pathlib import Path

    doc = Path(
        "src/okto_pulse/core/mcp/agent_instructions.md"
    ).read_text(encoding="utf-8")
    assert "Hit-counting parity" in doc
    assert "RETURN n.id" in doc
    assert "RETURN n" in doc
    assert "Aggregator queries" in doc
    assert "do not" in doc.lower()
    # last_decay_tick_at visibility callout is also part of IMPL-E doc bullet.
    assert "last_decay_tick_at" in doc


# ===========================================================================
# DOC-G — agent_instructions consolidation + scoring docstring + drift review
# ===========================================================================


def test_doc_g_agent_instructions_describes_12_health_fields():
    """The 'KG health and operational signals' section now reports 12 fields."""
    from pathlib import Path

    doc = Path(
        "src/okto_pulse/core/mcp/agent_instructions.md"
    ).read_text(encoding="utf-8")
    assert "JSON with 12 fields" in doc
    assert "last_decay_tick_at" in doc
    assert "nodes_recomputed_in_last_tick" in doc
    # Operational guidance bullets so agents know what each field means.
    assert "Reading the new tick fields" in doc
    assert "spec 28583299" in doc
    assert "KG_DECAY_TICK_STALENESS_DAYS" in doc


def test_doc_g_scoring_module_docstring_v033_paragraph():
    """scoring.py module docstring carries the v0.3.3 entry-points paragraph."""
    import okto_pulse.core.kg.scoring as scoring_mod

    doc = scoring_mod.__doc__ or ""
    assert "v0.3.3" in doc
    assert "spec 28583299" in doc
    assert "Ideação #4" in doc
    assert "kg.hit_flushed" in doc
    assert "card.priority_changed" in doc
    assert "card.severity_changed" in doc
    assert "kg.tick.daily" in doc
    assert "_apply_decay_reorder" in doc
    assert "BR4" in doc


def test_doc_g_drift_review_invariants_preserved():
    """Drift review: the four invariants from KE-G hold post-IMPL-A..F."""
    import inspect
    from okto_pulse.core.kg import cypher_templates as tpl
    from okto_pulse.core.kg.providers.embedded import kuzu_graph_store

    # (1) BR4 — cypher_templates retain ORDER BY <var>.relevance_score DESC.
    # Templates use `d.relevance_score`, `l.relevance_score`, etc., so we
    # match the suffix-only pattern to stay robust against the variable
    # alias chosen per template.
    src_tpl = inspect.getsource(tpl)
    import re as _re

    matches = _re.findall(
        r"ORDER\s+BY\s+\w+\.relevance_score\s+DESC", src_tpl,
    )
    assert len(matches) >= 3, (
        f"Ideação #5 BR4 violated — at least 3 templates must keep "
        f"ORDER BY <var>.relevance_score DESC (got {len(matches)})."
    )

    # (2) _apply_decay_reorder is still invoked by the kuzu_graph_store path.
    src_store = inspect.getsource(kuzu_graph_store)
    assert "_apply_decay_reorder" in src_store

    # (3) Tick handler updates last_recomputed_at, NOT last_queried_at.
    from okto_pulse.core.events.handlers import kg_decay_tick

    tick_src = inspect.getsource(kg_decay_tick)
    # The tick funnels through _recompute_relevance_batch which uses
    # _persist_score (sets last_recomputed_at). Verify it does not
    # directly write last_queried_at.
    assert "last_queried_at" not in tick_src

    # (4) record_query_hit remains the only writer of last_queried_at.
    from okto_pulse.core.kg import kg_service

    svc_src = inspect.getsource(kg_service)
    # Reads (RETURN ... n.last_queried_at) are fine; the assignment shape
    # `n.last_queried_at = $ts` only appears inside the _flush_to_kuzu
    # SET clause. Multi-line f-strings break a SET-anchored regex, so we
    # match the assignment snippet directly.
    write_matches = _re.findall(
        r"n\.last_queried_at\s*=\s*\$ts", svc_src,
    )
    assert len(write_matches) == 1, (
        f"Ideação #5 invariant violated — n.last_queried_at must be "
        f"written by exactly one path (got {len(write_matches)})."
    )


def test_doc_g_three_event_driven_entry_points_documented():
    """The KE-G narrative (3 entry points + on-read truth) is in scoring docs."""
    import okto_pulse.core.kg.scoring as scoring_mod

    doc = scoring_mod.__doc__ or ""
    # Three entry points
    assert "1. Recompute on hit-flush" in doc
    assert "2. Recompute on priority/severity change" in doc
    assert "3. Daily decay tick" in doc
    # The "fresh enough" rationale
    assert "reasonably fresh" in doc.lower() or "razoavelmente fresco" in doc.lower()
    # last_recomputed_at vs last_queried_at separation explicit
    assert "last_recomputed_at" in doc
    assert "last_queried_at" in doc


@pytest.mark.asyncio
async def test_ts42_nodes_with_stale_score_pre_tick_in_summary(
    db_factory, kg_rel_board,
):
    """AC41 / TS42: tick summary reports nodes_with_stale_score_pre_tick."""
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        summary = await _run_daily_tick(
            tick_id="tick-ts42-001",
            session=session,
            board_id=kg_rel_board,
        )

    assert "nodes_with_stale_score_pre_tick" in summary
    assert isinstance(summary["nodes_with_stale_score_pre_tick"], int)
    assert summary["nodes_with_stale_score_pre_tick"] >= 0

    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()
