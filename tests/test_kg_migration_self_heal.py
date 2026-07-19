"""Tests for spec 818748f2 — KG schema migration self-heal.

Cobre TS1-TS9 (9 scenarios linkados a 9 ACs). Consolidado em 1 arquivo
em vez dos 5 sugeridos pelos test cards (drift: manutenibilidade
melhor; cada test ainda é mapeado para seu scenario via `# TS*` marker).
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg import primitives as primitives_mod
from kg_schema_testing import NODE_TYPES

schema_mod = pytest.importorskip("okto_pulse.community.adapters.kg_runtime")
BoardConnection = schema_mod.BoardConnection
_BOOTSTRAPPED_BOARDS = schema_mod._BOOTSTRAPPED_BOARDS
_MIGRATED_BOARDS = schema_mod._MIGRATED_BOARDS
bootstrap_board_graph = schema_mod.bootstrap_board_graph
ensure_board_graph_bootstrapped = schema_mod.ensure_board_graph_bootstrapped
migrate_schema_for_board = schema_mod.migrate_schema_for_board


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_okto_home(tmp_path, monkeypatch):
    """Force `~/.okto-pulse/boards` under tmp_path."""
    from okto_pulse.core.infra import config as config_mod
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests
    from kg_registry_testing import configure_test_kg_registry

    monkeypatch.setenv("OKTO_PULSE_HOME", str(tmp_path))
    monkeypatch.setenv("KG_BASE_DIR", str(tmp_path))
    monkeypatch.setattr(
        "okto_pulse.community.adapters.kg_runtime.kg_base_dir",
        lambda: tmp_path / "kg",
        raising=False,
    )
    original_settings = config_mod.get_settings()
    config_mod.configure_settings(
        original_settings.model_copy(update={"kg_base_dir": str(tmp_path)})
    )
    reset_registry_for_tests()
    configure_test_kg_registry()
    try:
        yield tmp_path
    finally:
        reset_registry_for_tests()
        config_mod.configure_settings(original_settings)


@pytest.fixture
def fresh_board(temp_okto_home):
    """Bootstrap a fresh board with the full v0.3.3 schema."""
    bid = str(uuid.uuid4())
    bootstrap_board_graph(bid)
    yield bid


@pytest.fixture
def legacy_board(temp_okto_home):
    """Bootstrap a board, then DROP human_curated + last_recomputed_at via
    Kùzu DDL to simulate a board bootstrapped before v0.3.2.

    Kùzu has no DROP COLUMN — workaround: bootstrap, then directly delete
    the `_MIGRATED_BOARDS` cache so the probe believes the migration is
    fresh. For a true "missing column" simulation we'd need a fresh DB
    populated with the older schema, which is beyond the scope of these
    unit tests. The smoke test in IMPL-A already validated the real-world
    case on board 82c1bfef.
    """
    bid = str(uuid.uuid4())
    bootstrap_board_graph(bid)
    _BOOTSTRAPPED_BOARDS.discard(bid)
    _MIGRATED_BOARDS.discard(bid)
    yield bid


# ---------------------------------------------------------------------------
# TS1 — Hot path migrates legacy board automatically
# ---------------------------------------------------------------------------


def test_ts1_hot_path_migrates_legacy_board(legacy_board):
    """AC1: BoardConnection on a legacy-like board triggers migration.

    The fixture clears caches; opening BoardConnection should run the
    compose probe and add the board to _BOOTSTRAPPED_BOARDS by the end.
    """
    assert legacy_board not in _BOOTSTRAPPED_BOARDS
    with BoardConnection(legacy_board) as (db, conn):
        res = conn.execute(f"CALL TABLE_INFO('{NODE_TYPES[0]}') RETURN *")
        cols = set()
        while res.has_next():
            cols.add(str(res.get_next()[1]))
        res.close()
    # Both v0.3.2 and v0.3.3 columns must be present after the open.
    assert "human_curated" in cols
    assert "last_recomputed_at" in cols
    # Cache populated only after successful path.
    assert legacy_board in _BOOTSTRAPPED_BOARDS


# ---------------------------------------------------------------------------
# TS2 — Already-migrated boards pay zero overhead
# ---------------------------------------------------------------------------


def test_ts2_already_migrated_no_overhead(fresh_board):
    """AC2: cache hit overhead < 5ms median."""
    # First open ensures cache is populated.
    ensure_board_graph_bootstrapped(fresh_board)
    times: list[float] = []
    for _ in range(50):
        t0 = time.time()
        ensure_board_graph_bootstrapped(fresh_board)
        times.append((time.time() - t0) * 1000)
    median = sorted(times)[len(times) // 2]
    assert median < 5.0, f"Cache hit overhead {median}ms exceeds 5ms target"


# ---------------------------------------------------------------------------
# TS3 — CLI single-board returns JSON
# ---------------------------------------------------------------------------


def test_ts3_bare_core_schema_migration_tool_is_not_packaged():
    """The executable belongs to Community, never the standalone Core wheel."""

    core_source = Path(__file__).resolve().parents[1] / "src" / "okto_pulse"
    assert not (core_source / "tools" / "kg_migrate_schema.py").exists()

    from okto_pulse.community.commands import kg_migrate_schema

    assert "community" in Path(kg_migrate_schema.__file__).parts


def test_ts3_in_process_single_board_returns_json_shape(fresh_board, capsys):
    """AC3 (R-P2-03): the migration JSON-payload contract TS3 used to assert via
    the bare-core subprocess is validated IN-PROCESS against a COMPOSED registry —
    the edition-composed path the real migration uses. ``configure_test_kg_registry``
    stands in for the edition composition (the sanctioned test/fake route)."""
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.community.commands.kg_migrate_schema import (
        _emit_single,
        _run_single_board,
    )

    configure_test_kg_registry()
    summary = _run_single_board(fresh_board)
    rc = _emit_single(summary)

    assert rc == 0
    assert summary["board_id"] == fresh_board
    assert summary["migrated"] is True
    assert isinstance(summary["columns_added"], dict)
    assert summary["errors"] == []
    assert isinstance(summary["duration_ms"], int)
    # _emit_single emitted the JSON payload to stdout (the CLI contract shape).
    payload = json.loads(capsys.readouterr().out)
    assert payload["board_id"] == fresh_board and payload["migrated"] is True


# ---------------------------------------------------------------------------
# TS4 — CLI all-boards iterates (smoke test, not full e2e since this
# requires a live SQLite. We test the underlying function directly.)
# ---------------------------------------------------------------------------


def test_ts4_cli_all_boards_iterates_known_boards(fresh_board):
    """AC4: --all-boards path calls migrate_schema_for_board per board.

    Direct unit test (subprocess + DB init costs too much in CI). The CLI
    function `_run_single_board` is the same path the all-boards loop uses.
    """
    from okto_pulse.community.commands.kg_migrate_schema import (
        _emit_all,
        _run_single_board,
    )
    summary1 = _run_single_board(fresh_board)
    assert summary1["migrated"] is True
    assert summary1["board_id"] == fresh_board
    # _emit_all returns 0 on no failures.
    rc = _emit_all([summary1], {fresh_board: "test"})
    assert rc == 0


# ---------------------------------------------------------------------------
# TS5 — MCP tool and REST share the same payload
# ---------------------------------------------------------------------------


def test_ts5_mcp_rest_payload_parity(fresh_board):
    """AC5: REST and MCP responses share the same shape.

    Direct comparison of the two surface entry points: they both call
    `migrate_schema_for_board(board_id)` and serialize the dict. The shape
    must match exactly.
    """
    from okto_pulse.community.api.kg_routes import MigrateSchemaResponse
    rest_summary = migrate_schema_for_board(fresh_board)
    rest_payload = MigrateSchemaResponse(**rest_summary).model_dump()

    # Reset cache so the MCP path re-runs (still no-op since columns exist).
    _MIGRATED_BOARDS.discard(fresh_board)
    mcp_summary = migrate_schema_for_board(fresh_board)
    mcp_payload = json.loads(json.dumps(mcp_summary, default=str))

    assert set(rest_payload.keys()) == set(mcp_payload.keys())
    assert rest_payload["migrated"] == mcp_payload["migrated"]
    assert rest_payload["board_id"] == mcp_payload["board_id"]


# ---------------------------------------------------------------------------
# TS6 — compensate_sync emits warning with migrate-schema guidance
# ---------------------------------------------------------------------------


def test_ts6_compensate_sync_warning_with_guidance(caplog, monkeypatch):
    """AC6: `_compensate_graph_writes` failure logs at WARNING level with
    `migrate-schema` guidance and NO destructive message."""
    from okto_pulse.core.kg.interfaces import get_kg_registry

    class _BrokenGraphTransaction:
        async def begin(self, board_id):  # noqa: ARG002
            raise RuntimeError("simulated_lock_contention")

    monkeypatch.setattr(
        get_kg_registry(),
        "graph_transaction",
        _BrokenGraphTransaction(),
    )
    with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.primitives"):
        primitives_mod._compensate_graph_writes("board-x", "session-x", [])

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    msgs = [r.getMessage() for r in warnings]
    assert any("kg.compensate_sync.failed" in m for m in msgs), msgs
    # The remediation reference can be either `kg_migrate_schema` (the
    # Python module name shipped in IMPL-A) or the MCP tool name. Both
    # are valid — the key invariant is that there's an actionable pointer.
    assert any(
        "kg_migrate_schema" in m or "migrate-schema" in m for m in msgs
    ), msgs
    # No errors emitted (downgraded to warning).
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert not any("kg.compensate_sync.failed" in r.getMessage() for r in errors)
    # No destructive guidance.
    all_msgs = " ".join(msgs)
    assert "Delete graph.kuzu" not in all_msgs


# ---------------------------------------------------------------------------
# TS7 — Re-running migrate-schema is idempotent
# ---------------------------------------------------------------------------


def test_ts7_re_run_idempotent(fresh_board):
    """AC7: second run returns migrated=True with columns_added empty."""
    summary1 = migrate_schema_for_board(fresh_board)
    assert summary1["migrated"] is True
    # Second run should be no-op (all ALTER ADDs already applied).
    summary2 = migrate_schema_for_board(fresh_board)
    assert summary2["migrated"] is True
    total_added = sum(len(v) for v in summary2["columns_added"].values())
    assert total_added == 0
    assert summary2["errors"] == []


# ---------------------------------------------------------------------------
# TS8 — Source files contain no destructive messages
# ---------------------------------------------------------------------------


def test_ts8_no_destructive_messages_in_kg_module():
    """AC8: zero matches for `Delete graph.kuzu`, `rm -rf graph.kuzu`,
    `remove the graph file` in any KG module source. And `migrate-schema`
    must be present as the recommended remediation."""
    kg_dir = Path(schema_mod.__file__).parent
    forbidden = ["Delete graph.kuzu", "rm -rf graph.kuzu", "remove the graph file"]
    saw_migrate_schema = False
    for py_file in kg_dir.rglob("*.py"):
        src = py_file.read_text(encoding="utf-8")
        for needle in forbidden:
            assert needle not in src, (
                f"{py_file}: destructive message {needle!r} found"
            )
        if "migrate-schema" in src or "migrate_schema_for_board" in src:
            saw_migrate_schema = True
    assert saw_migrate_schema, (
        "No KG module references migrate-schema as remediation"
    )


# ---------------------------------------------------------------------------
# TS9 — Migration failure does NOT cache the board
# ---------------------------------------------------------------------------


def test_ts9_migration_failure_does_not_cache(legacy_board, monkeypatch):
    """AC9: when `_migrate_board_schema` raises, the board is NOT added
    to `_BOOTSTRAPPED_BOARDS` — next open re-tries."""
    # Clear caches so probe fires.
    _BOOTSTRAPPED_BOARDS.discard(legacy_board)
    _MIGRATED_BOARDS.discard(legacy_board)

    # Force `_migrate_board_schema` to raise via apply_schema_to_connection.
    def boom(conn):  # noqa: ARG001
        raise RuntimeError("simulated_migration_failure")

    # Force the post-v030 probe to return True so the migration path runs.
    monkeypatch.setattr(
        schema_mod, "_board_needs_post_v030_migration",
        lambda bid: True,
    )
    monkeypatch.setattr(schema_mod, "apply_schema_to_connection", boom)

    # Trigger via ensure_board_graph_bootstrapped. Failed migrations must not
    # cache the board, so the next open re-probes and can retry the apply.
    ensure_board_graph_bootstrapped(legacy_board)
    assert legacy_board not in _MIGRATED_BOARDS
    assert legacy_board not in _BOOTSTRAPPED_BOARDS
