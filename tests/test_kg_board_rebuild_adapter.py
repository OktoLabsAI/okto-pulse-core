"""Tests for rebuild source ingestion into ConsolidationQueue."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from okto_pulse.core.kg.board_rebuild_adapter import BoardRebuildIngestionAdapter
from okto_pulse.core.kg.rebuild_service import RebuildStepInput


def test_enqueue_sources_maps_task_test_bug_to_card_and_skips_refinement(
    tmp_path: Path,
) -> None:
    """Refinement is semantic-only. task/test/bug sources are deterministic
    card-derived rows and must be queued through the legacy worker artifact
    type ``card``."""

    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE consolidation_queue ("
            "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
            "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
            "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
            "claim_timeout_at TEXT, next_retry_at TEXT)"
        )
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(db_path=db_path)
    counts = adapter.enqueue_sources(
        board_id="b1",
        run_id="run-1",
        sources=[
            {"artifact_type": "spec", "id": "s1"},
            {"artifact_type": "refinement", "id": "r1"},
            {"artifact_type": "task", "id": "t1"},
            {"artifact_type": "test", "id": "tc1"},
            {"artifact_type": "bug", "id": "bug1"},
            {"artifact_type": "ideation", "id": "i1"},
        ],
    )

    assert counts == {"inserted": 4, "reset_to_pending": 0, "left_alone": 0}
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT artifact_type, artifact_id, priority FROM consolidation_queue "
            "ORDER BY artifact_type, artifact_id"
        ).fetchall()
    assert rows == [
        ("card", "bug1", "high"),
        ("card", "t1", "high"),
        ("card", "tc1", "high"),
        ("spec", "s1", "high"),
    ]


def test_enqueue_sources_resets_existing_pending_failures_for_new_rebuild(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE consolidation_queue ("
            "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
            "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
            "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
            "claim_timeout_at TEXT, next_retry_at TEXT)"
        )
        conn.execute(
            "INSERT INTO consolidation_queue "
            "(id, board_id, artifact_type, artifact_id, priority, source, status, "
            "triggered_at, attempts, last_error, claimed_by_session_id, claimed_at, "
            "worker_id, claim_timeout_at, next_retry_at) "
            "VALUES ('q1', 'b1', 'spec', 's1', 'low', 'old-rebuild', "
            "'pending', datetime('now'), 4, 'corrupt graph', 'old-worker', "
            "'2026-05-27 10:00:00', 'old-worker', '2026-05-27 10:30:00', "
            "'2026-05-27 11:00:00')"
        )
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(db_path=db_path)
    counts = adapter.enqueue_sources(
        board_id="b1",
        run_id="new-manifest",
        sources=[{"artifact_type": "spec", "id": "s1"}],
    )

    assert counts == {"inserted": 0, "reset_to_pending": 1, "left_alone": 0}
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT status, attempts, last_error, claimed_by_session_id, "
            "claimed_at, worker_id, claim_timeout_at, next_retry_at, priority, source "
            "FROM consolidation_queue WHERE id='q1'"
        ).fetchone()
    assert row == (
        "pending",
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        "high",
        "rebuild:new-manifest",
    )


def test_prepare_board_graph_storage_quarantines_existing_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import schema

    graph = tmp_path / "boards" / "b1" / "graph.lbug"
    graph.parent.mkdir(parents=True)
    graph.write_text("graph", encoding="utf-8")
    wal = tmp_path / "boards" / "b1" / "graph.lbug.wal"
    wal.write_text("wal", encoding="utf-8")

    def fake_purge(board_id: str, *, reason: str = "manual") -> list[str]:
        assert board_id == "b1"
        assert reason == "explicit_rebuild:test"
        moved: list[str] = []
        for path in (graph, wal):
            if path.exists():
                moved.append(str(path))
                path.unlink()
        return moved

    monkeypatch.setattr(schema, "board_kuzu_path", lambda board_id: graph)
    monkeypatch.setattr(schema, "purge_board_graph_storage", fake_purge)

    adapter = BoardRebuildIngestionAdapter(db_path=tmp_path / "pulse.db")
    moved = adapter.prepare_board_graph_storage(
        board_id="b1",
        reason="explicit_rebuild:test",
    )

    assert moved == (str(graph), str(wal))
    assert not graph.exists()
    assert not wal.exists()


def test_rebuild_step_fails_when_worker_queue_does_not_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rebuild step must not report success just because rows were queued.

    Without a running worker, the queue remains pending. The adapter must
    return ok=False so KGRebuildService does not promote a generation whose
    graph has not actually been materialized.
    """

    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE consolidation_queue ("
            "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
            "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
            "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
            "claim_timeout_at TEXT, next_retry_at TEXT)"
        )
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(
        db_path=db_path,
        drain_timeout_seconds=0.01,
        drain_poll_interval_seconds=0.01,
        drain_final_grace_seconds=0.0,
    )
    monkeypatch.setattr(
        BoardRebuildIngestionAdapter,
        "prepare_board_graph_storage",
        lambda self, **_: (),
    )
    step = adapter.build_step_adapter(
        source_resolver=lambda _req: ({"artifact_type": "spec", "id": "s1"},),
    )

    result = step(
        RebuildStepInput(
            board_id="b1",
            manifest_ref="manifest-1",
            source_set_hash="a" * 64,
            actor_id="user-1",
            operation="rebuild",
            owner_token="owner-token",
            previous_kg_generation_id=None,
            candidate_kg_generation_id="gen-1",
        )
    )

    assert result.ok is False
    assert result.detail is not None
    assert result.detail.startswith("queue_drain_timeout:")
    assert result.drilldown["queue_drain"]["idle"] is False
    assert result.drilldown["queue_drain"]["grace_applied"] is False


def test_drain_until_idle_uses_final_grace_for_nearly_drained_queue(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE consolidation_queue ("
            "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
            "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
            "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
            "claim_timeout_at TEXT, next_retry_at TEXT)"
        )
        conn.execute(
            "INSERT INTO consolidation_queue "
            "(id, board_id, artifact_type, artifact_id, priority, source, status, "
            "triggered_at, attempts) "
            "VALUES ('q1', 'b1', 'spec', 's1', 'high', 'rebuild:test', "
            "'pending', datetime('now'), 0)"
        )
        conn.commit()

    def finish_pending_row() -> None:
        time.sleep(0.6)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE consolidation_queue SET status='done' WHERE id='q1'"
            )
            conn.commit()

    worker = threading.Thread(target=finish_pending_row)
    worker.start()
    try:
        adapter = BoardRebuildIngestionAdapter(
            db_path=db_path,
            drain_final_grace_seconds=1.0,
            drain_low_depth_threshold=2,
        )
        result = adapter.drain_until_idle(
            board_id="b1",
            timeout_seconds=0.01,
            poll_interval_seconds=0.01,
        )
    finally:
        worker.join(timeout=1.0)

    assert result["idle"] is True
    assert result["final_depth"] == 0
    assert result["grace_applied"] is True
    assert result["grace_reason"] == "low_depth_near_timeout"


def test_drain_until_idle_does_not_grace_large_stuck_backlog(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE consolidation_queue ("
            "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
            "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
            "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
            "claim_timeout_at TEXT, next_retry_at TEXT)"
        )
        for idx in range(5):
            conn.execute(
                "INSERT INTO consolidation_queue "
                "(id, board_id, artifact_type, artifact_id, priority, source, status, "
                "triggered_at, attempts) "
                "VALUES (?, 'b1', 'spec', ?, 'high', 'rebuild:test', "
                "'pending', datetime('now'), 0)",
                (f"q{idx}", f"s{idx}"),
            )
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(
        db_path=db_path,
        drain_final_grace_seconds=0.5,
        drain_low_depth_threshold=2,
    )
    result = adapter.drain_until_idle(
        board_id="b1",
        timeout_seconds=0.01,
        poll_interval_seconds=0.01,
    )

    assert result["idle"] is False
    assert result["final_depth"] == 5
    assert result["grace_applied"] is False


def test_ladybug_lifecycle_reopen_probe_fails_on_unopenable_existing_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import schema

    graph = tmp_path / "boards" / "b1" / "graph.lbug"
    graph.parent.mkdir(parents=True)
    graph.write_bytes(b"not-a-valid-ladybug-graph")

    monkeypatch.setattr(schema, "board_kuzu_path", lambda board_id: graph)
    monkeypatch.setattr(schema, "close_all_connections", lambda *_: None)
    monkeypatch.setattr(schema, "close_board_db_cache", lambda *_: None)
    monkeypatch.setattr(
        schema,
        "_open_kuzu_db_path_cached",
        lambda _path: (_ for _ in ()).throw(RuntimeError("bad wal")),
    )

    result = schema.apply_ladybug_lifecycle_step(
        "b1",
        "board_graph",
        "close_reopen_probe",
    )

    assert result.ok is False
    assert result.detail is not None
    assert "bad wal" in result.detail


def test_rebuild_endpoint_wires_real_ladybug_lifecycle_adapter() -> None:
    endpoint = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "api"
        / "kg_rebuild.py"
    ).read_text(encoding="utf-8")

    assert "step_adapter=lambda b, g, s: LifecycleStepResult(ok=True)" not in endpoint
    assert "step_adapter=apply_ladybug_lifecycle_step" in endpoint
