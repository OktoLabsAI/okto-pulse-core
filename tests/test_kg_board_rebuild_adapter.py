"""Tests for rebuild source ingestion into ConsolidationQueue."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from okto_pulse.core.kg.interfaces.graph_lifecycle import PurgeReport
from okto_pulse.core.kg.rebuild_service import RebuildStepInput

_board_rebuild_ingestion = pytest.importorskip(
    "okto_pulse.community.adapters.board_rebuild_ingestion",
    reason="AF-04 Community integration test requires the Community rebuild ingestion adapter.",
)
BoardRebuildIngestionAdapter = _board_rebuild_ingestion.BoardRebuildIngestionAdapter
CONFIRMATION_REF = "conf_fp_" + ("a" * 64)


def _noop_purge_report(self, *, board_id: str, reason: str) -> PurgeReport:
    return PurgeReport(board_id=board_id, status="noop", reason=reason)


def _create_consolidation_queue(conn: sqlite3.Connection) -> None:
    """Create the governed multi-kind queue shape used by current runtime."""

    conn.execute(
        "CREATE TABLE consolidation_queue ("
        "id TEXT PRIMARY KEY, board_id TEXT, artifact_type TEXT, artifact_id TEXT, "
        "priority TEXT, source TEXT, status TEXT, triggered_at TEXT, attempts INTEGER, "
        "last_error TEXT, claimed_by_session_id TEXT, claimed_at TEXT, worker_id TEXT, "
        "claim_timeout_at TEXT, next_retry_at TEXT, claim_token TEXT, "
        "triggered_by_event TEXT, payload TEXT, "
        "work_kind TEXT NOT NULL DEFAULT 'consolidate', "
        "generation INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX uq_queue_consolidate_board_artifact "
        "ON consolidation_queue(board_id, artifact_type, artifact_id) "
        "WHERE work_kind='consolidate'"
    )


def _enqueue_counts(**overrides: int) -> dict[str, int]:
    counts = {
        "inserted": 0,
        "reset_to_pending": 0,
        "reordered_pending": 0,
        "fenced_claimed": 0,
        "deferred_unrelated": 0,
        "preserved_live_intent": 0,
        "left_alone": 0,
    }
    counts.update(overrides)
    return counts


def test_enqueue_sources_maps_cards_and_preserves_working_artifacts(
    tmp_path: Path,
) -> None:
    """task/test/bug sources are card-derived rows and must be queued
    through the legacy worker artifact type ``card``. Pre-spec working
    artifacts stay first-class so explicit rebuild restores working graph
    lineage instead of rebuilding an empty board."""

    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
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
            {"artifact_type": "story", "id": "st1"},
            {"artifact_type": "sprint", "id": "sp1"},
            {"artifact_type": "decision", "id": "d1"},
        ],
    )

    assert counts == _enqueue_counts(inserted=8)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT artifact_type, artifact_id, priority FROM consolidation_queue "
            "ORDER BY artifact_type, artifact_id"
        ).fetchall()
    assert rows == [
        ("card", "bug1", "high"),
        ("card", "t1", "high"),
        ("card", "tc1", "high"),
        ("ideation", "i1", "high"),
        ("refinement", "r1", "high"),
        ("spec", "s1", "high"),
        ("sprint", "sp1", "high"),
        ("story", "st1", "high"),
    ]


@pytest.mark.parametrize(
    ("status", "adoption_counter"),
    (("pending", "reordered_pending"), ("claimed", "fenced_claimed")),
)
def test_enqueue_sources_adopts_active_rows_for_new_rebuild(
    tmp_path: Path,
    status: str,
    adoption_counter: str,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
        conn.execute(
            "INSERT INTO consolidation_queue "
            "(id, board_id, artifact_type, artifact_id, priority, source, status, "
            "triggered_at, attempts, last_error, claimed_by_session_id, claimed_at, "
            "worker_id, claim_timeout_at, next_retry_at) "
            "VALUES ('q1', 'b1', 'spec', 's1', 'low', 'old-rebuild', "
            "?, datetime('now'), 4, 'corrupt graph', 'old-session', "
            "'2026-05-27 10:00:00', 'old-worker', '2026-05-27 10:30:00', "
            "'2026-05-27 11:00:00')",
            (status,),
        )
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(db_path=db_path)
    counts = adapter.enqueue_sources(
        board_id="b1",
        run_id="new-manifest",
        sources=[{"artifact_type": "spec", "id": "s1"}],
    )

    assert counts == _enqueue_counts(
        **{adoption_counter: 1, "preserved_live_intent": 1}
    )
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT status, attempts, last_error, claimed_by_session_id, "
            "claimed_at, worker_id, claim_timeout_at, next_retry_at, priority, source, "
            "payload "
            "FROM consolidation_queue WHERE id='q1'"
        ).fetchone()
    assert row is not None
    assert row[:-1] == (
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
    assert json.loads(row[-1])["_rebuild_deferred_live"] == {
        "source": "old-rebuild",
        "triggered_by_event": None,
        "payload": None,
    }


def test_enqueue_sources_preserves_row_claimed_after_source_enumeration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
        conn.execute(
            "INSERT INTO consolidation_queue "
            "(id, board_id, artifact_type, artifact_id, priority, source, status, "
            "triggered_at, attempts, last_error, claimed_by_session_id, claimed_at, "
            "worker_id, claim_timeout_at, next_retry_at) "
            "VALUES ('q1', 'b1', 'spec', 's1', 'low', 'old-rebuild', "
            "'pending', datetime('now'), 4, 'corrupt graph', NULL, "
            "NULL, NULL, NULL, '2026-05-27 11:00:00')"
        )
        conn.commit()

    # The rebuild source set was already enumerated while the queue row was
    # pending. A worker then claims the same row before enqueue_sources writes.
    enumerated_sources = [{"artifact_type": "spec", "id": "s1"}]
    claim_started = threading.Event()
    claim_done = threading.Event()
    worker_errors: list[BaseException] = []

    def claim_pending_row() -> None:
        claim_started.wait(timeout=1.0)
        try:
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    "UPDATE consolidation_queue SET "
                    "status='claimed', claimed_by_session_id='race-session', "
                    "claimed_at='2026-05-27 10:00:00', worker_id='race-worker', "
                    "claim_timeout_at='2026-05-27 10:30:00' "
                    "WHERE id='q1'"
                )
                conn.commit()
        except BaseException as exc:  # pragma: no cover - surfaced below.
            worker_errors.append(exc)
        finally:
            claim_done.set()

    worker = threading.Thread(target=claim_pending_row)
    worker.start()
    try:
        claim_started.set()
        assert claim_done.wait(timeout=1.0)
        assert worker_errors == []

        adapter = BoardRebuildIngestionAdapter(db_path=db_path)
        counts = adapter.enqueue_sources(
            board_id="b1",
            run_id="new-manifest",
            sources=enumerated_sources,
        )
    finally:
        worker.join(timeout=1.0)

    assert counts == _enqueue_counts(fenced_claimed=1, preserved_live_intent=1)
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT status, attempts, last_error, claimed_by_session_id, "
            "claimed_at, worker_id, claim_timeout_at, next_retry_at, priority, source, "
            "payload "
            "FROM consolidation_queue WHERE id='q1'"
        ).fetchone()
    assert row is not None
    assert row[:-1] == (
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
    assert json.loads(row[-1])["_rebuild_deferred_live"] == {
        "source": "old-rebuild",
        "triggered_by_event": None,
        "payload": None,
    }


def test_enqueue_sources_resets_terminal_rows_for_new_rebuild(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
        conn.execute(
            "INSERT INTO consolidation_queue "
            "(id, board_id, artifact_type, artifact_id, priority, source, status, "
            "triggered_at, attempts, last_error, claimed_by_session_id, claimed_at, "
            "worker_id, claim_timeout_at, next_retry_at) "
            "VALUES ('q1', 'b1', 'spec', 's1', 'low', 'old-rebuild', "
            "'failed', datetime('now'), 4, 'corrupt graph', 'old-session', "
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

    assert counts == _enqueue_counts(reset_to_pending=1)
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


def test_enqueue_sources_docstring_documents_active_row_contract() -> None:
    doc = BoardRebuildIngestionAdapter.enqueue_sources.__doc__ or ""
    lowered = " ".join(doc.lower().split())

    assert "pending target rows are adopted and re-enqueued" in lowered
    assert "claimed target rows have their exact claim token revoked" in lowered
    assert "live intent carried by an adopted row is preserved" in lowered
    assert "terminal/retryable rows are reset" in lowered


def test_prepare_board_graph_storage_quarantines_existing_graph(
    tmp_path: Path,
) -> None:
    graph = tmp_path / "boards" / "b1" / "graph.lbug"
    graph.parent.mkdir(parents=True)
    graph.write_text("graph", encoding="utf-8")
    wal = tmp_path / "boards" / "b1" / "graph.lbug.wal"
    wal.write_text("wal", encoding="utf-8")

    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.interfaces.graph_lifecycle import PurgeReport
    from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
    import okto_pulse.community.adapters.kg_runtime as kg_runtime

    class _Lifecycle:
        async def purge(self, board_id: str, *, reason: str) -> PurgeReport:
            assert board_id == "b1"
            assert reason == "explicit_rebuild:test"
            moved: list[str] = []
            for path in (graph, wal):
                if path.exists():
                    moved.append(str(path))
                    path.unlink()
            return PurgeReport(
                board_id=board_id,
                status="purged",
                reason=reason,
                affected_storage_refs=tuple(
                    StorageRef(path, "test_graph") for path in moved
                ),
                quarantined=True,
            )

    configure_test_kg_registry()
    get_kg_registry().graph_lifecycle = _Lifecycle()
    original_board_path = kg_runtime.board_kuzu_path
    kg_runtime.board_kuzu_path = lambda board_id: graph

    try:
        adapter = BoardRebuildIngestionAdapter(db_path=tmp_path / "pulse.db")
        moved = adapter.prepare_board_graph_storage(
            board_id="b1",
            reason="explicit_rebuild:test",
        )
    finally:
        kg_runtime.board_kuzu_path = original_board_path

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
        _create_consolidation_queue(conn)
        conn.commit()

    adapter = BoardRebuildIngestionAdapter(
        db_path=db_path,
        drain_timeout_seconds=0.01,
        drain_poll_interval_seconds=0.01,
        drain_final_grace_seconds=0.0,
    )
    monkeypatch.setattr(
        BoardRebuildIngestionAdapter,
        "prepare_board_graph_storage_report",
        _noop_purge_report,
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
            authorized_confirmation_ref=CONFIRMATION_REF,
            previous_kg_generation_id=None,
            candidate_kg_generation_id="gen-1",
        )
    )

    assert result.ok is False
    assert result.detail is not None
    assert result.detail.startswith("queue_drain_timeout:")
    assert result.drilldown["queue_drain"]["idle"] is False
    assert result.drilldown["queue_drain"]["grace_applied"] is False


def test_rebuild_step_result_counts_include_enqueue_admission_buckets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import canonical_cognitive_preservation

    adapter = BoardRebuildIngestionAdapter(
        db_path=tmp_path / "pulse.db",
        drain_timeout_seconds=0.01,
        drain_hard_timeout_seconds=0.02,
        drain_poll_interval_seconds=0.001,
    )
    monkeypatch.setattr(
        canonical_cognitive_preservation,
        "snapshot_canonical_cognitive",
        lambda board_id: canonical_cognitive_preservation.CognitiveSnapshot(
            board_id=board_id,
            readable=True,
        ),
    )
    monkeypatch.setattr(
        BoardRebuildIngestionAdapter,
        "prepare_board_graph_storage_report",
        _noop_purge_report,
    )
    monkeypatch.setattr(
        BoardRebuildIngestionAdapter,
        "enqueue_sources",
        lambda self, **_: _enqueue_counts(
            inserted=2,
            reset_to_pending=3,
            reordered_pending=5,
            fenced_claimed=6,
            preserved_live_intent=7,
        ),
    )
    monkeypatch.setattr(
        BoardRebuildIngestionAdapter,
        "queue_depth",
        lambda self, _board_id: 4,
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
            authorized_confirmation_ref=CONFIRMATION_REF,
            previous_kg_generation_id=None,
            candidate_kg_generation_id="gen-1",
        )
    )

    assert result.ok is False
    assert result.counts["enqueue_inserted"] == 2
    assert result.counts["enqueue_reset_to_pending"] == 3
    assert result.counts["enqueue_reordered_pending"] == 5
    assert result.counts["enqueue_fenced_claimed"] == 6
    assert result.counts["enqueue_left_alone"] == 0
    assert result.drilldown["enqueue"]["preserved_live_intent"] == 7


def test_drain_until_idle_uses_final_grace_for_nearly_drained_queue(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
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
        _create_consolidation_queue(conn)
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


def test_drain_until_idle_renews_window_while_queue_progresses(
    tmp_path: Path,
) -> None:
    """Campo 2026-06-10: board grande (520+ sources a ~6 entries/min)
    estourava o teto fixo de 900s — rebuild failed, generation não
    promovida, cognitive pending nunca materializado (zero badges) apesar
    de o worker completar o grafo em background. ``timeout_seconds`` agora
    é janela de ESTAGNAÇÃO: enquanto a profundidade cair, o drain segue."""
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
        for idx in range(6):
            conn.execute(
                "INSERT INTO consolidation_queue "
                "(id, board_id, artifact_type, artifact_id, priority, source, status, "
                "triggered_at, attempts) "
                "VALUES (?, 'b1', 'spec', ?, 'high', 'rebuild:test', "
                "'pending', datetime('now'), 0)",
                (f"q{idx}", f"s{idx}"),
            )
        conn.commit()

    def slow_worker() -> None:
        # Drena 1 entry a cada 0.25s — mais LENTO que a stall window de
        # 0.4s mas sempre progredindo. Tempo total (1.5s) >> janela.
        for idx in range(6):
            time.sleep(0.25)
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    "UPDATE consolidation_queue SET status='done' WHERE id=?",
                    (f"q{idx}",),
                )
                conn.commit()

    worker = threading.Thread(target=slow_worker)
    worker.start()
    try:
        adapter = BoardRebuildIngestionAdapter(
            db_path=db_path,
            drain_final_grace_seconds=0.0,
        )
        result = adapter.drain_until_idle(
            board_id="b1",
            timeout_seconds=0.4,
            poll_interval_seconds=0.02,
            hard_timeout_seconds=30.0,
        )
    finally:
        worker.join(timeout=5.0)

    assert result["idle"] is True, f"drain falhou com fila progredindo: {result}"
    assert result["final_depth"] == 0
    assert result["progress_events"] >= 5
    assert result["hard_timed_out"] is False


def test_drain_until_idle_hard_ceiling_stops_endless_progress(
    tmp_path: Path,
) -> None:
    """O teto absoluto protege contra produtor que re-enfileira para
    sempre: progresso constante mas fila nunca zera → hard timeout."""
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        _create_consolidation_queue(conn)
        for idx in range(50):
            conn.execute(
                "INSERT INTO consolidation_queue "
                "(id, board_id, artifact_type, artifact_id, priority, source, status, "
                "triggered_at, attempts) "
                "VALUES (?, 'b1', 'spec', ?, 'high', 'rebuild:test', "
                "'pending', datetime('now'), 0)",
                (f"q{idx}", f"s{idx}"),
            )
        conn.commit()

    stop = threading.Event()

    def churning_worker() -> None:
        # Drena 1 e re-enfileira 1 (depth líquido cai 1 por ciclo bem
        # devagar) — progresso eterno sem nunca zerar dentro do teto.
        idx = 0
        while not stop.is_set():
            time.sleep(0.05)
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    "UPDATE consolidation_queue SET status='done' WHERE id=?",
                    (f"q{idx}",),
                )
                conn.commit()
            idx += 1
            if idx >= 20:
                break

    worker = threading.Thread(target=churning_worker)
    worker.start()
    try:
        adapter = BoardRebuildIngestionAdapter(
            db_path=db_path,
            drain_final_grace_seconds=0.0,
        )
        result = adapter.drain_until_idle(
            board_id="b1",
            timeout_seconds=0.3,
            poll_interval_seconds=0.02,
            hard_timeout_seconds=0.6,
        )
    finally:
        stop.set()
        worker.join(timeout=5.0)

    assert result["idle"] is False
    assert result["hard_timed_out"] is True


def test_ladybug_lifecycle_reopen_probe_fails_on_unopenable_existing_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.community.adapters import kg_runtime

    graph = tmp_path / "boards" / "b1" / "graph.lbug"
    graph.parent.mkdir(parents=True)
    graph.write_bytes(b"not-a-valid-ladybug-graph")

    monkeypatch.setattr(kg_runtime, "board_kuzu_path", lambda board_id: graph)
    monkeypatch.setattr(kg_runtime, "close_all_connections", lambda *_: None)
    monkeypatch.setattr(kg_runtime, "close_board_db_cache", lambda *_: None)
    monkeypatch.setattr(
        kg_runtime,
        "_open_kuzu_db_path_cached",
        lambda _path: (_ for _ in ()).throw(RuntimeError("bad wal")),
    )

    result = kg_runtime.apply_ladybug_lifecycle_step(
        "b1",
        "board_graph",
        "close_reopen_probe",
    )

    assert result.ok is False
    assert result.detail is not None
    assert "bad wal" in result.detail


def test_rebuild_endpoint_is_recovery_only_offline() -> None:
    endpoint_module = pytest.importorskip("okto_pulse.community.api.kg_rebuild")
    endpoint = Path(endpoint_module.__file__).resolve().read_text(encoding="utf-8")

    assert "step_adapter=lambda b, g, s: LifecycleStepResult(ok=True)" not in endpoint
    assert "step_adapter=resolve_graph_lifecycle().apply_step" not in endpoint
    assert '"recovery_execution_required"' in endpoint
    assert '"recovery_only_offline"' in endpoint
    assert "okto-pulse-kg-recovery-only" in endpoint
