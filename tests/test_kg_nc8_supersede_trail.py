"""MKG-D C5 — NC-8 reuse trail (scenario S7).

Real commit path: a re-consolidation whose candidate TITLE (normalized)
differs from the existing non-curated node supersedes it with a walkable
trail (generation+1); same-title content refinement stays an in-place
UPDATE; human_curated content remains untouchable.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board,
    _drive_one_session,
    _query_one_async,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


@pytest.fixture
def trail_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_nc8trail_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()

    yield base

    try:
        from kg_schema_testing import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _entities_for_ref(board_id: str, ref: str) -> list[dict]:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r "
            "RETURN n.id, n.title, n.superseded_by, n.generation "
            "ORDER BY n.id",
            {"r": ref},
        )
        rows = []
        try:
            while res.has_next():
                r = res.get_next()
                rows.append(
                    {
                        "id": r[0],
                        "title": r[1],
                        "superseded_by": r[2],
                        "generation": int(r[3]) if r[3] is not None else 0,
                    }
                )
        finally:
            try:
                res.close()
            except Exception:
                pass
        return rows


async def _entities_for_ref_async(board_id: str, ref: str) -> list[dict]:
    return await run_blocking_graph_io(
        lambda: _entities_for_ref(board_id, ref),
        task_name="tests.nc8_supersede_trail.entities_for_ref",
    )


def _count_supersedes_edge_sync(
    board_id: str,
    *,
    successor_id: str,
    predecessor_id: str,
) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, connection):
        result = connection.execute(
            "MATCH (a:Entity)-[r:supersedes]->(b:Entity) "
            "WHERE a.id = $new AND b.id = $old RETURN count(r)",
            {"new": successor_id, "old": predecessor_id},
        )
        try:
            return int(result.get_next()[0])
        finally:
            result.close()


async def test_s7_title_change_supersedes_with_trail(trail_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Titulo original"
    )
    old_id = (await _query_one_async(board_id, artifact_ref))["id"]

    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Titulo NOVO"
    )
    assert commit2.nodes_superseded == 1
    assert commit2.nodes_added == 0

    nodes = await _entities_for_ref_async(board_id, artifact_ref)
    by_id = {n["id"]: n for n in nodes}
    old = by_id[old_id]
    successor_id = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", "[MKG-D] Titulo NOVO"),
        1,
    )
    assert old["superseded_by"] == successor_id
    successor = by_id[successor_id]
    assert successor["title"] == "[MKG-D] Titulo NOVO"
    assert successor["generation"] == 1

    # Walkable trail edge exists (universal :supersedes).
    assert await run_blocking_graph_io(
        lambda: _count_supersedes_edge_sync(
            board_id,
            successor_id=successor_id,
            predecessor_id=old_id,
        ),
        task_name="tests.nc8_supersede_trail.count_edge",
    ) == 1


async def test_s7_replay_uses_active_successor_without_duplicate_pk(
    trail_tempdir, monkeypatch
):
    """An at-least-once replay must resolve generation 1, not stale generation 0.

    Both generations intentionally retain the same source_artifact_ref.  If
    NC-8 uses an unordered ``LIMIT 1``, Ladybug returns generation 0 again and
    the replay tries to CREATE the deterministic generation-1 primary key a
    second time.
    """

    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    original_title = "[MKG-D] Titulo antes do replay"
    successor_title = "[MKG-D] Titulo depois do replay"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, original_title
    )
    old_id = (await _query_one_async(board_id, artifact_ref))["id"]
    first_successor = await _drive_one_session(
        session_factory, board_id, artifact_ref, successor_title
    )
    assert first_successor.nodes_superseded == 1

    replay = await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        successor_title,
        force_add=True,
    )
    assert replay.nodes_superseded == 0
    assert replay.nodes_merged >= 1

    expected_successor_id = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", successor_title),
        1,
    )
    nodes = await _entities_for_ref_async(board_id, artifact_ref)
    assert len(nodes) == 2
    by_id = {node["id"]: node for node in nodes}
    assert by_id[old_id]["superseded_by"] == expected_successor_id
    assert by_id[expected_successor_id]["superseded_by"] is None
    assert by_id[expected_successor_id]["generation"] == 1

    assert await run_blocking_graph_io(
        lambda: _count_supersedes_edge_sync(
            board_id,
            successor_id=expected_successor_id,
            predecessor_id=old_id,
        ),
        task_name="tests.nc8_supersede_trail.count_replay_edge",
    ) == 1


async def test_s7_same_title_content_change_stays_inplace_update(
    trail_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Mesmo titulo",
        content="conteudo v1",
    )
    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Mesmo titulo",
        content="conteudo v2 refinado",
    )
    assert commit2.nodes_superseded == 0
    nodes = await _entities_for_ref_async(board_id, artifact_ref)
    assert len(nodes) == 1
    assert nodes[0]["superseded_by"] is None


async def test_s7_case_and_unicode_variants_do_not_supersede(
    trail_tempdir, monkeypatch
):
    # NFKC casefold normalization: caixa/composição não são mudança de
    # identidade (mesma regra da chave de conteúdo da MKG-A).
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Título É estável"
    )
    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] TÍTULO É ESTÁVEL"
    )
    assert commit2.nodes_superseded == 0
    assert len(await _entities_for_ref_async(board_id, artifact_ref)) == 1
