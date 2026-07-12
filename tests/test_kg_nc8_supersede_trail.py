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

from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board,
    _drive_one_session,
    _query_one,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine():
    from okto_pulse.core.infra.database import create_database, get_engine

    prior_url = str(get_engine().url)
    yield
    if str(get_engine().url) != prior_url:
        create_database(prior_url, echo=False)


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


async def test_s7_title_change_supersedes_with_trail(trail_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Titulo original"
    )
    old_id = _query_one(board_id, artifact_ref)["id"]

    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-D] Titulo NOVO"
    )
    assert commit2.nodes_superseded == 1
    assert commit2.nodes_added == 0

    nodes = _entities_for_ref(board_id, artifact_ref)
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
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (a:Entity)-[r:supersedes]->(b:Entity) "
            "WHERE a.id = $new AND b.id = $old RETURN count(r)",
            {"new": successor_id, "old": old_id},
        )
        assert int(res.get_next()[0]) == 1
        res.close()


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
    nodes = _entities_for_ref(board_id, artifact_ref)
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
    assert len(_entities_for_ref(board_id, artifact_ref)) == 1
