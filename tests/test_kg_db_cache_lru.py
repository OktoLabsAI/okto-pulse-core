"""Cap LRU do cache de Databases Kùzu (campo 2026-06-10).

Cada kuzu.Database aloca um buffer pool de até 512MB; sem o close-por-commit
(KGDL.01), todo board visitado ficava aberto para sempre — um backfill
multi-board acumulou 7+ buffer pools e o processo morreu por exaustão de
memória nativa. O cache agora evicta o Database menos-recentemente-usado ao
exceder o cap (env KG_DB_CACHE_CAP, default 4), drenando leitores via close
guard antes de fechar.
"""

from __future__ import annotations

import os

import pytest

from kg_schema_testing import (
    bootstrap_board_graph,
    board_kuzu_path,
    close_all_connections,
    open_board_connection,
)

kg_runtime = pytest.importorskip(
    "okto_pulse.community.adapters.kg_runtime",
    reason="AF-04 Community integration test requires the Community KG runtime adapter.",
)


@pytest.fixture
def lru_boards(monkeypatch):
    monkeypatch.setenv("KG_DB_CACHE_CAP", "2")
    bids = [f"board-lru-{i}-{os.urandom(2).hex()}" for i in range(3)]
    # Esvazia o cache para o teste medir só os boards dele.
    close_all_connections()
    for bid in bids:
        bootstrap_board_graph(bid)
    yield bids
    monkeypatch.delenv("KG_DB_CACHE_CAP", raising=False)
    close_all_connections()


def _cached_keys() -> set[str]:
    with kg_runtime._board_db_cache_lock:
        return set(kg_runtime._board_db_cache.keys())


def test_lru_eviction_respects_cap_and_reopens(lru_boards):
    b0, b1, b2 = lru_boards
    # O bootstrap dos 3 boards já passou pelo cache: cap=2 deve valer.
    assert len(_cached_keys()) <= 2, "cache excedeu o cap apos bootstraps"

    # Acessa b0 (recarrega se foi evicted) e em seguida b1 e b2: o cap deve
    # continuar valendo e o LRU (b0 ou b1, conforme a ordem) sai.
    for bid in (b0, b1, b2):
        with open_board_connection(bid) as (_db, conn):
            res = conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
            assert int(res.get_next()[0]) >= 1
        assert len(_cached_keys()) <= 2

    # O board mais antigo foi evicted; reabrir funciona (cache miss → open).
    evicted = str(board_kuzu_path(b0))
    if evicted not in _cached_keys():
        with open_board_connection(b0) as (_db, conn):
            res = conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
            assert int(res.get_next()[0]) >= 1


def test_active_reader_survives_eviction_of_other_board(lru_boards):
    b0, b1, b2 = lru_boards
    # Leitor ativo em b2 enquanto abre b0 e b1 (forçando evictions): o leitor
    # de b2 não pode quebrar (o guard drena por board; boards distintos não
    # se bloqueiam — e se b2 for o LRU, o guard espera este leitor sair).
    with open_board_connection(b2) as (_db, conn):
        before = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        for bid in (b0, b1):
            with open_board_connection(bid) as (_db2, conn2):
                conn2.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        after = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        assert after == before
