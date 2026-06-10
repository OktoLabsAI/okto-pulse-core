"""Auto-recovery de checkpoint interrompido (campo 2026-06-10).

Crash no meio de um CHECKPOINT deixa ``graph.lbug.shadow`` (0 bytes) e
``graph.lbug.wal.checkpoint`` órfãos; o replay do Ladybug aborta com
UNREACHABLE_CODE em wal_record.cpp com o main file 100% íntegro (3926 nodes
recuperados em campo ao remover os sidecars). ``_open_kuzu_db`` agora
quarentena os sidecars (preservando evidência) e re-tenta a abertura.

Critérios de segurança testados:
- shadow com bytes > 0 → estado ambíguo → NADA é movido (fail-closed);
- main file e .wal principal nunca são tocados;
- caminho de retry: falha com marcador de corrupção + sidecars presentes →
  quarentena + segunda tentativa abre.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import ladybug
from okto_pulse.core.kg import schema
from okto_pulse.core.kg.schema import (
    _open_kuzu_db,
    _quarantine_interrupted_checkpoint_sidecars,
    board_kuzu_path,
    bootstrap_board_graph,
    close_all_connections,
)


@pytest.fixture
def icr_board():
    bid = f"board-icr-{os.urandom(3).hex()}"
    bootstrap_board_graph(bid)
    close_all_connections(bid)
    yield bid
    close_all_connections(bid)


def _quarantine_root(path: Path) -> Path:
    return path.parents[2] / "quarantine"


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


def test_moves_empty_shadow_and_wal_checkpoint(icr_board):
    path = board_kuzu_path(icr_board)
    shadow = path.parent / (path.name + ".shadow")
    ckpt = path.parent / (path.name + ".wal.checkpoint")
    shadow.write_bytes(b"")
    ckpt.write_bytes(b"garbage-checkpoint-bytes")

    moved = _quarantine_interrupted_checkpoint_sidecars(path)

    assert moved is True
    assert not shadow.exists()
    assert not ckpt.exists()
    assert path.exists(), "main file deve permanecer intocado"
    quarantined = sorted(_quarantine_root(path).glob("interrupted-checkpoint-*"))
    assert quarantined, "sidecars devem ir para a quarentena, nao ser apagados"
    latest = quarantined[-1]
    assert (latest / (path.name + ".shadow")).exists()
    assert (latest / (path.name + ".wal.checkpoint")).exists()
    assert (latest / "manifest.txt").exists()


def test_nonempty_shadow_is_also_quarantined(icr_board):
    """Campo (2ª ocorrência): shadow de 283KB de um checkpoint interrompido
    — main file íntegro (3929 nodes). O shadow só vira main na conclusão
    atômica do checkpoint, então shadow órfão de QUALQUER tamanho é lixo
    recuperável; vai para a quarentena (preservado), nunca deletado."""
    path = board_kuzu_path(icr_board)
    shadow = path.parent / (path.name + ".shadow")
    ckpt = path.parent / (path.name + ".wal.checkpoint")
    shadow.write_bytes(b"partial checkpoint payload")
    ckpt.write_bytes(b"garbage")

    moved = _quarantine_interrupted_checkpoint_sidecars(path)

    assert moved is True
    assert not shadow.exists() and not ckpt.exists()
    assert path.exists(), "main file deve permanecer intocado"
    quarantined = sorted(_quarantine_root(path).glob("interrupted-checkpoint-*"))
    latest = quarantined[-1]
    assert (latest / (path.name + ".shadow")).read_bytes() == b"partial checkpoint payload"


def test_no_sidecars_is_noop(icr_board):
    path = board_kuzu_path(icr_board)
    assert _quarantine_interrupted_checkpoint_sidecars(path) is False


# ---------------------------------------------------------------------------
# Retry path integration (simulated corruption error on first open)
# ---------------------------------------------------------------------------


def test_open_retries_after_sidecar_quarantine(icr_board, monkeypatch):
    path = board_kuzu_path(icr_board)
    shadow = path.parent / (path.name + ".shadow")
    ckpt = path.parent / (path.name + ".wal.checkpoint")
    shadow.write_bytes(b"")
    ckpt.write_bytes(b"garbage")

    real_database = ladybug.Database
    calls = {"n": 0}

    class FlakyDatabase:
        def __new__(cls, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError(
                    'Assertion failed in file "wal_record.cpp" on line 76: '
                    "UNREACHABLE_CODE"
                )
            return real_database(*args, **kwargs)

    monkeypatch.setattr(ladybug, "Database", FlakyDatabase)

    db = _open_kuzu_db(path)
    assert db is not None
    assert calls["n"] == 2, "deve re-tentar apos quarentenar os sidecars"
    assert not shadow.exists() and not ckpt.exists()
    # devolve o handle para nao vazar lock do arquivo
    db.close()
    # restaura o cache de bootstrap/board para o teardown da fixture
    schema.close_board_db_cache()
