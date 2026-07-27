"""MKG-D C1 — embedding guard in the physical open path (scenarios S1/S2/S3).

Runs against a real throwaway graph via the community kg_runtime, with the
effective provider metadata controlled by monkeypatching
``_effective_embedding_meta`` (the seam the guard reads).
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

import okto_pulse.community.adapters.kg_runtime as kg_runtime
from okto_pulse.core.kg.embedding_guard import EmbeddingIncompatibleError

from kg_schema_testing import close_all_connections


def _meta(model, dim, *, stub=False):
    return {"model_name": model, "embedding_dimension": dim, "is_stub": stub}


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_embguard_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    kg_runtime.reset_bootstrap_cache_for_tests()
    yield base
    try:
        close_all_connections()
    except Exception:
        pass
    kg_runtime.reset_bootstrap_cache_for_tests()
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _read_stamp(board_id: str):
    with kg_runtime.registered_raw_connection(board_id) as (_db, conn):
        return kg_runtime._read_board_meta_embedding(conn, board_id)


def test_s1_first_open_stamps_and_compatible_reopen_passes(
    kg_tempdir, monkeypatch
):
    board_id = str(uuid.uuid4())
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("all-MiniLM-L6-v2", 384),
    )
    kg_runtime.ensure_board_graph_bootstrapped(board_id)
    model, dim = _read_stamp(board_id)
    assert (model, dim) == ("all-MiniLM-L6-v2", 384)

    # Re-open with identical provider: passes and stamp unchanged.
    kg_runtime.reset_bootstrap_cache_for_tests()
    kg_runtime.ensure_board_graph_bootstrapped(board_id)
    assert _read_stamp(board_id) == ("all-MiniLM-L6-v2", 384)


def test_s1_rebootstrap_preserves_stamp(kg_tempdir, monkeypatch):
    board_id = str(uuid.uuid4())
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("all-MiniLM-L6-v2", 384),
    )
    kg_runtime.ensure_board_graph_bootstrapped(board_id)
    # Re-run the full bootstrap (DELETE+CREATE of BoardMeta) — the stamp
    # must survive the re-bootstrap (risk R4).
    kg_runtime.bootstrap_board_graph(board_id)
    assert _read_stamp(board_id) == ("all-MiniLM-L6-v2", 384)


def test_s2_confirmed_mismatch_refuses_open_and_preserves_stamp(
    kg_tempdir, monkeypatch
):
    board_id = str(uuid.uuid4())
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("all-MiniLM-L6-v2", 384),
    )
    kg_runtime.ensure_board_graph_bootstrapped(board_id)

    # Swap the effective provider (model AND dimension differ).
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("tenant-model", 1536),
    )
    kg_runtime.reset_bootstrap_cache_for_tests()
    with pytest.raises(EmbeddingIncompatibleError) as excinfo:
        kg_runtime.ensure_board_graph_bootstrapped(board_id)
    err = excinfo.value
    assert err.code == "kg_embedding_incompatible"
    assert err.persisted_model == "all-MiniLM-L6-v2"
    assert err.persisted_dimension == 384
    assert err.effective_model == "tenant-model"
    assert err.effective_dimension == 1536
    assert err.remediation

    # Stamp preserved; restoring the original provider opens again
    # (mismatch must never be cached as bootstrapped).
    assert _read_stamp(board_id) == ("all-MiniLM-L6-v2", 384)
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("all-MiniLM-L6-v2", 384),
    )
    kg_runtime.ensure_board_graph_bootstrapped(board_id)


def test_s3_indeterminate_stub_passes_without_restamping(
    kg_tempdir, monkeypatch
):
    board_id = str(uuid.uuid4())
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta("all-MiniLM-L6-v2", 384),
    )
    kg_runtime.ensure_board_graph_bootstrapped(board_id)

    # Stub provider (indeterminate): open passes, stamp intact.
    monkeypatch.setattr(
        kg_runtime,
        "_effective_embedding_meta",
        lambda: _meta(None, 0, stub=True),
    )
    kg_runtime.reset_bootstrap_cache_for_tests()
    kg_runtime.ensure_board_graph_bootstrapped(board_id)
    assert _read_stamp(board_id) == ("all-MiniLM-L6-v2", 384)
