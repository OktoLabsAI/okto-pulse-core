"""MKG-B C3 — count-only re-attestation on nothing_changed (scenario S4).

An IDENTICAL re-consolidation (content_hash short-circuit at begin/propose)
bumps attestation_count on the origin session's nodes without reprocessing:
zero content writes, zero re-embedding, processing counters zeroed, and the
kg.attestation.count_only structured log (OR1). A full begin→propose→commit
flow counts the re-assertion exactly ONCE (session flag dedup).
"""

from __future__ import annotations

import gc
import logging
import shutil
import tempfile
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board,
)
from test_kg_provenance_commit_fill import (  # noqa: F401  (harness reuse)
    _drive_with_provenance,
    _prov_row,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


@pytest.fixture
def countonly_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_countonly_"))
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


async def _begin_identical(board_id: str, artifact_ref: str, title: str, content: str):
    """Re-run ONLY begin_consolidation with the exact same raw_content —
    the production short-circuit path for an unchanged artifact."""
    from okto_pulse.core.kg.primitives import begin_consolidation
    from okto_pulse.core.kg.schemas import BeginConsolidationRequest

    artifact_id = artifact_ref.split(":", 1)[1]
    raw_content = f"MKG-B provenance fill — {title} :: {content}"
    return await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=artifact_id,
            raw_content=raw_content,
            deterministic_candidates=[],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )


async def test_s4_identical_begin_counts_attestation_without_reprocessing(
    countonly_tempdir, monkeypatch, caplog
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    title, content = "[MKG-B] Fato estável", "conteudo identico"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )
    assert _prov_row(board_id, artifact_ref)["attestation_count"] == 1

    with caplog.at_level(logging.INFO, logger="okto_pulse.kg.primitives"):
        begin2 = await _begin_identical(board_id, artifact_ref, title, content)
    assert begin2.nothing_changed is True
    row = _prov_row(board_id, artifact_ref)
    assert row["attestation_count"] == 2
    # Content untouched by the count-only path.
    assert row["content"] == content
    # OR1 — structured log of the count-only attestation.
    assert any(
        "kg.attestation.count_only" in rec.getMessage() for rec in caplog.records
    )

    # Repeated identical re-assertions keep accumulating (origin audit is
    # still the last committed one — begin does not write audits).
    begin3 = await _begin_identical(board_id, artifact_ref, title, content)
    assert begin3.nothing_changed is True
    assert _prov_row(board_id, artifact_ref)["attestation_count"] == 3


async def test_s4_full_flow_counts_exactly_once_with_zero_counters(
    countonly_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    title, content = "[MKG-B] Fluxo completo", "conteudo identico"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )

    # Full begin→propose→commit with IDENTICAL content: begin counts the
    # re-assertion, propose must NOT count it again (session flag), and the
    # commit short-circuits to NOOP hints — processing counters zeroed.
    commit2, _hash2 = await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )
    assert commit2.nodes_added == 0
    assert commit2.nodes_updated == 0
    assert commit2.nodes_merged == 0
    assert commit2.nodes_superseded == 0

    assert _prov_row(board_id, artifact_ref)["attestation_count"] == 2


async def test_s4_changed_content_does_not_trigger_count_only(
    countonly_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Conteudo muda", content="v1",
    )
    begin2 = await _begin_identical(
        board_id, artifact_ref, "[MKG-B] Conteudo muda", "v2 diferente"
    )
    assert begin2.nothing_changed is False
    # No count-only bump — the changed-content path belongs to NC-8 (FR4).
    assert _prov_row(board_id, artifact_ref)["attestation_count"] == 1
