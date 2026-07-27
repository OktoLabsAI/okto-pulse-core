"""MKG-B C3 — nothing_changed re-attestation uses a guarded graph write.

An IDENTICAL begin short-circuit is read-only. Propose preserves FR5
corroboration only after health/state preflights and under the real
single-writer + durability lifecycle. The full NOOP flow counts the assertion
once and keeps processing counters deterministic.
"""

from __future__ import annotations

import gc
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
    _prov_row_async,
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

    async def _healthy(_board_id, _db, scheduler_control=None):
        return {
            "overall_state": "healthy",
            "graph_state": "healthy",
            "discovery_state": "healthy",
            "total_nodes": 1,
        }

    import okto_pulse.core.services.kg_health_service as health_service

    monkeypatch.setattr(health_service, "get_kg_health", _healthy)

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


async def test_s4_identical_begin_does_not_attest_without_write_lifecycle(
    countonly_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    title, content = "[MKG-B] Fato estável", "conteudo identico"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )
    assert (await _prov_row_async(board_id, artifact_ref))["attestation_count"] == 1

    begin2 = await _begin_identical(board_id, artifact_ref, title, content)
    assert begin2.nothing_changed is True
    row = await _prov_row_async(board_id, artifact_ref)
    assert row["attestation_count"] == 1
    assert row["content"] == content

    # Repeated uncommitted re-assertions remain pure reads.
    begin3 = await _begin_identical(board_id, artifact_ref, title, content)
    assert begin3.nothing_changed is True
    assert (await _prov_row_async(board_id, artifact_ref))["attestation_count"] == 1


async def test_s4_full_noop_flow_attests_once_with_zero_counters(
    countonly_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    title, content = "[MKG-B] Fluxo completo", "conteudo identico"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )

    # Full begin→propose→commit with IDENTICAL content: begin is read-only,
    # propose performs one guarded count-only bump, and commit is a NOOP.
    commit2, _hash2 = await _drive_with_provenance(
        session_factory, board_id, artifact_ref, title, content=content
    )
    assert commit2.nodes_added == 0
    assert commit2.nodes_updated == 0
    assert commit2.nodes_merged == 0
    assert commit2.nodes_superseded == 0

    assert (await _prov_row_async(board_id, artifact_ref))["attestation_count"] == 2


async def test_s4_changed_content_does_not_mutate_attestation_at_begin(
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
    # Changed-content begin remains read-only; NC-8 belongs to guarded commit.
    assert (await _prov_row_async(board_id, artifact_ref))["attestation_count"] == 1
