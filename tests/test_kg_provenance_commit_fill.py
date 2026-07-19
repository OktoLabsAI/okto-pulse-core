"""MKG-B C2 — commit fills extraction provenance + attestation (scenario S2).

Real commit path: fresh CREATE persists the optional span/extraction fields,
seeds attestation_count=1 / last_attested_at and stamps source_content_hash
from the session; NC-8 reuse re-attests on BOTH branches (non-curated update
and curated metadata-only); a title-change trail successor restarts at 1.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.session_manager import compute_content_hash

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board,
)

pytestmark = pytest.mark.asyncio

PROV_FIELDS = dict(
    source_span_start=10,
    source_span_end=42,
    source_span_quote="trecho literal extraído",
    extraction_model_id="claude-fable-5",
    extraction_prompt_hash="sha256:promptabc",
)


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


@pytest.fixture
def prov_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_provfill_"))
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


async def _drive_with_provenance(
    session_factory,
    board_id: str,
    artifact_ref: str,
    title: str,
    content: str = "",
    provenance: dict | None = None,
    agent_id: str = "system:layer1_worker",
):
    """begin -> propose -> commit for one Entity candidate carrying the
    optional MKG-B provenance fields. Returns (commit, expected_hash)."""

    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
    )

    root_cand = NodeCandidate(
        candidate_id="provfill_technical_root",
        node_type=KGNodeType.ENTITY,
        title="Provenance fill technical root",
        content="Allowlisted deterministic source root for MKG-B tests.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    cand = NodeCandidate(
        candidate_id=f"provfill_entity_{uuid.uuid4().hex[:8]}",
        node_type=KGNodeType.ENTITY,
        title=title,
        content=content,
        source_artifact_ref=artifact_ref,
        source_confidence=0.95,
        **(provenance or {}),
    )
    artifact_id = artifact_ref.split(":", 1)[1]
    raw_content = f"MKG-B provenance fill — {title} :: {content}"
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=artifact_id,
            raw_content=raw_content,
            deterministic_candidates=[root_cand, cand],
        ),
        agent_id=agent_id,
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id=f"edge_{cand.candidate_id}_belongs_to_root",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=cand.candidate_id,
                to_candidate_id=root_cand.candidate_id,
                confidence=1.0,
            ),
        ),
        agent_id=agent_id,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
    )
    async with session_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text=f"MKG-B commit — {title}",
            ),
            agent_id=agent_id,
            db=db,
        )
    return commit, compute_content_hash(raw_content, artifact_id, board_id)


def _prov_row(board_id: str, artifact_ref: str, *, active_only: bool = True):
    from kg_schema_testing import open_board_connection

    where = "n.source_artifact_ref = $r"
    if active_only:
        where += " AND n.superseded_by IS NULL"
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            f"MATCH (n:Entity) WHERE {where} "
            "RETURN n.id, n.source_span_start, n.source_span_end, "
            "n.source_span_quote, n.extraction_model_id, "
            "n.extraction_prompt_hash, n.source_content_hash, "
            "n.attestation_count, n.last_attested_at, n.content LIMIT 1",
            {"r": artifact_ref},
        )
        try:
            row = res.get_next()
            return {
                "id": row[0],
                "source_span_start": row[1],
                "source_span_end": row[2],
                "source_span_quote": row[3],
                "extraction_model_id": row[4],
                "extraction_prompt_hash": row[5],
                "source_content_hash": row[6],
                "attestation_count": row[7],
                "last_attested_at": row[8],
                "content": row[9],
            }
        finally:
            try:
                res.close()
            except Exception:
                pass


def _set_curated(board_id: str, node_id: str) -> None:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        kconn.execute(
            "MATCH (n:Entity) WHERE n.id = $id SET n.human_curated = true",
            {"id": node_id},
        )


async def _prov_row_async(
    board_id: str,
    artifact_ref: str,
    *,
    active_only: bool = True,
):
    return await run_blocking_graph_io(
        lambda: _prov_row(board_id, artifact_ref, active_only=active_only),
        task_name="tests.provenance_commit_fill.prov_row",
    )


async def _set_curated_async(board_id: str, node_id: str) -> None:
    await run_blocking_graph_io(
        lambda: _set_curated(board_id, node_id),
        task_name="tests.provenance_commit_fill.set_curated",
    )


async def test_s2_create_fills_provenance_and_seeds_attestation(
    prov_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit, expected_hash = await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Provenance na criação", content="conteudo v1",
        provenance=PROV_FIELDS,
    )
    assert commit.nodes_added >= 1

    row = await _prov_row_async(board_id, artifact_ref)
    assert row["source_span_start"] == 10
    assert row["source_span_end"] == 42
    assert row["source_span_quote"] == "trecho literal extraído"
    assert row["extraction_model_id"] == "claude-fable-5"
    assert row["extraction_prompt_hash"] == "sha256:promptabc"
    assert row["source_content_hash"] == expected_hash
    assert row["attestation_count"] == 1
    assert row["last_attested_at"] is not None


async def test_s2_create_without_provenance_stays_null_but_seeds_attestation(
    prov_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    _commit, expected_hash = await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Sem provenance", content="conteudo",
    )
    row = await _prov_row_async(board_id, artifact_ref)
    assert row["source_span_start"] is None
    assert row["source_span_quote"] is None
    assert row["extraction_model_id"] is None
    # The session hash is stamped regardless — drift detection must work
    # for legacy extractors that don't emit spans (BR1).
    assert row["source_content_hash"] == expected_hash
    assert row["attestation_count"] == 1


async def test_s2_quote_truncated_at_500_chars(prov_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    long_quote = "q" * 600
    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Quote longa", content="conteudo",
        provenance={**PROV_FIELDS, "source_span_quote": long_quote},
    )
    row = await _prov_row_async(board_id, artifact_ref)
    assert row["source_span_quote"] == "q" * 500


async def test_s2_nc8_reuse_increments_attestation_non_curated(
    prov_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Mesmo titulo", content="conteudo v1",
    )
    first = await _prov_row_async(board_id, artifact_ref)
    assert first["attestation_count"] == 1

    commit2, hash2 = await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Mesmo titulo", content="conteudo v2 refinado",
    )
    assert commit2.nodes_superseded == 0

    row = await _prov_row_async(board_id, artifact_ref)
    assert row["id"] == first["id"]
    assert row["attestation_count"] == 2
    assert row["last_attested_at"] >= first["last_attested_at"]
    # The rewrite is a NEW assertion (FR3/D5): the provenance anchor is
    # restamped to the session that wrote the current content, so a
    # re-consolidation clears kg_provenance_drift instead of flagging the
    # node forever.
    assert row["source_content_hash"] == hash2
    assert row["source_content_hash"] != first["source_content_hash"]


async def test_s2_nc8_reuse_increments_attestation_curated_branch(
    prov_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Curado", content="conteudo humano",
    )
    first = await _prov_row_async(board_id, artifact_ref)
    await _set_curated_async(board_id, first["id"])

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Curado", content="tentativa do agente",
    )
    row = await _prov_row_async(board_id, artifact_ref)
    assert row["id"] == first["id"]
    # Curated content untouched, but the re-attestation still counts.
    assert row["content"] == "conteudo humano"
    assert row["attestation_count"] == 2


async def test_s2_trail_successor_restarts_attestation(prov_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Titulo original", content="conteudo",
    )
    commit2, hash2 = await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-B] Titulo NOVO", content="conteudo",
        provenance=PROV_FIELDS,
    )
    assert commit2.nodes_superseded == 1

    successor = await _prov_row_async(board_id, artifact_ref, active_only=True)
    assert successor["attestation_count"] == 1
    assert successor["source_content_hash"] == hash2
    assert successor["source_span_quote"] == "trecho literal extraído"
