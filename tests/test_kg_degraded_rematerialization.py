"""Catch-22 fix (2026-06-10) — re-materialização de grafo vazio em recovery_needed.

Cenário de campo que motivou o fix: graph.lbug do board foi removido
manualmente; o bootstrap recriou um grafo vazio; o health classificou
``recovery_needed`` (``empty_after_materialized_history``); o gate de
degraded em ``_validate_degraded_connectivity_before_open`` bloqueava TODA
mutação nesse estado — inclusive a re-materialização, única cura da
condição. Resultado: 994 entries na DLQ, fila moendo erro para sempre, e o
fluxo oficial de rebuild (que enfileira no mesmo worker) igualmente preso.

Contrato novo (write-path):
- ``recovery_needed`` com grafo LEGÍVEL (``total_nodes`` presente — 0 ou N)
  → commit PASSA (estado efetivo ``recovery_needed_graph_readable``). Cobre
  tanto a re-materialização do grafo vazio quanto o retry após falha
  transitória de escrita (ex.: buffer manager exausto) que re-marcou
  recovery_needed com nodes presentes.
- ``recovery_needed`` sem telemetria de contagem (``total_nodes`` ausente =
  grafo ilegível) → segue bloqueado (conservador; contrato Zero-Orphan do
  teste legado preservado).
- ``quarantined`` → segue bloqueado SEMPRE, mesmo vazio.
- Commit bem-sucedido limpa falhas de write do ring buffer (self-heal) e
  invalida o cache de health do write-path.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

import okto_pulse.core.services.kg_health_service as health_service
from kg_registry_testing import configure_real_graph_test_kg_registry
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.memory_pressure import FailureEvent
from okto_pulse.core.kg.memory_pressure_collector import (
    clear_board,
    get_failures,
    record_failure,
    record_write_success,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    RECOVERY_WRITABLE_STATE,
    _resolve_commit_kg_health_state,
    commit_consolidation,
    reset_commit_health_cache_for_tests,
)
from okto_pulse.core.kg.schemas import CommitConsolidationRequest

from test_kg_primitives_connectivity_guard import (
    _begin_with_learning,
    _count_by_source_ref,
    _seed_learning_with_optional_parent,
)


@pytest.fixture(autouse=True)
def _real_graph_registry_for_rematerialization():
    configure_real_graph_test_kg_registry()


def _fake_health(state: str, total_nodes: int | None):
    async def _fake(_board_id, _db, scheduler_control=None):
        payload = {"graph_state": state, "overall_state": state}
        if total_nodes is not None:
            payload["total_nodes"] = total_nodes
        return payload

    return _fake


# ---------------------------------------------------------------------------
# Gate behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_needed_empty_graph_commit_passes_and_rematerializes(
    board_id, agent_id, db_factory, board_handle, monkeypatch,
):
    """O cenário de campo: grafo vazio + recovery_needed → o commit DEVE
    passar e materializar os nodes (é a cura do estado)."""
    # Candidato conectável: Learning ligado a um Entity existente no batch?
    # Reusa o seed do guard: Learning com parent pre-existente conectado.
    source_ref = f"learning:rematerialize:{uuid.uuid4()}"
    await run_blocking_graph_io(
        lambda: _seed_learning_with_optional_parent(
            board_id, source_ref=source_ref, connected=True
        ),
        task_name="tests.degraded_rematerialization.seed_learning",
    )

    begin = await _begin_with_learning(
        board_id, agent_id, db_factory,
        source_ref=source_ref, candidate_id="learning_rematerialize",
    )

    monkeypatch.setattr(
        health_service, "get_kg_health", _fake_health("recovery_needed", total_nodes=0),
    )
    reset_commit_health_cache_for_tests()

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    # dedup hit no seed conectado → merge conta como candidato processado;
    # o ponto central: NÃO levantou kg_graph_degraded e o grafo foi mutado.
    assert commit.processed_candidates == 1
    assert await run_blocking_graph_io(
        lambda: _count_by_source_ref(board_id, "Learning", source_ref),
        task_name="tests.degraded_rematerialization.count_learning",
    ) == 1


@pytest.mark.asyncio
async def test_recovery_needed_with_readable_graph_commit_passes(
    board_id, agent_id, db_factory, board_handle, monkeypatch,
):
    """Falha transitória de escrita re-marca recovery_needed com nodes
    presentes; o retry DEVE passar (grafo legível) — sem isso o feedback
    loop kg.commit.failed → recovery_needed → bloqueio se perpetua."""
    source_ref = f"learning:retry-after-transient:{uuid.uuid4()}"
    await run_blocking_graph_io(
        lambda: _seed_learning_with_optional_parent(
            board_id, source_ref=source_ref, connected=True
        ),
        task_name="tests.degraded_rematerialization.seed_retry_learning",
    )
    begin = await _begin_with_learning(
        board_id, agent_id, db_factory,
        source_ref=source_ref, candidate_id="learning_retry_transient",
    )
    monkeypatch.setattr(
        health_service, "get_kg_health", _fake_health("recovery_needed", total_nodes=42),
    )
    reset_commit_health_cache_for_tests()

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id, db=db,
        )
    assert commit.processed_candidates == 1
    assert await run_blocking_graph_io(
        lambda: _count_by_source_ref(board_id, "Learning", source_ref),
        task_name="tests.degraded_rematerialization.count_retry_learning",
    ) == 1


@pytest.mark.asyncio
async def test_quarantined_empty_graph_still_blocks(
    board_id, agent_id, db_factory, board_handle, monkeypatch,
):
    source_ref = f"learning:quarantined:{uuid.uuid4()}"
    begin = await _begin_with_learning(
        board_id, agent_id, db_factory,
        source_ref=source_ref, candidate_id="learning_quarantined",
    )
    monkeypatch.setattr(
        health_service, "get_kg_health", _fake_health("quarantined", total_nodes=0),
    )
    reset_commit_health_cache_for_tests()

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id, db=db,
            )
    assert exc_info.value.code == "kg_graph_degraded"


@pytest.mark.asyncio
async def test_recovery_needed_without_total_nodes_telemetry_blocks(
    board_id, db_factory, monkeypatch,
):
    """Payload sem total_nodes = contagem desconhecida → conservador."""
    monkeypatch.setattr(
        health_service, "get_kg_health", _fake_health("recovery_needed", total_nodes=None),
    )
    reset_commit_health_cache_for_tests()
    async with db_factory() as db:
        state = await _resolve_commit_kg_health_state(board_id, db)
    assert state == "recovery_needed"


@pytest.mark.asyncio
async def test_resolver_returns_permissive_state_for_empty_graph(
    board_id, db_factory, monkeypatch,
):
    monkeypatch.setattr(
        health_service, "get_kg_health", _fake_health("recovery_needed", total_nodes=0),
    )
    reset_commit_health_cache_for_tests()
    async with db_factory() as db:
        state = await _resolve_commit_kg_health_state(board_id, db)
    assert state == RECOVERY_WRITABLE_STATE
    from okto_pulse.core.kg.connectivity_guard import DEGRADED_KG_STATES

    assert state not in DEGRADED_KG_STATES


# ---------------------------------------------------------------------------
# Health cache (write-path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_health_state_is_cached_within_ttl(
    board_id, db_factory, monkeypatch,
):
    calls = {"n": 0}

    async def _counting(_board_id, _db, scheduler_control=None):
        calls["n"] += 1
        return {"overall_state": "healthy", "total_nodes": 7}

    monkeypatch.setattr(health_service, "get_kg_health", _counting)
    reset_commit_health_cache_for_tests()

    async with db_factory() as db:
        s1 = await _resolve_commit_kg_health_state(board_id, db)
        s2 = await _resolve_commit_kg_health_state(board_id, db)
        s3 = await _resolve_commit_kg_health_state(board_id, db)
    assert (s1, s2, s3) == ("healthy", "healthy", "healthy")
    assert calls["n"] == 1, "health deve ser computado 1x dentro do TTL"

    reset_commit_health_cache_for_tests(board_id)
    async with db_factory() as db:
        await _resolve_commit_kg_health_state(board_id, db)
    assert calls["n"] == 2, "reset deve invalidar o cache do board"


# ---------------------------------------------------------------------------
# Ring buffer self-heal
# ---------------------------------------------------------------------------


def _failure(kind: str) -> FailureEvent:
    return FailureEvent(
        timestamp=datetime.now(timezone.utc),
        event_kind=kind,
        graph_type="board",
        correlation_id=uuid.uuid4().hex,
    )


def test_record_write_success_clears_only_write_failures():
    bid = f"board-selfheal-{uuid.uuid4().hex[:8]}"
    clear_board(bid)
    record_failure(bid, _failure("kg.commit.failed"))
    record_failure(bid, _failure("kg.wal.flush.failed"))
    record_failure(bid, _failure("kg.buffer.exhausted"))  # não-write: preserva
    assert len(get_failures(bid)) == 3

    record_write_success(bid)

    survivors = get_failures(bid)
    assert [e.event_kind for e in survivors] == ["kg.buffer.exhausted"]
    clear_board(bid)


def test_record_write_success_noop_when_no_failures():
    bid = f"board-selfheal-{uuid.uuid4().hex[:8]}"
    clear_board(bid)
    record_write_success(bid)  # não levanta
    assert get_failures(bid) == []
