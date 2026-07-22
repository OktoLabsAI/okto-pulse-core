"""Focused contract tests for typed consolidation-audit lookup."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.interfaces.audit_dtos import (
    ConsolidationAuditData,
    OutboxEventData,
)
from okto_pulse.core.kg.primitives import begin_consolidation, compute_content_hash
from okto_pulse.core.kg.providers.testing.memory_audit_repo import (
    InMemoryAuditRepository,
)
from okto_pulse.core.kg.schemas import BeginConsolidationRequest
from kg_registry_testing import configure_test_kg_registry


@pytest.mark.asyncio
async def test_memory_lookup_scopes_same_id_by_artifact_type() -> None:
    repository = InMemoryAuditRepository()
    board_id = "board-typed-audit"
    artifact_id = "00000000-0000-4000-8000-000000000123"
    started_at = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)

    async def _commit(artifact_type: str, sequence: int) -> None:
        session_id = f"session-{artifact_type}"
        await repository.stage_consolidation_records(
            object(),
            ConsolidationAuditData(
                session_id=session_id,
                board_id=board_id,
                artifact_id=artifact_id,
                artifact_type=artifact_type,
                agent_id="agent-typed-audit",
                started_at=started_at,
                committed_at=started_at + timedelta(seconds=sequence),
                content_hash=f"hash-{artifact_type}",
            ),
            [],
            OutboxEventData(
                event_id=f"event-{artifact_type}",
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"artifact_type": artifact_type},
            ),
        )

    await _commit("spec", 1)
    await _commit("task", 2)

    with pytest.raises(TypeError):
        await repository.get_latest_for_artifact(board_id, artifact_id)

    spec_latest = await repository.get_latest_for_artifact(
        board_id,
        artifact_id,
        artifact_type="spec",
    )
    task_latest = await repository.get_latest_for_artifact(
        board_id,
        artifact_id,
        artifact_type="task",
    )

    assert spec_latest is not None
    assert spec_latest.artifact_type == "spec"
    assert spec_latest.content_hash == "hash-spec"
    assert task_latest is not None
    assert task_latest.artifact_type == "task"
    assert task_latest.content_hash == "hash-task"


@pytest.mark.asyncio
async def test_nothing_changed_never_crosses_artifact_type_for_same_id() -> None:
    repository = InMemoryAuditRepository()
    configure_test_kg_registry(graph_provider="inmemory", audit_repo=repository)
    board_id = "board-cross-type-dedup"
    artifact_id = "00000000-0000-4000-8000-000000000456"
    raw_content = "identical bytes across two artifact types"
    now = datetime.now(timezone.utc)

    await repository.stage_consolidation_records(
        object(),
        ConsolidationAuditData(
            session_id="session-spec-cross-type",
            board_id=board_id,
            artifact_id=artifact_id,
            artifact_type="spec",
            agent_id="agent-cross-type",
            started_at=now,
            committed_at=now,
            content_hash=compute_content_hash(raw_content, artifact_id, board_id),
        ),
        [],
        OutboxEventData(
            event_id="event-spec-cross-type",
            board_id=board_id,
            session_id="session-spec-cross-type",
            event_type="consolidation_committed",
            payload={},
        ),
    )

    result = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="task",
            artifact_id=artifact_id,
            raw_content=raw_content,
        ),
        agent_id="agent-cross-type",
    )

    assert result.nothing_changed is False
    assert result.previous_session_id is None
