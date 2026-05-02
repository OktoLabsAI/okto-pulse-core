"""Wave 2 NC 1ede3471 — DLQ Inspector tests (spec ed17b1fe).

Cobre TS1 (paginated shape) + TS2 (empty board) + TS3 (helper sharing
between REST + MCP) — todos exercitam `list_dead_letter_rows` direto.
"""

from __future__ import annotations

import uuid

import pytest


pytestmark = pytest.mark.asyncio


async def _insert_dlq_row(db, board_id: str, idx: int) -> str:
    """Insert one fabricated DLQ row + return its id."""
    from okto_pulse.core.models.db import ConsolidationDeadLetter
    row_id = f"dlq_test_{uuid.uuid4().hex[:8]}_{idx}"
    row = ConsolidationDeadLetter(
        id=row_id,
        board_id=board_id,
        artifact_type="spec",
        artifact_id=f"spec-{idx}-{uuid.uuid4().hex[:8]}",
        original_queue_id=f"q-{idx}",
        attempts=5,
        errors=[
            {
                "attempt": n,
                "occurred_at": "2026-04-27T10:00:00",
                "error_type": "TestError",
                "message": f"failure {n} for row {idx}",
                "traceback": None,
            }
            for n in range(1, 4)
        ],
    )
    db.add(row)
    await db.flush()
    return row_id


async def test_ts1_paginated_shape_with_three_rows():
    """TS1 — 3 DLQ rows pré-existentes em board → response retorna shape
    `{rows: [3], total: 3, limit, offset}` com cada row carregando errors[].
    """
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    board_id = f"board-ts1-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        for i in range(3):
            await _insert_dlq_row(db, board_id, i)
        await db.commit()

    async with factory() as db:
        result = await list_dead_letter_rows(
            db, board_id, limit=10, offset=0,
        )

    assert result["total"] == 3
    assert result["limit"] == 10
    assert result["offset"] == 0
    assert len(result["rows"]) == 3
    first = result["rows"][0]
    expected_keys = {
        "id", "board_id", "artifact_type", "artifact_id",
        "original_queue_id", "attempts", "errors", "dead_lettered_at",
    }
    assert set(first.keys()) >= expected_keys
    assert first["board_id"] == board_id
    assert isinstance(first["errors"], list)
    assert len(first["errors"]) == 3
    assert first["errors"][0]["attempt"] == 1


async def test_ts2_empty_board_returns_empty_rows():
    """TS2 — board sem DLQ rows retorna `{rows: [], total: 0}`.

    Não inserimos nada — apenas chamamos o service com board_id novo.
    """
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    board_id = f"board-empty-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        result = await list_dead_letter_rows(db, board_id)

    assert result == {
        "rows": [],
        "total": 0,
        "limit": 50,
        "offset": 0,
    }


async def test_ts3_helper_signature_shared_between_rest_and_mcp():
    """TS3 — helper `list_dead_letter_rows` retorna dict shape estável
    independente do caller. REST e MCP wrap o mesmo helper, garantindo
    que o payload seja idêntico.
    """
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    board_id = f"board-ts3-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        await _insert_dlq_row(db, board_id, 0)
        await db.commit()

    async with factory() as db:
        rest_payload = await list_dead_letter_rows(
            db, board_id, limit=50, offset=0,
        )

    # MCP tool faz o mesmo call + json.dumps. Verificar que dict é
    # JSON-serializable (não levanta) garante shape compatível.
    import json
    serialised = json.dumps(rest_payload, default=str)
    parsed = json.loads(serialised)
    assert parsed["total"] == 1
    assert parsed["limit"] == 50


async def test_malformed_error_payload_is_normalised():
    """A malformed legacy DLQ errors value should not break the inspector."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import ConsolidationDeadLetter
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    board_id = f"board-malformed-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        db.add(
            ConsolidationDeadLetter(
                id=f"dlq-malformed-{uuid.uuid4().hex[:8]}",
                board_id=board_id,
                artifact_type="card",
                artifact_id=f"card-{uuid.uuid4().hex[:8]}",
                original_queue_id=None,
                attempts=1,
                errors="Corrupted wal file",
            )
        )
        await db.commit()

    async with factory() as db:
        result = await list_dead_letter_rows(db, board_id)

    assert result["total"] == 1
    assert result["rows"][0]["errors"] == [
        {
            "attempt": 1,
            "occurred_at": "",
            "error_type": "LegacyError",
            "message": "Corrupted wal file",
            "traceback": None,
        }
    ]


async def test_reprocess_moves_dlq_row_back_to_queue():
    """DLQ reprocess should create one pending queue row and clear the DLQ row."""
    from sqlalchemy import select

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import (
        ConsolidationDeadLetter,
        ConsolidationQueue,
    )
    from okto_pulse.core.services.dead_letter_inspector_service import (
        reprocess_dead_letter_rows,
    )

    board_id = f"board-reprocess-{uuid.uuid4().hex[:8]}"
    artifact_id = f"spec-{uuid.uuid4().hex[:8]}"
    dlq_id = f"dlq-reprocess-{uuid.uuid4().hex[:8]}"
    factory = get_session_factory()
    async with factory() as db:
        db.add(
            ConsolidationDeadLetter(
                id=dlq_id,
                board_id=board_id,
                artifact_type="spec",
                artifact_id=artifact_id,
                original_queue_id="q-dead",
                attempts=5,
                errors=[{"attempt": 5, "message": "schema fixed"}],
            )
        )
        await db.commit()

    async with factory() as db:
        result = await reprocess_dead_letter_rows(
            db,
            board_id,
            dead_letter_ids=[dlq_id],
        )
        await db.commit()

    assert result["success"] is True
    assert result["selected"] == 1
    assert result["requeued_count"] == 1
    assert result["already_queued_count"] == 0

    async with factory() as db:
        assert await db.get(ConsolidationDeadLetter, dlq_id) is None
        queue = (
            await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "spec",
                    ConsolidationQueue.artifact_id == artifact_id,
                )
            )
        ).scalar_one()
        assert queue.status == "pending"
        assert queue.source == "dead_letter_reprocess"
        assert queue.attempts == 0
