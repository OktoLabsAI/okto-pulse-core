from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from okto_pulse.core.kg import canonical_partition_integrity
from okto_pulse.core.services import kg_health_service


@pytest.mark.asyncio
async def test_digest_overlay_uses_an_isolated_cancel_safe_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_db = object()
    events: list[str] = []

    @asynccontextmanager
    async def isolated_scope():
        events.append("enter")
        try:
            yield isolated_db
        finally:
            events.append("exit")

    async def load_overlay(db: object, *, board_id: str) -> dict[str, str]:
        assert db is isolated_db
        assert board_id == "board-overlay"
        events.append("query")
        return {"bug:1": "pending"}

    monkeypatch.setattr(kg_health_service, "cancel_safe_session", isolated_scope)
    monkeypatch.setattr(
        canonical_partition_integrity,
        "pending_or_debt_exclusions",
        load_overlay,
    )

    result = await kg_health_service._load_digest_partition_overlay(
        board_id="board-overlay"
    )

    assert result == {"bug:1": "pending"}
    assert events == ["enter", "query", "exit"]


@pytest.mark.asyncio
async def test_digest_overlay_timeout_exits_isolated_scope_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_db = object()
    events: list[str] = []

    @asynccontextmanager
    async def isolated_scope():
        events.append("enter")
        try:
            yield isolated_db
        finally:
            events.append("exit")

    async def blocked_overlay(db: object, *, board_id: str) -> dict[str, str]:
        assert db is isolated_db
        assert board_id == "board-timeout"
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise
        return {}

    monkeypatch.setattr(kg_health_service, "cancel_safe_session", isolated_scope)
    monkeypatch.setattr(
        kg_health_service,
        "_DIGEST_OVERLAY_TIMEOUT_S",
        0.001,
    )
    monkeypatch.setattr(
        canonical_partition_integrity,
        "pending_or_debt_exclusions",
        blocked_overlay,
    )

    with pytest.raises(TimeoutError):
        await kg_health_service._load_digest_partition_overlay(
            board_id="board-timeout"
        )

    assert events == ["enter", "cancelled", "exit"]
