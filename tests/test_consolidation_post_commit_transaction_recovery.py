from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.kg import canonical_debt_replay
from okto_pulse.core.kg import canonical_learning_partition


class _RollbackPort:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self._events = events
        self._fail = fail

    async def rollback(self, context: object) -> None:
        assert context is _DB
        self._events.append("rollback")
        if self._fail:
            raise RuntimeError("rollback unavailable")


_DB = object()
_ENTRY = SimpleNamespace(
    board_id="board-transaction-recovery",
    artifact_type="card",
    artifact_id="card-transaction-recovery",
)


@pytest.mark.parametrize(
    ("failed_stage", "expected_events"),
    (
        ("debt", ["debt", "rollback", "partition", "replay"]),
        ("partition", ["debt", "partition", "rollback", "replay"]),
        ("replay", ["debt", "partition", "replay", "rollback"]),
    ),
)
@pytest.mark.asyncio
async def test_each_post_commit_failure_rolls_back_before_continuing(
    monkeypatch: pytest.MonkeyPatch,
    failed_stage: str,
    expected_events: list[str],
) -> None:
    events: list[str] = []

    async def debt(*_args, **_kwargs) -> dict[str, int]:
        events.append("debt")
        if failed_stage == "debt":
            raise RuntimeError("debt flush failed")
        return {"committed_count": 0}

    async def partition(*_args, **_kwargs) -> dict[str, int]:
        events.append("partition")
        if failed_stage == "partition":
            raise RuntimeError("partition flush failed")
        return {"opened": 0, "closed": 0}

    async def replay(*_args, **_kwargs) -> dict[str, int]:
        events.append("replay")
        if failed_stage == "replay":
            raise RuntimeError("replay flush failed")
        return {"committed_count": 0}

    monkeypatch.setattr(
        consolidation,
        "mark_canonical_debt_committed_for_artifact",
        debt,
    )
    monkeypatch.setattr(
        canonical_learning_partition,
        "run_canonical_learning_partition_maintenance",
        partition,
    )
    monkeypatch.setattr(
        canonical_debt_replay,
        "replay_canonical_debt_by_maturity",
        replay,
    )
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: _RollbackPort(events),
    )

    await consolidation._run_post_commit_maintenance(
        _DB,
        entry=_ENTRY,
        session_id="kgses_transaction_recovery",
    )

    assert events == expected_events


@pytest.mark.asyncio
async def test_post_commit_rollback_failure_escapes_to_outer_session_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    async def debt(*_args, **_kwargs) -> dict[str, int]:
        events.append("debt")
        raise RuntimeError("debt flush failed")

    async def must_not_continue(*_args, **_kwargs) -> dict[str, int]:
        events.append("unexpected")
        return {"committed_count": 0}

    monkeypatch.setattr(
        consolidation,
        "mark_canonical_debt_committed_for_artifact",
        debt,
    )
    monkeypatch.setattr(
        canonical_learning_partition,
        "run_canonical_learning_partition_maintenance",
        must_not_continue,
    )
    monkeypatch.setattr(
        canonical_debt_replay,
        "replay_canonical_debt_by_maturity",
        must_not_continue,
    )
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: _RollbackPort(events, fail=True),
    )

    with pytest.raises(RuntimeError, match="rollback unavailable"):
        await consolidation._run_post_commit_maintenance(
            _DB,
            entry=_ENTRY,
            session_id="kgses_transaction_recovery",
        )

    assert events == ["debt", "rollback"]
