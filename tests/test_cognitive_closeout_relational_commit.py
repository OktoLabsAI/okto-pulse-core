"""Cognitive closeout confirms relational durability before graph success."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import cognitive_closeout_production as closeout
from okto_pulse.core.kg import primitives


class _RelationalScope:
    def __init__(self, events: list[str], *, fail_commit: bool) -> None:
        self.events = events
        self.fail_commit = fail_commit

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc, _tb) -> None:
        if exc_type is not None:
            self.events.append("scope_rollback")

    async def commit(self) -> None:
        self.events.append("db_commit")
        if self.fail_commit:
            raise RuntimeError("closeout relational commit failed")


def _install_pipeline_doubles(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
) -> None:
    async def _begin(*_args, **_kwargs):
        return SimpleNamespace(session_id="session-closeout-deferred")

    async def _no_op(*_args, **_kwargs) -> None:
        return None

    async def _commit(*_args, **kwargs):
        assert kwargs["defer_session_finalization"] is True
        events.append("graph_and_relational_staged")
        return SimpleNamespace()

    async def _finalize(session_id: str, *, agent_id: str) -> None:
        assert session_id == "session-closeout-deferred"
        assert agent_id == "closeout-agent"
        events.append("finalize")

    async def _abort(session_id: str, *, agent_id: str, **_kwargs) -> None:
        assert session_id == "session-closeout-deferred"
        assert agent_id == "closeout-agent"
        events.append("abort")

    monkeypatch.setattr(primitives, "begin_consolidation", _begin)
    monkeypatch.setattr(primitives, "add_node_candidate", _no_op)
    monkeypatch.setattr(primitives, "add_edge_candidate", _no_op)
    monkeypatch.setattr(primitives, "propose_reconciliation", _no_op)
    monkeypatch.setattr(primitives, "commit_consolidation", _commit)
    monkeypatch.setattr(primitives, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(primitives, "abort_deferred_consolidation", _abort)

    class _WriteLease:
        def ensure_durable(self, **_kwargs) -> None:
            events.append("durability")

        def ensure_owned(self, **_kwargs) -> None:
            return None

    @contextmanager
    def _guarded_write(*_args, **_kwargs):
        yield _WriteLease()

    from okto_pulse.core.kg import guarded_write

    monkeypatch.setattr(guarded_write, "guarded_board_write", _guarded_write)


@pytest.mark.asyncio
async def test_closeout_commits_relational_uow_before_finalizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_pipeline_doubles(monkeypatch, events)
    persister = closeout.ConsolidationPipelinePersister(
        lambda: _RelationalScope(events, fail_commit=False),
        agent_id="closeout-agent",
    )
    monkeypatch.setattr(
        persister,
        "already_persisted",
        lambda *_args: events.append("queryable_probe") or True,
    )

    persisted = await persister.persist(
        "board-closeout",
        "spec",
        closeout.CloseoutCandidate(
            node_type="Alternative",
            title="Alternative",
            content="Reasoning",
            source_artifact_ref="spec:closeout:alternative:1",
        ),
    )

    assert persisted is True
    assert events == [
        "graph_and_relational_staged",
        "durability",
        "db_commit",
        "finalize",
        "queryable_probe",
    ]


@pytest.mark.asyncio
async def test_closeout_commit_failure_aborts_graph_and_never_reports_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_pipeline_doubles(monkeypatch, events)
    persister = closeout.ConsolidationPipelinePersister(
        lambda: _RelationalScope(events, fail_commit=True),
        agent_id="closeout-agent",
    )
    monkeypatch.setattr(
        persister,
        "already_persisted",
        lambda *_args: events.append("unexpected_probe") or True,
    )

    persisted = await persister.persist(
        "board-closeout",
        "spec",
        closeout.CloseoutCandidate(
            node_type="Alternative",
            title="Alternative",
            content="Reasoning",
            source_artifact_ref="spec:closeout:alternative:1",
        ),
    )

    assert persisted is False
    assert "finalize" not in events
    assert "unexpected_probe" not in events
    assert events == [
        "graph_and_relational_staged",
        "durability",
        "db_commit",
        "scope_rollback",
        "abort",
        "durability",
    ]


@pytest.mark.asyncio
async def test_closeout_graph_error_still_drains_possible_autocommit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_pipeline_doubles(monkeypatch, events)

    async def _partial_commit(*_args, **_kwargs):
        events.append("graph_partial_error")
        raise RuntimeError("graph result materialization failed")

    monkeypatch.setattr(primitives, "commit_consolidation", _partial_commit)
    persister = closeout.ConsolidationPipelinePersister(
        lambda: _RelationalScope(events, fail_commit=False),
        agent_id="closeout-agent",
    )

    persisted = await persister.persist(
        "board-closeout",
        "spec",
        closeout.CloseoutCandidate(
            node_type="Alternative",
            title="Alternative",
            content="Reasoning",
            source_artifact_ref="spec:closeout:alternative:partial",
        ),
    )

    assert persisted is False
    assert events == [
        "graph_partial_error",
        "durability",
        "scope_rollback",
        "abort",
    ]


@pytest.mark.asyncio
async def test_closeout_lifecycle_failure_compensates_before_fence_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _install_pipeline_doubles(monkeypatch, events)

    class _FailFirstLifecycleLease:
        calls = 0

        def ensure_durable(self, **_kwargs) -> None:
            self.calls += 1
            events.append("durability")
            if self.calls == 1:
                raise RuntimeError("forced graph lifecycle failure")

        def ensure_owned(self, **_kwargs) -> None:
            return None

    @contextmanager
    def _guarded_write(*_args, **_kwargs):
        events.append("lock_acquire")
        try:
            yield _FailFirstLifecycleLease()
        finally:
            events.append("lock_release")

    from okto_pulse.core.kg import guarded_write

    monkeypatch.setattr(guarded_write, "guarded_board_write", _guarded_write)
    persister = closeout.ConsolidationPipelinePersister(
        lambda: _RelationalScope(events, fail_commit=False),
        agent_id="closeout-agent",
    )

    persisted = await persister.persist(
        "board-closeout",
        "spec",
        closeout.CloseoutCandidate(
            node_type="Alternative",
            title="Alternative",
            content="Reasoning",
            source_artifact_ref="spec:closeout:alternative:lifecycle",
        ),
    )

    assert persisted is False
    assert events == [
        "lock_acquire",
        "graph_and_relational_staged",
        "durability",
        "scope_rollback",
        "abort",
        "durability",
        "lock_release",
    ]
