"""Cognitive closeout confirms relational durability before graph success."""

from __future__ import annotations

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
        "db_commit",
        "scope_rollback",
        "abort",
    ]
