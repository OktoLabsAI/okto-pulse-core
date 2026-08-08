"""Board-scoped administrative DecisionDigest layer reconciliation."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.application.use_cases import (
    ActorContext,
    EntityNotFoundError,
    ReconcileDigestLayerCommand,
    ReconcileDigestLayerUseCase,
)
from okto_pulse.core.kg.canonical_demotion_global_sync import (
    enqueue_digest_layer_reconciliation,
)
from okto_pulse.core.mcp.outcome import McpToolOutcome


class _CapturingAuditPort:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def emit_outbox_event(self, context: object, **event: Any) -> None:
        self.events.append({"context": context, **event})


@pytest.mark.asyncio
async def test_generic_reconcile_event_is_zero_ref_audited_and_effect_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The synthetic event carries no graph refs and can safely be repeated."""
    from okto_pulse.core.kg import canonical_demotion_global_sync as sync_module

    audit = _CapturingAuditPort()
    monkeypatch.setattr(sync_module, "get_kg_worker_audit_port", lambda: audit)
    context = object()

    first = await enqueue_digest_layer_reconciliation(
        context,
        board_id="board-a",
        reason="incident_42_digest_drift",
    )
    second = await enqueue_digest_layer_reconciliation(
        context,
        board_id="board-a",
        reason="incident_42_digest_drift",
    )

    assert len(audit.events) == 2
    assert first["event_id"] != second["event_id"]
    assert first["session_id"] != second["session_id"]
    assert first["effect_idempotent"] is True
    for event in audit.events:
        assert event["context"] is context
        assert event["board_id"] == "board-a"
        assert event["event_type"] == "consolidation_committed"
        assert event["payload"] == {
            "session_id": event["session_id"],
            "nodes_added": 0,
            "reason": "incident_42_digest_drift",
        }
        assert not ({"node_refs", "nodes", "added_node_refs"} & event["payload"].keys())


@pytest.mark.asyncio
async def test_generic_reconcile_rejects_free_form_reason_before_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import canonical_demotion_global_sync as sync_module

    audit = _CapturingAuditPort()
    monkeypatch.setattr(sync_module, "get_kg_worker_audit_port", lambda: audit)

    with pytest.raises(ValueError, match="audit code"):
        await enqueue_digest_layer_reconciliation(
            object(),
            board_id="board-a",
            reason="contains free form prose",
        )
    assert audit.events == []


@pytest.mark.asyncio
async def test_synthetic_event_persists_without_consolidation_session_refs(
    db_factory,
) -> None:
    """The correlation session_id has no audit/ref parent dependency."""
    from sqlalchemy_test_models import (
        Board,
        ConsolidationAudit,
        GlobalUpdateOutbox,
        KuzuNodeRef,
    )

    board_id = f"digest-reconcile-{uuid.uuid4().hex[:10]}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="digest reconcile", owner_id="owner-a"))
        await db.flush()
        accepted = await enqueue_digest_layer_reconciliation(
            db,
            board_id=board_id,
            reason="incident_42_digest_drift",
        )
        await db.commit()

    async with db_factory() as db:
        event = (
            await db.execute(
                select(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.event_id == accepted["event_id"]
                )
            )
        ).scalar_one()
        audit_parent = await db.get(ConsolidationAudit, accepted["session_id"])
        node_refs = (
            await db.execute(
                select(KuzuNodeRef).where(
                    KuzuNodeRef.session_id == accepted["session_id"]
                )
            )
        ).scalars().all()

    assert event.payload["nodes_added"] == 0
    assert audit_parent is None
    assert node_refs == []


class _Boards:
    def __init__(self, board_id: str) -> None:
        self.board_id = board_id
        self.get_calls: list[str] = []

    async def get(self, board_id: str) -> object | None:
        self.get_calls.append(board_id)
        if board_id != self.board_id:
            return None
        return SimpleNamespace(id=board_id, owner_id="owner", realm_id=None)


class _KgOperations:
    def __init__(
        self,
        result: dict[str, object] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.result = result or {
            "enqueued": True,
            "board_id": "board-a",
            "event_id": "event-a",
            "session_id": "digestreconcile_a",
            "reason": "incident_42_digest_drift",
            "effect_idempotent": True,
        }
        self.error = error
        self.calls: list[dict[str, str]] = []

    async def enqueue_digest_layer_reconciliation(
        self, *, board_id: str, reason: str
    ) -> dict[str, object]:
        self.calls.append({"board_id": board_id, "reason": reason})
        if self.error is not None:
            raise self.error
        return dict(self.result)


class _FakeUow:
    def __init__(self, board_id: str = "board-a") -> None:
        self.boards = _Boards(board_id)
        self.kg = _KgOperations()
        self.services = SimpleNamespace(kg=self.kg)
        self.commits = 0

    async def __aenter__(self) -> "_FakeUow":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1


@pytest.mark.asyncio
async def test_use_case_checks_board_scope_then_commits_durable_event() -> None:
    uow = _FakeUow()
    actor = ActorContext("agent-a", "mcp", board_id="board-a")

    result = await ReconcileDigestLayerUseCase().execute(
        ReconcileDigestLayerCommand(
            "board-a", reason="incident_42_digest_drift"
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    assert result.data["enqueued"] is True
    assert uow.kg.calls == [
        {"board_id": "board-a", "reason": "incident_42_digest_drift"}
    ]
    assert uow.commits == 1


@pytest.mark.asyncio
async def test_use_case_fails_closed_for_cross_board_actor_without_write() -> None:
    uow = _FakeUow()
    actor = ActorContext("agent-a", "mcp", board_id="board-b")

    with pytest.raises(EntityNotFoundError):
        await ReconcileDigestLayerUseCase().execute(
            ReconcileDigestLayerCommand(
                "board-a", reason="incident_42_digest_drift"
            ),
            actor=actor,
            uow=uow,  # type: ignore[arg-type]
        )

    assert uow.boards.get_calls == []
    assert uow.kg.calls == []
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_mcp_tool_inventory_schema_and_success_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server as mcp_server

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_digest_layer_reconcile")
    assert tool.parameters["required"] == ["board_id", "reason"]
    assert "Administrative WRITE" in tool.description
    assert "keyset-inventories publishable board" in tool.description
    assert "guards stale prune against derived edges" in tool.description
    assert "repairs identities/Board links" in tool.description
    assert "post-flush" in tool.description
    assert "fresh-handle" in tool.description
    assert "audit code" in tool.parameters["properties"]["reason"]["description"]

    ctx = mcp_server.AgentContext(
        "agent-a",
        "Agent A",
        "board-a",
        ["kg.admin.settings_write"],
    )
    uow = _FakeUow()

    async def _ctx(board_id: str) -> object:
        assert board_id == "board-a"
        return ctx

    class _Factory:
        def __call__(self, **_kwargs: object) -> _FakeUow:
            return uow

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _ctx)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )

    outcome = await tool.fn(
        board_id="board-a",
        reason="incident_42_digest_drift",
    )
    assert isinstance(outcome, McpToolOutcome)
    assert outcome.is_error is False
    assert outcome.payload["enqueued"] is True
    assert uow.commits == 1


@pytest.mark.asyncio
async def test_mcp_tool_returns_structured_permission_and_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server as mcp_server

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_digest_layer_reconcile")
    denied_ctx = mcp_server.AgentContext(
        "agent-a",
        "Agent A",
        "board-a",
        ["kg.admin.historical_consolidation"],
    )

    async def _denied_ctx(_board_id: str) -> object:
        return denied_ctx

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _denied_ctx)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: (_ for _ in ()).throw(AssertionError("UoW must not open")),
    )
    denied = await tool.fn(
        board_id="board-a",
        reason="incident_42_digest_drift",
    )
    assert isinstance(denied, McpToolOutcome)
    assert denied.is_error is True
    assert denied.code == "permission_denied"
    assert denied.details["required_permission"] == (
        "kg.operations.integrity.reconcile"
    )

    admin_ctx = mcp_server.AgentContext(
        "agent-a",
        "Agent A",
        "board-a",
        ["kg.admin.settings_write"],
    )
    uow = _FakeUow()
    uow.kg = _KgOperations(error=ValueError("reason must be an audit code"))
    uow.services = SimpleNamespace(kg=uow.kg)

    async def _admin_ctx(_board_id: str) -> object:
        return admin_ctx

    class _Factory:
        def __call__(self, **_kwargs: object) -> _FakeUow:
            return uow

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _admin_ctx)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )
    invalid = await tool.fn(board_id="board-a", reason="free form")
    assert isinstance(invalid, McpToolOutcome)
    assert invalid.is_error is True
    assert invalid.code == "validation_failed"
    assert uow.commits == 0


def test_reconcile_tool_is_discoverable_in_resources() -> None:
    from pathlib import Path

    root = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "resources"
    )
    tool_name = "okto_pulse_kg_digest_layer_reconcile"
    long_form = (root / "reference" / "tool-docs" / "kg.md").read_text(
        encoding="utf-8"
    )
    assert tool_name in long_form
    for wording in (
        "per-board graph as authoritative",
        "keyset-paginates",
        "physical source count other than one fails closed",
        "DECISION_DERIVES_FROM",
        "fresh-handle read verifies",
        "one total inbound `CONTAINS_DECISION`",
        "isolated per board",
        "duplicate_count",
        "invalid_link_pruned_count",
    ):
        assert wording in long_form
    assert tool_name in (root / "reference" / "tools_catalog.md").read_text(
        encoding="utf-8"
    )
    assert tool_name in (root / "workflows" / "kg.md").read_text(encoding="utf-8")
