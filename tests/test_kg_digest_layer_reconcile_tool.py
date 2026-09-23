"""F4 retires manual reconciliation; internal durable events remain valid."""

from __future__ import annotations

from typing import Any
import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.kg.canonical_demotion_global_sync import (
    enqueue_digest_layer_reconciliation,
)


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



@pytest.mark.asyncio
async def test_public_reconcile_has_no_handler_or_registered_capability():
    from okto_pulse.core.mcp import server
    from okto_pulse.core.application import use_cases
    names = set(await server.mcp.get_tools())
    assert "okto_pulse_kg_digest_layer_reconcile" not in names
    assert not hasattr(server, "okto_pulse_kg_digest_layer_reconcile")
    assert not hasattr(use_cases, "ReconcileDigestLayerUseCase")
