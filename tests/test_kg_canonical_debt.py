"""CanonicalDebt service tests."""

from __future__ import annotations

import json

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, CanonicalDebt
from sqlalchemy_test_models import ConsolidationQueue
from sqlalchemy_test_models import Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor
from okto_pulse.core.services.canonical_debt_service import (
    list_canonical_debt,
    mark_canonical_debt_committed_for_artifact,
    reconcile_canonical_debt_with_evidence,
    schedule_canonical_debt_retry,
    summarize_canonical_debt,
    upsert_canonical_debt,
)


BOARD_ID = "board-canonical-debt-test"
USER_ID = "user-canonical-debt-test"


@pytest.mark.asyncio
async def test_canonical_debt_summary_counts_open_states(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s1",
                source_ref="spec:s1",
                content_hash="h1",
                target_status="validated",
                canonical_state="failed",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="task",
                artifact_id="t1",
                source_ref="task:t1",
                content_hash="h2",
                target_status="done",
                canonical_state="committed",
            ),
        ])
        await session.commit()

        summary = await summarize_canonical_debt(session, BOARD_ID)
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert summary["open_count"] == 1
    assert summary["retryable_count"] == 1
    assert summary["terminal_count"] == 1
    assert listed.total == 2
    assert listed.counts["open_count"] == 1


@pytest.mark.asyncio
async def test_mcp_canonical_debt_list_exposes_debt_drilldown(
    db_factory,
    monkeypatch,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-mcp",
                source_ref="spec:s-mcp",
                content_hash="h-mcp",
                target_status="done",
                canonical_state="failed",
                failure_reason="connectivity_guard",
                last_error="guard rejected commit",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="task",
                artifact_id="t-mcp",
                source_ref="task:t-mcp",
                content_hash="h-task",
                target_status="done",
                canonical_state="committed",
            ),
        ])
        await session.commit()

    async def _fake_ctx(board_id: str):
        assert board_id == BOARD_ID
        # MCP-FU5: the migrated tool builds the actor via MCPAdapterContract.actor
        # (needs agent_id/agent_name/permissions) and runs over the MCP
        # UnitOfWorkFactory — not a raw get_db_for_mcp() session.
        return type(
            "Ctx",
            (),
            {"agent_id": "mcp-agent", "agent_name": "mcp-agent", "permissions": None},
        )()

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)
    # The tool now resolves its session through get_unit_of_work_factory_for_mcp()
    # over the registered _mcp_session_factory; point it at the test factory.
    monkeypatch.setattr(mcp_server, "_mcp_session_factory", db_factory)

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_canonical_debt_list")
    raw = await tool.fn(
        board_id=BOARD_ID,
        artifact_type="spec",
        state="failed",
        limit=20,
        offset=0,
    )
    payload = json.loads(raw)

    assert payload["board_id"] == BOARD_ID
    assert payload["total"] == 1
    assert payload["counts"]["open_count"] == 1
    assert payload["items"][0]["artifact_id"] == "s-mcp"
    assert payload["items"][0]["target_status"] == "done"
    assert payload["items"][0]["failure_reason"] == "connectivity_guard"


@pytest.mark.asyncio
async def test_upsert_canonical_debt_is_idempotent_by_artifact_target_hash(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        first = await upsert_canonical_debt(
            session,
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="s2",
            source_ref="spec:s2",
            content_hash="h-same",
            target_status="validated",
            canonical_state="failed",
            failure_reason="connectivity_guard",
            last_error="first",
        )
        second = await upsert_canonical_debt(
            session,
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="s2",
            source_ref="spec:s2",
            content_hash="h-same",
            target_status="validated",
            canonical_state="deferred",
            failure_reason="kg_health_backpressure",
            last_error="second",
        )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert second.id == first.id
    assert listed.total == 1
    assert listed.items[0]["canonical_state"] == "deferred"
    assert listed.items[0]["last_error"] == "second"


@pytest.mark.asyncio
async def test_upsert_canonical_debt_requires_content_hash_and_persists_no_partial_row(
    db_factory,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.commit()

        with pytest.raises(ValueError, match="content_hash"):
            await upsert_canonical_debt(
                session,
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-no-hash",
                source_ref="spec:s-no-hash",
                content_hash="",
                target_status="validated",
                canonical_state="failed",
                failure_reason="connectivity_guard",
                last_error="hash missing",
            )
        await session.rollback()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 0


@pytest.mark.asyncio
async def test_schedule_canonical_debt_retry_respects_health_state(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        debt = CanonicalDebt(
            board_id=BOARD_ID,
            artifact_type="refinement",
            artifact_id="r1",
            source_ref="refinement:r1",
            content_hash="h3",
            target_status="done",
            canonical_state="failed",
        )
        session.add(debt)
        await session.commit()
        await session.refresh(debt)

        blocked = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state="at_risk",
        )
        scheduled = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state="healthy",
        )

    assert blocked["ok"] is False
    assert blocked["attempt_consumed"] is False
    assert blocked["error"] == "kg_health_blocks_retry"
    assert scheduled["ok"] is True
    assert scheduled["attempt_consumed"] is False
    assert scheduled["debt"]["canonical_state"] == "retry_scheduled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "health_state",
    ["at_risk", "backpressure", "recovery_needed", "quarantined"],
)
async def test_schedule_canonical_debt_retry_blocks_without_consuming_attempt_for_degraded_health(
    db_factory,
    health_state: str,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        debt = CanonicalDebt(
            board_id=BOARD_ID,
            artifact_type="refinement",
            artifact_id=f"r-{health_state}",
            source_ref=f"refinement:r-{health_state}",
            content_hash=f"h-{health_state}",
            target_status="done",
            canonical_state="failed",
            retry_count=2,
        )
        session.add(debt)
        await session.commit()
        await session.refresh(debt)

        result = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state=health_state,
        )
        await session.refresh(debt)

    assert result["ok"] is False
    assert result["attempt_consumed"] is False
    assert result["error"] == "kg_health_blocks_retry"
    assert debt.retry_count == 2
    assert debt.canonical_state == "blocked"
    assert debt.failure_reason == f"kg_health_{health_state}"


@pytest.mark.asyncio
async def test_reconcile_canonical_debt_commits_only_matching_evidence(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s3",
                source_ref="spec:s3",
                source_version="v1",
                content_hash="hash-match",
                target_status="validated",
                canonical_state="failed",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s4",
                source_ref="spec:s4",
                source_version="v1",
                content_hash="hash-open",
                target_status="validated",
                canonical_state="failed",
            ),
        ])
        await session.commit()

        result = await reconcile_canonical_debt_with_evidence(
            session,
            board_id=BOARD_ID,
            actor_id="agent-x",
            report_ref="report:1",
            canonical_evidence=[
                {
                    "source_ref": "spec:s3",
                    "source_version": "v1",
                    "content_hash": "hash-match",
                    "node_ref": "Spec:s3",
                },
                {
                    "source_ref": "spec:s4",
                    "source_version": "v2",
                    "content_hash": "hash-open",
                    "node_ref": "Spec:s4",
                },
            ],
        )
        await session.commit()
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    states = {item["source_ref"]: item["canonical_state"] for item in listed.items}
    assert result["committed_count"] == 1
    assert states["spec:s3"] == "committed"
    assert states["spec:s4"] == "failed"


@pytest.mark.asyncio
async def test_successful_artifact_consolidation_closes_queue_failure_debt(
    db_factory,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-later-ok",
                source_ref="spec:s-later-ok",
                content_hash="queue-attempt-hash",
                target_status="canonical_consolidation",
                canonical_state="failed",
                failure_reason="consolidation_failed",
                last_error="connectivity_guard",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-later-ok",
                source_ref="spec:s-later-ok",
                content_hash="semantic-target-hash",
                target_status="validated",
                canonical_state="failed",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-other",
                source_ref="spec:s-other",
                content_hash="other-hash",
                target_status="canonical_consolidation",
                canonical_state="failed",
            ),
        ])
        await session.commit()

        result = await mark_canonical_debt_committed_for_artifact(
            session,
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="s-later-ok",
            actor_id="worker-ok",
            evidence_ref="kg_session:ok",
        )
        await session.commit()
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    states = {
        (item["artifact_id"], item["target_status"]): item
        for item in listed.items
    }
    assert result["committed_count"] == 1
    assert states[("s-later-ok", "canonical_consolidation")]["canonical_state"] == "committed"
    assert states[("s-later-ok", "canonical_consolidation")]["evidence_ref"] == "kg_session:ok"
    assert states[("s-later-ok", "validated")]["canonical_state"] == "failed"
    assert states[("s-other", "canonical_consolidation")]["canonical_state"] == "failed"
    assert listed.counts["open_count"] == 2


@pytest.mark.asyncio
async def test_consolidation_failure_marks_canonical_debt(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        session.add(Spec(
            id="spec-failed",
            board_id=BOARD_ID,
            title="Done spec with failed consolidation",
            description="Canonical-ready source.",
            status=SpecStatus.DONE,
            created_by=USER_ID,
        ))
        entry = ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="spec-failed",
            status="claimed",
            worker_id="worker-test",
        )
        session.add(entry)
        await session.flush()

        worker = ConsolidationProcessor(lambda: None)
        await worker._mark_failed(
            session,
            entry,
            error_text="KG node connectivity guard rejected the commit",
            max_attempts=3,
        )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 1
    debt = listed.items[0]
    assert debt["artifact_type"] == "spec"
    assert debt["artifact_id"] == "spec-failed"
    assert debt["canonical_state"] == "failed"
    assert debt["failure_reason"] == "consolidation_failed"
    assert debt["queue_ref"] == entry.id


@pytest.mark.asyncio
async def test_missing_artifact_queue_entry_stays_visible_without_canonical_debt(
    db_factory,
):
    """RKG-04 AC3 (ts_317b11ef): a missing source artifact is a persistent
    failure and must stay VISIBLE — the worker returns False, so the entry is
    failure-handled (attempts++, backoff, re-pending; DLQ only after
    ``kg_queue_max_attempts``) instead of silently acked. A missing artifact
    still never mints canonical debt rows.

    Traceability: formerly ``test_missing_artifact_queue_entry_is_acked_
    without_canonical_debt`` — introduced together with the ack-on-missing
    regression later reverted (bug 99ac1fc3); rewritten to the adjudicated
    contract (FU-1 57c3cab9, census 649abf70).
    """
    missing_spec_id = "spec-stale-missing"
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        # Hermetic seed: process_batch's claim is board-AGNOSTIC with
        # limit=batch_size, and the test DB is session-scoped/shared —
        # leftover queue rows from earlier tests crowd this entry out of
        # the batch, leaving it unclaimed (attempts stays 0). Drain the
        # whole queue so the worker under test sees exactly one entry.
        await session.execute(ConsolidationQueue.__table__.delete())
        await session.execute(
            Spec.__table__.delete().where(Spec.id == missing_spec_id)
        )
        entry = ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=missing_spec_id,
            status="pending",
            worker_id=None,
        )
        session.add(entry)
        await session.commit()
        entry_id = entry.id

    worker = ConsolidationProcessor(db_factory, batch_size=1)
    processed = await worker.process_batch()

    async with db_factory() as session:
        queue_row = await session.get(ConsolidationQueue, entry_id)
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    # Not a success: False routes the entry through _mark_failed.
    assert processed == 0
    # The entry stays visible — re-pended with the failure recorded, bound
    # for the DLQ only after kg_queue_max_attempts consecutive failures.
    assert queue_row is not None
    assert queue_row.status == "pending"
    assert queue_row.attempts == 1
    assert queue_row.next_retry_at is not None
    assert queue_row.last_error == "processing returned False"
    # A missing artifact never mints canonical debt.
    assert listed.total == 0


@pytest.mark.asyncio
async def test_canonical_debt_persist_failure_rolls_back_before_queue_update(
    db_factory,
    monkeypatch,
):
    spec_id = "spec-debt-persist-rollback"

    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        await session.execute(Spec.__table__.delete().where(Spec.id == spec_id))
        spec = Spec(
            id=spec_id,
            board_id=BOARD_ID,
            title="Done spec with debt persistence failure",
            description="Canonical-ready source.",
            status=SpecStatus.DONE,
            created_by=USER_ID,
        )
        entry = ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=spec_id,
            status="claimed",
            worker_id="worker-test",
        )
        session.add_all([spec, entry])
        await session.commit()
        entry_id = entry.id

    async def broken_upsert(db, **kwargs):
        db.add(CanonicalDebt(
            board_id="missing-board-for-fk",
            artifact_type=kwargs["artifact_type"],
            artifact_id=kwargs["artifact_id"],
            source_ref=kwargs["source_ref"],
            content_hash=kwargs["content_hash"],
            target_status=kwargs["target_status"],
            canonical_state=kwargs["canonical_state"],
        ))
        await db.flush()

    monkeypatch.setattr(
        "okto_pulse.core.application.processors.consolidation.upsert_canonical_debt",
        broken_upsert,
    )

    async with db_factory() as session:
        entry = await session.get(ConsolidationQueue, entry_id)
        worker = ConsolidationProcessor(lambda: None)
        await worker._mark_failed(
            session,
            entry,
            error_text="KG node connectivity guard rejected the commit",
            max_attempts=3,
        )
        await session.commit()

    async with db_factory() as session:
        entry = await session.get(ConsolidationQueue, entry_id)
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert entry is not None
    assert entry.attempts == 1
    assert entry.status == "pending"
    assert entry.last_error == "KG node connectivity guard rejected the commit"
    assert listed.total == 0


@pytest.mark.asyncio
async def test_spec_consolidation_failure_creates_canonical_debt_only_when_done(
    db_factory,
):
    validated_id = "spec-validated-failed"
    done_id = "spec-done-failed"
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        await session.execute(
            Spec.__table__.delete().where(Spec.id.in_([validated_id, done_id]))
        )
        session.add_all([
            Spec(
                id=validated_id,
                board_id=BOARD_ID,
                title="Validated spec",
                description="Still pre-done.",
                status=SpecStatus.VALIDATED,
                created_by=USER_ID,
            ),
            Spec(
                id=done_id,
                board_id=BOARD_ID,
                title="Done spec",
                description="Canonical-ready.",
                status=SpecStatus.DONE,
                created_by=USER_ID,
            ),
        ])
        entries = [
            ConsolidationQueue(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=validated_id,
                status="claimed",
                worker_id="worker-test",
            ),
            ConsolidationQueue(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=done_id,
                status="claimed",
                worker_id="worker-test",
            ),
        ]
        session.add_all(entries)
        await session.flush()

        worker = ConsolidationProcessor(lambda: None)
        for entry in entries:
            await worker._mark_failed(
                session,
                entry,
                error_text="KG node connectivity guard rejected the commit",
                max_attempts=3,
            )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 1
    assert listed.items[0]["artifact_id"] == done_id
    assert listed.items[0]["graph_layer"] == "canonical"
    assert listed.items[0]["maturity_status"] == "canonical_eligible"


@pytest.mark.asyncio
async def test_working_bug_consolidation_failure_does_not_create_canonical_debt(
    db_factory,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        bug = Card(
            id="bug-working-failure",
            board_id=BOARD_ID,
            title="Working bug should not become canonical debt",
            description="Bug is still not_started.",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            created_by=USER_ID,
            expected_behavior="Expected behavior",
            observed_behavior="Observed behavior",
            steps_to_reproduce="Step 1",
        )
        entry = ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="card",
            artifact_id=bug.id,
            status="claimed",
            worker_id="worker-test",
        )
        session.add_all([bug, entry])
        await session.flush()

        worker = ConsolidationProcessor(lambda: None)
        await worker._mark_failed(
            session,
            entry,
            error_text="KG node connectivity guard rejected the commit",
            max_attempts=3,
        )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 0
