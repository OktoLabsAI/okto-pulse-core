"""Card 6 integrated ownership transfer across Core and Community.

This module deliberately composes the real Core worker, Community SQLAlchemy
queue/ledger adapters, the SQLite test database and the real board graph.  It
owns the crash window that unit tests on either side of the hexagonal boundary
cannot prove in isolation.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, func, select

os.environ.setdefault(
    "KG_BASE_DIR",
    tempfile.mkdtemp(prefix="okto_kg_card6_delivery_integration_"),
)

from kg_registry_testing import (  # noqa: E402
    RealBoardCypherExecutorForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)
from okto_pulse.community.adapters.sqlalchemy_consolidation import (  # noqa: E402
    CommunitySqlAlchemyConsolidationPersistence,
)
from okto_pulse.community.adapters.sqlalchemy_delivery_ledger import (  # noqa: E402
    CommunitySqlAlchemyDeliveryLedger,
)
from okto_pulse.core.application.processors import consolidation  # noqa: E402
from okto_pulse.core.application.processors.consolidation import (  # noqa: E402
    ConsolidationProcessor,
)
from okto_pulse.core.ports.consolidation import (  # noqa: E402
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)
from okto_pulse.core.ports.delivery_ledger import (  # noqa: E402
    DeliveryState,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)
from okto_pulse.core.ports.reconcile_intent import (  # noqa: E402
    ReconcileIntentCreate,
)
from okto_pulse.core.ports.tombstone import DeletionTombstoneAdvance  # noqa: E402
from r2_scenario_helpers import (  # noqa: E402
    first_canonical_node,
    new_board,
    node_layer,
    seed_done_spec_canonical,
)
from sqlalchemy_test_models import (  # noqa: E402
    ConsolidationQueue,
    GlobalDiscoveryDeliveryLedger,
    GlobalUpdateOutbox,
    Spec,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
    )


@contextmanager
def _registered_community_delivery_adapters():
    previous_queue = get_consolidation_persistence_port()
    try:
        previous_delivery = get_delivery_ledger_port()
    except RuntimeError:
        previous_delivery = None

    queue = CommunitySqlAlchemyConsolidationPersistence()
    delivery = CommunitySqlAlchemyDeliveryLedger()
    register_consolidation_persistence_port(queue)
    register_delivery_ledger_port(delivery)
    try:
        yield queue, delivery
    finally:
        register_consolidation_persistence_port(previous_queue)
        if previous_delivery is None:
            reset_delivery_ledger_port_for_tests()
        else:
            register_delivery_ledger_port(previous_delivery)


class _CrashAfterGraphCommit(BaseException):
    """Abrupt worker termination outside its normal Exception recovery path."""


@pytest.mark.asyncio
async def test_graph_commit_crash_recovers_to_one_atomic_attempt_zero(
    db_factory,
    monkeypatch: pytest.MonkeyPatch,
):
    """TS4/TS5: graph commit survives, empty retry transfers exactly once."""

    board_id = await new_board(db_factory, "card6-int")
    spec_id, source_ref = await seed_done_spec_canonical(db_factory, board_id)
    requirement = await first_canonical_node(board_id, "Requirement")
    assert requirement is not None
    requirement_id, _ = requirement
    assert await node_layer(board_id, "Requirement", requirement_id) == "canonical"

    delete_event_id = f"card6-delete-{spec_id}"
    observed_reconciliations = []
    from okto_pulse.core.kg import canonical_stale_reconciler

    real_reconcile = canonical_stale_reconciler.reconcile_stale_canonical
    real_transfer = consolidation._transfer_stale_reconcile_ownership

    async def _observe_reconciliation(*args, **kwargs):
        result = await real_reconcile(*args, **kwargs)
        observed_reconciliations.append(result)
        return result

    async def _crash_before_relational_transfer(*_args, **_kwargs):
        raise _CrashAfterGraphCommit("crash after graph commit")

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _observe_reconciliation,
    )

    with _registered_community_delivery_adapters() as (queue, _delivery):
        async with db_factory() as db:
            spec = await db.get(Spec, spec_id)
            assert spec is not None
            await db.delete(spec)
            tombstone = await queue.advance_deletion_tombstone(
                db,
                DeletionTombstoneAdvance(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    delete_event_id=delete_event_id,
                ),
            )
            intent = await queue.persist_reconcile_intent(
                db,
                ReconcileIntentCreate(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    generation=tombstone.generation,
                    delete_event_id=delete_event_id,
                    source_refs=(source_ref,),
                ),
            )
            # This integration test owns exactly one worker identity.  The
            # shared ``db_factory`` can retain unrelated pending rows from
            # earlier modules in a broad regression order; letting the
            # batch-size-one worker claim one of those rows makes the crash
            # boundary nondeterministic.  Keep only the intent created above.
            await db.execute(
                delete(ConsolidationQueue).where(
                    ConsolidationQueue.id != intent.intent_id
                )
            )
            # Initial consolidation may have emitted its own GD event.  This
            # scenario starts the Card 6 ownership boundary with a clean outbox.
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

        processor = ConsolidationProcessor(db_factory, batch_size=1)
        monkeypatch.setattr(
            consolidation,
            "_transfer_stale_reconcile_ownership",
            _crash_before_relational_transfer,
        )

        with pytest.raises(_CrashAfterGraphCommit, match="after graph commit"):
            await processor.process_batch()

        assert len(observed_reconciliations) == 1
        assert observed_reconciliations[0].demoted
        assert await node_layer(board_id, "Requirement", requirement_id) == "working"

        async with db_factory() as db:
            claimed = await db.get(ConsolidationQueue, intent.intent_id)
            assert claimed is not None
            assert claimed.status == "claimed"
            assert claimed.claim_token
            assert int(
                await db.scalar(
                    select(func.count()).select_from(
                        GlobalDiscoveryDeliveryLedger
                    )
                )
                or 0
            ) == 0
            assert int(
                await db.scalar(
                    select(func.count()).select_from(GlobalUpdateOutbox)
                )
                or 0
            ) == 0

            # Model expiry without sleeping so the production recovery path
            # performs a genuine reclaim with a new claim token.
            claimed.claim_timeout_at = datetime(2000, 1, 1, tzinfo=timezone.utc)
            await db.commit()

        assert await processor.recover_stale_claims() == 1
        async with db_factory() as db:
            recovered = await db.get(ConsolidationQueue, intent.intent_id)
            assert recovered is not None
            assert recovered.status == "pending"
            assert recovered.claim_token is None

        monkeypatch.setattr(
            consolidation,
            "_transfer_stale_reconcile_ownership",
            real_transfer,
        )
        assert await processor.process_batch() == 1

        assert len(observed_reconciliations) == 2
        assert observed_reconciliations[1].demoted == []
        assert await node_layer(board_id, "Requirement", requirement_id) == "working"

        async with db_factory() as db:
            assert await db.get(ConsolidationQueue, intent.intent_id) is None
            ledgers = (
                await db.execute(select(GlobalDiscoveryDeliveryLedger))
            ).scalars().all()
            outbox = (
                await db.execute(select(GlobalUpdateOutbox))
            ).scalars().all()

        assert len(ledgers) == 1
        assert len(outbox) == 1
        ledger = ledgers[0]
        attempt_zero = outbox[0]
        assert ledger.state == DeliveryState.OUTBOX_PERSISTED.value
        assert ledger.attempt == 0
        assert ledger.attempt_event_key == attempt_zero.event_id
        assert attempt_zero.event_id.endswith(":attempt:0")
        assert attempt_zero.payload["attempt"] == 0
        assert attempt_zero.payload["delivery_key"] == ledger.delivery_key
