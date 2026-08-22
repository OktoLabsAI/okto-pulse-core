"""Executable specification for the late governed-takedown scenarios.

These tests intentionally keep clocks and race barriers in the harness.  They
exercise Core orchestration and contracts without sleeping or starting Pulse.
"""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import pytest

from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.ports.consolidation import (
    ConsolidationProjectionInputs,
    ConsolidationQueueRecord,
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryAttemptEnvelope,
    DeliveryAttemptOutcome,
    DeliveryAttemptResult,
    DeliveryCircuitSnapshot,
    DeliveryMaintenanceReceipt,
    DeliveryState,
    DeliveryTransferReceipt,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)
from okto_pulse.core.ports.takedown_telemetry import (
    TAKEDOWN_NORMAL_SLO_SECONDS,
    TAKEDOWN_RECOVERY_SLO_SECONDS,
    TakedownAggregates,
    TakedownState,
    TakedownTelemetryQuery,
    TakedownTelemetrySnapshot,
    TakedownTransition,
    build_takedown_slo_alert,
    register_takedown_telemetry_read_port,
)
from okto_pulse.core.runtime_context import (
    capture_runtime_values_for_tests,
    restore_runtime_values_for_tests,
)


BOARD_ID = "spec-ts17-ts27-board"
ARTIFACT_ID = "spec-ts17-ts27-artifact"
DELETE_EVENT_ID = "spec-ts17-ts27-delete-g1"
DELIVERY_KEY = f"gd_parity:{BOARD_ID}:spec:{ARTIFACT_ID}:1"
T0 = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _transition(
    state: TakedownState,
    *,
    occurred_at: datetime,
    delete_event_id: str = DELETE_EVENT_ID,
    attempt: int | None = None,
    details: dict[str, object] | None = None,
) -> TakedownTransition:
    return TakedownTransition(
        delete_event_id=delete_event_id,
        delivery_key=(None if state is TakedownState.INTENT_CREATED else DELIVERY_KEY),
        board_id=BOARD_ID,
        artifact_type="spec",
        artifact_id=ARTIFACT_ID,
        generation=1,
        state=state,
        occurred_at=occurred_at,
        attempt=attempt,
        details=details or {},
    )


class _ScenarioTelemetryReader:
    def __init__(self, snapshots: dict[str, TakedownTelemetrySnapshot]) -> None:
        self.snapshots = snapshots
        self.queries: list[TakedownTelemetryQuery] = []

    async def query_takedown_telemetry(
        self,
        _context: object,
        query: TakedownTelemetryQuery,
    ) -> TakedownTelemetrySnapshot | None:
        self.queries.append(query)
        return self.snapshots.get(str(query.delete_event_id))


@pytest.mark.asyncio
async def test_ts18_controlled_normal_and_sweep_recovery_slo_use_delivered_parity(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """TS18: both paths use controlled time and the canonical health predicate."""

    runtime_before = capture_runtime_values_for_tests()
    request.addfinalizer(lambda: restore_runtime_values_for_tests(runtime_before))
    normal_delivered_at = T0 + timedelta(seconds=90)
    recovery_delete_at = T0 + timedelta(days=2)
    recovery_intent_at = recovery_delete_at + timedelta(hours=25)
    recovery_delivered_at = recovery_intent_at + timedelta(seconds=45)
    recovery_event = "catchup:spec-ts17-ts27:spec:artifact:epoch:1"

    normal = TakedownTelemetrySnapshot(
        board_id=BOARD_ID,
        delete_event_id=DELETE_EVENT_ID,
        delivery_key=DELIVERY_KEY,
        artifact_type="spec",
        artifact_id=ARTIFACT_ID,
        generation=1,
        states=(
            _transition(TakedownState.INTENT_CREATED, occurred_at=T0),
            _transition(
                TakedownState.GRAPH_DEMOTED, occurred_at=T0 + timedelta(seconds=30)
            ),
            _transition(
                TakedownState.OUTBOX_PERSISTED,
                occurred_at=T0 + timedelta(seconds=30),
                attempt=0,
            ),
            _transition(
                TakedownState.DELIVERED,
                occurred_at=normal_delivered_at,
                attempt=0,
            ),
        ),
        aggregates=TakedownAggregates(
            delivery_debt_backlog=0,
            oldest_debt_age_seconds=None,
            circuit_breaker_state="closed",
            circuit_breaker_reason="healthy",
            p95_seconds_1h=90.0,
            p95_sample_count=1,
        ),
    )
    recovery_states = (
        _transition(
            TakedownState.INTENT_CREATED,
            occurred_at=recovery_intent_at,
            delete_event_id=recovery_event,
            details={
                "source": "stale_sweep",
                "original_delete_observed_at": recovery_delete_at.isoformat(),
            },
        ),
        _transition(
            TakedownState.GRAPH_DEMOTED,
            occurred_at=recovery_intent_at + timedelta(seconds=20),
            delete_event_id=recovery_event,
        ),
        _transition(
            TakedownState.OUTBOX_PERSISTED,
            occurred_at=recovery_intent_at + timedelta(seconds=20),
            delete_event_id=recovery_event,
            attempt=0,
        ),
        _transition(
            TakedownState.DELIVERED,
            occurred_at=recovery_delivered_at,
            delete_event_id=recovery_event,
            attempt=0,
        ),
    )
    recovery = TakedownTelemetrySnapshot(
        board_id=BOARD_ID,
        delete_event_id=recovery_event,
        delivery_key=DELIVERY_KEY,
        artifact_type="spec",
        artifact_id=ARTIFACT_ID,
        generation=1,
        states=recovery_states,
        aggregates=TakedownAggregates(
            delivery_debt_backlog=0,
            oldest_debt_age_seconds=None,
            circuit_breaker_state="closed",
            circuit_breaker_reason="healthy",
            p95_seconds_1h=45.0,
            p95_sample_count=1,
        ),
    )
    reader = _ScenarioTelemetryReader(
        {DELETE_EVENT_ID: normal, recovery_event: recovery}
    )
    register_takedown_telemetry_read_port(reader)

    async def _clean_parity(_context, *, board_id: str, limit: int, offset: int):
        assert (board_id, limit, offset) == (BOARD_ID, 200, 0)
        return {
            "board_id": board_id,
            "items": [],
            "count": 0,
            "mutation_allowed": False,
            "global_discovery_evaluation": "evaluated",
            "global_discovery_stale_digest_count": 0,
        }

    from okto_pulse.core.kg import stale_canonical_parity

    monkeypatch.setattr(
        stale_canonical_parity,
        "list_stale_canonical_parity",
        _clean_parity,
    )
    observed_at = recovery_delivered_at + timedelta(seconds=1)
    operations = CoreKnowledgeGraphOperations(object(), clock=lambda: observed_at)

    normal_payload = await operations.query_takedown_telemetry(
        board_id=BOARD_ID, delete_event_id=DELETE_EVENT_ID
    )
    recovery_payload = await operations.query_takedown_telemetry(
        board_id=BOARD_ID, delete_event_id=recovery_event
    )

    assert (normal_delivered_at - T0).total_seconds() <= TAKEDOWN_NORMAL_SLO_SECONDS
    assert (
        recovery_delivered_at - recovery_delete_at
    ).total_seconds() <= TAKEDOWN_RECOVERY_SLO_SECONDS
    for payload in (normal_payload, recovery_payload):
        assert payload["e2e_health"]["predicate"] == (
            "delivered_state_and_evaluable_parity_probe"
        )
        assert payload["e2e_health"]["healthy"] is True
        assert payload["e2e_health"]["delivered"] is True
        assert payload["e2e_health"]["parity_clean"] is True
    assert all(query.now == observed_at for query in reader.queries)

    alert = build_takedown_slo_alert(
        TakedownAggregates(
            delivery_debt_backlog=1,
            oldest_debt_age_seconds=TAKEDOWN_RECOVERY_SLO_SECONDS + 1,
            circuit_breaker_state="open",
            circuit_breaker_reason="debt",
            p95_seconds_1h=TAKEDOWN_NORMAL_SLO_SECONDS + 1,
            p95_sample_count=1,
        )
    )
    assert alert is not None
    assert alert["event"] == "kg.takedown.slo_breach"
    assert set(alert["reasons"]) == {
        "takedown_p95_above_120_seconds",
        "oldest_delivery_debt_above_26_hours",
    }


def _queue_entry(
    *,
    entry_id: str,
    work_kind: str,
    generation: int,
    delete_event_id: str | None,
    claim_token: str,
) -> ConsolidationQueueRecord:
    payload = None
    if work_kind == "stale_reconcile":
        payload = {
            "schema_version": 1,
            "delete_event_id": delete_event_id,
            "source_refs": [f"spec:{ARTIFACT_ID}"],
        }
    return ConsolidationQueueRecord(
        id=entry_id,
        board_id=BOARD_ID,
        artifact_type="spec",
        artifact_id=ARTIFACT_ID,
        status="claimed",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=T0,
        claim_timeout_at=None,
        worker_id="worker",
        claimed_by_session_id="worker",
        triggered_at=T0,
        priority="high",
        work_kind=work_kind,
        generation=generation,
        payload=payload,
        delete_event_id=delete_event_id,
        claim_token=claim_token,
    )


class _RaceStore:
    def __init__(self) -> None:
        self.fence_checks: list[dict[str, object]] = []

    async def load_artifact(self, _context, *, artifact_type: str, artifact_id: str):
        assert (artifact_type, artifact_id) == ("spec", ARTIFACT_ID)
        return SimpleNamespace(title="stale-window")

    async def load_projection_inputs(
        self,
        _context,
        **identity,
    ) -> ConsolidationProjectionInputs:
        assert identity == {
            "board_id": BOARD_ID,
            "artifact_type": "spec",
            "artifact_id": ARTIFACT_ID,
            "artifact": SimpleNamespace(title="stale-window"),
        }
        return ConsolidationProjectionInputs()

    async def queue_claim_is_current_and_unfenced(self, _context, **identity):
        self.fence_checks.append(identity)
        return True


class _RaceDeliveryLedger:
    def __init__(self, timeline: list[tuple[str, datetime]]) -> None:
        self.timeline = timeline
        self.request = None

    async def read_circuit_snapshot(self, _context, *, board_id: str):
        assert board_id == BOARD_ID
        return DeliveryCircuitSnapshot(degraded=False, reason="closed")

    async def transfer_delivery_ownership(self, _context, request):
        self.request = request
        assert request.occurred_at == T0 + timedelta(seconds=90)
        self.timeline.extend(
            [
                ("graph_demoted", request.occurred_at),
                ("outbox_persisted", request.occurred_at),
            ]
        )
        return DeliveryTransferReceipt(
            delivery_key=request.delivery_key,
            state=DeliveryState.OUTBOX_PERSISTED,
            attempt=0,
            attempt_event_key=request.attempt_event_key,
        )

    async def apply_attempt_outcomes(self, _context, outcomes) -> None:
        result = outcomes[0]
        assert result.outcome is DeliveryAttemptOutcome.DELIVERED
        self.timeline.append(("delivered", result.occurred_at))

    async def reconcile_orphaned_attempts(self, *_args, **_kwargs):
        return DeliveryMaintenanceReceipt(scanned=0)

    async def redrive_delivery_debt(self, *_args, **_kwargs):
        return DeliveryMaintenanceReceipt(scanned=0)


@pytest.mark.asyncio
async def test_ts24_post_recheck_stale_publication_is_observable_then_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TS24: delete after the final fence may publish once, then intent wins."""

    legacy = _queue_entry(
        entry_id="legacy-claim",
        work_kind="consolidate",
        generation=0,
        delete_event_id=None,
        claim_token="legacy-token",
    )
    reconcile = _queue_entry(
        entry_id="reconcile-g1",
        work_kind="stale_reconcile",
        generation=1,
        delete_event_id=DELETE_EVENT_ID,
        claim_token="reconcile-token",
    )
    store = _RaceStore()
    publication = {"graph": "absent", "digest": "absent"}
    timeline: list[tuple[str, datetime]] = []
    commit_entered = asyncio.Event()
    allow_commit = asyncio.Event()

    monkeypatch.setattr(
        consolidation,
        "_run_deterministic_worker",
        lambda *_args, **_kwargs: consolidation.WorkerResult(
            nodes=[object()], edges=[], missing_link_candidates=[], raw_content="x"
        ),
    )

    async def _passthrough(_db, _entry, _artifact_or_board, result):
        return result

    monkeypatch.setattr(
        consolidation, "_materialize_lineage_endpoint_nodes", _passthrough
    )
    monkeypatch.setattr(
        consolidation,
        "_resolve_missing_link_candidates",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=_args[-1]),
    )
    monkeypatch.setattr(
        consolidation,
        "_worker_node_to_candidate",
        lambda _node: {
            "candidate_id": "n",
            "node_type": "Requirement",
            "title": "stale",
        },
    )
    monkeypatch.setattr(
        consolidation,
        "begin_consolidation",
        lambda *_args, **_kwargs: asyncio.sleep(
            0, result=SimpleNamespace(session_id="race-session")
        ),
    )
    monkeypatch.setattr(
        consolidation,
        "propose_reconciliation",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=SimpleNamespace()),
    )

    async def _commit(*_args, **_kwargs):
        commit_entered.set()
        await allow_commit.wait()
        publication.update(graph="canonical", digest="canonical")
        timeline.append(("stale_published", T0 + timedelta(seconds=1)))
        return SimpleNamespace(nodes_added=1, edges_added=0)

    monkeypatch.setattr(consolidation, "commit_consolidation", _commit)
    monkeypatch.setattr(
        consolidation,
        "guarded_board_write",
        lambda *_a, **_k: nullcontext(
            SimpleNamespace(
                durability_applied=True,
                ensure_owned=lambda **_kwargs: None,
            )
        ),
    )
    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        consolidation,
        "_run_post_commit_maintenance",
        lambda *_args, **_kwargs: asyncio.sleep(0),
    )

    async def _reconcile(*_args, **_kwargs):
        _kwargs["before_graph_write"]()
        assert publication == {"graph": "canonical", "digest": "canonical"}
        publication["graph"] = "working_stale"
        return SimpleNamespace(
            incomplete=False,
            incomplete_cause=None,
            failed_types=[],
            demoted=["requirement:n"],
            demoted_count=1,
            routed_to_debt=[],
            routed_to_debt_count=0,
            scanned=1,
            target_identity_count=1,
            target_found_count=1,
            target_demoted_count=1,
            target_already_converged_count=0,
            target_skipped_cognitive_count=0,
            target_preserved_canonical_count=0,
        )

    from okto_pulse.core.kg import canonical_stale_reconciler

    monkeypatch.setattr(
        canonical_stale_reconciler, "reconcile_stale_canonical", _reconcile
    )

    previous_store = get_consolidation_persistence_port()
    try:
        previous_ledger = get_delivery_ledger_port()
    except RuntimeError:
        previous_ledger = None
    ledger = _RaceDeliveryLedger(timeline)
    register_consolidation_persistence_port(store)
    register_delivery_ledger_port(ledger)
    try:
        legacy_task = asyncio.create_task(
            consolidation._process_queue_entry(object(), legacy)
        )
        await asyncio.wait_for(commit_entered.wait(), timeout=2)
        # The authoritative claim CAS now runs once, after graph-writer
        # acquisition. Production holds that relational writer through graph
        # commit/ACK; this synthetic post-check publication still proves the
        # stale-reconcile convergence defense without asserting the old
        # DB-writer -> graph-writer order.
        assert len(store.fence_checks) == 1
        timeline.append(("intent_created", T0 + timedelta(seconds=1)))
        allow_commit.set()
        assert await legacy_task is True
        assert publication == {"graph": "canonical", "digest": "canonical"}

        assert (
            await consolidation._process_queue_entry(
                object(),
                reconcile,
                clock=SimpleNamespace(now=lambda: T0 + timedelta(seconds=90)),
            )
            is True
        )
        receipt, _reason = await consolidation._transfer_stale_reconcile_ownership(
            object(),
            reconcile,
            reconcile_details={"demoted_count": 1},
            occurred_at=T0 + timedelta(seconds=90),
        )
        envelope = DeliveryAttemptEnvelope(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=ARTIFACT_ID,
            generation=1,
            delete_event_id=DELETE_EVENT_ID,
            attempt=receipt.attempt,
        )
        delivered_at = T0 + timedelta(seconds=119)
        await ledger.apply_attempt_outcomes(
            object(),
            [
                DeliveryAttemptResult(
                    envelope=envelope,
                    outcome=DeliveryAttemptOutcome.DELIVERED,
                    occurred_at=delivered_at,
                )
            ],
        )
        publication["digest"] = "absent"

        assert publication == {"graph": "working_stale", "digest": "absent"}
        assert [state for state, _at in timeline] == [
            "intent_created",
            "stale_published",
            "graph_demoted",
            "outbox_persisted",
            "delivered",
        ]
        assert (delivered_at - T0).total_seconds() <= TAKEDOWN_NORMAL_SLO_SECONDS
    finally:
        register_consolidation_persistence_port(previous_store)
        if previous_ledger is None:
            reset_delivery_ledger_port_for_tests()
        else:
            register_delivery_ledger_port(previous_ledger)
