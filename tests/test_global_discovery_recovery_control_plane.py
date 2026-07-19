from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.global_discovery_recovery_control import (
    GLOBAL_RECOVERY_SLOT_ID,
    RecoveryAuditReasonInvalid,
    RecoveryBindingConflict,
    RecoveryCheckpoint,
    RecoveryCheckpointConflict,
    RecoveryConfirmationState,
    RecoveryControlPlane,
    RecoveryDispatchKind,
    RecoveryInProgress,
    RecoveryLeaseTakeoverPolicy,
    RecoveryPhaseIncoherent,
    RecoveryPhysicalTruth,
    RecoveryProgressInvariantViolation,
    RecoveryPreparationCommand,
    RecoveryPreparedResult,
    RecoveryProgressCounts,
    RecoveryResumeRejected,
    RecoveryRunBinding,
    RecoveryRunPhase,
    RecoveryRunStatus,
    RecoveryRunState,
    RecoveryStartCommand,
    RecoveryTerminalOutcome,
    RecoveryTransitionEvent,
    RecoveryWorkerResult,
    recovery_attempt_id,
)
from okto_pulse.core.kg.providers.testing.memory_recovery_run_store import (
    MemoryRecoveryControlStore,
)


NOW = datetime.now(timezone.utc)


class RecordingDispatcher:
    def __init__(self) -> None:
        self.dispatched: list[tuple[str, int, str, RecoveryDispatchKind]] = []

    def dispatch(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        kind: RecoveryDispatchKind,
    ) -> None:
        self.dispatched.append((run_id, epoch, attempt_id, kind))


def test_recovery_transition_events_project_each_successful_cas_with_bounded_labels(
) -> None:
    initial = RecoveryRunStatus.initial_preparation(preparation_command())
    initial_event = RecoveryTransitionEvent.from_status(initial)
    running = initial.mark_running(at=NOW + timedelta(seconds=1))
    running_event = RecoveryTransitionEvent.from_status(
        running,
        previous=initial,
    )
    terminal = running.complete(
        result=RecoveryWorkerResult(
            outcome=RecoveryTerminalOutcome.FAILED,
            reason_code="global_discovery_recovery_preparation_failed",
            retryable=True,
            counts=running.counts,
        ),
        completed_at=NOW + timedelta(seconds=2),
        active_elapsed_ms=1_000,
    )
    terminal_event = RecoveryTransitionEvent.from_status(
        terminal,
        previous=running,
    )

    assert initial_event.metric_labels == {
        "operation": "preflight",
        "outcome": "queued",
        "phase": "queued",
        "reason_code": "recovery_preparation_queued",
    }
    assert running_event.metric_labels == {
        "operation": "dispatch",
        "outcome": "running",
        "phase": "preparing",
        "reason_code": "recovery_preparing",
    }
    assert terminal_event.metric_labels == {
        "operation": "recovery_terminal",
        "outcome": "failed",
        "phase": "terminal",
        "reason_code": "global_discovery_recovery_preparation_failed",
    }
    assert {
        event.progress_seq for event in (initial_event, running_event, terminal_event)
    } == {0, 1, 2}


def command(**binding_changes: str) -> RecoveryStartCommand:
    binding = RecoveryRunBinding(
        run_id="run-control",
        actor_id="agent-one",
        confirmation_fingerprint="sha256:confirmation",
        manifest_ref="global_discovery_manifest_content",
        preflight_hash="preflight-one",
        reason="operator confirmed bounded recovery",
    )
    return RecoveryStartCommand(
        binding=replace(binding, **binding_changes),
        started_at=NOW,
        counts=prepared_result().counts,
    )


def preparation_command(*, run_id: str = "run-preparation") -> RecoveryPreparationCommand:
    return RecoveryPreparationCommand(
        binding=RecoveryRunBinding(run_id=run_id, actor_id="agent-one"),
        admitted_at=NOW,
        counts=RecoveryProgressCounts(),
    )


def prepared_result() -> RecoveryPreparedResult:
    return RecoveryPreparedResult(
        manifest_ref="global_discovery_manifest_content",
        preflight_hash="preflight-one",
        snapshot_fingerprint="snapshot-one",
        prepared_at=NOW,
        expires_at=NOW + timedelta(seconds=300),
        counts=RecoveryProgressCounts(
            boards_total=2,
            boards_scanned=2,
            sources_total=3,
            sources_processed=3,
        ),
    )


def stage_prepared(
    control: RecoveryControlPlane,
    store: MemoryRecoveryControlStore,
    *,
    run_id: str = "run-control",
) -> None:
    queued = control.prepare(preparation_command(run_id=run_id))
    preparing = store.mark_preparing(run_id=run_id, epoch=queued.epoch, at=NOW)
    store.mark_prepared(
        run_id=run_id,
        epoch=preparing.epoch,
        expected_progress_seq=preparing.progress_seq,
        prepared=prepared_result(),
    )


def test_preparation_uses_the_single_global_slot_and_dispatches_physical_attempt_identity() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)

    admitted = control.prepare(preparation_command())
    with pytest.raises(RecoveryBindingConflict):
        control.prepare(
            RecoveryPreparationCommand(
                binding=RecoveryRunBinding(
                    run_id=admitted.run_id,
                    actor_id="agent-two",
                ),
                admitted_at=NOW + timedelta(seconds=1),
                counts=RecoveryProgressCounts(),
            )
        )

    assert admitted.run_id == "run-preparation"
    assert admitted.state is RecoveryRunState.PENDING
    assert admitted.phase is RecoveryRunPhase.QUEUED
    assert admitted.confirmation_state is RecoveryConfirmationState.UNCONFIRMED
    assert admitted.slot_ownership.slot_id == GLOBAL_RECOVERY_SLOT_ID
    assert dispatcher.dispatched == [
        (
            admitted.run_id,
            1,
            recovery_attempt_id(admitted.run_id, 1),
            RecoveryDispatchKind.PREPARATION,
        )
    ]
    with pytest.raises(RecoveryInProgress) as exc_info:
        control.prepare(
            RecoveryPreparationCommand(
                binding=RecoveryRunBinding(
                    run_id="different-racing-run",
                    actor_id="agent-two",
                ),
                admitted_at=NOW + timedelta(seconds=1),
                counts=RecoveryProgressCounts(),
            )
        )
    assert str(exc_info.value) == "recovery_in_progress"
    assert vars(exc_info.value) == {}


def test_preparing_to_prepared_is_the_only_complete_manifest_transition() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    queued = control.prepare(preparation_command())
    preparing = store.mark_preparing(run_id=queued.run_id, epoch=1, at=NOW)
    prepared = store.mark_prepared(
        run_id=queued.run_id,
        epoch=1,
        expected_progress_seq=preparing.progress_seq,
        prepared=prepared_result(),
    )

    assert preparing.state is RecoveryRunState.RUNNING
    assert preparing.phase is RecoveryRunPhase.PREPARING
    assert prepared.state is RecoveryRunState.PENDING
    assert prepared.phase is RecoveryRunPhase.PREPARED
    assert prepared.preparation_state == "prepared"
    assert prepared.attempt_id == "run-preparation/attempt-1"
    assert prepared.to_dict()["counts"]["boards_scanned"] == 2
    assert prepared.to_dict()["manifest_ref"] == prepared_result().manifest_ref
    assert prepared.to_dict()["preflight_hash"] == prepared_result().preflight_hash
    assert (
        prepared.to_dict()["action_required"]
        == "call_okto_pulse_kg_global_discovery_recovery_confirm"
    )

    takeover = RecoveryLeaseTakeoverPolicy().evaluate(
        prepared,
        now=prepared.heartbeat_at + timedelta(seconds=20),
    )
    assert takeover.admitted is False
    assert takeover.reason_code == "prepared_run_not_adoptable"


def test_prepared_cancel_is_immediate_reason_aware_and_phase_coherent() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    queued = control.prepare(preparation_command())
    preparing = store.mark_preparing(run_id=queued.run_id, epoch=1, at=NOW)
    store.mark_prepared(
        run_id=queued.run_id,
        epoch=1,
        expected_progress_seq=preparing.progress_seq,
        prepared=prepared_result(),
    )

    cancelled = control.cancel(
        run_id=queued.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=6),
        requested_by_actor_id="cancelling-admin",
        reason="operator withdrew approval",
    )
    assert cancelled.state is RecoveryRunState.CANCELLED
    assert cancelled.phase is RecoveryRunPhase.TERMINAL
    assert cancelled.audit_reason == "operator withdrew approval"
    assert cancelled.actor_id == "agent-one"
    assert cancelled.cancel_requested_by_actor_id == "cancelling-admin"

    with pytest.raises(RecoveryAuditReasonInvalid) as exc_info:
        control.cancel(
            run_id=queued.run_id,
            expected_epoch=1,
            requested_at=NOW + timedelta(seconds=7),
            requested_by_actor_id="cancelling-admin",
            reason="x" * 513,
        )
    assert exc_info.value.code == "recovery_audit_reason_invalid"


def test_incoherent_state_phase_pair_fails_with_typed_code() -> None:
    status = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=status, dispatcher=RecordingDispatcher())
    queued = control.prepare(preparation_command())
    with pytest.raises(RecoveryPhaseIncoherent) as exc_info:
        replace(queued, state=RecoveryRunState.PENDING, phase=RecoveryRunPhase.PREPARING)
    assert exc_info.value.code == "recovery_phase_incoherent"


def test_start_is_idempotent_only_for_the_exact_immutable_binding() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)

    stage_prepared(control, store)
    first = control.start(command())
    replay = control.start(command())

    assert replay == first
    assert first.state is RecoveryRunState.PENDING
    assert first.epoch == 1
    assert first.phase is RecoveryRunPhase.CONFIRMED
    assert first.progress_seq == 3
    assert dispatcher.dispatched == [
        (
            "run-control",
            1,
            "run-control/attempt-1",
            RecoveryDispatchKind.PREPARATION,
        ),
        (
            "run-control",
            1,
            "run-control/attempt-1",
            RecoveryDispatchKind.RECOVERY,
        ),
        (
            "run-control",
            1,
            "run-control/attempt-1",
            RecoveryDispatchKind.RECOVERY,
        ),
    ]

    with pytest.raises(RecoveryBindingConflict) as exc_info:
        control.start(command(manifest_ref="global_discovery_manifest_other"))

    assert exc_info.value.code == "recovery_binding_conflict"
    assert control.status("run-control") == first
    assert len(dispatcher.dispatched) == 3


def test_status_projection_is_closed_and_cancel_is_durable_idempotent() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)
    stage_prepared(control, store)
    control.start(command())
    running = store.mark_running(run_id="run-control", epoch=1, at=NOW)

    cancelled = control.cancel(
        run_id="run-control",
        expected_epoch=1,
        requested_at=NOW,
        requested_by_actor_id="cancelling-admin",
    )
    replay = control.cancel(
        run_id="run-control",
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=1),
        requested_by_actor_id="different-admin",
    )

    assert cancelled.state is RecoveryRunState.RUNNING
    assert cancelled.cancel_requested_at == NOW
    assert cancelled.cancel_requested_by_actor_id == "cancelling-admin"
    assert replay.cancel_requested_by_actor_id == "cancelling-admin"
    assert replay == cancelled
    assert cancelled.progress_seq == running.progress_seq + 1
    assert cancelled.reason_code == "recovery_cancel_requested"
    assert {
        "run_id",
        "actor_id",
        "epoch",
        "attempt_id",
        "state",
        "progress_seq",
        "phase",
        "counts",
        "heartbeat_at",
        "started_at",
        "updated_at",
        "active_elapsed_ms",
        "active_deadline_at",
        "cumulative_active_ms",
        "cancel_requested_at",
        "cancel_requested_by_actor_id",
        "resume_requested_at",
        "resume_requested_by_actor_id",
        "resume_audit_reason",
        "terminal_outcome",
        "reason_code",
        "retryable",
        "supersedes_epoch",
        "superseded_by_epoch",
        "preparation_state",
        "confirmation_state",
        "status_tool",
        "audit_reason",
        "physical_truth",
    } <= set(cancelled.to_dict())


def test_cancel_rejects_a_stale_expected_epoch_without_mutation() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    control.start(command())
    running = store.mark_running(run_id="run-control", epoch=1, at=NOW)

    with pytest.raises(RecoveryCheckpointConflict) as exc_info:
        control.cancel(
            run_id="run-control",
            expected_epoch=2,
            requested_at=NOW + timedelta(seconds=1),
            requested_by_actor_id="cancelling-admin",
        )

    assert exc_info.value.code == "recovery_epoch_conflict"
    assert exc_info.value.expected_epoch == 2
    assert exc_info.value.actual_epoch == 1
    assert control.status("run-control") == running


def test_worker_transitions_checkpoint_and_complete_under_epoch_progress_cas() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)
    checkpoint = store.compare_and_set_checkpoint(
        RecoveryCheckpoint(
            run_id=confirmed.run_id,
            epoch=1,
            expected_progress_seq=running.progress_seq,
            phase=RecoveryRunPhase.CUTOVER,
            heartbeat_at=NOW + timedelta(seconds=5),
            active_elapsed_ms=5_000,
            counts=RecoveryProgressCounts(
                boards_total=2,
                boards_scanned=2,
                sources_total=3,
                sources_processed=3,
            ),
        )
    )

    with pytest.raises(RecoveryCheckpointConflict):
        store.compare_and_set_checkpoint(
            RecoveryCheckpoint(
                run_id=confirmed.run_id,
                epoch=1,
                expected_progress_seq=running.progress_seq,
                phase=RecoveryRunPhase.CUTOVER,
                heartbeat_at=NOW + timedelta(seconds=6),
                active_elapsed_ms=6_000,
                counts=checkpoint.counts,
            )
        )

    terminal = store.complete(
        run_id=confirmed.run_id,
        epoch=1,
        expected_progress_seq=checkpoint.progress_seq,
        completed_at=NOW + timedelta(seconds=7),
        active_elapsed_ms=7_000,
        result=RecoveryWorkerResult(
            outcome="success",
            reason_code="recovery_completed",
            retryable=False,
            counts=RecoveryProgressCounts(
                boards_total=2,
                boards_scanned=2,
                sources_total=3,
                sources_processed=3,
                nodes_written=5,
                edges_written=4,
                outbox_events_drained=1,
                errors=0,
            ),
        ),
    )

    assert terminal.state is RecoveryRunState.SUCCESS
    assert terminal.terminal_outcome.value == "success"
    assert terminal.progress_seq == checkpoint.progress_seq + 1
    assert terminal.cumulative_active_ms == 7_000
    assert control.status(confirmed.run_id) == terminal


def test_takeover_policy_enforces_exact_lease_boundary_and_charges_crash() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    control.start(command())
    running = store.mark_running(run_id="run-control", epoch=1, at=NOW)
    heartbeat = store.compare_and_set_checkpoint(
        RecoveryCheckpoint(
            run_id="run-control",
            epoch=1,
            expected_progress_seq=running.progress_seq,
            phase=RecoveryRunPhase.CUTOVER,
            heartbeat_at=NOW + timedelta(seconds=5),
            active_elapsed_ms=5_000,
            counts=prepared_result().counts,
        )
    )
    policy = RecoveryLeaseTakeoverPolicy()

    before = policy.evaluate(
        heartbeat,
        now=heartbeat.heartbeat_at + timedelta(milliseconds=14_999),
    )
    at_boundary = policy.evaluate(
        heartbeat,
        now=heartbeat.heartbeat_at + timedelta(seconds=15),
    )

    assert before.admitted is False
    assert before.reason_code == "worker_lease_active"
    assert before.next_epoch is None
    assert at_boundary.admitted is True
    assert at_boundary.reason_code == "recovery_takeover_admitted"
    assert at_boundary.next_epoch == 2
    assert at_boundary.charged_cumulative_ms == 20_000
    assert at_boundary.attempt_budget_ms == 10 * 60 * 1_000


def test_retryable_is_meaningful_only_for_failed_worker_results() -> None:
    with pytest.raises(ValueError, match="only valid for failed"):
        RecoveryWorkerResult(
            outcome="success",
            reason_code="invalid_success",
            retryable=True,
            counts=RecoveryProgressCounts(),
        )


class FailOnceDispatcher(RecordingDispatcher):
    def __init__(self, *, fail_kind: RecoveryDispatchKind) -> None:
        super().__init__()
        self.fail_kind = fail_kind
        self.failed = False

    def dispatch(self, **kwargs) -> None:
        super().dispatch(**kwargs)
        if kwargs["kind"] is self.fail_kind and not self.failed:
            self.failed = True
            raise RuntimeError("simulated_dispatch_crash")


def test_exact_queued_replay_redispatches_after_store_commit_dispatch_crash() -> None:
    store = MemoryRecoveryControlStore()
    command_to_prepare = preparation_command()
    crashing = RecoveryControlPlane(
        store=store,
        dispatcher=FailOnceDispatcher(fail_kind=RecoveryDispatchKind.PREPARATION),
    )

    with pytest.raises(RuntimeError, match="simulated_dispatch_crash"):
        crashing.prepare(command_to_prepare)

    replay_dispatcher = RecordingDispatcher()
    replay = RecoveryControlPlane(
        store=store,
        dispatcher=replay_dispatcher,
    ).prepare(command_to_prepare)
    assert replay.phase is RecoveryRunPhase.QUEUED
    assert replay_dispatcher.dispatched == [
        (
            replay.run_id,
            replay.epoch,
            replay.attempt_id,
            RecoveryDispatchKind.PREPARATION,
        )
    ]


def test_exact_confirmed_replay_redispatches_after_store_commit_dispatch_crash() -> None:
    store = MemoryRecoveryControlStore()
    setup = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(setup, store)
    crashing = RecoveryControlPlane(
        store=store,
        dispatcher=FailOnceDispatcher(fail_kind=RecoveryDispatchKind.RECOVERY),
    )

    with pytest.raises(RuntimeError, match="simulated_dispatch_crash"):
        crashing.start(command())

    replay_dispatcher = RecordingDispatcher()
    replay = RecoveryControlPlane(
        store=store,
        dispatcher=replay_dispatcher,
    ).start(command())
    assert replay.phase is RecoveryRunPhase.CONFIRMED
    assert replay_dispatcher.dispatched == [
        (
            replay.run_id,
            replay.epoch,
            replay.attempt_id,
            RecoveryDispatchKind.RECOVERY,
        )
    ]


def test_explicit_resume_preserves_original_actor_and_audits_requester() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)
    terminal = store.complete(
        run_id=running.run_id,
        epoch=1,
        expected_progress_seq=running.progress_seq,
        completed_at=NOW + timedelta(seconds=1),
        active_elapsed_ms=1_000,
        result=RecoveryWorkerResult(
            outcome="partial",
            reason_code="recovery_partial",
            retryable=False,
            counts=running.counts,
            physical_truth=RecoveryPhysicalTruth(
                attempt_id=running.attempt_id,
                journal_phase="pointer_replaced",
                pointer_replaced=True,
                rollback_performed=False,
                evidence_ref="journal://attempt-1",
            ),
        ),
    )

    resumed = control.resume(
        run_id=terminal.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=2),
        requested_by_actor_id="resuming-admin",
        reason="retry partial cutover",
    )
    replay = control.resume(
        run_id=terminal.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=3),
        requested_by_actor_id="resuming-admin",
        reason="retry partial cutover",
    )

    assert resumed == replay
    assert resumed.actor_id == "agent-one"
    assert resumed.epoch == 2
    assert resumed.attempt_id == "run-control/attempt-2"
    assert resumed.phase is RecoveryRunPhase.CONFIRMED
    assert resumed.resume_requested_by_actor_id == "resuming-admin"
    assert resumed.resume_requested_at == NOW + timedelta(seconds=2)
    assert resumed.resume_audit_reason == "retry partial cutover"
    assert terminal.physical_truth is not None
    assert terminal.physical_truth.attempt_id == "run-control/attempt-1"
    assert resumed.physical_truth is None
    assert store.get_attempt_history(run_id=terminal.run_id, epoch=1) == terminal
    assert dispatcher.dispatched[-2:] == [
        (
            resumed.run_id,
            2,
            resumed.attempt_id,
            RecoveryDispatchKind.RECOVERY,
        ),
        (
            resumed.run_id,
            2,
            resumed.attempt_id,
            RecoveryDispatchKind.RECOVERY,
        ),
    ]


def test_lease_takeover_clears_prior_attempt_physical_truth_from_epoch_two() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)
    prior_attempt = replace(
        running,
        physical_truth=RecoveryPhysicalTruth(
            attempt_id=running.attempt_id,
            journal_phase="candidate_written",
            pointer_replaced=False,
            rollback_performed=False,
            evidence_ref="journal://attempt-1-running",
        ),
    )
    takeover_at = prior_attempt.heartbeat_at + timedelta(seconds=15)
    decision = RecoveryLeaseTakeoverPolicy().evaluate(
        prior_attempt,
        now=takeover_at,
    )

    taken_over = prior_attempt.take_over_lease(
        decision=decision,
        requested_at=takeover_at,
        requested_by_actor_id="takeover-admin",
        reason="expired worker lease",
    )

    assert prior_attempt.physical_truth is not None
    assert prior_attempt.physical_truth.attempt_id == "run-control/attempt-1"
    assert taken_over.epoch == 2
    assert taken_over.attempt_id == "run-control/attempt-2"
    assert taken_over.physical_truth is None


def test_cancel_pending_running_lease_cannot_be_taken_over() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)
    cancel_pending = control.cancel(
        run_id=running.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=1),
        requested_by_actor_id="cancelling-admin",
        reason="cancel survives worker crash",
    )

    with pytest.raises(RecoveryResumeRejected) as exc_info:
        control.resume(
            run_id=running.run_id,
            expected_epoch=1,
            requested_at=NOW + timedelta(seconds=15),
            requested_by_actor_id="takeover-admin",
            reason="expired lease",
        )
    assert exc_info.value.code == "recovery_cancel_pending"
    assert control.status(running.run_id) == cancel_pending


def test_memory_store_admits_expired_lease_takeover_as_attempt_two() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)

    taken_over = control.resume(
        run_id=running.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=15),
        requested_by_actor_id="takeover-admin",
        reason="expired lease takeover",
    )

    assert taken_over.epoch == 2
    assert taken_over.attempt_id == "run-control/attempt-2"
    assert taken_over.phase is RecoveryRunPhase.CUTOVER
    assert taken_over.resume_requested_by_actor_id == "takeover-admin"
    assert store.get_attempt_history(run_id=running.run_id, epoch=1) == running
    assert dispatcher.dispatched[-1] == (
        taken_over.run_id,
        2,
        taken_over.attempt_id,
        RecoveryDispatchKind.RECOVERY,
    )


def test_retryable_preparation_failure_resumes_queued_with_zero_counts() -> None:
    store = MemoryRecoveryControlStore()
    dispatcher = RecordingDispatcher()
    control = RecoveryControlPlane(store=store, dispatcher=dispatcher)
    queued = control.prepare(preparation_command())
    preparing = store.mark_preparing(run_id=queued.run_id, epoch=1, at=NOW)
    partial = store.compare_and_set_checkpoint(
        RecoveryCheckpoint(
            run_id=preparing.run_id,
            epoch=1,
            expected_progress_seq=preparing.progress_seq,
            phase=RecoveryRunPhase.PREPARING,
            heartbeat_at=NOW + timedelta(seconds=1),
            active_elapsed_ms=1_000,
            counts=RecoveryProgressCounts(
                boards_total=2,
                boards_scanned=1,
                sources_total=3,
                sources_processed=1,
            ),
        )
    )
    failed = store.complete(
        run_id=partial.run_id,
        epoch=1,
        expected_progress_seq=partial.progress_seq,
        completed_at=NOW + timedelta(seconds=2),
        active_elapsed_ms=2_000,
        result=RecoveryWorkerResult(
            outcome="failed",
            reason_code="preparation_retryable_failure",
            retryable=True,
            counts=partial.counts,
        ),
    )

    resumed = control.resume(
        run_id=failed.run_id,
        expected_epoch=1,
        requested_at=NOW + timedelta(seconds=3),
        requested_by_actor_id="resuming-admin",
        reason="retry preparation",
    )

    assert failed.counts.boards_scanned == 1
    assert resumed.epoch == 2
    assert resumed.phase is RecoveryRunPhase.QUEUED
    assert resumed.counts == RecoveryProgressCounts()
    assert resumed.binding.manifest_ref is None
    assert dispatcher.dispatched[-1] == (
        resumed.run_id,
        2,
        resumed.attempt_id,
        RecoveryDispatchKind.PREPARATION,
    )


def test_checkpoint_rejects_changed_totals_and_regressing_counters() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)

    for counts in (
        replace(running.counts, sources_total=4),
        replace(running.counts, sources_processed=2),
    ):
        with pytest.raises(RecoveryProgressInvariantViolation) as exc_info:
            store.compare_and_set_checkpoint(
                RecoveryCheckpoint(
                    run_id=running.run_id,
                    epoch=1,
                    expected_progress_seq=running.progress_seq,
                    phase=RecoveryRunPhase.CUTOVER,
                    heartbeat_at=NOW + timedelta(seconds=1),
                    active_elapsed_ms=1_000,
                    counts=counts,
                )
            )
        assert exc_info.value.code == "recovery_progress_invariant_violation"
    assert store.get_status(run_id=running.run_id) == running


def test_completion_rejects_counter_regression_and_budget_overrun() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    confirmed = control.start(command())
    running = store.mark_running(run_id=confirmed.run_id, epoch=1, at=NOW)

    with pytest.raises(RecoveryProgressInvariantViolation):
        store.complete(
            run_id=running.run_id,
            epoch=1,
            expected_progress_seq=running.progress_seq,
            completed_at=NOW + timedelta(seconds=1),
            active_elapsed_ms=1_000,
            result=RecoveryWorkerResult(
                outcome="failed",
                reason_code="regressed",
                retryable=True,
                counts=replace(running.counts, boards_scanned=1),
            ),
        )

    with pytest.raises(ValueError, match="attempt budget"):
        store.complete(
            run_id=running.run_id,
            epoch=1,
            expected_progress_seq=running.progress_seq,
            completed_at=NOW + timedelta(days=30),
            active_elapsed_ms=10 * 60 * 1_000 + 1,
            result=RecoveryWorkerResult(
                outcome="success",
                reason_code="late_success",
                retryable=False,
                counts=running.counts,
            ),
        )

    terminal = store.complete(
        run_id=running.run_id,
        epoch=1,
        expected_progress_seq=running.progress_seq,
        completed_at=NOW + timedelta(days=365),
        active_elapsed_ms=1_000,
        result=RecoveryWorkerResult(
            outcome="success",
            reason_code="monotonic_budget_success",
            retryable=False,
            counts=running.counts,
        ),
    )
    assert terminal.state is RecoveryRunState.SUCCESS


def test_prepared_and_confirmation_transitions_enforce_monotonic_ttl_fence() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    queued = control.prepare(preparation_command(run_id="run-control"))
    preparing = store.mark_preparing(run_id=queued.run_id, epoch=1, at=NOW)
    with pytest.raises(ValueError, match="prepared_at"):
        store.mark_prepared(
            run_id=queued.run_id,
            epoch=1,
            expected_progress_seq=preparing.progress_seq,
            prepared=replace(
                prepared_result(),
                prepared_at=NOW - timedelta(seconds=1),
                expires_at=NOW + timedelta(seconds=299),
            ),
        )

    prepared = store.mark_prepared(
        run_id=queued.run_id,
        epoch=1,
        expected_progress_seq=preparing.progress_seq,
        prepared=prepared_result(),
    )
    expired_command = replace(
        command(),
        started_at=prepared.expires_at,
        confirmation_consumed_at=prepared.expires_at,
    )
    with pytest.raises(ValueError, match="expired"):
        control.start(expired_command)
    assert control.status(prepared.run_id) == prepared


def test_restart_replay_can_publish_a_still_fresh_earlier_manifest() -> None:
    store = MemoryRecoveryControlStore()
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    queued = control.prepare(preparation_command(run_id="run-restarted-preparation"))
    preparing = store.mark_preparing(
        run_id=queued.run_id,
        epoch=1,
        at=NOW + timedelta(seconds=2),
    )
    manifest = replace(
        prepared_result(),
        prepared_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=301),
    )

    prepared = store.mark_prepared(
        run_id=queued.run_id,
        epoch=1,
        expected_progress_seq=preparing.progress_seq,
        prepared=manifest,
    )

    assert prepared.prepared_at == manifest.prepared_at
    assert prepared.updated_at == preparing.updated_at


def test_store_observed_clock_fences_delayed_confirmation_at_expiry() -> None:
    observed_at = [NOW + timedelta(seconds=1)]
    store = MemoryRecoveryControlStore(clock=lambda: observed_at[0])
    control = RecoveryControlPlane(store=store, dispatcher=RecordingDispatcher())
    stage_prepared(control, store)
    before_expiry_command = replace(
        command(),
        started_at=NOW + timedelta(seconds=1),
        confirmation_consumed_at=NOW + timedelta(seconds=1),
    )
    observed_at[0] = prepared_result().expires_at

    with pytest.raises(ValueError, match="store admission CAS"):
        control.start(before_expiry_command)
    status = control.status("run-control")
    assert status.phase is RecoveryRunPhase.PREPARED
    assert status.confirmation_state is RecoveryConfirmationState.PREPARED
