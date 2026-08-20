from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from okto_pulse.core.ports.consolidation import (
    build_exact_consolidation_compensation_binding,
)

from okto_pulse.core.application.rebuild_processor import (
    CompensationAction,
    QueueObservation,
    RebuildCheckpoint,
    RebuildCommand,
    RebuildEffectReceipt,
    RebuildOutcomeCode,
    RebuildPlan,
    RebuildProcessor,
    RebuildState,
    validate_legacy_manual_restore_queue_only_outcome,
)


class SimulatedProcessCrash(BaseException):
    pass


class FakeClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 11, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class FakeEffects:
    def __init__(self, clock: FakeClock, depths: list[int]) -> None:
        self.clock = clock
        self.depths = list(depths)
        self.checkpoints: dict[str, RebuildCheckpoint] = {}
        self.receipts: dict[str, RebuildEffectReceipt] = {}
        self.fail_effect: str | None = None
        self.calls: list[str] = []
        self.compensation_actions: tuple[CompensationAction, ...] = ()
        self.compensation_failed_state: RebuildState | None = None
        self.compensation_intent: RebuildEffectReceipt | None = None
        self.crash_after: str | None = None
        self.crashed = False
        self.receipt_creations: dict[str, int] = {}
        self.blocking_reason: str | None = None

    def load_checkpoint(self, run_id: str):  # noqa: ANN201
        return self.checkpoints.get(run_id)

    def save_checkpoint(self, checkpoint: RebuildCheckpoint) -> None:
        self.checkpoints[checkpoint.command.run_id] = checkpoint

    def _effect(self, name: str, effect_key: str) -> RebuildEffectReceipt:
        self.calls.append(name)
        receipt = self.receipts.get(effect_key)
        if receipt is None:
            self.receipt_creations[name] = self.receipt_creations.get(name, 0) + 1
            receipt = RebuildEffectReceipt(
                effect_key=effect_key,
                effect=name,
                ok=self.fail_effect != name,
                code="forced_failure" if self.fail_effect == name else "ok",
            )
            self.receipts[effect_key] = receipt
        if self.crash_after == name and not self.crashed:
            self.crashed = True
            raise SimulatedProcessCrash("simulated process crash after durable effect")
        return receipt

    def snapshot(self, command, *, effect_key):  # noqa: ANN001, ANN201
        return self._effect("snapshot", effect_key)

    def quarantine(self, command, *, effect_key):  # noqa: ANN001, ANN201
        return self._effect("quarantine", effect_key)

    def enqueue(self, command, *, effect_key):  # noqa: ANN001, ANN201
        return self._effect("enqueue", effect_key)

    def restore(self, command, *, effect_key):  # noqa: ANN001, ANN201
        return self._effect("restore", effect_key)

    def promote(self, command, *, effect_key):  # noqa: ANN001, ANN201
        return self._effect("promote", effect_key)

    def wait_for_queue_observation(
        self,
        command,
        *,
        after_sequence,
        max_wait_seconds,  # noqa: ANN001
    ) -> QueueObservation:
        del command
        self.calls.append("observe")
        self.clock.advance(max_wait_seconds)
        depth = self.depths.pop(0) if self.depths else 0
        return QueueObservation(
            depth,
            self.clock(),
            after_sequence + 1,
            blocking_reason=self.blocking_reason,
        )

    def compensate(self, command, *, effect_key):  # noqa: ANN001, ANN201
        self.calls.append("compensate")
        self.compensation_actions = command.actions
        self.compensation_failed_state = command.failed_state
        self.compensation_intent = command.reconciliation_intent
        if command.reconciliation_intent is not None:
            intent_details = dict(command.reconciliation_intent.details)
            intent_digest = intent_details["intent_digest"]
            queue = dict(intent_details["queue"])
            expected_row_count = len(queue["rows"])
            return RebuildEffectReceipt(
                effect_key,
                "compensate",
                True,
                code="legacy_manual_restore_queue_only_reconciled",
                details={
                    "actions": ["cancel_enqueued_sources"],
                    "reconciliation_kind": "legacy_manual_restore_queue_only",
                    "intent_digest": intent_digest,
                    "queue": {
                        "source": queue["source"],
                        "expected_row_count": expected_row_count,
                        "terminal_fingerprint": "b" * 64,
                        "pending_compensated": 1,
                        "claimed_compensated": 1,
                        "already_compensated": 0,
                        "active_remaining": 0,
                        "live_intents_restored": 0,
                        "total_compensated": 2,
                        "evidence_digest": intent_digest,
                    },
                },
            )
        details = {}
        if CompensationAction.COMPENSATE_EXACT_RELATIONAL_COMMITS in command.actions:
            details["exact_relational_compensation"] = (
                build_exact_consolidation_compensation_binding(
                    board_id=command.board_id,
                    source="rebuild:manifest-1",
                    reservation_lineage_id="a" * 64,
                    result=None,
                )
            )
        return RebuildEffectReceipt(
            effect_key,
            "compensate",
            True,
            details=details,
        )

    def record_audit(self, outcome, *, effect_key):  # noqa: ANN001, ANN201
        self.calls.append(f"audit:{outcome.code.value}")
        return RebuildEffectReceipt(effect_key, "audit", True)


def _command(**overrides) -> RebuildCommand:  # noqa: ANN003
    values = {
        "run_id": "run-1",
        "board_id": "board-1",
        "manifest_ref": "manifest-1",
        "operation": "rebuild",
        "actor_id": "operator",
        "reason": "test",
        "candidate_generation_id": "gen-2",
    }
    values.update(overrides)
    return RebuildCommand(**values)


def _processor(effects: FakeEffects, clock: FakeClock) -> RebuildProcessor:
    return RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
    )


def _legacy_blocked_receipts(
    command: RebuildCommand,
) -> dict[str, RebuildEffectReceipt]:
    return {
        f"{command.run_id}:{effect}": RebuildEffectReceipt(
            effect_key=f"{command.run_id}:{effect}",
            effect=effect,
            ok=True,
        )
        for effect in ("snapshot", "quarantine", "enqueue")
    }


def _legacy_blocked_intent(command: RebuildCommand) -> RebuildEffectReceipt:
    return RebuildEffectReceipt(
        effect_key=(
            f"{command.run_id}:legacy_manually_restored_blocked_after_enqueue_intent"
        ),
        effect="legacy_manually_restored_blocked_after_enqueue_intent",
        ok=True,
        code="legacy_manual_restore_queue_only_authorized",
        details={
            "legacy_run_id": command.run_id,
            "board_id": command.board_id,
            "manifest_ref": command.manifest_ref,
            "intent_ref": "legacy-intent-1",
            "intent_digest": "a" * 64,
            "recovery_run_id": "offline-recovery-1",
            "recovery_actor_id": "board-owner",
            "recovery_reason": "adopt governed manual restore and fence legacy queue",
            "preapplied_actions": [
                "restore_quarantine",
                "discard_candidate_generation",
            ],
            "remaining_actions": ["cancel_enqueued_sources"],
            "queue": {
                "source": f"rebuild:{command.manifest_ref}",
                "rows": [{"id": "queue-1"}, {"id": "queue-2"}],
            },
        },
    )


def _legacy_queue_only_compensation(
    command: RebuildCommand,
    intent: RebuildEffectReceipt,
) -> RebuildEffectReceipt:
    intent_details = dict(intent.details)
    intent_digest = intent_details["intent_digest"]
    queue = dict(intent_details["queue"])
    return RebuildEffectReceipt(
        f"{command.run_id}:compensate",
        "compensate",
        True,
        code="legacy_manual_restore_queue_only_reconciled",
        details={
            "actions": ["cancel_enqueued_sources"],
            "reconciliation_kind": "legacy_manual_restore_queue_only",
            "intent_digest": intent_digest,
            "queue": {
                "source": queue["source"],
                "expected_row_count": len(queue["rows"]),
                "terminal_fingerprint": "b" * 64,
                "pending_compensated": 1,
                "claimed_compensated": 1,
                "already_compensated": 0,
                "active_remaining": 0,
                "live_intents_restored": 0,
                "total_compensated": 2,
                "evidence_digest": intent_digest,
            },
        },
    )


def test_f06_success_orders_effects_and_allows_promotion() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [3, 2, 0])
    outcome = _processor(effects, clock).execute(_command())

    assert outcome.code is RebuildOutcomeCode.COMPLETED
    assert outcome.state is RebuildState.COMPLETED
    assert outcome.promotion_allowed is True
    assert effects.calls[:8] == [
        "snapshot",
        "quarantine",
        "enqueue",
        "observe",
        "observe",
        "observe",
        "restore",
        "promote",
    ]


def test_f06_stalled_drain_compensates_without_promotion() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4, 4, 4, 4])
    outcome = _processor(effects, clock).execute(_command())

    assert outcome.code is RebuildOutcomeCode.DRAIN_STALLED
    assert outcome.promotion_allowed is False
    assert CompensationAction.CANCEL_ENQUEUED_SOURCES in outcome.compensation_actions
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert effects.compensation_failed_state is RebuildState.DRAINING
    assert "promote" not in effects.calls


def test_f06_exact_stalled_drain_requires_bound_relational_compensation() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4, 4, 4, 4])
    command = _command(
        exact_relational_compensation=True,
        reservation_lineage_id="a" * 64,
    )

    outcome = _processor(effects, clock).execute(command)

    assert outcome.state is RebuildState.FAILED
    assert (
        CompensationAction.COMPENSATE_EXACT_RELATIONAL_COMMITS
        in outcome.compensation_actions
    )
    compensation = next(
        receipt for receipt in outcome.receipts if receipt.effect == "compensate"
    )
    assert compensation.ok is True
    assert dict(compensation.details)["exact_relational_compensation"]["status"] == (
        "not_required"
    )


def test_f06_exact_compensation_rejects_missing_relational_binding() -> None:
    class MissingBindingEffects(FakeEffects):
        def compensate(self, command, *, effect_key):  # noqa: ANN001, ANN201
            self.compensation_actions = command.actions
            return RebuildEffectReceipt(effect_key, "compensate", True)

    clock = FakeClock()
    effects = MissingBindingEffects(clock, [4, 4, 4, 4])
    command = _command(
        exact_relational_compensation=True,
        reservation_lineage_id="a" * 64,
    )

    outcome = _processor(effects, clock).execute(command)

    assert outcome.state is RebuildState.COMPENSATION_FAILED
    compensation = next(
        receipt for receipt in outcome.receipts if receipt.effect == "compensate"
    )
    assert compensation.ok is False
    assert compensation.code == "exact_relational_compensation_receipt_invalid"


def test_f06_exact_compensation_detaches_adapter_owned_nested_binding() -> None:
    class MutableBindingEffects(FakeEffects):
        binding: dict[str, object] | None = None

        def compensate(self, command, *, effect_key):  # noqa: ANN001, ANN201
            self.compensation_actions = command.actions
            self.binding = build_exact_consolidation_compensation_binding(
                board_id=command.board_id,
                source="rebuild:manifest-1",
                reservation_lineage_id="a" * 64,
                result=None,
            )
            return RebuildEffectReceipt(
                effect_key,
                "compensate",
                True,
                details={"exact_relational_compensation": self.binding},
            )

    clock = FakeClock()
    effects = MutableBindingEffects(clock, [4, 4, 4, 4])
    outcome = _processor(effects, clock).execute(
        _command(
            exact_relational_compensation=True,
            reservation_lineage_id="a" * 64,
        )
    )
    assert effects.binding is not None
    effects.binding["status"] = "tampered"

    compensation = next(
        receipt for receipt in outcome.receipts if receipt.effect == "compensate"
    )
    assert dict(compensation.details)["exact_relational_compensation"]["status"] == (
        "not_required"
    )


def test_f06_exact_pre_enqueue_failure_does_not_require_relational_binding() -> None:
    class QuarantineFailureEffects(FakeEffects):
        def quarantine(self, command, *, effect_key):  # noqa: ANN001, ANN201
            self.calls.append("quarantine")
            return RebuildEffectReceipt(
                effect_key,
                "quarantine",
                False,
                code="quarantine_failed",
            )

        def compensate(self, command, *, effect_key):  # noqa: ANN001, ANN201
            self.compensation_actions = command.actions
            assert (
                CompensationAction.COMPENSATE_EXACT_RELATIONAL_COMMITS
                not in command.actions
            )
            return RebuildEffectReceipt(effect_key, "compensate", True)

    clock = FakeClock()
    effects = QuarantineFailureEffects(clock, [])
    outcome = _processor(effects, clock).execute(
        _command(
            exact_relational_compensation=True,
            reservation_lineage_id="a" * 64,
        )
    )

    assert outcome.code is RebuildOutcomeCode.QUARANTINE_FAILED
    assert outcome.state is RebuildState.FAILED
    compensation = next(
        receipt for receipt in outcome.receipts if receipt.effect == "compensate"
    )
    assert compensation.ok is True


def test_f06_post_drain_manifest_drift_compensates_before_restore_or_promotion() -> (
    None
):
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    processor = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        source_revalidate=lambda: False,
    )

    outcome = processor.execute(_command())

    assert outcome.code is RebuildOutcomeCode.MANIFEST_DRIFT
    assert outcome.promotion_allowed is False
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls
    assert effects.calls == [
        "snapshot",
        "quarantine",
        "enqueue",
        "observe",
        "compensate",
        "audit:manifest_drift",
    ]
    assert CompensationAction.RESTORE_QUARANTINE in effects.compensation_actions
    assert outcome.promotion_allowed is False
    assert CompensationAction.CANCEL_ENQUEUED_SOURCES in outcome.compensation_actions
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert effects.compensation_failed_state is RebuildState.DRAINING
    assert "promote" not in effects.calls


def test_f06_known_queue_blocker_compensates_without_waiting_for_stall() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [11])
    effects.blocking_reason = "graph_memory_pressure"

    outcome = _processor(effects, clock).execute(_command())

    assert outcome.code is RebuildOutcomeCode.DRAIN_STALLED
    assert outcome.detail == "queue blocked:graph_memory_pressure"
    assert effects.calls.count("observe") == 1
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert "promote" not in effects.calls


def test_f06_new_dead_letter_blocks_even_when_queue_depth_is_zero() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    effects.blocking_reason = "rebuild_new_dead_letter"

    outcome = _processor(effects, clock).execute(_command())

    assert outcome.code is RebuildOutcomeCode.DRAIN_STALLED
    assert outcome.detail == "queue blocked:rebuild_new_dead_letter"
    assert effects.calls.count("observe") == 1
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_non_increasing_observation_sequence_compensates_fail_closed() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [1])

    def stale_observation(
        command,
        *,
        after_sequence,
        max_wait_seconds,  # noqa: ANN001, ANN202
    ):
        del command, max_wait_seconds
        effects.calls.append("observe")
        return QueueObservation(1, clock(), after_sequence)

    effects.wait_for_queue_observation = stale_observation  # type: ignore[method-assign]

    outcome = _processor(effects, clock).execute(_command())

    assert outcome.code is RebuildOutcomeCode.EFFECT_FAILED
    assert outcome.detail == "queue_observation_sequence_not_increasing"
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert "compensate" in effects.calls
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_cancellation_after_enqueue_compensates_and_stops() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4])
    checks = iter((False, False, False, True))
    processor = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        cancel_requested=lambda: next(checks),
    )

    outcome = processor.execute(_command())

    assert outcome.code is RebuildOutcomeCode.CANCELLED
    assert outcome.detail == "cancellation requested"
    assert CompensationAction.CANCEL_ENQUEUED_SOURCES in outcome.compensation_actions
    assert CompensationAction.RESTORE_QUARANTINE in outcome.compensation_actions
    assert "observe" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_lease_loss_after_enqueue_blocks_without_unsafe_compensation() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4])
    renewals = iter((True, True, True, False))
    processor = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        lease_renew=lambda: next(renewals),
    )

    outcome = processor.execute(_command())

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is RebuildState.BLOCKED
    assert outcome.detail == "single-writer lease lost"
    assert outcome.compensation_actions == ()
    assert "compensate" not in effects.calls
    assert "observe" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_drain_handoff_rebinds_writer_before_restore_and_promotion() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [2, 0])
    events: list[str] = []
    writer_released = False

    original_restore = effects.restore
    original_promote = effects.promote

    def restore(command, *, effect_key):  # noqa: ANN001, ANN201
        events.append(f"restore:{command.owner_token}")
        return original_restore(command, effect_key=effect_key)

    def promote(command, *, effect_key):  # noqa: ANN001, ANN201
        events.append(f"promote:{command.owner_token}")
        return original_promote(command, effect_key=effect_key)

    effects.restore = restore  # type: ignore[method-assign]
    effects.promote = promote  # type: ignore[method-assign]

    def renew() -> bool:
        assert writer_released is False
        events.append("renew")
        return True

    def release() -> bool:
        nonlocal writer_released
        events.append("release:token-a")
        writer_released = True
        return True

    def reacquire() -> str:
        nonlocal writer_released
        events.append("reacquire:token-b")
        writer_released = False
        return "token-b"

    outcome = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        lease_renew=renew,
        release_writer_for_drain=release,
        reacquire_writer_after_drain=reacquire,
    ).execute(_command(owner_token="token-a"))

    assert outcome.code is RebuildOutcomeCode.COMPLETED
    assert events.index("release:token-a") < events.index("reacquire:token-b")
    assert events.index("reacquire:token-b") < events.index("restore:token-b")
    assert events.index("restore:token-b") < events.index("promote:token-b")
    checkpoint = effects.checkpoints["run-1"]
    assert checkpoint.command.owner_token == "token-b"
    assert checkpoint.writer_handoff_count == 1
    assert checkpoint.writer_reacquire_count == 1


def test_f06_drain_failure_reacquires_before_compensation() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4, 4, 4, 4])
    events: list[str] = []
    original_compensate = effects.compensate

    def compensate(command, *, effect_key):  # noqa: ANN001, ANN201
        checkpoint = effects.checkpoints["run-1"]
        events.append(f"compensate:{checkpoint.command.owner_token}")
        return original_compensate(command, effect_key=effect_key)

    effects.compensate = compensate  # type: ignore[method-assign]

    outcome = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        lease_renew=lambda: True,
        release_writer_for_drain=lambda: events.append("release") or True,
        reacquire_writer_after_drain=lambda: events.append("reacquire") or "token-b",
    ).execute(_command(owner_token="token-a"))

    assert outcome.code is RebuildOutcomeCode.DRAIN_STALLED
    assert events[-2:] == ["reacquire", "compensate:token-b"]
    assert "promote" not in effects.calls


def test_f06_compensation_receipt_is_durable_before_post_effect_fence_loss() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [4, 4, 4, 4])
    compensation_applied = False
    original_compensate = effects.compensate

    def compensate(command, *, effect_key):  # noqa: ANN001, ANN201
        nonlocal compensation_applied
        receipt = original_compensate(command, effect_key=effect_key)
        compensation_applied = True
        return receipt

    effects.compensate = compensate  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        lease_renew=lambda: not compensation_applied,
    )

    outcome = processor.execute(_command())

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    checkpoint = effects.checkpoints["run-1"]
    receipt = checkpoint.receipts["run-1:compensate"]
    assert receipt.ok is True
    assert effects.calls.count("compensate") == 1
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_reacquire_contention_blocks_without_compensation_or_promotion() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])

    outcome = RebuildProcessor(
        effects,
        clock=clock,
        plan=RebuildPlan(
            stall_timeout_seconds=3,
            hard_timeout_seconds=8,
            observation_wait_seconds=1,
        ),
        lease_renew=lambda: True,
        release_writer_for_drain=lambda: True,
        reacquire_writer_after_drain=lambda: None,
    ).execute(_command(owner_token="token-a"))

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is RebuildState.BLOCKED
    assert outcome.compensation_actions == ()
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls
    assert "compensate" not in effects.calls
    checkpoint = effects.checkpoints["run-1"]
    assert checkpoint.writer_handoff_count == 1
    assert checkpoint.writer_reacquire_count == 0


def test_f06_handoff_checkpoint_failure_still_reacquires_before_raising() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    events: list[str] = []
    original_save = effects.save_checkpoint

    def save(checkpoint: RebuildCheckpoint) -> None:
        if (
            checkpoint.writer_handoff_count == 1
            and checkpoint.writer_reacquire_count == 0
        ):
            events.append("handoff-save-failed")
            raise RuntimeError("checkpoint unavailable")
        original_save(checkpoint)

    effects.save_checkpoint = save  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        release_writer_for_drain=lambda: events.append("release-a") or True,
        reacquire_writer_after_drain=lambda: events.append("reacquire-b") or "token-b",
    )

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        processor.execute(_command(owner_token="token-a"))

    assert events == ["release-a", "handoff-save-failed", "reacquire-b"]
    assert effects.calls == ["snapshot", "quarantine", "enqueue"]


@pytest.mark.parametrize("reacquire_mode", ("raises", "returns_none"))
def test_f06_crash_during_drain_is_not_masked_by_reacquire_failure(
    reacquire_mode: str,
) -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [1])
    events: list[str] = []

    def crash_during_observation(*_args, **_kwargs):  # noqa: ANN201
        raise SimulatedProcessCrash("original-drain-crash")

    def reacquire():  # noqa: ANN201
        events.append("reacquire-attempted")
        if reacquire_mode == "raises":
            raise RuntimeError("reacquire-failed")
        return None

    effects.wait_for_queue_observation = crash_during_observation  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        release_writer_for_drain=lambda: True,
        reacquire_writer_after_drain=reacquire,
    )

    with pytest.raises(SimulatedProcessCrash, match="original-drain-crash"):
        processor.execute(_command(owner_token="token-a"))

    assert events == ["reacquire-attempted"]


def test_f06_reservation_loss_during_drain_blocks_without_compensation() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [3])
    reservation_owned = True
    events: list[str] = []
    original_observe = effects.wait_for_queue_observation

    def observe(*args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        nonlocal reservation_owned
        observation = original_observe(*args, **kwargs)
        reservation_owned = False
        return observation

    effects.wait_for_queue_observation = observe  # type: ignore[method-assign]

    outcome = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: reservation_owned,
        release_writer_for_drain=lambda: events.append("release-a") or True,
        # The service callback refuses token B after proving reservation loss.
        reacquire_writer_after_drain=lambda: events.append("reacquire-refused"),
    ).execute(_command(owner_token="token-a"))

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is RebuildState.BLOCKED
    assert events == ["release-a", "reacquire-refused"]
    assert "compensate" not in effects.calls
    assert "restore" not in effects.calls
    assert "promote" not in effects.calls
    assert not any(call.startswith("audit:") for call in effects.calls)


def test_f06_reservation_loss_during_failed_effect_blocks_before_compensation() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    reservation_owned = True
    original_restore = effects.restore

    def restore(command, *, effect_key):  # noqa: ANN001, ANN201
        nonlocal reservation_owned
        effects.fail_effect = "restore"
        receipt = original_restore(command, effect_key=effect_key)
        reservation_owned = False
        return receipt

    effects.restore = restore  # type: ignore[method-assign]
    outcome = RebuildProcessor(
        effects,
        clock=clock,
        orchestration_renew=lambda: reservation_owned,
    ).execute(_command())

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is RebuildState.BLOCKED
    assert "compensate" not in effects.calls
    assert "promote" not in effects.calls
    assert not any(call.startswith("audit:") for call in effects.calls)


def test_f06_compensated_checkpoint_requires_a_fresh_manifest_attempt() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    command = _command()
    compensate = RebuildEffectReceipt(
        effect_key="run-1:compensate",
        effect="compensate",
        ok=True,
        code="compensated",
    )
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=command,
        state=RebuildState.FAILED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts={compensate.effect_key: compensate},
    )

    outcome = _processor(effects, clock).execute(command)

    assert outcome.code is RebuildOutcomeCode.RESUME_REQUIRES_NEW_MANIFEST
    assert outcome.state is RebuildState.BLOCKED
    assert outcome.detail == "rebuild_resume_requires_new_manifest"
    assert effects.calls == ["audit:rebuild_resume_requires_new_manifest"]


def test_f06_compensating_checkpoint_without_receipt_resumes_original_cleanup() -> None:
    clock = FakeClock()

    class _CrashBeforeCompensation(FakeEffects):
        def __init__(self) -> None:
            super().__init__(clock, [0])
            self.failed_once = False

        def save_checkpoint(self, checkpoint: RebuildCheckpoint) -> None:
            super().save_checkpoint(checkpoint)
            if checkpoint.state is RebuildState.COMPENSATING and not self.failed_once:
                self.failed_once = True
                raise SimulatedProcessCrash("crash after compensation intent")

    effects = _CrashBeforeCompensation()
    effects.fail_effect = "restore"

    with pytest.raises(SimulatedProcessCrash, match="compensation intent"):
        _processor(effects, clock).execute(_command())

    interrupted = effects.checkpoints["run-1"]
    assert interrupted.state is RebuildState.COMPENSATING
    assert interrupted.compensation_failed_state is RebuildState.RESTORED
    assert interrupted.compensation_failure_code is RebuildOutcomeCode.RESTORE_FAILED
    assert interrupted.compensation_failure_detail == "forced_failure"
    assert interrupted.compensation_actions == (
        CompensationAction.CANCEL_ENQUEUED_SOURCES,
        CompensationAction.RESTORE_QUARANTINE,
        CompensationAction.DISCARD_CANDIDATE_GENERATION,
    )
    assert "compensate" not in effects.calls

    resumed = _processor(effects, clock).execute(_command())

    assert resumed.code is RebuildOutcomeCode.RESTORE_FAILED
    assert resumed.state is RebuildState.FAILED
    assert effects.compensation_failed_state is RebuildState.RESTORED
    assert effects.calls.count("compensate") == 1
    assert effects.checkpoints["run-1"].receipts["run-1:compensate"].ok


def test_f06_failed_compensation_receipt_retries_instead_of_closing_attempt() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    command = _command()
    failed_receipt = RebuildEffectReceipt(
        effect_key="run-1:compensate",
        effect="compensate",
        ok=False,
        code="compensation_incomplete",
    )
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=command,
        state=RebuildState.COMPENSATION_FAILED,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.DRAINING,
        compensation_failure_code=RebuildOutcomeCode.HARD_TIMEOUT,
        compensation_failure_detail="queue did not drain",
        compensation_actions=(
            CompensationAction.CANCEL_ENQUEUED_SOURCES,
            CompensationAction.RESTORE_QUARANTINE,
            CompensationAction.DISCARD_CANDIDATE_GENERATION,
        ),
        receipts={failed_receipt.effect_key: failed_receipt},
    )

    resumed = _processor(effects, clock).execute(command)

    assert resumed.code is RebuildOutcomeCode.HARD_TIMEOUT
    assert resumed.detail == "queue did not drain"
    assert resumed.state is RebuildState.FAILED
    assert effects.calls[0] == "compensate"
    assert "audit:rebuild_resume_requires_new_manifest" not in effects.calls


@pytest.mark.parametrize(
    ("state", "expected_actions"),
    (
        (
            RebuildState.QUARANTINED,
            (
                CompensationAction.RESTORE_QUARANTINE,
                CompensationAction.DISCARD_CANDIDATE_GENERATION,
            ),
        ),
        (
            RebuildState.ENQUEUED,
            (
                CompensationAction.CANCEL_ENQUEUED_SOURCES,
                CompensationAction.RESTORE_QUARANTINE,
                CompensationAction.DISCARD_CANDIDATE_GENERATION,
            ),
        ),
        (
            RebuildState.DRAINING,
            (
                CompensationAction.CANCEL_ENQUEUED_SOURCES,
                CompensationAction.RESTORE_QUARANTINE,
                CompensationAction.DISCARD_CANDIDATE_GENERATION,
            ),
        ),
        (
            RebuildState.COMPLETED,
            (
                CompensationAction.CANCEL_ENQUEUED_SOURCES,
                CompensationAction.DEMOTE_CANDIDATE_GENERATION,
                CompensationAction.RESTORE_QUARANTINE,
                CompensationAction.DISCARD_CANDIDATE_GENERATION,
            ),
        ),
    ),
)
def test_f06_fail_existing_compensates_only_the_persisted_attempt(
    state: RebuildState,
    expected_actions: tuple[CompensationAction, ...],
) -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    command = _command(owner_token="writer-b")
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token="writer-a"),
        state=state,
        started_at=clock(),
        last_progress_at=clock(),
    )

    outcome = _processor(effects, clock).fail_existing(
        command,
        code=RebuildOutcomeCode.MANIFEST_DRIFT,
        detail="manifest unavailable during authorized resume",
    )

    assert outcome.code is RebuildOutcomeCode.MANIFEST_DRIFT
    assert outcome.state is RebuildState.FAILED
    assert outcome.detail == "manifest unavailable during authorized resume"
    assert effects.calls == ["compensate", "audit:manifest_drift"]
    assert effects.compensation_failed_state is state
    assert effects.compensation_actions == expected_actions
    assert not any(
        call in effects.calls
        for call in (
            "snapshot",
            "quarantine",
            "enqueue",
            "observe",
            "restore",
            "promote",
        )
    )


def test_f06_legacy_manually_restored_blocked_enqueue_cancels_queue_only() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )
    original_compensate = effects.compensate

    def compensate(command_arg, *, effect_key):  # noqa: ANN001, ANN201
        durable = effects.checkpoints[command.run_id]
        assert durable.state is RebuildState.COMPENSATING
        assert durable.receipts[intent.effect_key] == intent
        return original_compensate(command_arg, effect_key=effect_key)

    effects.compensate = compensate  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    )

    outcome = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert outcome.state is RebuildState.FAILED
    assert outcome.code is RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
    assert outcome.detail == "legacy_blocked_after_enqueue_predecessor_already_restored"
    validate_legacy_manual_restore_queue_only_outcome(
        outcome,
        command=command,
        intent_receipt=intent,
    )
    assert outcome.compensation_actions == (CompensationAction.CANCEL_ENQUEUED_SOURCES,)
    assert effects.compensation_failed_state is RebuildState.ENQUEUED
    assert effects.compensation_intent == intent
    assert CompensationAction.RESTORE_QUARANTINE not in effects.compensation_actions
    assert (
        CompensationAction.DISCARD_CANDIDATE_GENERATION
        not in effects.compensation_actions
    )


def test_f06_legacy_blocked_intent_requires_exact_prefix_and_live_authority() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    receipts = _legacy_blocked_receipts(command)
    receipts.pop(f"{command.run_id}:enqueue")
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=receipts,
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    )

    with pytest.raises(RuntimeError, match="legacy_blocked_receipt_prefix_invalid"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert "compensate" not in effects.calls

    effects.checkpoints[command.run_id] = replace(
        effects.checkpoints[command.run_id],
        receipts=_legacy_blocked_receipts(command),
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: False,
    )
    with pytest.raises(RuntimeError, match="legacy_blocked_intent_not_current"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert "compensate" not in effects.calls


def test_f06_generic_execute_never_resumes_legacy_queue_only_intent() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.COMPENSATING,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail="legacy blocked queue reconciliation",
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts={**_legacy_blocked_receipts(command), intent.effect_key: intent},
    )

    outcome = _processor(effects, clock).execute(command)

    assert outcome.state is RebuildState.BLOCKED
    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.detail == "legacy_blocked_recovery_authority_required"
    assert effects.calls == []


def test_f06_legacy_queue_only_retry_reuses_same_durable_intent() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.COMPENSATING,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail="legacy blocked queue reconciliation",
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts={**_legacy_blocked_receipts(command), intent.effect_key: intent},
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    )

    outcome = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert outcome.state is RebuildState.FAILED
    assert effects.calls == [
        "compensate",
        "audit:legacy_manual_restore_queue_only_reconciled",
    ]
    assert effects.compensation_intent == intent


def test_f06_legacy_queue_only_retry_crash_persists_resumable_intent() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    failed_compensation = RebuildEffectReceipt(
        effect_key=f"{command.run_id}:compensate",
        effect="compensate",
        ok=False,
        code="transient_queue_failure",
    )
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.COMPENSATION_FAILED,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail=(
            "legacy_blocked_after_enqueue_predecessor_already_restored"
        ),
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts={
            **_legacy_blocked_receipts(command),
            intent.effect_key: intent,
            failed_compensation.effect_key: failed_compensation,
        },
    )
    original_save = effects.save_checkpoint
    crashed = False

    def crash_after_retry_intent(checkpoint: RebuildCheckpoint) -> None:
        nonlocal crashed
        original_save(checkpoint)
        if checkpoint.state is RebuildState.COMPENSATING and not crashed:
            crashed = True
            raise SimulatedProcessCrash("crash after retry intent")

    effects.save_checkpoint = crash_after_retry_intent  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    )

    with pytest.raises(SimulatedProcessCrash, match="crash after retry intent"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )

    durable = effects.checkpoints[command.run_id]
    assert durable.state is RebuildState.COMPENSATING
    assert f"{command.run_id}:compensate" not in durable.receipts
    assert durable.receipts[intent.effect_key] == intent

    effects.save_checkpoint = original_save  # type: ignore[method-assign]
    resumed = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert resumed.state is RebuildState.FAILED
    assert resumed.code is RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
    validate_legacy_manual_restore_queue_only_outcome(
        resumed,
        command=command,
        intent_receipt=intent,
    )


def test_f06_legacy_queue_only_first_admission_rejects_compensation_metadata() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts=_legacy_blocked_receipts(command),
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    )

    with pytest.raises(RuntimeError, match="legacy_blocked_first_admission_invalid"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []


def test_f06_legacy_queue_only_terminal_replay_requires_exact_receipt() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    generic_compensation = RebuildEffectReceipt(
        f"{command.run_id}:compensate",
        "compensate",
        True,
    )
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.FAILED,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail=(
            "legacy_blocked_after_enqueue_predecessor_already_restored"
        ),
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts={
            **_legacy_blocked_receipts(command),
            intent.effect_key: intent,
            generic_compensation.effect_key: generic_compensation,
        },
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    )

    with pytest.raises(
        RuntimeError,
        match="legacy_blocked_compensation_receipt_invalid",
    ):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []

    exact = _legacy_queue_only_compensation(command, intent)
    effects.checkpoints[command.run_id] = replace(
        effects.checkpoints[command.run_id],
        receipts={
            **_legacy_blocked_receipts(command),
            intent.effect_key: intent,
            exact.effect_key: exact,
        },
    )
    outcome = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )
    assert outcome.code is RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
    assert outcome.detail == "legacy_blocked_after_enqueue_predecessor_already_restored"
    assert effects.calls == ["audit:legacy_manual_restore_queue_only_reconciled"]


def test_f06_legacy_queue_only_invalid_effect_receipt_cannot_emit_nominal_audit() -> (
    None
):
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )

    def _invalid_compensate(_command, *, effect_key):  # noqa: ANN001, ANN202
        effects.calls.append("compensate")
        return RebuildEffectReceipt(effect_key, "compensate", True)

    effects.compensate = _invalid_compensate  # type: ignore[method-assign]
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    )

    outcome = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert outcome.state is RebuildState.COMPENSATION_FAILED
    assert outcome.code is RebuildOutcomeCode.COMPENSATION_FAILED
    assert effects.calls == ["compensate", "audit:compensation_failed"]
    durable = effects.checkpoints[command.run_id]
    invalid = durable.receipts[f"{command.run_id}:compensate"]
    assert invalid.ok is False
    assert invalid.code == "legacy_blocked_compensation_receipt_invalid"


def test_f06_legacy_queue_only_rejects_blocked_intent_without_durable_context() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts={**_legacy_blocked_receipts(command), intent.effect_key: intent},
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    )

    with pytest.raises(
        RuntimeError,
        match="legacy_blocked_compensation_context_invalid",
    ):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []


def test_f06_legacy_queue_only_terminal_replay_reproves_physical_intent() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    compensation = _legacy_queue_only_compensation(command, intent)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.FAILED,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail=(
            "legacy_blocked_after_enqueue_predecessor_already_restored"
        ),
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts={
            **_legacy_blocked_receipts(command),
            intent.effect_key: intent,
            compensation.effect_key: compensation,
        },
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: False,
    )

    with pytest.raises(RuntimeError, match="legacy_blocked_intent_not_current"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []


def test_f06_legacy_queue_only_rejects_mutated_probe_payload_and_checkpoint() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    original = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )
    effects.checkpoints[command.run_id] = original

    def _mutate_probe(_command, receipt):  # noqa: ANN001, ANN202
        dict(receipt.details["queue"])["rows"].append({"id": "forged"})
        return True

    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=_mutate_probe,
    )
    with pytest.raises(RuntimeError, match="legacy_blocked_intent_not_current"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.checkpoints[command.run_id] == original
    assert effects.calls == []

    def _mutate_checkpoint(_command, _receipt):  # noqa: ANN001, ANN202
        effects.checkpoints[command.run_id] = replace(original, last_sequence=1)
        return True

    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=_mutate_checkpoint,
    )
    with pytest.raises(
        RuntimeError,
        match="legacy_blocked_checkpoint_changed_during_probe",
    ):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []


def test_f06_legacy_queue_only_post_effect_fence_loss_preserves_terminal_replay() -> (
    None
):
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )
    writer_renewals = iter((True, True, True, False))
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: next(writer_renewals),
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    )

    lost = processor.reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert lost.code is RebuildOutcomeCode.LEASE_LOST
    assert lost.state is RebuildState.FAILED
    durable = effects.checkpoints[command.run_id]
    assert durable.state is RebuildState.FAILED
    assert durable.receipts[f"{command.run_id}:compensate"].ok is True
    assert effects.calls == ["compensate"]

    resumed = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    ).reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )
    assert resumed.code is RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
    assert effects.calls == [
        "compensate",
        "audit:legacy_manual_restore_queue_only_reconciled",
    ]


def test_f06_legacy_queue_only_initial_fence_loss_writes_no_generic_audit() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    checkpoint = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )
    effects.checkpoints[command.run_id] = checkpoint
    outcome = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: False,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    ).reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )
    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is RebuildState.BLOCKED
    assert effects.checkpoints[command.run_id] == checkpoint
    assert effects.calls == []


@pytest.mark.parametrize(
    ("state", "receipt_mode"),
    [
        (RebuildState.FAILED, "absent"),
        (RebuildState.COMPENSATING, "success"),
        (RebuildState.COMPENSATION_FAILED, "absent"),
    ],
)
def test_f06_legacy_queue_only_state_and_terminal_receipt_are_bijective(
    state: RebuildState,
    receipt_mode: str,
) -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    receipts = {**_legacy_blocked_receipts(command), intent.effect_key: intent}
    if receipt_mode == "success":
        terminal = _legacy_queue_only_compensation(command, intent)
        receipts[terminal.effect_key] = terminal
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=state,
        started_at=clock(),
        last_progress_at=clock(),
        compensation_failed_state=RebuildState.ENQUEUED,
        compensation_failure_code=(
            RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        ),
        compensation_failure_detail=(
            "legacy_blocked_after_enqueue_predecessor_already_restored"
        ),
        compensation_actions=(CompensationAction.CANCEL_ENQUEUED_SOURCES,),
        receipts=receipts,
    )
    processor = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: True,
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, _receipt: True,
    )

    with pytest.raises(RuntimeError, match="legacy_blocked_state_receipt_mismatch"):
        processor.reconcile_manually_restored_blocked_after_enqueue(
            command,
            intent_receipt=intent,
            recovery_actor_id="board-owner",
            recovery_reason="adopt governed manual restore and fence legacy queue",
        )
    assert effects.calls == []


@pytest.mark.parametrize(
    ("receipt_ok", "expected_state"),
    [
        (None, RebuildState.COMPENSATING),
        (False, RebuildState.COMPENSATION_FAILED),
    ],
)
def test_f06_legacy_queue_only_fence_loss_preserves_resumable_phase(
    receipt_ok: bool | None,
    expected_state: RebuildState,
) -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    command = _command(owner_token="writer-b")
    intent = _legacy_blocked_intent(command)
    effects.checkpoints[command.run_id] = RebuildCheckpoint(
        command=replace(command, owner_token=None),
        state=RebuildState.BLOCKED,
        started_at=clock(),
        last_progress_at=clock(),
        receipts=_legacy_blocked_receipts(command),
    )
    if receipt_ok is False:

        def _failed_compensate(_command, *, effect_key):  # noqa: ANN001, ANN202
            effects.calls.append("compensate")
            return RebuildEffectReceipt(
                effect_key,
                "compensate",
                False,
                code="simulated_queue_failure",
            )

        effects.compensate = _failed_compensate  # type: ignore[method-assign]
        writer_renewals = iter((True, True, True, False))
    else:
        writer_renewals = iter((True, True, False))
    outcome = RebuildProcessor(
        effects,
        clock=clock,
        lease_renew=lambda: next(writer_renewals),
        orchestration_renew=lambda: True,
        legacy_blocked_intent_probe=lambda _command, receipt: receipt == intent,
    ).reconcile_manually_restored_blocked_after_enqueue(
        command,
        intent_receipt=intent,
        recovery_actor_id="board-owner",
        recovery_reason="adopt governed manual restore and fence legacy queue",
    )

    assert outcome.code is RebuildOutcomeCode.LEASE_LOST
    assert outcome.state is expected_state
    durable = effects.checkpoints[command.run_id]
    assert durable.state is expected_state
    assert durable.receipts[intent.effect_key] == intent
    assert all(not call.startswith("audit:") for call in effects.calls)


def test_f06_hard_timeout_is_monotonic_even_while_progressing() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, list(range(20, 0, -1)))
    outcome = _processor(effects, clock).execute(_command())
    assert outcome.code is RebuildOutcomeCode.HARD_TIMEOUT
    assert outcome.state is RebuildState.FAILED


def test_f06_quarantine_failure_does_not_enqueue_or_promote() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    effects.fail_effect = "quarantine"
    outcome = _processor(effects, clock).execute(_command())
    assert outcome.code is RebuildOutcomeCode.QUARANTINE_FAILED
    assert "enqueue" not in effects.calls
    assert "promote" not in effects.calls


def test_f06_restore_failure_compensates_and_never_promotes() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    effects.fail_effect = "restore"
    outcome = _processor(effects, clock).execute(_command())
    assert outcome.code is RebuildOutcomeCode.RESTORE_FAILED
    assert outcome.promotion_allowed is False
    assert "promote" not in effects.calls


@pytest.mark.parametrize(
    "crash_after",
    ["snapshot", "quarantine", "enqueue", "restore", "promote"],
)
def test_f06_retry_after_each_durable_effect_has_no_duplicate(
    crash_after: str,
) -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    effects.crash_after = crash_after

    with pytest.raises(SimulatedProcessCrash):
        _processor(effects, clock).execute(_command())

    second = _processor(effects, clock).execute(_command())

    assert second.code is RebuildOutcomeCode.COMPLETED
    assert effects.receipt_creations[crash_after] == 1
    assert (
        len([key for key in effects.receipts if key.endswith(f":{crash_after}")]) == 1
    )


def test_f06_retry_stops_before_destructive_effect_when_salvage_becomes_pending() -> (
    None
):
    clock = FakeClock()
    effects = FakeEffects(clock, [0])
    effects.crash_after = "snapshot"

    with pytest.raises(SimulatedProcessCrash):
        _processor(effects, clock).execute(_command())

    outcome = _processor(effects, clock).execute(
        replace(_command(), salvage_pending=True)
    )

    assert outcome.code is RebuildOutcomeCode.SALVAGE_PENDING
    assert "quarantine" not in effects.calls


def test_f06_salvage_pending_blocks_before_any_destructive_effect() -> None:
    clock = FakeClock()
    effects = FakeEffects(clock, [])
    outcome = _processor(effects, clock).execute(_command(salvage_pending=True))
    assert outcome.code is RebuildOutcomeCode.SALVAGE_PENDING
    assert outcome.state is RebuildState.BLOCKED
    assert not any(
        call in effects.calls for call in ("snapshot", "quarantine", "enqueue")
    )


def test_f06_processor_has_no_local_runtime_primitives() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "application"
        / "rebuild_processor.py"
    )
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules = {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    modules.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert "sqlite3" not in modules
    assert "time" not in modules
    assert "pathlib" not in modules
    assert not any("ladybug" in module or "kuzu" in module for module in modules)
    assert ".sleep(" not in source
