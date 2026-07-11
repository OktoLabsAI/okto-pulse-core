from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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
        self.crash_after: str | None = None
        self.crashed = False
        self.receipt_creations: dict[str, int] = {}

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
        self, command, *, after_sequence, max_wait_seconds  # noqa: ANN001
    ) -> QueueObservation:
        del command
        self.calls.append("observe")
        self.clock.advance(max_wait_seconds)
        depth = self.depths.pop(0) if self.depths else 0
        return QueueObservation(depth, self.clock(), after_sequence + 1)

    def compensate(self, command, *, effect_key):  # noqa: ANN001, ANN201
        self.calls.append("compensate")
        self.compensation_actions = command.actions
        self.compensation_failed_state = command.failed_state
        return RebuildEffectReceipt(effect_key, "compensate", True)

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
    assert len([key for key in effects.receipts if key.endswith(f":{crash_after}")]) == 1


def test_f06_retry_stops_before_destructive_effect_when_salvage_becomes_pending() -> None:
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
    assert not any(call in effects.calls for call in ("snapshot", "quarantine", "enqueue"))


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
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
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
