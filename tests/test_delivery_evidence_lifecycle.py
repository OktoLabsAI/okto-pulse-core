from dataclasses import replace

import pytest

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.application.delivery_errors import DeliveryGateBlocked
from okto_pulse.core.domain.delivery_assessment import (
    ReadinessReason, aggregate_delivery_metrics, assess_delivery_gate,
    assess_obligation_delivery,
)
from okto_pulse.core.domain.delivery_verification import (
    NativeVerificationPopulation, NativeVerificationRun,
)
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.services.main import SpecService
from delivery_evidence_testing import install_complete_delivery_port
from test_acceptance_coverage_gate import _seed_spec_with_identified_criteria, USER_ID


@pytest.mark.asyncio
async def test_done_checks_readiness_then_rechecks_under_write_fence(
    db_factory, monkeypatch
):
    _, spec_id = await _seed_spec_with_identified_criteria(db_factory)
    store = install_complete_delivery_port(monkeypatch)
    async with db_factory() as db:
        moved = await SpecService(db).move_spec(
            spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
        )
        await db.commit()
        assert moved.status == SpecStatus.DONE
    assert store.load_snapshot.await_count == 2
    store.lock_scope.assert_awaited_once()


@pytest.mark.asyncio
async def test_receipt_change_between_preview_and_fence_blocks_done(
    db_factory, monkeypatch
):
    _, spec_id = await _seed_spec_with_identified_criteria(db_factory)
    store = install_complete_delivery_port(monkeypatch)
    original = store.load_snapshot.side_effect
    calls = 0

    async def changed(scope):
        nonlocal calls
        calls += 1
        snapshot = await original(scope)
        if calls == 1:
            assert assess_delivery_gate(assess_obligation_delivery(snapshot)).allowed
            return snapshot
        # The adapter no longer admits the revoked receipts as positive joins,
        # but retains their authenticated rejection in the native population.
        # Removing all population data would test an unavailable verifier, not
        # a receipt that changed between the preview and the write fence.
        revoked = NativeVerificationPopulation(tuple(
            NativeVerificationRun(
                scope=scope, scenario_id=test.fact.scenario_id,
                scenario_sha256="a" * 64, method=test.verification_method,
                receipt_id=test.fact.receipt_id, evidence_ref=test.fact.id,
                outcome=test.fact.result, authenticated=False,
                rejection_reason=ReadinessReason.VERIFICATION_RECEIPT_INVALID,
                native_reason_codes=("test_execution_receipt_revoked",),
            ) for test in snapshot.effective_context.tests
        ), complete=True)
        return replace(
            snapshot, tests=(),
            effective_context=replace(
                snapshot.effective_context, tests=(), native_verification=revoked,
            ),
        )

    store.load_snapshot.side_effect = changed
    async with db_factory() as db:
        with pytest.raises(DeliveryGateBlocked) as error:
            await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
            )
        assert error.value.gate.status == "blocked"
        reasons = [reason for blocker in error.value.gate.blockers
                   for reason in blocker.reasons]
        assert reasons and all(
            reason.code == ReadinessReason.VERIFICATION_RECEIPT_INVALID
            and reason.native_reason_codes == ("test_execution_receipt_revoked",)
            for reason in reasons
        )
        metrics = aggregate_delivery_metrics(error.value.assessment)
        assert metrics.implementation.percent == 100
        assert metrics.verification.percent == 0
        await db.rollback()
        assert (
            await SpecService(db).get_spec(spec_id)
        ).status == SpecStatus.IN_PROGRESS
    store.lock_scope.assert_awaited_once()
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", ["skip_test_coverage", "skip_code_evidence_coverage"])
async def test_existing_skip_flags_do_not_bypass_delivery(
    db_factory, monkeypatch, skip
):
    _, spec_id = await _seed_spec_with_identified_criteria(db_factory)
    store = install_complete_delivery_port(monkeypatch)
    original = store.load_snapshot.side_effect

    async def missing(scope):
        snapshot = await original(scope)
        return replace(
            snapshot, implementations=(), tests=(),
            effective_context=replace(
                snapshot.effective_context, implementations=(), tests=()
            ),
        )

    store.load_snapshot.side_effect = missing
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        setattr(spec, skip, True)
        await db.commit()
        with pytest.raises(ValueError, match="delivery_evidence_incomplete"):
            await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
            )
        await db.rollback()
