from dataclasses import replace

import pytest

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.services.main import SpecService
from delivery_evidence_testing import install_complete_delivery_port
from test_acceptance_coverage_gate import _seed_spec_with_indexed_criteria, USER_ID


@pytest.mark.asyncio
async def test_done_checks_readiness_then_rechecks_under_write_fence(
    db_factory, monkeypatch
):
    _, spec_id = await _seed_spec_with_indexed_criteria(db_factory)
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
    _, spec_id = await _seed_spec_with_indexed_criteria(db_factory)
    store = install_complete_delivery_port(monkeypatch)
    original = store.load_snapshot.side_effect
    calls = 0

    async def changed(scope):
        nonlocal calls
        calls += 1
        snapshot = await original(scope)
        return snapshot if calls == 1 else replace(snapshot, tests=())

    store.load_snapshot.side_effect = changed
    async with db_factory() as db:
        with pytest.raises(ValueError, match="delivery_test_result_missing"):
            await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DONE)
            )
        await db.rollback()
        assert (
            await SpecService(db).get_spec(spec_id)
        ).status == SpecStatus.IN_PROGRESS
    store.lock_scope.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", ["skip_test_coverage", "skip_code_evidence_coverage"])
async def test_existing_skip_flags_do_not_bypass_delivery(
    db_factory, monkeypatch, skip
):
    _, spec_id = await _seed_spec_with_indexed_criteria(db_factory)
    store = install_complete_delivery_port(monkeypatch)
    original = store.load_snapshot.side_effect

    async def missing(scope):
        return replace(await original(scope), implementations=(), tests=())

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
