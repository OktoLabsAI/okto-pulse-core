"""Integration tests — RefinementCognitiveCompletionGuard wired into the
real ``RefinementService.move_refinement`` path (Codex audit val_d2ff8599).

These tests cover the REAL service execution, not direct guard calls:

    pending blocks approved -> done
    in_progress blocks
    failed blocks
    consolidated allows
    skipped allows
    no cognitive item allows
    store unavailable fails closed (ValueError)
    blocked move leaves status / snapshot / version unchanged
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingMarker,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.models.db import (
    Board,
    DomainEventRow,
    Ideation,
    IdeationComplexity,
    IdeationStatus,
    Refinement,
    RefinementSnapshot,
    RefinementStatus,
)
from okto_pulse.core.models.schemas import RefinementMove
from okto_pulse.core.services.main import RefinementService


USER_ID = "refinement-cog-guard-agent"


def _id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def isolated_kg_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03-7-int"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture
async def seeded_refinement() -> tuple[str, str, str]:
    """Provision board + ideation + refinement in APPROVED status so it
    is ready to transition to DONE."""

    board_id = _id()
    ideation_id = _id()
    refinement_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="cog-guard board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="ideation",
                description="d",
                problem_statement="p",
                proposed_approach="a",
                scope_assessment={"domains": 1, "ambiguity": 1, "dependencies": 1},
                complexity=IdeationComplexity.SMALL,
                status=IdeationStatus.DONE,
                version=1,
                created_by=USER_ID,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="refinement under test",
                description="desc",
                analysis="analysis summary",
                in_scope=["scope-1"],
                status=RefinementStatus.APPROVED,
                version=1,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, ideation_id, refinement_id


async def _move_to_done(refinement_id: str) -> tuple[Refinement | None, Exception | None]:
    db_factory = get_session_factory()
    async with db_factory() as db:
        svc = RefinementService(db)
        try:
            result = await svc.move_refinement(
                refinement_id=refinement_id,
                user_id=USER_ID,
                data=RefinementMove(status=RefinementStatus.DONE),
                actor_name=USER_ID,
            )
            await db.commit()
            return result, None
        except Exception as exc:  # noqa: BLE001 — capture for assertion.
            await db.rollback()
            return None, exc


async def _refinement_snapshot_count(refinement_id: str) -> int:
    from sqlalchemy import select, func
    db_factory = get_session_factory()
    async with db_factory() as db:
        stmt = select(func.count()).select_from(RefinementSnapshot).where(
            RefinementSnapshot.refinement_id == refinement_id
        )
        return int((await db.execute(stmt)).scalar() or 0)


async def _refinement_status(refinement_id: str) -> str:
    db_factory = get_session_factory()
    async with db_factory() as db:
        from sqlalchemy import select
        stmt = select(Refinement).where(Refinement.id == refinement_id)
        ref = (await db.execute(stmt)).scalar_one()
        return ref.status.value


def _seed_cognitive_item(
    base_dir: Path, board_id: str, refinement_id: str, status: str
) -> str:
    """Seed one cognitive item for the refinement source_ref. If status
    is non-pending, drive it through the store update path so the
    deterministic guard does not trip."""

    gen = generate_kg_generation_id()
    del base_dir
    artifact_store = require_rebuild_audit_artifact_store()
    marker = CognitivePendingMarker(artifact_store=artifact_store)
    marker.mark_for_generation(
        board_id=board_id,
        kg_generation_id=gen,
        source_set=[
            {
                "artifact_type": "refinement",
                "id": refinement_id,
                "source_ref": f"refinement:{refinement_id}",
            }
        ],
        event_ref="evt_cog_int",
    )
    if status != CognitiveItemStatus.PENDING.value:
        store = CognitiveConsolidationItemStore(artifact_store=artifact_store)
        items = store.list_items(board_id, gen)
        store.update_item(
            board_id=board_id,
            kg_generation_id=gen,
            item_id=items[0].item_id,
            new_status=status,
            updated_by_agent_id="agent-1",
            consolidation_session_id="sess-int"
            if status == CognitiveItemStatus.CONSOLIDATED.value
            else None,
            reason="manual"
            if status in (CognitiveItemStatus.SKIPPED.value, CognitiveItemStatus.FAILED.value)
            else None,
        )
    return gen


# -------- pending blocks approved → done --------------------------------


@pytest.mark.asyncio
async def test_pending_cognitive_item_blocks_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    _seed_cognitive_item(
        isolated_kg_dir,
        board_id,
        refinement_id,
        CognitiveItemStatus.PENDING.value,
    )
    result, exc = await _move_to_done(refinement_id)
    assert result is None
    assert isinstance(exc, ValueError)
    assert "cognitive_consolidation_pending" in str(exc).lower()
    # Status preserved.
    assert await _refinement_status(refinement_id) == "approved"
    # No snapshot side-effects.
    assert await _refinement_snapshot_count(refinement_id) == 0


@pytest.mark.asyncio
async def test_in_progress_cognitive_item_blocks_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    _seed_cognitive_item(
        isolated_kg_dir,
        board_id,
        refinement_id,
        CognitiveItemStatus.IN_PROGRESS.value,
    )
    result, exc = await _move_to_done(refinement_id)
    assert result is None
    assert isinstance(exc, ValueError)
    assert "cognitive_consolidation_pending" in str(exc).lower()
    assert await _refinement_status(refinement_id) == "approved"
    assert await _refinement_snapshot_count(refinement_id) == 0


@pytest.mark.asyncio
async def test_failed_cognitive_item_blocks_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    _seed_cognitive_item(
        isolated_kg_dir,
        board_id,
        refinement_id,
        CognitiveItemStatus.FAILED.value,
    )
    result, exc = await _move_to_done(refinement_id)
    assert result is None
    assert isinstance(exc, ValueError)
    assert "cognitive_consolidation_pending" in str(exc).lower()
    assert await _refinement_status(refinement_id) == "approved"
    assert await _refinement_snapshot_count(refinement_id) == 0


# -------- terminal statuses allow done -----------------------------------


@pytest.mark.asyncio
async def test_consolidated_cognitive_item_allows_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    _seed_cognitive_item(
        isolated_kg_dir,
        board_id,
        refinement_id,
        CognitiveItemStatus.CONSOLIDATED.value,
    )
    result, exc = await _move_to_done(refinement_id)
    # The move may still fail downstream (e.g. ResourceGate), but the
    # cognitive guard MUST NOT be the blocker.
    if exc is not None:
        assert "cognitive_consolidation_pending" not in str(exc).lower()


@pytest.mark.asyncio
async def test_skipped_cognitive_item_allows_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    _seed_cognitive_item(
        isolated_kg_dir,
        board_id,
        refinement_id,
        CognitiveItemStatus.SKIPPED.value,
    )
    result, exc = await _move_to_done(refinement_id)
    if exc is not None:
        assert "cognitive_consolidation_pending" not in str(exc).lower()


@pytest.mark.asyncio
async def test_no_cognitive_item_allows_refinement_done(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    _, _, refinement_id = seeded_refinement
    result, exc = await _move_to_done(refinement_id)
    if exc is not None:
        assert "cognitive_consolidation_pending" not in str(exc).lower()


@pytest.mark.asyncio
async def test_move_refinement_emits_semantic_changed_for_status(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    board_id, _, refinement_id = seeded_refinement
    db_factory = get_session_factory()
    async with db_factory() as db:
        svc = RefinementService(db)
        result = await svc.move_refinement(
            refinement_id=refinement_id,
            user_id=USER_ID,
            data=RefinementMove(status=RefinementStatus.REVIEW),
            actor_name=USER_ID,
        )
        await db.commit()

    assert result is not None
    async with db_factory() as db:
        from sqlalchemy import select

        event = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "refinement.semantic_changed",
                )
            )
        ).scalar_one()
        assert event.payload_json["refinement_id"] == refinement_id
        assert event.payload_json["changed_fields"] == ["status"]


# -------- store unavailable → fail-closed ValueError --------------------


@pytest.mark.asyncio
async def test_store_unavailable_fails_closed_with_value_error(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    """Codex required path: if the cognitive store is unavailable, the
    move must be rejected — no snapshot, no history claim of done."""

    board_id, _, refinement_id = seeded_refinement

    class _RaisingGuard:
        def evaluate(self, **_kwargs):
            raise OSError("store gone")

    db_factory = get_session_factory()
    async with db_factory() as db:
        svc = RefinementService(db)
        svc._cognitive_done_guard_factory = lambda: _RaisingGuard()
        try:
            await svc.move_refinement(
                refinement_id=refinement_id,
                user_id=USER_ID,
                data=RefinementMove(status=RefinementStatus.DONE),
                actor_name=USER_ID,
            )
            await db.commit()
            raise AssertionError("expected ValueError on store unavailable")
        except ValueError as exc:
            await db.rollback()
            assert "cognitive_status_unavailable" in str(exc).lower()

    # Status preserved.
    assert await _refinement_status(refinement_id) == "approved"
    assert await _refinement_snapshot_count(refinement_id) == 0


@pytest.mark.asyncio
async def test_blocked_done_via_unavailable_response_preserves_state(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    """When the guard returns ``cognitive_status_unavailable``
    (allowed=False), move rejects + state preserved."""

    _, _, refinement_id = seeded_refinement

    from okto_pulse.core.kg.refinement_cognitive_guard import (
        RefinementGuardReason,
        RefinementGuardResult,
    )

    class _UnavailableGuard:
        def evaluate(self, **_kwargs):
            return RefinementGuardResult(
                allowed=False,
                reason=RefinementGuardReason.COGNITIVE_STATUS_UNAVAILABLE.value,
            )

    db_factory = get_session_factory()
    async with db_factory() as db:
        svc = RefinementService(db)
        svc._cognitive_done_guard_factory = lambda: _UnavailableGuard()
        try:
            await svc.move_refinement(
                refinement_id=refinement_id,
                user_id=USER_ID,
                data=RefinementMove(status=RefinementStatus.DONE),
                actor_name=USER_ID,
            )
            await db.commit()
            raise AssertionError("expected blocked move")
        except ValueError as exc:
            await db.rollback()
            assert "cognitive_status_unavailable" in str(exc).lower()

    assert await _refinement_status(refinement_id) == "approved"
    assert await _refinement_snapshot_count(refinement_id) == 0


# -------- non-done transitions don't invoke the guard ------------------


@pytest.mark.asyncio
async def test_non_done_transition_does_not_invoke_guard(
    isolated_kg_dir: Path,
    seeded_refinement,
) -> None:
    """The guard only applies to target_status=done. Approved → Review
    (or Approved → Cancelled) MUST not invoke the guard at all."""

    _, _, refinement_id = seeded_refinement

    invoked = {"count": 0}

    class _CountingGuard:
        def evaluate(self, **_kwargs):
            invoked["count"] += 1
            return None  # unused on non-done path

    db_factory = get_session_factory()
    async with db_factory() as db:
        svc = RefinementService(db)
        svc._cognitive_done_guard_factory = lambda: _CountingGuard()
        await svc.move_refinement(
            refinement_id=refinement_id,
            user_id=USER_ID,
            data=RefinementMove(status=RefinementStatus.REVIEW),
            actor_name=USER_ID,
        )
        await db.commit()

    assert invoked["count"] == 0
    assert await _refinement_status(refinement_id) == "review"
