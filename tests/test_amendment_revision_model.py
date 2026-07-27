"""SPEC 7ea1e4be / card f5dca74a — AmendmentHotfixRevision model, lifecycle,
store, eligibility policy and audit.

Pure eligibility matrix (codex card #1 refinement) + persistence/audit/AC1
immutability over the real store. Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_amendment_revision_model.py
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
    amendment_status_is_blocking,
    evaluate_amendment_eligibility,
)
from sqlalchemy_test_models import (
    ActivityLog,
    AmendmentHotfixRevision,
    Board,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.amendment_revision import (
    AmendmentRevisionError,
    AmendmentRevisionService,
)

USER = "amendment-coder"


# ---------------------------------------------------------------------------
# Pure eligibility matrix (FR4 / G3 / AC5) — exactly the matrix codex required.
# ---------------------------------------------------------------------------


def test_eligibility_matrix():
    S, L = AmendmentRevisionStatus, AmendmentLineageState

    # draft/review + complete -> block (G3: complete lineage alone is not enough)
    for status in (S.DRAFT, S.REVIEW):
        v = evaluate_amendment_eligibility(status, L.COMPLETE)
        assert v.blocked and not v.lineage_eligible
        assert v.reason_code == "amendment_status_blocking"

    # approved + incomplete -> block
    v = evaluate_amendment_eligibility(S.APPROVED, L.INCOMPLETE)
    assert v.blocked and v.reason_code == "amendment_lineage_incomplete"

    # approved + complete -> lineage_eligible, NOT canonicalization_candidate
    v = evaluate_amendment_eligibility(S.APPROVED, L.COMPLETE)
    assert v.lineage_eligible and not v.canonicalization_candidate
    assert not v.blocked and v.reason_code == "ok"

    # done + complete -> lineage_eligible AND canonicalization_candidate
    v = evaluate_amendment_eligibility(S.DONE, L.COMPLETE)
    assert v.lineage_eligible and v.canonicalization_candidate and not v.blocked

    # done + incomplete -> block
    assert evaluate_amendment_eligibility(S.DONE, L.INCOMPLETE).blocked

    # cancelled / superseded -> block with stable, distinct reason codes
    assert evaluate_amendment_eligibility(S.CANCELLED, L.COMPLETE).reason_code == "amendment_cancelled"
    assert evaluate_amendment_eligibility(S.SUPERSEDED, L.COMPLETE).reason_code == "amendment_superseded"

    # unknown status / unknown lineage -> fail closed
    assert evaluate_amendment_eligibility("bogus", L.COMPLETE).reason_code == "amendment_status_unknown"
    unknown_lineage = evaluate_amendment_eligibility(S.APPROVED, "bogus")
    assert unknown_lineage.blocked and unknown_lineage.reason_code == "amendment_lineage_incomplete"


def test_amendment_status_is_blocking():
    S = AmendmentRevisionStatus
    assert amendment_status_is_blocking(S.DRAFT)
    assert amendment_status_is_blocking(S.REVIEW)
    assert amendment_status_is_blocking(S.CANCELLED)
    assert amendment_status_is_blocking(S.SUPERSEDED)
    assert amendment_status_is_blocking("bogus")  # unknown -> blocking
    assert not amendment_status_is_blocking(S.APPROVED)
    assert not amendment_status_is_blocking(S.DONE)


# ---------------------------------------------------------------------------
# Store + audit (FR1 / TR5) over the real DB (table auto-created by create_all)
# ---------------------------------------------------------------------------


async def _seed_board_spec(db_factory, *, spec_status=SpecStatus.DONE):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="AMD", owner_id=USER))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Original",
                status=spec_status,
                created_by=USER,
                version=3,
                test_scenarios=[{"id": "ts_x", "title": "X", "status": "passed"}],
            )
        )
        await db.commit()
    return board_id, spec_id


async def test_create_round_trips_all_fr1_fields(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory)
    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        amendment = await svc.create(
            board_id=board_id,
            original_spec_id=spec_id,
            origin_bug_id="bug-1",
            author=USER,
            origin_task_ids=["task-a"],
            affected_task_ids=["task-b", "task-c"],
            revision_spec_id="rev-spec-1",
            regression_scenario_ids=["ts_reg"],
            regression_test_task_ids=["testcard-1"],
            automated_regression_refs=["tests/test_x.py::test_y"],
            validation_metadata={"val_id": "v1"},
        )
        amendment_id = amendment.id
        await db.commit()

    async with db_factory() as db:
        amendment = await db.get(AmendmentHotfixRevision, amendment_id)
        assert amendment is not None
        assert amendment.board_id == board_id
        assert amendment.original_spec_id == spec_id
        assert amendment.origin_bug_id == "bug-1"
        assert amendment.origin_task_ids == ["task-a"]
        assert amendment.affected_task_ids == ["task-b", "task-c"]
        assert amendment.revision_spec_id == "rev-spec-1"
        assert amendment.regression_scenario_ids == ["ts_reg"]
        assert amendment.regression_test_task_ids == ["testcard-1"]
        # first-class automated regression ref (covers tooling/test-infra evidence)
        assert amendment.automated_regression_refs == ["tests/test_x.py::test_y"]
        assert amendment.validation_metadata == {"val_id": "v1"}
        # defaults: fresh amendment is working-only + incomplete lineage
        assert amendment.status is AmendmentRevisionStatus.DRAFT
        assert amendment.lineage_state is AmendmentLineageState.INCOMPLETE


async def test_create_does_not_mutate_original_spec(db_factory):
    # AC1: creating an amendment for a done spec leaves the original spec's
    # version, status and test scenario set untouched.
    board_id, spec_id = await _seed_board_spec(db_factory, spec_status=SpecStatus.DONE)
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
        before_version, before_status = before.version, before.status
        before_scenarios = list(before.test_scenarios or [])

    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        await svc.create(
            board_id=board_id, original_spec_id=spec_id, origin_bug_id="bug-1", author=USER
        )
        await db.commit()

    async with db_factory() as db:
        after = await db.get(Spec, spec_id)
        assert after.version == before_version
        assert after.status == before_status == SpecStatus.DONE
        assert list(after.test_scenarios or []) == before_scenarios


async def test_status_and_lineage_transitions_emit_audit(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory)
    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        amendment = await svc.create(
            board_id=board_id, original_spec_id=spec_id, origin_bug_id="bug-1", author=USER
        )
        amendment_id = amendment.id
        await svc.set_lineage_state(amendment_id, AmendmentLineageState.COMPLETE, USER)
        await svc.set_status(amendment_id, AmendmentRevisionStatus.APPROVED, USER)
        await db.commit()

    async with db_factory() as db:
        amendment = await db.get(AmendmentHotfixRevision, amendment_id)
        assert amendment.status is AmendmentRevisionStatus.APPROVED
        assert amendment.lineage_state is AmendmentLineageState.COMPLETE
        # approved + complete -> lineage_eligible but not canonicalization_candidate
        verdict = AmendmentRevisionService(db).eligibility(amendment)
        assert verdict.lineage_eligible and not verdict.canonicalization_candidate

        rows = (
            await db.execute(
                select(ActivityLog).where(ActivityLog.board_id == board_id)
            )
        ).scalars().all()
        actions = {r.action for r in rows}
        assert "amendment_revision_created" in actions
        assert "amendment_revision_lineage_changed" in actions
        assert "amendment_revision_status_changed" in actions


async def test_done_complete_is_canonicalization_candidate(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory)
    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        amendment = await svc.create(
            board_id=board_id, original_spec_id=spec_id, origin_bug_id="bug-1", author=USER
        )
        amendment = await svc.set_lineage_state(
            amendment.id,
            AmendmentLineageState.COMPLETE,
            USER,
        )
        amendment = await svc.set_status(
            amendment.id,
            AmendmentRevisionStatus.DONE,
            USER,
        )
        verdict = svc.eligibility(amendment)
        assert verdict.lineage_eligible and verdict.canonicalization_candidate


async def test_unknown_status_fails_closed(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory)
    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        amendment = await svc.create(
            board_id=board_id, original_spec_id=spec_id, origin_bug_id="bug-1", author=USER
        )
        with pytest.raises(AmendmentRevisionError, match="unknown_amendment_status"):
            await svc.set_status(amendment.id, "totally_made_up", USER)


@pytest.mark.parametrize(
    "terminal_status",
    [
        AmendmentRevisionStatus.CANCELLED,
        AmendmentRevisionStatus.SUPERSEDED,
    ],
)
async def test_terminal_revision_rejects_every_subsequent_mutation(
    db_factory,
    terminal_status,
):
    """Cancelled/superseded revisions are immutable at the lowest write layer."""
    board_id, spec_id = await _seed_board_spec(db_factory)
    async with db_factory() as db:
        svc = AmendmentRevisionService(db)
        amendment = await svc.create(
            board_id=board_id,
            original_spec_id=spec_id,
            origin_bug_id="bug-1",
            author=USER,
        )
        amendment = await svc.set_status(amendment.id, terminal_status, USER)
        before_updated_at = amendment.updated_at
        before_state = {
            "status": amendment.status,
            "lineage_state": amendment.lineage_state,
            "validation_metadata": dict(amendment.validation_metadata or {}),
            "regression_test_task_ids": list(
                amendment.regression_test_task_ids or []
            ),
            "regression_scenario_ids": list(
                amendment.regression_scenario_ids or []
            ),
            "automated_regression_refs": list(
                amendment.automated_regression_refs or []
            ),
        }
        audit_count = len(
            (
                await db.execute(
                    select(ActivityLog).where(ActivityLog.board_id == board_id)
                )
            )
            .scalars()
            .all()
        )

        other_terminal = (
            AmendmentRevisionStatus.SUPERSEDED
            if terminal_status is AmendmentRevisionStatus.CANCELLED
            else AmendmentRevisionStatus.CANCELLED
        )
        mutations = [
            (
                "status resurrection",
                lambda: svc.set_status(
                    amendment.id, AmendmentRevisionStatus.DRAFT, USER
                ),
            ),
            (
                "terminal status replacement",
                lambda: svc.set_status(amendment.id, other_terminal, USER),
            ),
            (
                "lineage",
                lambda: svc.set_lineage_state(
                    amendment.id, AmendmentLineageState.COMPLETE, USER
                ),
            ),
            (
                "coverage",
                lambda: svc.set_coverage_confirmation(
                    amendment.id,
                    confirmation={
                        "regression_test_task_id": "test-1",
                        "regression_scenario_id": "ts-1",
                    },
                    actor=USER,
                ),
            ),
            (
                "artifact association",
                lambda: svc.associate_artifacts(
                    amendment.id,
                    regression_test_task_ids=["test-1"],
                    regression_scenario_ids=["ts-1"],
                    automated_regression_refs=["tests/test_reg.py::test_fix"],
                    actor=USER,
                ),
            ),
        ]
        for label, mutate in mutations:
            with pytest.raises(
                AmendmentRevisionError,
                match="terminal_amendment_revision",
            ) as exc:
                await mutate()
            assert exc.value.code == "terminal_amendment_revision", label
            if label == "status resurrection":
                assert "target_status='draft'" in str(exc.value)
            elif label == "terminal status replacement":
                assert f"target_status='{other_terminal.value}'" in str(exc.value)

        # Retrying the exact terminal status is allowed, but must have no effect.
        retried = await svc.set_status(amendment.id, terminal_status, USER)
        assert retried.id == amendment.id
        # SQLite round-trips UTC timestamps without tzinfo; normalize only that
        # storage representation difference when proving no timestamp bump.
        assert retried.updated_at.replace(tzinfo=None) == before_updated_at.replace(
            tzinfo=None
        )
        assert {
            "status": retried.status,
            "lineage_state": retried.lineage_state,
            "validation_metadata": dict(retried.validation_metadata or {}),
            "regression_test_task_ids": list(
                retried.regression_test_task_ids or []
            ),
            "regression_scenario_ids": list(
                retried.regression_scenario_ids or []
            ),
            "automated_regression_refs": list(
                retried.automated_regression_refs or []
            ),
        } == before_state
        assert (
            len(
                (
                    await db.execute(
                        select(ActivityLog).where(
                            ActivityLog.board_id == board_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            == audit_count
        )
