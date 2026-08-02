"""SK-B2-S1 I2 — impact_evidence enforcement in the report_target choke
point (FR-5/FR-6, TR-3/TR-4/TR-8; AC-1/6/8/9/17)."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from sqlalchemy_test_models import ActivityLog, Board, Spec
from test_card_lifecycle import BOARD_ID, USER_ID, _seed_board

from okto_pulse.core.domain.enums import CardStatus, SpecStatus
from okto_pulse.core.models.schemas import BoardSettings, CardCreate, CardMove
from okto_pulse.core.services.impact_evidence import (
    IMPACT_EVIDENCE_MODES,
    resolve_impact_evidence_mode,
)
from okto_pulse.core.services.main import CardOperationError, CardService

pytestmark = pytest.mark.asyncio

_VALID_BLOCK = {
    "schema_version": 1,
    "files": [
        {"repo": "core", "path": "src/x.py", "change_kind": "modified"}
    ],
    "evidence_refs": ["tests/test_x.py::test_a"],
}

_REPORT_KWARGS = dict(
    conclusion="Executed the planned change end to end",
    completeness=100,
    completeness_justification="All planned behavior implemented",
    drift=0,
    drift_justification="No deviation from plan",
)


async def _card_in_progress(db_factory, *, mode: object = None):
    await _seed_board(db_factory)
    async with db_factory() as db:
        board = await db.get(Board, BOARD_ID)
        settings = dict(board.settings or {})
        if mode is None:
            settings.pop("impact_evidence_mode", None)
        else:
            # Direct DB write deliberately bypasses pydantic so tampered
            # values can be seeded (AC-9 read side).
            settings["impact_evidence_mode"] = mode
        board.settings = settings
        svc = CardService(db)
        specs = (
            (await db.execute(select(Spec).where(Spec.board_id == BOARD_ID)))
            .scalars()
            .all()
        )
        specs[0].status = SpecStatus.IN_PROGRESS
        specs[0].require_task_validation = False
        card = await svc.create_card(
            BOARD_ID,
            USER_ID,
            CardCreate(
                title=f"IE card {uuid.uuid4().hex[:8]}",
                status=CardStatus.NOT_STARTED,
                spec_id=specs[0].id,
            ),
        )
        card.status = CardStatus.IN_PROGRESS
        await db.commit()
        return card.id


async def _advisory_entries(db, card_id):
    rows = (
        (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.card_id == card_id,
                    ActivityLog.action == "impact_evidence_missing",
                )
            )
        )
        .scalars()
        .all()
    )
    return rows


async def test_require_rejects_missing_block(db_factory):
    """AC-6: require + no block -> CardOperationError with remediation."""

    card_id = await _card_in_progress(db_factory, mode="require")
    async with db_factory() as db:
        svc = CardService(db)
        with pytest.raises(CardOperationError) as excinfo:
            await svc.move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.VALIDATION, **_REPORT_KWARGS),
            )
        assert excinfo.value.code == "impact_evidence_required"
        assert excinfo.value.remediation
        assert excinfo.value.facts["target_status"] == "validation"


async def test_require_rejects_empty_sections_block(db_factory):
    """AC-6: a block whose four sections are empty does not satisfy require."""

    card_id = await _card_in_progress(db_factory, mode="require")
    async with db_factory() as db:
        svc = CardService(db)
        with pytest.raises(CardOperationError):
            await svc.move_card(
                card_id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    impact_evidence={
                        "schema_version": 1,
                        "evidence_refs": ["ts_1"],
                    },
                    **_REPORT_KWARGS,
                ),
            )


async def test_require_accepts_populated_block_and_persists_it(db_factory):
    """AC-2/AC-6: >=1 populated section succeeds and the block persists."""

    card_id = await _card_in_progress(db_factory, mode="require")
    async with db_factory() as db:
        svc = CardService(db)
        moved = await svc.move_card(
            card_id,
            USER_ID,
            CardMove(
                status=CardStatus.VALIDATION,
                impact_evidence=_VALID_BLOCK,
                **_REPORT_KWARGS,
            ),
        )
        assert moved.status == CardStatus.VALIDATION
        stored = moved.conclusions[-1]["impact_evidence"]
        assert stored["schema_version"] == 1
        assert stored["files"][0]["path"] == "src/x.py"
        # Omitted optional fields stay omitted (round-trip of the submitted
        # shape).
        assert "previous_path" not in stored["files"][0]


async def test_advisory_missing_block_logs_exact_activity_entry(db_factory):
    """AC-8/AC-17: advisory move succeeds AND logs the exact entry."""

    card_id = await _card_in_progress(db_factory, mode="advisory")
    async with db_factory() as db:
        svc = CardService(db)
        moved = await svc.move_card(
            card_id,
            USER_ID,
            CardMove(status=CardStatus.VALIDATION, **_REPORT_KWARGS),
        )
        assert moved.status == CardStatus.VALIDATION
        await db.commit()
    async with db_factory() as db:
        rows = await _advisory_entries(db, card_id)
        assert len(rows) == 1
        assert rows[0].details == {
            "mode": "advisory",
            "target_status": "validation",
            "author_id": USER_ID,
        }


async def test_advisory_with_block_logs_nothing(db_factory):
    card_id = await _card_in_progress(db_factory, mode="advisory")
    async with db_factory() as db:
        svc = CardService(db)
        await svc.move_card(
            card_id,
            USER_ID,
            CardMove(
                status=CardStatus.VALIDATION,
                impact_evidence=_VALID_BLOCK,
                **_REPORT_KWARGS,
            ),
        )
        await db.commit()
    async with db_factory() as db:
        assert await _advisory_entries(db, card_id) == []


async def test_off_and_absent_preserve_legacy_behavior(db_factory):
    """AC-1: absent (legacy) setting — move succeeds, no entry, no block."""

    for mode in (None, "off"):
        card_id = await _card_in_progress(db_factory, mode=mode)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.VALIDATION, **_REPORT_KWARGS),
            )
            assert moved.status == CardStatus.VALIDATION
            assert "impact_evidence" not in moved.conclusions[-1]
            await db.commit()
        async with db_factory() as db:
            assert await _advisory_entries(db, card_id) == []


async def test_tampered_mode_resolves_off_and_never_raises(db_factory):
    """AC-9 read side: persisted 'banana' resolves to off via the resolver."""

    card_id = await _card_in_progress(db_factory, mode="banana")
    async with db_factory() as db:
        svc = CardService(db)
        moved = await svc.move_card(
            card_id,
            USER_ID,
            CardMove(status=CardStatus.VALIDATION, **_REPORT_KWARGS),
        )
        assert moved.status == CardStatus.VALIDATION


def test_write_side_rejects_out_of_enum_mode():
    """AC-9 write side: BoardSettings refuses 'banana'."""

    with pytest.raises(ValidationError):
        BoardSettings(impact_evidence_mode="banana")
    assert BoardSettings().impact_evidence_mode == "off"
    assert BoardSettings(impact_evidence_mode="require").impact_evidence_mode == (
        "require"
    )


def test_resolver_contract():
    """TR-4: invalid_value_fail_compat pattern, source is auditable."""

    class _B:
        def __init__(self, settings):
            self.settings = settings

    assert resolve_impact_evidence_mode(None) == ("off", "legacy_absent_compat")
    assert resolve_impact_evidence_mode(_B({})) == (
        "off",
        "legacy_absent_compat",
    )
    assert resolve_impact_evidence_mode(_B({"impact_evidence_mode": "banana"})) == (
        "off",
        "invalid_value_fail_compat",
    )
    assert resolve_impact_evidence_mode(
        _B({"impact_evidence_mode": " REQUIRE "})
    ) == ("require", "board_settings")
    assert IMPACT_EVIDENCE_MODES == {"off", "advisory", "require"}


async def test_require_exemptions_inherited_from_report_target(db_factory):
    """TS-7/AC-7: under 'require', the two exempt paths never demand the
    block — a TEST card moving to validation (no report gate at all) and
    submit_task_validation approving a card straight to DONE."""

    from test_card_lifecycle import _mark_all_resources_na

    from okto_pulse.core.domain.enums import CardType

    await _seed_board(db_factory)
    async with db_factory() as db:
        board = await db.get(Board, BOARD_ID)
        board.settings = {
            **dict(board.settings or {}),
            "impact_evidence_mode": "require",
        }
        svc = CardService(db)
        specs = (
            (await db.execute(select(Spec).where(Spec.board_id == BOARD_ID)))
            .scalars()
            .all()
        )
        specs[0].status = SpecStatus.IN_PROGRESS
        specs[0].require_task_validation = False

        # Path 1: TEST card -> validation, no conclusion, no block.
        test_card = await svc.create_card(
            BOARD_ID,
            USER_ID,
            CardCreate(
                title=f"IE exempt test card {uuid.uuid4().hex[:6]}",
                status=CardStatus.NOT_STARTED,
                spec_id=specs[0].id,
                card_type="test",
                test_scenario_ids=[
                    (specs[0].test_scenarios or [{}])[0].get("id", "ts_seed")
                ]
                if specs[0].test_scenarios
                else ["ts_seed"],
            ),
        )
        test_card.status = CardStatus.IN_PROGRESS

        # Path 2: normal card approved via submit_task_validation -> DONE.
        exec_card = await svc.create_card(
            BOARD_ID,
            USER_ID,
            CardCreate(
                title=f"IE exempt exec card {uuid.uuid4().hex[:6]}",
                status=CardStatus.NOT_STARTED,
                spec_id=specs[0].id,
            ),
        )
        exec_card.status = CardStatus.IN_PROGRESS
        await db.commit()
        test_card_id, exec_card_id = test_card.id, exec_card.id

    async with db_factory() as db:
        svc = CardService(db)
        moved = await svc.move_card(
            test_card_id,
            USER_ID,
            CardMove(status=CardStatus.VALIDATION),
        )
        assert moved.status == CardStatus.VALIDATION
        assert getattr(moved, "card_type", None) == CardType.TEST

        await svc.move_card(
            exec_card_id,
            USER_ID,
            CardMove(status=CardStatus.VALIDATION, **_REPORT_KWARGS,
                     impact_evidence=_VALID_BLOCK),
        )
        await db.commit()
        await _mark_all_resources_na(db, "card", exec_card_id)
        result = await svc.submit_task_validation(
            exec_card_id,
            "reviewer-exempt",
            "Reviewer Exempt",
            {
                "confidence": 95,
                "confidence_justification": "Reviewed the delivered change",
                "estimated_completeness": 100,
                "completeness_justification": "Complete against the plan",
                "estimated_drift": 0,
                "drift_justification": "No deviation",
                "general_justification": "Approved; exemption path proof.",
                "recommendation": "approve",
            },
        )
        assert result["outcome"] == "success"
        await db.commit()

    async with db_factory() as db:
        from sqlalchemy_test_models import Card

        persisted = (
            await db.execute(select(Card).where(Card.id == exec_card_id))
        ).scalar_one()
        # submit_task_validation set DONE directly WITHOUT demanding a new
        # impact block (the validator conclusion path is exempt).
        assert persisted.status == CardStatus.DONE
