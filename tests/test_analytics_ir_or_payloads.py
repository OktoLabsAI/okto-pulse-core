"""IR/OR Analytics payload tests for board rows, spec details and MCP parity."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.services.analytics_service import _spec_detail
from okto_pulse.core.services.analytics_service import compute_spec_analytics
from okto_pulse.core.mcp.server import _mcp_spec_coverage_summary
from okto_pulse.core.models.db import Board, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.services.analytics_service import (
    _coverage_row_for_spec,
    compute_coverage,
)


BOARD_ID = "analytics-iror-board"
SPEC_ID = "analytics-iror-spec"
OWNER_ID = "owner-iror"


IR_ACTIVE = {"id": "ir_active", "status": "active", "linked_task_ids": ["card-active"]}
IR_CANCELLED = {
    "id": "ir_cancelled",
    "status": "active",
    "linked_task_ids": ["card-cancelled"],
}
OR_ACTIVE = {"id": "or_active", "status": "active", "linked_task_ids": ["card-active"]}
OR_CANCELLED = {
    "id": "or_cancelled",
    "status": "active",
    "linked_task_ids": ["card-cancelled"],
}


async def _seed_ir_or_board(db_factory) -> None:
    from sqlalchemy import select

    async with db_factory() as db:
        existing = (
            await db.execute(select(Board).where(Board.id == BOARD_ID))
        ).scalar_one_or_none()
        if existing is not None:
            return
        db.add(Board(id=BOARD_ID, name="Analytics IR/OR", owner_id=OWNER_ID))
        db.add(
            Spec(
                id=SPEC_ID,
                board_id=BOARD_ID,
                title="Spec with IR/OR",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1"],
                functional_requirements=["FR1"],
                test_scenarios=[
                    {"id": "ts1", "linked_criteria": [0], "linked_task_ids": ["card-active"]}
                ],
                business_rules=[
                    {"id": "br1", "linked_requirements": [0], "linked_task_ids": ["card-active"]}
                ],
                api_contracts=[],
                technical_requirements=[
                    {"id": "tr1", "text": "TR", "linked_task_ids": ["card-active"]}
                ],
                decisions=[
                    {"id": "dec1", "status": "active", "linked_task_ids": ["card-active"]}
                ],
                integration_requirements=[IR_ACTIVE, IR_CANCELLED],
                observability_requirements=[OR_ACTIVE, OR_CANCELLED],
                created_by=OWNER_ID,
            )
        )
        db.add(
            Card(
                id="card-active",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Active implementation",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=OWNER_ID,
            )
        )
        db.add(
            Card(
                id="card-cancelled",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Cancelled implementation",
                status=CardStatus.CANCELLED,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=OWNER_ID,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_compute_coverage_row_includes_ir_or_fields_additively(db_factory):
    await _seed_ir_or_board(db_factory)
    async with db_factory() as db:
        rows = await compute_coverage(db, BOARD_ID)

    row = next(item for item in rows if item["spec_id"] == SPEC_ID)
    for legacy_key in (
        "spec_id",
        "title",
        "total_ac",
        "covered_ac",
        "total_scenarios",
        "scenario_status_counts",
        "business_rules_count",
        "api_contracts_count",
        "fr_with_rules_pct",
        "fr_with_contracts_pct",
        "tr_task_linkage_pct",
        "decisions_coverage_pct",
    ):
        assert legacy_key in row
    assert row["irs_total"] == 2
    assert row["irs_linked"] == 1
    assert row["ir_task_linkage_pct"] == 50.0
    assert row["irs_uncovered_ids"] == ["ir_cancelled"]
    assert row["ors_total"] == 2
    assert row["ors_linked"] == 1
    assert row["or_task_linkage_pct"] == 50.0
    assert row["ors_uncovered_ids"] == ["or_cancelled"]


@pytest.mark.asyncio
async def test_modern_and_legacy_spec_detail_include_ir_or_arrays_and_summary(db_factory):
    await _seed_ir_or_board(db_factory)
    async with db_factory() as db:
        modern = await compute_spec_analytics(db, BOARD_ID, SPEC_ID)
        legacy = await _spec_detail(db, BOARD_ID, SPEC_ID)

    for payload in (modern, legacy):
        assert payload["integration_requirements"] == [IR_ACTIVE, IR_CANCELLED]
        assert payload["observability_requirements"] == [OR_ACTIVE, OR_CANCELLED]
        coverage = payload["coverage_summary"]
        assert coverage["irs_total"] == 2
        assert coverage["irs_linked"] == 1
        assert coverage["irs_uncovered_ids"] == ["ir_cancelled"]
        assert coverage["ors_total"] == 2
        assert coverage["ors_linked"] == 1
        assert coverage["ors_uncovered_ids"] == ["or_cancelled"]


@pytest.mark.asyncio
async def test_legacy_spec_detail_normalizes_structured_fr_ac_text(db_factory):
    board_id = "analytics-structured-fr-ac-board"
    spec_id = "analytics-structured-fr-ac-spec"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Analytics structured FR AC", owner_id=OWNER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Structured FR AC Spec",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=[
                    {"id": "ac_one", "text": "Structured AC", "status": "active"}
                ],
                functional_requirements=[
                    {"id": "fr_one", "text": "Structured FR", "status": "active"}
                ],
                test_scenarios=[
                    {
                        "id": "ts1",
                        "title": "Scenario",
                        "status": "passed",
                        "linked_criteria": ["ac_one"],
                    }
                ],
                business_rules=[
                    {"id": "br1", "linked_requirements": ["fr_one"], "linked_task_ids": []}
                ],
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
                integration_requirements=[],
                observability_requirements=[],
                created_by=OWNER_ID,
            )
        )
        await db.commit()

        payload = await _spec_detail(db, board_id, spec_id)

    assert payload["ac_details"] == [
        {"index": 0, "id": "ac_one", "text": "Structured AC", "covered": True}
    ]
    assert payload["fr_details"] == [
        {
            "index": 0,
            "id": "fr_one",
            "text": "Structured FR",
            "has_rule": True,
            "has_contract": False,
        }
    ]


def test_mcp_spec_coverage_summary_exposes_same_ir_or_fields_as_rest_summary():
    spec = SimpleNamespace(
        id="spec-mcp",
        title="MCP Spec",
        acceptance_criteria=[],
        functional_requirements=[],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        technical_requirements=[],
        decisions=[],
        integration_requirements=[IR_ACTIVE, IR_CANCELLED],
        observability_requirements=[OR_ACTIVE, OR_CANCELLED],
        skip_test_coverage=False,
        skip_rules_coverage=False,
        skip_decisions_coverage=False,
        skip_ir_coverage=False,
        skip_or_coverage=False,
        cards=[
            SimpleNamespace(id="card-active", status=SimpleNamespace(value="done")),
            SimpleNamespace(id="card-cancelled", status=SimpleNamespace(value="cancelled")),
        ],
    )

    mcp_summary = _mcp_spec_coverage_summary(spec)
    rest_row = _coverage_row_for_spec(spec, cards=spec.cards)

    for key in (
        "irs_total",
        "irs_linked",
        "ir_task_linkage_pct",
        "irs_uncovered_ids",
        "ors_total",
        "ors_linked",
        "or_task_linkage_pct",
        "ors_uncovered_ids",
    ):
        assert mcp_summary[key] == rest_row[key]
    assert mcp_summary["integration_requirements_total"] == 2
    assert mcp_summary["observability_requirements_total"] == 2
