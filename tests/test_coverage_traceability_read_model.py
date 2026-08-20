from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
)
from okto_pulse.core.services.coverage_traceability_read_model import (
    build_coverage_traceability_projection,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW + timedelta(seconds=1)),
        as_of=NOW,
    )


def _spec(*, skip: bool = False, acceptance_id: str | None = "ac-1"):
    return SimpleNamespace(
        id="spec-1",
        edition=3,
        acceptance_criteria=[
            {"id": acceptance_id, "text": "Accepted behavior", "status": "active"}
        ],
        functional_requirements=[],
        test_scenarios=[
            {
                "id": "ts-1",
                "title": "Scenario",
                "status": "active",
                "linked_criteria": ["ac-1"],
                "linked_task_ids": ["card-test"],
            }
        ],
        business_rules=[],
        api_contracts=[],
        technical_requirements=[],
        decisions=[],
        integration_requirements=[],
        observability_requirements=[],
        skip_test_coverage=skip,
        skip_rules_coverage=False,
        skip_contract_coverage=False,
        skip_trs_coverage=False,
        skip_decisions_coverage=False,
        skip_ir_coverage=False,
        skip_or_coverage=False,
    )


def _card(*, status: str = "done", archived: bool = False):
    return SimpleNamespace(
        id="card-test",
        spec_id="spec-1",
        card_type="test",
        status=status,
        archived=archived,
        policy_version=4,
    )


def test_current_test_evidence_covers_scenario_and_linked_acceptance_criterion():
    projection = build_coverage_traceability_projection(
        query=_query(),
        as_of=NOW,
        specs=[_spec(skip=True)],
        cards=[_card()],
    )

    assert projection.totals.applicable == 2
    assert projection.totals.covered == 2
    assert projection.totals.uncovered == 0
    assert projection.totals.skipped == 2
    assert projection.totals.value == 100.0
    assert projection.code_evidence.state.value == "unavailable"
    rows = [row for group in projection.coverage for row in group.rows]
    assert all(row.skip.effective for row in rows)
    assert all(row.covered is True for row in rows)


def test_cancelled_or_archived_card_is_drillable_but_never_satisfies_coverage():
    projection = build_coverage_traceability_projection(
        query=_query(),
        as_of=NOW,
        specs=[_spec()],
        cards=[_card(status="cancelled")],
    )

    assert projection.totals.covered == 0
    assert projection.totals.uncovered == 2
    rows = [row for group in projection.coverage for row in group.rows]
    assert all(row.evidence for row in rows)
    assert {
        evidence.eligibility.value for row in rows for evidence in row.evidence
    } == {"ineligible_cancelled_or_archived"}


def test_missing_structured_identity_fails_closed_instead_of_inventing_coverage():
    projection = build_coverage_traceability_projection(
        query=_query(),
        as_of=NOW,
        specs=[_spec(acceptance_id=None)],
        cards=[_card()],
    )

    acceptance = next(
        group for group in projection.coverage if group.obligation_type.value == "ac"
    )
    assert acceptance.counts.state.value == "unavailable"
    assert acceptance.rows[0].state.value == "unavailable"
    assert acceptance.rows[0].reason == "structured_identity_missing"
