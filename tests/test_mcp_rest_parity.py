"""REST vs MCP parity tests — ideação #9 Fase 2 gate anti-drift.

Strategy
--------
Each service function (`compute_*`, `spec_coverage_summary`,
`filter_decisions_by_status`, `decisions_stats`) is invoked twice with the
argument signatures used by REST and MCP respectively. For seed data that
is entirely non-archived, both invocations MUST produce structurally
equivalent output:

- Dict returns: identical top-level keys, identical type per key.
- List returns: identical per-row key set, identical per-key type.

When a key's type differs between the two calls, the test fails — this is
how drift is caught at the contract level.

Structural (not value-level) assertions because `include_archived` can
legitimately change numeric totals. Parity is about the *contract*:
callers should never have to branch on which path produced the payload.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.analytics_service import (
    aggregate_spec_validation_gate,
    aggregate_task_validation_gate,
    classify_spec_violation,
    classify_task_violation,
    compute_blockers,
    compute_coverage,
    compute_funnel,
    compute_velocity,
    decisions_stats,
    filter_decisions_by_status,
    spec_coverage_summary,
)


BOARD_ID = "parity-board-001"
SPEC_ID = "parity-spec-001"


async def _seed_board(db_factory) -> None:
    """Minimal fixture: 1 board, 1 ideation done, 1 refinement done, 1 spec
    done, 2 cards (1 done impl, 1 in_progress test).

    Idempotent — skips if board already seeded by a prior test in this session.
    """
    from sqlalchemy import select
    async with db_factory() as db:
        existing = (
            await db.execute(select(Board).where(Board.id == BOARD_ID))
        ).scalar_one_or_none()
        if existing is not None:
            return
    ideation_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=BOARD_ID, name="Parity Board", owner_id="owner-1"))
        db.add(Ideation(
            id=ideation_id,
            board_id=BOARD_ID,
            title="Parity ideation",
            status=IdeationStatus.DONE,
            archived=False,
            created_by="user-1",
        ))
        db.add(Refinement(
            id=str(uuid.uuid4()),
            ideation_id=ideation_id,
            board_id=BOARD_ID,
            title="Parity refinement",
            status=RefinementStatus.DONE,
            archived=False,
            created_by="user-1",
        ))
        db.add(Spec(
            id=SPEC_ID,
            board_id=BOARD_ID,
            title="Parity spec",
            status=SpecStatus.DONE,
            archived=False,
            acceptance_criteria=["AC1", "AC2", "AC3"],
            functional_requirements=["FR1", "FR2"],
            test_scenarios=[
                {"id": "ts_x", "title": "S1", "linked_criteria": [0, "1"], "status": "passed",
                 "linked_task_ids": ["card-done-1"]},
            ],
            business_rules=[
                {"id": "br_1", "title": "BR1", "linked_requirements": [0],
                 "linked_task_ids": ["card-done-1"]},
            ],
            api_contracts=[],
            technical_requirements=[
                {"id": "tr_1", "text": "TR1", "linked_task_ids": ["card-done-1"]},
            ],
            decisions=[
                {"id": "d1", "title": "Decision 1", "status": "active"},
                {"id": "d2", "title": "Decision 2", "status": "superseded"},
            ],
            created_by="user-1",
        ))
        yesterday = datetime.now(timezone.utc) - timedelta(hours=12)
        db.add(Card(
            id="card-done-1",
            board_id=BOARD_ID,
            spec_id=SPEC_ID,
            title="Done impl",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            archived=False,
            created_by="user-1",
            created_at=yesterday,
            updated_at=yesterday,
        ))
        db.add(Card(
            id="card-test-1",
            board_id=BOARD_ID,
            spec_id=SPEC_ID,
            title="Open test",
            status=CardStatus.IN_PROGRESS,
            card_type=CardType.TEST,
            archived=False,
            created_by="user-1",
        ))
        await db.commit()


def _type_map(d: dict) -> dict:
    return {k: type(v).__name__ for k, v in d.items()}


def _assert_dict_shape_equal(a: dict, b: dict, label: str) -> None:
    ka, kb = set(a.keys()), set(b.keys())
    assert ka == kb, (
        f"{label}: key mismatch\n"
        f"  only in A (REST): {ka - kb}\n"
        f"  only in B (MCP):  {kb - ka}"
    )
    ta, tb = _type_map(a), _type_map(b)
    # Allow NoneType ↔ non-None divergence when value is absent on one side
    # (totals can be 0/None based on include_archived). Type parity only
    # required when both sides have a non-None value.
    for k in ka:
        va, vb = a[k], b[k]
        if va is None or vb is None:
            continue
        assert type(va) is type(vb), (
            f"{label}: key {k!r} type mismatch REST={type(va).__name__} vs MCP={type(vb).__name__}"
        )
    # Recurse into nested dicts with same rule
    for k in ka:
        if isinstance(a[k], dict) and isinstance(b[k], dict):
            _assert_dict_shape_equal(a[k], b[k], f"{label}.{k}")


@pytest.mark.asyncio
class TestCoverageParity:
    """D-1 — compute_coverage returns identical shape for REST and MCP paths."""

    async def test_coverage_row_keys_match(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            rest = await compute_coverage(db, BOARD_ID, include_archived=False)
            mcp = await compute_coverage(db, BOARD_ID, include_archived=True)
        assert rest, "REST: expected at least 1 coverage row"
        assert mcp, "MCP: expected at least 1 coverage row"
        _assert_dict_shape_equal(rest[0], mcp[0], "coverage[0]")

    async def test_coverage_row_has_migrated_keys(self, db_factory):
        """Regression guard: MCP previously omitted BR/contract counts + FR pcts.
        After D-1 migration both paths MUST include them.
        """
        await _seed_board(db_factory)
        async with db_factory() as db:
            rows = await compute_coverage(db, BOARD_ID)
        assert rows
        row = rows[0]
        for k in (
            "spec_id", "title", "total_ac", "covered_ac", "total_scenarios",
            "scenario_status_counts", "business_rules_count",
            "api_contracts_count", "fr_with_rules_pct", "fr_with_contracts_pct",
        ):
            assert k in row, f"coverage row missing migrated key: {k}"


@pytest.mark.asyncio
class TestFunnelParity:
    """D-4 — compute_funnel returns the full rich dict for both paths."""

    async def test_funnel_top_level_keys_match(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            rest = await compute_funnel(db, BOARD_ID, include_archived=False)
            mcp = await compute_funnel(db, BOARD_ID, include_archived=True)
        _assert_dict_shape_equal(rest, mcp, "funnel")

    async def test_funnel_has_rich_shape(self, db_factory):
        """Regression guard: MCP previously returned only 6 keys
        (ideations/refinements/specs/cards/done/...). Migration requires the
        rich shape with status breakdowns + cycle_time_by_phase.
        """
        await _seed_board(db_factory)
        async with db_factory() as db:
            funnel = await compute_funnel(db, BOARD_ID)
        for k in (
            "ideations", "refinements", "specs", "sprints", "cards", "done",
            "ideations_done", "specs_done", "refinements_done",
            "cards_impl", "cards_test", "cards_bug",
            "rules_count", "contracts_count",
            "specs_with_rules", "specs_with_contracts",
            "spec_status_breakdown", "card_status_breakdown",
            "sprint_status_breakdown",
            "bugs_total", "bugs_open", "bugs_by_severity",
            "avg_cycle_hours", "cycle_time_by_phase",
        ):
            assert k in funnel, f"funnel missing key: {k}"
        cycle = funnel["cycle_time_by_phase"]
        assert set(cycle.keys()) == {"ideation", "refinement", "spec", "sprint", "card"}


@pytest.mark.asyncio
class TestVelocityParity:
    """D-5 — compute_velocity returns list of bucket dicts, same shape both paths."""

    async def test_velocity_weekly_bucket_keys_match(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            rest = await compute_velocity(
                db, BOARD_ID, granularity="week", weeks=12,
            )
            mcp = await compute_velocity(
                db, BOARD_ID, granularity="week", weeks=12,
                include_archived=True,
            )
        assert rest and mcp
        _assert_dict_shape_equal(rest[0], mcp[0], "velocity[week][0]")

    async def test_velocity_daily_granularity_accepted(self, db_factory):
        """MCP previously hardcoded weekly — migration must accept day."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            daily = await compute_velocity(
                db, BOARD_ID, granularity="day", days=7,
            )
        assert daily
        assert "day" in daily[0]
        assert "week" not in daily[0]

    async def test_velocity_bucket_has_migrated_keys(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            buckets = await compute_velocity(db, BOARD_ID, granularity="week", weeks=4)
        for k in ("impl", "test", "bug", "validation_bounce", "spec_done", "sprint_done"):
            assert k in buckets[0], f"velocity bucket missing migrated key: {k}"


class TestSpecCoverageSummaryParity:
    """D-7 — spec_coverage_summary is a pure function; verify all 22 keys present."""

    EXPECTED_KEYS = {
        "ac_coverage_pct", "ac_covered", "ac_total", "ac_uncovered_indices",
        "fr_coverage_pct", "fr_covered", "fr_total", "fr_uncovered_indices",
        "scenario_task_linkage_pct", "scenarios_linked", "scenarios_total",
        "br_task_linkage_pct", "brs_linked", "brs_total",
        "contract_task_linkage_pct", "contracts_linked", "contracts_total",
        "tr_task_linkage_pct", "trs_linked", "trs_total",
        # Ideação #10 Fase 1 — decisions paridade
        "decisions_coverage_pct", "decisions_linked", "decisions_total",
        "decisions_uncovered_ids",
        "skip_test_coverage", "skip_rules_coverage", "skip_decisions_coverage",
    }

    class _FakeSpec:
        acceptance_criteria = ["AC1", "AC2"]
        functional_requirements = ["FR1"]
        test_scenarios = [{"id": "t1", "linked_criteria": [0], "linked_task_ids": ["c1"]}]
        business_rules = [{"id": "b1", "linked_requirements": [0], "linked_task_ids": ["c1"]}]
        api_contracts = []
        technical_requirements = [{"id": "tr1", "text": "TR", "linked_task_ids": ["c1"]}]
        skip_test_coverage = False
        skip_rules_coverage = False

    def test_all_expected_keys_present(self):
        out = spec_coverage_summary(self._FakeSpec())
        assert set(out.keys()) == self.EXPECTED_KEYS

    def test_override_args_do_not_change_shape(self):
        """MCP path may pass override args (scenarios=, trs=) for in-flight specs.
        Shape must remain identical regardless of override presence.
        """
        base = spec_coverage_summary(self._FakeSpec())
        overridden = spec_coverage_summary(
            self._FakeSpec(),
            scenarios=[],
            rules=[],
            contracts=[],
            trs=[],
        )
        assert set(base.keys()) == set(overridden.keys())


class TestDecisionsHelpersParity:
    """D-8 — filter_decisions_by_status + decisions_stats used by both REST and MCP."""

    FIXTURE = [
        {"id": "d1", "status": "active"},
        {"id": "d2", "status": "superseded"},
        {"id": "d3", "status": "revoked"},
        {"id": "d4"},  # legacy → active
    ]

    def test_filter_shape_stable(self):
        default = filter_decisions_by_status(self.FIXTURE)
        widened = filter_decisions_by_status(self.FIXTURE, include_superseded=True)
        # Returns list[dict] regardless of flag
        assert isinstance(default, list) and all(isinstance(d, dict) for d in default)
        assert isinstance(widened, list) and all(isinstance(d, dict) for d in widened)
        # Each dict retains the input keys (no projection)
        if default:
            assert set(default[0].keys()) <= set(self.FIXTURE[0].keys()) | {"status"}

    def test_stats_has_fixed_keys(self):
        stats = decisions_stats(self.FIXTURE)
        assert set(stats.keys()) == {"total", "active", "superseded", "revoked", "other"}
        # All counts are ints
        for v in stats.values():
            assert isinstance(v, int)


@pytest.mark.asyncio
class TestBlockersParity:
    """D-6 — compute_blockers returns identical payload for REST and MCP paths."""

    async def test_blockers_payload_shape(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            payload = await compute_blockers(db, BOARD_ID)
        assert set(payload.keys()) == {
            "board_id", "summary", "total",
            "stale_hours_threshold", "filter_type", "blockers",
        }
        assert isinstance(payload["summary"], dict)
        assert isinstance(payload["blockers"], list)
        assert payload["filter_type"] is None
        assert payload["stale_hours_threshold"] == 72

    async def test_filter_type_echoed_and_applied(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            filtered = await compute_blockers(db, BOARD_ID, filter_type="stale")
        assert filtered["filter_type"] == "stale"
        for b in filtered["blockers"]:
            assert b["type"] == "stale"

    async def test_invalid_stale_hours_raises(self, db_factory):
        async with db_factory() as db:
            with pytest.raises(ValueError):
                await compute_blockers(db, BOARD_ID, stale_hours=0)


class TestValidationGateParity:
    """D-2 / D-3 — task + spec validation gate aggregators are pure, shape fixed."""

    TASK_EXPECTED_KEYS = {
        "total_submitted", "total_success", "total_failed", "success_rate",
        "avg_attempts_per_card", "first_pass_rate", "avg_scores",
        "rejection_reasons", "cards_with_validation",
    }
    SPEC_EXPECTED_KEYS = {
        "total_submitted", "total_success", "total_failed", "success_rate",
        "avg_attempts_per_spec", "avg_scores", "rejection_reasons",
        "specs_with_validation",
    }

    class _FakeCard:
        validations = [
            {"outcome": "success", "confidence": 90, "completeness": 95, "drift": 5,
             "recommendation": "approve"},
            {"outcome": "failed", "confidence": 50, "estimated_completeness": 40,
             "estimated_drift": 80, "recommendation": "reject",
             "threshold_violations": ["confidence below 70", "drift above 50"]},
        ]

    class _FakeSpec:
        validations = [
            {"outcome": "success", "completeness": 90, "assertiveness": 85,
             "ambiguity": 15, "recommendation": "approve"},
            {"outcome": "failed", "completeness": 60, "assertiveness": 70,
             "ambiguity": 45, "recommendation": "reject",
             "threshold_violations": ["completeness below 80", "ambiguity above 30"]},
        ]

    def test_task_gate_has_all_expected_keys(self):
        out = aggregate_task_validation_gate([self._FakeCard()])
        assert set(out.keys()) == self.TASK_EXPECTED_KEYS

    def test_spec_gate_has_all_expected_keys(self):
        out = aggregate_spec_validation_gate([self._FakeSpec()])
        assert set(out.keys()) == self.SPEC_EXPECTED_KEYS

    def test_task_gate_empty_input(self):
        out = aggregate_task_validation_gate([])
        assert out["total_submitted"] == 0
        assert out["success_rate"] is None
        assert set(out["rejection_reasons"].keys()) == {
            "confidence_below", "completeness_below",
            "drift_above", "reject_recommendation",
        }

    def test_spec_gate_empty_input(self):
        out = aggregate_spec_validation_gate([])
        assert out["total_submitted"] == 0
        assert out["success_rate"] is None
        assert set(out["rejection_reasons"].keys()) == {
            "completeness_below", "assertiveness_below",
            "ambiguity_above", "reject_recommendation",
        }

    def test_task_gate_first_pass_tracked(self):
        out = aggregate_task_validation_gate([self._FakeCard()])
        assert out["first_pass_rate"] == 100.0  # first validation was success

    def test_classify_multi_count(self):
        """D3 regression guard — a single record can contribute multiple reasons."""
        reasons = classify_task_violation(
            ["confidence below 70", "drift above 50", "completeness below 80"],
            "reject",
        )
        assert set(reasons) == {
            "confidence_below", "drift_above",
            "completeness_below", "reject_recommendation",
        }

    def test_classify_spec_multi_count(self):
        reasons = classify_spec_violation(
            ["completeness below 80", "ambiguity above 30"],
            "reject",
        )
        assert set(reasons) == {
            "completeness_below", "ambiguity_above", "reject_recommendation",
        }


# ---------------------------------------------------------------------------
# Structural contract — delegation wiring
# ---------------------------------------------------------------------------


class TestDelegationContract:
    """Source-level contract checks: REST + MCP both reach the service.

    If anyone re-adds inline aggregation, one of these will fail.
    """

    def test_rest_analytics_imports_service_functions(self):
        from okto_pulse.core.api import analytics as rest_mod
        src = inspect.getsource(rest_mod)
        for fn in (
            "compute_coverage", "compute_funnel", "compute_velocity",
            "compute_blockers",
            "aggregate_task_validation_gate", "aggregate_spec_validation_gate",
        ):
            assert fn in src, f"REST analytics missing service import: {fn}"

    def test_mcp_server_imports_service_functions(self):
        from okto_pulse.core.mcp import server as mcp_mod
        src = inspect.getsource(mcp_mod)
        for fn in (
            "compute_coverage", "compute_funnel", "compute_velocity",
            "compute_blockers",
            "aggregate_task_validation_gate",
        ):
            assert fn in src, f"MCP server missing service import: {fn}"

    def test_mcp_re_exports_decisions_helpers(self):
        """D-8 — MCP server re-exports filter_decisions_by_status + decisions_stats
        from the service (backwards compat shim)."""
        from okto_pulse.core.mcp import server as mcp_mod
        assert hasattr(mcp_mod, "_filter_decisions_by_status")
        assert hasattr(mcp_mod, "_decisions_stats")

    def test_mcp_re_exports_spec_coverage(self):
        from okto_pulse.core.mcp import server as mcp_mod
        assert hasattr(mcp_mod, "_spec_coverage")
