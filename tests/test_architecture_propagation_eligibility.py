"""Spec A — canonical Architecture Design propagation eligibility policy.

Covers the five spec scenarios:
- TS-A1 (negative): an active finding in the current run blocks propagation.
- TS-A2 (unit): a valid suppressed warning is non-blocking.
- TS-A3 (integration): a missing/stale run is revalidated; revalidation blockers fail closed.
- TS-A4 (negative): an acknowledgement never releases an active finding (audit-only).
- TS-A5 (negative): an unloadable source/critic fails closed without mutating state.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from sqlalchemy_test_models import (
    ArchitectureDesign,
    ArchitectureFinding,
    ArchitectureFindingRun,
    Board,
    Ideation,
)
from okto_pulse.core.services.architecture import (
    PROPAGATION_REVALIDATION_MISSING_RUN,
    PROPAGATION_REVALIDATION_VERSION_INCOMPATIBLE,
    PROPAGATION_VERDICT_CURRENT,
    PROPAGATION_VERDICT_REVALIDATED,
    PROPAGATION_VERDICT_UNAVAILABLE,
    ArchitectureDesignRepository,
    ArchitectureFindingRunStore,
    ArchitecturePropagationBlocked,
    ArchitecturePropagationEligibilityPolicy,
    build_propagation_eligibility,
)

USER_ID = "arch-propagation-user"

CANONICAL_ERROR_FIELDS = (
    "source_design_id",
    "source_ref",
    "source_version",
    "parent_source",
    "critic_run_id",
    "design_version",
    "blocker_counts",
    "finding_keys",
    "issues",
    "warnings",
    "verdict_status",
    "remediation",
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_design(
    db_factory,
    *,
    version: int = 1,
    global_description: str = "Canonical eligibility source design.",
    entities: list | None = None,
    interfaces: list | None = None,
    diagrams: list | None = None,
) -> tuple[str, str]:
    board_id = _id("prop-board")
    ideation_id = _id("prop-ideation")
    design_id = _id("prop-design")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Propagation Eligibility Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Propagation Ideation",
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=design_id,
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Propagation Source",
                global_description=global_description,
                entities=entities or [],
                interfaces=interfaces or [],
                diagrams=diagrams or [],
                version=version,
                source_ref="architecture_design:source-parent",
                source_version=3,
                source_design_id="source-parent-design",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, design_id


def _warning(
    code: str,
    *,
    element_id: str = "node-a",
    diagram_id: str = "runtime",
    path: str = "diagrams[0].adapter_payload.elements[0]",
) -> dict:
    return {
        "code": code,
        "severity": "warning",
        "message": f"{code} message",
        "path": path,
        "suggested_fix": f"fix {code}",
        "diagram_id": diagram_id,
        "diagram_type": "runtime",
        "element_id": element_id,
    }


async def _persist_run_with_active_finding(
    db_factory,
    board_id: str,
    design_id: str,
    *,
    design_version: int = 1,
    code: str = "isolated_entity_node",
) -> dict:
    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        result = await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=design_version,
            critic_run_id=f"critic-{uuid.uuid4().hex[:8]}",
            actor={"actor_type": "agent", "actor_id": "agent-a", "actor_name": "Agent A"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[_warning(code)],
        )
        await db.commit()
    return result


async def _count_runs_and_findings(db_factory, design_id: str) -> tuple[int, int]:
    async with db_factory() as db:
        runs = (
            await db.execute(
                select(func.count())
                .select_from(ArchitectureFindingRun)
                .where(ArchitectureFindingRun.design_id == design_id)
            )
        ).scalar_one()
        findings = (
            await db.execute(
                select(func.count())
                .select_from(ArchitectureFinding)
                .where(ArchitectureFinding.design_id == design_id)
            )
        ).scalar_one()
    return runs, findings


# --------------------------------------------------------------------------- #
# TS-A1 (negative): active finding in the current run blocks propagation.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_a1_active_finding_blocks_propagation(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    run = await _persist_run_with_active_finding(db_factory, board_id, design_id)
    finding_key = run["findings"][0]["finding_key"]
    before = await _count_runs_and_findings(db_factory, design_id)

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    assert eligibility.eligible is False
    assert eligibility.verdict_status == PROPAGATION_VERDICT_CURRENT
    assert finding_key in eligibility.finding_keys
    assert eligibility.blocking_warnings[0]["finding_key"] == finding_key
    assert eligibility.blocker_counts["blocking_warnings"] == 1
    assert eligibility.blocker_counts["total"] == 1
    assert eligibility.remediation

    # AC ac_433cf728 / TS-A1: no downstream mutation is performed by an eligibility check.
    after = await _count_runs_and_findings(db_factory, design_id)
    assert before == after == (1, 1)


@pytest.mark.asyncio
async def test_ts_a1_require_eligible_raises_canonical_structured_error(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    await _persist_run_with_active_finding(db_factory, board_id, design_id)

    async with db_factory() as db:
        policy = ArchitecturePropagationEligibilityPolicy(db)
        with pytest.raises(ArchitecturePropagationBlocked) as excinfo:
            await policy.require_eligible(design_id)

    payload = excinfo.value.to_payload()
    # FR b64537ee / validator lock: all canonical fields materialized as a contract.
    for field in CANONICAL_ERROR_FIELDS:
        assert field in payload, f"missing canonical field: {field}"
    assert payload["code"] == "architecture_propagation_blocked"
    assert payload["error"] == "architecture_propagation_blocked"
    assert payload["verdict_status"] == PROPAGATION_VERDICT_CURRENT
    assert payload["source_design_id"] == "source-parent-design"
    assert payload["source_ref"] == "architecture_design:source-parent"
    assert payload["source_version"] == 3
    assert payload["parent_source"]["parent_type"] == "ideation"
    assert payload["design_version"] == 1
    assert payload["finding_keys"]
    assert payload["blocker_counts"]["total"] >= 1
    # to_error_dict alias mirrors to_payload (REST/MCP parity with resource errors).
    assert excinfo.value.to_error_dict() == payload


# --------------------------------------------------------------------------- #
# TS-A2 (unit): a valid suppressed warning is non-blocking.
# --------------------------------------------------------------------------- #
def test_ts_a2_suppressed_warning_is_non_blocking_unit():
    eligibility = build_propagation_eligibility(
        design_id="design-1",
        source_design_id=None,
        source_ref=None,
        source_version=None,
        parent_type="ideation",
        parent_id="ideation-1",
        design_version=1,
        critic_run_id="critic-1",
        verdict_status=PROPAGATION_VERDICT_CURRENT,
        revalidation_reason=None,
        issues=[],
        blocking_warnings=[],
        suppressed_warnings=[
            {"code": "conceptual_runtime_only", "justification": "valid conceptual diagram"}
        ],
    )

    assert eligibility.eligible is True
    assert eligibility.finding_keys == []
    assert eligibility.blocker_counts["total"] == 0
    assert eligibility.non_blocking["suppressed_warnings_count"] == 1
    assert eligibility.remediation is None
    payload = eligibility.to_dict()
    assert payload["non_blocking"]["suppressed_warnings"][0]["code"] == "conceptual_runtime_only"


def test_ts_a2_blocking_warning_flips_eligible_unit():
    eligibility = build_propagation_eligibility(
        design_id="design-1",
        source_design_id=None,
        source_ref=None,
        source_version=None,
        parent_type="ideation",
        parent_id="ideation-1",
        design_version=1,
        critic_run_id="critic-1",
        verdict_status=PROPAGATION_VERDICT_CURRENT,
        revalidation_reason=None,
        issues=[],
        blocking_warnings=[{"finding_key": "k1", "code": "isolated_entity_node"}],
        suppressed_warnings=[{"code": "conceptual_runtime_only"}],
    )

    assert eligibility.eligible is False
    assert eligibility.finding_keys == ["k1"]
    assert eligibility.blocker_counts["blocking_warnings"] == 1
    assert eligibility.remediation


# --------------------------------------------------------------------------- #
# TS-A3 (integration): missing/stale run is revalidated; blockers fail closed.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_a3_missing_run_revalidates_clean_eligible(db_factory):
    board_id, design_id = await _seed_design(db_factory)  # clean payload, no finding run

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    assert eligibility.verdict_status == PROPAGATION_VERDICT_REVALIDATED
    assert eligibility.revalidation_reason == PROPAGATION_REVALIDATION_MISSING_RUN
    assert eligibility.eligible is True
    assert eligibility.issues == []


@pytest.mark.asyncio
async def test_ts_a3_stale_version_triggers_revalidation(db_factory):
    board_id, design_id = await _seed_design(db_factory, version=2)
    # Persist a run at an older design_version so the current run is version-incompatible.
    await _persist_run_with_active_finding(db_factory, board_id, design_id, design_version=1)

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    assert eligibility.verdict_status == PROPAGATION_VERDICT_REVALIDATED
    assert eligibility.revalidation_reason == PROPAGATION_REVALIDATION_VERSION_INCOMPATIBLE
    # clean payload -> revalidation clears it even though a stale active finding exists
    assert eligibility.eligible is True


@pytest.mark.asyncio
async def test_ts_a3_revalidation_blockers_fail_closed_without_persist(db_factory):
    # An unloadable/invalid persisted payload (blank global_description) with no run:
    # revalidation re-runs the deterministic critic, finds an issue, and fails closed.
    board_id, design_id = await _seed_design(db_factory, global_description="   ")
    before = await _count_runs_and_findings(db_factory, design_id)

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    assert eligibility.verdict_status == PROPAGATION_VERDICT_REVALIDATED
    assert eligibility.eligible is False
    assert eligibility.issues  # global_description is required
    assert eligibility.blocker_counts["issues"] >= 1
    # Revalidation is in-memory only: no finding run / finding rows are written.
    after = await _count_runs_and_findings(db_factory, design_id)
    assert before == after == (0, 0)


# --------------------------------------------------------------------------- #
# TS-A4 (negative): acknowledgement never releases an active finding (audit-only).
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_a4_acknowledgement_does_not_release_active_finding(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    run = await _persist_run_with_active_finding(db_factory, board_id, design_id)
    finding_key = run["findings"][0]["finding_key"]
    critic_run_id = run["critic_run_id"]

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        await store.record_acknowledgements(
            board_id=board_id,
            design_id=design_id,
            critic_run_id=critic_run_id,
            finding_keys=[finding_key],
            actor={"actor_type": "user", "actor_id": USER_ID, "actor_name": USER_ID},
            statement="Acknowledged for authoring; conceptual diagram.",
        )
        await db.commit()

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    # AC ac_739c4d97: acknowledgement is audit evidence only and never authorizes propagation.
    assert eligibility.eligible is False
    assert finding_key in eligibility.finding_keys
    assert eligibility.acknowledgements_audit
    assert all(ack["audit_only"] is True for ack in eligibility.acknowledgements_audit)
    assert any(ack["finding_key"] == finding_key for ack in eligibility.acknowledgements_audit)


# --------------------------------------------------------------------------- #
# TS-A5 (negative): unloadable source/critic fails closed without mutating state.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_a5_missing_source_is_fail_closed(db_factory):
    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(
            "design-does-not-exist"
        )

    assert eligibility.eligible is False
    assert eligibility.verdict_status == PROPAGATION_VERDICT_UNAVAILABLE
    assert eligibility.revalidation_reason == "source_design_not_found"
    assert eligibility.issues
    assert eligibility.remediation
    payload = eligibility.to_dict()
    for field in CANONICAL_ERROR_FIELDS:
        assert field in payload


@pytest.mark.asyncio
async def test_ts_a5_load_exception_is_fail_closed_without_mutation(db_factory, monkeypatch):
    board_id, design_id = await _seed_design(db_factory)  # no run -> revalidation path
    before = await _count_runs_and_findings(db_factory, design_id)

    def _boom(self, payload):
        raise RuntimeError("critic payload could not be loaded")

    monkeypatch.setattr(ArchitectureDesignRepository, "critique_payload", _boom)

    async with db_factory() as db:
        eligibility = await ArchitecturePropagationEligibilityPolicy(db).evaluate(design_id)

    # AC ac_2c06ae77: fail-closed structured error, no downstream state change.
    assert eligibility.eligible is False
    assert eligibility.verdict_status == PROPAGATION_VERDICT_UNAVAILABLE
    assert eligibility.revalidation_reason == "evaluation_error"
    after = await _count_runs_and_findings(db_factory, design_id)
    assert before == after == (0, 0)
