"""SK-B B01 acceptance tests for the public guideline policy port."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.domain.guideline_policy import (
    GuidelineRevisionPageCursor,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicyWaiverStatus,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelineImpactListQuery,
    GuidelinePolicyBindingConflict,
    GuidelinePolicyCasConflict,
    GuidelinePolicyCursorConflict,
    GuidelinePolicyDigestConflict,
    GuidelinePolicyHeadConflict,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyPersistencePort,
    GuidelinePolicyRevisionConflict,
    GuidelinePolicySubjectConflict,
    GuidelinePolicyVersionConflict,
    GuidelineRevisionListQuery,
    PolicyComplianceFindingListQuery,
    PolicyComplianceReceiptListQuery,
    PolicyWaiverListQuery,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
PORT_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "ports"
    / "guideline_policy.py"
)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


class FakeGuidelinePolicyPersistence:
    async def get_guideline(self, *, guideline_id):
        return None

    async def get_head(self, *, guideline_id):
        return None

    async def get_revision(self, *, guideline_id, revision_id):
        return None

    async def get_retirement(self, *, guideline_id):
        return None

    async def list_revisions(self, query):
        raise NotImplementedError

    async def get_revision_result_by_idempotency(self, **kwargs):
        return None

    async def get_retirement_result_by_idempotency(self, **kwargs):
        return None

    async def create_guideline(self, **kwargs):
        raise NotImplementedError

    async def append_revision_cas(self, **kwargs):
        raise NotImplementedError

    async def record_revision_noop_cas(self, **kwargs):
        raise NotImplementedError

    async def retire_guideline_cas(self, **kwargs):
        raise NotImplementedError

    async def get_binding(self, *, board_id, guideline_id):
        return None

    async def list_bindings(self, *, board_id):
        return ()

    async def append_binding_cas(self, **kwargs):
        raise NotImplementedError

    async def adopt_revision_cas(self, **kwargs):
        raise NotImplementedError

    async def unlink_binding_cas(self, **kwargs):
        raise NotImplementedError

    async def save_impact_preview(self, **kwargs):
        raise NotImplementedError

    async def get_impact_receipt(self, *, board_id, impact_receipt_id):
        return None

    async def get_impact_receipt_by_idempotency(self, *, board_id, idempotency_key):
        return None

    async def get_adoption_result_by_idempotency(self, *, board_id, idempotency_key):
        return None

    async def list_impact_items(self, query):
        raise NotImplementedError

    async def list_policy_subjects(self, *, board_id):
        return ()

    async def resolve_policy_subject_snapshot(self, **kwargs):
        return None

    async def list_board_waivers(self, *, board_id):
        return ()

    async def resolve_transition_snapshot(self, **kwargs):
        raise NotImplementedError

    async def save_evaluation_result(self, **kwargs):
        raise NotImplementedError

    async def get_compliance_receipt(self, *, board_id, receipt_id):
        return None

    async def get_current_compliance_receipt(self, *, subject):
        return None

    async def list_compliance_receipts(self, query):
        raise NotImplementedError

    async def list_compliance_findings(self, query):
        raise NotImplementedError

    async def get_waiver(self, *, board_id, waiver_id):
        return None

    async def list_waivers(self, query):
        raise NotImplementedError

    async def list_waiver_events(self, *, board_id, waiver_id):
        return ()

    async def create_waiver(self, **kwargs):
        raise NotImplementedError

    async def transition_waiver_cas(self, **kwargs):
        raise NotImplementedError

    async def resolve_effective_waiver(self, **kwargs):
        return None

    async def resolve_policy_waiver_source(self, **kwargs):
        return None

    async def resolve_idempotent_result(self, **kwargs):
        return None

    async def export_guideline_snapshot(self, **kwargs):
        raise NotImplementedError

    async def load_guideline_import_snapshot(self, **kwargs):
        raise NotImplementedError

    async def apply_guideline_import_plan(self, plan, **kwargs):
        raise NotImplementedError


def test_port_is_runtime_checkable_and_framework_free() -> None:
    assert isinstance(
        FakeGuidelinePolicyPersistence(),
        GuidelinePolicyPersistencePort,
    )

    imports = _imported_modules(PORT_PATH)
    assert not any(
        module == forbidden or module.startswith(f"{forbidden}.")
        for module in imports
        for forbidden in (
            "fastapi",
            "pydantic",
            "sqlalchemy",
            "okto_pulse.community",
            "okto_pulse.core.infra",
        )
    )
    source = PORT_PATH.read_text(encoding="utf-8").lower()
    assert "asyncsession" not in source
    assert "def commit(" not in source
    assert "def rollback(" not in source


def test_each_persisted_family_has_a_typed_keyset_query() -> None:
    revision_context = GuidelineRevisionListQuery(
        guideline_id="guideline-1",
        limit=25,
    )
    revision_cursor = GuidelineRevisionPageCursor(
        revision_number=7,
        item_id="revision-cursor-item",
        filter_digest=revision_context.filter_digest,
        projection_digest=revision_context.projection_digest,
    )
    revision_query = GuidelineRevisionListQuery(
        guideline_id="guideline-1",
        limit=25,
        cursor=revision_cursor,
    )
    impact_query = GuidelineImpactListQuery(
        board_id="board-1",
        impact_receipt_id="impact-1",
    )
    receipt_query = PolicyComplianceReceiptListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        outcome=PolicyEvaluationOutcome.FAIL,
    )
    finding_query = PolicyComplianceFindingListQuery(
        board_id="board-1",
        receipt_id="receipt-1",
        outcome=PolicyEvaluationOutcome.FAIL,
    )
    waiver_query = PolicyWaiverListQuery(
        board_id="board-1",
        evaluated_at=NOW,
        guideline_id="guideline-1",
        status=PolicyWaiverStatus.REQUESTED,
    )

    assert revision_query.cursor is revision_cursor
    assert revision_query.ordering == (
        "revision_number DESC",
        "revision_id DESC",
    )
    assert impact_query.ordering == (
        "entity_type ASC",
        "entity_id ASC",
        "impact_item_id ASC",
    )
    assert receipt_query.entity_type is PolicyEntityType.SPEC
    assert finding_query.receipt_id == "receipt-1"
    assert waiver_query.status is PolicyWaiverStatus.REQUESTED


@pytest.mark.parametrize(
    "factory",
    [
        lambda: GuidelineRevisionListQuery(guideline_id="guideline-1", limit=0),
        lambda: GuidelineImpactListQuery(
            board_id=" ",
            impact_receipt_id="impact-1",
            limit=50,
        ),
        lambda: PolicyComplianceReceiptListQuery(
            board_id="board-1",
            entity_type="all",  # type: ignore[arg-type]
        ),
        lambda: PolicyComplianceFindingListQuery(
            board_id="board-1",
            outcome="fail",  # type: ignore[arg-type]
        ),
        lambda: PolicyWaiverListQuery(
            board_id="board-1",
            evaluated_at=NOW,
            status="requested",  # type: ignore[arg-type]
        ),
        lambda: PolicyWaiverListQuery(
            board_id="board-1",
            evaluated_at=NOW,
            subject_version=2_147_483_648,
        ),
    ],
    ids=(
        "zero-limit",
        "blank-board",
        "all-is-not-a-target",
        "string-outcome",
        "string-waiver-status",
        "waiver-subject-version-overflow",
    ),
)
def test_queries_fail_closed_on_invalid_windows_and_enum_filters(factory) -> None:
    with pytest.raises(ValueError):
        factory()


def test_conflicts_are_specific_machine_readable_types() -> None:
    conflicts = (
        GuidelinePolicyHeadConflict,
        GuidelinePolicyCasConflict,
        GuidelinePolicyRevisionConflict,
        GuidelinePolicyBindingConflict,
        GuidelinePolicySubjectConflict,
        GuidelinePolicyVersionConflict,
        GuidelinePolicyDigestConflict,
        GuidelinePolicyIdempotencyConflict,
        GuidelinePolicyCursorConflict,
    )

    assert len({conflict.code for conflict in conflicts}) == len(conflicts)
    for conflict_type in conflicts:
        error = conflict_type(details=(("entity_id", "value-1"),))
        assert error.code.startswith("guideline_policy_")
        assert error.details == (("entity_id", "value-1"),)
