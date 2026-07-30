"""SK-B B01 acceptance tests for the pure guideline-domain/v1 contract."""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_DOMAIN_CONTRACT_VERSION,
    AdoptedGuidelineRevisionRef,
    BoardGuidelineBinding,
    Guideline,
    GuidelineContextScope,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineImpactItemKind,
    GuidelinePage,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRevisionPage,
    GuidelineRevisionPageCursor,
    GuidelineRule,
    GuidelineRuleOperator,
    GuidelineScope,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_ENTITY_ID_MAX_LENGTH,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    PolicyComplianceFinding,
    PolicyComplianceReceipt,
    PolicyComplianceRuleResult,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationInput,
    PolicyEvaluationOutcome,
    PolicyEvaluationResult,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    PolicyWaiver,
    PolicyWaiverEventType,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_impact import (
    GuidelineImpactPreviewCommand,
    assess_guideline_impact_currentness,
    impact_fence_from_receipt,
    plan_guideline_impact_preview,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DOMAIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_policy.py"
)


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def _rule(**overrides) -> GuidelineRule:
    values = {
        "rule_id": "rule-1",
        "code": "requirements.acceptance_coverage",
        "title": "Acceptance coverage",
        "description": "Every applicable requirement has acceptance coverage.",
        "target_entity_types": (PolicyEntityType.SPEC,),
        "predicates": (
            GuidelinePredicate(
                "gte",
                (("fact", "coverage_percent"), ("value", 100)),
            ),
        ),
    }
    values.update(overrides)
    return GuidelineRule(**values)


def _binding(**overrides) -> BoardGuidelineBinding:
    values = {
        "binding_id": "binding-1",
        "board_id": "board-1",
        "guideline_id": "guideline-1",
        "revision_id": "revision-1",
        "semantic_version": "1.0.0",
        "revision_digest": DIGEST_A,
        "priority": 0,
        "binding_revision": 1,
        "adopted_by": "agent-1",
        "adopted_at": NOW,
    }
    values.update(overrides)
    return BoardGuidelineBinding(**values)


def _subject() -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=4,
    )


def _finding(
    *,
    finding_id: str = "finding-1",
    outcome: PolicyEvaluationOutcome = PolicyEvaluationOutcome.FAIL,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    waiver_id: str | None = None,
) -> PolicyComplianceFinding:
    return PolicyComplianceFinding(
        finding_id=finding_id,
        receipt_id="receipt-1",
        subject=_subject(),
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id="rule-1",
        outcome=outcome,
        enforcement=enforcement,
        message="Coverage is below the adopted policy.",
        created_at=NOW,
        waiver_id=waiver_id,
    )


def test_contract_literal_and_closed_enum_values_are_frozen() -> None:
    assert GUIDELINE_DOMAIN_CONTRACT_VERSION == "guideline-domain/v1"
    assert {item.value for item in PolicyEntityType} == {
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "card",
        "test_scenario",
    }
    assert {item.value for item in GuidelineEnforcement} == {
        "advisory",
        "blocking",
    }
    assert {item.value for item in GuidelineRuleOperator} == {"all", "any"}
    assert {item.value for item in PolicyEvaluationOutcome} == {
        "pass",
        "fail",
        "not_applicable",
        "error",
    }
    assert {item.value for item in PolicyComplianceState} == {
        "ready",
        "blocked",
        "ready_with_waivers",
        "not_applicable",
    }
    assert {item.value for item in PolicyWaiverStatus} == {
        "requested",
        "approved",
        "rejected",
        "revoked",
        "expired",
    }
    assert {item.value for item in PolicyCurrentness} == {
        "current",
        "stale",
    }


def test_policy_subject_uses_the_canonical_opaque_entity_id_boundary() -> None:
    prefixed_board_id = "board-15877207-c147-4805-96d7-d53a625571df"
    assert len(prefixed_board_id) > 36
    assert (
        POLICY_BOARD_ID_MAX_LENGTH
        == POLICY_SUBJECT_ID_MAX_LENGTH
        == POLICY_ENTITY_ID_MAX_LENGTH
        == 255
    )

    subject = PolicySubjectRef(
        board_id=prefixed_board_id,
        entity_type=PolicyEntityType.TEST_SCENARIO,
        subject_id="test-scenario:" + ("s" * 100),
        subject_version=1,
    )

    assert subject.board_id == prefixed_board_id
    assert subject.subject_id.startswith("test-scenario:")

    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_subject_board_id_required",
    ):
        PolicySubjectRef(
            board_id="b" * (POLICY_BOARD_ID_MAX_LENGTH + 1),
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=1,
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_subject_id_required",
    ):
        PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="s" * (POLICY_SUBJECT_ID_MAX_LENGTH + 1),
            subject_version=1,
        )


def test_domain_module_is_standard_library_only_and_not_reexported_ambiguously() -> (
    None
):
    assert _import_roots(DOMAIN_PATH) <= {
        "__future__",
        "dataclasses",
        "datetime",
        "enum",
        "hashlib",
        "json",
        "math",
        "re",
        "typing",
        "unicodedata",
    }

    import okto_pulse.core.domain as domain_package

    assert not hasattr(domain_package, "Guideline")


def test_guideline_identity_separates_global_and_inline_scope() -> None:
    global_guideline = Guideline(
        guideline_id="guideline-1",
        owner_id="owner-1",
        scope=GuidelineScope.GLOBAL,
        created_at=NOW,
    )
    inline_guideline = Guideline(
        guideline_id="guideline-2",
        owner_id="owner-1",
        scope=GuidelineScope.INLINE,
        board_id="board-1",
        created_at=NOW,
    )

    assert global_guideline.id == "guideline-1"
    assert inline_guideline.board_id == "board-1"

    with pytest.raises(
        GuidelinePolicyContractError,
        match="inline_guideline_board_id_required",
    ):
        Guideline(
            guideline_id="guideline-3",
            owner_id="owner-1",
            scope=GuidelineScope.INLINE,
            created_at=NOW,
        )


def test_guideline_context_defaults_to_all_without_making_all_a_rule_target() -> None:
    guideline = Guideline(
        guideline_id="guideline-1",
        owner_id="owner-1",
        scope=GuidelineScope.GLOBAL,
        created_at=NOW,
    )

    assert guideline.context_scope is GuidelineContextScope.ALL
    assert "all" not in {item.value for item in PolicyEntityType}


def test_rule_requires_explicit_targets_defaults_advisory_and_deep_freezes() -> None:
    mutable_parameters = [["minimum", 0.95]]
    predicate = GuidelinePredicate("coverage.minimum", mutable_parameters)
    rule = _rule(predicates=[predicate])
    mutable_parameters[0][1] = 0

    assert rule.target_entity_types == (PolicyEntityType.SPEC,)
    assert rule.enforcement is GuidelineEnforcement.ADVISORY
    assert rule.operator is GuidelineRuleOperator.ALL
    assert rule.predicates[0].parameters == (("minimum", 0.95),)
    assert rule.applies_to(PolicyEntityType.SPEC)
    assert not rule.applies_to(PolicyEntityType.IDEATION)

    with pytest.raises(FrozenInstanceError):
        rule.code = "changed"  # type: ignore[misc]


def test_rule_rejects_missing_target_and_protected_waivable_class() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_rule_target_entity_types_required",
    ):
        _rule(target_entity_types=())

    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_rule_protected_class_must_be_non_waivable",
    ):
        _rule(policy_class="permissions", waivable=True)

    protected = _rule(policy_class="permissions", waivable=False)
    assert protected.waivable is False


def test_revision_is_immutable_versioned_and_rejects_invalid_lineage() -> None:
    mutable_rules = [_rule()]
    revision = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Engineering policy",
        content="Keep all acceptance criteria traceable.",
        content_digest=DIGEST_A.upper(),
        rules=mutable_rules,
        created_by="agent-1",
        created_at=NOW,
        tags=("security", "architecture"),
    )
    mutable_rules.clear()

    assert revision.rules == (_rule(),)
    assert revision.content_digest == DIGEST_A
    assert revision.tags == ("architecture", "security")

    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_tags_invalid",
    ):
        GuidelineRevision(
            revision_id="revision-tags-invalid",
            guideline_id="guideline-1",
            revision_number=1,
            semantic_version="1.0.0",
            title="Engineering policy",
            content="Changed.",
            content_digest=DIGEST_B,
            rules=(),
            created_by="agent-1",
            created_at=NOW,
            tags=("duplicate", "duplicate"),
        )

    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_parent_required",
    ):
        GuidelineRevision(
            revision_id="revision-2",
            guideline_id="guideline-1",
            revision_number=2,
            semantic_version="1.1.0",
            title="Engineering policy",
            content="Changed.",
            content_digest=DIGEST_B,
            rules=(),
            created_by="agent-1",
            created_at=NOW,
        )


@pytest.mark.parametrize(
    "semantic_version",
    (
        "1١.0.0",
        "١.0.0",
        f"{'9' * 129}.0.0",
        f"{'9' * 5000}.0.0",
        f"1.0.0-{'9' * 129}",
    ),
    ids=(
        "mixed-unicode-digit",
        "unicode-major",
        "oversized-major",
        "python-int-limit-major",
        "oversized-numeric-prerelease",
    ),
)
def test_revision_semver_rejects_unicode_and_oversized_numeric_identifiers(
    semantic_version: str,
) -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_semantic_version_invalid",
    ):
        GuidelineRevision(
            revision_id="revision-invalid-semver",
            guideline_id="guideline-1",
            revision_number=1,
            semantic_version=semantic_version,
            title="Engineering policy",
            content="Keep evidence current.",
            content_digest=DIGEST_A,
            rules=(_rule(),),
            created_by="agent-1",
            created_at=NOW,
        )


def test_head_binding_and_impact_are_exact_revision_evidence() -> None:
    head = GuidelineHead(
        guideline_id="guideline-1",
        revision_id="revision-2",
        revision_number=2,
        semantic_version="1.1.0",
        head_revision=2,
        updated_at=NOW,
    )
    binding = _binding(default_enforcement=GuidelineEnforcement.BLOCKING)
    revision_1 = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Policy",
        content="Initial.",
        content_digest=DIGEST_A,
        rules=(_rule(),),
        created_by="agent-1",
        created_at=NOW,
    )
    revision_2 = GuidelineRevision(
        revision_id="revision-2",
        guideline_id="guideline-1",
        revision_number=2,
        semantic_version="1.1.0",
        title="Policy",
        content="Changed.",
        content_digest=DIGEST_B,
        rules=(_rule(title="Changed coverage"),),
        created_by="agent-1",
        created_at=NOW + timedelta(minutes=1),
        parent_revision_id="revision-1",
    )
    impact = plan_guideline_impact_preview(
        GuidelineImpactPreviewCommand(
            impact_receipt_id="impact-1",
            board_id="board-1",
            guideline_id="guideline-1",
            head=head,
            to_revision=revision_2,
            current_binding=binding,
            from_revision=revision_1,
            active_bindings=(binding,),
            active_revisions=(revision_1,),
            subjects=(_subject(),),
            waivers=(),
            proposed_priority=binding.priority,
            proposed_default_enforcement=(GuidelineEnforcement.BLOCKING),
            requested_by="agent-1",
            created_at=NOW + timedelta(minutes=2),
            idempotency_key="impact:1",
        )
    ).receipt
    currentness = assess_guideline_impact_currentness(
        impact,
        impact_fence_from_receipt(impact),
    )

    assert head.head_revision == 2
    assert binding.revision_id == "revision-1"
    assert impact.requires_explicit_adoption is True
    assert currentness.currentness is PolicyCurrentness.CURRENT


def test_binding_configuration_change_declares_revision_targets() -> None:
    head = GuidelineHead(
        guideline_id="guideline-1",
        revision_id="revision-1",
        revision_number=1,
        semantic_version="1.0.0",
        head_revision=1,
        updated_at=NOW,
    )
    current = _binding(
        priority=10,
        default_enforcement=GuidelineEnforcement.ADVISORY,
    )
    revision = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Policy",
        content="Initial.",
        content_digest=DIGEST_A,
        rules=(_rule(),),
        created_by="agent-1",
        created_at=NOW,
    )

    receipt = plan_guideline_impact_preview(
        GuidelineImpactPreviewCommand(
            impact_receipt_id="impact-binding-configuration",
            board_id="board-1",
            guideline_id="guideline-1",
            head=head,
            to_revision=revision,
            current_binding=current,
            from_revision=revision,
            active_bindings=(current,),
            active_revisions=(revision,),
            subjects=(_subject(),),
            waivers=(),
            proposed_priority=20,
            proposed_default_enforcement=GuidelineEnforcement.BLOCKING,
            requested_by="agent-1",
            created_at=NOW + timedelta(minutes=1),
            idempotency_key="impact:binding-configuration",
        )
    ).receipt

    assert receipt.added_rule_ids == ()
    assert receipt.changed_rule_ids == ()
    assert receipt.removed_rule_ids == ()
    assert receipt.affected_entity_types == (PolicyEntityType.SPEC,)
    assert any(
        item.item_kind is GuidelineImpactItemKind.TARGET
        and item.entity_type == PolicyEntityType.SPEC.value
        for item in receipt.items
    )


def test_subject_snapshot_and_input_are_bound_to_board_version_and_digest() -> None:
    snapshot = PolicySubjectSnapshot(
        subject=_subject(),
        content_digest=DIGEST_A,
        captured_at=NOW,
        attributes=[("status", "review"), ("coverage", 1.0)],
    )
    evaluation_input = PolicyEvaluationInput(
        evaluation_id="evaluation-1",
        subject_snapshot=snapshot,
        bindings=[_binding()],
        input_digest=DIGEST_B,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        evaluator_version="guideline-evaluator/v1",
        requested_by="agent-1",
        requested_at=NOW,
        idempotency_key="evaluate:spec-1:v4",
    )

    assert evaluation_input.subject_snapshot.subject.subject_version == 4
    assert evaluation_input.bindings == (_binding(),)
    assert evaluation_input.policy_set_digest == "c" * 64
    assert evaluation_input.binding_head_digest == "d" * 64

    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_evaluation_binding_board_mismatch",
    ):
        PolicyEvaluationInput(
            evaluation_id="evaluation-2",
            subject_snapshot=snapshot,
            bindings=[_binding(board_id="board-2")],
            input_digest=DIGEST_B,
            policy_set_digest="c" * 64,
            binding_head_digest="d" * 64,
            catalog_version="guideline-predicate-catalog/v1",
            ruleset_version="guideline-ruleset/v1",
            evaluator_version="guideline-evaluator/v1",
            requested_by="agent-1",
            requested_at=NOW,
            idempotency_key="evaluate:spec-1:v4:other",
        )


def test_compliance_receipt_preserves_blocking_and_waived_states() -> None:
    blocking_finding = _finding()
    adopted = AdoptedGuidelineRevisionRef.from_binding(_binding())
    blocked_receipt = PolicyComplianceReceipt(
        receipt_id="receipt-1",
        subject=_subject(),
        subject_content_digest=DIGEST_A,
        input_digest=DIGEST_B,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        adopted_revisions=(adopted,),
        outcome=PolicyEvaluationOutcome.FAIL,
        state=PolicyComplianceState.BLOCKED,
        currentness=PolicyCurrentness.CURRENT,
        findings=(blocking_finding,),
        evaluator_version="guideline-evaluator/v1",
        evaluated_by="agent-1",
        evaluated_at=NOW,
        rule_results=(
            PolicyComplianceRuleResult(
                guideline_id=blocking_finding.guideline_id,
                revision_id=blocking_finding.revision_id,
                rule_id=blocking_finding.rule_id,
                outcome=blocking_finding.outcome,
                enforcement=blocking_finding.enforcement,
            ),
        ),
    )
    result = PolicyEvaluationResult(
        evaluation_id="evaluation-1",
        input_digest=DIGEST_B,
        receipt=blocked_receipt,
    )
    waived_finding = _finding(waiver_id="waiver-1")
    waived_receipt = PolicyComplianceReceipt(
        receipt_id="receipt-1",
        subject=_subject(),
        subject_content_digest=DIGEST_A,
        input_digest=DIGEST_B,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        adopted_revisions=(adopted,),
        outcome=PolicyEvaluationOutcome.FAIL,
        state=PolicyComplianceState.READY_WITH_WAIVERS,
        currentness=PolicyCurrentness.CURRENT,
        findings=(waived_finding,),
        evaluator_version="guideline-evaluator/v1",
        evaluated_by="agent-1",
        evaluated_at=NOW,
        rule_results=(
            PolicyComplianceRuleResult(
                guideline_id=waived_finding.guideline_id,
                revision_id=waived_finding.revision_id,
                rule_id=waived_finding.rule_id,
                outcome=waived_finding.outcome,
                enforcement=waived_finding.enforcement,
                waiver_id=waived_finding.waiver_id,
            ),
        ),
    )

    assert result.receipt.state is PolicyComplianceState.BLOCKED
    assert blocking_finding.blocking is True
    assert waived_receipt.findings[0].blocking is False


def test_waiver_requires_independent_review_and_auditable_lifecycle() -> None:
    requested = PolicyWaiver(
        waiver_id="waiver-1",
        board_id="board-1",
        finding_id="finding-1",
        receipt_id="receipt-1",
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id="rule-1",
        subject=_subject(),
        status=PolicyWaiverStatus.REQUESTED,
        justification="Temporary exception during governed migration.",
        evidence_refs=("ticket://migration-1",),
        requested_by="agent-1",
        requested_at=NOW,
        waiver_revision=1,
        expires_at=NOW + timedelta(days=7),
        last_event_id="waiver-event-1",
        last_event_type=PolicyWaiverEventType.REQUEST,
        last_event_at=NOW,
    )
    approved = PolicyWaiver(
        waiver_id="waiver-1",
        board_id="board-1",
        finding_id=requested.finding_id,
        receipt_id=requested.receipt_id,
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id="rule-1",
        subject=_subject(),
        status=PolicyWaiverStatus.APPROVED,
        justification=requested.justification,
        evidence_refs=requested.evidence_refs,
        requested_by="agent-1",
        requested_at=NOW,
        waiver_revision=2,
        expires_at=NOW + timedelta(days=7),
        reviewed_by="agent-2",
        reviewed_at=NOW + timedelta(minutes=5),
        review_reason="Bounded exception with an expiry.",
        last_event_id="waiver-event-2",
        last_event_type=PolicyWaiverEventType.APPROVE,
        last_event_at=NOW + timedelta(minutes=5),
    )
    revalidated = PolicyWaiver(
        waiver_id="waiver-1",
        board_id="board-1",
        finding_id=requested.finding_id,
        receipt_id=requested.receipt_id,
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id="rule-1",
        subject=_subject(),
        status=PolicyWaiverStatus.APPROVED,
        justification=requested.justification,
        evidence_refs=requested.evidence_refs,
        requested_by="agent-1",
        requested_at=NOW,
        waiver_revision=approved.waiver_revision + 1,
        expires_at=NOW + timedelta(days=14),
        reviewed_by="agent-3",
        reviewed_at=NOW + timedelta(days=6),
        review_reason="Revalidated against the unchanged exact scope.",
        last_event_id="waiver-event-3",
        last_event_type=PolicyWaiverEventType.REVALIDATE,
        last_event_at=NOW + timedelta(days=6),
    )

    assert approved.reviewed_by != approved.requested_by
    assert revalidated.status is PolicyWaiverStatus.APPROVED
    assert revalidated.waiver_revision == approved.waiver_revision + 1
    assert "revalidate" not in {item.value for item in PolicyWaiverStatus}

    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_waiver_independent_reviewer_required",
    ):
        PolicyWaiver(
            waiver_id="waiver-2",
            board_id="board-1",
            finding_id="finding-2",
            receipt_id="receipt-2",
            guideline_id="guideline-1",
            revision_id="revision-1",
            rule_id="rule-1",
            subject=_subject(),
            status=PolicyWaiverStatus.APPROVED,
            justification="Invalid self approval.",
            evidence_refs=("ticket://invalid",),
            requested_by="agent-1",
            requested_at=NOW,
            waiver_revision=2,
            expires_at=NOW + timedelta(days=1),
            last_event_id="waiver-event-invalid",
            last_event_type=PolicyWaiverEventType.APPROVE,
            last_event_at=NOW,
            reviewed_by="agent-1",
            reviewed_at=NOW,
            review_reason="Self approved.",
        )


def test_keyset_page_fails_closed_on_cursor_mismatch() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_page_cursor_mismatch",
    ):
        GuidelinePage(
            items=(),
            limit=50,
            next_cursor=None,
            has_more=True,
        )

    revision_cursor = GuidelineRevisionPageCursor(
        revision_number=2,
        item_id="revision-2",
        filter_digest="0" * 64,
        projection_digest="1" * 64,
    )
    page = GuidelineRevisionPage(
        items=(),
        limit=50,
        next_cursor=None,
        has_more=False,
    )
    assert revision_cursor.revision_number == 2
    assert page.ordering == (
        "revision_number DESC",
        "revision_id DESC",
    )
