from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentAuthoritySnapshot,
    AssessmentCommitResult,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    FindingAnchorType,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    canonical_sha256,
    clarification_digest_v1,
    policy_digest_v1,
    semantic_content_digest_v1,
)
from okto_pulse.core.domain.quality_taxonomy import (
    AMBIGUITY_TAXONOMY_CATEGORY_IDS,
    AMBIGUITY_TAXONOMY_DIGEST,
    AMBIGUITY_TAXONOMY_VERSION,
)
from okto_pulse.core.domain.requirement_lint import (
    REQUIREMENT_LINT_ANALYZER_VERSION,
    REQUIREMENT_LINT_RULESET_DIGEST,
    REQUIREMENT_LINT_RULESET_VERSION,
    RequirementLocale,
)
from okto_pulse.core.ports.quality_assessment import (
    AssessmentHeadRevisionConflict,
)
from okto_pulse.core.ports.requirement_lint import (
    RequirementLintWriteCommand,
    RequirementLintWriter,
    RequirementLintWriterContractError,
)
from okto_pulse.core.services.quality_assessment import (
    QualityAssessmentConflictError,
    QualityAssessmentService,
)
from okto_pulse.core.services.requirement_lint_assessment import (
    REQUIREMENT_LINT_AUTHORITY_VERSION,
    REQUIREMENT_LINT_CALCULATION_POLICY_VERSION,
    RequirementLintAssessmentInput,
    build_requirement_lint_assessment_bundle,
    commit_requirement_lint_assessment,
    requirement_lint_authority_snapshot_v1,
    requirement_lint_calculation_policy_manifest_v1,
    requirement_lint_normative_digests_v1,
)

NOW = datetime(2026, 7, 27, 15, 0, tzinfo=timezone.utc)
AUTHORITY_DIGEST = canonical_sha256("requirement-lint-authority")


class _Ids:
    def __init__(self) -> None:
        self._counter = 0

    def __call__(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter}"


def _service() -> QualityAssessmentService:
    return QualityAssessmentService(id_factory=_Ids(), clock=lambda: NOW)


def test_semantic_writer_authority_is_rederivable_and_identity_bound() -> None:
    channel = "semantic_writer:structured_crud"
    authority = requirement_lint_authority_snapshot_v1(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=7,
        actor_id="actor-1",
        channel=channel,
    )
    replay = requirement_lint_authority_snapshot_v1(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=7,
        actor_id="actor-1",
        channel=channel,
    )
    different_actor = requirement_lint_authority_snapshot_v1(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=7,
        actor_id="actor-2",
        channel=channel,
    )

    assert REQUIREMENT_LINT_AUTHORITY_VERSION.endswith("/v1")
    assert authority == replay
    assert authority.domain_write is True
    assert authority.quality_assess is False
    assert authority.authority_digest != different_actor.authority_digest

    with pytest.raises(RequirementLintWriterContractError) as exc_info:
        requirement_lint_authority_snapshot_v1(
            board_id="board-1",
            spec_id="spec-1",
            spec_version=7,
            actor_id="actor-1",
            channel="semantic_writer:unknown",
        )
    assert exc_info.value.args[0] == "requirement_lint_authority_channel_invalid"


def _payload() -> dict[str, Any]:
    return {
        "id": "spec-1",
        "board_id": "board-1",
        "version": 7,
        "title": "Requirement lint",
        "description": "Automatic advisory analysis.",
        "context": "SK-A",
        "functional_requirements": [
            {
                "id": "fr_pt",
                "text": "O fluxo deve ser fácil.",
                "status": "active",
                "locale": "pt",
            }
        ],
        "acceptance_criteria": [
            {
                "id": "ac_unknown",
                "text": "Given X When Y Then returns status within 100 ms.",
                "status": "active",
            },
            {
                "id": "ac_inactive",
                "text": "",
                "status": "deprecated",
                "locale": "en",
            },
        ],
        "technical_requirements": [
            {
                "id": "tr_missing",
                "text": "It must be robust.",
                "status": "active",
            }
        ],
        "test_scenarios": [],
        "business_rules": [],
        "api_contracts": [],
        "integration_requirements": [],
        "observability_requirements": [],
        "decisions": [],
    }


def _command(
    *,
    payload: Mapping[str, Any] | None = None,
    writer: RequirementLintWriter = RequirementLintWriter.BULK_UPDATE,
) -> RequirementLintWriteCommand:
    return RequirementLintWriteCommand(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=7,
        actor_id="agent-1",
        writer=writer,
        spec_status="in_progress",
        spec_archived=False,
        changed_fields=("functional_requirements",),
        spec_payload=payload or _payload(),
    )


def _authority() -> AssessmentAuthoritySnapshot:
    return AssessmentAuthoritySnapshot(
        domain_write=True,
        quality_assess=False,
        qa_ask=False,
        authority_digest=AUTHORITY_DIGEST,
    )


def _input(
    *,
    command: RequirementLintWriteCommand | None = None,
    default_locale: RequirementLocale = RequirementLocale.UNKNOWN,
    head_revision: int = 2,
    head_receipt_id: str | None = "qar_previous",
) -> RequirementLintAssessmentInput:
    return RequirementLintAssessmentInput(
        command=command or _command(),
        authority=_authority(),
        qa_items=(
            {
                "id": "qa_timeout",
                "revision": 2,
                "question": "Which timeout?",
                "answer": "100 ms",
                "answered_at": "2026-07-27T14:00:00Z",
            },
            {
                "id": "qa_unanswered",
                "revision": 1,
                "question": "Which fallback?",
                "answer": None,
            },
        ),
        default_locale=default_locale,
        current_head_revision=head_revision,
        current_head_receipt_id=head_receipt_id,
    )


def _primitive(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _primitive(getattr(value, item.name)) for item in fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _primitive(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_primitive(item) for item in value]
    if isinstance(value, frozenset | set):
        return sorted(_primitive(item) for item in value)
    return value


def test_builder_computes_all_normative_digests_and_versions() -> None:
    assessment_input = _input()
    bundle = build_requirement_lint_assessment_bundle(
        assessment_input,
        quality_service=_service(),
    )
    receipt = bundle.receipt
    policy_manifest = requirement_lint_calculation_policy_manifest_v1(
        RequirementLocale.UNKNOWN
    )

    assert receipt.digests.content_digest == semantic_content_digest_v1(
        "spec",
        assessment_input.command.spec_payload,
    )
    assert receipt.digests.clarification_digest == clarification_digest_v1(
        assessment_input.qa_items
    )
    assert receipt.digests.ruleset_digest == REQUIREMENT_LINT_RULESET_DIGEST
    assert receipt.digests.taxonomy_digest == AMBIGUITY_TAXONOMY_DIGEST
    assert receipt.digests.policy_digest == policy_digest_v1(
        REQUIREMENT_LINT_CALCULATION_POLICY_VERSION,
        policy_manifest,
    )
    assert receipt.digests == requirement_lint_normative_digests_v1(
        content_digest=receipt.digests.content_digest,
        clarification_digest=receipt.digests.clarification_digest,
        default_locale=RequirementLocale.UNKNOWN,
    )
    assert receipt.versions.ruleset_version == REQUIREMENT_LINT_RULESET_VERSION
    assert receipt.versions.taxonomy_version == AMBIGUITY_TAXONOMY_VERSION
    assert receipt.versions.analyzer_version == REQUIREMENT_LINT_ANALYZER_VERSION
    assert (
        receipt.versions.policy_version == REQUIREMENT_LINT_CALCULATION_POLICY_VERSION
    )
    assert policy_manifest["scope"] == "calculation_only"
    assert not {
        "actor_id",
        "authority",
        "head_revision",
        "spec_status",
        "writer",
    } & set(policy_manifest)


def test_builder_uses_child_locale_then_explicit_unknown_default() -> None:
    bundle = build_requirement_lint_assessment_bundle(
        _input(default_locale=RequirementLocale.UNKNOWN),
        quality_service=_service(),
    )
    findings_by_child: dict[str, list] = {}
    for finding in bundle.findings:
        findings_by_child.setdefault(finding.anchor.anchor_ref or "", []).append(
            finding
        )

    assert findings_by_child["fr_pt"][0].title == "Resultado funcional vago"
    assert "ac_unknown" not in findings_by_child
    assert "ac_inactive" not in findings_by_child
    assert [finding.rule_code for finding in findings_by_child["tr_missing"]] == [
        "tr_technical_restriction_missing"
    ]
    assert bundle.receipt.scale.maximum == 10.0


def test_taxonomy_is_the_shared_closed_set_of_ten_categories() -> None:
    bundle = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=_service(),
    )

    assert len(AMBIGUITY_TAXONOMY_CATEGORY_IDS) == 10
    assert len(set(AMBIGUITY_TAXONOMY_CATEGORY_IDS)) == 10
    assert {finding.category_code for finding in bundle.findings} <= set(
        AMBIGUITY_TAXONOMY_CATEGORY_IDS
    )
    assert all(
        finding.taxonomy_version == AMBIGUITY_TAXONOMY_VERSION
        for finding in bundle.findings
    )


def test_builder_carries_head_cas_writer_channel_origin_and_advisory_outcome() -> None:
    bundle = build_requirement_lint_assessment_bundle(
        _input(head_revision=4, head_receipt_id="qar_head_4"),
        quality_service=_service(),
    )

    assert bundle.expected_head_revision == 4
    assert bundle.expected_head_receipt_id == "qar_head_4"
    assert bundle.receipt.predecessor_receipt_id == "qar_head_4"
    assert bundle.next_head.revision == 5
    assert bundle.receipt.channel == "semantic_writer:bulk_update"
    assert bundle.receipt.origin is AssessmentOrigin.SEMANTIC_WRITER
    assert bundle.receipt.outcome is AssessmentOutcome.ADVISORY
    assert bundle.receipt.assessment_kind is AssessmentKind.REQUIREMENT_LINT
    assert bundle.expected_authority_digest == AUTHORITY_DIGEST

    with pytest.raises(
        RequirementLintWriterContractError,
        match="requirement_lint_head_identity_mismatch",
    ):
        _input(head_revision=0, head_receipt_id="qar_impossible")


def test_bundle_is_byte_identical_with_deterministic_ids_and_clock() -> None:
    first = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=_service(),
    )
    second = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=_service(),
    )

    assert first == second
    assert canonical_json_bytes(_primitive(first)) == canonical_json_bytes(
        _primitive(second)
    )
    assert first.idempotency_key == second.idempotency_key
    assert first.request_fingerprint == second.request_fingerprint


def test_bundle_materializes_proposed_questions_and_stable_child_anchors() -> None:
    payload = _payload()
    payload["acceptance_criteria"] = [
        {
            "id": f"ac_{suffix}",
            "text": "",
            "status": "active",
            "locale": "en",
        }
        for suffix in ("c", "a", "b")
    ]
    bundle = build_requirement_lint_assessment_bundle(
        _input(command=_command(payload=payload)),
        quality_service=_service(),
    )
    finding_ids = {finding.id for finding in bundle.findings}
    question_ids = {question.qa_id for question in bundle.proposed_questions}

    assert len(bundle.proposed_questions) == 5
    assert len(bundle.finding_qa_links) == 5
    assert {link.finding_id for link in bundle.finding_qa_links} <= finding_ids
    assert {link.qa_id for link in bundle.finding_qa_links} == question_ids
    assert all(
        finding.anchor.anchor_type is FindingAnchorType.STRUCTURED_CHILD
        and finding.anchor.anchor_ref
        and "[" not in finding.anchor.anchor_ref
        and finding.anchor.input_digest == bundle.expected_input_digest
        for finding in bundle.findings
    )
    assert [question.client_key for question in bundle.proposed_questions] == sorted(
        (question.client_key for question in bundle.proposed_questions),
        key=lambda key: next(
            (
                -{
                    "info": 0,
                    "low": 1,
                    "medium": 2,
                    "high": 3,
                    "critical": 4,
                }[finding.severity.value],
                -finding.confidence,
                finding.finding_key,
            )
            for finding in bundle.findings
            if f"question:{finding.finding_key}" == key
        ),
    )


class _Persistence:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.apply_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.bundles = []

    async def apply_bundle_cas(self, bundle):
        self.apply_calls += 1
        self.bundles.append(bundle)
        if self.error is not None:
            raise self.error
        return AssessmentCommitResult(
            board_id=bundle.receipt.subject.board_id,
            subject_type=bundle.receipt.subject.subject_type,
            subject_id=bundle.receipt.subject.subject_id,
            subject_version=bundle.receipt.subject.subject_version,
            assessment_kind=bundle.receipt.assessment_kind,
            request_fingerprint=bundle.request_fingerprint,
            receipt_id=bundle.receipt.id,
            head_revision=bundle.next_head.revision,
            event_id=bundle.audit_intent.event_id,
            history_id=bundle.audit_intent.history_id,
            outbox_id=bundle.audit_intent.outbox_id,
            qa_id_map=tuple(
                (question.client_key, question.qa_id)
                for question in bundle.proposed_questions
            ),
        )

    async def commit(self) -> None:
        self.commit_calls += 1
        raise AssertionError("helper_must_not_commit")

    async def rollback(self) -> None:
        self.rollback_calls += 1
        raise AssertionError("helper_must_not_rollback")


@pytest.mark.asyncio
async def test_commit_helper_calls_cas_once_and_never_owns_the_uow() -> None:
    service = _service()
    bundle = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=service,
    )
    persistence = _Persistence()

    result = await commit_requirement_lint_assessment(
        bundle,
        persistence=persistence,
        quality_service=service,
    )

    assert persistence.apply_calls == 1
    assert persistence.bundles == [bundle]
    assert persistence.commit_calls == 0
    assert persistence.rollback_calls == 0
    assert result.receipt_id == bundle.receipt.id
    assert result.head_revision == bundle.next_head.revision
    assert result.evaluated_rule_count == int(bundle.receipt.scale.maximum)
    assert result.finding_count == len(bundle.findings)
    assert result.replayed is False


@pytest.mark.asyncio
async def test_commit_helper_propagates_head_cas_failure() -> None:
    service = _service()
    bundle = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=service,
    )
    persistence = _Persistence(error=AssessmentHeadRevisionConflict())

    with pytest.raises(QualityAssessmentConflictError) as exc_info:
        await commit_requirement_lint_assessment(
            bundle,
            persistence=persistence,
            quality_service=service,
        )

    assert exc_info.value.code == "assessment_head_revision_conflict"
    assert persistence.apply_calls == 1
    assert persistence.commit_calls == 0
    assert persistence.rollback_calls == 0


@pytest.mark.asyncio
async def test_unclassified_persistence_failure_propagates_for_facade_normalization() -> (
    None
):
    service = _service()
    bundle = build_requirement_lint_assessment_bundle(
        _input(),
        quality_service=service,
    )
    failure = RuntimeError("storage unavailable")
    persistence = _Persistence(error=failure)

    with pytest.raises(RuntimeError) as exc_info:
        await commit_requirement_lint_assessment(
            bundle,
            persistence=persistence,
            quality_service=service,
        )

    assert exc_info.value is failure
    assert persistence.apply_calls == 1
    assert persistence.commit_calls == 0
    assert persistence.rollback_calls == 0

def test_builder_never_rematerializes_an_existing_question() -> None:
    """Regression: every semantic write re-issues the lint receipt; without
    text dedup each write materialized up to five byte-identical Q&A items
    (observed live: 300 duplicates of 19 findings on one spec)."""

    first = build_requirement_lint_assessment_bundle(_input())
    assert first.proposed_questions, (
        "fixture must propose at least one question"
    )
    first_texts = {
        question.question for question in first.proposed_questions
    }

    already_asked = tuple(
        {
            "id": f"qa_lint_{index}",
            "revision": 1,
            "question": text,
            "answer": None,
        }
        for index, text in enumerate(sorted(first_texts))
    )
    second = build_requirement_lint_assessment_bundle(
        RequirementLintAssessmentInput(
            command=_command(),
            authority=_authority(),
            qa_items=already_asked,
            default_locale=RequirementLocale.UNKNOWN,
            current_head_revision=3,
            current_head_receipt_id="qar_previous_2",
        )
    )

    assert second.proposed_questions == ()
    # Findings remain first-class on every receipt; only the HITL question
    # materialization is deduplicated.
    assert len(second.findings) == len(first.findings) > 0

    # Whitespace/case variants of an existing question do not slip through.
    noisy = tuple(
        {
            "id": f"qa_noise_{index}",
            "revision": 1,
            "question": f"  {text.upper()}  ",
            "answer": None,
        }
        for index, text in enumerate(sorted(first_texts))
    )
    third = build_requirement_lint_assessment_bundle(
        RequirementLintAssessmentInput(
            command=_command(),
            authority=_authority(),
            qa_items=noisy,
            default_locale=RequirementLocale.UNKNOWN,
            current_head_revision=3,
            current_head_receipt_id="qar_previous_2",
        )
    )
    assert third.proposed_questions == ()
