"""Pure D0 builder for automatic Spec requirement-lint assessments.

Preparation is deliberately separated from persistence.  The builder computes
the complete immutable assessment bundle without opening, committing, or
rolling back a unit of work.  The async helper delegates the already-prepared
bundle to the transaction-bound quality persistence port exactly once.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from okto_pulse.core.domain.quality_assessment import (
    MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
    AssessmentAnchorCatalog,
    AssessmentAuthoritySnapshot,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentPreflight,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentSubmission,
    AssessmentVersionSet,
    AssessmentWriteBundle,
)
from okto_pulse.core.domain.quality_canonicalization import (
    SEMANTIC_FIELD_MANIFEST_V1,
    authority_digest_v1,
    canonical_sha256,
    clarification_digest_v1,
    policy_digest_v1,
    semantic_content_digest_v1,
)
from okto_pulse.core.domain.quality_taxonomy import (
    AMBIGUITY_TAXONOMY_CATEGORY_ID_SET,
    AMBIGUITY_TAXONOMY_DIGEST,
    AMBIGUITY_TAXONOMY_VERSION,
)
from okto_pulse.core.domain.requirement_lint import (
    REQUIREMENT_LINT_ANALYZER_VERSION,
    REQUIREMENT_LINT_RULESET_DIGEST,
    REQUIREMENT_LINT_RULESET_VERSION,
    RequirementLintContext,
    RequirementLocale,
    lint_requirements,
    requirement_lint_children_from_spec_payload,
)
from okto_pulse.core.ports.quality_assessment import (
    QualityAssessmentPersistencePort,
)
from okto_pulse.core.ports.requirement_lint import (
    RequirementLintWriteCommand,
    RequirementLintWriteResult,
    RequirementLintWriter,
    RequirementLintWriterContractError,
)
from okto_pulse.core.services.quality_assessment import (
    QualityAssessmentService,
)

REQUIREMENT_LINT_CALCULATION_POLICY_VERSION: Final = (
    "requirement-lint-calculation-policy/v1"
)
REQUIREMENT_LINT_AUTHORITY_VERSION: Final = (
    "requirement-lint-semantic-writer-authority/v1"
)
REQUIREMENT_LINT_CHANNEL_PREFIX: Final = "semantic_writer"


@dataclass(frozen=True, slots=True)
class RequirementLintAssessmentInput:
    """Edition-neutral facts needed to prepare one automatic lint bundle."""

    command: RequirementLintWriteCommand
    authority: AssessmentAuthoritySnapshot
    qa_items: tuple[Mapping[str, Any], ...] = ()
    default_locale: RequirementLocale = RequirementLocale.UNKNOWN
    current_head_revision: int = 0
    current_head_receipt_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.command, RequirementLintWriteCommand):
            raise RequirementLintWriterContractError(
                "requirement_lint_write_command_invalid"
            )
        if not isinstance(self.authority, AssessmentAuthoritySnapshot):
            raise RequirementLintWriterContractError(
                "requirement_lint_authority_snapshot_invalid"
            )
        if not isinstance(self.default_locale, RequirementLocale):
            raise RequirementLintWriterContractError(
                "requirement_lint_default_locale_invalid"
            )
        if not isinstance(self.qa_items, Sequence) or isinstance(
            self.qa_items,
            str | bytes | bytearray,
        ):
            raise RequirementLintWriterContractError(
                "requirement_lint_qa_items_invalid"
            )
        qa_items: list[Mapping[str, Any]] = []
        for item in self.qa_items:
            if not isinstance(item, Mapping):
                raise RequirementLintWriterContractError(
                    "requirement_lint_qa_items_invalid"
                )
            qa_items.append(copy.deepcopy(dict(item)))
        object.__setattr__(self, "qa_items", tuple(qa_items))

        revision = self.current_head_revision
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise RequirementLintWriterContractError(
                "requirement_lint_head_revision_invalid"
            )
        receipt_id = self.current_head_receipt_id
        if receipt_id is not None:
            if not isinstance(receipt_id, str) or not receipt_id.strip():
                raise RequirementLintWriterContractError(
                    "requirement_lint_head_receipt_id_invalid"
                )
            receipt_id = receipt_id.strip()
            object.__setattr__(self, "current_head_receipt_id", receipt_id)
        if (revision == 0) != (receipt_id is None):
            raise RequirementLintWriterContractError(
                "requirement_lint_head_identity_mismatch"
            )


def requirement_lint_calculation_policy_manifest_v1(
    default_locale: RequirementLocale,
) -> Mapping[str, object]:
    """Return only deterministic calculation choices, never authority/gates."""

    if not isinstance(default_locale, RequirementLocale):
        raise RequirementLintWriterContractError(
            "requirement_lint_default_locale_invalid"
        )
    return MappingProxyType(
        {
            "scope": "calculation_only",
            "default_locale": default_locale.value,
            "locale_resolution": "explicit_child_then_default",
            "unknown_locale_profile": "neutral_only",
            "included_child_statuses": ("active",),
            "score": MappingProxyType(
                {
                    "kind": "finding_count",
                    "minimum": 0,
                    "maximum": "evaluated_rule_count",
                    "direction": "lower_better",
                    "empty_input_evaluated_rule_count": 1,
                }
            ),
            "question_selection": MappingProxyType(
                {
                    "maximum": MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
                    "ordering": (
                        "severity_desc",
                        "confidence_desc",
                        "finding_key_asc",
                    ),
                }
            ),
            "findings": MappingProxyType(
                {
                    "deterministic": True,
                    "blocking_eligible": False,
                }
            ),
        }
    )


def requirement_lint_authority_snapshot_v1(
    *,
    board_id: str,
    spec_id: str,
    spec_version: int,
    actor_id: str,
    channel: str,
) -> AssessmentAuthoritySnapshot:
    """Derive the narrow authority inherited from an authorized Spec writer.

    Requirement lint has no independent mutation permission.  Its authority is
    the already-authorized semantic Spec write that invokes the hook inside the
    same transaction.  The digest binds that fact to the exact subject version,
    actor and canonical writer channel so the Community adapter can rederive it
    immediately before the CAS without trusting a caller-supplied hash.
    """

    allowed_channels = frozenset(
        f"{REQUIREMENT_LINT_CHANNEL_PREFIX}:{writer.value}"
        for writer in RequirementLintWriter
    )
    normalized = {
        "board_id": board_id,
        "spec_id": spec_id,
        "spec_version": spec_version,
        "actor_id": actor_id,
        "channel": channel,
    }
    if any(
        not isinstance(normalized[field], str)
        or not str(normalized[field]).strip()
        for field in ("board_id", "spec_id", "actor_id", "channel")
    ):
        raise RequirementLintWriterContractError(
            "requirement_lint_authority_identity_invalid"
        )
    if (
        not isinstance(spec_version, int)
        or isinstance(spec_version, bool)
        or spec_version < 1
    ):
        raise RequirementLintWriterContractError(
            "requirement_lint_authority_spec_version_invalid"
        )
    if channel not in allowed_channels:
        raise RequirementLintWriterContractError(
            "requirement_lint_authority_channel_invalid"
        )
    manifest = {
        "authority_kind": "authorized_semantic_spec_writer",
        "board_id": board_id.strip(),
        "spec_id": spec_id.strip(),
        "spec_version": spec_version,
        "actor_id": actor_id.strip(),
        "channel": channel,
        "domain_write": True,
        "quality_assess": False,
        "qa_ask": False,
        "spec_validation_submit": False,
        "reviewer_separation_satisfied": True,
    }
    return AssessmentAuthoritySnapshot(
        domain_write=True,
        quality_assess=False,
        qa_ask=False,
        spec_validation_submit=False,
        reviewer_separation_satisfied=True,
        authority_digest=authority_digest_v1(
            REQUIREMENT_LINT_AUTHORITY_VERSION,
            manifest,
        ),
    )


def requirement_lint_normative_digests_v1(
    *,
    content_digest: str,
    clarification_digest: str,
    default_locale: RequirementLocale,
) -> AssessmentDigestSet:
    """Rederive the complete normative digest set for the A1a v1 runtime."""

    policy_manifest = requirement_lint_calculation_policy_manifest_v1(
        default_locale
    )
    return AssessmentDigestSet(
        content_digest=content_digest,
        clarification_digest=clarification_digest,
        ruleset_digest=REQUIREMENT_LINT_RULESET_DIGEST,
        taxonomy_digest=AMBIGUITY_TAXONOMY_DIGEST,
        policy_digest=policy_digest_v1(
            REQUIREMENT_LINT_CALCULATION_POLICY_VERSION,
            policy_manifest,
        ),
    )


def _qa_anchor_ids(qa_items: Sequence[Mapping[str, Any]]) -> frozenset[str]:
    # ``clarification_digest_v1`` runs first and validates the complete shape.
    return frozenset(
        str(item.get("id", item.get("qa_id"))).strip() for item in qa_items
    )


def _normalized_question_text(value: object) -> str:
    return " ".join(str(value or "").split()).casefold()


def _dedupe_proposed_questions(
    proposed: Sequence[Any],
    qa_items: Sequence[Mapping[str, Any]],
) -> tuple[Any, ...]:
    """Never re-materialize a lint question that already exists on the Spec.

    Every semantic write re-issues the advisory lint receipt. Without this
    filter each write materialized up to five NEW Q&A items whose generated
    text was byte-identical to questions from earlier writes, flooding the
    Spec board with duplicates (observed: 300 copies of 19 findings). The
    findings themselves remain first-class on every receipt; only the HITL
    question is deduplicated, by normalized question text, against the
    Spec's existing Q&A — answered or not.
    """

    existing = {
        _normalized_question_text(item.get("question"))
        for item in qa_items
    }
    existing.discard("")
    return tuple(
        question
        for question in proposed
        if _normalized_question_text(question.question) not in existing
    )


def requirement_lint_natural_idempotency_key(
    command: RequirementLintWriteCommand,
    *,
    input_digest: str,
) -> str:
    """Return the stable run identity used for lookup before head preflight."""

    if not isinstance(command, RequirementLintWriteCommand):
        raise RequirementLintWriterContractError(
            "requirement_lint_write_command_invalid"
        )
    if not isinstance(input_digest, str) or not input_digest.strip():
        raise RequirementLintWriterContractError(
            "requirement_lint_input_digest_invalid"
        )
    identity_digest = canonical_sha256(
        {
            "contract": "requirement-lint-natural-idempotency/v1",
            "board_id": command.board_id,
            "spec_id": command.spec_id,
            "spec_version": command.spec_version,
            "assessment_kind": AssessmentKind.REQUIREMENT_LINT,
            "ruleset_version": REQUIREMENT_LINT_RULESET_VERSION,
            "input_digest": input_digest,
        }
    )
    return f"requirement-lint:{identity_digest}"


def build_requirement_lint_assessment_bundle(
    assessment_input: RequirementLintAssessmentInput,
    *,
    quality_service: QualityAssessmentService | None = None,
) -> AssessmentWriteBundle:
    """Build one complete advisory D0 bundle without performing any I/O."""

    if not isinstance(assessment_input, RequirementLintAssessmentInput):
        raise RequirementLintWriterContractError(
            "requirement_lint_assessment_input_invalid"
        )
    command = assessment_input.command

    content_digest = semantic_content_digest_v1(
        AssessmentSubjectType.SPEC,
        command.spec_payload,
    )
    clarification_digest = clarification_digest_v1(assessment_input.qa_items)
    digests = requirement_lint_normative_digests_v1(
        content_digest=content_digest,
        clarification_digest=clarification_digest,
        default_locale=assessment_input.default_locale,
    )
    versions = AssessmentVersionSet(
        ruleset_version=REQUIREMENT_LINT_RULESET_VERSION,
        taxonomy_version=AMBIGUITY_TAXONOMY_VERSION,
        analyzer_version=REQUIREMENT_LINT_ANALYZER_VERSION,
        policy_version=REQUIREMENT_LINT_CALCULATION_POLICY_VERSION,
    )

    children = requirement_lint_children_from_spec_payload(command.spec_payload)
    analysis = lint_requirements(
        children,
        context=RequirementLintContext(
            board_id=command.board_id,
            spec_id=command.spec_id,
            spec_version=command.spec_version,
            input_digest=digests.input_digest or "",
            locale=assessment_input.default_locale,
        ),
    )
    subject = AssessmentSubjectRef(
        board_id=command.board_id,
        subject_type=AssessmentSubjectType.SPEC,
        subject_id=command.spec_id,
        subject_version=command.spec_version,
    )
    preflight = AssessmentPreflight(
        subject=subject,
        status=command.spec_status,
        current_head_revision=assessment_input.current_head_revision,
        current_head_receipt_id=assessment_input.current_head_receipt_id,
        channel=f"{REQUIREMENT_LINT_CHANNEL_PREFIX}:{command.writer.value}",
        expected_scale=analysis.scale,
        digests=digests,
        versions=versions,
        anchors=AssessmentAnchorCatalog(
            fields=frozenset(SEMANTIC_FIELD_MANIFEST_V1["spec"]),
            structured_child_ids=frozenset(child.child_id for child in children),
            qa_ids=_qa_anchor_ids(assessment_input.qa_items),
        ),
        allowed_category_codes=AMBIGUITY_TAXONOMY_CATEGORY_ID_SET,
        authority=assessment_input.authority,
        origin=AssessmentOrigin.SEMANTIC_WRITER,
        subject_archived=command.spec_archived,
    )
    submission = AssessmentSubmission(
        board_id=command.board_id,
        subject_type=AssessmentSubjectType.SPEC,
        subject_id=command.spec_id,
        assessment_kind=AssessmentKind.REQUIREMENT_LINT,
        idempotency_key=requirement_lint_natural_idempotency_key(
            command,
            input_digest=digests.input_digest or "",
        ),
        expected_subject_version=command.spec_version,
        expected_head_revision=assessment_input.current_head_revision,
        score=analysis.score,
        justification=(
            "Automatic deterministic requirement lint (advisory): "
            f"{len(analysis.findings)} finding(s) across "
            f"{analysis.evaluated_rule_count} evaluated rule(s)."
        ),
        scale=analysis.scale,
        findings=analysis.findings,
        proposed_questions=_dedupe_proposed_questions(
            analysis.proposed_questions,
            assessment_input.qa_items,
        ),
    )
    service = quality_service or QualityAssessmentService()
    return service.prepare_submission(
        submission,
        actor_id=command.actor_id,
        preflight=preflight,
    )


async def commit_requirement_lint_assessment(
    bundle: AssessmentWriteBundle,
    *,
    persistence: QualityAssessmentPersistencePort,
    quality_service: QualityAssessmentService | None = None,
) -> RequirementLintWriteResult:
    """Apply one prepared bundle; transaction ownership remains with caller."""

    if (
        not isinstance(bundle, AssessmentWriteBundle)
        or bundle.receipt.assessment_kind is not AssessmentKind.REQUIREMENT_LINT
    ):
        raise RequirementLintWriterContractError("requirement_lint_bundle_invalid")
    service = quality_service or QualityAssessmentService()
    result = await service.commit_prepared(
        bundle,
        persistence=persistence,
    )
    evaluated_rule_count = bundle.receipt.scale.maximum
    if not float(evaluated_rule_count).is_integer() or evaluated_rule_count < 1:
        raise RequirementLintWriterContractError(
            "requirement_lint_evaluated_rule_count_invalid"
        )
    return RequirementLintWriteResult(
        receipt_id=result.receipt_id,
        head_revision=result.head_revision,
        evaluated_rule_count=int(evaluated_rule_count),
        finding_count=len(bundle.findings),
        replayed=result.replayed,
    )


__all__ = [
    "REQUIREMENT_LINT_AUTHORITY_VERSION",
    "REQUIREMENT_LINT_CALCULATION_POLICY_VERSION",
    "REQUIREMENT_LINT_CHANNEL_PREFIX",
    "RequirementLintAssessmentInput",
    "build_requirement_lint_assessment_bundle",
    "commit_requirement_lint_assessment",
    "requirement_lint_authority_snapshot_v1",
    "requirement_lint_calculation_policy_manifest_v1",
    "requirement_lint_natural_idempotency_key",
    "requirement_lint_normative_digests_v1",
]
