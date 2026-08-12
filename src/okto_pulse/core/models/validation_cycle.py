"""JSON projections for validation-cycle reads."""

from __future__ import annotations

from typing import Any
from collections.abc import Mapping

from okto_pulse.core.domain.validation_cycle import (
    RequirementLintPreflight,
    ValidationCycleCheckSummary,
    ValidationCycleResultSummary,
    ValidationCycleResultType,
    ValidationCycleSummary,
    ValidationSubmissionFence,
    ValidationTechnicalAudit,
)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def project_validation_submission_fence(
    value: ValidationSubmissionFence,
) -> dict[str, int]:
    return {
        "expected_validation_edition": value.expected_validation_edition,
        "expected_subject_version": value.expected_subject_version,
        "expected_head_revision": value.expected_head_revision,
    }


def project_validation_result_summary(
    value: ValidationCycleResultSummary,
) -> dict[str, Any]:
    return {
        "result_id": value.result_id,
        "result_type": value.result_type.value,
        "subject_edition": value.subject_edition,
        "status": value.status,
        "summary": _thaw_json(value.summary),
    }


def project_validation_check_summary(
    value: ValidationCycleCheckSummary,
) -> dict[str, Any]:
    return {
        "result_type": value.result_type.value,
        "status": value.status,
        "summary": value.summary,
    }


def project_validation_cycle(value: ValidationCycleSummary) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "subject_type": value.subject_type.value,
        "subject_id": value.subject_id,
        "edition": value.edition,
        "subject_status": value.status,
        "visible_sections": [item.value for item in value.visible_sections],
    }
    primary_result_type = (
        ValidationCycleResultType.SPEC_VALIDATION
        if value.subject_type.value == "spec"
        else ValidationCycleResultType.AMBIGUITY_ASSESSMENT
    )
    if primary_result_type in value.visible_sections:
        # Guaranteed by the domain visibility invariant.
        assert value.cycle_state is not None
        assert value.previous_result_count is not None
        assert value.submission_fence is not None
        payload.update(
            {
                "cycle_state": value.cycle_state.value,
                "current_result": (
                    project_validation_result_summary(value.current_result)
                    if value.current_result is not None
                    else None
                ),
                "previous_result_count": value.previous_result_count,
                "previous_results": [
                    project_validation_result_summary(item)
                    for item in value.previous_results
                ],
                "submission_fence": project_validation_submission_fence(
                    value.submission_fence
                ),
            }
        )
    if value.subject_type.value == "spec":
        payload["checks"] = [
            project_validation_check_summary(item) for item in value.checks
        ]
        payload["remaining_actions"] = list(value.remaining_actions)
    return payload


def project_validation_technical_audit(
    value: ValidationTechnicalAudit,
) -> dict[str, Any]:
    audit = value.technical_audit
    return {
        "subject_type": value.subject_type.value,
        "subject_id": value.subject_id,
        "result_id": value.result_id,
        "result_type": value.result_type.value,
        "subject_edition": value.subject_edition,
        "technical_audit": {
            "receipt_id": audit.receipt_id,
            "subject_version": audit.subject_version,
            "head_revision": audit.head_revision,
            "digests": dict(audit.digests),
            "visible_exception_types": [
                item.value for item in audit.visible_exception_types
            ],
            "exceptions": [
                {
                    "exception_id": item.exception_id,
                    "exception_type": item.exception_type.value,
                    "subject_edition": item.subject_edition,
                    "status": item.status,
                    "reason": item.reason,
                    "actor_id": item.actor_id,
                    "recorded_at": item.recorded_at.isoformat(),
                }
                for item in audit.exceptions
            ],
        },
    }


def project_requirement_lint_preflight(
    value: RequirementLintPreflight,
) -> dict[str, Any]:
    fence = value.submission_fence
    return {
        "assessment_kind": value.assessment_kind,
        "subject_edition": value.subject_edition,
        "subject_status": value.subject_status,
        "ruleset_digest": value.ruleset_digest,
        "requirement_anchors": [
            {
                "anchor_type": item.anchor_type,
                "anchor_ref": item.anchor_ref,
                "excerpt_hash": item.excerpt_hash,
            }
            for item in value.requirement_anchors
        ],
        "submission_fence": {
            "expected_subject_edition": fence.expected_validation_edition,
            "expected_subject_version": fence.expected_subject_version,
            "expected_head_revision": fence.expected_head_revision,
        },
    }


__all__ = [
    "project_requirement_lint_preflight",
    "project_validation_check_summary",
    "project_validation_cycle",
    "project_validation_result_summary",
    "project_validation_submission_fence",
    "project_validation_technical_audit",
]
