"""B13 closed error-envelope tests for guideline-policy REST/MCP parity."""

from __future__ import annotations

import pytest

from okto_pulse.core.application.use_cases.base import (
    CommandValidationError,
    ConflictError,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    GuidelineRevisionUnderBump,
)
from okto_pulse.core.domain.guideline_impact import GuidelineImpactError
from okto_pulse.core.domain.guideline_lifecycle import GuidelineVersionBump
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineImportExportError,
)
from okto_pulse.core.domain.guideline_policy import GuidelinePolicyContractError
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentInadmissibleError,
    SemanticAssessmentInadmissibilityCause,
)
from okto_pulse.core.inbound.guideline_policy_cursor import (
    GuidelinePolicyCursorConfigurationError,
)
from okto_pulse.core.inbound.guideline_policy_error import (
    UnsupportedGuidelinePolicyError,
    guideline_policy_http_status,
    project_guideline_policy_error,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyAdapterMissing,
    GuidelinePolicyBindingConflict,
    GuidelinePolicyCasConflict,
    GuidelinePolicyDigestConflict,
    GuidelinePolicyHeadConflict,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyInvalidCursor,
    GuidelinePolicyPersistenceError,
    GuidelinePolicyRevisionConflict,
    GuidelinePolicySubjectConflict,
    GuidelinePolicyVersionConflict,
)


ENVELOPE_KEYS = {
    "outcome",
    "error",
    "code",
    "error_code",
    "message",
    "category",
    "status_category",
    "http_status",
    "retryable",
    "next_action",
    "details",
}


@pytest.mark.parametrize(
    ("error", "reason_code"),
    [
        (
            GuidelinePolicyContractError("guideline_semver_invalid"),
            "guideline_semver_invalid",
        ),
        (
            GuidelineImportExportError(
                "guideline_import_envelope_invalid",
                path="$.items[0]",
            ),
            "guideline_import_envelope_invalid",
        ),
        (CommandValidationError("unsafe free-form input"), None),
        (ValueError("guideline_projection_invalid"), "guideline_projection_invalid"),
    ],
)
def test_validation_and_import_errors_map_to_closed_http_400(
    error: Exception,
    reason_code: str | None,
) -> None:
    projected = project_guideline_policy_error(error)

    assert set(projected) == ENVELOPE_KEYS
    assert projected["code"] == "validation_failed"
    assert projected["http_status"] == 400
    assert projected["status_category"] == "invalid_argument"
    assert projected["retryable"] is False
    assert projected["next_action"] == "fix_input"
    assert projected["details"].get("reason_code") == reason_code
    assert guideline_policy_http_status(error) == 400


def test_invalid_cursor_has_specific_code_but_remains_http_400() -> None:
    projected = project_guideline_policy_error(
        GuidelinePolicyInvalidCursor("guideline_revision_cursor_context_mismatch")
    )

    assert projected["code"] == "invalid_cursor"
    assert projected["http_status"] == 400
    assert projected["status_category"] == "invalid_argument"
    assert projected["retryable"] is False
    assert projected["next_action"] == "restart_pagination"
    assert projected["details"] == {"reason_code": "invalid_cursor"}


def test_under_bump_has_closed_http_400_diagnostics_without_raw_text() -> None:
    error = GuidelineRevisionUnderBump(
        minimum_bump=GuidelineVersionBump.MINOR,
        minimum_semantic_version="2.1.0",
        declared_semantic_version="2.0.1",
    )

    projected = project_guideline_policy_error(error)

    assert set(projected) == ENVELOPE_KEYS
    assert projected["error"] == "under_bump"
    assert projected["code"] == "under_bump"
    assert projected["error_code"] == "under_bump"
    assert projected["http_status"] == 400
    assert projected["retryable"] is False
    assert projected["next_action"] == "increase_semantic_version"
    assert projected["details"] == {
        "minimum_bump": "minor",
        "minimum_semantic_version": "2.1.0",
        "declared_semantic_version": "2.0.1",
    }
    assert "guideline_semver_below_minimum" not in repr(projected)


def test_domain_invalid_cursor_is_projected_identically() -> None:
    projected = project_guideline_policy_error(
        GuidelinePolicyContractError("invalid_cursor")
    )

    assert projected["code"] == "invalid_cursor"
    assert projected["http_status"] == 400
    assert projected["details"] == {"reason_code": "invalid_cursor"}


@pytest.mark.parametrize(
    ("cause", "next_action"),
    (
        (
            SemanticAssessmentInadmissibilityCause.ASSESSOR_SEPARATION_REQUIRED,
            "request_independent_assessor",
        ),
        (
            SemanticAssessmentInadmissibilityCause.CONFIDENCE_BELOW_MINIMUM,
            "reassess_with_sufficient_confidence",
        ),
    ),
)
def test_assessment_inadmissibility_exposes_only_closed_remediation(
    cause: SemanticAssessmentInadmissibilityCause,
    next_action: str,
) -> None:
    projected = project_guideline_policy_error(
        SemanticAssessmentInadmissibleError(cause)
    )

    assert projected["code"] == "policy_assessment_inadmissible"
    assert projected["next_action"] == next_action
    assert projected["retryable"] is False
    assert projected["details"] == {
        "reason_code": "policy_assessment_inadmissible",
        "inadmissibility_cause": cause.value,
    }
    assert "agent_id" not in repr(projected)


def test_unchanged_impact_has_specific_non_retryable_projection() -> None:
    projected = project_guideline_policy_error(
        GuidelineImpactError("guideline_impact_no_changes")
    )

    assert projected["code"] == "guideline_impact_no_changes"
    assert projected["http_status"] == 400
    assert projected["status_category"] == "invalid_argument"
    assert projected["retryable"] is False
    assert projected["next_action"] == "no_action_required"
    assert projected["details"] == {
        "reason_code": "guideline_impact_no_changes"
    }


def test_permission_error_maps_to_403_without_leaking_raw_message() -> None:
    secret = "token=SECRET-DO-NOT-LEAK"
    projected = project_guideline_policy_error(PermissionDeniedError(secret))

    assert projected["code"] == "permission_denied"
    assert projected["http_status"] == 403
    assert projected["status_category"] == "permission_denied"
    assert projected["retryable"] is False
    assert secret not in repr(projected)
    assert guideline_policy_http_status(PermissionDeniedError(secret)) == 403


def test_not_found_maps_to_404_without_leaking_resource_identifier() -> None:
    secret_id = "guideline-secret-identifier"
    projected = project_guideline_policy_error(
        EntityNotFoundError("guideline", secret_id)
    )

    assert projected["code"] == "not_found"
    assert projected["http_status"] == 404
    assert projected["details"] == {
        "reason_code": "entity_not_found",
        "entity_type": "guideline",
    }
    assert secret_id not in repr(projected)


@pytest.mark.parametrize(
    "error_type",
    [
        GuidelinePolicyCasConflict,
        GuidelinePolicyHeadConflict,
        GuidelinePolicyVersionConflict,
        GuidelinePolicyDigestConflict,
        GuidelinePolicyIdempotencyConflict,
        GuidelinePolicyBindingConflict,
        GuidelinePolicySubjectConflict,
        GuidelinePolicyRevisionConflict,
    ],
)
def test_all_policy_conflict_families_map_to_http_409(
    error_type: type[GuidelinePolicyPersistenceError],
) -> None:
    error = error_type("database row contains SECRET-DO-NOT-LEAK")
    projected = project_guideline_policy_error(error)

    assert projected["code"] == "conflict"
    assert projected["http_status"] == 409
    assert projected["status_category"] == "conflict"
    assert projected["retryable"] is True
    assert "SECRET-DO-NOT-LEAK" not in repr(projected)
    assert guideline_policy_http_status(error) == 409


def test_stale_impact_conflict_preserves_only_closed_currentness_diagnostics() -> None:
    secret = "token=SECRET-DO-NOT-LEAK"
    projected = project_guideline_policy_error(
        GuidelinePolicyCasConflict(
            "guideline_impact_stale",
            details=(
                (
                    "stale_reasons",
                    "guideline_head_changed,waiver_snapshot_changed",
                ),
                ("private_context", secret),
            ),
        )
    )

    assert projected["code"] == "conflict"
    assert projected["http_status"] == 409
    assert projected["details"] == {
        "reason_code": "guideline_impact_stale",
        "stale_reasons": (
            "guideline_head_changed,waiver_snapshot_changed"
        ),
    }
    assert secret not in repr(projected)


def test_generic_use_case_conflict_maps_to_http_409() -> None:
    projected = project_guideline_policy_error(
        ConflictError("guideline", "guideline-secret-id")
    )

    assert projected["code"] == "conflict"
    assert projected["http_status"] == 409
    assert projected["details"] == {"entity_type": "guideline"}
    assert "guideline-secret-id" not in repr(projected)


@pytest.mark.parametrize(
    "error",
    [
        GuidelinePolicyAdapterMissing("adapter detail SECRET"),
        GuidelinePolicyPersistenceError("database DSN SECRET"),
        GuidelinePolicyCursorConfigurationError(),
    ],
)
def test_unavailable_errors_map_to_http_503_without_internal_details(
    error: Exception,
) -> None:
    projected = project_guideline_policy_error(error)

    assert projected["code"] == "service_unavailable"
    assert projected["http_status"] == 503
    assert projected["status_category"] == "service_unavailable"
    assert projected["retryable"] is True
    assert "SECRET" not in repr(projected)
    assert guideline_policy_http_status(error) == 503


def test_free_form_validation_text_is_not_echoed_or_promoted_to_reason_code() -> None:
    raw = "invalid input token=SECRET-DO-NOT-LEAK"
    projected = project_guideline_policy_error(ValueError(raw))

    assert projected["code"] == "validation_failed"
    assert projected["details"] == {}
    assert raw not in repr(projected)


def test_import_path_is_omitted_even_when_it_looks_structural() -> None:
    raw_path = "$.SECRET"
    projected = project_guideline_policy_error(
        GuidelineImportExportError(
            "guideline_import_envelope_invalid",
            path=raw_path,
        )
    )

    assert projected["details"] == {"reason_code": "guideline_import_envelope_invalid"}
    assert raw_path not in repr(projected)


def test_unknown_programming_errors_are_not_silently_reclassified() -> None:
    with pytest.raises(
        UnsupportedGuidelinePolicyError,
        match="guideline_policy_error_type_unsupported",
    ):
        project_guideline_policy_error(RuntimeError("unexpected bug"))
