"""Shared REST/MCP projection for Policy Compliance transition rejections."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionRejected,
)


def project_policy_transition_rejection(
    error: PolicyTransitionRejected,
) -> dict[str, Any]:
    """Return the stable transport-neutral rejection envelope."""

    if not isinstance(error, PolicyTransitionRejected):
        raise TypeError("policy_transition_rejection_type_unsupported")
    decision = error.decision
    return {
        "outcome": "error",
        "error": error.code,
        "code": error.code,
        "message": str(error),
        "reason_codes": [reason.value for reason in error.reason_codes],
        "decision_digest": error.decision_digest,
        "fence_digest": error.fence_digest,
        "receipt_ids": list(error.receipt_ids),
        "currentness": (
            error.currentness.value if error.currentness is not None else None
        ),
        "currentness_reasons": [reason.value for reason in error.currentness_reasons],
        "counts": {
            "applicable_metrics": error.applicable_metric_count,
            "applicable_blocking_metrics": (
                error.applicable_blocking_metric_count
            ),
            "failed_metrics": error.failed_metric_count,
            "blocking_metrics": error.blocking_metric_count,
            "waived_metrics": decision.waived_metric_count,
            "advisory_issues": decision.advisory_issue_count,
            "skipped_bindings": decision.skipped_binding_count,
        },
        "diagnostic_codes": [
            diagnostic.value for diagnostic in error.diagnostic_codes
        ],
        "binding_decisions": [
            item.to_payload() for item in decision.binding_decisions
        ],
        "transition": {
            "entity_type": decision.entity_type.value,
            "subject_id": decision.subject_id,
            "from_status": decision.from_status,
            "to_status": decision.to_status,
        },
        "policy_compliance_required": decision.policy_compliance_required,
    }


__all__ = ["project_policy_transition_rejection"]
