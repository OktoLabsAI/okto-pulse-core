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
        "receipt_id": error.receipt_id,
        "currentness": (
            error.currentness.value if error.currentness is not None else None
        ),
        "currentness_reasons": [reason.value for reason in error.currentness_reasons],
        "counts": {
            "applicable_rules": error.applicable_rule_count,
            "applicable_blocking_rules": error.applicable_blocking_rule_count,
            "blocking_rules": error.blocking_rule_count,
            "waived_rules": decision.waived_rule_count,
            "advisory_issues": decision.advisory_issue_count,
        },
        "transition": {
            "entity_type": decision.entity_type.value,
            "subject_id": decision.subject_id,
            "from_status": decision.from_status,
            "to_status": decision.to_status,
        },
        "policy_compliance_required": decision.policy_compliance_required,
    }


__all__ = ["project_policy_transition_rejection"]
