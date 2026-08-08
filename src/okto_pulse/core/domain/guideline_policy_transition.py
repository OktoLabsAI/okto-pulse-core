"""Compatibility import path for the semantic native policy gate.

The public seam keeps its historical module path so application and edition
adapters can migrate atomically.  The active contract is implemented entirely
by :mod:`guideline_semantic_transition`; no executable-rule evaluator remains
on this path.
"""

from okto_pulse.core.domain.guideline_semantic_transition import (
    POLICY_TRANSITION_CONTRACT_VERSION,
    PolicyTransitionDecision,
    PolicyTransitionDiagnosticCode,
    PolicyTransitionReasonCode,
    PolicyTransitionRejected,
    PolicyTransitionSnapshot,
    SemanticBindingComplianceDecision,
    SemanticBindingComplianceSnapshot,
    evaluate_policy_transition,
    raise_for_policy_transition,
    require_policy_transition_decision_match,
)


__all__ = [
    "POLICY_TRANSITION_CONTRACT_VERSION",
    "PolicyTransitionDecision",
    "PolicyTransitionDiagnosticCode",
    "PolicyTransitionReasonCode",
    "PolicyTransitionRejected",
    "PolicyTransitionSnapshot",
    "SemanticBindingComplianceDecision",
    "SemanticBindingComplianceSnapshot",
    "evaluate_policy_transition",
    "raise_for_policy_transition",
    "require_policy_transition_decision_match",
]
