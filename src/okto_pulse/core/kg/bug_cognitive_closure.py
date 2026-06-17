"""S2 / card 6eb348fb — bug closure branch over the SHARED cognitive store.

Bug closure is a BRANCH of the shared :class:`CognitiveConsolidationItemStore`
(dec_62316865 / br_b995b384), NOT a parallel store/table/queue/enum. A bug:

  * uses ``artifact_type="bug"`` on the shared store (already a
    ``CONSOLIDABLE_ARTIFACT_TYPE``);
  * reconciles with a canonical-debt ``card:<uuid>`` via the S1.1 normalized
    ``artifact_id`` (``normalize_cognitive_artifact_id`` collapses
    ``bug:<uuid>`` → ``card:<uuid>``), keeping ``source_ref_original`` auditable;
  * reuses the SAME reason-code registry + ``CognitiveItemStatus`` /
    ``CognitivePendingOutcomeType`` of the core spec (tr_f90ee0b8);
  * maps closure to existing status/outcome: ``consolidated`` for a real commit,
    ``skipped`` + ``no_action_required`` for trivial / duplicate / no-reusable-
    learning or a revisit-required Path B pending (fr_75877c3f).

The bug-specific ACTION LABELS (fr_f7791c82) live ONLY here as a DTO projection.
They are NEVER a persisted ``CognitiveItemStatus`` / ``CognitivePendingOutcomeType``,
never a new on-disk enum, store, table, queue, or a Path B store of their own
(Path B reuses ``BugWorkflowRemediationPath.PATH_B_SEMANTIC_GAP``).
"""

from __future__ import annotations

from enum import Enum

from okto_pulse.core.kg.cognitive_readiness import CognitiveReasonCode
from okto_pulse.core.kg.rebuild_audit import CognitivePendingOutcomeType


class BugCognitiveActionLabel(str, Enum):
    """DTO-only label describing how a bug's cognitive closure resolved.

    Projected for UI/MCP/REST surfaces; NEVER persisted as a status/outcome enum
    (br_b995b384). Disjoint from ``CognitiveItemStatus`` and
    ``CognitivePendingOutcomeType`` by construction.
    """

    CREATE_LEARNING_VALIDATES_BUG = "create_learning_validates_bug"
    LINK_EXISTING_LEARNING = "link_existing_learning"
    CONSUME_PATH_B_LINEAGE = "consume_path_b_lineage"
    NO_REUSABLE_LEARNING = "no_reusable_learning"


# A real, reusable Learning/Decision was produced by the consolidation.
_CREATE_LEARNING_OUTCOMES: frozenset[str] = frozenset({
    CognitivePendingOutcomeType.RELATION_CREATED.value,
    CognitivePendingOutcomeType.CANDIDATE_CREATED.value,
    CognitivePendingOutcomeType.FORMAL_DECISION_PROMOTED.value,
})

# reason_codes that mean "no reusable learning exists for this bug" — a skip
# with no fabricated Learning/Decision (tr_f90ee0b8 / dec_62316865).
_NO_REUSABLE_LEARNING_REASONS: frozenset[str] = frozenset({
    CognitiveReasonCode.NO_REUSABLE_LEARNING.value,
    CognitiveReasonCode.DUPLICATE_BUG.value,
    CognitiveReasonCode.TRIVIAL_FIX.value,
})


def project_bug_action_label(
    *, reason_code: str | None, outcome_type: str | None
) -> str | None:
    """Pure DTO projection of a bug closure's recorded ``(reason_code,
    outcome_type)`` to its bug-specific action label.

    Returns ``None`` when no bug-specific label applies (the caller projects it
    as an absent field — never a fabricated label). Deterministic; performs no
    I/O and is never persisted.
    """

    rc = str(reason_code or "")
    ot = str(outcome_type or "")

    if ot in _CREATE_LEARNING_OUTCOMES:
        return BugCognitiveActionLabel.CREATE_LEARNING_VALIDATES_BUG.value
    if ot == CognitivePendingOutcomeType.EXISTING_DECISION_LINKED.value:
        return BugCognitiveActionLabel.LINK_EXISTING_LEARNING.value
    if rc == CognitiveReasonCode.PATH_B_PENDING.value:
        return BugCognitiveActionLabel.CONSUME_PATH_B_LINEAGE.value
    if rc in _NO_REUSABLE_LEARNING_REASONS:
        return BugCognitiveActionLabel.NO_REUSABLE_LEARNING.value
    return None


__all__ = [
    "BugCognitiveActionLabel",
    "project_bug_action_label",
]
