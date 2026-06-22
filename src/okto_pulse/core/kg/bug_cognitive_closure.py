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

from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
    CognitiveReasonCode,
    CognitiveReadinessVerdict,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitivePendingOutcomeType,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessage,
    BugWorkflowRemediationMessageBuilder,
    BugWorkflowRemediationPath,
)


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


def bug_cognitive_source_ref(bug_id: str) -> str:
    """Cognitive source_ref for a bug card — the canonical ``bug:<id>`` alias
    that normalizes (S1.1) to the same artifact_id as a canonical-debt
    ``card:<id>``."""
    return f"bug:{bug_id}"


async def record_bug_cognitive_skip(
    service: CognitiveReadinessService,
    db: AsyncSession,
    *,
    board_id: str,
    bug_id: str,
    reason_code: str,
    actor: str,
    justification: str | None = None,
    evidence_refs: Sequence[str] | None = None,
    revisit_at: str | None = None,
    kg_generation_id: str | None = None,
) -> CognitiveConsolidationItem:
    """THIN bug-specific wrapper over the central write-path
    :meth:`CognitiveReadinessService.record_cognitive_skip` (fr_8c95dd95).

    Adds NO local precedence / store / enum / Path B lineage store: it only maps
    the bug to ``source_ref=bug:<id>`` and forwards reason_code / justification /
    evidence_refs / actor / revisit_at. Every refusal comes from the central
    write-path, unchanged:
      * 400 ``invalid_reason_code`` (reason not in the closed cognitive registry);
      * 400 ``revisit_at_required`` (e.g. ``path_b_pending`` without revisit_at —
        tr_e004a794);
      * 409 ``technical_debt_cannot_be_skipped`` when an open DLQ / canonical_debt
        exists for the SAME normalized artifact_id (a card:<uuid> debt blocks the
        bug:<uuid> skip — tr_f550ab3f / no-mask).

    duplicate_bug / trivial_fix / no_reusable_learning record ``skipped`` +
    ``no_action_required`` WITHOUT a fabricated Learning (br_4113fbc0 /
    dec_6d34ae43).
    """

    return await service.record_cognitive_skip(
        db,
        board_id=board_id,
        source_ref=bug_cognitive_source_ref(bug_id),
        reason_code=reason_code,
        actor=actor,
        justification=justification,
        evidence_refs=evidence_refs,
        revisit_at=revisit_at,
        kg_generation_id=kg_generation_id,
    )


def build_bug_path_b_remediation(
    *, reason_code: str = CognitiveReasonCode.PATH_B_PENDING.value
) -> BugWorkflowRemediationMessage:
    """Consume the EXISTING ``BugWorkflowRemediationPath.PATH_B_SEMANTIC_GAP`` for
    a locked-spec bug semantic gap (fr_0092774e / dec_085b0f9e). Reuses the
    canonical remediation builder — it never merges with
    ``PATH_C_HOTFIX_LANE`` and never creates a Path B lineage store of its own."""
    return BugWorkflowRemediationMessageBuilder().build_semantic_gap(
        reason_code=reason_code,
    )


# --- bug evidence evaluation (card 13b43f3d) -------------------------------

# Requested action vocabulary for the evaluate surface (REST + MCP twin).
EVALUATE = "evaluate"
SKIP = "skip"
NO_ACTION = "no_action"
CREATE_LEARNING = "create_learning"
_SKIP_ACTIONS = frozenset({SKIP, NO_ACTION})

# fr_2e10da4e — the deterministic bug-evidence categories classified by evaluate.
_EVIDENCE_CATEGORIES: tuple[str, ...] = (
    "root_cause",
    "fix_narrative",
    "impact",
    "regression_proof",
    "validation",
    "technical_comments",
    "test_scenarios",
    "lineage",
)


def classify_bug_evidence(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Classify bug evidence by the deterministic categories (fr_2e10da4e).

    ``has_reusable_learning`` is True ONLY when the deterministic preconditions
    for a reusable Learning/Decision are present — at minimum a root cause AND a
    fix narrative (fr_85b5f425). No reusable learning ⇒ the closure is a
    no_action, never a fabricated Learning (dec_6d34ae43).
    """

    ev = evidence or {}
    present = {cat: bool(ev.get(cat)) for cat in _EVIDENCE_CATEGORIES}
    has_reusable_learning = present["root_cause"] and present["fix_narrative"]
    return {
        "categories_present": present,
        "has_reusable_learning": has_reusable_learning,
    }


def _evaluate_response(
    *,
    status: str,
    outcome_type: str | None,
    reason_code: str | None,
    graph_commit_required: bool,
    verdict: CognitiveReadinessVerdict,
    evidence_classification: dict[str, Any],
    bug_action_label: str | None,
    remediation: str | None = None,
) -> dict[str, Any]:
    # readiness_effect / blocking / precedence_explanation are MIRRORED from the
    # central CognitiveReadinessService verdict — never recomputed (tr_28465cc7).
    return {
        "status": status,
        "outcome_type": outcome_type,
        "reason_code": reason_code,
        "graph_commit_required": graph_commit_required,
        "readiness_effect": verdict.readiness_effect,
        "blocking": verdict.blocking,
        "precedence_explanation": dict(verdict.precedence_explanation),
        "artifact_id": verdict.artifact_id,
        "bug_action_label": bug_action_label,
        "evidence_classification": evidence_classification,
        "technical_remediation": remediation,
    }


async def evaluate_bug_cognitive_closure(
    service: CognitiveReadinessService,
    db: AsyncSession,
    *,
    board_id: str,
    bug_id: str,
    evidence: Mapping[str, Any] | None,
    requested_action: str = EVALUATE,
    reason_code: str | None = None,
    actor: str = "system",
    justification: str | None = None,
    evidence_refs: Sequence[str] | None = None,
    revisit_at: str | None = None,
    kg_generation_id: str | None = None,
) -> dict[str, Any]:
    """Bug cognitive-closure evidence evaluation (api_8c29ce5d / br_4f1fedd9 /
    dec_7b75ce29). The SAME classification the REST/UI and the MCP twin run.

    Readiness (readiness_effect / blocking / precedence_explanation) is obtained
    from ``CognitiveReadinessService.evaluate_artifact`` and mirrored verbatim —
    the bug branch never recomputes precedence (tr_28465cc7). Any resulting
    skip/no_action goes through the central write-path
    (``record_bug_cognitive_skip``), so its refusals are unchanged: 400
    ``revisit_at_required`` (revisit-required reason w/o revisit_at) and 409
    ``technical_debt_cannot_be_skipped`` (open DLQ / canonical_debt). A missing
    bug node or a technical failure NEVER fabricates a relationship nor converts
    to no_action — it surfaces as technical remediation / blocking.
    """

    if not evidence:
        raise CognitiveReadinessError(
            "missing_bug_evidence",
            "Bug evidence is required to evaluate cognitive closure.",
            http_status=400,
        )

    source_ref = bug_cognitive_source_ref(bug_id)
    classification = classify_bug_evidence(evidence)
    action = str(requested_action or EVALUATE)

    # Skip / no_action → delegate to the central write-path (400/409 there).
    if action in _SKIP_ACTIONS:
        item = await record_bug_cognitive_skip(
            service, db,
            board_id=board_id, bug_id=bug_id,
            reason_code=str(reason_code or ""), actor=actor,
            justification=justification, evidence_refs=evidence_refs,
            revisit_at=revisit_at, kg_generation_id=kg_generation_id,
        )
        verdict = await service.evaluate_artifact(
            db, board_id=board_id, source_ref=source_ref,
            kg_generation_id=kg_generation_id,
        )
        return _evaluate_response(
            status=item.status,
            outcome_type=item.outcome_type,
            reason_code=item.reason_code,
            graph_commit_required=False,
            verdict=verdict,
            evidence_classification=classification,
            bug_action_label=project_bug_action_label(
                reason_code=item.reason_code, outcome_type=item.outcome_type,
            ),
        )

    verdict = await service.evaluate_artifact(
        db, board_id=board_id, source_ref=source_ref,
        kg_generation_id=kg_generation_id,
    )

    if action == CREATE_LEARNING:
        # Open technical debt/DLQ blocks any graph commit — stays blocking, never
        # silently converted to no_action.
        if verdict.blocking and verdict.readiness_effect == "blocking_technical":
            return _evaluate_response(
                status="blocked", outcome_type=None, reason_code=None,
                graph_commit_required=True, verdict=verdict,
                evidence_classification=classification, bug_action_label=None,
                remediation="resolve_technical_debt_before_commit",
            )
        # No reusable learning ⇒ closure is no_action, NOT a fabricated Learning.
        if not classification["has_reusable_learning"]:
            return _evaluate_response(
                status="no_reusable_learning", outcome_type=None,
                reason_code=CognitiveReasonCode.NO_REUSABLE_LEARNING.value,
                graph_commit_required=False, verdict=verdict,
                evidence_classification=classification,
                bug_action_label=BugCognitiveActionLabel.NO_REUSABLE_LEARNING.value,
            )
        # Missing bug node ⇒ NO fabricated relation; technical remediation/debt.
        if not service.cognitive_items_for(board_id, source_ref, kg_generation_id):
            return _evaluate_response(
                status="technical_remediation_required", outcome_type=None,
                reason_code=None, graph_commit_required=True, verdict=verdict,
                evidence_classification=classification, bug_action_label=None,
                remediation="bug_node_absent",
            )
        # Reusable learning + node present ⇒ a real commit is required downstream.
        return _evaluate_response(
            status="ready_to_commit", outcome_type=None, reason_code=None,
            graph_commit_required=True, verdict=verdict,
            evidence_classification=classification,
            bug_action_label=BugCognitiveActionLabel.CREATE_LEARNING_VALIDATES_BUG.value,
        )

    # Pure classification (no write) — surfaces readiness + recommendation.
    return _evaluate_response(
        status="evaluated", outcome_type=None, reason_code=None,
        graph_commit_required=False, verdict=verdict,
        evidence_classification=classification, bug_action_label=None,
    )


__all__ = [
    "BugCognitiveActionLabel",
    "BugWorkflowRemediationPath",
    "CREATE_LEARNING",
    "EVALUATE",
    "NO_ACTION",
    "SKIP",
    "build_bug_path_b_remediation",
    "bug_cognitive_source_ref",
    "classify_bug_evidence",
    "evaluate_bug_cognitive_closure",
    "project_bug_action_label",
    "record_bug_cognitive_skip",
]
