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
from okto_pulse.core.ports.bug_cognitive_context import BugCognitiveContext


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
    db: object,
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

_REQUIRED_EVIDENCE_CATEGORIES: tuple[str, ...] = (
    "root_cause",
    "fix_narrative",
    "impact",
    "regression_proof",
    "validation",
    "test_scenarios",
    "lineage",
)

_POSITIVE_STATES = frozenset({
    "done", "passed", "pass", "success", "succeeded", "verified",
    "approved", "accepted", "complete", "completed", "implemented",
    "confirmed", "valid",
})
_NEGATIVE_STATES = frozenset({
    "", "false", "failed", "fail", "failure", "pending", "unknown",
    "unverified", "rejected", "invalid", "incomplete", "not_run",
    "not-run", "not run", "none", "null", "n/a", "no",
})
_CONTROL_BOOLEAN_KEYS = frozenset({
    "confirmed", "implemented", "passed", "product_runtime_exercised",
    "success", "verified",
})


def _meaningful_text(value: Any, *, minimum: int = 1) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return len(text) >= minimum and text.lower() not in _NEGATIVE_STATES


def _semantic_content(value: Any) -> bool:
    """Content/state check that never treats a container as evidence by itself."""

    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return _meaningful_text(value)
    if isinstance(value, Mapping):
        for key in _CONTROL_BOOLEAN_KEYS:
            if key in value and value.get(key) is not True:
                return False
        for key in ("status", "state", "verdict", "outcome"):
            if key in value:
                state = str(value.get(key) or "").strip().lower()
                if state in _NEGATIVE_STATES:
                    return False
                if state in _POSITIVE_STATES:
                    return True
        return any(
            _semantic_content(value.get(key))
            for key in (
                "content", "description", "details", "evidence", "narrative",
                "reason", "root_cause", "summary", "text", "value",
            )
            if key in value
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_semantic_content(item) for item in value)
    return False


def _successful_state(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if not isinstance(value, Mapping):
        return False
    for key in _CONTROL_BOOLEAN_KEYS:
        if key in value:
            return value.get(key) is True
    for key in ("status", "state", "verdict", "outcome", "recommendation"):
        if key in value:
            return str(value.get(key) or "").strip().lower() in _POSITIVE_STATES
    return False


def _has_conclusion(rows: Sequence[Mapping[str, object]]) -> bool:
    return any(
        _meaningful_text(str(row.get("text") or row.get("content") or ""), minimum=4)
        for row in rows
    )


def _scenario_has_verified_execution(
    scenario: Mapping[str, object], context: BugCognitiveContext
) -> bool:
    status = str(scenario.get("status") or "").strip().lower()
    if status not in _POSITIVE_STATES:
        return False
    evidence = scenario.get("evidence")
    if not isinstance(evidence, Mapping):
        return False
    from okto_pulse.core.services.test_scenario_lifecycle import (
        compute_test_scenario_semantic_sha256,
        verify_mcp_replay_evidence_v2,
    )

    if not context.spec_id:
        return False
    try:
        scenario_sha256 = compute_test_scenario_semantic_sha256(
            board_id=context.board_id,
            spec_id=context.spec_id,
            scenario=scenario,
            acceptance_criteria=list(context.acceptance_criteria),
        )
    except (TypeError, ValueError):
        return False
    result = verify_mcp_replay_evidence_v2(
        status,
        evidence,
        scenario_id=str(scenario.get("id") or "") or None,
        scenario_sha256=scenario_sha256,
    )
    if not result.verified:
        return False
    from okto_pulse.core.ports.test_evidence import (
        resolve_test_evidence_write_verifier,
    )

    verifier = resolve_test_evidence_write_verifier()
    if verifier is None:
        return False
    verification = verifier.verify(
        board_id=context.board_id,
        spec_id=context.spec_id,
        scenario_id=str(scenario.get("id") or ""),
        scenario_sha256=scenario_sha256,
        status=status,
        actor_id=None,
        evidence=evidence,
    )
    return verification.verified


def _linked_test_passed(context: BugCognitiveContext) -> bool:
    for task in context.linked_test_tasks:
        if (task.card_type or "").strip().lower() != "test":
            continue
        if (task.status or "").strip().lower() not in _POSITIVE_STATES:
            continue
        if any(_successful_state(row) for row in task.validations) or _has_conclusion(
            task.conclusions
        ):
            return True
    return False


def _canonical_evidence(context: BugCognitiveContext | None) -> dict[str, bool]:
    if context is None:
        return {category: False for category in _EVIDENCE_CATEGORIES}

    scenarios_present = any(
        isinstance(row, Mapping)
        and (
            _meaningful_text(str(row.get("id") or ""))
            or _meaningful_text(str(row.get("title") or ""), minimum=4)
        )
        for row in context.test_scenarios
    )
    regression_in_lineage = any(
        str(row.get("lineage_state") or "").strip().lower() == "complete"
        and str(row.get("status") or "").strip().lower() in _POSITIVE_STATES
        and _successful_state(
            row.get("validation_metadata")
            if isinstance(row.get("validation_metadata"), Mapping)
            else {}
        )
        and (
            _semantic_content(row.get("automated_regression_refs"))
            or _semantic_content(row.get("regression_scenario_ids"))
            or _semantic_content(row.get("regression_test_task_ids"))
        )
        for row in context.lineage
    )
    return {
        "root_cause": _meaningful_text(context.action_plan, minimum=20),
        "fix_narrative": _has_conclusion(context.conclusions),
        "impact": (
            _meaningful_text(context.expected_behavior, minimum=4)
            and _meaningful_text(context.observed_behavior, minimum=4)
            and context.expected_behavior.strip() != context.observed_behavior.strip()
        ),
        "regression_proof": (
            _linked_test_passed(context)
            or any(
                _scenario_has_verified_execution(row, context)
                for row in context.test_scenarios
            )
            or regression_in_lineage
        ),
        "validation": any(_successful_state(row) for row in context.validations),
        "technical_comments": any(
            _meaningful_text(str(row.get("content") or ""), minimum=4)
            for row in context.comments
        ),
        "test_scenarios": scenarios_present,
        "lineage": bool(
            context.spec_id and (context.origin_task_id or context.lineage)
        ),
    }


def _caller_evidence(evidence: Mapping[str, Any] | None) -> dict[str, bool]:
    ev = evidence or {}
    return {
        category: _semantic_content(ev.get(category))
        for category in _EVIDENCE_CATEGORIES
    }


def classify_bug_evidence(
    evidence: Mapping[str, Any] | None,
    *,
    context: BugCognitiveContext | None = None,
) -> dict[str, Any]:
    """Classify bug evidence by the deterministic categories (fr_2e10da4e).

    ``has_reusable_learning`` is True ONLY when the deterministic preconditions
    for a reusable Learning/Decision are present — at minimum a root cause AND a
    fix narrative (fr_85b5f425). No reusable learning ⇒ the closure is a
    no_action, never a fabricated Learning (dec_6d34ae43).
    """

    canonical = _canonical_evidence(context)
    caller = _caller_evidence(evidence)
    # Caller evidence is additive: it can contribute a missing signal but can
    # neither delete nor replace a canonical one.  Sources remain explicit.
    present = {
        category: canonical[category] or caller[category]
        for category in _EVIDENCE_CATEGORIES
    }
    sources = {
        category: tuple(
            source
            for source, available in (
                ("canonical_context", canonical[category]),
                (f"caller_evidence:{category}", caller[category]),
            )
            if available
        )
        for category in _EVIDENCE_CATEGORIES
    }
    has_reusable_learning = present["root_cause"] and present["fix_narrative"]
    missing = tuple(
        category
        for category in _REQUIRED_EVIDENCE_CATEGORIES
        if not present[category]
    )
    context_verified = bool(context is not None and context.verified)
    return {
        "categories_present": present,
        "category_sources": sources,
        "has_reusable_learning": has_reusable_learning,
        "required_categories": _REQUIRED_EVIDENCE_CATEGORIES,
        "missing_categories": missing,
        "context_verified": context_verified,
        "evidence_ready": context_verified and not missing,
        "caller_evidence_additive": True,
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
    readiness_effect: str | None = None,
    blocking: bool | None = None,
    precedence_explanation: Mapping[str, Any] | None = None,
    bug_context: BugCognitiveContext | None = None,
    cognitive_work_item_present: bool | None = None,
) -> dict[str, Any]:
    effective_readiness = readiness_effect or verdict.readiness_effect
    effective_blocking = verdict.blocking if blocking is None else blocking
    effective_precedence = dict(
        precedence_explanation or verdict.precedence_explanation
    )
    return {
        "status": status,
        "outcome_type": outcome_type,
        "reason_code": reason_code,
        "graph_commit_required": graph_commit_required,
        "readiness_effect": effective_readiness,
        "blocking": effective_blocking,
        "precedence_explanation": effective_precedence,
        "artifact_id": verdict.artifact_id,
        "bug_action_label": bug_action_label,
        "evidence_classification": evidence_classification,
        "pipeline_readiness": {
            "readiness_effect": verdict.readiness_effect,
            "blocking": verdict.blocking,
            "precedence_explanation": dict(verdict.precedence_explanation),
        },
        "evidence_readiness": {
            "ready": evidence_classification["evidence_ready"],
            "blocking": not evidence_classification["evidence_ready"],
            "missing_categories": evidence_classification["missing_categories"],
            "context_verified": evidence_classification["context_verified"],
        },
        "cognitive_work_item": {
            "present": cognitive_work_item_present,
            "required": bool(bug_context and bug_context.eligible_for_closeout),
        },
        "context": {
            "contract_version": bug_context.contract_version if bug_context else None,
            "verified": bool(bug_context and bug_context.verified),
            "eligible_for_closeout": bool(
                bug_context and bug_context.eligible_for_closeout
            ),
            "canonical_bug_present": (
                bug_context.canonical_bug_present if bug_context else None
            ),
            "provenance_refs": (
                bug_context.provenance_refs if bug_context else ()
            ),
            "load_errors": bug_context.load_errors if bug_context else (
                "bug_cognitive_context_unavailable",
            ),
        },
        "technical_remediation": remediation,
    }


def _aggregate_readiness(
    *,
    verdict: CognitiveReadinessVerdict,
    context: BugCognitiveContext | None,
    classification: Mapping[str, Any],
    cognitive_work_item_present: bool,
    action: str,
) -> tuple[str, bool, dict[str, Any], str | None, str | None]:
    """Aggregate pipeline, context and evidence without masking any blocker."""

    pipeline = dict(verdict.precedence_explanation)
    if verdict.blocking and verdict.readiness_effect == "blocking_technical":
        return (
            verdict.readiness_effect,
            True,
            pipeline,
            "blocked",
            "resolve_technical_debt_before_commit",
        )
    if context is None:
        return (
            "blocking_technical",
            True,
            {"tier": "bug_cognitive_context_unavailable", "pipeline": pipeline},
            "bug_cognitive_context_unavailable",
            "assemble_bug_cognitive_context",
        )
    if context.load_errors:
        return (
            "blocking_technical",
            True,
            {
                "tier": "bug_cognitive_context_load_failed",
                "errors": context.load_errors,
                "pipeline": pipeline,
            },
            "bug_cognitive_context_load_failed",
            "retry_bug_cognitive_context_assembly",
        )
    if not context.card_exists:
        return (
            "blocking_technical",
            True,
            {"tier": "bug_source_not_found", "pipeline": pipeline},
            "bug_source_not_found",
            "restore_or_reconcile_bug_source",
        )
    if context.eligible_for_closeout and not cognitive_work_item_present:
        return (
            "blocking_cognitive",
            True,
            {"tier": "missing_cognitive_work_item", "pipeline": pipeline},
            "missing_cognitive_work_item",
            "requeue_cognitive_closeout",
        )
    if action == CREATE_LEARNING and not context.eligible_for_closeout:
        return (
            "blocking_cognitive",
            True,
            {"tier": "bug_not_eligible_for_closeout", "pipeline": pipeline},
            "bug_not_eligible_for_closeout",
            "move_bug_to_done_before_cognitive_closeout",
        )
    if context.canonical_bug_present is not True:
        tier = (
            "canonical_bug_node_absent"
            if context.canonical_bug_present is False
            else "canonical_bug_state_unverified"
        )
        return (
            "blocking_technical",
            True,
            {"tier": tier, "pipeline": pipeline},
            tier,
            "reconcile_canonical_bug_node",
        )
    if context.eligible_for_closeout and not classification["evidence_ready"]:
        return (
            "blocking_cognitive",
            True,
            {
                "tier": "bug_evidence_incomplete",
                "missing_categories": classification["missing_categories"],
                "pipeline": pipeline,
            },
            "evidence_incomplete",
            "complete_bug_cognitive_evidence",
        )
    if (
        action == CREATE_LEARNING
        and cognitive_work_item_present
        and verdict.blocking
        and verdict.readiness_effect == "blocking_cognitive"
    ):
        # An active cognitive item is the work CREATE_LEARNING is meant to
        # resolve.  It remains visible under pipeline_readiness, but is not an
        # admission blocker once source, graph and evidence are all verified.
        return (
            "ready_for_cognitive_commit",
            False,
            {"tier": "bug_closeout_ready_to_commit", "pipeline": pipeline},
            None,
            None,
        )
    return (
        verdict.readiness_effect,
        verdict.blocking,
        pipeline,
        None,
        None,
    )


async def evaluate_bug_cognitive_closure(
    service: CognitiveReadinessService,
    db: object,
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
    bug_context: BugCognitiveContext | None = None,
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

    if not evidence and bug_context is None:
        raise CognitiveReadinessError(
            "missing_bug_evidence",
            "Bug evidence is required to evaluate cognitive closure.",
            http_status=400,
        )

    source_ref = bug_cognitive_source_ref(bug_id)
    classification = classify_bug_evidence(evidence, context=bug_context)
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
            bug_context=bug_context,
            cognitive_work_item_present=True,
        )

    verdict = await service.evaluate_artifact(
        db, board_id=board_id, source_ref=source_ref,
        kg_generation_id=kg_generation_id,
    )

    work_item_present = bool(
        service.cognitive_items_for(board_id, source_ref, kg_generation_id)
    )
    (
        aggregate_effect,
        aggregate_blocking,
        aggregate_precedence,
        blocking_status,
        aggregate_remediation,
    ) = _aggregate_readiness(
        verdict=verdict,
        context=bug_context,
        classification=classification,
        cognitive_work_item_present=work_item_present,
        action=action,
    )

    if action == CREATE_LEARNING:
        if aggregate_blocking:
            return _evaluate_response(
                status=blocking_status or "blocked",
                outcome_type=None,
                reason_code=None,
                graph_commit_required=False,
                verdict=verdict,
                evidence_classification=classification, bug_action_label=None,
                remediation=aggregate_remediation,
                readiness_effect=aggregate_effect,
                blocking=True,
                precedence_explanation=aggregate_precedence,
                bug_context=bug_context,
                cognitive_work_item_present=work_item_present,
            )
        return _evaluate_response(
            status="ready_to_commit", outcome_type=None, reason_code=None,
            graph_commit_required=True, verdict=verdict,
            evidence_classification=classification,
            bug_action_label=BugCognitiveActionLabel.CREATE_LEARNING_VALIDATES_BUG.value,
            readiness_effect=aggregate_effect,
            blocking=aggregate_blocking,
            precedence_explanation=aggregate_precedence,
            bug_context=bug_context,
            cognitive_work_item_present=work_item_present,
        )

    # Pure classification is still fail-closed: a source, graph, work-item or
    # evidence gap is surfaced as the status instead of reporting a false ready.
    return _evaluate_response(
        status=blocking_status or "evaluated", outcome_type=None, reason_code=None,
        graph_commit_required=False, verdict=verdict,
        evidence_classification=classification, bug_action_label=None,
        remediation=aggregate_remediation,
        readiness_effect=aggregate_effect,
        blocking=aggregate_blocking,
        precedence_explanation=aggregate_precedence,
        bug_context=bug_context,
        cognitive_work_item_present=work_item_present,
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
