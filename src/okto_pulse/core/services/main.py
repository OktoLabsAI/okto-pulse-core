"""Service layer for business logic."""

import hashlib
import logging
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.infra.storage import get_storage_provider
from okto_pulse.core.models.db import (
    ActivityLog,
    Agent,
    AgentBoard,
    Attachment,
    Board,
    BoardGuideline,
    BoardShare,
    Card,
    CardDependency,
    CardStatus,
    CardType,
    Comment,
    Guideline,
    Ideation,
    IdeationComplexity,
    IdeationHistory,
    IdeationKnowledgeBase,
    IdeationQAItem,
    IdeationStatus,
    PermissionPreset,
    QAItem,
    Refinement,
    RefinementHistory,
    RefinementKnowledgeBase,
    RefinementQAItem,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
    SpecHistory,
    SpecKnowledgeBase,
    SpecQAItem,
    SpecStatus,
    Story,
    StoryIdeationLink,
    StoryStatus,
    Sprint,
    SprintHistory,
    SprintLaneType,
    SprintQAItem,
    SprintStatus,
    Topic,
)
from okto_pulse.core.models.schemas import (
    AgentCreate,
    AgentUpdate,
    BoardCreate,
    BoardShareCreate,
    BoardShareUpdate,
    BoardUpdate,
    CardCreate,
    CardMove,
    CardUpdate,
    CommentCreate,
    CommentUpdate,
    GuidelineCreate,
    GuidelineUpdate,
    IdeationCreate,
    IdeationKnowledgeCreate,
    IdeationKnowledgeUpdate,
    IdeationMove,
    IdeationQAAnswer,
    IdeationQACreate,
    IdeationUpdate,
    QACreate,
    QAAnswer,
    RefinementCreate,
    RefinementKnowledgeCreate,
    RefinementMove,
    RefinementQAAnswer,
    RefinementQACreate,
    RefinementUpdate,
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeUpdate,
    SpecMove,
    SpecQAAnswer,
    SpecQACreate,
    SpecUpdate,
    StoryConversionRequest,
    StoryCreate,
    StoryMove,
    StoryUpdate,
    SprintCreate,
    SprintMove,
    SprintUpdate,
    TopicCreate,
    TopicUpdate,
)
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionCoverageState,
    BugRegressionGateValidator,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessage,
    BugWorkflowRemediationMessageBuilder,
    serialize_bug_workflow_remediation,
)
from okto_pulse.core.services.bug_regression_observability import (
    emit_no_unlock_invariant,
    observe_bug_regression_resolution,
    record_bug_regression_decision,
)
from okto_pulse.core.services.board_governance import (
    BoardGovernanceService,
    QA_SELF_ANSWER_DENIED_ACTION,
    QASelfAnsweringNotAllowedError,
    build_qa_self_answer_denied_details,
)
from okto_pulse.core.services.critical_context_guard import (
    CRITICAL_CONTEXT_DECISION_ACTION,
    CriticalAction,
    CriticalContextDecision,
    FullContextCriticalActionGuard,
    FullContextGuardError,
    build_default_full_context_resolvers,
)
from okto_pulse.core.services.governance_observability import (
    build_board_governance_setting_changed_details,
    build_board_missing_context_warning_details,
    emit_governance_metric,
)
from okto_pulse.core.services.analytics_service import (
    _structured_ref_text,
    resolve_linked_criteria_to_ids,
    resolve_linked_criteria_to_indices,
    resolve_linked_fr_indices,
)
from okto_pulse.core.services.spec_entity_canonicalization import canonicalize_fr_ac
from okto_pulse.core.services.test_scenario_lifecycle import (
    GATED_STATUSES,
    StatusNotMutableError,
    VALID_SCENARIO_STATUSES,
    evidence_invalidated_by_semantic_edit,
    require_test_scenario_status_mutable,
    scenario_has_required_evidence,
    validate_scenario_type,
    validate_scenario_types_for_write,
    validate_test_scenario_evidence,
)
from okto_pulse.core.services.reference_resolution import compile_ideation_parent_context
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService

settings = get_settings()


def _build_default_cognitive_closeout_gate() -> Any:
    """Build the shared cognitive closeout gate lazily.

    The lazy import keeps the service layer from importing KG storage at module
    import time while still letting tests inject a lightweight fake gate.
    """

    from okto_pulse.core.kg.cognitive_closeout_gate import (
        build_default_cognitive_closeout_gate,
    )

    return build_default_cognitive_closeout_gate()


def _board_skip_cognitive_consolidation(board: Board | None) -> bool:
    settings = (board.settings or {}) if board else {}
    return bool(settings.get("skip_cognitive_consolidation", False))


# S1.3 Cognitive Closure rollout — per-board policy + global feature flag.
COGNITIVE_READINESS_POLICY_ADVISORY = "advisory"
COGNITIVE_READINESS_POLICY_BLOCKING = "blocking"


def _board_cognitive_readiness_policy(board: Board | None) -> str:
    """Per-board cognitive readiness policy (fr_9d42c5e2). Default ``advisory``
    so existing boards never begin blocking on rollout."""
    settings = (board.settings or {}) if board else {}
    value = str(
        settings.get("cognitive_readiness_policy", COGNITIVE_READINESS_POLICY_ADVISORY)
    ).lower()
    if value not in (
        COGNITIVE_READINESS_POLICY_ADVISORY,
        COGNITIVE_READINESS_POLICY_BLOCKING,
    ):
        return COGNITIVE_READINESS_POLICY_ADVISORY
    return value


def _cognitive_readiness_blocking_active(board: Board | None) -> bool:
    """True only when BOTH the global feature flag is enabled AND the board
    policy is ``blocking`` — the two-key safe rollout (dec_41db6a36). Default-off:
    any failure or unset value resolves to advisory (non-blocking)."""
    if _board_cognitive_readiness_policy(board) != COGNITIVE_READINESS_POLICY_BLOCKING:
        return False
    try:
        from okto_pulse.core.infra.config import get_settings

        return bool(get_settings().cognitive_readiness_blocking_enabled)
    except Exception:
        return False


def _board_qa_require_role_separation(board: Board | None) -> bool:
    """Return True if the board requires that Q&A answers come from a different
    principal than the one who asked the question (qa_require_role_separation)."""
    settings = (board.settings or {}) if board else {}
    return BoardGovernanceService.from_settings(settings).qa_require_role_separation


async def _attach_open_qa_counts(
    db: AsyncSession,
    rows: list[Any],
    qa_model: Any,
    fk_name: str,
) -> None:
    """Attach an ``open_qa_count`` attribute to each ORM row for summary projection.

    A Q&A item is OPEN (unanswered) when ``answered_at IS NULL`` — the only reliable
    predicate, because choice/multi_choice answers leave ``answer`` NULL and persist
    ``selected`` instead, yet every answer path sets ``answered_at`` once something is
    saved. The list queries don't eager-load qa_items, so a single grouped COUNT keyed
    by the foreign key avoids both N+1 and an async lazy-load during serialization.
    """
    if not rows:
        return
    ids = [r.id for r in rows]
    fk_col = getattr(qa_model, fk_name)
    result = await db.execute(
        select(fk_col, func.count())
        .where(fk_col.in_(ids), qa_model.answered_at.is_(None))
        .group_by(fk_col)
    )
    counts = dict(result.all())
    for r in rows:
        r.open_qa_count = counts.get(r.id, 0)


async def backfill_qa_answered_at(db: AsyncSession) -> dict[str, int]:
    """One-shot self-heal: carimba ``answered_at`` em Q&A respondidas órfãs.

    A herança de Q&A (``propagate_artifacts``) copiava resposta/seleção sem
    ``answered_at`` — e o badge ``open_qa_count`` define "aberta" como
    ``answered_at IS NULL``, então toda Q&A respondida herdada virava
    falso-aberta em refinements/specs derivados (em campo: 100% dos badges
    do board 0.2.3 eram falsos). Idempotente: só toca linhas com resposta
    (``answer`` ou ``selected`` preenchidos) e timestamp ausente, usando o
    ``created_at`` da própria linha como melhor aproximação histórica.
    Retorna {tabela: linhas_corrigidas} para o log estruturado do boot.
    """
    from sqlalchemy import text as sa_text

    tables = (
        ("ideation_qa_items", True),
        ("refinement_qa_items", True),
        ("spec_qa_items", True),
        ("sprint_qa_items", True),
        ("qa_items", False),  # card Q&A: text-only, sem coluna selected
    )
    fixed: dict[str, int] = {}
    for table, has_selected in tables:
        answered_predicate = "(answer IS NOT NULL AND answer != '')"
        if has_selected:
            answered_predicate = (
                f"({answered_predicate} OR (selected IS NOT NULL "
                "AND CAST(selected AS TEXT) NOT IN ('', '[]', 'null')))"
            )
        result = await db.execute(
            sa_text(
                f"UPDATE {table} "
                "SET answered_at = COALESCE(created_at, CURRENT_TIMESTAMP) "
                f"WHERE answered_at IS NULL AND {answered_predicate}"
            )
        )
        count = result.rowcount if result.rowcount and result.rowcount > 0 else 0
        if count:
            fixed[table] = count
    await db.commit()
    return fixed


async def _authorize_qa_answer_or_raise(
    db: AsyncSession,
    *,
    board: Board | None,
    qa: Any,
    user_id: str,
    entity_type: str,
    question_id: str,
    card_id: str | None = None,
    actor_type: str = "user",
    surface: str = "service",
) -> None:
    """Authorize a Q&A answer and emit a safe denial event before failing closed."""
    try:
        BoardGovernanceService.authorize_qa_answer(
            (board.settings if board else None),
            asked_by=getattr(qa, "asked_by", None),
            answered_by=user_id,
        )
    except QASelfAnsweringNotAllowedError:
        if board is not None:
            actor_name = await resolve_actor_name(db, user_id, board.id)
            details = build_qa_self_answer_denied_details(
                board_id=board.id,
                actor_id=user_id,
                entity_type=entity_type,
                question_id=question_id,
                surface=surface,
            )
            db.add(
                ActivityLog(
                    board_id=board.id,
                    card_id=card_id,
                    action=QA_SELF_ANSWER_DENIED_ACTION,
                    actor_type=actor_type,
                    actor_id=user_id,
                    actor_name=actor_name,
                    details=details,
                )
            )
            emit_governance_metric(details, raise_on_violation=False)
            await db.flush()
        raise


async def _record_critical_context_decision(
    db: AsyncSession,
    *,
    decision: CriticalContextDecision,
    actor_name: str | None = None,
    actor_type: str = "user",
    card_id: str | None = None,
) -> None:
    resolved_name = actor_name or await resolve_actor_name(
        db, decision.actor_id, decision.board_id
    )
    db.add(
        ActivityLog(
            board_id=decision.board_id,
            card_id=card_id if decision.entity_type != "card" else decision.entity_id,
            action=CRITICAL_CONTEXT_DECISION_ACTION,
            actor_type=actor_type,
            actor_id=decision.actor_id,
            actor_name=resolved_name,
            details=decision.audit_details(),
        )
    )
    emit_governance_metric(decision.metric_labels(), raise_on_violation=False)
    emit_governance_metric(
        decision.latency_metric_labels(),
        value=round(float(decision.latency_ms), 3),
        raise_on_violation=False,
    )
    if decision.outcome == "deny" and decision.reason in {
        "full_context_required",
        "full_context_unavailable",
    }:
        emit_governance_metric(
            decision.resolution_failure_metric_labels(),
            raise_on_violation=False,
        )
    await db.flush()


async def _authorize_critical_context_or_raise(
    db: AsyncSession,
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    entity_id: str,
    critical_action: CriticalAction,
    surface: str = "service",
    actor_type: str = "user",
    actor_name: str | None = None,
    card_id: str | None = None,
) -> CriticalContextDecision:
    """Resolve full context for a critical action and persist a safe audit event."""

    guard = FullContextCriticalActionGuard(
        db,
        resolvers=build_default_full_context_resolvers(db),
    )
    try:
        decision = await guard.authorize_and_resolve(
            board_id=board_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            critical_action=critical_action,
            surface=surface,
        )
    except FullContextGuardError as exc:
        await _record_critical_context_decision(
            db,
            decision=exc.decision,
            actor_name=actor_name,
            actor_type=actor_type,
            card_id=card_id,
        )
        raise

    await _record_critical_context_decision(
        db,
        decision=decision,
        actor_name=actor_name,
        actor_type=actor_type,
        card_id=card_id,
    )
    return decision


def _critical_card_move_action(target_status: CardStatus) -> CriticalAction:
    if target_status == CardStatus.IN_PROGRESS:
        return CriticalAction.CARD_START_IMPLEMENTATION
    if target_status == CardStatus.DONE:
        return CriticalAction.CARD_CLOSEOUT
    if target_status == CardStatus.CANCELLED:
        return CriticalAction.CARD_CANCEL
    return CriticalAction.CARD_MOVE_STATUS


def _critical_spec_move_action(target_status: SpecStatus) -> CriticalAction:
    if target_status == SpecStatus.APPROVED:
        return CriticalAction.SPEC_APPROVE
    if target_status == SpecStatus.DONE:
        return CriticalAction.SPEC_CLOSEOUT
    if target_status == SpecStatus.CANCELLED:
        return CriticalAction.SPEC_CANCEL
    return CriticalAction.SPEC_MOVE_STATUS


def _critical_sprint_move_action(target_status: SprintStatus) -> CriticalAction:
    if target_status == SprintStatus.CLOSED:
        return CriticalAction.SPRINT_CLOSEOUT
    if target_status == SprintStatus.CANCELLED:
        return CriticalAction.SPRINT_CANCEL
    return CriticalAction.SPRINT_MOVE_STATUS


def _critical_ideation_move_action(target_status: IdeationStatus) -> CriticalAction:
    if target_status == IdeationStatus.DONE:
        return CriticalAction.IDEATION_CLOSEOUT
    if target_status == IdeationStatus.CANCELLED:
        return CriticalAction.IDEATION_CANCEL
    return CriticalAction.IDEATION_MOVE_STATUS


def _critical_refinement_move_action(target_status: RefinementStatus) -> CriticalAction:
    if target_status == RefinementStatus.DONE:
        return CriticalAction.REFINEMENT_CLOSEOUT
    if target_status == RefinementStatus.CANCELLED:
        return CriticalAction.REFINEMENT_CANCEL
    return CriticalAction.REFINEMENT_MOVE_STATUS


def _card_cognitive_entity_type(card: Card) -> str:
    card_type = getattr(card, "card_type", CardType.NORMAL)
    card_type_value = getattr(card_type, "value", str(card_type)).lower()
    if card_type_value == CardType.TEST.value:
        return "test"
    if card_type_value == CardType.BUG.value:
        return "bug"
    return "task"


def _cognitive_blocking_count(result: Any) -> int:
    count = getattr(result, "blocking_count", None)
    if count is not None:
        return int(count)
    blocking_items = getattr(result, "blocking_items", ()) or ()
    return len(blocking_items)


def _evaluate_cognitive_closeout_or_raise(
    *,
    gate_factory: Callable[[], Any],
    board: Board | None,
    board_id: str,
    entity_type: str,
    entity_id: str,
    entity: Any,
    target_label: str,
    graph_state: str | None = None,
) -> None:
    """Evaluate the shared closeout gate and raise a stable service error.

    This helper is intentionally side-effect free. Callers must invoke it before
    any status assignment, resource-gate side effect, conclusion append, or
    lifecycle activity write for a ``done`` transition.
    """

    skip_enabled = _board_skip_cognitive_consolidation(board)
    gate = gate_factory()
    try:
        result = gate.evaluate(
            board_id=board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            entity=entity,
            target_status="done",
            board_skip_enabled=skip_enabled,
            graph_state=graph_state,
        )
    except Exception as exc:
        raise ValueError(
            f"cognitive_status_unavailable: {target_label} done transition "
            f"blocked ({type(exc).__name__})"
        ) from exc

    if getattr(result, "allowed", False):
        return

    reason = str(getattr(result, "reason", "cognitive_consolidation_pending"))
    blocking_count = _cognitive_blocking_count(result)
    if reason == "cognitive_status_unavailable":
        detail = (
            "because cognitive status could not be read. "
            "The KG graph may be in a degraded state (recovery_needed / quarantined). "
            "Per the Degraded-KG Fallback Rule, if the board is confirmed degraded "
            "you may enable the board setting `skip_cognitive_consolidation` to allow "
            "done transitions while the graph is unavailable. "
            "To restore full cognitive closeout, follow the KG Health recovery flow: "
            "call `okto_pulse_kg_health` to confirm the graph_state, then consult "
            "the resource `okto-pulse://reference/kg-health` for the operator-driven "
            "recovery steps."
        )
    else:
        detail = "by active cognitive consolidation items"
    raise ValueError(
        f"{reason}: {target_label} done transition blocked {detail} "
        f"({blocking_count})"
    )


def _build_default_cognitive_readiness_service() -> Any:
    """Default ``CognitiveReadinessService`` over the shared cognitive item
    store (S1.2/S1.3). Injectable factory mirrors
    ``_build_default_cognitive_closeout_gate`` so tests can swap a fake."""

    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        default_rebuild_base_dir,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
    )


async def _evaluate_cognitive_readiness_or_raise(
    *,
    service_factory: Callable[[], Any],
    db: AsyncSession,
    board_id: str,
    entity_type: str,
    entity_id: str,
    entity: Any,
    target_label: str,
    policy_blocking: bool,
) -> None:
    """S1.3 production wiring: consult the single ``CognitiveReadinessService``
    on a ``done`` transition and block on the readiness tiers the legacy gate
    does NOT cover — technical DLQ, canonical_debt OPEN, and a lapsed
    revisit-required skip — BEFORE any status / conclusion / snapshot / activity
    mutation. The legacy ``CognitiveCloseoutGate`` still governs active cognitive
    items.

    Rollout safety (fr_9d42c5e2 / dec_41db6a36): when ``policy_blocking`` is
    False (the default for existing boards, or the global flag off) this is a
    NO-OP — readiness stays advisory. Carve-out: a task/test (no reusable
    cognition) never blocks on the cognitive/advisory tiers, but the technical
    no-mask tiers (DLQ / open canonical_debt) still block when policy is active.

    Failure semantics: while ``policy_blocking`` is False this is a NO-OP. Once
    blocking is ACTIVE, a resolution/evaluation failure is fail-CLOSED with a
    visible ``cognitive_readiness_unavailable`` error BEFORE any mutation — a
    silent skip would make the enforcement point an appearance of control
    (validator carry-forward).
    """

    if not policy_blocking:
        return

    from okto_pulse.core.kg.cognitive_closeout_gate import (
        CognitiveCloseoutGateError,
        resolve_cognitive_source_refs,
    )
    from okto_pulse.core.kg.cognitive_readiness import GATE_BLOCKING_TIERS

    def _unavailable(reason: str) -> ValueError:
        return ValueError(
            f"cognitive_readiness_unavailable: {target_label} done transition "
            f"blocked — {reason} (blocking policy active)"
        )

    try:
        refs = resolve_cognitive_source_refs(
            entity_type=entity_type, entity=entity, entity_id=entity_id,
        ).source_refs
    except CognitiveCloseoutGateError as exc:
        # An entity type that is genuinely NOT eligible for cognitive closeout is
        # a safe no-op; any other gate error on a covered type is fail-closed.
        if getattr(exc, "code", "") == "unsupported_entity_type":
            return
        raise _unavailable(
            "source resolution failed "
            f"({getattr(exc, 'code', None) or type(exc).__name__})"
        ) from exc
    except Exception as exc:
        raise _unavailable(f"source resolution failed ({type(exc).__name__})") from exc
    if not refs:
        return

    # Carve-out: the entity's own ref is refs[0] (``<normalized_type>:<id>``).
    # task/test carry no reusable cognition → advisory for cognitive tiers (the
    # technical DLQ/debt no-mask tiers still apply via compose_readiness).
    primary_type = refs[0].split(":", 1)[0]
    has_reusable_cognition = primary_type not in ("task", "test")

    try:
        service = service_factory()
    except Exception as exc:
        raise _unavailable(
            f"readiness service unavailable ({type(exc).__name__})"
        ) from exc
    blocking_tiers = GATE_BLOCKING_TIERS
    for ref in refs:
        try:
            verdict = await service.evaluate_artifact(
                db,
                board_id=board_id,
                source_ref=ref,
                has_reusable_cognition=has_reusable_cognition,
            )
        except Exception as exc:
            raise _unavailable(
                f"readiness evaluation failed for {ref} ({type(exc).__name__})"
            ) from exc
        if verdict.blocking and verdict.tier in blocking_tiers:
            raise ValueError(
                f"{verdict.tier}: {target_label} done transition blocked by "
                "cognitive readiness "
                f"({verdict.readiness_signal or verdict.reason_code or verdict.tier})"
            )


async def _resolve_closeout_graph_state(
    board_id: str, db: AsyncSession
) -> str | None:
    """Resolve the board's current ``graph_state`` for the cognitive closeout
    gate (F16). Runs in the ASYNC caller where an ``AsyncSession`` is in scope
    and threads the result into the SYNC ``gate.evaluate(...)`` so the gate stays
    pure (no I/O).

    Fail-safe (FR6): on ANY failure (e.g. ``BoardNotFoundError``) or a missing
    ``graph_state`` key, return ``None`` — so the gate's ``resolved_generation``-
    is-None liveness check still governs and a degraded signal is never swallowed
    into ALLOWED. Reuses ``get_kg_health`` as-is (no new health-composition logic).
    """
    try:
        from okto_pulse.core.services.kg_health_service import get_kg_health

        health = await get_kg_health(board_id, db)
        state = health.get("graph_state")
    except Exception:
        return None
    return str(state) if state is not None else None


# ---------------------------------------------------------------------------
# Spec Validation Gate — exception and lock helper
# ---------------------------------------------------------------------------


class SpecLockedError(Exception):
    """Raised when a content-edit operation is attempted on a locked spec.

    A spec is locked when its current_validation_id points to a validation
    record with outcome='success'. To edit, the spec must be moved back to
    draft or approved (any backward transition from validated/in_progress/done),
    which atomically clears current_validation_id but preserves validations history.
    """

    def __init__(self, spec_id: str, current_validation_id: str | None = None, message: str | None = None):
        self.spec_id = spec_id
        self.current_validation_id = current_validation_id
        self.message = message or (
            "Spec is locked because validation passed. "
            "Move the spec back to draft or approved to edit (validation will be cleared, history preserved)."
        )
        super().__init__(self.message)


async def _require_spec_unlocked(db: AsyncSession, spec_id: str) -> None:
    """Raise SpecLockedError if spec has an active passed validation.

    Called at the top of every content-edit method on SpecService to enforce
    the Spec Validation Gate content lock. Skips silently when spec doesn't
    exist (caller handles that) or when no validation is active.
    """
    spec = await db.get(Spec, spec_id)
    if not spec:
        return
    current_id = getattr(spec, "current_validation_id", None)
    if not current_id:
        return
    validations = getattr(spec, "validations", None) or []
    current = next((v for v in validations if v.get("id") == current_id), None)
    if current and current.get("outcome") == "success":
        raise SpecLockedError(spec_id=spec_id, current_validation_id=current_id)


# ---------------------------------------------------------------------------
# Artifact propagation utility
# ---------------------------------------------------------------------------


def _filter_mockups(
    mockups: list[dict] | None,
    mockup_ids: list[str] | None,
) -> list[dict]:
    """Filter and copy mockups, adding origin_id for traceability."""
    if not mockups:
        return []
    source = mockups if mockup_ids is None else [m for m in mockups if m.get("id") in mockup_ids]
    copied = []
    for m in source:
        new_m = dict(m)
        new_m["origin_id"] = m.get("id")
        origin_token = f"{m.get('id')}{id(new_m)}"
        new_m["id"] = f"sm_{hashlib.md5(origin_token.encode()).hexdigest()[:8]}"
        copied.append(new_m)
    return copied


def _compile_qa_context(qa_items: list) -> str | None:
    """Compile answered Q&A items into a context section."""
    answered = [qa for qa in (qa_items or []) if getattr(qa, "answer", None) or (isinstance(qa, dict) and qa.get("answer"))]
    if not answered:
        return None
    lines = []
    for qa in answered:
        q = getattr(qa, "question", None) or qa.get("question", "")
        a = getattr(qa, "answer", None) or qa.get("answer", "")
        lines.append(f"**Q:** {q}\n**A:** {a}")
    return "## Q&A Decisions\n" + "\n\n".join(lines)


_PROPAGATED_KB_PREFIX = "[propagated from parent]"


def _propagated_kb_description(description: str | None) -> str:
    """R6-IMP1 (FR1/AC1) — apply the propagation marker AT MOST ONCE.

    In a multi-hop chain (ideation -> refinement -> spec -> card) the source KB
    already carries the prefix from the previous hop, because every hop copies the
    parent's (already-prefixed) description through this same path. Prepending
    again would stack ``[propagated from parent] [propagated from parent] ...``.
    Idempotent: if the stripped description already starts with the marker, return
    it unchanged; otherwise prepend once. Origin metadata (source_*/source_kb_id)
    is untouched — only the human-readable marker is normalized."""
    body = (description or "").strip()
    if body.startswith(_PROPAGATED_KB_PREFIX):
        return body
    return f"{_PROPAGATED_KB_PREFIX} {body}".strip()


async def propagate_artifacts(
    db: AsyncSession,
    source_mockups: list[dict] | None,
    source_qa_items: list | None,
    source_knowledge_bases: list | None,
    target_entity: Any,
    target_kb_class: type | None,
    user_id: str,
    mockup_ids: list[str] | None = None,
    kb_ids: list[str] | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    source_title: str | None = None,
    source_version: int | None = None,
) -> None:
    """Propagate mockups, KBs and Q&A from a parent entity to a target entity.

    - Mockups: copied as JSON with origin_id. Default=all, filter by mockup_ids.
    - KBs: copied as new DB rows with source metadata when the target model supports it.
    - Q&A: compiled into context (appended, not replaced).
    - Existing artifacts on target are preserved (additive, not replacement).
    """
    # Propagate mockups
    copied_mockups = _filter_mockups(source_mockups, mockup_ids)
    if copied_mockups:
        existing = list(target_entity.screen_mockups or [])
        new_set = existing + copied_mockups
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): a propagated/copied
        # mockup is a NEW entry on the target board — gate it (delta vs the existing set)
        # BEFORE assigning so a non-compliant mockup can't be laundered onto a blocking
        # board via propagation. Covers create_refinement propagation + copy_mockups_to_card.
        from okto_pulse.core.services.design_system import gate_entity_screen_mockups
        target_entity.screen_mockups = existing  # keep baseline for the gate's delta
        await gate_entity_screen_mockups(
            db, target_entity, new_set, entity_type=type(target_entity).__name__.lower()
        )
        target_entity.screen_mockups = new_set

    # Propagate knowledge bases (DB rows) — accepts ORM objects or dicts
    if target_kb_class and source_knowledge_bases:
        kbs = source_knowledge_bases if kb_ids is None else [
            kb for kb in source_knowledge_bases
            if (kb.get("id") if isinstance(kb, dict) else getattr(kb, "id", None)) in kb_ids
        ]
        # Determine FK field name from target_kb_class table
        target_id_field = None
        for col in ["spec_id", "refinement_id", "ideation_id"]:
            if hasattr(target_kb_class, col):
                target_id_field = col
                break
        if target_id_field:
            for kb in kbs:
                _get = (lambda k: kb.get(k)) if isinstance(kb, dict) else (lambda k: getattr(kb, k, None))
                kb_payload = {
                    target_id_field: target_entity.id,
                    "title": _get("title"),
                    # R6-IMP1: idempotent prefix — never stack across multi-hop chains.
                    "description": _propagated_kb_description(_get("description")),
                    "content": _get("content"),
                    "mime_type": _get("mime_type") or "text/markdown",
                    "created_by": user_id,
                }
                # R6-IMP4: multi-hop KB lineage. The immediate parent is the KB
                # being copied; the root is the parent's OWN root when it already
                # has one (so a 3rd hop keeps the canonical origin), else the
                # parent itself. source_kb_id stays == immediate parent (back-compat).
                parent_kb_id = _get("id")
                parent_root = _get("root_source_kb_id")
                source_values = {
                    "source_type": source_type,
                    "source_id": source_id,
                    "source_title": source_title,
                    "source_version": source_version,
                    "source_kb_id": parent_kb_id,
                    "immediate_parent_kb_id": parent_kb_id,
                    "root_source_kb_id": parent_root or parent_kb_id,
                }
                for attr, value in source_values.items():
                    if value is not None and hasattr(target_kb_class, attr):
                        kb_payload[attr] = value
                new_kb = target_kb_class(
                    **kb_payload,
                )
                db.add(new_kb)
            await db.flush()

    # Propagate Q&A items as proper QA rows on the target entity
    if source_qa_items:
        from okto_pulse.core.models.db import SpecQAItem, RefinementQAItem
        # Determine target QA class based on entity type
        target_qa_class = None
        target_fk_field = None
        if hasattr(target_entity, "spec_id") or target_entity.__tablename__ == "specs":
            target_qa_class = SpecQAItem
            target_fk_field = "spec_id"
        elif hasattr(target_entity, "refinement_id") or (hasattr(target_entity, "__tablename__") and target_entity.__tablename__ == "refinements"):
            target_qa_class = RefinementQAItem
            target_fk_field = "refinement_id"

        if target_qa_class and target_fk_field:
            for qa in source_qa_items:
                _get = (lambda k: qa.get(k)) if isinstance(qa, dict) else (lambda k: getattr(qa, k, None))
                # Only copy ANSWERED Q&A items. Choice questions (choice/
                # single_choice/multi_choice) store the answer in `selected`
                # and leave `answer` as None — the original `if not answer`
                # silently dropped every choice-type response, so derived
                # entities lost the decisions made on the parent. Treat the
                # item as answered when EITHER `answer` OR `selected` is set.
                answer = _get("answer")
                selected = _get("selected")
                has_selection = bool(selected) and len(selected) > 0
                if not answer and not has_selection:
                    continue
                qa_payload: dict[str, Any] = {
                    target_fk_field: target_entity.id,
                    "question": _get("question") or "",
                    "question_type": _get("question_type") or "text",
                    "choices": _get("choices"),
                    "allow_free_text": _get("allow_free_text") or False,
                    "answer": answer,
                    "selected": selected,
                    "asked_by": _get("asked_by") or user_id,
                    "answered_by": _get("answered_by"),
                    # `answered_at` DEVE acompanhar a resposta copiada: o badge
                    # open_qa_count usa `answered_at IS NULL` como definição de
                    # "aberta" (choice answers deixam `answer` NULL), então uma
                    # herança sem o timestamp marcava TODA Q&A respondida
                    # herdada como falso-aberta no refinement/spec derivado.
                    # Fallback para created_at/now cobre pais antigos que já
                    # perderam o timestamp em heranças anteriores ao fix —
                    # este branch só roda para itens RESPONDIDOS.
                    "answered_at": (
                        _get("answered_at")
                        or _get("created_at")
                        or datetime.now(timezone.utc)
                    ),
                }
                # Preserva a data original da pergunta quando disponível
                # (ordenacão/histórico); ausente, o default do modelo cobre.
                if _get("created_at") is not None:
                    qa_payload["created_at"] = _get("created_at")
                new_qa = target_qa_class(**qa_payload)
                db.add(new_qa)
            await db.flush()


async def resolve_actor_name(db: AsyncSession, user_id: str, board_id: str) -> str:
    """Resolve a user/agent ID to a friendly display name."""
    agent = await db.get(Agent, user_id)
    if agent:
        return agent.name
    board = await db.get(Board, board_id)
    if board and board.owner_id == user_id:
        return "Owner"
    if user_id == "dev-user":
        return "Owner"
    return user_id[:20]


async def propagate_architecture_designs(
    db: AsyncSession,
    *,
    source_parent_type: str,
    source_parent_id: str,
    target_parent_type: str,
    target_parent_id: str,
    actor_id: str,
    mode: str | None = "copy",
    design_ids: list[str] | None = None,
) -> list[Any]:
    """Propagate architecture designs between SDLC artifacts.

    Modes:
    - copy/derive: snapshot copy, retaining source_design_id/source_ref.
    - reference_only/none: no snapshot copy; parent linkage carries traceability.
    """
    normalized = (mode or "copy").strip().lower()
    if normalized not in {"copy", "derive", "reference_only", "none"}:
        raise ValueError(
            "architecture_propagation_mode must be one of: copy, derive, "
            "reference_only, none"
        )
    if normalized in {"reference_only", "none"}:
        return []

    from okto_pulse.core.models.schemas import ArchitectureWarningAcknowledgementRequest
    from okto_pulse.core.services.architecture import ArchitecturePropagationService

    # Bug eded2f0e (R3, option B): SDLC artifact propagation is an INTERNAL
    # snapshot copy of an already-acknowledged source architecture design — not a
    # new authoring action. The copy still gets its OWN copy-scoped acknowledgement
    # record (copy_from_parent enforces an explicit ack for warning-bearing copies;
    # the gate is NOT weakened), supplied here by the system on the artifact's
    # behalf so legitimate propagation is not blocked.
    return await ArchitecturePropagationService(db).copy_from_parent(
        source_parent_type=source_parent_type,
        source_parent_id=source_parent_id,
        target_parent_type=target_parent_type,
        target_parent_id=target_parent_id,
        actor_id=actor_id,
        design_ids=design_ids,
        architecture_warning_acknowledgement=ArchitectureWarningAcknowledgementRequest(
            accepted=True,
            statement=(
                f"internal snapshot propagation of an already-acknowledged "
                f"{source_parent_type} architecture design"
            ),
        ),
    )


class BoardService:
    """Service for board operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_board(self, user_id: str, data: BoardCreate, realm_id: str | None = None) -> Board:
        """Create a new board."""
        from okto_pulse.core.services.default_board_configuration import (
            BOARD_EVENT_APPLIED,
            BOARD_EVENT_FALLBACK,
            DefaultBoardConfigurationService,
        )

        # FR3: the single provider resolves the active default template (if any)
        # and produces the effective settings + snapshot metadata in THIS same
        # transaction. No active template -> graceful fallback (BoardSettings()
        # default, no snapshot, no error — AC11). Snapshot metadata is persisted on
        # Board.default_config_snapshot, OUTSIDE Board.settings (FR4).
        _config_service = DefaultBoardConfigurationService(self.db)
        effective_settings, snapshot_meta = await _config_service.build_snapshot_for_create(
            settings_override=getattr(data, "settings", None), applied_by=user_id
        )
        board = Board(
            name=data.name,
            description=data.description,
            owner_id=user_id,
            realm_id=realm_id,
            settings=effective_settings,
            default_config_snapshot=snapshot_meta,
        )
        self.db.add(board)
        await self.db.flush()
        actor_name = await resolve_actor_name(self.db, user_id, board.id)
        await self._log_activity(
            board_id=board.id,
            action="board_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"name": data.name},
        )
        # FR9: board-scoped audit of which default-config path created the board.
        if snapshot_meta is not None:
            await self._log_activity(
                board_id=board.id,
                action=BOARD_EVENT_APPLIED,
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details={
                    "template_id": snapshot_meta["template_id"],
                    "template_version": snapshot_meta["template_version"],
                    "override_summary": snapshot_meta["override_summary"],
                },
            )
        else:
            await self._log_activity(
                board_id=board.id,
                action=BOARD_EVENT_FALLBACK,
                actor_type="user",
                actor_id=user_id,
                actor_name=actor_name,
                details={"reason": "no_active_default_board_configuration"},
            )
        # FR5/FR6/#3/#4: the umbrella service orchestrates every default adapter
        # (guidelines + design system) onto the new board IN THIS transaction. Any
        # adapter failure raises default_materialization_failed so the whole
        # create_board reverts (no partial board/link/snapshot); no active
        # template -> no-op.
        await _config_service.apply_default_config_to_board(board.id, actor=user_id)
        # Eagerly bootstrap the per-board Kùzu graph. This keeps board
        # creation on the slow path (~1-2s) so subsequent consolidation /
        # MCP query paths stay on the hot path.
        # Failures are logged but don't abort board creation — the
        # lazy bootstrap in BoardConnection.__init__ is the safety net.
        try:
            from okto_pulse.core.kg.schema import ensure_board_graph_bootstrapped
            ensure_board_graph_bootstrapped(board.id)
        except Exception as exc:
            import logging
            logging.getLogger("okto_pulse.core.services.main").warning(
                "board_create.bootstrap_failed board=%s err=%s — lazy path will retry",
                board.id, exc,
            )
        return board

    async def get_board(self, board_id: str, user_id: str | None = None) -> Board | None:
        """Get a board by ID with all relationships."""
        query = (
            select(Board)
            .options(selectinload(Board.cards).selectinload(Card.attachments))
            .options(selectinload(Board.cards).selectinload(Card.qa_items))
            .options(selectinload(Board.cards).selectinload(Card.comments))
            .options(selectinload(Board.cards).selectinload(Card.architecture_designs))
            .where(Board.id == board_id)
        )
        if user_id:
            query = query.where(Board.owner_id == user_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_boards(
        self, user_id: str, offset: int = 0, limit: int = 20, realm_id: str | None = None,
        view: str = "my",
    ) -> tuple[list[Board], int]:
        """List boards for a user.

        view: "my" (owned), "shared" (shared with user), "all" (union)
        """
        from okto_pulse.core.models.db import BoardShare

        filters = []
        if realm_id:
            filters.append(Board.realm_id == realm_id)

        if view == "shared":
            # Boards shared with the user (not owned)
            base = (
                select(Board)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
            count_base = (
                select(func.count())
                .select_from(Board)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
        elif view == "all":
            # Owned OR shared
            owned = select(Board.id).where(Board.owner_id == user_id, *filters)
            shared = (
                select(Board.id)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
            combined_ids = owned.union(shared).subquery()
            base = select(Board).where(Board.id.in_(select(combined_ids)))
            count_base = select(func.count()).select_from(Board).where(Board.id.in_(select(combined_ids)))
        else:
            # "my" - owned boards only
            base = select(Board).where(Board.owner_id == user_id, *filters)
            count_base = select(func.count()).select_from(Board).where(Board.owner_id == user_id, *filters)

        total = (await self.db.execute(count_base)).scalar() or 0
        query = base.order_by(Board.updated_at.desc()).offset(offset).limit(limit)
        result = await self.db.execute(query)
        boards = list(result.scalars().all())
        return boards, total

    async def update_board(self, board_id: str, user_id: str, data: BoardUpdate) -> Board | None:
        """Update a board."""
        board = await self.get_board(board_id, user_id)
        if not board:
            return None

        previous_settings = dict(board.settings or {})
        update_data = data.model_dump(exclude_unset=True)
        # Serialize settings if present
        if "settings" in update_data and update_data["settings"] is not None:
            update_data["settings"] = BoardGovernanceService.merge_settings_patch(
                previous_settings,
                update_data["settings"],
            )
        for key, value in update_data.items():
            setattr(board, key, value)
            if key == "settings":
                flag_modified(board, "settings")

        settings_changed = "settings" in update_data and update_data.get("settings") is not None
        if settings_changed:
            next_settings = dict(board.settings or {})
            previous_auto = bool(previous_settings.get("auto_derive_spec_resources_enabled", False))
            next_auto = bool(next_settings.get("auto_derive_spec_resources_enabled", False))
            previous_types = list(previous_settings.get("auto_derive_spec_resource_types") or [])
            next_types = list(next_settings.get("auto_derive_spec_resource_types") or [])
            resource_automation_changed = (
                previous_auto != next_auto
                or previous_types != next_types
            )
            if next_auto and resource_automation_changed:
                await self.db.flush()
                await SpecResourcePropagationService(self.db).propagate_for_board(
                    board_id=board_id,
                    actor_id=user_id,
                    trigger="board_settings_auto_derive_changed",
                )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        if settings_changed:
            for setting_key, old_value, new_value in (
                BoardGovernanceService.changed_governance_settings(
                    previous_settings,
                    board.settings,
                )
            ):
                details = build_board_governance_setting_changed_details(
                    board_id=board_id,
                    actor_id=user_id,
                    setting_key=setting_key,
                    old_effective_value=old_value,
                    new_effective_value=new_value,
                    surface="board_patch",
                )
                await self._log_activity(
                    board_id=board_id,
                    action="board_governance_setting_changed",
                    actor_type="user",
                    actor_id=user_id,
                    actor_name=actor_name,
                    details=details,
                )
                emit_governance_metric(details, raise_on_violation=False)
        if "description" in update_data and not (board.description or "").strip():
            details = build_board_missing_context_warning_details(
                board_id=board_id,
                warning_code="board_summary_missing",
                surface="board_patch",
            )
            emit_governance_metric(details, raise_on_violation=False)
        await self._log_activity(
            board_id=board_id,
            action="board_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details=update_data,
        )
        return board

    async def delete_board(self, board_id: str, user_id: str) -> bool:
        """Delete a board."""
        board = await self.get_board(board_id, user_id)
        if not board:
            return False

        await self.db.delete(board)
        return True

    async def _log_activity(
        self,
        board_id: str,
        action: str,
        actor_type: str,
        actor_id: str,
        actor_name: str,
        card_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log an activity."""
        log = ActivityLog(
            board_id=board_id,
            card_id=card_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            details=details,
        )
        self.db.add(log)


class CardOperationError(ValueError):
    """Typed card workflow error for API/MCP callers."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        remediation: str | None = None,
        facts: dict[str, Any] | None = None,
        workflow_remediation: BugWorkflowRemediationMessage | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.facts = facts or {}
        self.workflow_remediation = workflow_remediation

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        if self.facts:
            payload["facts"] = self.facts
        if self.workflow_remediation:
            serialized = serialize_bug_workflow_remediation(self.workflow_remediation)
            if serialized:
                payload["remediation_message"] = serialized
                for key in (
                    "reason_code",
                    "remediation_path",
                    "next_action",
                    "semantic_gap_required",
                    "eligible_scenarios_count",
                    "hotfix_lane_status",
                    "actions",
                ):
                    payload[key] = serialized[key]
        return payload


class CardService:
    """Service for card operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self._cognitive_closeout_gate_factory: Callable[
            [], Any
        ] = _build_default_cognitive_closeout_gate
        self._cognitive_readiness_service_factory: Callable[
            [], Any
        ] = _build_default_cognitive_readiness_service

    async def create_card(
        self, board_id: str, user_id: str, data: CardCreate, skip_ownership_check: bool = False
    ) -> Card | None:
        """Create a new card in a board."""
        if skip_ownership_check:
            # Just verify the board exists (for MCP agents)
            board_query = select(Board).where(Board.id == board_id)
        else:
            # Check if board exists and user owns it (for REST API)
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        # --- Bug card validations (before spec check, since spec is auto-resolved) ---
        card_type_val = getattr(data, "card_type", "normal") or "normal"
        if card_type_val == "bug":
            if not data.origin_task_id:
                raise ValueError("origin_task_id is required for bug cards")

            # Validate origin task exists
            origin_task = await self.db.get(Card, data.origin_task_id)
            if not origin_task:
                raise ValueError("Origin task not found")

            # Validate origin task has a spec
            if not origin_task.spec_id:
                raise ValueError(
                    "Origin task has no linked spec — bug cards require a spec-linked task"
                )

            # Auto-resolve spec_id from origin task
            data.spec_id = origin_task.spec_id

            # Validate required bug fields
            if not data.severity:
                raise ValueError("severity is required for bug cards (critical, major, minor)")
            if not data.expected_behavior:
                raise ValueError("expected_behavior is required for bug cards")
            if not data.observed_behavior:
                raise ValueError("observed_behavior is required for bug cards")

            # Bug cards must start as not_started — they must go through
            # the move_card workflow to reach in_progress/done (which enforces
            # test task linkage). Prevent bypassing via create with status=done.
            if data.status not in (CardStatus.NOT_STARTED, CardStatus.STARTED):
                raise ValueError(
                    "Bug cards can only be created with status 'not_started' or 'started'. "
                    "Use move_card to advance status — this enforces test task linkage requirements."
                )

        # Enforce: every card must be linked to a spec
        if not data.spec_id:
            raise ValueError(
                "Every task must be linked to a spec. Provide spec_id when creating a card. "
                "If this task is not related to any spec, create a spec first."
            )

        # --- Test card validations ---
        if card_type_val == "test":
            if not data.test_scenario_ids:
                raise ValueError(
                    "test_scenario_ids is required for test cards and must contain at least one scenario ID"
                )

        # Enforce: spec status rules for card creation
        # - Normal tasks: spec must be 'approved' or 'in_progress'
        # - Bug cards: also allowed when spec is 'done'
        # - Test cards: also allowed when spec is 'validated'
        spec = await self.db.get(Spec, data.spec_id)
        if not spec:
            raise ValueError(f"Spec '{data.spec_id}' not found")

        if card_type_val == "bug":
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'in_progress', or 'done'"
        elif card_type_val == "test":
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'validated', 'in_progress', or 'done'"
        else:
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'in_progress', or 'done'"

        if spec.status not in allowed_statuses:
            raise ValueError(
                f"{card_type_val.capitalize()} cards can only be created for specs in {status_msg} status. "
                f"Spec '{spec.title}' is currently '{spec.status.value}'."
            )

        # Validate test_scenario_ids against spec for test cards
        if card_type_val == "test" and data.test_scenario_ids:
            spec_scenario_ids = {s["id"] for s in (spec.test_scenarios or [])}
            invalid_ids = [sid for sid in data.test_scenario_ids if sid not in spec_scenario_ids]
            if invalid_ids:
                raise ValueError(
                    f"Test scenario(s) not found in spec '{spec.title}': {invalid_ids}. "
                    f"Available scenarios: {sorted(spec_scenario_ids)}"
                )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=board_id,
            actor_id=user_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.CARD_CREATE,
            surface="service",
            actor_type="user",
        )

        # Get max position for the status column
        pos_query = (
            select(func.max(Card.position))
            .where(Card.board_id == board_id, Card.status == data.status)
        )
        max_pos = (await self.db.execute(pos_query)).scalar() or -1

        card = Card(
            board_id=board_id,
            spec_id=data.spec_id,
            title=data.title,
            description=data.description,
            details=data.details,
            status=data.status,
            priority=data.priority,
            position=max_pos + 1,
            assignee_id=data.assignee_id,
            created_by=user_id,
            due_date=data.due_date,
            labels=data.labels,
            test_scenario_ids=data.test_scenario_ids,
            card_type=card_type_val,
            origin_task_id=getattr(data, "origin_task_id", None),
            severity=getattr(data, "severity", None),
            expected_behavior=getattr(data, "expected_behavior", None),
            observed_behavior=getattr(data, "observed_behavior", None),
            steps_to_reproduce=getattr(data, "steps_to_reproduce", None),
            action_plan=getattr(data, "action_plan", None),
        )
        self.db.add(card)
        await self.db.flush()

        if card_type_val == "bug":
            await self._inherit_bug_origin_traceability(
                bug_card=card,
                origin_task_id=getattr(data, "origin_task_id", None),
                spec=spec,
            )

        await SpecResourcePropagationService(self.db).propagate_for_card(
            board_id=board_id,
            spec_id=card.spec_id,
            card_id=card.id,
            actor_id=user_id,
            trigger="card_created",
        )

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardCreated

        await event_publish(
            CardCreated(
                board_id=board_id,
                actor_id=user_id,
                card_id=card.id,
                spec_id=card.spec_id,
                sprint_id=card.sprint_id,
                card_type=card_type_val,
                priority=data.priority.value,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            card_id=card.id,
            action="card_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "status": data.status.value},
        )
        return card

    async def _inherit_bug_origin_traceability(
        self,
        *,
        bug_card: Card,
        origin_task_id: str | None,
        spec: Spec | None = None,
    ) -> None:
        """Attach a new bug to the same spec traceability items as its origin task."""
        if not origin_task_id or not bug_card.spec_id:
            return

        if spec is None:
            spec = await self.db.get(Spec, bug_card.spec_id)
        if spec is None:
            return

        inherited_scenario_ids: list[str] = []

        def inherit_linked_task_ids(field_name: str, *, collect_scenarios: bool = False) -> None:
            items = getattr(spec, field_name, None) or []
            changed = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                linked_task_ids = list(item.get("linked_task_ids") or [])
                origin_is_linked = origin_task_id in linked_task_ids
                if origin_is_linked and bug_card.id not in linked_task_ids:
                    linked_task_ids.append(bug_card.id)
                    item["linked_task_ids"] = linked_task_ids
                    changed = True
                if collect_scenarios and origin_is_linked:
                    scenario_id = item.get("id")
                    if scenario_id and scenario_id not in inherited_scenario_ids:
                        inherited_scenario_ids.append(scenario_id)
            if changed:
                flag_modified(spec, field_name)

        inherit_linked_task_ids("test_scenarios", collect_scenarios=True)
        inherit_linked_task_ids("business_rules")
        inherit_linked_task_ids("api_contracts")
        inherit_linked_task_ids("integration_requirements")
        inherit_linked_task_ids("observability_requirements")
        inherit_linked_task_ids("technical_requirements")
        inherit_linked_task_ids("decisions")

        if inherited_scenario_ids:
            current_scenarios = list(bug_card.test_scenario_ids or [])
            merged = current_scenarios + [
                scenario_id
                for scenario_id in inherited_scenario_ids
                if scenario_id not in current_scenarios
            ]
            if merged != current_scenarios:
                bug_card.test_scenario_ids = merged
                flag_modified(bug_card, "test_scenario_ids")

    async def get_card(self, card_id: str) -> Card | None:
        """Get a card by ID with all relationships."""
        query = (
            select(Card)
            .options(selectinload(Card.attachments))
            .options(selectinload(Card.qa_items))
            .options(selectinload(Card.comments))
            .options(selectinload(Card.architecture_designs))
            .where(Card.id == card_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def update_card(self, card_id: str, user_id: str, data: CardUpdate) -> Card | None:
        """Update a card."""
        card = await self.get_card(card_id)
        if not card:
            return None

        if getattr(card, "archived", False):
            raise ValueError(
                "This card is archived. Restore it first using restore_tree before making changes."
            )

        update_data = data.model_dump(exclude_unset=True)

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=user_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=CriticalAction.CARD_UPDATE,
            surface="service",
            actor_type="user",
            card_id=card.id,
        )

        # spec 28583299 (Ideação #4, IMPL-C): snapshot priority/severity BEFORE
        # mutation so the DomainEvent payload carries the actual transition.
        # In-memory mutation may leave enums as raw strings (Pydantic dump);
        # _enum_value handles both shapes uniformly.
        def _enum_value(value):
            if value is None:
                return None
            return getattr(value, "value", value)

        old_priority = _enum_value(card.priority)
        old_severity = _enum_value(getattr(card, "severity", None))
        old_spec_id = card.spec_id

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, card, update_data["screen_mockups"], entity_type="card"
            )

        card_json_fields = {"labels", "test_scenario_ids", "conclusions", "screen_mockups", "knowledge_bases"}
        for key, value in update_data.items():
            setattr(card, key, value)
            if key in card_json_fields:
                flag_modified(card, key)

        if "spec_id" in update_data and card.spec_id and card.spec_id != old_spec_id:
            await SpecResourcePropagationService(self.db).propagate_for_card(
                board_id=card.board_id,
                spec_id=card.spec_id,
                card_id=card.id,
                actor_id=user_id,
                trigger="card_linked_via_update",
            )

        actor_name = await resolve_actor_name(self.db, user_id, card.board_id)
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details=update_data,
        )

        # spec 28583299 (Ideação #4, FR6/FR7 + api_21467ada/api_ff834434):
        # emit a typed event when priority or severity changed so the
        # consolidation worker recomputes priority_boost on the KG node.
        new_priority = _enum_value(card.priority)
        new_severity = _enum_value(getattr(card, "severity", None))
        card_type_value = _enum_value(card.card_type)

        if "priority" in update_data and old_priority != new_priority:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardPriorityChanged

            await event_publish(
                CardPriorityChanged(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    old_priority=old_priority,
                    new_priority=new_priority,
                    spec_id=card.spec_id,
                    changed_by=user_id,
                ),
                session=self.db,
            )

        # BR1: severity transitions only matter for Bug cards.
        if (
            card_type_value == "bug"
            and "severity" in update_data
            and old_severity != new_severity
        ):
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardSeverityChanged

            await event_publish(
                CardSeverityChanged(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    old_severity=old_severity,
                    new_severity=new_severity,
                    spec_id=card.spec_id,
                    changed_by=user_id,
                ),
                session=self.db,
            )

        return card

    # ---- Dependency methods ----

    async def add_dependency(
        self, card_id: str, depends_on_id: str
    ) -> CardDependency | None:
        """Add a dependency. Returns None if circular."""
        if card_id == depends_on_id:
            return None
        # Check circular
        if await self._would_create_cycle(card_id, depends_on_id):
            return None
        dep = CardDependency(card_id=card_id, depends_on_id=depends_on_id)
        self.db.add(dep)
        await self.db.flush()
        return dep

    async def remove_dependency(self, card_id: str, depends_on_id: str) -> bool:
        stmt = delete(CardDependency).where(
            CardDependency.card_id == card_id,
            CardDependency.depends_on_id == depends_on_id,
        )
        result = await self.db.execute(stmt)
        return result.rowcount > 0

    async def get_dependencies(self, card_id: str) -> list[Card]:
        """Get cards that this card depends on."""
        query = (
            select(Card)
            .join(CardDependency, CardDependency.depends_on_id == Card.id)
            .where(CardDependency.card_id == card_id)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_dependents(self, card_id: str) -> list[Card]:
        """Get cards that depend on this card."""
        query = (
            select(Card)
            .join(CardDependency, CardDependency.card_id == Card.id)
            .where(CardDependency.depends_on_id == card_id)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def check_dependencies_met(self, card_id: str) -> tuple[bool, list[str]]:
        """Check if all dependencies are met (done or cancelled).
        Returns (all_met, list_of_blocking_card_titles).
        """
        deps = await self.get_dependencies(card_id)
        blocking = [
            d.title for d in deps
            if d.status not in (CardStatus.DONE, CardStatus.CANCELLED)
        ]
        return len(blocking) == 0, blocking

    async def _would_create_cycle(self, card_id: str, new_dep_id: str) -> bool:
        """Check if adding card_id -> new_dep_id would create a cycle.
        A cycle exists if new_dep_id (directly or transitively) depends on card_id.
        """
        visited: set[str] = set()
        stack = [new_dep_id]
        while stack:
            current = stack.pop()
            if current == card_id:
                return True
            if current in visited:
                continue
            visited.add(current)
            # Get what 'current' depends on
            query = select(CardDependency.depends_on_id).where(
                CardDependency.card_id == current
            )
            result = await self.db.execute(query)
            for (dep_id,) in result.all():
                stack.append(dep_id)
        return False

    # ---- Status progression order ----
    _STATUS_ORDER = {
        CardStatus.NOT_STARTED: 0,
        CardStatus.STARTED: 1,
        CardStatus.IN_PROGRESS: 2,
        CardStatus.VALIDATION: 2,  # same level as in_progress — lateral move into gate
        CardStatus.ON_HOLD: 2,  # same level — lateral move
        CardStatus.DONE: 3,
        CardStatus.CANCELLED: 3,
    }

    # ---- Task Validation Gate ----

    def _resolve_validation_config(
        self, card: Card, spec: "Spec | None", sprint: "Sprint | None", board_settings: dict
    ) -> dict:
        """Resolve validation gate config from hierarchy: sprint → spec → board.

        Returns dict with: required (bool), min_confidence, min_completeness, max_drift, resolved_from.
        """
        # Defaults from board settings
        board_required = board_settings.get("require_task_validation", True)
        board_min_conf = board_settings.get("min_confidence", 70)
        board_min_comp = board_settings.get("min_completeness", 80)
        board_max_drift = board_settings.get("max_drift", 50)

        # Spec overrides
        spec_required = getattr(spec, "require_task_validation", None) if spec else None
        spec_min_conf = getattr(spec, "validation_min_confidence", None) if spec else None
        spec_min_comp = getattr(spec, "validation_min_completeness", None) if spec else None
        spec_max_drift = getattr(spec, "validation_max_drift", None) if spec else None

        # Sprint overrides
        spr_required = getattr(sprint, "require_task_validation", None) if sprint else None
        spr_min_conf = getattr(sprint, "validation_min_confidence", None) if sprint else None
        spr_min_comp = getattr(sprint, "validation_min_completeness", None) if sprint else None
        spr_max_drift = getattr(sprint, "validation_max_drift", None) if sprint else None

        # Resolve with null-coalescing: sprint ?? spec ?? board
        def _coalesce(*vals, default):
            for v in vals:
                if v is not None:
                    return v
            return default

        required = _coalesce(spr_required, spec_required, board_required, default=False)
        resolved_from = "board"
        if spr_required is not None:
            resolved_from = "sprint"
        elif spec_required is not None:
            resolved_from = "spec"

        return {
            "required": bool(required),
            "min_confidence": _coalesce(spr_min_conf, spec_min_conf, board_min_conf, default=70),
            "min_completeness": _coalesce(spr_min_comp, spec_min_comp, board_min_comp, default=80),
            "max_drift": _coalesce(spr_max_drift, spec_max_drift, board_max_drift, default=50),
            "resolved_from": resolved_from,
        }

    async def submit_task_validation(
        self,
        card_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a task validation for a card in 'validation' status.

        Executes threshold check, computes outcome, persists validation,
        and routes card (success→done, failed stays in validation).
        """
        import uuid as _uuid

        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")

        if card.status != CardStatus.VALIDATION:
            raise ValueError(
                f"Card is not in 'validation' status (currently '{card.status.value}'). "
                f"Only cards in 'validation' status can receive validations."
            )
        old_status = card.status

        if getattr(card, "card_type", CardType.NORMAL) == CardType.TEST:
            # R4-IMP1: normalized contract pointing at the test-card operational
            # path (scenario status update + move_card done). Same rejection.
            from okto_pulse.core.services.gate_contracts import (
                task_validation_unsupported_for_test_card_error,
            )
            raise task_validation_unsupported_for_test_card_error(
                card_id=card.id, board_id=card.board_id, spec_id=card.spec_id,
            )

        # Resolve thresholds from hierarchy
        board = await self.db.get(Board, card.board_id)
        board_settings = board.settings or {} if board else {}
        spec = await self.db.get(Spec, card.spec_id) if card.spec_id else None
        sprint = await self.db.get(Sprint, card.sprint_id) if card.sprint_id else None
        config = self._resolve_validation_config(card, spec, sprint, board_settings)

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=reviewer_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=CriticalAction.CARD_SUBMIT_VALIDATION,
            surface="service",
            actor_type="agent",
            actor_name=reviewer_name,
            card_id=card.id,
        )

        # Extract scores
        confidence = data["confidence"]
        completeness = data["estimated_completeness"]
        drift = data["estimated_drift"]
        recommendation = data["recommendation"]

        # Threshold check
        violations = []
        if confidence < config["min_confidence"]:
            violations.append(f"confidence {confidence} < min {config['min_confidence']}")
        if completeness < config["min_completeness"]:
            violations.append(f"completeness {completeness} < min {config['min_completeness']}")
        if drift > config["max_drift"]:
            violations.append(f"drift {drift} > max {config['max_drift']}")

        # Compute outcome
        if violations or recommendation == "reject":
            outcome = "failed"
        else:
            outcome = "success"

        if outcome == "success":
            graph_state = await _resolve_closeout_graph_state(card.board_id, self.db)
            _evaluate_cognitive_closeout_or_raise(
                gate_factory=self._cognitive_closeout_gate_factory,
                board=board,
                board_id=card.board_id,
                entity_type=_card_cognitive_entity_type(card),
                entity_id=card.id,
                entity=card,
                target_label="card",
                graph_state=graph_state,
            )
            await _evaluate_cognitive_readiness_or_raise(
                service_factory=self._cognitive_readiness_service_factory,
                db=self.db,
                board_id=card.board_id,
                entity_type=_card_cognitive_entity_type(card),
                entity_id=card.id,
                entity=card,
                target_label="card",
                policy_blocking=_cognitive_readiness_blocking_active(board),
            )

        # Build validation entry.
        # Dual naming: we persist BOTH the legacy names (estimated_*, outcome, reviewer_id,
        # general_justification) and the clean frontend-compatible names (completeness, drift,
        # verdict, evaluator_id, summary). This keeps backward compat for any downstream code
        # that reads the legacy names while allowing the IDE ValidationsTab (which reads the
        # clean names) to render correctly. Going forward, consumers should prefer the clean
        # names; the legacy aliases can be removed in a future cleanup.
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        _general = data["general_justification"].strip()
        validation = {
            "id": validation_id,
            "card_id": card_id,
            "board_id": card.board_id,
            # Reviewer — legacy name + clean alias for frontend
            "reviewer_id": reviewer_id,
            "evaluator_id": reviewer_id,
            # Confidence
            "confidence": confidence,
            "confidence_justification": data["confidence_justification"].strip(),
            # Completeness — legacy estimated_* + clean name
            "estimated_completeness": completeness,
            "completeness": completeness,
            "completeness_justification": data["completeness_justification"].strip(),
            # Drift — legacy estimated_* + clean name
            "estimated_drift": drift,
            "drift": drift,
            "drift_justification": data["drift_justification"].strip(),
            # General justification — legacy + frontend "summary" alias
            "general_justification": _general,
            "summary": _general,
            # Recommendation + outcome — legacy "outcome" + frontend "verdict" alias
            "recommendation": recommendation,
            "outcome": outcome,
            "verdict": "pass" if outcome == "success" else "fail",
            "threshold_violations": violations,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Persist validation (append-only)
        validations = list(card.validations or [])
        validations.append(validation)
        card.validations = validations
        flag_modified(card, "validations")

        # Auto-populate conclusion only for legacy cards that reached validation
        # before execution reports were required on the validation handoff.
        conclusions_list = list(card.conclusions or [])
        has_executor_report = any(
            isinstance(entry, dict) and entry.get("source") == "move_to_validation"
            for entry in conclusions_list
        )
        if outcome == "success" and not has_executor_report:
            conclusions_list.append({
                "text": _general,
                "author_id": reviewer_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completeness": completeness,
                "completeness_justification": data["completeness_justification"].strip(),
                "drift": drift,
                "drift_justification": data["drift_justification"].strip(),
                "source": "task_validation",
                "validation_id": validation_id,
            })
            card.conclusions = conclusions_list
            flag_modified(card, "conclusions")

            # Spec 4007e4a3 (Ideação #3): re-enqueue parent spec via
            # CardConclusionAdded so the KG reflects the card's narrative
            # outcome alongside its final state. Orphan cards (spec_id=None)
            # are handled gracefully by the enqueuer.
            if _general:
                from okto_pulse.core.events import publish as event_publish
                from okto_pulse.core.events.types import CardConclusionAdded

                await event_publish(
                    CardConclusionAdded(
                        board_id=card.board_id,
                        actor_id=reviewer_id,
                        card_id=card_id,
                        spec_id=card.spec_id,
                        conclusion_excerpt=_general[:280],
                        added_by=reviewer_id,
                    ),
                    session=self.db,
                )

        # Route card based on outcome (atomic with validation persist).
        # NC-7 fix: outcome=failed keeps the card in VALIDATION instead of
        # bouncing back to NOT_STARTED. This avoids forcing the operator to
        # re-walk the whole state machine just to retry a threshold tweak;
        # the failed validation entry is appended to card.validations so the
        # history is preserved.
        if outcome == "success":
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                card.board_id,
                "card",
                card.id,
                phase="task_validation_success",
            )
            card.status = CardStatus.DONE
        else:
            card.status = CardStatus.VALIDATION

        # Auto-position at end of target column
        pos_query = (
            select(func.max(Card.position))
            .where(Card.board_id == card.board_id, Card.status == card.status)
        )
        max_pos = (await self.db.execute(pos_query)).scalar() or -1
        card.position = max_pos + 1

        if old_status != card.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardMoved

            await event_publish(
                CardMoved(
                    board_id=card.board_id,
                    actor_id=reviewer_id,
                    card_id=card.id,
                    from_status=old_status.value,
                    to_status=card.status.value,
                    spec_id=card.spec_id,
                    moved_by=reviewer_id,
                ),
                session=self.db,
            )

        # Activity log
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="validation_submitted",
            actor_type="agent",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "validation_id": validation_id,
                "outcome": outcome,
                "recommendation": recommendation,
                "confidence": confidence,
                "estimated_completeness": completeness,
                "estimated_drift": drift,
                "threshold_violations": violations,
                "card_title": card.title,
            },
        )

        return {
            **validation,
            "card_status": card.status.value,
            "resolved_thresholds": config,
        }

    async def list_task_validations(self, card_id: str) -> list[dict]:
        """List all validations for a card in reverse chronological order."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = list(card.validations or [])
        validations.reverse()
        return validations

    async def get_task_validation(self, card_id: str, validation_id: str) -> dict | None:
        """Get a single validation by ID."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        for v in (card.validations or []):
            if v.get("id") == validation_id:
                return v
        return None

    async def delete_task_validation(self, card_id: str, validation_id: str, user_id: str) -> bool:
        """Delete a validation entry. Requires card.validation.delete permission."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = list(card.validations or [])
        new_validations = [v for v in validations if v.get("id") != validation_id]
        if len(new_validations) == len(validations):
            return False
        card.validations = new_validations
        flag_modified(card, "validations")
        return True

    async def confirm_amendment_coverage(
        self,
        *,
        amendment_id: str,
        regression_test_task_id: str,
        regression_scenario_id: str,
        reviewer_id: str,
        reviewer_name: str,
    ) -> dict:
        """Validator-only writer of the Path B coverage attestation (G2 / c9cf9781).

        Enforces, fail-closed, BEFORE persisting:
        * artifact binding — the test task + scenario MUST be declared by THIS
          amendment (regression_test_task_ids / regression_scenario_ids);
        * real validator identity — the same critical-context authorization the
          task-validation gate uses (not a free-text validator_id);
        * reexecutable evidence (NECESSARY, not sufficient) — the regression test
          task is DONE and its declared scenario is passed/automated with SPEC3
          reexecutable evidence (test_file_path+test_function or test_run_id).
        Persists the bound attestation via the single reserved-key writer. The bug
        gate later DERIVES coverage_confirmed from this record (never a passed
        bool), so a generic/forged metadata write cannot grant coverage."""
        from okto_pulse.core.services.amendment_revision import AmendmentRevisionService

        svc = AmendmentRevisionService(self.db)
        amendment = await svc.get(amendment_id)
        if amendment is None:
            raise ValueError(f"Amendment '{amendment_id}' not found")

        # 1. binding: the artifact MUST be declared by THIS amendment.
        if regression_test_task_id not in (amendment.regression_test_task_ids or []):
            raise CardOperationError(
                "coverage_binding_invalid",
                f"Regression test task '{regression_test_task_id}' is not declared by "
                f"amendment '{amendment_id}'. Coverage can only be confirmed for an "
                "artifact bound to this amendment.",
                remediation="bind_regression_artifact_to_amendment",
                facts={"amendment_id": amendment_id},
            )
        if regression_scenario_id not in (amendment.regression_scenario_ids or []):
            raise CardOperationError(
                "coverage_binding_invalid",
                f"Regression scenario '{regression_scenario_id}' is not declared by "
                f"amendment '{amendment_id}'.",
                remediation="bind_regression_artifact_to_amendment",
                facts={"amendment_id": amendment_id},
            )

        test_task = await self.db.get(Card, regression_test_task_id)
        if not test_task or test_task.board_id != amendment.board_id:
            raise ValueError(
                f"Regression test task '{regression_test_task_id}' not found on this board"
            )

        # 2. real validator identity — same critical-context gate as task validation.
        await _authorize_critical_context_or_raise(
            self.db,
            board_id=amendment.board_id,
            actor_id=reviewer_id,
            entity_type="card",
            entity_id=test_task.id,
            critical_action=CriticalAction.CARD_SUBMIT_VALIDATION,
            surface="service",
            actor_type="agent",
            actor_name=reviewer_name,
            card_id=test_task.id,
        )

        # 3. reexecutable evidence is NECESSARY (not sufficient): test task done +
        #    declared scenario passed/automated with SPEC3 reexecutable evidence.
        if test_task.status != CardStatus.DONE:
            raise CardOperationError(
                "coverage_precondition_unmet",
                f"Regression test task '{regression_test_task_id}' is not done "
                f"(status='{getattr(test_task.status, 'value', test_task.status)}').",
                remediation="complete_regression_test_task",
                facts={"amendment_id": amendment_id},
            )
        evidence_ref = await self._reexecutable_evidence_ref(
            test_task, regression_scenario_id
        )
        if not evidence_ref:
            raise CardOperationError(
                "coverage_precondition_unmet",
                f"Scenario '{regression_scenario_id}' has no reexecutable evidence "
                "(needs status passed/automated with test_file_path+test_function or "
                "test_run_id). Lineage + a generic status are NOT sufficient (G2).",
                remediation="attach_reexecutable_evidence",
                facts={"amendment_id": amendment_id},
            )

        confirmation = {
            "validator_id": reviewer_id,
            "amendment_revision_id": amendment.id,
            "regression_test_task_id": regression_test_task_id,
            "regression_scenario_id": regression_scenario_id,
            "evidence_ref": evidence_ref,
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
        await svc.set_coverage_confirmation(
            amendment_id, confirmation=confirmation, actor=reviewer_id
        )
        return confirmation

    async def _reexecutable_evidence_ref(self, test_task: Card, scenario_id: str) -> str:
        """Reexecutable evidence ref for the scenario if it is passed/automated
        with SPEC3 evidence, else ''. Searches the test task's spec first, then the
        board's specs (a Path B regression scenario may be cross-spec)."""
        spec_ids: list[str] = []
        if test_task.spec_id:
            spec_ids.append(str(test_task.spec_id))
        rows = await self.db.execute(
            select(Spec.id).where(Spec.board_id == test_task.board_id)
        )
        spec_ids.extend(str(sid) for (sid,) in rows.all())
        seen: set[str] = set()
        for spec_id in spec_ids:
            if spec_id in seen:
                continue
            seen.add(spec_id)
            spec = await self.db.get(Spec, spec_id)
            if not spec:
                continue
            for sc in (spec.test_scenarios or []):
                if not isinstance(sc, dict) or str(sc.get("id")) != scenario_id:
                    continue
                if str(sc.get("status") or "").lower() not in ("passed", "automated"):
                    return ""
                ev = sc.get("evidence")
                if isinstance(ev, dict):
                    fp = str(ev.get("test_file_path") or "").strip()
                    fn = str(ev.get("test_function") or "").strip()
                    if fp and fn:
                        return f"{fp}::{fn}"
                    trid = str(ev.get("test_run_id") or "").strip()
                    if trid:
                        return f"test_run:{trid}"
                return ""
        return ""

    # ---- Coverage gate functions (used by SpecService.move_spec) ----

    async def check_ac_scenario_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every acceptance criterion is covered by at least one test scenario.

        Mirrors the AC→Scenario gate enforced at move_spec→done, but runs at
        submit_spec_validation time so the failure surfaces BEFORE the spec is
        locked. Without this pre-check, validation could succeed (locking the
        spec) and then move→done would fail because uncovered ACs cannot be
        addressed without first unlocking and resubmitting validation.
        """
        skip_global = (board.settings or {}).get("skip_test_coverage_global", False) if board else False
        if spec.skip_test_coverage or skip_global:
            return
        criteria = list(spec.acceptance_criteria or [])
        scenarios = list(spec.test_scenarios or [])
        if not criteria:
            return
        covered_indices: set[int] = set()
        for scenario in scenarios:
            covered_indices |= resolve_linked_criteria_to_indices(
                scenario.get("linked_criteria"),
                criteria,
            )
        uncovered = [
            f"[{i}] {_structured_ref_text(criterion)[:80]}..."
            for i, criterion in enumerate(criteria)
            if i not in covered_indices
        ]
        if uncovered:
            raise ValueError(
                f"Cannot validate spec: {len(uncovered)} acceptance criteria lack test scenarios. "
                f"Uncovered: {'; '.join(uncovered[:5])}"
                f"{f' (and {len(uncovered) - 5} more)' if len(uncovered) > 5 else ''}. "
                f"Create test scenarios linked to each AC BEFORE submitting validation — "
                f"once validation passes the spec is locked and scenarios cannot be added. "
                f"Alternatively, enable 'skip test coverage' on the spec or board."
            )

    async def check_test_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every test scenario has at least one linked card of type TEST."""
        skip_global = (board.settings or {}).get("skip_test_coverage_global", False) if board else False
        if spec.skip_test_coverage or skip_global:
            return
        scenarios = list(spec.test_scenarios or [])
        if not scenarios:
            return
        # Collect all card IDs from linked_task_ids across scenarios
        all_card_ids: set[str] = set()
        for s in scenarios:
            for cid in (s.get("linked_task_ids") or []):
                all_card_ids.add(cid)
        # Batch query to get card_type for all linked cards
        test_card_ids: set[str] = set()
        if all_card_ids:
            result = await self.db.execute(
                select(Card.id, Card.card_type).where(Card.id.in_(all_card_ids))
            )
            for cid, ctype in result.all():
                if ctype == CardType.TEST:
                    test_card_ids.add(cid)
        # Check each scenario has at least one TEST card
        unlinked = []
        for s in scenarios:
            task_ids = s.get("linked_task_ids") or []
            has_test = any(tid in test_card_ids for tid in task_ids)
            if not has_test:
                unlinked.append(s)
        if unlinked:
            titles = ", ".join(f'"{s["title"]}"' for s in unlinked[:3])
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} test scenario(s) "
                f"in spec '{spec.title}' have no linked test cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Create test cards (card_type='test') with test_scenario_ids "
                f"for each uncovered scenario. Only cards of type 'test' count for coverage. "
                f"Alternatively, enable 'skip test coverage' on the spec or board."
            )

    async def check_rules_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every FR has a BR and every BR has a linked task."""
        skip_global = (board.settings or {}).get("skip_rules_coverage_global", False) if board else False
        if getattr(spec, "skip_rules_coverage", False) or skip_global:
            return
        frs = list(spec.functional_requirements or [])
        brs = list(spec.business_rules or [])
        if not frs:
            return
        # Check FR → BR coverage. Structured-FR aware: resolve linked_requirements
        # (0-based index, exact/substring FR text, or fr_ id) to FR indices via the
        # shared resolver, so the gate works whether FRs are structured dicts
        # {id,text,status} or legacy strings (the old inline loop did `ref in fr`
        # where `fr` could be a dict -> TypeError / missed coverage).
        covered_fr_indices: set[int] = set()
        for br in brs:
            if isinstance(br, dict):
                covered_fr_indices |= resolve_linked_fr_indices(
                    br.get("linked_requirements") or [], frs
                )
        uncovered = [
            (i, _structured_ref_text(fr))
            for i, fr in enumerate(frs)
            if i not in covered_fr_indices
        ]
        if uncovered:
            previews = ", ".join(
                f'"[{i}] {text[:40]}..."' if len(text) > 40 else f'"[{i}] {text}"'
                for i, text in uncovered[:3]
            )
            suffix = f" and {len(uncovered) - 3} more" if len(uncovered) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(uncovered)} functional requirement(s) "
                f"in spec '{spec.title}' have no linked business rules "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Create business rules with linked_requirements "
                f"for each uncovered FR. "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )
        # Check BR → Task coverage
        unlinked_rules = [
            br for br in brs
            if isinstance(br, dict) and not br.get("linked_task_ids")
        ]
        if unlinked_rules:
            titles = ", ".join(
                f'"{br.get("title", br.get("id", "?"))}"'
                for br in unlinked_rules[:3]
            )
            suffix = f" and {len(unlinked_rules) - 3} more" if len(unlinked_rules) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_rules)} business rule(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each business rule via "
                f"okto_pulse_link_task_to_rule. "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )

    async def check_trs_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every structured TR has a linked task."""
        skip_global = (board.settings or {}).get("skip_trs_coverage_global", False) if board else False
        if getattr(spec, "skip_trs_coverage", False) or skip_global:
            return
        trs = list(spec.technical_requirements or [])
        structured_trs = [tr for tr in trs if isinstance(tr, dict) and tr.get("id")]
        if not structured_trs:
            return
        unlinked_trs = [tr for tr in structured_trs if not tr.get("linked_task_ids")]
        if unlinked_trs:
            previews = ", ".join(
                f'"{tr.get("text", tr.get("id", "?"))[:40]}"'
                for tr in unlinked_trs[:3]
            )
            suffix = f" and {len(unlinked_trs) - 3} more" if len(unlinked_trs) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_trs)} technical requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each TR via "
                f"okto_pulse_link_task_to_tr. "
                f"Alternatively, enable 'skip TRs coverage' on the spec or board."
            )

    async def check_contract_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every API contract has a linked task."""
        skip_global = (board.settings or {}).get("skip_contract_coverage_global", False) if board else False
        if getattr(spec, "skip_contract_coverage", False) or skip_global:
            return
        contracts = list(spec.api_contracts or [])
        if not contracts:
            return
        unlinked = [c for c in contracts if not c.get("linked_task_ids")]
        if unlinked:
            previews = ", ".join(
                f'"{c.get("method", "?")} {c.get("path", "?")}"'
                for c in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} API contract(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each API contract via "
                f"okto_pulse_link_task_to_contract. "
                f"Alternatively, enable 'skip contract coverage' on the spec or board."
            )

    async def check_ir_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active integration requirement has a linked task."""
        skip_global = (board.settings or {}).get("skip_ir_coverage_global", False) if board else False
        if getattr(spec, "skip_ir_coverage", False) or skip_global:
            return
        requirements = [
            ir for ir in (getattr(spec, "integration_requirements", None) or [])
            if isinstance(ir, dict) and ir.get("status", "active") == "active"
        ]
        if not requirements:
            return
        unlinked = [ir for ir in requirements if not ir.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{ir.get("title", ir.get("id", "?"))}"'
                for ir in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} integration requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each IR via "
                f"okto_pulse_link_task_to_integration_requirement. "
                f"Alternatively, enable 'skip IR coverage' on the spec or board."
            )

    async def check_or_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active observability requirement has a linked task."""
        skip_global = (board.settings or {}).get("skip_or_coverage_global", False) if board else False
        if getattr(spec, "skip_or_coverage", False) or skip_global:
            return
        requirements = [
            req for req in (getattr(spec, "observability_requirements", None) or [])
            if isinstance(req, dict) and req.get("status", "active") == "active"
        ]
        if not requirements:
            return
        unlinked = [req for req in requirements if not req.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{req.get("title", req.get("id", "?"))}"'
                for req in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} observability requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each OR via "
                f"okto_pulse_link_task_to_observability_requirement. "
                f"Alternatively, enable 'skip OR coverage' on the spec or board."
            )

    async def check_decisions_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active Decision has a linked task (OPT-IN).

        Specs and boards default `skip_decisions_coverage=True`, so this is a
        no-op unless the user explicitly enables the gate. Only `active`
        decisions are checked — `superseded` and `revoked` are historical and
        don't need linkage.
        """
        skip_global = (board.settings or {}).get("skip_decisions_coverage_global", False) if board else False
        # Default True at both levels — if either says skip, skip.
        skip_spec = getattr(spec, "skip_decisions_coverage", True)
        if skip_spec or skip_global:
            return
        decisions = list(spec.decisions or [])
        active = [d for d in decisions if isinstance(d, dict) and d.get("status", "active") == "active"]
        if not active:
            return
        unlinked = [d for d in active if not d.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{d.get("title", d.get("id", "?"))}"'
                for d in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} Decision(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each Decision via "
                f"okto_pulse_link_task_to_decision. "
                f"Alternatively, enable 'skip decisions coverage' on the spec or board."
            )

    async def move_card(
        self, card_id: str, user_id: str, data: CardMove, actor_name: str | None = None
    ) -> Card | None:
        """Move a card to a different column/position. Blocks if dependencies not met.

        Moving execution work to 'validation' or 'done' requires an execution
        report. The report is appended to the card's conclusions list so
        reviewers can validate the executor's claim before approving it.
        """
        card = await self.get_card(card_id)
        if not card:
            return None

        if getattr(card, "archived", False):
            raise ValueError(
                "This card is archived. Restore it first using restore_tree before making changes."
            )

        old_status = card.status
        old_position = card.position

        # Load board settings for governance
        board = await self.db.get(Board, card.board_id)
        board_settings = board.settings or {} if board else {}
        skip_global = board_settings.get("skip_test_coverage_global", False)

        # Block forward moves based on card_type and spec status.
        # Uses level comparison: spec must have reached the minimum required status.
        # Once a spec reaches IN_PROGRESS or DONE, cards can advance freely.
        old_level = self._STATUS_ORDER.get(old_status, 0)
        new_level = self._STATUS_ORDER.get(data.status, 0)
        if new_level > old_level and card.spec_id:
            spec_for_status = await self.db.get(Spec, card.spec_id)
            if spec_for_status:
                from okto_pulse.core.services.main import SpecService
                spec_level = SpecService._STATUS_ORDER.get(spec_for_status.status, 0)
                card_type = getattr(card, "card_type", CardType.NORMAL)
                if card_type == CardType.TEST:
                    # Test cards can start when spec >= validated (level 3)
                    min_spec_level = SpecService._STATUS_ORDER.get(SpecStatus.VALIDATED, 3)
                else:
                    # Normal and bug cards can start when spec >= in_progress (level 4)
                    min_spec_level = SpecService._STATUS_ORDER.get(SpecStatus.IN_PROGRESS, 4)
                if spec_level < min_spec_level:
                    raise ValueError(
                        f"Cannot move card forward: spec '{spec_for_status.title}' must be at least "
                        f"'{SpecStatus.VALIDATED.value if card_type == CardType.TEST else SpecStatus.IN_PROGRESS.value}' "
                        f"(currently '{spec_for_status.status.value}'). "
                        f"Move the spec forward before starting work on its cards."
                    )

        # Sprint gate: if spec has sprints, card must have sprint_id and sprint must be active
        if new_level > old_level and card.spec_id:
            spec_for_sprint = await self.db.get(Spec, card.spec_id)
            if spec_for_sprint:
                sprint_count_q = select(func.count()).select_from(Sprint).where(
                    Sprint.spec_id == card.spec_id, Sprint.archived.is_(False),
                )
                sprint_count = (await self.db.execute(sprint_count_q)).scalar() or 0
                if sprint_count > 0:
                    hotfix_count_q = select(func.count()).select_from(Sprint).where(
                        Sprint.spec_id == card.spec_id,
                        Sprint.lane_type == SprintLaneType.HOTFIX,
                        Sprint.archived.is_(False),
                    )
                    hotfix_count = (await self.db.execute(hotfix_count_q)).scalar() or 0
                    post_closure_bug = (
                        getattr(card, "card_type", CardType.NORMAL) == CardType.BUG
                        and (
                            spec_for_sprint.status == SpecStatus.DONE
                            or hotfix_count > 0
                        )
                    )
                    if not card.sprint_id:
                        remediation = "assign_hotfix_lane" if post_closure_bug else "assign_sprint"
                        facts = {
                            "card_id": card.id,
                            "spec_id": card.spec_id,
                            "spec_status": spec_for_sprint.status.value,
                            "lane_type": "hotfix" if post_closure_bug else None,
                            "next_action": remediation,
                        }
                        raise CardOperationError(
                            "sprint_required",
                            "This spec uses sprints. Card must be assigned to a sprint before advancing. "
                            "Use okto_pulse_update_card or assign_tasks_to_sprint to assign it.",
                            remediation=remediation,
                            facts=facts,
                            workflow_remediation=(
                                BugWorkflowRemediationMessageBuilder()
                                .build_from_sprint_lane_block(
                                    code="sprint_required",
                                    remediation=remediation,
                                    facts=facts,
                                )
                            ),
                        )
                    sprint_obj = await self.db.get(Sprint, card.sprint_id)
                    if not sprint_obj:
                        remediation = "assign_hotfix_lane" if post_closure_bug else "assign_sprint"
                        facts = {
                            "card_id": card.id,
                            "spec_id": card.spec_id,
                            "sprint_id": card.sprint_id,
                            "spec_status": spec_for_sprint.status.value,
                            "lane_type": "hotfix" if post_closure_bug else None,
                            "next_action": remediation,
                        }
                        raise CardOperationError(
                            "sprint_not_found",
                            "Card's assigned sprint no longer exists. Assign it to an active sprint before advancing.",
                            remediation=remediation,
                            facts=facts,
                            workflow_remediation=(
                                BugWorkflowRemediationMessageBuilder()
                                .build_from_sprint_lane_block(
                                    code="sprint_not_found",
                                    remediation=remediation,
                                    facts=facts,
                                )
                            ),
                        )
                    if sprint_obj.status != SprintStatus.ACTIVE:
                        inactive_hotfix = sprint_obj.lane_type == SprintLaneType.HOTFIX
                        remediation = (
                            "activate_hotfix_lane"
                            if inactive_hotfix
                            else ("assign_hotfix_lane" if post_closure_bug else "activate_sprint")
                        )
                        facts = {
                            "card_id": card.id,
                            "spec_id": card.spec_id,
                            "sprint_id": sprint_obj.id,
                            "sprint_status": sprint_obj.status.value,
                            "lane_type": sprint_obj.lane_type.value,
                            "next_action": remediation,
                        }
                        raise CardOperationError(
                            "sprint_not_active",
                            f"Card's sprint '{sprint_obj.title}' is not active "
                            f"(status: '{sprint_obj.status.value}'). "
                            f"Only cards in active sprints can advance.",
                            remediation=remediation,
                            facts=facts,
                            workflow_remediation=(
                                BugWorkflowRemediationMessageBuilder()
                                .build_from_sprint_lane_block(
                                    code="sprint_not_active",
                                    remediation=remediation,
                                    facts=facts,
                                    message=(
                                        f"Card's sprint '{sprint_obj.title}' is not active "
                                        f"(status: '{sprint_obj.status.value}')."
                                    ),
                                )
                            ),
                        )

        # --- Task Validation Gate: block in_progress→done when gate active ---
        if (
            data.status == CardStatus.DONE
            and old_status in (CardStatus.IN_PROGRESS, CardStatus.STARTED, CardStatus.NOT_STARTED)
            and getattr(card, "card_type", CardType.NORMAL) != CardType.TEST
        ):
            spec_for_gate = await self.db.get(Spec, card.spec_id) if card.spec_id else None
            sprint_for_gate = await self.db.get(Sprint, card.sprint_id) if card.sprint_id else None
            gate_config = self._resolve_validation_config(
                card, spec_for_gate, sprint_for_gate, board_settings
            )
            if gate_config["required"]:
                raise ValueError(
                    "Validation gate is active. Move card to 'validation' status first. "
                    "A reviewer must submit a task validation before the card can move to 'done'. "
                    "Use move_card(status='validation', conclusion=..., completeness=..., "
                    "completeness_justification=..., drift=..., drift_justification=...) "
                    "then submit_task_validation."
                )

        # Block Done on test cards if linked scenarios not updated
        if data.status == CardStatus.DONE and card.spec_id and card.test_scenario_ids:
            spec_for_test_scenarios = await self.db.get(Spec, card.spec_id)
            if spec_for_test_scenarios and not skip_global:
                all_scenarios = {s["id"]: s for s in (spec_for_test_scenarios.test_scenarios or [])}
                stale = []
                for sid in (card.test_scenario_ids or []):
                    sc = all_scenarios.get(sid)
                    if sc and sc.get("status") in ("draft", "ready"):
                        stale.append({
                            "id": sid,
                            "title": sc.get("title", sid),
                            "status": sc.get("status"),
                        })
                if stale:
                    # R4-IMP1: normalized test_card_completion contract with the
                    # actionable pending scenarios. Same block (draft/ready scenarios
                    # prevent done); no auto-promotion.
                    from okto_pulse.core.services.gate_contracts import (
                        incomplete_test_card_completion_error,
                    )
                    raise incomplete_test_card_completion_error(
                        card_id=card.id,
                        current_status=old_status.value if old_status else None,
                        pending_scenarios=stale,
                        board_id=card.board_id,
                        spec_id=card.spec_id,
                    )

        # --- Bug card: block in_progress/done without properly linked test tasks ---
        # Gate triggers when moving TO in_progress or done FROM a status before in_progress
        # (i.e. not_started or started). Once in_progress is reached, the gate was already passed.
        # NC-6 fix: gate is now conditional on board settings:
        #   - require_test_task_for_bug=False → gate desligado (qualquer bug avança)
        #   - bug_test_gate_min_severity controla qual severity entra no gate
        #     ("minor"=default, sempre exige; "major"=pula minor; "critical"=só critical)
        # Severity ordering (lower → higher): minor < major < critical
        _SEVERITY_ORDER = {"minor": 1, "major": 2, "critical": 3}
        _board_settings = (board.settings or {}) if board else {}
        _bug_gate_enabled = _board_settings.get(
            "require_test_task_for_bug", True
        )
        _bug_gate_min_sev = _board_settings.get(
            "bug_test_gate_min_severity", "minor"
        )
        _card_severity = getattr(card, "severity", None) or "minor"
        _gate_applies = (
            _bug_gate_enabled
            and _SEVERITY_ORDER.get(_card_severity, 1)
            >= _SEVERITY_ORDER.get(_bug_gate_min_sev, 1)
        )

        if (
            _gate_applies
            and data.status in (CardStatus.IN_PROGRESS, CardStatus.DONE)
            and old_level < self._STATUS_ORDER.get(CardStatus.IN_PROGRESS, 2)
            and getattr(card, "card_type", CardType.NORMAL) == CardType.BUG
        ):
            bug_gate_started = time.perf_counter()
            linked_tests = card.linked_test_task_ids or []
            if not linked_tests:
                workflow_remediation = (
                    BugWorkflowRemediationMessageBuilder()
                    .build_missing_regression_test_task()
                )
                raise CardOperationError(
                    "missing_regression_test_task",
                    "Bug card requires at least 1 new test task linked before moving to in_progress. "
                    "REQUIRED STEPS: "
                    "(1) Create a regression test card with card_type='test', spec_id, and test_scenario_ids "
                    "using okto_pulse_create_card. The referenced scenario may be an existing scenario on a "
                    "validated/locked spec; leave spec content unchanged for Path A regression evidence. "
                    "(2) Link the test task to this bug using okto_pulse_update_card with linked_test_task_ids, "
                    "(3) Then retry moving this bug card to in_progress. "
                    "TO BYPASS: set require_test_task_for_bug=false on the board, or raise "
                    "bug_test_gate_min_severity above this bug's severity.",
                    remediation="create_regression_test_card",
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "next_action": workflow_remediation.next_action.value,
                    },
                    workflow_remediation=workflow_remediation,
                )

            # Validate each linked test task
            bug_created = card.created_at
            spec_for_bug = await self.db.get(Spec, card.spec_id) if card.spec_id else None
            all_scenarios = {
                str(s["id"]): s
                for s in (spec_for_bug.test_scenarios or [])
                if isinstance(s, dict) and s.get("id") is not None
            } if spec_for_bug else {}
            # Path B (spec f5a7cae7 / card ead17e4d): amendments formally linked
            # to THIS bug+spec feed the shared Path A/B predicate so a cross-spec
            # regression artifact is admissible ONLY with valid amendment lineage.
            # coverage_confirmed is hardcoded False here — no production path may
            # confirm coverage before card c9cf9781 (ADJ-B). An empty list means
            # Path B context is active but no amendment exists -> cross-spec stays
            # fail-closed (ADJ-C).
            amendment_rows = (
                await AmendmentRevisionService(self.db).list_for_bug(
                    board_id=card.board_id,
                    original_spec_id=card.spec_id,
                    origin_bug_id=card.id,
                )
                if card.spec_id
                else []
            )
            amendment_facts = [
                AmendmentLineageFact.from_row(row) for row in amendment_rows
            ]
            validated_test_tasks: list[Card] = []
            candidate_scenario_ids: list[str] = []

            for test_task_id in linked_tests:
                test_task = await self.db.get(Card, test_task_id)
                if not test_task:
                    raise ValueError(
                        f"Linked test task '{test_task_id}' not found. "
                        f"Remove it from linked_test_task_ids using okto_pulse_update_card "
                        f"and link a valid test task instead."
                    )

                # Validate test task is of type TEST
                if getattr(test_task, "card_type", "normal") != CardType.TEST:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' is not a test card "
                        f"(type: {getattr(test_task, 'card_type', 'normal')}). "
                        f"Bug cards require linked test cards of type 'test'."
                    )

                # Validate test task has test_scenario_ids
                if not test_task.test_scenario_ids:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' has no test_scenario_ids. "
                        f"A test task must be linked to at least one test scenario. "
                        f"Use okto_pulse_link_task_to_scenario to link the test task to a scenario, "
                        f"or create a new test task with test_scenario_ids set."
                    )

                # Validate test task belongs to the same spec (Path A). A
                # cross-spec test task is admissible ONLY via Path B: when an
                # amendment formally links this bug we defer the decision to the
                # shared predicate (which fail-closes); with no amendment context
                # the cross-spec test task stays blocked (ADJ-C).
                if test_task.spec_id != card.spec_id and not amendment_facts:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' belongs to spec '{test_task.spec_id}' "
                        f"but this bug belongs to spec '{card.spec_id}'. "
                        f"Test tasks must belong to the same spec as the bug card."
                    )

                if (
                    bug_created
                    and test_task.created_at
                    and test_task.created_at.isoformat() < bug_created.isoformat()
                ):
                    raise ValueError(
                        f"Linked test task '{test_task.title}' was created before this bug card. "
                        "Create or link a regression test task created after the bug so the bug has "
                        "fresh validation coverage without editing a locked spec."
                    )

                validated_test_tasks.append(test_task)
                candidate_scenario_ids.extend(str(sid) for sid in (test_task.test_scenario_ids or []))

            missing_scenario_ids = {
                sid for sid in candidate_scenario_ids if sid not in all_scenarios
            }
            candidate_spec_ids_by_scenario_id: dict[str, str] = {}
            if missing_scenario_ids:
                other_specs_result = await self.db.execute(
                    select(Spec).where(
                        Spec.board_id == card.board_id,
                        Spec.id != card.spec_id,
                    )
                )
                for other_spec in other_specs_result.scalars():
                    for scenario in other_spec.test_scenarios or []:
                        if not isinstance(scenario, dict) or scenario.get("id") is None:
                            continue
                        scenario_id = str(scenario["id"])
                        if scenario_id in missing_scenario_ids:
                            candidate_spec_ids_by_scenario_id.setdefault(
                                scenario_id,
                                other_spec.id,
                            )

            for test_task in validated_test_tasks:
                # Validate scenarios exist in spec. Regression test cards may
                # reference existing scenarios even when the spec is locked.
                for sid in test_task.test_scenario_ids:
                    scenario_id = str(sid)
                    sc = all_scenarios.get(scenario_id)
                    if not sc:
                        other_spec_id = candidate_spec_ids_by_scenario_id.get(scenario_id)
                        if other_spec_id:
                            # TR1: cross-spec evidence is admissible ONLY via Path
                            # B. Always defer to the shared predicate
                            # (validate_linked_test_tasks below) — it fail-closes
                            # with a stable Path B reason (missing_amendment_revision
                            # when no formal amendment links this bug), replacing
                            # the old direct same-spec equality reject.
                            continue
                        observe_bug_regression_resolution(
                            board_id=card.board_id,
                            result=None,
                            duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
                            spec_id=card.spec_id,
                            error_code="scenario_not_found",
                        )
                        await record_bug_regression_decision(
                            board_id=card.board_id,
                            bug_id=card.id,
                            spec_id=card.spec_id,
                            decision="semantic_gap",
                            reason_code="scenario_not_found",
                            scenario_count=len(candidate_scenario_ids),
                            test_task_count=len(validated_test_tasks),
                            actor_id=user_id,
                            session=self.db,
                        )
                        workflow_remediation = (
                            BugWorkflowRemediationMessageBuilder()
                            .build_semantic_gap(reason_code="scenario_not_found")
                        )
                        raise CardOperationError(
                            "scenario_not_found",
                            f"Test scenario '{scenario_id}' referenced by test task '{test_task.title}' "
                            f"does not exist in spec '{spec_for_bug.title if spec_for_bug else card.spec_id}'. "
                            f"The scenario may have been deleted. Link the test task to an existing scenario, "
                            "or create an amendment/refinement/spec revision/hotfix spec if new canonical "
                            "coverage is truly required. reason=scenario_not_found; "
                            "next_action=escalate_semantic_gap."
                            ,
                            remediation="escalate_semantic_gap",
                            facts={
                                "card_id": card.id,
                                "spec_id": card.spec_id,
                                "next_action": workflow_remediation.next_action.value,
                            },
                            workflow_remediation=workflow_remediation,
                        )

            origin_task = await self.db.get(Card, card.origin_task_id) if card.origin_task_id else None
            if not origin_task:
                observe_bug_regression_resolution(
                    board_id=card.board_id,
                    result=None,
                    duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
                    spec_id=card.spec_id,
                    error_code="origin_task_missing",
                )
                await record_bug_regression_decision(
                    board_id=card.board_id,
                    bug_id=card.id,
                    spec_id=card.spec_id,
                    decision="semantic_gap",
                    reason_code="origin_task_missing",
                    scenario_count=len(candidate_scenario_ids),
                    test_task_count=len(validated_test_tasks),
                    actor_id=user_id,
                    session=self.db,
                )
                workflow_remediation = (
                    BugWorkflowRemediationMessageBuilder()
                    .build_semantic_gap(reason_code="origin_task_missing")
                )
                raise CardOperationError(
                    "origin_task_missing",
                    "Bug card requires a valid origin_task_id before regression scenario eligibility "
                    "can be evaluated. reason=origin_task_missing; next_action=escalate_semantic_gap.",
                    remediation="escalate_semantic_gap",
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "next_action": workflow_remediation.next_action.value,
                    },
                    workflow_remediation=workflow_remediation,
                )

            gate_result = BugRegressionGateValidator().validate_linked_test_tasks(
                bug_card=card,
                linked_test_tasks=validated_test_tasks,
                spec=spec_for_bug,
                origin_task=origin_task,
                candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
                # G2 (c9cf9781): coverage is NOT passed in (a bool would be
                # forgeable). It is derived from the persisted, artifact-bound
                # validator attestation carried on each amendment fact
                # (validation_metadata.coverage_confirmation) — fail-closed.
                amendment_facts=amendment_facts,
            )
            eligibility = gate_result.eligibility
            observe_bug_regression_resolution(
                board_id=card.board_id,
                result=eligibility,
                duration_ms=(time.perf_counter() - bug_gate_started) * 1000,
            )
            if eligibility.rejected_scenarios:
                primary_reason = eligibility.rejected_scenarios[0].reason.value
            elif eligibility.eligible_scenarios:
                primary_reason = eligibility.eligible_scenarios[0].reason.value
            elif eligibility.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING:
                primary_reason = "coverage_pending"
            else:
                primary_reason = "no_eligible_scenarios"

            await record_bug_regression_decision(
                board_id=card.board_id,
                bug_id=card.id,
                spec_id=card.spec_id,
                # The bounded decision vocabulary (eligible/rejected/semantic_gap)
                # is owned by the observability schema; extending it belongs to
                # 966c7e7c. coverage_pending is a non-allow block -> recorded as
                # "rejected"; the precise signal travels in reason_code below.
                decision=(
                    "eligible"
                    if gate_result.allowed
                    else ("semantic_gap" if eligibility.semantic_gap_required else "rejected")
                ),
                reason_code=primary_reason,
                coverage_state=eligibility.coverage_state.value,
                scenario_count=len(candidate_scenario_ids),
                test_task_count=len(validated_test_tasks),
                actor_id=user_id,
                session=self.db,
            )
            if not gate_result.allowed:
                rejected = ", ".join(
                    f"{item.scenario_id}:{item.reason.value}"
                    + (f"({item.detail})" if item.detail else "")
                    for item in eligibility.rejected_scenarios
                ) or "none"
                eligible_ids = ", ".join(
                    item.scenario_id for item in eligibility.eligible_scenarios
                ) or "none"
                workflow_remediation = (
                    BugWorkflowRemediationMessageBuilder()
                    .build_from_eligibility(eligibility)
                )
                raise CardOperationError(
                    gate_result.decision.value,
                    "Bug linked test task scenarios do not satisfy regression eligibility. "
                    f"decision={gate_result.decision.value}; "
                    f"eligible_scenario_ids=[{eligible_ids}]; "
                    f"rejected_scenarios=[{rejected}]; "
                    f"semantic_gap_required={str(eligibility.semantic_gap_required).lower()}; "
                    f"spec_mutation_required={str(eligibility.spec_mutation_required).lower()}; "
                    f"next_action={eligibility.next_action.value}. "
                    "Reuse only scenarios linked to the bug origin task or explicit affected tasks. "
                    "If expected behavior changed or no eligible scenario exists, create an "
                    "amendment/refinement/spec revision/hotfix spec instead of editing the current spec."
                    ,
                    remediation=workflow_remediation.next_action.value,
                    facts={
                        "card_id": card.id,
                        "spec_id": card.spec_id,
                        "decision": gate_result.decision.value,
                        "next_action": workflow_remediation.next_action.value,
                        "eligible_scenarios_count": len(eligibility.eligible_scenarios),
                    },
                    workflow_remediation=workflow_remediation,
                )
            emit_no_unlock_invariant(
                board_id=card.board_id,
                spec_id=card.spec_id,
            )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=card.board_id,
            actor_id=user_id,
            entity_type="card",
            entity_id=card.id,
            critical_action=_critical_card_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=actor_name,
            card_id=card.id,
        )

        if data.status == CardStatus.DONE:
            graph_state = await _resolve_closeout_graph_state(card.board_id, self.db)
            _evaluate_cognitive_closeout_or_raise(
                gate_factory=self._cognitive_closeout_gate_factory,
                board=board,
                board_id=card.board_id,
                entity_type=_card_cognitive_entity_type(card),
                entity_id=card.id,
                entity=card,
                target_label="card",
                graph_state=graph_state,
            )
            await _evaluate_cognitive_readiness_or_raise(
                service_factory=self._cognitive_readiness_service_factory,
                db=self.db,
                board_id=card.board_id,
                entity_type=_card_cognitive_entity_type(card),
                entity_id=card.id,
                entity=card,
                target_label="card",
                policy_blocking=_cognitive_readiness_blocking_active(board),
            )

        report_target = None
        if data.status == CardStatus.DONE:
            report_target = "Done"
        elif (
            data.status == CardStatus.VALIDATION
            and old_status in (
                CardStatus.NOT_STARTED,
                CardStatus.STARTED,
                CardStatus.IN_PROGRESS,
                CardStatus.ON_HOLD,
            )
            and getattr(card, "card_type", CardType.NORMAL) != CardType.TEST
        ):
            report_target = "Validation"

        # Require an execution report before handoff to Validation/Done.
        if report_target:
            if not data.conclusion or not data.conclusion.strip():
                raise ValueError(
                    f"A conclusion is required when moving a card to {report_target}. "
                    "The conclusion must be the executor's detailed claim including: "
                    "(1) what was done — specific changes and files modified, "
                    "(2) technical decisions and reasoning, "
                    "(3) what was tested and results, "
                    "(4) any side effects or follow-ups. "
                    "Provide the conclusion in the 'conclusion' parameter."
                )
            # Validate completeness (0-100)
            if data.completeness is None:
                raise ValueError(
                    f"completeness (0-100) is required when moving a card to {report_target}. "
                    "It indicates how much of the planned work was actually implemented. "
                    "100 = fully complete, 0 = nothing delivered."
                )
            if not (0 <= data.completeness <= 100):
                raise ValueError("completeness must be between 0 and 100.")
            if not data.completeness_justification or not data.completeness_justification.strip():
                raise ValueError(
                    f"completeness_justification is required when moving a card to {report_target}. "
                    "Explain why the completeness score is what it is."
                )
            # Validate drift (0-100)
            if data.drift is None:
                raise ValueError(
                    f"drift (0-100) is required when moving a card to {report_target}. "
                    "It indicates how much the implementation deviated from the original plan. "
                    "0 = no deviation, 100 = completely different from plan."
                )
            if not (0 <= data.drift <= 100):
                raise ValueError("drift must be between 0 and 100.")
            if not data.drift_justification or not data.drift_justification.strip():
                raise ValueError(
                    f"drift_justification is required when moving a card to {report_target}. "
                    "Explain what caused the deviation from the original plan."
                )

            report_source = (
                "move_to_validation"
                if data.status == CardStatus.VALIDATION
                else "move_to_done"
            )
            conclusions = list(card.conclusions or [])
            conclusions.append({
                "text": data.conclusion.strip(),
                "author_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completeness": data.completeness,
                "completeness_justification": data.completeness_justification.strip(),
                "drift": data.drift,
                "drift_justification": data.drift_justification.strip(),
                "source": report_source,
            })
            card.conclusions = conclusions
            flag_modified(card, "conclusions")

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import CardConclusionAdded

            await event_publish(
                CardConclusionAdded(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card_id,
                    spec_id=card.spec_id,
                    conclusion_excerpt=data.conclusion.strip()[:280],
                    added_by=user_id,
                ),
                session=self.db,
            )

        # Block forward moves if dependencies not met
        if new_level > old_level:
            deps_met, blocking = await self.check_dependencies_met(card_id)
            if not deps_met:
                raise ValueError(
                    f"Dependências não concluídas: {', '.join(blocking)}"
                )

        if data.status == CardStatus.DONE:
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                card.board_id,
                "card",
                card.id,
                phase="card_done",
            )

        card.status = data.status
        if data.position is not None:
            card.position = data.position
        else:
            # Move to end of new column
            pos_query = (
                select(func.max(Card.position))
                .where(Card.board_id == card.board_id, Card.status == data.status)
            )
            max_pos = (await self.db.execute(pos_query)).scalar() or -1
            card.position = max_pos + 1

        # Auto-rollback: if card cancelled and spec is validated → revert to approved
        if data.status == CardStatus.CANCELLED and card.spec_id:
            spec_for_rollback = await self.db.get(Spec, card.spec_id)
            if spec_for_rollback and spec_for_rollback.status == SpecStatus.VALIDATED:
                spec_for_rollback.status = SpecStatus.APPROVED
                if spec_for_rollback.evaluations:
                    for ev in spec_for_rollback.evaluations:
                        ev["stale"] = True
                    flag_modified(spec_for_rollback, "evaluations")
                rollback_name = actor_name or await resolve_actor_name(self.db, user_id, card.board_id)
                spec_service = SpecService(self.db)
                await spec_service._record_history(
                    spec_id=card.spec_id, action="status_changed",
                    actor_id=user_id, actor_name=rollback_name,
                    changes=[{"field": "status", "old": "validated", "new": "approved"}],
                    summary=f"Auto-rollback: card '{card.title}' cancelled — spec reverted for revalidation",
                    version=spec_for_rollback.version,
                )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, card.board_id)

        # Emit CardMoved + optional CardCancelled / CardRestored so downstream
        # handlers (e.g. KG decay on cancel) can react.
        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                CardCancelled,
                CardMoved,
                CardRestored,
            )

            await event_publish(
                CardMoved(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                    spec_id=card.spec_id,
                    moved_by=user_id,
                ),
                session=self.db,
            )
            if data.status == CardStatus.CANCELLED:
                await event_publish(
                    CardCancelled(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        previous_status=old_status.value,
                    ),
                    session=self.db,
                )
            elif old_status == CardStatus.CANCELLED:
                await event_publish(
                    CardRestored(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        to_status=data.status.value,
                    ),
                    session=self.db,
                )

        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "from_status": old_status.value,
                "to_status": data.status.value,
                "from_position": old_position,
                "to_position": card.position,
            },
        )
        return card

    async def delete_card(self, card_id: str, user_id: str) -> bool:
        """Delete a card.

        Cascade-cleans orphan references before the row delete so the next
        update_spec/create_card on the same spec doesn't trip
        _validate_spec_linked_refs. Cleans 5 JSON containers on the parent
        spec and the linked_test_task_ids column on any bug card that pointed
        at this one. Same transaction as the delete.
        """
        card = await self.get_card(card_id)
        if not card:
            return False

        board_id = card.board_id

        # Cascade cleanup: strip card_id from every reference list on the
        # parent spec. Must run BEFORE db.delete(card) so any validator
        # running on the same session sees a consistent state.
        if card.spec_id:
            spec = await self.db.get(Spec, card.spec_id)
            if spec is not None:
                _SPEC_LINK_CONTAINERS = (
                    "test_scenarios",
                    "business_rules",
                    "api_contracts",
                    "integration_requirements",
                    "observability_requirements",
                    "technical_requirements",
                    "decisions",
                )
                for container_name in _SPEC_LINK_CONTAINERS:
                    items = getattr(spec, container_name, None) or []
                    changed = False
                    for item in items:
                        linked = item.get("linked_task_ids") or []
                        if card_id in linked:
                            item["linked_task_ids"] = [
                                tid for tid in linked if tid != card_id
                            ]
                            changed = True
                    if changed:
                        flag_modified(spec, container_name)

        # Cascade cleanup: bug cards on the same board may reference this
        # card via their columnar linked_test_task_ids. Non-bug cards only —
        # deleting a bug card doesn't leave references elsewhere.
        if getattr(card, "card_type", CardType.NORMAL) != CardType.BUG:
            bugs_q = select(Card).where(
                Card.board_id == board_id,
                Card.card_type == CardType.BUG,
            )
            bugs_res = await self.db.execute(bugs_q)
            for bug in bugs_res.scalars().all():
                linked = bug.linked_test_task_ids or []
                if card_id in linked:
                    bug.linked_test_task_ids = [
                        tid for tid in linked if tid != card_id
                    ]
                    flag_modified(bug, "linked_test_task_ids")

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(card)

        await self._log_activity(
            board_id=board_id,
            card_id=card_id,
            action="card_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
        )
        return True

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class AgentService:
    """Service for agent operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def generate_api_key() -> str:
        """Generate a secure API key."""
        return f"dash_{secrets.token_hex(24)}"

    @staticmethod
    def hash_api_key(key: str) -> str:
        """Hash an API key for storage."""
        return hashlib.sha256(key.encode()).hexdigest()

    async def create_agent(
        self, user_id: str, data: AgentCreate
    ) -> tuple[Agent, str]:
        """Create a new global agent (no board_id).

        If preset_id is provided, agent.permission_flags is initialised from
        that preset's flags so the agent immediately reflects the preset.
        Otherwise, permission_flags defaults to a deep copy of the full
        registry (all True), giving new agents full access by default.
        """
        import copy
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY

        api_key = self.generate_api_key()

        flags: dict | None = data.permission_flags
        preset_id = data.preset_id
        if preset_id and flags is None:
            preset = await self.db.get(PermissionPreset, preset_id)
            if preset and preset.flags:
                flags = copy.deepcopy(preset.flags)
        if flags is None:
            flags = copy.deepcopy(PERMISSION_REGISTRY)

        agent = Agent(
            name=data.name,
            description=data.description,
            objective=data.objective,
            api_key=api_key,
            api_key_hash=self.hash_api_key(api_key),
            permissions=data.permissions,
            preset_id=preset_id,
            permission_flags=flags,
            created_by=user_id,
        )
        self.db.add(agent)
        await self.db.flush()
        return agent, api_key

    async def get_agent(self, agent_id: str) -> Agent | None:
        """Get an agent by ID."""
        query = select(Agent).where(Agent.id == agent_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_agent_by_key(self, api_key: str) -> Agent | None:
        """Get an agent by API key."""
        key_hash = self.hash_api_key(api_key)
        query = select(Agent).where(Agent.api_key_hash == key_hash, Agent.is_active.is_(True))
        result = await self.db.execute(query)
        agent = result.scalar_one_or_none()
        if agent:
            agent.last_used_at = datetime.now(timezone.utc)
        return agent

    async def list_agents_for_user(self, user_id: str) -> list[Agent]:
        """List all agents owned by a user."""
        query = select(Agent).where(Agent.created_by == user_id).order_by(Agent.created_at)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def list_agents_for_board(self, board_id: str) -> list[Agent]:
        """List all agents that have access to a board (via junction)."""
        query = (
            select(Agent)
            .join(AgentBoard, AgentBoard.agent_id == Agent.id)
            .where(AgentBoard.board_id == board_id)
            .order_by(Agent.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def list_agents(self, board_id: str) -> list[Agent]:
        """Backward-compat alias for list_agents_for_board."""
        return await self.list_agents_for_board(board_id)

    async def agent_has_board_access(self, agent_id: str, board_id: str) -> bool:
        """Check if an agent has access to a board."""
        query = select(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None

    async def grant_board_access(
        self, agent_id: str, board_id: str, granted_by: str
    ) -> AgentBoard:
        """Grant an agent access to a board."""
        grant = AgentBoard(
            agent_id=agent_id,
            board_id=board_id,
            granted_by=granted_by,
        )
        self.db.add(grant)
        await self.db.flush()
        return grant

    async def revoke_board_access(self, agent_id: str, board_id: str) -> bool:
        """Revoke an agent's access to a board."""
        query = delete(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        return result.rowcount > 0

    async def update_board_overrides(
        self, agent_id: str, board_id: str, permission_overrides: dict | None
    ) -> AgentBoard | None:
        """Update permission overrides for an agent on a specific board."""
        query = select(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        ab = result.scalar_one_or_none()
        if not ab:
            return None
        ab.permission_overrides = permission_overrides
        return ab

    async def list_boards_for_agent(self, agent_id: str) -> list[Board]:
        """List all boards an agent has access to."""
        query = (
            select(Board)
            .join(AgentBoard, AgentBoard.board_id == Board.id)
            .where(AgentBoard.agent_id == agent_id)
            .order_by(Board.name)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_agent(self, agent_id: str, data: AgentUpdate) -> Agent | None:
        """Update an agent.

        Special handling:
        - If `preset_id` is set (and `permission_flags` is NOT in the same
          payload), agent.permission_flags is reset from the preset's flags.
          This makes selecting a preset in the UI behave intuitively: the
          agent's effective permissions immediately match the preset.
        - If `preset_id` is explicitly cleared (None), permission_flags is
          reset to the full registry (all True) — i.e. "Full Control".
        """
        import copy
        from sqlalchemy.orm.attributes import flag_modified
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY

        agent = await self.get_agent(agent_id)
        if not agent:
            return None

        update_data = data.model_dump(exclude_unset=True)

        preset_id_in_payload = "preset_id" in update_data
        flags_in_payload = "permission_flags" in update_data

        for key, value in update_data.items():
            setattr(agent, key, value)

        if preset_id_in_payload and not flags_in_payload:
            new_preset_id = update_data.get("preset_id")
            if new_preset_id:
                preset = await self.db.get(PermissionPreset, new_preset_id)
                if preset and preset.flags:
                    agent.permission_flags = copy.deepcopy(preset.flags)
                    flag_modified(agent, "permission_flags")
            else:
                agent.permission_flags = copy.deepcopy(PERMISSION_REGISTRY)
                flag_modified(agent, "permission_flags")
        elif flags_in_payload:
            flag_modified(agent, "permission_flags")

        return agent

    async def regenerate_key(self, agent_id: str) -> tuple[Agent | None, str | None]:
        """Regenerate an agent's API key."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return None, None

        new_key = self.generate_api_key()
        agent.api_key = new_key
        agent.api_key_hash = self.hash_api_key(new_key)
        return agent, new_key

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return False
        await self.db.delete(agent)
        return True


class AttachmentService:
    """Service for attachment operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def upload_attachment(
        self,
        card_id: str,
        user_id: str,
        filename: str,
        content: bytes,
        mime_type: str,
    ) -> Attachment | None:
        """Upload a file attachment."""
        # Verify card exists
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        # Delegate to the registered storage provider
        storage = get_storage_provider()
        file_path = await storage.save(card.board_id, filename, content)
        unique_name = Path(file_path).name

        attachment = Attachment(
            card_id=card_id,
            filename=unique_name,
            original_filename=filename,
            mime_type=mime_type,
            size=len(content),
            path=file_path,
            uploaded_by=user_id,
        )
        self.db.add(attachment)
        await self.db.flush()
        return attachment

    async def get_attachment(self, attachment_id: str) -> Attachment | None:
        """Get an attachment by ID."""
        return await self.db.get(Attachment, attachment_id)

    async def delete_attachment(self, attachment_id: str) -> bool:
        """Delete an attachment."""
        attachment = await self.get_attachment(attachment_id)
        if not attachment:
            return False

        # Delete file via storage provider
        storage = get_storage_provider()
        await storage.delete(attachment.path)

        await self.db.delete(attachment)
        return True


class QAService:
    """Service for Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(
        self, card_id: str, user_id: str, data: QACreate
    ) -> QAItem | None:
        """Create a Q&A question."""
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        qa = QAItem(
            card_id=card_id,
            question=data.question,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: QAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> QAItem | None:
        """Answer a Q&A question."""
        qa = await self.db.get(QAItem, qa_id)
        if not qa:
            return None

        card = await self.db.get(Card, qa.card_id)
        board = await self.db.get(Board, card.board_id) if card else None
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="card",
            question_id=qa_id,
            card_id=card.id if card else None,
            actor_type=actor_type,
            surface=surface,
        )

        qa.answer = data.answer
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(QAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class CommentService:
    """Service for comment operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_comment(
        self, card_id: str, user_id: str, data: CommentCreate
    ) -> Comment | None:
        """Create a comment (text or choice board)."""
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        comment = Comment(
            card_id=card_id,
            content=data.content,
            author_id=user_id,
            comment_type=data.comment_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            responses=[],
            allow_free_text=data.allow_free_text,
        )
        self.db.add(comment)
        await self.db.flush()
        return comment

    async def respond_to_choice(
        self, comment_id: str, responder_id: str, responder_name: str,
        selected: list[str], free_text: str | None = None,
    ) -> Comment | None:
        """Add a response to a choice board comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.comment_type == "text":
            return None

        # Validate selected options exist
        valid_ids = {c["id"] for c in (comment.choices or [])}
        for sel in selected:
            if sel not in valid_ids:
                return None

        # Single-choice: only one selection allowed
        if comment.comment_type == "choice" and len(selected) > 1:
            selected = selected[:1]

        responses = list(comment.responses or [])
        # Replace existing response from same responder
        responses = [r for r in responses if r.get("responder_id") != responder_id]
        responses.append({
            "responder_id": responder_id,
            "responder_name": responder_name,
            "selected": selected,
            "free_text": free_text,
        })
        comment.responses = responses
        await self.db.flush()
        return comment

    async def update_comment(
        self, comment_id: str, user_id: str, data: CommentUpdate
    ) -> Comment | None:
        """Update a comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.author_id != user_id:
            return None

        comment.content = data.content
        return comment

    async def delete_comment(self, comment_id: str, user_id: str) -> bool:
        """Delete a comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.author_id != user_id:
            return False
        await self.db.delete(comment)
        return True


async def _validate_spec_linked_refs(
    db: AsyncSession,
    current_spec: Any,
    update_data: dict[str, Any],
) -> None:
    """Reject orphan references in linked_* fields before they hit the DB.

    Computes the *final* state of each spec collection (incoming value when
    the field is in `update_data`, otherwise the current persisted value)
    and validates that every `linked_*` reference points to an existing
    target:

    - linked_criteria (test_scenarios → AC):
        Must be a 0-based string index "0".."N-1" OR the exact AC text.
        AC labels like "AC1" are rejected — the SpecModal coverage widget
        does not recognise them and they would silently appear uncovered.

    - linked_requirements (business_rules + api_contracts + IR + OR → FR):
        Same rule — index "0".."N-1" OR exact FR text. Anything else
        (including "FR1" labels) is rejected.

    - linked_rules (api_contracts → BR):
        Must match an existing business_rule.id in the same spec.

    - linked_api_contracts (IR → API contract):
        Must match an existing api_contract.id in the same spec.

    - linked_integration_requirements (OR → IR):
        Must match an existing integration_requirement.id in the same spec.

    - linked_task_ids (test_scenarios + business_rules + api_contracts +
      IR + OR + structured_trs → Card):
        Each id must resolve to an existing Card row in the DB.

    Raises ValueError with all offenders enumerated so the caller can fix
    them in one round-trip instead of one-by-one.
    """
    def _final(field: str, default: Any):
        if field in update_data:
            return update_data[field] if update_data[field] is not None else default
        return getattr(current_spec, field, None) or default

    def _child_text(item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("text") or item.get("title") or item.get("description") or "")
        return str(item)

    def _child_id(item: Any) -> str | None:
        if isinstance(item, dict):
            raw = item.get("id")
            return str(raw) if raw not in (None, "") else None
        return None

    final_frs_raw: list[Any] = list(_final("functional_requirements", []) or [])
    final_acs_raw: list[Any] = list(_final("acceptance_criteria", []) or [])
    final_frs: list[str] = [_child_text(item) for item in final_frs_raw]
    final_acs: list[str] = [_child_text(item) for item in final_acs_raw]
    final_brs: list[dict] = [
        b if isinstance(b, dict) else b.model_dump()
        for b in (_final("business_rules", []) or [])
    ]
    final_contracts: list[dict] = [
        c if isinstance(c, dict) else c.model_dump()
        for c in (_final("api_contracts", []) or [])
    ]
    final_irs: list[dict] = [
        ir if isinstance(ir, dict) else ir.model_dump()
        for ir in (_final("integration_requirements", []) or [])
    ]
    final_ors: list[dict] = [
        req if isinstance(req, dict) else req.model_dump()
        for req in (_final("observability_requirements", []) or [])
    ]
    final_scenarios: list[dict] = [
        s if isinstance(s, dict) else s.model_dump()
        for s in (_final("test_scenarios", []) or [])
    ]
    final_decisions: list[dict] = [
        d if isinstance(d, dict) else d.model_dump()
        for d in (_final("decisions", []) or [])
    ]
    final_trs_raw: list = list(_final("technical_requirements", []) or [])
    final_trs_structured: list[dict] = []
    for tr in final_trs_raw:
        if isinstance(tr, dict) and tr.get("id"):
            final_trs_structured.append(tr)
        elif hasattr(tr, "model_dump") and getattr(tr, "id", None):
            final_trs_structured.append(tr.model_dump())

    valid_fr_indices = {str(i) for i in range(len(final_frs))}
    valid_ac_indices = {str(i) for i in range(len(final_acs))}
    valid_fr_texts = {text for text in final_frs if text}
    valid_ac_texts = {text for text in final_acs if text}
    valid_fr_ids = {child_id for item in final_frs_raw if (child_id := _child_id(item))}
    valid_ac_ids = {child_id for item in final_acs_raw if (child_id := _child_id(item))}
    valid_br_ids = {br.get("id") for br in final_brs if br.get("id")}
    valid_contract_ids = {ct.get("id") for ct in final_contracts if ct.get("id")}
    valid_ir_ids = {ir.get("id") for ir in final_irs if ir.get("id")}

    errors: list[str] = []

    _DIM_TARGET = {"requirements": "FR", "criteria": "AC"}
    def _check_index_text_or_id(
        refs: list[str],
        valid_indices: set,
        valid_texts: set,
        valid_ids: set,
        dim: str,
        owner_label: str,
    ):
        target = _DIM_TARGET.get(dim, dim.upper()[:2])
        for ref in refs or []:
            ref_str = str(ref)
            if ref_str in valid_indices or ref_str in valid_texts or ref_str in valid_ids:
                continue
            max_idx = max(0, len(valid_indices) - 1)
            errors.append(
                f"{owner_label}: linked_{dim} reference '{ref_str}' is not a valid 0-based index "
                f"(0..{max_idx}), existing {target} text, or structured {target} id."
            )

    # business_rules.linked_requirements → FR
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        _check_index_text_or_id(
            br.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts,
            valid_fr_ids,
            "requirements",
            owner,
        )

    # api_contracts.linked_requirements → FR
    # api_contracts.linked_rules → BR.id
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or (ct.get('method', '?') + ' ' + ct.get('path', '?'))}'"
        _check_index_text_or_id(
            ct.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts,
            valid_fr_ids,
            "requirements",
            owner,
        )
        for ref in ct.get("linked_rules") or []:
            if str(ref) not in valid_br_ids:
                errors.append(
                    f"{owner}: linked_rules reference '{ref}' does not match any business_rule.id "
                    f"in the spec (valid: {sorted(valid_br_ids) or 'none'})."
                )

    # integration_requirements.linked_requirements → FR
    # integration_requirements.linked_api_contracts → api_contract.id
    for ir in final_irs:
        owner = f"IR '{ir.get('id') or ir.get('title') or '?'}'"
        _check_index_text_or_id(
            ir.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts,
            valid_fr_ids,
            "requirements",
            owner,
        )
        for ref in ir.get("linked_api_contracts") or []:
            if str(ref) not in valid_contract_ids:
                errors.append(
                    f"{owner}: linked_api_contracts reference '{ref}' does not match any api_contract.id "
                    f"in the spec (valid: {sorted(valid_contract_ids) or 'none'})."
                )

    # observability_requirements.linked_requirements → FR
    # observability_requirements.linked_integration_requirements → IR.id
    for req in final_ors:
        owner = f"OR '{req.get('id') or req.get('title') or '?'}'"
        _check_index_text_or_id(
            req.get("linked_requirements") or [],
            valid_fr_indices,
            valid_fr_texts,
            valid_fr_ids,
            "requirements",
            owner,
        )
        for ref in req.get("linked_integration_requirements") or []:
            if str(ref) not in valid_ir_ids:
                errors.append(
                    f"{owner}: linked_integration_requirements reference '{ref}' does not match any integration_requirement.id "
                    f"in the spec (valid: {sorted(valid_ir_ids) or 'none'})."
                )

    # test_scenarios.linked_criteria → AC
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        _check_index_text_or_id(
            sc.get("linked_criteria") or [],
            valid_ac_indices,
            valid_ac_texts,
            valid_ac_ids,
            "criteria",
            owner,
        )

    # decisions.linked_requirements → FR  +  supersedes_decision_id → Decision.id
    valid_decision_ids = {d.get("id") for d in final_decisions if d.get("id")}
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        _check_index_text_or_id(
            dec.get("linked_requirements") or [],
            valid_fr_indices, valid_fr_texts, valid_fr_ids, "requirements", owner,
        )
        sup = dec.get("supersedes_decision_id")
        if sup and sup not in valid_decision_ids:
            errors.append(
                f"{owner}: supersedes_decision_id '{sup}' does not match any decision.id "
                f"in the spec (valid: {sorted(valid_decision_ids) or 'none'})."
            )

    # linked_task_ids → Card.id (DB existence check). Collect all in one batch.
    all_task_ids: set[str] = set()
    task_owners: dict[str, list[str]] = {}
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        for tid in sc.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        for tid in br.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or '?'}'"
        for tid in ct.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for ir in final_irs:
        owner = f"IR '{ir.get('id') or ir.get('title') or '?'}'"
        for tid in ir.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for req in final_ors:
        owner = f"OR '{req.get('id') or req.get('title') or '?'}'"
        for tid in req.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for tr in final_trs_structured:
        owner = f"TR '{tr.get('id')}'"
        for tid in tr.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        for tid in dec.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)

    if all_task_ids:
        existing_ids: set[str] = set()
        result = await db.execute(select(Card.id).where(Card.id.in_(all_task_ids)))
        for (cid,) in result.all():
            existing_ids.add(cid)
        for missing in all_task_ids - existing_ids:
            owners = ", ".join(task_owners.get(missing, []))
            errors.append(
                f"linked_task_ids reference card '{missing}' that does not exist in the database. "
                f"Referenced by: {owners}."
            )

    if errors:
        joined = "; ".join(errors[:10])
        more = f" (and {len(errors) - 10} more)" if len(errors) > 10 else ""
        raise ValueError(
            f"Cannot update spec: {len(errors)} orphan link reference(s) found. {joined}{more}. "
            f"Use 0-based string indices (\"0\", \"1\", ...) for FR/AC, the BR.id for linked_rules, "
            f"the api_contract.id / integration_requirement.id for cross-resource links, "
            f"and an existing Card.id for linked_task_ids."
        )


class SpecService:
    """Service for spec operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self._cognitive_closeout_gate_factory: Callable[
            [], Any
        ] = _build_default_cognitive_closeout_gate
        self._cognitive_readiness_service_factory: Callable[
            [], Any
        ] = _build_default_cognitive_readiness_service

    # ---- Status progression order ----
    _STATUS_ORDER = {
        SpecStatus.DRAFT: 0,
        SpecStatus.REVIEW: 1,
        SpecStatus.APPROVED: 2,
        SpecStatus.VALIDATED: 3,
        SpecStatus.IN_PROGRESS: 4,
        SpecStatus.DONE: 5,
        SpecStatus.CANCELLED: 5,
    }

    async def _record_history(
        self,
        spec_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a spec."""
        entry = SpecHistory(
            spec_id=spec_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, spec_id: str, limit: int = 50) -> list[SpecHistory]:
        """List history entries for a spec, newest first."""
        query = (
            select(SpecHistory)
            .where(SpecHistory.spec_id == spec_id)
            .order_by(SpecHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            # Normalize enum values
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_spec(
        self, board_id: str, user_id: str, data: SpecCreate, skip_ownership_check: bool = False
    ) -> Spec | None:
        """Create a new spec in a board."""
        if skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id)
        else:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        # Fail-closed scenario_type (spec ac16b3c9): every scenario in a NEW spec
        # is a new write — reject an unsupported type before insert/flush, never
        # normalize.
        if data.test_scenarios:
            validate_scenario_types_for_write(
                [s.model_dump() for s in data.test_scenarios], None
            )

        spec = Spec(
            board_id=board_id,
            title=data.title,
            description=data.description,
            context=data.context,
            functional_requirements=canonicalize_fr_ac(
                "functional_requirement", data.functional_requirements
            ),
            technical_requirements=data.technical_requirements,
            acceptance_criteria=canonicalize_fr_ac(
                "acceptance_criterion", data.acceptance_criteria
            ),
            test_scenarios=[s.model_dump() for s in data.test_scenarios] if data.test_scenarios else None,
            screen_mockups=None,  # assigned after the Design System gate (below)
            business_rules=[r.model_dump() for r in data.business_rules] if data.business_rules else None,
            api_contracts=[c.model_dump() for c in data.api_contracts] if data.api_contracts else None,
            integration_requirements=[ir.model_dump() for ir in data.integration_requirements] if data.integration_requirements else None,
            observability_requirements=[req.model_dump() for req in data.observability_requirements] if data.observability_requirements else None,
            decisions=[d.model_dump() for d in data.decisions] if data.decisions else None,
            status=data.status,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
            ideation_id=data.ideation_id,
            refinement_id=data.refinement_id,
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate mockups submitted
        # at creation BEFORE persistence — the create twin of the update_spec gate. The
        # baseline is the entity's (empty) mockups, so every submitted screen is
        # evaluated; assign only if the gate does not raise.
        _submitted_mockups = (
            [s.model_dump() for s in data.screen_mockups] if data.screen_mockups else None
        )
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, spec, _submitted_mockups, entity_type="spec"
            )
            spec.screen_mockups = _submitted_mockups
        self.db.add(spec)
        await self.db.flush()

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SpecCreated

        spec_source: str = "manual"
        origin_id: str | None = None
        if data.refinement_id:
            spec_source = "derived_refinement"
            origin_id = data.refinement_id
        elif data.ideation_id:
            spec_source = "derived_ideation"
            origin_id = data.ideation_id

        await event_publish(
            SpecCreated(
                board_id=board_id,
                actor_id=user_id,
                spec_id=spec.id,
                source=spec_source,
                origin_id=origin_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="spec_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "spec_id": spec.id},
        )
        await self._record_history(
            spec_id=spec.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Spec created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": data.status.value},
                *([{"field": "functional_requirements", "old": None, "new": data.functional_requirements}] if data.functional_requirements else []),
                *([{"field": "technical_requirements", "old": None, "new": data.technical_requirements}] if data.technical_requirements else []),
                *([{"field": "acceptance_criteria", "old": None, "new": data.acceptance_criteria}] if data.acceptance_criteria else []),
                *([{"field": "integration_requirements", "old": None, "new": [ir.model_dump() for ir in data.integration_requirements]}] if data.integration_requirements else []),
                *([{"field": "observability_requirements", "old": None, "new": [req.model_dump() for req in data.observability_requirements]}] if data.observability_requirements else []),
            ],
        )
        return spec

    async def get_spec(self, spec_id: str) -> Spec | None:
        """Get a spec by ID with its cards and knowledge bases."""
        query = (
            select(Spec)
            .options(selectinload(Spec.cards))
            .options(selectinload(Spec.knowledge_bases))
            .options(selectinload(Spec.qa_items))
            .options(selectinload(Spec.architecture_designs))
            .where(Spec.id == spec_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_specs(self, board_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Spec]:
        """List specs for a board, optionally filtered by status."""
        query = (
            select(Spec)
            .options(selectinload(Spec.architecture_designs))
            .where(Spec.board_id == board_id)
        )
        if status_filter:
            query = query.where(Spec.status == SpecStatus(status_filter))
        if not include_archived:
            query = query.where(Spec.archived.is_(False))
        query = query.order_by(Spec.updated_at.desc())
        result = await self.db.execute(query)
        rows = list(result.scalars().all())
        await _attach_open_qa_counts(self.db, rows, SpecQAItem, "spec_id")
        return rows

    async def update_test_scenario(
        self,
        spec_id: str,
        user_id: str,
        scenario_id: str,
        *,
        title: str | None = None,
        given: str | None = None,
        when: str | None = None,
        then: str | None = None,
        scenario_type: str | None = None,
        linked_criteria: list[str] | None = None,
        notes: str | None = None,
        clear: list[str] | None = None,
    ) -> dict:
        """Edit the BODY of a test scenario (spec 6f1e75bf, FR2/FR5).

        ``None`` means "leave unchanged"; a non-None value sets the field.
        ``clear`` lists field names (``notes``/``linked_criteria``) to reset to
        empty — this is how a caller distinguishes "omitted" from "cleared".
        ``status`` is NOT accepted (that stays exclusive to the status path so no
        second NC-9 bypass is created). Editing any SEMANTIC field
        (given/when/then/scenario_type/linked_criteria) of a scenario that holds
        evidence invalidates it (status→ready, evidence dropped); cosmetic edits
        (title/notes) preserve status and evidence. Respects the content-lock.
        """
        await _require_spec_unlocked(self.db, spec_id)
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        scenarios = [
            dict(s) for s in (spec.test_scenarios or []) if isinstance(s, dict)
        ]
        target = next((s for s in scenarios if s.get("id") == scenario_id), None)
        if target is None:
            raise ValueError(f"scenario_not_found: {scenario_id}")

        # Fail-closed scenario_type (spec ac16b3c9): an explicit new value on the
        # body-edit path must be a supported type — reject before mutation, never
        # normalize. ``None`` means "leave unchanged" and is not validated.
        if scenario_type is not None:
            validate_scenario_type(scenario_type)

        clearable = {"notes", "linked_criteria"}
        clear_set = set(clear or [])
        bad_clear = clear_set - clearable
        if bad_clear:
            raise ValueError(
                f"clear only supports {sorted(clearable)}; got {sorted(bad_clear)}"
            )

        changed_fields: list[str] = []

        # Resolve linked_criteria against the spec's ACs (reuse #2 resolver,
        # fail-closed on unresolved tokens).
        if linked_criteria is not None:
            resolved, unresolved = resolve_linked_criteria_to_ids(
                linked_criteria, list(spec.acceptance_criteria or [])
            )
            if unresolved:
                raise ValueError(f"unresolved_criteria: {', '.join(unresolved)}")
            if target.get("linked_criteria") != resolved:
                target["linked_criteria"] = resolved
                changed_fields.append("linked_criteria")

        for field, value in (
            ("title", title),
            ("given", given),
            ("when", when),
            ("then", then),
            ("scenario_type", scenario_type),
            ("notes", notes),
        ):
            if value is not None and target.get(field) != value:
                target[field] = value
                changed_fields.append(field)

        # Explicit clears (distinguish "omitted" from "emptied").
        for field in clear_set:
            empty: object = [] if field == "linked_criteria" else ""
            if target.get(field) != empty:
                target[field] = empty
                if field not in changed_fields:
                    changed_fields.append(field)

        # Evidence invalidation on semantic edit (spec FR5/BR6): if a SEMANTIC
        # field changed and the scenario currently holds evidence, the old
        # evidence no longer proves the new behaviour — reset to ready + drop it.
        evidence_invalidated = False
        if evidence_invalidated_by_semantic_edit(changed_fields) and (
            target.get("evidence") or target.get("latest_evidence")
        ):
            target["status"] = "ready"
            target["evidence"] = None
            target.pop("latest_evidence", None)
            evidence_invalidated = True

        if not changed_fields:
            return {
                "scenario_id": scenario_id,
                "updated_fields": [],
                "evidence_invalidated": False,
                "scenario": target,
            }

        updated = await self.update_spec(
            spec_id, user_id, SpecUpdate(test_scenarios=scenarios)
        )
        new_target = next(
            (
                s
                for s in (updated.test_scenarios or [])
                if isinstance(s, dict) and s.get("id") == scenario_id
            ),
            target,
        )
        self.db.add(
            ActivityLog(
                board_id=spec.board_id,
                action="test_scenario_body_changed",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "updated_fields": changed_fields,
                    "evidence_invalidated": evidence_invalidated,
                },
            )
        )
        await self.db.commit()
        logging.getLogger("okto_pulse.spec.test_scenario").info(
            "test_scenario.body_changed scenario=%s spec=%s fields=%s invalidated=%s",
            scenario_id,
            spec_id,
            changed_fields,
            evidence_invalidated,
            extra={
                "event": "test_scenario.body_changed",
                "scenario_id": scenario_id,
                "spec_id": spec_id,
                "board_id": spec.board_id,
                "actor_id": user_id,
                "updated_fields": changed_fields,
                "evidence_invalidated": evidence_invalidated,
            },
        )
        return {
            "scenario_id": scenario_id,
            "updated_fields": changed_fields,
            "evidence_invalidated": evidence_invalidated,
            "scenario": new_target,
        }

    async def delete_test_scenario(
        self, spec_id: str, user_id: str, scenario_id: str
    ) -> dict:
        """Delete a test scenario and clean ``Card.test_scenario_ids`` in cascade
        (spec 6f1e75bf, FR3/BR4).

        Atomic: the spec's ``test_scenarios`` and every referencing card are
        mutated in a single transaction (all-or-nothing). Does not block on
        existing links — the cascade removes them. Respects the content-lock.
        """
        await _require_spec_unlocked(self.db, spec_id)
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        scenarios = [s for s in (spec.test_scenarios or []) if isinstance(s, dict)]
        remaining = [s for s in scenarios if s.get("id") != scenario_id]
        if len(remaining) == len(scenarios):
            raise ValueError(f"scenario_not_found: {scenario_id}")

        spec.test_scenarios = remaining
        flag_modified(spec, "test_scenarios")

        # Cascade: drop the scenario id from every card that references it, in
        # the SAME transaction → all-or-nothing, no orphan in Card.test_scenario_ids.
        result = await self.db.execute(select(Card).where(Card.spec_id == spec_id))
        cards_unlinked: list[str] = []
        for card in result.scalars().all():
            ids = list(card.test_scenario_ids or [])
            if scenario_id in ids:
                card.test_scenario_ids = [i for i in ids if i != scenario_id]
                flag_modified(card, "test_scenario_ids")
                cards_unlinked.append(card.id)

        self.db.add(
            ActivityLog(
                board_id=spec.board_id,
                action="test_scenario_deleted",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "cards_unlinked": cards_unlinked,
                },
            )
        )
        await self.db.commit()

        logging.getLogger("okto_pulse.spec.test_scenario").info(
            "test_scenario.deleted scenario=%s spec=%s cards_unlinked=%s",
            scenario_id,
            spec_id,
            len(cards_unlinked),
            extra={
                "event": "test_scenario.deleted",
                "scenario_id": scenario_id,
                "spec_id": spec_id,
                "board_id": spec.board_id,
                "actor_id": user_id,
                "cards_unlinked": cards_unlinked,
            },
        )
        return {"scenario_id": scenario_id, "cards_unlinked": cards_unlinked}

    async def set_test_scenario_status(
        self,
        spec_id: str,
        user_id: str,
        scenario_id: str,
        status: str,
        evidence: dict | None = None,
    ) -> dict:
        """Scoped operational status mutation for ONE test scenario (spec
        6f1e75bf, FR4/FR6) — the single helper shared by the MCP status tool and
        the REST status endpoint.

        - Guards by spec STATUS (require_test_scenario_status_mutable): blocks
          arbitrary ``validated``/``done`` status edits, permits
          ``in_progress``. Does NOT use the content-lock (which would wrongly
          block in_progress). Narrow exception: a ``validated``/``done`` spec
          may receive operational evidence/status for a scenario that is already
          linked to an executable test card.
        - Applies the NC-9 evidence gate (validate_test_scenario_evidence) unless
          ``skip_test_evidence_global`` is set (then allows + emits a forensic log).
        - Mutates ONLY the target scenario (status + inline evidence) and persists
          narrow — it does NOT go through update_spec, does NOT bump version and
          does NOT replace the full list, so every other scenario is preserved.

        Returns ``{scenario_id, old_status, new_status, evidence_provided,
        evidence_gate_skipped}``. Raises :class:`StatusNotMutableError` and
        ``ValueError`` (``status_not_valid`` / ``evidence_required`` /
        ``scenario_not_found``).
        """
        from sqlalchemy import update as sql_update

        if status not in VALID_SCENARIO_STATUSES:
            raise ValueError(
                f"status_not_valid: must be one of {list(VALID_SCENARIO_STATUSES)}"
            )

        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("scenario_not_found: spec not found")

        # Guard by STATUS (NOT the content-lock): blocks validated/done, allows
        # in_progress — the execution phase where scenarios become passed.
        # Post-lock exception: a done/validated spec may still receive
        # operational test evidence when the target scenario is already tied to
        # a real test card in an execution state. This preserves content lock
        # semantics while making the documented "fresh post-bug/regression test
        # card on a locked spec" flow reachable.
        try:
            require_test_scenario_status_mutable(getattr(spec, "status", None))
        except StatusNotMutableError:
            if not await self._has_executable_test_card_for_scenario(spec, scenario_id):
                raise

        board = await self.db.get(Board, spec.board_id)
        skip = (
            bool((board.settings or {}).get("skip_test_evidence_global", False))
            if board
            else False
        )
        if not skip:
            # for_write: a NEW gated write must satisfy the re-executable
            # evidence contract (spec 9e0bf979) — explicit evidence_class is
            # strict, and an unclassed run-log-like payload is rejected before
            # persisting (only a direct test pointer is grandfathered).
            ok, missing = validate_test_scenario_evidence(
                status, evidence, for_write=True
            )
            if not ok:
                raise ValueError(f"evidence_required: {', '.join(missing)}")

        scenarios = [
            dict(s) for s in (spec.test_scenarios or []) if isinstance(s, dict)
        ]
        old_status = None
        found = False
        for s in scenarios:
            if s.get("id") == scenario_id:
                old_status = s.get("status")
                s["status"] = status
                if evidence is not None:
                    s["evidence"] = evidence
                found = True
                break
        if not found:
            raise ValueError(f"scenario_not_found: {scenario_id}")

        # Narrow persist: write only the test_scenarios column, no version bump,
        # no content-lock. The other scenarios in the list are untouched.
        await self.db.execute(
            sql_update(Spec).where(Spec.id == spec_id).values(test_scenarios=scenarios)
        )
        self.db.add(
            ActivityLog(
                board_id=spec.board_id,
                action="test_scenario_status_changed",
                actor_type="agent",
                actor_id=user_id,
                actor_name=user_id,
                details={
                    "spec_id": spec_id,
                    "scenario_id": scenario_id,
                    "from_status": old_status,
                    "to_status": status,
                    "evidence_provided": evidence is not None,
                    "evidence_gate_skipped": skip,
                },
            )
        )
        await self.db.commit()

        logger = logging.getLogger("okto_pulse.spec.test_scenario")
        logger.info(
            "test_scenario.status_changed scenario=%s board=%s from=%s to=%s "
            "evidence=%s skip=%s",
            scenario_id,
            spec.board_id,
            old_status,
            status,
            evidence is not None,
            skip,
            extra={
                "event": "test_scenario.status_changed",
                "scenario_id": scenario_id,
                "board_id": spec.board_id,
                "spec_id": spec_id,
                "from_status": old_status,
                "to_status": status,
                "evidence_provided": evidence is not None,
                "evidence_gate_skipped": skip,
                "changed_by_agent_id": user_id,
            },
        )
        if skip and status in GATED_STATUSES:
            logger.info(
                "test_scenario.evidence_gate_skipped scenario=%s board=%s status=%s",
                scenario_id,
                spec.board_id,
                status,
                extra={
                    "event": "test_scenario.evidence_gate_skipped",
                    "scenario_id": scenario_id,
                    "board_id": spec.board_id,
                    "spec_id": spec_id,
                    "status": status,
                    "skip": True,
                    "agent_id": user_id,
                },
            )

        return {
            "scenario_id": scenario_id,
            "old_status": old_status,
            "new_status": status,
            "evidence_provided": evidence is not None,
            "evidence_gate_skipped": skip,
        }

    async def _has_executable_test_card_for_scenario(
        self, spec: Spec, scenario_id: str
    ) -> bool:
        """Return True when a locked/done spec scenario has a concrete test card
        that can legitimately carry post-lock evidence.

        This is intentionally narrower than "scenario exists": the status path
        remains blocked for arbitrary scenario mutation on locked specs. The
        exception only applies after a fresh/existing test card is linked and has
        entered the execution/review lifecycle.
        """

        rows = await self.db.execute(
            select(Card).where(
                Card.spec_id == spec.id,
                Card.card_type == CardType.TEST,
                Card.status.in_(
                    [
                        CardStatus.STARTED,
                        CardStatus.IN_PROGRESS,
                        CardStatus.VALIDATION,
                        CardStatus.DONE,
                    ]
                ),
            )
        )
        for card in rows.scalars().all():
            if scenario_id in (card.test_scenario_ids or []):
                return True
        return False

    async def _enforce_test_scenario_evidence_gate(
        self, spec: "Spec", new_scenarios: list, user_id: str
    ) -> None:
        """NC-9 service gate (spec 6f1e75bf, FR1/BR2).

        Reject any test scenario whose FINAL status is gated
        (passed/automated/failed) without valid structured evidence when the
        scenario is NEW, its status CHANGED, or its previously-valid evidence was
        removed/invalidated. Old vs new are matched by scenario id. Respects
        ``skip_test_evidence_global`` (allows but emits a forensic audit log so
        reactivation analytics can flag boards that bypass the gate).
        """
        board = await self.db.get(Board, spec.board_id)
        skip = (
            bool((board.settings or {}).get("skip_test_evidence_global", False))
            if board
            else False
        )

        old_by_id = {
            s.get("id"): s
            for s in (spec.test_scenarios or [])
            if isinstance(s, dict)
        }
        offenders: list[str] = []
        for s in new_scenarios:
            if not isinstance(s, dict):
                continue
            status = s.get("status")
            if status not in GATED_STATUSES:
                continue
            if scenario_has_required_evidence(s):
                continue
            sid = s.get("id")
            old = old_by_id.get(sid)
            is_new = old is None
            status_changed = (old or {}).get("status") != status
            old_had_evidence = scenario_has_required_evidence(old) if old else False
            # Enforce on: new scenario already gated, status transition into a
            # gated state, or evidence removed/altered from a previously-valid
            # scenario. A pre-existing gated scenario that was always evidenceless
            # and is left unchanged is NOT newly rejected (legacy data, not
            # introduced by this write).
            if is_new or status_changed or old_had_evidence:
                offenders.append(str(sid) if sid else "(new)")

        if not offenders:
            return

        logger = logging.getLogger("okto_pulse.spec.test_scenario")
        if not skip:
            raise ValueError(
                "evidence_required: test scenario(s) "
                f"{', '.join(offenders)} marked passed/automated/failed without "
                "structured evidence. Provide evidence via the status tool or "
                "endpoint, or enable skip_test_evidence_global on the board."
            )
        # skip ON — allow but emit a forensic audit record (spec OR or_536eca62).
        for sid in offenders:
            logger.info(
                "test_scenario.evidence_gate_skipped scenario=%s board=%s spec=%s",
                sid,
                spec.board_id,
                spec.id,
                extra={
                    "event": "test_scenario.evidence_gate_skipped",
                    "scenario_id": sid,
                    "board_id": spec.board_id,
                    "spec_id": spec.id,
                    "actor_id": user_id,
                    "source": "update_spec",
                    "skip": True,
                },
            )

    async def update_spec(self, spec_id: str, user_id: str, data: SpecUpdate) -> Spec | None:
        """Update a spec. Bumps version on content changes. Records field-level diffs.

        Enforces the Spec Validation Gate content lock: if the spec has an active
        validation with outcome='success', raises SpecLockedError. All content tools
        (business rules, contracts, scenarios, mockups, knowledge) flow
        through this method via SpecUpdate, so applying the lock check here covers
        the whole surface in one place.

        Also enforces referential integrity for `linked_*` fields: any
        `linked_criteria`/`linked_requirements`/`linked_rules`/`linked_task_ids`
        that points to a non-existent target raises ValueError before any write.
        """
        await _require_spec_unlocked(self.db, spec_id)

        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError("This spec is archived. Restore it first before making changes.")

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "functional_requirements", "technical_requirements",
            "acceptance_criteria", "context", "description",
        }
        # Spec eaf78891 (Ideação #2): semantic_fields are KG-relevant fields
        # that DO NOT bump version (they are not in content_fields), but DO
        # need to trigger re-consolidation. We emit SpecSemanticChanged for
        # them so ConsolidationEnqueuer re-extracts the spec into the KG.
        semantic_fields = {
            "decisions", "business_rules", "api_contracts",
            "integration_requirements", "observability_requirements",
            "test_scenarios", "screen_mockups",
        }
        bumps_version = bool(content_fields & update_data.keys())
        bumps_semantic = bool(semantic_fields & update_data.keys())

        # Capture old values for diff
        old_data = {k: getattr(spec, k) for k in update_data.keys()}

        # Serialize structured JSON list fields if present.
        for json_list_field in (
            "test_scenarios",
            "screen_mockups",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
        ):
            if json_list_field in update_data and update_data[json_list_field] is not None:
                update_data[json_list_field] = [
                    s.model_dump() if hasattr(s, "model_dump") else s
                    for s in update_data[json_list_field]
                ]

        # Canonicalize FR/AC to structured dicts with stable ids, preserving
        # text + existing ids (spec 9d66847f). Runs BEFORE
        # _validate_spec_linked_refs so the validator sees canonical ids; the
        # text is preserved, so text-based linked refs keep resolving (no
        # breaking change).
        if "functional_requirements" in update_data:
            update_data["functional_requirements"] = canonicalize_fr_ac(
                "functional_requirement",
                update_data["functional_requirements"],
                existing_items=spec.functional_requirements,
            )
        if "acceptance_criteria" in update_data:
            update_data["acceptance_criteria"] = canonicalize_fr_ac(
                "acceptance_criterion",
                update_data["acceptance_criteria"],
                existing_items=spec.acceptance_criteria,
            )

        # FR5 — lazy ref migration (spec c61569b2, IMPL-4).
        # When FR/AC lists are materialised by canonicalize_fr_ac above,
        # rewrite any index/text refs in downstream fields to the newly
        # assigned fr_/ac_ ids.  This runs on-touch only: specs not passed
        # through update_spec keep resolving via the permanent read-tolerant
        # resolvers (resolve_linked_fr_indices / resolve_linked_criteria_*).
        # No batch sweep; no one-shot migration tool.
        if "functional_requirements" in update_data and update_data["functional_requirements"]:
            from okto_pulse.core.services.spec_structured_entities import (  # noqa: PLC0415
                migrate_legacy_fr_refs,
            )
            old_frs = list(spec.functional_requirements or [])
            new_frs = list(update_data["functional_requirements"] or [])
            _fr_dep_collections = {
                field: list(
                    update_data[field]
                    if field in update_data and update_data[field] is not None
                    else getattr(spec, field, None) or []
                )
                for field in (
                    "business_rules",
                    "api_contracts",
                    "integration_requirements",
                    "observability_requirements",
                    "decisions",
                )
            }
            _fr_migration_updates = migrate_legacy_fr_refs(old_frs, new_frs, _fr_dep_collections)
            # Apply migration results unconditionally: the collections dict was
            # already built from update_data (if present) or spec, so _updated
            # already reflects the caller's new data with refs rewritten.
            for _field, _updated in _fr_migration_updates.items():
                update_data[_field] = _updated

        if "acceptance_criteria" in update_data and update_data["acceptance_criteria"]:
            from okto_pulse.core.services.spec_structured_entities import (  # noqa: PLC0415
                migrate_legacy_ac_refs,
            )
            old_acs = list(spec.acceptance_criteria or [])
            new_acs = list(update_data["acceptance_criteria"] or [])
            _current_scenarios = list(
                update_data["test_scenarios"]
                if "test_scenarios" in update_data and update_data["test_scenarios"] is not None
                else getattr(spec, "test_scenarios", None) or []
            )
            _migrated_scenarios = migrate_legacy_ac_refs(old_acs, new_acs, _current_scenarios)
            if _migrated_scenarios is not None:
                update_data["test_scenarios"] = _migrated_scenarios

        # Re-evaluate bumps_semantic after FR5 migration may have added
        # semantic fields (e.g. business_rules) to update_data.
        bumps_semantic = bool(semantic_fields & update_data.keys())
        # Capture old values for any fields added to update_data by FR5 migration
        # (these were absent from the original update_data so old_data missed them).
        for _migrated_field in update_data:
            if _migrated_field not in old_data:
                old_data[_migrated_field] = getattr(spec, _migrated_field, None)

        # Validate referential integrity of all `linked_*` fields BEFORE
        # mutating the spec. The validator computes the final state of each
        # collection (incoming value OR current state if untouched) and
        # rejects orphan references with a precise error message.
        await _validate_spec_linked_refs(self.db, spec, update_data)

        # Fail-closed scenario_type service gate — defense in depth (spec
        # ac16b3c9, FR2/IR). Closes the same whole-list bypass for scenario_type:
        # any caller (UI full-list, REST PUT or MCP) replacing test_scenarios must
        # not introduce a new/changed invalid scenario_type. Grandfathers unchanged
        # historical values (matched by id) so legacy data keeps re-serializing;
        # runs BEFORE any mutation/flush and never normalizes.
        if update_data.get("test_scenarios") is not None:
            validate_scenario_types_for_write(
                update_data["test_scenarios"], spec.test_scenarios
            )

        # NC-9 (test-theater) service gate — defense in depth (spec 6f1e75bf,
        # FR1/BR2). Closes the bypass where any caller (UI full-list, REST or
        # MCP) could replace test_scenarios with a gated status and no evidence;
        # the evidence rule previously ran only in the MCP status tool. Runs on
        # the incoming list, comparing against the current persisted scenarios.
        if update_data.get("test_scenarios") is not None:
            await self._enforce_test_scenario_evidence_gate(
                spec, update_data["test_scenarios"], user_id
            )

        # MockupDesignSystemGate (spec 3a006f65, card 0192f58d) — defense in depth:
        # gate the bulk screen_mockups write (UI full-list / REST) the same way the MCP
        # tool does, BEFORE persistence. Delta-only: only new/changed mockups; legacy
        # untouched mockups are skipped; screens already gated by the MCP tool in this
        # transaction are skipped. Blocking raises pre-persist; advisory audits.
        if update_data.get("screen_mockups") is not None:
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, spec, update_data["screen_mockups"], entity_type="spec"
            )

        json_fields = {
            "test_scenarios",
            "screen_mockups",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
            "functional_requirements",
            "technical_requirements",
            "acceptance_criteria",
            "labels",
        }
        for key, value in update_data.items():
            setattr(spec, key, value)
            if key in json_fields:
                flag_modified(spec, key)

        old_version = spec.version
        if bumps_version:
            spec.version += 1

        # Compute diffs
        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        if bumps_version:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecVersionBumped

            changed_struct_fields = sorted(content_fields & update_data.keys())
            await event_publish(
                SpecVersionBumped(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    old_version=old_version,
                    new_version=spec.version,
                    changed_fields=changed_struct_fields,
                ),
                session=self.db,
            )

        # Spec eaf78891 (Ideação #2): emit SpecSemanticChanged whenever
        # KG-relevant non-content fields are mutated, INDEPENDENTLY of whether
        # SpecVersionBumped also fired. Both events are recorded in the
        # outbox for audit completeness; ConsolidationEnqueuer's dedup
        # collapses them into a single ConsolidationQueue row anyway.
        if bumps_semantic:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecSemanticChanged

            changed_semantic = sorted(semantic_fields & update_data.keys())
            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    changed_fields=changed_semantic,
                ),
                session=self.db,
            )

        actor_name = await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"spec_id": spec_id, "version": spec.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                spec_id=spec_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=spec.version,
                summary=f"Updated: {changed_fields}",
            )
        if "screen_mockups" in update_data:
            await SpecResourcePropagationService(self.db).propagate_for_spec(
                board_id=spec.board_id,
                spec_id=spec.id,
                actor_id=user_id,
                trigger="spec_mockups_changed",
            )
        return spec

    # ---- Spec state machine ----
    # Direct APPROVED→DRAFT and VALIDATED→DRAFT transitions added for the Spec
    # Validation Gate: editing a validated spec requires one click/call, not three
    # hops (validated→approved→review→draft). Both transitions trigger the backward
    # clear of current_validation_id in move_spec().
    _SPEC_TRANSITIONS = {
        SpecStatus.DRAFT: [SpecStatus.REVIEW, SpecStatus.CANCELLED],
        SpecStatus.REVIEW: [SpecStatus.DRAFT, SpecStatus.APPROVED, SpecStatus.CANCELLED],
        SpecStatus.APPROVED: [SpecStatus.REVIEW, SpecStatus.VALIDATED, SpecStatus.DRAFT, SpecStatus.CANCELLED],
        SpecStatus.VALIDATED: [SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DRAFT, SpecStatus.CANCELLED],
        SpecStatus.IN_PROGRESS: [SpecStatus.VALIDATED, SpecStatus.DONE, SpecStatus.CANCELLED],
        SpecStatus.DONE: [SpecStatus.DRAFT],
        SpecStatus.CANCELLED: [SpecStatus.DRAFT],
    }

    # Statuses from which a backward move clears current_validation_id.
    # Any move from {validated, in_progress, done} to {draft, review, approved}
    # unlocks content editing but preserves spec.validations history.
    _SPEC_LOCKED_STATUSES = frozenset(
        {SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
    )
    _SPEC_EDITABLE_STATUSES = frozenset(
        {SpecStatus.DRAFT, SpecStatus.REVIEW, SpecStatus.APPROVED}
    )

    async def move_spec(
        self, spec_id: str, user_id: str, data: SpecMove, actor_name: str | None = None
    ) -> Spec | None:
        """Move a spec to a different status.

        Enforces a strict state machine. Coverage gates run on approved→validated.
        Qualitative validation runs on validated→in_progress.
        Moving to 'done' requires full test coverage and task completion.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError("This spec is archived. Restore it first before changing status.")

        # Enforce state machine transitions
        allowed = self._SPEC_TRANSITIONS.get(spec.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move spec from '{spec.status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_values}"
            )

        # Load board for settings
        board = await self.db.get(Board, spec.board_id)

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=user_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=_critical_spec_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=actor_name,
        )

        # Enforce coverage gates when moving to validated
        if data.status == SpecStatus.VALIDATED:
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)
            await card_service.check_ir_coverage(spec, board)
            await card_service.check_or_coverage(spec, board)
            await card_service.check_decisions_coverage(spec, board)

            # Spec Validation Gate: when enabled, the only path to validated is via
            # submit_spec_validation (which runs the semantic gate). Direct move_spec
            # from approved→validated is blocked so users/agents cannot bypass the
            # quality check. Backward transitions from validated/in_progress/done→
            # draft/review/approved are intentionally unaffected (they preserve the
            # unlock flow).
            board_settings = (board.settings or {}) if board else {}
            if (
                spec.status == SpecStatus.APPROVED
                and board_settings.get("require_spec_validation", True)
            ):
                # R4-IMP1: same block, normalized operational contract (GateContractError
                # subclasses ValueError — no state-machine change, no auto-promotion).
                from okto_pulse.core.services.gate_contracts import (
                    spec_validation_gate_error,
                )
                raise spec_validation_gate_error(
                    spec_id=spec.id, current_status=spec.status.value,
                )

        # Re-execute coverage gates + qualitative validation when moving to in_progress
        if data.status == SpecStatus.IN_PROGRESS and spec.status == SpecStatus.VALIDATED:
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)
            await card_service.check_ir_coverage(spec, board)
            await card_service.check_or_coverage(spec, board)
            await card_service.check_decisions_coverage(spec, board)

            # Qualitative validation gate
            auto_validate = (board.settings or {}).get("auto_validate", False) if board else False
            skip_qualitative = getattr(spec, "skip_qualitative_validation", False)
            if not auto_validate and not skip_qualitative:
                evaluations = [e for e in (spec.evaluations or []) if not e.get("stale")]
                approvals = [e for e in evaluations if e.get("recommendation") == "approve"]
                rejections = [e for e in evaluations if e.get("recommendation") == "reject"]
                if rejections:
                    reject_names = ", ".join(
                        e.get("evaluator_name", e.get("evaluator_id", "?")) for e in rejections
                    )
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': {len(rejections)} evaluation(s) "
                        f"with 'reject' recommendation exist (by: {reject_names}). "
                        f"Remove or replace the rejecting evaluations before proceeding."
                    )
                if not approvals:
                    raise ValueError(
                        "Cannot move spec to 'in_progress': no evaluation with "
                        "'approve' recommendation found. At least one approval is required. "
                        "Submit an evaluation via okto_pulse_submit_spec_evaluation."
                    )
                threshold = (
                    getattr(spec, "validation_threshold", None)
                    or (board.settings or {}).get("validation_threshold_global", 70) if board else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if avg_score < threshold:
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': average approval score "
                        f"({avg_score:.0f}) is below threshold ({threshold}). "
                        f"Submit additional evaluations with higher scores or lower the threshold."
                    )

        # Enforce test coverage when moving to Done
        skip_global = (board.settings or {}).get("skip_test_coverage_global", False) if board else False
        if data.status == SpecStatus.DONE and not spec.skip_test_coverage and not skip_global:
            criteria = spec.acceptance_criteria or []
            scenarios = spec.test_scenarios or []
            if criteria:
                covered_indices: set[int] = set()
                for scenario in scenarios:
                    covered_indices |= resolve_linked_criteria_to_indices(
                        scenario.get("linked_criteria"),
                        criteria,
                    )
                uncovered = [
                    f"[{i}] {_structured_ref_text(criterion)[:80]}..."
                    for i, criterion in enumerate(criteria)
                    if i not in covered_indices
                ]
                if uncovered:
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(uncovered)} acceptance criteria lack test scenarios. "
                        f"Uncovered: {'; '.join(uncovered[:5])}"
                        f"{f' (and {len(uncovered) - 5} more)' if len(uncovered) > 5 else ''}. "
                        f"Create test scenarios for all criteria, or set skip_test_coverage flag in the spec."
                    )

        # Sprint done gate: all sprints must be closed|cancelled (min 1 closed)
        if data.status == SpecStatus.DONE:
            sprints_q = select(Sprint).where(
                Sprint.spec_id == spec_id, Sprint.archived.is_(False),
            )
            sprints_result = await self.db.execute(sprints_q)
            spec_sprints = list(sprints_result.scalars().all())
            if spec_sprints:
                pending = [
                    s for s in spec_sprints
                    if s.status not in (SprintStatus.CLOSED, SprintStatus.CANCELLED)
                ]
                has_closed = any(s.status == SprintStatus.CLOSED for s in spec_sprints)
                if pending:
                    sprint_list = "; ".join(
                        f"'{s.title}' ({s.status.value})" for s in pending[:5]
                    )
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(pending)} sprint(s) are not closed or cancelled. "
                        f"Pending: {sprint_list}. Close or cancel all sprints first."
                    )
                if not has_closed:
                    raise ValueError(
                        "Cannot move spec to 'done': at least 1 sprint must be closed "
                        "(all are cancelled). Close at least one sprint."
                    )

        # Enforce all linked tasks (non-bug) must be done/cancelled before spec can be done
        if data.status == SpecStatus.DONE:
            linked_tasks_q = select(Card).where(
                Card.spec_id == spec_id,
                Card.card_type == CardType.NORMAL,
                Card.archived.is_(False),
                Card.status.notin_([CardStatus.DONE, CardStatus.CANCELLED]),
            )
            result = await self.db.execute(linked_tasks_q)
            pending_tasks = result.scalars().all()
            if pending_tasks:
                task_list = "; ".join(
                    f"'{t.title}' ({t.status.value})" for t in pending_tasks[:5]
                )
                extra = f" (and {len(pending_tasks) - 5} more)" if len(pending_tasks) > 5 else ""
                raise ValueError(
                    f"Cannot move spec to 'done': {len(pending_tasks)} linked task(s) are not yet done or cancelled. "
                    f"Pending: {task_list}{extra}. "
                    f"Complete or cancel all linked tasks before finalizing the spec."
                )

            graph_state = await _resolve_closeout_graph_state(spec.board_id, self.db)
            _evaluate_cognitive_closeout_or_raise(
                gate_factory=self._cognitive_closeout_gate_factory,
                board=board,
                board_id=spec.board_id,
                entity_type="spec",
                entity_id=spec.id,
                entity=spec,
                target_label="spec",
                graph_state=graph_state,
            )
            await _evaluate_cognitive_readiness_or_raise(
                service_factory=self._cognitive_readiness_service_factory,
                db=self.db,
                board_id=spec.board_id,
                entity_type="spec",
                entity_id=spec.id,
                entity=spec,
                target_label="spec",
                policy_blocking=_cognitive_readiness_blocking_active(board),
            )

            resource_gate = ResourceGateService(self.db)
            await resource_gate.validate_or_raise_spec_resource_task_coverage(
                spec.board_id,
                spec.id,
                phase="spec_done",
                enabled=resource_gate.is_spec_resource_task_coverage_required(board),
            )
            # AFG na spec (investigacao 2026-06-10): specs com findings de
            # arquitetura ativos completavam - o finding gate so rodava em
            # card/ideation/refinement via entity_completion.
            await resource_gate.validate_or_raise_architecture_findings(
                spec.board_id,
                "spec",
                spec.id,
                phase="spec_done",
            )

        old_status = spec.status
        spec.status = data.status

        # Spec Validation Gate: any backward transition from validated/in_progress/done
        # to an editable status (draft/review/approved) clears current_validation_id,
        # releasing the content lock. spec.validations array is preserved intact.
        if (
            old_status in self._SPEC_LOCKED_STATUSES
            and data.status in self._SPEC_EDITABLE_STATUSES
            and getattr(spec, "current_validation_id", None) is not None
        ):
            spec.current_validation_id = None

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecMoved

            await event_publish(
                SpecMoved(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "spec_id": spec_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
            },
        )
        await self._record_history(
            spec_id=spec_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=spec.version,
        )
        return spec

    async def delete_spec(self, spec_id: str, user_id: str) -> bool:
        """Delete a spec. Unlinks cards but doesn't delete them."""
        spec = await self.get_spec(spec_id)
        if not spec:
            return False

        # Unlink cards
        await self.db.execute(
            update(Card).where(Card.spec_id == spec_id).values(spec_id=None)
        )

        board_id = spec.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(spec)

        await self._log_activity(
            board_id=board_id,
            action="spec_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"spec_id": spec_id},
        )
        return True

    async def link_card(
        self, spec_id: str, card_id: str, user_id: str | None = None
    ) -> bool:
        """Link an existing card to a spec. Spec must be in 'approved', 'in_progress', or 'done' status.

        Spec eaf78891 (Ideação #2): emits CardLinkedToSpec on success so the
        ConsolidationEnqueuer re-enqueues the SPEC (not the card) — the spec
        extractor reflects the updated cards list while the card extractor
        does not reference spec_id.
        """
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return False
        if spec.status not in (SpecStatus.APPROVED, SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE):
            raise ValueError(f"Cards can only be linked to a spec in 'approved', 'validated', 'in_progress', or 'done' status (current: '{spec.status.value}')")
        card = await self.db.get(Card, card_id)
        if not card or card.board_id != spec.board_id:
            return False
        card.spec_id = spec_id

        await SpecResourcePropagationService(self.db).propagate_for_card(
            board_id=spec.board_id,
            spec_id=spec_id,
            card_id=card_id,
            actor_id=user_id or card.created_by,
            trigger="card_linked_to_spec",
        )

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardLinkedToSpec

        await event_publish(
            CardLinkedToSpec(
                board_id=spec.board_id,
                actor_id=user_id,
                card_id=card_id,
                spec_id=spec_id,
            ),
            session=self.db,
        )
        return True

    async def unlink_card(
        self, card_id: str, user_id: str | None = None
    ) -> bool:
        """Unlink a card from its spec.

        Spec eaf78891 (Ideação #2): emits CardUnlinkedFromSpec so the
        ConsolidationEnqueuer re-enqueues the (now-orphaned) spec for
        re-extraction.
        """
        card = await self.db.get(Card, card_id)
        if not card or not card.spec_id:
            return False
        old_spec_id = card.spec_id
        card.spec_id = None

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardUnlinkedFromSpec

        await event_publish(
            CardUnlinkedFromSpec(
                board_id=card.board_id,
                actor_id=user_id,
                card_id=card_id,
                spec_id=old_spec_id,
            ),
            session=self.db,
        )
        return True

    # ---- Spec Validation Gate ----

    @staticmethod
    def _resolve_spec_validation_config(board: Board | None) -> dict[str, Any]:
        """Resolve Spec Validation Gate thresholds from board settings.

        Defaults are more rigorous than the Task Validation Gate (70/80/50)
        because poor spec quality has amplified downstream cost.
        """
        settings = (board.settings if board else None) or {}
        return {
            "require_spec_validation": bool(settings.get("require_spec_validation", True)),
            "min_spec_completeness": int(settings.get("min_spec_completeness", 80)),
            "min_spec_assertiveness": int(settings.get("min_spec_assertiveness", 80)),
            "max_spec_ambiguity": int(settings.get("max_spec_ambiguity", 30)),
        }

    async def submit_spec_validation(
        self,
        spec_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a Spec Validation Gate record for a spec in 'approved' status.

        Mirrors CardService.submit_task_validation: runs coverage gates as
        pre-requisite, computes outcome atomically, appends to spec.validations
        array (append-only history), sets current_validation_id, and on success
        atomically moves spec.status to validated.

        Outcome rule: failed if any threshold violated OR recommendation=reject;
        success only if ALL thresholds ok AND recommendation=approve.
        """
        import uuid as _uuid

        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        if spec.status != SpecStatus.APPROVED:
            raise ValueError(
                f"Spec must be in 'approved' status to receive validation "
                f"(current: '{spec.status.value}')."
            )

        board = await self.db.get(Board, spec.board_id)
        config = self._resolve_spec_validation_config(board)
        if not config["require_spec_validation"]:
            raise ValueError(
                "This board does not require spec validation. "
                "To advance the spec without the gate: call "
                "move_spec(spec_id, status='validated'). "
                "To enforce the gate first: enable 'require_spec_validation' "
                "in board settings, then re-submit."
            )

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=reviewer_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.SPEC_SUBMIT_VALIDATION,
            surface="service",
            actor_type="agent" if reviewer_name and "agent" in reviewer_name.lower() else "user",
            actor_name=reviewer_name,
        )

        # Run coverage gates as pre-requisite — reuses existing CardService checks.
        # AC→Scenario coverage must run FIRST so uncovered ACs are caught before
        # the spec gets locked by a successful validation (the move→done gate
        # checks the same thing, but by then the spec is already locked).
        card_service = CardService(self.db)
        await card_service.check_ac_scenario_coverage(spec, board)
        await card_service.check_test_coverage(spec, board)
        await card_service.check_rules_coverage(spec, board)
        await card_service.check_trs_coverage(spec, board)
        await card_service.check_contract_coverage(spec, board)
        await card_service.check_ir_coverage(spec, board)
        await card_service.check_or_coverage(spec, board)
        # Decisions coverage is OPT-IN — no-op when skip_decisions_coverage=True
        # (spec or board). See check_decisions_coverage for details.
        await card_service.check_decisions_coverage(spec, board)
        resource_gate = ResourceGateService(self.db)
        await resource_gate.validate_or_raise_spec_resource_task_coverage(
            spec.board_id,
            spec.id,
            phase="spec_validation",
            enabled=resource_gate.is_spec_resource_task_coverage_required(board),
        )

        # Extract and validate inputs
        completeness = int(data["completeness"])
        assertiveness = int(data["assertiveness"])
        ambiguity = int(data["ambiguity"])
        recommendation = data["recommendation"]
        if recommendation not in ("approve", "reject"):
            raise ValueError("recommendation must be 'approve' or 'reject'")
        for name, score in (
            ("completeness", completeness),
            ("assertiveness", assertiveness),
            ("ambiguity", ambiguity),
        ):
            if not (0 <= score <= 100):
                raise ValueError(f"{name} must be between 0 and 100")

        # Threshold check (ambiguity is max_drift-style — lower is better)
        violations: list[str] = []
        if completeness < config["min_spec_completeness"]:
            violations.append(f"completeness {completeness} < min {config['min_spec_completeness']}")
        if assertiveness < config["min_spec_assertiveness"]:
            violations.append(f"assertiveness {assertiveness} < min {config['min_spec_assertiveness']}")
        if ambiguity > config["max_spec_ambiguity"]:
            violations.append(f"ambiguity {ambiguity} > max {config['max_spec_ambiguity']}")

        # Compute outcome: failed if any violation OR reject; success only if
        # all thresholds ok AND approve.
        if violations or recommendation == "reject":
            outcome = "failed"
        else:
            outcome = "success"

        # Build validation record (id <= 32 chars: "val_" + 8 hex = 12 chars)
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        resolved_thresholds = {
            "min_spec_completeness": config["min_spec_completeness"],
            "min_spec_assertiveness": config["min_spec_assertiveness"],
            "max_spec_ambiguity": config["max_spec_ambiguity"],
        }
        validation = {
            "id": validation_id,
            "spec_id": spec_id,
            "board_id": spec.board_id,
            "reviewer_id": reviewer_id,
            "reviewer_name": reviewer_name,
            "completeness": completeness,
            "completeness_justification": data["completeness_justification"].strip(),
            "assertiveness": assertiveness,
            "assertiveness_justification": data["assertiveness_justification"].strip(),
            "ambiguity": ambiguity,
            "ambiguity_justification": data["ambiguity_justification"].strip(),
            "general_justification": data["general_justification"].strip(),
            "recommendation": recommendation,
            "outcome": outcome,
            "threshold_violations": violations,
            "resolved_thresholds": resolved_thresholds,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Append-only: never overwrite history. flag_modified is required for JSONB.
        validations = list(spec.validations or [])
        validations.append(validation)
        spec.validations = validations
        flag_modified(spec, "validations")
        spec.current_validation_id = validation_id

        # Atomic state transition on success — same transaction as the persist.
        old_status = spec.status
        if outcome == "success":
            spec.status = SpecStatus.VALIDATED
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecMoved, SpecSemanticChanged

            await event_publish(
                SpecMoved(
                    board_id=spec.board_id,
                    actor_id=reviewer_id,
                    spec_id=spec.id,
                    from_status=old_status.value,
                    to_status=spec.status.value,
                ),
                session=self.db,
            )
            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=reviewer_id,
                    spec_id=spec.id,
                    changed_fields=["status"],
                ),
                session=self.db,
            )

        # Activity log
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_validation_submitted",
            actor_type="agent" if reviewer_name and "agent" in reviewer_name.lower() else "user",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "spec_id": spec_id,
                "validation_id": validation_id,
                "outcome": outcome,
                "recommendation": recommendation,
                "completeness": completeness,
                "assertiveness": assertiveness,
                "ambiguity": ambiguity,
                "threshold_violations": violations,
                "from_status": old_status.value,
                "to_status": spec.status.value,
            },
        )

        return {
            **validation,
            "spec_status": spec.status.value,
            "active": True,
        }

    async def list_spec_validations(self, spec_id: str) -> dict[str, Any]:
        """List all spec validations in reverse chronological order.

        Returns a dict with current_validation_id and validations list where
        each record has an 'active' flag indicating if it's the current pointer.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        validations = list(spec.validations or [])
        current_id = getattr(spec, "current_validation_id", None)

        # Reverse chronological order + mark active
        result_list = []
        for v in reversed(validations):
            result_list.append({**v, "active": v.get("id") == current_id})

        return {
            "current_validation_id": current_id,
            "validations": result_list,
        }

    # Dimensões qualitativas da spec evaluation — fonte única compartilhada
    # entre o endpoint REST e o MCP tool okto_pulse_submit_spec_evaluation.
    SPEC_EVALUATION_DIMENSIONS: tuple[tuple[str, str], ...] = (
        ("breakdown_completeness", "breakdown_justification"),
        ("granularity", "granularity_justification"),
        ("dependency_coherence", "dependency_justification"),
        ("test_coverage_quality", "test_coverage_justification"),
    )
    SPEC_EVALUATION_RECOMMENDATIONS: tuple[str, ...] = (
        "approve", "request_changes", "reject",
    )

    async def submit_spec_evaluation(
        self,
        spec_id: str,
        actor_id: str,
        actor_name: str,
        data: dict,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> dict:
        """Submit a qualitative evaluation for a spec in 'validated' status.

        Caminho de escrita ÚNICO da spec evaluation — consumido pelo endpoint
        REST ``POST /specs/{id}/evaluations`` e pelo MCP tool
        ``okto_pulse_submit_spec_evaluation`` (paridade REST/MCP; antes o
        tool era o único caminho e usuários UI/REST ficavam presos no gate
        validated→in_progress sem como satisfazê-lo).

        Multiple evaluators can submit independent evaluations (append-only).
        Caller owns the commit. Raises ValueError on status/input problems.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")
        if spec.status != SpecStatus.VALIDATED:
            raise ValueError(
                f"Spec must be in 'validated' status to submit evaluations "
                f"(currently '{spec.status.value}')"
            )

        recommendation = data.get("recommendation")
        if recommendation not in self.SPEC_EVALUATION_RECOMMENDATIONS:
            raise ValueError(
                "Recommendation must be one of: "
                + ", ".join(self.SPEC_EVALUATION_RECOMMENDATIONS)
            )
        score_fields = [name for name, _ in self.SPEC_EVALUATION_DIMENSIONS]
        score_fields.append("overall_score")
        for name in score_fields:
            score = int(data[name])
            if not (0 <= score <= 100):
                raise ValueError(f"{name} must be between 0 and 100")

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=spec.board_id,
            actor_id=actor_id,
            entity_type="spec",
            entity_id=spec.id,
            critical_action=CriticalAction.SPEC_SUBMIT_EVALUATION,
            surface=surface,
            actor_type=actor_type,
            actor_name=actor_name,
        )

        import uuid as _uuid

        evaluation = {
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "spec_id": spec_id,
            "evaluator_id": actor_id,
            "evaluator_name": actor_name,
            "evaluator_type": actor_type,
            "dimensions": {
                name: {
                    "score": int(data[name]),
                    "justification": data[justification],
                }
                for name, justification in self.SPEC_EVALUATION_DIMENSIONS
            },
            "overall_score": int(data["overall_score"]),
            "overall_justification": data["overall_justification"],
            "recommendation": recommendation,
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        evaluations = list(spec.evaluations or [])
        evaluations.append(evaluation)
        spec.evaluations = evaluations
        flag_modified(spec, "evaluations")

        await self._log_activity(
            board_id=spec.board_id,
            action="spec_evaluation_submitted",
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            details={
                "spec_id": spec_id,
                "evaluation_id": evaluation["id"],
                "overall_score": evaluation["overall_score"],
                "recommendation": recommendation,
            },
        )
        return evaluation

    async def list_spec_evaluations(self, spec_id: str) -> dict[str, Any]:
        """List spec evaluations (newest first) with an active (non-stale) count."""
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")
        evaluations = list(spec.evaluations or [])
        return {
            "spec_id": spec_id,
            "spec_status": spec.status.value,
            "evaluations": list(reversed(evaluations)),
            "active_count": len([e for e in evaluations if not e.get("stale")]),
        }

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class SpecQAService:
    """Service for spec Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, spec_id: str, user_id: str, data: SpecQACreate) -> SpecQAItem | None:
        """Create a question on a spec (text or choice)."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return None
        qa = SpecQAItem(
            spec_id=spec_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: SpecQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> SpecQAItem | None:
        """Answer a spec Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await self.db.get(SpecQAItem, qa_id)
        if not qa:
            return None

        spec = await self.db.get(Spec, qa.spec_id)
        board = await self.db.get(Board, spec.board_id) if spec else None
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="spec",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, spec_id: str) -> list[SpecQAItem]:
        """List all Q&A items for a spec."""
        query = (
            select(SpecQAItem)
            .where(SpecQAItem.spec_id == spec_id)
            .order_by(SpecQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(SpecQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class SpecKnowledgeService:
    """Service for spec knowledge base operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_knowledge(self, spec_id: str, user_id: str, data: SpecKnowledgeCreate) -> SpecKnowledgeBase | None:
        """Create a knowledge base item on a spec."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return None
        kb = SpecKnowledgeBase(
            spec_id=spec_id,
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            created_by=user_id,
        )
        self.db.add(kb)
        await self.db.flush()
        await SpecResourcePropagationService(self.db).propagate_for_spec(
            board_id=spec.board_id,
            spec_id=spec_id,
            actor_id=user_id,
            trigger="spec_knowledge_created",
        )
        return kb

    async def get_knowledge(self, knowledge_id: str) -> SpecKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await self.db.get(SpecKnowledgeBase, knowledge_id)

    async def list_knowledge(self, spec_id: str) -> list[SpecKnowledgeBase]:
        """List all knowledge base items for a spec."""
        query = (
            select(SpecKnowledgeBase)
            .where(SpecKnowledgeBase.spec_id == spec_id)
            .order_by(SpecKnowledgeBase.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_knowledge(self, knowledge_id: str, data: SpecKnowledgeUpdate) -> SpecKnowledgeBase | None:
        """Update a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(kb, key, value)
        await self.db.flush()
        spec = await self.db.get(Spec, kb.spec_id)
        if spec is not None:
            await SpecResourcePropagationService(self.db).propagate_for_spec(
                board_id=spec.board_id,
                spec_id=kb.spec_id,
                actor_id=kb.created_by or "system",
                trigger="spec_knowledge_updated",
            )
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        spec_id = kb.spec_id
        kb_id = kb.id
        actor_id = kb.created_by or "system"
        spec = await self.db.get(Spec, spec_id)
        await self.db.delete(kb)
        await self.db.flush()
        if spec is not None:
            await SpecResourcePropagationService(self.db).propagate_for_spec(
                board_id=spec.board_id,
                spec_id=spec_id,
                actor_id=actor_id,
                trigger="spec_knowledge_deleted",
                removed_kb_ids={kb_id},
            )
        return True


class IdeationKnowledgeService:
    """Service for ideation knowledge base operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_knowledge(
        self,
        ideation_id: str,
        user_id: str,
        data: IdeationKnowledgeCreate,
    ) -> IdeationKnowledgeBase | None:
        """Create a knowledge base item on an ideation."""
        ideation = await self.db.get(Ideation, ideation_id)
        if not ideation:
            return None
        kb = IdeationKnowledgeBase(
            ideation_id=ideation_id,
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            created_by=user_id,
        )
        self.db.add(kb)
        await self.db.flush()
        return kb

    async def get_knowledge(self, knowledge_id: str) -> IdeationKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await self.db.get(IdeationKnowledgeBase, knowledge_id)

    async def list_knowledge(self, ideation_id: str) -> list[IdeationKnowledgeBase]:
        """List all knowledge base items for an ideation."""
        query = (
            select(IdeationKnowledgeBase)
            .where(IdeationKnowledgeBase.ideation_id == ideation_id)
            .order_by(IdeationKnowledgeBase.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_knowledge(
        self,
        knowledge_id: str,
        data: IdeationKnowledgeUpdate,
    ) -> IdeationKnowledgeBase | None:
        """Update a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(kb, key, value)
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        await self.db.delete(kb)
        return True


class ShareService:
    """Service for board sharing operations."""

    VALID_PERMISSIONS = ("viewer", "editor", "admin")

    def __init__(self, db: AsyncSession):
        self.db = db

    async def share_board(
        self, board_id: str, owner_id: str, realm_id: str, data: BoardShareCreate
    ) -> BoardShare | None:
        """Share a board with another user. Only owner/admin can share."""
        # Check board exists and caller is owner or admin
        if not await self._can_manage_shares(board_id, owner_id):
            return None

        if data.user_id == owner_id:
            return None  # Can't share with yourself

        share = BoardShare(
            board_id=board_id,
            user_id=data.user_id,
            realm_id=realm_id,
            permission=data.permission,
            shared_by=owner_id,
        )
        self.db.add(share)
        await self.db.flush()
        return share

    async def list_shares(self, board_id: str) -> list[BoardShare]:
        """List all shares for a board."""
        query = (
            select(BoardShare)
            .where(BoardShare.board_id == board_id)
            .order_by(BoardShare.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_share(
        self, share_id: str, caller_id: str, data: BoardShareUpdate
    ) -> BoardShare | None:
        """Update a share permission. Only owner/admin can update."""
        share = await self.db.get(BoardShare, share_id)
        if not share:
            return None

        if not await self._can_manage_shares(share.board_id, caller_id):
            return None

        share.permission = data.permission
        return share

    async def revoke_share(self, share_id: str, caller_id: str) -> bool:
        """Revoke a share. Owner/admin can revoke, or user can leave."""
        share = await self.db.get(BoardShare, share_id)
        if not share:
            return False

        # Allow if caller is the shared user (leaving) or can manage shares
        if share.user_id != caller_id and not await self._can_manage_shares(share.board_id, caller_id):
            return False

        await self.db.delete(share)
        return True

    async def get_user_permission(self, board_id: str, user_id: str) -> str | None:
        """Get a user's permission level for a board. Returns None if no access."""
        # Check if owner
        board = await self.db.get(Board, board_id)
        if not board:
            return None
        if board.owner_id == user_id:
            return "owner"

        # Check shares
        query = select(BoardShare).where(
            BoardShare.board_id == board_id,
            BoardShare.user_id == user_id,
        )
        result = await self.db.execute(query)
        share = result.scalar_one_or_none()
        return share.permission if share else None

    async def _can_manage_shares(self, board_id: str, user_id: str) -> bool:
        """Check if user is owner or admin of the board."""
        board = await self.db.get(Board, board_id)
        if not board:
            return False
        if board.owner_id == user_id:
            return True

        # Check if admin via share
        query = select(BoardShare).where(
            BoardShare.board_id == board_id,
            BoardShare.user_id == user_id,
            BoardShare.permission == "admin",
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None


class TopicOperationError(ValueError):
    """Domain error with a stable code for Topic operations."""

    def __init__(self, message: str, *, code: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class TopicNameConflictError(TopicOperationError):
    def __init__(self, message: str = "Topic name already exists in this board"):
        super().__init__(message, code="topic_name_conflict")


class TopicNotEmptyError(TopicOperationError):
    def __init__(self, *, active_count: int, archived_count: int):
        total_count = active_count + archived_count
        super().__init__(
            "Topic has associated Stories and cannot be deleted",
            code="topic_not_empty",
            details={
                "active_count": active_count,
                "archived_count": archived_count,
                "total_associated_count": total_count,
                "suggested_actions": ["merge", "move_stories", "archive"],
            },
        )


class InvalidTopicMergeError(TopicOperationError):
    def __init__(self, message: str):
        super().__init__(message, code="invalid_merge")


class StoryService:
    """Service for Topic and Story operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _STORY_TRANSITIONS: dict[StoryStatus, list[StoryStatus]] = {
        StoryStatus.DRAFT: [StoryStatus.TRIAGE, StoryStatus.READY],
        StoryStatus.TRIAGE: [StoryStatus.DRAFT, StoryStatus.READY],
        StoryStatus.READY: [StoryStatus.TRIAGE],
        StoryStatus.CONVERTED: [],
    }
    _EDITABLE_IDEATION_STATUSES = (
        IdeationStatus.DRAFT,
        IdeationStatus.REVIEW,
        IdeationStatus.APPROVED,
        IdeationStatus.EVALUATING,
    )

    async def _ensure_board(self, board_id: str, user_id: str, skip_ownership_check: bool = False) -> Board | None:
        query = select(Board).where(Board.id == board_id)
        if not skip_ownership_check:
            query = query.where(Board.owner_id == user_id)
        return (await self.db.execute(query)).scalar_one_or_none()

    async def _topic_for_board(self, topic_id: str, board_id: str) -> Topic | None:
        return (await self.db.execute(
            select(Topic).where(Topic.id == topic_id, Topic.board_id == board_id)
        )).scalar_one_or_none()

    async def _log_activity(self, **kwargs: Any) -> None:
        self.db.add(ActivityLog(**kwargs))

    @staticmethod
    def _archived_topic_name(name: str, topic_id: str) -> str:
        suffix = f" [archived {topic_id[:8]}]"
        return f"{name[: max(1, 255 - len(suffix))]}{suffix}"

    async def _topic_story_counts(self, topic_id: str) -> dict[str, int]:
        rows = (await self.db.execute(
            select(Story.archived, func.count(Story.id))
            .where(Story.topic_id == topic_id)
            .group_by(Story.archived)
        )).all()
        counts = {"active_count": 0, "archived_count": 0}
        for archived, count in rows:
            key = "archived_count" if archived else "active_count"
            counts[key] = int(count or 0)
        counts["total_associated_count"] = counts["active_count"] + counts["archived_count"]
        return counts

    async def _attach_topic_counts(self, topic: Topic) -> Topic:
        counts = await self._topic_story_counts(topic.id)
        setattr(topic, "story_count", counts["active_count"])
        setattr(topic, "active_count", counts["active_count"])
        setattr(topic, "archived_count", counts["archived_count"])
        setattr(topic, "total_associated_count", counts["total_associated_count"])
        return topic

    async def _ensure_active_topic_name_available(
        self,
        board_id: str,
        name: str,
        *,
        exclude_topic_id: str | None = None,
    ) -> None:
        conditions = [
            Topic.board_id == board_id,
            Topic.archived.is_(False),
            func.lower(Topic.name) == name.lower(),
        ]
        if exclude_topic_id:
            conditions.append(Topic.id != exclude_topic_id)
        existing = await self.db.execute(select(Topic.id).where(*conditions).limit(1))
        if existing.scalar_one_or_none():
            raise TopicNameConflictError()

    async def _free_archived_exact_name(
        self,
        board_id: str,
        name: str,
        *,
        exclude_topic_id: str | None = None,
    ) -> list[str]:
        conditions = [
            Topic.board_id == board_id,
            Topic.archived.is_(True),
            Topic.name == name,
        ]
        if exclude_topic_id:
            conditions.append(Topic.id != exclude_topic_id)
        archived_topics = list((await self.db.execute(select(Topic).where(*conditions))).scalars().all())
        renamed: list[str] = []
        for archived_topic in archived_topics:
            archived_topic.name = self._archived_topic_name(archived_topic.name, archived_topic.id)
            renamed.append(archived_topic.id)
        return renamed

    async def create_topic(
        self, board_id: str, user_id: str, data: TopicCreate, skip_ownership_check: bool = False
    ) -> Topic | None:
        if not await self._ensure_board(board_id, user_id, skip_ownership_check):
            return None
        name = data.name.strip()
        await self._ensure_active_topic_name_available(board_id, name)
        renamed_archived_topics = await self._free_archived_exact_name(board_id, name)
        topic = Topic(board_id=board_id, name=name, description=data.description, created_by=user_id)
        self.db.add(topic)
        await self.db.flush()
        await self.db.refresh(topic)
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="topic_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "topic_id": topic.id,
                "name": topic.name,
                "renamed_archived_topics": renamed_archived_topics,
            },
        )
        return await self._attach_topic_counts(topic)

    async def list_topics(self, board_id: str, include_archived: bool = False) -> list[Topic]:
        query = select(Topic).where(Topic.board_id == board_id)
        if not include_archived:
            query = query.where(Topic.archived.is_(False))
        topics = list((await self.db.execute(query.order_by(Topic.name.asc()))).scalars().all())
        count_rows = (await self.db.execute(
            select(Story.topic_id, Story.archived, func.count(Story.id))
            .where(Story.board_id == board_id)
            .group_by(Story.topic_id, Story.archived)
        )).all()
        counts: dict[str, dict[str, int]] = {}
        for topic_id, archived, count in count_rows:
            if not topic_id:
                continue
            bucket = counts.setdefault(topic_id, {"active_count": 0, "archived_count": 0})
            key = "archived_count" if archived else "active_count"
            bucket[key] = int(count or 0)
        for topic in topics:
            topic_counts = counts.get(topic.id, {"active_count": 0, "archived_count": 0})
            total_count = topic_counts["active_count"] + topic_counts["archived_count"]
            setattr(topic, "story_count", topic_counts["active_count"])
            setattr(topic, "active_count", topic_counts["active_count"])
            setattr(topic, "archived_count", topic_counts["archived_count"])
            setattr(topic, "total_associated_count", total_count)
        return topics

    async def update_topic(self, topic_id: str, user_id: str, data: TopicUpdate) -> Topic | None:
        topic = await self.db.get(Topic, topic_id)
        if not topic:
            return None
        original_archived = bool(topic.archived)
        original_name = topic.name
        update_data = data.model_dump(exclude_unset=True)
        target_archived = bool(update_data.get("archived", topic.archived))
        if "name" in update_data and update_data["name"] is not None:
            name = update_data.pop("name").strip()
            if not target_archived:
                await self._ensure_active_topic_name_available(topic.board_id, name, exclude_topic_id=topic.id)
                await self._free_archived_exact_name(topic.board_id, name, exclude_topic_id=topic.id)
            topic.name = name
        elif original_archived and not target_archived:
            await self._ensure_active_topic_name_available(topic.board_id, topic.name, exclude_topic_id=topic.id)
            await self._free_archived_exact_name(topic.board_id, topic.name, exclude_topic_id=topic.id)
        for key, value in update_data.items():
            setattr(topic, key, value)
        counts = await self._topic_story_counts(topic.id)
        if original_archived != bool(topic.archived):
            action = "topic_restored" if original_archived else "topic_archived"
        else:
            action = "topic_updated"
        actor_name = await resolve_actor_name(self.db, user_id, topic.board_id)
        await self._log_activity(
            board_id=topic.board_id,
            action=action,
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "topic_id": topic.id,
                "fields": list(data.model_dump(exclude_unset=True).keys()),
                "previous_name": original_name,
                "name": topic.name,
                "previous_archived": original_archived,
                "archived": bool(topic.archived),
                **counts,
            },
        )
        await self.db.flush()
        await self.db.refresh(topic)
        return await self._attach_topic_counts(topic)

    async def delete_topic(self, topic_id: str, user_id: str) -> Topic | None:
        topic = await self.db.get(Topic, topic_id)
        if not topic:
            return None
        counts = await self._topic_story_counts(topic.id)
        if counts["total_associated_count"] > 0:
            raise TopicNotEmptyError(
                active_count=counts["active_count"],
                archived_count=counts["archived_count"],
            )
        actor_name = await resolve_actor_name(self.db, user_id, topic.board_id)
        await self._log_activity(
            board_id=topic.board_id,
            action="topic_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"topic_id": topic.id, "name": topic.name, **counts},
        )
        await self.db.delete(topic)
        await self.db.flush()
        return topic

    async def merge_topics(self, source_topic_id: str, target_topic_id: str, user_id: str) -> dict[str, Any] | None:
        if source_topic_id == target_topic_id:
            raise InvalidTopicMergeError("Source and target Topics must be different")
        source_topic = await self.db.get(Topic, source_topic_id)
        target_topic = await self.db.get(Topic, target_topic_id)
        if not source_topic or not target_topic:
            return None
        if source_topic.board_id != target_topic.board_id:
            raise InvalidTopicMergeError("Source and target Topics must belong to the same board")
        if target_topic.archived:
            raise InvalidTopicMergeError("Target Topic must be active")

        source_counts = await self._topic_story_counts(source_topic.id)
        target_counts_before = await self._topic_story_counts(target_topic.id)
        await self.db.execute(
            update(Story)
            .where(Story.topic_id == source_topic.id)
            .values(topic_id=target_topic.id)
        )
        source_topic.archived = True
        await self.db.flush()
        target_counts_after = await self._topic_story_counts(target_topic.id)
        actor_name = await resolve_actor_name(self.db, user_id, source_topic.board_id)
        await self._log_activity(
            board_id=source_topic.board_id,
            action="topic_merged",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={
                "source_topic_id": source_topic.id,
                "source_topic_name": source_topic.name,
                "target_topic_id": target_topic.id,
                "target_topic_name": target_topic.name,
                "moved_count": source_counts["total_associated_count"],
                "active_count": source_counts["active_count"],
                "archived_count": source_counts["archived_count"],
                "target_total_before": target_counts_before["total_associated_count"],
                "target_total_after": target_counts_after["total_associated_count"],
            },
        )
        await self.db.flush()
        await self.db.refresh(source_topic)
        await self.db.refresh(target_topic)
        await self._attach_topic_counts(source_topic)
        await self._attach_topic_counts(target_topic)
        return {
            "success": True,
            "source": source_topic,
            "target": target_topic,
            "moved_count": source_counts["total_associated_count"],
            "active_count": source_counts["active_count"],
            "archived_count": source_counts["archived_count"],
            "target_total_before": target_counts_before["total_associated_count"],
            "target_total_after": target_counts_after["total_associated_count"],
        }

    async def create_story(
        self, board_id: str, user_id: str, data: StoryCreate, skip_ownership_check: bool = False
    ) -> Story | None:
        if not await self._ensure_board(board_id, user_id, skip_ownership_check):
            return None
        topic = await self._topic_for_board(data.topic_id, board_id)
        if not topic or topic.archived:
            raise ValueError("Topic not found in this board")
        story = Story(
            board_id=board_id,
            topic_id=data.topic_id,
            title=data.title.strip(),
            description=data.description,
            actor=data.actor,
            goal=data.goal,
            benefit=data.benefit,
            labels=data.labels,
            status=data.status,
            assignee_id=data.assignee_id,
            created_by=user_id,
            screen_mockups=None,  # assigned after the Design System gate (below)
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate mockups submitted
        # at creation BEFORE persistence (old=[] baseline so every screen is evaluated).
        _submitted_mockups = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in (data.screen_mockups or [])
        ] or None
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, story, _submitted_mockups, entity_type="story"
            )
            story.screen_mockups = _submitted_mockups
        self.db.add(story)
        await self.db.flush()
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="story_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id, "topic_id": story.topic_id, "title": story.title},
        )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import StoryCreated

        await event_publish(
            StoryCreated(
                board_id=board_id,
                actor_id=user_id,
                story_id=story.id,
                topic_id=story.topic_id,
                status=story.status.value,
            ),
            session=self.db,
        )
        return await self.get_story(story.id)

    async def get_story(self, story_id: str) -> Story | None:
        query = (
            select(Story)
            .options(selectinload(Story.topic))
            .options(selectinload(Story.ideation_links))
            .where(Story.id == story_id)
        )
        return (await self.db.execute(query)).scalar_one_or_none()

    async def list_stories(
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        topic_id: str | None = None,
        search: str | None = None,
        linked: bool | None = None,
        converted: bool | None = None,
        include_archived: bool = False,
    ) -> list[Story]:
        query = (
            select(Story)
            .options(selectinload(Story.topic))
            .options(selectinload(Story.ideation_links))
            .where(Story.board_id == board_id)
        )
        if status_filter:
            query = query.where(Story.status == StoryStatus(status_filter))
        if topic_id:
            query = query.where(Story.topic_id == topic_id)
        if search:
            pattern = f"%{search}%"
            query = query.where(
                Story.title.ilike(pattern)
                | Story.description.ilike(pattern)
                | Story.actor.ilike(pattern)
                | Story.goal.ilike(pattern)
                | Story.benefit.ilike(pattern)
            )
        if converted is not None:
            query = query.where(Story.status == StoryStatus.CONVERTED if converted else Story.status != StoryStatus.CONVERTED)
        if not include_archived:
            query = query.where(Story.archived.is_(False))
        stories = list((await self.db.execute(query.order_by(Story.updated_at.desc()))).scalars().all())
        if linked is None:
            return stories
        return [story for story in stories if (len(story.ideation_links or []) > 0) is linked]

    async def update_story(self, story_id: str, user_id: str, data: StoryUpdate) -> Story | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        if story.archived:
            raise ValueError("This story is archived. Restore it before editing.")
        update_data = data.model_dump(exclude_unset=True)
        if "topic_id" in update_data and update_data["topic_id"] is not None:
            topic = await self._topic_for_board(update_data["topic_id"], story.board_id)
            if not topic or topic.archived:
                raise ValueError("Topic not found in this board")
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                item.model_dump() if hasattr(item, "model_dump") else item
                for item in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, story, update_data["screen_mockups"], entity_type="story"
            )
        for key, value in update_data.items():
            setattr(story, key, value)
            if key in {"labels", "screen_mockups"}:
                flag_modified(story, key)
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id, "fields": list(update_data.keys())},
        )
        if update_data:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import StoryUpdated

            await event_publish(
                StoryUpdated(
                    board_id=story.board_id,
                    actor_id=user_id,
                    story_id=story.id,
                    changed_fields=list(update_data.keys()),
                ),
                session=self.db,
            )
        await self.db.flush()
        return await self.get_story(story_id)

    async def move_story(self, story_id: str, user_id: str, data: StoryMove) -> Story | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        if story.archived:
            raise ValueError("This story is archived. Restore it before changing status.")
        old_status = story.status
        allowed = self._STORY_TRANSITIONS.get(old_status, [])
        if data.status not in allowed and data.status != old_status:
            allowed_str = ", ".join(status.value for status in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move story from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )
        story.status = data.status
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id, "from_status": old_status.value, "to_status": data.status.value},
        )
        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import StoryMoved

            await event_publish(
                StoryMoved(
                    board_id=story.board_id,
                    actor_id=user_id,
                    story_id=story.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
        await self.db.flush()
        return await self.get_story(story_id)

    async def archive_story(self, story_id: str, user_id: str, archived: bool = True) -> Story | None:
        story = await self.get_story(story_id)
        if not story:
            return None
        story.archived = archived
        story.pre_archive_status = story.status.value if archived else None
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_archived" if archived else "story_restored",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story.id},
        )
        await self.db.flush()
        return await self.get_story(story_id)

    async def link_story_to_ideation(
        self, story_id: str, ideation_id: str, user_id: str, *, mark_converted: bool = True
    ) -> StoryIdeationLink | None:
        story = await self.get_story(story_id)
        ideation = await self.db.get(Ideation, ideation_id)
        if not story or not ideation or story.board_id != ideation.board_id:
            return None
        if ideation.status not in self._EDITABLE_IDEATION_STATUSES:
            allowed = ", ".join(status.value for status in self._EDITABLE_IDEATION_STATUSES)
            raise ValueError(
                f"Story can only be linked to editable Ideations. "
                f"Current ideation status is '{ideation.status.value}'. Allowed statuses: {allowed}."
            )
        link = (await self.db.execute(
            select(StoryIdeationLink).where(
                StoryIdeationLink.story_id == story_id,
            )
        )).scalar_one_or_none()
        if link:
            if link.ideation_id == ideation_id:
                raise ValueError("Story is already linked to this Ideation.")
            raise ValueError("Story is already linked to another Ideation. A Story can only link to one Ideation.")
        link = StoryIdeationLink(
            board_id=story.board_id,
            story_id=story_id,
            ideation_id=ideation_id,
            created_by=user_id,
        )
        self.db.add(link)
        await self.db.flush()
        # mark_converted remains accepted for API compatibility; successful links now always convert.
        if story.status != StoryStatus.CONVERTED:
            story.status = StoryStatus.CONVERTED
            await self.db.flush()
        actor_name = await resolve_actor_name(self.db, user_id, story.board_id)
        await self._log_activity(
            board_id=story.board_id,
            action="story_linked_to_ideation",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"story_id": story_id, "ideation_id": ideation_id},
        )
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import StoryLinkedToIdeation

        await event_publish(
            StoryLinkedToIdeation(
                board_id=story.board_id,
                actor_id=user_id,
                story_id=story_id,
                ideation_id=ideation_id,
            ),
            session=self.db,
        )
        return link

    async def convert_stories(
        self,
        board_id: str,
        user_id: str,
        data: StoryConversionRequest,
        *,
        skip_ownership_check: bool = False,
    ) -> tuple[Ideation, list[StoryIdeationLink], int] | None:
        if not await self._ensure_board(board_id, user_id, skip_ownership_check):
            return None
        stories = list((await self.db.execute(
            select(Story)
            .options(selectinload(Story.topic))
            .options(selectinload(Story.ideation_links))
            .where(Story.board_id == board_id, Story.id.in_(data.story_ids), Story.archived.is_(False))
        )).scalars().all())
        if len(stories) != len(set(data.story_ids)):
            raise ValueError("One or more Stories were not found in this board")
        not_ready = [
            story.title
            for story in stories
            if story.status not in (StoryStatus.READY, StoryStatus.CONVERTED)
        ]
        if not_ready:
            raise ValueError("Only ready Stories can be converted to Ideation")

        if data.ideation_id:
            ideation = await self.db.get(Ideation, data.ideation_id)
            if not ideation or ideation.board_id != board_id:
                raise ValueError("Ideation not found in this board")
        else:
            story_lines = []
            for story in stories:
                topic_name = story.topic.name if story.topic else story.topic_id
                story_lines.append(
                    f"- {story.title} (topic: {topic_name})"
                    f"{f'; actor: {story.actor}' if story.actor else ''}"
                    f"{f'; goal: {story.goal}' if story.goal else ''}"
                    f"{f'; benefit: {story.benefit}' if story.benefit else ''}"
                )
            ideation = await IdeationService(self.db).create_ideation(
                board_id,
                user_id,
                IdeationCreate(
                    title=data.title or stories[0].title,
                    description=data.description or "Ideation created from selected Stories.",
                    problem_statement=data.problem_statement or "Selected Stories:\n" + "\n".join(story_lines),
                    proposed_approach=data.proposed_approach,
                    labels=sorted({label for story in stories for label in (story.labels or [])}) or None,
                ),
                skip_ownership_check=skip_ownership_check,
            )
            if not ideation:
                return None

        links: list[StoryIdeationLink] = []
        for story in stories:
            link = await self.link_story_to_ideation(
                story.id, ideation.id, user_id, mark_converted=data.mark_converted
            )
            if link:
                links.append(link)

        _old_ideation_mockups = list(ideation.screen_mockups or [])
        propagated = self._propagate_story_mockups(stories, ideation, data.mockup_ids)
        if propagated:
            flag_modified(ideation, "screen_mockups")
            # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): convert_stories
            # rewrites story mockups with FRESH ids onto the ideation — gate the new
            # entries (delta vs the pre-propagation set) BEFORE flush so a non-compliant
            # legacy mockup can't be laundered onto a blocking board.
            from okto_pulse.core.services.design_system import MockupDesignSystemGate
            await MockupDesignSystemGate(self.db).gate_delta(
                ideation.board_id, _old_ideation_mockups, list(ideation.screen_mockups or []),
                entity_type="ideation", entity_id=ideation.id,
            )
        await self.db.flush()
        await self.db.refresh(ideation)
        for link in links:
            await self.db.refresh(link)
        return ideation, links, propagated

    def _propagate_story_mockups(
        self,
        stories: list[Story],
        ideation: Ideation,
        mockup_ids: list[str] | None,
    ) -> int:
        selected = set(mockup_ids) if mockup_ids is not None else None
        target = list(ideation.screen_mockups or [])
        propagated = 0
        for story in stories:
            for mockup in story.screen_mockups or []:
                if not isinstance(mockup, dict):
                    continue
                mockup_id = mockup.get("id")
                if selected is not None and mockup_id not in selected:
                    continue
                copied = dict(mockup)
                copied["id"] = f"story_mockup_{secrets.token_hex(8)}"
                copied["origin_id"] = mockup_id
                copied["origin_story_id"] = story.id
                copied["origin_entity_type"] = "story"
                copied["order"] = len(target)
                target.append(copied)
                propagated += 1
        if propagated:
            ideation.screen_mockups = target
        return propagated


class AmbiguityGateError(ValueError):
    """Raised when the Max ambiguity gate blocks an evaluating -> done transition.

    A ValueError subclass (spec 2485780b, BR4) so the MCP move tool's existing
    ``except ValueError`` surfaces the actionable detail unchanged, while REST
    callers catch it specifically to return HTTP 400 without altering the
    behavior of unrelated move errors.
    """


class IdeationService:
    """Service for ideation operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _STATUS_ORDER = {
        IdeationStatus.DRAFT: 0,
        IdeationStatus.REVIEW: 1,
        IdeationStatus.APPROVED: 2,
        IdeationStatus.EVALUATING: 3,
        IdeationStatus.DONE: 4,
        IdeationStatus.CANCELLED: 4,
    }

    async def _record_history(
        self,
        ideation_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for an ideation."""
        entry = IdeationHistory(
            ideation_id=ideation_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, ideation_id: str, limit: int = 50) -> list[IdeationHistory]:
        """List history entries for an ideation, newest first."""
        query = (
            select(IdeationHistory)
            .where(IdeationHistory.ideation_id == ideation_id)
            .order_by(IdeationHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_ideation(
        self, board_id: str, user_id: str, data: IdeationCreate, skip_ownership_check: bool = False
    ) -> Ideation | None:
        """Create a new ideation in a board."""
        if skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id)
        else:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        ideation = Ideation(
            board_id=board_id,
            title=data.title,
            description=data.description,
            problem_statement=data.problem_statement,
            proposed_approach=data.proposed_approach,
            scope_assessment=data.scope_assessment,
            complexity=IdeationComplexity(data.complexity) if data.complexity else None,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
        )
        self.db.add(ideation)
        await self.db.flush()

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="ideation_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "ideation_id": ideation.id},
        )
        await self._record_history(
            ideation_id=ideation.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Ideation created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": IdeationStatus.DRAFT.value},
                *([{"field": "problem_statement", "old": None, "new": data.problem_statement}] if data.problem_statement else []),
                *([{"field": "proposed_approach", "old": None, "new": data.proposed_approach}] if data.proposed_approach else []),
            ],
        )
        return ideation

    async def get_ideation(self, ideation_id: str) -> Ideation | None:
        """Get an ideation by ID with refinements, specs, and qa_items."""
        query = (
            select(Ideation)
            .options(selectinload(Ideation.refinements).selectinload(Refinement.architecture_designs))
            .options(selectinload(Ideation.specs).selectinload(Spec.architecture_designs))
            .options(
                selectinload(Ideation.story_links)
                .selectinload(StoryIdeationLink.story)
                .selectinload(Story.ideation_links)
            )
            .options(selectinload(Ideation.knowledge_bases))
            .options(selectinload(Ideation.qa_items))
            .options(selectinload(Ideation.architecture_designs))
            .where(Ideation.id == ideation_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_ideations(self, board_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Ideation]:
        """List ideations for a board, optionally filtered by status."""
        query = (
            select(Ideation)
            .options(selectinload(Ideation.architecture_designs))
            .where(Ideation.board_id == board_id)
        )
        if status_filter:
            query = query.where(Ideation.status == IdeationStatus(status_filter))
        if not include_archived:
            query = query.where(Ideation.archived.is_(False))
        query = query.order_by(Ideation.updated_at.desc())
        result = await self.db.execute(query)
        rows = list(result.scalars().all())
        await _attach_open_qa_counts(self.db, rows, IdeationQAItem, "ideation_id")
        return rows

    async def update_ideation(self, ideation_id: str, user_id: str, data: IdeationUpdate) -> Ideation | None:
        """Update an ideation. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("This ideation is archived. Restore it first before making changes.")

        if ideation.status != IdeationStatus.DRAFT:
            raise ValueError(
                f"Cannot edit ideation in '{ideation.status.value}' status. "
                f"Move it back to 'draft' to make changes."
            )

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "description", "problem_statement", "proposed_approach",
            "scope_assessment",
        }
        bumps_version = bool(content_fields & update_data.keys())

        old_data = {k: getattr(ideation, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, ideation, update_data["screen_mockups"], entity_type="ideation"
            )

        ideation_json_fields = {"scope_assessment", "labels", "screen_mockups"}
        for key, value in update_data.items():
            if key == "complexity" and value is not None:
                setattr(ideation, key, IdeationComplexity(value))
            else:
                setattr(ideation, key, value)
            if key in ideation_json_fields:
                flag_modified(ideation, key)

        if bumps_version:
            ideation.version += 1

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id, "version": ideation.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                ideation_id=ideation_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=ideation.version,
                summary=f"Updated: {changed_fields}",
            )
        return ideation

    # Allowed ideation transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Evaluating, Cancelled
    # Evaluating → Approved, Done, Cancelled
    # Done → Draft (new version)
    _IDEATION_TRANSITIONS: dict[IdeationStatus, list[IdeationStatus]] = {
        IdeationStatus.DRAFT: [IdeationStatus.REVIEW, IdeationStatus.CANCELLED],
        IdeationStatus.REVIEW: [IdeationStatus.DRAFT, IdeationStatus.APPROVED, IdeationStatus.CANCELLED],
        IdeationStatus.APPROVED: [IdeationStatus.REVIEW, IdeationStatus.EVALUATING, IdeationStatus.CANCELLED],
        IdeationStatus.EVALUATING: [IdeationStatus.APPROVED, IdeationStatus.DONE, IdeationStatus.CANCELLED],
        IdeationStatus.DONE: [IdeationStatus.DRAFT],
        IdeationStatus.CANCELLED: [],
    }

    @staticmethod
    def _resolve_ideation_ambiguity_config(board: Board | None) -> dict[str, Any]:
        """Resolve the Max ambiguity gate config from board settings (spec 2485780b).

        Reads through the same ``settings.get(key, default)`` normalization path
        used by other governance settings, so missing legacy settings resolve to
        defaults (gate disabled, threshold 3). The threshold is clamped to 1-5
        defensively in case a legacy row persisted an out-of-range value before
        BoardSettings validation existed.
        """
        settings = (board.settings if board else None) or {}
        threshold = int(settings.get("max_ideation_ambiguity", 3))
        threshold = max(1, min(5, threshold))
        return {
            "require_ideation_ambiguity_gate": bool(
                settings.get("require_ideation_ambiguity_gate", False)
            ),
            "max_ideation_ambiguity": threshold,
        }

    async def set_ambiguity_gate_skip(
        self,
        ideation_id: str,
        user_id: str,
        skip: bool,
        *,
        source: str,
        actor_name: str | None = None,
    ) -> Ideation | None:
        """Dedicated write path for the per-ideation skip_ambiguity_gate flag (spec 2485780b).

        Works while the ideation is in evaluating status (or any non-draft
        status) WITHOUT routing through the generic update_ideation draft-only
        guard — so it cannot be used to smuggle other non-draft edits past that
        guard. Rejects archived ideations. Emits an auditable activity entry
        (ideation.ambiguity_gate_skip_updated) carrying actor, source path and
        the old -> new skip value. Both the REST endpoint and the MCP mirror
        call THIS method, so their behavior, validation and audit trail are
        identical (BR7 / FR5 / FR14 / FR15).
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("Cannot update ambiguity gate skip for archived ideation.")

        old_value = bool(ideation.skip_ambiguity_gate)
        new_value = bool(skip)
        ideation.skip_ambiguity_gate = new_value

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation.ambiguity_gate_skip_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "ideation_id": ideation_id,
                "source": source,
                "old_value": old_value,
                "new_value": new_value,
            },
        )
        return ideation

    @staticmethod
    def _parse_ambiguity_score(raw: Any) -> int | None:
        """Parse scope_assessment.ambiguity as an integer 1-5 (spec 2485780b TR8).

        Returns None when the value is missing, non-numeric, or outside the
        1-5 range so the gate treats it as 'not properly evaluated' and
        fails closed. ``bool`` is rejected explicitly (it is an ``int``
        subclass but never a valid ambiguity score).
        """
        if isinstance(raw, bool):
            return None
        if isinstance(raw, int):
            value = raw
        elif isinstance(raw, float) and raw.is_integer():
            value = int(raw)
        elif isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
            value = int(raw.strip())
        else:
            return None
        if not 1 <= value <= 5:
            return None
        return value

    async def _enforce_ambiguity_gate(self, ideation: Ideation) -> None:
        """Enforce the Max ambiguity gate on an evaluating -> done transition.

        Spec 2485780b (TR6/TR8/TR9, BR2/BR4/BR6): only acts when the board gate
        is enabled and the ideation has not explicitly skipped it. Blocks the
        transition when the evaluated ambiguity is missing, non-numeric, or
        greater than the configured threshold, raising AmbiguityGateError with
        an actionable detail. Reads board settings through the shared
        normalization path and never touches evaluation/KG/cognitive/resource
        subsystems. The caller invokes this BEFORE ResourceGate so ambiguity
        errors take precedence.
        """
        board = await self.db.get(Board, ideation.board_id)
        config = self._resolve_ideation_ambiguity_config(board)
        if not config["require_ideation_ambiguity_gate"]:
            return
        if bool(getattr(ideation, "skip_ambiguity_gate", False)):
            return

        threshold = config["max_ideation_ambiguity"]
        scope = ideation.scope_assessment or {}
        score = self._parse_ambiguity_score(scope.get("ambiguity"))
        if score is None:
            raise AmbiguityGateError(
                "Max ambiguity gate failed: ambiguity has not been evaluated. "
                "Evaluate the ideation's ambiguity (e.g. via Q&A). Skipping the gate "
                "for this ideation is a human decision applied through the authorized "
                "UI/REST control — an agent cannot apply the skip and should request a "
                "human decision."
            )
        if score > threshold:
            raise AmbiguityGateError(
                f"Max ambiguity gate failed: ambiguity score {score} exceeds "
                f"configured max {threshold}. Reduce ambiguity through Q&A, or raise "
                f"the threshold / disable the board gate. Skipping the gate for this "
                f"ideation is a human decision applied through the authorized UI/REST "
                f"control — an agent cannot apply the skip and should request a human "
                f"decision."
            )

    async def move_ideation(
        self, ideation_id: str, user_id: str, data: IdeationMove, actor_name: str | None = None
    ) -> Ideation | None:
        """Move an ideation to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Evaluating → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Evaluation can only happen in Evaluating status
        - Editing only allowed in Draft
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("This ideation is archived. Restore it first before changing status.")

        old_status = ideation.status
        allowed = self._IDEATION_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move ideation from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, ideation.board_id)

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=ideation.board_id,
            actor_id=user_id,
            entity_type="ideation",
            entity_id=ideation.id,
            critical_action=_critical_ideation_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=resolved_name,
        )

        # Snapshot on done
        if data.status == IdeationStatus.DONE:
            # Max ambiguity gate (spec 2485780b): only evaluating -> done, and
            # BEFORE ResourceGate so ambiguity errors take precedence (BR4).
            if old_status == IdeationStatus.EVALUATING:
                await self._enforce_ambiguity_gate(ideation)
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                ideation.board_id,
                "ideation",
                ideation.id,
                phase="ideation_done",
            )
            await self._create_snapshot(ideation, user_id)

        # Version bump on back-to-draft from done
        if data.status == IdeationStatus.DRAFT and old_status == IdeationStatus.DONE:
            ideation.version += 1

        ideation.status = data.status

        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "ideation_id": ideation_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": ideation.version,
            },
        )
        summary = f"Status: {old_status.value} → {data.status.value}"
        if data.status == IdeationStatus.DONE:
            summary += f" (snapshot v{ideation.version} created)"
        elif data.status == IdeationStatus.DRAFT and old_status == IdeationStatus.DONE:
            summary += f" (new iteration v{ideation.version})"

        await self._record_history(
            ideation_id=ideation_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=summary,
            version=ideation.version,
        )
        return ideation

    async def _create_snapshot(self, ideation: "Ideation", user_id: str) -> Any:
        """Create an immutable snapshot of the ideation's current state."""
        from okto_pulse.core.models.db import IdeationSnapshot

        qa_snapshot = []
        for qa in (ideation.qa_items or []):
            qa_snapshot.append({
                "question": qa.question,
                "question_type": qa.question_type,
                "choices": qa.choices,
                "answer": qa.answer,
                "selected": qa.selected,
                "asked_by": qa.asked_by,
                "answered_by": qa.answered_by,
            })

        snapshot = IdeationSnapshot(
            ideation_id=ideation.id,
            version=ideation.version,
            title=ideation.title,
            description=ideation.description,
            problem_statement=ideation.problem_statement,
            proposed_approach=ideation.proposed_approach,
            scope_assessment=ideation.scope_assessment,
            complexity=ideation.complexity.value if ideation.complexity else None,
            labels=ideation.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            created_by=user_id,
        )
        self.db.add(snapshot)
        await self.db.flush()
        return snapshot

    async def list_snapshots(self, ideation_id: str) -> list:
        """List all snapshots for an ideation."""
        from okto_pulse.core.models.db import IdeationSnapshot
        query = (
            select(IdeationSnapshot)
            .where(IdeationSnapshot.ideation_id == ideation_id)
            .order_by(IdeationSnapshot.version.desc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_snapshot(self, ideation_id: str, version: int):
        """Get a specific version snapshot."""
        from okto_pulse.core.models.db import IdeationSnapshot
        query = select(IdeationSnapshot).where(
            IdeationSnapshot.ideation_id == ideation_id,
            IdeationSnapshot.version == version,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def delete_ideation(self, ideation_id: str, user_id: str) -> bool:
        """Delete an ideation."""
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return False

        board_id = ideation.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(ideation)

        await self._log_activity(
            board_id=board_id,
            action="ideation_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id},
        )
        return True

    async def evaluate_complexity(self, ideation_id: str, user_id: str) -> Ideation | None:
        """Evaluate and set complexity based on scope_assessment.

        Only allowed in Evaluating status.

        Rules:
        - domains >= 3 OR ambiguity >= 3 OR dependencies >= 3 -> large
        - any >= 2 -> medium
        - else -> small
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.EVALUATING:
            raise ValueError(
                f"Evaluation can only be performed in 'evaluating' status (current: '{ideation.status.value}'). "
                f"Move the ideation to 'evaluating' first."
            )

        scope = ideation.scope_assessment or {}
        domains = scope.get("domains", 1)
        ambiguity = scope.get("ambiguity", 1)
        dependencies = scope.get("dependencies", 1)

        if domains >= 3 or ambiguity >= 3 or dependencies >= 3:
            new_complexity = IdeationComplexity.LARGE
        elif domains >= 2 or ambiguity >= 2 or dependencies >= 2:
            new_complexity = IdeationComplexity.MEDIUM
        else:
            new_complexity = IdeationComplexity.SMALL

        old_complexity = ideation.complexity
        ideation.complexity = new_complexity

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._record_history(
            ideation_id=ideation_id, action="complexity_evaluated", actor_id=user_id, actor_name=actor_name,
            changes=[{"field": "complexity", "old": old_complexity.value if old_complexity else None, "new": new_complexity.value}],
            summary=f"Complexity evaluated: {new_complexity.value}",
            version=ideation.version,
        )
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_complexity_evaluated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id, "complexity": new_complexity.value},
        )
        return ideation

    async def derive_spec(
        self, ideation_id: str, user_id: str, skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None, kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
    ) -> Spec | None:
        """Create a Spec draft linked to an ideation.

        Compiles context from the ideation's problem statement, proposed approach,
        scope assessment, and Q&A history. Artifacts (mockups, KBs) are automatically
        propagated. Use mockup_ids/kb_ids to select specific ones.

        Only allowed when ideation status is 'done'.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.DONE:
            raise ValueError("Spec can only be created from a 'done' ideation")

        if ideation.complexity and ideation.complexity != IdeationComplexity.SMALL:
            raise ValueError(
                f"Ideation has complexity '{ideation.complexity.value}' — "
                "create refinements first, then derive specs from refinements"
            )

        # Compile rich context from ideation data
        context_parts: list[str] = []
        if ideation.problem_statement:
            context_parts.append(f"## Problem Statement\n{ideation.problem_statement}")
        if ideation.proposed_approach:
            context_parts.append(f"## Proposed Approach\n{ideation.proposed_approach}")
        if ideation.scope_assessment:
            sa = ideation.scope_assessment
            context_parts.append(
                f"## Scope Assessment\n"
                f"- Domains: {sa.get('domains', '?')}/5\n"
                f"- Ambiguity: {sa.get('ambiguity', '?')}/5\n"
                f"- Dependencies: {sa.get('dependencies', '?')}/5\n"
                f"- Complexity: {ideation.complexity.value if ideation.complexity else 'not evaluated'}"
            )
        context = "\n\n".join(context_parts) if context_parts else ideation.description

        # Snapshot parent collections before flush: eager-loaded collections can
        # expire after create_spec flushes the new child entity.
        snapshot_qa = list(ideation.qa_items or [])
        snapshot_kbs = list(ideation.knowledge_bases or [])

        spec_data = SpecCreate(
            title=ideation.title,
            description=ideation.description,
            context=context,
            ideation_id=ideation_id,
            labels=ideation.labels,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            ideation.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
            # Propagate mockups and Q&A from ideation to spec
            await propagate_artifacts(
                db=self.db,
                source_mockups=ideation.screen_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=snapshot_kbs,
                target_entity=spec,
                target_kb_class=SpecKnowledgeBase,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=kb_ids,
                source_type="ideation",
                source_id=ideation.id,
                source_title=ideation.title,
                source_version=ideation.version,
            )
            await propagate_architecture_designs(
                self.db,
                source_parent_type="ideation",
                source_parent_id=ideation_id,
                target_parent_type="spec",
                target_parent_id=spec.id,
                actor_id=user_id,
                mode=architecture_propagation_mode,
                design_ids=architecture_design_ids,
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import IdeationDerivedToSpec

            await event_publish(
                IdeationDerivedToSpec(
                    board_id=ideation.board_id,
                    actor_id=user_id,
                    ideation_id=ideation_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
            await self._record_history(
                ideation_id=ideation_id, action="spec_draft_created", actor_id=user_id, actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=ideation.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class IdeationQAService:
    """Service for ideation Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, ideation_id: str, user_id: str, data: IdeationQACreate) -> IdeationQAItem | None:
        """Create a question on an ideation (text or choice)."""
        ideation = await self.db.get(Ideation, ideation_id)
        if not ideation:
            return None
        qa = IdeationQAItem(
            ideation_id=ideation_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: IdeationQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> IdeationQAItem | None:
        """Answer an ideation Q&A question (text or choice selection).

        Accepts `question_type in {"choice","single_choice","multi_choice"}`
        — `single_choice` is treated as an alias of `choice`. Only commits
        `answered_at`/`answered_by` when something was actually persisted,
        otherwise returns None so the route surfaces a 404 instead of a
        false-positive 200 (which caused the "toast says saved but the
        question flips back to unanswered" UX bug).
        """
        qa = await self.db.get(IdeationQAItem, qa_id)
        if not qa:
            return None

        ideation = await self.db.get(Ideation, qa.ideation_id)
        board = await self.db.get(Board, ideation.board_id) if ideation else None
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="ideation",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True
        elif qa.question_type not in choice_types and data.answer == "":
            # Explicit clear of a free-text answer.
            qa.answer = None

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, ideation_id: str) -> list[IdeationQAItem]:
        """List all Q&A items for an ideation."""
        query = (
            select(IdeationQAItem)
            .where(IdeationQAItem.ideation_id == ideation_id)
            .order_by(IdeationQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(IdeationQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


def _build_default_refinement_cognitive_done_guard() -> Any:
    """Backward-compatible alias for the shared closeout gate factory."""

    return _build_default_cognitive_closeout_gate()


class RefinementService:
    """Service for refinement operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        # Cognitive closeout must run BEFORE snapshot/status mutation.
        # Keep the historical attribute name so existing tests and callers
        # that inject a fake guard continue to work.
        self._cognitive_done_guard_factory: Callable[
            [], Any
        ] = _build_default_refinement_cognitive_done_guard
        self._cognitive_readiness_service_factory: Callable[
            [], Any
        ] = _build_default_cognitive_readiness_service

    _STATUS_ORDER = {
        RefinementStatus.DRAFT: 0,
        RefinementStatus.REVIEW: 1,
        RefinementStatus.APPROVED: 2,
        RefinementStatus.DONE: 3,
        RefinementStatus.CANCELLED: 3,
    }

    async def _record_history(
        self,
        refinement_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a refinement."""
        entry = RefinementHistory(
            refinement_id=refinement_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, refinement_id: str, limit: int = 50) -> list[RefinementHistory]:
        """List history entries for a refinement, newest first."""
        query = (
            select(RefinementHistory)
            .where(RefinementHistory.refinement_id == refinement_id)
            .order_by(RefinementHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_refinement(
        self, ideation_id: str, user_id: str, data: RefinementCreate, skip_ownership_check: bool = False
    ) -> Refinement | None:
        """Create a new refinement for a done ideation.

        The ideation must be in 'done' status (snapshotted) before refinements
        can be created from it — same governance as spec derivation.

        Always preserves the parent ideation's structured context as a
        derivation snapshot. If a custom description is provided, the inherited
        context is appended instead of being skipped.
        """
        ideation_service = IdeationService(self.db)
        ideation = await ideation_service.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.DONE:
            raise ValueError("Refinements can only be created from a 'done' ideation")

        board_id = ideation.board_id
        if not skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
            result = await self.db.execute(board_query)
            if not result.scalar_one_or_none():
                return None

        description = data.description.strip() if data.description else None
        parent_context = compile_ideation_parent_context(ideation)
        if parent_context:
            if description:
                if "## Parent Ideation Context" not in description:
                    description = f"{description}\n\n{parent_context}"
            else:
                description = parent_context

        # Parse optional mockup/kb filters from data (if present)
        prop_mockup_ids = getattr(data, "mockup_ids", None)
        prop_kb_ids = getattr(data, "kb_ids", None)
        prop_architecture_ids = getattr(data, "architecture_design_ids", None)
        architecture_mode = getattr(data, "architecture_propagation_mode", "copy")

        refinement = Refinement(
            ideation_id=ideation_id,
            board_id=board_id,
            title=data.title,
            description=description,
            in_scope=data.in_scope,
            out_of_scope=data.out_of_scope,
            analysis=data.analysis,
            decisions=data.decisions,
            screen_mockups=None,  # assigned after the Design System gate (below)
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels or ideation.labels,
        )
        # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): gate the MANUAL mockups
        # submitted at creation BEFORE persistence (old=[] baseline). Propagated mockups
        # added below by propagate_artifacts are gated inside that helper.
        _submitted_mockups = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in (data.screen_mockups or [])
        ] or None
        if _submitted_mockups:
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, refinement, _submitted_mockups, entity_type="refinement"
            )
            refinement.screen_mockups = _submitted_mockups
        self.db.add(refinement)
        await self.db.flush()

        # Propagate artifacts from ideation (mockups, KBs, Q&A)
        await propagate_artifacts(
            db=self.db,
            source_mockups=ideation.screen_mockups,
            source_qa_items=ideation.qa_items,
            source_knowledge_bases=ideation.knowledge_bases,
            target_entity=refinement,
            target_kb_class=RefinementKnowledgeBase,
            user_id=user_id,
            mockup_ids=prop_mockup_ids,
            kb_ids=prop_kb_ids,
            source_type="ideation",
            source_id=ideation.id,
            source_title=ideation.title,
            source_version=ideation.version,
        )
        await propagate_architecture_designs(
            self.db,
            source_parent_type="ideation",
            source_parent_id=ideation_id,
            target_parent_type="refinement",
            target_parent_id=refinement.id,
            actor_id=user_id,
            mode=architecture_mode,
            design_ids=prop_architecture_ids,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="refinement_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "refinement_id": refinement.id, "ideation_id": ideation_id},
        )
        await self._record_history(
            refinement_id=refinement.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Refinement created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": RefinementStatus.DRAFT.value},
                *([{"field": "in_scope", "old": None, "new": data.in_scope}] if data.in_scope else []),
                *([{"field": "out_of_scope", "old": None, "new": data.out_of_scope}] if data.out_of_scope else []),
                *([{"field": "analysis", "old": None, "new": data.analysis}] if data.analysis else []),
                *([{"field": "decisions", "old": None, "new": data.decisions}] if data.decisions else []),
            ],
        )
        return refinement

    async def get_refinement(self, refinement_id: str) -> Refinement | None:
        """Get a refinement by ID with specs, knowledge_bases, and qa_items."""
        query = (
            select(Refinement)
            .options(selectinload(Refinement.ideation).selectinload(Ideation.qa_items))
            .options(selectinload(Refinement.ideation).selectinload(Ideation.knowledge_bases))
            .options(selectinload(Refinement.ideation).selectinload(Ideation.architecture_designs))
            .options(selectinload(Refinement.specs).selectinload(Spec.architecture_designs))
            .options(selectinload(Refinement.knowledge_bases))
            .options(selectinload(Refinement.qa_items))
            .options(selectinload(Refinement.architecture_designs))
            .where(Refinement.id == refinement_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_refinements(self, ideation_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Refinement]:
        """List refinements for an ideation, optionally filtered by status."""
        query = (
            select(Refinement)
            .options(selectinload(Refinement.architecture_designs))
            .where(Refinement.ideation_id == ideation_id)
        )
        if status_filter:
            query = query.where(Refinement.status == RefinementStatus(status_filter))
        if not include_archived:
            query = query.where(Refinement.archived.is_(False))
        query = query.order_by(Refinement.updated_at.desc())
        result = await self.db.execute(query)
        rows = list(result.scalars().all())
        await _attach_open_qa_counts(self.db, rows, RefinementQAItem, "refinement_id")
        return rows

    async def update_refinement(self, refinement_id: str, user_id: str, data: RefinementUpdate) -> Refinement | None:
        """Update a refinement. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError("This refinement is archived. Restore it first before making changes.")

        if refinement.status != RefinementStatus.DRAFT:
            raise ValueError(
                f"Cannot edit refinement in '{refinement.status.value}' status. "
                f"Move it back to 'draft' to make changes."
            )

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {"description", "scope", "analysis", "decisions"}
        # Spec eaf78891 (Ideação #2): refinement_semantic_fields cover all
        # update_data keys that affect KG extraction. Refinements have a much
        # smaller surface than specs, so any update is treated as semantic.
        bumps_version = bool(content_fields & update_data.keys())
        bumps_semantic = bool(update_data)

        old_data = {k: getattr(refinement, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]
            # MockupDesignSystemGate (spec 3a006f65) — defense in depth pre-persist.
            from okto_pulse.core.services.design_system import gate_entity_screen_mockups
            await gate_entity_screen_mockups(
                self.db, refinement, update_data["screen_mockups"], entity_type="refinement"
            )

        refinement_json_fields = {"in_scope", "out_scope", "labels", "screen_mockups"}
        for key, value in update_data.items():
            setattr(refinement, key, value)
            if key in refinement_json_fields:
                flag_modified(refinement, key)

        if bumps_version:
            refinement.version += 1

        if bumps_semantic:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementSemanticChanged

            await event_publish(
                RefinementSemanticChanged(
                    board_id=refinement.board_id,
                    actor_id=user_id,
                    refinement_id=refinement.id,
                    changed_fields=sorted(update_data.keys()),
                ),
                session=self.db,
            )

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"refinement_id": refinement_id, "version": refinement.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                refinement_id=refinement_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=refinement.version,
                summary=f"Updated: {changed_fields}",
            )
        return refinement

    # Allowed refinement transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Done, Cancelled
    # Done → Draft (new version)
    _REFINEMENT_TRANSITIONS: dict[RefinementStatus, list[RefinementStatus]] = {
        RefinementStatus.DRAFT: [RefinementStatus.REVIEW, RefinementStatus.CANCELLED],
        RefinementStatus.REVIEW: [RefinementStatus.DRAFT, RefinementStatus.APPROVED, RefinementStatus.CANCELLED],
        RefinementStatus.APPROVED: [RefinementStatus.REVIEW, RefinementStatus.DONE, RefinementStatus.CANCELLED],
        RefinementStatus.DONE: [RefinementStatus.DRAFT],
        RefinementStatus.CANCELLED: [],
    }

    async def move_refinement(
        self, refinement_id: str, user_id: str, data: RefinementMove, actor_name: str | None = None
    ) -> Refinement | None:
        """Move a refinement to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Editing only allowed in Draft
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError("This refinement is archived. Restore it first before changing status.")

        old_status = refinement.status
        allowed = self._REFINEMENT_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move refinement from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        # Content gate — draft→review requires at least one non-empty in_scope
        # entry. Prevents stub refinements (no design intent captured) from
        # leaking into review / approved / done where downstream tools
        # (derive_spec, get_refinement_context) would operate on them.
        if (
            old_status == RefinementStatus.DRAFT
            and data.status == RefinementStatus.REVIEW
        ):
            in_scope_items = refinement.in_scope or []
            if not any(isinstance(s, str) and s.strip() for s in in_scope_items):
                raise ValueError(
                    "Refinement must have at least one in_scope item before "
                    "moving to review.",
                )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, refinement.board_id)

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=refinement.board_id,
            actor_id=user_id,
            entity_type="refinement",
            entity_id=refinement.id,
            critical_action=_critical_refinement_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=resolved_name,
        )

        # Fail-closed cognitive closeout gate. This MUST run BEFORE
        # ResourceGateService, snapshot creation, activity logging, or status
        # mutation. A blocked response preserves the refinement in its current
        # status with no snapshot/history/activity changes.
        if data.status == RefinementStatus.DONE:
            board = await self.db.get(Board, refinement.board_id)
            graph_state = await _resolve_closeout_graph_state(
                refinement.board_id, self.db
            )
            _evaluate_cognitive_closeout_or_raise(
                gate_factory=self._cognitive_done_guard_factory,
                board=board,
                board_id=refinement.board_id,
                entity_type="refinement",
                entity_id=refinement.id,
                entity=refinement,
                target_label="refinement",
                graph_state=graph_state,
            )
            await _evaluate_cognitive_readiness_or_raise(
                service_factory=self._cognitive_readiness_service_factory,
                db=self.db,
                board_id=refinement.board_id,
                entity_type="refinement",
                entity_id=refinement.id,
                entity=refinement,
                target_label="refinement",
                policy_blocking=_cognitive_readiness_blocking_active(board),
            )

        # Snapshot on done
        if data.status == RefinementStatus.DONE:
            await ResourceGateService(self.db).validate_or_raise_entity_completion(
                refinement.board_id,
                "refinement",
                refinement.id,
                phase="refinement_done",
            )
            await self._create_snapshot(refinement, user_id)

        # Version bump on back-to-draft from done
        if data.status == RefinementStatus.DRAFT and old_status == RefinementStatus.DONE:
            refinement.version += 1

        refinement.status = data.status
        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import RefinementSemanticChanged

        await event_publish(
            RefinementSemanticChanged(
                board_id=refinement.board_id,
                actor_id=user_id,
                refinement_id=refinement.id,
                changed_fields=["status"],
            ),
            session=self.db,
        )

        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "refinement_id": refinement_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": refinement.version,
            },
        )
        summary = f"Status: {old_status.value} \u2192 {data.status.value}"
        if data.status == RefinementStatus.DONE:
            summary += f" (snapshot v{refinement.version} created)"
        elif data.status == RefinementStatus.DRAFT and old_status == RefinementStatus.DONE:
            summary += f" (new iteration v{refinement.version})"

        await self._record_history(
            refinement_id=refinement_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=summary,
            version=refinement.version,
        )
        return refinement

    async def _create_snapshot(self, refinement: "Refinement", user_id: str) -> "RefinementSnapshot":
        """Create an immutable snapshot of the refinement's current state."""
        qa_snapshot = []
        for qa in (refinement.qa_items or []):
            qa_snapshot.append({
                "question": qa.question,
                "question_type": qa.question_type,
                "choices": qa.choices,
                "answer": qa.answer,
                "selected": qa.selected,
                "asked_by": qa.asked_by,
                "answered_by": qa.answered_by,
            })

        snapshot = RefinementSnapshot(
            refinement_id=refinement.id,
            version=refinement.version,
            title=refinement.title,
            description=refinement.description,
            in_scope=refinement.in_scope,
            out_of_scope=refinement.out_of_scope,
            analysis=refinement.analysis,
            decisions=refinement.decisions,
            labels=refinement.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            created_by=user_id,
        )
        self.db.add(snapshot)
        await self.db.flush()
        return snapshot

    async def list_snapshots(self, refinement_id: str) -> list:
        """List all snapshots for a refinement."""
        query = (
            select(RefinementSnapshot)
            .where(RefinementSnapshot.refinement_id == refinement_id)
            .order_by(RefinementSnapshot.version.desc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_snapshot(self, refinement_id: str, version: int):
        """Get a specific version snapshot."""
        query = select(RefinementSnapshot).where(
            RefinementSnapshot.refinement_id == refinement_id,
            RefinementSnapshot.version == version,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def delete_refinement(self, refinement_id: str, user_id: str) -> bool:
        """Delete a refinement."""
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return False

        board_id = refinement.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(refinement)

        await self._log_activity(
            board_id=board_id,
            action="refinement_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"refinement_id": refinement_id},
        )
        return True

    async def derive_spec(
        self, refinement_id: str, user_id: str, skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None, kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
    ) -> Spec | None:
        """Create a Spec draft linked to a refinement.

        Artifacts (mockups, KBs) are automatically propagated. Use mockup_ids/kb_ids
        to select specific ones. Compiles context from the refinement's scope, analysis, decisions,
        technical_requirements, acceptance_criteria) are left empty — they must be
        filled by the agent or human through deliberate analysis.

        Only allowed when refinement status is 'done'.
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if refinement.status != RefinementStatus.DONE:
            raise ValueError("Spec can only be created from a 'done' refinement")

        # Compile rich context from refinement data plus the parent ideation
        # intent. Existing refinements created before parent context was
        # appended to description still carry the original idea into specs.
        context_parts: list[str] = []
        if refinement.description:
            context_parts.append(f"## Refinement Description\n{refinement.description}")
        if refinement.in_scope:
            scope_text = "\n".join(f"- {s}" for s in refinement.in_scope)
            context_parts.append(f"## In Scope\n{scope_text}")
        if refinement.out_of_scope:
            out_text = "\n".join(f"- {s}" for s in refinement.out_of_scope)
            context_parts.append(f"## Out of Scope\n{out_text}")
        if refinement.analysis:
            context_parts.append(f"## Analysis\n{refinement.analysis}")
        if refinement.decisions:
            decisions_text = "\n".join(f"- {d}" for d in refinement.decisions)
            context_parts.append(f"## Decisions\n{decisions_text}")
        parent_context = compile_ideation_parent_context(getattr(refinement, "ideation", None))
        if parent_context and not (
            refinement.description and "## Parent Ideation Context" in refinement.description
        ):
            context_parts.append(parent_context)
        context = "\n\n".join(context_parts) if context_parts else refinement.description

        # Snapshot artifact data BEFORE create_spec — flush() in create_spec
        # expires all session objects, making eagerly-loaded collections inaccessible.
        snapshot_qa = list(refinement.qa_items or [])
        snapshot_mockups = list(refinement.screen_mockups or [])
        snapshot_kbs = [
            {"title": kb.title, "description": kb.description, "content": kb.content,
             "mime_type": getattr(kb, "mime_type", "text/markdown"), "id": kb.id,
             "source_type": getattr(kb, "source_type", None),
             "source_id": getattr(kb, "source_id", None),
             "source_title": getattr(kb, "source_title", None),
             "source_version": getattr(kb, "source_version", None),
             "source_kb_id": getattr(kb, "source_kb_id", None)}
            for kb in (refinement.knowledge_bases or [])
        ]

        spec_data = SpecCreate(
            title=refinement.title,
            description=refinement.description,
            context=context,
            ideation_id=refinement.ideation_id,
            refinement_id=refinement_id,
            labels=refinement.labels,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            refinement.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
            # Propagate artifacts using pre-flush snapshots
            await propagate_artifacts(
                db=self.db,
                source_mockups=snapshot_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=snapshot_kbs,
                target_entity=spec,
                target_kb_class=SpecKnowledgeBase,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=kb_ids,
                source_type="refinement",
                source_id=refinement.id,
                source_title=refinement.title,
                source_version=refinement.version,
            )
            await propagate_architecture_designs(
                self.db,
                source_parent_type="refinement",
                source_parent_id=refinement_id,
                target_parent_type="spec",
                target_parent_id=spec.id,
                actor_id=user_id,
                mode=architecture_propagation_mode,
                design_ids=architecture_design_ids,
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementDerivedToSpec

            await event_publish(
                RefinementDerivedToSpec(
                    board_id=refinement.board_id,
                    actor_id=user_id,
                    refinement_id=refinement_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
            await self._record_history(
                refinement_id=refinement_id, action="spec_draft_created", actor_id=user_id, actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=refinement.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class RefinementQAService:
    """Service for refinement Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, refinement_id: str, user_id: str, data: RefinementQACreate) -> RefinementQAItem | None:
        """Create a question on a refinement (text or choice)."""
        refinement = await self.db.get(Refinement, refinement_id)
        if not refinement:
            return None
        qa = RefinementQAItem(
            refinement_id=refinement_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self,
        qa_id: str,
        user_id: str,
        data: RefinementQAAnswer,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> RefinementQAItem | None:
        """Answer a refinement Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await self.db.get(RefinementQAItem, qa_id)
        if not qa:
            return None

        refinement = await self.db.get(Refinement, qa.refinement_id)
        board = await self.db.get(Board, refinement.board_id) if refinement else None
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="refinement",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, refinement_id: str) -> list[RefinementQAItem]:
        """List all Q&A items for a refinement."""
        query = (
            select(RefinementQAItem)
            .where(RefinementQAItem.refinement_id == refinement_id)
            .order_by(RefinementQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(RefinementQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class RefinementKnowledgeService:
    """Service for refinement knowledge base operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_knowledge(self, refinement_id: str, user_id: str, data: RefinementKnowledgeCreate) -> RefinementKnowledgeBase | None:
        """Create a knowledge base item on a refinement."""
        refinement = await self.db.get(Refinement, refinement_id)
        if not refinement:
            return None
        kb = RefinementKnowledgeBase(
            refinement_id=refinement_id,
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            created_by=user_id,
        )
        self.db.add(kb)
        await self.db.flush()
        return kb

    async def get_knowledge(self, knowledge_id: str) -> RefinementKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await self.db.get(RefinementKnowledgeBase, knowledge_id)

    async def list_knowledge(self, refinement_id: str) -> list[RefinementKnowledgeBase]:
        """List all knowledge base items for a refinement."""
        query = (
            select(RefinementKnowledgeBase)
            .where(RefinementKnowledgeBase.refinement_id == refinement_id)
            .order_by(RefinementKnowledgeBase.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        await self.db.delete(kb)
        return True


class GuidelineService:
    """Service for guideline operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_guideline(self, owner_id: str, data: GuidelineCreate) -> Guideline:
        """Create a new guideline."""
        guideline = Guideline(
            title=data.title,
            content=data.content,
            tags=data.tags,
            scope=data.scope,
            board_id=data.board_id,
            owner_id=owner_id,
        )
        self.db.add(guideline)
        await self.db.flush()
        return guideline

    async def get_guideline(self, guideline_id: str) -> Guideline | None:
        """Get a guideline by ID."""
        return await self.db.get(Guideline, guideline_id)

    async def list_guidelines(
        self, owner_id: str, offset: int = 0, limit: int = 50, tag: str | None = None,
    ) -> list[Guideline]:
        """List global guidelines for an owner, optionally filtered by tag."""
        query = (
            select(Guideline)
            .where(Guideline.owner_id == owner_id, Guideline.scope == "global")
            .order_by(Guideline.created_at.desc())
        )
        if tag:
            query = query.where(Guideline.tags.contains([tag]))
        query = query.offset(offset).limit(limit)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_guideline(
        self, guideline_id: str, owner_id: str, data: GuidelineUpdate,
    ) -> Guideline | None:
        """Update a guideline."""
        guideline = await self.get_guideline(guideline_id)
        if not guideline or guideline.owner_id != owner_id:
            return None
        changed = False
        if data.title is not None and data.title != guideline.title:
            guideline.title = data.title
            changed = True
        if data.content is not None and data.content != guideline.content:
            guideline.content = data.content
            changed = True
        if data.tags is not None:
            guideline.tags = data.tags
            flag_modified(guideline, "tags")
            changed = True
        if changed and guideline.scope == "global":
            guideline.version = (guideline.version or 1) + 1
        await self.db.flush()
        return guideline

    async def delete_guideline(self, guideline_id: str, owner_id: str) -> bool:
        """Delete a guideline."""
        guideline = await self.get_guideline(guideline_id)
        if not guideline or guideline.owner_id != owner_id:
            return False
        await self.db.delete(guideline)
        return True

    async def get_board_guidelines(self, board_id: str, *, surface: str = "service") -> list[dict]:
        """Get all guidelines for a board — linked globals + inline, sorted by priority."""
        # Linked global guidelines
        linked_query = (
            select(Guideline, BoardGuideline.priority)
            .join(BoardGuideline, BoardGuideline.guideline_id == Guideline.id)
            .where(BoardGuideline.board_id == board_id)
        )
        linked_result = await self.db.execute(linked_query)
        linked_rows = linked_result.all()

        # Inline (board-scoped) guidelines
        inline_query = (
            select(Guideline)
            .where(Guideline.board_id == board_id, Guideline.scope == "inline")
            .order_by(Guideline.created_at)
        )
        inline_result = await self.db.execute(inline_query)
        inline_rows = inline_result.scalars().all()

        items: list[dict] = []
        for guideline, priority in linked_rows:
            items.append({
                "id": guideline.id,
                "guideline": {
                    "id": guideline.id,
                    "title": guideline.title,
                    "content": guideline.content,
                    "tags": guideline.tags,
                    "scope": guideline.scope,
                    "board_id": guideline.board_id,
                    "owner_id": guideline.owner_id,
                    "created_at": guideline.created_at.isoformat() if guideline.created_at else None,
                    "version": guideline.version or 1,
                    "updated_at": guideline.updated_at.isoformat() if guideline.updated_at else None,
                },
                "priority": priority,
                "scope": guideline.scope,
            })
        for guideline in inline_rows:
            items.append({
                "id": guideline.id,
                "guideline": {
                    "id": guideline.id,
                    "title": guideline.title,
                    "content": guideline.content,
                    "tags": guideline.tags,
                    "scope": guideline.scope,
                    "board_id": guideline.board_id,
                    "owner_id": guideline.owner_id,
                    "created_at": guideline.created_at.isoformat() if guideline.created_at else None,
                    "updated_at": guideline.updated_at.isoformat() if guideline.updated_at else None,
                },
                "priority": 0,
                "scope": "inline",
            })

        items.sort(key=lambda x: x["priority"])
        if not items:
            details = build_board_missing_context_warning_details(
                board_id=board_id,
                warning_code="board_rules_missing",
                surface=surface,
            )
            emit_governance_metric(details, raise_on_violation=False)
        return items

    async def link_guideline_to_board(
        self, board_id: str, guideline_id: str, priority: int = 0,
    ) -> BoardGuideline:
        """Link a global guideline to a board."""
        link = BoardGuideline(
            board_id=board_id,
            guideline_id=guideline_id,
            priority=priority,
        )
        self.db.add(link)
        await self.db.flush()
        return link

    async def unlink_guideline_from_board(self, board_id: str, guideline_id: str) -> bool:
        """Unlink a guideline from a board."""
        query = select(BoardGuideline).where(
            BoardGuideline.board_id == board_id,
            BoardGuideline.guideline_id == guideline_id,
        )
        result = await self.db.execute(query)
        link = result.scalar_one_or_none()
        if not link:
            return False
        await self.db.delete(link)
        return True

    async def update_priority(self, board_id: str, guideline_id: str, priority: int) -> bool:
        """Update the priority of a linked guideline."""
        query = select(BoardGuideline).where(
            BoardGuideline.board_id == board_id,
            BoardGuideline.guideline_id == guideline_id,
        )
        result = await self.db.execute(query)
        link = result.scalar_one_or_none()
        if not link:
            return False
        link.priority = priority
        await self.db.flush()
        return True

    async def apply_default_guidelines(
        self,
        board_id: str,
        refs: list,
        *,
        template_id: str,
        template_version: int,
        actor: str = "system",
    ) -> list[BoardGuideline]:
        """Materialize a new board's default guideline links from the active default
        template's resolved refs (spec 8a2fad91 / card 2803c136 / FR3). Each created
        link records the priority + the template provenance (template_id,
        template_version) + the guideline_version captured in the ref. Runs in the
        CALLER's transaction (TR3 — no commit here), so any failure aborts the whole
        ``create_board`` with no partial board / orphan links. Idempotent per
        ``uq_board_guideline``: an existing board/guideline link is preserved untouched;
        intra-batch duplicate guideline_ids are de-duped first-wins, so the resulting
        priority/provenance is deterministic and never duplicated (TR4). The umbrella
        owns resolution + fail-closed validation; this is purely the writer (it is the
        single materialization point and the ts_a48e70ee failure-injection target)."""
        existing = await self.db.execute(
            select(BoardGuideline.guideline_id).where(BoardGuideline.board_id == board_id)
        )
        already_linked = set(existing.scalars())
        created: list[BoardGuideline] = []
        seen: set[str] = set()
        for ref in refs or []:
            guideline_id = ref["guideline_id"]
            if guideline_id in seen or guideline_id in already_linked:
                continue  # uq_board_guideline + intra-template de-dup (first wins)
            seen.add(guideline_id)
            link = BoardGuideline(
                board_id=board_id,
                guideline_id=guideline_id,
                priority=int(ref.get("priority", 0) or 0),
                template_id=template_id,
                template_version=template_version,
                guideline_version=ref.get("guideline_version"),
            )
            self.db.add(link)
            created.append(link)
        if created:
            await self.db.flush()
        return created


# ============================================================================
# Archive Service
# ============================================================================


class ArchiveService:
    """Service for archiving and restoring entity trees."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def _resolve_tree(
        self, entity_type: str, entity_id: str
    ) -> dict[str, list]:
        """Resolve the full descendant tree from a given entity.
        Returns {ideations: [...], refinements: [...], specs: [...], cards: [...]}.
        """
        from okto_pulse.core.models.db import Ideation, Refinement, Spec, Card

        tree: dict[str, list] = {"ideations": [], "refinements": [], "specs": [], "cards": []}

        if entity_type == "ideation":
            ideation = await self.db.get(Ideation, entity_id)
            if not ideation:
                raise ValueError("Ideation not found")
            tree["ideations"].append(ideation)

            # Refinements from this ideation
            q = select(Refinement).where(Refinement.ideation_id == entity_id)
            refinements = list((await self.db.execute(q)).scalars().all())
            tree["refinements"].extend(refinements)

            # Specs from refinements + direct from ideation
            ref_ids = [r.id for r in refinements]
            spec_q = select(Spec).where(
                (Spec.ideation_id == entity_id) | (Spec.refinement_id.in_(ref_ids) if ref_ids else False)
            )
            specs = list((await self.db.execute(spec_q)).scalars().all())
            tree["specs"].extend(specs)

        elif entity_type == "refinement":
            refinement = await self.db.get(Refinement, entity_id)
            if not refinement:
                raise ValueError("Refinement not found")
            tree["refinements"].append(refinement)

            spec_q = select(Spec).where(Spec.refinement_id == entity_id)
            specs = list((await self.db.execute(spec_q)).scalars().all())
            tree["specs"].extend(specs)

        elif entity_type == "spec":
            spec = await self.db.get(Spec, entity_id)
            if not spec:
                raise ValueError("Spec not found")
            tree["specs"].append(spec)

        else:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be ideation, refinement, or spec.")

        # Cards from all specs in tree
        spec_ids = [s.id for s in tree["specs"]]
        if spec_ids:
            card_q = select(Card).where(Card.spec_id.in_(spec_ids))
            cards = list((await self.db.execute(card_q)).scalars().all())
            tree["cards"].extend(cards)

            # Bug cards linked to these cards via origin_task_id
            card_ids = [c.id for c in cards]
            if card_ids:
                bug_q = select(Card).where(
                    Card.origin_task_id.in_(card_ids),
                    Card.id.notin_(card_ids),  # avoid duplicates
                )
                bugs = list((await self.db.execute(bug_q)).scalars().all())
                tree["cards"].extend(bugs)

        return tree

    async def archive_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Archive an entity and all its descendants."""
        tree = await self._resolve_tree(entity_type, entity_id)

        counts = {"ideations": 0, "refinements": 0, "specs": 0, "cards": 0}

        for ideation in tree["ideations"]:
            if not ideation.archived:
                ideation.pre_archive_status = ideation.status.value if hasattr(ideation.status, "value") else str(ideation.status)
                ideation.archived = True
                counts["ideations"] += 1

        for refinement in tree["refinements"]:
            if not refinement.archived:
                refinement.pre_archive_status = refinement.status.value if hasattr(refinement.status, "value") else str(refinement.status)
                refinement.archived = True
                counts["refinements"] += 1

        for spec in tree["specs"]:
            if not spec.archived:
                spec.pre_archive_status = spec.status.value if hasattr(spec.status, "value") else str(spec.status)
                spec.archived = True
                counts["specs"] += 1

        for card in tree["cards"]:
            if not card.archived:
                card.pre_archive_status = card.status.value if hasattr(card.status, "value") else str(card.status)
                card.archived = True
                counts["cards"] += 1

        await self.db.flush()
        return counts

    async def restore_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Restore an archived entity and all its descendants."""
        from okto_pulse.core.models.db import (
            IdeationStatus, RefinementStatus, SpecStatus, CardStatus,
        )

        tree = await self._resolve_tree(entity_type, entity_id)

        counts = {"ideations": 0, "refinements": 0, "specs": 0, "cards": 0}

        for ideation in tree["ideations"]:
            if ideation.archived:
                if ideation.pre_archive_status:
                    try:
                        ideation.status = IdeationStatus(ideation.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                ideation.archived = False
                ideation.pre_archive_status = None
                counts["ideations"] += 1

        for refinement in tree["refinements"]:
            if refinement.archived:
                if refinement.pre_archive_status:
                    try:
                        refinement.status = RefinementStatus(refinement.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                refinement.archived = False
                refinement.pre_archive_status = None
                counts["refinements"] += 1

        for spec in tree["specs"]:
            if spec.archived:
                if spec.pre_archive_status:
                    try:
                        spec.status = SpecStatus(spec.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                spec.archived = False
                spec.pre_archive_status = None
                counts["specs"] += 1

        for card in tree["cards"]:
            if card.archived:
                if card.pre_archive_status:
                    try:
                        card.status = CardStatus(card.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                card.archived = False
                card.pre_archive_status = None
                counts["cards"] += 1

        await self.db.flush()
        return counts


# ============================================================================
# SPRINT SERVICE
# ============================================================================


class SprintOperationError(ValueError):
    """Typed sprint workflow error for API/MCP callers."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        remediation: str | None = None,
        facts: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.facts = facts or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        if self.facts:
            payload["facts"] = self.facts
        return payload


class SprintService:
    """Service for sprint operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _SPRINT_TRANSITIONS = {
        SprintStatus.DRAFT: [SprintStatus.ACTIVE, SprintStatus.CANCELLED],
        SprintStatus.ACTIVE: [SprintStatus.DRAFT, SprintStatus.REVIEW, SprintStatus.CANCELLED],
        SprintStatus.REVIEW: [SprintStatus.ACTIVE, SprintStatus.CLOSED, SprintStatus.CANCELLED],
        SprintStatus.CLOSED: [SprintStatus.DRAFT],
        SprintStatus.CANCELLED: [SprintStatus.DRAFT],
    }

    async def _record_history(
        self, sprint_id: str, action: str, actor_id: str, actor_name: str,
        actor_type: str = "user", changes: list[dict] | None = None,
        summary: str | None = None, version: int | None = None,
    ) -> None:
        entry = SprintHistory(
            sprint_id=sprint_id, action=action, actor_type=actor_type,
            actor_id=actor_id, actor_name=actor_name,
            changes=changes, summary=summary, version=version,
        )
        self.db.add(entry)

    async def _log_activity(self, **kwargs: Any) -> None:
        log = ActivityLog(**kwargs)
        self.db.add(log)

    @staticmethod
    def _lane_activity_details(sprint: Sprint) -> dict[str, Any]:
        lane_type = (
            sprint.lane_type.value
            if getattr(sprint.lane_type, "value", None)
            else str(sprint.lane_type or SprintLaneType.NORMAL.value)
        )
        return {
            "lane_type": lane_type,
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": sprint.normal_sprint_created,
        }

    async def _validate_hotfix_lane_create(
        self,
        board_id: str,
        spec: Spec,
        data: SprintCreate,
    ) -> tuple[Sprint | None, Card | None]:
        """Validate hotfix lane creation inputs without mutating source artifacts."""
        if data.lane_type != SprintLaneType.HOTFIX:
            return None, None

        origin_sprint: Sprint | None = None
        if data.origin_sprint_id:
            origin_sprint = await self.db.get(Sprint, data.origin_sprint_id)
            if (
                not origin_sprint
                or origin_sprint.board_id != board_id
                or origin_sprint.spec_id != data.spec_id
            ):
                raise SprintOperationError(
                    "origin_sprint_not_found",
                    "origin_sprint_id does not reference a sprint in this board/spec.",
                    remediation="provide_same_spec_origin_sprint",
                    facts={
                        "origin_sprint_id": data.origin_sprint_id,
                        "spec_id": data.spec_id,
                        "board_id": board_id,
                    },
                )

        origin_bug: Card | None = None
        if data.origin_bug_id:
            origin_bug = await self.db.get(Card, data.origin_bug_id)
            if (
                not origin_bug
                or origin_bug.board_id != board_id
                or origin_bug.spec_id != data.spec_id
                or origin_bug.card_type != CardType.BUG
            ):
                raise SprintOperationError(
                    "origin_bug_not_found",
                    "origin_bug_id does not reference a bug in this board/spec.",
                    remediation="provide_same_spec_bug_card",
                    facts={
                        "origin_bug_id": data.origin_bug_id,
                        "spec_id": data.spec_id,
                        "board_id": board_id,
                    },
                )

        spec_done = spec.status == SpecStatus.DONE
        origin_sprint_closed = bool(origin_sprint and origin_sprint.status == SprintStatus.CLOSED)
        if not spec_done and not origin_sprint_closed:
            raise SprintOperationError(
                "hotfix_lane_not_eligible",
                "Hotfix lane requires a done spec or a closed same-spec origin sprint.",
                remediation="assign_hotfix_lane_after_done_spec_or_closed_origin_sprint",
                facts={
                    "spec_id": spec.id,
                    "spec_status": spec.status.value,
                    "origin_sprint_id": data.origin_sprint_id,
                    "origin_sprint_status": (
                        origin_sprint.status.value if origin_sprint else None
                    ),
                },
            )

        return origin_sprint, origin_bug

    async def create_sprint(
        self, board_id: str, user_id: str, data: SprintCreate,
        skip_ownership_check: bool = False,
    ) -> Sprint | None:
        """Create a new sprint for a spec."""
        spec = await self.db.get(Spec, data.spec_id)
        if not spec or spec.board_id != board_id:
            return None
        if not skip_ownership_check:
            board = await self.db.get(Board, board_id)
            if not board or board.owner_id != user_id:
                return None

        await self._validate_hotfix_lane_create(board_id, spec, data)

        # Validate scoped IDs exist in spec
        if data.test_scenario_ids:
            spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
            invalid = set(data.test_scenario_ids) - spec_ts_ids
            if invalid:
                raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if data.business_rule_ids:
            spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
            invalid = set(data.business_rule_ids) - spec_br_ids
            if invalid:
                raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        sprint = Sprint(
            board_id=board_id, spec_id=data.spec_id,
            title=data.title, description=data.description,
            objective=data.objective,
            expected_outcome=data.expected_outcome,
            spec_version=spec.version,
            lane_type=data.lane_type or SprintLaneType.NORMAL,
            origin_sprint_id=data.origin_sprint_id,
            origin_bug_id=data.origin_bug_id,
            test_scenario_ids=data.test_scenario_ids,
            business_rule_ids=data.business_rule_ids,
            start_date=data.start_date, end_date=data.end_date,
            labels=data.labels, created_by=user_id,
        )
        self.db.add(sprint)
        await self.db.flush()

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SprintCreated as SprintCreatedEvent

        await event_publish(
            SprintCreatedEvent(
                board_id=board_id,
                actor_id=user_id,
                sprint_id=sprint.id,
                spec_id=data.spec_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id, action="sprint_created",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={
                "title": data.title,
                "sprint_id": sprint.id,
                "spec_id": data.spec_id,
                "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
                "origin_sprint_id": sprint.origin_sprint_id,
                "origin_bug_id": sprint.origin_bug_id,
                "normal_sprint_created": sprint.normal_sprint_created,
            },
        )
        await self._record_history(
            sprint_id=sprint.id, action="created", actor_id=user_id, actor_name=actor_name,
            changes=[{
                "field": "lane",
                **self._lane_activity_details(sprint),
            }],
            summary=(
                f"Hotfix lane created: {data.title}"
                if sprint.lane_type == SprintLaneType.HOTFIX
                else f"Sprint created: {data.title}"
            ),
            version=1,
        )
        return sprint

    async def get_sprint(self, sprint_id: str) -> Sprint | None:
        """Get a sprint by ID with cards, Q&A, and history."""
        query = (
            select(Sprint)
            .options(selectinload(Sprint.cards))
            .options(selectinload(Sprint.qa_items))
            .options(selectinload(Sprint.history))
            .where(Sprint.id == sprint_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_sprints(
        self, spec_id: str, include_archived: bool = False,
    ) -> list[Sprint]:
        """List sprints for a spec."""
        query = select(Sprint).where(Sprint.spec_id == spec_id)
        if not include_archived:
            query = query.where(Sprint.archived.is_(False))
        query = query.order_by(Sprint.created_at.asc())
        result = await self.db.execute(query)
        rows = list(result.scalars().all())
        await _attach_open_qa_counts(self.db, rows, SprintQAItem, "sprint_id")
        return rows

    async def list_board_sprints(
        self, board_id: str, status_filter: str | None = None,
        spec_id: str | None = None,
        include_archived: bool = False,
    ) -> list[Sprint]:
        """List all sprints for a board, optionally filtered by status and/or spec."""
        from sqlalchemy.orm import selectinload
        query = select(Sprint).where(Sprint.board_id == board_id)
        if status_filter:
            query = query.where(Sprint.status == SprintStatus(status_filter))
        if spec_id:
            query = query.where(Sprint.spec_id == spec_id)
        if not include_archived:
            query = query.where(Sprint.archived.is_(False))
        query = query.options(selectinload(Sprint.spec))
        query = query.order_by(Sprint.updated_at.desc())
        result = await self.db.execute(query)
        rows = list(result.scalars().all())
        await _attach_open_qa_counts(self.db, rows, SprintQAItem, "sprint_id")
        return rows

    async def update_sprint(
        self, sprint_id: str, user_id: str, data: SprintUpdate,
    ) -> Sprint | None:
        """Update a sprint. Bumps version on content changes."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        update_data = data.model_dump(exclude_unset=True)
        old_data = {k: getattr(sprint, k) for k in update_data.keys()}

        lane_fields = {"lane_type", "origin_sprint_id", "origin_bug_id"}
        if lane_fields & update_data.keys() and sprint.status != SprintStatus.DRAFT:
            raise ValueError("Sprint lane metadata can only be updated while the sprint is draft")

        # Validate scoped IDs if changed
        if "test_scenario_ids" in update_data and update_data["test_scenario_ids"] is not None:
            spec = await self.db.get(Spec, sprint.spec_id)
            if spec:
                spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
                invalid = set(update_data["test_scenario_ids"]) - spec_ts_ids
                if invalid:
                    raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if "business_rule_ids" in update_data and update_data["business_rule_ids"] is not None:
            spec = spec if "test_scenario_ids" in update_data else await self.db.get(Spec, sprint.spec_id)
            if spec:
                spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
                invalid = set(update_data["business_rule_ids"]) - spec_br_ids
                if invalid:
                    raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        content_fields = {
            "title",
            "description",
            "test_scenario_ids",
            "business_rule_ids",
            "lane_type",
            "origin_sprint_id",
            "origin_bug_id",
        }
        bumps_version = bool(content_fields & update_data.keys())

        json_fields = {"test_scenario_ids", "business_rule_ids", "labels"}
        for key, value in update_data.items():
            setattr(sprint, key, value)
            if key in json_fields:
                flag_modified(sprint, key)

        if bumps_version:
            sprint.version += 1

        actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
        await self._log_activity(
            board_id=sprint.board_id, action="sprint_updated",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={"sprint_id": sprint_id, "version": sprint.version, "fields": list(update_data.keys())},
        )
        changes = SpecService._compute_diff(old_data, update_data, list(update_data.keys()))
        if changes:
            await self._record_history(
                sprint_id=sprint_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=sprint.version,
                summary=f"Updated: {', '.join(c['field'] for c in changes)}",
            )
        await self.db.commit()
        return sprint

    async def move_sprint(
        self, sprint_id: str, user_id: str, data: SprintMove,
        actor_name: str | None = None,
    ) -> Sprint | None:
        """Move a sprint to a different status with gates."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        allowed = self._SPRINT_TRANSITIONS.get(sprint.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move sprint from '{sprint.status.value}' to '{data.status.value}'. "
                f"Allowed: {allowed_values}"
            )

        spec = await self.db.get(Spec, sprint.spec_id)
        board = await self.db.get(Board, sprint.board_id) if spec else None

        await _authorize_critical_context_or_raise(
            self.db,
            board_id=sprint.board_id,
            actor_id=user_id,
            entity_type="sprint",
            entity_id=sprint.id,
            critical_action=_critical_sprint_move_action(data.status),
            surface="service",
            actor_type="user",
            actor_name=actor_name,
        )

        # Gate: draft → active requires at least 1 card assigned
        if data.status == SprintStatus.ACTIVE:
            cards_q = select(func.count()).select_from(Card).where(
                Card.sprint_id == sprint_id, Card.archived.is_(False),
            )
            card_count = (await self.db.execute(cards_q)).scalar() or 0
            if card_count == 0:
                raise ValueError(
                    "Cannot activate sprint: no cards assigned. "
                    "Assign at least one card to this sprint before activating."
                )

        # Gate: active → review requires scoped test coverage check
        if data.status == SprintStatus.REVIEW:
            skip_tc = sprint.skip_test_coverage or (
                (board.settings or {}).get("skip_test_coverage_global", False) if board else False
            )
            if not skip_tc and spec and sprint.test_scenario_ids:
                scenarios = spec.test_scenarios or []
                scoped = [s for s in scenarios if s.get("id") in (sprint.test_scenario_ids or [])]
                not_covered = [s for s in scoped if s.get("status") != "passed"]
                if not_covered:
                    names = "; ".join(s.get("title", s.get("id", "?"))[:60] for s in not_covered[:5])
                    raise ValueError(
                        f"Cannot submit sprint for review: {len(not_covered)} scoped test scenario(s) "
                        f"not passed. Pending: {names}"
                        f"{f' (and {len(not_covered) - 5} more)' if len(not_covered) > 5 else ''}."
                    )

        # Gate: review → closed defesa em profundidade do test theater
        # prevention (Wave 2 NC-9, spec 873e98cc). Itera test cards do sprint,
        # checa se scenarios linked com status passed/automated têm evidence
        # persisted. Honra board.settings.skip_test_evidence_global.
        if data.status == SprintStatus.CLOSED and spec is not None:
            skip_evidence = bool(
                (board.settings or {}).get("skip_test_evidence_global", False)
                if board
                else False
            )
            if not skip_evidence:
                evidenceless: list[str] = []
                # Sprint -> Test cards -> linked scenarios -> evidence check
                test_cards_q = select(Card).where(
                    Card.sprint_id == sprint_id,
                    Card.archived.is_(False),
                    Card.card_type == "test",
                )
                test_cards = (await self.db.execute(test_cards_q)).scalars().all()
                spec_scenarios_by_id: dict[str, dict] = {
                    s.get("id"): s for s in (spec.test_scenarios or [])
                }
                for card in test_cards:
                    for sid in (card.test_scenario_ids or []):
                        sc = spec_scenarios_by_id.get(sid)
                        if not sc:
                            continue
                        if not scenario_has_required_evidence(sc):
                            evidenceless.append(sid)
                if evidenceless:
                    # NC-9 BR4 — sprint close gate as defense in depth.
                    raise ValueError(
                        f"Cannot close sprint: {len(evidenceless)} scenario(s) "
                        f"marked passed/automated without structured evidence: "
                        f"{', '.join(evidenceless[:5])}"
                        f"{f' (and {len(evidenceless) - 5} more)' if len(evidenceless) > 5 else ''}. "
                        "Provide evidence via update_test_scenario_status OR "
                        "enable skip_test_evidence_global on the board to bypass."
                    )
            elif spec is not None:
                # Skip ON — log forensics record so reactivation analytics
                # can flag boards that bypass the gate at sprint close.
                import logging as _logging
                _ev_logger = _logging.getLogger("okto_pulse.spec.test_scenario")
                _ev_logger.info(
                    "sprint.evidence_gate_skipped sprint=%s board=%s",
                    sprint_id, sprint.board_id,
                    extra={
                        "event": "sprint.evidence_gate_skipped",
                        "sprint_id": sprint_id,
                        "board_id": sprint.board_id,
                        "skip": True,
                    },
                )

        # Gate: review → closed requires evaluation
        if data.status == SprintStatus.CLOSED:
            skip_qual = sprint.skip_qualitative_validation
            if not skip_qual:
                evaluations = [e for e in (sprint.evaluations or []) if not e.get("stale")]
                approvals = [e for e in evaluations if e.get("recommendation") == "approve"]
                rejections = [e for e in evaluations if e.get("recommendation") == "reject"]
                if rejections:
                    names = ", ".join(e.get("evaluator_name", "?") for e in rejections)
                    raise ValueError(
                        f"Cannot close sprint: {len(rejections)} evaluation(s) with 'reject' "
                        f"recommendation (by: {names}). Remove or replace rejections."
                    )
                if not approvals:
                    raise ValueError(
                        "Cannot close sprint: no evaluation with 'approve' recommendation. "
                        "Submit an evaluation before closing."
                    )
                threshold = (
                    sprint.validation_threshold
                    or (board.settings or {}).get("validation_threshold_global", 70) if board else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if avg_score < threshold:
                    raise ValueError(
                        f"Cannot close sprint: average approval score ({avg_score:.0f}) "
                        f"is below threshold ({threshold})."
                    )

        old_status = sprint.status
        sprint.status = data.status

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                SprintClosed as SprintClosedEvent,
                SprintMoved as SprintMovedEvent,
            )

            await event_publish(
                SprintMovedEvent(
                    board_id=sprint.board_id,
                    actor_id=user_id,
                    sprint_id=sprint.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
            if data.status == SprintStatus.CLOSED:
                await event_publish(
                    SprintClosedEvent(
                        board_id=sprint.board_id,
                        actor_id=user_id,
                        sprint_id=sprint.id,
                    ),
                    session=self.db,
                )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, sprint.board_id)
        await self._log_activity(
            board_id=sprint.board_id, action="sprint_moved",
            actor_type="user", actor_id=user_id, actor_name=resolved_name,
            details={
                "sprint_id": sprint_id, "spec_id": sprint.spec_id,
                "from_status": old_status.value, "to_status": data.status.value,
                **self._lane_activity_details(sprint),
            },
        )
        await self._record_history(
            sprint_id=sprint_id, action="status_changed",
            actor_id=user_id, actor_name=resolved_name,
            changes=[{
                "field": "status",
                "old": old_status.value,
                "new": data.status.value,
                **self._lane_activity_details(sprint),
            }],
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=sprint.version,
        )
        await self.db.commit()
        return sprint

    async def delete_sprint(self, sprint_id: str, user_id: str) -> bool:
        """Delete a sprint. Unlinks cards but doesn't delete them."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return False
        await self.db.execute(
            update(Card).where(Card.sprint_id == sprint_id).values(sprint_id=None)
        )
        board_id = sprint.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(sprint)
        await self._log_activity(
            board_id=board_id, action="sprint_deleted",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={"sprint_id": sprint_id},
        )
        await self.db.commit()
        return True

    async def assign_tasks(
        self, sprint_id: str, card_ids: list[str], user_id: str,
    ) -> int:
        """Assign cards to a sprint. Cards must belong to the same spec."""
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            raise SprintOperationError(
                "sprint_not_found",
                "Sprint not found.",
                remediation="Refresh the sprint list and retry assignment with an existing sprint.",
                facts={"sprint_id": sprint_id},
            )
        cards_to_assign: list[Card] = []
        for card_id in card_ids:
            card = await self.db.get(Card, card_id)
            if not card:
                continue
            if card.spec_id != sprint.spec_id:
                raise ValueError(
                    f"Card '{card.title}' belongs to a different spec. "
                    f"Sprint spec: {sprint.spec_id}, card spec: {card.spec_id}"
                )
            if sprint.lane_type == SprintLaneType.HOTFIX and card.card_type not in {
                CardType.BUG,
                CardType.TEST,
            }:
                raise SprintOperationError(
                    "hotfix_lane_card_type_forbidden",
                    "Hotfix lanes accept only bug and test cards.",
                    remediation=(
                        "Assign only bug cards and regression test cards to the hotfix lane. "
                        "Use a normal sprint for implementation cards."
                    ),
                    facts={
                        "sprint_id": sprint_id,
                        "lane_type": sprint.lane_type.value,
                        "card_id": card.id,
                        "card_type": card.card_type.value,
                        "allowed_card_types": [CardType.BUG.value, CardType.TEST.value],
                    },
                )
            cards_to_assign.append(card)

        assigned = 0
        for card in cards_to_assign:
            card.sprint_id = sprint_id
            assigned += 1
        if assigned:
            actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
            await self._log_activity(
                board_id=sprint.board_id, action="sprint_tasks_assigned",
                actor_type="user", actor_id=user_id, actor_name=actor_name,
                details={
                    "sprint_id": sprint_id,
                    "card_ids": [card.id for card in cards_to_assign],
                    "count": assigned,
                    **self._lane_activity_details(sprint),
                    "accepted_card_types": (
                        [CardType.BUG.value, CardType.TEST.value]
                        if sprint.lane_type == SprintLaneType.HOTFIX
                        else [CardType.NORMAL.value, CardType.TEST.value, CardType.BUG.value]
                    ),
                },
            )
            await self._record_history(
                sprint_id=sprint_id, action="tasks_assigned",
                actor_id=user_id, actor_name=actor_name,
                changes=[{
                    "field": "cards",
                    "added": [card.id for card in cards_to_assign],
                    "count": assigned,
                    **self._lane_activity_details(sprint),
                }],
                summary=(
                    f"Assigned {assigned} card(s) to hotfix lane"
                    if sprint.lane_type == SprintLaneType.HOTFIX
                    else f"Assigned {assigned} card(s) to sprint"
                ),
                version=sprint.version,
            )
        await self.db.commit()
        return assigned

    async def submit_evaluation(
        self, sprint_id: str, user_id: str, evaluation: dict,
    ) -> Sprint | None:
        """Submit a qualitative evaluation for a sprint."""
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            return None
        if sprint.status != SprintStatus.REVIEW:
            raise ValueError(
                f"Evaluations can only be submitted for sprints in 'review' status "
                f"(current: '{sprint.status.value}')"
            )
        evaluator_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
        await _authorize_critical_context_or_raise(
            self.db,
            board_id=sprint.board_id,
            actor_id=user_id,
            entity_type="sprint",
            entity_id=sprint.id,
            critical_action=CriticalAction.SPRINT_SUBMIT_EVALUATION,
            surface="service",
            actor_type="user",
            actor_name=evaluator_name,
        )
        import uuid as _uuid
        eval_entry = {
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "evaluator_id": user_id,
            "evaluator_name": evaluator_name,
            "evaluator_type": "user",
            **evaluation,
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        evals = list(sprint.evaluations or [])
        evals.append(eval_entry)
        sprint.evaluations = evals
        flag_modified(sprint, "evaluations")

        await self._log_activity(
            board_id=sprint.board_id, action="sprint_evaluation_submitted",
            actor_type="user", actor_id=user_id, actor_name=eval_entry["evaluator_name"],
            details={
                "sprint_id": sprint_id,
                "evaluation_id": eval_entry["id"],
                "score": evaluation.get("overall_score"),
                **self._lane_activity_details(sprint),
            },
        )
        await self._record_history(
            sprint_id=sprint_id, action="evaluation_submitted",
            actor_id=user_id, actor_name=eval_entry["evaluator_name"],
            changes=[{
                "field": "evaluations",
                "evaluation_id": eval_entry["id"],
                "recommendation": evaluation.get("recommendation"),
                "overall_score": evaluation.get("overall_score"),
                **self._lane_activity_details(sprint),
            }],
            summary=f"Evaluation submitted: {evaluation.get('recommendation')} (score: {evaluation.get('overall_score')})",
            version=sprint.version,
        )
        await self.db.commit()
        return sprint

    async def list_history(self, sprint_id: str, limit: int = 50) -> list[SprintHistory]:
        query = (
            select(SprintHistory)
            .where(SprintHistory.sprint_id == sprint_id)
            .order_by(SprintHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def suggest_sprints(
        self, spec_id: str, threshold: int = 8,
    ) -> list[dict]:
        """Suggest sprint breakdown for a spec based on FRs, test scenarios, and dependencies.

        Algorithm:
        1. Group cards by linked FRs (via test_scenario_ids → linked_criteria).
        2. Consider card dependencies (dependent cards in same or later sprint).
        3. Distribute into N sprints where N = ceil(total_cards / threshold).
        4. Each sprint gets the test_scenario_ids and business_rule_ids for its cards.
        Returns suggestions without creating anything.
        """
        import math

        spec = await self.db.get(Spec, spec_id)
        if not spec:
            raise ValueError("Spec not found")

        cards_q = select(Card).where(
            Card.spec_id == spec_id, Card.archived.is_(False),
            Card.status.notin_([CardStatus.DONE, CardStatus.CANCELLED]),
        )
        result = await self.db.execute(cards_q)
        cards = list(result.scalars().all())

        if not cards:
            return []

        # Build FR→cards mapping via test_scenario_ids → linked_criteria
        scenarios = {s.get("id"): s for s in (spec.test_scenarios or [])}
        fr_groups: dict[str, list[Card]] = {}
        ungrouped: list[Card] = []

        for card in cards:
            linked_frs: set[str] = set()
            for ts_id in (card.test_scenario_ids or []):
                sc = scenarios.get(ts_id)
                if sc:
                    for crit in (sc.get("linked_criteria") or []):
                        linked_frs.add(crit)
            if linked_frs:
                primary_fr = sorted(linked_frs)[0]
                fr_groups.setdefault(primary_fr, []).append(card)
            else:
                ungrouped.append(card)

        # Build dependency graph
        deps_q = select(CardDependency).where(
            CardDependency.card_id.in_([c.id for c in cards])
        )
        deps_result = await self.db.execute(deps_q)
        dependencies = list(deps_result.scalars().all())
        dep_map: dict[str, set[str]] = {}
        for d in dependencies:
            dep_map.setdefault(d.card_id, set()).add(d.depends_on_id)

        # Flatten groups into ordered buckets
        all_groups = list(fr_groups.values())
        if ungrouped:
            all_groups.append(ungrouped)

        # Determine number of sprints
        total = len(cards)
        n_sprints = max(1, math.ceil(total / threshold))

        # Distribute groups across sprints
        suggested: list[list[Card]] = [[] for _ in range(n_sprints)]
        group_idx = 0
        for group in all_groups:
            target = group_idx % n_sprints
            suggested[target].extend(group)
            group_idx += 1

        # Ensure dependency ordering: if card A depends on B, B must be in same or earlier sprint
        card_sprint_map: dict[str, int] = {}
        for si, sprint_cards in enumerate(suggested):
            for c in sprint_cards:
                card_sprint_map[c.id] = si

        # Adjust: move cards earlier if their dependencies are in later sprints
        changed = True
        iterations = 0
        while changed and iterations < 10:
            changed = False
            iterations += 1
            for card_id, card_deps in dep_map.items():
                if card_id not in card_sprint_map:
                    continue
                card_si = card_sprint_map[card_id]
                for dep_id in card_deps:
                    dep_si = card_sprint_map.get(dep_id)
                    if dep_si is not None and dep_si > card_si:
                        # Move dependency to same sprint as dependent card
                        card_sprint_map[dep_id] = card_si
                        changed = True

        # Rebuild sprints from adjusted map
        final: list[list[Card]] = [[] for _ in range(n_sprints)]
        for card in cards:
            si = card_sprint_map.get(card.id, 0)
            final[si].append(card)

        # Build suggestion output
        suggestions = []
        for i, sprint_cards in enumerate(final):
            if not sprint_cards:
                continue
            # Collect scoped test scenario and BR IDs
            ts_ids: set[str] = set()
            br_ids: set[str] = set()
            for c in sprint_cards:
                for ts_id in (c.test_scenario_ids or []):
                    ts_ids.add(ts_id)
                    sc = scenarios.get(ts_id)
                    if sc:
                        for linked in (sc.get("linked_criteria") or []):
                            # Find BRs that reference this FR
                            for r in (spec.business_rules or []):
                                if linked in (r.get("linked_requirements") or []):
                                    br_ids.add(r.get("id"))

            suggestions.append({
                "title": f"Sprint {i + 1}",
                "description": f"Auto-suggested sprint ({len(sprint_cards)} tasks)",
                "card_ids": [c.id for c in sprint_cards],
                "card_titles": [c.title for c in sprint_cards],
                "test_scenario_ids": sorted(ts_ids) if ts_ids else None,
                "business_rule_ids": sorted(br_ids) if br_ids else None,
            })

        return suggestions


class SprintQAService:
    """Service for sprint Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(
        self, sprint_id: str, user_id: str, question: str,
        question_type: str = "text", choices: list | None = None,
        allow_free_text: bool = False,
    ) -> SprintQAItem | None:
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            return None
        qa = SprintQAItem(
            sprint_id=sprint_id, question=question,
            question_type=question_type or "text",
            choices=choices, allow_free_text=allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self, qa_id: str, user_id: str, answer: str | None = None,
        selected: list[str] | None = None,
        *,
        actor_type: str = "user",
        surface: str = "service",
    ) -> SprintQAItem | None:
        qa = await self.db.get(SprintQAItem, qa_id)
        if not qa:
            return None

        sprint = await self.db.get(Sprint, qa.sprint_id)
        board = await self.db.get(Board, sprint.board_id) if sprint else None
        await _authorize_qa_answer_or_raise(
            self.db,
            board=board,
            qa=qa,
            user_id=user_id,
            entity_type="sprint",
            question_id=qa_id,
            actor_type=actor_type,
            surface=surface,
        )

        qa.answer = answer
        qa.selected = selected
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        if selected is not None:
            flag_modified(qa, "selected")
        return qa

    async def list_qa(self, sprint_id: str) -> list[SprintQAItem]:
        query = (
            select(SprintQAItem)
            .where(SprintQAItem.sprint_id == sprint_id)
            .order_by(SprintQAItem.created_at.asc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(SprintQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True
