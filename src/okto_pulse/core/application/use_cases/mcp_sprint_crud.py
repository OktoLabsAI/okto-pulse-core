"""MCP-scoped sprint CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family: sprint).

PURITY GUARDRAIL (Codex-mandated, enforced by test_r01a_mcp_sprint_uow.py): this
module is transport-free. It MUST NOT import the MCP server/transport package nor any
server-side transport helper. The MCP adapter (server.py) stays THIN — it keeps only
the permission gate, the S-LANE-01 fail-closed DTO validation
(``_canonical_sprint_validation_error``) and the SprintOperationError-before-ValueError
envelopes; ALL aggregation (incl. cross-family) lives in these use cases (Clean Core).

Sprint family traits (from the parallel inventory wf_cd4ce2d2): board-scope is almost
entirely ABSENT (only get_sprint_context board-scopes the sprint, not the inner spec).
Most tools REUSE the existing REST sprint use cases (sprints_crud); only create_sprint,
get_sprint_context, answer/delete_sprint_question (and the delete_sprint_evaluation
boundary) need MCP-specific VARIANTS here. ``SprintOperationError`` IS a ``ValueError``
subclass — adapters that branch on it must catch it FIRST.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)


# --- create (skip_ownership; service only flushes -> use-case-commit) ---------


class McpCreateSprintCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class McpCreateSprintResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class McpCreateSprintUseCase:
    """Create a sprint (``skip_ownership_check=True`` — the binding divergence from the
    REST CreateSprintUseCase, which enforces ownership). ``SprintService.create_sprint``
    only FLUSHES, so this use case owns the single commit. The service emits its own
    ``_log_activity`` / ``_record_history`` / ``SprintCreated`` internally — do NOT add
    one. A ``None`` result (spec not found / wrong board) is returned as-is for the
    adapter's "Failed to create sprint (spec not found or wrong board)". ``Sprint-
    OperationError`` (a ``ValueError`` subclass) + ``ValueError`` propagate UNCAUGHT;
    the adapter maps them in that order. The adapter keeps the permission gate and the
    S-LANE-01 fail-closed ``SprintCreate`` DTO build (before this call)."""

    async def execute(
        self, command: McpCreateSprintCommand, *, actor: ActorContext, uow: Any
    ) -> McpCreateSprintResult:
        from okto_pulse.core.services.main import SprintService

        sprint = await SprintService(session_of(uow)).create_sprint(
            command.board_id, actor.actor_id, command.data, skip_ownership_check=True
        )
        await commit(uow)
        return McpCreateSprintResult(sprint)


# --- get (Clean Core: the full dict is built HERE, the adapter stays thin) ----


class McpGetSprintCommand:
    __slots__ = ("sprint_id",)

    def __init__(self, sprint_id: str) -> None:
        self.sprint_id = sprint_id


class McpGetSprintResult:
    __slots__ = ("result", "not_found")

    def __init__(self, result: Any = None, *, not_found: bool = False) -> None:
        self.result = result
        self.not_found = not_found


class McpGetSprintUseCase:
    """Fetch a sprint + build the FULL presentation dict (cards/qa_items lazy) HERE in
    the application layer (Clean Core: the MCP adapter must stay thin — no uow.session
    composed queries, no direct service). A ``None`` result -> ``not_found`` -> the
    adapter's "Sprint not found". No board-scope, auth-only (legacy parity)."""

    async def execute(
        self, command: McpGetSprintCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetSprintResult:
        from okto_pulse.core.services.main import SprintService

        sprint = await SprintService(session_of(uow)).get_sprint(command.sprint_id)
        if not sprint:
            return McpGetSprintResult(not_found=True)
        result = {
            "id": sprint.id, "spec_id": sprint.spec_id, "board_id": sprint.board_id,
            "title": sprint.title, "description": sprint.description,
            "status": sprint.status.value, "spec_version": sprint.spec_version,
            "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": sprint.normal_sprint_created,
            "start_date": sprint.start_date.isoformat() if sprint.start_date else None,
            "end_date": sprint.end_date.isoformat() if sprint.end_date else None,
            "test_scenario_ids": sprint.test_scenario_ids,
            "business_rule_ids": sprint.business_rule_ids,
            "evaluations": sprint.evaluations,
            "skip_test_coverage": sprint.skip_test_coverage,
            "skip_rules_coverage": sprint.skip_rules_coverage,
            "skip_qualitative_validation": sprint.skip_qualitative_validation,
            "version": sprint.version, "labels": sprint.labels,
            "cards": [
                {"id": c.id, "title": c.title, "status": c.status.value, "priority": c.priority.value}
                for c in sprint.cards
            ],
            "qa_items": [
                {"id": q.id, "question": q.question, "answer": q.answer, "asked_by": q.asked_by}
                for q in sprint.qa_items
            ],
            "created_by": sprint.created_by,
            "created_at": sprint.created_at.isoformat() if sprint.created_at else None,
        }
        return McpGetSprintResult(result)


# --- get_sprint_context (board-scoped sprint fetch only; adapter aggregates) --


class McpGetSprintContextCommand:
    __slots__ = ("sprint_id", "board_id", "include_spec")

    def __init__(self, sprint_id: str, board_id: str, include_spec: bool) -> None:
        self.sprint_id = sprint_id
        self.board_id = board_id
        self.include_spec = include_spec


class McpGetSprintContextResult:
    __slots__ = ("result",)

    def __init__(self, result: Any) -> None:
        self.result = result


class McpGetSprintContextUseCase:
    """Board-scoped sprint consolidated context (read, no commit). The WHOLE
    presentation aggregation is built HERE in the application layer (Codex: a
    cross-family aggregation must NOT leak into the MCP adapter — unlike the
    get_spec_context precedent, this block keeps the SpecService read in the use case):
    the sprint dict, the CROSS-FAMILY parent-spec read (``SpecService.get_spec``,
    intentionally NOT board-scoped — gated only on ``if spec``) and the scoped-item
    filtering. A missing OR cross-board sprint is ``EntityNotFoundError`` -> the
    adapter's ``"Sprint not found"``. The adapter only parses ``include_spec`` and
    ``json.dumps(result, default=str)``. The original two no-op read commits are
    dropped (read-no-commit)."""

    async def execute(
        self, command: McpGetSprintContextCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetSprintContextResult:
        from okto_pulse.core.services import SpecService
        from okto_pulse.core.services.main import SprintService

        session = session_of(uow)
        sprint = await SprintService(session).get_sprint(command.sprint_id)
        if not sprint or sprint.board_id != command.board_id:
            raise EntityNotFoundError("sprint", command.sprint_id)

        result: dict = {
            "id": sprint.id,
            "spec_id": sprint.spec_id,
            "board_id": sprint.board_id,
            "title": sprint.title,
            "description": sprint.description,
            "objective": getattr(sprint, "objective", None),
            "expected_outcome": getattr(sprint, "expected_outcome", None),
            "status": sprint.status.value,
            "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": sprint.normal_sprint_created,
            "spec_version": sprint.spec_version,
            "version": sprint.version,
            "start_date": sprint.start_date.isoformat() if sprint.start_date else None,
            "end_date": sprint.end_date.isoformat() if sprint.end_date else None,
            "test_scenario_ids": sprint.test_scenario_ids or [],
            "business_rule_ids": sprint.business_rule_ids or [],
            "evaluations": sprint.evaluations or [],
            "skip_test_coverage": sprint.skip_test_coverage,
            "skip_rules_coverage": sprint.skip_rules_coverage,
            "skip_qualitative_validation": sprint.skip_qualitative_validation,
            "labels": sprint.labels or [],
            "cards": [
                {
                    "id": c.id,
                    "title": c.title,
                    "status": c.status.value,
                    "priority": c.priority.value,
                    "card_type": c.card_type.value if c.card_type else "normal",
                    "test_scenario_ids": c.test_scenario_ids or [],
                }
                for c in sprint.cards
            ],
            "qa_items": [
                {"id": q.id, "question": q.question, "answer": q.answer, "asked_by": q.asked_by}
                for q in sprint.qa_items
            ],
            "created_by": sprint.created_by,
            "created_at": sprint.created_at.isoformat() if sprint.created_at else None,
        }

        if command.include_spec and sprint.spec_id:
            spec = await SpecService(session).get_spec(sprint.spec_id)
            if spec:
                sprint_card_ids = {c.id for c in sprint.cards}
                spec_ts = spec.test_scenarios or []
                spec_brs = spec.business_rules or []
                spec_trs = spec.technical_requirements or []
                spec_contracts = spec.api_contracts or []
                spec_irs = getattr(spec, "integration_requirements", None) or []
                spec_ors = getattr(spec, "observability_requirements", None) or []

                scoped_ts_ids = set(sprint.test_scenario_ids or [])
                scoped_ts = [
                    ts for ts in spec_ts
                    if ts.get("id") in scoped_ts_ids
                    or any(tid in sprint_card_ids for tid in (ts.get("linked_task_ids") or []))
                ]
                scoped_brs_ids = set(sprint.business_rule_ids or [])
                scoped_brs = [
                    br for br in spec_brs
                    if br.get("id") in scoped_brs_ids
                    or any(tid in sprint_card_ids for tid in (br.get("linked_task_ids") or []))
                ]
                scoped_trs = [
                    tr for tr in spec_trs
                    if isinstance(tr, dict)
                    and any(tid in sprint_card_ids for tid in (tr.get("linked_task_ids") or []))
                ]
                scoped_contracts = [
                    c for c in spec_contracts
                    if any(tid in sprint_card_ids for tid in (c.get("linked_task_ids") or []))
                ]
                scoped_irs = [
                    ir for ir in spec_irs
                    if any(tid in sprint_card_ids for tid in (ir.get("linked_task_ids") or []))
                ]
                scoped_ors = [
                    req for req in spec_ors
                    if any(tid in sprint_card_ids for tid in (req.get("linked_task_ids") or []))
                ]

                result["spec"] = {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "functional_requirements": spec.functional_requirements or [],
                    "technical_requirements": spec_trs,
                    "acceptance_criteria": spec.acceptance_criteria or [],
                    "test_scenarios": spec_ts,
                    "business_rules": spec_brs,
                    "api_contracts": spec_contracts,
                    "integration_requirements": spec_irs,
                    "observability_requirements": spec_ors,
                }
                result["scoped"] = {
                    "test_scenarios": scoped_ts,
                    "business_rules": scoped_brs,
                    "technical_requirements": scoped_trs,
                    "api_contracts": scoped_contracts,
                    "integration_requirements": scoped_irs,
                    "observability_requirements": scoped_ors,
                }

        return McpGetSprintContextResult(result)


# --- evaluations (Clean Core: aggregation/scan/mutation all out of the adapter) -


class McpListSprintEvaluationsCommand:
    __slots__ = ("sprint_id",)

    def __init__(self, sprint_id: str) -> None:
        self.sprint_id = sprint_id


class McpListSprintEvaluationsResult:
    __slots__ = ("result", "not_found")

    def __init__(self, result: Any = None, *, not_found: bool = False) -> None:
        self.result = result
        self.not_found = not_found


class McpListSprintEvaluationsUseCase:
    """List + aggregate a sprint's evaluations (read, no commit). The aggregation
    (total / non_stale / approvals / avg_score over the ``Sprint.evaluations`` JSON
    column) is built HERE (Clean Core: no composed read in the adapter). A ``None``
    sprint -> ``not_found`` -> "Sprint not found". No board-scope, auth-only."""

    async def execute(
        self, command: McpListSprintEvaluationsCommand, *, actor: ActorContext, uow: Any
    ) -> McpListSprintEvaluationsResult:
        from okto_pulse.core.services.main import SprintService

        sprint = await SprintService(session_of(uow)).get_sprint(command.sprint_id)
        if not sprint:
            return McpListSprintEvaluationsResult(not_found=True)
        evaluations = sprint.evaluations or []
        non_stale = [e for e in evaluations if not e.get("stale")]
        approvals = [e for e in non_stale if e.get("recommendation") == "approve"]
        result = {
            "sprint_id": command.sprint_id,
            "total": len(evaluations),
            "non_stale": len(non_stale),
            "approvals": len(approvals),
            "avg_score": (
                sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
            )
            if approvals
            else 0,
            "evaluations": evaluations,
        }
        return McpListSprintEvaluationsResult(result=result)


class McpGetSprintEvaluationCommand:
    __slots__ = ("sprint_id", "evaluation_id")

    def __init__(self, sprint_id: str, evaluation_id: str) -> None:
        self.sprint_id = sprint_id
        self.evaluation_id = evaluation_id


class McpGetSprintEvaluationResult:
    __slots__ = ("evaluation", "sprint_not_found", "eval_not_found")

    def __init__(
        self,
        evaluation: Any = None,
        *,
        sprint_not_found: bool = False,
        eval_not_found: bool = False,
    ) -> None:
        self.evaluation = evaluation
        self.sprint_not_found = sprint_not_found
        self.eval_not_found = eval_not_found


class McpGetSprintEvaluationUseCase:
    """Scan a sprint's evaluations for ``evaluation_id`` (read, no commit). The scan is
    HERE (Clean Core: no composed read in the adapter). Two-level not-found: missing
    sprint -> ``sprint_not_found`` -> "Sprint not found"; missing eval ->
    ``eval_not_found`` -> "Evaluation '<id>' not found". On hit, the raw eval dict is
    returned UNWRAPPED. No board-scope, auth-only."""

    async def execute(
        self, command: McpGetSprintEvaluationCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetSprintEvaluationResult:
        from okto_pulse.core.services.main import SprintService

        sprint = await SprintService(session_of(uow)).get_sprint(command.sprint_id)
        if not sprint:
            return McpGetSprintEvaluationResult(sprint_not_found=True)
        for e in sprint.evaluations or []:
            if e.get("id") == command.evaluation_id:
                return McpGetSprintEvaluationResult(e)
        return McpGetSprintEvaluationResult(eval_not_found=True)


class McpDeleteSprintEvaluationCommand:
    __slots__ = ("sprint_id", "evaluation_id")

    def __init__(self, sprint_id: str, evaluation_id: str) -> None:
        self.sprint_id = sprint_id
        self.evaluation_id = evaluation_id


class McpDeleteSprintEvaluationResult:
    __slots__ = ("status",)

    def __init__(self, status: str) -> None:
        self.status = status


class McpDeleteSprintEvaluationUseCase:
    """Delete a caller-owned evaluation (option A: the load + ownership gate + the
    ``Sprint.evaluations`` JSON mutation + the ORM dirty-flag live in the new
    ``SprintService.delete_evaluation`` — the relational ratchet keeps ORM mutation in
    the service). This use case ONLY orchestrates the UoW: it commits iff the service
    reports ``"deleted"`` and forwards the status for the adapter's envelope
    (``sprint_not_found`` / ``eval_not_found`` / ``not_owner`` / ``deleted``). No log,
    no board-scope (legacy parity)."""

    async def execute(
        self, command: McpDeleteSprintEvaluationCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteSprintEvaluationResult:
        from okto_pulse.core.services.main import SprintService

        status = await SprintService(session_of(uow)).delete_evaluation(
            command.sprint_id, actor.actor_id, command.evaluation_id
        )
        if status == "deleted":
            await commit(uow)
        return McpDeleteSprintEvaluationResult(status)


# --- Q&A (answer: NO perm/log + UNCONDITIONAL commit; delete: atomic log) -----


class McpAnswerSprintQuestionCommand:
    __slots__ = ("qa_id", "answer")

    def __init__(self, qa_id: str, answer: str) -> None:
        self.qa_id = qa_id
        self.answer = answer


class McpAnswerSprintQuestionResult:
    __slots__ = ("qa", "qa_not_found", "self_answer_error")

    def __init__(
        self, qa: Any = None, *, qa_not_found: bool = False, self_answer_error: Any = None
    ) -> None:
        self.qa = qa
        self.qa_not_found = qa_not_found
        self.self_answer_error = self_answer_error


class McpAnswerSprintQuestionUseCase:
    """Answer a sprint Q&A item. UNLIKE the refinement/ideation answer use cases: NO
    permission gate, NO activity log, and a PLAIN ``answer`` string (no choice payload).
    The commit is UNCONDITIONAL — the legacy commits AFTER the try-block and BEFORE the
    not-found check, so this commits even when ``qa`` is ``None``. A
    ``QASelfAnsweringNotAllowedError`` is caught + COMMITTED + surfaced. A ``None``
    result -> ``qa_not_found`` -> "Q&A item not found" (NOT "...or invalid selection")."""

    async def execute(
        self, command: McpAnswerSprintQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> McpAnswerSprintQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError
        from okto_pulse.core.services.main import SprintQAService

        service = SprintQAService(session_of(uow))
        try:
            qa = await service.answer_question(
                command.qa_id, actor.actor_id, command.answer,
                actor_type="agent", surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as exc:
            await commit(uow)
            return McpAnswerSprintQuestionResult(self_answer_error=exc)
        await commit(uow)  # UNCONDITIONAL (legacy parity — commits even on a None qa)
        if not qa:
            return McpAnswerSprintQuestionResult(qa_not_found=True)
        return McpAnswerSprintQuestionResult(qa)


class McpDeleteSprintQuestionCommand:
    __slots__ = ("board_id", "sprint_id", "qa_id")

    def __init__(self, board_id: str, sprint_id: str, qa_id: str) -> None:
        self.board_id = board_id
        self.sprint_id = sprint_id
        self.qa_id = qa_id


class McpDeleteSprintQuestionResult:
    __slots__ = ("qa_not_found",)

    def __init__(self, *, qa_not_found: bool = False) -> None:
        self.qa_not_found = qa_not_found


class McpDeleteSprintQuestionUseCase:
    """Delete a sprint Q&A item + atomic ``sprint_question_deleted`` activity log. A
    falsy delete short-circuits BEFORE the log and BEFORE the commit -> ``qa_not_found``
    -> "Q&A item not found". On success the log + commit run ATOMICALLY in the SAME
    transaction (``BoardService._log_activity``). The ``QA_DELETE`` permission gate
    stays in the adapter (present here, unlike the answer tool)."""

    async def execute(
        self, command: McpDeleteSprintQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteSprintQuestionResult:
        from okto_pulse.core.services import BoardService
        from okto_pulse.core.services.main import SprintQAService

        session = session_of(uow)
        deleted = await SprintQAService(session).delete_question(command.qa_id)
        if not deleted:
            return McpDeleteSprintQuestionResult(qa_not_found=True)
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            action="sprint_question_deleted",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={"sprint_id": command.sprint_id, "qa_id": command.qa_id},
        )
        await commit(uow)
        return McpDeleteSprintQuestionResult()
