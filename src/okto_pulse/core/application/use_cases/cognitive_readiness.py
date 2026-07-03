"""KG cognitive readiness read use cases (SaaS Refactor spec R01A MCP-FU3).

Transport-free reimplementations of the three READ-ONLY cognitive MCP tools that
open a relational session — ``evaluate_bug_cognitive_closure``,
``list_cognitive_readiness_items``, ``evaluate_cognitive_readiness`` — so
``mcp/server.py`` no longer opens a raw ``get_db_for_mcp()`` session for them. Each
delegates to the existing central ``CognitiveReadinessService`` /
``CognitiveActionCenterReadModel`` so verdicts/payloads are byte-identical and
precedence is NEVER recomputed. Read-only: no commit. ``CognitiveReadinessError``
propagates for the adapter to map.

Enforcement (``would_block_done`` / ``enforcement_active``) is resolved via the
transport-free ``cognitive_enforcement_active`` reader (extracted from server.py)
— never recomputed. The list/readiness use cases return the raw verdict/result +
enforcement flag; the adapter assembles the final payload (``would_block_done``
per item, summary), preserving the exact tool behaviour.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext, session_of


def _readiness_service():
    """Central readiness service over the shared item store — mirrors the legacy
    ``_build_cognitive_readiness_service`` (kg modules only; no server import)."""
    from okto_pulse.core.services.application_kg import build_cognitive_readiness_service

    return build_cognitive_readiness_service()


class EvaluateBugCognitiveClosureCommand:
    __slots__ = (
        "board_id", "bug_id", "evidence", "requested_action", "reason_code",
        "justification", "evidence_refs", "revisit_at",
    )

    def __init__(
        self,
        board_id: str,
        bug_id: str,
        *,
        evidence: dict | None = None,
        requested_action: str = "evaluate",
        reason_code: str | None = None,
        justification: str | None = None,
        evidence_refs: list[str] | None = None,
        revisit_at: str | None = None,
    ) -> None:
        self.board_id = board_id
        self.bug_id = bug_id
        self.evidence = evidence
        self.requested_action = requested_action
        self.reason_code = reason_code
        self.justification = justification
        self.evidence_refs = evidence_refs
        self.revisit_at = revisit_at


class EvaluateBugCognitiveClosureResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class EvaluateBugCognitiveClosureUseCase:
    """Read-only bug cognitive-closure verdict (no commit). ``CognitiveReadinessError``
    propagates."""

    async def execute(
        self, command: EvaluateBugCognitiveClosureCommand, *, actor: ActorContext, uow: Any
    ) -> EvaluateBugCognitiveClosureResult:
        from okto_pulse.core.services.application_kg import evaluate_bug_cognitive_closure

        data = await evaluate_bug_cognitive_closure(
            _readiness_service(),
            session_of(uow),
            board_id=command.board_id,
            bug_id=command.bug_id,
            evidence=command.evidence or {},
            requested_action=command.requested_action,
            reason_code=command.reason_code,
            actor=actor.actor_id,
            justification=command.justification,
            evidence_refs=command.evidence_refs,
            revisit_at=command.revisit_at,
        )
        return EvaluateBugCognitiveClosureResult(data)


class ListCognitiveReadinessItemsCommand:
    __slots__ = (
        "board_id", "signal", "artifact_id", "source_ref", "reason_code",
        "status", "search", "limit", "offset", "kg_generation_id",
    )

    def __init__(
        self,
        board_id: str,
        *,
        signal: str = "all",
        artifact_id: str | None = None,
        source_ref: str | None = None,
        reason_code: str | None = None,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
        kg_generation_id: str | None = None,
    ) -> None:
        self.board_id = board_id
        self.signal = signal
        self.artifact_id = artifact_id
        self.source_ref = source_ref
        self.reason_code = reason_code
        self.status = status
        self.search = search
        self.limit = limit
        self.offset = offset
        self.kg_generation_id = kg_generation_id


class ListCognitiveReadinessItemsResult:
    __slots__ = ("result", "enforcement_active")

    def __init__(self, result: dict[str, Any], enforcement_active: bool) -> None:
        self.result = result
        self.enforcement_active = enforcement_active


class ListCognitiveReadinessItemsUseCase:
    """List cognitive-readiness rows + the board enforcement flag (read-only)."""

    async def execute(
        self, command: ListCognitiveReadinessItemsCommand, *, actor: ActorContext, uow: Any
    ) -> ListCognitiveReadinessItemsResult:
        from okto_pulse.core.services.application_kg import (
            build_cognitive_action_center_read_model,
        )
        from okto_pulse.core.services.main import cognitive_enforcement_active

        session = session_of(uow)
        read_model = build_cognitive_action_center_read_model(_readiness_service())
        result = await read_model.list_signals(
            session,
            board_id=command.board_id,
            signal=command.signal,
            artifact_id=command.artifact_id,
            source_ref=command.source_ref,
            reason_code=command.reason_code,
            status=command.status,
            search=command.search,
            limit=command.limit,
            offset=command.offset,
            kg_generation_id=command.kg_generation_id,
        )
        enforcement_active = await cognitive_enforcement_active(session, command.board_id)
        return ListCognitiveReadinessItemsResult(result, enforcement_active)


class EvaluateCognitiveReadinessCommand:
    __slots__ = ("board_id", "source_ref", "kg_generation_id", "has_reusable_cognition")

    def __init__(
        self,
        board_id: str,
        *,
        source_ref: str,
        kg_generation_id: str | None = None,
        has_reusable_cognition: bool = True,
    ) -> None:
        self.board_id = board_id
        self.source_ref = source_ref
        self.kg_generation_id = kg_generation_id
        self.has_reusable_cognition = has_reusable_cognition


class EvaluateCognitiveReadinessResult:
    __slots__ = ("verdict", "enforcement_active")

    def __init__(self, verdict: Any, enforcement_active: bool) -> None:
        self.verdict = verdict
        self.enforcement_active = enforcement_active


class EvaluateCognitiveReadinessUseCase:
    """Central readiness verdict + the board enforcement flag (read-only).
    ``CognitiveReadinessError`` propagates."""

    async def execute(
        self, command: EvaluateCognitiveReadinessCommand, *, actor: ActorContext, uow: Any
    ) -> EvaluateCognitiveReadinessResult:
        from okto_pulse.core.services.main import cognitive_enforcement_active

        session = session_of(uow)
        verdict = await _readiness_service().evaluate_artifact(
            session,
            board_id=command.board_id,
            source_ref=command.source_ref,
            kg_generation_id=command.kg_generation_id,
            has_reusable_cognition=command.has_reusable_cognition,
        )
        enforcement_active = await cognitive_enforcement_active(session, command.board_id)
        return EvaluateCognitiveReadinessResult(verdict, enforcement_active)
