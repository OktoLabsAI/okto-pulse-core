"""Critical workflow full-context guard.

BG-01 v1 resolves full context server-side at mutation time. This proves the
server had the full entity context available before a critical action; it does
not prove the actor reasoned over that context and does not add semantic
re-validation beyond the existing workflow gates.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.governance_observability import (
    METRIC_CRITICAL_CONTEXT_GUARD_DECISION,
    METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE,
    build_critical_context_decision_audit_details,
    build_critical_context_decision_metric_labels,
    build_critical_context_resolution_failure_metric_labels,
    build_critical_context_resolution_latency_metric_labels,
)


CRITICAL_CONTEXT_DECISION_ACTION = "critical_context_guard_decision"
CRITICAL_CONTEXT_DECISION_METRIC = METRIC_CRITICAL_CONTEXT_GUARD_DECISION
CRITICAL_CONTEXT_RESOLUTION_FAILURE_ACTION = "critical_context_resolution_failed"
CRITICAL_CONTEXT_RESOLUTION_FAILURE_METRIC = METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE
CONTEXT_FINGERPRINT_ALG = "ctx_sha256_v1"


class CriticalAction(str, Enum):
    """Canonical critical action registry keys for BG-01."""

    CARD_CREATE = "card.create"
    CARD_UPDATE = "card.update"
    CARD_MOVE_STATUS = "card.move_status"
    CARD_START_IMPLEMENTATION = "card.start_implementation"
    CARD_SUBMIT_VALIDATION = "card.submit_validation"
    CARD_CLOSEOUT = "card.closeout"
    CARD_CANCEL = "card.cancel"
    CARD_ARCHIVE = "card.archive"
    SPEC_MOVE_STATUS = "spec.move_status"
    SPEC_SUBMIT_VALIDATION = "spec.submit_validation"
    SPEC_SUBMIT_EVALUATION = "spec.submit_evaluation"
    SPEC_APPROVE = "spec.approve"
    SPEC_CLOSEOUT = "spec.closeout"
    SPEC_CANCEL = "spec.cancel"
    SPEC_ARCHIVE = "spec.archive"
    SPRINT_MOVE_STATUS = "sprint.move_status"
    SPRINT_SUBMIT_EVALUATION = "sprint.submit_evaluation"
    SPRINT_CLOSEOUT = "sprint.closeout"
    SPRINT_CANCEL = "sprint.cancel"
    SPRINT_ARCHIVE = "sprint.archive"
    IDEATION_MOVE_STATUS = "ideation.move_status"
    IDEATION_CLOSEOUT = "ideation.closeout"
    IDEATION_CANCEL = "ideation.cancel"
    REFINEMENT_MOVE_STATUS = "refinement.move_status"
    REFINEMENT_CLOSEOUT = "refinement.closeout"
    REFINEMENT_CANCEL = "refinement.cancel"


@dataclass(frozen=True)
class CriticalActionDefinition:
    action: CriticalAction
    entity_type: str
    category: str
    description: str


@dataclass(frozen=True)
class NonCriticalMutationExclusion:
    """Documented mutation intentionally outside BG-01 critical-action scope."""

    service: str
    method: str
    entity_type: str
    reason: str


@dataclass(frozen=True)
class CriticalMutationGuardCoverage:
    """Documented service mutation covered by the critical-context guard."""

    service: str
    method: str
    critical_action: CriticalAction
    context_entity_type: str
    reason: str


CRITICAL_ACTION_REGISTRY: tuple[CriticalActionDefinition, ...] = (
    CriticalActionDefinition(CriticalAction.CARD_CREATE, "card", "gate_sensitive_write", "Create a card/task/test/bug under a spec."),
    CriticalActionDefinition(CriticalAction.CARD_UPDATE, "card", "gate_sensitive_write", "Update gate-sensitive card fields or links."),
    CriticalActionDefinition(CriticalAction.CARD_MOVE_STATUS, "card", "status_move", "Move a card between workflow states."),
    CriticalActionDefinition(CriticalAction.CARD_START_IMPLEMENTATION, "card", "implementation_start", "Advance a card into implementation work."),
    CriticalActionDefinition(CriticalAction.CARD_SUBMIT_VALIDATION, "card", "validation", "Submit task validation."),
    CriticalActionDefinition(CriticalAction.CARD_CLOSEOUT, "card", "closeout", "Complete a card or bug."),
    CriticalActionDefinition(CriticalAction.CARD_CANCEL, "card", "cancellation", "Cancel a card."),
    CriticalActionDefinition(CriticalAction.CARD_ARCHIVE, "card", "archive", "Archive a card."),
    CriticalActionDefinition(CriticalAction.SPEC_MOVE_STATUS, "spec", "status_move", "Move a spec between workflow states."),
    CriticalActionDefinition(CriticalAction.SPEC_SUBMIT_VALIDATION, "spec", "validation", "Submit spec validation."),
    CriticalActionDefinition(CriticalAction.SPEC_SUBMIT_EVALUATION, "spec", "evaluation", "Submit spec evaluation."),
    CriticalActionDefinition(CriticalAction.SPEC_APPROVE, "spec", "approval", "Approve a spec for implementation readiness."),
    CriticalActionDefinition(CriticalAction.SPEC_CLOSEOUT, "spec", "closeout", "Complete a spec."),
    CriticalActionDefinition(CriticalAction.SPEC_CANCEL, "spec", "cancellation", "Cancel a spec."),
    CriticalActionDefinition(CriticalAction.SPEC_ARCHIVE, "spec", "archive", "Archive a spec."),
    CriticalActionDefinition(CriticalAction.SPRINT_MOVE_STATUS, "sprint", "status_move", "Move a sprint between workflow states."),
    CriticalActionDefinition(CriticalAction.SPRINT_SUBMIT_EVALUATION, "sprint", "evaluation", "Submit sprint evaluation."),
    CriticalActionDefinition(CriticalAction.SPRINT_CLOSEOUT, "sprint", "closeout", "Close a sprint."),
    CriticalActionDefinition(CriticalAction.SPRINT_CANCEL, "sprint", "cancellation", "Cancel a sprint."),
    CriticalActionDefinition(CriticalAction.SPRINT_ARCHIVE, "sprint", "archive", "Archive a sprint."),
    CriticalActionDefinition(CriticalAction.IDEATION_MOVE_STATUS, "ideation", "status_move", "Move an ideation between workflow states."),
    CriticalActionDefinition(CriticalAction.IDEATION_CLOSEOUT, "ideation", "closeout", "Complete an ideation."),
    CriticalActionDefinition(CriticalAction.IDEATION_CANCEL, "ideation", "cancellation", "Cancel an ideation."),
    CriticalActionDefinition(CriticalAction.REFINEMENT_MOVE_STATUS, "refinement", "status_move", "Move a refinement between workflow states."),
    CriticalActionDefinition(CriticalAction.REFINEMENT_CLOSEOUT, "refinement", "closeout", "Complete a refinement."),
    CriticalActionDefinition(CriticalAction.REFINEMENT_CANCEL, "refinement", "cancellation", "Cancel a refinement."),
)

_CRITICAL_ACTION_BY_VALUE = {item.action.value: item for item in CRITICAL_ACTION_REGISTRY}

NON_CRITICAL_MUTATION_EXCLUSIONS: tuple[NonCriticalMutationExclusion, ...] = (
    NonCriticalMutationExclusion(
        service="StoryService",
        method="move_story",
        entity_type="story",
        reason=(
            "Stories are discovery/planning helpers in BG-01 v1 and are not "
            "first-line gate-sensitive entities. Critical full-context guard "
            "coverage starts at ideation/refinement/spec/card/test/bug/sprint."
        ),
    ),
    NonCriticalMutationExclusion(
        service="StoryService",
        method="archive_story",
        entity_type="story",
        reason=(
            "Story archive/restore remains outside critical workflow gates; "
            "it does not approve, validate, close, or execute spec-bound work."
        ),
    ),
    NonCriticalMutationExclusion(
        service="ArchiveService",
        method="archive_tree",
        entity_type="tree",
        reason=(
            "Bulk archive_tree is governed by the destructive-operations flow "
            "and soft-delete semantics. BG-01 v1 critical context focuses on "
            "workflow lifecycle, validation/evaluation, and gate-sensitive "
            "card mutations; tree archive is explicitly excluded rather than "
            "silently left uncovered."
        ),
    ),
)

CRITICAL_MUTATION_GUARD_COVERAGE: tuple[CriticalMutationGuardCoverage, ...] = (
    CriticalMutationGuardCoverage(
        "CardService",
        "create_card",
        CriticalAction.CARD_CREATE,
        "spec",
        "Pre-create guard resolves the parent spec context because the card row does not exist yet.",
    ),
    CriticalMutationGuardCoverage(
        "CardService",
        "update_card",
        CriticalAction.CARD_UPDATE,
        "card",
        "Card updates can affect gate-sensitive traceability and execution metadata.",
    ),
    CriticalMutationGuardCoverage(
        "CardService",
        "submit_task_validation",
        CriticalAction.CARD_SUBMIT_VALIDATION,
        "card",
        "Task validation can route the card to done.",
    ),
    CriticalMutationGuardCoverage(
        "CardService",
        "confirm_amendment_coverage",
        CriticalAction.CARD_SUBMIT_VALIDATION,
        "card",
        "Path B amendment coverage confirmation can unblock bug closure and must resolve the regression test context.",
    ),
    CriticalMutationGuardCoverage(
        "CardService",
        "move_card",
        CriticalAction.CARD_MOVE_STATUS,
        "card",
        "Card status movement includes implementation start, validation handoff, closeout and cancellation.",
    ),
    CriticalMutationGuardCoverage(
        "SpecService",
        "move_spec",
        CriticalAction.SPEC_MOVE_STATUS,
        "spec",
        "Spec status movement includes approval, implementation start, closeout and cancellation.",
    ),
    CriticalMutationGuardCoverage(
        "SpecService",
        "submit_spec_validation",
        CriticalAction.SPEC_SUBMIT_VALIDATION,
        "spec",
        "Spec validation locks content and can promote the spec to validated.",
    ),
    CriticalMutationGuardCoverage(
        "IdeationService",
        "move_ideation",
        CriticalAction.IDEATION_MOVE_STATUS,
        "ideation",
        "Ideation lifecycle movement can complete or cancel the ideation.",
    ),
    CriticalMutationGuardCoverage(
        "RefinementService",
        "move_refinement",
        CriticalAction.REFINEMENT_MOVE_STATUS,
        "refinement",
        "Refinement lifecycle movement can complete or cancel the refinement.",
    ),
    CriticalMutationGuardCoverage(
        "SprintService",
        "move_sprint",
        CriticalAction.SPRINT_MOVE_STATUS,
        "sprint",
        "Sprint status movement includes activation, review, closeout and cancellation.",
    ),
    CriticalMutationGuardCoverage(
        "SprintService",
        "submit_evaluation",
        CriticalAction.SPRINT_SUBMIT_EVALUATION,
        "sprint",
        "Sprint evaluation can unblock sprint closeout.",
    ),
)


class FullContextResolver(Protocol):
    """Resolve a full entity context for a guarded action."""

    async def resolve_full_context(
        self,
        *,
        board_id: str,
        entity_type: str,
        entity_id: str,
        critical_action: CriticalAction,
    ) -> Any:
        ...


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _model_snapshot(model: Any, *, include: tuple[str, ...] | None = None) -> dict[str, Any]:
    """Serialize mapped columns only; no lazy relationship traversal."""

    if model is None:
        return {}
    columns = getattr(getattr(model, "__mapper__", None), "columns", ())
    names = include or tuple(column.key for column in columns)
    return {
        name: _json_safe(getattr(model, name, None))
        for name in names
        if hasattr(model, name)
    }


class DatabaseFullContextResolver:
    """Resolve full server-side entity context from the local database.

    The resolved payload is used only to compute a fingerprint. It deliberately
    includes structural relationships and gate-relevant linked entities while
    avoiding lazy relationship access and avoiding persistence of the payload.
    """

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def resolve_full_context(
        self,
        *,
        board_id: str,
        entity_type: str,
        entity_id: str,
        critical_action: CriticalAction,
    ) -> Any:
        from okto_pulse.core.models.db import (
            Board,
            Card,
            Ideation,
            Refinement,
            Spec,
            Sprint,
        )

        model_by_type = {
            "card": Card,
            "spec": Spec,
            "sprint": Sprint,
            "ideation": Ideation,
            "refinement": Refinement,
        }
        model_cls = model_by_type.get(entity_type)
        if model_cls is None:
            raise ValueError(f"unsupported_full_context_entity_type: {entity_type}")

        entity = await self.db.get(model_cls, entity_id)
        if entity is None:
            raise ValueError(f"full_context_unavailable: {entity_type} not found")
        if getattr(entity, "board_id", None) != board_id:
            raise ValueError(
                "full_context_unavailable: entity belongs to a different board"
            )

        board = await self.db.get(Board, board_id)
        payload: dict[str, Any] = {
            "board": await self._snapshot(
                board,
                include=("id", "name", "description", "settings", "updated_at"),
            ),
            "entity_type": entity_type,
            "entity_id": entity_id,
            "critical_action": critical_action.value,
            entity_type: await self._snapshot(entity),
            "relations": await self._resolve_relations(entity_type, entity),
        }
        return payload

    async def _snapshot(
        self,
        model: Any,
        *,
        include: tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        if model is not None:
            await self.db.refresh(model)
        return _model_snapshot(model, include=include)

    async def _resolve_relations(self, entity_type: str, entity: Any) -> dict[str, Any]:
        from okto_pulse.core.models.db import (
            Card,
            CardDependency,
            Ideation,
            Refinement,
            Spec,
            Sprint,
        )

        if entity_type == "card":
            spec = await self.db.get(Spec, entity.spec_id) if entity.spec_id else None
            sprint = await self.db.get(Sprint, entity.sprint_id) if entity.sprint_id else None
            dependency_rows = (
                await self.db.execute(
                    select(CardDependency.depends_on_id)
                    .where(CardDependency.card_id == entity.id)
                    .order_by(CardDependency.depends_on_id.asc())
                )
            ).scalars().all()
            return {
                "spec": await self._snapshot(spec),
                "sprint": await self._snapshot(sprint),
                "depends_on_ids": list(dependency_rows),
                "resolved_dependency_count": len(dependency_rows),
                "linked_test_task_ids": list(entity.linked_test_task_ids or []),
                "test_scenario_ids": list(entity.test_scenario_ids or []),
            }

        if entity_type == "spec":
            card_count = (
                await self.db.execute(
                    select(func.count())
                    .select_from(Card)
                    .where(Card.spec_id == entity.id, Card.archived.is_(False))
                )
            ).scalar() or 0
            sprint_count = (
                await self.db.execute(
                    select(func.count())
                    .select_from(Sprint)
                    .where(Sprint.spec_id == entity.id, Sprint.archived.is_(False))
                )
            ).scalar() or 0
            cards = (
                await self.db.execute(
                    select(Card.id, Card.title, Card.status, Card.card_type)
                    .where(Card.spec_id == entity.id, Card.archived.is_(False))
                    .order_by(Card.created_at.asc())
                )
            ).all()
            return {
                "card_count": int(card_count),
                "sprint_count": int(sprint_count),
                "cards": [
                    {
                        "id": row.id,
                        "title": row.title,
                        "status": _json_safe(row.status),
                        "card_type": _json_safe(row.card_type),
                    }
                    for row in cards
                ],
            }

        if entity_type == "sprint":
            spec = await self.db.get(Spec, entity.spec_id) if entity.spec_id else None
            card_count = (
                await self.db.execute(
                    select(func.count())
                    .select_from(Card)
                    .where(Card.sprint_id == entity.id, Card.archived.is_(False))
                )
            ).scalar() or 0
            return {
                "spec": await self._snapshot(spec),
                "card_count": int(card_count),
                "test_scenario_ids": list(entity.test_scenario_ids or []),
            }

        if entity_type == "ideation":
            refinement_count = (
                await self.db.execute(
                    select(func.count())
                    .select_from(Refinement)
                    .where(Refinement.ideation_id == entity.id, Refinement.archived.is_(False))
                )
            ).scalar() or 0
            spec_count = (
                await self.db.execute(
                    select(func.count())
                    .select_from(Spec)
                    .where(Spec.ideation_id == entity.id, Spec.archived.is_(False))
                )
            ).scalar() or 0
            return {
                "refinement_count": int(refinement_count),
                "spec_count": int(spec_count),
            }

        if entity_type == "refinement":
            ideation = await self.db.get(Ideation, entity.ideation_id) if entity.ideation_id else None
            specs = (
                await self.db.execute(
                    select(Spec.id, Spec.title, Spec.status)
                    .where(Spec.refinement_id == entity.id, Spec.archived.is_(False))
                    .order_by(Spec.created_at.asc())
                )
            ).all()
            return {
                "ideation": await self._snapshot(ideation),
                "specs": [
                    {
                        "id": row.id,
                        "title": row.title,
                        "status": _json_safe(row.status),
                    }
                    for row in specs
                ],
            }

        return {}


def build_default_full_context_resolvers(
    db: AsyncSession,
) -> dict[str, FullContextResolver]:
    resolver = DatabaseFullContextResolver(db)
    return {
        "card": resolver,
        "spec": resolver,
        "sprint": resolver,
        "ideation": resolver,
        "refinement": resolver,
    }


class ContextFingerprintProvider:
    """Stable, content-addressed fingerprint for resolved full context."""

    @classmethod
    def compute(cls, full_context: Any) -> str:
        if full_context is None:
            raise ValueError("full_context_unavailable: context is None")
        if full_context == {} or full_context == [] or full_context == "":
            raise ValueError("full_context_unavailable: context is empty")
        canonical = json.dumps(
            full_context,
            sort_keys=True,
            separators=(",", ":"),
            default=cls._json_default,
            ensure_ascii=True,
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return f"{CONTEXT_FINGERPRINT_ALG}:{digest}"

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, Enum):
            return value.value
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if hasattr(value, "to_dict"):
            return value.to_dict()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


@dataclass(frozen=True)
class CriticalContextDecision:
    board_id: str
    actor_id: str
    entity_type: str
    entity_id: str
    critical_action: CriticalAction
    surface: str
    outcome: str
    reason: str
    context_profile: str
    context_fingerprint: str | None
    context_resolved_at: str | None
    latency_ms: float

    def metric_labels(self) -> dict[str, Any]:
        return build_critical_context_decision_metric_labels(
            board_id=self.board_id,
            actor_id=self.actor_id,
            entity_type=self.entity_type,
            critical_action=self.critical_action.value,
            surface=self.surface,
            outcome=self.outcome,
            reason=self.reason,
            context_profile=self.context_profile,
        )

    def audit_details(self) -> dict[str, Any]:
        details = build_critical_context_decision_audit_details(
            board_id=self.board_id,
            actor_id=self.actor_id,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            critical_action=self.critical_action.value,
            surface=self.surface,
            outcome=self.outcome,
            reason=self.reason,
            context_profile=self.context_profile,
            context_fingerprint=self.context_fingerprint or "",
            context_resolved_at=self.context_resolved_at or "",
        )
        return {key: value for key, value in details.items() if value not in ("", None)}

    def latency_metric_labels(self) -> dict[str, Any]:
        return build_critical_context_resolution_latency_metric_labels(
            board_id=self.board_id,
            entity_type=self.entity_type,
            critical_action=self.critical_action.value,
            surface=self.surface,
            outcome=self.outcome,
            context_profile=self.context_profile,
        )

    def resolution_failure_metric_labels(self) -> dict[str, Any]:
        return build_critical_context_resolution_failure_metric_labels(
            board_id=self.board_id,
            entity_type=self.entity_type,
            critical_action=self.critical_action.value,
            resolver_name=f"{self.entity_type}_full_context_resolver",
            failure_reason=self.reason,
            surface=self.surface,
            outcome=self.outcome,
        )


class FullContextGuardError(ValueError):
    """Base class for critical-context guard failures."""

    reason = "full_context_unavailable"

    def __init__(self, message: str, *, decision: CriticalContextDecision) -> None:
        super().__init__(message)
        self.decision = decision


class FullContextRequiredError(FullContextGuardError):
    reason = "full_context_required"


class FullContextUnavailableError(FullContextGuardError):
    reason = "full_context_unavailable"


class FullContextCriticalActionGuard:
    """Authorize critical actions against board full-context policy."""

    def __init__(
        self,
        db: AsyncSession,
        *,
        resolvers: Mapping[str, FullContextResolver] | None = None,
        fingerprint_provider: type[ContextFingerprintProvider] = ContextFingerprintProvider,
    ) -> None:
        self.db = db
        self.resolvers = dict(resolvers or {})
        self.fingerprint_provider = fingerprint_provider

    async def authorize_and_resolve(
        self,
        *,
        board_id: str,
        actor_id: str,
        entity_type: str,
        entity_id: str,
        critical_action: CriticalAction | str,
        surface: str = "service",
    ) -> CriticalContextDecision:
        action = coerce_critical_action(critical_action)
        started = time.perf_counter()
        settings = await BoardGovernanceService(self.db).resolve(board_id)
        if not settings.require_full_context_for_critical_actions:
            return self._decision(
                board_id=board_id,
                actor_id=actor_id,
                entity_type=entity_type,
                entity_id=entity_id,
                critical_action=action,
                surface=surface,
                outcome="allow",
                reason="guard_disabled",
                context_profile="not_required",
                context_fingerprint=None,
                context_resolved_at=None,
                started=started,
            )

        resolver = self.resolvers.get(entity_type)
        if resolver is None:
            decision = self._decision(
                board_id=board_id,
                actor_id=actor_id,
                entity_type=entity_type,
                entity_id=entity_id,
                critical_action=action,
                surface=surface,
                outcome="deny",
                reason=FullContextRequiredError.reason,
                context_profile="missing",
                context_fingerprint=None,
                context_resolved_at=None,
                started=started,
            )
            raise FullContextRequiredError(
                "full_context_required: no full-context resolver is registered for "
                f"entity_type={entity_type!r}.",
                decision=decision,
            )

        try:
            full_context = await resolver.resolve_full_context(
                board_id=board_id,
                entity_type=entity_type,
                entity_id=entity_id,
                critical_action=action,
            )
            fingerprint = self.fingerprint_provider.compute(full_context)
        except FullContextGuardError:
            raise
        except Exception as exc:
            decision = self._decision(
                board_id=board_id,
                actor_id=actor_id,
                entity_type=entity_type,
                entity_id=entity_id,
                critical_action=action,
                surface=surface,
                outcome="deny",
                reason=FullContextUnavailableError.reason,
                context_profile="unavailable",
                context_fingerprint=None,
                context_resolved_at=None,
                started=started,
            )
            raise FullContextUnavailableError(
                f"full_context_unavailable: failed to resolve or fingerprint full context ({exc}).",
                decision=decision,
            ) from exc

        return self._decision(
            board_id=board_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            critical_action=action,
            surface=surface,
            outcome="allow",
            reason="full_context_resolved",
            context_profile="full",
            context_fingerprint=fingerprint,
            context_resolved_at=datetime.now(timezone.utc).isoformat(),
            started=started,
        )

    @staticmethod
    def _decision(
        *,
        board_id: str,
        actor_id: str,
        entity_type: str,
        entity_id: str,
        critical_action: CriticalAction,
        surface: str,
        outcome: str,
        reason: str,
        context_profile: str,
        context_fingerprint: str | None,
        context_resolved_at: str | None,
        started: float,
    ) -> CriticalContextDecision:
        return CriticalContextDecision(
            board_id=board_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            critical_action=critical_action,
            surface=surface,
            outcome=outcome,
            reason=reason,
            context_profile=context_profile,
            context_fingerprint=context_fingerprint,
            context_resolved_at=context_resolved_at,
            latency_ms=(time.perf_counter() - started) * 1000,
        )


def coerce_critical_action(value: CriticalAction | str) -> CriticalAction:
    if isinstance(value, CriticalAction):
        return value
    try:
        return CriticalAction(value)
    except ValueError as exc:
        raise ValueError(f"unsupported_critical_action: {value!r}") from exc


def get_critical_action_definition(
    value: CriticalAction | str,
) -> CriticalActionDefinition | None:
    action = coerce_critical_action(value)
    return _CRITICAL_ACTION_BY_VALUE.get(action.value)


def critical_actions_for_entity(entity_type: str) -> tuple[CriticalActionDefinition, ...]:
    return tuple(item for item in CRITICAL_ACTION_REGISTRY if item.entity_type == entity_type)
