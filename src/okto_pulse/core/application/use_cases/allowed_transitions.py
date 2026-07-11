"""Allowed transition read model for lifecycle UI/action surfaces.

The transition authority stays in the services that enforce state moves. This
module projects those same programmatic tables into a read model for UI and MCP
consumers, so display affordances cannot drift into a parallel lifecycle map.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping, Sequence

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
)
from okto_pulse.core.domain.enums import IdeationStatus, RefinementStatus, SpecStatus
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.services import IdeationService, RefinementService, SpecService

ALLOWED_TRANSITIONS_SOURCE = "programmatic_backend_transition_authority"
ALLOWED_TRANSITIONS_DRIFT_METRIC = "allowed_transitions_contract_drift_total"


@dataclass(frozen=True)
class TransitionAuthority:
    status_enum: type[Enum]
    transitions: Callable[[], Mapping[Enum, Sequence[Enum]]]


_TRANSITION_AUTHORITIES: dict[str, TransitionAuthority] = {
    "ideation": TransitionAuthority(
        status_enum=IdeationStatus,
        transitions=lambda: IdeationService._IDEATION_TRANSITIONS,
    ),
    "refinement": TransitionAuthority(
        status_enum=RefinementStatus,
        transitions=lambda: RefinementService._REFINEMENT_TRANSITIONS,
    ),
    "spec": TransitionAuthority(
        status_enum=SpecStatus,
        transitions=lambda: SpecService._SPEC_TRANSITIONS,
    ),
}


@dataclass(frozen=True)
class AllowedTransition:
    to_status: str
    label: str
    gate: str
    blocked_reason: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "to_status": self.to_status,
            "label": self.label,
            "gate": self.gate,
            "blocked_reason": self.blocked_reason,
        }


@dataclass(frozen=True)
class AllowedTransitionsReadModel:
    board_id: str
    entity_type: str
    entity_id: str | None
    current_status: str
    allowed_transitions: list[AllowedTransition]
    source: str = ALLOWED_TRANSITIONS_SOURCE

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "current_status": self.current_status,
            "allowed_transitions": [item.to_dict() for item in self.allowed_transitions],
            "source": self.source,
        }


@dataclass(frozen=True)
class AllowedTransitionDriftReport:
    metric_name: str
    drift_total: int
    missing_edges: list[tuple[str, str, str]]
    extra_edges: list[tuple[str, str, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "drift_total": self.drift_total,
            "missing_edges": list(self.missing_edges),
            "extra_edges": list(self.extra_edges),
        }


class ListAllowedTransitionsCommand:
    __slots__ = ("board_id", "entity_type", "entity_id", "current_status")

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        *,
        entity_id: str | None = None,
        current_status: str | None = None,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.current_status = current_status


class ListAllowedTransitionsResult:
    __slots__ = ("read_model",)

    def __init__(self, read_model: AllowedTransitionsReadModel) -> None:
        self.read_model = read_model


def _authority_for(entity_type: str) -> TransitionAuthority:
    normalized = (entity_type or "").strip().lower()
    authority = _TRANSITION_AUTHORITIES.get(normalized)
    if authority is None:
        allowed = ", ".join(sorted(_TRANSITION_AUTHORITIES))
        raise CommandValidationError(f"Invalid entity_type. Must be one of: {allowed}")
    return authority


def _parse_status(entity_type: str, status: str) -> Enum:
    authority = _authority_for(entity_type)
    try:
        return authority.status_enum(status)
    except ValueError as exc:
        allowed = [item.value for item in authority.status_enum]
        raise CommandValidationError(
            f"Invalid status for {entity_type}. Must be one of: {allowed}"
        ) from exc


def _label_for(status: Enum) -> str:
    return str(status.value).replace("_", " ").title()


def _gate_for(entity_type: str, from_status: Enum, to_status: Enum) -> str:
    from_value = str(from_status.value)
    to_value = str(to_status.value)
    if entity_type == "spec":
        if from_value == "approved" and to_value == "validated":
            return "spec_validation"
        if from_value == "validated" and to_value == "in_progress":
            return "spec_evaluation"
        if from_value == "in_progress" and to_value == "done":
            return "coverage_and_tasks"
        if from_value in {"validated", "in_progress", "done"} and to_value in {
            "draft",
            "review",
            "approved",
        }:
            return "unlock_content"
    if entity_type == "ideation" and from_value == "evaluating" and to_value == "done":
        return "ambiguity_resource_cognitive"
    if entity_type == "refinement" and to_value == "done":
        return "resource_cognitive"
    return "none"


def allowed_transitions_for_status(entity_type: str, current_status: str) -> list[AllowedTransition]:
    """Project the enforced service transition table for one status."""

    normalized = (entity_type or "").strip().lower()
    from_status = _parse_status(normalized, current_status)
    authority = _authority_for(normalized)
    transitions = authority.transitions()
    return [
        AllowedTransition(
            to_status=str(to_status.value),
            label=_label_for(to_status),
            gate=_gate_for(normalized, from_status, to_status),
        )
        for to_status in transitions.get(from_status, [])
    ]


def allowed_transition_edges() -> dict[str, dict[str, list[str]]]:
    """Return the current programmatic authority as comparable string edges."""

    edges: dict[str, dict[str, list[str]]] = {}
    for entity_type, authority in _TRANSITION_AUTHORITIES.items():
        edges[entity_type] = {
            str(from_status.value): [str(to_status.value) for to_status in to_statuses]
            for from_status, to_statuses in authority.transitions().items()
        }
    return edges


def calculate_allowed_transition_drift(
    candidate_edges: Mapping[str, Mapping[str, Sequence[str]]],
) -> AllowedTransitionDriftReport:
    """Compare a proposed/docs-derived edge set with the runtime authority."""

    expected = allowed_transition_edges()
    missing: list[tuple[str, str, str]] = []
    extra: list[tuple[str, str, str]] = []
    for entity_type, status_edges in expected.items():
        candidate_status_edges = candidate_edges.get(entity_type, {})
        for from_status, to_statuses in status_edges.items():
            expected_set = set(to_statuses)
            candidate_set = set(candidate_status_edges.get(from_status, []))
            missing.extend(
                (entity_type, from_status, to_status)
                for to_status in sorted(expected_set - candidate_set)
            )
            extra.extend(
                (entity_type, from_status, to_status)
                for to_status in sorted(candidate_set - expected_set)
            )
    for entity_type, status_edges in candidate_edges.items():
        if entity_type in expected:
            continue
        for from_status, to_statuses in status_edges.items():
            extra.extend((entity_type, from_status, to_status) for to_status in to_statuses)
    return AllowedTransitionDriftReport(
        metric_name=ALLOWED_TRANSITIONS_DRIFT_METRIC,
        drift_total=len(missing) + len(extra),
        missing_edges=missing,
        extra_edges=extra,
    )


class ListAllowedTransitionsUseCase:
    """Read allowed transitions for an entity or explicit status."""

    async def execute(
        self,
        command: ListAllowedTransitionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListAllowedTransitionsResult:
        entity_type = (command.entity_type or "").strip().lower()
        _authority_for(entity_type)
        if actor.source == "mcp":
            board = await uow.services.boards.get_board(command.board_id)
        else:
            board = await uow.services.boards.get_board(
                command.board_id,
                actor.actor_id,
                query_scope=ActorScope.from_context(actor).query_scope(
                    target_board_id=command.board_id
                ),
            )
        if not board:
            raise EntityNotFoundError("board", command.board_id)

        entity_id = (command.entity_id or "").strip() or None
        if entity_id:
            entity = await self._load_entity(uow.services, entity_type, entity_id)
            if not entity or getattr(entity, "board_id", None) != command.board_id:
                raise EntityNotFoundError(entity_type, entity_id)
            current_status = str(entity.status.value)
        else:
            if not command.current_status:
                raise CommandValidationError(
                    "current_status is required when entity_id is not provided"
                )
            current_status = command.current_status
            _parse_status(entity_type, current_status)

        read_model = AllowedTransitionsReadModel(
            board_id=command.board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            current_status=current_status,
            allowed_transitions=allowed_transitions_for_status(entity_type, current_status),
        )
        return ListAllowedTransitionsResult(read_model)

    async def _load_entity(
        self,
        services: ApplicationServiceCatalog,
        entity_type: str,
        entity_id: str,
    ) -> Any:
        if entity_type == "ideation":
            return await services.ideations.get_ideation(entity_id)
        if entity_type == "refinement":
            return await services.refinements.get_refinement(entity_id)
        if entity_type == "spec":
            return await services.specs.get_spec(entity_id)
        raise CommandValidationError(f"Invalid entity_type: {entity_type}")
