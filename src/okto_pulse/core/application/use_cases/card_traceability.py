"""Transactional card traceability orchestration shared by create/link_task."""

from __future__ import annotations

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.card_traceability import (
    TraceabilityLinkResult,
    TraceabilityScenarioLimitError,
    link_card_traceability,
)
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)


_CARD_TRACEABILITY_WRITE_PERMISSIONS = {"editor", "admin"}


class LinkCardTraceabilityCommand:
    __slots__ = ("spec_id", "card_id", "targets")

    def __init__(
        self,
        spec_id: str,
        card_id: str,
        targets: list[tuple[str, str]],
    ) -> None:
        self.spec_id = spec_id
        self.card_id = card_id
        self.targets = targets


class LinkCardTraceabilityResult:
    __slots__ = ("link",)

    def __init__(self, link: TraceabilityLinkResult) -> None:
        self.link = link


class LinkCardTraceabilityUseCase:
    """Validate all targets then mutate backlinks/activity in one UoW commit."""

    async def execute(
        self,
        command: LinkCardTraceabilityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> LinkCardTraceabilityResult:
        spec = await uow.services.specs.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        board = await load_accessible_board(
            uow,
            spec.board_id,
            actor,
            allowed_share_permissions=_CARD_TRACEABILITY_WRITE_PERMISSIONS,
        )
        if board is None:
            raise EntityNotFoundError("spec", command.spec_id)
        card = await uow.services.cards.get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        if card.board_id != spec.board_id:
            raise EntityNotFoundError("card", command.card_id)
        require_card_operational_mutation_allowed(
            card,
            operation="link_card_traceability",
        )

        max_scenarios_per_card: int | None = None
        card_type = getattr(getattr(card, "card_type", None), "value", None)
        card_type = card_type or getattr(card, "card_type", None)
        if card_type == "test":
            max_scenarios_per_card = uow.services.cards._max_scenarios_per_card(board)
        try:
            link = link_card_traceability(
                spec=spec,
                card=card,
                targets=command.targets,
                max_scenarios_per_card=max_scenarios_per_card,
            )
        except TraceabilityScenarioLimitError as exc:
            uow.services.cards._raise_max_scenarios_per_card_exceeded(
                provided_count=exc.provided_count,
                max_per_card=exc.max_scenarios_per_card,
            )
            raise AssertionError("Card scenario-limit policy did not raise") from exc
        await uow.services.boards._log_activity(
            board_id=spec.board_id,
            card_id=card.id,
            action="card_traceability_linked",
            actor_type=actor.source,
            actor_id=actor.actor_id,
            actor_name=actor.actor_name or actor.actor_id,
            details={"spec_id": spec.id, **link.to_dict()},
        )
        await uow.synchronize()
        await commit(uow)
        return LinkCardTraceabilityResult(link)


__all__ = [
    "LinkCardTraceabilityCommand",
    "LinkCardTraceabilityResult",
    "LinkCardTraceabilityUseCase",
]
