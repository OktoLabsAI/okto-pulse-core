"""Authorized staging of a new Learning capture in the caller's UOW."""

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.board_access import load_accessible_card
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.ports.learning_capture import CreateLearningCapture

LEARNING_CAPTURE_CREATE_PERMISSIONS = (
    'board.read', 'card.entity.read', 'card.entity.context_read', 'card.validation.read',
    'card.comments.read', 'card.conclusion.read', 'card.tests.read', 'spec.entity.read', 'spec.tests.read',
    'kg.session.begin', 'kg.session.add_node', 'kg.session.add_edge', 'kg.session.commit',
)


class StageLearningCaptureUseCase:
    async def execute(self, command: CreateLearningCapture, *, actor, uow):
        if type(command) is not CreateLearningCapture:
            raise ValueError('learning_capture_request_invalid')
        await require_all(actor, *(PermissionRequirement(flag) for flag in LEARNING_CAPTURE_CREATE_PERMISSIONS),
            uow=uow, board_id=command.board_id)
        card = await load_accessible_card(uow, command.bug_id, actor, expected_board_id=command.board_id)
        if card is None:
            raise EntityNotFoundError('card', command.bug_id)
        # No commit: this operation can share the conclusion/Done transaction.
        # Caller identity comes from the authenticated actor, never the payload.
        return await uow.services.kg.stage_new_learning_capture(command, author_id=actor.actor_id)
