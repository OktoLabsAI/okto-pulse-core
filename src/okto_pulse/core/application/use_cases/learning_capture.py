"""Authorized staging of a new Learning capture in the caller's UOW."""

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.board_access import load_accessible_card
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, commit
from okto_pulse.core.ports.learning_capture import CreateLearningCapture

LEARNING_CAPTURE_READ_PERMISSIONS = (
    'board.read', 'card.entity.read', 'card.entity.context_read', 'card.validation.read',
    'card.comments.read', 'card.conclusion.read', 'card.tests.read', 'spec.entity.read', 'spec.tests.read',
)
LEARNING_CAPTURE_CREATE_PERMISSIONS = (
    *LEARNING_CAPTURE_READ_PERMISSIONS,
    'kg.session.begin', 'kg.session.add_node', 'kg.session.add_edge', 'kg.session.commit',
)
LEARNING_CAPTURE_HISTORY_PERMISSIONS = (*LEARNING_CAPTURE_READ_PERMISSIONS, 'kg.query.learning_from_bugs')


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


class CreateLearningCaptureUseCase:
    """Standalone submission; the staged variant remains available for Done."""
    async def execute(self, command: CreateLearningCapture, *, actor, uow):
        record = await StageLearningCaptureUseCase().execute(command, actor=actor, uow=uow)
        await commit(uow)
        return record


class GetLearningCaptureSourceUseCase:
    async def execute(self, *, board_id: str, bug_id: str, actor, uow):
        await require_all(actor, *(PermissionRequirement(flag) for flag in LEARNING_CAPTURE_READ_PERMISSIONS),
            uow=uow, board_id=board_id)
        card = await load_accessible_card(uow, bug_id, actor, expected_board_id=board_id)
        if card is None:
            raise EntityNotFoundError('card', bug_id)
        return await uow.services.kg.get_learning_capture_source(board_id=board_id, bug_id=bug_id)


class ListLearningCapturesUseCase:
    async def execute(self, *, board_id: str, bug_id: str, actor, uow, cursor=None, limit=20):
        await require_all(actor, *(PermissionRequirement(flag) for flag in LEARNING_CAPTURE_HISTORY_PERMISSIONS),
            uow=uow, board_id=board_id)
        card = await load_accessible_card(uow, bug_id, actor, expected_board_id=board_id)
        if card is None:
            raise EntityNotFoundError('card', bug_id)
        return await uow.services.kg.list_learning_captures(board_id=board_id, bug_id=bug_id,
            cursor=cursor, limit=limit)
