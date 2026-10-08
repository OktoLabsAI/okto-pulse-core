"""Optional semantic diagnostics behind the permissions of every source read."""
from copy import copy
import logging

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import PermissionDeniedError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.models.reference_context import MissingLinkContext
from okto_pulse.core.services.missing_link_gate import evaluate_missing_links


MISSING_LINK_CONTEXT_PERMISSIONS = {
    'card': ('board.read', 'card.entity.read', 'card.entity.context_read', 'card.tests.read',
             'spec.entity.read', 'spec.tests.read'),
    'spec': ('board.read', 'spec.entity.read', 'spec.tests.read', 'spec.rules.read',
             'spec.contracts.read', 'spec.integration_requirements.read',
             'spec.observability_requirements.read', 'card.entity.read'),
}


class GetMissingLinkContextUseCase:
    async def execute(self, *, board_id, entity_type, entity_id, actor, uow):
        try:
            flags = MISSING_LINK_CONTEXT_PERMISSIONS[entity_type]
            await require_all(actor, *(PermissionRequirement(flag) for flag in flags),
                              uow=uow, board_id=board_id)
            board = await load_accessible_board(uow, board_id, actor)
            if board is None:
                return MissingLinkContext(status='unavailable')
            subject = await uow.services.get_application_record(entity=entity_type, record_id=entity_id, includes=())
            if subject is None or subject.board_id != board_id:
                return MissingLinkContext(status='unavailable')
            service = uow.services.cards if entity_type == 'card' else uow.services.specs
            result = await evaluate_missing_links(service.db, subject=subject,
                entity_type=entity_type, settings=board.settings)
            return MissingLinkContext.model_validate(result.to_payload())
        except PermissionDeniedError:
            return MissingLinkContext(status='not_authorized')
        except Exception as exc:
            logging.getLogger(__name__).warning('missing_link_context_unavailable error_class=%s', type(exc).__name__)
            return MissingLinkContext(status='unavailable')


async def attach_missing_link_context(entity, *, entity_type, actor, uow):
    diagnostic = await GetMissingLinkContextUseCase().execute(
        board_id=entity.board_id, entity_type=entity_type, entity_id=entity.id, actor=actor, uow=uow)
    attach = getattr(entity, 'attach', None)
    if callable(attach):
        attach('missing_link_context', diagnostic)
        return entity
    projected = copy(entity)
    setattr(projected, 'missing_link_context', diagnostic)
    return projected
