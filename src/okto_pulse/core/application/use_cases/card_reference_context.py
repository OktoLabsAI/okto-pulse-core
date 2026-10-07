"""Re-evaluate authorized relational sources; audit history is never currentness."""
import logging
import json

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import PermissionDeniedError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.card_scenario_references import analyze_card_scenario_references
from okto_pulse.core.models.reference_context import CardScenarioReferenceContext, ScenarioReferenceFindingView
from okto_pulse.core.ports.application_persistence import ApplicationFilter, ApplicationQuery

_LOG = logging.getLogger(__name__)
CARD_REFERENCE_CONTEXT_PERMISSIONS = (
    'board.read', 'card.entity.read', 'card.entity.context_read', 'card.tests.read',
    'spec.entity.read', 'spec.tests.read',
)


class GetCardScenarioReferenceContextUseCase:
    """Optional context block, not a gate or a graph maintenance operation.

    All source reads occur after the complete permission check in the caller's
    UOW. Provider errors and unreadable/foreign scope never mean zero findings.
    The fingerprint describes this read, not a promise about a later mutation.
    """
    async def execute(self, *, board_id: str, card_id: str, actor, uow: PulseUnitOfWork):
        try:
            await require_all(actor, *(PermissionRequirement(flag) for flag in CARD_REFERENCE_CONTEXT_PERMISSIONS),
                uow=uow, board_id=board_id)
            if await load_accessible_board(uow, board_id, actor) is None:
                return CardScenarioReferenceContext(status='unavailable')
            rows = await uow.services.list_application_records(ApplicationQuery(entity='card',
                filters=(ApplicationFilter('id', 'eq', card_id), ApplicationFilter('board_id', 'eq', board_id)),
                limit=1, select_fields=('id', 'board_id', 'spec_id', 'test_scenario_ids')))
            card = rows[0] if rows else None
            if (card is None or getattr(card, 'id', None) != card_id or getattr(card, 'board_id', None) != board_id
                    or not hasattr(card, 'test_scenario_ids')):
                return CardScenarioReferenceContext(status='unavailable')
            spec_id = getattr(card, 'spec_id', None)
            parent = None
            if spec_id:
                # Identity/scope first; never load a foreign Spec's scenarios.
                metadata = await uow.services.list_application_records(ApplicationQuery(entity='spec',
                    filters=(ApplicationFilter('id', 'eq', spec_id),), limit=1, select_fields=('id', 'board_id')))
                if metadata:
                    if getattr(metadata[0], 'id', None) != spec_id or getattr(metadata[0], 'board_id', None) != board_id:
                        return CardScenarioReferenceContext(status='unavailable')
                    parents = await uow.services.list_application_records(ApplicationQuery(entity='spec',
                        filters=(ApplicationFilter('id', 'eq', spec_id), ApplicationFilter('board_id', 'eq', board_id)),
                        limit=1, select_fields=('id', 'board_id', 'test_scenarios')))
                    if not parents:
                        return CardScenarioReferenceContext(status='unavailable')
                    parent = parents[0]
            if parent is not None and (getattr(parent, 'id', None) != spec_id
                    or getattr(parent, 'board_id', None) != board_id or not hasattr(parent, 'test_scenarios')):
                return CardScenarioReferenceContext(status='unavailable')
            snapshot = analyze_card_scenario_references(board_id=board_id, card_id=card_id,
                spec_id=spec_id, card_links=card.test_scenario_ids, parent_exists=parent is not None,
                scenarios=(parent.test_scenarios if parent.test_scenarios is not None else [])
                    if parent is not None else None).snapshot
            items = []
            for item in snapshot.findings[:20]:
                view = ScenarioReferenceFindingView(finding_id=item.finding_id,
                source_selector=item.source_selector, target_ref=item.target_ref, reason_code=item.reason_code,
                correction_surface=('card_and_spec_scenario_links' if item.reason_code == 'source_disagreement'
                                    else 'spec_test_scenarios' if item.reason_code == 'target_ambiguous'
                                    or item.source_selector.startswith('spec:') else 'card_scenario_links'))
                # Omit a whole entry rather than corrupt an exact source/target
                # identity under MCP's response budget. Count remains complete.
                if len(json.dumps([value.model_dump() for value in [*items, view]],
                                  ensure_ascii=False).encode('utf-8')) > 7000:
                    break
                items.append(view)
            return CardScenarioReferenceContext(status='available', source_fingerprint=snapshot.source_fingerprint,
                finding_count=len(snapshot.findings), findings=items, truncated=len(snapshot.findings) > len(items))
        except PermissionDeniedError:
            return CardScenarioReferenceContext(status='not_authorized')
        except Exception as exc:
            # Optional diagnostics must not hide an otherwise readable Card;
            # never serialize provider text, IDs from foreign scope, or a zero.
            _LOG.warning('card_reference_context_unavailable error_class=%s', type(exc).__name__)
            return CardScenarioReferenceContext(status='unavailable')
