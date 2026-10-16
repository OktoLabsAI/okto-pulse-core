"""Task validation cannot bypass the human semantic closeout policy."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.enums import CardStatus, CardType
from okto_pulse.core.services.main import CardService
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.resource_gate import ResourceGateService
from sqlalchemy_test_models import Card
from test_allowed_transitions_mutation_parity_regressions import USER_ID, _board, _id, _persist


@pytest.mark.asyncio
@pytest.mark.parametrize('mode,unavailable', [('advisory', False), ('blocking', False), ('blocking', True)])
async def test_successful_reviewer_result_does_not_waive_missing_reference(db_factory, monkeypatch, mode, unavailable):
    board_id, card_id = _id('validation-link-board'), _id('validation-link-card')
    await _persist(db_factory,
        _board(board_id, settings={'missing_link_gate': mode, 'skip_cognitive_consolidation': True}),
        Card(id=card_id, board_id=board_id, title='Declared missing Test Card',
             created_by=USER_ID, status=CardStatus.VALIDATION, card_type=CardType.NORMAL,
             linked_test_task_ids=['absent-test-card']))
    async with db_factory() as db:
        resources = ResourceGateService(db)
        for resource_type in ('architecture', 'mockup', 'knowledge_base'):
            await resources.mark_not_applicable(board_id, 'card', card_id, resource_type, USER_ID,
                justification='This isolated policy fixture has no applicable resource.', source_channel='ui')
        await db.commit()
        data = dict(confidence=95, confidence_justification='Current evidence reviewed',
                    estimated_completeness=100, completeness_justification='Scope reviewed',
                    estimated_drift=0, drift_justification='No deviation', recommendation='approve',
                    general_justification='Independent review completed')
        service = CardService(db)
        if unavailable:
            from okto_pulse.core.services import missing_link_gate
            monkeypatch.setattr(missing_link_gate, 'get_application_persistence_port',
                lambda: SimpleNamespace(get=AsyncMock(side_effect=RuntimeError('source unavailable'))))
            with pytest.raises(GateContractError) as caught:
                await service.submit_task_validation(card_id, 'independent-reviewer', 'Reviewer', data)
            assert caught.value.code == 'missing_link_source_unavailable'
            persisted = await db.get(Card, card_id)
            assert not persisted.validations and persisted.status == CardStatus.VALIDATION
            return
        result = await service.submit_task_validation(card_id, 'independent-reviewer', 'Reviewer', data)
        assert result['validation_outcome'] == 'success'
        codes = {entry['code'] for entry in result['completion_gate_failures']}
        assert ('missing_links_open' in codes) == (mode == 'blocking')
        if mode == 'blocking':
            assert result['card_status'] == 'rejected'
            assert result['completion_outcome'] == 'rejected'
        else:
            assert result['card_status'] == 'done'
            assert result['completion_outcome'] == 'completed'
        await db.commit()
        persisted = await db.get(Card, card_id)
        before = deepcopy(persisted.validations)
        # Diagnostics do not rewrite the sealed review or create graph work.
        from okto_pulse.core.services.missing_link_gate import evaluate_missing_links
        await evaluate_missing_links(db, subject=persisted, entity_type='card', settings={'missing_link_gate': mode})
        assert persisted.validations == before
