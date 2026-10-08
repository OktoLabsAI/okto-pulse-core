"""BASE T23: a persisted confirmation must not stand in for current evidence."""
import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.services.main import SpecService, CardService, CardOperationError
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.domain.enums import CardStatus
from test_path_b_e2e import (
    USER_ID, _prepare_cross_spec_regression, _coverage_state,
)


@pytest.mark.asyncio
async def test_operational_evidence_reset_invalidates_path_b_readiness():
    async with get_session_factory()() as db:
        ids = await _prepare_cross_spec_regression(db, confirmed=True)
        from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
        from okto_pulse.core.services.amendment_coverage import current_amendment_facts
        rows = await AmendmentRevisionService(db).list_for_bug(
            board_id=ids['board'], original_spec_id=ids['spec'], origin_bug_id=ids['bug'])
        fact = (await current_amendment_facts(db, rows))[0]
        assert fact.coverage_confirmation.basis is not None, rows[0].validation_metadata
        assert fact.coverage_confirmation.basis == fact.current_coverage_basis
        assert await _coverage_state(db, ids) == 'path_b_ready'
        result = await SpecService(db).set_test_scenario_status(
            ids['other_spec'], USER_ID, ids['foreign_scenario'], 'ready')
        assert result['old_status'] == 'automated'
        assert result['new_status'] == 'ready'
        preview = await _coverage_state(db, ids)
        blocked = False
        try:
            moved = await CardService(db).move_card(
                ids['bug'], USER_ID, CardMove(status=CardStatus.IN_PROGRESS))
            assert moved.status == CardStatus.IN_PROGRESS
        except CardOperationError as error:
            blocked = True
            assert error.code
        assert (preview, blocked) == ('coverage_pending', True)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["evidence", "semantic", "edition", "test_reopened"])
async def test_confirmation_is_bound_to_current_source(mutation):
    from copy import deepcopy
    from sqlalchemy.orm.attributes import flag_modified
    from sqlalchemy_test_models import Spec, Card
    from okto_pulse.core.services.amendment_revision import AmendmentRevisionService

    async with get_session_factory()() as db:
        ids = await _prepare_cross_spec_regression(db, confirmed=True)
        service = AmendmentRevisionService(db)
        rows = await service.list_for_bug(board_id=ids['board'],
            original_spec_id=ids['spec'], origin_bug_id=ids['bug'])
        old = deepcopy(rows[0].validation_metadata)
        source = await db.get(Spec, ids['other_spec'])
        if mutation in {'evidence', 'semantic'}:
            source.test_scenarios = deepcopy(source.test_scenarios)
            scenario = source.test_scenarios[0]
            if mutation == 'evidence':
                scenario['evidence']['test_function'] = 'test_new_observation'
            else:
                scenario['then'] = 'a different expected behavior'
            flag_modified(source, 'test_scenarios')
        elif mutation == 'edition':
            source.edition += 1
        else:
            task = await db.get(Card, ids['test'])
            task.status = CardStatus.IN_PROGRESS
        await db.flush()
        assert await _coverage_state(db, ids) == 'coverage_pending'
        assert (await service.get(rows[0].id)).validation_metadata == old
        with pytest.raises(CardOperationError):
            await CardService(db).move_card(
                ids['bug'], USER_ID, CardMove(status=CardStatus.IN_PROGRESS))
