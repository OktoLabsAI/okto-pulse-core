"""BASE T19/T20: create owns both directions of scenario traceability."""
from copy import deepcopy
from uuid import uuid4

import pytest
from sqlalchemy import select

from sqlalchemy_test_models import ActivityLog, Board, Card, DomainEventRow, Spec, SpecStatus
from okto_pulse.core.models.schemas import CardCreate
from okto_pulse.core.services.main import CardService

USER_ID = 'atomic-creation-owner'


async def seed(db_factory):
    board_id, spec_id = str(uuid4()), str(uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name='Atomic creation', owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title='Creation Spec', created_by=USER_ID,
                    status=SpecStatus.APPROVED, test_scenarios=[
                        {'id': item, 'title': item, 'status': 'draft', 'linked_task_ids': []}
                        for item in ('ts-001', 'ts-002')]))
        await db.commit()
    return board_id, spec_id


@pytest.mark.asyncio
@pytest.mark.parametrize('abort', [False, True])
async def test_create_and_scenario_links_commit_or_rollback_together(db_factory, abort):
    board_id, spec_id = await seed(db_factory)
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        before = deepcopy(spec.test_scenarios)
        events = len((await db.execute(select(DomainEventRow))).scalars().all())
        activities = len((await db.execute(select(ActivityLog))).scalars().all())
        card = await CardService(db).create_card(board_id, USER_ID, CardCreate(
            title='Atomic Test Card', spec_id=spec_id, card_type='test',
            test_scenario_ids=['ts-001', 'ts-002']))
        card_id = card.id
        if abort:
            await db.rollback()
        else:
            await db.commit()
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        card = await db.get(Card, card_id)
        if abort:
            assert card is None
            assert spec.test_scenarios == before
            assert len((await db.execute(select(DomainEventRow))).scalars().all()) == events
            assert len((await db.execute(select(ActivityLog))).scalars().all()) == activities
        else:
            assert card.test_scenario_ids == ['ts-001', 'ts-002']
            assert all(scenario['linked_task_ids'] == [card_id] for scenario in spec.test_scenarios)


@pytest.mark.asyncio
@pytest.mark.parametrize('other_board', [False, True])
async def test_foreign_scenario_refuses_whole_create_even_with_valid_first_id(db_factory, other_board):
    board_id, spec_id = await seed(db_factory)
    foreign_board, foreign_spec = str(uuid4()), str(uuid4())
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        before = deepcopy(spec.test_scenarios)
        if other_board:
            db.add(Board(id=foreign_board, name='Other', owner_id='other-owner'))
        db.add(Spec(id=foreign_spec, board_id=foreign_board if other_board else board_id,
                    title='Other Spec', created_by=USER_ID,
                    test_scenarios=[{'id': 'foreign-scenario', 'title': 'Other', 'linked_task_ids': []}]))
        await db.commit()
        count = len((await db.execute(select(Card))).scalars().all())
        with pytest.raises(ValueError, match='not found in spec'):
            await CardService(db).create_card(board_id, USER_ID, CardCreate(
                title='Refused Test Card', spec_id=spec_id, card_type='test',
                test_scenario_ids=['ts-001', 'foreign-scenario']))
        # Commit after denial detects writes that a rollback-only assertion hides.
        await db.commit()
    async with db_factory() as db:
        assert len((await db.execute(select(Card))).scalars().all()) == count
        assert (await db.get(Spec, spec_id)).test_scenarios == before
        assert (await db.get(Spec, foreign_spec)).test_scenarios[0]['linked_task_ids'] == []
