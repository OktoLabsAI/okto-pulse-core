"""G14 invalidation retains removed consumers and refuses cross-Board targets."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
from okto_pulse.core.events.types import SpecSemanticChanged, CardScenarioProjectionChanged
from okto_pulse.core.ports.card_projection import scenario_linked_card_ids, CARD_PROJECTION_FIELDS


def test_removed_and_added_references_survive_as_bounded_event_metadata():
    before = [{'id': 'ts', 'linked_task_ids': ['old', 'shared']}]
    after = [{'id': 'ts', 'linked_task_ids': ['new', 'shared']}]
    event = SpecSemanticChanged(board_id='board', actor_id='owner', spec_id='spec',
        changed_fields=['test_scenarios'], projection_card_ids=scenario_linked_card_ids(before, after))
    assert event.projection_card_ids == ['new', 'old', 'shared']
    assert SpecSemanticChanged.model_validate_json(event.model_dump_json()).projection_card_ids == event.projection_card_ids
    assert scenario_linked_card_ids(before, []) == ['old', 'shared']


@pytest.mark.asyncio
@pytest.mark.parametrize('field', sorted(CARD_PROJECTION_FIELDS))
async def test_spec_change_invalidates_card_side_consumers_and_old_links_with_board_fence(monkeypatch, field):
    from okto_pulse.core.ports import application_persistence
    queries = []
    async def read(context, query):
        queries.append(query)
        assert query.entity == 'card'
        filters = {(f.field, f.operator): f.value for f in query.filters}
        assert filters[('board_id', 'eq')] == 'board'
        if ('spec_id', 'eq') in filters:
            assert filters[('spec_id', 'eq')] == 'spec'
            return (SimpleNamespace(id='card-side-only'), SimpleNamespace(id='new'))
        assert set(filters[('id', 'in')]) == {'old', 'new', 'foreign'}
        return (SimpleNamespace(id='old'), SimpleNamespace(id='new'))
    monkeypatch.setattr(application_persistence, 'get_application_persistence_port',
                        lambda: SimpleNamespace(list=read))
    handler = ConsolidationEnqueuer()
    handler._enqueue_one = AsyncMock()
    event = SpecSemanticChanged(board_id='board', actor_id='owner', spec_id='spec',
        changed_fields=[field], projection_card_ids=['old', 'new', 'foreign', 'old'])
    await handler.handle(event, None)
    targets = [(call.args[1], call.args[2]) for call in handler._enqueue_one.await_args_list]
    assert targets == [('spec', 'spec'), ('card', 'old'), ('card', 'new'), ('card', 'card-side-only')]
    assert len(queries) == 2


@pytest.mark.parametrize('old,new', [('old', 'new'), ('old', None), (None, 'new'), ('same', 'same')])
def test_direct_card_mutation_invalidates_both_parents_without_duplicates(old, new):
    event = CardScenarioProjectionChanged(board_id='board', actor_id='owner', card_id='card',
        old_spec_id=old, new_spec_id=new, changed_fields=['spec_id'])
    expected = [('spec', identity) for identity in dict.fromkeys((old, new)) if identity]
    assert ConsolidationEnqueuer()._map_targets(event) == expected + [('card', 'card')]


@pytest.mark.asyncio
async def test_real_scenario_unlink_retains_removed_card_in_outbox(db_factory):
    from sqlalchemy import select
    from sqlalchemy_test_models import Board, Spec, Card, DomainEventRow
    from okto_pulse.core.services.main import SpecService
    from okto_pulse.core.ports.application_persistence import get_application_persistence_port
    async with db_factory() as db:
        db.add(Board(id='board-invalidation', name='Board', owner_id='owner'))
        db.add(Spec(id='spec-invalidation', board_id='board-invalidation', title='Spec', status='draft',
            created_by='owner', test_scenarios=[{'id': 'ts_one', 'title': 'Scenario',
                'linked_task_ids': ['old-invalidation', 'kept-invalidation']}]))
        for identity in ('old-invalidation', 'kept-invalidation'):
            db.add(Card(id=identity, board_id='board-invalidation', spec_id='spec-invalidation',
                title='Card', status='not_started', card_type='normal', created_by='owner'))
        await db.commit()
        _, changed, current = await SpecService(db).remove_scenario_traceability_task_link(
            'spec-invalidation', 'owner', scenario_id='ts_one', card_id='old-invalidation')
        assert changed and current == ['kept-invalidation']
        await get_application_persistence_port().flush(db)
        saved = (await db.execute(select(Spec.test_scenarios).where(Spec.id == 'spec-invalidation'))).scalar_one()
        assert saved[0]['linked_task_ids'] == ['kept-invalidation']
        event = (await db.execute(select(DomainEventRow).where(
            DomainEventRow.board_id == 'board-invalidation',
            DomainEventRow.event_type == 'spec.semantic_changed'))).scalar_one()
        assert event.payload_json['projection_card_ids'] == ['kept-invalidation', 'old-invalidation']


@pytest.mark.asyncio
async def test_real_structured_child_unlink_retains_last_consumer(db_factory):
    from sqlalchemy import select
    from sqlalchemy_test_models import Card, Spec, DomainEventRow
    from test_spec_structured_entities import _seed_spec, _payload_for, _permission_set
    from okto_pulse.core.services.spec_structured_entities import (
        StructuredSpecEntityService, StructuredSpecEntityCommand,
    )
    async with db_factory() as db:
        await _seed_spec(db, board_id='child-board', spec_id='child-spec', actor_id='owner')
        spec = await db.get(Spec, 'child-spec')
        spec.decisions = [{**_payload_for('decision'), 'linked_task_ids': ['child-card']}]
        db.add(Card(id='child-card', board_id='child-board', spec_id='child-spec', title='Card',
            status='not_started', card_type='normal', created_by='owner'))
        await db.commit()
        result = await StructuredSpecEntityService(db).mutate(StructuredSpecEntityCommand(
            spec_id='child-spec', actor_id='owner', entity_type='decision', operation='unlink_task',
            entity_id='dec_struct', task_id='child-card', permission_set=_permission_set('Spec')))
        assert result.success
        await db.commit()
    async with db_factory() as db:
        assert (await db.get(Spec, 'child-spec')).decisions[0]['linked_task_ids'] == []
        rows = list(await db.scalars(select(DomainEventRow).where(
            DomainEventRow.board_id == 'child-board',
            DomainEventRow.event_type.in_(['spec.semantic_changed', 'spec.version_bumped']))))
        assert len(rows) == 2
        assert all(row.payload_json['projection_card_ids'] == ['child-card'] for row in rows)


@pytest.mark.asyncio
async def test_real_card_update_emits_one_durable_invalidation_and_no_event_for_noop(db_factory):
    from sqlalchemy import select
    from sqlalchemy_test_models import Board, Spec, Card, DomainEventRow
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.models.schemas import CardUpdate
    from okto_pulse.core.ports.application_persistence import get_application_persistence_port
    async with db_factory() as db:
        db.add(Board(id='board-card-change', name='Board', owner_id='owner'))
        db.add(Spec(id='spec-card-change', board_id='board-card-change', title='Spec', status='draft',
            created_by='owner', test_scenarios=[{'id': 'ts_old'}, {'id': 'ts_new'}]))
        db.add(Card(id='card-change', board_id='board-card-change', spec_id='spec-card-change',
            title='Card', status='not_started', card_type='normal', created_by='owner', test_scenario_ids=['ts_old']))
        await db.commit()
        for _ in range(2):
            card = await CardService(db).update_card('card-change', 'owner', CardUpdate(test_scenario_ids=['ts_new']))
            assert card.test_scenario_ids == ['ts_new']
            await get_application_persistence_port().flush(db)
            saved = (await db.execute(select(Card.test_scenario_ids).where(Card.id == 'card-change'))).scalar_one()
            assert saved == ['ts_new']
        event = (await db.execute(select(DomainEventRow).where(
            DomainEventRow.board_id == 'board-card-change',
            DomainEventRow.event_type == 'card.scenario_projection_changed.v1'))).scalar_one()
        assert event.payload_json['changed_fields'] == ['test_scenario_ids']
        assert event.payload_json['old_spec_id'] == event.payload_json['new_spec_id'] == 'spec-card-change'


@pytest.mark.asyncio
async def test_real_scenario_deletion_reenqueues_both_reference_sources_after_cascade(db_factory):
    from sqlalchemy import select
    from sqlalchemy_test_models import Board, Spec, Card, DomainEventRow
    from okto_pulse.core.services.main import SpecService
    async with db_factory() as db:
        db.add(Board(id='board-scenario-delete', name='Board', owner_id='owner'))
        db.add(Spec(id='spec-scenario-delete', board_id='board-scenario-delete', title='Spec',
            status='draft', created_by='owner', test_scenarios=[{'id': 'ts_remove', 'title': 'Scenario',
                'given': 'Given', 'when': 'When', 'then': 'Then', 'linked_task_ids': ['backlink-delete']}]))
        for identity, links in [('backlink-delete', []), ('card-side-delete', ['ts_remove'])]:
            db.add(Card(id=identity, board_id='board-scenario-delete', spec_id='spec-scenario-delete',
                title='Card', status='not_started', card_type='normal', created_by='owner', test_scenario_ids=links))
        await db.commit()
        result = await SpecService(db).delete_test_scenario('spec-scenario-delete', 'owner', 'ts_remove')
        assert result['cards_unlinked'] == ['card-side-delete']
        assert (await db.execute(select(Card.test_scenario_ids).where(Card.id == 'card-side-delete'))).scalar_one() == []
        row = (await db.execute(select(DomainEventRow).where(
            DomainEventRow.board_id == 'board-scenario-delete',
            DomainEventRow.event_type == 'spec.semantic_changed'))).scalar_one()
        event = SpecSemanticChanged(board_id=row.board_id, actor_id=row.actor_id, **row.payload_json)
        assert event.projection_card_ids == ['backlink-delete']
        handler = ConsolidationEnqueuer()
        handler._enqueue_one = AsyncMock()
        await handler.handle(event, db)
        targets = [(call.args[1], call.args[2]) for call in handler._enqueue_one.await_args_list]
        assert set(targets) == {('spec', 'spec-scenario-delete'), ('card', 'backlink-delete'), ('card', 'card-side-delete')}
        assert len(targets) == 3
