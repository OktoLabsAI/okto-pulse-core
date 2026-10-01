"""KG G3: persisted Card dependencies must preserve endpoint types and direction."""
import itertools
import json

import pytest

from sqlalchemy_test_models import Board, Card, Spec
from okto_pulse.core.services.main import CardService
from okto_pulse.core.ports.deterministic_projection import (
    DeterministicProjectionSource, make_deterministic_projection_planner,
)
from okto_pulse.community.adapters.sqlalchemy_consolidation import CommunitySqlAlchemyConsolidationPersistence


async def seed_pair(db, prerequisite_type, dependent_type, prefix):
    db.add(Board(id=prefix, name='Dependencies', owner_id='owner'))
    db.add(Spec(id=prefix + '-spec', board_id=prefix, title='Spec', status='draft', created_by='owner'))
    for role, kind in [('pre', prerequisite_type), ('dep', dependent_type)]:
        db.add(Card(id=prefix + '-' + role, board_id=prefix, spec_id=prefix + '-spec',
            title=role, description='Explicit dependency', status='in_progress',
            card_type=kind, created_by='owner', test_scenario_ids=[]))
    await db.commit()


@pytest.mark.asyncio
async def test_domain_accepts_all_typed_card_dependency_pairs(db_factory):
    async with db_factory() as db:
        service = CardService(db)
        for index, (source, target) in enumerate(itertools.product(('normal', 'test', 'bug'), repeat=2)):
            prefix = 'dependency-kind-' + str(index)
            await seed_pair(db, source, target, prefix)
            row = await service.add_dependency(prefix + '-dep', prefix + '-pre')
            await db.commit()
            assert row.card_id == prefix + '-dep' and row.depends_on_id == prefix + '-pre'
            assert [card.card_type for card in await service.get_dependencies(prefix + '-dep')] == [source]


@pytest.mark.asyncio
@pytest.mark.parametrize('prerequisite_type,dependent_type', itertools.product(('normal', 'test', 'bug'), repeat=2))
async def test_persisted_dependency_projects_prerequisite_to_dependent(db_factory, prerequisite_type, dependent_type):
    prefix = f'dependency-projection-{prerequisite_type}-{dependent_type}'
    async with db_factory() as db:
        await seed_pair(db, prerequisite_type, dependent_type, prefix)
        row = await CardService(db).add_dependency(prefix + '-dep', prefix + '-pre')
        identity = row.id
        for role in ('pre', 'dep'):
            card = await db.get(Card, prefix + '-' + role)
            card.status = 'done'
            card.conclusions = [{'summary': 'Completed'}]
        await db.commit()
        planner = make_deterministic_projection_planner(CommunitySqlAlchemyConsolidationPersistence())
        document = await planner.prepare(db,
            DeterministicProjectionSource(prefix, 'card', prefix + '-dep'))
        projection = json.loads(document.document)['projection']
        edges = [edge for edge in projection['edges'] if edge['edge_type'] == 'precedes']
        assert len(edges) == 1
        edge = edges[0]
        assert edge['from_candidate_id'] == (
            f'kgref:{"Bug" if prerequisite_type == "bug" else "Entity"}:card:{prefix}-pre')
        root = next(node for node in projection['nodes'] if node['candidate_id'] == edge['to_candidate_id'])
        assert root['source_artifact_ref'] == f'card:{prefix}-dep'
        assert root['node_type'] == ('Bug' if dependent_type == 'bug' else 'Entity')
        assert identity in edge['rule_id']


@pytest.mark.asyncio
async def test_dependency_outbox_retains_removal_and_cycle_is_not_persisted(db_factory):
    from sqlalchemy import select
    from sqlalchemy_test_models import DomainEventRow
    from okto_pulse.core.services.main import CardOperationError
    from okto_pulse.core.events.types import CardDependencyChanged, resolve_event_class
    from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
    async with db_factory() as db:
        await seed_pair(db, 'bug', 'normal', 'dependency-events')
        service = CardService(db)
        first = await service.add_dependency('dependency-events-dep', 'dependency-events-pre', actor_id='owner')
        duplicate = await service.add_dependency('dependency-events-dep', 'dependency-events-pre', actor_id='owner')
        assert duplicate.id == first.id
        with pytest.raises(CardOperationError) as caught:
            await service.add_dependency('dependency-events-pre', 'dependency-events-dep', actor_id='owner')
        assert caught.value.code == 'dependency_cycle_detected'
        assert await service.remove_dependency('dependency-events-dep', 'dependency-events-pre', actor_id='owner')
        await db.commit()
    async with db_factory() as db:
        rows = list(await db.scalars(select(DomainEventRow).where(DomainEventRow.board_id == 'dependency-events',
            DomainEventRow.event_type == CardDependencyChanged.event_type).order_by(DomainEventRow.occurred_at)))
        assert [row.payload_json['operation'] for row in rows] == ['added', 'removed']
        assert {row.payload_json['dependency_id'] for row in rows} == {first.id}
        for row in rows:
            event = resolve_event_class(row.event_type)(board_id=row.board_id, actor_id=row.actor_id, **row.payload_json)
            assert ConsolidationEnqueuer()._map_targets(event) == [
                ('card', 'dependency-events-pre'), ('card', 'dependency-events-dep')]
        assert await CardService(db).get_dependencies('dependency-events-pre') == []
        assert await CardService(db).get_dependencies('dependency-events-dep') == []


@pytest.mark.asyncio
async def test_inflight_dependency_rechecks_cycle_after_another_writer(db_factory, monkeypatch):
    from okto_pulse.core.services.main import CardOperationError
    async with db_factory() as seed:
        await seed_pair(seed, 'normal', 'bug', 'dependency-race')
    original = CardService.get_card
    changed = False
    async with db_factory() as first:
        async def interleave(self, identity, *args, **kwargs):
            nonlocal changed
            card = await original(self, identity, *args, **kwargs)
            if self.db is first and not changed:
                changed = True
                async with db_factory() as second:
                    await CardService(second).add_dependency('dependency-race-pre', 'dependency-race-dep', actor_id='owner')
                    await second.commit()
            return card
        monkeypatch.setattr(CardService, 'get_card', interleave)
        with pytest.raises(CardOperationError) as caught:
            await CardService(first).add_dependency('dependency-race-dep', 'dependency-race-pre', actor_id='owner')
        assert caught.value.code == 'dependency_cycle_detected'
        await first.commit()
    async with db_factory() as db:
        assert changed
        assert await CardService(db).get_dependencies('dependency-race-dep') == []
        assert [card.id for card in await CardService(db).get_dependencies('dependency-race-pre')] == ['dependency-race-dep']


@pytest.mark.asyncio
async def test_cross_board_dependency_is_refused_even_when_actor_owns_both(db_factory):
    from okto_pulse.core.application.use_cases.card_crud import AddCardDependencyUseCase, AddCardDependencyCommand
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    async with db_factory() as db:
        await seed_pair(db, 'normal', 'bug', 'dependency-local')
        await seed_pair(db, 'normal', 'bug', 'dependency-foreign')
    actor = ActorContext('owner', 'rest', board_id='dependency-local')
    factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    async with factory(actor=actor) as uow:
        with pytest.raises(EntityNotFoundError):
            await AddCardDependencyUseCase().execute(AddCardDependencyCommand(
                'dependency-local-dep', 'dependency-foreign-pre'), actor=actor, uow=uow)
    async with db_factory() as db:
        assert await CardService(db).get_dependencies('dependency-local-dep') == []


@pytest.mark.asyncio
async def test_prerequisite_change_enqueues_only_scoped_direct_consumers(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from okto_pulse.core.ports import application_persistence
    from okto_pulse.core.events.types import CardDependencyChanged
    from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
    queries = []
    async def read(context, query):
        queries.append(query)
        fields = {item.field: item.value for item in query.filters}
        if query.entity == 'card_dependency':
            assert fields == {'depends_on_id': 'changed'}
            return (SimpleNamespace(card_id='consumer'), SimpleNamespace(card_id='foreign'))
        assert query.entity == 'card' and fields == {'board_id':'board', 'id':('consumer', 'foreign')}
        return (SimpleNamespace(id='consumer'),)
    monkeypatch.setattr(application_persistence, 'get_application_persistence_port',
        lambda: SimpleNamespace(list=read))
    handler = ConsolidationEnqueuer()
    handler._enqueue_one = AsyncMock()
    await handler.handle(CardDependencyChanged(board_id='board', actor_id='owner', card_id='changed',
        prerequisite_card_id='previous', dependency_id='removed', operation='removed'), None)
    assert [(call.args[1], call.args[2]) for call in handler._enqueue_one.await_args_list] == [
        ('card', 'previous'), ('card', 'changed'), ('card', 'consumer')]
    assert len(queries) == 2
