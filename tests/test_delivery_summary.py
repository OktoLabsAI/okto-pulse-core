from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_readiness import DeliveryReadinessContext
from okto_pulse.core.models.delivery_summary import (
    DeliverySummaryCard, DeliverySummaryCardFacts, DeliverySummaryProjection,
    DeliverySummaryScope, TaskRequirementSummary,
)
from okto_pulse.core.services.delivery_summary import (
    build_delivery_summary_population, page_delivery_summary,
)
from test_delivery_readiness_contracts import metric

NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)
SCOPE = DeliverySummaryScope(board_id='board', spec_id='spec', spec_edition=2, spec_version=7)
PROJECTION = DeliverySummaryProjection(generation_id='generation', snapshot_id='a' * 64, freshness='current')


def card(identity='a', status='done', **changes):
    return DeliverySummaryCard(board_id='board', spec_id='spec', card_id=identity,
        card_version=1, card_type='normal', status=status, title='Work ' + identity,
        assignee_id='owner', updated_at=NOW).model_copy(update=changes)


def context(identity='a', status='done', *, implemented=True, verified=True, kind='normal'):
    implementation = metric(satisfied=int(implemented), missing=int(not implemented))
    verification = metric(satisfied=int(verified), missing=int(not verified))
    if kind == 'test':
        implementation = metric(satisfied=0, missing=0, not_applicable=1, status='not_applicable')
        implementation['unit'] = verification['unit'] = 'scenario'
    proof = implemented and verified
    return DeliveryReadinessContext.model_validate(dict(
        scope=dict(board_id='board', spec_id='spec', spec_edition=2, spec_version=7,
            card_id=identity, card_type=kind if identity else None, card_version=1 if identity else None),
        snapshot_id='a' * 64, evaluated_at=NOW, scope_complete=True, evaluation_complete=True,
        implementation=implementation, verification=verification,
        delivery_state=('delivered' if status == 'done' else 'ready') if proof else
            'implementation_pending' if not implemented else 'verification_pending',
        proof_complete=proof, delivery_gate=dict(status='allowed' if proof else 'blocked',
            mode='blocking', exception_applied=False, exception_refs=[]),
        lifecycle=dict(stored_status=status, check_status='not_evaluated', other_blocker_count=None,
            qualification_is_current=proof if status == 'done' else None),
        summary=dict(message_code='native', message='Canonical fixture', unresolved_units=int(not proof),
            root_causes=0, lifecycle_pending=None, exceptions=0)))


def facts(identity='a', status='done', decision='required', *, current=True, **changes):
    return DeliverySummaryCardFacts(context=context(identity, status, **changes),
        requirement=TaskRequirementSummary(decision=decision, current=current,
            decision_id=None if decision == 'required' else 'decision-' + identity))


def population(cards=None, rows=None, **changes):
    return build_delivery_summary_population(**(dict(scope=SCOPE, projection=PROJECTION,
        cards=[card()] if cards is None else cards, facts=[facts()] if rows is None else rows,
        spec_context=context(None, 'in_progress'), population_complete=True) | changes))


def test_done_needs_native_current_qualified_proof_and_live_metadata_is_preserved():
    result = page_delivery_summary(population())
    delivered = result.groups[0]
    assert delivered.count == result.task_count == result.required_task_count == 1
    assert delivered.items[0].assignee_id == 'owner' and delivered.items[0].updated_at == NOW
    assert result.unique_obligation_count == 1 and result.complete
    assert result.mutation_reauthorization_required is True


@pytest.mark.parametrize(('status', 'group', 'reason'), [
    ('in_progress', 'in_progress', 'in_progress'), ('validation', 'pending', 'awaiting_validation'),
    ('not_started', 'pending', 'not_started'), ('on_hold', 'pending', 'paused'),
    ('rejected', 'pending', 'needs_rework'), ('cancelled', 'pending', 'cancelled'),
    ('started', 'pending', 'unavailable'),
])
def test_lifecycle_does_not_turn_one_hundred_percent_into_delivered(status, group, reason):
    value = population([card(status=status)], [facts(status=status)])
    assert value.items[0].group == group and value.items[0].reason == reason
    assert value.items[0].historical_done is False


def test_pending_dependency_is_a_typed_current_fact_not_error_prose():
    value = population([card(status='not_started', dependency_blocked=True)], [facts(status='not_started')])
    assert value.items[0].reason == 'awaiting_dependency'


@pytest.mark.parametrize('damage', ['missing', 'stale', 'unknown', 'qualification', 'contradictory_flag'])
def test_historical_done_with_insufficient_or_unknown_proof_stays_pending(damage):
    row = facts()
    if damage == 'missing':
        row = facts(implemented=False)
    elif damage == 'stale':
        row = facts(verified=False)
    elif damage == 'unknown':
        row = row.model_copy(update={'context': row.context.model_copy(update={'evaluation_complete': False})})
    elif damage == 'qualification':
        row = row.model_copy(update={'context': row.context.model_copy(update={
            'lifecycle': row.context.lifecycle.model_copy(update={'qualification_is_current': None})})})
    else:
        missing = facts(implemented=False)
        row = missing.model_copy(update={'context': missing.context.model_copy(update={'proof_complete': True})})
    before = row.model_dump(mode='json')
    value = population([card()], [row])
    assert value.items[0].group == 'pending' and value.items[0].historical_done
    assert value.items[0].reason == 'awaiting_confirmation'
    assert row.model_dump(mode='json') == before  # Presentation never reopens or rewrites Done.


@pytest.mark.parametrize('decision', ['optional', 'waived'])
def test_current_task_exception_has_its_own_collapsed_group_and_no_delivery_count(decision):
    value = population([card()], [facts(decision=decision)])
    result = page_delivery_summary(value)
    assert result.task_count == 1 and result.required_task_count == 0
    assert result.groups[0].count == 0 and result.groups[3].count == 1
    assert not result.groups[3].loaded and not result.groups[3].items
    expanded = page_delivery_summary(value, group='optional_or_waived')
    assert expanded.groups[3].items[0].requirement == decision
    assert expanded.groups[3].items[0].status == 'done'  # An exception is not a lifecycle mutation.


def test_stale_task_exception_does_not_excuse_a_changed_scope():
    value = population([card()], [facts(decision='waived', current=False)])
    assert value.items[0].requirement == 'unknown' and value.items[0].group == 'pending'
    result = page_delivery_summary(value)
    assert not result.complete and all(group.count is None for group in result.groups)


@pytest.mark.parametrize('decision', ['optional', 'waived'])
@pytest.mark.parametrize('damage', ['unknown_proof', 'stale_decision', 'card_version'])
def test_exception_counts_need_current_decision_and_metadata_but_not_delivery_proof(decision, damage):
    row = facts(decision=decision)
    row = row.model_copy(update={'context': row.context.model_copy(update={
        'scope_complete': False, 'proof_complete': False,
        'delivery_gate': row.context.delivery_gate.model_copy(update={'status': 'blocked'}),
    })})
    if damage == 'stale_decision':
        row = row.model_copy(update={'requirement': row.requirement.model_copy(update={'current': False})})
    live_card = card(card_version=2) if damage == 'card_version' else card()
    before = row.model_dump(mode='json')
    result = page_delivery_summary(population([live_card], [row]))
    if damage == 'unknown_proof':
        assert result.complete and result.task_count == 1 and result.required_task_count == 0
        assert result.groups[0].count == 0 and result.groups[3].count == 1
    else:
        assert not result.complete and result.task_count is None
        assert result.groups[2].items[0].requirement == 'unknown'
    assert not result.groups[0].items
    assert row.model_dump(mode='json') == before


def test_shared_obligation_denominator_is_never_a_sum_of_card_metrics():
    result = page_delivery_summary(population([card('a'), card('b')], [facts('a'), facts('b')]))
    assert result.groups[0].count == result.task_count == 2
    assert result.unique_obligation_count == 1  # The global canonical scope owns this count.


def test_a_completed_test_card_uses_na_implementation_and_current_verification():
    value = population([card(card_type='test')], [facts(kind='test')])
    assert value.items[0].group == 'delivered'
    failed = population([card(card_type='test')], [facts(kind='test', verified=False)])
    assert failed.items[0].group == 'pending'


def test_legacy_completion_with_exception_remains_pending_confirmation_without_proof_credit():
    row = facts(implemented=False)
    changed = row.context.model_copy(update={
        'delivery_state': 'delivered_with_exceptions',
        'delivery_gate': row.context.delivery_gate.model_copy(update={
            'status': 'allowed', 'exception_applied': True, 'exception_refs': ('legacy-waiver',)}),
        'lifecycle': row.context.lifecycle.model_copy(update={'qualification_is_current': True}),
    })
    result = page_delivery_summary(population([card()], [row.model_copy(update={'context': changed})]))
    task = result.groups[2].items[0]
    assert task.reason == 'awaiting_confirmation' and task.historical_done and task.exception_applied
    assert result.groups[0].count == 0


@pytest.mark.parametrize('freshness', ['updating', 'pending_initial', 'unavailable'])
def test_noncurrent_generation_never_confirms_done_or_current_exceptions(freshness):
    value = population(projection=PROJECTION.model_copy(update={'freshness': freshness}))
    result = page_delivery_summary(value)
    assert result.groups[2].items[0].reason == 'updating'
    assert not result.complete and result.task_count is None and result.unique_obligation_count is None


def test_current_activity_is_visible_while_proof_generation_is_updating():
    value = population([card(status='in_progress')], [facts(status='in_progress')],
        projection=PROJECTION.model_copy(update={'freshness': 'updating'}))
    assert value.items[0].group == 'in_progress' and not value.complete


def test_missing_or_partial_population_is_unknown_rather_than_successful_empty():
    for value in [population(population_complete=False), population(rows=[]), population(spec_context=None)]:
        result = page_delivery_summary(value)
        assert not result.complete and result.task_count is None
        assert all(group.count is None for group in result.groups)
    empty = page_delivery_summary(population([], []))
    assert empty.complete and empty.task_count == 0 and all(group.count == 0 for group in empty.groups)


@pytest.mark.parametrize('damage', ['foreign_card', 'foreign_fact', 'duplicate_card', 'duplicate_fact', 'snapshot', 'version'])
def test_scope_and_generation_inconsistency_fail_closed(damage):
    cards, rows = [card()], [facts()]
    if damage == 'foreign_card': cards[0] = card(board_id='other')
    elif damage == 'duplicate_card': cards *= 2
    elif damage == 'duplicate_fact': rows *= 2
    else:
        value = rows[0].context
        if damage == 'foreign_fact': value = value.model_copy(update={'scope': value.scope.model_copy(update={'spec_id': 'other'})})
        elif damage == 'snapshot': value = value.model_copy(update={'snapshot_id': 'b' * 64})
        else: value = value.model_copy(update={'scope': value.scope.model_copy(update={'spec_version': 6})})
        rows[0] = rows[0].model_copy(update={'context': value})
    with pytest.raises(ValueError, match='delivery_summary_'):
        population(cards, rows)


def test_card_version_or_status_drift_cannot_reuse_a_done_assessment():
    for current in [card(card_version=2), card(status='validation')]:
        value = population([current], [facts()])
        assert value.items[0].group == 'pending' and not value.complete


def test_population_base_is_stable_but_fences_live_metadata_changes_within_one_generation():
    first = population([card('a'), card('b')], [facts('a'), facts('b')])
    reversed_input = population([card('b'), card('a')], [facts('b'), facts('a')])
    assert first.population_id == reversed_input.population_id
    renamed = population([card('a', title='New title'), card('b')], [facts('a'), facts('b')])
    assert renamed.projection.generation_id == first.projection.generation_id
    assert renamed.population_id != first.population_id
    assert page_delivery_summary(first).population_id == first.population_id


def test_pages_keep_full_counts_and_load_only_the_requested_visible_items():
    cards = [card(str(index)) for index in range(12)] + [card('optional')]
    rows = [facts(str(index)) for index in range(12)] + [facts('optional', decision='optional')]
    value = population(cards, rows)
    encoded = []
    def encode(group, offset):
        encoded.append((group, offset))
        return f'opaque-{group}-{offset}'
    first = page_delivery_summary(value, limit=20, encode_cursor=encode)
    assert len(first.groups[0].items) == 5 and first.groups[0].count == 12
    assert first.groups[0].has_more and encoded == [('delivered', 5)]
    assert first.groups[3].count == 1 and not first.groups[3].loaded
    second = page_delivery_summary(value, group='delivered', offset=5, limit=5, encode_cursor=encode)
    assert len(second.groups[0].items) == 5 and second.task_count == 13
    assert not set(row.card_id for row in first.groups[0].items) & set(row.card_id for row in second.groups[0].items)
    assert all(not group.loaded for group in second.groups[1:])


@pytest.mark.parametrize('options', [dict(limit=0), dict(limit=21), dict(limit=True), dict(offset=-1),
    dict(offset=1), dict(group='unknown')])
def test_invalid_page_inputs_are_rejected(options):
    with pytest.raises(ValueError, match='delivery_summary_page_invalid'):
        page_delivery_summary(population(), **options)


def test_current_projection_requires_exact_base_and_output_is_immutable():
    with pytest.raises(ValidationError):
        DeliverySummaryProjection(generation_id=None, snapshot_id=None, freshness='current')
    result = page_delivery_summary(population())
    with pytest.raises(ValidationError): result.complete = False
