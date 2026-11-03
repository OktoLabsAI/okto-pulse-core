from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding
from okto_pulse.core.domain.effective_delivery_inventory import EffectiveDeliveryInventory, EffectiveDeliveryObligation
from okto_pulse.core.domain.implementation_responsibility import RequirementContribution
from okto_pulse.core.domain.task_requirement_decision import (
    TaskRequirementScope, TaskRequirementDecisionRecord, resolve_task_requirement,
    resolve_task_requirement_allocation,
)
from okto_pulse.core.models.task_requirement_decision import TaskRequirementDecisionInput


NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)


def inventory():
    owners = tuple(RequirementContribution(identity, 'direct', 'whole_requirement', (), None, (), digest * 64)
                   for identity, digest in [('a', 'b'), ('b', 'c')])
    return EffectiveDeliveryInventory((EffectiveDeliveryObligation(
        DeliveryBinding('fr:shared', 'a' * 64), 'fr', owners, (), ('a', 'b'),
    ),), SimpleNamespace(), True, ())


def scope(identity='a', edition=1):
    return inventory().task_requirement_scope(board_id='board', spec_id='spec', edition=edition,
        card=dict(id=identity, board_id='board', spec_id='spec', card_type='normal', archived=False))


def record(**changes):
    return TaskRequirementDecisionRecord(**(dict(id='d1', operation_id='op1', scope=scope(), revision=1,
        decision='waived', previous_decision='required', supersedes=None,
        actor_id='human', recorded_at=NOW, user_note=None) | changes))


def test_shared_requirement_is_scoped_to_the_selected_card_and_preserves_inventory():
    native = inventory()
    before = native.snapshot_sha256
    first, other = scope(), scope('b')
    assert first.digest != other.digest
    assert resolve_task_requirement(first, [record()]).decision == 'waived'
    assert resolve_task_requirement(other, []).required
    assert native.snapshot_sha256 == before
    assert len(native.rows[0].contributions) == 2


def allocation(histories, native=None):
    return resolve_task_requirement_allocation(inventory=native or inventory(), board_id='board',
        spec_id='spec', edition=1, cards=[dict(id=identity, board_id='board', spec_id='spec',
            card_type='normal', archived=False) for identity in ['a', 'b']], histories=histories)[0]


def test_disposing_one_contribution_keeps_the_other_required():
    value = allocation({'a': [record()]})
    assert value.required_card_ids == ('b',)
    assert value.waived_card_ids == ('a',)
    assert not value.not_required


def test_all_optional_or_waived_is_explicitly_not_required_without_proof():
    value = allocation({'a': [record()], 'b': [record(id='d-b', scope=scope('b'), decision='optional')]})
    assert value.not_required and value.required_card_ids == ()
    assert value.optional_card_ids == ('b',) and value.waived_card_ids == ('a',)
    assert value.decision_ids == ('d-b', 'd1')
    assert not hasattr(value, 'implementation_satisfied')


def test_changed_contributions_restore_required_applicability_without_reusing_old_choice():
    original = inventory()
    row = original.rows[0]
    updated = replace(original, rows=(replace(row, contributions=(replace(row.contributions[0], scope_sha256='e' * 64), row.contributions[1])),))
    value = allocation({'a': [record()]}, updated)
    assert value.required_card_ids == ('a', 'b')
    assert not value.not_required and value.decision_ids == ()


def test_canonical_gate_keeps_a_shared_required_contribution_pending():
    from okto_pulse.core.domain.delivery_evidence import DeliveryEvidenceSnapshot, DeliveryObligation, DeliveryScope
    from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
    from okto_pulse.core.domain.delivery_verification import NativeVerificationPopulation
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, assess_delivery_gate, UnitState
    from okto_pulse.core.domain.enums import CardType
    native = inventory()
    decision = allocation({'a': [record()]})
    native = replace(native, rows=(replace(native.rows[0], requirement_allocation=decision),))
    context = EffectiveDeliveryContext(native, (), (), frozenset({'automated_test'}),
        required_scenarios=(), native_verification=NativeVerificationPopulation((), True))
    snapshot = DeliveryEvidenceSnapshot(DeliveryScope('board', 'spec', 1),
        (DeliveryObligation(native.rows[0].binding, 'Shared requirement'),), (), (), complete=True,
        effective_context=context)
    waived = assess_obligation_delivery(snapshot, card_id='a', card_type=CardType.NORMAL)
    required = assess_obligation_delivery(snapshot, card_id='b', card_type=CardType.NORMAL)
    global_result = assess_obligation_delivery(snapshot)
    assert all(row.implementation.state == row.verification.state == UnitState.NOT_APPLICABLE for row in waived.obligations)
    assert assess_delivery_gate(waived).allowed
    assert not assess_delivery_gate(required).allowed
    assert not assess_delivery_gate(global_result).allowed
    assert global_result.obligations[0].contribution_card_ids == ('a', 'b')


def test_new_edition_cannot_inherit_a_previous_decision():
    with pytest.raises(ValueError, match='history_invalid'):
        resolve_task_requirement(scope(edition=2), [record()])
    assert resolve_task_requirement(scope(edition=2), []).required


def test_changed_scope_requires_a_new_decision_and_never_falls_back_to_old_history():
    original = scope()
    changed = replace(original, contributions=(('fr:shared', 'd' * 64, 'b' * 64),))
    older = record(decision='optional')
    newer = record(id='d2', operation_id='op2', scope=changed, revision=2,
                   decision='required', previous_decision='optional', supersedes='d1')
    resolved = resolve_task_requirement(original, [older, newer])
    assert resolved.required and resolved.record is None and resolved.history_head == newer
    assert resolve_task_requirement(changed, [older, newer]).record == newer


def test_restore_is_an_append_only_successor_and_blank_note_is_optional():
    first = record(user_note='   ')
    restored = record(id='d2', operation_id='op2', revision=2, decision='required',
                      previous_decision='waived', supersedes=first.id)
    assert first.user_note is None
    assert first.decision == 'waived'
    result = resolve_task_requirement(scope(), [restored, first])
    assert result.required and result.record.supersedes == 'd1'
    assert TaskRequirementDecisionRecord.from_dict(restored.to_dict()) == restored


@pytest.mark.parametrize('changes', [dict(revision=2), dict(edition=0), dict(edition=True)])
def test_invalid_revision_or_edition_is_rejected(changes):
    with pytest.raises(ValueError, match='task_requirement_'):
        record(**changes) if 'revision' in changes else scope(edition=changes['edition'])


@pytest.mark.parametrize('history', [
    lambda: [record(), record(id='different')],
    lambda: [record(), record(id='d2', revision=3, previous_decision='waived', supersedes='d1')],
    lambda: [record(), record(id='d2', revision=2, previous_decision='optional', supersedes='d1')],
    lambda: [record(), record(id='d2', revision=2, previous_decision='waived', supersedes='other')],
    lambda: [record(scope=scope('b'))],
])
def test_ambiguous_or_foreign_history_cannot_remove_an_obligation(history):
    with pytest.raises(ValueError, match='history_invalid'):
        resolve_task_requirement(scope(), history())


def test_missing_allocation_and_foreign_board_do_not_become_blanket_waivers():
    native = inventory()
    for card in [dict(id='a', board_id='other', spec_id='spec', card_type='normal', archived=False),
                 dict(id='missing', board_id='board', spec_id='spec', card_type='normal', archived=False)]:
        with pytest.raises(ValueError, match='task_requirement_'):
            native.task_requirement_scope(board_id='board', spec_id='spec', edition=1, card=card)
    incomplete = replace(native, population_complete=False)
    with pytest.raises(ValueError, match='scope_unavailable'):
        incomplete.task_requirement_scope(board_id='board', spec_id='spec', edition=1,
            card=dict(id='a', board_id='board', spec_id='spec', card_type='normal', archived=False))


def test_test_card_scope_contains_only_its_own_scenario_execution():
    value = inventory().task_requirement_scope(board_id='board', spec_id='spec', edition=1,
        card=dict(id='test', board_id='board', spec_id='spec', card_type='test', archived=False,
                  test_scenario_ids=['scenario']), scenario_digests={'scenario': 'f' * 64})
    assert value.card_type == 'test'
    assert [row[0] for row in value.contributions] == ['scenario:scenario']
    assert scope().digest != value.digest


def input_value(**changes):
    return dict(contract_version='task-requirement-decision/v1', card_ids=['a'], decision='waived',
                basis='a' * 64, note=None, idempotency_key='stable-key') | changes


@pytest.mark.parametrize('changes', [dict(actor_id='human'), dict(recorded_at=NOW),
    dict(card_ids=[]), dict(card_ids=['a', 'a']), dict(card_ids=['a'] * 51),
    dict(card_ids=[True]), dict(decision='done'), dict(basis='stale'), dict(note=42)])
def test_transport_rejects_authority_fields_lifecycle_values_and_invalid_selection(changes):
    with pytest.raises(ValidationError):
        TaskRequirementDecisionInput.model_validate(input_value(**changes))


def test_optional_note_and_all_three_simple_decisions_are_supported():
    for decision in ['waived', 'optional', 'required']:
        value = TaskRequirementDecisionInput.model_validate(input_value(decision=decision, note='  '))
        assert value.note is None
        assert value.decision == decision


def test_tampered_scope_digest_is_not_accepted_as_a_record():
    value = record().to_dict()
    value['scope_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='record_invalid'):
        TaskRequirementDecisionRecord.from_dict(value)
