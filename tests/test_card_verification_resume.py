from types import SimpleNamespace

from test_implementation_responsibility import split_population, card
from test_verification_plan import scenario
from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
from okto_pulse.core.domain.execution_plan import resolve_spec_execution_plan, card_verification_plan


def fixture():
    data = {field: [] for _, field in COLLECTIONS}
    data.update(split_population())
    data['test_scenarios'] = [scenario(id='ui-test', linked_criteria=['ac-login']),
        scenario(id='auth-test', linked_criteria=['ac-lock'])]
    spec = SimpleNamespace(id='spec', board_id='board', title='Spec', description='Scope', context='Context', **data)
    cards = [card('ui'), card('authorization'), card('unrelated'),
        card('tester', card_type='test', test_scenario_ids=['ui-test', 'auth-test'])]
    for item in cards:
        item.update(title=item['id'], description='Scope', details='Details')
    return resolve_spec_execution_plan(spec=spec, cards=cards, admitted_methods=frozenset({'automated_test'}))


def test_split_and_inherited_rule_do_not_assign_every_scenario_to_every_card():
    plan = fixture()
    ui = card_verification_plan(plan, 'ui')
    auth = card_verification_plan(plan, 'authorization')
    assert ui['complete'] and auth['complete']
    assert [row['scenario_id'] for row in ui['items']] == ['ui-test']
    assert [row['scenario_id'] for row in auth['items']] == ['auth-test']
    assert auth['items'][0]['criterion_ids'] == ['ac-lock']
    assert auth['items'][0]['test_card_ids'] == ['tester']
    assert card_verification_plan(plan, 'unrelated')['items'] == []
    assert len(card_verification_plan(plan, 'tester')['items']) == 2


def test_legacy_does_not_invent_a_verification_plan():
    result = card_verification_plan(None, 'card')
    assert not result['complete'] and result['status'] == 'legacy_or_unavailable'
