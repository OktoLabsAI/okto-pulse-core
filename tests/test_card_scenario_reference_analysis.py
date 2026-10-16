"""Source absence is semantic; incomplete source reads never authorize prune."""
import pytest

from okto_pulse.core.domain.card_scenario_references import analyze_card_scenario_references


def analyze(**changes):
    values = dict(board_id='board', card_id='card', spec_id='spec', card_links=['ts_one'],
                  parent_exists=True, scenarios=[{'id': 'ts_one', 'linked_task_ids': ['card']}])
    return analyze_card_scenario_references(**(values | changes))


def test_known_removal_diagnoses_only_the_broken_reference_and_keeps_valid_links():
    result = analyze(card_links=['ts_one', 'ts_gone'])
    assert result.links == (('ts_one', 'supports/card_scenario_observed_reciprocal@v2.1'),)
    finding, = result.snapshot.findings
    assert (finding.target_ref, finding.reason_code) == ('spec:spec:test_scenario:ts_gone', 'target_absent')
    repaired = analyze()
    assert repaired.snapshot.findings == ()
    assert repaired.snapshot.source_fingerprint != result.snapshot.source_fingerprint


@pytest.mark.parametrize('spec_id', ['missing', None])
def test_known_absent_parent_retains_intended_selector_without_inventing_endpoint(spec_id):
    result = analyze(spec_id=spec_id, parent_exists=False, scenarios=None)
    assert result.links == ()
    finding, = result.snapshot.findings
    assert finding.reason_code == 'parent_absent'
    assert finding.target_ref == ('spec:missing:test_scenario:ts_one' if spec_id else 'ts_one')


def test_intentional_unlink_with_no_remaining_obligation_is_not_a_finding():
    result = analyze(spec_id=None, card_links=[], parent_exists=False, scenarios=None)
    assert result.links == () and result.snapshot.findings == ()


@pytest.mark.parametrize('scenarios', [None, {}, 'unavailable', [{'id': 'ts_one', 'linked_task_ids': 5}]])
def test_incomplete_source_refuses_to_produce_a_complete_snapshot(scenarios):
    with pytest.raises(ValueError, match='source_invalid'):
        analyze(scenarios=scenarios)


def test_ambiguous_target_is_not_selected_arbitrarily():
    result = analyze(scenarios=[{'id': 'ts_one'}, {'id': 'ts_one', 'linked_task_ids': ['card']}])
    assert result.links == ()
    assert [finding.reason_code for finding in result.snapshot.findings] == ['target_ambiguous']


def test_ambiguous_spec_only_reference_keeps_its_actual_source_selector():
    result = analyze(card_links=[], scenarios=[{'id': 'ts_one'}, {'id': 'ts_one', 'linked_task_ids': ['card']}])
    finding, = result.snapshot.findings
    assert finding.source_selector == 'spec:spec:test_scenario:ts_one:linked_task_ids'
    assert result.links == ()


def test_order_and_duplicates_in_card_reference_list_do_not_create_new_defects():
    first = analyze(card_links=['missing_b', 'missing_a'], scenarios=[])
    second = analyze(card_links=['missing_a', 'missing_b', 'missing_a'], scenarios=[])
    assert first == second
    assert len({finding.finding_id for finding in first.snapshot.findings}) == 2


@pytest.mark.parametrize("origin", ["card", "spec"])
def test_unilateral_reference_is_explicit_without_changing_observed_link(origin):
    result = analyze(
        card_links=["ts_one"] if origin == "card" else [],
        scenarios=[{"id": "ts_one", "linked_task_ids": ["card"] if origin == "spec" else []}],
    )
    assert result.links == (("ts_one", f"supports/card_scenario_observed_{origin}@v2.1"),)
    finding, = result.snapshot.findings
    assert finding.reason_code == "source_disagreement"
    assert finding.target_ref == "spec:spec:test_scenario:ts_one"
    assert finding.source_selector == (
        "card:card:test_scenario_ids" if origin == "card"
        else "spec:spec:test_scenario:ts_one:linked_task_ids"
    )
    assert analyze().snapshot.findings == ()
