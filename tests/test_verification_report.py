"""Specialized observations stay distinct from receipt authentication/authority."""

from copy import deepcopy

import pytest

from okto_pulse.core.domain.verification_report import parse_verification_report, require_verification_report_context


SOURCE = {'reference': 'repo:component', 'revision': 'commit-1', 'sha256': 'a' * 64}


def payload(method='inspection'):
    common = {'schema_version': 'verification-report/v1', 'method': method, 'report_id': 'report-1',
        'observed_at': '2026-09-23T13:00:00Z', 'sources': [SOURCE], 'conclusion': 'The observed condition holds.',
        'result': 'passed', 'observations': [{'observation_id': 'observation-1', 'criterion_id': 'ac-1',
            'observation_ref': 'report:report-1#item-1', 'expected': 'No forbidden dependency',
            'observed': 'The inspected imports use the public port.', 'outcome': 'passed'}]}
    if method == 'inspection':
        common['inspection_procedure'] = {**SOURCE, 'reference': 'procedure:review'}
    elif method == 'demonstration':
        common.update(procedure={**SOURCE, 'reference': 'procedure:exercise'},
            environment={**SOURCE, 'reference': 'environment:fixture'})
    else:
        common.update(tool_name='dependency-checker', tool_version='1.2',
            rules=[{**SOURCE, 'reference': 'rule:public-ports'}], configuration={**SOURCE, 'reference': 'config:rules'},
            analyzed_scope=['src/component'], findings=[])
    return common


@pytest.mark.parametrize('method', ['inspection', 'static_analysis', 'demonstration'])
def test_complete_report_is_data_not_an_approval_or_an_authenticated_receipt(method):
    report = parse_verification_report(payload(method))
    assert not hasattr(report, 'approved') and not hasattr(report, 'execution_receipt')
    require_verification_report_context(report, method=method, status='passed', criterion_ids=('ac-1',))


@pytest.mark.parametrize('field', ['author_id', 'approved', 'skip_independence', 'execution_receipt'])
def test_client_cannot_supply_authority_fields(field):
    value = payload()
    value[field] = True
    with pytest.raises(ValueError):
        parse_verification_report(value)


@pytest.mark.parametrize('field', ['tool_name', 'tool_version', 'rules', 'configuration', 'analyzed_scope', 'findings'])
def test_static_analysis_requires_versioned_rules_configuration_scope_and_findings(field):
    value = payload('static_analysis')
    del value[field]
    with pytest.raises(ValueError):
        parse_verification_report(value)


def test_unknown_rule_cannot_be_claimed_as_an_analyzed_finding():
    value = payload('static_analysis')
    value['findings'] = [{'finding_id': 'f1', 'rule_id': 'unconfigured-rule', 'location': 'src/component:1',
        'description': 'Unbound rule', 'severity': 'error'}]
    with pytest.raises(ValueError, match='rule_scope_invalid'):
        parse_verification_report(value)


@pytest.mark.parametrize('observed_at', [0, True, '2026-09-23T13:00:00'], ids=['numeric', 'boolean', 'naive'])
def test_observation_time_is_explicit_and_timezone_aware(observed_at):
    value = payload()
    value['observed_at'] = observed_at
    with pytest.raises(ValueError):
        parse_verification_report(value)


def test_failed_observation_cannot_be_hidden_by_a_passing_summary():
    value = payload()
    value['observations'][0]['outcome'] = 'failed'
    with pytest.raises(ValueError, match='result_mismatch'):
        parse_verification_report(value)
    value['result'] = 'failed'
    assert parse_verification_report(value).result == 'failed'


@pytest.mark.parametrize('outcome', ['inconclusive', 'aborted', 'unavailable'])
def test_nonconclusive_observation_remains_distinct_and_cannot_be_passing(outcome):
    value = payload()
    value['result'] = outcome
    value['observations'][0]['outcome'] = outcome
    report = parse_verification_report(value)
    assert report.result == outcome and report.observations[0].outcome == outcome
    from okto_pulse.core.domain.verification_report import verification_report_scenario_status
    assert verification_report_scenario_status(report) == 'ready'
    require_verification_report_context(report, method='inspection', status='ready', criterion_ids=('ac-1',))
    with pytest.raises(ValueError):
        require_verification_report_context(report, method='inspection', status='passed', criterion_ids=('ac-1',))
    value['result'] = 'passed'
    with pytest.raises(ValueError):
        parse_verification_report(value)


def test_current_method_result_and_entire_criterion_scope_are_required():
    report = parse_verification_report(payload())
    for method, status, criteria in [('static_analysis', 'passed', ('ac-1',)), ('inspection', 'automated', ('ac-1',)),
            ('inspection', 'failed', ('ac-1',)), ('inspection', 'passed', ('ac-1', 'ac-2')),
            ('inspection', 'passed', ('ac-2',)), ('inspection', 'passed', ())]:
        with pytest.raises(ValueError):
            require_verification_report_context(report, method=method, status=status, criterion_ids=criteria)


@pytest.mark.parametrize('outcome', ['failed', 'inconclusive', 'aborted', 'unavailable'])
def test_criterion_projection_requires_every_observation_to_pass(outcome):
    from okto_pulse.core.domain.verification_report import verification_report_passing_criteria
    value = payload()
    first = value['observations'][0]
    value['observations'] += [
        {**first, 'observation_id': 'other-criterion', 'criterion_id': 'ac-2'},
        {**first, 'observation_id': 'second-dimension', 'outcome': outcome},
    ]
    value['result'] = outcome
    report = parse_verification_report(value)
    assert verification_report_passing_criteria(report) == ('ac-2',)
    assert report.result == outcome


def test_aggregate_limit_applies_even_when_individual_observations_fit():
    value = payload()
    item = deepcopy(value['observations'][0])
    item['observed'] = 'observed ' * 900
    value['observations'] = [{**item, 'observation_id': f'item-{i}'} for i in range(10)]
    with pytest.raises(ValueError, match='aggregate_limit'):
        parse_verification_report(value)


@pytest.mark.parametrize('method', ['inspection', 'static_analysis', 'demonstration'])
def test_specialized_evidence_requires_its_own_method_and_never_automated_status(method):
    from okto_pulse.core.domain.verification_report import require_evidence_method_binding
    from okto_pulse.core.services.test_scenario_lifecycle import validate_test_scenario_evidence
    evidence = {'evidence_class': 'verification_report', 'verification_report': payload(method),
        'report_author_id': 'actor', 'scenario_sha256': 'sha256:' + 'a' * 64,
        'execution_receipt': 'ev2r.' + 'b' * 32 + '.' + 'c' * 64}
    assert validate_test_scenario_evidence('passed', evidence) == (True, [])
    assert not validate_test_scenario_evidence('automated', evidence)[0]
    require_evidence_method_binding(method, evidence)
    for wrong in (None, 'automated_test', 'unsupported'):
        with pytest.raises(ValueError):
            require_evidence_method_binding(wrong, evidence)
    with pytest.raises(ValueError):
        require_evidence_method_binding(method, {'execution_receipt': evidence['execution_receipt']})
    assert not validate_test_scenario_evidence('passed', {**evidence, 'manifest_ref': 'fake-replay'})[0]
    unsigned = dict(evidence)
    del unsigned['execution_receipt']
    assert not validate_test_scenario_evidence('passed', unsigned)[0]
