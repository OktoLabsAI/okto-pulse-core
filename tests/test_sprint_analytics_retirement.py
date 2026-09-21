"""The removed Sprint analytics cannot remain as a callable product surface."""
from importlib.util import find_spec

import pytest

from okto_pulse.core.application import use_cases
from okto_pulse.core.application.service_catalog import CoreAnalyticsOperations
from okto_pulse.core.ports.application_services import AnalyticsOperations
from okto_pulse.core.services import analytics_service
from okto_pulse.core.services.test_scenario_lifecycle import scenario_has_required_evidence


@pytest.mark.parametrize('module', [
    'services.sprint_scope', 'services.delivery_commitment',
    'ports.delivery_commitment', 'ports.sprint_activation_baseline',
])
def test_sprint_scope_and_commitment_modules_are_absent(module):
    assert find_spec(f'okto_pulse.core.{module}') is None


def test_readers_ports_and_use_cases_do_not_expose_sprint_analytics():
    for name in ('sprint', 'sprints'):
        assert not hasattr(CoreAnalyticsOperations, name)
        assert not hasattr(AnalyticsOperations, name)
    for name in ('compute_sprint_analytics', 'compute_sprints_analytics', '_sprint_detail',
                 '_aggregate_sprint_evaluation', '_sprint_status_breakdown'):
        assert not hasattr(analytics_service, name)
    for name in ('BoardSprintAnalytics', 'BoardSprintsAnalytics'):
        for suffix in ('Command', 'Result', 'UseCase'):
            assert not hasattr(use_cases, name+suffix)
    # The imported lifecycle evidence rule remains independent of Sprint scope.
    assert not scenario_has_required_evidence({'status': 'passed'})
