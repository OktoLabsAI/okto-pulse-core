"""Retired Sprint wire DTOs and list fields must not remain public contracts."""
import pytest

from okto_pulse.core import models
from okto_pulse.core.models import schemas
from okto_pulse.core.inbound import enum_error_envelope


@pytest.mark.parametrize('name', [
    'SprintPageItem', 'SprintCreate', 'SprintUpdate', 'SprintMove',
    'SprintResponse', 'SprintSummary', 'SprintHistoryResponse',
    'SprintQAResponse', 'SprintEvaluationCreate',
])
def test_retired_sprint_dto_is_not_exposed(name):
    assert name not in vars(schemas)
    assert name not in models.__all__


def test_card_list_has_no_live_sprint_link():
    assert 'sprint_id' not in schemas.CardPageItem.model_fields
    assert 'sprint_id' not in schemas.CardPageItem.model_json_schema()['properties']


def test_scenario_envelope_remains_while_lane_normalizer_is_removed():
    assert not hasattr(enum_error_envelope, 'canonical_enum_error')
    errors = [{'type': 'literal_error', 'loc': ('body', 'scenario_type'), 'input': 'invalid'}]
    assert enum_error_envelope.canonical_scenario_type_error(errors)['error'] == 'invalid_scenario_type'
