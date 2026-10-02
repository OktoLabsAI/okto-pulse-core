"""Removed validation shapes fail before any persistence work."""
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.services import main
from okto_pulse.core.models.schemas import SpecValidationSubmit


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['score', 'summary', 'completeness',
                                  'completeness_justification', 'general_justification',
                                  'expected_spec_edition'])
@pytest.mark.parametrize('value', [None, 'old-value'])
async def test_old_input_is_refused_before_flush_or_lookup(monkeypatch, field, value):
    flush = AsyncMock(side_effect=AssertionError('must not flush incompatible request'))
    monkeypatch.setattr(main, '_application_flush', flush)
    payload = {field: value}
    with pytest.raises(ValueError, match='Unknown spec validation fields'):
        await main.SpecService(object()).submit_spec_validation(
            'spec', 'reviewer', 'Reviewer', payload)
    flush.assert_not_awaited()
    assert payload == {field: value}


def test_transport_does_not_accept_predecessor_edition_alias():
    payload = dict(expected_spec_edition=1, expected_spec_version=1, expected_head_revision=0,
                   recommendation='approve')
    for metric in ('confidence', 'clarity', 'assertiveness', 'decidability', 'ambiguity'):
        payload[metric] = 90
        payload[metric+'_justification'] = 'Independent reviewer justification'
    with pytest.raises(ValueError, match='Extra inputs'):
        SpecValidationSubmit.model_validate(payload)
    payload['expected_validation_edition'] = payload.pop('expected_spec_edition')
    assert SpecValidationSubmit.model_validate(payload).expected_validation_edition == 1
