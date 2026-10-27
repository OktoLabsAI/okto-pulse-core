"""New cards enter planning; Started remains a legacy persisted value only."""

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.models.schemas import CardCreate


@pytest.mark.parametrize("card_type", ["normal", "test", "bug"])
def test_new_card_status_is_not_started_for_each_type(card_type):
    assert CardCreate(title="Work", spec_id="spec", card_type=card_type).status is CardStatus.NOT_STARTED
    assert CardCreate(title="Work", spec_id="spec", card_type=card_type,
                      status="not_started").status is CardStatus.NOT_STARTED


@pytest.mark.parametrize("card_type", ["normal", "test", "bug"])
@pytest.mark.parametrize("status", [
    CardStatus.STARTED, CardStatus.IN_PROGRESS, CardStatus.VALIDATION,
    CardStatus.DONE, CardStatus.ON_HOLD, CardStatus.REJECTED, CardStatus.CANCELLED,
])
def test_new_card_cannot_skip_planning(card_type, status):
    with pytest.raises(ValidationError):
        CardCreate(title="Work", spec_id="spec", card_type=card_type, status=status)
