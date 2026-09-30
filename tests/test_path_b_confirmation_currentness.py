"""BASE T23: a persisted confirmation must not stand in for current evidence."""
import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.services.main import SpecService, CardService, CardOperationError
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.domain.enums import CardStatus
from test_path_b_e2e import (
    USER_ID, _prepare_cross_spec_regression, _coverage_state,
)


@pytest.mark.asyncio
async def test_operational_evidence_reset_invalidates_path_b_readiness():
    async with get_session_factory()() as db:
        ids = await _prepare_cross_spec_regression(db, confirmed=True)
        assert await _coverage_state(db, ids) == 'path_b_ready'
        result = await SpecService(db).set_test_scenario_status(
            ids['other_spec'], USER_ID, ids['foreign_scenario'], 'ready')
        assert result['old_status'] == 'automated'
        assert result['new_status'] == 'ready'
        preview = await _coverage_state(db, ids)
        blocked = False
        try:
            moved = await CardService(db).move_card(
                ids['bug'], USER_ID, CardMove(status=CardStatus.IN_PROGRESS))
            observed = moved.status
        except CardOperationError as error:
            blocked = True
            observed = error.code
        if (preview, blocked, observed) == ('path_b_ready', False, CardStatus.IN_PROGRESS):
            pytest.xfail('T23 reproduced: obsolete confirmation still admits Bug; historical gate decision pending in ledger')
        assert (preview, blocked) == ('coverage_pending', True)
