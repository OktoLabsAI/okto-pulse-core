"""Recovery authority and reservation token must both remain live per batch."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application import offline_kg_recovery as policy
from okto_pulse.core.ports.consolidation import ConsolidationClaimScope, ExactConsolidationBatchResult
from okto_pulse.core.ports.offline_kg_recovery import (
    issue_offline_recovery_capability, reserve_offline_consolidation,
)


class LeasePort:
    def __init__(self):
        self.current = None
        self.acquisitions, self.renewals, self.releases = 0, 0, 0

    def acquire_single_writer_sync(self, **request):
        self.acquisitions += 1
        if self.current is not None:
            return {'acquired': False}
        now = datetime.now(timezone.utc).timestamp()
        self.current = dict(owner_token='original-token', owner_id=request['owner_id'],
            operation=request['operation'], acquired_at_epoch=now,
            expires_at_epoch=now + request['ttl_seconds'], admin_lane=request['admin_lane'])
        return dict(acquired=True, owner_token='original-token')

    def inspect_single_writer_sync(self, **request):
        return self.current.copy() if self.current is not None else None

    def renew_single_writer_sync(self, **request):
        self.renewals += 1
        return bool(self.current and self.current['owner_token'] == request['owner_token'])

    def release_single_writer_sync(self, **request):
        self.releases += 1
        if self.current and self.current['owner_token'] == request['owner_token']:
            self.current = None
            return True
        return False


def scope(**values):
    return ConsolidationClaimScope(**({'board_id': 'board', 'source': 'rebuild:run',
        'reservation_lineage_id': 'a' * 64} | values))


def reserve(port, capability, **values):
    return reserve_offline_consolidation(claim_scope=scope(**values), recovery_capability=capability,
        write_lock_port=port, relational_scope_factory=lambda: None, owner_id='installer')


@pytest.mark.asyncio
async def test_public_scope_renews_passes_exact_authority_and_revokes_after_exit(monkeypatch):
    port, calls = LeasePort(), []
    async def process(**request):
        assert request['reservation_authority_probe']() is True
        calls.append(request)
        return ExactConsolidationBatchResult(request['claim_scope'], ())
    monkeypatch.setattr(policy, 'ConsolidationProcessor', lambda *a, **k: SimpleNamespace(process_exact_batch=process))
    with issue_offline_recovery_capability(board_id='board', lifetime_probe=lambda: True) as capability:
        with reserve(port, capability) as reserved:
            assert reserved.claim_scope == scope()
            assert (await reserved.process_next()).rows == ()
        assert not reserved.is_authorized()
        with pytest.raises(RuntimeError, match='authority_lost'):
            await reserved.process_next()
        with pytest.raises(RuntimeError, match='authority_required'):
            with reserve(port, capability, source='rebuild:another'):
                pytest.fail('capability admitted a second operation')
    assert len(calls) == 1 and port.renewals == port.releases == 1
    assert port.current is None


@pytest.mark.parametrize('kind', ['fake', 'foreign_board', 'dead', 'no_lineage'])
def test_invalid_recovery_scope_never_acquires_reservation(kind):
    port = LeasePort()
    with issue_offline_recovery_capability(board_id='other' if kind == 'foreign_board' else 'board',
            lifetime_probe=lambda: kind != 'dead') as capability:
        with pytest.raises((ValueError, RuntimeError), match='configuration_invalid|authority_required'):
            with reserve(port, {} if kind == 'fake' else capability,
                    **({'reservation_lineage_id': None} if kind == 'no_lineage' else {})):
                pytest.fail('invalid authority admitted')
    assert port.acquisitions == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('loss', ['token', 'operation', 'expired', 'external', 'scope_exit'])
async def test_lost_authority_does_not_claim_or_release_successor_token(monkeypatch, loss):
    port, live = LeasePort(), [True]
    async def forbidden(**request):
        pytest.fail('authority lost before worker admission')
    monkeypatch.setattr(policy, 'ConsolidationProcessor', lambda *a, **k: SimpleNamespace(process_exact_batch=forbidden))
    issuer = issue_offline_recovery_capability(board_id='board', lifetime_probe=lambda: live[0])
    capability = issuer.__enter__()
    try:
        with pytest.raises(RuntimeError, match='authority_lost'):
            with reserve(port, capability) as reserved:
                if loss == 'token':
                    port.current['owner_token'] = 'successor'
                elif loss == 'operation':
                    port.current['operation'] = 'erasure'
                elif loss == 'expired':
                    port.current['expires_at_epoch'] = 0
                elif loss == 'external':
                    live[0] = False
                else:
                    issuer.__exit__(None, None, None)
                assert not reserved.is_authorized()
                await reserved.process_next()
    finally:
        if loss != 'scope_exit':
            issuer.__exit__(None, None, None)
    assert port.renewals == 0
    if loss == 'token':
        assert port.current['owner_token'] == 'successor'
    else:
        assert port.current is None


def test_worker_construction_failure_releases_acquired_reservation(monkeypatch):
    port = LeasePort()
    def fail(*args, **kwargs):
        raise ValueError('construction failed')
    monkeypatch.setattr(policy, 'ConsolidationProcessor', fail)
    with issue_offline_recovery_capability(board_id='board', lifetime_probe=lambda: True) as capability:
        with pytest.raises(ValueError, match='construction failed'):
            with reserve(port, capability):
                pytest.fail('construction failed before yielding')
    assert port.current is None and port.releases == 1


@pytest.mark.asyncio
async def test_renewal_refusal_prevents_worker_mutation(monkeypatch):
    port = LeasePort()
    monkeypatch.setattr(port, 'renew_single_writer_sync', lambda **request: False)
    async def forbidden(**request):
        pytest.fail('failed renewal admitted mutation')
    monkeypatch.setattr(policy, 'ConsolidationProcessor', lambda *a, **k: SimpleNamespace(process_exact_batch=forbidden))
    with issue_offline_recovery_capability(board_id='board', lifetime_probe=lambda: True) as capability:
        with pytest.raises(RuntimeError, match='authority_lost'):
            with reserve(port, capability) as reserved:
                await reserved.process_next()
    assert port.current is None and port.releases == 1


def test_contention_keeps_existing_administrative_owner():
    port = LeasePort()
    port.current = {'owner_token': 'existing'}
    with issue_offline_recovery_capability(board_id='board', lifetime_probe=lambda: True) as capability:
        with pytest.raises(RuntimeError, match='reservation_contended'):
            with reserve(port, capability):
                pytest.fail('contended reservation admitted')
    assert port.current == {'owner_token': 'existing'} and port.releases == 0
