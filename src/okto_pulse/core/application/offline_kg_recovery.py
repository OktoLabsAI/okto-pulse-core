"""Core policy joining sealed recovery authority to the exact queue worker."""

from contextlib import contextmanager

from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor
from okto_pulse.core.kg.recovery_execution import validate_recovery_execution_capability
from okto_pulse.core.kg.single_writer_lock import KGAdministrativeOperationReservation, MAX_TTL_SECONDS
from okto_pulse.core.ports.consolidation import ConsolidationClaimScope


class _Reservation:
    def __init__(self, scope, capability, lock, token, processor, ttl):
        self._scope, self._capability, self._lock = scope, capability, lock
        self._token, self._processor, self._ttl = token, processor, ttl
        self._active, self._running = True, False

    @property
    def claim_scope(self):
        return self._scope

    def is_authorized(self):
        try:
            if not self._active or not validate_recovery_execution_capability(self._capability,
                    board_id=self._scope.board_id, run_id=self._scope.source.removeprefix('rebuild:')):
                return False
            current = self._lock.inspect(board_id=self._scope.board_id)
            return bool(current is not None
                and current.operation == 'kg02_rebuild_reservation:' + self._scope.source.removeprefix('rebuild:')
                and self._lock.is_owner(self._scope.board_id, self._token))
        except BaseException:
            return False

    async def process_next(self):
        if not self.is_authorized():
            raise RuntimeError('offline_consolidation_authority_lost')
        if self._running:
            raise RuntimeError('offline_consolidation_batch_already_running')
        self._running = True
        try:
            if not self._lock.renew(board_id=self._scope.board_id, owner_token=self._token,
                    ttl_seconds=self._ttl) or not self.is_authorized():
                raise RuntimeError('offline_consolidation_authority_lost')
            # The worker may have a durable ACK even when authority was lost
            # just after commit. Preserve its typed disposition; do not turn
            # a committed result into an apparent pre-commit refusal. Further
            # batches and normal context exit still recheck live authority.
            return await self._processor.process_exact_batch(claim_scope=self._scope,
                reservation_authority_probe=self.is_authorized)
        finally:
            self._running = False


@contextmanager
def reserve_consolidation(*, claim_scope, recovery_capability, write_lock_port,
        relational_scope_factory, owner_id, ttl_seconds):
    if (type(claim_scope) is not ConsolidationClaimScope or claim_scope.reservation_lineage_id is None
            or type(owner_id) is not str or not owner_id.strip() or len(owner_id) > 256
            or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_TTL_SECONDS
            or not callable(relational_scope_factory) or write_lock_port is None):
        raise ValueError('offline_consolidation_configuration_invalid')
    run_id = claim_scope.source.removeprefix('rebuild:')
    if not validate_recovery_execution_capability(recovery_capability,
            board_id=claim_scope.board_id, run_id=run_id):
        raise RuntimeError('offline_consolidation_recovery_authority_required')
    lock = KGAdministrativeOperationReservation(write_lock_port=write_lock_port)
    acquired = lock.acquire(board_id=claim_scope.board_id, operation='kg02_rebuild_reservation:' + run_id,
        owner_id=owner_id, ttl_seconds=ttl_seconds, admin_lane=True)
    if not acquired.acquired or not acquired.owner_token:
        raise RuntimeError('offline_consolidation_reservation_contended')
    reservation = None
    failed = False
    try:
        reservation = _Reservation(claim_scope, recovery_capability, lock, acquired.owner_token,
            ConsolidationProcessor(relational_scope_factory, batch_size=1), ttl_seconds)
        if not reservation.is_authorized():
            raise RuntimeError('offline_consolidation_authority_lost')
        yield reservation
        if not reservation.is_authorized():
            raise RuntimeError('offline_consolidation_authority_lost')
    except BaseException:
        failed = True
        raise
    finally:
        if reservation is not None:
            reservation._active = False
        released = lock.release(board_id=claim_scope.board_id, owner_token=acquired.owner_token)
        if not released and not failed:
            raise RuntimeError('offline_consolidation_reservation_release_failed')
