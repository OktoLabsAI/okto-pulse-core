"""Opaque authority for an offline governed KG rebuild execution.

The rebuild queue reservation fences graph consumers, but it is not a
transactional barrier for every relational SDLC writer.  Until that broader
barrier exists, a rebuild may execute only from the dedicated recovery-only
runner after it has proved that the application and its writers are offline.

HTTP/MCP request data cannot mint this authority.  Trusted in-process recovery
composition opens :func:`issue_recovery_execution_capability` around exactly
one board operation and supplies a live probe for its external recovery lease.
The returned value is deliberately opaque: validators accept only the exact
private implementation sealed by this module, for the exact board, while the
issuing scope and its lifetime probe remain live.
"""

from __future__ import annotations

import secrets
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager


_CAPABILITY_SEAL = object()


class _RecoveryExecutionCapability:
    """Module-sealed, board-bound recovery authority.

    The class is intentionally private and validators require exact type
    identity.  A nonce prevents accidental equality/copy semantics; authority
    comes from the private seal plus the live issuing state, never from a
    serialisable token.
    """

    __slots__ = (
        "_board_id",
        "_claimed_run_id",
        "_lifetime_probe",
        "_lock",
        "_nonce",
        "_revoked",
        "_seal",
    )

    def __init__(
        self,
        *,
        board_id: str,
        lifetime_probe: Callable[[], bool],
        seal: object,
    ) -> None:
        if seal is not _CAPABILITY_SEAL:
            raise TypeError("recovery_execution_capability_issuer_required")
        self._seal = seal
        self._board_id = board_id
        self._claimed_run_id: str | None = None
        self._lifetime_probe = lifetime_probe
        self._nonce = secrets.token_urlsafe(24)
        self._revoked = False
        self._lock = threading.Lock()

    def _revoke(self) -> None:
        with self._lock:
            self._revoked = True

    def _scope_is_live(self, board_id: str) -> bool:
        with self._lock:
            if self._revoked:
                return False
            if self._seal is not _CAPABILITY_SEAL:
                return False
            if self._board_id != board_id:
                return False
        try:
            probe_live = bool(self._lifetime_probe())
        except BaseException:
            # The external offline/recovery lease is authority, not telemetry.
            # An unavailable probe therefore fails closed.
            return False
        if not probe_live:
            return False
        # The scope may be revoked while a slow external probe runs in another
        # thread.  Linearize a successful validation after the probe: scope
        # exit wins the race and can never be followed by a stale True result.
        with self._lock:
            return bool(
                not self._revoked
                and self._seal is _CAPABILITY_SEAL
                and self._board_id == board_id
            )

    def _is_valid_for(self, board_id: str, run_id: str) -> bool:
        if not run_id or not self._scope_is_live(board_id):
            return False
        with self._lock:
            if self._revoked or self._board_id != board_id:
                return False
            if self._claimed_run_id is None:
                # A capability is one-shot even while its issuing scope stays
                # open. Claim the first recoverable operation atomically.
                self._claimed_run_id = run_id
            return self._claimed_run_id == run_id


@contextmanager
def issue_recovery_execution_capability(
    *,
    board_id: str,
    lifetime_probe: Callable[[], bool],
) -> Iterator[object]:
    """Issue one non-serialisable recovery capability for ``board_id``.

    ``lifetime_probe`` must continuously prove the external recovery-only
    lease (server ports down, exact launcher lock/heartbeat alive, no writer
    registry started).  The capability is revoked unconditionally when the
    scope exits and is safe to pass across ``asyncio.to_thread`` boundaries.
    """

    normalized_board_id = str(board_id).strip()
    if not normalized_board_id:
        raise ValueError("recovery_execution_board_id_required")
    if not callable(lifetime_probe):
        raise TypeError("recovery_execution_lifetime_probe_required")
    capability = _RecoveryExecutionCapability(
        board_id=normalized_board_id,
        lifetime_probe=lifetime_probe,
        seal=_CAPABILITY_SEAL,
    )
    try:
        yield capability
    finally:
        capability._revoke()


def validate_recovery_execution_capability(
    capability: object | None,
    *,
    board_id: str,
    run_id: str,
) -> bool:
    """Return whether ``capability`` authorises this exact board/run pair."""

    if type(capability) is not _RecoveryExecutionCapability:
        return False
    return capability._is_valid_for(
        str(board_id).strip(),
        str(run_id).strip(),
    )


def check_recovery_execution_capability_scope(
    capability: object | None,
    *,
    board_id: str,
) -> bool:
    """Probe board/lifetime authority without claiming an operation id."""

    if type(capability) is not _RecoveryExecutionCapability:
        return False
    return capability._scope_is_live(str(board_id).strip())


__all__ = [
    "check_recovery_execution_capability_scope",
    "issue_recovery_execution_capability",
    "validate_recovery_execution_capability",
]
