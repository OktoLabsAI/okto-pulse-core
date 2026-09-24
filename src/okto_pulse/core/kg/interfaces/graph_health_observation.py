"""Edition boundary for graph diagnostics that cannot perform maintenance."""

from contextlib import AbstractContextManager
from typing import Protocol


class GraphHealthObservation(Protocol):
    def scope(self, board_id: str) -> AbstractContextManager[None]:
        """Constrain synchronous graph providers for one Health worker step.

        Reads may join existing read participants, but must never recover an
        opening failure by checkpointing, opening a writer, initializing storage
        or closing rollback eligibility. Unobservable data stays unavailable.
        The edition owns isolation and restoration, including nested scopes and
        exceptions. This scope grants no authority and changes no ordinary
        foreground read policy outside the diagnostic call.
        """
        ...
