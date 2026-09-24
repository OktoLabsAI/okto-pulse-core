"""Edition boundary for graph diagnostics that cannot perform maintenance."""

from contextlib import AbstractContextManager
from typing import Protocol


class GraphHealthObservation(Protocol):
    def scope(
        self, board_id: str, *, timeout_seconds: float = 0.35,
    ) -> AbstractContextManager[None]:
        """Constrain synchronous graph providers for one Health worker step.

        Reads may join existing read participants, but must never recover an
        opening failure by checkpointing, opening a writer, initializing storage
        or closing rollback eligibility. Unobservable data stays unavailable.
        The edition owns isolation and restoration, including nested scopes and
        exceptions. This scope grants no authority and changes no ordinary
        foreground read policy outside the diagnostic call.

        Board Cypher queries share the remaining timeout across statements/retries;
        nested scopes cannot extend it. The finite timeout is greater than zero
        and at most five seconds. An expired observation refuses further I/O.
        This does not promise preemption of an operating-system storage call.
        Global graph reads and filesystem enumeration need their own budgets;
        this Board scope does not establish those bounds.
        """
        ...
