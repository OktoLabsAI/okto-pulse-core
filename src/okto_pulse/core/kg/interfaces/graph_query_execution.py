"""Edition boundary for a shared foreground query execution deadline."""
from contextlib import AbstractContextManager
from typing import Protocol


class GraphQueryExecution(Protocol):
    def scope(self, board_id: str, *, timeout_ms: int) -> AbstractContextManager[None]:
        """Constrain all synchronous reads in one already-authorized query.

        The edition shares one finite deadline across statements, retries and
        vector fallback, propagates remaining time to native execution, and
        refuses completion after expiry. Nested scopes cannot extend a deadline
        or change Boards. This grants no authority and imposes no count quota.
        Native foreground work limits also bound intermediate rows, traversal
        expansions/paths and query working memory. A refusal is explicit and
        must never be converted into a complete empty answer. These limits do
        not alter internal rebuild-reader policies outside the query scope.
        Timeout is an integer in 1..30000ms, already narrowed by Board policy.
        Callers must drain the owned worker on cancellation; a scope does not
        promise preemption of arbitrary external providers or operating-system I/O.
        """
        ...
