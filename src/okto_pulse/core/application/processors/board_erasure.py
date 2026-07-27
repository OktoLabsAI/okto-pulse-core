"""Durable board-erasure continuation processor.

The source Board and relational KG rows are committed before external stores
are touched. A row in ``kg_board_erasure_jobs`` bridges that transaction
boundary so process death, cancellation, or a transient storage error can be
retried without loading the already-deleted Board.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from okto_pulse.core.kg.governance import (
    BoardErasureLockContention,
    board_erasure_scope,
    record_board_erasure_failure,
    right_to_erasure,
)
from okto_pulse.core.ports.kg_governance import get_kg_governance_store
from okto_pulse.core.ports.runtime_workers import WorkerClockPort

logger = logging.getLogger("okto_pulse.kg.board_erasure")


class BoardErasureProcessor:
    """Retry due physical-erasure jobs under the same writer fences as DELETE."""

    def __init__(
        self,
        relational_scope_factory=None,
        *,
        batch_size: int = 10,
        clock: WorkerClockPort | None = None,
    ) -> None:
        if relational_scope_factory is None:
            from okto_pulse.core.ports.relational_runtime import get_db_session

            relational_scope_factory = get_db_session
        if isinstance(batch_size, bool) or not isinstance(batch_size, int):
            raise TypeError("batch_size must be an int")
        if not 1 <= batch_size <= 100:
            raise ValueError("batch_size must be within 1..100")
        self._relational_scope_factory = relational_scope_factory
        self._batch_size = batch_size
        self._clock = clock

    def _now(self) -> datetime:
        return (
            self._clock.now() if self._clock is not None else datetime.now(timezone.utc)
        )

    async def _list_due(self):
        async with self._relational_scope_factory() as db:
            return await get_kg_governance_store().list_due_board_erasure_jobs(
                db,
                now=self._now(),
                limit=self._batch_size,
            )

    async def _get_current(self, board_id: str):
        async with self._relational_scope_factory() as db:
            return await get_kg_governance_store().get_board_erasure_job(
                db,
                board_id=board_id,
            )

    async def _complete(self, board_id: str) -> bool:
        store = get_kg_governance_store()
        async with self._relational_scope_factory() as db:
            completed = await store.complete_board_erasure_job(
                db,
                board_id=board_id,
            )
            if completed:
                await store.commit(db)
            return completed

    async def _record_failure(self, board_id: str, error: Exception) -> None:
        store = get_kg_governance_store()
        try:
            async with self._relational_scope_factory() as db:
                await record_board_erasure_failure(db, board_id, error)
                await store.commit(db)
        except Exception:
            # The durable row already exists. Leaving it immediately due is
            # safer than hiding the original physical-storage failure.
            logger.exception(
                "board_erasure.retry_state_update_failed board=%s",
                board_id,
            )

    async def process_once(self) -> int:
        completed_count = 0
        for scheduled in await self._list_due():
            board_id = scheduled.board_id
            try:
                async with board_erasure_scope(
                    board_id,
                    actor_id="system:board-erasure-worker",
                ) as lease:
                    # Another process may have completed the same row after this
                    # iteration read its due batch but before the fence arrived.
                    if await self._get_current(board_id) is None:
                        continue
                    await right_to_erasure(
                        None,
                        board_id,
                        strict=True,
                        commit=False,
                        global_writer_guarded=True,
                        purge_relational=False,
                    )
                    lease.ensure_owned()
                    if not await self._complete(board_id):
                        raise RuntimeError(
                            f"board_erasure_continuation_missing board={board_id}"
                        )
                    completed_count += 1
                    logger.info(
                        "board_erasure.retry_completed board=%s attempts=%d",
                        board_id,
                        scheduled.attempts,
                    )
            except BoardErasureLockContention:
                # A request or peer worker owns the board. The persistent row
                # remains due and will be re-evaluated on the next iteration.
                logger.info("board_erasure.retry_contended board=%s", board_id)
            except Exception as exc:
                await self._record_failure(board_id, exc)
                logger.error(
                    "board_erasure.retry_failed board=%s error=%r",
                    board_id,
                    exc,
                )
        return completed_count


__all__ = ["BoardErasureProcessor"]
