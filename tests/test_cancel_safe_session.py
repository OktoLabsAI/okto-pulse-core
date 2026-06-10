"""Tests do cancel_safe_session — close de sessão blindado contra hard-cancel.

O bug de produção: o cancel scope do middleware (desconexão de cliente SSE)
aterrissava dentro do ``session.close()`` do ``async with``, e a conexão
nunca voltava ao pool. O contrato aqui: mesmo com a task cancelada no meio
do bloco, a conexão SEMPRE volta ao pool (checkedout == 0).
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from okto_pulse.core.infra.database import (
    cancel_safe_session,
    get_engine,
    get_pool_status,
)

pytestmark = pytest.mark.asyncio(loop_scope="session")


def _checkedout() -> int:
    return get_engine().sync_engine.pool.checkedout()


async def test_normal_path_returns_connection_to_pool():
    baseline = _checkedout()
    async with cancel_safe_session() as session:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar() == 1
        assert _checkedout() == baseline + 1
    await asyncio.sleep(0.1)
    assert _checkedout() == baseline


async def test_hard_cancel_inside_block_still_returns_connection():
    baseline = _checkedout()
    entered = asyncio.Event()

    async def _victim():
        async with cancel_safe_session() as session:
            await session.execute(text("SELECT 1"))  # força checkout
            entered.set()
            await asyncio.sleep(30)  # o cancel aterrissa aqui

    task = asyncio.create_task(_victim())
    await asyncio.wait_for(entered.wait(), timeout=5.0)
    assert _checkedout() == baseline + 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # O close roda numa task própria blindada — dá um tick para concluir.
    for _ in range(50):
        if _checkedout() == baseline:
            break
        await asyncio.sleep(0.05)
    assert _checkedout() == baseline, (
        f"conexão vazou após hard-cancel: pool={get_pool_status()}"
    )


async def test_cancel_during_query_execution_still_returns_connection():
    """Cancela enquanto a query está em voo (o caso real do SSE poll)."""
    baseline = _checkedout()
    started = asyncio.Event()

    async def _victim():
        async with cancel_safe_session() as session:
            started.set()
            # Query recursiva longa o suficiente para o cancel pegá-la em voo
            # na maioria das execuções; quando termina antes, o cancel pega o
            # sleep — ambos os caminhos devem devolver a conexão.
            await session.execute(text(
                "WITH RECURSIVE c(x) AS "
                "(SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 2000000) "
                "SELECT count(*) FROM c"
            ))
            await asyncio.sleep(30)

    task = asyncio.create_task(_victim())
    await asyncio.wait_for(started.wait(), timeout=5.0)
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    for _ in range(100):
        if _checkedout() == baseline:
            break
        await asyncio.sleep(0.05)
    assert _checkedout() == baseline, (
        f"conexão vazou após cancel em query: pool={get_pool_status()}"
    )
