"""Startup KG schema sweep helper."""

from __future__ import annotations

import logging
from typing import Any


async def run_startup_schema_sweep(
    *,
    session_factory: Any,
    logger: logging.Logger,
) -> None:
    """Run the idempotent per-board KG schema sweep used by the legacy lifespan."""

    from sqlalchemy import Column, MetaData, String, Table, select

    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.startup_schema_sweep import sweep_board_schemas

    boards = Table(
        "boards",
        MetaData(),
        Column("id", String),
    )

    async with session_factory() as session:
        board_ids = (await session.execute(select(boards.c.id))).scalars().all()

    kg_registry = get_kg_registry()
    await sweep_board_schemas(
        board_ids,
        graph_path_resolver=kg_registry.graph_path_resolver,
        graph_schema_manager=kg_registry.graph_schema_manager,
        logger=logger,
    )


__all__ = ["run_startup_schema_sweep"]
