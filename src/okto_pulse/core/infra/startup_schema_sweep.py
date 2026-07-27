"""Startup KG schema sweep helper."""

from __future__ import annotations

import logging
from typing import Any


async def run_startup_schema_sweep(
    *,
    uow_factory: Any | None = None,
    logger: logging.Logger,
) -> None:
    """Run the idempotent per-board KG schema sweep used by the legacy lifespan."""

    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.startup_schema_sweep import sweep_board_schemas
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    factory = uow_factory or resolve_unit_of_work_factory()
    realm_scope = factory.resolve_realm_scope()
    async with factory(realm_scope=realm_scope) as uow:
        board_ids = await uow.services.list_board_ids()

    kg_registry = get_kg_registry()
    await sweep_board_schemas(
        board_ids,
        graph_runtime_store=kg_registry.graph_runtime_store,
        graph_schema_manager=kg_registry.graph_schema_manager,
        logger=logger,
    )


__all__ = ["run_startup_schema_sweep"]
