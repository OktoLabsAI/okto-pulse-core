"""Narrow public accessors for edition-composed KG runtime ports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _registry() -> Any:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry()


@dataclass(frozen=True, slots=True)
class KGRuntimeSnapshot:
    graph_store_name: str | None
    embedding_provider: Any
    session_ttl_seconds: int
    kg_base_dir: str | None
    graph_initialized: bool | None


def resolve_graph_lifecycle() -> Any:
    return _registry().graph_lifecycle


def resolve_cypher_executor() -> Any:
    return _registry().cypher_executor


def resolve_graph_transaction() -> Any:
    return _registry().graph_transaction


def resolve_graph_schema_manager() -> Any:
    return _registry().graph_schema_manager


def require_rebuild_audit_artifact_store() -> Any:
    return _registry().require_rebuild_audit_artifact_store()


def describe_current_embedding_provider() -> dict[str, Any]:
    """Return metadata for the edition-composed embedding provider."""

    from okto_pulse.core.kg.interfaces.embedding import describe_embedding_provider

    return describe_embedding_provider(_registry().embedding_provider)


def snapshot_kg_runtime(*, board_id: str | None = None) -> KGRuntimeSnapshot:
    registry = _registry()
    config = registry.config
    graph_store = registry.graph_store
    return KGRuntimeSnapshot(
        graph_store_name=type(graph_store).__name__ if graph_store else None,
        embedding_provider=registry.embedding_provider,
        session_ttl_seconds=(config.kg_session_ttl_seconds if config else 900),
        kg_base_dir=str(config.kg_base_dir) if config else None,
        graph_initialized=(
            registry.graph_runtime_store.exists(board_id)
            if board_id is not None
            else None
        ),
    )


__all__ = [
    "KGRuntimeSnapshot",
    "describe_current_embedding_provider",
    "require_rebuild_audit_artifact_store",
    "resolve_cypher_executor",
    "resolve_graph_lifecycle",
    "resolve_graph_schema_manager",
    "resolve_graph_transaction",
    "snapshot_kg_runtime",
]
