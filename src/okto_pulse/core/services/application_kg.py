"""Application-facing KG facade.

This module centralizes the remaining calls from application use cases into the
concrete KG implementation. It is intentionally thin: behavior and exception
surfaces stay owned by the existing KG services while the pure use cases depend
on this transitional application-facing facade instead of outbound modules.
"""

from __future__ import annotations

from typing import Any, Callable


def build_cognitive_readiness_service() -> Any:
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        default_rebuild_base_dir,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
    )


def build_cognitive_action_center_read_model(readiness_service: Any) -> Any:
    from okto_pulse.core.kg.cognitive_action_center import CognitiveActionCenterReadModel

    return CognitiveActionCenterReadModel(readiness_service)


async def evaluate_bug_cognitive_closure(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.bug_cognitive_closure import (
        evaluate_bug_cognitive_closure as _evaluate,
    )

    return await _evaluate(*args, **kwargs)


async def begin_consolidation(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.primitives import begin_consolidation as _begin

    return await _begin(*args, **kwargs)


async def propose_reconciliation(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.primitives import propose_reconciliation as _propose

    return await _propose(*args, **kwargs)


async def commit_consolidation(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.primitives import commit_consolidation as _commit

    return await _commit(*args, **kwargs)


async def run_with_commit_lock_and_retry(
    board_id: str,
    operation: Callable[[], Any],
) -> Any:
    from okto_pulse.core.kg.commit_coordinator import (
        run_with_commit_lock_and_retry as _run_with_lock,
    )

    return await _run_with_lock(board_id, operation)


async def list_consolidation_audit(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.dashboard_readers import (
        list_consolidation_audit as _list_audit,
    )

    return await _list_audit(*args, **kwargs)


async def list_all_board_ids(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.dashboard_readers import list_all_board_ids as _list_ids

    return await _list_ids(*args, **kwargs)


def normalize_graph_layer(graph_layer: str) -> str:
    from okto_pulse.core.kg.kg_service import normalize_graph_layer as _normalize

    return _normalize(graph_layer)


def query_global(
    query: str,
    *,
    user_boards: list[str],
    top_k: int,
    min_similarity: float,
    graph_layer: str,
) -> list[Any]:
    from okto_pulse.core.kg.kg_service import get_kg_service

    return get_kg_service().query_global(
        query,
        user_boards=user_boards,
        top_k=top_k,
        min_similarity=min_similarity,
        graph_layer=graph_layer,
    )


async def start_historical_consolidation(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import (
        start_historical_consolidation as _start,
    )

    return await _start(*args, **kwargs)


async def cancel_historical(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import cancel_historical as _cancel

    return await _cancel(*args, **kwargs)


async def get_historical_progress(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import get_historical_progress as _progress

    return await _progress(*args, **kwargs)


async def right_to_erasure(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import right_to_erasure as _erase

    return await _erase(*args, **kwargs)


async def list_pending_entries(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.dashboard_readers import list_pending_entries as _list

    return await _list(*args, **kwargs)


async def build_pending_tree(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.dashboard_readers import build_pending_tree as _build

    return await _build(*args, **kwargs)


async def retry_pending_entry(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import retry_pending_entry as _retry

    return await _retry(*args, **kwargs)


async def boost_node(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import boost_node as _boost

    return await _boost(*args, **kwargs)


async def list_stale_canonical_parity(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.stale_canonical_parity import (
        list_stale_canonical_parity as _list,
    )

    return await _list(*args, **kwargs)


async def list_canonical_partition_integrity(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.canonical_partition_integrity import (
        list_canonical_partition_integrity as _list,
    )

    return await _list(*args, **kwargs)


async def list_digest_layer_mismatches(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.global_discovery.layer_parity import (
        list_digest_layer_mismatches as _list,
    )

    return await _list(*args, **kwargs)
