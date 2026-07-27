"""Application-facing KG facade.

This module centralizes the remaining calls from application use cases into the
concrete KG implementation. It is intentionally thin: behavior and exception
surfaces stay owned by the existing KG services while the pure use cases depend
on this transitional application-facing facade instead of outbound modules.
"""

from __future__ import annotations

from typing import Any, Callable


def create_provider_registry(**providers: Any) -> Any:
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry

    return KGProviderRegistry(**providers)


def configure_provider_registry(
    *,
    base_registry: Any | None = None,
    **kwargs: Any,
) -> None:
    from okto_pulse.core.kg.interfaces.registry import configure_kg_registry

    configure_kg_registry(base_registry=base_registry, **kwargs)


def configure_commit_coordinator() -> None:
    """Install a fresh process runtime for Core's graph commit policy."""

    from okto_pulse.core.kg.commit_coordinator import (
        CommitCoordinator,
        register_commit_coordinator,
    )

    register_commit_coordinator(CommitCoordinator())


def configure_write_barrier(mode: str) -> None:
    """Install an instance-owned write barrier configured by the edition."""

    from okto_pulse.core.kg.write_barrier import (
        WriteBarrierRuntime,
        configure_write_barrier_runtime,
    )

    configure_write_barrier_runtime(WriteBarrierRuntime(mode))


def revalidate_board_graph_write_lease(
    board_id: str,
    *,
    failure_phase: str = "graph_statement_precommit",
) -> None:
    """Public edition boundary for the final per-statement fence check."""

    from okto_pulse.core.kg.guarded_write import (
        revalidate_active_board_write_lease,
    )

    revalidate_active_board_write_lease(
        board_id,
        failure_phase=failure_phase,
    )


def drain_kg_health_probes(*, timeout_s: float = 30.0) -> int:
    """Drain health jobs owned by the current runtime composition.

    Editions call this public lifecycle facade before closing their concrete
    graph handles.  The daemon-pool implementation remains private to Core;
    the edition only observes the number of jobs left at the deadline.
    """

    from okto_pulse.core.services.kg_health_service import (
        drain_health_probe_runtime,
    )

    return drain_health_probe_runtime(timeout_s=timeout_s)


async def migrate_board_graph_schema(board_id: str) -> dict[str, Any]:
    """Apply the composed graph schema manager to one board."""

    from okto_pulse.core.kg.interfaces import get_kg_registry

    summary = await get_kg_registry().require_graph_schema_manager().migrate(board_id)
    summary.setdefault("duration_ms", 0)
    return summary


def get_current_provider_registry() -> Any:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry()


def build_cognitive_readiness_service() -> Any:
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        require_rebuild_audit_artifact_store,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )
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


def create_deterministic_worker(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker

    return DeterministicWorker(*args, **kwargs)


def create_consolidation_processor(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.application.processors import ConsolidationProcessor

    return ConsolidationProcessor(*args, **kwargs)


def signal_consolidation_worker() -> None:
    from okto_pulse.core.application.runtime_workers import signal_runtime_worker

    signal_runtime_worker("consolidation_worker")


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


async def mutate_boost_node_graph(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import (
        mutate_boost_node_graph as _mutate,
    )

    return await _mutate(*args, **kwargs)


def stage_boost_node_audit(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.governance import (
        stage_boost_node_audit as _stage,
    )

    return _stage(*args, **kwargs)


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


async def get_canonical_partition_integrity_detail(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.canonical_partition_integrity import (
        get_canonical_partition_integrity_detail as _get,
    )

    return await _get(*args, **kwargs)


async def list_digest_layer_mismatches(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.global_discovery.layer_parity import (
        list_digest_layer_mismatches as _list,
    )

    return await _list(*args, **kwargs)


def max_orphan_sample_limit() -> int:
    from okto_pulse.core.kg.orphan_integrity import MAX_ORPHAN_SAMPLE_LIMIT

    return MAX_ORPHAN_SAMPLE_LIMIT


def create_orphan_backfill_reconciler() -> Any:
    from okto_pulse.core.kg.orphan_integrity import OrphanBackfillReconciler

    return OrphanBackfillReconciler()


def audit_originates_from_contract(*args: Any, **kwargs: Any) -> Any:
    from okto_pulse.core.kg.originates_from_audit import (
        audit_originates_from_contract as _audit,
    )

    return _audit(*args, **kwargs)
