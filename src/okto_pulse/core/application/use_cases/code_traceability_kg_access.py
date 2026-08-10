"""Transport-neutral authorization for Code Traceability KG projections."""

from __future__ import annotations

from copy import deepcopy
import json
from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.code_traceability_kg import (
    CODE_TRACEABILITY_KG_READ_PERMISSIONS,
    CodeTraceabilityKGReadDecision,
    code_traceability_kg_read_decision,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class EvaluateCodeTraceabilityKGReadAccessUseCase:
    """Evaluate the all-of CT read contract without denying legacy KG reads."""

    async def execute(
        self,
        *,
        actor: ActorContext,
        board_id: str | None,
        uow: PulseUnitOfWork | None = None,
    ) -> CodeTraceabilityKGReadDecision:
        try:
            permissions = await resolve_actor_permissions(actor, uow, board_id)
        except Exception:
            return code_traceability_kg_read_decision(
                (),
                authority_resolved=False,
            )

        granted = tuple(
            permission
            for permission in CODE_TRACEABILITY_KG_READ_PERMISSIONS
            if decide_authorization(
                actor,
                PermissionRequirement(permission),
                permissions,
            ).allowed
        )
        return code_traceability_kg_read_decision(granted)


def require_code_traceability_safe_arbitrary_query(
    decision: CodeTraceabilityKGReadDecision,
) -> None:
    """Deny an unsanitized query without the complete CT read authority.

    A materialization probe followed by a raw query has an unavoidable TOCTOU
    window unless both operations share the graph writer's snapshot/lock.  The
    current graph port exposes no such atomic primitive, so arbitrary Cypher,
    natural and reflective queries fail closed whenever any CT leaf is absent.
    Filtered generic reads remain available for legacy non-CT material.
    """

    if decision.allowed:
        return
    raise PermissionDeniedError(
        json.dumps(
            {
                "error": "permission_denied",
                "reason": (
                    "code_traceability_kg_authority_unresolved"
                    if not decision.authority_resolved
                    else "code_traceability_kg_read_permissions_missing"
                ),
                "required_permissions": list(
                    CODE_TRACEABILITY_KG_READ_PERMISSIONS
                ),
                "missing_permissions": list(decision.missing_permissions),
            },
            sort_keys=True,
        )
    )


# Graph-derived aggregates can reveal whether/how much CT material exists even
# when every bounded node/edge reader is filtered. Operational queue/debt
# counters are intentionally absent: they remain part of the health stop-rule.
_CODE_TRACEABILITY_GRAPH_METRIC_KEYS = frozenset(
    {
        "total_nodes",
        "nodes_total",
        "node_count",
        "node_count_by_type",
        "node_counts_by_type",
        "edges_total",
        "edge_count",
        "edge_count_by_type",
        "edge_counts_by_type",
        "edge_count_by_layer",
        "edge_count_by_rule",
        "deterministic_edge_ratio",
        "cognitive_edge_ratio",
        "fallback_edge_ratio",
        "default_score_count",
        "default_score_ratio",
        "avg_relevance",
        "contradict_warn_count",
        "nodes_recomputed_in_last_tick",
        "materialized_node_count",
    }
)


def mask_code_traceability_graph_metrics(
    payload: dict[str, Any],
    decision: CodeTraceabilityKGReadDecision,
) -> dict[str, Any]:
    """Remove graph aggregates while preserving operational health stop-rules."""

    if decision.allowed:
        return payload

    def _mask(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: _mask(item)
                for key, item in value.items()
                if key not in _CODE_TRACEABILITY_GRAPH_METRIC_KEYS
            }
        if isinstance(value, list):
            return [_mask(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_mask(item) for item in value)
        return value

    masked = _mask(deepcopy(payload))
    masked["metric_status"] = "unavailable"
    masked["code_traceability_metric_visibility"] = {
        "status": "unavailable",
        "reason": (
            "code_traceability_kg_authority_unresolved"
            if not decision.authority_resolved
            else "code_traceability_kg_read_permissions_missing"
        ),
        "required_permissions": list(CODE_TRACEABILITY_KG_READ_PERMISSIONS),
    }
    return masked


__all__ = [
    "EvaluateCodeTraceabilityKGReadAccessUseCase",
    "mask_code_traceability_graph_metrics",
    "require_code_traceability_safe_arbitrary_query",
]
