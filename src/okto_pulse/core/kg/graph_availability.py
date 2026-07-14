"""Shared open-or-classify helper for the per-board KG read paths (F4).

A single classification site (TR1) that recognizes the fail-closed RuntimeError
raised by ``schema._raise_existing_graph_open_failed`` when a board's on-disk
graph EXISTS but cannot be opened, and turns it into a typed
``KGToolError(code="graph_unavailable", details={"graph_state": ...})`` so the
two divergent read paths surface the SAME structured error:

* the vector path (``KGService.find_similar_decisions`` ->
  ``search.find_similar_nodes_by_type``) which today swallows the open failure
  into a silent ``[]``, and
* the Cypher family (routed through ``KGService._cached_call``) which today
  flattens it into a generic ``graph_error``.

Scope guard (FR9/TR3): this module only CLASSIFIES an already-raised error. It
never opens, bootstraps, purges, repairs, or weakens the graph-open guard
(``schema._raise_existing_graph_open_failed`` / ``_graph_needs_bootstrap`` stay
untouched). Recovery remains the KG-02 flow. This module does NOT define the
``graph_unavailable`` response envelope contract beyond ``details["graph_state"]``.

Narrow detection (TR3): the fail-closed guard raises a plain ``RuntimeError``
with no marker attribute, so -- mirroring the established codebase idiom in
``global_discovery.outbox_worker.BOARD_READ_ERROR_MARKERS`` -- detection is a
lowercased-substring match against the guard's fixed message. Transient /
non-open failures (lock contention, a missing vector index, a malformed Cypher
query) do NOT match and are left to the caller's existing handling.

Single degraded predicate (TR2): the degraded ``graph_state`` is constrained to
``backpressure._RISK_STATE_HARD_REJECT`` (imported, never redefined) -- the two
``HealthState`` members ``recovery_needed`` / ``quarantined``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.kg.health_state import HealthState

if TYPE_CHECKING:  # avoid an import cycle with kg_service at module load
    from okto_pulse.core.kg.kg_service import KGToolError

def is_graph_unavailable_error(exc: BaseException) -> bool:
    """Return whether an edition adapter reported graph unavailability."""

    from okto_pulse.core.kg.interfaces.graph_errors import GraphUnavailable

    return isinstance(exc, GraphUnavailable)


def derive_graph_state(candidate: str | None = None) -> str:
    """Return a degraded ``graph_state`` constrained to ``_RISK_STATE_HARD_REJECT``.

    ``candidate`` is honored only when it is already a hard-reject member
    (``recovery_needed`` / ``quarantined``). Otherwise -- including the common
    case where the precise live state cannot be resolved synchronously from a
    read path (``get_kg_health`` is async and session-bound) -- this falls back
    to the conservative fail-closed member ``recovery_needed`` (FR3). The result
    is ALWAYS a member of ``_RISK_STATE_HARD_REJECT``.
    """
    if candidate is not None and candidate in _RISK_STATE_HARD_REJECT:
        return candidate
    return HealthState.RECOVERY_NEEDED.value


def graph_unavailable_error(
    board_id: str,
    *,
    graph_state: str | None = None,
) -> KGToolError:
    """Build the typed ``graph_unavailable`` error for an unopenable board graph.

    ``details["graph_state"]`` is derived via :func:`derive_graph_state` so it is
    always a member of ``_RISK_STATE_HARD_REJECT`` (FR3/FR7). ``KGToolError`` is
    imported lazily to keep this low-level module free of an import cycle with
    ``kg_service`` (which imports this helper from its read paths).
    """
    from okto_pulse.core.kg.kg_service import KGToolError

    state = derive_graph_state(graph_state)
    message = (
        f"Knowledge graph for board {board_id} could not be opened "
        f"(graph_state={state}); the on-disk graph is preserved. "
        "Use the explicit KG Health recovery flow."
    )
    return KGToolError(
        code="graph_unavailable",
        message=message,
        details={"graph_state": state},
    )


def open_or_classify(read: Callable[[], Any], *, board_id: str) -> Any:
    """Execute ``read()`` and classify a fail-closed graph-open failure.

    On success returns the read result UNCHANGED -- including an empty list: a
    healthy graph that opens with zero matching rows is NOT an error (FR8). If
    ``read()`` raises the fail-closed graph-open ``RuntimeError``, raises the
    typed ``graph_unavailable`` ``KGToolError``; any other exception propagates
    unchanged so the caller's existing handling (``[]`` for the vector path,
    ``graph_error`` for the Cypher path) is preserved (FR5/FR6).
    """
    try:
        return read()
    except Exception as exc:
        if is_graph_unavailable_error(exc):
            raise graph_unavailable_error(board_id) from exc
        raise
