"""critic_evaluate + reflect orchestrator (ideação db8e984f)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Any, Callable, Mapping

from okto_pulse.core.runtime_context import runtime_state

from .interfaces import (
    Adequacy,
    CriticAction,
    CriticDecision,
    ReflectResult,
)
from okto_pulse.core.kg.interfaces.reflective_query import (
    ReflectiveCriticPort,
    ReflectiveCriticRequest,
    ReflectiveRetrievalPort,
    ReflectiveRetrievalRequest,
    ReflectiveTelemetryPort,
)

logger = logging.getLogger("okto_pulse.kg.retrieve_critic")

#: Ceiling for expand_hops action — matches the adaptive_hops ceiling.
_HOPS_CEILING = 3

#: LLM critic contract.
CriticFn = Callable[[str, list[dict]], dict]

#: Caller-wired retrieval.
RetrievalFn = Callable[..., list[dict]]

#: Optional audit hook.
AuditSink = Callable[[dict], None]


# ---------------------------------------------------------------------------
# critic_evaluate
# ---------------------------------------------------------------------------


def _rows_signature(rows: list[dict]) -> tuple:
    """Stable hashable signature over rows — (node_id, similarity)
    per row. Used as the LRU cache key companion to query."""
    sig: list[tuple[str, float]] = []
    for r in rows or []:
        nid = str(r.get("node_id", ""))
        sim = float(r.get("similarity", 0.0) or 0.0)
        sig.append((nid, sim))
    return tuple(sig)


class _CallableIdentity:
    """Hashable strong-reference key for any callable, including unhashable ones."""

    __slots__ = ("_callable", "_hash")

    def __init__(self, value: CriticFn) -> None:
        self._callable = value
        self._hash = id(value)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _CallableIdentity)
            and self._callable is other._callable
        )


def critic_evaluate(
    query: str,
    rows: list[dict],
    critic_fn: CriticFn,
) -> CriticDecision:
    """Invoke critic_fn once with (query, rows) and map the JSON
    response to a CriticDecision. Unknown enum values fall back to
    Adequacy.PARTIAL + CriticAction.ACCEPT with a reason string.

    A bounded compatibility cache keeps a strong reference to the callable, so
    two consecutive calls with identical (query, rows_signature, critic_fn)
    return the same decision without the object-id reuse hazard.
    """
    cache_key = (query, _rows_signature(rows), _CallableIdentity(critic_fn))
    cached = _global_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        raw = critic_fn(query, rows)
    except Exception:
        raise  # caller wraps — reflect() catches this.

    if not isinstance(raw, dict):
        raw = {}

    adequacy_str = str(raw.get("adequacy", "")).strip().lower()
    action_str = str(raw.get("suggested_action", "")).strip().lower()
    reason = str(raw.get("reason", "")).strip()
    target_intent = str(raw.get("target_intent") or "").strip() or None
    rewritten_query = str(raw.get("rewritten_query") or "").strip() or None
    confidence_raw = raw.get("confidence")
    try:
        confidence = (
            None if confidence_raw is None else float(confidence_raw)
        )
    except (TypeError, ValueError):
        confidence = None

    try:
        adequacy = Adequacy(adequacy_str)
    except ValueError:
        adequacy = Adequacy.PARTIAL
        reason = (
            f"fallback:unknown_adequacy={adequacy_str!r}; "
            f"original_reason={reason!r}"
        )

    try:
        action = CriticAction(action_str)
    except ValueError:
        action = CriticAction.ACCEPT
        reason = (
            f"{reason} | fallback:unknown_action={action_str!r}"
        )

    decision = CriticDecision(
        adequacy=adequacy,
        reason=reason,
        suggested_action=action,
        confidence=confidence,
        target_intent=target_intent,
        rewritten_query=rewritten_query,
    )
    _global_cache[cache_key] = decision
    # Bounded cache — drop oldest when >64.
    if len(_global_cache) > 64:
        _global_cache.pop(next(iter(_global_cache)))
    return decision


# Simple dict acts as a bounded insertion-ordered cache.  The identity wrapper
# keeps a strong callable reference, so its id cannot be recycled while cached.
_global_cache = runtime_state("kg.retrieve_critic.cache", dict)
_reflective_cache = runtime_state("kg.retrieve_critic.reflective_cache", dict)


def reset_critic_cache() -> None:
    """Test helper — drops the cache between suites."""
    _global_cache.clear()
    _reflective_cache.clear()


# ---------------------------------------------------------------------------
# Action dispatcher
# ---------------------------------------------------------------------------


def _kwargs_for_action(
    action: CriticAction,
    current_hops_hint: int,
    *,
    target_intent: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Map a CriticAction to retrieval_fn kwargs.

    Returns (kwargs, new_hops_hint). The hint is threaded across
    iterations so consecutive EXPAND_HOPS actions stack up to the
    ceiling.
    """
    if action == CriticAction.RETRY_WITH_REWRITE:
        return ({"rewrite": "decompose"}, current_hops_hint)
    if action == CriticAction.EXPAND_HOPS:
        new_hint = min(current_hops_hint + 1, _HOPS_CEILING)
        return ({"fixed_hops_hint": new_hint}, new_hint)
    if action == CriticAction.FALLBACK_SEMANTIC:
        return ({"fallback_semantic": True}, current_hops_hint)
    if action == CriticAction.CHANGE_INTENT and target_intent:
        return ({"target_intent": target_intent}, current_hops_hint)
    # ACCEPT, REJECT and malformed CHANGE_INTENT don't retry.
    return ({}, current_hops_hint)


# ---------------------------------------------------------------------------
# reflect orchestrator
# ---------------------------------------------------------------------------


def _query_hash(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8", errors="ignore")).hexdigest()[:16]


def reflect(
    query: str,
    retrieval_fn: RetrievalFn,
    critic_fn: CriticFn,
    *,
    max_retries: int = 2,
    audit_sink: AuditSink | None = None,
) -> ReflectResult:
    """Agentic retrieve loop with corrective actions.

    - ``max_retries``: total retrieves = 1 + max_retries.
    - ``audit_sink``: optional callable that receives one dict per
      iteration.

    Any exception from ``critic_fn`` stops the loop with
    ``stopped_reason="critic_error"`` and preserves the last rows.
    ``retrieval_fn`` exceptions are NOT caught — those are bugs in
    the caller's wiring that should surface.
    """
    iterations: list[dict] = []
    qhash = _query_hash(query)
    current_hops_hint = 1  # baseline; EXPAND_HOPS will bump
    current_kwargs: dict[str, Any] = {}
    last_rows: list[dict] = []
    last_adequacy = Adequacy.PARTIAL

    for iter_idx in range(max_retries + 1):
        last_rows = retrieval_fn(**current_kwargs)

        # Evaluate the critic — on failure abort gracefully.
        try:
            decision = critic_evaluate(query, last_rows, critic_fn)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "reflect.critic_error iter=%d error=%s qhash=%s",
                iter_idx, type(e).__name__, qhash,
            )
            audit_record = {
                "iteration": iter_idx,
                "adequacy": "critic_error",
                "action": "none",
                "rows_count": len(last_rows),
                "query_hash": qhash,
            }
            iterations.append(audit_record)
            if audit_sink is not None:
                try:
                    audit_sink(audit_record)
                except Exception:  # noqa: BLE001 — audit failures are swallowed
                    pass
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=last_adequacy,
                stopped_reason="critic_error",
            )

        last_adequacy = decision.adequacy
        audit_record = {
            "iteration": iter_idx,
            "adequacy": decision.adequacy.value,
            "action": decision.suggested_action.value,
            "rows_count": len(last_rows),
            "query_hash": qhash,
        }
        iterations.append(audit_record)
        if audit_sink is not None:
            try:
                audit_sink(audit_record)
            except Exception:  # noqa: BLE001
                pass

        # Stop conditions.
        if decision.adequacy == Adequacy.SUFFICIENT:
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=decision.adequacy,
                stopped_reason="accepted",
            )

        if decision.suggested_action == CriticAction.ACCEPT:
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=decision.adequacy,
                stopped_reason="accepted",
            )

        if decision.suggested_action == CriticAction.REJECT:
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=decision.adequacy,
                stopped_reason="rejected",
            )

        if (
            decision.suggested_action == CriticAction.CHANGE_INTENT
            and not decision.target_intent
        ):
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=decision.adequacy,
                stopped_reason="critic_malformed",
            )

        # We would retry — but are retries exhausted?
        if iter_idx == max_retries:
            break

        # Prepare kwargs for the next retrieve.
        extra, current_hops_hint = _kwargs_for_action(
            decision.suggested_action,
            current_hops_hint,
            target_intent=decision.target_intent,
        )
        current_kwargs = {**current_kwargs, **extra}

    # Loop exited naturally without accepting — retries exhausted.
    return ReflectResult(
        final_rows=tuple(last_rows),
        iterations=tuple(iterations),
        final_adequacy=last_adequacy,
        stopped_reason="retries_exhausted",
    )


# ---------------------------------------------------------------------------
# Production reflective query state machine
# ---------------------------------------------------------------------------

_SAFE_REASON = re.compile(r"^[a-z0-9_.:-]{1,120}$")


def _rows_digest(rows: tuple[Mapping[str, Any], ...]) -> str:
    """Hash the exact semantic rows visible to the critic, including rank.

    A critic may evaluate title, body, provenance, lineage or any future public
    row field.  Keying only by node id and similarity could reuse a verdict
    after that content changed.  Preserve row order (rank is semantic), sort
    mapping keys, and stringify only non-JSON scalar adapters such as datetimes.
    """

    canonical = [dict(row) for row in rows]
    raw = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _safe_reason(reason: str) -> str:
    value = str(reason or "").strip().lower()
    if _SAFE_REASON.fullmatch(value):
        return value
    digest = hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"critic_reason:{digest}"


def _emit(sink: ReflectiveTelemetryPort | None, event: Mapping[str, Any]) -> None:
    if sink is None:
        return
    try:
        sink.emit(event)
    except Exception:  # noqa: BLE001 - telemetry never changes the verdict
        logger.warning("reflect.telemetry_error", exc_info=True)


def _critic_cache_key(
    *,
    board_id: str,
    query_hash: str,
    acl_scope_hash: str,
    limit: int,
    min_confidence: float,
    graph_layer: str,
    graph_version: str,
    rows_digest: str,
    iteration: int,
    previous_action: CriticAction | None,
    previous_rows_digest: str | None,
    remaining_budget_units: int,
    elapsed_ms: float,
    critic: ReflectiveCriticPort,
) -> tuple[Any, ...]:
    return (
        board_id,
        query_hash,
        acl_scope_hash,
        int(limit),
        round(float(min_confidence), 6),
        graph_layer,
        graph_version,
        rows_digest,
        int(iteration),
        previous_action.value if previous_action is not None else None,
        previous_rows_digest,
        int(remaining_budget_units),
        round(max(0.0, float(elapsed_ms)), 3),
        str(getattr(critic, "identity", "unknown")),
        str(getattr(critic, "version", "unknown")),
    )


def run_reflective_query(
    *,
    board_id: str,
    query: str,
    limit: int,
    min_confidence: float,
    graph_layer: str,
    max_iterations: int,
    deadline_ms: int,
    budget_units: int,
    acl_scope_hash: str,
    retrieval: ReflectiveRetrievalPort,
    critic: ReflectiveCriticPort,
    telemetry: ReflectiveTelemetryPort | None = None,
) -> dict[str, Any]:
    """Execute retrieve -> critic -> corrective-action until a terminal state.

    The function is deliberately synchronous: MCP runs it in a worker thread,
    while editions may use synchronous local graph adapters.  Every terminal
    path is explicit and fail-closed; rows are never labelled accepted without
    a schema-valid ``SUFFICIENT + ACCEPT`` critic decision.
    """

    if not callable(getattr(retrieval, "retrieve", None)):
        raise TypeError("reflective_retrieval_port_invalid")
    if not callable(getattr(critic, "evaluate", None)):
        raise TypeError("reflective_critic_port_invalid")
    if not 1 <= int(max_iterations) <= 8:
        raise ValueError("max_iterations_out_of_range")
    if not 1 <= int(budget_units) <= 10_000:
        raise ValueError("budget_units_out_of_range")
    if not 50 <= int(deadline_ms) <= 30_000:
        raise ValueError("deadline_ms_out_of_range")

    started = time.monotonic()
    query_hash = _query_hash(query)
    traces: list[dict[str, Any]] = []
    total_cost = 0
    action: CriticAction | None = None
    hops = 1
    target_intent: str | None = None
    rewritten_query: str | None = None
    previous_rows: tuple[Mapping[str, Any], ...] = ()
    previous_digest: str | None = None
    last_rows: tuple[Mapping[str, Any], ...] = ()
    last_graph_version = "unknown"
    final_adequacy = Adequacy.IRRELEVANT.value

    def elapsed_ms() -> float:
        return (time.monotonic() - started) * 1000.0

    def finish(reason: str, *, accepted: bool = False) -> dict[str, Any]:
        event = {
            "metric_name": "kg_reflective_terminal_total",
            "board_id": board_id,
            "query_hash": query_hash,
            "terminal_reason": reason,
            "iterations": len(traces),
            "accepted": accepted,
            "cost_units": total_cost,
            "elapsed_ms": round(elapsed_ms(), 3),
        }
        _emit(telemetry, event)
        return {
            "nodes": [dict(row) for row in last_rows],
            "total_matches": len(last_rows),
            "accepted": accepted,
            "final_adequacy": final_adequacy,
            "terminal_reason": reason,
            # Compatibility alias for clients of the former stub.
            "stopped_reason": reason,
            "iterations": traces,
            "budget": {
                "used_units": total_cost,
                "limit_units": budget_units,
                "elapsed_ms": round(elapsed_ms(), 3),
                "deadline_ms": deadline_ms,
            },
            "critic": {
                "identity": str(getattr(critic, "identity", "unknown")),
                "version": str(getattr(critic, "version", "unknown")),
            },
            "retrieval": {
                "identity": str(getattr(retrieval, "identity", "unknown")),
                "version": str(getattr(retrieval, "version", "unknown")),
                "graph_version": last_graph_version,
            },
        }

    for iteration in range(max_iterations):
        if elapsed_ms() >= deadline_ms:
            return finish("deadline_exhausted")
        request = ReflectiveRetrievalRequest(
            board_id=board_id,
            query=query,
            limit=limit,
            min_confidence=min_confidence,
            graph_layer=graph_layer,
            iteration=iteration,
            action=action,
            fixed_hops_hint=hops,
            target_intent=target_intent,
            rewritten_query=rewritten_query,
            previous_rows=previous_rows,
        )
        try:
            batch = retrieval.retrieve(request)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "reflect.retrieval_error iter=%d error=%s qhash=%s",
                iteration,
                type(exc).__name__,
                query_hash,
            )
            return finish("retrieval_error")

        if not hasattr(batch, "rows") or not isinstance(batch.rows, tuple):
            return finish("retrieval_malformed")
        if any(not isinstance(row, Mapping) for row in batch.rows):
            return finish("retrieval_malformed")
        raw_cost = getattr(batch, "cost_units", 1)
        if (
            isinstance(raw_cost, bool)
            or not isinstance(raw_cost, int)
            or raw_cost < 1
        ):
            return finish("retrieval_malformed")
        cost = raw_cost
        total_cost += cost
        last_rows = batch.rows
        last_graph_version = str(getattr(batch, "graph_version", "unknown"))
        digest = _rows_digest(last_rows)
        if total_cost > budget_units:
            return finish("budget_exhausted")
        if elapsed_ms() >= deadline_ms:
            return finish("deadline_exhausted")

        critic_request = ReflectiveCriticRequest(
            board_id=board_id,
            query=query,
            iteration=iteration,
            rows=last_rows,
            rows_digest=digest,
            previous_rows_digest=previous_digest,
            previous_action=action,
            remaining_budget_units=budget_units - total_cost,
            elapsed_ms=elapsed_ms(),
        )
        cache_key = _critic_cache_key(
            board_id=board_id,
            query_hash=query_hash,
            acl_scope_hash=acl_scope_hash,
            limit=limit,
            min_confidence=min_confidence,
            graph_layer=graph_layer,
            graph_version=last_graph_version,
            rows_digest=digest,
            iteration=critic_request.iteration,
            previous_action=critic_request.previous_action,
            previous_rows_digest=critic_request.previous_rows_digest,
            remaining_budget_units=critic_request.remaining_budget_units,
            elapsed_ms=critic_request.elapsed_ms,
            critic=critic,
        )
        decision = _reflective_cache.get(cache_key)
        cache_hit = decision is not None
        if decision is None:
            try:
                decision = critic.evaluate(critic_request)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "reflect.critic_error iter=%d error=%s qhash=%s",
                    iteration,
                    type(exc).__name__,
                    query_hash,
                )
                return finish("critic_error")
            if not isinstance(decision, CriticDecision):
                return finish("critic_malformed")
            if not isinstance(decision.adequacy, Adequacy):
                return finish("critic_malformed")
            if not isinstance(decision.suggested_action, CriticAction):
                return finish("critic_malformed")
            if decision.confidence is not None and not (
                0.0 <= float(decision.confidence) <= 1.0
            ):
                return finish("critic_malformed")
            _reflective_cache[cache_key] = decision
            if len(_reflective_cache) > 256:
                _reflective_cache.pop(next(iter(_reflective_cache)))

        final_adequacy = decision.adequacy.value
        trace = {
            "iteration": iteration,
            "query_hash": query_hash,
            "adequacy": decision.adequacy.value,
            "action": decision.suggested_action.value,
            "rationale": _safe_reason(decision.reason),
            "rows_count": len(last_rows),
            "result_digest": digest[:16],
            "retrieval_mode": str(getattr(batch, "retrieval_mode", "unknown")),
            "graph_version": last_graph_version,
            "fixed_hops": hops,
            "cost_units": cost,
            "elapsed_ms": round(elapsed_ms(), 3),
            "critic_cache_hit": cache_hit,
        }
        traces.append(trace)
        _emit(
            telemetry,
            {
                "metric_name": "kg_reflective_iteration_total",
                "board_id": board_id,
                **trace,
            },
        )

        if decision.suggested_action == CriticAction.ACCEPT:
            if decision.adequacy != Adequacy.SUFFICIENT:
                return finish("critic_malformed")
            return finish("accepted", accepted=True)
        if decision.suggested_action == CriticAction.REJECT:
            return finish("rejected")

        next_action = decision.suggested_action
        next_hops = hops
        next_target_intent = target_intent
        next_rewritten_query = rewritten_query
        if decision.suggested_action == CriticAction.RETRY_WITH_REWRITE:
            next_rewritten_query = decision.rewritten_query
        elif decision.suggested_action == CriticAction.EXPAND_HOPS:
            next_hops = min(3, hops + 1)
        elif decision.suggested_action == CriticAction.FALLBACK_SEMANTIC:
            pass
        elif decision.suggested_action == CriticAction.CHANGE_INTENT:
            if not decision.target_intent:
                return finish("critic_malformed")
            next_target_intent = decision.target_intent
        else:
            return finish("critic_malformed")

        current_retrieval_state = (
            action.value if action is not None else None,
            hops,
            target_intent,
            rewritten_query,
        )
        next_retrieval_state = (
            next_action.value,
            next_hops,
            next_target_intent,
            next_rewritten_query,
        )
        if (
            previous_digest is not None
            and digest == previous_digest
            and next_retrieval_state == current_retrieval_state
        ):
            return finish("no_progress")
        if iteration + 1 >= max_iterations:
            return finish("max_iterations")

        action = next_action
        hops = next_hops
        target_intent = next_target_intent
        rewritten_query = next_rewritten_query
        previous_digest = digest
        previous_rows = last_rows

    return finish("max_iterations")
