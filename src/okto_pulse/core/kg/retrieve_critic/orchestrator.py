"""critic_evaluate + reflect orchestrator (ideação db8e984f)."""

from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Any, Callable

from .interfaces import (
    Adequacy,
    CriticAction,
    CriticDecision,
    ReflectResult,
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


#: Module-level LRU cache keyed by (query, rows_signature). The cache
#: is keyed inside ``critic_evaluate`` via an inner helper; outer
#: function threads the critic_fn through.
@lru_cache(maxsize=64)
def _cached_decision(
    query: str, rows_sig: tuple, critic_fn_id: int,
    raw_dict_key: str,
) -> CriticDecision:
    # critic_fn_id + raw_dict_key together form the unique identity
    # so two different critic_fn instances don't share cache lines.
    # Actual invocation happens in critic_evaluate; this shell just
    # participates in the cache.
    raise RuntimeError("Should never reach — populated via __wrapped__")


def critic_evaluate(
    query: str,
    rows: list[dict],
    critic_fn: CriticFn,
) -> CriticDecision:
    """Invoke critic_fn once with (query, rows) and map the JSON
    response to a CriticDecision. Unknown enum values fall back to
    Adequacy.PARTIAL + CriticAction.ACCEPT with a reason string.

    The LRU cache is implemented via a closure — two consecutive
    calls with identical (query, rows_signature) return the same
    CriticDecision without invoking critic_fn again.
    """
    cache_key = (query, _rows_signature(rows), id(critic_fn))
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
    )
    _global_cache[cache_key] = decision
    # Bounded cache — drop oldest when >64.
    if len(_global_cache) > 64:
        _global_cache.pop(next(iter(_global_cache)))
    return decision


# Simple dict acts as an ordered LRU (insertion order) for the shell.
# Small size (64 entries) — no need for OrderedDict + proper LRU.
_global_cache: dict[tuple, CriticDecision] = {}


def reset_critic_cache() -> None:
    """Test helper — drops the cache between suites."""
    _global_cache.clear()


# ---------------------------------------------------------------------------
# Action dispatcher
# ---------------------------------------------------------------------------


def _kwargs_for_action(
    action: CriticAction,
    current_hops_hint: int,
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
    # ACCEPT and CHANGE_INTENT don't retry.
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

        if decision.suggested_action == CriticAction.CHANGE_INTENT:
            return ReflectResult(
                final_rows=tuple(last_rows),
                iterations=tuple(iterations),
                final_adequacy=decision.adequacy,
                stopped_reason="change_intent_v1_not_implemented",
            )

        # We would retry — but are retries exhausted?
        if iter_idx == max_retries:
            break

        # Prepare kwargs for the next retrieve.
        extra, current_hops_hint = _kwargs_for_action(
            decision.suggested_action, current_hops_hint,
        )
        current_kwargs = {**current_kwargs, **extra}

    # Loop exited naturally without accepting — retries exhausted.
    return ReflectResult(
        final_rows=tuple(last_rows),
        iterations=tuple(iterations),
        final_adequacy=last_adequacy,
        stopped_reason="retries_exhausted",
    )
