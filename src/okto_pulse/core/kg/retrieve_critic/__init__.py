"""Critic / reflection loop over KG retrieval (ideação db8e984f).

Closes the agentic loop over the retrieve stage. Given a query and a
retrieval_fn (a caller-wired wrapper over kg_search_hybrid), the
reflect() orchestrator runs:

    retrieve → critic evaluates adequacy → if IRRELEVANT, dispatch a
    corrective action (rewrite/expand_hops/fallback_semantic) →
    retrieve retry → up to max_retries iterations.

Integrates with the other retrieve specs (rewrite, adaptive_hops,
hybrid_search) via kwargs the caller maps inside retrieval_fn — the
module itself stays decoupled from the concrete downstream params.

Usage::

    from okto_pulse.core.kg.retrieve_critic import reflect

    def my_retrieval(**kwargs):
        return kg_search_hybrid(
            query=q, vector_provider=..., graph_expander=...,
            rewrite=kwargs.get("rewrite", "none"),
            hop_strategy="fixed",
            **kwargs,
        ).ranked

    result = reflect(
        query="...",
        retrieval_fn=my_retrieval,
        critic_fn=my_llm_critic,
        max_retries=2,
        audit_sink=my_audit_logger,
    )
"""

from .interfaces import (
    Adequacy,
    CriticAction,
    CriticDecision,
    ReflectResult,
)
from .orchestrator import critic_evaluate, reflect, reset_critic_cache

__all__ = [
    "Adequacy",
    "CriticAction",
    "CriticDecision",
    "ReflectResult",
    "critic_evaluate",
    "reflect",
    "reset_critic_cache",
]
