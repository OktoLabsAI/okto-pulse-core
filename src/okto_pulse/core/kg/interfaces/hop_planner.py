"""HopPlanner Protocol — runtime decision of max_hops for graph expand.

Ideação 1fb13b51. The hybrid search currently uses a fixed ``max_hops``
baked into each intent. Queries of different depths pay the same
traversal cost: simple queries over-walk, complex queries under-walk.
A HopPlanner decides ``max_hops`` at runtime based on the query and
seed titles — typically 1-3 hops.

Implementations satisfy this Protocol via duck typing.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.adaptive_hops.interfaces import HopDecision


@runtime_checkable
class HopPlanner(Protocol):
    """Runtime planner for graph expand depth.

    Implementations MUST clamp the returned hops to [1, 3] (ceiling
    hard to avoid combinatorial explosion) and MUST NOT raise under
    normal flow — LLM/tool exceptions are caught internally and
    translated to a fallback HopDecision.
    """

    name: str

    def plan(
        self, *, query: str, intent_name: str, seed_titles: list[str]
    ) -> HopDecision:
        """Decide the number of graph-expand hops for this query.

        Args:
            query: The user's original query (pre-rewrite).
            intent_name: The resolved search intent name.
            seed_titles: Titles of the top vector seeds, used as
                optional context for LLM-based planners.
        """
        ...
