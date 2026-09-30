"""Optional ranked graph retrieval; no driver, physical identity or index API.

Availability and per-scope readiness are separate. Unsupported/not-ready providers
raise GraphCapabilityUnavailable; operational failures must not become empty hits.
BM25/RRF scores are ranking evidence, never probabilities or cosine similarities.
The vector mode returns explicit cosine scores, native regime and achieved-k;
it does not imply exhaustive semantic coverage or authorize a Learning intent.
"""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class RankedGraphQuery:
    node_type: str
    query: str
    mode: str = "text"
    limit: int = 20
    graph_layer: str = "canonical"
    include_superseded: bool = False
    include_code_traceability: bool = False
    min_confidence: float = 0.5
    vector: tuple[float, ...] = ()
    candidate_limit: int = 100
    max_filter_rows: int = 10_000
    timeout_seconds: float = 10.0
    phrase: bool = False


class RankedGraphSearch(Protocol):
    def readiness(self, board_id: str, node_type: str) -> dict:
        """Lexical/hybrid explorer readiness; vector queries qualify their own space."""
        ...


    def search(self, board_id: str, request: RankedGraphQuery) -> dict:
        """One-snapshot filtered ranked hits with qualified application identities.

        Return hits, mode, ranking, snapshot, lexical_regime and vector_regime.
        Each hit includes node_id/node_type/title and independent rank/source scores.
        Vector hits additionally carry generation/content/context/source_artifact_ref
        from the same snapshot for bounded comparison with authoritative source heads.
        An admission refusal is not pagination or a successful partial result.
        """
        ...
