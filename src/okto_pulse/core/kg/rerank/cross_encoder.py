"""CrossEncoderReranker — sentence-transformers cross-encoder second stage.

Ideação 3070cd53. Loads ``cross-encoder/ms-marco-MiniLM-L-6-v2`` (the
canonical MS MARCO passage reranker — ~80 MB, ~10 ms per pair on CPU)
and scores every (query, candidate) pair. Much more precise than
bi-encoder cosine similarity because attention flows between query
and passage rather than each being embedded in isolation.

Lazy-imports `sentence_transformers` so projects that don't install
the `[kg-embeddings]` extra never pay the loading cost. Falls back
to raising at construction time with a clear pip hint — callers
(factory.py) catch this and downgrade to TokenOverlapReranker.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Sequence

from .token_overlap import TokenOverlapReranker


class CrossEncoderReranker:
    """Cross-encoder second stage over hybrid_search top-K.

    Construction loads the model once; subsequent rerank() calls are
    pure inference. Thread-safety matches the underlying
    sentence-transformers model (safe to call from async contexts).
    """

    name = "cross_encoder"
    default_model = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    def __init__(self, *, model_name: str | None = None) -> None:
        try:
            from sentence_transformers import CrossEncoder  # type: ignore[import-not-found]
        except ImportError as e:  # pragma: no cover — optional dep
            raise ImportError(
                "CrossEncoderReranker needs `sentence-transformers`. "
                "Install the optional extra: `pip install "
                "'okto-pulse-core[kg-embeddings]'`."
            ) from e
        self._model = CrossEncoder(model_name or self.default_model)

    def rerank(
        self,
        query: str,
        candidates: Sequence,
        *,
        top_n: int = 10,
    ) -> list:
        if top_n <= 0 or not candidates:
            return []
        if not query.strip():
            return list(candidates[:top_n])

        pairs = [(query, TokenOverlapReranker._text_of(c)) for c in candidates]
        # `predict` returns a numpy array of shape (N,) with logit scores.
        # Higher = more relevant. Model-specific range (typically [-10, 10]
        # for MS MARCO); we don't normalise — only the order matters.
        scores = self._model.predict(pairs).tolist()

        scored = list(zip(scores, range(len(candidates)), candidates))
        # Stable sort: higher score first, ties broken by original index
        # so reproducibility across runs.
        scored.sort(key=lambda t: (t[0], -t[1]), reverse=True)

        out: list = []
        for new_score, _idx, cand in scored[:top_n]:
            # Overwrite candidate.score with the cross-encoder score when
            # the dataclass has that field — keeps downstream consumers
            # uniform with the first-stage `RankedNode.score`.
            if hasattr(cand, "score") and hasattr(cand, "__dataclass_fields__"):
                try:
                    out.append(replace(cand, score=float(new_score)))
                    continue
                except (TypeError, ValueError):
                    pass
            out.append(cand)
        return out
