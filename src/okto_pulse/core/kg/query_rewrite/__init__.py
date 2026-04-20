"""Pre-retrieve query rewriting strategies (ideação 2cf21a31).

Usage::

    from okto_pulse.core.kg.query_rewrite import get_rewriter

    rr = get_rewriter("hyde", llm_fn=my_hyde_fn)
    result = rr.rewrite("which decisions superseded X?")
    # result.hyde_passage is ready to be embedded for the HNSW seed.

Strategies:
- ``none`` — passthrough (default).
- ``hyde`` — Hypothetical Document Embeddings.
- ``decompose`` — sub-query decomposition.
- ``fusion`` — RAG-Fusion with Reciprocal Rank Fusion merge.

See individual modules for detailed docs and LLM callable contracts.
"""

from .decompose import DecomposeRewriter
from .factory import get_rewriter, reset_rewriter_cache
from .fusion import FusionRewriter
from .hyde import HyDERewriter
from .interfaces import RewriteResult
from .noop import NoopRewriter
from .rrf import merge_rrf

__all__ = [
    "DecomposeRewriter",
    "FusionRewriter",
    "HyDERewriter",
    "NoopRewriter",
    "RewriteResult",
    "get_rewriter",
    "merge_rrf",
    "reset_rewriter_cache",
]
