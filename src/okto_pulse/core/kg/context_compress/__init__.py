"""Context compression for KG retrieval (ideação fe55ff7c).

When the top-K retrieved nodes carry verbose content, the agent's
context window gets saturated. This module provides an opt-in
summarizer: when the aggregate approximate token count exceeds a
configurable threshold, an injected LLM callable produces a condensed
summary. The original nodes are NOT replaced — the summary is appended
to the response as metadata so the caller can choose detail vs. summary.

Public API:
- ``CompressionResult`` — frozen dataclass with applied/summary/
  compressed_from_nodes/approx_original_tokens/approx_compressed_tokens.
- ``compress_if_needed(nodes, compress_llm_fn, max_tokens, ...)`` —
  skips the LLM when below threshold; invokes it once above.
- ``default_token_count(text)`` — baseline ``len(text) // 4`` estimator
  suitable for SDLC corpus; injectable for tokenizer-backed accuracy.
"""

from .compress import (
    CompressionResult,
    compress_if_needed,
    default_token_count,
)

__all__ = [
    "CompressionResult",
    "compress_if_needed",
    "default_token_count",
]
