"""Context compression implementation.

Ideação fe55ff7c.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger("okto_pulse.kg.context_compress")

#: Prompt template the default caller can wrap around the concatenated
#: node content. Kept generic so Claude/GPT/Gemini all handle it well.
#: Callers that want a bespoke prompt just ignore this and feed their
#: own text to ``compress_llm_fn``.
DEFAULT_SUMMARY_PROMPT = (
    "Summarize the following knowledge-graph nodes in under 120 words. "
    "Preserve entity names, relationships (supersedes/contradicts/"
    "depends_on), dates and links between nodes. Write prose, no "
    "bullet points. Nodes:\n\n"
)


@dataclass(frozen=True)
class CompressionResult:
    """Outcome of ``compress_if_needed``.

    ``applied=False`` means the input was below the threshold OR the
    LLM failed — in both cases the caller should proceed with the
    original nodes intact. ``applied=True`` means ``summary`` is
    populated and the caller can surface it alongside (not instead of)
    the original rows.
    """

    applied: bool
    summary: str | None
    compressed_from_nodes: int
    approx_original_tokens: int
    approx_compressed_tokens: int


def default_token_count(text: str) -> int:
    """Approximate token count as ``len(text) // 4``.

    Rough but good enough for a threshold gate — English/Portuguese
    SDLC text averages ~3.5 chars per token. Callers that need
    precision inject a tokenizer-backed callable via
    ``approx_token_count_fn``.
    """
    return max(0, len(text) // 4)


def _extract_text(node: Any) -> str:
    """Best-effort text extraction from a retrieval row.

    Checks, in order: ``.content``, ``.title``, dict keys of the same
    names. Falls back to ``str(node)`` so we never crash on a weirdly
    shaped row.
    """
    if node is None:
        return ""
    content = getattr(node, "content", None)
    title = getattr(node, "title", None)
    if content or title:
        parts = [t for t in (title, content) if t]
        return "\n".join(str(p) for p in parts)
    if isinstance(node, dict):
        parts = [node.get("title", ""), node.get("content", "")]
        return "\n".join(str(p) for p in parts if p)
    return str(node)


def compress_if_needed(
    nodes: list[Any],
    *,
    compress_llm_fn: Callable[[str], str] | None,
    max_tokens: int,
    approx_token_count_fn: Callable[[str], int] | None = None,
) -> CompressionResult:
    """Condense ``nodes`` via LLM when aggregate content exceeds
    ``max_tokens``.

    - Below threshold OR ``max_tokens <= 0`` OR no nodes → returns
      ``applied=False`` without invoking ``compress_llm_fn``.
    - Above threshold with a usable ``compress_llm_fn`` → invokes the
      LLM once with the concatenated text and returns the summary.
    - LLM exception → caught, logged as warning, returns
      ``applied=False`` so the caller keeps the original rows.
    """
    count_fn = approx_token_count_fn or default_token_count

    if not nodes or max_tokens <= 0 or compress_llm_fn is None:
        return CompressionResult(
            applied=False,
            summary=None,
            compressed_from_nodes=0,
            approx_original_tokens=0,
            approx_compressed_tokens=0,
        )

    pieces = [_extract_text(n) for n in nodes]
    concatenated = "\n\n".join(p for p in pieces if p)
    original_tokens = count_fn(concatenated)

    if original_tokens < max_tokens:
        return CompressionResult(
            applied=False,
            summary=None,
            compressed_from_nodes=0,
            approx_original_tokens=original_tokens,
            approx_compressed_tokens=0,
        )

    try:
        summary = compress_llm_fn(concatenated)
    except Exception as e:  # noqa: BLE001 — never propagate LLM failures
        logger.warning(
            "context_compress.llm_failed error=%s original_tokens=%d",
            type(e).__name__, original_tokens,
        )
        return CompressionResult(
            applied=False,
            summary=None,
            compressed_from_nodes=0,
            approx_original_tokens=original_tokens,
            approx_compressed_tokens=0,
        )

    if not isinstance(summary, str) or not summary.strip():
        logger.warning("context_compress.empty_summary")
        return CompressionResult(
            applied=False,
            summary=None,
            compressed_from_nodes=0,
            approx_original_tokens=original_tokens,
            approx_compressed_tokens=0,
        )

    return CompressionResult(
        applied=True,
        summary=summary,
        compressed_from_nodes=len(nodes),
        approx_original_tokens=original_tokens,
        approx_compressed_tokens=count_fn(summary),
    )
