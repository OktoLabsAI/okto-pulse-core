"""Unit tests for kg.context_compress (ideação fe55ff7c)."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from okto_pulse.core.kg.context_compress import (
    CompressionResult,
    compress_if_needed,
    default_token_count,
)


# ===========================================================================
# default_token_count
# ===========================================================================


def test_default_token_count_approx_chars_over_4():
    assert default_token_count("") == 0
    assert default_token_count("abcd") == 1  # 4 chars → 1 token
    assert default_token_count("x" * 400) == 100


# ===========================================================================
# CompressionResult shape
# ===========================================================================


def test_compression_result_is_frozen():
    cr = CompressionResult(
        applied=False,
        summary=None,
        compressed_from_nodes=0,
        approx_original_tokens=0,
        approx_compressed_tokens=0,
    )
    with pytest.raises(Exception):
        cr.applied = True  # type: ignore[misc]


# ===========================================================================
# compress_if_needed — gate + LLM invocation
# ===========================================================================


@dataclass
class _Node:
    title: str
    content: str


def test_below_threshold_skips_llm():
    counter = {"count": 0}

    def llm_fn(text: str) -> str:
        counter["count"] += 1
        return "SUMMARY"

    # 3 short nodes → aggregate ~40 chars → ~10 tokens. Threshold 500.
    nodes = [
        _Node(title="t1", content="short content a"),
        _Node(title="t2", content="short content b"),
        _Node(title="t3", content="short content c"),
    ]
    result = compress_if_needed(
        nodes, compress_llm_fn=llm_fn, max_tokens=500,
    )
    assert result.applied is False
    assert result.summary is None
    assert result.compressed_from_nodes == 0
    assert result.approx_compressed_tokens == 0
    assert counter["count"] == 0  # LLM never invoked


def test_above_threshold_invokes_llm():
    seen: list[str] = []

    def llm_fn(text: str) -> str:
        seen.append(text)
        return "CONDENSED"

    # 5 nodes with ~100 chars each → ~500 chars → ~125 tokens. Threshold 50.
    nodes = [_Node(title=f"t{i}", content="x" * 100) for i in range(5)]
    result = compress_if_needed(
        nodes, compress_llm_fn=llm_fn, max_tokens=50,
    )
    assert result.applied is True
    assert result.summary == "CONDENSED"
    assert result.compressed_from_nodes == 5
    assert result.approx_original_tokens > 50
    assert result.approx_compressed_tokens == len("CONDENSED") // 4
    assert len(seen) == 1  # called exactly once


def test_zero_max_tokens_disables():
    counter = {"count": 0}

    def llm_fn(text: str) -> str:
        counter["count"] += 1
        return "nope"

    nodes = [_Node(title="t", content="x" * 10000)]
    result = compress_if_needed(
        nodes, compress_llm_fn=llm_fn, max_tokens=0,
    )
    assert result.applied is False
    assert counter["count"] == 0


def test_none_llm_fn_disables():
    nodes = [_Node(title="t", content="x" * 10000)]
    result = compress_if_needed(
        nodes, compress_llm_fn=None, max_tokens=10,
    )
    assert result.applied is False


def test_empty_nodes_returns_not_applied():
    result = compress_if_needed(
        [], compress_llm_fn=lambda t: "x", max_tokens=10,
    )
    assert result.applied is False
    assert result.compressed_from_nodes == 0


def test_llm_exception_returns_not_applied():
    def exploding(text: str) -> str:
        raise RuntimeError("LLM timeout")

    nodes = [_Node(title="t", content="x" * 1000)]
    result = compress_if_needed(
        nodes, compress_llm_fn=exploding, max_tokens=10,
    )
    assert result.applied is False
    assert result.summary is None


def test_llm_empty_response_returns_not_applied():
    nodes = [_Node(title="t", content="x" * 1000)]
    result = compress_if_needed(
        nodes, compress_llm_fn=lambda t: "   ", max_tokens=10,
    )
    assert result.applied is False


def test_custom_token_counter():
    # Custom counter returns a fixed large value → forces compression.
    def big_counter(text: str) -> int:
        return 9999

    def llm_fn(text: str) -> str:
        return "OK"

    nodes = [_Node(title="t", content="short")]
    result = compress_if_needed(
        nodes,
        compress_llm_fn=llm_fn,
        max_tokens=100,
        approx_token_count_fn=big_counter,
    )
    assert result.applied is True
    assert result.approx_original_tokens == 9999


def test_dict_nodes_supported():
    """Nodes as dicts (retrieval payload shape) are handled."""
    nodes = [
        {"title": "Card", "content": "x" * 500},
        {"title": "Spec", "content": "y" * 500},
    ]
    result = compress_if_needed(
        nodes,
        compress_llm_fn=lambda t: "OK",
        max_tokens=50,
    )
    assert result.applied is True
