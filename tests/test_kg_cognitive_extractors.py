"""Tests for cognitive extractors (cards 14cd6bd9 + b4df0783)."""

from __future__ import annotations

from okto_pulse.core.kg.agent.extractors import (
    extract_alternatives,
)


# ===========================================================================
# Alternative extractor
# ===========================================================================


def test_alternatives_from_analysis_section_only():
    ctx = (
        "## Scope\n- x\n\n"
        "## Analysis\n"
        "Considerada a alternativa MQTT, mas rejeitada por complexidade.\n"
        "Optou-se por Redis Streams ao invés de RabbitMQ por latência.\n\n"
        "## Out\n- stuff with alternativa que NÃO deve entrar.\n"
    )
    results = extract_alternatives(
        spec_context=ctx, source_ref="spec:abc",
    )
    assert len(results) == 2
    titles = [r.title for r in results]
    assert any("MQTT" in t for t in titles)
    assert any("Redis Streams" in t for t in titles)
    assert all(r.source_section == "analysis" for r in results)
    # Per-concept source_ref (spec eca49df9): ``spec:abc:alternative:<hash8>``.
    assert all(r.source_ref.startswith("spec:abc:alternative:") for r in results)


def test_alternatives_returns_empty_when_no_analysis_section():
    ctx = "## Only Scope\nFoo considered alternatives but we don't see this."
    assert extract_alternatives(spec_context=ctx, source_ref="spec:x") == []


def test_alternatives_from_qa_texts():
    qa = [
        "Considered using Kafka instead of Redis Streams.",
        "Q: por que não MongoDB? A: poderia ter usado MongoDB, mas descartamos.",
        "Nothing to see here.",
    ]
    results = extract_alternatives(spec_context="", qa_texts=qa, source_ref="spec:y")
    assert len(results) == 2
    assert all(r.source_section == "qa" for r in results)


def test_alternatives_extracts_english_patterns():
    ctx = "## Analysis\nWe considered NATS but discarded it due to ops burden."
    results = extract_alternatives(spec_context=ctx, source_ref="s:1")
    assert len(results) == 1
    assert "NATS" in results[0].title


def test_alternatives_title_trimmed_to_120_chars():
    long_sentence = (
        "Foi considerada a alternativa " + "X" * 500 + "."
    )
    ctx = f"## Analysis\n{long_sentence}"
    results = extract_alternatives(spec_context=ctx, source_ref="s:1")
    assert len(results) == 1
    assert len(results[0].title) <= 120


def test_alternatives_case_insensitive_header():
    ctx = "## ANALYSIS\nPoderia ter usado gRPC, mas descartamos por overhead."
    results = extract_alternatives(spec_context=ctx, source_ref="s:1")
    assert len(results) == 1
