"""Tests for cognitive_policy + layer_violation enforcement in primitives."""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.cognitive_policy import (
    COGNITIVE_EDGE_TYPES,
    DETERMINISTIC_EDGE_TYPES,
    FALLBACK_CONFIDENCE_CAP,
    LayerViolationError,
    check_cognitive_edge_allowed,
    clamp_fallback_confidence,
)


def test_deterministic_edges_catalog_matches_spec():
    assert DETERMINISTIC_EDGE_TYPES == frozenset({
        "tests", "implements", "violates", "derives_from", "mentions",
        "belongs_to",
    })


def test_cognitive_edges_catalog_matches_spec():
    assert COGNITIVE_EDGE_TYPES == frozenset({
        "contradicts", "supersedes", "depends_on", "relates_to", "validates",
    })


def test_catalogs_are_disjoint():
    assert DETERMINISTIC_EDGE_TYPES.isdisjoint(COGNITIVE_EDGE_TYPES)


@pytest.mark.parametrize("edge_type", sorted(DETERMINISTIC_EDGE_TYPES))
def test_check_raises_on_deterministic_edge(edge_type):
    with pytest.raises(LayerViolationError) as exc:
        check_cognitive_edge_allowed(edge_type)
    assert exc.value.edge_type == edge_type
    assert "contradicts" in exc.value.allowed_edges


@pytest.mark.parametrize("edge_type", sorted(COGNITIVE_EDGE_TYPES))
def test_check_allows_cognitive_edge(edge_type):
    check_cognitive_edge_allowed(edge_type)  # does not raise


def test_clamp_fallback_below_cap_unchanged():
    clamped, was_clamped = clamp_fallback_confidence(0.7, layer="fallback")
    assert clamped == 0.7
    assert was_clamped is False


def test_clamp_fallback_above_cap_clamps():
    clamped, was_clamped = clamp_fallback_confidence(0.95, layer="fallback")
    assert clamped == FALLBACK_CONFIDENCE_CAP
    assert was_clamped is True


def test_clamp_nonfallback_layer_never_clamps():
    clamped, was_clamped = clamp_fallback_confidence(0.99, layer="cognitive")
    assert clamped == 0.99
    assert was_clamped is False


def test_active_prompt_loads_and_mentions_forbidden_set():
    from okto_pulse.core.kg.agent.prompts import ACTIVE_PROMPT_VERSION, load_prompt
    text = load_prompt(ACTIVE_PROMPT_VERSION)
    assert "tests, implements, violates, derives_from, mentions" in text
    assert "contradicts" in text
    assert "supersedes" in text
    assert "Cognitive Fallback Confidence Cap" in text
