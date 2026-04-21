"""Unit tests for the cognitive heuristics (cards b0120a89 + f700479d).

Each heuristic is covered by fixtures + a deterministic DummyLLM. We don't
exercise Kùzu or the embedding provider here — those live in the hybrid
search e2e tests. The aim is to nail down the pipeline semantics:

    vector seed → entity/scope filter → LLM polarity → confidence clamp
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pytest

from okto_pulse.core.kg.agent.heuristics import (
    CONTRADICTS_CEILING,
    DEPENDS_ON_CEILING,
    DEPENDS_ON_FLOOR,
    SUPERSEDES_CEILING,
    LLMVerdict,
    run_contradiction_heuristic,
    run_depends_on_heuristic,
    run_supersedence_heuristic,
)
from okto_pulse.core.kg.agent.heuristics.contradiction import (
    DecisionNeighbor,
    DecisionNode,
)
from okto_pulse.core.kg.agent.heuristics.depends_on import (
    DEPENDS_ON_MIN_SHARED_ENTITIES,
    CandidatePair,
)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


@dataclass
class DummyLLM:
    """Deterministic LLM whose verdict is controlled by a lambda.

    `decider(prompt_id, text_a, text_b, context) → LLMVerdict`.
    """

    decider: Callable[..., LLMVerdict]
    calls: list[tuple[str, str, str, dict]] = field(default_factory=list)

    def ask_polarity(self, *, prompt_id, text_a, text_b, context=None) -> LLMVerdict:
        ctx = context or {}
        self.calls.append((prompt_id, text_a, text_b, ctx))
        return self.decider(prompt_id, text_a, text_b, ctx)


def _yes(conf: float, reason: str = "Observed mutually exclusive semantics") -> LLMVerdict:
    return LLMVerdict(answer=True, confidence=conf, reasoning=reason)


def _no(conf: float, reason: str = "Can coexist") -> LLMVerdict:
    return LLMVerdict(answer=False, confidence=conf, reasoning=reason)


# ---------------------------------------------------------------------------
# CONTRADICTS
# ---------------------------------------------------------------------------


def _dec(node_id: str, content: str, entities=(), spec="spec-A") -> DecisionNode:
    return DecisionNode(
        node_id=node_id, content=content,
        entity_ids=frozenset(entities), spec_id=spec,
    )


def test_contradicts_emits_when_llm_answers_no():
    src = _dec("d_src", "Poll SmartThings every 60 min", entities=["SmartThings"])
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "SmartThings must sync in real time",
                      entities=["SmartThings"], spec="spec-B"),
        similarity=0.82,
    )
    llm = DummyLLM(decider=lambda *a, **kw: _no(0.8))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert len(out) == 1
    c = out[0]
    assert c.from_node_id == "d_src"
    assert c.to_node_id == "d_n"
    assert c.confidence == pytest.approx(0.8 * CONTRADICTS_CEILING)
    assert "SmartThings" in c.shared_entities
    assert len(llm.calls) == 1


def test_contradicts_drops_when_llm_answers_yes():
    src = _dec("d_src", "A", entities=["E"])
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "B", entities=["E"], spec="spec-B"),
        similarity=0.9,
    )
    llm = DummyLLM(decider=lambda *a, **kw: _yes(0.95))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert out == []


def test_contradicts_drops_below_vector_threshold():
    src = _dec("d_src", "A", entities=["E"])
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "B", entities=["E"], spec="spec-B"),
        similarity=0.4,  # below default 0.6
    )
    llm = DummyLLM(decider=lambda *a, **kw: _no(0.9))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert out == []
    assert llm.calls == []  # LLM never invoked


def test_contradicts_requires_shared_entity():
    src = _dec("d_src", "A", entities=["SmartThings"])
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "B", entities=["HomeAssistant"], spec="spec-B"),
        similarity=0.9,
    )
    llm = DummyLLM(decider=lambda *a, **kw: _no(0.9))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert out == []
    assert llm.calls == []


def test_contradicts_cold_start_skips_entity_filter():
    """A fresh Decision (no Entity mentions yet) still gets LLM review."""
    src = _dec("d_src", "A", entities=())  # no entities
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "B", entities=["E"], spec="spec-B"),
        similarity=0.9,
    )
    llm = DummyLLM(decider=lambda *a, **kw: _no(0.8))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert len(out) == 1


def test_contradicts_confidence_never_exceeds_ceiling():
    src = _dec("d_src", "A", entities=["E"])
    neigh = DecisionNeighbor(
        decision=_dec("d_n", "B", entities=["E"], spec="spec-B"),
        similarity=0.9,
    )
    llm = DummyLLM(decider=lambda *a, **kw: _no(1.5))  # over-optimistic LLM
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert out[0].confidence <= CONTRADICTS_CEILING


def test_contradicts_results_sorted_by_confidence_desc():
    src = _dec("d_src", "A", entities=["E"])
    neighbors = [
        DecisionNeighbor(decision=_dec(f"d_{i}", f"B{i}", entities=["E"], spec="spec-B"),
                         similarity=0.9)
        for i in range(3)
    ]
    confidences = [0.7, 0.9, 0.8]
    llm = DummyLLM(decider=lambda p, a, b, c:
                   _no(confidences[int(b[-1])]))
    out = run_contradiction_heuristic(src, neighbors, llm)
    assert [round(c.confidence, 3) for c in out] == sorted(
        [round(conf * CONTRADICTS_CEILING, 3) for conf in confidences],
        reverse=True,
    )


def test_contradicts_skips_self_loop():
    src = _dec("d_src", "A", entities=["E"])
    neigh = DecisionNeighbor(decision=src, similarity=0.99)
    llm = DummyLLM(decider=lambda *a, **kw: _no(0.9))
    out = run_contradiction_heuristic(src, [neigh], llm)
    assert out == []


# ---------------------------------------------------------------------------
# SUPERSEDES
# ---------------------------------------------------------------------------


def test_supersedes_emits_when_llm_confirms():
    new = _dec("new", "Use Redis streams for event bus",
               entities=["Redis"], spec="spec-A")
    old = _dec("old", "Use RabbitMQ for event bus",
               entities=["Redis"], spec="spec-A")
    neigh = DecisionNeighbor(decision=old, similarity=0.8)
    llm = DummyLLM(decider=lambda *a, **kw: _yes(0.9))
    out = run_supersedence_heuristic(new, [neigh], llm)
    assert len(out) == 1
    c = out[0]
    assert c.from_node_id == "new"
    assert c.to_node_id == "old"
    assert c.confidence == pytest.approx(0.9 * SUPERSEDES_CEILING)
    assert c.mark_retired is True


def test_supersedes_requires_scope_match():
    new = _dec("new", "A", entities=["X"], spec="spec-A")
    old = _dec("old", "B", entities=["Y"], spec="spec-B")
    neigh = DecisionNeighbor(decision=old, similarity=0.9)
    llm = DummyLLM(decider=lambda *a, **kw: _yes(0.9))
    out = run_supersedence_heuristic(new, [neigh], llm)
    assert out == []
    assert llm.calls == []


def test_supersedes_same_spec_scope_match():
    """Same spec_id always qualifies as same scope, even without Entity overlap."""
    new = _dec("new", "A", entities=(), spec="spec-A")
    old = _dec("old", "B", entities=(), spec="spec-A")
    neigh = DecisionNeighbor(decision=old, similarity=0.9)
    llm = DummyLLM(decider=lambda *a, **kw: _yes(0.95))
    out = run_supersedence_heuristic(new, [neigh], llm)
    assert len(out) == 1


def test_supersedes_drops_below_llm_threshold():
    new = _dec("new", "A", entities=["E"], spec="spec-A")
    old = _dec("old", "B", entities=["E"], spec="spec-A")
    neigh = DecisionNeighbor(decision=old, similarity=0.9)
    llm = DummyLLM(decider=lambda *a, **kw: _yes(0.5))  # below 0.7 default
    out = run_supersedence_heuristic(new, [neigh], llm)
    assert out == []


# ---------------------------------------------------------------------------
# DEPENDS_ON
# ---------------------------------------------------------------------------


def test_depends_on_emits_with_two_shared_entities():
    a = _dec("A", "Use PG with JSONB for events", entities=["PG", "JSONB"], spec="spec-A")
    b = _dec("B", "Migrate PG schema for JSONB tables", entities=["PG", "JSONB"], spec="spec-B")
    llm = DummyLLM(decider=lambda *args, **kw: _yes(0.8))
    out = run_depends_on_heuristic([CandidatePair(source=a, target=b)], llm)
    assert len(out) == 1
    c = out[0]
    assert c.confidence >= DEPENDS_ON_FLOOR
    assert c.confidence <= DEPENDS_ON_CEILING
    assert c.shared_entities == ("JSONB", "PG")


def test_depends_on_requires_minimum_shared_entities():
    a = _dec("A", "x", entities=["PG"], spec="spec-A")
    b = _dec("B", "y", entities=["PG"], spec="spec-B")
    llm = DummyLLM(decider=lambda *args, **kw: _yes(0.9))
    out = run_depends_on_heuristic([CandidatePair(source=a, target=b)], llm)
    # default min=2 and they share only 1 ⇒ dropped
    assert out == []
    assert llm.calls == []


def test_depends_on_requires_cross_spec():
    a = _dec("A", "x", entities=["E1", "E2"], spec="spec-A")
    b = _dec("B", "y", entities=["E1", "E2"], spec="spec-A")  # same spec
    llm = DummyLLM(decider=lambda *args, **kw: _yes(0.9))
    out = run_depends_on_heuristic([CandidatePair(source=a, target=b)], llm)
    assert out == []


def test_depends_on_confidence_clamps_to_floor():
    """LLM confidence below the explicit floor still fails the LLM gate."""
    a = _dec("A", "x", entities=["E1", "E2"], spec="spec-A")
    b = _dec("B", "y", entities=["E1", "E2"], spec="spec-B")
    # 0.69 fails both llm_threshold (0.7) and floor
    llm = DummyLLM(decider=lambda *args, **kw: _yes(0.69))
    out = run_depends_on_heuristic([CandidatePair(source=a, target=b)], llm)
    assert out == []


def test_depends_on_confidence_capped_at_ceiling():
    a = _dec("A", "x", entities=["E1", "E2"], spec="spec-A")
    b = _dec("B", "y", entities=["E1", "E2"], spec="spec-B")
    llm = DummyLLM(decider=lambda *args, **kw: _yes(0.95))  # above ceiling 0.85
    out = run_depends_on_heuristic([CandidatePair(source=a, target=b)], llm)
    assert out[0].confidence == DEPENDS_ON_CEILING


def test_depends_on_min_shared_entities_constant():
    assert DEPENDS_ON_MIN_SHARED_ENTITIES == 2  # mirrors the TR requirement
