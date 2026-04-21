"""Tests for cognitive extractors (cards 14cd6bd9 + b4df0783)."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.kg.agent.extractors import (
    LEARNING_MIN_ACTION_PLAN_CHARS,
    extract_alternatives,
    extract_learning_from_bug,
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
    assert all(r.source_ref == "spec:abc" for r in results)


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


# ===========================================================================
# Learning extractor
# ===========================================================================


@dataclass
class DummySummariser:
    title: str
    body: str
    calls: int = 0

    def summarise(self, *, bug_title, action_plan, context=None):
        self.calls += 1
        return self.title, self.body


def test_learning_emitted_for_done_bug_with_long_action_plan():
    summariser = DummySummariser(
        title="Guard unsafe user input",
        body="Always normalise encoding before regex matching.",
    )
    result = extract_learning_from_bug(
        bug_node_id="bug_01",
        bug_title="Regex misfires on é chars",
        bug_status="done",
        card_type="bug",
        action_plan="Ran repro locally; normalised NFC; added test_regex_normalised.",
        summariser=summariser,
    )
    assert result is not None
    assert result.bug_node_id == "bug_01"
    assert result.learning_title == "Guard unsafe user input"
    assert result.confidence == 0.9
    assert result.cognitive_evidence  # populated with action plan
    assert summariser.calls == 1


def test_learning_rejects_non_bug_card_type():
    summariser = DummySummariser("T", "B")
    assert extract_learning_from_bug(
        bug_node_id="x", bug_title="y", bug_status="done",
        card_type="normal", action_plan="X" * 100,
        summariser=summariser,
    ) is None
    assert summariser.calls == 0


def test_learning_rejects_non_done_status():
    summariser = DummySummariser("T", "B")
    assert extract_learning_from_bug(
        bug_node_id="x", bug_title="y", bug_status="in_progress",
        card_type="bug", action_plan="X" * 100,
        summariser=summariser,
    ) is None


def test_learning_rejects_short_action_plan():
    summariser = DummySummariser("T", "B")
    plan = "x" * (LEARNING_MIN_ACTION_PLAN_CHARS - 1)
    assert extract_learning_from_bug(
        bug_node_id="x", bug_title="y", bug_status="done",
        card_type="bug", action_plan=plan,
        summariser=summariser,
    ) is None


def test_learning_rejects_when_summariser_returns_empty():
    summariser = DummySummariser("", "")
    result = extract_learning_from_bug(
        bug_node_id="x", bug_title="y", bug_status="done",
        card_type="bug", action_plan="X" * 100,
        summariser=summariser,
    )
    assert result is None


def test_learning_accepts_custom_min_action_plan_chars():
    summariser = DummySummariser("T", "B")
    result = extract_learning_from_bug(
        bug_node_id="x", bug_title="y", bug_status="done",
        card_type="bug", action_plan="x" * 30,
        summariser=summariser,
        min_action_plan_chars=20,
    )
    assert result is not None


def test_learning_carries_linked_constraint_hint():
    summariser = DummySummariser("T", "B")
    result = extract_learning_from_bug(
        bug_node_id="b1", bug_title="y", bug_status="done",
        card_type="bug", action_plan="X" * 80,
        summariser=summariser,
        linked_constraint_hint="constraint_id_42",
    )
    assert result.linked_constraint_hint == "constraint_id_42"
