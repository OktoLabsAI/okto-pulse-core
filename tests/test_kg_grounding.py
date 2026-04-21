"""Unit tests for kg.grounding (ideação d3dfdab8)."""

from __future__ import annotations

import dataclasses

import pytest

from okto_pulse.core.kg.grounding import (
    Claim,
    GroundingResult,
    check_entities_present,
    score_semantic_grounding,
    verify_grounding,
)


# ===========================================================================
# Dataclass contracts
# ===========================================================================


def test_claim_is_frozen():
    c = Claim(text="hi", mentioned_entities=("x",))
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.text = "other"  # type: ignore[misc]


def test_grounding_result_frozen_five_fields():
    r = GroundingResult(
        overall_grounded=True,
        confidence=1.0,
    )
    fields = {f.name for f in dataclasses.fields(r)}
    assert fields == {
        "overall_grounded",
        "confidence",
        "hallucinated_entities",
        "unsupported_claims",
        "attribution_map",
    }
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.overall_grounded = False  # type: ignore[misc]


# ===========================================================================
# check_entities_present
# ===========================================================================


def test_exact_match_populates_present_and_hallucinated():
    rows = [{"title": "Decision X"}, {"title": "Card A"}]
    present, hallucinated = check_entities_present(
        ["Decision X", "Decision Y"], rows,
    )
    assert present == {"Decision X"}
    assert hallucinated == {"Decision Y"}


def test_jaccard_fallback_matches_above_threshold():
    # "supersedence chain rule extra" vs title "supersedence chain rule"
    # tokens {supersedence, chain, rule, extra} ∩ {supersedence, chain, rule}
    # = 3; union = 4; ratio = 0.75 >= 0.7 → match
    rows = [{"title": "supersedence chain rule"}]
    present, hallucinated = check_entities_present(
        ["supersedence chain rule extra"], rows, threshold=0.7,
    )
    assert present == {"supersedence chain rule extra"}


def test_jaccard_fallback_rejects_below_threshold():
    # "supersedence rule" vs "supersedence chain rule" → 2/3 ≈ 0.67 < 0.7
    rows = [{"title": "supersedence chain rule"}]
    present, hallucinated = check_entities_present(
        ["supersedence rule"], rows, threshold=0.7,
    )
    assert "supersedence rule" in hallucinated


def test_normalization_case_and_diacritics():
    rows = [{"title": "CAFÉ"}]
    present, hallucinated = check_entities_present(["cafe"], rows)
    assert "cafe" in present
    assert not hallucinated


def test_empty_entities_returns_empty_sets():
    assert check_entities_present([], [{"title": "x"}]) == (set(), set())


# ===========================================================================
# score_semantic_grounding
# ===========================================================================


def test_score_invokes_grounder_once():
    counter = {"count": 0}

    def fn(claims, rows):
        counter["count"] += 1
        return [
            {"grounded": True, "confidence": 1.0, "supporting_ids": []}
        ] * len(claims)

    claims = [Claim(text=f"c{i}") for i in range(3)]
    scores = score_semantic_grounding(claims, [], fn)
    assert counter["count"] == 1
    assert len(scores) == 3


def test_score_fills_defaults_when_grounder_returns_wrong_len():
    def fn(claims, rows):
        return [{"grounded": True, "confidence": 0.9, "supporting_ids": []}]

    claims = [Claim(text=f"c{i}") for i in range(3)]
    scores = score_semantic_grounding(claims, [], fn)
    assert len(scores) == 3
    assert scores[0]["grounded"] is True
    assert scores[1] == {"grounded": False, "confidence": 0.0, "supporting_ids": []}
    assert scores[2] == {"grounded": False, "confidence": 0.0, "supporting_ids": []}


def test_score_non_list_returns_all_defaults():
    def fn(claims, rows):
        return "not a list"  # type: ignore[return-value]

    claims = [Claim(text="c")]
    scores = score_semantic_grounding(claims, [], fn)
    assert len(scores) == 1
    assert scores[0] == {"grounded": False, "confidence": 0.0, "supporting_ids": []}


# ===========================================================================
# verify_grounding — orchestrator
# ===========================================================================


def test_verify_grounding_happy_path():
    rows = [
        {"node_id": "n1", "title": "Decision X"},
        {"node_id": "n2", "title": "Rule Y"},
    ]

    def extractor(answer):
        return [
            Claim(text="decision x is valid",
                  mentioned_entities=("Decision X",)),
            Claim(text="rule y applies",
                  mentioned_entities=("Rule Y",)),
        ]

    def grounder(claims, rows):
        return [
            {"grounded": True, "confidence": 1.0, "supporting_ids": ["n1"]},
            {"grounded": True, "confidence": 1.0, "supporting_ids": ["n2"]},
        ]

    r = verify_grounding(
        "answer", rows, extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.overall_grounded is True
    assert r.confidence == 1.0
    assert r.hallucinated_entities == ()
    assert r.unsupported_claims == ()
    assert len(r.attribution_map) == 2


def test_verify_grounding_hallucination_reproves():
    rows = [{"node_id": "n1", "title": "Decision X"}]

    def extractor(answer):
        return [
            Claim(text="claim",
                  mentioned_entities=("Decision X", "Decision_fake"))
        ]

    def grounder(claims, rows):
        return [{"grounded": True, "confidence": 1.0, "supporting_ids": ["n1"]}]

    r = verify_grounding(
        "answer", rows, extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.overall_grounded is False
    assert "Decision_fake" in r.hallucinated_entities


def test_verify_grounding_low_confidence_reproves():
    rows = [{"node_id": "n1", "title": "X"}]

    def extractor(answer):
        return [
            Claim(text="c1", mentioned_entities=("X",)),
            Claim(text="c2", mentioned_entities=("X",)),
        ]

    def grounder(claims, rows):
        return [
            {"grounded": True, "confidence": 0.3, "supporting_ids": []},
            {"grounded": True, "confidence": 0.4, "supporting_ids": []},
        ]

    r = verify_grounding(
        "answer", rows, extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.overall_grounded is False
    assert r.confidence == pytest.approx(0.35, rel=1e-3)


def test_verify_grounding_extractor_exception_safe_verdict():
    def extractor(answer):
        raise RuntimeError("LLM down")

    def grounder(claims, rows):
        return []

    r = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.overall_grounded is False
    assert r.confidence == 0.0


def test_verify_grounding_grounder_exception_safe_verdict():
    def extractor(answer):
        return [Claim(text="c")]

    def grounder(claims, rows):
        raise RuntimeError("LLM down")

    r = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.overall_grounded is False
    assert r.confidence == 0.0


def test_verify_grounding_attribution_map_carries_supporting_ids():
    rows = [{"node_id": "n1", "title": "X"}]

    def extractor(answer):
        return [
            Claim(text="claim 1", mentioned_entities=("X",)),
            Claim(text="claim 2", mentioned_entities=("X",)),
        ]

    def grounder(claims, rows):
        return [
            {"grounded": True, "confidence": 0.9,
             "supporting_ids": ["n1", "n2"]},
            {"grounded": True, "confidence": 0.8,
             "supporting_ids": ["n3"]},
        ]

    r = verify_grounding(
        "answer", rows, extractor_fn=extractor, grounder_fn=grounder,
    )
    assert r.attribution_map[0]["claim"] == "claim 1"
    assert r.attribution_map[0]["source_node_ids"] == ("n1", "n2")
    assert r.attribution_map[1]["source_node_ids"] == ("n3",)


def test_verify_grounding_empty_claims_returns_grounded_true_trivially():
    def extractor(answer):
        return []

    def grounder(claims, rows):
        return []

    r = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder,
    )
    # Vacuous truth — no claims to verify.
    assert r.overall_grounded is True
    assert r.confidence == 1.0
