"""Spec 3d907a87 (FR4 / D2 / TS7) — extract_assumptions extractor.

Closes the gap of an Assumption node type listed in NODE_TYPES with no
extractor to feed it. Tests pin the regex behavior against the markers
declared in the BR4 contract: PT (assumindo que, presume-se, assume-se,
pressupõe-se), EN (assuming, we assume, assumed that), conditionals
(caso X então, if X then).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.agent.extractors import (
    AssumptionExtraction,
    extract_assumptions,
)
from okto_pulse.core.kg.agent.extractors.assumptions import _MARKER_RE


def test_pt_assumindo_que_marker_is_extracted():
    text = "## Analysis\n\nAssumindo que o usuario tem permissao admin, o fluxo segue."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1
    assert "assumindo que" in out[0].body.lower()
    assert out[0].source_section == "analysis"
    assert out[0].source_ref == "spec:1"


def test_pt_presume_se_marker_is_extracted():
    text = "## Analysis\n\nPresume-se que a base de dados nunca exceda 1M linhas."
    out = extract_assumptions(spec_context=text, source_ref="ref:9")
    assert len(out) == 1
    assert "presume-se" in out[0].body.lower()


def test_pt_pressupoe_se_with_diacritic():
    text = "## Analysis\n\nPressupõe-se autenticacao previa via OAuth."
    out = extract_assumptions(spec_context=text, source_ref="spec:2")
    assert len(out) == 1
    assert "pressupõe-se" in out[0].body.lower() or "pressupoe-se" in out[0].body.lower()


def test_en_assuming_marker_is_extracted():
    text = "## Analysis\n\nAssuming the cache hit rate is above 95 percent, we use TTL of 1h."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1
    assert "assuming" in out[0].body.lower()


def test_en_we_assume_marker_is_extracted():
    text = "## Analysis\n\nWe assume that the upstream service is always reachable."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1


def test_conditional_caso_entao_pt():
    text = "## Analysis\n\nCaso o request falhe entao retornamos 503 imediatamente."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1


def test_conditional_if_then_en():
    text = "## Analysis\n\nIf the user has no role then we deny by default."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1


def test_qa_text_is_scanned_whole():
    qa = ["Q: como tratamos timeout? R: assumindo que os timeouts sao raros, log e retry."]
    out = extract_assumptions(spec_context="", qa_texts=qa, source_ref="spec:1")
    assert len(out) == 1
    assert out[0].source_section == "qa"


def test_no_analysis_section_returns_empty():
    text = "Some random text without analysis header. Assumindo que isso nao deveria casar."
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    # Without ## Analysis the spec_context branch is skipped entirely.
    assert out == []


def test_empty_inputs_return_empty():
    assert extract_assumptions(spec_context="", qa_texts=None, source_ref="") == []
    assert extract_assumptions(spec_context="", qa_texts=[], source_ref="") == []
    assert extract_assumptions(spec_context="", qa_texts=[""], source_ref="") == []


def test_multiple_markers_yield_multiple_extractions():
    text = (
        "## Analysis\n\n"
        "Assumindo que A. We assume B. If C then D."
    )
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 3


def test_extraction_is_a_frozen_dataclass():
    e = AssumptionExtraction(title="T", body="B", source_section="analysis", source_ref="r")
    with pytest.raises(Exception):
        e.title = "x"  # frozen → FrozenInstanceError


def test_title_is_truncated_to_120_chars():
    sentence = "Assumindo que " + ("x" * 200) + "."
    text = "## Analysis\n\n" + sentence
    out = extract_assumptions(spec_context=text, source_ref="spec:1")
    assert len(out) == 1
    assert len(out[0].title) <= 120


def test_marker_regex_is_case_insensitive():
    assert _MARKER_RE.search("ASSUMING X.")
    assert _MARKER_RE.search("Assumindo Que Y.")
    assert _MARKER_RE.search("we ASSUME z.")
