"""R13-D — Bridges LLMProvider -> grounding / heuristics / cognitive-extraction.

Covers the 4 test scenarios 1:1 with a real in-memory ``FakeLLMProvider``
(no vendor SDK):

  ts_1f20c2e3 (grounding)        -> test_ts_1f20c2e3_*
  ts_6836948d (heuristic)        -> test_ts_6836948d_*
  ts_45fe110f (opt-in/skips)     -> test_ts_45fe110f_*
  ts_6c2adf69 (closeout ledger)  -> test_ts_6c2adf69_*

Plus a contract guard for the 4 canonical purposes (grounding_extract /
grounding_score / heuristic_polarity / learning_summarise), the fail-closed
wiring, and secret-safe observability (tr_r13d_secret_safe_observability).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
    _summariser_factory,
)
from okto_pulse.core.events.handlers.llm_provider_bridges import (
    make_learning_summariser,
    reset_bridge_cache as reset_summariser_bridge_cache,
    summariser_from_config,
)
from okto_pulse.core.events.types import CardMoved, SpecMoved
from okto_pulse.core.kg.agent.extractors import (
    extract_alternatives,
    extract_assumptions,
)
from okto_pulse.core.kg.agent.heuristics import LLMVerdict
from okto_pulse.core.kg.agent.heuristics.contradiction import (
    DecisionNeighbor,
    DecisionNode,
)
from okto_pulse.core.kg.agent.heuristics.contradiction import (
    run_contradiction_heuristic,
)
from okto_pulse.core.kg.agent.heuristics.llm_provider_bridges import (
    make_heuristic_llm,
    reset_bridge_cache as reset_heuristic_bridge_cache,
)
from okto_pulse.core.kg.grounding import (
    Claim,
    score_semantic_grounding,
    verify_grounding,
)
from okto_pulse.core.kg.grounding.llm_provider_bridges import (
    make_extractor_fn,
    make_grounder_fn,
    reset_bridge_cache as reset_grounding_bridge_cache,
)
from okto_pulse.core.kg.interfaces.llm import (
    LLM_PROVIDER_ABSENT,
    LLM_PROVIDER_ERROR,
    LLM_TIMEOUT,
    LLMResponse,
)
from sqlalchemy_test_models import CardType

ANALYSIS = (
    "## Analysis\n"
    "We considered using Redis instead of Postgres for the cache layer.\n"
    "We assume the cache hit ratio stays above 90 percent in production.\n"
)


# ===========================================================================
# In-memory fake provider, routed by request.purpose.
# ===========================================================================
class FakeLLMProvider:
    def __init__(
        self,
        *,
        name: str = "fake",
        extract=None,
        score=None,
        verdict=None,
        summary=None,
        extract_text: str | None = None,
        fail: str | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.name = name
        self._extract = extract
        self._extract_text = extract_text
        self._score = score
        self._verdict = verdict
        self._summary = summary
        self._fail = fail
        self._raises = raises
        self.calls: list = []

    def complete(self, request):
        self.calls.append(request)
        if self._raises is not None:
            raise self._raises
        if self._fail is not None:
            return LLMResponse.failure(
                self._fail, failure_reason=f"detail:{self._fail}"
            )
        p = request.purpose
        if p == "grounding_extract":
            if self._extract_text is not None:
                return LLMResponse(text=self._extract_text)
            return LLMResponse(json=self._extract)
        if p == "grounding_score":
            return LLMResponse(json=self._score)
        if p == "heuristic_polarity":
            return LLMResponse(json=self._verdict)
        if p == "learning_summarise":
            return LLMResponse(json=self._summary)
        return LLMResponse()

    def capabilities(self) -> dict:
        return {"is_stub": True, "name": self.name}


@pytest.fixture(autouse=True)
def _reset_caches():
    reset_grounding_bridge_cache()
    reset_heuristic_bridge_cache()
    reset_summariser_bridge_cache()
    yield
    reset_grounding_bridge_cache()
    reset_heuristic_bridge_cache()
    reset_summariser_bridge_cache()


# ===========================================================================
# ts_1f20c2e3 (grounding) — never-raise; fail-closed; vacuous-truth; sanitize.
# ===========================================================================
def test_ts_1f20c2e3_happy_path_grounded():
    extractor = make_extractor_fn(
        FakeLLMProvider(
            extract=[{"text": "claim one", "mentioned_entities": ["Decision X"]}]
        ),
        board_id="b1",
    )
    grounder = make_grounder_fn(
        FakeLLMProvider(
            score=[{"grounded": True, "confidence": 0.9, "supporting_ids": ["n1"]}]
        )
    )
    res = verify_grounding(
        "answer text",
        [{"title": "Decision X", "node_id": "n1"}],
        extractor_fn=extractor,
        grounder_fn=grounder,
    )
    assert res.overall_grounded is True
    assert res.confidence == 0.9
    assert res.hallucinated_entities == ()


def test_ts_1f20c2e3_extractor_provider_failure_is_fail_closed():
    extractor = make_extractor_fn(FakeLLMProvider(fail=LLM_TIMEOUT))
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    res = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder
    )
    # provider failure -> raise -> verify_grounding catches -> (False, 0.0).
    assert res.overall_grounded is False
    assert res.confidence == 0.0


def test_ts_1f20c2e3_extractor_raw_exception_is_fail_closed():
    extractor = make_extractor_fn(FakeLLMProvider(raises=RuntimeError("boom")))
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    res = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder
    )
    assert res.overall_grounded is False and res.confidence == 0.0


def test_ts_1f20c2e3_invalid_extractor_payload_is_fail_closed_not_vacuous():
    # OK response but text is NOT a JSON array -> SAFETY: fail-closed, not 1.0.
    extractor = make_extractor_fn(
        FakeLLMProvider(extract_text="no claims here, just prose")
    )
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    res = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder
    )
    assert res.overall_grounded is False
    assert res.confidence == 0.0


def test_ts_1f20c2e3_no_claims_is_vacuous_truth():
    # Provider succeeded and returned an empty claim list -> vacuous (True, 1.0).
    extractor = make_extractor_fn(FakeLLMProvider(extract=[]))
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    res = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder
    )
    assert res.overall_grounded is True
    assert res.confidence == 1.0


def test_ts_1f20c2e3_grounder_provider_failure_is_fail_closed():
    extractor = make_extractor_fn(
        FakeLLMProvider(extract=[{"text": "c1", "mentioned_entities": []}])
    )
    grounder = make_grounder_fn(FakeLLMProvider(fail=LLM_PROVIDER_ERROR))
    res = verify_grounding(
        "answer", [], extractor_fn=extractor, grounder_fn=grounder
    )
    assert res.overall_grounded is False and res.confidence == 0.0


def test_ts_1f20c2e3_grounder_scores_are_sanitized_and_padded():
    # Grounder returns FEWER + missing-key items -> score_semantic_grounding
    # pads the tail with defaults and coerces missing keys (UNCHANGED).
    grounder = make_grounder_fn(
        FakeLLMProvider(score=[{"grounded": True}])  # 1 item, missing keys
    )
    claims = [Claim("a"), Claim("b")]
    scores = score_semantic_grounding(claims, [], grounder)
    assert len(scores) == 2
    assert scores[0] == {
        "grounded": True, "confidence": 0.0, "supporting_ids": []
    }
    assert scores[1] == {
        "grounded": False, "confidence": 0.0, "supporting_ids": []
    }


def test_ts_1f20c2e3_non_bool_grounded_is_not_marked_grounded():
    # SECURITY (codex val_b620e4ec): a non-bool `grounded` must NOT be coerced.
    # bool("false") is True in Python — so the string "false" (or "true", or an
    # int) is a malformed score and MUST be fail-closed (not grounded), never a
    # coerced-truthy grounded claim.
    extractor = make_extractor_fn(
        FakeLLMProvider(extract=[{"text": "c1", "mentioned_entities": []}])
    )
    grounder = make_grounder_fn(
        FakeLLMProvider(
            score=[{"grounded": "false", "confidence": 0.9, "supporting_ids": ["n1"]}]
        )
    )
    res = verify_grounding(
        "answer", [{"node_id": "n1", "title": "t"}],
        extractor_fn=extractor, grounder_fn=grounder,
    )
    assert res.overall_grounded is False  # "false" must NOT coerce to True
    assert len(res.unsupported_claims) == 1

    # Direct sanitizer check: a "true" STRING is also fail-closed (only a real
    # bool True marks a claim grounded).
    reset_grounding_bridge_cache()
    grounder2 = make_grounder_fn(
        FakeLLMProvider(score=[{"grounded": "true", "confidence": 0.5}])
    )
    scores = score_semantic_grounding([Claim("c1")], [], grounder2)
    assert scores[0]["grounded"] is False


# ===========================================================================
# ts_6836948d (heuristic) — ask_polarity preserves fields; conf clamp; reason.
# ===========================================================================
def test_ts_6836948d_ask_polarity_preserves_fields():
    llm = make_heuristic_llm(
        FakeLLMProvider(
            verdict={
                "answer": True,
                "confidence": 0.77,
                "reasoning": "These two decisions can clearly coexist.",
            }
        ),
        board_id="b1",
    )
    v = llm.ask_polarity(
        prompt_id="contradiction_v1", text_a="A", text_b="B", context={"k": "v"}
    )
    assert isinstance(v, LLMVerdict)
    assert v.answer is True
    assert v.confidence == 0.77
    assert v.reasoning == "These two decisions can clearly coexist."


def test_ts_6836948d_confidence_is_clamped_to_unit_interval():
    high = make_heuristic_llm(
        FakeLLMProvider(verdict={"answer": False, "confidence": 1.5, "reasoning": "x" * 25})
    )
    v_high = high.ask_polarity(prompt_id="p", text_a="a", text_b="b")
    assert v_high.confidence == 1.0

    low = make_heuristic_llm(
        FakeLLMProvider(verdict={"answer": True, "confidence": -0.3, "reasoning": "y" * 25})
    )
    v_low = low.ask_polarity(prompt_id="p", text_a="a", text_b="b")
    assert v_low.confidence == 0.0


def test_ts_6836948d_non_bool_answer_is_fail_closed():
    # SECURITY (codex val_b620e4ec): a non-bool `answer` must NOT be coerced —
    # bool("false") is True in Python, which would emit a cognitive edge from a
    # malformed verdict. Any non-bool value fails closed (answer=False, conf 0.0
    # -> below every heuristic floor -> NO edge).
    for bad in ("false", "true", 1, 0, "yes", None):
        reset_heuristic_bridge_cache()
        provider = FakeLLMProvider(
            verdict={"answer": bad, "confidence": 0.9, "reasoning": "z" * 25}
        )
        v = make_heuristic_llm(provider).ask_polarity(
            prompt_id="p", text_a="a", text_b="b"
        )
        assert v.answer is False, f"non-bool answer {bad!r} coerced to True"
        assert v.confidence == 0.0


def test_ts_6836948d_provider_failure_is_fail_closed_verdict():
    for provider in (
        FakeLLMProvider(fail=LLM_PROVIDER_ERROR),
        FakeLLMProvider(raises=RuntimeError("down")),
    ):
        llm = make_heuristic_llm(provider)
        v = llm.ask_polarity(prompt_id="p", text_a="a", text_b="b")
        assert v.answer is False
        assert v.confidence == 0.0
        assert len(v.reasoning) >= 20  # auditable, BR Cognitive Edge Evidence


def test_ts_6836948d_integration_with_real_heuristic_emits_and_gates():
    # Provider says "cannot coexist" (answer False) with high confidence ->
    # the contradiction heuristic emits a candidate via the bridge LLM.
    src = DecisionNode("d_src", "Poll every 60 min", entity_ids=frozenset({"X"}), spec_id="s1")
    neigh = DecisionNeighbor(
        DecisionNode("d_n", "Must sync real time", entity_ids=frozenset({"X"}), spec_id="s2"),
        similarity=0.82,
    )
    yes_contra = make_heuristic_llm(
        FakeLLMProvider(
            verdict={"answer": False, "confidence": 0.8, "reasoning": "Mutually exclusive cadence."}
        )
    )
    out = run_contradiction_heuristic(src, [neigh], yes_contra)
    assert len(out) == 1
    assert out[0].from_node_id == "d_src" and out[0].to_node_id == "d_n"

    # Provider failure -> fail-closed verdict (conf 0.0 < floor) -> NO edge.
    failing = make_heuristic_llm(FakeLLMProvider(fail=LLM_TIMEOUT))
    out2 = run_contradiction_heuristic(src, [neigh], failing)
    assert out2 == []


# ===========================================================================
# ts_45fe110f (opt-in / skips) — no_llm_config / unknown_provider / openai stub
# + Alternative/Assumption regex run WITHOUT any LLM.
# ===========================================================================
def test_ts_45fe110f_no_llm_config_skip():
    # Falsy config -> None (caller logs no_llm_config + skips Learning).
    assert summariser_from_config(None) is None
    assert summariser_from_config({}) is None


def test_ts_45fe110f_unknown_provider_skip():
    # No provider supplied + unknown provider in config -> None (skip).
    assert summariser_from_config({"provider": "anthropic"}) is None
    # The legacy factory the handler uses preserves the same skip.
    assert _summariser_factory({"provider": "anthropic"}) is None


def test_ts_45fe110f_openai_stub_is_deterministic():
    summ = summariser_from_config({"provider": "openai"})
    assert summ is not None
    title, body = summ.summarise(
        bug_title="Regex misfires on accents",
        action_plan="Repro; root cause missing NFC; fixed + regression test.",
    )
    # Deterministic stub shape (unchanged): title prefix + truncated body.
    assert title == "Lesson from: Regex misfires on accents"
    assert body == "Repro; root cause missing NFC; fixed + regression test."


def test_ts_45fe110f_provider_path_produces_summary():
    provider = FakeLLMProvider(
        summary={"title": "Always normalise NFC", "body": "Guard encoding first."}
    )
    summ = summariser_from_config({"provider": "x"}, provider=provider)
    assert summ is not None
    title, body = summ.summarise(bug_title="t", action_plan="p" * 60)
    assert title == "Always normalise NFC"
    assert body == "Guard encoding first."
    assert provider.calls[-1].purpose == "learning_summarise"


def test_ts_45fe110f_provider_failure_yields_no_learning():
    # Provider failure -> ("","") -> extract_learning_from_bug creates nothing.
    summ = make_learning_summariser(FakeLLMProvider(fail=LLM_PROVIDER_ERROR))
    assert summ.summarise(bug_title="t", action_plan="p" * 60) == ("", "")


def test_ts_45fe110f_alternative_and_assumption_regex_need_no_llm():
    alts = extract_alternatives(spec_context=ANALYSIS, qa_texts=None, source_ref="spec:x")
    assums = extract_assumptions(spec_context=ANALYSIS, qa_texts=None, source_ref="spec:x")
    # Regex extractors are LLM-independent — they yield candidates with no
    # provider in sight.
    assert len(alts) >= 1
    assert len(assums) >= 1


# ===========================================================================
# ts_6c2adf69 (closeout ledger boundary) — CRITICAL.
# Handler only OPENS the durable ledger pending in the drain; it writes NO KG
# node and does NOT run the worker persistence inline.
# ===========================================================================
def _spec_moved(*, spec_id="spec-1", to_status="done") -> SpecMoved:
    return SpecMoved(
        board_id="board-1", spec_id=spec_id, from_status="in_progress",
        to_status=to_status,
    )


def _bug_moved(*, card_id="card-1", to_status="done") -> CardMoved:
    return CardMoved(
        board_id="board-1", card_id=card_id, from_status="validation",
        to_status=to_status,
    )


def _bug_card(*, card_id="card-1", action_plan="x" * 200):
    return SimpleNamespace(
        id=card_id, card_type=CardType.BUG, spec_id=None, action_plan=action_plan
    )


def _board(*, llm_config=None):
    return SimpleNamespace(
        settings={"cognitive_llm_config": llm_config} if llm_config else {}
    )


def _make_session(*, card=None, board=None, spec=None) -> AsyncMock:
    sess = AsyncMock()

    async def fake_get(model, oid):
        name = model.__name__
        if name == "Card":
            return card
        if name == "Board":
            return board
        if name == "Spec":
            return spec
        return None

    sess.get = AsyncMock(side_effect=fake_get)
    return sess


@pytest.fixture
def _ledger_boundary_spies(monkeypatch):
    """Patch the ledger open + guard against any KG node write / inline worker
    run, returning the recorded ledger opens."""
    opened: list[tuple] = []

    def _rec_open(*, board_id, source_ref, artifact_type, **_k):
        opened.append((board_id, source_ref, artifact_type))
        return 1

    def _guard_node_write(*_a, **_k):
        raise AssertionError("KG node write attempted inside the event drain!")

    def _guard_worker(*_a, **_k):
        raise AssertionError("worker persistence invoked inside the event drain!")

    monkeypatch.setattr(
        "okto_pulse.core.kg.cognitive_closeout_production."
        "open_cognitive_closeout_pending",
        _rec_open,
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.primitives._apply_kuzu_node_create_with_timestamp",
        _guard_node_write,
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.cognitive_closeout_production.run_cognitive_closeout",
        _guard_worker,
        raising=False,
    )
    # Avoid any real graph read in the bug branch idempotency probe.
    monkeypatch.setattr(
        "okto_pulse.core.events.handlers.cognitive_extraction."
        "_learning_already_exists",
        lambda *_a, **_k: False,
    )
    return opened


@pytest.mark.asyncio
async def test_ts_6c2adf69_spec_done_opens_ledger_only_no_kg_write(
    _ledger_boundary_spies,
):
    opened = _ledger_boundary_spies
    handler = CognitiveExtractionHandler()
    await handler.handle(_spec_moved(spec_id="spec-9"), _make_session())
    # Exactly the ledger pending was opened; no KG write, no inline worker run
    # (the write guards would have raised AssertionError otherwise).
    assert opened == [("board-1", "spec:spec-9", "spec")]


@pytest.mark.asyncio
async def test_ts_6c2adf69_bug_done_opens_ledger_only_no_kg_write(
    _ledger_boundary_spies,
):
    opened = _ledger_boundary_spies
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=_bug_card(card_id="card-7"),
        board=_board(llm_config={"provider": "openai", "model": "gpt-4o-mini"}),
    )
    await handler.handle(_bug_moved(card_id="card-7"), sess)
    assert ("board-1", "bug:card-7", "bug") in opened


@pytest.mark.asyncio
async def test_ts_6c2adf69_non_done_transition_opens_nothing(
    _ledger_boundary_spies,
):
    opened = _ledger_boundary_spies
    handler = CognitiveExtractionHandler()
    await handler.handle(
        _spec_moved(to_status="in_progress"), _make_session()
    )
    assert opened == []  # no ledger handoff for non-terminal transitions


# ===========================================================================
# tr_r13d_secret_safe_observability — no full prompt / action_plan / creds /
# full LLM responses leak through logs or error reasons.
# ===========================================================================
def test_tr_r13d_secret_safe_grounding_failure_logs_no_prompt(caplog):
    import logging

    secret_answer = "SUPER_SECRET_ANSWER_PAYLOAD_DO_NOT_LOG"
    extractor = make_extractor_fn(FakeLLMProvider(fail=LLM_PROVIDER_ERROR))
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.grounding"):
        res = verify_grounding(
            secret_answer, [], extractor_fn=extractor, grounder_fn=grounder
        )
    assert res.overall_grounded is False
    blob = " ".join(r.getMessage() for r in caplog.records)
    # The full answer text is never logged; only a short normalized reason.
    assert secret_answer not in blob


@pytest.mark.asyncio
async def test_tr_r13d_secret_safe_closeout_truncates_and_hides_creds(
    _ledger_boundary_spies, caplog,
):
    import logging

    secret_marker = "SECRET_AT_END_OF_PLAN"
    api_key_env = "MY_PRIVATE_KEY_ENV"
    action_plan = "A" * 600 + secret_marker  # marker sits past the 500 excerpt
    handler = CognitiveExtractionHandler()
    sess = _make_session(
        card=_bug_card(card_id="card-x", action_plan=action_plan),
        board=_board(
            llm_config={
                "provider": "openai",
                "model": "gpt-4o-mini",
                "api_key_env": api_key_env,
            }
        ),
    )
    with caplog.at_level(
        logging.DEBUG, logger="okto_pulse.core.events.cognitive_extraction"
    ):
        await handler.handle(_bug_moved(card_id="card-x"), sess)
    blob = " ".join(r.getMessage() for r in caplog.records)
    extras = " ".join(str(getattr(r, "action_plan_excerpt", "")) for r in caplog.records)
    combined = blob + " " + extras
    # action_plan is truncated (>500 marker absent) and the api key env is never logged.
    assert secret_marker not in combined
    assert api_key_env not in combined


# ===========================================================================
# Contract: canonical purposes (api_r13d_cognitive_extraction_bridge_contract).
# Regression guard — fails if a bridge drifts off the canonical purpose names.
# ===========================================================================
def test_contract_canonical_purposes_for_four_flows():
    p_ex = FakeLLMProvider(extract=[])
    make_extractor_fn(p_ex, board_id="b1", actor_id="a1")("answer text")
    req = p_ex.calls[-1]
    assert req.purpose == "grounding_extract"
    assert req.telemetry_labels == {"flow": "grounding_extract"}
    assert req.board_id == "b1" and req.actor_id == "a1"

    p_sc = FakeLLMProvider(score=[])
    make_grounder_fn(p_sc)([Claim("c")], [{"node_id": "n1", "title": "t"}])
    req = p_sc.calls[-1]
    assert req.purpose == "grounding_score"
    assert req.telemetry_labels == {"flow": "grounding_score"}

    p_he = FakeLLMProvider(verdict={"answer": True, "confidence": 0.5, "reasoning": "z" * 25})
    make_heuristic_llm(p_he).ask_polarity(prompt_id="contradiction_v1", text_a="a", text_b="b")
    req = p_he.calls[-1]
    assert req.purpose == "heuristic_polarity"
    assert req.telemetry_labels == {"flow": "heuristic_polarity"}
    assert req.prompt_id == "contradiction_v1"

    p_ls = FakeLLMProvider(summary={"title": "t", "body": "b"})
    make_learning_summariser(p_ls).summarise(bug_title="bt", action_plan="p" * 60)
    req = p_ls.calls[-1]
    assert req.purpose == "learning_summarise"
    assert req.telemetry_labels == {"flow": "learning_summarise"}


def test_contract_fail_closed_wiring_requires_provider():
    with pytest.raises(ValueError):
        make_extractor_fn(None)
    with pytest.raises(ValueError):
        make_grounder_fn(None)
    with pytest.raises(ValueError):
        make_heuristic_llm(None)
    with pytest.raises(ValueError):
        make_learning_summariser(None)


def test_contract_provider_absent_status_grounding_fail_closed():
    extractor = make_extractor_fn(FakeLLMProvider(fail=LLM_PROVIDER_ABSENT))
    grounder = make_grounder_fn(FakeLLMProvider(score=[]))
    res = verify_grounding("a", [], extractor_fn=extractor, grounder_fn=grounder)
    assert res.overall_grounded is False and res.confidence == 0.0
