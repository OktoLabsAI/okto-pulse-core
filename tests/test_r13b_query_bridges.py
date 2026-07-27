"""R13-B — Bridges LLMProvider -> legacy query_rewrite / adaptive_hops callables.

Covers the 4 test scenarios of the card 1:1 with a real in-memory
``FakeLLMProvider`` (no vendor SDK):

  ts_7cf9f8e3 (unit)     -> test_ts_7cf9f8e3_bridges_produce_same_rewrite_formats
  ts_588a227f (negative) -> test_ts_588a227f_fail_closed_*
  ts_65ae2ec1 (negative) -> test_ts_65ae2ec1_adaptive_hops_*
  ts_0aaa4359 (cache)    -> test_ts_0aaa4359_cache_identity_*

Principle R13-B: the bridge adapts the LLM *contract* to the callable shape and
NOTHING ELSE — prompts, parse, RRF, hop clamp, and the fail-closed / fallback
behaviours of the consuming flows are preserved bit-for-bit.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.adaptive_hops import (
    get_hop_planner,
    reset_planner_cache,
)
from okto_pulse.core.kg.adaptive_hops.llm_provider_bridges import (
    make_hop_llm_fn,
)
from okto_pulse.core.kg.adaptive_hops.llm_provider_bridges import (
    reset_bridge_cache as reset_hop_bridge_cache,
)
from okto_pulse.core.kg.adaptive_hops.utils import (
    MAX_HOPS_CEILING,
    MIN_HOPS_FLOOR,
)
from okto_pulse.core.kg.interfaces.llm import (
    LLM_PROVIDER_ABSENT,
    LLM_PROVIDER_ERROR,
    LLM_TIMEOUT,
    LLMProviderError,
    LLMResponse,
)
from okto_pulse.core.kg.query_rewrite import (
    RewriteResult,
    get_rewriter,
    reset_rewriter_cache,
)
from okto_pulse.core.kg.query_rewrite.llm_provider_bridges import (
    make_decompose_llm_fn,
    make_fusion_llm_fn,
    make_hyde_llm_fn,
)
from okto_pulse.core.kg.query_rewrite.llm_provider_bridges import (
    reset_bridge_cache as reset_qr_bridge_cache,
)


# ===========================================================================
# In-memory fake provider (satisfies the R13-A LLMProvider Protocol via duck
# typing — no vendor SDK, no network, fully deterministic).
# ===========================================================================
class FakeLLMProvider:
    """Routes by ``request.purpose`` so one fake serves every flow.

    Configure the per-purpose payloads at construction. ``fail`` returns a
    normalized failure response; ``raises`` raises a raw exception from
    ``complete`` (the two provider-failure modes a bridge must surface).
    """

    def __init__(
        self,
        *,
        name: str = "fake",
        hyde_text: str | None = None,
        hyde_json=None,
        decompose=None,
        decompose_text: str | None = None,
        fusion=None,
        hops=None,
        hops_text: str | None = None,
        fail: str | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.name = name
        self._hyde_text = hyde_text
        self._hyde_json = hyde_json
        self._decompose = decompose
        self._decompose_text = decompose_text
        self._fusion = fusion
        self._hops = hops
        self._hops_text = hops_text
        self._fail = fail
        self._raises = raises
        self.calls: list = []

    def complete(self, request) -> LLMResponse:
        self.calls.append(request)
        if self._raises is not None:
            raise self._raises
        if self._fail is not None:
            return LLMResponse.failure(
                self._fail, failure_reason=f"detail:{self._fail}"
            )
        purpose = request.purpose
        if purpose == "query_hyde":
            return LLMResponse(text=self._hyde_text, json=self._hyde_json)
        if purpose == "query_decompose":
            if self._decompose is not None:
                return LLMResponse(json=list(self._decompose))
            return LLMResponse(text=self._decompose_text)
        if purpose == "query_fusion":
            # Faithful to the legacy callable: returns the full configured
            # list regardless of k; the rewriter truncates to k.
            if self._fusion is not None:
                return LLMResponse(json=list(self._fusion))
            return LLMResponse()
        if purpose == "adaptive_hops":
            if self._hops_text is not None:
                return LLMResponse(text=self._hops_text)
            return LLMResponse(json=self._hops)
        return LLMResponse()

    def capabilities(self) -> dict:
        return {"is_stub": True, "name": self.name}


@pytest.fixture(autouse=True)
def _reset_all_caches():
    """Every test starts from a clean factory + bridge cache so identity
    assertions are not polluted by sibling tests."""
    reset_rewriter_cache()
    reset_planner_cache()
    reset_qr_bridge_cache()
    reset_hop_bridge_cache()
    yield
    reset_rewriter_cache()
    reset_planner_cache()
    reset_qr_bridge_cache()
    reset_hop_bridge_cache()


# ===========================================================================
# ts_7cf9f8e3 (unit) — bridges feed compatible callables; rewrite formats are
# byte-for-byte the same as the legacy plain callables.
# ===========================================================================
def test_ts_7cf9f8e3_bridges_produce_same_rewrite_formats():
    # ---- HyDE ----------------------------------------------------------
    provider = FakeLLMProvider(hyde_text="hypothetical passage text")
    via_bridge = get_rewriter(
        "hyde", llm_fn=make_hyde_llm_fn(provider, board_id="b1")
    ).rewrite("q hyde A")

    reset_rewriter_cache()
    via_plain = get_rewriter(
        "hyde", llm_fn=lambda q: "hypothetical passage text"
    ).rewrite("q hyde A")

    assert isinstance(via_bridge, RewriteResult)
    assert via_bridge == via_plain
    assert via_bridge.strategy == "hyde"
    assert via_bridge.hyde_passage == "hypothetical passage text"
    assert via_bridge.rewritten_queries == ("q hyde A",)
    # Request was built per the canonical contract (field names + flow label).
    req = provider.calls[-1]
    assert req.purpose == "query_hyde"
    assert req.input == "q hyde A"
    assert req.board_id == "b1"
    assert req.telemetry_labels == {"flow": "query_hyde"}

    # ---- decompose -----------------------------------------------------
    reset_rewriter_cache()
    reset_qr_bridge_cache()
    provider_d = FakeLLMProvider(decompose=["sub1", "sub2", "sub3"])
    via_bridge_d = get_rewriter(
        "decompose", llm_fn=make_decompose_llm_fn(provider_d)
    ).rewrite("q dec A")

    reset_rewriter_cache()
    via_plain_d = get_rewriter(
        "decompose", llm_fn=lambda q: ["sub1", "sub2", "sub3"]
    ).rewrite("q dec A")

    assert via_bridge_d == via_plain_d
    assert via_bridge_d.strategy == "decompose"
    assert via_bridge_d.rewritten_queries == ("sub1", "sub2", "sub3")
    assert provider_d.calls[-1].purpose == "query_decompose"
    assert provider_d.calls[-1].telemetry_labels == {"flow": "query_decompose"}

    # ---- fusion (rewriter truncates to k; bridge propagates k) ---------
    reset_rewriter_cache()
    reset_qr_bridge_cache()
    provider_f = FakeLLMProvider(fusion=["p1", "p2", "p3", "p4", "p5"])
    via_bridge_f = get_rewriter(
        "fusion",
        llm_fn=make_fusion_llm_fn(provider_f),
        fusion_paraphrases=2,
    ).rewrite("q fus A")

    reset_rewriter_cache()
    via_plain_f = get_rewriter(
        "fusion",
        llm_fn=lambda q, k: ["p1", "p2", "p3", "p4", "p5"],
        fusion_paraphrases=2,
    ).rewrite("q fus A")

    assert via_bridge_f == via_plain_f
    assert via_bridge_f.strategy == "fusion"
    # rewriter's own truncation to k=2 is unchanged
    assert via_bridge_f.rewritten_queries == ("p1", "p2")
    # bridge propagated k through the request (no contract field exists)
    fusion_req = provider_f.calls[-1]
    assert fusion_req.purpose == "query_fusion"
    assert fusion_req.telemetry_labels.get("flow") == "query_fusion"
    assert fusion_req.telemetry_labels.get("paraphrase_count") == "2"


def test_ts_7cf9f8e3_decompose_bridge_parses_text_lines():
    """The decompose bridge also accepts a text payload, splitting into
    stripped non-empty lines — the rewriter's filtering stays in charge."""
    provider = FakeLLMProvider(decompose_text="sub one\n  sub two \n\n")
    result = get_rewriter(
        "decompose", llm_fn=make_decompose_llm_fn(provider)
    ).rewrite("q dec text")
    assert result.strategy == "decompose"
    assert result.rewritten_queries == ("sub one", "sub two")


def test_ts_7cf9f8e3_hyde_empty_passage_degrades_via_rewriter():
    """An empty/blank passage flows through the unchanged rewriter
    empty-passage degradation -> strategy 'none'."""
    provider = FakeLLMProvider(hyde_text="   ")
    result = get_rewriter(
        "hyde", llm_fn=make_hyde_llm_fn(provider)
    ).rewrite("q hyde empty")
    assert result.strategy == "none"
    assert result.hyde_passage is None


# ===========================================================================
# ts_588a227f (negative) — fail-closed. A missing provider/callable never
# silently degrades to noop; the factory still raises ValueError.
# ===========================================================================
def test_ts_588a227f_fail_closed_factory_still_raises_without_provider():
    # No provider -> no llm_fn -> the rewrite factory still raises (no noop).
    for strat in ("hyde", "decompose", "fusion"):
        with pytest.raises(ValueError, match="llm_fn"):
            get_rewriter(strat, llm_fn=None)
    # Symmetric for the adaptive-hops factory.
    with pytest.raises(ValueError, match="llm_fn"):
        get_hop_planner("llm", llm_fn=None)


def test_ts_588a227f_fail_closed_bridges_refuse_missing_provider():
    # Every bridge refuses a missing provider at wiring time (no no-op).
    for maker in (
        make_hyde_llm_fn,
        make_decompose_llm_fn,
        make_fusion_llm_fn,
    ):
        with pytest.raises(ValueError):
            maker(None)
    with pytest.raises(ValueError):
        make_hop_llm_fn(None)


def test_ts_588a227f_provider_failure_raises_then_rewriter_degrades():
    """A provider that FAILS during the call: the bridge RAISES
    LLMProviderError; routed through the rewriter that raise is caught and
    the rewriter degrades to passthrough (behaviour unchanged)."""
    provider = FakeLLMProvider(fail=LLM_TIMEOUT)
    hyde_fn = make_hyde_llm_fn(provider, board_id="b1")

    # 1) The bridge surfaces the failure explicitly (does NOT mask it).
    with pytest.raises(LLMProviderError) as exc:
        hyde_fn("direct call")
    assert exc.value.reason == LLM_TIMEOUT

    # 2) Through the rewriter, the raise degrades silently to 'none'.
    result = get_rewriter("hyde", llm_fn=hyde_fn).rewrite("q hyde fail uniq")
    assert result.strategy == "none"
    assert result.hyde_passage is None

    # decompose + fusion: same — the raise degrades to passthrough.
    provider_d = FakeLLMProvider(fail=LLM_PROVIDER_ERROR)
    dec_fn = make_decompose_llm_fn(provider_d)
    with pytest.raises(LLMProviderError):
        dec_fn("direct")
    res_d = get_rewriter("decompose", llm_fn=dec_fn).rewrite("q dec fail uniq")
    assert res_d.strategy == "none"
    assert res_d.rewritten_queries == ("q dec fail uniq",)

    provider_f = FakeLLMProvider(fail=LLM_PROVIDER_ABSENT)
    fus_fn = make_fusion_llm_fn(provider_f)
    with pytest.raises(LLMProviderError):
        fus_fn("direct", 3)
    res_f = get_rewriter("fusion", llm_fn=fus_fn).rewrite("q fus fail uniq")
    assert res_f.strategy == "none"
    assert res_f.rewritten_queries == ("q fus fail uniq",)


def test_ts_588a227f_raw_exception_also_degrades_via_rewriter():
    """A provider that raises a RAW exception (not a normalized failure) is
    likewise caught by the rewriter -> passthrough."""
    provider = FakeLLMProvider(raises=RuntimeError("boom"))
    hyde_fn = make_hyde_llm_fn(provider)
    with pytest.raises(RuntimeError):
        hyde_fn("direct")
    result = get_rewriter("hyde", llm_fn=hyde_fn).rewrite("q hyde raw uniq")
    assert result.strategy == "none"


# ===========================================================================
# ts_65ae2ec1 (negative) — adaptive hops fallback. A failing provider makes
# the bridge raise; LLMHopPlanner catches it -> llm_error_fallback (clamped),
# never propagating to the consumer.
# ===========================================================================
def test_ts_65ae2ec1_adaptive_hops_provider_failure_falls_back():
    provider = FakeLLMProvider(fail=LLM_PROVIDER_ERROR)
    hop_fn = make_hop_llm_fn(provider, board_id="b1")

    # The bridge raises rather than returning a silent fallback hop.
    with pytest.raises(LLMProviderError) as exc:
        hop_fn("direct", "contradiction", [])
    assert exc.value.reason == LLM_PROVIDER_ERROR

    # The planner catches it -> llm_error_fallback within the clamp range,
    # without propagating any exception to the caller.
    planner = get_hop_planner("llm", llm_fn=hop_fn, fallback_hops=2)
    decision = planner.plan(
        query="hop fail uniq", intent_name="contradiction", seed_titles=[]
    )
    assert decision.reason == "llm_error_fallback"
    assert MIN_HOPS_FLOOR <= decision.hops <= MAX_HOPS_CEILING
    assert decision.hops == 2  # clamped fallback_hops


def test_ts_65ae2ec1_adaptive_hops_raw_exception_falls_back():
    provider = FakeLLMProvider(raises=RuntimeError("provider down"))
    hop_fn = make_hop_llm_fn(provider, board_id="b1")
    planner = get_hop_planner("llm", llm_fn=hop_fn, fallback_hops=3)
    decision = planner.plan(
        query="hop raw uniq", intent_name="impact", seed_titles=["t"]
    )
    assert decision.reason == "llm_error_fallback"
    assert decision.hops == 3


def test_ts_65ae2ec1_invalid_int_payload_falls_back():
    """An OK response whose payload is not a parseable int is an invalid
    response -> bridge raises -> planner falls back (no silent default)."""
    provider = FakeLLMProvider(hops_text="not a number")
    hop_fn = make_hop_llm_fn(provider)
    with pytest.raises(LLMProviderError):
        hop_fn("direct", "contradiction", [])
    planner = get_hop_planner("llm", llm_fn=hop_fn, fallback_hops=2)
    decision = planner.plan(
        query="hop invalid uniq", intent_name="contradiction", seed_titles=[]
    )
    assert decision.reason == "llm_error_fallback"
    assert decision.hops == 2


def test_ts_65ae2ec1_success_path_planner_clamps_not_bridge():
    """The bridge returns the RAW int; the planner clamps to the ceiling and
    reports reason 'llm' (hop ceiling semantics preserved at the planner)."""
    provider = FakeLLMProvider(hops=10)  # absurdly high
    hop_fn = make_hop_llm_fn(provider)
    # Bridge returns the raw, unclamped value.
    assert hop_fn("direct", "contradiction", []) == 10
    planner = get_hop_planner("llm", llm_fn=hop_fn)
    decision = planner.plan(
        query="hop ceiling uniq", intent_name="contradiction", seed_titles=[]
    )
    assert decision.reason == "llm"
    assert decision.hops == MAX_HOPS_CEILING  # clamped by the planner


def test_ts_65ae2ec1_success_path_valid_hops():
    provider = FakeLLMProvider(hops=2)
    hop_fn = make_hop_llm_fn(provider)
    planner = get_hop_planner("llm", llm_fn=hop_fn)
    decision = planner.plan(
        query="hop ok uniq", intent_name="contradiction", seed_titles=[]
    )
    assert decision.reason == "llm"
    assert decision.hops == 2


# ===========================================================================
# ts_0aaa4359 (cache identity) — same (provider, context) reuses; different
# providers/contexts do not. Proven in BOTH directions for query_rewrite
# (factory id(llm_fn) cache) and adaptive_hops (factory + planner LRU).
# ===========================================================================
def test_ts_0aaa4359_query_rewrite_same_identity_reuses():
    provider = FakeLLMProvider(hyde_text="p")
    fn1 = make_hyde_llm_fn(provider, board_id="b1")
    fn2 = make_hyde_llm_fn(provider, board_id="b1")
    # Memoized bridge -> stable callable identity.
    assert fn1 is fn2
    r1 = get_rewriter("hyde", llm_fn=fn1)
    r2 = get_rewriter("hyde", llm_fn=fn2)
    # Same id(llm_fn) -> factory cache hit -> same rewriter instance.
    assert r1 is r2


def test_ts_0aaa4359_query_rewrite_different_provider_misses():
    provider_a = FakeLLMProvider(hyde_text="pa")
    provider_b = FakeLLMProvider(hyde_text="pb")
    fn_a = make_hyde_llm_fn(provider_a, board_id="b1")
    fn_b = make_hyde_llm_fn(provider_b, board_id="b1")
    assert fn_a is not fn_b
    r_a = get_rewriter("hyde", llm_fn=fn_a)
    r_b = get_rewriter("hyde", llm_fn=fn_b)
    # Different id(llm_fn) -> factory cache miss -> distinct instances.
    assert r_a is not r_b


def test_ts_0aaa4359_query_rewrite_different_context_misses():
    provider = FakeLLMProvider(hyde_text="p")
    fn_b1 = make_hyde_llm_fn(provider, board_id="b1")
    fn_b2 = make_hyde_llm_fn(provider, board_id="b2")
    fn_actor = make_hyde_llm_fn(provider, board_id="b1", actor_id="agent-1")
    # Different board/actor context -> different callable identity.
    assert fn_b1 is not fn_b2
    assert fn_b1 is not fn_actor
    assert get_rewriter("hyde", llm_fn=fn_b1) is not get_rewriter(
        "hyde", llm_fn=fn_b2
    )


def test_ts_0aaa4359_adaptive_hops_same_identity_reuses_lru():
    provider = FakeLLMProvider(hops=2)
    fn1 = make_hop_llm_fn(provider, board_id="b1")
    fn2 = make_hop_llm_fn(provider, board_id="b1")
    assert fn1 is fn2  # memoized -> stable identity

    p1 = get_hop_planner("llm", llm_fn=fn1)
    p2 = get_hop_planner("llm", llm_fn=fn2)
    assert p1 is p2  # factory cache hit (same id(llm_fn))

    # Same (query, intent) on the shared planner -> LRU hit -> the provider
    # is invoked exactly once (response reused within the same identity).
    p1.plan(query="same q", intent_name="contradiction", seed_titles=[])
    p1.plan(query="same q", intent_name="contradiction", seed_titles=[])
    assert len(provider.calls) == 1


def test_ts_0aaa4359_adaptive_hops_different_provider_no_reuse():
    provider_a = FakeLLMProvider(hops=2)
    provider_b = FakeLLMProvider(hops=3)
    fn_a = make_hop_llm_fn(provider_a, board_id="b1")
    fn_b = make_hop_llm_fn(provider_b, board_id="b1")
    assert fn_a is not fn_b

    p_a = get_hop_planner("llm", llm_fn=fn_a)
    p_b = get_hop_planner("llm", llm_fn=fn_b)
    assert p_a is not p_b  # distinct planners -> distinct LRUs

    d_a = p_a.plan(query="q", intent_name="contradiction", seed_titles=[])
    d_b = p_b.plan(query="q", intent_name="contradiction", seed_titles=[])
    # No cross-provider reuse: each provider was consulted independently.
    assert len(provider_a.calls) == 1
    assert len(provider_b.calls) == 1
    assert d_a.hops == 2
    assert d_b.hops == 3


# ===========================================================================
# Contract: canonical purposes (api_r13b_query_bridge_contract). Regression
# guard — fails if a bridge drifts back to the non-canonical purpose names.
# ===========================================================================
def test_contract_canonical_purposes_for_all_four_flows():
    """purpose ∈ {query_hyde, query_decompose, query_fusion, adaptive_hops}
    and the ``flow`` telemetry label mirrors it (paraphrase_count kept on
    fusion)."""
    p_hyde = FakeLLMProvider(hyde_text="passage")
    make_hyde_llm_fn(p_hyde, board_id="b1", actor_id="a1")("q")
    req = p_hyde.calls[-1]
    assert req.purpose == "query_hyde"
    assert req.telemetry_labels == {"flow": "query_hyde"}
    assert req.board_id == "b1" and req.actor_id == "a1"

    p_dec = FakeLLMProvider(decompose=["s1", "s2"])
    make_decompose_llm_fn(p_dec)("q")
    req = p_dec.calls[-1]
    assert req.purpose == "query_decompose"
    assert req.telemetry_labels == {"flow": "query_decompose"}

    p_fus = FakeLLMProvider(fusion=["p1", "p2"])
    make_fusion_llm_fn(p_fus)("q", 4)
    req = p_fus.calls[-1]
    assert req.purpose == "query_fusion"
    assert req.telemetry_labels == {
        "flow": "query_fusion",
        "paraphrase_count": "4",
    }

    p_hop = FakeLLMProvider(hops=2)
    make_hop_llm_fn(p_hop)("q", "contradiction", [])
    req = p_hop.calls[-1]
    assert req.purpose == "adaptive_hops"
    assert req.telemetry_labels.get("flow") == "adaptive_hops"


# ===========================================================================
# tr_r13b_factory_wiring — factories ACCEPT a provider (derive the bridge),
# keep the legacy llm_fn, keep fail-closed, and leave the strategy names AND
# the none/fixed/signal/iterative behaviour untouched.
# ===========================================================================
def test_tr_factory_wiring_query_rewrite_provider_matches_llm_fn():
    """``get_rewriter("hyde", provider=fake)`` derives the bridge and yields
    the SAME RewriteResult as wiring the equivalent llm_fn directly. Strategy
    name stays "hyde" — only the request purpose differs."""
    # via provider
    provider = FakeLLMProvider(hyde_text="hypothetical passage")
    via_provider = get_rewriter(
        "hyde", provider=provider, board_id="b1"
    ).rewrite("q wiring A")
    assert via_provider.strategy == "hyde"
    assert via_provider.hyde_passage == "hypothetical passage"
    # the derived request still carries the canonical purpose
    assert provider.calls[-1].purpose == "query_hyde"

    # via explicit llm_fn (legacy) — identical result
    reset_rewriter_cache()
    via_llm_fn = get_rewriter(
        "hyde", llm_fn=lambda q: "hypothetical passage"
    ).rewrite("q wiring A")
    assert via_provider == via_llm_fn

    # decompose + fusion via provider also work end-to-end
    reset_rewriter_cache()
    reset_qr_bridge_cache()
    p_dec = FakeLLMProvider(decompose=["sub1", "sub2", "sub3"])
    res_dec = get_rewriter("decompose", provider=p_dec).rewrite("q wiring D")
    assert res_dec.strategy == "decompose"
    assert res_dec.rewritten_queries == ("sub1", "sub2", "sub3")

    reset_rewriter_cache()
    reset_qr_bridge_cache()
    p_fus = FakeLLMProvider(fusion=["p1", "p2", "p3", "p4"])
    res_fus = get_rewriter(
        "fusion", provider=p_fus, fusion_paraphrases=2
    ).rewrite("q wiring F")
    assert res_fus.strategy == "fusion"
    assert res_fus.rewritten_queries == ("p1", "p2")


def test_tr_factory_wiring_query_rewrite_llm_fn_takes_precedence():
    """If BOTH llm_fn and provider are given, the legacy llm_fn wins and the
    provider is never consulted."""
    provider = FakeLLMProvider(hyde_text="FROM PROVIDER")
    res = get_rewriter(
        "hyde", llm_fn=lambda q: "FROM LLM_FN", provider=provider
    ).rewrite("q precedence")
    assert res.hyde_passage == "FROM LLM_FN"
    assert provider.calls == []  # provider not used


def test_tr_factory_wiring_query_rewrite_fail_closed_without_either():
    """No llm_fn AND no provider -> still ValueError (no silent noop). The
    message now mentions both wiring options."""
    for strat in ("hyde", "decompose", "fusion"):
        with pytest.raises(ValueError, match="llm_fn.*provider|provider"):
            get_rewriter(strat)


def test_tr_factory_wiring_query_rewrite_none_and_unknown_ignore_provider():
    """none/unknown strategies are untouched and ignore the provider — they
    never call it, returning the Noop passthrough."""
    provider = FakeLLMProvider(hyde_text="x")
    assert get_rewriter("none", provider=provider).name == "none"
    assert get_rewriter("wat_is_this", provider=provider).name == "none"
    assert provider.calls == []


def test_tr_factory_wiring_query_rewrite_provider_cache_identity():
    """Provider-derived wiring preserves the factory's id(llm_fn) cache:
    same provider+context -> same rewriter; different provider -> different."""
    provider = FakeLLMProvider(hyde_text="p")
    r1 = get_rewriter("hyde", provider=provider, board_id="b1")
    r2 = get_rewriter("hyde", provider=provider, board_id="b1")
    assert r1 is r2  # bridge memoized -> stable id -> factory cache hit

    other = FakeLLMProvider(hyde_text="p2")
    r3 = get_rewriter("hyde", provider=other, board_id="b1")
    assert r3 is not r1


def test_tr_factory_wiring_adaptive_hops_provider_matches_llm_fn():
    """``get_hop_planner("llm", provider=fake)`` derives the bridge and
    decides hops; strategy stays "llm"."""
    provider = FakeLLMProvider(hops=3)
    planner = get_hop_planner("llm", provider=provider, board_id="b1")
    assert planner.name == "llm"
    decision = planner.plan(
        query="q hop wiring", intent_name="contradiction", seed_titles=[]
    )
    assert decision.reason == "llm"
    assert decision.hops == 3
    assert provider.calls[-1].purpose == "adaptive_hops"


def test_tr_factory_wiring_adaptive_hops_llm_fn_precedence_and_fail_closed():
    # legacy llm_fn wins over provider
    provider = FakeLLMProvider(hops=1)
    planner = get_hop_planner(
        "llm", llm_fn=lambda q, i, s: 2, provider=provider
    )
    d = planner.plan(query="q prec", intent_name="contradiction", seed_titles=[])
    assert d.hops == 2
    assert provider.calls == []

    # neither -> fail-closed ValueError (no silent fixed)
    with pytest.raises(ValueError, match="llm_fn.*provider|provider"):
        get_hop_planner("llm")


def test_tr_factory_wiring_adaptive_hops_other_strategies_ignore_provider():
    """fixed/signal/iterative are untouched and never consult the provider."""
    provider = FakeLLMProvider(hops=2)
    assert get_hop_planner("fixed", provider=provider).name == "fixed"
    assert get_hop_planner("signal", provider=provider).name == "signal"
    assert get_hop_planner("iterative", provider=provider).name == "iterative"
    assert get_hop_planner("wat", provider=provider).name == "fixed"
    assert provider.calls == []
