"""R13-C — Bridges LLMProvider -> rerank / context-compress / retrieve-critic.

Covers the 4 test scenarios of the card 1:1 with a real in-memory
``FakeLLMProvider`` (no vendor SDK, no sentence_transformers):

  ts_8e99bf7c (rerank)      -> test_ts_8e99bf7c_*
  ts_f089a2f5 (compression) -> test_ts_f089a2f5_*
  ts_6205daf6 (critic)      -> test_ts_6205daf6_*
  ts_c9ff52b2 (negative/ML) -> test_ts_c9ff52b2_*

Plus a contract guard for the 3 canonical purposes (rerank_llm /
context_compress / retrieve_critic) and the rerank factory-via-provider wiring
(tr_r13c_rerank_bridge). The bridges adapt the LLM *contract* to the legacy
callables and preserve each flow's behaviour bit-for-bit.
"""

from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass

import pytest

from okto_pulse.core.kg.context_compress import compress_if_needed
from okto_pulse.core.kg.context_compress.llm_provider_bridges import (
    make_compress_llm_fn,
)
from okto_pulse.core.kg.context_compress.llm_provider_bridges import (
    reset_bridge_cache as reset_compress_bridge_cache,
)
from okto_pulse.core.kg.interfaces.llm import (
    LLM_PROVIDER_ABSENT,
    LLM_PROVIDER_ERROR,
    LLM_TIMEOUT,
    LLMProviderError,
    LLMResponse,
)
from okto_pulse.core.kg.rerank import get_reranker, reset_reranker_cache
from okto_pulse.core.kg.rerank.llm import LLMReranker
from okto_pulse.core.kg.rerank.llm_provider_bridges import make_llm_ranker_fn
from okto_pulse.core.kg.rerank.llm_provider_bridges import (
    reset_bridge_cache as reset_rerank_bridge_cache,
)
from okto_pulse.core.kg.retrieve_critic import (
    Adequacy,
    CriticAction,
    critic_evaluate,
    reflect,
    reset_critic_cache,
)
from okto_pulse.core.kg.retrieve_critic.llm_provider_bridges import (
    make_critic_fn,
)
from okto_pulse.core.kg.retrieve_critic.llm_provider_bridges import (
    reset_bridge_cache as reset_critic_bridge_cache,
)


# ===========================================================================
# In-memory fake provider (satisfies the R13-A LLMProvider Protocol by duck
# typing). Routed by request.purpose — one fake serves every flow.
# ===========================================================================
class FakeLLMProvider:
    def __init__(
        self,
        *,
        name: str = "fake",
        rerank_ids=None,
        rerank_text: str | None = None,
        compress_summary: str | None = None,
        critic=None,
        critic_text: str | None = None,
        fail: str | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.name = name
        self._rerank_ids = rerank_ids
        self._rerank_text = rerank_text
        self._compress_summary = compress_summary
        self._critic = critic
        self._critic_text = critic_text
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
        if purpose == "rerank_llm":
            if self._rerank_ids is not None:
                return LLMResponse(json=list(self._rerank_ids))
            return LLMResponse(text=self._rerank_text)
        if purpose == "context_compress":
            return LLMResponse(text=self._compress_summary)
        if purpose == "retrieve_critic":
            if self._critic is not None:
                return LLMResponse(json=dict(self._critic))
            return LLMResponse(text=self._critic_text)
        return LLMResponse()

    def capabilities(self) -> dict:
        return {"is_stub": True, "name": self.name}


@dataclass(frozen=True)
class _C:
    """Minimal candidate shape — mirrors the rerank suite's _FakeCandidate."""

    node_id: str
    title: str
    content: str | None = None
    score: float = 0.0


class _RetrievalSpy:
    def __init__(self, rows_per_call=None):
        self._rows = rows_per_call or [[{"node_id": "n1"}]]
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(dict(kwargs))
        idx = min(len(self.calls) - 1, len(self._rows) - 1)
        return list(self._rows[idx])


@pytest.fixture(autouse=True)
def _reset_all_caches():
    reset_reranker_cache()
    reset_critic_cache()
    reset_rerank_bridge_cache()
    reset_compress_bridge_cache()
    reset_critic_bridge_cache()
    yield
    reset_reranker_cache()
    reset_critic_cache()
    reset_rerank_bridge_cache()
    reset_compress_bridge_cache()
    reset_critic_bridge_cache()


# ===========================================================================
# ts_8e99bf7c (rerank)
# ===========================================================================
def test_ts_8e99bf7c_honours_ordering_and_fills_omitted():
    provider = FakeLLMProvider(rerank_ids=["c2", "c0"])
    rank_fn = make_llm_ranker_fn(provider, board_id="b1")
    rr = LLMReranker(rank_fn)
    items = [_C("c0", "alpha"), _C("c1", "beta"), _C("c2", "gamma")]
    out = rr.rerank("q", items, top_n=3)
    # LLM order first, then omitted id filled in input order.
    assert [c.node_id for c in out] == ["c2", "c0", "c1"]
    # canonical request shape
    assert provider.calls[-1].purpose == "rerank_llm"
    assert provider.calls[-1].telemetry_labels == {"flow": "rerank_llm"}


def test_ts_8e99bf7c_ignores_unknown_ids():
    provider = FakeLLMProvider(rerank_ids=["does_not_exist", "c1"])
    rr = LLMReranker(make_llm_ranker_fn(provider))
    items = [_C("c0", "alpha"), _C("c1", "beta")]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c1", "c0"]


def test_ts_8e99bf7c_top_n_truncates():
    provider = FakeLLMProvider(rerank_ids=["c2", "c1", "c0"])
    rr = LLMReranker(make_llm_ranker_fn(provider))
    items = [_C(f"c{i}", f"t{i}") for i in range(3)]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c2", "c1"]


def test_ts_8e99bf7c_fallback_to_input_order_on_failure():
    # Normalized provider failure -> bridge raises -> reranker input order.
    provider = FakeLLMProvider(fail=LLM_TIMEOUT)
    rank_fn = make_llm_ranker_fn(provider)
    with pytest.raises(LLMProviderError):
        rank_fn("q", [_C("c0", "a")])
    rr = LLMReranker(rank_fn)
    items = [_C("c0", "a"), _C("c1", "b")]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c0", "c1"]

    # Raw provider exception -> propagates -> reranker still input order.
    provider2 = FakeLLMProvider(raises=RuntimeError("quota"))
    rr2 = LLMReranker(make_llm_ranker_fn(provider2))
    out2 = rr2.rerank("q", items, top_n=2)
    assert [c.node_id for c in out2] == ["c0", "c1"]


def test_ts_8e99bf7c_empty_ranking_falls_back_to_input_order():
    provider = FakeLLMProvider(rerank_ids=[])  # OK but no ids
    rr = LLMReranker(make_llm_ranker_fn(provider))
    items = [_C("c0", "a"), _C("c1", "b")]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c0", "c1"]


def test_ts_8e99bf7c_llm_reranker_not_cached_but_bridge_fn_stable():
    """LLMReranker is never cached (provider-bound), but the derived
    ranker_fn is memoized so its identity is stable."""
    provider = FakeLLMProvider(rerank_ids=[])
    rr1 = get_reranker("llm", provider=provider, board_id="b1")
    rr2 = get_reranker("llm", provider=provider, board_id="b1")
    assert rr1 is not rr2  # NOT cached — preserved behaviour

    fn1 = make_llm_ranker_fn(provider, board_id="b1")
    fn2 = make_llm_ranker_fn(provider, board_id="b1")
    assert fn1 is fn2  # memoized -> stable id
    other = FakeLLMProvider(rerank_ids=[])
    assert make_llm_ranker_fn(other, board_id="b1") is not fn1


# ===========================================================================
# ts_f089a2f5 (compression)
# ===========================================================================
def _big_nodes():
    # ~100 approx tokens (len // 4) so any small max_tokens trips threshold.
    return [{"title": "T", "content": "x" * 400}]


def test_ts_f089a2f5_valid_above_threshold_applies():
    provider = FakeLLMProvider(compress_summary="a concise summary")
    fn = make_compress_llm_fn(provider, board_id="b1")
    nodes = _big_nodes()
    res = compress_if_needed(nodes, compress_llm_fn=fn, max_tokens=1)
    assert res.applied is True
    assert res.summary == "a concise summary"
    assert res.compressed_from_nodes == 1
    # rows preserved (input list untouched)
    assert nodes == _big_nodes()
    assert provider.calls[-1].purpose == "context_compress"


def test_ts_f089a2f5_below_threshold_does_not_invoke_provider():
    provider = FakeLLMProvider(compress_summary="s")
    fn = make_compress_llm_fn(provider)
    nodes = [{"title": "t", "content": "small"}]
    res = compress_if_needed(nodes, compress_llm_fn=fn, max_tokens=10_000)
    assert res.applied is False
    assert res.summary is None
    assert provider.calls == []  # never invoked below threshold
    assert nodes == [{"title": "t", "content": "small"}]


def test_ts_f089a2f5_provider_failure_keeps_applied_false():
    provider = FakeLLMProvider(fail=LLM_PROVIDER_ERROR)
    fn = make_compress_llm_fn(provider)
    # the callable returns "" on a normalized failure
    assert fn("a very long text " * 50) == ""
    nodes = _big_nodes()
    res = compress_if_needed(nodes, compress_llm_fn=fn, max_tokens=1)
    assert res.applied is False
    assert res.summary is None
    assert nodes == _big_nodes()


def test_ts_f089a2f5_empty_summary_keeps_applied_false():
    provider = FakeLLMProvider(compress_summary="   ")
    fn = make_compress_llm_fn(provider)
    assert fn("text") == ""
    res = compress_if_needed(_big_nodes(), compress_llm_fn=fn, max_tokens=1)
    assert res.applied is False
    assert res.summary is None


def test_ts_f089a2f5_raw_exception_propagates_then_compress_keeps_false():
    provider = FakeLLMProvider(raises=RuntimeError("boom"))
    fn = make_compress_llm_fn(provider)
    with pytest.raises(RuntimeError):
        fn("text")
    nodes = _big_nodes()
    # compress_if_needed catches the propagating exception -> applied=False.
    res = compress_if_needed(nodes, compress_llm_fn=fn, max_tokens=1)
    assert res.applied is False
    assert nodes == _big_nodes()


# ===========================================================================
# ts_6205daf6 (critic)
# ===========================================================================
def test_ts_6205daf6_maps_adequacy_and_action():
    provider = FakeLLMProvider(
        critic={
            "adequacy": "sufficient",
            "suggested_action": "accept",
            "reason": "ok",
        }
    )
    fn = make_critic_fn(provider, board_id="b1")
    d = critic_evaluate("q", [{"node_id": "n1", "similarity": 0.9}], fn)
    assert d.adequacy == Adequacy.SUFFICIENT
    assert d.suggested_action == CriticAction.ACCEPT
    assert d.reason == "ok"
    assert provider.calls[-1].purpose == "retrieve_critic"
    assert provider.calls[-1].telemetry_labels == {"flow": "retrieve_critic"}


def test_ts_6205daf6_unknown_enum_falls_back_partial_accept():
    provider = FakeLLMProvider(
        critic={"adequacy": "bogus", "suggested_action": "nope", "reason": "x"}
    )
    fn = make_critic_fn(provider)
    d = critic_evaluate("q-unknown", [], fn)
    assert d.adequacy == Adequacy.PARTIAL
    assert d.suggested_action == CriticAction.ACCEPT
    assert "bogus" in d.reason


def test_ts_6205daf6_non_dict_text_falls_back_partial_accept():
    """A free-text response (not JSON) is wrapped under reason; the missing
    enum keys flow through the orchestrator's PARTIAL/ACCEPT fallback."""
    provider = FakeLLMProvider(critic_text="the rows look fine to me")
    fn = make_critic_fn(provider)
    d = critic_evaluate("q-text", [], fn)
    assert d.adequacy == Adequacy.PARTIAL
    assert d.suggested_action == CriticAction.ACCEPT


def test_ts_6205daf6_provider_failure_yields_critic_error():
    provider = FakeLLMProvider(fail=LLM_PROVIDER_ERROR)
    critic = make_critic_fn(provider)
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": "n1"}]])
    result = reflect("q", retrieval_fn=spy, critic_fn=critic)
    assert result.stopped_reason == "critic_error"
    assert result.final_rows == ({"node_id": "n1"},)  # last rows preserved
    assert len(spy.calls) == 1  # critic failure stops further retries


def test_ts_6205daf6_raw_exception_yields_critic_error():
    provider = FakeLLMProvider(raises=RuntimeError("LLM down"))
    critic = make_critic_fn(provider)
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": "n7"}]])
    result = reflect("q", retrieval_fn=spy, critic_fn=critic)
    assert result.stopped_reason == "critic_error"
    assert result.final_rows == ({"node_id": "n7"},)


def test_ts_6205daf6_retrieval_fn_exception_propagates():
    """The orchestrator does NOT catch retrieval_fn exceptions — they must
    surface even though the critic bridge is healthy."""
    provider = FakeLLMProvider(
        critic={"adequacy": "sufficient", "suggested_action": "accept"}
    )
    critic = make_critic_fn(provider)

    def exploding_retrieval(**kwargs):
        raise RuntimeError("retrieval wiring bug")

    with pytest.raises(RuntimeError, match="retrieval wiring bug"):
        reflect("q", retrieval_fn=exploding_retrieval, critic_fn=critic)


def test_ts_6205daf6_critic_bridge_memoized_preserves_id_cache():
    provider = FakeLLMProvider(
        critic={"adequacy": "sufficient", "suggested_action": "accept"}
    )
    fn1 = make_critic_fn(provider, board_id="b1")
    fn2 = make_critic_fn(provider, board_id="b1")
    assert fn1 is fn2  # memoized -> stable identity
    rows = [{"node_id": "n1", "similarity": 0.9}]
    critic_evaluate("same q", rows, fn1)
    critic_evaluate("same q", rows, fn2)
    # critic_evaluate caches by id(critic_fn): one provider call for both.
    assert len(provider.calls) == 1
    other = FakeLLMProvider(
        critic={"adequacy": "sufficient", "suggested_action": "accept"}
    )
    assert make_critic_fn(other, board_id="b1") is not fn1


# ===========================================================================
# ts_c9ff52b2 (negative) — no eager ML import; cross_encoder lazy->token_overlap
# ===========================================================================
_FORBIDDEN_IMPORT_TOKENS = ("sentence_transformers", "torch")
_FORBIDDEN_NAME_TOKENS = ("CrossEncoder", "SentenceTransformer")


def _imported_module_names(module) -> set[str]:
    tree = ast.parse(inspect.getsource(module))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
    return names


def test_ts_c9ff52b2_bridges_have_no_eager_ml_import():
    import okto_pulse.core.kg.context_compress.llm_provider_bridges as cb
    import okto_pulse.core.kg.rerank.llm_provider_bridges as rb
    import okto_pulse.core.kg.retrieve_critic.llm_provider_bridges as crb

    for mod in (rb, cb, crb):
        imported = _imported_module_names(mod)
        for name in imported:
            low = name.lower()
            for token in _FORBIDDEN_IMPORT_TOKENS:
                assert token not in low, (
                    f"{mod.__name__} imports forbidden module {name!r}"
                )
        src = inspect.getsource(mod)
        for token in _FORBIDDEN_NAME_TOKENS:
            assert token not in src, (
                f"{mod.__name__} references concrete ML class {token!r}"
            )


def test_ts_c9ff52b2_cross_encoder_lazy_fallback_to_token_overlap(monkeypatch):
    """With sentence_transformers unavailable, the cross_encoder strategy
    still falls back to token_overlap (lazy import gate preserved)."""
    import sys

    for mod in list(sys.modules):
        if mod.startswith("sentence_transformers"):
            monkeypatch.delitem(sys.modules, mod, raising=False)

    class _Blocker:
        def __getattr__(self, name):
            raise ImportError("sentence_transformers blocked by fixture")

    monkeypatch.setitem(sys.modules, "sentence_transformers", _Blocker())

    reset_reranker_cache()
    rr = get_reranker("cross_encoder", cross_encoder_model="blocked-model")
    assert rr.name == "token_overlap"


# ===========================================================================
# Contract: canonical purposes (api_r13c_cognitive_bridge_contract). Regression
# guard — fails if a bridge drifts off the canonical purpose names.
# ===========================================================================
def test_contract_canonical_purposes_for_three_flows():
    p_re = FakeLLMProvider(rerank_ids=["c0"])
    make_llm_ranker_fn(p_re, board_id="b1", actor_id="a1")("q", [_C("c0", "t")])
    req = p_re.calls[-1]
    assert req.purpose == "rerank_llm"
    assert req.telemetry_labels == {"flow": "rerank_llm"}
    assert req.board_id == "b1" and req.actor_id == "a1"

    p_co = FakeLLMProvider(compress_summary="s")
    make_compress_llm_fn(p_co, board_id="b2")("some text")
    req = p_co.calls[-1]
    assert req.purpose == "context_compress"
    assert req.telemetry_labels == {"flow": "context_compress"}
    assert req.board_id == "b2"

    p_cr = FakeLLMProvider(
        critic={"adequacy": "partial", "suggested_action": "accept"}
    )
    make_critic_fn(p_cr, actor_id="a9")("q", [{"node_id": "n1"}])
    req = p_cr.calls[-1]
    assert req.purpose == "retrieve_critic"
    assert req.telemetry_labels == {"flow": "retrieve_critic"}
    assert req.actor_id == "a9"


# ===========================================================================
# tr_r13c_rerank_bridge — factory accepts provider, keeps legacy fn precedence,
# stays fail-closed, leaves none/token_overlap/cross_encoder untouched.
# ===========================================================================
def test_tr_rerank_factory_provider_wiring_matches_llm_fn():
    provider = FakeLLMProvider(rerank_ids=["c2", "c0"])
    rr = get_reranker("llm", provider=provider, board_id="b1")
    assert rr.name == "llm"
    items = [_C("c0", "alpha"), _C("c1", "beta"), _C("c2", "gamma")]
    out = rr.rerank("q", items, top_n=3)
    assert [c.node_id for c in out] == ["c2", "c0", "c1"]
    assert provider.calls[-1].purpose == "rerank_llm"

    # equivalent legacy llm_ranker_fn yields the same ordering
    reset_reranker_cache()
    rr_legacy = get_reranker("llm", llm_ranker_fn=lambda q, c: ["c2", "c0"])
    out_legacy = rr_legacy.rerank("q", items, top_n=3)
    assert [c.node_id for c in out] == [c.node_id for c in out_legacy]


def test_tr_rerank_factory_llm_fn_takes_precedence():
    provider = FakeLLMProvider(rerank_ids=["c2"])  # would put c2 first
    rr = get_reranker(
        "llm", llm_ranker_fn=lambda q, c: ["c1"], provider=provider
    )
    items = [_C("c0", "a"), _C("c1", "b"), _C("c2", "c")]
    out = rr.rerank("q", items, top_n=3)
    assert out[0].node_id == "c1"  # legacy fn wins
    assert provider.calls == []  # provider never consulted


def test_tr_rerank_factory_fail_closed_without_either():
    with pytest.raises(ValueError, match="llm_ranker_fn"):
        get_reranker("llm")


def test_tr_rerank_factory_other_strategies_ignore_provider():
    provider = FakeLLMProvider(rerank_ids=["c0"])
    assert get_reranker("none", provider=provider).name == "noop"
    assert get_reranker("token_overlap", provider=provider).name == "token_overlap"
    assert get_reranker("wat_is_this", provider=provider).name == "noop"
    assert provider.calls == []


def test_tr_rerank_factory_absent_provider_failure_propagates_as_value_error():
    """A None provider is fail-closed at the bridge wiring layer too."""
    with pytest.raises(ValueError):
        make_llm_ranker_fn(None)
    with pytest.raises(ValueError):
        make_compress_llm_fn(None)
    with pytest.raises(ValueError):
        make_critic_fn(None)


def test_provider_absent_status_surfaces_as_failure_in_rerank():
    """A provider that normalizes provider_absent still degrades gracefully:
    the reranker falls back to input order."""
    provider = FakeLLMProvider(fail=LLM_PROVIDER_ABSENT)
    rr = LLMReranker(make_llm_ranker_fn(provider))
    items = [_C("c0", "a"), _C("c1", "b")]
    out = rr.rerank("q", items, top_n=2)
    assert [c.node_id for c in out] == ["c0", "c1"]
