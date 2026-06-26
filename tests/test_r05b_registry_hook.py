"""R05-B (CORE target) — registry composition hook + reranker registration.

Scenarios here (core-target):

  ts_66c96a7e — configure_kg_registry(base_registry=...) does NOT instantiate the
                core embedded Onda A (cache/rate_limiter/session_store/embedding)
                BUT graph_store/audit_repo/event_bus are still present/wired; and
                the legacy path (no base) is unchanged (TR3 retro-compat).
  (supporting) — the rerank factory cross_encoder registration hook + the
                token_overlap fallback (feeds the community ts_34ab1390).
"""

from __future__ import annotations

import okto_pulse.core.kg.embedding as _emb_mod
import okto_pulse.core.kg.providers.embedded.memory_cache as _cache_mod
import okto_pulse.core.kg.providers.embedded.memory_rate_limiter as _rl_mod
import okto_pulse.core.kg.providers.embedded.memory_session_store as _ss_mod
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)
from okto_pulse.core.kg.rerank.factory import (
    get_reranker,
    register_cross_encoder_factory,
    reset_cross_encoder_factory,
    reset_reranker_cache,
)


class _Sentinel:
    """A unique opaque object standing in for a community-supplied provider."""


def _instrument_onda_a(monkeypatch):
    """Count instantiations of the core embedded Onda A providers."""
    counts = {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}

    orig_cache = _cache_mod.InMemoryCacheBackend
    orig_rl = _rl_mod.InMemoryTokenBucket
    orig_ss = _ss_mod.InMemorySessionStore
    orig_emb = _emb_mod._build_provider_from_config

    class _C(orig_cache):
        def __init__(self, *a, **k):
            counts["cache"] += 1
            super().__init__(*a, **k)

    class _R(orig_rl):
        def __init__(self, *a, **k):
            counts["rate_limiter"] += 1
            super().__init__(*a, **k)

    class _S(orig_ss):
        def __init__(self, *a, **k):
            counts["session_store"] += 1
            super().__init__(*a, **k)

    def _emb(config):
        counts["embedding"] += 1
        return orig_emb(config)

    monkeypatch.setattr(_cache_mod, "InMemoryCacheBackend", _C)
    monkeypatch.setattr(_rl_mod, "InMemoryTokenBucket", _R)
    monkeypatch.setattr(_ss_mod, "InMemorySessionStore", _S)
    monkeypatch.setattr(_emb_mod, "_build_provider_from_config", _emb)
    return counts


# ===========================================================================
# ts_66c96a7e — base_registry composition.
# ===========================================================================
def test_ts_66c96a7e_base_registry_skips_onda_a_but_keeps_graph_audit_eventbus(
    monkeypatch,
):
    counts = _instrument_onda_a(monkeypatch)
    reset_registry_for_tests()
    try:
        base = KGProviderRegistry(
            cache_backend=_Sentinel(),
            rate_limiter=_Sentinel(),
            session_store=_Sentinel(),
            embedding_provider=_Sentinel(),
        )
        configure_kg_registry(session_factory=object(), base_registry=base)
        reg = get_kg_registry()

        # Onda A slots are the community-supplied sentinels...
        assert reg.cache_backend is base.cache_backend
        assert reg.rate_limiter is base.rate_limiter
        assert reg.session_store is base.session_store
        assert reg.embedding_provider is base.embedding_provider
        # ...and the core embedded Onda A were NOT instantiated.
        assert counts == {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}

        # The core-owned NON-Onda-A slots ARE mounted by the core.
        assert reg.config is not None
        assert reg.graph_store is not None
        assert reg.cypher_executor is not None
        assert reg.graph_transaction is not None
        assert reg.graph_schema_manager is not None
        assert reg.graph_lifecycle is not None
        assert reg.graph_path_resolver is not None
        # audit_repo / event_bus auto-wired from the session_factory.
        assert reg.audit_repo is not None
        assert reg.event_bus is not None
    finally:
        reset_registry_for_tests()


def test_ts_66c96a7e_legacy_path_unchanged_retrocompat(monkeypatch):
    counts = _instrument_onda_a(monkeypatch)
    reset_registry_for_tests()
    try:
        # No base_registry/defaults_factory -> _build_defaults() exactly as before.
        configure_kg_registry(session_factory=object())
        reg = get_kg_registry()
        # The core embedded Onda A WERE instantiated (legacy behaviour) — the
        # instrumented subclasses prove it (counts == 1 each).
        assert counts["cache"] == 1
        assert counts["rate_limiter"] == 1
        assert counts["session_store"] == 1
        assert counts["embedding"] == 1
        # cache_backend is the (instrumented) core embedded — a real subclass.
        assert isinstance(reg.cache_backend, _cache_mod.InMemoryCacheBackend)
        assert reg.graph_store is not None
        assert reg.audit_repo is not None and reg.event_bus is not None
    finally:
        reset_registry_for_tests()


def test_ts_66c96a7e_defaults_factory_path_also_composes(monkeypatch):
    counts = _instrument_onda_a(monkeypatch)
    reset_registry_for_tests()
    try:
        def _factory():
            return KGProviderRegistry(
                cache_backend=_Sentinel(),
                rate_limiter=_Sentinel(),
                session_store=_Sentinel(),
                embedding_provider=_Sentinel(),
            )

        configure_kg_registry(session_factory=object(), defaults_factory=_factory)
        reg = get_kg_registry()
        assert counts == {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}
        assert reg.graph_store is not None
        assert reg.audit_repo is not None
    finally:
        reset_registry_for_tests()


# ===========================================================================
# Supporting: rerank cross_encoder registration hook + token_overlap fallback.
# ===========================================================================
def test_cross_encoder_registration_hook_and_fallback():
    reset_reranker_cache()
    reset_cross_encoder_factory()
    try:
        # A registered factory that raises ImportError (optional dep absent) ->
        # the core degrades to token_overlap (R13-C behaviour preserved).
        def _missing_dep_factory(model_name):
            raise ImportError("sentence-transformers absent")

        register_cross_encoder_factory(_missing_dep_factory)
        rr = get_reranker("cross_encoder")
        assert rr.name == "token_overlap"

        # A registered factory that succeeds -> its instance is used.
        reset_reranker_cache()

        class _FakeCE:
            name = "cross_encoder"

        register_cross_encoder_factory(lambda model_name: _FakeCE())
        rr2 = get_reranker("cross_encoder")
        assert isinstance(rr2, _FakeCE)

        # none/token_overlap/llm-floor unaffected.
        reset_reranker_cache()
        assert get_reranker("none").name == "noop"
        assert get_reranker("token_overlap").name == "token_overlap"
    finally:
        reset_cross_encoder_factory()
        reset_reranker_cache()
