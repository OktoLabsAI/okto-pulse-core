"""R05-B (CORE target) — registry composition hook + reranker registration.

Scenarios here (core-target):

  ts_66c96a7e — configure_kg_registry(base_registry=...) does NOT instantiate the
                core embedded Onda A (cache/rate_limiter/session_store/embedding)
                BUT graph_store is mounted while audit_repo/event_bus are supplied
                explicitly by composition; and
                the no-base path now FAILS CLOSED (R-P2-03 retired the TR3
                implicit Onda A escape).
  (supporting) — the rerank factory cross_encoder registration hook + the
                token_overlap fallback (feeds the community ts_34ab1390).
"""

from __future__ import annotations

import pytest

import okto_pulse.core.kg.providers.testing.memory as _cache_mod
import okto_pulse.core.kg.providers.testing.memory as _rl_mod
import okto_pulse.core.kg.providers.testing.memory as _ss_mod
import okto_pulse.core.kg.providers.testing.embedding as _emb_mod
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


class _SentinelBus:
    async def publish(self, event):
        return "evt_test"

    async def subscribe(self, event_type, handler):
        ...

    async def start(self):
        ...

    async def stop(self):
        ...


class _SentinelAudit:
    async def stage_consolidation_records(
        self, transaction_context, audit, node_refs, outbox_event,
    ):
        ...


def _instrument_onda_a(monkeypatch):
    """Count instantiations of the core embedded Onda A providers."""
    counts = {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}

    orig_cache = _cache_mod.InMemoryCacheBackend
    orig_rl = _rl_mod.InMemoryTokenBucket
    orig_ss = _ss_mod.InMemorySessionStore
    orig_emb = _emb_mod.build_testing_embedding_provider

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
    monkeypatch.setattr(_emb_mod, "build_testing_embedding_provider", _emb)
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
            config=_Sentinel(),  # R-P2-03D: config is composition-supplied (required)
            event_bus=_SentinelBus(),
            audit_repo=_SentinelAudit(),
            graph_store=_Sentinel(),
            cypher_executor=_Sentinel(),
            graph_transaction=_Sentinel(),
            graph_schema_manager=_Sentinel(),
            graph_lifecycle=_Sentinel(),
            graph_runtime_store=_Sentinel(),
            global_discovery_runtime=_Sentinel(),
            board_source_reader=_Sentinel(),
        )
        configure_kg_registry(base_registry=base)
        reg = get_kg_registry()

        # Onda A slots are the community-supplied sentinels...
        assert reg.cache_backend is base.cache_backend
        assert reg.rate_limiter is base.rate_limiter
        assert reg.session_store is base.session_store
        assert reg.embedding_provider is base.embedding_provider
        # ...and the core embedded Onda A were NOT instantiated.
        assert counts == {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}

        # R-P2-03D: config is the composition-supplied one, NOT a core-filled
        # implicit SettingsKGConfig (the embedded default was retired here).
        assert reg.config is base.config
        # R-P2-05: graph slots are composition-supplied, never mounted by core.
        assert reg.graph_store is base.graph_store
        assert reg.cypher_executor is base.cypher_executor
        assert reg.graph_transaction is base.graph_transaction
        assert reg.graph_schema_manager is base.graph_schema_manager
        assert reg.graph_lifecycle is base.graph_lifecycle
        assert reg.graph_runtime_store is base.graph_runtime_store
        # audit_repo / event_bus are composition-supplied, never auto-wired.
        assert reg.audit_repo is base.audit_repo
        assert reg.event_bus is base.event_bus
    finally:
        reset_registry_for_tests()


def test_ts_66c96a7e_no_base_path_fails_closed(monkeypatch):
    counts = _instrument_onda_a(monkeypatch)
    reset_registry_for_tests()
    try:
        # R-P2-03: the no-base / no-factory path is NO LONGER an escape to the
        # implicit Onda A defaults — it fails closed with an actionable error.
        with pytest.raises(RuntimeError):
            configure_kg_registry()
        # The raise happens BEFORE any build: the core embedded Onda A were NOT
        # instantiated (no implicit _build_defaults side-effect).
        assert counts == {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}
        # And the registry stays unconfigured — consuming it is fail-closed too.
        with pytest.raises(RuntimeError):
            get_kg_registry()
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
                config=_Sentinel(),  # R-P2-03D: config required from the composition
                event_bus=_SentinelBus(),
                audit_repo=_SentinelAudit(),
                graph_store=_Sentinel(),
                cypher_executor=_Sentinel(),
                graph_transaction=_Sentinel(),
                graph_schema_manager=_Sentinel(),
                graph_lifecycle=_Sentinel(),
                graph_runtime_store=_Sentinel(),
                global_discovery_runtime=_Sentinel(),
                board_source_reader=_Sentinel(),
            )

        configure_kg_registry(defaults_factory=_factory)
        reg = get_kg_registry()
        assert counts == {"cache": 0, "rate_limiter": 0, "session_store": 0, "embedding": 0}
        assert isinstance(reg.graph_store, _Sentinel)
        assert isinstance(reg.audit_repo, _SentinelAudit)
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
