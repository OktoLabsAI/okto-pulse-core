"""R14 - embedding stub ownership and read-time fail-closed gates."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import okto_pulse.core.kg.embedding as runtime_embedding
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)
from testing_kg_registry import build_testing_kg_registry
from okto_pulse.core.kg.providers.testing.embedding import (
    TestingStubEmbeddingProvider,
)


class _Sentinel:
    pass


class _Bus:
    async def publish(self, event):
        return None

    async def subscribe(self, event_type, handler):
        return None

    async def start(self):
        return None

    async def stop(self):
        return None


class _Audit:
    pass


def _required_composed_registry_without_read_time_slots() -> KGProviderRegistry:
    sentinel = _Sentinel()
    return KGProviderRegistry(
        config=sentinel,
        event_bus=_Bus(),
        audit_repo=_Audit(),
        graph_store=sentinel,
        cypher_executor=sentinel,
        graph_transaction=sentinel,
        graph_schema_manager=sentinel,
        graph_lifecycle=sentinel,
        graph_runtime_store=sentinel,
        global_discovery_runtime=sentinel,
        board_source_reader=sentinel,
    )


def test_core_runtime_embedding_module_exports_port_only() -> None:
    assert "StubEmbeddingProvider" not in getattr(runtime_embedding, "__all__", ())
    assert not hasattr(runtime_embedding, "StubEmbeddingProvider")

    source = Path(runtime_embedding.__file__).read_text(encoding="utf-8")
    assert "class StubEmbeddingProvider" not in source
    assert "_build_provider_from_config" not in source
    assert "TestingStubEmbeddingProvider" not in source

    with pytest.raises(ImportError):
        exec(
            "from okto_pulse.core.kg.embedding import StubEmbeddingProvider",
            {},
        )


def test_build_defaults_uses_sanctioned_testing_provider_namespace() -> None:
    reg = build_testing_kg_registry()
    provider = reg.embedding_provider

    assert isinstance(provider, TestingStubEmbeddingProvider)
    assert provider.encode("same") == provider.encode("same")
    assert len(provider.encode("dimension")) == 384


def test_missing_embedding_provider_remains_read_time_fail_closed() -> None:
    reset_registry_for_tests()
    try:
        configure_kg_registry(
            base_registry=_required_composed_registry_without_read_time_slots()
        )
        reg = get_kg_registry()

        assert reg.embedding_provider is None
        assert reg.cache_backend is None
        assert reg.rate_limiter is None
        assert reg.session_store is None
        assert reg.auth_context_factory is None

        for require_name, provider_key in (
            ("require_embedding_provider", "embedding_provider"),
            ("require_cache_backend", "cache_backend"),
            ("require_rate_limiter", "rate_limiter"),
            ("require_session_store", "session_store"),
        ):
            with pytest.raises(RuntimeProviderMissing) as exc:
                getattr(reg, require_name)()
            assert exc.value.code == "runtime_provider_missing"
            assert exc.value.provider_key == provider_key

        with pytest.raises(RuntimeProviderMissing) as exc:
            runtime_embedding.get_embedding_provider()
        assert exc.value.provider_key == "embedding_provider"
    finally:
        reset_registry_for_tests()


def test_registry_configure_missing_list_does_not_include_read_time_slots() -> None:
    source = Path(
        "src/okto_pulse/core/kg/interfaces/registry.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    configure_fn = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "configure_kg_registry"
    )
    names_in_configure_time_missing = {
        constant.value
        for node in ast.walk(configure_fn)
        if isinstance(node, ast.Tuple)
        for constant in node.elts
        if isinstance(constant, ast.Constant) and isinstance(constant.value, str)
    }

    assert {
        "embedding_provider",
        "cache_backend",
        "rate_limiter",
        "session_store",
        "auth_context_factory",
    }.isdisjoint(names_in_configure_time_missing)
