"""S06 runtime-provider ownership and SaaS-fake conformance coverage."""

from __future__ import annotations

import ast
from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys

import pytest

from okto_pulse.core.kg.interfaces import registry as registry_module
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.providers.testing.fake_saas import FakeSaaSRuntime


_RETIRED_MODULES = {
    "okto_pulse.core.kg.providers.embedded.memory_cache",
    "okto_pulse.core.kg.providers.embedded.memory_rate_limiter",
    "okto_pulse.core.kg.providers.embedded.memory_session_store",
    "okto_pulse.core.kg.providers.embedded.settings_config",
}


def _saas_registry(
    runtime: FakeSaaSRuntime,
    *,
    include_config: bool = True,
) -> KGProviderRegistry:
    providers: dict[str, object] = {
        "config": runtime.config if include_config else None,
        "cache_backend": runtime.cache_backend,
        "rate_limiter": runtime.rate_limiter,
        "session_store": runtime.session_store,
        "embedding_provider": object(),
        "event_bus": object(),
        "audit_repo": object(),
        "graph_store": object(),
        "cypher_executor": object(),
        "graph_transaction": object(),
        "graph_schema_manager": object(),
        "graph_lifecycle": object(),
        "graph_runtime_store": object(),
        "global_discovery_runtime": object(),
        "board_source_reader": object(),
    }
    return KGProviderRegistry(**providers)


def _restore_registry(saved: object | None) -> None:
    registry_module.restore_registry_state_for_tests(saved)


def test_productive_core_has_no_retired_embedded_provider_import_or_file() -> None:
    root = Path(__file__).resolve().parents[1]
    core_root = root / "src/okto_pulse/core"
    retired_files = {
        core_root / "kg/providers/embedded/memory_cache.py",
        core_root / "kg/providers/embedded/memory_rate_limiter.py",
        core_root / "kg/providers/embedded/memory_session_store.py",
        core_root / "kg/providers/embedded/settings_config.py",
    }
    assert all(not path.exists() for path in retired_files)

    offenders: list[str] = []
    for source_file in core_root.rglob("*.py"):
        tree = ast.parse(source_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module in _RETIRED_MODULES:
                    offenders.append(str(source_file.relative_to(core_root)))
            elif isinstance(node, ast.Import):
                if any(alias.name in _RETIRED_MODULES for alias in node.names):
                    offenders.append(str(source_file.relative_to(core_root)))
    assert offenders == []


def test_test_builder_uses_explicit_testing_fakes_without_community() -> None:
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    script = """
import sys
from okto_pulse.core.kg.providers.testing.registry import build_testing_kg_registry
registry = build_testing_kg_registry()
assert registry.cache_backend is not None
assert registry.rate_limiter is not None
assert registry.session_store is not None
assert registry.config is not None
assert not any(name == 'okto_pulse.community' or name.startswith('okto_pulse.community.') for name in sys.modules)
assert not any(name.startswith('okto_pulse.core.kg.providers.embedded.memory') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_fake_saas_registry_satisfies_the_same_core_port_seam() -> None:
    saved = registry_module.capture_registry_state_for_tests()
    try:
        reset_registry_for_tests()
        runtime = FakeSaaSRuntime()
        fake = _saas_registry(runtime)
        configure_kg_registry(base_registry=fake)

        registry = get_kg_registry()
        assert registry.config is runtime.config
        assert registry.require_cache_backend() is runtime.cache_backend
        assert registry.require_rate_limiter() is runtime.rate_limiter
        assert registry.require_session_store() is runtime.session_store
    finally:
        _restore_registry(saved)


def test_missing_required_saas_provider_fails_during_composition() -> None:
    saved = registry_module.capture_registry_state_for_tests()
    try:
        reset_registry_for_tests()
        with pytest.raises(RuntimeError, match="config"):
            configure_kg_registry(
                base_registry=_saas_registry(FakeSaaSRuntime(), include_config=False)
            )

        reset_registry_for_tests()
        configure_kg_registry(base_registry=_saas_registry(FakeSaaSRuntime()))
        registry = get_kg_registry()
        registry.cache_backend = None
        with pytest.raises(RuntimeProviderMissing, match="cache_backend"):
            registry.require_cache_backend()
    finally:
        _restore_registry(saved)


@pytest.mark.asyncio
async def test_fake_saas_runtime_preserves_tenant_isolation_and_expiry() -> None:
    first = FakeSaaSRuntime()
    second = FakeSaaSRuntime()

    first.cache_backend.put("query", "board-01", {"q": "x"}, {"value": 1})
    assert first.cache_backend.get("query", "board-01", {"q": "x"}) == (
        True,
        {"value": 1},
    )
    assert second.cache_backend.get("query", "board-01", {"q": "x"}) == (False, None)

    session = await first.session_store.create(
        session_id="session-01",
        board_id="board-01",
        artifact_id="artifact-01",
        artifact_type="spec",
        agent_id="service-account",
        raw_content="payload",
    )
    assert await second.session_store.get(session.session_id) is None
    session.expires_at = session.started_at - timedelta(seconds=1)
    assert await first.session_store.get(session.session_id) is None
    assert first.config.kg_session_ttl_seconds == first.session_store.default_ttl_seconds
