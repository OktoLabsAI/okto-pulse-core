from __future__ import annotations

import ast
import importlib
from pathlib import Path

from okto_pulse.core.application.boundary.singleton_gate import (
    BASELINE_SINGLETONS,
    SINGLETON_LEDGER,
    AntiSingletonGate,
    AntiSingletonGateInput,
)
from okto_pulse.core.kg.adaptive_hops.llm_provider_bridges import (
    make_hop_llm_fn,
    reset_bridge_cache as reset_hop_bridge_cache,
)
from okto_pulse.core.kg.llm_provider_bridge_cache import (
    DEFAULT_BRIDGE_CACHE_MAX_ENTRIES,
    INVENTORIED_PROVIDER_BRIDGE_NAMESPACES,
    BridgeCacheRegistry,
    bridge_cache_stats,
    reset_all_bridge_caches_for_tests,
)
from okto_pulse.core.kg.query_rewrite.llm_provider_bridges import (
    make_hyde_llm_fn,
    reset_bridge_cache as reset_query_bridge_cache,
)


class _Provider:
    pass


INVENTORIED_PROVIDER_BRIDGE_MODULES = (
    (
        "events.handlers.learning_summariser",
        "okto_pulse.core.events.handlers.llm_provider_bridges",
    ),
    ("kg.adaptive_hops", "okto_pulse.core.kg.adaptive_hops.llm_provider_bridges"),
    ("kg.agent.heuristics", "okto_pulse.core.kg.agent.heuristics.llm_provider_bridges"),
    ("kg.context_compress", "okto_pulse.core.kg.context_compress.llm_provider_bridges"),
    ("kg.grounding", "okto_pulse.core.kg.grounding.llm_provider_bridges"),
    ("kg.query_rewrite", "okto_pulse.core.kg.query_rewrite.llm_provider_bridges"),
    ("kg.rerank", "okto_pulse.core.kg.rerank.llm_provider_bridges"),
    ("kg.retrieve_critic", "okto_pulse.core.kg.retrieve_critic.llm_provider_bridges"),
)


def _module_rel_path(module) -> str:
    path = Path(module.__file__).resolve().as_posix()
    return "okto_pulse/" + path.rsplit("/okto_pulse/", 1)[1]


def _module_level_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names.update(
                target.id for target in node.targets if isinstance(target, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def test_af03_bridge_cache_reuses_identity_by_namespace_realm_and_key():
    registry = BridgeCacheRegistry(max_entries=4)

    first = registry.get_or_create("ns.a", ("provider", "b1", "a1"), object)
    second = registry.get_or_create("ns.a", ("provider", "b1", "a1"), object)
    other_namespace = registry.get_or_create("ns.b", ("provider", "b1", "a1"), object)
    other_realm = registry.get_or_create(
        "ns.a", ("provider", "b1", "a1"), object, realm="tenant-2"
    )

    assert first is second
    assert other_namespace is not first
    assert other_realm is not first


def test_af03_bridge_cache_evicts_lru_only_when_small_test_limit_is_exceeded():
    registry = BridgeCacheRegistry(max_entries=2)

    a = registry.get_or_create("ns", "a", object)
    b = registry.get_or_create("ns", "b", object)
    assert registry.get_or_create("ns", "a", object) is a
    c = registry.get_or_create("ns", "c", object)

    assert c is registry.get_or_create("ns", "c", object)
    assert registry.get_or_create("ns", "a", object) is a
    assert registry.get_or_create("ns", "b", object) is not b
    stats = registry.stats()
    assert len(stats) == 1
    assert stats[0].size == 2


def test_af03_bridge_cache_rejects_zero_entry_override():
    registry = BridgeCacheRegistry(max_entries=2)

    try:
        registry.get_or_create("ns", "a", object, max_entries=0)
    except ValueError as exc:
        assert "max_entries must be >= 1" in str(exc)
    else:  # pragma: no cover - explicit failure branch for assertion clarity.
        raise AssertionError("max_entries=0 must not silently fall back to default")


def test_af03_default_runtime_cache_does_not_evict_inventory_namespaces():
    assert DEFAULT_BRIDGE_CACHE_MAX_ENTRIES >= len(INVENTORIED_PROVIDER_BRIDGE_NAMESPACES)
    registry = BridgeCacheRegistry()
    created = {}

    for namespace in INVENTORIED_PROVIDER_BRIDGE_NAMESPACES:
        created[namespace] = registry.get_or_create(namespace, ("p", "b", "a"), object)

    for namespace, value in created.items():
        assert registry.get_or_create(namespace, ("p", "b", "a"), object) is value


def test_af03_inventoried_bridges_have_no_local_cache_and_keep_reset_wrapper():
    assert tuple(
        namespace for namespace, _module_name in INVENTORIED_PROVIDER_BRIDGE_MODULES
    ) == INVENTORIED_PROVIDER_BRIDGE_NAMESPACES

    rel_files: list[str] = []
    for _namespace, module_name in INVENTORIED_PROVIDER_BRIDGE_MODULES:
        module = importlib.import_module(module_name)

        assert callable(module.reset_bridge_cache)
        module.reset_bridge_cache()
        rel_files.append(_module_rel_path(module))

        source = Path(module.__file__).read_text(encoding="utf-8")
        module_names = _module_level_names(ast.parse(source))
        assert not ({"_bridge_cache", "_bridge_lock"} & module_names)

    report = AntiSingletonGate().run(
        AntiSingletonGateInput(only_files=tuple(rel_files))
    )

    assert report.status == "baseline"
    assert report.evidence["new_singletons"] == []


def test_af03_bridge_wrappers_reset_only_their_namespace():
    reset_all_bridge_caches_for_tests()
    provider = _Provider()

    hyde_1 = make_hyde_llm_fn(provider, board_id="b1")
    hyde_2 = make_hyde_llm_fn(provider, board_id="b1")
    hop_1 = make_hop_llm_fn(provider, board_id="b1")
    hop_2 = make_hop_llm_fn(provider, board_id="b1")

    assert hyde_1 is hyde_2
    assert hop_1 is hop_2
    assert hyde_1 is not hop_1
    assert {
        (stat.namespace, stat.size) for stat in bridge_cache_stats()
    } >= {("kg.query_rewrite", 1), ("kg.adaptive_hops", 1)}

    reset_query_bridge_cache()

    assert make_hyde_llm_fn(provider, board_id="b1") is not hyde_1
    assert make_hop_llm_fn(provider, board_id="b1") is hop_1

    reset_hop_bridge_cache()
    assert make_hop_llm_fn(provider, board_id="b1") is not hop_1


def test_af03_bridge_cache_registry_is_registered_in_singleton_ledger():
    ledger_name = "_bridge_cache_registry"
    baseline_key = "okto_pulse/core/kg/llm_provider_bridge_cache.py::_bridge_cache_registry"

    assert ledger_name in SINGLETON_LEDGER
    assert baseline_key in BASELINE_SINGLETONS
    assert SINGLETON_LEDGER[ledger_name]["target_provider"] == "llm_provider_bridge_cache"

    report = AntiSingletonGate().run(
        AntiSingletonGateInput(
            only_files=("okto_pulse/core/kg/llm_provider_bridge_cache.py",)
        )
    )

    assert report.status == "baseline"
    assert report.evidence["new_singletons"] == []
