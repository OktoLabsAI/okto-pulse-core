"""Advanced ports remain optional and contain no implementation dependency."""

import ast
from pathlib import Path
from okto_pulse.core.kg.interfaces import graph_observations, ranked_graph_search
from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry


def test_optional_capabilities_do_not_require_a_native_provider():
    registry = KGProviderRegistry()
    for name in ("graph_history", "graph_analytics", "ranked_graph_search"):
        assert getattr(registry, name) is None
        setattr(registry, name, object())
        assert getattr(registry, name) is not None


def test_new_ports_do_not_import_community_or_database_implementations():
    for module in (graph_observations, ranked_graph_search):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not any(
                    name in (node.module or "")
                    for name in ("community", "grafx", "neo4j", "ladybug")
                )
            elif isinstance(node, ast.Import):
                assert all(
                    not any(
                        name in alias.name
                        for name in ("community", "grafx", "neo4j", "ladybug")
                    )
                    for alias in node.names
                )
