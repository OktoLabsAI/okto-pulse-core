"""The suite must release native participants at module boundaries."""

from uuid import uuid4
from types import SimpleNamespace

import pytest

import conftest
from kg_schema_testing import close_all_connections, graph_composition


def test_module_registry_fixture_releases_native_graph_participants():
    lifecycle = conftest._kg_registry_module_bootstrap_seed.__wrapped__(
        SimpleNamespace(node=SimpleNamespace(nodeid="native-module-lifetime-probe"))
    )
    next(lifecycle)
    bundle = graph_composition()
    route = bundle.board.initialize_board_route(f"fixture-lifetime-{uuid4().hex}")
    bundle.board.grafx_pool.get(route.active_path, page_size=route.page_size)
    assert bundle.board.grafx_pool.pooled_paths()
    try:
        with pytest.raises(StopIteration):
            next(lifecycle)
        assert bundle.board.grafx_pool.pooled_paths() == ()
    finally:
        lifecycle.close()
        close_all_connections()


def test_strict_module_cleanup_attempts_all_pools_and_reports_failure(monkeypatch):
    import kg_schema_testing

    effects = []

    def failing():
        effects.append("first")
        raise RuntimeError("participant cleanup failed")

    monkeypatch.setattr(kg_schema_testing, "_pools", lambda: (
        SimpleNamespace(close_all=failing),
        SimpleNamespace(close_all=lambda: effects.append("second")),
    ))
    with pytest.raises(ExceptionGroup, match="test_graph_participant_cleanup_failed") as failure:
        close_all_connections(strict=True)
    assert effects == ["first", "second"]
    assert len(failure.value.exceptions) == 1
