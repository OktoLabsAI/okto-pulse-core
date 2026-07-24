from __future__ import annotations

import inspect
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.interfaces import get_kg_registry, reset_registry_for_tests


class _PathAccessForbidden:
    def board_graph_path(self, board_id: str) -> Path:
        raise AssertionError(
            f"core must not resolve a physical graph path for {board_id}"
        )

    def exists(self, board_id: str) -> bool:
        raise AssertionError(
            f"core must not call graph_path_resolver.exists for {board_id}"
        )

    def storage_state(self, board_id: str) -> Any:
        raise AssertionError(
            f"core must not inspect path-backed storage state for {board_id}"
        )


class _CountingCypher:
    def execute_read_only(
        self,
        board_id: str,
        cypher: str,
        params: dict[str, Any] | None = None,
        *,
        max_rows: int = 1000,
    ) -> dict[str, Any]:
        count = 1 if "MATCH (n:Decision)" in cypher else 0
        return {"rows": [[count]], "row_count": 1, "truncated": False}


class _NoopAsyncDb:
    def __init__(self) -> None:
        self.executed: list[Any] = []
        self.committed = False

    async def execute(self, statement: Any) -> None:
        self.executed.append(statement)

    async def get(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def commit(self) -> None:
        self.committed = True


@pytest.fixture(autouse=True)
def _isolated_kg_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


def _runtime_store():
    from okto_pulse.core.kg.interfaces.graph_runtime_store import (
        GraphPurgeResult,
        GraphRuntimeState,
        GraphStorageFootprint,
    )
    from okto_pulse.core.kg.interfaces.storage_ref import StorageRef

    class RuntimeStore:
        def __init__(self) -> None:
            self.purge_calls: list[tuple[str, str]] = []
            self.exists_calls: list[str] = []
            self.footprint_calls: list[str] = []

        def graph_state(self, board_id: str) -> GraphRuntimeState:
            return GraphRuntimeState(
                board_id=board_id,
                storage_ref=StorageRef(f"board:{board_id}", "logical_fake"),
                exists=True,
                status="healthy",
                backend="logical_fake",
                schema_version="0.3.7",
                locked=False,
                quarantined=False,
                unavailable_reason=None,
                details={"source": "test_fake"},
            )

        def exists(self, board_id: str) -> bool:
            self.exists_calls.append(board_id)
            return True

        def purge_board_graph(self, board_id: str, *, reason: str) -> GraphPurgeResult:
            self.purge_calls.append((board_id, reason))
            return GraphPurgeResult(
                board_id=board_id,
                removed=True,
                not_found=False,
                status="purged",
                reason=reason,
                backend="logical_fake",
                error_code=None,
            )

        def footprint(self, board_id: str) -> GraphStorageFootprint:
            self.footprint_calls.append(board_id)
            return GraphStorageFootprint(
                board_id=board_id,
                storage_ref=StorageRef(f"board:{board_id}", "logical_fake"),
                status="unavailable",
                source="runtime_capability",
                total_bytes=None,
                primary_bytes=None,
                sidecar_bytes=None,
                configured_max_bytes=1024,
                percentage=None,
                unavailable_reason="not_exposed_by_backend",
            )

    return RuntimeStore()


def test_ts1_graph_runtime_store_contract_has_no_path_surface() -> None:
    from okto_pulse.core.kg.interfaces.graph_runtime_store import GraphRuntimeStore

    required_methods = {
        "graph_state",
        "exists",
        "purge_board_graph",
        "erase_board_graph",
        "footprint",
    }
    for method_name in required_methods:
        method = getattr(GraphRuntimeStore, method_name)
        signature = inspect.signature(method)
        assert "Path" not in repr(signature)
        assert "path" not in repr(signature).lower()

    configure_test_kg_registry(graph_provider="inmemory")
    runtime_store = get_kg_registry().graph_runtime_store
    assert isinstance(runtime_store, GraphRuntimeStore)
    for value in asdict(runtime_store.graph_state("af17-contract-board")).values():
        assert "graph.lbug" not in str(value)
        assert "board_kuzu_path" not in str(value)


@pytest.mark.asyncio
async def test_ts2_right_to_erasure_uses_logical_purge_not_path(monkeypatch) -> None:
    runtime = _runtime_store()
    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_runtime_store=runtime,
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.global_discovery.clustering.board_delete_cascade",
        lambda board_id: {"deleted": board_id},
    )

    from okto_pulse.core.kg.governance import right_to_erasure

    result = await right_to_erasure(_NoopAsyncDb(), "af17-erasure-board")

    assert runtime.purge_calls == [("af17-erasure-board", "right_to_erasure")]
    assert result["graph_purge"] == {
        "board_id": "af17-erasure-board",
        "removed": True,
        "not_found": False,
        "status": "purged",
        "reason": "right_to_erasure",
        "backend": "logical_fake",
        "error_code": None,
    }
    assert "kuzu_file_removed" not in result
    assert "kuzu_file_error" not in result


def test_ts3_check_graph_uses_graph_state_without_physical_path() -> None:
    runtime = _runtime_store()
    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_runtime_store=runtime,
        cypher_executor=_CountingCypher(),
    )

    from okto_pulse.core.kg.health import check_graph

    health = check_graph("af17-health-board")

    assert health.healthy is True
    assert health.counts["total"] == 1
    rendered = f"{health.details} {health.counts}".lower()
    assert "graph not bootstrapped at" not in rendered
    assert "graph.lbug" not in rendered
    assert "board_kuzu_path" not in rendered
    assert "managed_metric" not in rendered
    assert "backend_classification" not in rendered


def test_ts1_ts3_footprint_uses_runtime_capability_without_path() -> None:
    runtime = _runtime_store()
    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_runtime_store=runtime,
    )

    from okto_pulse.core.services.kg_health_service import (
        _build_storage_footprint_proxy,
        _compute_board_graph_high_water_mark_pct,
    )

    assert _compute_board_graph_high_water_mark_pct("af17-footprint-board") is None
    proxy = _build_storage_footprint_proxy("af17-footprint-board")

    assert runtime.footprint_calls == [
        "af17-footprint-board",
        "af17-footprint-board",
    ]
    assert proxy["status"] == "unavailable"
    assert proxy["source"] == "runtime_capability"
    assert proxy["unavailable_reason"] == "not_exposed_by_backend"
    assert proxy["total_bytes"] is None
    rendered = str(proxy).lower()
    assert "graph.lbug" not in rendered
    assert "board_kuzu_path" not in rendered
    assert "managed_metric" not in rendered
    assert "backend_classification" not in rendered


def test_ts4_graph_runtime_surface_gate_blocks_path_and_backend_terms(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary import (
        GraphRuntimeSurfaceGate,
        GraphRuntimeSurfaceGateInput,
    )

    target = tmp_path / "okto_pulse" / "core" / "kg" / "interfaces" / "bad.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "from pathlib import Path\n"
        "GRAPH = 'graph.lbug'\n"
        "def board_kuzu_path(board_id: str) -> Path:\n"
        "    return Path('/tmp') / board_id / GRAPH\n",
        encoding="utf-8",
    )

    report = GraphRuntimeSurfaceGate().run(
        GraphRuntimeSurfaceGateInput(source_root=tmp_path, mode="blocking")
    )

    assert report.status == "blocking"
    violations = report.evidence["violations"]
    assert {v["term"] for v in violations} >= {
        "graph.lbug",
        "board_kuzu_path",
    }
    assert all(v["file"] == "okto_pulse/core/kg/interfaces/bad.py" for v in violations)


def test_ts4_current_core_common_contracts_are_backend_agnostic() -> None:
    from okto_pulse.core.application.boundary import (
        GraphRuntimeSurfaceGate,
        GraphRuntimeSurfaceGateInput,
        LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER,
    )

    report = GraphRuntimeSurfaceGate().run(
        GraphRuntimeSurfaceGateInput(mode="blocking")
    )

    assert report.status == "passed"
    assert report.evidence["violations"] == []
    assert report.evidence["compatibility_allowlist"] == []
    assert report.evidence["compatibility_ledger_findings"] == []

    assert LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER == ()
    assert len(report.evidence["scanned_files"]) > 100


def test_ts4_graph_runtime_gate_scans_services_outside_contract_package(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary import (
        GraphRuntimeSurfaceGate,
        GraphRuntimeSurfaceGateInput,
    )

    services_dir = tmp_path / "okto_pulse" / "core" / "services"
    services_dir.mkdir(parents=True)
    (services_dir / "bad_health.py").write_text(
        "def inspect_backend(conn):\n    return conn.execute('CALL SHOW_TABLES()')\n",
        encoding="utf-8",
    )

    report = GraphRuntimeSurfaceGate().run(
        GraphRuntimeSurfaceGateInput(source_root=tmp_path, mode="blocking")
    )

    assert report.status == "blocking"
    violations = report.evidence["violations"]
    assert {
        "file": "okto_pulse/core/services/bad_health.py",
        "line": 2,
        "term": "CALL SHOW_TABLES",
        "category": "backend_literal",
    } in violations
