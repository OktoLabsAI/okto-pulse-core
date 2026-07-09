from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.kg_service import KGService
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    WriteLifecycleViolation,
    require_global_write_token,
    set_barrier_mode,
    under_global_safe_write,
)


class _FakeEmbeddingProvider:
    def encode(self, _text: str) -> list[float]:
        return [1.0, 0.0]


class _FakeCypherExecutor:
    def execute_read_only(
        self,
        board_id: str,
        cypher: str,
        params: dict[str, Any] | None = None,
        *,
        max_rows: int = 1000,
    ) -> dict:
        assert board_id == "board-a"
        assert "MATCH (n) WHERE n.id IN $ids RETURN n.id" in cypher
        return {"rows": [["node-a"]], "row_count": 1, "truncated": False}


class _FakeResult:
    def __init__(self, rows: list[list[Any]]) -> None:
        self._rows = list(rows)

    def has_next(self) -> bool:
        return bool(self._rows)

    def get_next(self) -> list[Any]:
        return self._rows.pop(0)

    def close(self) -> None:
        pass


class _FakeGlobalConnection:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> _FakeResult:
        assert "QUERY_VECTOR_INDEX" in cypher
        assert params is not None
        assert params["boards"] == ["board-a"]
        assert params["graph_layer"] == "canonical"
        return _FakeResult(
            [[
                "board-a",
                "digest-a",
                "node-a",
                "Decision A",
                "Summary A",
                "Decision",
                "canonical",
                0.1,
            ]]
        )

    def close(self) -> None:
        self.closed = True


class _HealthGlobalConnection:
    def __init__(self, *, digest_count: int = 3) -> None:
        self.closed = False
        self.digest_count = digest_count
        self.executed: list[str] = []

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> _FakeResult:
        self.executed.append(cypher)
        if "CALL SHOW_TABLES()" in cypher:
            return _FakeResult([["DecisionDigest"]])
        if "MATCH (d:DecisionDigest" in cypher:
            assert params == {"bid": "board-a"}
            return _FakeResult([[self.digest_count]])
        return _FakeResult([])

    def close(self) -> None:
        self.closed = True


class _FakeGlobalDiscoveryRuntime:
    def __init__(self) -> None:
        self.ensure_calls = 0
        self.open_calls = 0

    def ensure_layer_schema(self) -> list[str]:
        self.ensure_calls += 1
        return []

    def open_connection(self) -> tuple[object, _FakeGlobalConnection]:
        self.open_calls += 1
        return object(), _FakeGlobalConnection()


class _HealthGlobalDiscoveryRuntime:
    def __init__(self, path: Path, *, digest_count: int = 3) -> None:
        self.path = path
        self.digest_count = digest_count
        self.open_calls = 0
        self.connections: list[_HealthGlobalConnection] = []

    def global_graph_path(self) -> Path:
        return self.path

    def open_connection(self) -> tuple[object, _HealthGlobalConnection]:
        self.open_calls += 1
        conn = _HealthGlobalConnection(digest_count=self.digest_count)
        self.connections.append(conn)
        return object(), conn


class _TokenCheckingGlobalDiscoveryRuntime(_FakeGlobalDiscoveryRuntime):
    def ensure_layer_schema(self) -> list[str]:
        require_global_write_token()
        return super().ensure_layer_schema()


class _FakeDb:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _RuntimeConnection:
    def __init__(self, executed: list[str]) -> None:
        self.executed = executed
        self.closed = False

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> _FakeResult:
        self.executed.append(cypher)
        if "QUERY_VECTOR_INDEX" in cypher:
            assert params is not None
            assert params["boards"] == ["board-a"]
            return _FakeResult(
                [[
                    "board-a",
                    "digest-a",
                    "node-a",
                    "Decision A",
                    "Summary A",
                    "Decision",
                    "canonical",
                    0.1,
                ]]
            )
        return _FakeResult([])

    def close(self) -> None:
        self.closed = True


class _FakeBoardGraphRuntime:
    def __init__(self) -> None:
        self.opened_dbs: list[_FakeDb] = []
        self.executed: list[str] = []
        self.loaded_extensions = 0

    def open_kuzu_db(self, path: Path) -> _FakeDb:
        db = _FakeDb(path)
        self.opened_dbs.append(db)
        return db

    def new_connection(self, _db: _FakeDb) -> _RuntimeConnection:
        return _RuntimeConnection(self.executed)

    def load_vector_extension(self, _conn: _RuntimeConnection) -> None:
        self.loaded_extensions += 1

    def is_ladybug_corruption_error(self, _exc: BaseException) -> bool:
        return False


def test_core_global_discovery_runtime_symbols_are_removed() -> None:
    core_root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    offenders: list[str] = []
    for py_file in sorted(core_root.rglob("*.py")):
        text = py_file.read_text(encoding="utf-8")
        for forbidden in ("_global_db", "_global_kuzu_path", "GLOBAL_DISCOVERY_FILENAME"):
            if forbidden in text:
                offenders.append(f"{py_file.relative_to(core_root)}::{forbidden}")

    assert offenders == []


def test_query_global_uses_global_discovery_runtime_provider() -> None:
    runtime = _FakeGlobalDiscoveryRuntime()
    configure_test_kg_registry(
        embedding_provider=_FakeEmbeddingProvider(),
        cypher_executor=_FakeCypherExecutor(),
        global_discovery_runtime=runtime,
    )

    rows = KGService().query_global(
        "decision",
        user_boards=["board-a"],
        top_k=1,
        min_similarity=0.1,
    )

    assert runtime.ensure_calls == 1
    assert runtime.open_calls == 1
    assert rows == [
        {
            "board_id": "board-a",
            "digest_id": "digest-a",
            "id": "node-a",
            "title": "Decision A",
            "summary": "Summary A",
            "node_type": "Decision",
            "graph_layer": "canonical",
            "similarity": 0.9,
        }
    ]


def test_outbox_worker_uses_global_discovery_runtime_provider() -> None:
    worker_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "kg"
        / "global_discovery"
        / "outbox_worker.py"
    )
    text = worker_path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    forbidden_helpers = {
        "ensure_global_discovery_layer_schema",
        "open_global_connection",
    }
    forbidden_imports: list[str] = []
    forbidden_calls: list[str] = []

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "okto_pulse.core.kg.global_discovery.schema"
        ):
            forbidden_imports.extend(
                alias.name
                for alias in node.names
                if alias.name in forbidden_helpers
            )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in forbidden_helpers
        ):
            forbidden_calls.append(node.func.id)

    assert forbidden_imports == []
    assert forbidden_calls == []
    assert "require_global_discovery_runtime" in text
    assert "global_runtime.ensure_layer_schema()" in text
    assert "global_runtime.open_connection()" in text


def test_kg_health_global_probe_uses_global_discovery_runtime_provider(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.services import kg_health_service

    path = tmp_path / "discovery.lbug"
    path.write_text("readable")
    runtime = _HealthGlobalDiscoveryRuntime(path)
    configure_test_kg_registry(global_discovery_runtime=runtime)

    telemetry = kg_health_service._probe_global_discovery_telemetry()

    assert telemetry.graph_type == "discovery"
    assert telemetry.recent_wal_errors == 0
    assert runtime.open_calls == 1
    assert runtime.connections[0].closed is True
    assert runtime.connections[0].executed == ["CALL SHOW_TABLES() RETURN name"]


def test_check_global_uses_global_discovery_runtime_provider(tmp_path: Path) -> None:
    from okto_pulse.core.kg.health import check_global

    path = tmp_path / "discovery.lbug"
    path.write_text("readable")
    runtime = _HealthGlobalDiscoveryRuntime(path, digest_count=3)
    configure_test_kg_registry(global_discovery_runtime=runtime)

    health = check_global("board-a")

    assert runtime.open_calls == 1
    assert runtime.connections[0].closed is True
    assert any(
        "MATCH (d:DecisionDigest {board_id: $bid}) RETURN count(d) AS c" in query
        for query in runtime.connections[0].executed
    )
    assert health.healthy is True
    assert health.counts["digests"] == 3
    assert health.details == "3 digests synced"


def test_kg_health_consumers_do_not_import_global_discovery_schema_open_or_path() -> None:
    core_root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    files = [
        core_root / "services" / "kg_health_service.py",
        core_root / "kg" / "health.py",
    ]
    forbidden_helpers = {
        "global_discovery_graph_path",
        "open_global_connection",
    }
    offenders: list[str] = []

    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "okto_pulse.core.kg.global_discovery.schema"
            ):
                offenders.extend(
                    f"{path.relative_to(core_root)}::{alias.name}"
                    for alias in node.names
                    if alias.name in forbidden_helpers
                )

    assert offenders == []


def test_query_global_schema_hardening_runs_inside_global_write_barrier() -> None:
    runtime = _TokenCheckingGlobalDiscoveryRuntime()
    configure_test_kg_registry(
        embedding_provider=_FakeEmbeddingProvider(),
        cypher_executor=_FakeCypherExecutor(),
        global_discovery_runtime=runtime,
    )

    set_barrier_mode(BarrierMode.STRICT)
    try:
        rows = KGService().query_global(
            "decision",
            user_boards=["board-a"],
            top_k=1,
            min_similarity=0.1,
        )
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert runtime.ensure_calls == 1
    assert runtime.open_calls == 1
    assert rows[0]["id"] == "node-a"


def test_community_global_discovery_bootstrap_with_token_runs_schema_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.community.adapters.global_discovery_runtime import (
        CommunityGlobalDiscoveryRuntime,
    )

    graph_runtime = _FakeBoardGraphRuntime()
    runtime = CommunityGlobalDiscoveryRuntime(graph_runtime=graph_runtime)
    primary = tmp_path / "global" / "discovery.lbug"
    monkeypatch.setattr(runtime, "global_graph_path", lambda: primary)
    monkeypatch.setattr(runtime, "_runtime", lambda: graph_runtime)

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with under_global_safe_write("r09-bootstrap", "bootstrap"):
            assert runtime.bootstrap() == primary
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert primary.parent.exists()
    assert any("CREATE NODE TABLE IF NOT EXISTS Board" in q for q in graph_runtime.executed)
    assert any("ALTER TABLE DecisionDigest ADD graph_layer" in q for q in graph_runtime.executed)
    assert graph_runtime.opened_dbs[0].closed is True


def test_community_global_discovery_purge_with_token_quarantines_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.community.adapters.global_discovery_runtime import (
        CommunityGlobalDiscoveryRuntime,
    )

    runtime = CommunityGlobalDiscoveryRuntime()
    storage_root = tmp_path / "global"
    storage_root.mkdir()
    primary = storage_root / "discovery.lbug"
    primary.write_text("old-graph", encoding="utf-8")
    sidecar = storage_root / "discovery.lbug.wal"
    sidecar.write_text("old-wal", encoding="utf-8")
    monkeypatch.setattr(runtime, "global_graph_path", lambda: primary)

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with under_global_safe_write("r09-purge", "purge"):
            removed = runtime.purge(reason="r09-positive")
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert sorted(Path(p).name for p in removed) == ["discovery.lbug", "discovery.lbug.wal"]
    assert not primary.exists()
    assert not sidecar.exists()
    quarantine_root = tmp_path / "quarantine"
    assert any((path / "manifest.json").exists() for path in quarantine_root.iterdir())


def test_community_global_discovery_close_reopen_preserves_query_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.community.adapters.global_discovery_runtime import (
        CommunityGlobalDiscoveryRuntime,
    )

    graph_runtime = _FakeBoardGraphRuntime()
    runtime = CommunityGlobalDiscoveryRuntime(graph_runtime=graph_runtime)
    primary = tmp_path / "global" / "discovery.lbug"
    primary.parent.mkdir()
    primary.write_text("existing-graph", encoding="utf-8")
    monkeypatch.setattr(runtime, "global_graph_path", lambda: primary)
    configure_test_kg_registry(
        embedding_provider=_FakeEmbeddingProvider(),
        cypher_executor=_FakeCypherExecutor(),
        board_graph_runtime=graph_runtime,
        global_discovery_runtime=runtime,
    )

    first = KGService().query_global(
        "decision",
        user_boards=["board-a"],
        top_k=1,
        min_similarity=0.1,
    )
    runtime.close()
    second = KGService().query_global(
        "decision",
        user_boards=["board-a"],
        top_k=1,
        min_similarity=0.1,
    )

    assert first == second
    assert first[0]["id"] == "node-a"
    assert len(graph_runtime.opened_dbs) == 2
    assert graph_runtime.opened_dbs[0].closed is True
    assert primary.read_text(encoding="utf-8") == "existing-graph"


def test_global_discovery_consumer_gate_blocks_core_direct_runtime_access(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary.conformance_suite import ConformanceSuite
    from okto_pulse.core.application.boundary.global_discovery_consumer_gate import (
        GlobalDiscoveryConsumerGate,
    )

    consumer = tmp_path / "okto_pulse" / "core" / "kg" / "bad_consumer.py"
    consumer.parent.mkdir(parents=True)
    consumer.write_text(
        "from pathlib import Path\n\n"
        "_global_db = None\n\n"
        "def leak(db):\n"
        "    global _global_db\n"
        "    _global_db = db\n"
        "    return Path('discovery.lbug')\n",
        encoding="utf-8",
    )

    report = GlobalDiscoveryConsumerGate().run(source_root=tmp_path)
    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    kinds = {offender["kind"] for offender in offenders}
    assert "forbidden_symbol_reference" in kinds
    assert "forbidden_path_resolution" in kinds

    suite = ConformanceSuite().run(source_root=tmp_path)
    assert suite["axes"]["global_discovery_consumer"]["status"] == "blocking"
    assert "global_discovery_consumer" in suite["blocking_axes"]


def test_global_discovery_residual_singleton_is_not_ledgered_and_blocks(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary.singleton_gate import (
        BASELINE_SINGLETONS,
        SINGLETON_LEDGER,
        AntiSingletonGate,
        AntiSingletonGateInput,
    )

    assert "_global_db" not in SINGLETON_LEDGER
    assert all("_global_db" not in item for item in BASELINE_SINGLETONS)

    leaked = tmp_path / "okto_pulse" / "core" / "kg" / "global_discovery" / "runtime.py"
    leaked.parent.mkdir(parents=True)
    leaked.write_text(
        "_global_db = None\n\n"
        "def remember(db):\n"
        "    global _global_db\n"
        "    _global_db = db\n",
        encoding="utf-8",
    )

    report = AntiSingletonGate().run(AntiSingletonGateInput(source_root=tmp_path))
    assert report.status == "blocking"
    assert report.evidence["error"] == "new_singleton"
    assert any(
        item["name"] == "_global_db"
        and item["file"] == "okto_pulse/core/kg/global_discovery/runtime.py"
        for item in report.evidence["new_singletons"]
    )


def test_community_global_discovery_purge_without_token_does_not_mutate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.community.adapters.global_discovery_runtime import (
        CommunityGlobalDiscoveryRuntime,
    )

    runtime = CommunityGlobalDiscoveryRuntime()
    storage_root = tmp_path / "global"
    storage_root.mkdir()
    primary = storage_root / "discovery.lbug"
    primary.write_text("preserve-me", encoding="utf-8")
    sidecar = storage_root / "discovery.lbug.wal"
    sidecar.write_text("preserve-sidecar", encoding="utf-8")
    monkeypatch.setattr(runtime, "global_graph_path", lambda: primary)

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with pytest.raises(WriteLifecycleViolation):
            runtime.purge(reason="negative-test")
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert primary.read_text(encoding="utf-8") == "preserve-me"
    assert sidecar.read_text(encoding="utf-8") == "preserve-sidecar"
