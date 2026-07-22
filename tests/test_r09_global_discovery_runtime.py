from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.kg_service import KGService
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphPurgeResult,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    require_global_write_token,
    set_barrier_mode,
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
            [
                [
                    "board-a",
                    "digest-a",
                    "node-a",
                    "Decision A",
                    "Summary A",
                    "Decision",
                    "canonical",
                    0.1,
                ]
            ]
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
        self.execute_calls = 0

    def ensure_layer_schema(self) -> tuple[str, ...]:
        self.ensure_calls += 1
        return ()

    def execute(self, statement: str, params=None) -> GraphStatementResult:
        self.execute_calls += 1
        assert "QUERY_VECTOR_INDEX" in statement
        assert params["boards"] == ["board-a"]
        return GraphStatementResult.from_rows(
            [
                [
                    "board-a",
                    "digest-a",
                    "node-a",
                    "Decision A",
                    "Summary A",
                    "Decision",
                    "canonical",
                    0.1,
                ]
            ]
        )

    def search_decision_digests(
        self,
        query_vector,
        *,
        board_ids,
        graph_layer,
        top_k,
        min_similarity,
        exhaustive=False,
    ):
        del query_vector, top_k, min_similarity, exhaustive
        self.execute_calls += 1
        assert board_ids == ("board-a",)
        assert graph_layer == "canonical"
        return [
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

    def list_schema_objects(self) -> tuple[str, ...]:
        return ("DecisionDigest",)


class _HealthGlobalDiscoveryRuntime:
    def __init__(self, path: Path, *, digest_count: int = 3) -> None:
        self.path = path
        self.digest_count = digest_count
        self.state_calls = 0
        self.execute_calls = 0
        self.executed: list[str] = []

    def state(self) -> GraphRuntimeState:
        self.state_calls += 1
        return GraphRuntimeState(
            board_id="_global",
            storage_ref=StorageRef("global-discovery", "test"),
            exists=self.path.exists(),
            status="healthy" if self.path.exists() else "absent",
            backend="test",
        )

    def execute(self, statement: str, params=None) -> GraphStatementResult:
        self.execute_calls += 1
        self.executed.append(statement)
        if "CALL SHOW_TABLES()" in statement:
            return GraphStatementResult.from_rows([["DecisionDigest"]])
        if "MATCH (d:DecisionDigest" in statement:
            assert params == {"bid": "board-a"}
            return GraphStatementResult.from_rows([[self.digest_count]])
        return GraphStatementResult()

    def list_schema_objects(self) -> tuple[str, ...]:
        self.execute_calls += 1
        return ("DecisionDigest",)


class _TokenCheckingGlobalDiscoveryRuntime(_FakeGlobalDiscoveryRuntime):
    def ensure_layer_schema(self) -> list[str]:
        from okto_pulse.core.kg.global_discovery_writer import (
            assert_global_discovery_writer_fence,
        )

        assert_global_discovery_writer_fence()
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
        if "CALL SHOW_TABLES()" in cypher:
            return _FakeResult(
                [["Board"], ["Topic"], ["Entity"], ["DecisionDigest"]]
            )
        if "CALL SHOW_INDEXES()" in cypher:
            return _FakeResult(
                [
                    ["board_summary_idx"],
                    ["topic_centroid_idx"],
                    ["entity_embedding_idx"],
                    ["digest_embedding_idx"],
                ]
            )
        if "QUERY_VECTOR_INDEX" in cypher:
            assert params is not None
            assert params["boards"] == ["board-a"]
            return _FakeResult(
                [
                    [
                        "board-a",
                        "digest-a",
                        "node-a",
                        "Decision A",
                        "Summary A",
                        "Decision",
                        "canonical",
                        0.1,
                    ]
                ]
            )
        return _FakeResult([])

    def close(self) -> None:
        self.closed = True


class _FakeBoardGraphRuntime:
    def __init__(self) -> None:
        self.opened_dbs: list[_FakeDb] = []
        self.executed: list[str] = []
        self.loaded_extensions = 0

    def open_global_kuzu_db(self, path: Path, *, on_corruption=None) -> _FakeDb:
        del on_corruption
        db = _FakeDb(path)
        self.opened_dbs.append(db)
        return db

    def new_connection(self, _db: _FakeDb) -> _RuntimeConnection:
        return _RuntimeConnection(self.executed)

    def load_vector_extension(
        self,
        _conn: _RuntimeConnection,
        *,
        install: bool = True,
    ) -> None:
        del install
        self.loaded_extensions += 1

    def is_ladybug_corruption_error(self, _exc: BaseException) -> bool:
        return False


def test_core_global_discovery_runtime_symbols_are_removed() -> None:
    core_root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    offenders: list[str] = []
    for py_file in sorted(core_root.rglob("*.py")):
        text = py_file.read_text(encoding="utf-8")
        for forbidden in (
            "_global_db",
            "_global_kuzu_path",
            "GLOBAL_DISCOVERY_FILENAME",
        ):
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
    assert runtime.execute_calls >= 1
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
        / "application"
        / "processors"
        / "global_outbox.py"
    )
    text = worker_path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    processor_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "GlobalOutboxProcessor"
    )
    methods = {
        node.name: node
        for node in processor_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def offloaded_operations(
        method: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> list[ast.expr]:
        operations: list[ast.expr] = []
        for node in ast.walk(method):
            if not (
                isinstance(node, ast.Await)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "_run_graph_io"
            ):
                continue
            if node.value.args:
                operations.append(node.value.args[0])
                continue
            operation_keyword = next(
                (
                    keyword.value
                    for keyword in node.value.keywords
                    if keyword.arg == "operation"
                ),
                None,
            )
            if operation_keyword is not None:
                operations.append(operation_keyword)
        return operations

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
                alias.name for alias in node.names if alias.name in forbidden_helpers
            )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in forbidden_helpers
        ):
            forbidden_calls.append(node.func.id)

    assert forbidden_imports == []
    assert forbidden_calls == []

    provider_function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_global_discovery_runtime"
    )
    provider_returns = [
        node.value for node in provider_function.body if isinstance(node, ast.Return)
    ]
    assert len(provider_returns) == 1
    provider_call = provider_returns[0]
    assert isinstance(provider_call, ast.Call)
    assert isinstance(provider_call.func, ast.Attribute)
    assert provider_call.func.attr == "require_global_discovery_runtime"

    apply_event = methods["_apply_event"]
    runtime_names = {
        target.id
        for node in ast.walk(apply_event)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_global_discovery_runtime"
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert runtime_names
    while True:
        aliases = {
            target.id
            for node in ast.walk(apply_event)
            if isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Name)
            and node.value.id in runtime_names
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        if aliases <= runtime_names:
            break
        runtime_names.update(aliases)

    apply_event_operations = offloaded_operations(apply_event)
    offloaded_node_ids = {
        id(node) for operation in apply_event_operations for node in ast.walk(operation)
    }
    runtime_method_refs = [
        node
        for node in ast.walk(apply_event)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id in runtime_names
    ]
    assert {
        "ensure_layer_schema",
        "link_board_digest",
        "upsert_board_summary",
        "upsert_decision_digest",
    } <= {node.attr for node in runtime_method_refs if id(node) in offloaded_node_ids}
    direct_runtime_calls = [
        node
        for node in ast.walk(apply_event)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in runtime_names
        and id(node) not in offloaded_node_ids
    ]
    assert direct_runtime_calls == []

    sync_execute_helpers = {
        method.name
        for method in methods.values()
        if isinstance(method, ast.FunctionDef)
        and any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in {argument.arg for argument in method.args.args}
            for node in ast.walk(method)
        )
    }
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in sync_execute_helpers
        and any(
            isinstance(argument, ast.Name) and argument.id in runtime_names
            for argument in node.args
        )
        for operation in apply_event_operations
        for node in ast.walk(operation)
    )

    process_once_operations = offloaded_operations(
        methods["_process_once_under_writer"]
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_verify_processed_batch"
        for operation in process_once_operations
        for node in ast.walk(operation)
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_flush_global_discovery_storage_after_batch"
        for node in ast.walk(methods["_verify_processed_batch"])
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "flush_after_write_batch"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "_global_discovery_runtime"
        for node in ast.walk(methods["_flush_global_discovery_storage_after_batch"])
    )


def test_kg_health_global_probe_uses_non_opening_global_discovery_runtime_state(
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
    assert runtime.state_calls == 1
    assert runtime.execute_calls == 0
    assert runtime.executed == []


def test_check_global_uses_global_discovery_runtime_provider(tmp_path: Path) -> None:
    from okto_pulse.core.kg.health import check_global

    path = tmp_path / "discovery.lbug"
    path.write_text("readable")
    runtime = _HealthGlobalDiscoveryRuntime(path, digest_count=3)
    configure_test_kg_registry(global_discovery_runtime=runtime)

    health = check_global("board-a")

    assert runtime.execute_calls == 1
    assert any(
        "MATCH (d:DecisionDigest {board_id: $bid}) RETURN count(d) AS c" in query
        for query in runtime.executed
    )
    assert health.healthy is True
    assert health.counts["digests"] == 3
    assert health.details == "3 digests synced"


def test_kg_health_consumers_do_not_import_global_discovery_schema_open_or_path() -> (
    None
):
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


def test_global_discovery_consumers_do_not_import_schema_lifecycle_facade() -> None:
    core_root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    consumer_files = [
        core_root / "application" / "processors" / "global_outbox.py",
        core_root / "kg" / "global_discovery" / "clustering.py",
        core_root / "kg" / "global_discovery" / "layer_parity.py",
        core_root / "kg" / "health.py",
        core_root / "services" / "kg_health_service.py",
    ]
    forbidden_helpers = {
        "bootstrap_global_discovery",
        "ensure_global_discovery_layer_schema",
        "global_discovery_graph_path",
        "open_global_connection",
        "purge_global_discovery_storage",
    }
    offenders: list[str] = []

    for path in consumer_files:
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
    assert runtime.execute_calls >= 1
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
    monkeypatch.setattr(runtime, "_global_graph_path", lambda: primary)
    monkeypatch.setattr(runtime, "_runtime", lambda: graph_runtime)

    from okto_pulse.core.kg.global_discovery_writer import (
        global_discovery_writer_scope,
    )

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with global_discovery_writer_scope(operation="r09_bootstrap_test"):
            handle = runtime.bootstrap()
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert primary.parent.exists()
    assert handle.storage_ref.token == "global-discovery"
    assert any(
        "CREATE NODE TABLE IF NOT EXISTS Board" in q for q in graph_runtime.executed
    )
    assert any(
        "ALTER TABLE DecisionDigest ADD graph_layer" in q
        for q in graph_runtime.executed
    )
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
    monkeypatch.setattr(runtime, "_global_graph_path", lambda: primary)

    from okto_pulse.core.kg.global_discovery_writer import (
        global_discovery_writer_scope,
    )

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with global_discovery_writer_scope(operation="r09_purge_test"):
            result = runtime.purge(reason="r09-positive")
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert isinstance(result, GraphPurgeResult)
    assert result.removed is True
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
    monkeypatch.setattr(runtime, "_global_graph_path", lambda: primary)
    configure_test_kg_registry(
        embedding_provider=_FakeEmbeddingProvider(),
        cypher_executor=_FakeCypherExecutor(),
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
    monkeypatch.setattr(runtime, "_global_graph_path", lambda: primary)

    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterFenceLost,
    )

    set_barrier_mode(BarrierMode.STRICT)
    try:
        with pytest.raises(GlobalDiscoveryWriterFenceLost):
            runtime.purge(reason="negative-test")
    finally:
        set_barrier_mode(BarrierMode.SOFT)

    assert primary.read_text(encoding="utf-8") == "preserve-me"
    assert sidecar.read_text(encoding="utf-8") == "preserve-sidecar"
