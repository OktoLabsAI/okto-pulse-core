"""v0.3.11 reversible-cancellation schema coverage."""

from __future__ import annotations

from contextlib import contextmanager
import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)
from okto_pulse.core.kg.schema_contract import (
    CANCELLATION_COLUMNS,
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_cancelmig_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield base
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _columns(connection, node_type: str) -> set[str]:
    result = connection.execute(f"CALL TABLE_INFO('{node_type}') RETURN *")
    columns: set[str] = set()
    try:
        while result.has_next():
            row = result.get_next()
            columns.add(str(row[1]))
    finally:
        try:
            result.close()
        except Exception:
            pass
    return columns


def test_fresh_bootstrap_has_reversible_cancellation_snapshot(kg_tempdir):
    assert SCHEMA_VERSION == "0.3.11"
    assert CANCELLATION_COLUMNS == (("pre_cancellation_relevance_score", "DOUBLE"),)
    assert "pre_cancellation_relevance_score" in STABLE_NODE_PROPERTIES

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_database, connection):
        for node_type in NODE_TYPES:
            assert "pre_cancellation_relevance_score" in _columns(
                connection, node_type
            ), node_type


def test_legacy_table_gains_cancellation_snapshot_idempotently(kg_tempdir):
    from okto_pulse.community.adapters.kg_runtime import (
        _ensure_cancellation_columns,
    )

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_database, connection):
        connection.execute(
            "CREATE NODE TABLE IF NOT EXISTS LegacyCancellationShim ("
            "id STRING PRIMARY KEY, relevance_score DOUBLE)"
        )
        assert _ensure_cancellation_columns(
            connection,
            "LegacyCancellationShim",
        ) == ["pre_cancellation_relevance_score"]
        assert (
            _ensure_cancellation_columns(
                connection,
                "LegacyCancellationShim",
            )
            == []
        )


def test_cancellation_alter_failure_is_not_silently_swallowed(monkeypatch):
    from okto_pulse.community.adapters import kg_runtime

    class _FailingConnection:
        def execute(self, ddl: str) -> None:
            if f"ALTER TABLE {NODE_TYPES[1]} " in ddl:
                raise RuntimeError("non-retryable binder failure")

    board_id = "board-cancellation-partial-alter"
    kg_runtime._MIGRATED_BOARDS.discard(board_id)

    @contextmanager
    def registered(_board_id: str):
        assert _board_id == board_id
        yield object(), _FailingConnection()

    def cancellation_only_schema(conn: object) -> None:
        # Simulate an ALTER failure on a later node type after an earlier type
        # already completed.
        for node_type in NODE_TYPES[:2]:
            kg_runtime._ensure_cancellation_columns(conn, node_type)

    monkeypatch.setattr(
        kg_runtime,
        "registered_raw_connection",
        registered,
    )
    monkeypatch.setattr(
        kg_runtime,
        "apply_schema_to_connection",
        cancellation_only_schema,
    )

    assert kg_runtime._migrate_board_schema(board_id) is False
    assert board_id not in kg_runtime._MIGRATED_BOARDS


def test_cancellation_probe_checks_every_node_type(monkeypatch):
    from okto_pulse.community.adapters import kg_runtime

    column_name = CANCELLATION_COLUMNS[0][0]

    class _TableInfoResult:
        def __init__(self, columns: tuple[str, ...]) -> None:
            self._rows = [
                [index, name, "STRING", None, False]
                for index, name in enumerate(columns)
            ]

        def has_next(self) -> bool:
            return bool(self._rows)

        def get_next(self) -> list[object]:
            return self._rows.pop(0)

        def close(self) -> None:
            return None

    class _PartialConnection:
        def execute(self, ddl: str) -> _TableInfoResult:
            node_type = ddl.split("'")[1]
            columns = ("id",) if node_type == NODE_TYPES[1] else ("id", column_name)
            return _TableInfoResult(columns)

    @contextmanager
    def registered(_board_id: str):
        yield object(), _PartialConnection()

    monkeypatch.setattr(
        kg_runtime,
        "registered_raw_connection",
        registered,
    )

    # The first table is complete; only a later table is missing the column.
    assert kg_runtime._board_needs_cancellation_migration("board-partial") is True
