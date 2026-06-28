from __future__ import annotations


def test_board_connection_loads_vector_extension_on_each_fresh_connection(
    monkeypatch, tmp_path
):
    """Hot board opens still need LOAD VECTOR on the fresh connection."""
    import okto_pulse.community.adapters.kg_runtime as kg_runtime

    class DummyConnection:
        def __init__(self, db):
            self.db = db

    dummy_db = object()
    loaded_connections = []

    monkeypatch.setattr(
        kg_runtime, "ensure_board_graph_bootstrapped", lambda board_id: None
    )
    monkeypatch.setattr(
        kg_runtime, "board_kuzu_path", lambda board_id: tmp_path / "graph.lbug"
    )
    monkeypatch.setattr(
        kg_runtime, "_open_kuzu_db_cached", lambda board_id, path: dummy_db
    )
    monkeypatch.setattr(kg_runtime.kuzu, "Connection", DummyConnection)
    monkeypatch.setattr(
        kg_runtime, "load_vector_extension", lambda conn: loaded_connections.append(conn)
    )

    conn = kg_runtime.BoardConnection("board-vector-hot")

    assert isinstance(conn.conn, DummyConnection)
    assert conn.conn.db is dummy_db
    assert loaded_connections == [conn.conn]


