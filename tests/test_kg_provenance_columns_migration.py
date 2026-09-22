"""MKG-B C1 — provenance + attestation columns migration (S1) and the
anti-legacy regression guard (S8)."""

from __future__ import annotations

import gc
import uuid

import pytest

from okto_pulse.core.kg.schema_contract import (
    ATTESTATION_COLUMNS,
    LEGACY_NODE_COLUMNS,
    PROVENANCE_COLUMNS,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
)

from kg_schema_testing import (
    NODE_TYPES,
    board_graph_columns,
    bootstrap_board_graph,
    close_all_connections,
)

NEW_COLUMNS = [name for name, _ in PROVENANCE_COLUMNS + ATTESTATION_COLUMNS]


@pytest.fixture
def kg_tempdir(monkeypatch):
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()


def test_s1_fresh_bootstrap_has_all_new_columns_and_version(kg_tempdir):
    assert SCHEMA_VERSION == "0.6.0"
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        cols = board_graph_columns(board_id, node_type)
        for col in NEW_COLUMNS:
            assert col in cols, f"{node_type} missing {col}"


def test_s8_legacy_names_never_reactivated(kg_tempdir):
    from okto_pulse.community.adapters.graph_ddl import COMMON_NODE_ATTRIBUTES

    # The retired names never appear in the new tuples, stable props or DDL.
    for legacy in LEGACY_NODE_COLUMNS:
        assert legacy not in NEW_COLUMNS
        assert legacy not in STABLE_NODE_PROPERTIES
        assert legacy not in COMMON_NODE_ATTRIBUTES
    # LEGACY_NODE_COLUMNS itself is byte-identical to the audited value.
    assert LEGACY_NODE_COLUMNS == ("validation_status", "corroboration_count")

    # A freshly bootstrapped board is NEVER classified as v0.2.0-legacy.
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        cols = board_graph_columns(board_id, node_type)
        assert not cols & set(LEGACY_NODE_COLUMNS), node_type
