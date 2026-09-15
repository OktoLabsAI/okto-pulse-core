"""Read-side KG projection for persisted Code Traceability metadata."""

from __future__ import annotations

from datetime import UTC, datetime

from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.cursor_codec import encode_cursor
from okto_pulse.core.kg.kg_service import KGService
from okto_pulse.core.kg.schema_contract import (
    CODE_TRACEABILITY_READ_PROPERTIES,
)


TRACEABILITY_VALUES = (
    "implementation_target",
    "receipt-01",
    "github:org/repo",
    "agent:trace",
    "abc123",
    "workspace-01",
    "src/payments/service.py",
    "payments.service.authorize",
    "function",
    "symbol",
    "a" * 64,
    "resolved",
)


class _Executor:
    def __init__(self, rows: list[list[object]]) -> None:
        self.rows = rows
        self.queries: list[str] = []
        self.params: list[dict] = []

    def execute_read_only(
        self,
        _board_id: str,
        query: str,
        params: dict | None = None,
        **_kwargs,
    ) -> dict:
        self.queries.append(query)
        self.params.append(dict(params or {}))
        return {"rows": self.rows}


def test_all_listing_templates_select_the_stable_traceability_projection():
    templates = (
        tpl.GET_ALL_NODES,
        tpl.GET_ALL_NODES_BY_TYPE,
        tpl.GET_ALL_NODES_AFTER_CURSOR,
        tpl.GET_ALL_NODES_BY_TYPE_AFTER_CURSOR,
    )

    for template in templates:
        for property_name in CODE_TRACEABILITY_READ_PROPERTIES:
            assert f"n.{property_name}" in template


def test_get_all_nodes_projects_subtype_and_traceability_metadata(monkeypatch):
    executor = _Executor(
        [[
            "target-01",
            "Entity",
            "Authorize payment",
            "Attested target",
            "2026-08-09T10:00:00+00:00",
            0.95,
            0.8,
            "implementation_target:target-01",
            "canonical",
            "canonical_eligible",
            *TRACEABILITY_VALUES,
        ]]
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.kg_service._get_cypher_executor",
        lambda: executor,
    )

    node = KGService().get_all_nodes(
        "board-projection-current",
        min_confidence=0.0,
        min_relevance=0.0,
    )[0]

    assert {
        name: node[name] for name in CODE_TRACEABILITY_READ_PROPERTIES
    } == dict(zip(CODE_TRACEABILITY_READ_PROPERTIES, TRACEABILITY_VALUES))


def test_get_all_nodes_binds_cursor_timestamp_as_a_typed_utc_value(monkeypatch):
    executor = _Executor([])
    monkeypatch.setattr(
        "okto_pulse.core.kg.kg_service._get_cypher_executor",
        lambda: executor,
    )

    KGService().get_all_nodes(
        "board-cursor-parameter",
        min_confidence=0.0,
        min_relevance=0.0,
        cursor=encode_cursor("2026-09-05T00:20:22.182147Z", "node-500"),
    )

    assert executor.params[0]["cursor_ts"] == datetime(
        2026, 9, 5, 0, 20, 22, 182147, tzinfo=UTC
    )
    assert executor.params[0]["cursor_id"] == "node-500"


def test_get_node_detail_projects_same_metadata(monkeypatch):
    executor = _Executor(
        [[
            "target-01",
            "Authorize payment",
            "Attested target",
            "Agent-submitted evidence",
            "implementation_target:target-01",
            0.95,
            0.8,
            2,
            None,
            "2026-08-09T10:00:00+00:00",
            None,
            *TRACEABILITY_VALUES,
        ]]
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.kg_service._get_cypher_executor",
        lambda: executor,
    )

    node = KGService().get_node_detail("board-detail-projection", "target-01")

    assert node is not None
    assert {
        name: node[name] for name in CODE_TRACEABILITY_READ_PROPERTIES
    } == dict(zip(CODE_TRACEABILITY_READ_PROPERTIES, TRACEABILITY_VALUES))
    assert all(
        f"n.{property_name}" in executor.queries[0]
        for property_name in CODE_TRACEABILITY_READ_PROPERTIES
    )


def test_legacy_shaped_rows_remain_readable_with_null_additive_fields(monkeypatch):
    executor = _Executor(
        [[
            "legacy-01",
            "Entity",
            "Legacy entity",
            "",
            "2026-08-09T10:00:00+00:00",
            0.7,
            0.5,
            "spec:legacy",
            "canonical",
            "canonical_eligible",
        ]]
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.kg_service._get_cypher_executor",
        lambda: executor,
    )

    node = KGService().get_all_nodes(
        "board-projection-legacy",
        min_confidence=0.0,
        min_relevance=0.0,
    )[0]

    assert all(node[name] is None for name in CODE_TRACEABILITY_READ_PROPERTIES)
