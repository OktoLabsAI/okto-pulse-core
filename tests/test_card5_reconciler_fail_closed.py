"""Card 5: stale reconciliation is explicit, bounded and fail-closed."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor
from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot
from okto_pulse.core.kg import canonical_stale_reconciler as reconciler
from okto_pulse.core.kg.canonical_stale_reconciler import (
    ALL_NODE_TYPES,
    STALE_RECONCILE_NODE_POLICY,
    StaleReconcileResult,
    reconcile_stale_canonical,
    validate_stale_reconcile_ontology_coverage,
)
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.ports.consolidation import (
    ConsolidationQueueRecord,
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryCircuitSnapshot,
    DeliveryTransferReceipt,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)


class _SourceReader:
    def __init__(self, snapshot: BoardSourceSnapshot) -> None:
        self.snapshot = snapshot
        self.fetch_calls: list[str] = []

    def fetch(self, board_id: str) -> BoardSourceSnapshot:
        self.fetch_calls.append(board_id)
        return self.snapshot


class _GraphScope:
    def __init__(
        self,
        *,
        rows_by_type: dict[
            str,
            list[
                tuple[str, str, str, str]
                | tuple[str, str, str, str, str]
                | tuple[str, str, str, str, str, str | None, float | None]
            ],
        ]
        | None = None,
        fail_query_types: frozenset[str] = frozenset(),
        fail_set_types: frozenset[str] = frozenset(),
    ) -> None:
        self.rows_by_type = rows_by_type or {}
        self.fail_query_types = fail_query_types
        self.fail_set_types = fail_set_types
        self.query_types: list[str] = []
        self.writes: list[tuple[str, str]] = []
        self.write_params: list[dict[str, Any]] = []

    def execute(self, query: str, params: dict[str, Any]) -> SimpleNamespace:
        matched = re.search(r"MATCH \(n:([A-Za-z]+)", query)
        assert matched is not None, query
        node_type = matched.group(1)
        if "RETURN n.id" in query:
            self.query_types.append(node_type)
            if node_type in self.fail_query_types:
                raise RuntimeError(f"injected QUERY failure for {node_type}")
            rows = self.rows_by_type.get(node_type, [])
            if "n.maturity_status, n.graph_layer" in query:
                normalized = []
                for row in rows:
                    values = list(
                        (*row, "canonical", None, None)
                        if len(row) == 4
                        else (*row, None, None)
                        if len(row) == 5
                        else row
                    )
                    while len(values) < 12:
                        values.append(None)
                    if len(values) == 12:
                        values.append(True)
                    if len(values) == 13:
                        values.append("")
                    normalized.append(tuple(values))
                rows = normalized
            return SimpleNamespace(rows=rows)
        if "SET n.graph_layer" in query:
            if node_type in self.fail_set_types:
                raise RuntimeError(f"injected SET failure for {node_type}")
            self.writes.append((node_type, str(params["node_id"])))
            self.write_params.append(dict(params))
            return SimpleNamespace(rows=[])
        if "SET n.source_content_hash" in query:
            return SimpleNamespace(rows=[])
        raise AssertionError(f"unexpected graph query: {query}")


class _GraphContext:
    def __init__(self, scope: _GraphScope) -> None:
        self.scope = scope

    async def __aenter__(self) -> _GraphScope:
        return self.scope

    async def __aexit__(self, *_args: object) -> None:
        return None


class _GraphTransaction:
    def __init__(self, scope: _GraphScope) -> None:
        self.scope = scope
        self.begin_calls: list[str] = []

    async def begin(self, board_id: str) -> _GraphContext:
        self.begin_calls.append(board_id)
        return _GraphContext(self.scope)


class _Registry:
    def __init__(
        self,
        reader: _SourceReader,
        transaction: _GraphTransaction,
    ) -> None:
        self.reader = reader
        self.graph_transaction = transaction

    def require_board_source_reader(self) -> _SourceReader:
        return self.reader


def _install_registry(
    monkeypatch: pytest.MonkeyPatch,
    *,
    snapshot: BoardSourceSnapshot,
    scope: _GraphScope | None = None,
) -> tuple[_SourceReader, _GraphTransaction, _GraphScope]:
    source_reader = _SourceReader(snapshot)
    graph_scope = scope or _GraphScope()
    graph_transaction = _GraphTransaction(graph_scope)
    registry = _Registry(source_reader, graph_transaction)
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    return source_reader, graph_transaction, graph_scope


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause",
    ["db_missing", "table_missing", "realm_incomplete"],
)
async def test_incomplete_snapshot_has_zero_graph_debt_and_sync_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    cause: str,
) -> None:
    reader, transaction, scope = _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            rows=(),
            complete=False,
            cause=cause,  # type: ignore[arg-type]
        ),
    )
    debt_calls: list[dict[str, Any]] = []
    sync_calls: list[str] = []
    graph_write_callbacks: list[str] = []

    async def _debt(*_args: object, **kwargs: Any) -> None:
        debt_calls.append(kwargs)

    async def _sync(_db: object, *, board_id: str) -> None:
        sync_calls.append(board_id)

    from okto_pulse.core.kg import canonical_demotion_global_sync

    monkeypatch.setattr(reconciler, "_route_cognitive_to_debt", _debt)
    monkeypatch.setattr(
        canonical_demotion_global_sync,
        "sync_stale_demotion_to_global_discovery",
        _sync,
    )

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
        correlation_id="delete-card5",
        before_graph_write=lambda: graph_write_callbacks.append("enter"),
    )

    assert reader.fetch_calls == ["board-card5"]
    assert transaction.begin_calls == []
    assert scope.query_types == []
    assert scope.writes == []
    assert debt_calls == []
    assert sync_calls == []
    assert graph_write_callbacks == []
    assert result.incomplete is True
    assert result.incomplete_cause == cause
    assert result.failed_types == []
    assert result.demoted == []
    assert result.to_dict()["incomplete_cause"] == cause


@pytest.mark.asyncio
async def test_graph_write_callback_runs_once_immediately_before_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
    )
    events: list[str] = []

    async def _scan(*_args: object, **_kwargs: object) -> list[dict[str, Any]]:
        events.append("scan")
        return []

    monkeypatch.setattr(reconciler, "_scan_and_demote", _scan)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        before_graph_write=lambda: events.append("enter"),
    )

    assert result.incomplete is False
    assert events == ["enter", "scan"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {"artifact_type": "spec", "id": ""},
        {"artifact_type": "", "id": "source-id"},
        {
            "artifact_type": "spec",
            "id": "source-id",
            "source_ref": "spec:different-id",
        },
        {"artifact_type": "unknown", "id": "source-id"},
    ],
    ids=("missing-id", "missing-type", "mismatched-ref", "unknown-type"),
)
async def test_malformed_row_in_complete_snapshot_fails_closed_before_graph(
    monkeypatch: pytest.MonkeyPatch,
    row: dict[str, Any],
) -> None:
    reader, transaction, scope = _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(rows=(row,)),
    )

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
    )

    assert reader.fetch_calls == ["board-card5"]
    assert transaction.begin_calls == []
    assert scope.writes == []
    assert result.incomplete is True
    assert result.incomplete_cause == "realm_incomplete"


@pytest.mark.parametrize(
    "source_refs",
    [
        [],
        ["malformed"],
        ["spec:"],
        [":artifact"],
        ["unknown:artifact"],
        [" spec:artifact"],
        ["board:board-card5"],
        [None],
    ],
    ids=(
        "empty",
        "missing-separator",
        "missing-id",
        "missing-type",
        "unknown-type",
        "untrimmed",
        "unsupported-infra-ref",
        "non-string",
    ),
)
@pytest.mark.asyncio
async def test_invalid_source_refs_fail_before_source_or_graph_access(
    monkeypatch: pytest.MonkeyPatch,
    source_refs: list[Any],
) -> None:
    reader, transaction, scope = _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
    )

    with pytest.raises(ValueError, match="invalid_source_refs"):
        await reconcile_stale_canonical(
            object(),
            board_id="board-card5",
            source_refs=source_refs,  # type: ignore[arg-type]
        )

    assert reader.fetch_calls == []
    assert transaction.begin_calls == []
    assert scope.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_refs", "expected_node_ids"),
    [
        (None, {"node-target", "node-other"}),
        (["spec:target:criterion:ac-1"], {"node-target"}),
    ],
    ids=("none-authorizes-full-sweep", "valid-list-preserves-exact-scope"),
)
async def test_source_refs_none_and_valid_list_have_distinct_scopes(
    monkeypatch: pytest.MonkeyPatch,
    source_refs: list[str] | None,
    expected_node_ids: set[str],
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Decision": [
                (
                    "node-target",
                    "spec:target:decision:d-1",
                    "system:deterministic",
                    "canonical_eligible",
                ),
                (
                    "node-other",
                    "spec:other:decision:d-2",
                    "system:deterministic",
                    "canonical_eligible",
                ),
            ]
        }
    )
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
        scope=scope,
    )
    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=source_refs,
    )

    assert {node_id for _, node_id in scope.writes} == expected_node_ids
    assert {item["node_id"] for item in result.demoted} == expected_node_ids
    assert result.incomplete is False
    assert result.failed_types == []
    assert result.completed_types == list(ALL_NODE_TYPES)
    # ``completed_types`` records successful per-type query execution, not a
    # positive row count.  A successful empty scan is complete and auditable.
    assert all(
        result.scanned_by_type[node_type] == 0
        for node_type in ALL_NODE_TYPES
        if node_type != "Decision"
    )
    # The reconciler owns graph mutation only.  Durable GD delivery is
    # transferred later by the queue worker in one relational transaction.
    assert result.global_sync_enqueued is False
    if source_refs is not None:
        assert result.target_identity_count == 1
        assert result.target_found_count == 1
        assert result.target_demoted_count == 1
        assert result.target_already_converged_count == 0
        assert result.target_skipped_cognitive_count == 0
        assert result.target_preserved_canonical_count == 0
    else:
        assert result.target_identity_count == 0
        assert result.target_found_count == 0


@pytest.mark.asyncio
async def test_targeted_retry_reports_existing_working_projection_as_converged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Entity": [
                (
                    "already-working-card",
                    "card:deleted-card",
                    "system:historical_consolidation",
                    "working_stale",
                        "working",
                        "source_deleted",
                        0.0,
                        "",
                        "",
                        "",
                        "",
                        "",
                        True,
                        "",
                    )
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["card:deleted-card"],
        correlation_id="delete-card-retry",
    )

    assert scope.writes == []
    assert result.demoted == []
    assert result.target_identity_count == 1
    assert result.target_found_count == 1
    assert result.target_demoted_count == 0
    assert result.target_already_converged_count == 1
    assert result.target_skipped_cognitive_count == 0
    assert result.target_preserved_canonical_count == 0
    assert result.to_dict()["target_already_converged_count"] == 1


@pytest.mark.asyncio
async def test_targeted_delete_tombstones_active_working_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Requirement": [
                (
                    "active-working-requirement",
                    "spec:deleted-spec",
                    "system:deterministic",
                    "working_immature",
                    "working",
                    None,
                    0.75,
                )
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["spec:deleted-spec"],
        correlation_id="delete-spec-working",
    )

    assert scope.writes == [("Requirement", "active-working-requirement")]
    assert scope.write_params == [
        {
            "node_id": "active-working-requirement",
            "graph_layer": "working",
            "maturity_status": "working_stale",
            "revocation_reason": "source_deleted",
            "relevance_score": 0.0,
            "erased_text": "",
        }
    ]
    assert result.target_found_count == 1
    assert result.target_demoted_count == 1
    assert result.target_already_converged_count == 0
    assert result.demoted[0]["prev_layer"] == "working"
    assert result.demoted[0]["revocation_reason"] == "source_deleted"


@pytest.mark.asyncio
async def test_deleted_refinement_tombstones_relational_rdl_but_preserves_cognitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refinement_id = "deleted-refinement"
    decision_ref = (
        f"refinement:{refinement_id}:rdl:ledger-decision:decision"
    )
    alternative_ref = (
        f"refinement:{refinement_id}:rdl:ledger-alternative:alternative:"
        + "a" * 64
    )
    cognitive_ref = (
        f"refinement:{refinement_id}:alternative:ordinary-cognitive"
    )
    scope = _GraphScope(
        rows_by_type={
            "Decision": [
                (
                    "relational-rdl-decision",
                    decision_ref,
                    "system:layer1_worker",
                    "canonical_eligible",
                    "canonical",
                    None,
                    0.75,
                )
            ],
            "Alternative": [
                (
                    "relational-rdl-alternative",
                    alternative_ref,
                    "system:layer1_worker",
                    "canonical_eligible",
                    "canonical",
                    None,
                    0.75,
                ),
                (
                    "ordinary-cognitive-alternative",
                    cognitive_ref,
                    "agent:cognitive",
                    "canonical_eligible",
                    "canonical",
                    None,
                    0.75,
                ),
            ],
        }
    )
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
        scope=scope,
    )

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=[f"refinement:{refinement_id}"],
        correlation_id="delete-refinement-rdl",
    )

    assert set(scope.writes) == {
        ("Decision", "relational-rdl-decision"),
        ("Alternative", "relational-rdl-alternative"),
    }
    assert {
        params["node_id"] for params in scope.write_params
    } == {
        "relational-rdl-decision",
        "relational-rdl-alternative",
    }
    assert all(
        params["graph_layer"] == "working"
        and params["maturity_status"] == "working_stale"
        and params["revocation_reason"] == "source_deleted"
        and params["relevance_score"] == 0.0
        and params["erased_text"] == ""
        for params in scope.write_params
    )
    assert {
        item["node_id"] for item in result.demoted
    } == {
        "relational-rdl-decision",
        "relational-rdl-alternative",
    }
    assert all(
        item["revocation_reason"] == "source_deleted"
        for item in result.demoted
    )
    assert [
        item["node_id"] for item in result.skipped_cognitive
    ] == ["ordinary-cognitive-alternative"]
    assert result.target_identity_count == 1
    assert result.target_found_count == 3
    assert result.target_demoted_count == 2
    assert result.target_skipped_cognitive_count == 1
    assert result.target_already_converged_count == 0
    assert result.target_preserved_canonical_count == 0


@pytest.mark.asyncio
async def test_fallback_fails_closed_when_deleted_node_has_embedding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Requirement": [
                (
                    "embedded-node",
                    "spec:deleted-spec",
                    "system:deterministic",
                    "canonical_eligible",
                    "canonical",
                    None,
                    0.75,
                    "private",
                    "private",
                    None,
                    None,
                    None,
                    False,
                    "private-hash",
                )
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(), board_id="board-card5", source_refs=["spec:deleted-spec"]
    )

    assert scope.writes == []
    assert result.incomplete is True
    assert result.failed_types == ["Requirement"]


@pytest.mark.asyncio
async def test_full_sweep_repairs_incomplete_working_tombstone_and_then_noops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Requirement": [
                (
                    "partial-working-requirement",
                    "spec:deleted-spec",
                    "system:deterministic",
                    "working_stale",
                    "working",
                    None,
                    0.75,
                ),
                (
                    "live-working-requirement",
                    "spec:existing-draft",
                    "system:deterministic",
                    "working_immature",
                    "working",
                    None,
                    0.75,
                ),
            ]
        }
    )
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            rows=(
                {
                    "artifact_type": "spec",
                    "id": "existing-draft",
                    "source_ref": "spec:existing-draft",
                    "status": "draft",
                    "content_hash": "live-working-source",
                },
            )
        ),
        scope=scope,
    )

    first = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
        correlation_id="full-sweep-repair",
    )

    assert scope.write_params == [
        {
            "node_id": "partial-working-requirement",
            "graph_layer": "working",
            "maturity_status": "working_stale",
            "revocation_reason": "source_deleted",
            "relevance_score": 0.0,
            "erased_text": "",
        }
    ]
    assert first.demoted[0]["revocation_reason"] == "source_deleted"

    scope.rows_by_type["Requirement"] = [
        (
            "partial-working-requirement",
            "spec:deleted-spec",
            "system:deterministic",
            "working_stale",
            "working",
            "source_deleted",
            0.0,
        ),
        (
            "live-working-requirement",
            "spec:existing-draft",
            "system:deterministic",
            "working_immature",
            "working",
            None,
            0.75,
        ),
    ]
    scope.writes.clear()
    scope.write_params.clear()

    second = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
        correlation_id="full-sweep-retry",
    )

    assert scope.writes == []
    assert scope.write_params == []
    assert second.demoted == []


@pytest.mark.asyncio
async def test_historical_consolidation_entity_is_a_deterministic_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Entity": [
                (
                    "historical-card-entity",
                    "card:deleted-card",
                    "system:historical_consolidation",
                    "canonical_eligible",
                )
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["card:deleted-card"],
        correlation_id="delete-card-event",
    )

    assert scope.writes == [("Entity", "historical-card-entity")]
    assert [record["node_id"] for record in result.demoted] == [
        "historical-card-entity"
    ]
    assert result.target_found_count == 1
    assert result.target_demoted_count == 1
    assert result.target_already_converged_count == 0
    assert result.target_skipped_cognitive_count == 0
    assert result.target_preserved_canonical_count == 0


@pytest.mark.asyncio
async def test_full_sweep_uses_type_qualified_identity_when_ids_collide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live card must not mask an absent spec that happens to share its id."""

    shared_id = "same-id-across-tables"
    scope = _GraphScope(
        rows_by_type={
            "Decision": [
                (
                    "spec-node",
                    f"spec:{shared_id}:decision:d-1",
                    "system:deterministic",
                    "canonical_eligible",
                )
            ],
            "Entity": [
                (
                    "card-node",
                    f"card:{shared_id}",
                    "system:deterministic",
                    "canonical_eligible",
                )
            ],
        }
    )
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            rows=(
                {
                    "artifact_type": "task",
                    "id": shared_id,
                    "source_ref": f"task:{shared_id}",
                    "status": "done",
                    "content_hash": "live-card-hash",
                },
            )
        ),
        scope=scope,
    )
    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
    )

    assert scope.writes == [("Decision", "spec-node")]
    assert [record["owning_source_type"] for record in result.demoted] == ["spec"]
    assert result.global_sync_enqueued is False


@pytest.mark.asyncio
async def test_fast_path_scope_is_type_qualified_when_ids_collide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared_id = "same-fast-path-id"
    scope = _GraphScope(
        rows_by_type={
            "Decision": [
                (
                    "spec-node",
                    f"spec:{shared_id}:decision:d-1",
                    "system:deterministic",
                    "canonical_eligible",
                )
            ],
            "Entity": [
                (
                    "card-node",
                    f"card:{shared_id}",
                    "system:deterministic",
                    "canonical_eligible",
                )
            ],
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=[f"spec:{shared_id}"],
    )

    assert scope.writes == [("Decision", "spec-node")]
    assert [record["node_id"] for record in result.demoted] == ["spec-node"]


@pytest.mark.asyncio
@pytest.mark.parametrize("card_source_type", ["card", "task", "test", "bug"])
async def test_card_source_aliases_cover_all_card_graph_projections(
    monkeypatch: pytest.MonkeyPatch,
    card_source_type: str,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Entity": [
                (
                    "card-root",
                    "card:card-alias-id",
                    "system:deterministic",
                    "canonical_eligible",
                ),
                (
                    "card-relationship-target",
                    "card_relationship_target:card-alias-id",
                    "system:deterministic",
                    "canonical_eligible",
                ),
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=[f"{card_source_type}:card-alias-id"],
    )

    assert {node_id for _, node_id in scope.writes} == {
        "card-root",
        "card-relationship-target",
    }
    assert {record["owning_source_type"] for record in result.demoted} == {"card"}


@pytest.mark.asyncio
async def test_full_sweep_leaves_infrastructure_and_unknown_refs_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Entity": [
                (
                    "board-root",
                    "board:board-card5",
                    "system:deterministic",
                    "canonical_eligible",
                ),
                (
                    "tech-root",
                    "tech_entities.yml",
                    "system:deterministic",
                    "canonical_eligible",
                ),
                (
                    "future-root",
                    "future_source:source-id",
                    "system:deterministic",
                    "canonical_eligible",
                ),
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=None,
    )

    assert result.scanned_by_type["Entity"] == 3
    assert result.demoted == []
    assert scope.writes == []


@pytest.mark.asyncio
async def test_deleted_bug_routes_preserved_learning_to_canonical_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Bug": [
                (
                    "deterministic-bug-from-deleted-source",
                    "card:deleted-bug-id",
                    "system:deterministic",
                    "canonical_eligible",
                )
            ],
            "Learning": [
                (
                    "learning-from-deleted-bug",
                    "bug:deleted-bug-id",
                    "cognitive:analyst",
                    "canonical_eligible",
                )
            ],
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)
    debt_calls: list[dict[str, Any]] = []

    async def _debt(
        _db: object,
        _board_id: str,
        intent: dict[str, Any],
        _correlation_id: str,
    ) -> None:
        debt_calls.append(intent)

    monkeypatch.setattr(reconciler, "_route_cognitive_to_debt", _debt)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["bug:deleted-bug-id"],
        correlation_id="delete-bug-event",
    )

    assert scope.writes == [("Bug", "deterministic-bug-from-deleted-source")]
    assert all(node_type != "Learning" for node_type, _node_id in scope.writes)
    assert len(debt_calls) == 1
    assert debt_calls[0]["bug_id"] == "deleted-bug-id"
    assert debt_calls[0]["reason_code"] == "source_absent"
    assert result.routed_to_debt == debt_calls
    assert result.incomplete is False
    assert result.failed_types == []


@pytest.mark.asyncio
async def test_deleted_bug_route_uses_shared_learning_debt_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import canonical_learning_partition as partition

    calls: list[dict[str, Any]] = []

    async def _upsert(_db: object, **kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(partition, "upsert_canonical_learning_debt", _upsert)

    await reconciler._route_cognitive_to_debt(
        object(),
        "board-card5",
        {
            "node_id": "learning-from-deleted-bug",
            "node_type": "Learning",
            "source_artifact_ref": "bug:deleted-bug-id",
            "bug_id": "deleted-bug-id",
            "reason_code": "source_absent",
        },
        "delete-bug-event",
    )

    assert calls == [
        {
            "board_id": "board-card5",
            "node_id": "learning-from-deleted-bug",
            "source_ref": "bug:deleted-bug-id",
            "failure_reason": "source_absent",
            "correlation_id": "delete-bug-event",
        }
    ]


@pytest.mark.asyncio
async def test_deleted_bug_debt_failure_is_explicit_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = _GraphScope(
        rows_by_type={
            "Learning": [
                (
                    "learning-debt-fails",
                    "bug:deleted-bug-id",
                    "cognitive:analyst",
                    "canonical_eligible",
                )
            ]
        }
    )
    _install_registry(monkeypatch, snapshot=BoardSourceSnapshot(), scope=scope)

    async def _fail_debt(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected debt persistence failure")

    monkeypatch.setattr(reconciler, "_route_cognitive_to_debt", _fail_debt)

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["card:deleted-bug-id"],
    )

    assert scope.writes == []
    assert result.routed_to_debt == []
    assert result.incomplete is True
    assert result.failed_types == ["Learning"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_boundary", ["query", "set"])
async def test_query_and_set_failures_are_explicit_per_node_type(
    monkeypatch: pytest.MonkeyPatch,
    failure_boundary: str,
) -> None:
    rows = {
        "Requirement": [
            (
                "requirement-fails",
                "spec:deleted:req:fr-1",
                "system:deterministic",
                "canonical_eligible",
            )
        ]
    }
    scope = _GraphScope(
        rows_by_type=rows,
        fail_query_types=(
            frozenset({"Requirement"}) if failure_boundary == "query" else frozenset()
        ),
        fail_set_types=(
            frozenset({"Requirement"}) if failure_boundary == "set" else frozenset()
        ),
    )
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
        scope=scope,
    )

    result = await reconcile_stale_canonical(
        object(),
        board_id="board-card5",
        source_refs=["spec:deleted"],
    )
    payload = result.to_dict()

    assert result.incomplete is True
    assert result.failed_types == ["Requirement"]
    assert "Requirement" not in result.completed_types
    assert set(result.completed_types) == set(ALL_NODE_TYPES) - {"Requirement"}
    assert payload["incomplete"] is True
    assert payload["failed_types"] == ["Requirement"]
    assert payload["completed_types"] == result.completed_types
    assert result.global_sync_enqueued is False
    assert scope.writes == []
    if failure_boundary == "query":
        assert result.scanned_by_type["Requirement"] == 0
    else:
        assert result.scanned_by_type["Requirement"] == 1


def test_new_schema_node_type_without_policy_fails_the_coverage_gate() -> None:
    hypothetical_schema = (*NODE_TYPES, "NewDeterministicFact")

    with pytest.raises(RuntimeError) as caught:
        validate_stale_reconcile_ontology_coverage(hypothetical_schema)

    message = str(caught.value)
    assert "stale_reconcile_ontology_coverage_mismatch" in message
    assert "NewDeterministicFact" in message
    assert tuple(ALL_NODE_TYPES) == tuple(NODE_TYPES)
    assert set(STALE_RECONCILE_NODE_POLICY) == set(NODE_TYPES)


def test_wrong_cognitive_policy_fails_the_coverage_gate() -> None:
    wrong_policy = dict(STALE_RECONCILE_NODE_POLICY)
    wrong_policy["Learning"] = "demote_deterministic"

    with pytest.raises(RuntimeError, match="Learning"):
        validate_stale_reconcile_ontology_coverage(NODE_TYPES, wrong_policy)


def _queue_entry() -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id="card5-retry-entry",
        board_id="board-card5",
        artifact_type="spec",
        artifact_id="deleted",
        status="pending",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=None,
        claim_timeout_at=None,
        worker_id=None,
        claimed_by_session_id=None,
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        work_kind="stale_reconcile",
        generation=1,
        payload={
            "schema_version": 1,
            "delete_event_id": "delete-card5",
            "source_refs": ["spec:deleted"],
        },
        delete_event_id="delete-card5",
        claim_token=None,
    )


class _QueueStore:
    def __init__(self, entry: ConsolidationQueueRecord) -> None:
        self.entries = {entry.id: entry}
        self.ack_calls: list[dict[str, Any]] = []

    async def count_pending(self, _db: object) -> int:
        return sum(row.status == "pending" for row in self.entries.values())

    async def list_claimed_board_ids(self, _db: object) -> frozenset[str]:
        return frozenset(
            row.board_id for row in self.entries.values() if row.status == "claimed"
        )

    async def list_ready_pending(
        self,
        _db: object,
        *,
        now: datetime,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        del now
        return tuple(row for row in self.entries.values() if row.status == "pending")

    async def save_queue_entries(
        self,
        _db: object,
        entries: tuple[ConsolidationQueueRecord, ...] | list[ConsolidationQueueRecord],
    ) -> None:
        for entry in entries:
            self.entries[entry.id] = entry

    async def commit(self, _db: object) -> None:
        return None

    async def rollback(self, _db: object) -> None:
        return None

    async def get_queue_entry(
        self,
        _db: object,
        *,
        entry_id: str,
    ) -> ConsolidationQueueRecord | None:
        return self.entries.get(entry_id)

    async def queue_claim_is_current_and_unfenced(
        self,
        _db: object,
        **_identity: Any,
    ) -> bool:
        return True

    async def ack_claimed_queue_entry(
        self,
        _db: object,
        **identity: Any,
    ) -> bool:
        self.ack_calls.append(identity)
        self.entries.pop(str(identity["entry_id"]), None)
        return True


class _DeliveryStore:
    def __init__(self, queue: _QueueStore) -> None:
        self.queue = queue
        self.requests: list[Any] = []

    async def read_circuit_snapshot(
        self,
        _db: object,
        *,
        board_id: str,
    ) -> DeliveryCircuitSnapshot:
        del board_id
        return DeliveryCircuitSnapshot(degraded=False, reason="test_healthy")

    async def transfer_delivery_ownership(
        self,
        _db: object,
        request: Any,
    ) -> DeliveryTransferReceipt:
        self.requests.append(request)
        self.queue.entries.pop(request.entry_id, None)
        return DeliveryTransferReceipt(
            delivery_key=request.delivery_key,
            state=request.target_state,
            attempt=request.attempt,
            attempt_event_key=request.attempt_event_key,
        )


@asynccontextmanager
async def _relational_scope():
    yield object()


@contextmanager
def _registered_queue_store(store: _QueueStore):
    previous = get_consolidation_persistence_port()
    register_consolidation_persistence_port(store)
    try:
        yield
    finally:
        register_consolidation_persistence_port(previous)


@contextmanager
def _registered_delivery_store(store: _DeliveryStore):
    try:
        previous = get_delivery_ledger_port()
    except RuntimeError:
        previous = None
    register_delivery_ledger_port(store)
    try:
        yield
    finally:
        if previous is None:
            reset_delivery_ledger_port_for_tests()
        else:
            register_delivery_ledger_port(previous)


@pytest.mark.asyncio
async def test_failed_types_result_retries_without_ack_then_success_acknowledges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _queue_entry()
    store = _QueueStore(entry)
    delivery = _DeliveryStore(store)
    results = [
        StaleReconcileResult(
            board_id=entry.board_id,
            correlation_id="attempt-partial",
            incomplete=True,
            failed_types=["Requirement"],
        ),
        StaleReconcileResult(
            board_id=entry.board_id,
            correlation_id="attempt-complete",
            target_identity_count=1,
        ),
    ]
    observed_results: list[StaleReconcileResult] = []

    async def _process(*_args: object, **_kwargs: object) -> bool:
        result = results.pop(0)
        observed_results.append(result)
        return consolidation._stale_reconcile_is_complete(result)

    async def _mark_retryable(
        _db: object,
        failed_entry: ConsolidationQueueRecord,
        **_kwargs: object,
    ) -> None:
        failed_entry.attempts += 1
        failed_entry.status = "pending"
        failed_entry.claimed_at = None
        failed_entry.claim_timeout_at = None
        failed_entry.worker_id = None
        failed_entry.claimed_by_session_id = None
        failed_entry.claim_token = None

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _process)
    processor = ConsolidationProcessor(_relational_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_retryable)

    with _registered_queue_store(store), _registered_delivery_store(delivery):
        assert await processor.process_batch() == 0
        assert entry.id in store.entries
        assert entry.attempts == 1
        assert entry.status == "pending"
        assert store.ack_calls == []
        assert delivery.requests == []

        assert await processor.process_batch() == 1

    assert [item.failed_types for item in observed_results] == [
        ["Requirement"],
        [],
    ]
    assert store.ack_calls == []
    assert len(delivery.requests) == 1
    assert entry.id not in store.entries
