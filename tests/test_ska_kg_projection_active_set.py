from __future__ import annotations

import pytest

from okto_pulse.core.kg.interfaces.graph_transaction import (
    ProjectionActiveSetIntent,
    ProjectionNodeRef,
    SOURCE_PROJECTION_REMOVED_REASON,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.relational_projection import (
    parse_relational_projection_ref,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator


BOARD_ID = "board-ska-projection"
REFINEMENT_ID = "refinement-a"
LEDGER_ID = "ledger-a"
ROOT_ID = "refinement-root"
DECISION_ID = "rdl-decision"
ALTERNATIVE_ID = "rdl-alternative"
DECISION_REF = (
    f"refinement:{REFINEMENT_ID}:rdl:{LEDGER_ID}:decision"
)
ALTERNATIVE_REF = (
    f"refinement:{REFINEMENT_ID}:rdl:{LEDGER_ID}:alternative:"
    + "a" * 64
)


def _node_attrs(source_ref: str, *, content: str) -> dict[str, object]:
    return {
        "source_artifact_ref": source_ref,
        "created_by_agent": "system:layer1_worker",
        "content": content,
        "embedding": [0.1, 0.2],
        "revocation_reason": "",
    }


def _edge_attrs() -> dict[str, object]:
    return {
        "confidence": 1.0,
        "created_by_session_id": "prior-session",
        "layer": "deterministic",
        "rule_id": "belongs_to/relational_rdl_decision@v2.0",
        "created_by": "worker_layer1",
        "fallback_reason": "",
    }


def _seed() -> tuple[InMemoryGraphStore, _InMemoryGraphTransactionScope]:
    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID,
        "Entity",
        ROOT_ID,
        {"source_artifact_ref": f"refinement:{REFINEMENT_ID}"},
    )
    store.create_node(
        BOARD_ID,
        "Decision",
        DECISION_ID,
        _node_attrs(DECISION_REF, content="keep decision"),
    )
    store.create_node(
        BOARD_ID,
        "Alternative",
        ALTERNATIVE_ID,
        _node_attrs(ALTERNATIVE_REF, content="keep alternative"),
    )
    for node_type, node_id, rule_id in (
        (
            "Decision",
            DECISION_ID,
            "belongs_to/relational_rdl_decision@v2.0",
        ),
        (
            "Alternative",
            ALTERNATIVE_ID,
            "belongs_to/relational_rdl_alternative@v2.0",
        ),
    ):
        edge_attrs = _edge_attrs()
        edge_attrs["rule_id"] = rule_id
        store.create_edge(
            BOARD_ID,
            "belongs_to",
            node_id,
            ROOT_ID,
            edge_attrs,
            from_type=node_type,
            to_type="Entity",
        )
    return store, _InMemoryGraphTransactionScope(BOARD_ID, store)


def test_projection_reference_parser_is_closed_not_prefix_based() -> None:
    parsed = parse_relational_projection_ref(DECISION_REF)
    assert parsed is not None
    assert parsed.owner_id == REFINEMENT_ID
    assert parsed.node_type == "Decision"
    assert parse_relational_projection_ref(f"{DECISION_REF}:suffix") is None
    assert parse_relational_projection_ref(
        f"refinement:{REFINEMENT_ID}-collision:rdl:{LEDGER_ID}:decision"
    ).owner_id == f"{REFINEMENT_ID}-collision"
    assert parse_relational_projection_ref(
        f"refinement:{REFINEMENT_ID}:rdl:{LEDGER_ID}:alternative:not-a-digest"
    ) is None


def test_projection_active_set_tombstones_only_stale_children() -> None:
    store, scope = _seed()
    receipt = scope.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="refinement",
            owner_id=REFINEMENT_ID,
            namespace="rdl",
            owner_node_id=ROOT_ID,
            active_nodes=(
                ProjectionNodeRef(
                    node_type="Decision",
                    node_id=DECISION_ID,
                    source_artifact_ref=DECISION_REF,
                ),
            ),
        )
    )

    decision = store._board_nodes(BOARD_ID)[DECISION_ID]
    alternative = store._board_nodes(BOARD_ID)[ALTERNATIVE_ID]
    assert decision["revocation_reason"] == ""
    assert alternative["revocation_reason"] == SOURCE_PROJECTION_REMOVED_REASON
    assert alternative["content"] == "keep alternative"
    assert alternative["embedding"] == [0.1, 0.2]
    assert all(
        edge.get("_from") != ALTERNATIVE_ID
        and edge.get("_to") != ALTERNATIVE_ID
        for edge in store._board_edges(BOARD_ID)
    )
    assert len(receipt.before_images) == 1


def test_projection_compensation_restores_node_and_edges_exactly() -> None:
    store, scope = _seed()
    original_edges = list(store._board_edges(BOARD_ID))
    receipt = scope.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="refinement",
            owner_id=REFINEMENT_ID,
            namespace="rdl",
            owner_node_id=ROOT_ID,
            active_nodes=(),
        )
    )
    scope.compensate_projection_active_set(receipt)

    assert store._board_nodes(BOARD_ID)[DECISION_ID]["revocation_reason"] == ""
    assert store._board_nodes(BOARD_ID)[ALTERNATIVE_ID]["revocation_reason"] == ""
    assert store._board_edges(BOARD_ID) == original_edges


def test_projection_reactivation_only_clears_projection_reason() -> None:
    store, scope = _seed()
    nodes = store._board_nodes(BOARD_ID)
    nodes[DECISION_ID]["revocation_reason"] = SOURCE_PROJECTION_REMOVED_REASON
    nodes[ALTERNATIVE_ID]["revocation_reason"] = "source_deleted"
    store._edges[BOARD_ID] = [
        edge
        for edge in store._board_edges(BOARD_ID)
        if edge.get("_from") != DECISION_ID
    ]

    scope.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="refinement",
            owner_id=REFINEMENT_ID,
            namespace="rdl",
            owner_node_id=ROOT_ID,
            active_nodes=(
                ProjectionNodeRef("Decision", DECISION_ID, DECISION_REF),
                ProjectionNodeRef(
                    "Alternative",
                    ALTERNATIVE_ID,
                    ALTERNATIVE_REF,
                ),
            ),
        )
    )

    assert nodes[DECISION_ID]["revocation_reason"] == ""
    assert nodes[ALTERNATIVE_ID]["revocation_reason"] == "source_deleted"


def test_projection_scope_requires_exact_owner_root_and_rule() -> None:
    store, scope = _seed()
    wrong_root_id = "wrong-refinement-root"
    store.create_node(
        BOARD_ID,
        "Entity",
        wrong_root_id,
        {"source_artifact_ref": "refinement:other-refinement"},
    )
    wrong_root_node_id = "rdl-wrong-root"
    wrong_rule_node_id = "rdl-wrong-rule"
    wrong_root_ref = (
        f"refinement:{REFINEMENT_ID}:rdl:ledger-wrong-root:decision"
    )
    wrong_rule_ref = (
        f"refinement:{REFINEMENT_ID}:rdl:ledger-wrong-rule:alternative:"
        + "b" * 64
    )
    store.create_node(
        BOARD_ID,
        "Decision",
        wrong_root_node_id,
        _node_attrs(wrong_root_ref, content="wrong owner root"),
    )
    store.create_node(
        BOARD_ID,
        "Alternative",
        wrong_rule_node_id,
        _node_attrs(wrong_rule_ref, content="wrong ownership rule"),
    )
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        wrong_root_node_id,
        wrong_root_id,
        {
            **_edge_attrs(),
            "rule_id": "belongs_to/relational_rdl_decision@v2.0",
        },
        from_type="Decision",
        to_type="Entity",
    )
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        wrong_rule_node_id,
        ROOT_ID,
        {
            **_edge_attrs(),
            "rule_id": "belongs_to/refinement_rdl@1.0",
        },
        from_type="Alternative",
        to_type="Entity",
    )

    scope.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="refinement",
            owner_id=REFINEMENT_ID,
            namespace="rdl",
            owner_node_id=ROOT_ID,
            active_nodes=(),
        )
    )

    nodes = store._board_nodes(BOARD_ID)
    assert (
        nodes[DECISION_ID]["revocation_reason"]
        == SOURCE_PROJECTION_REMOVED_REASON
    )
    assert (
        nodes[ALTERNATIVE_ID]["revocation_reason"]
        == SOURCE_PROJECTION_REMOVED_REASON
    )
    assert nodes[wrong_root_node_id]["revocation_reason"] == ""
    assert nodes[wrong_rule_node_id]["revocation_reason"] == ""


class _ApplyThenRaiseScope(_InMemoryGraphTransactionScope):
    def update_node(self, node_type, node_id, attrs):  # noqa: ANN001, ANN201
        super().update_node(node_type, node_id, attrs)
        raise RuntimeError("driver_failed_after_apply")


@pytest.mark.asyncio
async def test_property_before_image_compensates_apply_then_raise() -> None:
    store, _scope = _seed()
    scope = _ApplyThenRaiseScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="current-session",
        board_id=BOARD_ID,
    )

    with pytest.raises(RuntimeError, match="driver_failed_after_apply"):
        orchestrator.update_node(
            "Decision",
            DECISION_ID,
            {"content": "temporary"},
        )
    assert store._board_nodes(BOARD_ID)[DECISION_ID]["content"] == "temporary"

    await orchestrator.compensate()

    assert store._board_nodes(BOARD_ID)[DECISION_ID]["content"] == "keep decision"
