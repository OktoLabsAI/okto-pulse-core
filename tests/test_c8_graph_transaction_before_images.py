from __future__ import annotations

from collections.abc import Callable

import pytest

from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphTransactionScope,
    ProjectionActiveSetIntent,
    ProjectionActiveSetReconciliationError,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator


BOARD_ID = "board-c8-before-images"
NODE_ID = "decision-c8-before-images"
ROOT_ID = "refinement-c8-root"


def _seed_scope() -> tuple[
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
]:
    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID,
        "Entity",
        ROOT_ID,
        {
            "source_artifact_ref": "refinement:refinement-c8",
        },
    )
    store.create_node(
        BOARD_ID,
        "Decision",
        NODE_ID,
        {
            "content": "original content",
            "superseded_by": "",
            "superseded_at": "",
            "revocation_reason": "",
            "attestation_count": 3,
            "last_attested_at": "2026-07-27T12:00:00.000000",
            "source_artifact_ref": (
                "refinement:refinement-c8:rdl:ledger-c8:decision"
            ),
            "created_by_agent": "system:layer1_worker",
        },
    )
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        NODE_ID,
        ROOT_ID,
        {
            "confidence": 1.0,
            "layer": "deterministic",
            "rule_id": "belongs_to/relational_rdl_decision@v2.0",
            "created_by": "worker_layer1",
        },
        from_type="Decision",
        to_type="Entity",
    )
    return store, _InMemoryGraphTransactionScope(BOARD_ID, store)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_method", "invoke", "mutated_properties"),
    (
        (
            "update_node",
            lambda orchestrator: orchestrator.update_node(
                "Decision",
                NODE_ID,
                {"content": "temporary content"},
            ),
            {"content": "temporary content"},
        ),
        (
            "mark_superseded",
            lambda orchestrator: orchestrator.mark_superseded(
                "Decision",
                NODE_ID,
                superseded_by="replacement-decision",
                superseded_at="2026-07-28T12:00:00.000000",
                revocation_reason="superseded",
            ),
            {
                "superseded_by": "replacement-decision",
                "superseded_at": "2026-07-28T12:00:00.000000",
                "revocation_reason": "superseded",
            },
        ),
        (
            "increment_attestation",
            lambda orchestrator: orchestrator.increment_attestation(
                "Decision",
                NODE_ID,
                attested_at="2026-07-28T12:00:00.000000",
            ),
            {
                "attestation_count": 4,
                "last_attested_at": "2026-07-28T12:00:00.000000",
            },
        ),
    ),
    ids=("update", "supersede-mark", "attestation"),
)
async def test_every_in_place_mutation_restores_after_apply_then_raise(
    monkeypatch: pytest.MonkeyPatch,
    scope_method: str,
    invoke: Callable[[TransactionOrchestrator], None],
    mutated_properties: dict[str, object],
) -> None:
    store, scope = _seed_scope()
    assert isinstance(scope, GraphTransactionScope)
    before = dict(store._board_nodes(BOARD_ID)[NODE_ID])
    original = getattr(scope, scope_method)

    def _apply_then_raise(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError(f"injected_after_{scope_method}")

    monkeypatch.setattr(scope, scope_method, _apply_then_raise)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="current-session",
        board_id=BOARD_ID,
    )

    with pytest.raises(RuntimeError, match=f"injected_after_{scope_method}"):
        invoke(orchestrator)

    mutated = store._board_nodes(BOARD_ID)[NODE_ID]
    for name, expected in mutated_properties.items():
        assert mutated[name] == expected
    assert len(orchestrator.records) == 1
    assert orchestrator.records[0].property_before_image is not None

    await orchestrator.compensate()

    restored = store._board_nodes(BOARD_ID)[NODE_ID]
    for name in mutated_properties:
        assert restored[name] == before[name]


class _ProjectionApplyThenRaiseScope(_InMemoryGraphTransactionScope):
    def reconcile_projection_active_set(self, intent):
        receipt = super().reconcile_projection_active_set(intent)
        raise ProjectionActiveSetReconciliationError(
            "injected_projection_after_apply",
            "injected failure after the active-set mutation was applied",
            receipt=receipt,
        )


@pytest.mark.asyncio
async def test_projection_receipt_is_retained_for_apply_then_raise_compensation(
) -> None:
    store, _scope = _seed_scope()
    scope = _ProjectionApplyThenRaiseScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="current-session",
        board_id=BOARD_ID,
    )

    with pytest.raises(ProjectionActiveSetReconciliationError) as excinfo:
        orchestrator.reconcile_projection_active_set(
            ProjectionActiveSetIntent(
                owner_type="refinement",
                owner_id="refinement-c8",
                namespace="rdl",
                owner_node_id=ROOT_ID,
                active_nodes=(),
            )
        )

    assert excinfo.value.code == "injected_projection_after_apply"
    assert (
        store._board_nodes(BOARD_ID)[NODE_ID]["revocation_reason"]
        == "source_projection_removed"
    )
    assert len(orchestrator.records) == 1
    assert orchestrator.records[0].projection_receipt is excinfo.value.receipt

    await orchestrator.compensate()

    assert store._board_nodes(BOARD_ID)[NODE_ID]["revocation_reason"] == ""


@pytest.mark.parametrize(
    ("owner_type", "namespace"),
    (
        ("spec", "rdl"),
        ("refinement", "quality"),
    ),
)
def test_projection_active_set_rejects_any_other_protocol_scope(
    owner_type: str,
    namespace: str,
) -> None:
    _store, scope = _seed_scope()

    with pytest.raises(ProjectionActiveSetReconciliationError) as excinfo:
        scope.reconcile_projection_active_set(
            ProjectionActiveSetIntent(
                owner_type=owner_type,
                owner_id="refinement-c8",
                namespace=namespace,
                active_nodes=(),
            )
        )

    assert excinfo.value.code == "projection_active_set_scope_invalid"
