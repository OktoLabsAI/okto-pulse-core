"""Generic session cleanup must not delete what projection compensation just restored.

Compensation puts a projection's edges back from their before-image, and those edges belong to
the very session being compensated.  The generic sweep that follows deletes everything the
session created, so without being told otherwise it undoes the restore it just watched succeed
-- and the failure is invisible from inside compensation, which reports success.
"""

from __future__ import annotations

from typing import Any

import pytest

from okto_pulse.core.kg.interfaces.graph_transaction import (
    ProjectionActiveSetIntent,
    ProjectionActiveSetReconciliationError,
    ProjectionNodeRef,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator

BOARD_ID = "board-projection-cleanup"
SESSION_ID = "session-under-compensation"
REFINEMENT_ID = "refinement-a"
LEDGER_ID = "ledger-a"
ROOT_ID = "refinement-root"
KEEP_ID = "rdl-decision-keep"
STALE_ID = "rdl-decision-stale"
UNRELATED_ID = "unrelated-entity"
LATER_ID = "rdl-decision-later"
SPEC_ROOT_ID = "spec-dependency-root"
PREREQ_ID = "spec-prerequisite"
DEPENDENCY_RULE = "precedes/spec_dependency/rule-a"
OTHER_DEPENDENCY_RULE = "precedes/spec_dependency/rule-b"

DECISION_RULE = "belongs_to/relational_rdl_decision@v2.0"
KEEP_REF = f"refinement:{REFINEMENT_ID}:rdl:{LEDGER_ID}:decision"
STALE_REF = f"refinement:{REFINEMENT_ID}:rdl:ledger-stale:decision"
_CLEANUP_MUST_NOT_RUN = "cleanup must not run when preservation is required"


def _node_attrs(source_ref: str) -> dict[str, Any]:
    return {
        "source_artifact_ref": source_ref,
        "created_by_agent": "system:layer1_worker",
        "content": f"payload of {source_ref}",
        "embedding": [0.1, 0.2],
        "revocation_reason": "",
    }


def _edge_attrs(rule_id: str, session_id: str) -> dict[str, Any]:
    return {
        "confidence": 1.0,
        "created_by_session_id": session_id,
        "layer": "deterministic",
        "rule_id": rule_id,
        "created_by": "worker_layer1",
        "fallback_reason": "",
    }


def _seed() -> tuple[InMemoryGraphStore, _InMemoryGraphTransactionScope]:
    """A board whose projection edges belong to the session that will be compensated."""

    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID,
        "Entity",
        ROOT_ID,
        {"source_artifact_ref": f"refinement:{REFINEMENT_ID}"},
    )
    store.create_node(BOARD_ID, "Entity", UNRELATED_ID, {"source_artifact_ref": ""})
    for node_id, source_ref in ((KEEP_ID, KEEP_REF), (STALE_ID, STALE_REF)):
        store.create_node(BOARD_ID, "Decision", node_id, _node_attrs(source_ref))
        store.create_edge(
            BOARD_ID,
            "belongs_to",
            node_id,
            ROOT_ID,
            _edge_attrs(DECISION_RULE, SESSION_ID),
            from_type="Decision",
            to_type="Entity",
        )
    # Written by the same session and owned by nothing in the projection: the sweep must
    # still take this one, or "preserved" would just mean "cleanup stopped working".
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        UNRELATED_ID,
        ROOT_ID,
        _edge_attrs("belongs_to/unrelated@1.0", SESSION_ID),
        from_type="Entity",
        to_type="Entity",
    )
    return store, _InMemoryGraphTransactionScope(BOARD_ID, store)


def _intent() -> ProjectionActiveSetIntent:
    return ProjectionActiveSetIntent(
        owner_type="refinement",
        owner_id=REFINEMENT_ID,
        namespace="rdl",
        owner_node_id=ROOT_ID,
        active_nodes=(ProjectionNodeRef("Decision", KEEP_ID, KEEP_REF),),
    )


def _edges(store: InMemoryGraphStore) -> list[tuple[str, str, str]]:
    return sorted(
        (
            str(edge.get("_from") or ""),
            str(edge.get("_to") or ""),
            str(edge.get("rule_id") or ""),
        )
        for edge in store._board_edges(BOARD_ID)
    )


@pytest.mark.asyncio
async def test_a_restored_projection_edge_survives_the_session_sweep() -> None:
    """The edge compensation put back belongs to this session, and must still be there."""

    store, scope = _seed()
    before = _edges(store)
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in before

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(_intent())
    # The reconciliation really did remove it, so the restore below is a real restore.
    assert (STALE_ID, ROOT_ID, DECISION_RULE) not in _edges(store)

    await orchestrator.compensate()

    assert (STALE_ID, ROOT_ID, DECISION_RULE) in _edges(store), (
        "the sweep deleted the edge compensation had just restored"
    )
    # Cleanup is still cleanup: an edge this session wrote and no projection owns is gone.
    assert (UNRELATED_ID, ROOT_ID, "belongs_to/unrelated@1.0") not in _edges(store)


@pytest.mark.asyncio
async def test_compensation_runs_before_the_generic_sweep() -> None:
    """Order is the whole mechanism: sweeping first would delete before restoring."""

    _store, scope = _seed()
    operations: list[str] = []
    original_compensate = scope.compensate_projection_active_set
    original_preserving = scope.delete_edges_by_session_preserving_spec_lineage
    original_plain = scope.delete_edges_by_session

    def _record_compensate(receipt: Any) -> None:
        operations.append("compensate")
        original_compensate(receipt)

    def _record_preserving(
        session_id: str,
        preserved_edges: Any,
        *,
        preserved_projection_edges: Any = (),
    ) -> None:
        # Declared explicitly, because **kwargs no longer counts as the capability: it would
        # swallow the argument and report nothing.
        operations.append("sweep_preserving")
        original_preserving(
            session_id,
            preserved_edges,
            preserved_projection_edges=preserved_projection_edges,
        )

    def _record_plain(session_id: str) -> None:
        operations.append("sweep_plain")
        original_plain(session_id)

    scope.compensate_projection_active_set = _record_compensate  # type: ignore[method-assign]
    scope.delete_edges_by_session_preserving_spec_lineage = _record_preserving  # type: ignore[method-assign]
    scope.delete_edges_by_session = _record_plain  # type: ignore[method-assign]

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(_intent())
    await orchestrator.compensate()

    assert operations[0] == "compensate", operations
    # And the sweep that ran is the one that can be told what to keep.
    assert "sweep_preserving" in operations, operations
    assert "sweep_plain" not in operations, operations


@pytest.mark.asyncio
async def test_projection_edges_are_preserved_with_no_spec_lineage_at_all() -> None:
    """The preservation path must not be reachable only as a passenger of Spec lineage."""

    store, scope = _seed()
    received: dict[str, Any] = {}
    original_preserving = scope.delete_edges_by_session_preserving_spec_lineage

    def _capture(
        session_id: str,
        preserved_edges: Any,
        *,
        preserved_projection_edges: Any = (),
    ) -> None:
        received["preserved_edges"] = preserved_edges
        received["preserved_projection_edges"] = preserved_projection_edges
        original_preserving(
            session_id,
            preserved_edges,
            preserved_projection_edges=preserved_projection_edges,
        )

    scope.delete_edges_by_session_preserving_spec_lineage = _capture  # type: ignore[method-assign]

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(_intent())
    await orchestrator.compensate()

    assert received["preserved_edges"] == ()
    preserved = received["preserved_projection_edges"]
    assert preserved, "no projection edge was offered to cleanup"
    assert any(
        edge.from_id == STALE_ID and edge.to_id == ROOT_ID for edge in preserved
    ), preserved
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in _edges(store)


@pytest.mark.asyncio
async def test_two_reconciliations_preserve_only_the_state_the_session_started_from() -> (
    None
):
    """Compensation walks A <- B <- C, so only A may be preserved -- B and C are the way back.

    Each receipt records the state its own reconciliation replaced.  Aggregating them tells
    the sweep to keep intermediate states too, and an edge that exists only in an
    intermediate state is a session effect that has to go: nothing older vouches for it, and
    compensation has already undone the state that contained it.
    """

    store, scope = _seed()
    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )

    # A -> B: the seeded stale member leaves the active set.
    orchestrator.reconcile_projection_active_set(_intent())

    # B -> C: the session materializes a NEW projection member and its edge, then a second
    # reconciliation drops it again.  This member exists in no state older than B.
    later_ref = f"refinement:{REFINEMENT_ID}:rdl:ledger-later:decision"
    orchestrator.create_node("Decision", LATER_ID, _node_attrs(later_ref))
    orchestrator.create_edge(
        "belongs_to",
        LATER_ID,
        ROOT_ID,
        attrs=_edge_attrs(DECISION_RULE, SESSION_ID),
        from_type="Decision",
        to_type="Entity",
    )
    orchestrator.reconcile_projection_active_set(_intent())
    assert (LATER_ID, ROOT_ID, DECISION_RULE) not in _edges(store)

    await orchestrator.compensate()

    final = _edges(store)
    # A is restored: the edge that existed before the session touched anything is back.
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in final, final
    # C, the intermediate the session invented, is not preserved past its own compensation.
    assert (LATER_ID, ROOT_ID, DECISION_RULE) not in final, final
    # And the sweep still did its ordinary work.
    assert (UNRELATED_ID, ROOT_ID, "belongs_to/unrelated@1.0") not in final, final


@pytest.mark.asyncio
async def test_a_scope_that_cannot_preserve_fails_typed_before_deleting_anything() -> (
    None
):
    """A pre-contract implementation must be refused, not discovered by losing the edges."""

    store, scope = _seed()

    def _legacy_preserving(session_id: str, preserved_edges: Any) -> None:
        raise AssertionError(_CLEANUP_MUST_NOT_RUN)

    scope.delete_edges_by_session_preserving_spec_lineage = _legacy_preserving  # type: ignore[method-assign]

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(_intent())
    restored_state = None

    with pytest.raises(Exception) as raised:
        await orchestrator.compensate()

    cause = raised.value
    while cause is not None and not isinstance(
        cause, ProjectionActiveSetReconciliationError
    ):
        cause = cause.__cause__
    assert isinstance(cause, ProjectionActiveSetReconciliationError), raised.value
    assert cause.code == "projection_active_set_compensation_capability_unavailable"

    # Refused before the sweep: the restore that already happened is still on the board.
    restored_state = _edges(store)
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in restored_state
    assert (UNRELATED_ID, ROOT_ID, "belongs_to/unrelated@1.0") in restored_state


@pytest.mark.asyncio
async def test_a_session_without_projections_makes_the_call_it_always_made() -> None:
    """Backward compatibility is a claim about the call shape, so it is asserted here."""

    _store, scope = _seed()
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    original_plain = scope.delete_edges_by_session

    def _record_plain(session_id: str) -> None:
        calls.append(((session_id,), {}))
        original_plain(session_id)

    scope.delete_edges_by_session = _record_plain  # type: ignore[method-assign]

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    # A plain node write: it records something to compensate without producing a lineage or
    # projection receipt, which is exactly the session shape this call path is claimed for.
    orchestrator.create_node(
        "Decision",
        "rdl-decision-plain",
        _node_attrs(f"refinement:{REFINEMENT_ID}:rdl:ledger-plain:decision"),
    )
    assert orchestrator.records
    await orchestrator.compensate()

    # Nothing to preserve: the plain sweep is used, exactly as before this contract existed.
    assert calls == [((SESSION_ID,), {})]


@pytest.mark.asyncio
async def test_two_removals_of_different_members_both_come_back() -> None:
    """A+X -> X -> nothing must end at A+X: every before-image vouches for its own member.

    Preserving only the oldest receipt loses X here, because the receipt that vouches for X
    is the newer one and the older one never mentions it.  Reverse compensation restores
    both, so preservation has to be the union, not a pick.
    """

    store, scope = _seed()
    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )

    keep_only = ProjectionActiveSetIntent(
        owner_type="refinement",
        owner_id=REFINEMENT_ID,
        namespace="rdl",
        owner_node_id=ROOT_ID,
        active_nodes=(ProjectionNodeRef("Decision", KEEP_ID, KEEP_REF),),
    )
    nothing_active = ProjectionActiveSetIntent(
        owner_type="refinement",
        owner_id=REFINEMENT_ID,
        namespace="rdl",
        owner_node_id=ROOT_ID,
        active_nodes=(),
    )

    orchestrator.reconcile_projection_active_set(keep_only)
    orchestrator.reconcile_projection_active_set(nothing_active)
    mid = _edges(store)
    assert (STALE_ID, ROOT_ID, DECISION_RULE) not in mid
    assert (KEEP_ID, ROOT_ID, DECISION_RULE) not in mid

    await orchestrator.compensate()

    final = _edges(store)
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in final, final
    assert (KEEP_ID, ROOT_ID, DECISION_RULE) in final, final


@pytest.mark.asyncio
async def test_a_provider_that_only_swallows_kwargs_is_never_called() -> None:
    """``**kwargs`` is not consent: it would accept the argument and ignore it silently."""

    store, scope = _seed()
    swept: list[str] = []

    def _kwargs_swallowing_sweep(
        session_id: str, preserved_edges: Any, **kwargs: Any
    ) -> None:
        # A pre-contract implementation: it accepts anything and preserves nothing.
        swept.append(session_id)
        scope.delete_edges_by_session(session_id)

    scope.delete_edges_by_session_preserving_spec_lineage = _kwargs_swallowing_sweep  # type: ignore[method-assign]

    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(_intent())

    with pytest.raises(Exception) as raised:
        await orchestrator.compensate()

    cause = raised.value
    while cause is not None and not isinstance(
        cause, ProjectionActiveSetReconciliationError
    ):
        cause = cause.__cause__
    assert isinstance(cause, ProjectionActiveSetReconciliationError), raised.value
    assert cause.code == "projection_active_set_compensation_capability_unavailable"

    assert swept == [], "the swallowing sweep was called and the restore was lost"
    assert (STALE_ID, ROOT_ID, DECISION_RULE) in _edges(store)


@pytest.mark.asyncio
async def test_a_before_image_does_not_shelter_a_second_edge_of_the_same_identity() -> (
    None
):
    """One vouched copy keeps one copy, and only the payload that was vouched for.

    The Spec dependency route is used deliberately: its receipt carries edge before-images
    with no node attached, so compensation does not clear incident edges first.  That leaves
    the preservation key as the only thing deciding, which is what this is about -- through
    the node route an incident sweep would remove the divergent edge for its own reasons and
    the test would pass without the key ever being consulted.
    """

    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID, "Entity", SPEC_ROOT_ID, {"source_artifact_ref": "spec:root"}
    )
    store.create_node(
        BOARD_ID, "Entity", PREREQ_ID, {"source_artifact_ref": "spec:prereq"}
    )
    original = _edge_attrs(DEPENDENCY_RULE, SESSION_ID)
    store.create_edge(
        BOARD_ID,
        "precedes",
        PREREQ_ID,
        SPEC_ROOT_ID,
        original,
        from_type="Entity",
        to_type="Entity",
    )
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="spec",
            owner_id=REFINEMENT_ID,
            namespace="dependencies",
            owner_node_id=SPEC_ROOT_ID,
            active_edges=(),
        )
    )

    # Something writes a DIFFERENT relationship sharing the removed one's identity.  The
    # before-image describes the original payload and vouches for nothing else.
    divergent = _edge_attrs(DEPENDENCY_RULE, SESSION_ID)
    divergent["confidence"] = 0.11
    store.create_edge(
        BOARD_ID,
        "precedes",
        PREREQ_ID,
        SPEC_ROOT_ID,
        divergent,
        from_type="Entity",
        to_type="Entity",
    )

    await orchestrator.compensate()

    surviving = [
        edge
        for edge in store._board_edges(BOARD_ID)
        if str(edge.get("_type") or "") == "precedes"
    ]
    assert len(surviving) == 1, surviving
    assert surviving[0]["confidence"] == 1.0, surviving[0]


@pytest.mark.asyncio
async def test_a_new_dependency_rule_does_not_cancel_the_old_one_it_shares_endpoints_with() -> (
    None
):
    """For Spec dependencies the rule is part of the name, so netting must carry it.

    One prerequisite may precede one owner under more than one rule.  Netting by endpoints
    alone would let the rule the session just wrote cancel the rule the before-image vouches
    for, and the sweep would then delete exactly the edge compensation restored.
    """

    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID, "Entity", SPEC_ROOT_ID, {"source_artifact_ref": "spec:root"}
    )
    store.create_node(
        BOARD_ID, "Entity", PREREQ_ID, {"source_artifact_ref": "spec:prereq"}
    )
    store.create_edge(
        BOARD_ID,
        "precedes",
        PREREQ_ID,
        SPEC_ROOT_ID,
        _edge_attrs(DEPENDENCY_RULE, SESSION_ID),
        from_type="Entity",
        to_type="Entity",
    )
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id=SESSION_ID,
        board_id=BOARD_ID,
    )
    orchestrator.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="spec",
            owner_id=REFINEMENT_ID,
            namespace="dependencies",
            owner_node_id=SPEC_ROOT_ID,
            active_edges=(),
        )
    )

    # The session writes a DIFFERENT dependency over the same pair.
    orchestrator.create_edge(
        "precedes",
        PREREQ_ID,
        SPEC_ROOT_ID,
        attrs=_edge_attrs(OTHER_DEPENDENCY_RULE, SESSION_ID),
        from_type="Entity",
        to_type="Entity",
    )

    await orchestrator.compensate()

    rules = sorted(
        str(edge.get("rule_id") or "")
        for edge in store._board_edges(BOARD_ID)
        if str(edge.get("_type") or "") == "precedes"
    )
    # OLD survives because its own before-image vouches for it; NEW goes because the session
    # wrote it and nothing older vouches for it.
    assert rules == [DEPENDENCY_RULE], rules
