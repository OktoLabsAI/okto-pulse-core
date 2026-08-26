from __future__ import annotations

from collections.abc import Callable

import pytest

from okto_pulse.core.kg.interfaces.graph_errors import GraphCapabilityUnavailable
from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphTransactionScope,
    ProjectionActiveSetIntent,
    ProjectionActiveSetReconciliationError,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.transaction import (
    TransactionOrchestrator,
    _StoreBackedGraphScope,
)


BOARD_ID = "board-c8-before-images"
NODE_ID = "decision-c8-before-images"
ROOT_ID = "refinement-c8-root"
IDENTICAL_WEIGHT = 0.9
"""The weight the two indistinguishable parallel edges share."""


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


def _incident_multiset(store: InMemoryGraphStore, node_id: str) -> tuple[str, ...]:
    """Return a canonical, order-independent rendering of every edge touching one node.

    A multiset rather than a set, and a rendering rather than the dicts, because the claim is
    that NOTHING about the incident edges moved: two parallel edges that differ only in a
    property are two edges, and a list comparison would also fail on a reordering that changed
    nothing. Sorting the renderings separates "the same edges" from "the same order".
    """
    return tuple(
        sorted(
            repr(sorted(edge.items(), key=lambda item: str(item[0])))
            for edge in store._board_edges(BOARD_ID)
            if edge.get("_from") == node_id or edge.get("_to") == node_id
        )
    )


def _seed_incident_shapes(store: InMemoryGraphStore) -> None:
    """Give the decision every incident shape the contract promises to preserve."""
    store.create_node(BOARD_ID, "Entity", "entity-parallel", {"kind": "parallel"})
    # Incoming, so direction is observable rather than assumed. Every shape here is one the
    # schema contract actually accepts; inventing a pair it forbids would test the seed, not the
    # replacement.
    store.create_edge(
        BOARD_ID,
        "supports",
        ROOT_ID,
        NODE_ID,
        {"weight": 0.5},
        from_type="Entity",
        to_type="Decision",
    )
    # Two PARALLEL edges: same type and endpoints, different properties.
    for weight in (0.25, 0.75):
        store.create_edge(
            BOARD_ID,
            "relates_to",
            NODE_ID,
            "entity-parallel",
            {"weight": weight},
            from_type="Decision",
            to_type="Entity",
        )
    # A self-loop, incident on both sides of the same node and counted once.
    store.create_edge(
        BOARD_ID,
        "supersedes",
        NODE_ID,
        NODE_ID,
        {"note": "self"},
        from_type="Decision",
        to_type="Decision",
    )
    # And two BYTE-IDENTICAL parallels, which is what makes "multiset" a claim rather than a
    # word: a set-valued fingerprint collapses these two into one and stops noticing when one
    # of them is lost.
    for _ in range(2):
        store.create_edge(
            BOARD_ID,
            "relates_to",
            NODE_ID,
            "entity-parallel",
            {"weight": IDENTICAL_WEIGHT},
            from_type="Decision",
            to_type="Entity",
        )


def test_memory_payload_replacement_is_exact_and_preserves_edges() -> None:
    store, scope = _seed_scope()
    _seed_incident_shapes(store)
    before_incident = _incident_multiset(store, NODE_ID)
    before_all = [dict(edge) for edge in store._board_edges(BOARD_ID)]

    # The premise, asserted: the shapes the claim is about are actually present. Seven incident
    # edges -- one outgoing, one incoming, two parallel that differ, one self-loop, and two that
    # are byte-identical -- so a replacement that dropped direction, collapsed the parallels or
    # lost the loop could not pass silently.
    assert len(before_incident) == 7
    assert sum("'relates_to'" in edge for edge in before_incident) == 4
    assert len(set(before_incident)) == 6  # exactly one pair is indistinguishable

    assert scope.replace_node_payload(
        "Decision",
        NODE_ID,
        {
            "content": "literal durable replacement",
            "human_curated": False,
            "embedding": [0.25, 0.5],
        },
        source_session_id="durable-replay-session",
    )
    assert store._board_nodes(BOARD_ID)[NODE_ID] == {
        "id": NODE_ID,
        "_type": "Decision",
        "content": "literal durable replacement",
        "human_curated": False,
        "embedding": [0.25, 0.5],
        "source_session_id": "durable-replay-session",
    }
    assert _incident_multiset(store, NODE_ID) == before_incident
    assert store._board_edges(BOARD_ID) == before_all
    assert not scope.replace_node_payload(
        "Decision",
        "absent-decision",
        {},
        source_session_id="durable-replay-session",
    )


class _EdgeDamagingScope(_InMemoryGraphTransactionScope):
    """A scope whose publication step also loses ONE of two indistinguishable edges.

    Losing an edge nothing else looks like would be caught by any comparison at all. Losing one
    of an identical pair is caught only by a comparison that counts, which is the difference
    between a multiset and a set and the reason the fingerprint is one.
    """

    def _publish_node_replacement(self, nodes, node_id, replacement):  # type: ignore[no-untyped-def]
        super()._publish_node_replacement(nodes, node_id, replacement)
        edges = self.store._board_edges(self.board_id)
        for position, edge in enumerate(edges):
            if edge.get("weight") == IDENTICAL_WEIGHT and (
                edge.get("_from") == node_id or edge.get("_to") == node_id
            ):
                del edges[position]
                break


def test_memory_replacement_confirms_the_incident_multiset_itself() -> None:
    """The refusal comes from the IMPLEMENTATION, not from a test looking on afterwards.

    The contract asks every implementation to confirm the payload and the incident multiset
    before reporting success. "This code does not touch the edges" is an argument about the code
    as it stands, and it stops being true the moment someone adds a second way to publish -- so
    the confirmation has to be a step that runs, which is what this proves by making the
    publication itself lose an edge.
    """
    store, _scope = _seed_scope()
    _seed_incident_shapes(store)
    scope = _EdgeDamagingScope(BOARD_ID, store)

    with pytest.raises(RuntimeError, match="graph_node_payload_replacement_edges_unconfirmed"):
        scope.replace_node_payload(
            "Decision",
            NODE_ID,
            {"content": "replacement that damages an edge"},
            source_session_id="durable-replay-session",
        )


class _ApplyThenRaisePublisherScope(_InMemoryGraphTransactionScope):
    """A scope whose publication step damages the board and then fails outright.

    The confirmations never run for this one. That is the point: a repair attached to the checks
    would be attached to code this failure skips, and the board would keep the damage while the
    caller saw an ordinary error.
    """

    def _publish_node_replacement(self, nodes, node_id, replacement):  # type: ignore[no-untyped-def]
        super()._publish_node_replacement(nodes, node_id, replacement)
        edges = self.store._board_edges(self.board_id)
        for position, edge in enumerate(edges):
            if edge.get("_from") == node_id or edge.get("_to") == node_id:
                del edges[position]
                break
        raise RuntimeError("publisher failed after applying")


def test_a_publisher_that_applies_then_raises_leaves_nothing_behind() -> None:
    """The caller's error survives, and so does the board it was told nothing happened to."""
    store, _scope = _seed_scope()
    _seed_incident_shapes(store)
    scope = _ApplyThenRaisePublisherScope(BOARD_ID, store)
    before_nodes = {key: dict(value) for key, value in store._board_nodes(BOARD_ID).items()}
    before_edges = [dict(edge) for edge in store._board_edges(BOARD_ID)]

    # The ORIGINAL failure reaches the caller: the repair is not allowed to replace the reason.
    with pytest.raises(RuntimeError, match="publisher failed after applying"):
        scope.replace_node_payload(
            "Decision",
            NODE_ID,
            {"content": "replacement whose publisher fails"},
            source_session_id="durable-replay-session",
        )

    assert store._board_nodes(BOARD_ID) == before_nodes
    assert store._board_edges(BOARD_ID) == before_edges


def test_a_refused_replacement_leaves_the_board_exactly_as_it_found_it() -> None:
    """Detecting damage is half the job; the other half is not leaving it behind.

    The refusal tells the caller the replacement did not happen, so the board has to agree with
    that sentence. Nothing above this can put it back -- the scope has no undo for a
    half-applied replacement -- which is why the whole state is compared, node payload and edge
    list together, rather than only the part the damage touched.
    """
    store, _scope = _seed_scope()
    _seed_incident_shapes(store)
    scope = _EdgeDamagingScope(BOARD_ID, store)
    before_nodes = {key: dict(value) for key, value in store._board_nodes(BOARD_ID).items()}
    before_edges = [dict(edge) for edge in store._board_edges(BOARD_ID)]

    with pytest.raises(RuntimeError, match="graph_node_payload_replacement_edges_unconfirmed"):
        scope.replace_node_payload(
            "Decision",
            NODE_ID,
            {"content": "replacement that damages an edge"},
            source_session_id="durable-replay-session",
        )

    assert store._board_nodes(BOARD_ID) == before_nodes
    assert store._board_edges(BOARD_ID) == before_edges


@pytest.mark.parametrize("reserved", ("id", "source_session_id", "_type"))
def test_replacement_attrs_may_not_carry_a_structural_key(reserved: str) -> None:
    """Identity and provenance are the operation's to set, not the payload's to smuggle.

    ``_type`` matters most and is the one that was missing: the replacement spreads ``attrs``
    after it, so a payload naming ``_type`` would relabel the very node it claims only to
    rewrite, and every later lookup by type would miss it.
    """
    store, scope = _seed_scope()
    before_nodes = {key: dict(value) for key, value in store._board_nodes(BOARD_ID).items()}

    with pytest.raises(ValueError, match="must exclude id, source_session_id and _type"):
        scope.replace_node_payload(
            "Decision",
            NODE_ID,
            {"content": "smuggled", reserved: "Entity"},
            source_session_id="durable-replay-session",
        )
    assert store._board_nodes(BOARD_ID) == before_nodes


def test_store_backed_compatibility_scope_fails_closed_without_atomic_replace() -> None:
    store = InMemoryGraphStore()

    class _RawScope:
        def execute(self, *_args, **_kwargs):
            return ()

    scope = _StoreBackedGraphScope(BOARD_ID, store, _RawScope())
    # The TYPE is the assertion, not just the message: a caller has to be able to tell a missing
    # capability from any other failure, because the alternative it must not take is a
    # read-modify-write that publishes a half-replaced payload.
    with pytest.raises(
        GraphCapabilityUnavailable,
        match="graph_node_payload_replacement_capability_unavailable",
    ) as refused:
        scope.replace_node_payload(
            "Decision",
            NODE_ID,
            {"content": "must not degrade to SET"},
            source_session_id="durable-replay-session",
        )
    assert refused.value.code == "graph_capability_unavailable"


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
