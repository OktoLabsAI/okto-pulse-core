"""Tests for KG-02.5 deterministic structural rebuild + hash comparison.

Covers:
* TR15 — structural hash includes source refs, versions, content hashes,
  schema version, reconciliation decisions; excludes kg_generation_id,
  run_id, timestamps, random ids.
* BR br_10a33261 — equivalent source sets produce equivalent hashes;
  changed sources/decisions/schema produce mismatch.
* OR or_c67ee067 — kg_rebuild_structural_hash_total counter labels.
* OR or_a938f80b — kg_rebuild_structural_hash_mismatch_total counter labels.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.rebuild_deterministic import (
    DeterministicStructuralRebuilder,
    EXCLUDED_HASH_FIELDS,
    HashMismatchReason,
    StructuralHashStatus,
    build_structural_inputs,
    compare_structural_hashes,
    compare_structural_inputs,
    compute_source_hash,
    compute_structural_hash,
    get_structural_hash_count,
    get_structural_hash_counter_labels,
    get_structural_hash_mismatch_count,
    get_structural_hash_mismatch_counter_labels,
    reset_structural_hash_counter,
    reset_structural_hash_mismatch_counter,
)


BOARD = "board-001"


def _row(id_: str = "s1", version: str = "v1", content: str = "h1") -> dict:
    return {
        "artifact_type": "spec",
        "id": id_,
        "source_ref": f"ref:{id_}",
        "source_version": version,
        "content_hash": content,
        "created_at": "2026-05-26T00:00:00Z",  # MUST be excluded
        "status": "validated",
    }


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_structural_hash_counter()
    reset_structural_hash_mismatch_counter()
    yield
    reset_structural_hash_counter()
    reset_structural_hash_mismatch_counter()


# -------- TR15 — deterministic equivalence -------------------------------


def test_same_source_set_produces_same_hash() -> None:
    a = build_structural_inputs(sources=[_row(), _row("s2", "v2", "h2")])
    b = build_structural_inputs(
        # reverse order — must still match after canonical sort
        sources=[_row("s2", "v2", "h2"), _row()]
    )
    assert compute_structural_hash(a) == compute_structural_hash(b)


def test_change_in_source_version_changes_hash() -> None:
    a = build_structural_inputs(sources=[_row("s1", "v1", "h1")])
    b = build_structural_inputs(sources=[_row("s1", "v2", "h1")])
    assert compute_structural_hash(a) != compute_structural_hash(b)


def test_change_in_content_hash_changes_hash() -> None:
    a = build_structural_inputs(sources=[_row("s1", "v1", "h1")])
    b = build_structural_inputs(sources=[_row("s1", "v1", "h2")])
    assert compute_structural_hash(a) != compute_structural_hash(b)


def test_change_in_schema_version_changes_hash() -> None:
    a = build_structural_inputs(sources=[_row()], schema_version="0.3.5")
    b = build_structural_inputs(sources=[_row()], schema_version="0.3.6")
    assert compute_structural_hash(a) != compute_structural_hash(b)


def test_change_in_reconciliation_decision_changes_hash() -> None:
    a = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[{"kind": "keep", "node": "Spec:1"}],
    )
    b = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[{"kind": "drop", "node": "Spec:1"}],
    )
    assert compute_structural_hash(a) != compute_structural_hash(b)


def test_change_in_node_payload_changes_hash() -> None:
    a = build_structural_inputs(
        sources=[_row()],
        nodes=[{"type": "Spec", "id": "S1", "label": "alpha"}],
    )
    b = build_structural_inputs(
        sources=[_row()],
        nodes=[{"type": "Spec", "id": "S1", "label": "beta"}],
    )
    assert compute_structural_hash(a) != compute_structural_hash(b)


def test_change_in_edge_payload_changes_hash() -> None:
    a = build_structural_inputs(
        sources=[_row()],
        edges=[{"type": "REFERENCES", "from": "A", "to": "B"}],
    )
    b = build_structural_inputs(
        sources=[_row()],
        edges=[{"type": "REFERENCES", "from": "A", "to": "C"}],
    )
    assert compute_structural_hash(a) != compute_structural_hash(b)


# -------- TR15 exclusions — operational fields MUST NOT change hash -------


@pytest.mark.parametrize(
    "excluded_field",
    sorted(EXCLUDED_HASH_FIELDS),
)
def test_excluded_fields_do_not_affect_hash(excluded_field: str) -> None:
    """Adding any excluded field to the source row, decision payload,
    node or edge MUST NOT change the structural hash."""

    base_node = {"type": "Spec", "id": "S1"}
    polluted_node = {**base_node, excluded_field: "anything-different"}
    a = build_structural_inputs(sources=[_row()], nodes=[base_node])
    b = build_structural_inputs(sources=[_row()], nodes=[polluted_node])
    assert compute_structural_hash(a) == compute_structural_hash(b), (
        f"excluded field {excluded_field!r} leaked into hash"
    )


def test_source_row_created_at_is_excluded_from_hash() -> None:
    a = build_structural_inputs(
        sources=[{**_row(), "created_at": "2026-01-01T00:00:00Z"}]
    )
    b = build_structural_inputs(
        sources=[{**_row(), "created_at": "2099-12-31T23:59:59Z"}]
    )
    assert compute_structural_hash(a) == compute_structural_hash(b)


def test_excluded_keys_inside_reconciliation_decisions_are_dropped() -> None:
    a = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[{"kind": "keep", "node": "Spec:1"}],
    )
    b = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[
            {
                "kind": "keep",
                "node": "Spec:1",
                "kg_generation_id": "some-uuid",
                "run_id": "run_x",
                "started_at": "2026-05-26T00:00:00Z",
                "finished_at": "2026-05-26T00:00:05Z",
                "actor_id": "ops-1",
            }
        ],
    )
    assert compute_structural_hash(a) == compute_structural_hash(b)


# -------- compare_structural_inputs ---------------------------------------


def test_compare_inputs_equivalent_bumps_equivalent_counter() -> None:
    a = build_structural_inputs(sources=[_row()])
    b = build_structural_inputs(sources=[_row()])
    result = compare_structural_inputs(a, b, board_id=BOARD)
    assert result.equivalent is True
    assert result.mismatch_reasons == ()
    assert (
        get_structural_hash_count(
            BOARD,
            status=StructuralHashStatus.COMPARED_EQUIVALENT.value,
            deterministic=True,
        )
        == 1
    )
    assert get_structural_hash_mismatch_count(BOARD) == 0


def test_compare_inputs_source_set_changed_bumps_mismatch_counter() -> None:
    a = build_structural_inputs(sources=[_row("s1", "v1", "h1")])
    b = build_structural_inputs(sources=[_row("s1", "v2", "h1")])
    result = compare_structural_inputs(a, b, board_id=BOARD)
    assert result.equivalent is False
    assert (
        HashMismatchReason.SOURCE_SET_CHANGED.value in result.mismatch_reasons
    )
    assert (
        get_structural_hash_mismatch_count(
            BOARD, reason=HashMismatchReason.SOURCE_SET_CHANGED.value
        )
        == 1
    )


def test_compare_inputs_schema_change_flags_schema_reason() -> None:
    a = build_structural_inputs(sources=[_row()], schema_version="0.3.5")
    b = build_structural_inputs(sources=[_row()], schema_version="0.3.6")
    result = compare_structural_inputs(a, b, board_id=BOARD)
    assert result.equivalent is False
    assert (
        HashMismatchReason.SCHEMA_VERSION_CHANGED.value
        in result.mismatch_reasons
    )


def test_compare_inputs_decision_change_flags_reconciliation_reason() -> None:
    a = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[{"kind": "keep", "node": "Spec:1"}],
    )
    b = build_structural_inputs(
        sources=[_row()],
        reconciliation_decisions=[{"kind": "drop", "node": "Spec:1"}],
    )
    result = compare_structural_inputs(a, b, board_id=BOARD)
    assert result.equivalent is False
    assert (
        HashMismatchReason.RECONCILIATION_CHANGED.value
        in result.mismatch_reasons
    )


# -------- compare_structural_hashes (digest only) -------------------------


def test_compare_hashes_equivalent() -> None:
    h = "a" * 64
    result = compare_structural_hashes(
        board_id=BOARD, expected_hash=h, actual_hash=h
    )
    assert result.equivalent is True
    assert (
        get_structural_hash_count(
            BOARD,
            status=StructuralHashStatus.COMPARED_EQUIVALENT.value,
        )
        == 1
    )


def test_compare_hashes_mismatch_uses_reason_hint() -> None:
    result = compare_structural_hashes(
        board_id=BOARD,
        expected_hash="a" * 64,
        actual_hash="b" * 64,
        reason_hint=HashMismatchReason.NODES_CHANGED.value,
    )
    assert result.equivalent is False
    assert result.mismatch_reasons == (
        HashMismatchReason.NODES_CHANGED.value,
    )
    assert (
        get_structural_hash_mismatch_count(
            BOARD, reason=HashMismatchReason.NODES_CHANGED.value
        )
        == 1
    )


# -------- compute_source_hash isolates source-only changes ----------------


def test_source_hash_isolates_from_decisions() -> None:
    sources = [_row()]
    a = compute_source_hash(sources)
    b = compute_source_hash(sources)
    assert a == b
    # Adding a decision must NOT change source_hash (decisions live in
    # structural_hash, not source_hash).
    inputs_with_decision = build_structural_inputs(
        sources=sources,
        reconciliation_decisions=[{"kind": "keep", "node": "x"}],
    )
    inputs_without_decision = build_structural_inputs(sources=sources)
    assert (
        compute_structural_hash(inputs_with_decision)
        != compute_structural_hash(inputs_without_decision)
    )
    # source_hash unchanged.
    assert compute_source_hash(sources) == a


def test_source_hash_changes_when_version_changes() -> None:
    a = compute_source_hash([_row("s1", "v1", "h1")])
    b = compute_source_hash([_row("s1", "v2", "h1")])
    assert a != b


# -------- DeterministicStructuralRebuilder -------------------------------


def test_rebuilder_returns_deterministic_result_for_repeated_runs() -> None:
    rebuilder = DeterministicStructuralRebuilder()
    sources = [_row(), _row("s2", "v2", "h2")]
    r1 = rebuilder.rebuild(board_id=BOARD, sources=sources)
    r2 = rebuilder.rebuild(board_id=BOARD, sources=sources)
    assert r1.structural_hash == r2.structural_hash
    assert r1.source_hash == r2.source_hash
    assert r1.counts == {
        "sources": 2,
        "nodes": 2,
        "edges": 0,
        "reconciliation_decisions": 0,
    }
    # Two COMPUTED bumps.
    assert (
        get_structural_hash_count(
            BOARD,
            status=StructuralHashStatus.COMPUTED.value,
            deterministic=True,
        )
        == 2
    )


def test_rebuilder_includes_working_artifacts_and_filters_decision_rows() -> None:
    """Working graph artifacts are deterministic rebuild inputs. Decision
    source rows stay excluded because they are emitted from the owning spec
    payload and would otherwise duplicate materialization."""

    rebuilder = DeterministicStructuralRebuilder()
    sources = [
        _row("s1", "v1", "h1"),
        {
            "artifact_type": "refinement",
            "id": "r1",
            "source_ref": "refinement:r1",
            "source_version": "1",
            "content_hash": "refinement-hash",
        },
        {
            "artifact_type": "task",
            "id": "t1",
            "source_ref": "task:t1",
            "source_version": "1",
            "content_hash": "task-hash",
        },
        {
            "artifact_type": "decision",
            "id": "d1",
            "source_ref": "decision:s1:idx-0-abc",
            "source_version": "1",
            "content_hash": "decision-hash",
        },
    ]

    result = rebuilder.rebuild(board_id=BOARD, sources=sources)
    assert result.counts["sources"] == 3
    assert [row["artifact_type"] for row in result.inputs.sources] == [
        "refinement",
        "spec",
        "task",
    ]
    assert "refinement:r1" in {
        row["source_ref"] for row in result.inputs.sources
    }
    assert "decision:s1:idx-0-abc" not in {
        row["source_ref"] for row in result.inputs.sources
    }


def test_rebuilder_step_adapter_returns_rebuild_step_result() -> None:
    from okto_pulse.core.kg.rebuild_service import (
        RebuildStepInput,
        RebuildStepResult,
    )

    rebuilder = DeterministicStructuralRebuilder()
    adapter = rebuilder.as_rebuild_step_adapter()

    # Wrap the input with the optional ``sources_payload`` the
    # production wiring layer supplies via closure. The adapter accepts
    # any attribute on the request — we use a simple namespace.
    class _Req:
        board_id = BOARD
        manifest_ref = "rebuild_manifest_abc"
        source_set_hash = "f" * 64
        actor_id = "user-1"
        operation = "rebuild"
        owner_token = "tok"
        previous_kg_generation_id = None
        candidate_kg_generation_id = "11111111-1111-4111-8111-111111111111"
        sources_payload = (_row(), _row("s2", "v2", "h2"))

    result = adapter(_Req())
    assert isinstance(result, RebuildStepResult)
    assert result.ok is True
    assert result.structural_hash and len(result.structural_hash) == 64
    assert result.source_hash and len(result.source_hash) == 64
    assert result.counts["nodes"] == 2
    assert result.drilldown["node_count"] == 2
    assert result.current_kg_generation_id == (
        "11111111-1111-4111-8111-111111111111"
    )


def test_counter_labels_are_bounded() -> None:
    assert get_structural_hash_counter_labels() == (
        "board_id",
        "status",
        "deterministic",
    )
    assert get_structural_hash_mismatch_counter_labels() == (
        "board_id",
        "reason",
    )


# -------- val_ebffe9ce regression: fail-closed adapter --------------------


def test_adapter_fails_closed_without_sources_payload() -> None:
    """val_ebffe9ce issue 1: bare RebuildStepInput (no sources_payload
    attribute, no source_resolver wired) MUST NOT silently produce an
    ok=True empty graph. The adapter must fail-closed with
    ``ok=False, detail="source_payload_required"``."""

    from okto_pulse.core.kg.rebuild_service import RebuildStepInput

    rebuilder = DeterministicStructuralRebuilder()
    adapter = rebuilder.as_rebuild_step_adapter()  # no resolver

    req = RebuildStepInput(
        board_id=BOARD,
        manifest_ref="rebuild_manifest_xyz",
        source_set_hash="f" * 64,
        actor_id="user-1",
        operation="rebuild",
        owner_token="tok",
        previous_kg_generation_id=None,
        candidate_kg_generation_id="11111111-1111-4111-8111-111111111111",
    )
    result = adapter(req)
    assert result.ok is False
    assert result.detail == "source_payload_required"
    assert result.current_kg_generation_id is None
    assert result.structural_hash is None
    assert result.source_hash is None


def test_adapter_uses_source_resolver_when_provided() -> None:
    """val_ebffe9ce issue 1: with a ``source_resolver`` wired, the
    adapter materialises the resolved sources deterministically."""

    from okto_pulse.core.kg.rebuild_service import RebuildStepInput

    resolved: list = [_row(), _row("s2", "v2", "h2")]

    def _resolver(req):
        assert req.manifest_ref == "rebuild_manifest_xyz"
        return resolved

    rebuilder = DeterministicStructuralRebuilder()
    adapter = rebuilder.as_rebuild_step_adapter(source_resolver=_resolver)

    req = RebuildStepInput(
        board_id=BOARD,
        manifest_ref="rebuild_manifest_xyz",
        source_set_hash="f" * 64,
        actor_id="user-1",
        operation="rebuild",
        owner_token="tok",
        previous_kg_generation_id=None,
        candidate_kg_generation_id="11111111-1111-4111-8111-111111111111",
    )
    result = adapter(req)
    assert result.ok is True
    assert result.structural_hash and len(result.structural_hash) == 64
    assert result.source_hash and len(result.source_hash) == 64
    assert result.counts["sources"] == 2
    assert result.counts["nodes"] == 2


def test_adapter_source_resolver_exception_fails_closed() -> None:
    """If the resolver raises, the adapter must NOT swallow it into an
    empty rebuild — it must return ok=False with a typed detail so
    KGRebuildService finalises on the FAILED path."""

    from okto_pulse.core.kg.rebuild_service import RebuildStepInput

    def _boom(_req):
        raise RuntimeError("manifest_io_error")

    rebuilder = DeterministicStructuralRebuilder()
    adapter = rebuilder.as_rebuild_step_adapter(source_resolver=_boom)

    req = RebuildStepInput(
        board_id=BOARD,
        manifest_ref="rebuild_manifest_xyz",
        source_set_hash="f" * 64,
        actor_id="user-1",
        operation="rebuild",
        owner_token="tok",
        previous_kg_generation_id=None,
        candidate_kg_generation_id="11111111-1111-4111-8111-111111111111",
    )
    result = adapter(req)
    assert result.ok is False
    assert result.detail == "source_resolver_failed:RuntimeError"


def test_adapter_empty_source_list_from_resolver_is_deterministic() -> None:
    """Distinct from missing payload: the resolver may legitimately
    return an empty sequence (board has no eligible sources). That path
    is ok=True with an empty deterministic hash so the report records
    the run; only the missing-payload path is fail-closed."""

    from okto_pulse.core.kg.rebuild_service import RebuildStepInput

    rebuilder = DeterministicStructuralRebuilder()
    adapter = rebuilder.as_rebuild_step_adapter(
        source_resolver=lambda _req: []
    )
    req = RebuildStepInput(
        board_id=BOARD,
        manifest_ref="rebuild_manifest_xyz",
        source_set_hash="f" * 64,
        actor_id="user-1",
        operation="rebuild",
        owner_token="tok",
        previous_kg_generation_id=None,
        candidate_kg_generation_id="11111111-1111-4111-8111-111111111111",
    )
    result = adapter(req)
    assert result.ok is True
    assert result.counts == {
        "sources": 0,
        "nodes": 0,
        "edges": 0,
        "reconciliation_decisions": 0,
    }


# -------- val_ebffe9ce regression: bounded reason_hint -------------------


def test_compare_hashes_coerces_arbitrary_reason_hint_to_unknown() -> None:
    """val_ebffe9ce issue 2: ``reason_hint`` may come from operator
    input; the counter label must stay closed (OR or_a938f80b). Any
    value outside ``HashMismatchReason`` becomes ``unknown``."""

    from okto_pulse.core.kg.rebuild_deterministic import (
        get_structural_hash_mismatch_samples,
    )

    result = compare_structural_hashes(
        board_id=BOARD,
        expected_hash="a" * 64,
        actual_hash="b" * 64,
        reason_hint="actor_id=user@example.com",
    )
    assert result.equivalent is False
    assert result.mismatch_reasons == (HashMismatchReason.UNKNOWN.value,)
    samples = get_structural_hash_mismatch_samples()
    # No sample carries the raw arbitrary string.
    assert all(
        s["reason"] != "actor_id=user@example.com" for s in samples
    )
    # The 'unknown' bucket received the bump.
    assert (
        get_structural_hash_mismatch_count(
            BOARD, reason=HashMismatchReason.UNKNOWN.value
        )
        == 1
    )


def test_compare_hashes_accepts_canonical_reason_enum_value() -> None:
    """Canonical enum values pass through unchanged."""

    result = compare_structural_hashes(
        board_id=BOARD,
        expected_hash="a" * 64,
        actual_hash="b" * 64,
        reason_hint=HashMismatchReason.EDGES_CHANGED.value,
    )
    assert result.equivalent is False
    assert result.mismatch_reasons == (
        HashMismatchReason.EDGES_CHANGED.value,
    )
    assert (
        get_structural_hash_mismatch_count(
            BOARD, reason=HashMismatchReason.EDGES_CHANGED.value
        )
        == 1
    )


def test_compare_hashes_none_reason_falls_back_to_unknown() -> None:
    result = compare_structural_hashes(
        board_id=BOARD,
        expected_hash="a" * 64,
        actual_hash="b" * 64,
        reason_hint=None,
    )
    assert result.equivalent is False
    assert result.mismatch_reasons == (HashMismatchReason.UNKNOWN.value,)
    assert (
        get_structural_hash_mismatch_count(
            BOARD, reason=HashMismatchReason.UNKNOWN.value
        )
        == 1
    )
