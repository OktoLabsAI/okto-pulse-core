"""Tests for KG-03.6 — CognitivePendingEntityBadgeResolver (api_49fce0e1)
+ batch endpoint GET-style /api/v1/kg/cognitive-pending/badges
(api_28a22fec).

Coverage:
    * FR11/AC12 — active item → show_badge=true.
    * FR11/AC13 — terminal-only → show_badge=false, reason=terminal_status.
    * FR12/tr_c6bc7f21 — eligibility enum: spec, decision, refinement, task, test, bug.
    * FR13/br_b7535ce1/ir_21ec0034/dec_65db8384 — refinement INCLUDED.
    * tr_f5d92d97 regression — ideation, other = ineligible.
    * Severity order failed > in_progress > pending.
    * NOT_FOUND reason for refs not in the ledger.
    * Counter or_41797e76 / or_90fa2709 bounded labels.
    * br_858a0859 — no raw artifact body / source_ref / item_id labels.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.router import api_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.kg.cognitive_badge_resolver import (
    BADGE_LABEL_ACTIVE,
    CognitiveBadgeReason,
    CognitiveBadgeResolveOutcome,
    ELIGIBLE_ENTITY_TYPES,
    INELIGIBLE_ENTITY_TYPES,
    get_badge_resolve_counter_labels,
    get_badge_resolve_event_count,
    get_badge_resolve_samples,
    reset_badge_resolve_counter,
    resolve_entity_badges,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingMarker,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-kg03-6"


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03-6"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_badge_resolve_counter()
    yield
    reset_badge_resolve_counter()


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user() -> str:
        return "user-kg03-6-test"

    app.dependency_overrides[require_user] = _fake_user
    return TestClient(app)


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
    }


def _seed(base_dir: Path, sources: list[dict]) -> tuple[str, list]:
    del base_dir
    gen = generate_kg_generation_id()
    artifact_store = require_rebuild_audit_artifact_store()
    marker = CognitivePendingMarker(artifact_store=artifact_store)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_kg03_6",
    )
    store = CognitiveConsolidationItemStore(artifact_store=artifact_store)
    return gen, store.list_items(BOARD, gen)


# -------- Eligibility (FR12 + FR13 + tr_c6bc7f21) -----------------------


def test_eligible_types_include_refinement_per_FR13_and_dec_65db8384() -> None:
    """br_b7535ce1 + ir_21ec0034 + dec_65db8384 — refinement is an
    eligible badge target. The earlier exclusion (ir_aa48bd8b /
    or_9d11c2ee) is obsoleted by the authoritative model."""

    assert "refinement" in ELIGIBLE_ENTITY_TYPES
    assert ELIGIBLE_ENTITY_TYPES == frozenset({
        "spec", "decision", "refinement", "task", "test", "bug",
    })


def test_ineligible_types_are_exactly_ideation_other() -> None:
    """tr_f5d92d97 regression: ideation and other are the only ineligible
    types — refinement and decision MUST NOT be ineligible."""

    assert INELIGIBLE_ENTITY_TYPES == frozenset({"ideation", "other"})
    assert "refinement" not in INELIGIBLE_ENTITY_TYPES
    assert "decision" not in INELIGIBLE_ENTITY_TYPES


@pytest.mark.parametrize(
    "entity_type",
    ["spec", "decision", "refinement", "task", "test", "bug"],
)
def test_eligible_consolidable_entity_shows_badge_for_active_pending_item(
    isolated_base_dir: Path,
    entity_type: str,
) -> None:
    """AC12 + FR11: any eligible+consolidable source_ref with an active
    pending item gets show_badge=true with reason=active_cognitive_item."""

    _, _ = _seed(isolated_base_dir, [_row(entity_type, "s1")])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    badges, eligible = resolve_entity_badges(
        board_id=BOARD,
        source_refs=[f"{entity_type}:s1"],
        card_entity_types={f"{entity_type}:s1": entity_type},
        kg_generation_id=None,
        store=store,
    )
    badge = badges[f"{entity_type}:s1"]
    assert badge.show_badge is True
    assert badge.reason == CognitiveBadgeReason.ACTIVE_COGNITIVE_ITEM.value
    assert badge.status == "pending"
    assert badge.label == BADGE_LABEL_ACTIVE
    assert "refinement" in eligible
    assert "decision" in eligible
    assert "ideation" not in eligible


@pytest.mark.parametrize("entity_type", ["task", "test", "bug"])
def test_eligible_without_ledger_row_resolves_to_not_found(
    isolated_base_dir: Path,
    entity_type: str,
) -> None:
    """task/test/bug are ELIGIBLE. If no generation materialized an item
    for the source_ref yet, the resolver returns not_found."""

    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    badges, eligible = resolve_entity_badges(
        board_id=BOARD,
        source_refs=[f"{entity_type}:t1"],
        card_entity_types={f"{entity_type}:t1": entity_type},
        kg_generation_id=None,
        store=store,
    )
    badge = badges[f"{entity_type}:t1"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.NOT_FOUND.value
    assert badge.status is None
    # Entity type IS in the eligible set, just no ledger row.
    assert entity_type in eligible


# -------- Severity order -------------------------------------------------


def test_severity_order_failed_beats_in_progress_beats_pending(
    isolated_base_dir: Path,
) -> None:
    """When multiple items exist for the same source_ref (across
    different generations), the most severe active status wins:
    failed > in_progress > pending."""

    # Seed three generations, each with the same source_ref but different
    # status (mutated post-materialize so we can drive the picker).
    gen_a, items_a = _seed(isolated_base_dir, [_row("spec", "s1")])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen_a,
        item_id=items_a[0].item_id,
        new_status=CognitiveItemStatus.IN_PROGRESS.value,
        updated_by_agent_id="agent-1",
    )

    # Same generation - it's only one item but updated to in_progress.
    badges_a, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:s1"],
        card_entity_types={"spec:s1": "spec"},
        kg_generation_id=gen_a,
        store=store,
    )
    assert badges_a["spec:s1"].status == "in_progress"
    assert badges_a["spec:s1"].show_badge is True

    # Flip to failed → resolver picks failed over the previously-shown in_progress.
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen_a,
        item_id=items_a[0].item_id,
        new_status=CognitiveItemStatus.FAILED.value,
        updated_by_agent_id="agent-1",
        reason="adapter raised",
    )
    badges_b, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:s1"],
        card_entity_types={"spec:s1": "spec"},
        kg_generation_id=gen_a,
        store=store,
    )
    assert badges_b["spec:s1"].status == "failed"
    assert badges_b["spec:s1"].show_badge is True


# -------- Terminal status hides badge (AC13 + br_50cc5700) -------------


def test_terminal_status_hides_badge(
    isolated_base_dir: Path,
) -> None:
    """AC13: source_ref whose only items are consolidated/skipped → no badge.
    Reason=terminal_status, show_badge=false."""

    gen, items = _seed(isolated_base_dir, [_row("spec", "s1")])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:s1"],
        card_entity_types={"spec:s1": "spec"},
        kg_generation_id=gen,
        store=store,
    )
    badge = badges["spec:s1"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.TERMINAL_STATUS.value
    assert badge.status == "consolidated"
    assert badge.label == ""


# -------- Ineligible entity types (tr_f5d92d97 regression) -------------


@pytest.mark.parametrize(
    "ineligible_type",
    ["ideation", "other"],
)
def test_ineligible_entity_types_never_show_badge(
    isolated_base_dir: Path,
    ineligible_type: str,
) -> None:
    """tr_f5d92d97 regression: ideation and other are the only ineligible
    types. They always get show_badge=false with
    reason=ineligible_entity_type, even when an active cognitive item
    exists for their source_ref."""

    # Seed a pending item with the SAME source_ref to prove suppression
    # is driven by the entity type, not by the item state.
    _, _ = _seed(isolated_base_dir, [_row("spec", "x1")])
    # Pretend the same source_ref maps to an ineligible card type.
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:x1"],
        card_entity_types={"spec:x1": ineligible_type},
        kg_generation_id=None,
        store=store,
    )
    badge = badges["spec:x1"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value
    assert badge.status is None


def test_refinement_is_NOT_ineligible_per_or_90fa2709(
    isolated_base_dir: Path,
) -> None:
    """or_90fa2709 invariant: reason=ineligible_entity_type may only
    apply to ideation/other — NEVER refinement or decision."""

    _, _ = _seed(isolated_base_dir, [_row("refinement", "r1")])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["refinement:r1"],
        card_entity_types={"refinement:r1": "refinement"},
        kg_generation_id=None,
        store=store,
    )
    assert (
        badges["refinement:r1"].reason
        != CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value
    )
    # And the counter NEVER fires ineligible_entity_type for refinement.
    assert (
        get_badge_resolve_event_count(
            eligible_entity_type="refinement",
            reason=CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value,
        )
        == 0
    )


# -------- Not found ------------------------------------------------------


def test_source_ref_with_no_items_returns_not_found(
    isolated_base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:never-existed"],
        card_entity_types={"spec:never-existed": "spec"},
        kg_generation_id=None,
        store=store,
    )
    badge = badges["spec:never-existed"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.NOT_FOUND.value
    assert badge.status is None


# -------- Counter labels (or_41797e76 + or_90fa2709) -------------------


def test_counter_label_set_is_bounded(
    isolated_base_dir: Path,
) -> None:
    _, _ = _seed(isolated_base_dir, [_row("spec", "s1")])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:s1"],
        card_entity_types={"spec:s1": "spec"},
        kg_generation_id=None,
        store=store,
    )
    assert get_badge_resolve_counter_labels() == (
        "board_id_hash",
        "outcome",
        "requested_count_bucket",
        "eligible_entity_type",
        "reason",
    )
    samples = get_badge_resolve_samples()
    assert samples, "expected at least one resolve sample"
    forbidden = {
        "source_ref",
        "item_id",
        "board_id",
        "agent",
        "reason_text",
        "artifact_title",
    }
    for sample in samples:
        assert set(sample.keys()) == {
            "board_id_hash",
            "outcome",
            "requested_count_bucket",
            "eligible_entity_type",
            "reason",
        }
        assert not forbidden.intersection(sample.keys())


def test_counter_requested_count_bucket_is_bounded(
    isolated_base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    # 1 ref → bucket 1-10.
    resolve_entity_badges(
        board_id=BOARD,
        source_refs=["spec:s1"],
        card_entity_types={"spec:s1": "spec"},
        kg_generation_id=None,
        store=store,
    )
    # 20 refs → bucket 11-50.
    refs = [f"spec:s{i}" for i in range(20)]
    resolve_entity_badges(
        board_id=BOARD,
        source_refs=refs,
        card_entity_types={r: "spec" for r in refs},
        kg_generation_id=None,
        store=store,
    )
    buckets = {s["requested_count_bucket"] for s in get_badge_resolve_samples()}
    assert buckets <= {"0", "1-10", "11-50", "51-100", "101-200", "200+"}


# -------- REST endpoint --------------------------------------------------


def test_badge_endpoint_route_is_registered() -> None:
    app = FastAPI()
    app.include_router(api_router)
    paths = set(app.openapi()["paths"])
    assert "/api/v1/kg/cognitive-pending/badges" in paths


def test_endpoint_returns_contract_shape(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    _, _ = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("refinement", "r1"),
    ])
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[
            ("board_id", BOARD),
            ("source_refs", "spec:s1"),
            ("source_refs", "refinement:r1"),
            ("source_refs", "ideation:i1"),
        ],
    )
    assert response.status_code == 200, response.text
    body = response.json()
    required = {
        "board_id",
        "selected_kg_generation_id",
        "readonly",
        "eligible_entity_types",
        "badges",
    }
    assert required.issubset(body.keys())
    assert body["readonly"] is True
    assert set(body["eligible_entity_types"]) == ELIGIBLE_ENTITY_TYPES
    # Eligible refinement gets badge ON (active item exists).
    assert body["badges"]["refinement:r1"]["show_badge"] is True
    # Ineligible ideation never gets a badge (server derives type from
    # the ``ideation:`` prefix and rejects it).
    assert body["badges"]["ideation:i1"]["show_badge"] is False
    assert (
        body["badges"]["ideation:i1"]["reason"]
        == CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value
    )


def test_endpoint_rejects_empty_batch_with_400(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params={"board_id": BOARD},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "invalid_source_refs"


def test_endpoint_rejects_oversize_batch_with_400(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    params = [("board_id", BOARD)] + [
        ("source_refs", f"spec:s{i}") for i in range(201)
    ]
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=params,
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "invalid_source_refs"
    # Counter saw the invalid batch outcome.
    assert (
        get_badge_resolve_event_count(
            outcome=CognitiveBadgeResolveOutcome.INVALID_SOURCE_REFS.value
        )
        >= 1
    )


def test_endpoint_only_supports_get_not_mutating_methods(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """ir_0b66c7af + br_2065f80b: only GET is exposed; mutating verbs
    return 405/404. POST is also forbidden (the spec mandates GET)."""

    for method in ("POST", "PUT", "PATCH", "DELETE"):
        response = client.request(
            method,
            "/api/v1/kg/cognitive-pending/badges",
            params={"board_id": BOARD, "source_refs": "spec:s1"},
        )
        assert response.status_code in (405, 404), (
            f"{method} should not be a registered route, got "
            f"{response.status_code}"
        )


def test_endpoint_response_items_carry_only_contract_fields(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    _, _ = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[("board_id", BOARD), ("source_refs", "spec:s1")],
    )
    body = response.json()
    badge = body["badges"]["spec:s1"]
    expected = {"show_badge", "label", "status", "item_id", "updated_at", "reason"}
    assert set(badge.keys()) == expected
    # Storage-only fields MUST NOT leak.
    forbidden = {"board_id", "kg_generation_id", "event_ref", "reason_code", "artifact_type"}
    assert not forbidden.intersection(badge.keys())


def test_endpoint_includes_refinement_in_eligible_entity_types(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """ir_21ec0034 supersedes the obsolete refinement-excluded view."""

    _, _ = _seed(isolated_base_dir, [_row("refinement", "r1")])
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[("board_id", BOARD), ("source_refs", "refinement:r1")],
    )
    body = response.json()
    assert "refinement" in body["eligible_entity_types"]
    assert body["badges"]["refinement:r1"]["show_badge"] is True


def test_endpoint_derives_entity_type_from_source_ref_prefix(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """Server-side derivation: ``decision:d1`` collapses to entity_type
    decision (eligible) without the client sending a mapping."""

    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[
            ("board_id", BOARD),
            ("source_refs", "decision:d1"),
            ("source_refs", "malformed_no_colon"),
        ],
    )
    body = response.json()
    # decision prefix → eligible, but no ledger row exists for this ref.
    assert body["badges"]["decision:d1"]["show_badge"] is False
    assert (
        body["badges"]["decision:d1"]["reason"]
        == CognitiveBadgeReason.NOT_FOUND.value
    )
    # No-prefix string collapses to "other" → also ineligible.
    assert body["badges"]["malformed_no_colon"]["show_badge"] is False
    assert (
        body["badges"]["malformed_no_colon"]["reason"]
        == CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value
    )


# -------- Regression: existing KG-02/03 routes still up -----------------


def test_existing_kg_routes_still_registered() -> None:
    """AC11 regression — adding KG-03.6 does not unwire KG-02/03 surfaces."""

    app = FastAPI()
    app.include_router(api_router)
    paths = set(app.openapi()["paths"])
    for required in (
        "/api/v1/kg/rebuild/preflight",
        "/api/v1/kg/rebuild/confirm",
        "/api/v1/kg/rebuild/run",
        "/api/v1/kg/cognitive-pending",
        "/api/v1/kg/cognitive-pending/badges",
    ):
        assert required in paths, f"{required} unwired"
